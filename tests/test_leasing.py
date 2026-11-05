import threading
import unittest

from support import START, QueueTestCase

from jobqueue import (
    AttemptOutcome,
    FixedBackoff,
    InvalidTransition,
    JobNotFound,
    JobQueue,
    JobState,
    LeaseLost,
    ManualClock,
    PayloadError,
)


class LeaseOrderTests(QueueTestCase):
    def test_higher_priority_first(self):
        low = self.enqueue(priority=1)
        high = self.enqueue(priority=10)
        order = [self.lease_one().id, self.lease_one().id]
        self.assertEqual(order, [high.id, low.id])

    def test_same_priority_runs_by_due_time_then_id(self):
        later = self.enqueue(run_at=START - 1)
        earlier = self.enqueue(run_at=START - 5)
        tied = self.enqueue(run_at=START - 5)
        order = [job.id for job in self.queue.lease("w", limit=3)]
        self.assertEqual(order, [earlier.id, tied.id, later.id])

    def test_delayed_job_waits_until_due(self):
        job = self.enqueue(delay=10)
        self.assertEqual(self.queue.lease("w"), [])
        self.clock.advance(9.999)
        self.assertEqual(self.queue.lease("w"), [])
        self.clock.advance(0.001)
        self.assertEqual(self.lease_one().id, job.id)

    def test_higher_priority_future_job_does_not_block_a_ready_one(self):
        self.enqueue(priority=100, delay=60)
        ready = self.enqueue()
        self.assertEqual(self.lease_one().id, ready.id)

    def test_queue_filter(self):
        self.enqueue(queue="mail")
        other = self.enqueue(queue="reports")
        self.assertEqual(self.lease_one(queues=["reports"]).id, other.id)
        self.assertEqual(self.queue.lease("w", queues=["reports"]), [])

    def test_no_queues_means_nothing_and_none_means_all(self):
        self.enqueue(queue="anything")
        self.assertEqual(self.queue.lease("w", queues=[]), [])
        self.assertEqual(len(self.queue.lease("w", queues=None)), 1)

    def test_limit(self):
        for _ in range(5):
            self.enqueue()
        self.assertEqual(len(self.queue.lease("w", limit=3)), 3)
        self.assertEqual(len(self.queue.lease("w", limit=3)), 2)
        with self.assertRaises(ValueError):
            self.queue.lease("w", limit=0)

    def test_leased_job_fields(self):
        self.enqueue()
        job = self.lease_one("worker-7", lease_seconds=40)
        self.assertEqual(job.state, JobState.LEASED)
        self.assertEqual(job.attempts, 1)
        self.assertEqual(job.lease_owner, "worker-7")
        self.assertEqual(job.lease_expires_at, START + 40)
        self.assertTrue(job.lease_token)

    def test_each_lease_gets_a_new_token(self):
        self.enqueue()
        first = self.lease_one(lease_seconds=1)
        self.clock.advance(1)
        self.queue.reclaim_expired()
        self.clock.advance(3600)
        second = self.lease_one()
        self.assertNotEqual(first.lease_token, second.lease_token)


class LeaseLossTests(QueueTestCase):
    def test_complete_with_live_lease(self):
        self.enqueue()
        job = self.lease_one()
        self.clock.advance(5)
        done = self.queue.complete(job.id, job.lease_token, {"ok": 1})
        self.assertEqual(done.state, JobState.SUCCEEDED)
        self.assertEqual(done.result, {"ok": 1})
        self.assertEqual(done.finished_at, START + 5)
        self.assertIsNone(done.lease_token)
        self.assertIsNone(done.lease_owner)

    def test_complete_after_expiry_raises_even_if_not_yet_reclaimed(self):
        self.enqueue()
        job = self.lease_one(lease_seconds=10)
        self.clock.advance(11)
        with self.assertRaises(LeaseLost):
            self.queue.complete(job.id, job.lease_token)
        self.assertEqual(self.queue.get(job.id).state, JobState.LEASED)

    def test_expiry_boundary_is_exclusive(self):
        self.enqueue()
        job = self.lease_one(lease_seconds=10)
        self.clock.advance(10)
        with self.assertRaises(LeaseLost):
            self.queue.heartbeat(job.id, job.lease_token)

    def test_stale_worker_cannot_touch_the_new_holders_lease(self):
        self.enqueue()
        stale = self.lease_one("old", lease_seconds=10)
        self.queue._backoff = FixedBackoff(0)
        self.clock.advance(10)
        current = self.lease_one("new", lease_seconds=10)
        self.assertEqual(current.id, stale.id)
        self.assertEqual(current.attempts, 2)

        for call in (
            lambda: self.queue.complete(stale.id, stale.lease_token),
            lambda: self.queue.fail(stale.id, stale.lease_token, "late"),
            lambda: self.queue.heartbeat(stale.id, stale.lease_token),
            lambda: self.queue.bury(stale.id, stale.lease_token, "late"),
            lambda: self.queue.release(stale.id, stale.lease_token),
        ):
            with self.assertRaises(LeaseLost):
                call()

        self.assertEqual(self.queue.get(current.id).lease_owner, "new")
        self.assertEqual(
            self.queue.complete(current.id, current.lease_token).state, JobState.SUCCEEDED
        )

    def test_wrong_token_and_unleased_job(self):
        job = self.enqueue()
        with self.assertRaises(LeaseLost):
            self.queue.complete(job.id, "made-up")
        leased = self.lease_one()
        with self.assertRaises(LeaseLost):
            self.queue.complete(leased.id, "made-up")

    def test_completing_twice_raises(self):
        self.enqueue()
        job = self.lease_one()
        self.queue.complete(job.id, job.lease_token)
        with self.assertRaises(LeaseLost):
            self.queue.complete(job.id, job.lease_token)

    def test_purged_job_reports_lost_lease(self):
        job = self.enqueue()
        leased = self.lease_one()
        self.queue.cancel(job.id)
        self.queue.prune(states=["cancelled"])
        with self.assertRaises(LeaseLost):
            self.queue.complete(leased.id, leased.lease_token)

    def test_unserialisable_result_leaves_the_lease_intact(self):
        self.enqueue()
        job = self.lease_one()
        with self.assertRaises(PayloadError):
            self.queue.complete(job.id, job.lease_token, object())
        self.assertEqual(self.queue.get(job.id).state, JobState.LEASED)
        self.queue.complete(job.id, job.lease_token, "fine")


class HeartbeatTests(QueueTestCase):
    def test_extends_from_now(self):
        self.enqueue()
        job = self.lease_one(lease_seconds=10)
        self.clock.advance(8)
        expires = self.queue.heartbeat(job.id, job.lease_token, extend=10)
        self.assertEqual(expires, START + 18)
        self.clock.advance(9)
        self.queue.complete(job.id, job.lease_token)

    def test_default_extension_is_the_queue_lease(self):
        queue = self.open_queue(lease_seconds=33)
        queue.enqueue("h")
        job = queue.lease("w")[0]
        self.assertEqual(queue.heartbeat(job.id, job.lease_token), START + 33)

    def test_never_shortens(self):
        self.enqueue()
        job = self.lease_one(lease_seconds=100)
        self.assertEqual(self.queue.heartbeat(job.id, job.lease_token, extend=1), START + 100)

    def test_bad_extension(self):
        self.enqueue()
        job = self.lease_one()
        with self.assertRaises(ValueError):
            self.queue.heartbeat(job.id, job.lease_token, extend=0)


class ReclaimTests(QueueTestCase):
    def test_reclaim_counts_as_an_attempt_and_applies_backoff(self):
        self.enqueue(backoff="fixed:30", max_attempts=3)
        job = self.lease_one(lease_seconds=10)
        self.clock.advance(10)
        reclaimed = self.queue.reclaim_expired()
        self.assertEqual([j.id for j in reclaimed], [job.id])
        self.assertEqual(reclaimed[0].state, JobState.QUEUED)
        self.assertEqual(reclaimed[0].attempts, 1)
        self.assertEqual(reclaimed[0].run_at, START + 40)
        self.assertIn("lease expired", reclaimed[0].last_error)
        self.assertIsNone(reclaimed[0].lease_token)

    def test_reclaim_with_no_attempts_left_buries(self):
        self.enqueue(max_attempts=1)
        job = self.lease_one(lease_seconds=10)
        self.clock.advance(10)
        reclaimed = self.queue.reclaim_expired()
        self.assertEqual(reclaimed[0].state, JobState.DEAD)
        self.assertEqual(self.queue.dead.count(), 1)
        self.assertEqual(self.queue.attempts(job.id)[-1].outcome, AttemptOutcome.EXPIRED)

    def test_lease_reclaims_expired_jobs_first(self):
        self.enqueue(backoff="fixed:0")
        first = self.lease_one(lease_seconds=5)
        self.clock.advance(5)
        again = self.lease_one()
        self.assertEqual(again.id, first.id)
        self.assertEqual(again.attempts, 2)

    def test_live_leases_are_left_alone(self):
        self.enqueue()
        self.lease_one(lease_seconds=10)
        self.clock.advance(9)
        self.assertEqual(self.queue.reclaim_expired(), [])

    def test_max_attempts_counts_every_kind_of_failure(self):
        self.enqueue(backoff="fixed:0", max_attempts=3)
        job = self.lease_one(lease_seconds=5)
        self.queue.fail(job.id, job.lease_token, "first")
        job = self.lease_one(lease_seconds=5)
        self.clock.advance(5)
        self.queue.reclaim_expired()
        job = self.lease_one(lease_seconds=5)
        self.assertEqual(job.attempts, 3)
        self.assertEqual(self.queue.fail(job.id, job.lease_token, "third").state, JobState.DEAD)


class FailAndReleaseTests(QueueTestCase):
    def test_fail_requeues_with_explicit_delay(self):
        self.enqueue()
        job = self.lease_one()
        retried = self.queue.fail(job.id, job.lease_token, "oops", delay=12)
        self.assertEqual(retried.state, JobState.QUEUED)
        self.assertEqual(retried.run_at, START + 12)
        self.assertEqual(retried.last_error, "oops")

    def test_fail_on_the_last_attempt_buries(self):
        self.enqueue(max_attempts=1)
        job = self.lease_one()
        dead = self.queue.fail(job.id, job.lease_token, "oops")
        self.assertEqual(dead.state, JobState.DEAD)
        self.assertEqual(dead.finished_at, START)

    def test_success_clears_the_previous_error(self):
        self.enqueue(backoff="fixed:0")
        job = self.lease_one()
        self.queue.fail(job.id, job.lease_token, "oops")
        job = self.lease_one()
        self.assertEqual(job.last_error, "oops")
        self.assertIsNone(self.queue.complete(job.id, job.lease_token).last_error)

    def test_huge_error_keeps_its_tail(self):
        self.enqueue()
        job = self.lease_one()
        error = "x" * 20000 + "the real cause"
        stored = self.queue.fail(job.id, job.lease_token, error).last_error
        self.assertLess(len(stored), 9000)
        self.assertTrue(stored.startswith("[truncated]"))
        self.assertTrue(stored.endswith("the real cause"))

    def test_negative_delay_is_refused(self):
        self.enqueue()
        job = self.lease_one()
        with self.assertRaises(ValueError):
            self.queue.fail(job.id, job.lease_token, "x", delay=-1)

    def test_release_gives_the_attempt_back(self):
        self.enqueue(max_attempts=1)
        job = self.lease_one()
        released = self.queue.release(job.id, job.lease_token, delay=3)
        self.assertEqual(released.state, JobState.QUEUED)
        self.assertEqual(released.attempts, 0)
        self.assertEqual(released.run_at, START + 3)
        self.clock.advance(3)
        self.assertEqual(self.lease_one().attempts, 1)

    def test_history_records_each_attempt(self):
        self.enqueue(backoff="fixed:0", max_attempts=3)
        job = self.lease_one("a")
        self.clock.advance(2)
        self.queue.fail(job.id, job.lease_token, "boom")
        job = self.lease_one("b")
        self.queue.release(job.id, job.lease_token)
        job = self.lease_one("c")
        self.queue.complete(job.id, job.lease_token)

        history = self.queue.attempts(job.id)
        self.assertEqual([h.worker for h in history], ["a", "b", "c"])
        self.assertEqual([h.attempt for h in history], [1, 2, 2])
        self.assertEqual(
            [h.outcome for h in history],
            [AttemptOutcome.RETRIED, AttemptOutcome.RELEASED, AttemptOutcome.SUCCEEDED],
        )
        self.assertEqual(history[0].error, "boom")
        self.assertEqual(history[0].ended_at - history[0].started_at, 2)


class CancelTests(QueueTestCase):
    def test_cancel_queued(self):
        job = self.enqueue()
        cancelled = self.queue.cancel(job.id)
        self.assertEqual(cancelled.state, JobState.CANCELLED)
        self.assertEqual(self.queue.lease("w"), [])

    def test_cancel_leased_takes_the_lease_away(self):
        self.enqueue()
        job = self.lease_one()
        self.queue.cancel(job.id)
        with self.assertRaises(LeaseLost):
            self.queue.complete(job.id, job.lease_token)
        self.assertEqual(self.queue.attempts(job.id)[0].outcome, AttemptOutcome.CANCELLED)

    def test_cancel_finished_job_is_refused(self):
        self.enqueue()
        job = self.lease_one()
        self.queue.complete(job.id, job.lease_token)
        with self.assertRaises(InvalidTransition):
            self.queue.cancel(job.id)

    def test_cancel_unknown_job(self):
        with self.assertRaises(JobNotFound):
            self.queue.cancel(404)


class ReadingAndPruningTests(QueueTestCase):
    def test_get_unknown_job(self):
        with self.assertRaises(JobNotFound):
            self.queue.get(404)

    def test_list_filters_newest_first_with_paging(self):
        ids = [self.enqueue("a", queue="q1").id, self.enqueue("b", queue="q2").id]
        ids.append(self.enqueue("a", queue="q2").id)
        self.assertEqual([j.id for j in self.queue.list_jobs()], ids[::-1])
        self.assertEqual([j.id for j in self.queue.list_jobs(handler="a")], [ids[2], ids[0]])
        self.assertEqual([j.id for j in self.queue.list_jobs(queue="q1")], [ids[0]])
        self.assertEqual([j.id for j in self.queue.list_jobs(limit=1, offset=1)], [ids[1]])
        self.assertEqual(self.queue.list_jobs(state="dead"), [])
        with self.assertRaises(ValueError):
            self.queue.list_jobs(state="sleeping")

    def test_prune_by_age_and_state(self):
        self.enqueue()
        old = self.lease_one()
        self.queue.complete(old.id, old.lease_token)
        self.clock.advance(100)
        keep = self.enqueue()
        self.queue.cancel(keep.id)

        self.assertEqual(self.queue.prune(older_than=50), 1)
        with self.assertRaises(JobNotFound):
            self.queue.get(old.id)
        self.assertEqual(self.queue.attempts(old.id), [])
        self.assertEqual(self.queue.get(keep.id).state, JobState.CANCELLED)

    def test_prune_leaves_dead_jobs_by_default_and_refuses_active_states(self):
        self.enqueue(max_attempts=1)
        job = self.lease_one()
        self.queue.fail(job.id, job.lease_token, "x")
        self.assertEqual(self.queue.prune(), 0)
        with self.assertRaises(InvalidTransition):
            self.queue.prune(states=["queued"])
        self.assertEqual(self.queue.prune(states=["dead"]), 1)


class ConcurrencyTests(QueueTestCase):
    def test_threads_never_lease_a_job_twice(self):
        total = 200
        for _ in range(total):
            self.enqueue()

        leased: list[int] = []
        lock = threading.Lock()
        errors = []

        def drain(name: str) -> None:
            try:
                while True:
                    jobs = self.queue.lease(name, limit=3)
                    if not jobs:
                        return
                    for job in jobs:
                        self.queue.complete(job.id, job.lease_token)
                    with lock:
                        leased.extend(job.id for job in jobs)
            except Exception as exc:  # collected for the assertion below
                errors.append(exc)

        threads = [threading.Thread(target=drain, args=(f"w{i}",)) for i in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(leased), total)
        self.assertEqual(len(set(leased)), total)

    def test_two_queue_objects_on_one_file_see_each_other(self):
        other = self.open_queue()
        job = other.enqueue("h").job
        self.assertEqual(self.lease_one().id, job.id)
        self.assertEqual(other.lease("w2"), [])


class MemoryDatabaseTests(unittest.TestCase):
    def test_full_cycle_and_threads_in_memory(self):
        with JobQueue(":memory:", clock=ManualClock(0.0)) as queue:
            for _ in range(20):
                queue.enqueue("h")
            done = []

            def drain():
                while jobs := queue.lease("w"):
                    done.append(queue.complete(jobs[0].id, jobs[0].lease_token).id)

            threads = [threading.Thread(target=drain) for _ in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(sorted(done), list(range(1, 21)))


if __name__ == "__main__":
    unittest.main()
