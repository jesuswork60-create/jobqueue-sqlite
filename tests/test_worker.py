import sqlite3
import unittest
from unittest import mock

from support import START, QueueTestCase

from jobqueue import (
    Abandon,
    FixedBackoff,
    HandlerRegistry,
    JobState,
    LeaseLost,
    Retry,
    Worker,
)


class WorkerTestCase(QueueTestCase):
    def setUp(self):
        super().setUp()
        self.registry = HandlerRegistry()
        self.events = []

    def worker(self, **options):
        options.setdefault("sleep", self.clock.advance)
        options.setdefault("on_event", self.events.append)
        options.setdefault("worker_id", "test-worker")
        return Worker(self.queue, self.registry, **options)

    def event_kinds(self):
        return [event.kind for event in self.events]


class OutcomeTests(WorkerTestCase):
    def test_success_stores_the_result(self):
        self.registry.register("double", lambda payload: payload["n"] * 2)
        job = self.enqueue("double", {"n": 21})
        stats = self.worker().run(until_idle=True)
        done = self.queue.get(job.id)
        self.assertEqual(done.state, JobState.SUCCEEDED)
        self.assertEqual(done.result, 42)
        self.assertEqual(stats.succeeded, 1)
        self.assertEqual(self.event_kinds(), ["started", "succeeded"])

    def test_exception_is_retried_with_the_job_backoff(self):
        def boom(payload):
            raise ValueError("kaboom")

        self.registry.register("boom", boom)
        job = self.enqueue("boom", backoff="fixed:30")
        stats = self.worker().run(until_idle=True)

        retried = self.queue.get(job.id)
        self.assertEqual(retried.state, JobState.QUEUED)
        self.assertEqual(retried.run_at, START + 30)
        self.assertIn("ValueError: kaboom", retried.last_error)
        self.assertIn("Traceback", retried.last_error)
        self.assertEqual(stats.retried, 1)

    def test_attempts_run_out_into_the_dead_letters(self):
        calls = []

        def boom(payload):
            calls.append(1)
            raise ValueError("always")

        self.registry.register("boom", boom)
        job = self.enqueue("boom", max_attempts=3, backoff="fixed:10")
        stats = self.worker().run(max_jobs=3)
        self.assertEqual(len(calls), 3)
        self.assertEqual(self.queue.get(job.id).state, JobState.DEAD)
        self.assertEqual((stats.retried, stats.dead), (2, 1))

    def test_retry_exception_with_delay_beats_every_policy(self):
        def later(payload):
            raise Retry(delay=77, reason="not yet")

        self.registry.register("later", later, backoff=FixedBackoff(1))
        job = self.enqueue("later", backoff="fixed:2")
        self.worker().run(until_idle=True)
        retried = self.queue.get(job.id)
        self.assertEqual(retried.run_at, START + 77)
        self.assertEqual(retried.last_error, "not yet")

    def test_retry_without_delay_uses_the_policy(self):
        def later(payload):
            raise Retry()

        self.registry.register("later", later)
        job = self.enqueue("later", backoff="fixed:2")
        self.worker().run(until_idle=True)
        self.assertEqual(self.queue.get(job.id).run_at, START + 2)

    def test_retry_exception_on_the_last_attempt_buries(self):
        def later(payload):
            raise Retry(delay=1)

        self.registry.register("later", later)
        job = self.enqueue("later", max_attempts=1)
        stats = self.worker().run(until_idle=True)
        self.assertEqual(self.queue.get(job.id).state, JobState.DEAD)
        self.assertEqual(stats.dead, 1)

    def test_negative_retry_delay_is_refused_when_raised(self):
        with self.assertRaises(ValueError):
            Retry(delay=-1)

    def test_abandon_buries_with_attempts_left(self):
        def give_up(payload):
            raise Abandon("this payload is hopeless")

        self.registry.register("give_up", give_up)
        job = self.enqueue("give_up", max_attempts=5)
        stats = self.worker().run(until_idle=True)
        dead = self.queue.get(job.id)
        self.assertEqual(dead.state, JobState.DEAD)
        self.assertEqual(dead.attempts, 1)
        self.assertEqual(dead.last_error, "this payload is hopeless")
        self.assertEqual(stats.dead, 1)
        self.assertEqual(self.event_kinds(), ["started", "dead"])

    def test_handler_backoff_beats_the_job_policy(self):
        def boom(payload):
            raise ValueError("x")

        self.registry.register("boom", boom, backoff=FixedBackoff(11))
        job = self.enqueue("boom", backoff="fixed:2")
        self.worker().run(until_idle=True)
        self.assertEqual(self.queue.get(job.id).run_at, START + 11)

    def test_unknown_handler_fails_like_any_error(self):
        job = self.enqueue("nobody.home", max_attempts=1)
        self.worker().run(until_idle=True)
        dead = self.queue.get(job.id)
        self.assertEqual(dead.state, JobState.DEAD)
        self.assertIn("no handler registered", dead.last_error)

    def test_unserialisable_result_counts_as_failure(self):
        self.registry.register("weird", lambda payload: object())
        job = self.enqueue("weird", max_attempts=1)
        stats = self.worker().run(until_idle=True)
        dead = self.queue.get(job.id)
        self.assertEqual(dead.state, JobState.DEAD)
        self.assertIn("not JSON", dead.last_error)
        self.assertEqual(stats.succeeded, 0)

    def test_handlers_own_sqlite_error_is_a_normal_failure(self):
        def broken(payload):
            raise sqlite3.OperationalError("the application's own database is down")

        self.registry.register("broken", broken)
        job = self.enqueue("broken", backoff="fixed:0")
        stats = self.worker().run(max_jobs=1)
        retried = self.queue.get(job.id)
        self.assertEqual(retried.state, JobState.QUEUED)
        self.assertIn("own database is down", retried.last_error)
        self.assertEqual(stats.database_errors, 0)

    def test_keyboard_interrupt_releases_the_job_and_propagates(self):
        def interrupted(payload):
            raise KeyboardInterrupt

        self.registry.register("interrupted", interrupted)
        job = self.enqueue("interrupted", max_attempts=1)
        worker = self.worker()
        with self.assertRaises(KeyboardInterrupt):
            worker.run(until_idle=True)
        released = self.queue.get(job.id)
        self.assertEqual(released.state, JobState.QUEUED)
        self.assertEqual(released.attempts, 0)
        self.assertEqual(worker.stats.released, 1)


class ContextTests(WorkerTestCase):
    def test_context_properties(self):
        seen = {}

        def inspect_me(payload, ctx):
            seen.update(
                job_id=ctx.job_id,
                payload=ctx.payload,
                attempt=ctx.attempt,
                worker=ctx.worker_id,
                now=ctx.clock.now(),
                stop=ctx.stop_requested,
                lost=ctx.lease_lost,
            )

        self.registry.register("inspect", inspect_me)
        job = self.enqueue("inspect", {"k": "v"})
        self.worker().run(until_idle=True)
        self.assertEqual(
            seen,
            {
                "job_id": job.id,
                "payload": {"k": "v"},
                "attempt": 1,
                "worker": "test-worker",
                "now": START,
                "stop": False,
                "lost": False,
            },
        )

    def test_attempt_number_grows(self):
        attempts = []

        def boom(payload, ctx):
            attempts.append(ctx.attempt)
            raise Retry(delay=0)

        self.registry.register("boom", boom)
        self.enqueue("boom", max_attempts=3)
        self.worker().run(until_idle=True)
        self.assertEqual(attempts, [1, 2, 3])

    def test_heartbeat_keeps_a_long_job_alive(self):
        def long_job(payload, ctx):
            for _ in range(5):
                self.clock.advance(8)
                ctx.heartbeat()

        self.registry.register("long", long_job)
        job = self.enqueue("long")
        stats = self.worker(lease_seconds=10).run(until_idle=True)
        self.assertEqual(self.queue.get(job.id).state, JobState.SUCCEEDED)
        self.assertEqual(stats.lost, 0)

    def test_outliving_the_lease_without_heartbeat_is_lost(self):
        def slow(payload):
            self.clock.advance(11)
            return "too late"

        self.registry.register("slow", slow)
        job = self.enqueue("slow", backoff="fixed:0")
        stats = self.worker(lease_seconds=10).run(max_jobs=1)
        self.assertEqual(stats.lost, 1)
        self.assertEqual(stats.succeeded, 0)
        self.assertEqual(self.event_kinds(), ["started", "lost"])
        self.assertIsNone(self.queue.get(job.id).result)

    def test_heartbeat_after_expiry_raises_and_is_absorbed(self):
        observed = []

        def slow(payload, ctx):
            self.clock.advance(11)
            try:
                ctx.heartbeat()
            finally:
                observed.append(ctx.lease_lost)

        self.registry.register("slow", slow)
        self.enqueue("slow")
        stats = self.worker(lease_seconds=10).run(max_jobs=1)
        self.assertEqual(observed, [True])
        self.assertEqual(stats.lost, 1)

    def test_cancelling_a_running_job_loses_the_lease(self):
        def cancel_self(payload, ctx):
            self.queue.cancel(ctx.job_id)
            return "ignored"

        self.registry.register("cancel_self", cancel_self)
        job = self.enqueue("cancel_self")
        stats = self.worker().run(until_idle=True)
        self.assertEqual(self.queue.get(job.id).state, JobState.CANCELLED)
        self.assertEqual(stats.lost, 1)

    def test_failing_after_losing_the_lease_changes_nothing(self):
        def slow_failure(payload):
            self.clock.advance(11)
            raise ValueError("late failure")

        self.registry.register("slow", slow_failure)
        job = self.enqueue("slow", backoff="fixed:0")
        stats = self.worker(lease_seconds=10).run(max_jobs=1)
        self.assertEqual(stats.lost, 1)
        self.assertEqual(stats.retried, 0)
        self.assertEqual(self.queue.get(job.id).attempts, 1)


class LoopTests(WorkerTestCase):
    def test_until_idle_ignores_delayed_jobs(self):
        ran = []
        self.registry.register("h", ran.append)
        self.enqueue("h", 1)
        self.enqueue("h", 2, delay=100)
        self.worker().run(until_idle=True)
        self.assertEqual(ran, [1])

    def test_loop_sleeps_until_a_delayed_job_comes_due(self):
        ran = []

        def record(payload):
            ran.append(self.clock.now())

        self.registry.register("h", record)
        self.enqueue("h", delay=5)
        worker = self.worker(poll_interval=2.0)
        worker.run(max_jobs=1)
        self.assertEqual(ran, [START + 6])

    def test_max_jobs(self):
        self.registry.register("h", lambda payload: None)
        for _ in range(4):
            self.enqueue("h")
        stats = self.worker().run(max_jobs=2)
        self.assertEqual(stats.succeeded, 2)
        self.assertEqual(len(self.queue.list_jobs(state="queued")), 2)

    def test_stop_finishes_the_current_job_then_exits(self):
        worker = self.worker()
        ran = []

        def stopper(payload, ctx):
            worker.stop()
            ran.append(ctx.stop_requested)

        self.registry.register("h", stopper)
        first = self.enqueue("h")
        second = self.enqueue("h")
        worker.run()
        self.assertEqual(ran, [True])
        self.assertEqual(self.queue.get(first.id).state, JobState.SUCCEEDED)
        self.assertEqual(self.queue.get(second.id).state, JobState.QUEUED)

    def test_stop_during_idle_sleep(self):
        worker = self.worker(sleep=lambda seconds: worker.stop())
        stats = worker.run()
        self.assertEqual(stats.leased, 0)

    def test_queue_selection(self):
        ran = []
        self.registry.register("h", ran.append)
        self.enqueue("h", "a", queue="a")
        self.enqueue("h", "b", queue="b")
        self.worker(queues=["b"]).run(until_idle=True)
        self.assertEqual(ran, ["b"])

    def test_worker_promotes_schedules(self):
        ran = []
        self.registry.register("tick", lambda payload: ran.append(self.clock.now()))
        self.queue.schedules.add("s", "tick", every=10)
        self.worker(poll_interval=10).run(max_jobs=3)
        self.assertEqual(ran, [START + 10, START + 20, START + 30])

    def test_promotion_can_be_turned_off(self):
        self.queue.schedules.add("s", "tick", every=10, start_at=START)
        self.worker(promote_schedules=False).run(until_idle=True)
        self.assertEqual(self.queue.list_jobs(), [])

    def test_expired_lease_from_a_dead_worker_is_picked_up(self):
        ran = []
        self.registry.register("h", lambda payload, ctx: ran.append(ctx.attempt))
        self.enqueue("h", backoff="fixed:0")
        self.queue.lease("crashed", lease_seconds=5)
        self.clock.advance(5)
        self.worker().run(until_idle=True)
        self.assertEqual(ran, [2])

    def test_run_once(self):
        ran = []
        self.registry.register("h", ran.append)
        self.enqueue("h", 1)
        worker = self.worker()
        self.assertTrue(worker.run_once())
        self.assertFalse(worker.run_once())
        self.assertEqual(ran, [1])

    def test_failing_observer_does_not_disturb_the_job(self):
        def bad_observer(event):
            raise RuntimeError("observer bug")

        self.registry.register("h", lambda payload: "fine")
        job = self.enqueue("h")
        with self.assertLogs("jobqueue.worker", level="ERROR"):
            self.worker(on_event=bad_observer).run(until_idle=True)
        self.assertEqual(self.queue.get(job.id).state, JobState.SUCCEEDED)

    def test_invalid_settings(self):
        for options in ({"concurrency": 0}, {"lease_seconds": 0}, {"poll_interval": 0}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.worker(**options)

    def test_event_reports_elapsed_time(self):
        self.registry.register("h", lambda payload: self.clock.advance(3))
        self.enqueue("h")
        self.worker().run(until_idle=True)
        self.assertEqual(self.events[-1].elapsed, 3)


class DatabaseTroubleTests(WorkerTestCase):
    def test_service_mode_rides_out_a_poll_error(self):
        self.registry.register("h", lambda payload: None)
        self.enqueue("h")
        worker = self.worker()
        real_lease = self.queue.lease
        calls = []

        def flaky_lease(*args, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                raise sqlite3.OperationalError("database is locked")
            return real_lease(*args, **kwargs)

        with mock.patch.object(self.queue, "lease", flaky_lease):
            with self.assertLogs("jobqueue.worker", level="WARNING"):
                stats = worker.run(max_jobs=1)
        self.assertEqual(stats.database_errors, 1)
        self.assertEqual(stats.succeeded, 1)

    def test_until_idle_mode_propagates_a_poll_error(self):
        with mock.patch.object(
            self.queue, "lease", side_effect=sqlite3.OperationalError("database is locked")
        ):
            with self.assertRaises(sqlite3.OperationalError):
                self.worker().run(until_idle=True)

    def test_failing_to_record_the_outcome_leaves_the_lease_to_expire(self):
        self.registry.register("h", lambda payload: "ok")
        job = self.enqueue("h")
        worker = self.worker()
        with mock.patch.object(
            self.queue, "complete", side_effect=sqlite3.OperationalError("disk I/O error")
        ):
            with self.assertLogs("jobqueue.worker", level="ERROR"):
                stats = worker.run(max_jobs=1)
        self.assertEqual(stats.database_errors, 1)
        self.assertEqual(self.queue.get(job.id).state, JobState.LEASED)

    def test_lease_lost_raised_by_the_queue_is_not_a_crash(self):
        self.registry.register("h", lambda payload: "ok")
        self.enqueue("h")
        with mock.patch.object(self.queue, "complete", side_effect=LeaseLost(1)):
            stats = self.worker().run(max_jobs=1)
        self.assertEqual(stats.lost, 1)


if __name__ == "__main__":
    unittest.main()
