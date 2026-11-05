import unittest

from support import START, QueueTestCase

from jobqueue import DuplicateJob, InvalidTransition, JobNotFound, JobState


class DeadLetterTests(QueueTestCase):
    def kill(self, handler="h", **options):
        options.setdefault("max_attempts", 1)
        self.enqueue(handler, **options)
        job = self.lease_one()
        return self.queue.fail(job.id, job.lease_token, f"{handler} failed")

    def test_listing_most_recent_first_with_filters(self):
        first = self.kill("a", queue="q1")
        self.clock.advance(1)
        second = self.kill("b", queue="q2")
        self.assertEqual([j.id for j in self.queue.dead.list()], [second.id, first.id])
        self.assertEqual([j.id for j in self.queue.dead.list(queue="q1")], [first.id])
        self.assertEqual([j.id for j in self.queue.dead.list(handler="b")], [second.id])
        self.assertEqual([j.id for j in self.queue.dead.list(limit=1, offset=1)], [first.id])
        self.assertEqual(self.queue.dead.count(), 2)
        self.assertEqual(self.queue.dead.count(queue="q2"), 1)
        self.assertEqual(self.queue.dead.count(handler="nope"), 0)

    def test_only_dead_jobs_are_listed(self):
        self.enqueue()
        self.assertEqual(self.queue.dead.list(), [])

    def test_retry_resets_attempts_and_keeps_the_error_for_reference(self):
        dead = self.kill(max_attempts=2, backoff="fixed:0")
        # The first failure above still had an attempt left; finish it off.
        self.assertEqual(dead.state, JobState.QUEUED)
        job = self.lease_one()
        dead = self.queue.fail(job.id, job.lease_token, "second")
        self.assertEqual(dead.state, JobState.DEAD)

        self.clock.advance(10)
        revived = self.queue.dead.retry(dead.id)
        self.assertEqual(revived.state, JobState.QUEUED)
        self.assertEqual(revived.attempts, 0)
        self.assertEqual(revived.run_at, START + 10)
        self.assertIsNone(revived.finished_at)
        self.assertEqual(revived.last_error, "second")

    def test_retry_with_delay(self):
        dead = self.kill()
        self.assertEqual(self.queue.dead.retry(dead.id, delay=60).run_at, START + 60)
        self.assertEqual(self.queue.lease("w"), [])
        with self.assertRaises(ValueError):
            self.queue.dead.retry(dead.id, delay=-1)

    def test_keeping_attempts_allows_exactly_one_more_try(self):
        dead = self.kill(max_attempts=1)
        revived = self.queue.dead.retry(dead.id, reset_attempts=False)
        self.assertEqual(revived.attempts, 1)
        job = self.lease_one()
        self.assertEqual(job.attempts, 2)
        self.assertEqual(self.queue.fail(job.id, job.lease_token, "x").state, JobState.DEAD)

    def test_retry_of_a_job_that_is_not_dead(self):
        job = self.enqueue()
        with self.assertRaises(InvalidTransition):
            self.queue.dead.retry(job.id)
        with self.assertRaises(JobNotFound):
            self.queue.dead.retry(404)

    def test_retry_refused_when_unique_key_was_taken(self):
        dead = self.kill(unique_key="k")
        holder = self.enqueue(unique_key="k")
        with self.assertRaises(DuplicateJob) as caught:
            self.queue.dead.retry(dead.id)
        self.assertEqual(caught.exception.existing_id, holder.id)
        self.assertEqual(self.queue.get(dead.id).state, JobState.DEAD)

    def test_retry_all_reports_what_it_skipped(self):
        taken = self.kill("a", unique_key="k")
        plain = self.kill("b")
        other_queue = self.kill("c", queue="other")
        holder = self.enqueue(unique_key="k")

        revived, skipped = self.queue.dead.retry_all(queue="default")
        self.assertEqual([j.id for j in revived], [plain.id])
        self.assertEqual(skipped, [taken.id])
        self.assertEqual(self.queue.get(other_queue.id).state, JobState.DEAD)
        self.assertEqual(self.queue.get(holder.id).state, JobState.QUEUED)

    def test_two_dead_jobs_with_one_key_revive_only_once(self):
        first = self.kill(unique_key="k")
        second = self.kill(unique_key="k")
        revived, skipped = self.queue.dead.retry_all()
        self.assertEqual([j.id for j in revived], [first.id])
        self.assertEqual(skipped, [second.id])

    def test_purge_by_age(self):
        old = self.kill()
        self.clock.advance(100)
        recent = self.kill()
        self.assertEqual(self.queue.dead.purge(older_than=50), 1)
        with self.assertRaises(JobNotFound):
            self.queue.get(old.id)
        self.assertEqual(self.queue.attempts(old.id), [])
        self.assertEqual(self.queue.get(recent.id).state, JobState.DEAD)

    def test_purge_by_queue_and_ids(self):
        self.kill(queue="a")
        second = self.kill(queue="b")
        third = self.kill(queue="b")
        self.assertEqual(self.queue.dead.purge(queue="a"), 1)
        self.assertEqual(self.queue.dead.purge(job_ids=[third.id]), 1)
        self.assertEqual(self.queue.dead.purge(job_ids=[]), 0)
        self.assertEqual([j.id for j in self.queue.dead.list()], [second.id])

    def test_purge_never_touches_other_states(self):
        active = self.enqueue()
        self.assertEqual(self.queue.dead.purge(job_ids=[active.id]), 0)
        self.assertEqual(self.queue.get(active.id).state, JobState.QUEUED)


if __name__ == "__main__":
    unittest.main()
