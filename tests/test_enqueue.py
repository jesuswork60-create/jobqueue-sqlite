import math
import unittest

from support import START, QueueTestCase

from jobqueue import (
    BackoffSpecError,
    DuplicateJob,
    FixedBackoff,
    JobSpec,
    JobState,
    PayloadError,
)


class EnqueueTests(QueueTestCase):
    def test_defaults(self):
        job = self.enqueue("mail.send")
        self.assertEqual(job.state, JobState.QUEUED)
        self.assertEqual(job.queue, "default")
        self.assertEqual(job.priority, 0)
        self.assertEqual(job.attempts, 0)
        self.assertEqual(job.max_attempts, 5)
        self.assertEqual(job.run_at, START)
        self.assertEqual(job.created_at, START)
        self.assertIsNone(job.lease_token)
        self.assertIsNone(job.payload)

    def test_payload_round_trips_through_json(self):
        payload = {"to": ["a@example.com"], "n": 3, "nested": {"ok": True, "x": None}}
        job = self.enqueue(payload=payload)
        self.assertEqual(self.queue.get(job.id).payload, payload)

    def test_unserialisable_payload_is_refused(self):
        with self.assertRaisesRegex(PayloadError, "not JSON"):
            self.enqueue(payload={"when": object()})
        self.assertEqual(self.queue.list_jobs(), [])

    def test_nan_payload_is_refused(self):
        with self.assertRaises(PayloadError):
            self.enqueue(payload={"x": math.nan})

    def test_delay_and_explicit_run_at(self):
        self.assertEqual(self.enqueue(delay=30).run_at, START + 30)
        self.assertEqual(self.enqueue(run_at=START + 99).run_at, START + 99)

    def test_invalid_timing_is_refused(self):
        with self.assertRaises(ValueError):
            self.enqueue(delay=-1)
        with self.assertRaises(ValueError):
            self.enqueue(delay=5, run_at=START + 1)

    def test_empty_handler_and_bad_attempts_are_refused(self):
        with self.assertRaises(ValueError):
            self.enqueue("")
        with self.assertRaises(ValueError):
            self.enqueue(max_attempts=0)

    def test_queue_default_max_attempts_applies(self):
        queue = self.open_queue(default_max_attempts=2)
        self.assertEqual(queue.enqueue("h").job.max_attempts, 2)

    def test_backoff_is_stored_as_text(self):
        self.assertEqual(self.enqueue(backoff=FixedBackoff(7)).backoff, "fixed:7")
        self.assertEqual(self.enqueue(backoff="fixed:9").backoff, "fixed:9")

    def test_bad_backoff_text_is_refused_at_enqueue(self):
        with self.assertRaises(BackoffSpecError):
            self.enqueue(backoff="wobbly:3")

    def test_backoff_without_text_form_is_refused(self):
        class Custom:
            def delay(self, attempt):
                return 1.0

            def spec(self):
                return None

        with self.assertRaisesRegex(ValueError, "text form"):
            self.enqueue(backoff=Custom())


class UniqueKeyTests(QueueTestCase):
    def test_second_enqueue_returns_the_active_job(self):
        first = self.queue.enqueue("h", {"v": 1}, unique_key="k")
        second = self.queue.enqueue("h", {"v": 2}, unique_key="k")
        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(second.job.id, first.job.id)
        self.assertEqual(second.job.payload, {"v": 1})
        self.assertEqual(len(self.queue.list_jobs()), 1)

    def test_conflict_can_raise(self):
        first = self.enqueue(unique_key="k")
        with self.assertRaises(DuplicateJob) as caught:
            self.queue.enqueue("h", unique_key="k", on_conflict="raise")
        self.assertEqual(caught.exception.existing_id, first.id)

    def test_key_still_held_while_leased(self):
        self.enqueue(unique_key="k")
        self.lease_one()
        self.assertFalse(self.queue.enqueue("h", unique_key="k").created)

    def test_key_is_free_after_each_terminal_state(self):
        job = self.enqueue(unique_key="k", max_attempts=1)
        leased = self.lease_one()
        self.queue.complete(job.id, leased.lease_token)
        job = self.queue.enqueue("h", unique_key="k")
        self.assertTrue(job.created)

        leased = self.lease_one()
        self.queue.bury(leased.id, leased.lease_token, "no")
        job = self.queue.enqueue("h", unique_key="k")
        self.assertTrue(job.created)

        self.queue.cancel(job.job.id)
        self.assertTrue(self.queue.enqueue("h", unique_key="k").created)

    def test_keys_are_scoped_to_a_queue(self):
        self.assertTrue(self.queue.enqueue("h", queue="a", unique_key="k").created)
        self.assertTrue(self.queue.enqueue("h", queue="b", unique_key="k").created)

    def test_jobs_without_a_key_never_collide(self):
        self.assertNotEqual(self.enqueue().id, self.enqueue().id)


class EnqueueManyTests(QueueTestCase):
    def test_empty_batch(self):
        self.assertEqual(self.queue.enqueue_many([]), [])

    def test_adds_all_in_order(self):
        results = self.queue.enqueue_many(
            [JobSpec("a"), JobSpec("b", priority=3), JobSpec("c", delay=5)]
        )
        self.assertEqual([r.job.handler for r in results], ["a", "b", "c"])
        self.assertEqual(results[2].job.run_at, START + 5)

    def test_validation_failure_adds_nothing(self):
        with self.assertRaises(PayloadError):
            self.queue.enqueue_many([JobSpec("a"), JobSpec("b", payload=object())])
        self.assertEqual(self.queue.list_jobs(), [])

    def test_conflict_raise_rolls_back_the_whole_batch(self):
        self.enqueue(unique_key="k")
        with self.assertRaises(DuplicateJob):
            self.queue.enqueue_many(
                [JobSpec("new"), JobSpec("x", unique_key="k")], on_conflict="raise"
            )
        self.assertEqual([j.handler for j in self.queue.list_jobs()], ["h"])

    def test_duplicates_inside_one_batch_are_deduplicated(self):
        results = self.queue.enqueue_many(
            [JobSpec("a", unique_key="k"), JobSpec("b", unique_key="k")]
        )
        self.assertEqual([r.created for r in results], [True, False])
        self.assertEqual(results[0].job.id, results[1].job.id)

    def test_bad_conflict_mode(self):
        with self.assertRaises(ValueError):
            self.queue.enqueue_many([JobSpec("a")], on_conflict="replace")


if __name__ == "__main__":
    unittest.main()
