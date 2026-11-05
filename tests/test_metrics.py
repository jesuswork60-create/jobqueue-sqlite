import json
import unittest

from support import START, QueueTestCase

from jobqueue.metrics import _percentile


class QueueFigureTests(QueueTestCase):
    def test_empty_database(self):
        snapshot = self.queue.stats()
        self.assertEqual(snapshot.queues, [])
        self.assertEqual(snapshot.handlers, [])
        self.assertEqual(snapshot.schedules_total, 0)
        self.assertIsNone(snapshot.next_schedule_at)

    def test_counts_per_queue(self):
        self.enqueue(queue="a")
        self.enqueue(queue="a", delay=60)
        self.enqueue(queue="b")
        self.enqueue(queue="b", max_attempts=1)
        self.enqueue(queue="b")
        self.enqueue(queue="b")
        first = self.queue.lease("w", queues=["b"])[0]
        self.queue.complete(first.id, first.lease_token)
        second = self.queue.lease("w", queues=["b"])[0]
        self.queue.fail(second.id, second.lease_token, "x")
        self.queue.lease("w", queues=["b"])
        last = self.queue.list_jobs(queue="b", state="queued")[0]
        self.queue.cancel(last.id)

        snapshot = self.queue.stats()
        a = snapshot.queue("a")
        self.assertEqual((a.ready, a.delayed, a.backlog), (1, 1, 2))
        b = snapshot.queue("b")
        self.assertEqual(
            (b.ready, b.leased, b.succeeded, b.dead, b.cancelled), (0, 1, 1, 1, 1)
        )
        self.assertEqual([row.queue for row in snapshot.queues], ["a", "b"])

    def test_unknown_queue_reads_as_zero(self):
        row = self.queue.stats().queue("nothing")
        self.assertEqual((row.ready, row.leased, row.dead), (0, 0, 0))

    def test_oldest_ready_age_ignores_delayed_and_leased(self):
        self.enqueue(delay=100)
        self.assertIsNone(self.queue.stats().queue("default").oldest_ready_age)
        self.clock.advance(150)
        self.assertEqual(self.queue.stats().queue("default").oldest_ready_age, 50)
        self.lease_one()
        self.assertIsNone(self.queue.stats().queue("default").oldest_ready_age)

    def test_expired_leases_are_visible_before_reclaim(self):
        self.enqueue()
        self.lease_one(lease_seconds=5)
        self.assertEqual(self.queue.stats().queue("default").expired_leases, 0)
        self.clock.advance(5)
        row = self.queue.stats().queue("default")
        self.assertEqual((row.leased, row.expired_leases), (1, 1))

    def test_schedule_figures(self):
        self.queue.schedules.add("a", "h", every=60)
        self.queue.schedules.add("b", "h", every=30)
        self.queue.schedules.pause("b")
        snapshot = self.queue.stats()
        self.assertEqual(snapshot.schedules_total, 2)
        self.assertEqual(snapshot.schedules_paused, 1)
        self.assertEqual(snapshot.next_schedule_at, START + 60)

    def test_snapshot_is_json_ready(self):
        self.enqueue()
        self.assertEqual(json.loads(json.dumps(self.queue.stats().to_dict()))["at"], START)


class HandlerFigureTests(QueueTestCase):
    def run_job(self, handler, seconds, *, fail=False):
        self.enqueue(handler, max_attempts=1)
        job = self.lease_one()
        self.clock.advance(seconds)
        if fail:
            self.queue.fail(job.id, job.lease_token, "x")
        else:
            self.queue.complete(job.id, job.lease_token)

    def test_durations_and_outcomes(self):
        for seconds in (1, 2, 3, 4):
            self.run_job("fast", seconds)
        self.run_job("fast", 10, fail=True)
        metrics = self.queue.stats().handlers[0]
        self.assertEqual(metrics.handler, "fast")
        self.assertEqual((metrics.succeeded, metrics.dead), (4, 1))
        self.assertEqual(metrics.finished, 5)
        self.assertAlmostEqual(metrics.failure_rate, 0.2)
        self.assertEqual(metrics.mean_seconds, 4)
        self.assertEqual(metrics.p95_seconds, 10)
        self.assertEqual(metrics.max_seconds, 10)

    def test_window_excludes_old_attempts(self):
        self.run_job("old", 1)
        self.clock.advance(100)
        self.run_job("new", 1)
        self.assertEqual([m.handler for m in self.queue.stats(window=50).handlers], ["new"])
        self.assertEqual(
            [m.handler for m in self.queue.stats(window=500).handlers], ["new", "old"]
        )

    def test_expired_attempts_count_but_have_no_duration(self):
        self.enqueue("h", max_attempts=1)
        self.lease_one(lease_seconds=5)
        self.clock.advance(5000)
        self.queue.reclaim_expired()
        metrics = self.queue.stats().handlers[0]
        self.assertEqual(metrics.expired, 1)
        self.assertEqual(metrics.failure_rate, 1.0)
        self.assertIsNone(metrics.mean_seconds)

    def test_released_and_running_attempts_are_left_out(self):
        self.enqueue("h")
        job = self.lease_one()
        self.queue.release(job.id, job.lease_token)
        self.lease_one()
        self.assertEqual(self.queue.stats().handlers, [])

    def test_failure_rate_without_attempts(self):
        from jobqueue import HandlerMetrics

        self.assertIsNone(HandlerMetrics("h").failure_rate)

    def test_bad_window(self):
        with self.assertRaises(ValueError):
            self.queue.stats(window=0)

    def test_percentile_nearest_rank(self):
        values = [float(n) for n in range(1, 101)]
        self.assertEqual(_percentile(values, 0.95), 95.0)
        self.assertEqual(_percentile([7.0], 0.95), 7.0)
        self.assertEqual(_percentile([1.0, 2.0], 0.5), 1.0)


if __name__ == "__main__":
    unittest.main()
