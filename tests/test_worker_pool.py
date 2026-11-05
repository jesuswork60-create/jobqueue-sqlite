"""Thread pool mode. These use real threads, coordinated with events and barriers;
the queue's own time still comes from the manual clock."""

import threading
import time
import unittest

from support import QueueTestCase

from jobqueue import HandlerRegistry, JobState, Worker

WAIT = 10.0  # upper bound for a thread rendezvous; only reached if a test is failing


class PoolTestCase(QueueTestCase):
    def setUp(self):
        super().setUp()
        self.registry = HandlerRegistry()

    def worker(self, **options):
        options.setdefault("concurrency", 3)
        options.setdefault("poll_interval", 0.01)
        options.setdefault("worker_id", "pool-worker")
        return Worker(self.queue, self.registry, **options)

    def start(self, worker, **options):
        thread = threading.Thread(target=worker.run, kwargs=options)
        thread.start()
        self.addCleanup(thread.join, WAIT)
        return thread


class PoolTests(PoolTestCase):
    def test_handlers_really_run_at_the_same_time(self):
        barrier = threading.Barrier(3)
        self.registry.register("meet", lambda payload: barrier.wait(WAIT))
        jobs = [self.enqueue("meet") for _ in range(3)]
        stats = self.worker().run(until_idle=True)
        self.assertEqual(stats.succeeded, 3)
        for job in jobs:
            self.assertEqual(self.queue.get(job.id).state, JobState.SUCCEEDED)

    def test_never_more_than_concurrency_at_once(self):
        lock = threading.Lock()
        running = [0]
        peak = [0]
        gate = threading.Barrier(2)

        def track(payload):
            with lock:
                running[0] += 1
                peak[0] = max(peak[0], running[0])
            gate.wait(WAIT)
            with lock:
                running[0] -= 1

        self.registry.register("track", track)
        for _ in range(8):
            self.enqueue("track")
        stats = self.worker(concurrency=2).run(until_idle=True)
        self.assertEqual(stats.succeeded, 8)
        self.assertEqual(peak[0], 2)

    def test_max_jobs(self):
        self.registry.register("h", lambda payload: None)
        for _ in range(7):
            self.enqueue("h")
        stats = self.worker().run(max_jobs=5)
        self.assertEqual(stats.leased, 5)
        self.assertEqual(stats.succeeded, 5)
        self.assertEqual(len(self.queue.list_jobs(state="queued")), 2)

    def test_failures_are_recorded_from_worker_threads(self):
        def boom(payload):
            raise ValueError("from a thread")

        self.registry.register("boom", boom)
        job = self.enqueue("boom", max_attempts=1)
        stats = self.worker().run(until_idle=True)
        self.assertEqual(stats.dead, 1)
        self.assertIn("from a thread", self.queue.get(job.id).last_error)

    def test_two_workers_on_one_file_share_the_work_without_overlap(self):
        lock = threading.Lock()
        seen = []

        def record(payload):
            with lock:
                seen.append(payload)

        self.registry.register("h", record)
        for number in range(60):
            self.enqueue("h", number)
        workers = [
            Worker(
                self.open_queue(),
                self.registry,
                concurrency=3,
                poll_interval=0.01,
                worker_id=f"w{index}",
            )
            for index in range(2)
        ]
        threads = [
            threading.Thread(target=worker.run, kwargs={"until_idle": True})
            for worker in workers
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)
        self.assertEqual(sorted(seen), list(range(60)))

    def test_system_exit_in_a_thread_releases_the_job(self):
        def leave(payload):
            raise SystemExit(3)

        self.registry.register("leave", leave)
        job = self.enqueue("leave")
        worker = self.worker()
        with self.assertLogs("jobqueue.worker", level="ERROR"):
            worker.run(max_jobs=1)
        released = self.queue.get(job.id)
        self.assertEqual(released.state, JobState.QUEUED)
        self.assertEqual(released.attempts, 0)
        self.assertEqual(worker.stats.released, 1)


class AutomaticHeartbeatTests(PoolTestCase):
    def test_lease_is_renewed_while_the_handler_runs(self):
        release = threading.Event()
        started = threading.Event()
        renewed = threading.Semaphore(0)

        def on_event(event):
            if event.kind == "renewed":
                renewed.release()

        def block(payload):
            started.set()
            self.assertTrue(release.wait(WAIT))
            return "finished"

        self.registry.register("block", block)
        job = self.enqueue("block")
        worker = self.worker(lease_seconds=30, on_event=on_event)
        thread = self.start(worker, max_jobs=1)
        self.assertTrue(started.wait(WAIT))

        # Three times the lease goes by, renewing at each half-lease step.
        for _ in range(6):
            self.clock.advance(15)
            self.assertTrue(renewed.acquire(timeout=WAIT))
        release.set()
        thread.join(WAIT)

        done = self.queue.get(job.id)
        self.assertEqual(done.state, JobState.SUCCEEDED)
        self.assertEqual(done.result, "finished")
        self.assertEqual(worker.stats.lost, 0)

    def test_no_renewal_before_half_the_lease(self):
        release = threading.Event()
        started = threading.Event()
        kinds = []

        def block(payload):
            started.set()
            self.assertTrue(release.wait(WAIT))

        self.registry.register("block", block)
        self.enqueue("block")
        worker = self.worker(lease_seconds=30, on_event=lambda event: kinds.append(event.kind))
        thread = self.start(worker, max_jobs=1)
        self.assertTrue(started.wait(WAIT))
        self.clock.advance(14)
        release.set()
        thread.join(WAIT)
        self.assertNotIn("renewed", kinds)

    def test_renewal_notices_a_cancelled_job(self):
        release = threading.Event()
        started = threading.Event()
        contexts = []

        def block(payload, ctx):
            contexts.append(ctx)
            started.set()
            self.assertTrue(release.wait(WAIT))
            return "done anyway"

        self.registry.register("block", block)
        job = self.enqueue("block")
        worker = self.worker(lease_seconds=30)
        thread = self.start(worker, max_jobs=1)
        self.assertTrue(started.wait(WAIT))

        self.queue.cancel(job.id)
        self.clock.advance(15)
        with self.assertLogs("jobqueue.worker", level="WARNING"):
            deadline = time.monotonic() + WAIT
            while not contexts[0].lease_lost and time.monotonic() < deadline:
                time.sleep(0.005)
            self.assertTrue(contexts[0].lease_lost)
            release.set()
            thread.join(WAIT)
        self.assertEqual(worker.stats.lost, 1)
        self.assertEqual(self.queue.get(job.id).state, JobState.CANCELLED)


class GracefulStopTests(PoolTestCase):
    def test_stop_waits_for_running_handlers(self):
        release = threading.Event()
        started = threading.Event()

        def block(payload, ctx):
            started.set()
            self.assertTrue(release.wait(WAIT))
            return ctx.stop_requested

        self.registry.register("block", block)
        running = self.enqueue("block")
        worker = self.worker(concurrency=2)
        thread = self.start(worker)
        self.assertTrue(started.wait(WAIT))

        worker.stop()
        thread.join(0.05)
        self.assertTrue(thread.is_alive())
        waiting = self.enqueue("block")

        release.set()
        thread.join(WAIT)
        self.assertFalse(thread.is_alive())
        finished = self.queue.get(running.id)
        self.assertEqual(finished.state, JobState.SUCCEEDED)
        self.assertIs(finished.result, True)
        self.assertEqual(self.queue.get(waiting.id).state, JobState.QUEUED)

    def test_drain_timeout_gives_up_on_a_stuck_handler(self):
        release = threading.Event()
        started = threading.Event()
        self.addCleanup(release.set)

        def stuck(payload):
            started.set()
            release.wait(WAIT)

        self.registry.register("stuck", stuck)
        job = self.enqueue("stuck")
        worker = self.worker(drain_timeout=5, lease_seconds=30)
        thread = self.start(worker)
        self.assertTrue(started.wait(WAIT))

        worker.stop()
        with self.assertLogs("jobqueue.worker", level="WARNING"):
            # Nudge the clock until the drain loop has seen its deadline pass.
            deadline = time.monotonic() + WAIT
            while thread.is_alive() and time.monotonic() < deadline:
                self.clock.advance(1)
                thread.join(0.01)
        self.assertFalse(thread.is_alive())
        self.assertEqual(self.queue.get(job.id).state, JobState.LEASED)


if __name__ == "__main__":
    unittest.main()
