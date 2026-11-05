"""Shared test scaffolding: a real database file in a temp directory and a manual clock."""

from __future__ import annotations

import logging
import os
import tempfile
import unittest

from jobqueue import JobQueue, ManualClock

START = 1_700_000_000.0

# Expected warnings (lost leases and so on) stay out of the test output.
logging.getLogger("jobqueue").addHandler(logging.NullHandler())


class QueueTestCase(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.dir = directory.name
        self.path = os.path.join(self.dir, "queue.db")
        self.clock = ManualClock(START)
        self.queue = self.open_queue()

    def open_queue(self, **options) -> JobQueue:
        options.setdefault("clock", self.clock)
        queue = JobQueue(self.path, **options)
        self.addCleanup(queue.close)
        return queue

    def lease_one(self, worker: str = "w1", **options):
        jobs = self.queue.lease(worker, **options)
        self.assertEqual(len(jobs), 1)
        return jobs[0]

    def enqueue(self, handler: str = "h", payload=None, **options):
        return self.queue.enqueue(handler, payload, **options).job
