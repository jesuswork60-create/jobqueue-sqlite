"""The worker loop: lease jobs, run handlers, record what happened.

With ``concurrency=1`` handlers run inline on the calling thread. With more, they
run on a thread pool and the calling thread keeps leasing and renews the leases
of running jobs. Inline handlers that may outlive their lease must call
``ctx.heartbeat()`` themselves, since nothing else is running to do it.
"""

from __future__ import annotations

import logging
import os
import secrets
import socket
import sqlite3
import threading
import traceback
from collections.abc import Callable, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any

from .errors import Abandon, HandlerNotFound, LeaseLost, PayloadError, Retry
from .models import Job, JobState
from .queue import JobQueue
from .registry import Handler, HandlerRegistry

log = logging.getLogger("jobqueue.worker")


def default_worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{secrets.token_hex(3)}"


def _token(job: Job) -> str:
    assert job.lease_token is not None, "jobs given to handlers always hold a lease"
    return job.lease_token


@dataclass(frozen=True, slots=True)
class WorkerEvent:
    """Passed to the ``on_event`` observer.

    ``kind`` is one of started, succeeded, retried, dead, lost, released, renewed.
    ``elapsed`` is the handler's run time for events that end an attempt.
    """

    kind: str
    job: Job
    elapsed: float | None = None
    error: str | None = None


@dataclass(slots=True)
class WorkerStats:
    leased: int = 0
    succeeded: int = 0
    retried: int = 0
    dead: int = 0
    lost: int = 0
    released: int = 0
    database_errors: int = 0


class JobContext:
    """What a handler can see and do while it runs."""

    def __init__(self, worker: "Worker", job: Job) -> None:
        self.job = job
        self.worker_id = worker.worker_id
        self.clock = worker.queue.clock
        self._worker = worker
        self._lease_lost = threading.Event()

    @property
    def job_id(self) -> int:
        return self.job.id

    @property
    def payload(self) -> Any:
        return self.job.payload

    @property
    def attempt(self) -> int:
        """1 on the first run of this job."""
        return self.job.attempts

    @property
    def stop_requested(self) -> bool:
        """The worker is shutting down; a long job may want to wrap up or raise Retry."""
        return self._worker.stopping

    @property
    def lease_lost(self) -> bool:
        """The worker found out this lease is gone. Any further work may be duplicated."""
        return self._lease_lost.is_set()

    def heartbeat(self, extend: float | None = None) -> float:
        """Extend the lease and return the new expiry. Raises LeaseLost if it is gone.

        Letting that exception propagate is the right response: the worker notes
        the loss and leaves the job's state to whoever holds it now.
        """
        duration = self._worker.lease_seconds if extend is None else extend
        try:
            return self._worker.queue.heartbeat(self.job.id, _token(self.job), extend=duration)
        except LeaseLost:
            self._lease_lost.set()
            raise


@dataclass(slots=True)
class _Running:
    job: Job
    context: JobContext
    renewed_at: float


class Worker:
    """Runs jobs from a queue until stopped.

    ``queues=None`` serves every queue. ``sleep`` replaces the idle wait, which
    by default returns early when ``stop()`` is called. ``drain_timeout`` limits
    how long a pool-mode stop waits for running handlers; after that their leases
    are left to expire and be reclaimed.
    """

    def __init__(
        self,
        queue: JobQueue,
        registry: HandlerRegistry,
        *,
        queues: Sequence[str] | None = None,
        worker_id: str | None = None,
        concurrency: int = 1,
        lease_seconds: float = 60.0,
        poll_interval: float = 1.0,
        drain_timeout: float | None = None,
        promote_schedules: bool = True,
        sleep: Callable[[float], None] | None = None,
        on_event: Callable[[WorkerEvent], None] | None = None,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        self.queue = queue
        self.registry = registry
        self.queues = queues
        self.worker_id = worker_id or default_worker_id()
        self.concurrency = concurrency
        self.lease_seconds = lease_seconds
        self.poll_interval = poll_interval
        self.drain_timeout = drain_timeout
        self.promote_schedules = promote_schedules
        self.stats = WorkerStats()
        self._stop = threading.Event()
        self._sleep = sleep or self._stop.wait
        self._on_event = on_event
        self._stats_lock = threading.Lock()

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def stop(self) -> None:
        """Ask the loop to finish. Safe from signal handlers and other threads.

        No new jobs are leased; handlers already running get to finish.
        """
        self._stop.set()

    def run_once(self) -> bool:
        """Promote schedules, then run at most one job on the calling thread.

        Returns whether a job ran.
        """
        jobs = self._poll(1)
        if jobs:
            self._execute(jobs[0], JobContext(self, jobs[0]))
        return bool(jobs)

    def run(self, *, until_idle: bool = False, max_jobs: int | None = None) -> WorkerStats:
        """Run until ``stop()``.

        ``until_idle`` also returns once nothing is ready to run (delayed jobs do
        not count), and ``max_jobs`` once that many jobs have been leased and
        have finished. In ``until_idle`` mode a database error propagates; in
        service mode it is logged and the loop tries again after a pause.
        """
        if self.concurrency == 1:
            self._run_inline(until_idle, max_jobs)
        else:
            self._run_pool(until_idle, max_jobs)
        return self.stats

    def _run_inline(self, until_idle: bool, max_jobs: int | None) -> None:
        while not self._stop.is_set():
            if max_jobs is not None and self.stats.leased >= max_jobs:
                return
            jobs = self._guarded_poll(1, until_idle)
            if jobs:
                self._execute(jobs[0], JobContext(self, jobs[0]))
            elif jobs is None or not until_idle:
                self._sleep(self.poll_interval)
            else:
                return

    def _run_pool(self, until_idle: bool, max_jobs: int | None) -> None:
        in_flight: dict[Future[None], _Running] = {}
        pool = ThreadPoolExecutor(self.concurrency, thread_name_prefix="jobqueue-worker")
        drained = True
        # Wake up in time to renew leases even if the poll interval is long.
        tick = min(self.poll_interval, self.lease_seconds / 4)
        try:
            while not self._stop.is_set():
                self._reap(in_flight)
                self._renew(in_flight)

                room = self.concurrency - len(in_flight)
                if max_jobs is not None:
                    room = min(room, max_jobs - self.stats.leased)
                jobs = self._guarded_poll(room, until_idle) if room > 0 else []
                if jobs is None:
                    self._sleep(self.poll_interval)
                    continue

                now = self.queue.clock.now()
                for job in jobs:
                    context = JobContext(self, job)
                    future = pool.submit(self._execute, job, context)
                    in_flight[future] = _Running(job, context, now)

                if in_flight:
                    wait(in_flight, timeout=tick, return_when=FIRST_COMPLETED)
                elif until_idle or (max_jobs is not None and self.stats.leased >= max_jobs):
                    return
                else:
                    self._sleep(self.poll_interval)
            drained = self._drain(in_flight, tick)
        finally:
            pool.shutdown(wait=drained)

    def _drain(self, in_flight: dict[Future[None], _Running], tick: float) -> bool:
        """Wait for running handlers. Returns False if the drain timeout ran out."""
        clock = self.queue.clock
        deadline = None if self.drain_timeout is None else clock.now() + self.drain_timeout
        while True:
            self._reap(in_flight)
            if not in_flight:
                return True
            if deadline is not None and clock.now() >= deadline:
                log.warning(
                    "%s: giving up on %d running job(s) after the drain timeout",
                    self.worker_id,
                    len(in_flight),
                )
                return False
            self._renew(in_flight)
            wait(in_flight, timeout=tick, return_when=FIRST_COMPLETED)

    def _reap(self, in_flight: dict[Future[None], _Running]) -> None:
        for future in [f for f in in_flight if f.done()]:
            running = in_flight.pop(future)
            error = future.exception()
            if error is not None:
                log.error("job %d: worker thread failed: %r", running.job.id, error)

    def _renew(self, in_flight: dict[Future[None], _Running]) -> None:
        """Heartbeat running jobs once half of their lease has gone by."""
        now = self.queue.clock.now()
        for future, running in in_flight.items():
            due = now - running.renewed_at >= self.lease_seconds / 2
            if not due or future.done() or running.context.lease_lost:
                continue
            try:
                running.context.heartbeat()
            except LeaseLost:
                # The handler finds out when it next heartbeats or finishes.
                log.warning("job %d: lease lost while running", running.job.id)
            except sqlite3.OperationalError as exc:
                self._count("database_errors")
                log.warning("job %d: heartbeat failed: %s", running.job.id, exc)
            else:
                running.renewed_at = now
                self._emit(WorkerEvent("renewed", running.job))

    def _guarded_poll(self, limit: int, until_idle: bool) -> list[Job] | None:
        """Poll, or return None after a database error that the loop should ride out."""
        try:
            return self._poll(limit)
        except sqlite3.OperationalError as exc:
            if until_idle:
                raise
            self._count("database_errors")
            log.warning("%s: poll failed: %s", self.worker_id, exc)
            return None

    def _poll(self, limit: int) -> list[Job]:
        if self.promote_schedules:
            self.queue.schedules.promote_due()
        jobs = self.queue.lease(
            self.worker_id,
            queues=self.queues,
            limit=limit,
            lease_seconds=self.lease_seconds,
        )
        if jobs:
            self._count("leased", len(jobs))
        return jobs

    def _execute(self, job: Job, context: JobContext) -> None:
        self._emit(WorkerEvent("started", job))
        clock = self.queue.clock
        started = clock.now()
        try:
            self._run_handler(job, context, started)
        except LeaseLost as exc:
            self._count("lost")
            log.warning("job %d: %s", job.id, exc.reason)
            self._emit(WorkerEvent("lost", job, clock.now() - started, exc.reason))
        except sqlite3.OperationalError as exc:
            # The outcome could not be stored. The lease will expire and the job
            # will run again, which at-least-once delivery allows.
            self._count("database_errors")
            log.error("job %d: could not record the outcome: %s", job.id, exc)

    def _run_handler(self, job: Job, context: JobContext, started: float) -> None:
        """Run the handler and store the outcome. LeaseLost and database errors
        from storing it, and LeaseLost from a heartbeat, reach ``_execute``."""
        clock = self.queue.clock
        handler: Handler | None = None
        try:
            handler = self.registry.resolve(job.handler)
            result = handler(job.payload, context)
        except LeaseLost:
            raise
        except Retry as exc:
            self._record_failure(job, handler, exc.reason, exc.delay, clock.now() - started)
        except Abandon as exc:
            buried = self.queue.bury(job.id, _token(job), exc.reason)
            self._count("dead")
            self._emit(WorkerEvent("dead", buried, clock.now() - started, exc.reason))
        except HandlerNotFound as exc:
            self._record_failure(job, None, str(exc), None, clock.now() - started)
        except Exception:
            error = traceback.format_exc()
            self._record_failure(job, handler, error, None, clock.now() - started)
        except BaseException:
            # KeyboardInterrupt, SystemExit: this is about the process, not the
            # job, so give the job back without charging it an attempt.
            released = self.queue.release(job.id, _token(job))
            self._count("released")
            self._emit(WorkerEvent("released", released, clock.now() - started))
            raise
        else:
            self._finish(job, handler, result, clock.now() - started)

    def _finish(self, job: Job, handler: Handler, result: Any, elapsed: float) -> None:
        try:
            done = self.queue.complete(job.id, _token(job), result)
        except PayloadError as exc:
            # The side effects happened, but the result cannot be stored. Treat
            # it as a failure so that the problem is visible in the job's history.
            self._record_failure(job, handler, str(exc), None, elapsed)
            return
        self._count("succeeded")
        self._emit(WorkerEvent("succeeded", done, elapsed))

    def _record_failure(
        self,
        job: Job,
        handler: Handler | None,
        error: str,
        delay: float | None,
        elapsed: float,
    ) -> None:
        if delay is None and handler is not None and handler.backoff is not None:
            delay = handler.backoff.delay(job.attempts)
        updated = self.queue.fail(job.id, _token(job), error, delay=delay)
        kind = "dead" if updated.state is JobState.DEAD else "retried"
        self._count(kind)
        self._emit(WorkerEvent(kind, updated, elapsed, error))

    def _count(self, name: str, amount: int = 1) -> None:
        with self._stats_lock:
            setattr(self.stats, name, getattr(self.stats, name) + amount)

    def _emit(self, event: WorkerEvent) -> None:
        if self._on_event is None:
            return
        try:
            self._on_event(event)
        except Exception:
            log.exception("on_event observer failed for job %d", event.job.id)
