"""The queue itself: enqueue, lease, and every state change that follows.

State machine::

    queued --lease--> leased --complete--> succeeded
       ^                 |---fail/expiry, attempts left---> queued
       |                 |---fail/expiry, none left, bury-> dead
       |                 |---release----------------------> queued
       |--dead.retry---- dead
    queued or leased --cancel--> cancelled

An attempt is counted when a job is leased, so a worker that dies mid-job has
used up an attempt just as a handler that raised has.
"""

from __future__ import annotations

import os
import random
import secrets
import sqlite3
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from .backoff import Backoff, ExponentialBackoff, parse_backoff
from .clock import Clock, SystemClock
from .db import Database
from .deadletters import DeadLetters
from .errors import DuplicateJob, InvalidTransition, JobNotFound, JobQueueError, LeaseLost
from .jobstore import (
    bury,
    close_attempt,
    delete_finished,
    encode_json,
    fetch_job,
    find_active_by_key,
    insert_job,
    requeue,
    trim_error,
)
from .metrics import MetricsSnapshot, collect_metrics
from .models import (
    TERMINAL_STATES,
    Attempt,
    AttemptOutcome,
    EnqueueResult,
    Job,
    JobState,
)
from .schedules import ScheduleStore

DEFAULT_QUEUE = "default"
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_LEASE_SECONDS = 60.0


@dataclass(frozen=True)
class JobSpec:
    """One job to enqueue; the arguments of ``JobQueue.enqueue`` as a value."""

    handler: str
    payload: Any = None
    queue: str = DEFAULT_QUEUE
    priority: int = 0
    delay: float = 0.0
    run_at: float | None = None
    unique_key: str | None = None
    max_attempts: int | None = None
    backoff: str | Backoff | None = None


class JobQueue:
    """A durable job queue in one SQLite database.

    ``target`` is a file path, ``":memory:"``, or an open Database (which then
    stays the caller's to close). All time comes from ``clock`` and all jitter
    from ``rng``.
    """

    def __init__(
        self,
        target: str | os.PathLike[str] | Database,
        *,
        clock: Clock | None = None,
        rng: random.Random | None = None,
        backoff: Backoff | None = None,
        default_max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        busy_timeout: float = 30.0,
    ) -> None:
        if default_max_attempts < 1:
            raise ValueError("default_max_attempts must be at least 1")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.clock: Clock = clock or SystemClock()
        self._rng = rng if rng is not None else random.Random()
        self._backoff: Backoff = backoff or ExponentialBackoff(base=5.0, factor=2.0, maximum=3600.0)
        self._default_max_attempts = default_max_attempts
        self._lease_seconds = lease_seconds
        self._owns_db = not isinstance(target, Database)
        self.db = (
            target
            if isinstance(target, Database)
            else Database(target, busy_timeout=busy_timeout, clock=self.clock)
        )
        self.dead = DeadLetters(self.db, self.clock)
        self.schedules = ScheduleStore(
            self.db, self.clock, default_max_attempts=default_max_attempts
        )

    def close(self) -> None:
        if self._owns_db:
            self.db.close()

    def __enter__(self) -> "JobQueue":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # Enqueueing

    def enqueue(
        self,
        handler: str,
        payload: Any = None,
        *,
        queue: str = DEFAULT_QUEUE,
        priority: int = 0,
        delay: float = 0.0,
        run_at: float | None = None,
        unique_key: str | None = None,
        max_attempts: int | None = None,
        backoff: str | Backoff | None = None,
        on_conflict: str = "return",
    ) -> EnqueueResult:
        """Add a job. Higher ``priority`` runs first.

        A ``unique_key`` is held while a job with it is queued or leased. A
        second enqueue with the same key in the same queue returns the existing
        job (``created=False``), or with ``on_conflict="raise"`` raises
        DuplicateJob. Once the job finishes, the key is free again.
        """
        spec = JobSpec(
            handler=handler,
            payload=payload,
            queue=queue,
            priority=priority,
            delay=delay,
            run_at=run_at,
            unique_key=unique_key,
            max_attempts=max_attempts,
            backoff=backoff,
        )
        return self.enqueue_many([spec], on_conflict=on_conflict)[0]

    def enqueue_many(
        self, specs: Iterable[JobSpec], *, on_conflict: str = "return"
    ) -> list[EnqueueResult]:
        """Add several jobs in one transaction: all of them or, on error, none."""
        if on_conflict not in ("return", "raise"):
            raise ValueError("on_conflict must be 'return' or 'raise'")
        prepared = [self._prepare(spec) for spec in specs]
        now = self.clock.now()
        results = []
        with self.db.transaction() as conn:
            for spec, payload_json, attempts, backoff_spec in prepared:
                results.append(
                    self._insert(conn, spec, payload_json, attempts, backoff_spec, now, on_conflict)
                )
        return results

    def _prepare(self, spec: JobSpec) -> tuple[JobSpec, str, int, str | None]:
        if not spec.handler:
            raise ValueError("a job needs a handler name")
        if spec.delay < 0:
            raise ValueError("delay cannot be negative")
        if spec.delay and spec.run_at is not None:
            raise ValueError("give either delay or run_at, not both")
        attempts = self._default_max_attempts if spec.max_attempts is None else spec.max_attempts
        if attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        return spec, encode_json(spec.payload), attempts, self._backoff_spec(spec.backoff)

    def _backoff_spec(self, backoff: str | Backoff | None) -> str | None:
        if backoff is None:
            return None
        if isinstance(backoff, str):
            parse_backoff(backoff, rng=self._rng)  # validate now, not at the first failure
            return backoff
        spec = backoff.spec()
        if spec is None:
            raise ValueError(
                "this backoff has no text form, so it cannot be stored on a job; "
                "register it on the handler instead"
            )
        return spec

    def _insert(
        self,
        conn: sqlite3.Connection,
        spec: JobSpec,
        payload_json: str,
        attempts: int,
        backoff_spec: str | None,
        now: float,
        on_conflict: str,
    ) -> EnqueueResult:
        if spec.unique_key is not None:
            holder = find_active_by_key(conn, spec.queue, spec.unique_key)
            if holder is not None:
                if on_conflict == "raise":
                    raise DuplicateJob(spec.queue, spec.unique_key, holder.id)
                return EnqueueResult(holder, created=False)

        job_id = insert_job(
            conn,
            queue=spec.queue,
            handler=spec.handler,
            payload_json=payload_json,
            priority=spec.priority,
            run_at=spec.run_at if spec.run_at is not None else now + spec.delay,
            max_attempts=attempts,
            now=now,
            unique_key=spec.unique_key,
            backoff=backoff_spec,
        )
        if job_id is None:
            # INSERT OR IGNORE only skips on a uniqueness clash, and the lookup
            # above ran in this same write transaction.
            raise JobQueueError("job insert was ignored unexpectedly")
        return EnqueueResult(fetch_job(conn, job_id), created=True)

    # Reading

    def get(self, job_id: int) -> Job:
        rows = self.db.query("SELECT * FROM jobs WHERE id = ?", (job_id,))
        if not rows:
            raise JobNotFound(job_id)
        return Job.from_row(rows[0])

    def list_jobs(
        self,
        *,
        state: JobState | str | None = None,
        queue: str | None = None,
        handler: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        """Jobs matching every given filter, newest first."""
        clauses = []
        params: list[Any] = []
        if state is not None:
            clauses.append("state = ?")
            params.append(JobState(state).value)
        if queue is not None:
            clauses.append("queue = ?")
            params.append(queue)
        if handler is not None:
            clauses.append("handler = ?")
            params.append(handler)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.db.query(
            f"SELECT * FROM jobs {where} ORDER BY id DESC LIMIT ? OFFSET ?",
            [*params, limit, offset],
        )
        return [Job.from_row(row) for row in rows]

    def attempts(self, job_id: int) -> list[Attempt]:
        """The run history of a job, oldest first."""
        rows = self.db.query("SELECT * FROM job_attempts WHERE job_id = ? ORDER BY id", (job_id,))
        return [Attempt.from_row(row) for row in rows]

    def stats(self, *, window: float = 3600.0) -> MetricsSnapshot:
        return collect_metrics(self.db, self.clock.now(), window=window)

    # Leasing

    def lease(
        self,
        worker_id: str,
        *,
        queues: Sequence[str] | None = None,
        limit: int = 1,
        lease_seconds: float | None = None,
    ) -> list[Job]:
        """Atomically claim up to ``limit`` ready jobs, best priority first.

        Expired leases are reclaimed first, in the same transaction. Each job
        returned carries a fresh ``lease_token``, needed for every later call
        about that attempt. ``queues=None`` means all queues.
        """
        if limit < 1:
            raise ValueError("limit must be at least 1")
        duration = self._lease_seconds if lease_seconds is None else lease_seconds
        if duration <= 0:
            raise ValueError("lease_seconds must be positive")
        if queues is not None and not queues:
            return []

        now = self.clock.now()
        with self.db.transaction() as conn:
            self._reclaim(conn, now)

            params: list[Any] = [now]
            queue_clause = ""
            if queues is not None:
                queue_clause = f"AND queue IN ({', '.join('?' * len(queues))})"
                params.extend(queues)
            rows = conn.execute(
                f"""
                SELECT id, attempts FROM jobs
                WHERE state = 'queued' AND run_at <= ? {queue_clause}
                ORDER BY priority DESC, run_at, id
                LIMIT ?
                """,
                [*params, limit],
            ).fetchall()

            leased = []
            for row in rows:
                attempt = row["attempts"] + 1
                conn.execute(
                    """
                    UPDATE jobs SET state = 'leased', attempts = ?, lease_token = ?,
                        lease_owner = ?, lease_expires_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (attempt, secrets.token_hex(16), worker_id, now + duration, now, row["id"]),
                )
                conn.execute(
                    """
                    INSERT INTO job_attempts (job_id, attempt, worker, started_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (row["id"], attempt, worker_id, now),
                )
                leased.append(fetch_job(conn, row["id"]))
            return leased

    def _held_lease(
        self, conn: sqlite3.Connection, job_id: int, token: str, now: float
    ) -> Job:
        row = conn.execute(
            """
            SELECT * FROM jobs WHERE id = ? AND state = 'leased' AND lease_token = ?
            """,
            (job_id, token),
        ).fetchone()
        if row is None:
            raise LeaseLost(job_id)
        job = Job.from_row(row)
        # A lease past its expiry is gone even if nobody has reclaimed it yet:
        # another worker may be about to, and the outcome must not depend on that race.
        if job.lease_expires_at is None or job.lease_expires_at <= now:
            raise LeaseLost(job_id, "lease expired")
        return job

    def heartbeat(self, job_id: int, token: str, *, extend: float | None = None) -> float:
        """Push the lease expiry out to ``extend`` seconds from now, and return it.

        A lease is never shortened by this. Raises LeaseLost if the lease is gone.
        """
        duration = self._lease_seconds if extend is None else extend
        if duration <= 0:
            raise ValueError("extend must be positive")
        now = self.clock.now()
        with self.db.transaction() as conn:
            job = self._held_lease(conn, job_id, token, now)
            expires = max(job.lease_expires_at or now, now + duration)
            conn.execute(
                "UPDATE jobs SET lease_expires_at = ?, updated_at = ? WHERE id = ?",
                (expires, now, job_id),
            )
            return expires

    def complete(self, job_id: int, token: str, result: Any = None) -> Job:
        """Finish a job successfully. Raises LeaseLost if the lease is gone."""
        result_json = None if result is None else encode_json(result, "result")
        now = self.clock.now()
        with self.db.transaction() as conn:
            self._held_lease(conn, job_id, token, now)
            conn.execute(
                """
                UPDATE jobs SET state = 'succeeded', result = ?, last_error = NULL,
                    lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL,
                    finished_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (result_json, now, now, job_id),
            )
            close_attempt(conn, job_id, now, AttemptOutcome.SUCCEEDED)
            return fetch_job(conn, job_id)

    def fail(self, job_id: int, token: str, error: str, *, delay: float | None = None) -> Job:
        """Record a failed attempt: retry if attempts remain, otherwise bury.

        ``delay`` overrides the backoff policy. The returned job's state says
        which of the two happened.
        """
        if delay is not None and delay < 0:
            raise ValueError("delay cannot be negative")
        error = trim_error(error)
        now = self.clock.now()
        with self.db.transaction() as conn:
            job = self._held_lease(conn, job_id, token, now)
            if job.attempts >= job.max_attempts:
                bury(conn, job_id, now, error)
                close_attempt(conn, job_id, now, AttemptOutcome.DEAD, error)
            else:
                wait = self._retry_delay(job) if delay is None else delay
                requeue(conn, job_id, now, now + wait, error)
                close_attempt(conn, job_id, now, AttemptOutcome.RETRIED, error)
            return fetch_job(conn, job_id)

    def bury(self, job_id: int, token: str, reason: str) -> Job:
        """Send a leased job to the dead letters, whatever attempts it has left."""
        reason = trim_error(reason)
        now = self.clock.now()
        with self.db.transaction() as conn:
            self._held_lease(conn, job_id, token, now)
            bury(conn, job_id, now, reason)
            close_attempt(conn, job_id, now, AttemptOutcome.DEAD, reason)
            return fetch_job(conn, job_id)

    def release(self, job_id: int, token: str, *, delay: float = 0.0) -> Job:
        """Hand a leased job back unharmed; the attempt is not counted against it."""
        if delay < 0:
            raise ValueError("delay cannot be negative")
        now = self.clock.now()
        with self.db.transaction() as conn:
            job = self._held_lease(conn, job_id, token, now)
            requeue(conn, job_id, now, now + delay, None)
            conn.execute(
                "UPDATE jobs SET attempts = ? WHERE id = ?", (job.attempts - 1, job_id)
            )
            close_attempt(conn, job_id, now, AttemptOutcome.RELEASED)
            return fetch_job(conn, job_id)

    def cancel(self, job_id: int) -> Job:
        """Cancel a queued or leased job.

        A worker holding the lease cannot be interrupted, but its next heartbeat
        or completion gets LeaseLost.
        """
        now = self.clock.now()
        with self.db.transaction() as conn:
            job = fetch_job(conn, job_id)
            if job.state in TERMINAL_STATES:
                raise InvalidTransition(f"job {job_id} is already {job.state.value}")
            conn.execute(
                """
                UPDATE jobs SET state = 'cancelled', lease_token = NULL, lease_owner = NULL,
                    lease_expires_at = NULL, finished_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (now, now, job_id),
            )
            close_attempt(conn, job_id, now, AttemptOutcome.CANCELLED)
            return fetch_job(conn, job_id)

    # Maintenance

    def reclaim_expired(self) -> list[Job]:
        """Requeue or bury jobs whose lease ran out, and return them.

        ``lease`` already does this; call it directly only when nothing is polling.
        """
        with self.db.transaction() as conn:
            return self._reclaim(conn, self.clock.now())

    def _reclaim(self, conn: sqlite3.Connection, now: float) -> list[Job]:
        rows = conn.execute(
            "SELECT * FROM jobs WHERE state = 'leased' AND lease_expires_at <= ? ORDER BY id",
            (now,),
        ).fetchall()
        reclaimed = []
        for row in rows:
            job = Job.from_row(row)
            error = f"lease expired (worker {job.lease_owner})"
            if job.attempts >= job.max_attempts:
                bury(conn, job.id, now, error)
            else:
                requeue(conn, job.id, now, now + self._retry_delay(job), error)
            close_attempt(conn, job.id, now, AttemptOutcome.EXPIRED, error)
            reclaimed.append(fetch_job(conn, job.id))
        return reclaimed

    def _retry_delay(self, job: Job) -> float:
        policy = self._backoff if job.backoff is None else parse_backoff(job.backoff, rng=self._rng)
        return policy.delay(job.attempts)

    def prune(
        self,
        *,
        older_than: float = 0.0,
        states: Iterable[JobState | str] = (JobState.SUCCEEDED, JobState.CANCELLED),
        queue: str | None = None,
    ) -> int:
        """Delete finished jobs and their history; returns how many went.

        Only finished states may be pruned: active jobs must be cancelled first.
        """
        if older_than < 0:
            raise ValueError("older_than cannot be negative")
        wanted = [JobState(state) for state in states]
        for state in wanted:
            if state not in TERMINAL_STATES:
                raise InvalidTransition(f"cannot prune {state.value} jobs")
        if not wanted:
            return 0
        cutoff = self.clock.now() - older_than
        with self.db.transaction() as conn:
            return delete_finished(conn, wanted, finished_before=cutoff, queue=queue)
