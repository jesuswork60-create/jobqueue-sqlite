"""Inspect, revive and purge jobs that ran out of attempts or were abandoned."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .clock import Clock
from .db import Database
from .errors import DuplicateJob, InvalidTransition
from .jobstore import delete_finished, fetch_job, find_active_by_key
from .models import Job, JobState


class DeadLetters:
    """The dead-letter view of a queue, available as ``JobQueue.dead``."""

    def __init__(self, db: Database, clock: Clock) -> None:
        self._db = db
        self._clock = clock

    def list(
        self,
        *,
        queue: str | None = None,
        handler: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        """Dead jobs, most recently buried first."""
        where, params = self._filter(queue, handler)
        rows = self._db.query(
            f"SELECT * FROM jobs WHERE {where} ORDER BY finished_at DESC, id DESC "
            "LIMIT ? OFFSET ?",
            [*params, limit, offset],
        )
        return [Job.from_row(row) for row in rows]

    def count(self, *, queue: str | None = None, handler: str | None = None) -> int:
        where, params = self._filter(queue, handler)
        return int(self._db.query(f"SELECT COUNT(*) FROM jobs WHERE {where}", params)[0][0])

    def retry(self, job_id: int, *, delay: float = 0.0, reset_attempts: bool = True) -> Job:
        """Put one dead job back in the queue.

        With ``reset_attempts`` the job gets its full allowance again; without
        it the job gets a single further try. Raises DuplicateJob if another
        active job has taken the same unique key since this one died.
        """
        if delay < 0:
            raise ValueError("delay cannot be negative")
        now = self._clock.now()
        with self._db.transaction() as conn:
            job = fetch_job(conn, job_id)
            if job.state is not JobState.DEAD:
                raise InvalidTransition(f"job {job_id} is {job.state.value}, not dead")
            if job.unique_key is not None:
                holder = find_active_by_key(conn, job.queue, job.unique_key)
                if holder is not None:
                    raise DuplicateJob(job.queue, job.unique_key, holder.id)
            conn.execute(
                """
                UPDATE jobs SET state = 'queued', run_at = ?, finished_at = NULL,
                    attempts = CASE WHEN ? THEN 0 ELSE attempts END, updated_at = ?
                WHERE id = ?
                """,
                (now + delay, reset_attempts, now, job_id),
            )
            return fetch_job(conn, job_id)

    def retry_all(
        self,
        *,
        queue: str | None = None,
        handler: str | None = None,
        delay: float = 0.0,
        reset_attempts: bool = True,
    ) -> tuple[list[Job], list[int]]:
        """Revive every matching dead job in one transaction.

        Returns the revived jobs and the ids that were skipped because an active
        job already holds their unique key.
        """
        where, params = self._filter(queue, handler)
        revived: list[Job] = []
        skipped: list[int] = []
        with self._db.transaction() as conn:
            rows = conn.execute(f"SELECT id FROM jobs WHERE {where} ORDER BY id", params).fetchall()
            for row in rows:
                try:
                    revived.append(
                        self.retry(row["id"], delay=delay, reset_attempts=reset_attempts)
                    )
                except DuplicateJob:
                    skipped.append(row["id"])
        return revived, skipped

    def purge(
        self,
        *,
        older_than: float | None = None,
        queue: str | None = None,
        job_ids: Iterable[int] | None = None,
    ) -> int:
        """Delete dead jobs and their attempt history; returns how many went."""
        cutoff = None if older_than is None else self._clock.now() - older_than
        with self._db.transaction() as conn:
            return delete_finished(
                conn, [JobState.DEAD], finished_before=cutoff, queue=queue, job_ids=job_ids
            )

    @staticmethod
    def _filter(queue: str | None, handler: str | None) -> tuple[str, list[Any]]:
        clauses = ["state = 'dead'"]
        params: list[Any] = []
        if queue is not None:
            clauses.append("queue = ?")
            params.append(queue)
        if handler is not None:
            clauses.append("handler = ?")
            params.append(handler)
        return " AND ".join(clauses), params
