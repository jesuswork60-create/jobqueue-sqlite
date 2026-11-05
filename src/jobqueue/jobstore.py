"""Low-level job rows, shared by the queue, the dead letters and the schedules.

Every function takes an open connection and leaves transaction control to the
caller.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from typing import Any

from .errors import JobNotFound, PayloadError
from .models import AttemptOutcome, Job, JobState

# Tracebacks can be huge. Keep the end, which is where the actual error is.
MAX_ERROR_CHARS = 8000


def encode_json(value: Any, what: str = "payload") -> str:
    try:
        return json.dumps(value, separators=(",", ":"), sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise PayloadError(f"{what} is not JSON serialisable: {exc}") from exc


def trim_error(error: str) -> str:
    if len(error) <= MAX_ERROR_CHARS:
        return error
    return "[truncated]\n" + error[-MAX_ERROR_CHARS:]


def fetch_job(conn: sqlite3.Connection, job_id: int) -> Job:
    row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if row is None:
        raise JobNotFound(job_id)
    return Job.from_row(row)


def find_active_by_key(conn: sqlite3.Connection, queue: str, unique_key: str) -> Job | None:
    row = conn.execute(
        """
        SELECT * FROM jobs
        WHERE queue = ? AND unique_key = ? AND state IN ('queued', 'leased')
        """,
        (queue, unique_key),
    ).fetchone()
    return None if row is None else Job.from_row(row)


def insert_job(
    conn: sqlite3.Connection,
    *,
    queue: str,
    handler: str,
    payload_json: str,
    priority: int,
    run_at: float,
    max_attempts: int,
    now: float,
    unique_key: str | None = None,
    backoff: str | None = None,
    schedule_id: int | None = None,
    scheduled_for: float | None = None,
) -> int | None:
    """Insert a queued job and return its id.

    Returns None if the schedule-fire index says this fire time already has a
    job, which is how a repeated promotion stays harmless.
    """
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO jobs (
            queue, handler, payload, state, priority, run_at, unique_key,
            max_attempts, backoff, created_at, updated_at, schedule_id, scheduled_for
        ) VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            queue,
            handler,
            payload_json,
            priority,
            run_at,
            unique_key,
            max_attempts,
            backoff,
            now,
            now,
            schedule_id,
            scheduled_for,
        ),
    )
    return cursor.lastrowid if cursor.rowcount else None


def close_attempt(
    conn: sqlite3.Connection,
    job_id: int,
    now: float,
    outcome: AttemptOutcome,
    error: str | None = None,
) -> None:
    """Record how the attempt currently in progress ended, if there is one."""
    conn.execute(
        """
        UPDATE job_attempts SET ended_at = ?, outcome = ?, error = ?
        WHERE job_id = ? AND ended_at IS NULL
        """,
        (now, outcome.value, error, job_id),
    )


def bury(conn: sqlite3.Connection, job_id: int, now: float, error: str | None) -> None:
    """Move a job to the dead letters, dropping any lease it has."""
    conn.execute(
        """
        UPDATE jobs SET state = 'dead', last_error = COALESCE(?, last_error),
            lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL,
            finished_at = ?, updated_at = ?
        WHERE id = ?
        """,
        (error, now, now, job_id),
    )


def requeue(
    conn: sqlite3.Connection,
    job_id: int,
    now: float,
    run_at: float,
    error: str | None,
) -> None:
    """Put a job back in line, dropping any lease it has."""
    conn.execute(
        """
        UPDATE jobs SET state = 'queued', run_at = ?, last_error = COALESCE(?, last_error),
            lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL,
            updated_at = ?
        WHERE id = ?
        """,
        (run_at, error, now, job_id),
    )


def delete_finished(
    conn: sqlite3.Connection,
    states: Iterable[JobState],
    *,
    finished_before: float | None = None,
    queue: str | None = None,
    job_ids: Iterable[int] | None = None,
) -> int:
    """Delete jobs in the given states, narrowed by the optional filters."""
    state_values = [state.value for state in states]
    clauses = [f"state IN ({', '.join('?' * len(state_values))})"]
    params: list[Any] = list(state_values)
    if finished_before is not None:
        clauses.append("finished_at <= ?")
        params.append(finished_before)
    if queue is not None:
        clauses.append("queue = ?")
        params.append(queue)
    if job_ids is not None:
        ids = list(job_ids)
        clauses.append(f"id IN ({', '.join('?' * len(ids))})")
        params.extend(ids)
    cursor = conn.execute(f"DELETE FROM jobs WHERE {' AND '.join(clauses)}", params)
    return cursor.rowcount
