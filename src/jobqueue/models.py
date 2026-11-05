"""Plain value types returned by the queue. None of them hold a database handle."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class JobState(StrEnum):
    QUEUED = "queued"
    LEASED = "leased"
    SUCCEEDED = "succeeded"
    DEAD = "dead"
    CANCELLED = "cancelled"


ACTIVE_STATES = frozenset({JobState.QUEUED, JobState.LEASED})
TERMINAL_STATES = frozenset({JobState.SUCCEEDED, JobState.DEAD, JobState.CANCELLED})


class AttemptOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    RETRIED = "retried"
    DEAD = "dead"
    EXPIRED = "expired"
    RELEASED = "released"
    CANCELLED = "cancelled"


class Misfire(StrEnum):
    """What a schedule does about fire times that passed while nothing was promoting.

    COALESCE enqueues one job for the whole gap. CATCH_UP enqueues one per missed
    fire time, up to a cap.
    """

    COALESCE = "coalesce"
    CATCH_UP = "catch_up"


def _decode(text: str | None) -> Any:
    return None if text is None else json.loads(text)


@dataclass(frozen=True, slots=True)
class Job:
    id: int
    queue: str
    handler: str
    payload: Any
    state: JobState
    priority: int
    run_at: float
    unique_key: str | None
    attempts: int
    max_attempts: int
    backoff: str | None
    lease_token: str | None
    lease_owner: str | None
    lease_expires_at: float | None
    last_error: str | None
    result: Any
    created_at: float
    updated_at: float
    finished_at: float | None
    schedule_id: int | None
    scheduled_for: float | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Job":
        return cls(
            id=row["id"],
            queue=row["queue"],
            handler=row["handler"],
            payload=_decode(row["payload"]),
            state=JobState(row["state"]),
            priority=row["priority"],
            run_at=row["run_at"],
            unique_key=row["unique_key"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            backoff=row["backoff"],
            lease_token=row["lease_token"],
            lease_owner=row["lease_owner"],
            lease_expires_at=row["lease_expires_at"],
            last_error=row["last_error"],
            result=_decode(row["result"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            finished_at=row["finished_at"],
            schedule_id=row["schedule_id"],
            scheduled_for=row["scheduled_for"],
        )

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def attempts_left(self) -> int:
        return max(0, self.max_attempts - self.attempts)

    def to_dict(self) -> dict[str, Any]:
        """A JSON-ready view. The lease token is left out: it is a capability."""
        return {
            "id": self.id,
            "queue": self.queue,
            "handler": self.handler,
            "payload": self.payload,
            "state": self.state.value,
            "priority": self.priority,
            "run_at": self.run_at,
            "unique_key": self.unique_key,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "backoff": self.backoff,
            "lease_owner": self.lease_owner,
            "lease_expires_at": self.lease_expires_at,
            "last_error": self.last_error,
            "result": self.result,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "finished_at": self.finished_at,
            "schedule_id": self.schedule_id,
            "scheduled_for": self.scheduled_for,
        }


@dataclass(frozen=True, slots=True)
class EnqueueResult:
    """``created`` is False when a unique key matched an active job, which is then
    returned instead of a new one."""

    job: Job
    created: bool


@dataclass(frozen=True, slots=True)
class Attempt:
    """One run of a job. ``outcome`` stays None while the attempt is in progress."""

    job_id: int
    attempt: int
    worker: str
    started_at: float
    ended_at: float | None
    outcome: AttemptOutcome | None
    error: str | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Attempt":
        outcome = row["outcome"]
        return cls(
            job_id=row["job_id"],
            attempt=row["attempt"],
            worker=row["worker"],
            started_at=row["started_at"],
            ended_at=row["ended_at"],
            outcome=None if outcome is None else AttemptOutcome(outcome),
            error=row["error"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "attempt": self.attempt,
            "worker": self.worker,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "outcome": None if self.outcome is None else self.outcome.value,
            "error": self.error,
        }


@dataclass(frozen=True, slots=True)
class Schedule:
    id: int
    name: str
    kind: str
    expr: str
    handler: str
    payload: Any
    queue: str
    priority: int
    max_attempts: int
    backoff: str | None
    misfire: Misfire
    paused: bool
    next_run_at: float
    last_fired_at: float | None
    created_at: float
    updated_at: float

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Schedule":
        return cls(
            id=row["id"],
            name=row["name"],
            kind=row["kind"],
            expr=row["expr"],
            handler=row["handler"],
            payload=_decode(row["payload"]),
            queue=row["queue"],
            priority=row["priority"],
            max_attempts=row["max_attempts"],
            backoff=row["backoff"],
            misfire=Misfire(row["misfire"]),
            paused=bool(row["paused"]),
            next_run_at=row["next_run_at"],
            last_fired_at=row["last_fired_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "kind": self.kind,
            "expr": self.expr,
            "handler": self.handler,
            "payload": self.payload,
            "queue": self.queue,
            "priority": self.priority,
            "max_attempts": self.max_attempts,
            "backoff": self.backoff,
            "misfire": self.misfire.value,
            "paused": self.paused,
            "next_run_at": self.next_run_at,
            "last_fired_at": self.last_fired_at,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
