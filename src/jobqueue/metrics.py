"""Point-in-time queue metrics, computed from the jobs and attempt tables."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Any

from .db import Database


@dataclass(slots=True)
class QueueMetrics:
    queue: str
    ready: int = 0
    delayed: int = 0
    leased: int = 0
    expired_leases: int = 0
    succeeded: int = 0
    dead: int = 0
    cancelled: int = 0
    oldest_ready_age: float | None = None

    @property
    def backlog(self) -> int:
        return self.ready + self.delayed


@dataclass(slots=True)
class HandlerMetrics:
    """How one handler's attempts ended during the metrics window."""

    handler: str
    succeeded: int = 0
    retried: int = 0
    dead: int = 0
    expired: int = 0
    mean_seconds: float | None = None
    p95_seconds: float | None = None
    max_seconds: float | None = None

    @property
    def finished(self) -> int:
        return self.succeeded + self.retried + self.dead + self.expired

    @property
    def failure_rate(self) -> float | None:
        if self.finished == 0:
            return None
        return (self.finished - self.succeeded) / self.finished


@dataclass(slots=True)
class MetricsSnapshot:
    at: float
    window_seconds: float
    queues: list[QueueMetrics] = field(default_factory=list)
    handlers: list[HandlerMetrics] = field(default_factory=list)
    schedules_total: int = 0
    schedules_paused: int = 0
    next_schedule_at: float | None = None

    def queue(self, name: str) -> QueueMetrics:
        """Metrics for one queue; an unseen queue is simply all zeroes."""
        for row in self.queues:
            if row.queue == name:
                return row
        return QueueMetrics(name)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _percentile(sorted_values: list[float], fraction: float) -> float:
    # Nearest-rank percentile.
    rank = max(1, math.ceil(fraction * len(sorted_values)))
    return sorted_values[rank - 1]


def _queue_metrics(db: Database, now: float) -> list[QueueMetrics]:
    rows = db.query(
        """
        SELECT queue,
            SUM(state = 'queued' AND run_at <= :now) AS ready,
            SUM(state = 'queued' AND run_at >  :now) AS delayed,
            SUM(state = 'leased') AS leased,
            SUM(state = 'leased' AND lease_expires_at <= :now) AS expired_leases,
            SUM(state = 'succeeded') AS succeeded,
            SUM(state = 'dead') AS dead,
            SUM(state = 'cancelled') AS cancelled,
            MIN(CASE WHEN state = 'queued' AND run_at <= :now THEN run_at END) AS oldest_ready
        FROM jobs GROUP BY queue ORDER BY queue
        """,
        {"now": now},
    )
    return [
        QueueMetrics(
            queue=row["queue"],
            ready=row["ready"],
            delayed=row["delayed"],
            leased=row["leased"],
            expired_leases=row["expired_leases"],
            succeeded=row["succeeded"],
            dead=row["dead"],
            cancelled=row["cancelled"],
            oldest_ready_age=None if row["oldest_ready"] is None else now - row["oldest_ready"],
        )
        for row in rows
    ]


def _handler_metrics(db: Database, now: float, window: float) -> list[HandlerMetrics]:
    rows = db.query(
        """
        SELECT j.handler, a.outcome, a.ended_at - a.started_at AS duration
        FROM job_attempts a JOIN jobs j ON j.id = a.job_id
        WHERE a.ended_at > ? AND a.outcome IN ('succeeded', 'retried', 'dead', 'expired')
        """,
        (now - window,),
    )
    handlers: dict[str, HandlerMetrics] = {}
    durations: dict[str, list[float]] = {}
    for row in rows:
        metrics = handlers.setdefault(row["handler"], HandlerMetrics(row["handler"]))
        outcome = row["outcome"]
        setattr(metrics, outcome, getattr(metrics, outcome) + 1)
        # An expired attempt ends when somebody noticed, not when the handler
        # stopped, so its duration says nothing about the handler.
        if outcome != "expired":
            durations.setdefault(row["handler"], []).append(row["duration"])

    for name, values in durations.items():
        values.sort()
        metrics = handlers[name]
        metrics.mean_seconds = sum(values) / len(values)
        metrics.p95_seconds = _percentile(values, 0.95)
        metrics.max_seconds = values[-1]
    return [handlers[name] for name in sorted(handlers)]


def collect_metrics(db: Database, now: float, *, window: float = 3600.0) -> MetricsSnapshot:
    """Take a snapshot. ``window`` bounds the per-handler attempt figures, in seconds."""
    if window <= 0:
        raise ValueError("window must be positive")
    schedules = db.query(
        """
        SELECT COUNT(*) AS total, COALESCE(SUM(paused), 0) AS paused,
            MIN(CASE WHEN paused = 0 THEN next_run_at END) AS next_at
        FROM schedules
        """
    )[0]
    return MetricsSnapshot(
        at=now,
        window_seconds=window,
        queues=_queue_metrics(db, now),
        handlers=_handler_metrics(db, now, window),
        schedules_total=schedules["total"],
        schedules_paused=schedules["paused"],
        next_schedule_at=schedules["next_at"],
    )
