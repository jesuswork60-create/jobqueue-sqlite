"""Recurring schedules, promoted into ordinary jobs when they come due.

Inserting the job and advancing ``next_run_at`` happen in one transaction, so a
crash can never lose a fire or repeat one. If a schedule row is ever rewound (a
restored backup, say), the ``(schedule_id, scheduled_for)`` unique index still
stops the same fire time from producing a second job.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from .backoff import Backoff, parse_backoff
from .clock import Clock
from .db import Database
from .durations import parse_duration
from .errors import DuplicateSchedule, ScheduleError, ScheduleNotFound
from .jobstore import encode_json, insert_job
from .models import Misfire, Schedule
from .triggers import CronTrigger, IntervalTrigger, Trigger, trigger_from


@dataclass(frozen=True, slots=True)
class Promotion:
    """What one schedule produced in one promotion pass.

    ``truncated`` means a catch-up hit the cap and later missed fires were skipped.
    """

    schedule: str
    job_ids: tuple[int, ...]
    next_run_at: float
    truncated: bool = False


def _build_trigger(every: str | float | None, cron: str | None) -> Trigger:
    if (every is None) == (cron is None):
        raise ScheduleError("a schedule needs exactly one of every= or cron=")
    if cron is not None:
        return CronTrigger(cron)
    return IntervalTrigger(parse_duration(every))  # type: ignore[arg-type]


class ScheduleStore:
    """The recurring schedules of a queue, available as ``JobQueue.schedules``."""

    def __init__(
        self,
        db: Database,
        clock: Clock,
        *,
        default_max_attempts: int,
        max_catchup: int = 100,
    ) -> None:
        self._db = db
        self._clock = clock
        self._default_max_attempts = default_max_attempts
        self._max_catchup = max_catchup

    def add(
        self,
        name: str,
        handler: str,
        *,
        every: str | float | None = None,
        cron: str | None = None,
        payload: Any = None,
        queue: str = "default",
        priority: int = 0,
        max_attempts: int | None = None,
        backoff: str | Backoff | None = None,
        misfire: Misfire | str = Misfire.COALESCE,
        start_at: float | None = None,
        replace: bool = False,
    ) -> Schedule:
        """Create a schedule, or with ``replace`` update the one of this name.

        Replacing with an unchanged trigger keeps ``next_run_at``, so declaring
        schedules at every application start does not keep pushing them back.
        """
        if not name:
            raise ScheduleError("a schedule needs a name")
        if not handler:
            raise ScheduleError("a schedule needs a handler name")
        trigger = _build_trigger(every, cron)
        misfire = Misfire(misfire)
        attempts = self._default_max_attempts if max_attempts is None else max_attempts
        if attempts < 1:
            raise ScheduleError("max_attempts must be at least 1")
        backoff_spec = self._backoff_spec(backoff)
        payload_json = encode_json(payload)
        now = self._clock.now()

        with self._db.transaction() as conn:
            existing = conn.execute("SELECT * FROM schedules WHERE name = ?", (name,)).fetchone()
            if existing is not None and not replace:
                raise DuplicateSchedule(name)

            next_run_at = start_at if start_at is not None else trigger.first_fire(now)
            if existing is not None:
                same_trigger = existing["kind"] == trigger.kind and existing["expr"] == trigger.expr
                if same_trigger and start_at is None:
                    next_run_at = existing["next_run_at"]
                conn.execute(
                    """
                    UPDATE schedules SET kind = ?, expr = ?, handler = ?, payload = ?, queue = ?,
                        priority = ?, max_attempts = ?, backoff = ?, misfire = ?,
                        next_run_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        trigger.kind,
                        trigger.expr,
                        handler,
                        payload_json,
                        queue,
                        priority,
                        attempts,
                        backoff_spec,
                        misfire.value,
                        next_run_at,
                        now,
                        existing["id"],
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO schedules (
                        name, kind, expr, handler, payload, queue, priority, max_attempts,
                        backoff, misfire, next_run_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        name,
                        trigger.kind,
                        trigger.expr,
                        handler,
                        payload_json,
                        queue,
                        priority,
                        attempts,
                        backoff_spec,
                        misfire.value,
                        next_run_at,
                        now,
                        now,
                    ),
                )
            return self._fetch(conn, name)

    @staticmethod
    def _backoff_spec(backoff: str | Backoff | None) -> str | None:
        if backoff is None:
            return None
        if isinstance(backoff, str):
            parse_backoff(backoff)  # a bad one must not surface at the first failure
            return backoff
        spec = backoff.spec()
        if spec is None:
            raise ScheduleError("a schedule's backoff policy must have a text form")
        return spec

    @staticmethod
    def _fetch(conn: sqlite3.Connection, name: str) -> Schedule:
        row = conn.execute("SELECT * FROM schedules WHERE name = ?", (name,)).fetchone()
        if row is None:
            raise ScheduleNotFound(name)
        return Schedule.from_row(row)

    def get(self, name: str) -> Schedule:
        rows = self._db.query("SELECT * FROM schedules WHERE name = ?", (name,))
        if not rows:
            raise ScheduleNotFound(name)
        return Schedule.from_row(rows[0])

    def list(self) -> list[Schedule]:
        rows = self._db.query("SELECT * FROM schedules ORDER BY name")
        return [Schedule.from_row(row) for row in rows]

    def remove(self, name: str) -> None:
        """Delete a schedule. Jobs it already produced stay and keep running."""
        with self._db.transaction() as conn:
            cursor = conn.execute("DELETE FROM schedules WHERE name = ?", (name,))
            if cursor.rowcount == 0:
                raise ScheduleNotFound(name)

    def pause(self, name: str) -> Schedule:
        now = self._clock.now()
        with self._db.transaction() as conn:
            schedule = self._fetch(conn, name)
            conn.execute(
                "UPDATE schedules SET paused = 1, updated_at = ? WHERE id = ?",
                (now, schedule.id),
            )
            return self._fetch(conn, name)

    def resume(self, name: str) -> Schedule:
        """Unpause. Fire times that passed during the pause are dropped on purpose."""
        now = self._clock.now()
        with self._db.transaction() as conn:
            schedule = self._fetch(conn, name)
            next_run_at = schedule.next_run_at
            if schedule.paused and next_run_at <= now:
                next_run_at = trigger_from(schedule.kind, schedule.expr).first_fire(now)
            conn.execute(
                "UPDATE schedules SET paused = 0, next_run_at = ?, updated_at = ? WHERE id = ?",
                (next_run_at, now, schedule.id),
            )
            return self._fetch(conn, name)

    def promote_due(self) -> list[Promotion]:
        """Turn every due schedule into jobs. Safe to call from any number of workers."""
        now = self._clock.now()
        # Polling workers call this constantly; do not take the write lock for nothing.
        due = self._db.query(
            "SELECT 1 FROM schedules WHERE paused = 0 AND next_run_at <= ? LIMIT 1", (now,)
        )
        if not due:
            return []

        with self._db.transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM schedules WHERE paused = 0 AND next_run_at <= ? "
                "ORDER BY next_run_at, id",
                (now,),
            ).fetchall()
            return [self._promote(conn, row, now) for row in rows]

    def _promote(self, conn: sqlite3.Connection, row: sqlite3.Row, now: float) -> Promotion:
        schedule = Schedule.from_row(row)
        trigger = trigger_from(schedule.kind, schedule.expr)
        fires, next_run_at, truncated = self._plan_fires(schedule, trigger, now)

        job_ids = []
        for fire in fires:
            job_id = insert_job(
                conn,
                queue=schedule.queue,
                handler=schedule.handler,
                payload_json=row["payload"],
                priority=schedule.priority,
                run_at=fire,
                max_attempts=schedule.max_attempts,
                backoff=schedule.backoff,
                now=now,
                schedule_id=schedule.id,
                scheduled_for=fire,
            )
            if job_id is not None:
                job_ids.append(job_id)

        conn.execute(
            "UPDATE schedules SET next_run_at = ?, last_fired_at = ?, updated_at = ? WHERE id = ?",
            (next_run_at, fires[-1], now, schedule.id),
        )
        return Promotion(schedule.name, tuple(job_ids), next_run_at, truncated)

    def _plan_fires(
        self, schedule: Schedule, trigger: Trigger, now: float
    ) -> tuple[list[float], float, bool]:
        first = schedule.next_run_at
        if schedule.misfire is Misfire.COALESCE:
            return [first], trigger.skip_past(first, now), False

        fires = []
        fire = first
        while fire <= now and len(fires) < self._max_catchup:
            fires.append(fire)
            fire = trigger.next_fire(fire)
        if fire <= now:
            return fires, trigger.skip_past(fire, now), True
        return fires, fire, False
