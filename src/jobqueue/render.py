"""Plain-text rendering for the command line."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from .durations import format_duration, format_timestamp
from .metrics import HandlerMetrics, MetricsSnapshot
from .models import Attempt, Job, Schedule


def table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    widths = [len(header) for header in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))
    lines = []
    for row in [headers, *rows]:
        cells = (cell.ljust(width) for cell, width in zip(row, widths))
        lines.append("  ".join(cells).rstrip())
    return "\n".join(lines)


def shorten(text: str, limit: int = 48) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."


def last_line(error: str | None) -> str:
    """The most telling line of a traceback: its last non-empty one."""
    if not error:
        return "-"
    lines = [line for line in error.splitlines() if line.strip()]
    return lines[-1].strip() if lines else "-"


def to_json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True)


def job_table(jobs: Sequence[Job]) -> str:
    return table(
        ("ID", "STATE", "QUEUE", "HANDLER", "PRI", "TRIES", "RUN AT"),
        [
            (
                str(job.id),
                job.state.value,
                job.queue,
                job.handler,
                str(job.priority),
                f"{job.attempts}/{job.max_attempts}",
                format_timestamp(job.run_at),
            )
            for job in jobs
        ],
    )


def dead_table(jobs: Sequence[Job]) -> str:
    return table(
        ("ID", "QUEUE", "HANDLER", "TRIES", "DIED AT", "LAST ERROR"),
        [
            (
                str(job.id),
                job.queue,
                job.handler,
                f"{job.attempts}/{job.max_attempts}",
                format_timestamp(job.finished_at),
                shorten(last_line(job.last_error)),
            )
            for job in jobs
        ],
    )


def job_detail(job: Job, attempts: Sequence[Attempt]) -> str:
    fields = [
        ("id", str(job.id)),
        ("state", job.state.value),
        ("queue", job.queue),
        ("handler", job.handler),
        ("payload", json.dumps(job.payload)),
        ("priority", str(job.priority)),
        ("attempts", f"{job.attempts}/{job.max_attempts}"),
        ("run at", format_timestamp(job.run_at)),
        ("created", format_timestamp(job.created_at)),
    ]
    if job.unique_key is not None:
        fields.append(("unique key", job.unique_key))
    if job.backoff is not None:
        fields.append(("backoff", job.backoff))
    if job.lease_owner is not None:
        fields.append(("leased by", job.lease_owner))
        fields.append(("lease ends", format_timestamp(job.lease_expires_at)))
    if job.finished_at is not None:
        fields.append(("finished", format_timestamp(job.finished_at)))
    if job.result is not None:
        fields.append(("result", json.dumps(job.result)))
    if job.scheduled_for is not None:
        fields.append(("scheduled", format_timestamp(job.scheduled_for)))
    width = max(len(name) for name, _ in fields)
    lines = [f"{name.ljust(width)}  {value}" for name, value in fields]

    if attempts:
        lines += ["", "attempts:"]
        lines.append(
            table(
                ("#", "WORKER", "STARTED", "TOOK", "OUTCOME", "ERROR"),
                [
                    (
                        str(attempt.attempt),
                        attempt.worker,
                        format_timestamp(attempt.started_at),
                        "-"
                        if attempt.ended_at is None
                        else format_duration(attempt.ended_at - attempt.started_at),
                        "running" if attempt.outcome is None else attempt.outcome.value,
                        shorten(last_line(attempt.error)),
                    )
                    for attempt in attempts
                ],
            )
        )
    if job.last_error:
        lines += ["", "last error:", job.last_error.rstrip()]
    return "\n".join(lines)


def schedule_table(schedules: Sequence[Schedule]) -> str:
    def describe(schedule: Schedule) -> str:
        if schedule.kind == "interval":
            return f"every {format_duration(float(schedule.expr))}"
        return schedule.expr

    return table(
        ("NAME", "WHEN", "HANDLER", "QUEUE", "MISFIRE", "NEXT RUN", "LAST FIRE"),
        [
            (
                schedule.name,
                describe(schedule),
                schedule.handler,
                schedule.queue,
                schedule.misfire.value,
                "paused" if schedule.paused else format_timestamp(schedule.next_run_at),
                format_timestamp(schedule.last_fired_at),
            )
            for schedule in schedules
        ],
    )


def _seconds(value: float | None) -> str:
    return "-" if value is None else format_duration(value)


def _failure_rate(metrics: HandlerMetrics) -> str:
    rate = metrics.failure_rate
    return "-" if rate is None else f"{rate:.0%}"


def status_report(snapshot: MetricsSnapshot) -> str:
    if not snapshot.queues:
        sections = ["no jobs yet"]
    else:
        sections = [
            table(
                ("QUEUE", "READY", "DELAYED", "LEASED", "EXPIRED", "DONE", "DEAD", "CANCELLED",
                 "OLDEST READY"),
                [
                    (
                        row.queue,
                        str(row.ready),
                        str(row.delayed),
                        str(row.leased),
                        str(row.expired_leases),
                        str(row.succeeded),
                        str(row.dead),
                        str(row.cancelled),
                        _seconds(row.oldest_ready_age),
                    )
                    for row in snapshot.queues
                ],
            )
        ]
    if snapshot.handlers:
        window = format_duration(snapshot.window_seconds)
        sections.append(
            f"attempts in the last {window}:\n"
            + table(
                ("HANDLER", "OK", "RETRIED", "DEAD", "EXPIRED", "FAIL%", "MEAN", "P95", "MAX"),
                [
                    (
                        row.handler,
                        str(row.succeeded),
                        str(row.retried),
                        str(row.dead),
                        str(row.expired),
                        _failure_rate(row),
                        _seconds(row.mean_seconds),
                        _seconds(row.p95_seconds),
                        _seconds(row.max_seconds),
                    )
                    for row in snapshot.handlers
                ],
            )
        )
    if snapshot.schedules_total:
        sections.append(
            f"schedules: {snapshot.schedules_total} "
            f"({snapshot.schedules_paused} paused), "
            f"next fire {format_timestamp(snapshot.next_schedule_at)}"
        )
    return "\n\n".join(sections)
