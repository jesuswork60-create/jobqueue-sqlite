"""The ``jobqueue`` command line tool."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sqlite3
import sys
from collections.abc import Callable, Sequence
from types import FrameType
from typing import Any, TextIO

from . import render
from .clock import Clock
from .demo import registry as demo_registry
from .durations import format_duration, format_timestamp, parse_duration
from .errors import JobQueueError
from .models import JobState, Misfire, TERMINAL_STATES
from .queue import DEFAULT_QUEUE, JobQueue
from .registry import HandlerRegistry, load_registry
from .worker import Worker, WorkerEvent


def _json_value(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(f"not valid JSON: {exc}") from exc


def _emit(args: argparse.Namespace, out: TextIO, value: Any, text: str) -> None:
    print(render.to_json(value) if getattr(args, "json", False) else text, file=out)


# Commands. Each returns the process exit code.


def cmd_enqueue(args: argparse.Namespace, queue: JobQueue, out: TextIO) -> int:
    outcome = queue.enqueue(
        args.handler,
        args.payload,
        queue=args.queue,
        priority=args.priority,
        delay=args.delay,
        unique_key=args.unique,
        max_attempts=args.max_attempts,
        backoff=args.backoff,
    )
    verb = "enqueued" if outcome.created else "already active:"
    summary = f"{verb} job {outcome.job.id} on queue {outcome.job.queue}"
    _emit(args, out, {"created": outcome.created, "job": outcome.job.to_dict()}, summary)
    return 0


def cmd_list(args: argparse.Namespace, queue: JobQueue, out: TextIO) -> int:
    jobs = queue.list_jobs(
        state=args.state,
        queue=args.queue,
        handler=args.handler,
        limit=args.limit,
        offset=args.offset,
    )
    text = render.job_table(jobs) if jobs else "no matching jobs"
    _emit(args, out, [job.to_dict() for job in jobs], text)
    return 0


def cmd_show(args: argparse.Namespace, queue: JobQueue, out: TextIO) -> int:
    job = queue.get(args.id)
    attempts = queue.attempts(args.id)
    value = {**job.to_dict(), "history": [attempt.to_dict() for attempt in attempts]}
    _emit(args, out, value, render.job_detail(job, attempts))
    return 0


def cmd_status(args: argparse.Namespace, queue: JobQueue, out: TextIO) -> int:
    snapshot = queue.stats(window=args.window)
    _emit(args, out, snapshot.to_dict(), render.status_report(snapshot))
    return 0


def cmd_cancel(args: argparse.Namespace, queue: JobQueue, out: TextIO) -> int:
    job = queue.cancel(args.id)
    print(f"cancelled job {job.id}", file=out)
    return 0


def cmd_dead(args: argparse.Namespace, queue: JobQueue, out: TextIO) -> int:
    jobs = queue.dead.list(queue=args.queue, handler=args.handler, limit=args.limit)
    text = render.dead_table(jobs) if jobs else "no dead jobs"
    _emit(args, out, [job.to_dict() for job in jobs], text)
    return 0


def cmd_retry(args: argparse.Namespace, queue: JobQueue, out: TextIO) -> int:
    if args.all == bool(args.ids):
        raise ValueError("give either job ids or --all")
    reset = not args.keep_attempts
    if args.all:
        revived, skipped = queue.dead.retry_all(
            queue=args.queue, handler=args.handler, delay=args.delay, reset_attempts=reset
        )
        print(f"requeued {len(revived)} dead job(s)", file=out)
        for job_id in skipped:
            print(f"skipped job {job_id}: its unique key is held by an active job", file=out)
        return 0
    for job_id in args.ids:
        queue.dead.retry(job_id, delay=args.delay, reset_attempts=reset)
        print(f"requeued job {job_id}", file=out)
    return 0


def cmd_purge(args: argparse.Namespace, queue: JobQueue, out: TextIO) -> int:
    removed = queue.prune(
        older_than=args.older_than,
        states=[JobState(state) for state in args.state],
        queue=args.queue,
    )
    print(f"purged {removed} job(s)", file=out)
    return 0


def cmd_schedule_add(args: argparse.Namespace, queue: JobQueue, out: TextIO) -> int:
    schedule = queue.schedules.add(
        args.name,
        args.handler,
        every=args.every,
        cron=args.cron,
        payload=args.payload,
        queue=args.queue,
        priority=args.priority,
        max_attempts=args.max_attempts,
        backoff=args.backoff,
        misfire=args.misfire,
        replace=args.replace,
    )
    print(f"schedule {schedule.name}: next run {format_timestamp(schedule.next_run_at)}", file=out)
    return 0


def cmd_schedule_list(args: argparse.Namespace, queue: JobQueue, out: TextIO) -> int:
    schedules = queue.schedules.list()
    text = render.schedule_table(schedules) if schedules else "no schedules"
    _emit(args, out, [schedule.to_dict() for schedule in schedules], text)
    return 0


def cmd_schedule_remove(args: argparse.Namespace, queue: JobQueue, out: TextIO) -> int:
    queue.schedules.remove(args.name)
    print(f"removed schedule {args.name}", file=out)
    return 0


def cmd_schedule_pause(args: argparse.Namespace, queue: JobQueue, out: TextIO) -> int:
    queue.schedules.pause(args.name)
    print(f"paused schedule {args.name}", file=out)
    return 0


def cmd_schedule_resume(args: argparse.Namespace, queue: JobQueue, out: TextIO) -> int:
    queue.schedules.resume(args.name)
    print(f"resumed schedule {args.name}", file=out)
    return 0


def _build_registry(references: Sequence[str], with_demo: bool) -> HandlerRegistry:
    # A console script does not have the working directory on sys.path, and
    # 'work --handlers tasks:registry' should find ./tasks.py.
    if os.getcwd() not in sys.path:
        sys.path.insert(0, os.getcwd())
    merged = HandlerRegistry()
    for reference in references:
        merged.merge(load_registry(reference))
    if with_demo or not references:
        merged.merge(demo_registry)
    return merged


def cmd_work(args: argparse.Namespace, queue: JobQueue, out: TextIO) -> int:
    registry = _build_registry(args.handlers, args.with_demo)

    def report(event: WorkerEvent) -> None:
        if event.kind in ("started", "renewed"):
            return
        took = "" if event.elapsed is None else f" in {format_duration(event.elapsed)}"
        print(f"job {event.job.id} {event.job.handler} {event.kind}{took}", file=out, flush=True)

    worker = Worker(
        queue,
        registry,
        queues=args.queue or None,
        worker_id=args.id,
        concurrency=args.concurrency,
        lease_seconds=args.lease,
        poll_interval=args.poll,
        drain_timeout=args.drain_timeout,
        on_event=None if args.quiet else report,
    )

    def request_stop(signum: int, frame: FrameType | None) -> None:
        worker.stop()
        # A second signal should take effect at once, however stuck a handler is.
        signal.signal(signum, previous[signum] or signal.SIG_DFL)

    previous: dict[int, Callable[..., Any] | int | None] = {
        signum: signal.signal(signum, request_stop)
        for signum in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        stats = worker.run(until_idle=args.burst, max_jobs=args.max_jobs)
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler or signal.SIG_DFL)

    print(
        f"worker finished: {stats.succeeded} succeeded, {stats.retried} retried, "
        f"{stats.dead} dead, {stats.lost} lost, {stats.released} released",
        file=out,
    )
    return 0


# Argument parsing


def _add_job_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--payload", type=_json_value, default=None, help="JSON payload")
    parser.add_argument("--queue", default=DEFAULT_QUEUE)
    parser.add_argument("--priority", type=int, default=0, help="higher runs first")
    parser.add_argument("--max-attempts", type=int, default=None)
    parser.add_argument("--backoff", default=None, help="e.g. fixed:30 or exp:base=2,max=600")


def _add_json_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true", help="print JSON")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="jobqueue", description=__doc__)
    parser.add_argument(
        "--db",
        default=os.environ.get("JOBQUEUE_DB", "jobqueue.db"),
        help="database file (default: $JOBQUEUE_DB or ./jobqueue.db)",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    enqueue = commands.add_parser("enqueue", help="add a job")
    enqueue.add_argument("handler")
    _add_job_options(enqueue)
    enqueue.add_argument("--delay", type=parse_duration, default=0.0, help="e.g. 30s or 1h30m")
    enqueue.add_argument("--unique", default=None, help="deduplicate on this key")
    _add_json_flag(enqueue)
    enqueue.set_defaults(run=cmd_enqueue)

    listing = commands.add_parser("list", help="list jobs, newest first")
    listing.add_argument("--state", choices=[state.value for state in JobState])
    listing.add_argument("--queue")
    listing.add_argument("--handler")
    listing.add_argument("--limit", type=int, default=50)
    listing.add_argument("--offset", type=int, default=0)
    _add_json_flag(listing)
    listing.set_defaults(run=cmd_list)

    show = commands.add_parser("show", help="one job with its attempt history")
    show.add_argument("id", type=int)
    _add_json_flag(show)
    show.set_defaults(run=cmd_show)

    status = commands.add_parser("status", help="queue metrics")
    status.add_argument(
        "--window", type=parse_duration, default=3600.0, help="attempt figures cover this long"
    )
    _add_json_flag(status)
    status.set_defaults(run=cmd_status)

    cancel = commands.add_parser("cancel", help="cancel a queued or leased job")
    cancel.add_argument("id", type=int)
    cancel.set_defaults(run=cmd_cancel)

    dead = commands.add_parser("dead", help="list dead jobs")
    dead.add_argument("--queue")
    dead.add_argument("--handler")
    dead.add_argument("--limit", type=int, default=50)
    _add_json_flag(dead)
    dead.set_defaults(run=cmd_dead)

    retry = commands.add_parser("retry", help="requeue dead jobs")
    retry.add_argument("ids", nargs="*", type=int, help="job ids")
    retry.add_argument("--all", action="store_true", help="every matching dead job")
    retry.add_argument("--queue", help="with --all: only this queue")
    retry.add_argument("--handler", help="with --all: only this handler")
    retry.add_argument("--delay", type=parse_duration, default=0.0)
    retry.add_argument(
        "--keep-attempts", action="store_true", help="allow one more try, not a full allowance"
    )
    retry.set_defaults(run=cmd_retry)

    purge = commands.add_parser("purge", help="delete finished jobs and their history")
    purge.add_argument(
        "--state",
        action="append",
        required=True,
        choices=sorted(state.value for state in TERMINAL_STATES),
        help="repeatable",
    )
    purge.add_argument(
        "--older-than", type=parse_duration, default=0.0, help="only jobs finished this long ago"
    )
    purge.add_argument("--queue")
    purge.set_defaults(run=cmd_purge)

    schedule = commands.add_parser("schedule", help="manage recurring schedules")
    schedule_commands = schedule.add_subparsers(dest="schedule_command", required=True)

    add = schedule_commands.add_parser("add", help="create a schedule")
    add.add_argument("name")
    add.add_argument("handler")
    when = add.add_mutually_exclusive_group(required=True)
    when.add_argument("--every", type=parse_duration, help="interval, e.g. 15m")
    when.add_argument("--cron", help="five-field cron expression, in UTC")
    _add_job_options(add)
    add.add_argument(
        "--misfire",
        choices=[misfire.value for misfire in Misfire],
        default=Misfire.COALESCE.value,
    )
    add.add_argument("--replace", action="store_true", help="update if the name exists")
    add.set_defaults(run=cmd_schedule_add)

    schedule_list = schedule_commands.add_parser("list", help="list schedules")
    _add_json_flag(schedule_list)
    schedule_list.set_defaults(run=cmd_schedule_list)

    for name, run, description in (
        ("remove", cmd_schedule_remove, "delete a schedule"),
        ("pause", cmd_schedule_pause, "stop a schedule from firing"),
        ("resume", cmd_schedule_resume, "let a paused schedule fire again"),
    ):
        action = schedule_commands.add_parser(name, help=description)
        action.add_argument("name")
        action.set_defaults(run=run)

    work = commands.add_parser("work", help="run jobs until stopped")
    work.add_argument(
        "--handlers",
        action="append",
        default=[],
        metavar="MODULE:REGISTRY",
        help="repeatable; without it the demo handlers are used",
    )
    work.add_argument("--with-demo", action="store_true", help="add the demo handlers too")
    work.add_argument("--queue", action="append", help="repeatable; default is all queues")
    work.add_argument("--concurrency", type=int, default=1, help="more than 1 uses a thread pool")
    work.add_argument("--lease", type=parse_duration, default=60.0)
    work.add_argument("--poll", type=parse_duration, default=1.0)
    work.add_argument("--drain-timeout", type=parse_duration, default=None)
    work.add_argument("--burst", action="store_true", help="exit once nothing is ready to run")
    work.add_argument("--max-jobs", type=int, default=None)
    work.add_argument("--id", default=None, help="worker id shown in job history")
    work.add_argument("--quiet", action="store_true", help="no per-job output")
    work.add_argument("--verbose", action="store_true", help="enable logging")
    work.set_defaults(run=cmd_work)

    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    clock: Clock | None = None,
    out: TextIO | None = None,
    err: TextIO | None = None,
) -> int:
    args = build_parser().parse_args(argv)
    out = out if out is not None else sys.stdout
    err = err if err is not None else sys.stderr
    if getattr(args, "verbose", False):
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    try:
        with JobQueue(args.db, clock=clock) as queue:
            return args.run(args, queue, out)
    except (JobQueueError, ValueError, sqlite3.Error) as exc:
        print(f"error: {exc}", file=err)
        return 1
    except BrokenPipeError:
        # The reader went away ('jobqueue list | head'); that is not a failure.
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        return 0
