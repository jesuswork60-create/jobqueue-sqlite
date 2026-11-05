"""A background job queue that keeps everything in one SQLite file."""

from .backoff import Backoff, ExponentialBackoff, FixedBackoff, JitterBackoff, parse_backoff
from .clock import Clock, ManualClock, SystemClock
from .db import Database
from .deadletters import DeadLetters
from .durations import format_duration, parse_duration
from .errors import (
    Abandon,
    BackoffSpecError,
    CronSyntaxError,
    DuplicateJob,
    DuplicateSchedule,
    DurationError,
    HandlerLoadError,
    HandlerNotFound,
    InvalidTransition,
    JobNotFound,
    JobQueueError,
    LeaseLost,
    PayloadError,
    Retry,
    ScheduleError,
    ScheduleNotFound,
    SchemaError,
)
from .metrics import HandlerMetrics, MetricsSnapshot, QueueMetrics
from .models import Attempt, AttemptOutcome, EnqueueResult, Job, JobState, Misfire, Schedule
from .queue import JobQueue, JobSpec
from .registry import Handler, HandlerRegistry, load_registry
from .schedules import Promotion, ScheduleStore
from .worker import JobContext, Worker, WorkerEvent, WorkerStats

__version__ = "0.1.0"

__all__ = [
    "Abandon",
    "Attempt",
    "AttemptOutcome",
    "Backoff",
    "BackoffSpecError",
    "Clock",
    "CronSyntaxError",
    "Database",
    "DeadLetters",
    "DuplicateJob",
    "DuplicateSchedule",
    "DurationError",
    "EnqueueResult",
    "ExponentialBackoff",
    "FixedBackoff",
    "Handler",
    "HandlerLoadError",
    "HandlerMetrics",
    "HandlerNotFound",
    "HandlerRegistry",
    "InvalidTransition",
    "JitterBackoff",
    "Job",
    "JobContext",
    "JobNotFound",
    "JobQueue",
    "JobQueueError",
    "JobSpec",
    "JobState",
    "LeaseLost",
    "ManualClock",
    "MetricsSnapshot",
    "Misfire",
    "PayloadError",
    "Promotion",
    "QueueMetrics",
    "Retry",
    "Schedule",
    "ScheduleError",
    "ScheduleNotFound",
    "ScheduleStore",
    "SchemaError",
    "SystemClock",
    "Worker",
    "WorkerEvent",
    "WorkerStats",
    "format_duration",
    "load_registry",
    "parse_backoff",
    "parse_duration",
]
