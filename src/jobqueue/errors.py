"""Exception types, including the two a handler raises to steer its own job."""

from __future__ import annotations


class JobQueueError(Exception):
    """Base class for every error this package raises on purpose."""


class SchemaError(JobQueueError):
    """The database schema cannot be brought to the version this code expects."""


class JobNotFound(JobQueueError):
    def __init__(self, job_id: int) -> None:
        super().__init__(f"no job with id {job_id}")
        self.job_id = job_id


class LeaseLost(JobQueueError):
    """The caller no longer holds the lease it is trying to act on.

    Raised when the lease expired, was reclaimed and handed to another worker,
    or the job was cancelled or purged in the meantime. The work done under the
    lease must be treated as possibly duplicated.
    """

    def __init__(self, job_id: int, reason: str = "lease no longer held") -> None:
        super().__init__(f"job {job_id}: {reason}")
        self.job_id = job_id
        self.reason = reason


class InvalidTransition(JobQueueError):
    """The requested state change is not legal from the job's current state."""


class DuplicateJob(JobQueueError):
    """An active job already holds this unique key in this queue."""

    def __init__(self, queue: str, unique_key: str, existing_id: int) -> None:
        super().__init__(
            f"queue {queue!r} already has active job {existing_id} "
            f"with unique key {unique_key!r}"
        )
        self.queue = queue
        self.unique_key = unique_key
        self.existing_id = existing_id


class PayloadError(JobQueueError):
    """A payload or result cannot be represented as JSON."""


class BackoffSpecError(JobQueueError, ValueError):
    """A backoff policy string could not be parsed."""


class DurationError(JobQueueError, ValueError):
    """A duration string such as '1h30m' could not be parsed."""


class ScheduleError(JobQueueError):
    """A recurring schedule is invalid."""


class CronSyntaxError(ScheduleError, ValueError):
    """A cron expression could not be parsed, or can never fire."""


class ScheduleNotFound(ScheduleError):
    def __init__(self, name: str) -> None:
        super().__init__(f"no schedule named {name!r}")
        self.name = name


class DuplicateSchedule(ScheduleError):
    def __init__(self, name: str) -> None:
        super().__init__(f"a schedule named {name!r} already exists")
        self.name = name


class HandlerNotFound(JobQueueError):
    def __init__(self, name: str) -> None:
        super().__init__(f"no handler registered for {name!r}")
        self.name = name


class HandlerLoadError(JobQueueError):
    """A 'module:attribute' handler reference could not be imported."""


class Retry(Exception):
    """Raised by a handler to ask for another attempt.

    With ``delay`` set the job runs again after that many seconds; otherwise the
    job's backoff policy decides. It still consumes an attempt, so a job that has
    used all of its attempts goes to the dead letters instead.
    """

    def __init__(self, delay: float | None = None, reason: str = "retry requested") -> None:
        super().__init__(reason)
        if delay is not None and delay < 0:
            raise ValueError("retry delay cannot be negative")
        self.delay = delay
        self.reason = reason


class Abandon(Exception):
    """Raised by a handler to send its job straight to the dead letters."""

    def __init__(self, reason: str = "abandoned by handler") -> None:
        super().__init__(reason)
        self.reason = reason
