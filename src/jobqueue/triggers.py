"""When a schedule fires: a fixed interval or a cron expression."""

from __future__ import annotations

import math
from typing import Protocol

from .cron import CronExpression
from .durations import plain_number
from .errors import ScheduleError


class Trigger(Protocol):
    kind: str
    expr: str

    def first_fire(self, now: float) -> float:
        """The first fire time for a schedule created (or resumed) at ``now``."""
        ...

    def next_fire(self, fire: float) -> float:
        """The fire time immediately after ``fire``."""
        ...

    def skip_past(self, fire: float, now: float) -> float:
        """The first fire time later than ``now``, in the series containing ``fire``."""
        ...


class IntervalTrigger:
    """Fires every N seconds, counted from the previous fire time so it never drifts."""

    kind = "interval"

    def __init__(self, seconds: float) -> None:
        if not math.isfinite(seconds) or seconds <= 0:
            raise ScheduleError("a schedule interval must be positive")
        self.seconds = float(seconds)
        self.expr = plain_number(self.seconds)

    def first_fire(self, now: float) -> float:
        return now + self.seconds

    def next_fire(self, fire: float) -> float:
        return fire + self.seconds

    def skip_past(self, fire: float, now: float) -> float:
        if fire > now:
            return fire
        # Arithmetic, not a loop: a one-second schedule may have been idle for months.
        skipped = math.floor((now - fire) / self.seconds) + 1
        candidate = fire + skipped * self.seconds
        return candidate if candidate > now else candidate + self.seconds


class CronTrigger:
    kind = "cron"

    def __init__(self, expression: str) -> None:
        self._cron = CronExpression(expression)
        self.expr = self._cron.text

    def first_fire(self, now: float) -> float:
        return self._cron.next_after(now)

    def next_fire(self, fire: float) -> float:
        return self._cron.next_after(fire)

    def skip_past(self, fire: float, now: float) -> float:
        return self._cron.next_after(max(fire, now))


def trigger_from(kind: str, expr: str) -> Trigger:
    """Rebuild a trigger from the two columns stored on a schedule row."""
    if kind == "interval":
        return IntervalTrigger(float(expr))
    if kind == "cron":
        return CronTrigger(expr)
    raise ScheduleError(f"unknown schedule kind {kind!r}")
