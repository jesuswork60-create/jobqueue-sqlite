"""Time sources. Everything that reads the time takes a clock, so tests never sleep."""

from __future__ import annotations

import threading
import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float:
        """Current time as seconds since the Unix epoch."""
        ...

    def sleep(self, seconds: float) -> None:
        ...


class SystemClock:
    """The real wall clock."""

    def now(self) -> float:
        return time.time()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


class ManualClock:
    """A clock that only moves when told to. Sleeping advances it instantly."""

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self._now = float(start)
        self._lock = threading.Lock()

    def now(self) -> float:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> float:
        if seconds < 0:
            raise ValueError("a clock cannot go backwards")
        with self._lock:
            self._now += seconds
            return self._now

    def sleep(self, seconds: float) -> None:
        self.advance(max(0.0, seconds))
