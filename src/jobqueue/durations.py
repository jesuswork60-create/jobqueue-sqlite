"""Human-friendly durations and timestamps, shared by the schedules and the CLI."""

from __future__ import annotations

import math
import re
from datetime import datetime, timezone

from .errors import DurationError

_UNITS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0, "w": 604800.0}
_TOKEN = re.compile(r"(\d+(?:\.\d+)?)(ms|s|m|h|d|w)")


def parse_duration(text: str | int | float) -> float:
    """Turn '90', '90s', '1h30m' or '250ms' into seconds.

    A bare number means seconds. Negative and non-finite values are refused.
    """
    if isinstance(text, bool):
        raise DurationError(f"not a duration: {text!r}")
    if isinstance(text, (int, float)):
        value = float(text)
        if not math.isfinite(value) or value < 0:
            raise DurationError(f"not a duration: {text!r}")
        return value

    cleaned = text.strip().lower().replace(" ", "")
    if not cleaned:
        raise DurationError("empty duration")
    try:
        value = float(cleaned)
    except ValueError:
        pass
    else:
        if not math.isfinite(value) or value < 0:
            raise DurationError(f"not a duration: {text!r}")
        return value

    position = 0
    total = 0.0
    while position < len(cleaned):
        match = _TOKEN.match(cleaned, position)
        if match is None:
            raise DurationError(f"not a duration: {text!r}")
        total += float(match.group(1)) * _UNITS[match.group(2)]
        position = match.end()
    return total


def plain_number(value: float) -> str:
    """The shortest text that reads back as exactly ``value``: 300.0 -> '300'."""
    text = repr(float(value))
    return text[:-2] if text.endswith(".0") else text


def format_duration(seconds: float) -> str:
    """Compact rendering: 0.25 -> '250ms', 5400 -> '1h30m'. Sub-second parts of
    longer values are dropped."""
    if seconds < 0:
        return "-" + format_duration(-seconds)
    if seconds == 0:
        return "0s"
    if seconds < 1:
        return f"{round(seconds * 1000)}ms"
    if seconds < 60 and seconds != int(seconds):
        return f"{seconds:.1f}s"

    remaining = int(seconds)
    pieces = []
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60), ("s", 1)):
        count, remaining = divmod(remaining, size)
        if count:
            pieces.append(f"{count}{unit}")
    return "".join(pieces)


def format_timestamp(ts: float | None) -> str:
    """ISO 8601 in UTC to the second, or '-' for no value."""
    if ts is None:
        return "-"
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
