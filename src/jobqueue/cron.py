"""A small cron subset: five fields, always interpreted in UTC.

Supported: ``*``, numbers, ranges, steps, comma lists, month and weekday names,
7 as Sunday, and the aliases @hourly, @daily, @midnight, @weekly, @monthly,
@yearly and @annually. Seconds, ``L``, ``W``, ``#`` and time zones are not.

Day matching follows Vixie cron: when both the day-of-month and day-of-week
fields are restricted, a day matches if either does. A field that starts with
``*`` (including ``*/2``) does not count as restricted for this rule.
"""

from __future__ import annotations

import bisect
from datetime import date, datetime, timedelta, timezone

from .errors import CronSyntaxError

_ALIASES = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}

_MONTHS = {
    name: number
    for number, name in enumerate(
        ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"),
        start=1,
    )
}
_WEEKDAYS = {
    name: number
    for number, name in enumerate(("sun", "mon", "tue", "wed", "thu", "fri", "sat"))
}

# Long enough to reach a 29 February across a skipped century leap year.
_SEARCH_YEARS = 9


def _parse_value(text: str, low: int, high: int, names: dict[str, int], field: str) -> int:
    lowered = text.lower()
    if lowered in names:
        return names[lowered]
    if not text.isdigit():
        raise CronSyntaxError(f"{field} field: {text!r} is not a valid value")
    value = int(text)
    if not low <= value <= high:
        raise CronSyntaxError(f"{field} field: {value} is outside {low}-{high}")
    return value


def _parse_field(
    text: str,
    low: int,
    high: int,
    field: str,
    names: dict[str, int] | None = None,
) -> frozenset[int]:
    names = names or {}
    values: set[int] = set()
    for part in text.split(","):
        if not part:
            raise CronSyntaxError(f"{field} field: empty list item in {text!r}")
        range_text, slash, step_text = part.partition("/")
        step = 1
        if slash:
            if not step_text.isdigit() or int(step_text) == 0:
                raise CronSyntaxError(f"{field} field: bad step in {part!r}")
            step = int(step_text)

        if range_text == "*":
            start, stop = low, high
        elif "-" in range_text:
            first, _, last = range_text.partition("-")
            start = _parse_value(first, low, high, names, field)
            stop = _parse_value(last, low, high, names, field)
            if start > stop:
                raise CronSyntaxError(f"{field} field: range {range_text!r} runs backwards")
        else:
            start = _parse_value(range_text, low, high, names, field)
            # 'n/step' means from n to the top of the field.
            stop = high if slash else start
        values.update(range(start, stop + 1, step))
    return frozenset(values)


class CronExpression:
    """A parsed cron expression. Instances are immutable by convention."""

    def __init__(self, text: str) -> None:
        self.text = text.strip()
        expanded = _ALIASES.get(self.text.lower(), self.text)
        if expanded.startswith("@"):
            raise CronSyntaxError(f"unknown cron alias {self.text!r}")
        fields = expanded.split()
        if len(fields) != 5:
            raise CronSyntaxError(
                f"expected 5 fields (minute hour day month weekday), got {len(fields)}"
            )

        minutes = _parse_field(fields[0], 0, 59, "minute")
        hours = _parse_field(fields[1], 0, 23, "hour")
        self.days = _parse_field(fields[2], 1, 31, "day")
        self.months = _parse_field(fields[3], 1, 12, "month", _MONTHS)
        weekdays = _parse_field(fields[4], 0, 7, "weekday", _WEEKDAYS)
        self.weekdays = frozenset(day % 7 for day in weekdays)

        self._day_restricted = not fields[2].startswith("*")
        self._weekday_restricted = not fields[4].startswith("*")
        self._minutes_of_day = sorted(hour * 60 + minute for hour in hours for minute in minutes)

        # Fail now rather than at the first promotion if this can never fire
        # (30 February, say). 2000 is a leap year, so 29 February is reachable.
        self.next_after(datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp())

    def _day_matches(self, day: date) -> bool:
        cron_weekday = (day.weekday() + 1) % 7
        day_ok = day.day in self.days
        weekday_ok = cron_weekday in self.weekdays
        if self._day_restricted and self._weekday_restricted:
            return day_ok or weekday_ok
        return day_ok and weekday_ok

    def matches(self, ts: float) -> bool:
        """Whether the minute containing ``ts`` is a fire time."""
        moment = datetime.fromtimestamp(ts, timezone.utc)
        minute_of_day = moment.hour * 60 + moment.minute
        index = bisect.bisect_left(self._minutes_of_day, minute_of_day)
        in_day = index < len(self._minutes_of_day) and self._minutes_of_day[index] == minute_of_day
        return in_day and moment.month in self.months and self._day_matches(moment.date())

    def next_after(self, ts: float) -> float:
        """The first fire time strictly later than ``ts``, as an epoch timestamp."""
        moment = datetime.fromtimestamp(ts, timezone.utc).replace(second=0, microsecond=0)
        moment += timedelta(minutes=1)

        day = moment.date()
        earliest_minute = moment.hour * 60 + moment.minute
        last_year = day.year + _SEARCH_YEARS
        while day.year <= last_year:
            if day.month not in self.months:
                day = (day.replace(day=1) + timedelta(days=32)).replace(day=1)
                earliest_minute = 0
                continue
            if self._day_matches(day):
                index = bisect.bisect_left(self._minutes_of_day, earliest_minute)
                if index < len(self._minutes_of_day):
                    hour, minute = divmod(self._minutes_of_day[index], 60)
                    fire = datetime(day.year, day.month, day.day, hour, minute, tzinfo=timezone.utc)
                    return fire.timestamp()
            day += timedelta(days=1)
            earliest_minute = 0
        raise CronSyntaxError(f"cron expression {self.text!r} never fires")

    def __repr__(self) -> str:
        return f"CronExpression({self.text!r})"
