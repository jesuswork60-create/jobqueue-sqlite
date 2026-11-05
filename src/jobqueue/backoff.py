"""Retry delay policies.

A policy maps the number of attempts already made to a delay in seconds. The
built-in ones also have a text form, stored on the job, so a process that knows
nothing about the handler (for instance one reclaiming an expired lease) can
still apply the right delay.

Text forms::

    fixed:30
    exp:base=2,factor=2,max=600      (any key may be left out)
    jitter:full:exp:base=2           (or jitter:equal:<inner policy>)
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Protocol

from .durations import parse_duration, plain_number
from .errors import BackoffSpecError, DurationError


class Backoff(Protocol):
    def delay(self, attempt: int) -> float:
        """Seconds to wait after attempt number ``attempt`` (1-based) has failed."""
        ...

    def spec(self) -> str | None:
        """The text form, or None if this policy has none (custom classes may)."""
        ...


@dataclass(frozen=True)
class FixedBackoff:
    seconds: float

    def __post_init__(self) -> None:
        if self.seconds < 0:
            raise ValueError("backoff delay cannot be negative")

    def delay(self, attempt: int) -> float:
        return self.seconds

    def spec(self) -> str:
        return f"fixed:{plain_number(self.seconds)}"


@dataclass(frozen=True)
class ExponentialBackoff:
    """``base * factor ** (attempt - 1)``, capped at ``maximum``."""

    base: float = 1.0
    factor: float = 2.0
    maximum: float = 3600.0

    def __post_init__(self) -> None:
        if self.base < 0 or self.maximum < 0:
            raise ValueError("backoff delay cannot be negative")
        if self.factor < 1:
            raise ValueError("backoff factor must be at least 1")

    def delay(self, attempt: int) -> float:
        try:
            value = self.base * self.factor ** max(0, attempt - 1)
        except OverflowError:
            return self.maximum
        return min(value, self.maximum)

    def spec(self) -> str:
        return (
            f"exp:base={plain_number(self.base)},factor={plain_number(self.factor)},"
            f"max={plain_number(self.maximum)}"
        )


class JitterBackoff:
    """Randomises another policy so that failing jobs do not retry in lockstep.

    ``full`` picks uniformly in [0, d]; ``equal`` keeps at least half of d and
    randomises the rest.
    """

    def __init__(
        self,
        inner: Backoff,
        *,
        mode: str = "full",
        rng: random.Random | None = None,
    ) -> None:
        if mode not in ("full", "equal"):
            raise ValueError(f"unknown jitter mode {mode!r}")
        self.inner = inner
        self.mode = mode
        self.rng = rng if rng is not None else random.Random()

    def delay(self, attempt: int) -> float:
        ceiling = self.inner.delay(attempt)
        if self.mode == "full":
            return self.rng.uniform(0.0, ceiling)
        return ceiling / 2 + self.rng.uniform(0.0, ceiling / 2)

    def spec(self) -> str | None:
        inner = self.inner.spec()
        return None if inner is None else f"jitter:{self.mode}:{inner}"


def _seconds(value: str, spec: str) -> float:
    try:
        return parse_duration(value)
    except DurationError as exc:
        raise BackoffSpecError(f"bad backoff {spec!r}: {exc}") from exc


def _parse_exponential(body: str, spec: str) -> ExponentialBackoff:
    options: dict[str, float] = {}
    for item in filter(None, body.split(",")):
        key, separator, value = item.partition("=")
        if not separator or key not in ("base", "factor", "max"):
            raise BackoffSpecError(f"bad backoff {spec!r}: unexpected {item!r}")
        if key == "factor":
            try:
                options["factor"] = float(value)
            except ValueError:
                raise BackoffSpecError(f"bad backoff {spec!r}: factor {value!r}") from None
        else:
            options["maximum" if key == "max" else key] = _seconds(value, spec)
    try:
        return ExponentialBackoff(**options)
    except ValueError as exc:
        raise BackoffSpecError(f"bad backoff {spec!r}: {exc}") from exc


def parse_backoff(spec: str, *, rng: random.Random | None = None) -> Backoff:
    """Build a policy from its text form. ``rng`` feeds any jitter layer."""
    kind, _, body = spec.strip().partition(":")
    if kind == "fixed":
        return FixedBackoff(_seconds(body, spec))
    if kind == "exp":
        return _parse_exponential(body, spec)
    if kind == "jitter":
        mode, separator, inner = body.partition(":")
        if not separator or mode not in ("full", "equal"):
            raise BackoffSpecError(f"bad backoff {spec!r}: expected jitter:full|equal:<policy>")
        return JitterBackoff(parse_backoff(inner, rng=rng), mode=mode, rng=rng)
    raise BackoffSpecError(f"bad backoff {spec!r}: unknown kind {kind!r}")
