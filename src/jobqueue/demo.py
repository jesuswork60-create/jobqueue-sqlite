"""Handlers for trying the queue from the command line without writing any code."""

from __future__ import annotations

from typing import Any

from .errors import Abandon, Retry
from .registry import HandlerRegistry
from .worker import JobContext

registry = HandlerRegistry()


@registry.handler("demo.echo")
def echo(payload: Any) -> Any:
    """Succeed, with the payload as the result."""
    return payload


@registry.handler("demo.add")
def add(payload: dict[str, Any]) -> float:
    """Sum ``payload["numbers"]``. A bad payload fails like any handler bug would."""
    return sum(payload["numbers"])


@registry.handler("demo.flaky")
def flaky(payload: dict[str, Any] | None, ctx: JobContext) -> str:
    """Raise on the first ``fail_times`` attempts (default 1), then succeed."""
    fail_times = (payload or {}).get("fail_times", 1)
    if ctx.attempt <= fail_times:
        raise RuntimeError(f"simulated failure on attempt {ctx.attempt}")
    return f"succeeded on attempt {ctx.attempt}"


@registry.handler("demo.slow")
def slow(payload: dict[str, Any] | None, ctx: JobContext) -> str:
    """Work for ``seconds`` (default 5), heartbeating each second.

    If the worker is told to stop, the job asks to run again rather than holding
    up the shutdown.
    """
    remaining = float((payload or {}).get("seconds", 5))
    while remaining > 0:
        if ctx.stop_requested:
            raise Retry(delay=0, reason="worker is stopping")
        step = min(1.0, remaining)
        ctx.clock.sleep(step)
        remaining -= step
        ctx.heartbeat()
    return "done"


@registry.handler("demo.fail")
def fail(payload: Any) -> None:
    """Always raise, to fill up the dead letters."""
    raise RuntimeError(f"this handler always fails (payload: {payload!r})")


@registry.handler("demo.abandon")
def abandon(payload: Any) -> None:
    """Give up at once, without using the remaining attempts."""
    raise Abandon(f"cannot process payload {payload!r}")
