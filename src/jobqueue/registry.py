"""Mapping handler names to Python callables, with per-handler defaults."""

from __future__ import annotations

import importlib
import inspect
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

from .backoff import Backoff
from .errors import HandlerLoadError, HandlerNotFound
from .models import EnqueueResult
from .queue import JobQueue

HandlerFunc = Callable[..., Any]


def _wants_context(func: HandlerFunc) -> bool:
    """True if the callable can take two positional arguments: payload and context."""
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        return False
    positional = 0
    for parameter in signature.parameters.values():
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            return True
        if parameter.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            positional += 1
    return positional >= 2


@dataclass(frozen=True)
class Handler:
    """A registered callable and the defaults that apply when enqueueing through it."""

    name: str
    func: HandlerFunc
    wants_context: bool
    queue: str | None = None
    priority: int | None = None
    max_attempts: int | None = None
    backoff: Backoff | None = None

    def __call__(self, payload: Any, context: Any) -> Any:
        if self.wants_context:
            return self.func(payload, context)
        return self.func(payload)


class HandlerRegistry:
    """Named job handlers.

    A handler takes ``(payload)`` or ``(payload, context)``; which one is worked
    out from its signature::

        registry = HandlerRegistry()

        @registry.handler("mail.send", max_attempts=3)
        def send(payload, ctx):
            ...
    """

    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    def handler(
        self,
        name: str | None = None,
        *,
        queue: str | None = None,
        priority: int | None = None,
        max_attempts: int | None = None,
        backoff: Backoff | None = None,
    ) -> Callable[[HandlerFunc], HandlerFunc]:
        """Decorator form of ``register``. The name defaults to the function's name."""

        def decorate(func: HandlerFunc) -> HandlerFunc:
            self.register(
                name or func.__name__,
                func,
                queue=queue,
                priority=priority,
                max_attempts=max_attempts,
                backoff=backoff,
            )
            return func

        return decorate

    def register(
        self,
        name: str,
        func: HandlerFunc,
        *,
        queue: str | None = None,
        priority: int | None = None,
        max_attempts: int | None = None,
        backoff: Backoff | None = None,
    ) -> Handler:
        if not name:
            raise ValueError("a handler needs a name")
        if name in self._handlers:
            raise ValueError(f"handler {name!r} is already registered")
        if not callable(func):
            raise TypeError(f"handler {name!r} is not callable")
        handler = Handler(
            name=name,
            func=func,
            wants_context=_wants_context(func),
            queue=queue,
            priority=priority,
            max_attempts=max_attempts,
            backoff=backoff,
        )
        self._handlers[name] = handler
        return handler

    def resolve(self, name: str) -> Handler:
        try:
            return self._handlers[name]
        except KeyError:
            raise HandlerNotFound(name) from None

    def __contains__(self, name: object) -> bool:
        return name in self._handlers

    def __iter__(self) -> Iterator[Handler]:
        return iter(self._handlers.values())

    def __len__(self) -> int:
        return len(self._handlers)

    def merge(self, other: "HandlerRegistry", *, prefix: str = "") -> None:
        """Copy another registry's handlers in, optionally under a name prefix."""
        for handler in other:
            self.register(
                prefix + handler.name,
                handler.func,
                queue=handler.queue,
                priority=handler.priority,
                max_attempts=handler.max_attempts,
                backoff=handler.backoff,
            )

    def enqueue(
        self, job_queue: JobQueue, name: str, /, payload: Any = None, **overrides: Any
    ) -> EnqueueResult:
        """Enqueue with this handler's registered defaults; keyword arguments win.

        A backoff with no text form is not stored on the job. Workers using this
        registry still apply it, but a reclaim of an expired lease cannot know
        about it and uses the queue's default.
        """
        handler = self.resolve(name)
        options: dict[str, Any] = {}
        if handler.queue is not None:
            options["queue"] = handler.queue
        if handler.priority is not None:
            options["priority"] = handler.priority
        if handler.max_attempts is not None:
            options["max_attempts"] = handler.max_attempts
        if handler.backoff is not None and handler.backoff.spec() is not None:
            options["backoff"] = handler.backoff
        options.update(overrides)
        return job_queue.enqueue(name, payload, **options)


def load_registry(reference: str) -> HandlerRegistry:
    """Import a registry from a 'package.module:attribute' reference."""
    module_name, separator, attribute = reference.partition(":")
    if not separator or not module_name or not attribute:
        raise HandlerLoadError(f"expected 'module:attribute', got {reference!r}")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise HandlerLoadError(f"cannot import {module_name!r}: {exc}") from exc
    try:
        registry = getattr(module, attribute)
    except AttributeError:
        raise HandlerLoadError(f"module {module_name!r} has no {attribute!r}") from None
    if not isinstance(registry, HandlerRegistry):
        raise HandlerLoadError(f"{reference!r} is not a HandlerRegistry")
    return registry
