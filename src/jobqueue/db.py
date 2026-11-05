"""SQLite connection management.

File databases get one connection per thread, in WAL mode, so readers do not
block the single writer. ``:memory:`` databases exist only inside one connection,
so that connection is shared and an in-process lock serialises access to it.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
import threading
import time
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from .clock import Clock, SystemClock
from .errors import JobQueueError
from .migrations import migrate


class Database:
    """Owns the connections to one SQLite database and migrates it on open."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        busy_timeout: float = 30.0,
        clock: Clock | None = None,
    ) -> None:
        self.path = os.fspath(path)
        self._busy_timeout = busy_timeout
        self._in_memory = self.path == ":memory:"
        self._local = threading.local()
        self._registry_lock = threading.Lock()
        self._connections: list[tuple[threading.Thread, sqlite3.Connection]] = []
        self._closed = False
        self._guard: Any = threading.RLock() if self._in_memory else contextlib.nullcontext()
        self._shared: sqlite3.Connection | None = None

        now = (clock or SystemClock()).now()
        try:
            migrate(self.connection(), now)
        except BaseException:
            self.close()
            raise

    def _open(self) -> sqlite3.Connection:
        # isolation_level=None: the sqlite3 module never opens transactions on
        # its own, so BEGIN IMMEDIATE below means exactly what it says.
        conn = sqlite3.connect(
            self.path,
            timeout=self._busy_timeout,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys = ON")
            if not self._in_memory:
                self._enable_wal(conn)
                conn.execute("PRAGMA synchronous = NORMAL")
        except sqlite3.Error:
            conn.close()
            raise
        return conn

    def _enable_wal(self, conn: sqlite3.Connection) -> None:
        # Switching a new file to WAL needs an exclusive lock, and SQLite can
        # report "locked" at once instead of using the busy timeout, so workers
        # starting together on a fresh database have to retry here themselves.
        deadline = time.monotonic() + self._busy_timeout
        while True:
            try:
                conn.execute("PRAGMA journal_mode = WAL")
                return
            except sqlite3.OperationalError as exc:
                busy = exc.sqlite_errorcode & 0xFF == sqlite3.SQLITE_BUSY
                if not busy or time.monotonic() >= deadline:
                    raise
            time.sleep(0.01)

    def connection(self) -> sqlite3.Connection:
        """The connection for the calling thread."""
        if self._closed:
            raise JobQueueError("database is closed")
        if self._in_memory:
            if self._shared is None:
                self._shared = self._open()
            return self._shared

        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._open()
            self._local.conn = conn
            with self._registry_lock:
                self._sweep_dead_threads()
                self._connections.append((threading.current_thread(), conn))
        return conn

    def _sweep_dead_threads(self) -> None:
        # A finished thread can no longer use its connection, and nothing else
        # will ever close it. Called with the registry lock held.
        alive = []
        for thread, conn in self._connections:
            if thread.is_alive():
                alive.append((thread, conn))
            else:
                conn.close()
        self._connections = alive

    @contextlib.contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """A write transaction, holding the database write lock from the start.

        Nesting joins the outer transaction: only the outermost block commits,
        and an exception that escapes any block rolls back the whole thing.
        """
        with self._guard:
            conn = self.connection()
            if conn.in_transaction:
                yield conn
                return
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.execute("COMMIT")
            except BaseException:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise

    def query(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()
    ) -> list[sqlite3.Row]:
        """Run a read and return all of its rows."""
        with self._guard:
            return self.connection().execute(sql, params).fetchall()

    def close(self) -> None:
        with self._registry_lock:
            self._closed = True
            for _, conn in self._connections:
                conn.close()
            self._connections = []
            if self._shared is not None:
                self._shared.close()
                self._shared = None
