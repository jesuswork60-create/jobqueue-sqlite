"""Versioned schema migrations.

Each migration runs in its own write transaction, and DDL in SQLite is
transactional, so a failing migration leaves the database at the previous
version. Several processes opening a fresh file at once is safe: the version
check happens after the write lock is taken.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass

from .errors import SchemaError


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    statements: tuple[str, ...]


MIGRATIONS: tuple[Migration, ...] = (
    Migration(
        1,
        "jobs table",
        (
            """
            CREATE TABLE jobs (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                queue            TEXT    NOT NULL,
                handler          TEXT    NOT NULL,
                payload          TEXT    NOT NULL,
                state            TEXT    NOT NULL CHECK (state IN
                                 ('queued', 'leased', 'succeeded', 'dead', 'cancelled')),
                priority         INTEGER NOT NULL DEFAULT 0,
                run_at           REAL    NOT NULL,
                unique_key       TEXT,
                attempts         INTEGER NOT NULL DEFAULT 0,
                max_attempts     INTEGER NOT NULL CHECK (max_attempts >= 1),
                backoff          TEXT,
                lease_token      TEXT,
                lease_owner      TEXT,
                lease_expires_at REAL,
                last_error       TEXT,
                result           TEXT,
                created_at       REAL    NOT NULL,
                updated_at       REAL    NOT NULL,
                finished_at      REAL
            )
            """,
            """
            CREATE INDEX jobs_ready ON jobs (queue, priority DESC, run_at, id)
            WHERE state = 'queued'
            """,
            """
            CREATE INDEX jobs_lease_expiry ON jobs (lease_expires_at)
            WHERE state = 'leased'
            """,
            """
            CREATE UNIQUE INDEX jobs_unique_active ON jobs (queue, unique_key)
            WHERE unique_key IS NOT NULL AND state IN ('queued', 'leased')
            """,
            "CREATE INDEX jobs_finished ON jobs (state, finished_at)",
        ),
    ),
    Migration(
        2,
        "attempt history",
        (
            """
            CREATE TABLE job_attempts (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id     INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
                attempt    INTEGER NOT NULL,
                worker     TEXT    NOT NULL,
                started_at REAL    NOT NULL,
                ended_at   REAL,
                outcome    TEXT,
                error      TEXT
            )
            """,
            "CREATE INDEX job_attempts_job ON job_attempts (job_id, id)",
            "CREATE INDEX job_attempts_ended ON job_attempts (ended_at)",
        ),
    ),
    Migration(
        3,
        "recurring schedules",
        (
            """
            CREATE TABLE schedules (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                name          TEXT    NOT NULL UNIQUE,
                kind          TEXT    NOT NULL CHECK (kind IN ('interval', 'cron')),
                expr          TEXT    NOT NULL,
                handler       TEXT    NOT NULL,
                payload       TEXT    NOT NULL,
                queue         TEXT    NOT NULL,
                priority      INTEGER NOT NULL DEFAULT 0,
                max_attempts  INTEGER NOT NULL CHECK (max_attempts >= 1),
                backoff       TEXT,
                misfire       TEXT    NOT NULL CHECK (misfire IN ('coalesce', 'catch_up')),
                paused        INTEGER NOT NULL DEFAULT 0,
                next_run_at   REAL    NOT NULL,
                last_fired_at REAL,
                created_at    REAL    NOT NULL,
                updated_at    REAL    NOT NULL
            )
            """,
            "CREATE INDEX schedules_due ON schedules (next_run_at) WHERE paused = 0",
            "ALTER TABLE jobs ADD COLUMN schedule_id INTEGER "
            "REFERENCES schedules (id) ON DELETE SET NULL",
            "ALTER TABLE jobs ADD COLUMN scheduled_for REAL",
            """
            CREATE UNIQUE INDEX jobs_schedule_fire ON jobs (schedule_id, scheduled_for)
            WHERE schedule_id IS NOT NULL
            """,
        ),
    ),
)


_BOOKKEEPING = """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version    INTEGER PRIMARY KEY,
        name       TEXT NOT NULL,
        applied_at REAL NOT NULL
    )
"""


def _check_sequence(migrations: Sequence[Migration]) -> None:
    for expected, migration in enumerate(migrations, start=1):
        if migration.version != expected:
            raise SchemaError(
                f"migration list is out of order: expected version {expected}, "
                f"found {migration.version} ({migration.name})"
            )


def current_version(conn: sqlite3.Connection) -> int:
    """The highest applied version, or 0 for a database that was never migrated."""
    table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'"
    ).fetchone()
    if table is None:
        return 0
    row = conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()
    return int(row[0])


def migrate(
    conn: sqlite3.Connection,
    now: float,
    migrations: Sequence[Migration] = MIGRATIONS,
) -> list[int]:
    """Apply every pending migration and return the versions applied.

    ``conn`` must be in autocommit mode with no transaction open. Raises
    SchemaError if the file was written by a newer version of this package.
    """
    _check_sequence(migrations)
    latest = migrations[-1].version if migrations else 0
    applied: list[int] = []

    # The bookkeeping table and each migration get separate transactions, so
    # progress survives a later migration failing.
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(_BOOKKEEPING)
        version = current_version(conn)
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")

    if version > latest:
        raise SchemaError(
            f"database is at schema version {version}, but this code only "
            f"knows up to version {latest}; upgrade the package"
        )

    for migration in migrations:
        conn.execute("BEGIN IMMEDIATE")
        try:
            if current_version(conn) >= migration.version:
                conn.execute("ROLLBACK")
                continue
            for statement in migration.statements:
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)",
                (migration.version, migration.name, now),
            )
        except sqlite3.Error as exc:
            conn.execute("ROLLBACK")
            raise SchemaError(
                f"migration {migration.version} ({migration.name}) failed: {exc}"
            ) from exc
        conn.execute("COMMIT")
        applied.append(migration.version)
    return applied
