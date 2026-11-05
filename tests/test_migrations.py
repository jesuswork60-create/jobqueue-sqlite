import os
import sqlite3
import tempfile
import threading
import unittest

from jobqueue import Database, JobQueue, ManualClock, SchemaError
from jobqueue.migrations import MIGRATIONS, Migration, current_version, migrate


def plain_connection(path):
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    return conn


class MigrationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = os.path.join(directory.name, "m.db")

    def connect(self):
        conn = plain_connection(self.path)
        self.addCleanup(conn.close)
        return conn

    def test_fresh_database_reaches_latest_version(self):
        conn = self.connect()
        self.assertEqual(current_version(conn), 0)
        applied = migrate(conn, 100.0)
        self.assertEqual(applied, [m.version for m in MIGRATIONS])
        self.assertEqual(current_version(conn), MIGRATIONS[-1].version)
        row = conn.execute(
            "SELECT applied_at FROM schema_migrations WHERE version = 1"
        ).fetchone()
        self.assertEqual(row["applied_at"], 100.0)

    def test_second_run_applies_nothing(self):
        conn = self.connect()
        migrate(conn, 1.0)
        self.assertEqual(migrate(conn, 2.0), [])

    def test_upgrade_keeps_existing_rows(self):
        conn = self.connect()
        migrate(conn, 1.0, MIGRATIONS[:1])
        conn.execute(
            """
            INSERT INTO jobs (queue, handler, payload, state, run_at, max_attempts,
                              created_at, updated_at)
            VALUES ('default', 'old', '{"kept": true}', 'queued', 5.0, 3, 5.0, 5.0)
            """
        )
        self.assertEqual(migrate(conn, 2.0), [2, 3])

        with JobQueue(self.path, clock=ManualClock(10.0)) as queue:
            job = queue.lease("w")[0]
            self.assertEqual(job.handler, "old")
            self.assertEqual(job.payload, {"kept": True})
            self.assertIsNone(job.schedule_id)

    def test_newer_database_is_refused(self):
        conn = self.connect()
        migrate(conn, 1.0)
        conn.execute(
            "INSERT INTO schema_migrations (version, name, applied_at) VALUES (99, 'future', 1)"
        )
        with self.assertRaisesRegex(SchemaError, "version 99"):
            migrate(conn, 2.0)
        with self.assertRaises(SchemaError):
            Database(self.path)

    def test_failed_migration_rolls_back_only_itself(self):
        conn = self.connect()
        broken = (
            MIGRATIONS[0],
            Migration(
                2,
                "half good",
                ("CREATE TABLE extra (id INTEGER)", "CREATE TABLE jobs (id INTEGER)"),
            ),
        )
        with self.assertRaisesRegex(SchemaError, "half good"):
            migrate(conn, 1.0, broken)
        self.assertEqual(current_version(conn), 1)
        tables = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        self.assertIn("jobs", tables)
        self.assertNotIn("extra", tables)
        self.assertFalse(conn.in_transaction)

    def test_out_of_order_list_is_rejected(self):
        conn = self.connect()
        with self.assertRaisesRegex(SchemaError, "out of order"):
            migrate(conn, 1.0, (Migration(2, "skips one", ()),))

    def test_concurrent_open_of_a_fresh_file(self):
        # The race around the first switch to WAL is narrow, so try it repeatedly.
        for round_number in range(25):
            path = f"{self.path}.{round_number}"
            errors = []
            barrier = threading.Barrier(6)

            def opener():
                barrier.wait()
                try:
                    Database(path).close()
                except Exception as exc:  # collected for the assertion below
                    errors.append(exc)

            threads = [threading.Thread(target=opener) for _ in range(6)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(errors, [])
            conn = plain_connection(path)
            self.addCleanup(conn.close)
            self.assertEqual(current_version(conn), MIGRATIONS[-1].version)


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = os.path.join(directory.name, "d.db")

    def test_transaction_rolls_back_on_exception(self):
        db = Database(self.path)
        self.addCleanup(db.close)
        with self.assertRaises(RuntimeError):
            with db.transaction() as conn:
                conn.execute("CREATE TABLE t (x)")
                raise RuntimeError("boom")
        self.assertEqual(db.query("SELECT name FROM sqlite_master WHERE name = 't'"), [])

    def test_nested_transaction_joins_outer(self):
        db = Database(self.path)
        self.addCleanup(db.close)
        with self.assertRaises(RuntimeError):
            with db.transaction() as outer:
                outer.execute("CREATE TABLE t (x)")
                with db.transaction() as inner:
                    self.assertIs(inner, outer)
                    inner.execute("INSERT INTO t VALUES (1)")
                raise RuntimeError("after the inner block ended")
        self.assertEqual(db.query("SELECT name FROM sqlite_master WHERE name = 't'"), [])

    def test_each_thread_gets_its_own_connection(self):
        db = Database(self.path)
        self.addCleanup(db.close)
        seen = []
        thread = threading.Thread(target=lambda: seen.append(db.connection()))
        thread.start()
        thread.join()
        self.assertIsNot(seen[0], db.connection())

    def test_connection_of_finished_thread_is_closed_by_the_sweep(self):
        db = Database(self.path)
        self.addCleanup(db.close)
        seen = []
        thread = threading.Thread(target=lambda: seen.append(db.connection()))
        thread.start()
        thread.join()
        other = threading.Thread(target=db.connection)
        other.start()
        other.join()
        with self.assertRaises(sqlite3.ProgrammingError):
            seen[0].execute("SELECT 1")

    def test_memory_database_is_shared_across_threads(self):
        db = Database(":memory:")
        self.addCleanup(db.close)
        with db.transaction() as conn:
            conn.execute("CREATE TABLE t (x)")
            conn.execute("INSERT INTO t VALUES (7)")
        found = []
        thread = threading.Thread(target=lambda: found.extend(db.query("SELECT x FROM t")))
        thread.start()
        thread.join()
        self.assertEqual(found[0]["x"], 7)

    def test_using_a_closed_database_fails_clearly(self):
        db = Database(self.path)
        db.close()
        with self.assertRaisesRegex(Exception, "closed"):
            db.query("SELECT 1")

    def test_foreign_keys_are_enforced(self):
        db = Database(self.path)
        self.addCleanup(db.close)
        with self.assertRaises(sqlite3.IntegrityError):
            with db.transaction() as conn:
                conn.execute(
                    "INSERT INTO job_attempts (job_id, attempt, worker, started_at) "
                    "VALUES (424242, 1, 'w', 1.0)"
                )


if __name__ == "__main__":
    unittest.main()
