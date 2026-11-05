import contextlib
import io
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

from support import START

from jobqueue import JobQueue, ManualClock
from jobqueue.cli import main

SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")


class CliTestCase(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.dir = directory.name
        self.path = os.path.join(self.dir, "cli.db")
        self.clock = ManualClock(START)

    def run_cli(self, *argv):
        out = io.StringIO()
        err = io.StringIO()
        code = main(["--db", self.path, *argv], clock=self.clock, out=out, err=err)
        return code, out.getvalue(), err.getvalue()

    def ok(self, *argv):
        code, out, err = self.run_cli(*argv)
        self.assertEqual(code, 0, msg=err)
        return out

    def run_json(self, *argv):
        return json.loads(self.ok(*argv, "--json"))

    def assert_usage_error(self, *argv):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as caught:
                self.run_cli(*argv)
        self.assertEqual(caught.exception.code, 2)


class EnqueueCommandTests(CliTestCase):
    def test_enqueue_and_show(self):
        self.assertIn("enqueued job 1", self.ok("enqueue", "demo.echo", "--payload", '{"a": 1}'))
        job = self.run_json("show", "1")
        self.assertEqual(job["payload"], {"a": 1})
        self.assertEqual(job["state"], "queued")
        self.assertEqual(job["history"], [])
        self.assertNotIn("lease_token", job)

    def test_options(self):
        result = self.run_json(
            "enqueue", "h", "--queue", "q", "--priority", "3", "--delay", "1m",
            "--max-attempts", "2", "--backoff", "fixed:9",
        )
        job = result["job"]
        self.assertTrue(result["created"])
        self.assertEqual(job["queue"], "q")
        self.assertEqual(job["priority"], 3)
        self.assertEqual(job["run_at"], START + 60)
        self.assertEqual(job["max_attempts"], 2)
        self.assertEqual(job["backoff"], "fixed:9")

    def test_unique_reports_the_existing_job(self):
        self.ok("enqueue", "h", "--unique", "k")
        self.assertIn("already active: job 1", self.ok("enqueue", "h", "--unique", "k"))

    def test_bad_arguments_are_usage_errors(self):
        self.assert_usage_error("enqueue", "h", "--payload", "{not json")
        self.assert_usage_error("enqueue", "h", "--delay", "soon")
        self.assert_usage_error("enqueue")
        self.assert_usage_error("frobnicate")
        self.assert_usage_error()

    def test_invalid_values_are_runtime_errors(self):
        code, _, err = self.run_cli("enqueue", "h", "--max-attempts", "0")
        self.assertEqual(code, 1)
        self.assertIn("max_attempts", err)
        code, _, err = self.run_cli("enqueue", "h", "--backoff", "nope")
        self.assertEqual(code, 1)
        self.assertIn("nope", err)


class InspectionCommandTests(CliTestCase):
    def test_list_filters_and_text_form(self):
        self.ok("enqueue", "a", "--queue", "q1")
        self.ok("enqueue", "b", "--queue", "q2")
        text = self.ok("list")
        self.assertIn("HANDLER", text)
        self.assertIn("2023-11-14T22:13:20Z", text)
        self.assertEqual([j["handler"] for j in self.run_json("list")], ["b", "a"])
        self.assertEqual([j["handler"] for j in self.run_json("list", "--queue", "q1")], ["a"])
        self.assertEqual([j["handler"] for j in self.run_json("list", "--handler", "b")], ["b"])
        self.assertEqual(len(self.run_json("list", "--limit", "1")), 1)
        self.assertEqual(self.run_json("list", "--state", "dead"), [])
        self.assertIn("no matching jobs", self.ok("list", "--state", "dead"))
        self.assert_usage_error("list", "--state", "bogus")

    def test_show_unknown_job(self):
        code, _, err = self.run_cli("show", "99")
        self.assertEqual(code, 1)
        self.assertIn("no job with id 99", err)

    def test_show_text_includes_history_and_error(self):
        self.ok("enqueue", "demo.fail", "--max-attempts", "1", "--unique", "k")
        self.ok("work", "--burst", "--quiet", "--id", "w-1")
        text = self.ok("show", "1")
        self.assertIn("unique key", text)
        self.assertIn("w-1", text)
        self.assertIn("dead", text)
        self.assertIn("RuntimeError: this handler always fails", text)

    def test_status(self):
        self.assertIn("no jobs yet", self.ok("status"))
        self.ok("enqueue", "demo.echo", "--payload", "1")
        self.ok("enqueue", "demo.echo", "--delay", "1h")
        self.ok("schedule", "add", "s", "demo.echo", "--every", "1h")
        self.ok("work", "--burst", "--quiet")
        text = self.ok("status")
        self.assertIn("default", text)
        self.assertIn("demo.echo", text)
        self.assertIn("schedules: 1 (0 paused)", text)
        data = self.run_json("status", "--window", "1m")
        self.assertEqual(data["window_seconds"], 60.0)
        self.assertEqual(data["queues"][0]["succeeded"], 1)
        self.assertEqual(data["queues"][0]["delayed"], 1)

    def test_cancel(self):
        self.ok("enqueue", "h")
        self.assertIn("cancelled job 1", self.ok("cancel", "1"))
        code, _, err = self.run_cli("cancel", "1")
        self.assertEqual(code, 1)
        self.assertIn("already cancelled", err)


class DeadLetterCommandTests(CliTestCase):
    def fill(self):
        self.ok("enqueue", "demo.fail", "--max-attempts", "1", "--payload", '"x"')
        self.ok("enqueue", "demo.abandon", "--payload", '"y"', "--queue", "other")
        self.ok("work", "--burst", "--quiet")

    def test_dead_listing(self):
        self.assertIn("no dead jobs", self.ok("dead"))
        self.fill()
        text = self.ok("dead")
        self.assertIn("this handler always fails", text)
        self.assertIn("cannot process payload", text)
        self.assertEqual(len(self.run_json("dead")), 2)
        self.assertEqual(len(self.run_json("dead", "--queue", "other")), 1)
        self.assertEqual(self.run_json("dead", "--handler", "demo.fail")[0]["id"], 1)

    def test_retry_by_id(self):
        self.fill()
        self.assertIn("requeued job 1", self.ok("retry", "1", "--delay", "10s"))
        job = self.run_json("show", "1")
        self.assertEqual(job["state"], "queued")
        self.assertEqual(job["attempts"], 0)
        self.assertEqual(job["run_at"], START + 10)

    def test_retry_keep_attempts(self):
        self.fill()
        self.ok("retry", "1", "--keep-attempts")
        self.assertEqual(self.run_json("show", "1")["attempts"], 1)

    def test_retry_all_with_filter(self):
        self.fill()
        self.assertIn("requeued 1 dead", self.ok("retry", "--all", "--queue", "other"))
        self.assertEqual(self.run_json("show", "1")["state"], "dead")
        self.assertEqual(self.run_json("show", "2")["state"], "queued")

    def test_retry_all_reports_skips(self):
        self.ok("enqueue", "demo.fail", "--max-attempts", "1", "--unique", "k")
        self.ok("work", "--burst", "--quiet")
        self.ok("enqueue", "demo.echo", "--unique", "k")
        self.assertIn("skipped job 1", self.ok("retry", "--all"))

    def test_retry_needs_a_target(self):
        for argv in (("retry",), ("retry", "1", "--all")):
            code, _, err = self.run_cli(*argv)
            self.assertEqual(code, 1)
            self.assertIn("either job ids or --all", err)

    def test_retry_of_a_live_job(self):
        self.ok("enqueue", "h")
        code, _, err = self.run_cli("retry", "1")
        self.assertEqual(code, 1)
        self.assertIn("not dead", err)

    def test_purge(self):
        self.fill()
        self.assertIn("purged 0", self.ok("purge", "--state", "dead", "--older-than", "1h"))
        self.clock.advance(7200)
        self.assertIn("purged 1", self.ok("purge", "--state", "dead", "--queue", "other"))
        self.assertIn("purged 1", self.ok("purge", "--state", "dead", "--state", "succeeded"))
        self.assertEqual(self.run_json("list"), [])
        self.assert_usage_error("purge")
        self.assert_usage_error("purge", "--state", "queued")


class ScheduleCommandTests(CliTestCase):
    def test_lifecycle(self):
        self.assertIn("no schedules", self.ok("schedule", "list"))
        self.assertIn(
            "next run 2023-11-14T22:18:20Z",
            self.ok("schedule", "add", "tidy", "demo.echo", "--every", "5m", "--payload", "[1]"),
        )
        self.ok("schedule", "add", "night", "demo.echo", "--cron", "0 3 * * *", "--misfire", "catch_up")
        text = self.ok("schedule", "list")
        self.assertIn("every 5m", text)
        self.assertIn("0 3 * * *", text)
        rows = self.run_json("schedule", "list")
        self.assertEqual([row["name"] for row in rows], ["night", "tidy"])
        self.assertEqual(rows[0]["misfire"], "catch_up")
        self.assertEqual(rows[1]["payload"], [1])

        self.ok("schedule", "pause", "tidy")
        self.assertIn("paused", self.ok("schedule", "list"))
        self.ok("schedule", "resume", "tidy")
        self.ok("schedule", "remove", "tidy")
        self.assertEqual(len(self.run_json("schedule", "list")), 1)

    def test_duplicates_and_replace(self):
        self.ok("schedule", "add", "s", "h", "--every", "1m")
        code, _, err = self.run_cli("schedule", "add", "s", "h", "--every", "1m")
        self.assertEqual(code, 1)
        self.assertIn("already exists", err)
        self.ok("schedule", "add", "s", "other", "--every", "1m", "--replace")
        self.assertEqual(self.run_json("schedule", "list")[0]["handler"], "other")

    def test_errors(self):
        code, _, err = self.run_cli("schedule", "add", "s", "h", "--cron", "99 * * * *")
        self.assertEqual(code, 1)
        self.assertIn("minute", err)
        code, _, err = self.run_cli("schedule", "remove", "ghost")
        self.assertEqual(code, 1)
        self.assertIn("ghost", err)
        self.assert_usage_error("schedule", "add", "s", "h")
        self.assert_usage_error("schedule", "add", "s", "h", "--every", "1m", "--cron", "* * * * *")
        self.assert_usage_error("schedule")

    def test_worker_promotes_due_schedule(self):
        self.ok("schedule", "add", "s", "demo.echo", "--every", "1m", "--payload", '"tick"')
        self.clock.advance(60)
        self.ok("work", "--burst", "--quiet")
        job = self.run_json("show", "1")
        self.assertEqual(job["state"], "succeeded")
        self.assertEqual(job["result"], "tick")


class WorkCommandTests(CliTestCase):
    def test_burst_with_demo_handlers(self):
        self.ok("enqueue", "demo.add", "--payload", '{"numbers": [1, 2, 3]}')
        self.ok("enqueue", "demo.flaky", "--backoff", "fixed:0")
        self.ok("enqueue", "demo.slow", "--payload", '{"seconds": 2.5}')
        text = self.ok("work", "--burst")
        self.assertIn("job 1 demo.add succeeded", text)
        self.assertIn("job 2 demo.flaky retried", text)
        self.assertIn("job 2 demo.flaky succeeded", text)
        self.assertIn("job 3 demo.slow succeeded in 2.5s", text)
        self.assertIn("worker finished: 3 succeeded, 1 retried", text)
        self.assertEqual(self.run_json("show", "1")["result"], 6)
        self.assertEqual(self.run_json("show", "2")["result"], "succeeded on attempt 2")

    def test_quiet_and_max_jobs(self):
        self.ok("enqueue", "demo.echo")
        self.ok("enqueue", "demo.echo")
        text = self.ok("work", "--max-jobs", "1", "--quiet")
        self.assertNotIn("job 1", text)
        self.assertIn("1 succeeded", text)

    def test_pool_mode(self):
        for _ in range(5):
            self.ok("enqueue", "demo.echo")
        self.assertIn("5 succeeded", self.ok("work", "--burst", "--concurrency", "3", "--quiet"))

    def test_queue_selection(self):
        self.ok("enqueue", "demo.echo", "--queue", "a")
        self.ok("enqueue", "demo.echo", "--queue", "b")
        self.assertIn("1 succeeded", self.ok("work", "--burst", "--queue", "b", "--quiet"))
        self.assertEqual(self.run_json("show", "1")["state"], "queued")

    def _write_handlers(self, name):
        with open(os.path.join(self.dir, f"{name}.py"), "w", encoding="utf-8") as handle:
            handle.write(
                "from jobqueue import HandlerRegistry\n"
                "registry = HandlerRegistry()\n"
                "@registry.handler('custom.job')\n"
                "def job(payload):\n"
                "    return {'seen': payload}\n"
            )
        self.addCleanup(sys.modules.pop, name, None)

    def _in_project_dir(self):
        previous = os.getcwd()
        os.chdir(self.dir)
        self.addCleanup(os.chdir, previous)
        self.addCleanup(lambda: self.dir in sys.path and sys.path.remove(self.dir))

    def test_custom_handlers_from_the_working_directory(self):
        self._write_handlers("cli_custom_handlers")
        self._in_project_dir()
        self.ok("enqueue", "custom.job", "--payload", "5")
        self.ok("enqueue", "demo.echo", "--max-attempts", "1")
        self.ok("work", "--handlers", "cli_custom_handlers:registry", "--burst", "--quiet")
        self.assertEqual(self.run_json("show", "1")["result"], {"seen": 5})
        # Demo handlers are not loaded alongside custom ones unless asked for.
        self.assertEqual(self.run_json("show", "2")["state"], "dead")

    def test_with_demo_adds_the_builtins(self):
        self._write_handlers("cli_custom_handlers_two")
        self._in_project_dir()
        self.ok("enqueue", "demo.echo")
        self.ok(
            "work", "--handlers", "cli_custom_handlers_two:registry",
            "--with-demo", "--burst", "--quiet",
        )
        self.assertEqual(self.run_json("show", "1")["state"], "succeeded")

    def test_bad_handler_reference(self):
        self._in_project_dir()
        code, _, err = self.run_cli("work", "--handlers", "no_such_module_at_all:registry")
        self.assertEqual(code, 1)
        self.assertIn("cannot import", err)

    def test_signal_handlers_are_restored(self):
        before = signal.getsignal(signal.SIGTERM)
        self.ok("work", "--burst", "--quiet")
        self.assertIs(signal.getsignal(signal.SIGTERM), before)

    def test_sigterm_stops_the_worker(self):
        ready = threading.Event()

        def fake_run(worker, **kwargs):
            ready.set()
            os.kill(os.getpid(), signal.SIGTERM)
            # Python runs signal handlers on the main thread between bytecodes,
            # so the stop flag is set by the time this wait returns.
            self.assertTrue(worker._stop.wait(10))
            return worker.stats

        with mock.patch("jobqueue.cli.Worker.run", fake_run):
            text = self.ok("work", "--quiet")
        self.assertTrue(ready.is_set())
        self.assertIn("worker finished", text)


class ProcessTests(CliTestCase):
    """Runs the real entry point, once, in a separate process."""

    def run_process(self, *argv, **options):
        environment = {**os.environ, "PYTHONPATH": SRC, "JOBQUEUE_DB": self.path}
        return subprocess.run(
            [sys.executable, "-m", "jobqueue", *argv],
            capture_output=True,
            text=True,
            env=environment,
            timeout=60,
            cwd=self.dir,
            **options,
        )

    def test_module_entry_point_uses_the_environment_database(self):
        done = self.run_process("enqueue", "demo.echo", "--payload", '{"hello": "world"}')
        self.assertEqual(done.returncode, 0, msg=done.stderr)
        worked = self.run_process("work", "--burst")
        self.assertEqual(worked.returncode, 0, msg=worked.stderr)
        self.assertIn("1 succeeded", worked.stdout)
        with JobQueue(self.path) as queue:
            self.assertEqual(queue.get(1).result, {"hello": "world"})

    def test_exit_codes(self):
        self.assertEqual(self.run_process("show", "1").returncode, 1)
        self.assertEqual(self.run_process("nonsense").returncode, 2)


if __name__ == "__main__":
    unittest.main()
