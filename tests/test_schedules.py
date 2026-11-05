import threading
import unittest
from datetime import datetime, timezone

from support import START, QueueTestCase

from jobqueue import (
    BackoffSpecError,
    CronSyntaxError,
    DuplicateSchedule,
    FixedBackoff,
    JobState,
    Misfire,
    ScheduleError,
    ScheduleNotFound,
)
from jobqueue.schedules import ScheduleStore
from jobqueue.triggers import CronTrigger, IntervalTrigger


class ScheduleCrudTests(QueueTestCase):
    def test_add_interval(self):
        schedule = self.queue.schedules.add(
            "tidy", "h", every="5m", payload={"a": 1}, queue="q", priority=4, max_attempts=2,
            backoff=FixedBackoff(3),
        )
        self.assertEqual(schedule.kind, "interval")
        self.assertEqual(schedule.expr, "300")
        self.assertEqual(schedule.next_run_at, START + 300)
        self.assertEqual(schedule.payload, {"a": 1})
        self.assertEqual(schedule.backoff, "fixed:3")
        self.assertFalse(schedule.paused)
        self.assertIsNone(schedule.last_fired_at)

    def test_long_interval_keeps_its_exact_value(self):
        schedule = self.queue.schedules.add("s", "h", every=1234567)
        self.assertEqual(schedule.expr, "1234567")
        self.assertEqual(self.queue.schedules.get("s").next_run_at, START + 1234567)

    def test_add_cron_first_fire(self):
        schedule = self.queue.schedules.add("nightly", "h", cron="0 3 * * *")
        self.assertEqual(schedule.kind, "cron")
        expected = datetime(2023, 11, 15, 3, 0, tzinfo=timezone.utc).timestamp()
        self.assertEqual(schedule.next_run_at, expected)

    def test_start_at_overrides_first_fire(self):
        schedule = self.queue.schedules.add("s", "h", every=60, start_at=START + 5)
        self.assertEqual(schedule.next_run_at, START + 5)

    def test_needs_exactly_one_trigger(self):
        with self.assertRaises(ScheduleError):
            self.queue.schedules.add("s", "h")
        with self.assertRaises(ScheduleError):
            self.queue.schedules.add("s", "h", every=1, cron="* * * * *")

    def test_invalid_inputs(self):
        with self.assertRaises(ScheduleError):
            self.queue.schedules.add("", "h", every=1)
        with self.assertRaises(ScheduleError):
            self.queue.schedules.add("s", "", every=1)
        with self.assertRaises(ScheduleError):
            self.queue.schedules.add("s", "h", every=0)
        with self.assertRaises(CronSyntaxError):
            self.queue.schedules.add("s", "h", cron="bad")
        with self.assertRaises(ScheduleError):
            self.queue.schedules.add("s", "h", every=1, max_attempts=0)
        with self.assertRaises(BackoffSpecError):
            self.queue.schedules.add("s", "h", every=1, backoff="nope")
        with self.assertRaises(ValueError):
            self.queue.schedules.add("s", "h", every=1, misfire="whenever")
        self.assertEqual(self.queue.schedules.list(), [])

    def test_duplicate_name(self):
        self.queue.schedules.add("s", "h", every=60)
        with self.assertRaises(DuplicateSchedule):
            self.queue.schedules.add("s", "h", every=60)

    def test_replace_with_same_trigger_keeps_the_next_fire(self):
        self.queue.schedules.add("s", "h", every=60)
        self.clock.advance(30)
        updated = self.queue.schedules.add("s", "other", every=60, payload=7, replace=True)
        self.assertEqual(updated.next_run_at, START + 60)
        self.assertEqual(updated.handler, "other")
        self.assertEqual(updated.payload, 7)

    def test_replace_with_new_trigger_recomputes_the_next_fire(self):
        original = self.queue.schedules.add("s", "h", every=60)
        self.clock.advance(30)
        updated = self.queue.schedules.add("s", "h", every=120, replace=True)
        self.assertEqual(updated.id, original.id)
        self.assertEqual(updated.next_run_at, START + 150)

    def test_replace_creates_when_missing(self):
        self.assertEqual(self.queue.schedules.add("s", "h", every=1, replace=True).name, "s")

    def test_list_get_remove(self):
        self.queue.schedules.add("b", "h", every=1)
        self.queue.schedules.add("a", "h", every=1)
        self.assertEqual([s.name for s in self.queue.schedules.list()], ["a", "b"])
        self.queue.schedules.remove("a")
        with self.assertRaises(ScheduleNotFound):
            self.queue.schedules.get("a")
        with self.assertRaises(ScheduleNotFound):
            self.queue.schedules.remove("a")


class PromotionTests(QueueTestCase):
    def jobs(self):
        return sorted(self.queue.list_jobs(), key=lambda job: job.id)

    def test_nothing_before_the_fire_time(self):
        self.queue.schedules.add("s", "h", every=60)
        self.clock.advance(59)
        self.assertEqual(self.queue.schedules.promote_due(), [])
        self.assertEqual(self.jobs(), [])

    def test_fire_creates_a_job_with_the_schedule_settings(self):
        schedule = self.queue.schedules.add(
            "s", "h", every=60, payload={"p": 1}, queue="q", priority=9, max_attempts=2,
            backoff="fixed:1",
        )
        self.clock.advance(60)
        promotions = self.queue.schedules.promote_due()
        self.assertEqual(len(promotions), 1)
        self.assertEqual(promotions[0].schedule, "s")
        self.assertEqual(promotions[0].next_run_at, START + 120)

        job = self.queue.get(promotions[0].job_ids[0])
        self.assertEqual(job.state, JobState.QUEUED)
        self.assertEqual(job.handler, "h")
        self.assertEqual(job.payload, {"p": 1})
        self.assertEqual(job.queue, "q")
        self.assertEqual(job.priority, 9)
        self.assertEqual(job.max_attempts, 2)
        self.assertEqual(job.backoff, "fixed:1")
        self.assertEqual(job.schedule_id, schedule.id)
        self.assertEqual(job.scheduled_for, START + 60)
        self.assertEqual(self.queue.schedules.get("s").last_fired_at, START + 60)

    def test_promoting_twice_creates_one_job(self):
        self.queue.schedules.add("s", "h", every=60)
        self.clock.advance(60)
        self.assertEqual(len(self.queue.schedules.promote_due()), 1)
        self.assertEqual(self.queue.schedules.promote_due(), [])
        self.assertEqual(len(self.jobs()), 1)

    def test_restart_does_not_duplicate(self):
        self.queue.schedules.add("s", "h", every=60)
        self.clock.advance(60)
        self.queue.schedules.promote_due()
        self.queue.close()

        reopened = self.open_queue()
        self.assertEqual(reopened.schedules.promote_due(), [])
        self.assertEqual(len(reopened.list_jobs()), 1)
        self.clock.advance(60)
        self.assertEqual(len(reopened.schedules.promote_due()), 1)
        self.assertEqual(len(reopened.list_jobs()), 2)

    def test_rewound_schedule_cannot_duplicate_a_fire(self):
        schedule = self.queue.schedules.add("s", "h", every=60)
        self.clock.advance(60)
        self.queue.schedules.promote_due()
        with self.queue.db.transaction() as conn:
            conn.execute(
                "UPDATE schedules SET next_run_at = ? WHERE id = ?", (START + 60, schedule.id)
            )
        promotions = self.queue.schedules.promote_due()
        self.assertEqual(promotions[0].job_ids, ())
        self.assertEqual(promotions[0].next_run_at, START + 120)
        self.assertEqual(len(self.jobs()), 1)

    def test_coalesce_makes_one_job_for_a_long_gap(self):
        self.queue.schedules.add("s", "h", every=60)
        self.clock.advance(60 * 10 + 5)
        promotion = self.queue.schedules.promote_due()[0]
        self.assertEqual(len(promotion.job_ids), 1)
        self.assertEqual(promotion.next_run_at, START + 60 * 11)
        self.assertEqual(self.queue.get(promotion.job_ids[0]).scheduled_for, START + 60)

    def test_coalesce_exactly_on_a_fire_time_moves_past_it(self):
        self.queue.schedules.add("s", "h", every=60)
        self.clock.advance(120)
        self.assertEqual(self.queue.schedules.promote_due()[0].next_run_at, START + 180)

    def test_catch_up_makes_one_job_per_missed_fire(self):
        self.queue.schedules.add("s", "h", every=60, misfire=Misfire.CATCH_UP)
        self.clock.advance(185)
        promotion = self.queue.schedules.promote_due()[0]
        self.assertEqual(len(promotion.job_ids), 3)
        self.assertFalse(promotion.truncated)
        self.assertEqual(promotion.next_run_at, START + 240)
        self.assertEqual(
            [job.scheduled_for for job in self.jobs()],
            [START + 60, START + 120, START + 180],
        )
        self.assertEqual(self.queue.schedules.get("s").last_fired_at, START + 180)

    def test_catch_up_stops_at_the_cap(self):
        store = ScheduleStore(
            self.queue.db, self.clock, default_max_attempts=5, max_catchup=2
        )
        store.add("s", "h", every=60, misfire="catch_up")
        self.clock.advance(60 * 5 + 1)
        promotion = store.promote_due()[0]
        self.assertEqual(len(promotion.job_ids), 2)
        self.assertTrue(promotion.truncated)
        self.assertEqual(promotion.next_run_at, START + 60 * 6)

    def test_cron_schedule_fires_on_the_minute(self):
        fire = datetime(2023, 11, 15, 3, 0, tzinfo=timezone.utc).timestamp()
        self.queue.schedules.add("s", "h", cron="0 3 * * *")
        self.clock.advance(fire - START)
        promotion = self.queue.schedules.promote_due()[0]
        self.assertEqual(self.queue.get(promotion.job_ids[0]).scheduled_for, fire)
        self.assertEqual(promotion.next_run_at, fire + 86400)

    def test_cron_catch_up(self):
        fire = datetime(2023, 11, 15, 3, 0, tzinfo=timezone.utc).timestamp()
        self.queue.schedules.add("s", "h", cron="0 3 * * *", misfire="catch_up")
        self.clock.advance(fire - START + 86400 * 2 + 10)
        promotion = self.queue.schedules.promote_due()[0]
        self.assertEqual(len(promotion.job_ids), 3)
        self.assertEqual(promotion.next_run_at, fire + 86400 * 3)

    def test_due_schedules_all_promote_together(self):
        self.queue.schedules.add("a", "h", every=60)
        self.queue.schedules.add("b", "h", every=30)
        self.queue.schedules.add("c", "h", every=600)
        self.clock.advance(60)
        promoted = [p.schedule for p in self.queue.schedules.promote_due()]
        self.assertEqual(promoted, ["b", "a"])

    def test_threads_promoting_at_once_make_one_job(self):
        self.queue.schedules.add("s", "h", every=60)
        self.clock.advance(60)
        barrier = threading.Barrier(5)
        errors = []

        def promote():
            barrier.wait()
            try:
                self.queue.schedules.promote_due()
            except Exception as exc:  # collected for the assertion below
                errors.append(exc)

        threads = [threading.Thread(target=promote) for _ in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(self.jobs()), 1)

    def test_removed_schedule_leaves_its_jobs(self):
        self.queue.schedules.add("s", "h", every=60)
        self.clock.advance(60)
        job_id = self.queue.schedules.promote_due()[0].job_ids[0]
        self.queue.schedules.remove("s")
        job = self.queue.get(job_id)
        self.assertEqual(job.state, JobState.QUEUED)
        self.assertIsNone(job.schedule_id)

    def test_new_schedule_of_the_same_name_does_not_clash_with_old_jobs(self):
        self.queue.schedules.add("s", "h", every=60)
        self.clock.advance(60)
        self.queue.schedules.promote_due()
        self.queue.schedules.remove("s")
        self.queue.schedules.add("s", "h", every=60, start_at=START + 60)
        self.assertEqual(len(self.queue.schedules.promote_due()[0].job_ids), 1)


class PauseTests(QueueTestCase):
    def test_paused_schedule_does_not_fire(self):
        self.queue.schedules.add("s", "h", every=60)
        self.assertTrue(self.queue.schedules.pause("s").paused)
        self.clock.advance(600)
        self.assertEqual(self.queue.schedules.promote_due(), [])

    def test_resume_drops_fires_missed_during_the_pause(self):
        self.queue.schedules.add("s", "h", every=60, misfire="catch_up")
        self.queue.schedules.pause("s")
        self.clock.advance(600)
        resumed = self.queue.schedules.resume("s")
        self.assertFalse(resumed.paused)
        self.assertEqual(resumed.next_run_at, START + 660)
        self.assertEqual(self.queue.schedules.promote_due(), [])

    def test_resume_before_the_next_fire_keeps_it(self):
        self.queue.schedules.add("s", "h", every=60)
        self.queue.schedules.pause("s")
        self.clock.advance(30)
        self.assertEqual(self.queue.schedules.resume("s").next_run_at, START + 60)

    def test_resume_of_a_running_schedule_changes_nothing(self):
        self.queue.schedules.add("s", "h", every=60)
        self.clock.advance(90)
        # Overdue but never paused: the overdue fire must not be lost.
        self.assertEqual(self.queue.schedules.resume("s").next_run_at, START + 60)
        self.assertEqual(len(self.queue.schedules.promote_due()), 1)

    def test_unknown_names(self):
        with self.assertRaises(ScheduleNotFound):
            self.queue.schedules.pause("nope")
        with self.assertRaises(ScheduleNotFound):
            self.queue.schedules.resume("nope")


class TriggerTests(unittest.TestCase):
    def test_interval_skip_past_is_arithmetic(self):
        trigger = IntervalTrigger(1.0)
        three_years = 3 * 365 * 86400.0
        self.assertEqual(trigger.skip_past(0.0, three_years), three_years + 1.0)

    def test_interval_skip_past_when_not_yet_due(self):
        self.assertEqual(IntervalTrigger(10).skip_past(50.0, 20.0), 50.0)

    def test_interval_skip_past_stays_on_the_series(self):
        trigger = IntervalTrigger(0.1)
        result = trigger.skip_past(0.0, 1000.05)
        self.assertGreater(result, 1000.05)
        self.assertAlmostEqual(result, 1000.1, places=6)

    def test_invalid_interval(self):
        for value in (0, -1, float("inf"), float("nan")):
            with self.subTest(value=value), self.assertRaises(ScheduleError):
                IntervalTrigger(value)

    def test_cron_skip_past(self):
        trigger = CronTrigger("0 * * * *")
        hour = datetime(2026, 1, 1, 5, tzinfo=timezone.utc).timestamp()
        self.assertEqual(trigger.skip_past(hour, hour + 7200.5), hour + 3 * 3600)
        self.assertEqual(trigger.skip_past(hour, hour - 86400), hour + 3600)


if __name__ == "__main__":
    unittest.main()
