import unittest
from datetime import datetime, timezone

from jobqueue import CronSyntaxError
from jobqueue.cron import CronExpression


def ts(*parts):
    return datetime(*parts, tzinfo=timezone.utc).timestamp()


def stamp(value):
    return datetime.fromtimestamp(value, timezone.utc).strftime("%Y-%m-%d %H:%M")


class NextAfterTests(unittest.TestCase):
    def check(self, expr, start, *expected):
        cron = CronExpression(expr)
        current = start
        for want in expected:
            current = cron.next_after(current)
            self.assertEqual(stamp(current), want)

    def test_every_minute_is_strictly_after(self):
        self.check("* * * * *", ts(2026, 3, 1, 12, 0), "2026-03-01 12:01", "2026-03-01 12:02")

    def test_seconds_round_up_to_the_next_minute(self):
        self.check("* * * * *", ts(2026, 3, 1, 12, 0, 59), "2026-03-01 12:01")

    def test_later_today_then_tomorrow(self):
        self.check(
            "30 9,17 * * *",
            ts(2026, 3, 1, 10, 0),
            "2026-03-01 17:30",
            "2026-03-02 09:30",
        )

    def test_range_and_step(self):
        self.check(
            "10-20/5 8 * * *",
            ts(2026, 1, 1),
            "2026-01-01 08:10",
            "2026-01-01 08:15",
            "2026-01-01 08:20",
            "2026-01-02 08:10",
        )

    def test_value_with_step_runs_to_the_top_of_the_field(self):
        self.check("50/5 3 * * *", ts(2026, 1, 1), "2026-01-01 03:50", "2026-01-01 03:55")
        self.check("50/5 3 * * *", ts(2026, 1, 1, 3, 55), "2026-01-02 03:50")

    def test_month_and_year_rollover(self):
        self.check("0 0 1 * *", ts(2026, 12, 15), "2027-01-01 00:00", "2027-02-01 00:00")

    def test_day_31_skips_short_months(self):
        self.check("0 0 31 * *", ts(2026, 1, 31), "2026-03-31 00:00", "2026-05-31 00:00")

    def test_leap_day(self):
        self.check("0 12 29 2 *", ts(2026, 1, 1), "2028-02-29 12:00", "2032-02-29 12:00")

    def test_leap_day_across_a_skipped_century(self):
        self.check("0 12 29 2 *", ts(2097, 1, 1), "2104-02-29 12:00")

    def test_weekday(self):
        # 2 March 2026 is a Monday.
        self.check("0 8 * * 1", ts(2026, 3, 1), "2026-03-02 08:00", "2026-03-09 08:00")

    def test_sunday_as_zero_seven_and_name(self):
        for field in ("0", "7", "sun", "SUN"):
            self.check(f"0 0 * * {field}", ts(2026, 3, 2), "2026-03-08 00:00")

    def test_names_in_ranges_and_any_case(self):
        self.check("0 0 1 Jan-Feb *", ts(2026, 1, 5), "2026-02-01 00:00", "2027-01-01 00:00")
        self.check("0 0 * * mon-tue", ts(2026, 3, 3), "2026-03-09 00:00")

    def test_restricted_day_and_weekday_match_either(self):
        # The 15th, or any Friday: 6 March 2026 is a Friday.
        self.check(
            "0 0 15 * 5",
            ts(2026, 3, 1),
            "2026-03-06 00:00",
            "2026-03-13 00:00",
            "2026-03-15 00:00",
            "2026-03-20 00:00",
        )

    def test_star_step_day_does_not_trigger_the_either_rule(self):
        # Odd days AND Mondays. 2 March is even, 9 March odd.
        self.check("0 0 */2 * 1", ts(2026, 3, 1), "2026-03-09 00:00")

    def test_star_step_weekday_does_not_trigger_the_either_rule(self):
        # The 15th AND a Sunday, so the Sundays 8 March and 22 March do not fire.
        self.check("0 0 15 * */7", ts(2026, 3, 1), "2026-03-15 00:00", "2026-11-15 00:00")

    def test_aliases(self):
        self.check("@hourly", ts(2026, 1, 1, 5, 10), "2026-01-01 06:00")
        self.check("@daily", ts(2026, 1, 1, 5, 10), "2026-01-02 00:00")
        self.check("@midnight", ts(2026, 1, 1, 5, 10), "2026-01-02 00:00")
        self.check("@weekly", ts(2026, 3, 2), "2026-03-08 00:00")
        self.check("@monthly", ts(2026, 1, 1, 5, 10), "2026-02-01 00:00")
        self.check("@yearly", ts(2026, 1, 1, 5, 10), "2027-01-01 00:00")
        self.check("@annually", ts(2026, 1, 1, 5, 10), "2027-01-01 00:00")


class MatchesTests(unittest.TestCase):
    def test_matches_any_second_of_the_minute(self):
        cron = CronExpression("15 10 * * *")
        self.assertTrue(cron.matches(ts(2026, 1, 1, 10, 15)))
        self.assertTrue(cron.matches(ts(2026, 1, 1, 10, 15, 59)))
        self.assertFalse(cron.matches(ts(2026, 1, 1, 10, 16)))
        self.assertFalse(cron.matches(ts(2026, 1, 1, 11, 15)))

    def test_month_and_day_rules(self):
        cron = CronExpression("0 0 1 6 *")
        self.assertTrue(cron.matches(ts(2026, 6, 1)))
        self.assertFalse(cron.matches(ts(2026, 7, 1)))
        self.assertFalse(cron.matches(ts(2026, 6, 2)))

    def test_every_next_after_result_matches(self):
        cron = CronExpression("*/7 3,4 1-10 */2 *")
        current = ts(2026, 1, 1)
        for _ in range(100):
            current = cron.next_after(current)
            self.assertTrue(cron.matches(current))


class SyntaxTests(unittest.TestCase):
    def test_bad_expressions(self):
        cases = {
            "": "5 fields",
            "* * * *": "5 fields",
            "* * * * * *": "5 fields",
            "60 * * * *": "minute",
            "* 24 * * *": "hour",
            "* * 0 * *": "day",
            "* * 32 * *": "day",
            "* * * 13 *": "month",
            "* * * * 8": "weekday",
            "a * * * *": "minute",
            "*/0 * * * *": "step",
            "*/x * * * *": "step",
            "5-1 * * * *": "backwards",
            "1,,2 * * * *": "empty",
            "-1 * * * *": "minute",
            "* * * foo *": "month",
            "@sometimes": "alias",
        }
        for text, fragment in cases.items():
            with self.subTest(text=text):
                with self.assertRaisesRegex(CronSyntaxError, fragment):
                    CronExpression(text)

    def test_impossible_date_is_refused_at_parse_time(self):
        for text in ("0 0 30 2 *", "0 0 31 4 *", "0 0 31 2,4,6,9,11 *"):
            with self.subTest(text=text):
                with self.assertRaisesRegex(CronSyntaxError, "never fires"):
                    CronExpression(text)

    def test_impossible_date_rescued_by_weekday(self):
        # Vixie rule: 30 February never happens, but Mondays in February do.
        cron = CronExpression("0 0 30 2 1")
        self.assertEqual(stamp(cron.next_after(ts(2026, 1, 1))), "2026-02-02 00:00")

    def test_whitespace_is_forgiven(self):
        self.assertEqual(
            CronExpression("  0   0  *  * * ").next_after(ts(2026, 1, 1)),
            ts(2026, 1, 2),
        )


if __name__ == "__main__":
    unittest.main()
