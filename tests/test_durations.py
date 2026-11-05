import unittest

from jobqueue import DurationError, format_duration, parse_duration
from jobqueue.durations import format_timestamp, plain_number


class ParseDurationTests(unittest.TestCase):
    def test_accepted_forms(self):
        cases = {
            "90": 90.0,
            "1.5": 1.5,
            "90s": 90.0,
            "5m": 300.0,
            "2h": 7200.0,
            "1d": 86400.0,
            "1w": 604800.0,
            "250ms": 0.25,
            "1h30m": 5400.0,
            "1h 30m": 5400.0,
            "1.5h": 5400.0,
            " 10S ": 10.0,
            "0": 0.0,
        }
        for text, seconds in cases.items():
            with self.subTest(text=text):
                self.assertEqual(parse_duration(text), seconds)

    def test_numbers_pass_through(self):
        self.assertEqual(parse_duration(5), 5.0)
        self.assertEqual(parse_duration(2.5), 2.5)

    def test_refused(self):
        for value in (
            "",
            "  ",
            "abc",
            "5x",
            "m5",
            "5m3",
            "-5",
            "-5m",
            "nan",
            "inf",
            -1,
            float("inf"),
            float("nan"),
            True,
        ):
            with self.subTest(value=value), self.assertRaises(DurationError):
                parse_duration(value)

    def test_error_is_a_value_error_so_argparse_reports_it(self):
        self.assertTrue(issubclass(DurationError, ValueError))


class FormattingTests(unittest.TestCase):
    def test_format_duration(self):
        cases = {
            0: "0s",
            0.25: "250ms",
            1: "1s",
            1.5: "1.5s",
            59: "59s",
            60: "1m",
            5400: "1h30m",
            90061: "1d1h1m1s",
            -60: "-1m",
            3600.7: "1h",
        }
        for seconds, text in cases.items():
            with self.subTest(seconds=seconds):
                self.assertEqual(format_duration(seconds), text)

    def test_format_round_trips_through_parse(self):
        for seconds in (1.0, 60.0, 5400.0, 90061.0, 0.25):
            self.assertEqual(parse_duration(format_duration(seconds)), seconds)

    def test_format_timestamp(self):
        self.assertEqual(format_timestamp(0.0), "1970-01-01T00:00:00Z")
        self.assertEqual(format_timestamp(None), "-")

    def test_plain_number(self):
        self.assertEqual(plain_number(300.0), "300")
        self.assertEqual(plain_number(1234567.0), "1234567")
        self.assertEqual(plain_number(0.1), "0.1")
        self.assertEqual(float(plain_number(1e20)), 1e20)


if __name__ == "__main__":
    unittest.main()
