import random
import unittest

from support import START, QueueTestCase

from jobqueue import (
    BackoffSpecError,
    ExponentialBackoff,
    FixedBackoff,
    JitterBackoff,
    parse_backoff,
)


class PolicyTests(unittest.TestCase):
    def test_fixed(self):
        policy = FixedBackoff(30)
        self.assertEqual([policy.delay(n) for n in (1, 2, 50)], [30, 30, 30])
        with self.assertRaises(ValueError):
            FixedBackoff(-1)

    def test_exponential_grows_then_caps(self):
        policy = ExponentialBackoff(base=2, factor=3, maximum=50)
        self.assertEqual([policy.delay(n) for n in (1, 2, 3, 4, 5)], [2, 6, 18, 50, 50])

    def test_exponential_survives_enormous_attempt_numbers(self):
        policy = ExponentialBackoff(base=1.0, factor=10.0, maximum=99.0)
        self.assertEqual(policy.delay(100_000), 99.0)

    def test_exponential_rejects_bad_settings(self):
        with self.assertRaises(ValueError):
            ExponentialBackoff(factor=0.5)
        with self.assertRaises(ValueError):
            ExponentialBackoff(base=-1)

    def test_full_jitter_stays_inside_the_range_and_is_reproducible(self):
        first = JitterBackoff(FixedBackoff(100), rng=random.Random(42))
        second = JitterBackoff(FixedBackoff(100), rng=random.Random(42))
        values = [first.delay(1) for _ in range(200)]
        self.assertEqual(values, [second.delay(1) for _ in range(200)])
        self.assertTrue(all(0 <= value <= 100 for value in values))
        self.assertGreater(len(set(values)), 100)

    def test_equal_jitter_keeps_at_least_half(self):
        policy = JitterBackoff(FixedBackoff(100), mode="equal", rng=random.Random(1))
        values = [policy.delay(1) for _ in range(200)]
        self.assertTrue(all(50 <= value <= 100 for value in values))

    def test_unknown_jitter_mode(self):
        with self.assertRaises(ValueError):
            JitterBackoff(FixedBackoff(1), mode="wild")


class SpecTests(unittest.TestCase):
    def test_round_trips(self):
        for policy in (
            FixedBackoff(30),
            FixedBackoff(1234567.5),
            ExponentialBackoff(2, 3, 600),
            ExponentialBackoff(0.5, 1.5, 1000000),
        ):
            rebuilt = parse_backoff(policy.spec())
            self.assertEqual(rebuilt, policy)

    def test_jitter_round_trip(self):
        policy = JitterBackoff(ExponentialBackoff(2, 2, 60), mode="equal")
        rebuilt = parse_backoff(policy.spec())
        self.assertIsInstance(rebuilt, JitterBackoff)
        self.assertEqual(rebuilt.mode, "equal")
        self.assertEqual(rebuilt.inner, ExponentialBackoff(2, 2, 60))

    def test_jitter_spec_is_none_when_inner_has_no_text_form(self):
        class Custom:
            def delay(self, attempt):
                return 1.0

            def spec(self):
                return None

        self.assertIsNone(JitterBackoff(Custom()).spec())

    def test_parsing_accepts_units_and_partial_options(self):
        self.assertEqual(parse_backoff("fixed:1m"), FixedBackoff(60))
        self.assertEqual(parse_backoff("exp:base=2s,max=1h"), ExponentialBackoff(2, 2, 3600))
        self.assertEqual(parse_backoff("exp:"), ExponentialBackoff())

    def test_rng_reaches_the_jitter_layer(self):
        rng = random.Random(9)
        self.assertIs(parse_backoff("jitter:full:fixed:5", rng=rng).rng, rng)

    def test_bad_specs(self):
        for spec in (
            "",
            "fixed",
            "fixed:",
            "fixed:soon",
            "exp:base",
            "exp:speed=2",
            "exp:factor=x",
            "exp:factor=0.1",
            "jitter:fixed:1",
            "jitter:full:",
            "wobbly:1",
        ):
            with self.subTest(spec=spec), self.assertRaises(BackoffSpecError):
                parse_backoff(spec)


class QueueBackoffTests(QueueTestCase):
    def test_job_policy_decides_the_retry_time(self):
        self.enqueue(backoff=ExponentialBackoff(10, 2, 1000))
        for expected in (10, 20, 40):
            job = self.lease_one()
            now = self.clock.now()
            retried = self.queue.fail(job.id, job.lease_token, "x")
            self.assertEqual(retried.run_at, now + expected)
            self.clock.advance(expected)

    def test_queue_default_policy_when_job_has_none(self):
        queue = self.open_queue(backoff=FixedBackoff(7))
        queue.enqueue("h")
        job = queue.lease("w")[0]
        self.assertEqual(queue.fail(job.id, job.lease_token, "x").run_at, START + 7)

    def test_jitter_on_a_job_uses_the_injected_rng(self):
        queue = self.open_queue(rng=random.Random(5))
        queue.enqueue("h", backoff="jitter:full:fixed:100")
        job = queue.lease("w")[0]
        expected = random.Random(5).uniform(0.0, 100.0)
        self.assertAlmostEqual(
            queue.fail(job.id, job.lease_token, "x").run_at, START + expected
        )


if __name__ == "__main__":
    unittest.main()
