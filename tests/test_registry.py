import os
import sys
import unittest

from support import START, QueueTestCase

from jobqueue import (
    FixedBackoff,
    HandlerLoadError,
    HandlerNotFound,
    HandlerRegistry,
    load_registry,
)


class RegistrationTests(unittest.TestCase):
    def test_decorator_names_and_returns_the_function(self):
        registry = HandlerRegistry()

        @registry.handler("named")
        def first(payload):
            return "first"

        @registry.handler()
        def second(payload):
            return "second"

        self.assertEqual(first(None), "first")
        self.assertIn("named", registry)
        self.assertIn("second", registry)
        self.assertNotIn("first", registry)
        self.assertEqual(len(registry), 2)
        self.assertEqual({h.name for h in registry}, {"named", "second"})

    def test_context_argument_is_detected_from_the_signature(self):
        registry = HandlerRegistry()

        def one(payload):
            return ("one", payload)

        def two(payload, ctx):
            return ("two", payload, ctx)

        def varargs(*args):
            return ("varargs", *args)

        def keyword_only(payload, *, flag=True):
            return ("kw", payload, flag)

        def default_second(payload, ctx=None):
            return ("default", payload, ctx)

        for func in (one, two, varargs, keyword_only, default_second):
            registry.register(func.__name__, func)

        self.assertEqual(registry.resolve("one")(1, "c"), ("one", 1))
        self.assertEqual(registry.resolve("two")(1, "c"), ("two", 1, "c"))
        self.assertEqual(registry.resolve("varargs")(1, "c"), ("varargs", 1, "c"))
        self.assertEqual(registry.resolve("keyword_only")(1, "c"), ("kw", 1, True))
        self.assertEqual(registry.resolve("default_second")(1, "c"), ("default", 1, "c"))

    def test_bound_methods_and_callable_objects(self):
        class Service:
            def run(self, payload):
                return payload * 2

            def __call__(self, payload, ctx):
                return (payload, ctx)

        registry = HandlerRegistry()
        registry.register("method", Service().run)
        registry.register("object", Service())
        self.assertEqual(registry.resolve("method")(2, "c"), 4)
        self.assertEqual(registry.resolve("object")(2, "c"), (2, "c"))

    def test_duplicates_and_bad_input(self):
        registry = HandlerRegistry()
        registry.register("x", print)
        with self.assertRaises(ValueError):
            registry.register("x", print)
        with self.assertRaises(ValueError):
            registry.register("", print)
        with self.assertRaises(TypeError):
            registry.register("y", "not callable")

    def test_resolve_unknown(self):
        with self.assertRaises(HandlerNotFound):
            HandlerRegistry().resolve("nope")

    def test_merge_with_prefix_keeps_defaults(self):
        source = HandlerRegistry()
        source.register("job", lambda payload: payload, queue="q", max_attempts=2)
        target = HandlerRegistry()
        target.merge(source, prefix="app.")
        merged = target.resolve("app.job")
        self.assertEqual(merged.queue, "q")
        self.assertEqual(merged.max_attempts, 2)

    def test_merge_clash_raises(self):
        source = HandlerRegistry()
        source.register("job", print)
        target = HandlerRegistry()
        target.register("job", print)
        with self.assertRaises(ValueError):
            target.merge(source)


class RegistryEnqueueTests(QueueTestCase):
    def setUp(self):
        super().setUp()
        self.registry = HandlerRegistry()
        self.registry.register(
            "job", print, queue="q", priority=5, max_attempts=2, backoff=FixedBackoff(9)
        )

    def test_defaults_apply(self):
        job = self.registry.enqueue(self.queue, "job", {"x": 1}).job
        self.assertEqual(job.queue, "q")
        self.assertEqual(job.priority, 5)
        self.assertEqual(job.max_attempts, 2)
        self.assertEqual(job.backoff, "fixed:9")
        self.assertEqual(job.payload, {"x": 1})

    def test_keywords_win(self):
        job = self.registry.enqueue(self.queue, "job", queue="other", priority=1, delay=4).job
        self.assertEqual(job.queue, "other")
        self.assertEqual(job.priority, 1)
        self.assertEqual(job.run_at, START + 4)

    def test_handler_backoff_without_text_form_is_not_stored(self):
        class Custom:
            def delay(self, attempt):
                return 1.0

            def spec(self):
                return None

        self.registry.register("custom", print, backoff=Custom())
        self.assertIsNone(self.registry.enqueue(self.queue, "custom").job.backoff)

    def test_unknown_handler(self):
        with self.assertRaises(HandlerNotFound):
            self.registry.enqueue(self.queue, "nope")


class LoadRegistryTests(QueueTestCase):
    def write_module(self, name: str, body: str) -> None:
        with open(os.path.join(self.dir, f"{name}.py"), "w", encoding="utf-8") as handle:
            handle.write(body)
        sys.path.insert(0, self.dir)
        self.addCleanup(sys.path.remove, self.dir)
        self.addCleanup(sys.modules.pop, name, None)

    def test_loads(self):
        self.write_module(
            "handlers_ok",
            "from jobqueue import HandlerRegistry\nregistry = HandlerRegistry()\n"
            "registry.register('x', print)\n",
        )
        self.assertIn("x", load_registry("handlers_ok:registry"))

    def test_bad_references(self):
        self.write_module("handlers_bad", "registry = 42\n")
        for reference, fragment in (
            ("no-colon", "module:attribute"),
            (":registry", "module:attribute"),
            ("handlers_bad:", "module:attribute"),
            ("no_such_module_here:registry", "cannot import"),
            ("handlers_bad:missing", "has no"),
            ("handlers_bad:registry", "not a HandlerRegistry"),
        ):
            with self.subTest(reference=reference):
                with self.assertRaisesRegex(HandlerLoadError, fragment):
                    load_registry(reference)


if __name__ == "__main__":
    unittest.main()
