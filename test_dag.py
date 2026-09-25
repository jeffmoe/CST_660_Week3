import logging
import unittest

from dag import (DAG, CycleError, DuplicateTaskError, MissingUpstreamError,
                 TaskFailedError, TaskState)

logging.disable(logging.CRITICAL)


def make_dag(edges, fail=(), calls=None):
    """Build a DAG from {name: [upstream, ...]}; tasks named in `fail` raise."""
    calls = [] if calls is None else calls
    dag = DAG("test")
    for name, upstream in edges.items():
        def fn(name=name):
            calls.append(name)
            if name in fail:
                raise ValueError(f"{name} boom")
            return name.upper()
        dag.add_task(name, fn, upstream)
    return dag


class TopologicalOrderTests(unittest.TestCase):
    def test_upstreams_come_first(self):
        edges = {"load": ["transform", "fuel"], "transform": ["extract"],
                 "fuel": [], "extract": [], "report": ["load"]}
        order = make_dag(edges).topological_order()
        for name, ups in edges.items():
            for u in ups:
                self.assertLess(order.index(u), order.index(name))
        self.assertEqual(len(order), len(edges))

    def test_order_is_deterministic_by_registration(self):
        dag = make_dag({"b": [], "a": [], "c": ["a", "b"]})
        self.assertEqual(dag.topological_order(), ["b", "a", "c"])

    def test_decorator_registration(self):
        dag = DAG()

        @dag.task()
        def extract():
            return 1

        @dag.task(upstream="extract")
        def transform():
            return 2

        self.assertEqual(dag.topological_order(), ["extract", "transform"])
        self.assertEqual(dag.run().results["transform"].output, 2)


class CycleTests(unittest.TestCase):
    def test_cycle_raises_with_path(self):
        dag = make_dag({"a": ["c"], "b": ["a"], "c": ["b"], "d": []})
        with self.assertRaises(CycleError) as ctx:
            dag.topological_order()
        cycle = ctx.exception.cycle
        self.assertEqual(cycle[0], cycle[-1])
        self.assertEqual(set(cycle), {"a", "b", "c"})
        self.assertIn("->", str(ctx.exception))

    def test_self_dependency_is_a_cycle(self):
        with self.assertRaises(CycleError):
            make_dag({"a": ["a"]}).topological_order()

    def test_cycle_fails_before_any_task_runs(self):
        calls = []
        dag = make_dag({"ok": [], "a": ["b"], "b": ["a"]}, calls=calls)
        with self.assertRaises(CycleError):
            dag.run()
        self.assertEqual(calls, [])

    def test_missing_upstream(self):
        with self.assertRaises(MissingUpstreamError):
            make_dag({"a": ["nope"]}).run()

    def test_duplicate_task(self):
        dag = DAG()
        dag.add_task("a", lambda: None)
        with self.assertRaises(DuplicateTaskError):
            dag.add_task("a", lambda: None)


class FailureTests(unittest.TestCase):
    def setUp(self):
        # extract -> transform -> load -> report, plus an independent fuel branch
        self.edges = {"extract": [], "fuel": [], "transform": ["extract"],
                      "load": ["transform", "fuel"], "report": ["load"],
                      "fuel_check": ["fuel"]}

    def test_all_succeed(self):
        result = make_dag(self.edges).run()
        self.assertTrue(result.succeeded)
        self.assertEqual(result.results["report"].output, "REPORT")
        result.raise_for_failures()  # no-op

    def test_failure_skips_all_downstream_but_not_siblings(self):
        calls = []
        result = make_dag(self.edges, fail={"transform"}, calls=calls).run()
        states = {n: r.state for n, r in result.results.items()}
        self.assertEqual(states["transform"], TaskState.FAILED)
        self.assertEqual(states["load"], TaskState.SKIPPED)
        self.assertEqual(states["report"], TaskState.SKIPPED)
        self.assertEqual(states["fuel"], TaskState.SUCCESS)
        self.assertEqual(states["fuel_check"], TaskState.SUCCESS)
        self.assertNotIn("load", calls)
        self.assertNotIn("report", calls)
        self.assertEqual(result.results["load"].skipped_because, "transform")
        self.assertEqual(result.results["report"].skipped_because, "load")
        self.assertIsInstance(result.results["transform"].error, ValueError)

    def test_raise_for_failures(self):
        result = make_dag(self.edges, fail={"fuel"}).run()
        with self.assertRaises(TaskFailedError) as ctx:
            result.raise_for_failures()
        self.assertIn("fuel", str(ctx.exception))
        self.assertIn("load", str(ctx.exception))
        self.assertIsInstance(ctx.exception.__cause__, ValueError)


if __name__ == "__main__":
    unittest.main()
