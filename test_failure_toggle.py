"""The NWF_FAIL_TASK failure switch: downstream tasks are skipped, the failed
write rolls back, and a rerun with the switch unset recovers cleanly."""

import contextlib
import io
import os
import unittest
from datetime import date
from unittest import mock

import duckdb

import demo_failure_recovery
from dag import TaskState
from run_models import (FAIL_TASK_ENV, InjectedFailure, ModelError, backfill, build, main,
                        run_batch)
from test_models import RUN_DATE, ScratchModelsTestCase
from verify_idempotency import compare, snapshot

INT = "intermediate.int_shipment_lane_costs"
MART = "marts.mart_daily_lane_margin"


@contextlib.contextmanager
def switch(value):
    """Set NWF_FAIL_TASK for the duration of a with-block (None = unset)."""
    with mock.patch.dict(os.environ):  # restores the original environment on exit
        os.environ.pop(FAIL_TASK_ENV, None)
        if value is not None:
            os.environ[FAIL_TASK_ENV] = value
        yield


class FailureToggleTests(ScratchModelsTestCase):
    def run_build(self, fail=None, con=None, run_date=RUN_DATE):
        with switch(fail):
            return self.build(run_date, con=con)

    def states(self, result):
        return {n: r.state for n, r in result.results.items()}

    def assertSameTables(self, first, second):
        self.assertTrue(first)
        self.assertEqual([t for t, _, _, ok in compare(first, second) if not ok], [])

    def reference(self, run_date=RUN_DATE):
        """Snapshot of a clean build in a separate database."""
        con = duckdb.connect(":memory:")
        self.addCleanup(con.close)
        self.run_build(con=con, run_date=run_date).raise_for_failures()
        return snapshot(con)

    def test_unset_or_empty_switch_does_nothing(self):
        for value in (None, "", " , "):
            with self.subTest(value=value):
                self.assertTrue(self.run_build(value).succeeded)

    def test_intermediate_fails_and_mart_is_skipped(self):
        result = self.run_build(INT)
        states = self.states(result)
        self.assertEqual(states[INT], TaskState.FAILED)
        self.assertIsInstance(result.results[INT].error, InjectedFailure)
        self.assertEqual(states[MART], TaskState.SKIPPED)
        self.assertEqual(result.results[MART].skipped_because, INT)
        for name in ("staging.stg_fuel_surcharge", "staging.stg_lanes", "staging.stg_shipments"):
            self.assertEqual(states[name], TaskState.SUCCESS, name)

    def test_run_log_records_the_failure_and_the_skip(self):
        self.run_build(INT)
        rows = dict(((task, (status, error, skipped)) for task, status, error, skipped in self.con.execute(
            "select task_name, status, error, skipped_because from ops.run_log").fetchall()))
        self.assertEqual(rows[INT][0], "failed")
        self.assertIn("InjectedFailure", rows[INT][1])
        self.assertIn(FAIL_TASK_ENV, rows[INT][1])
        self.assertEqual(rows[MART][0], "skipped")
        self.assertEqual(rows[MART][2], INT)

    def test_failed_write_rolls_back_to_the_previous_rows(self):
        self.run_build().raise_for_failures()
        baseline = snapshot(self.con)
        rows = self.count(INT, f"pickup_date = date '{RUN_DATE}'")
        self.assertGreater(rows, 0)

        result = self.run_build(INT)
        # The failure happened after the delete, and the message reports the rows it deleted...
        self.assertIn(f"the {rows} rows just deleted are rolled back", str(result.results[INT].error))
        # ...but the rollback restored them, and the skipped mart was not touched.
        self.assertSameTables(baseline, snapshot(self.con))

    def test_rerun_recovers_to_a_clean_build(self):
        self.run_build().raise_for_failures()
        self.run_build(INT)
        self.assertTrue(self.run_build().succeeded)
        self.assertSameTables(self.reference(), snapshot(self.con))

    def test_failing_first_run_creates_nothing_then_recovers(self):
        self.run_build(INT)
        tables = self.tables()
        self.assertNotIn(INT, tables)   # its create table was rolled back with the rest
        self.assertNotIn(MART, tables)  # never ran
        self.assertTrue(self.run_build().succeeded)
        self.assertSameTables(self.reference(), snapshot(self.con))

    def test_failing_an_upstream_task_skips_everything_below_it(self):
        # stg_lanes feeds the intermediate model, so the intermediate task is skipped
        # rather than failed, and the mart is skipped in turn. Names are trimmed and
        # case-insensitive.
        result = self.run_build(" Staging.STG_LANES , intermediate.int_shipment_lane_costs ")
        states = self.states(result)
        self.assertEqual(states["staging.stg_lanes"], TaskState.FAILED)
        self.assertEqual(states[INT], TaskState.SKIPPED)
        self.assertEqual(result.results[INT].skipped_because, "staging.stg_lanes")
        self.assertEqual(states[MART], TaskState.SKIPPED)

    def test_unknown_task_name_is_rejected_before_anything_runs(self):
        with self.assertRaisesRegex(ModelError, "unknown task.*int_lane_costs"):
            self.run_build("int_lane_costs")
        self.assertEqual(self.tables(), set())

    def test_batch_stops_at_the_failure_and_rerun_completes(self):
        as_of = date(2026, 6, 20)
        with switch(INT):
            runs = run_batch(self.con, self.models, self.data, as_of, lookback=3)
        self.assertEqual(len(runs), 1, "batch should stop at its first partition")

        with switch(None):
            runs = run_batch(self.con, self.models, self.data, as_of, lookback=3)
        self.assertEqual(len(runs), 4)
        self.assertTrue(all(r.succeeded for _, r in runs))

    def test_backfill_stops_and_a_rerun_matches_a_clean_backfill(self):
        start, end = date(2026, 6, 18), date(2026, 6, 20)
        with switch(INT):
            batches = backfill(self.con, self.models, self.data, start, end, lookback=1)
        self.assertEqual(len(batches), 1)
        with switch(None):
            backfill(self.con, self.models, self.data, start, end, lookback=1)

        clean = duckdb.connect(":memory:")
        self.addCleanup(clean.close)
        with switch(None):
            backfill(clean, self.models, self.data, start, end, lookback=1)
        self.assertSameTables(snapshot(clean), snapshot(self.con))

    def test_cli_exits_non_zero_while_the_switch_is_on(self):
        db = str(self.models.parent / "cli.duckdb")
        argv = ["run", "--date", "2026-06-20", "--lookback", "0", "--db", db,
                "--models-dir", str(self.models), "--data-dir", str(self.data)]
        with switch(INT), contextlib.redirect_stdout(io.StringIO()) as out:
            with self.assertRaises(SystemExit) as ctx:
                main(argv)
        self.assertIn("failed", str(ctx.exception))
        self.assertIn("SKIPPED", out.getvalue())
        with switch(None), contextlib.redirect_stdout(io.StringIO()) as out:
            main(argv)
        self.assertIn("Ran 1 batch(es)", out.getvalue())


class DemoScriptTests(unittest.TestCase):
    def test_demo_passes_all_checks(self):
        with switch(None), contextlib.redirect_stdout(io.StringIO()) as out:
            demo_failure_recovery.main(["--lookback", "2"])
        self.assertIn("All checks passed", out.getvalue())
        self.assertNotIn("[FAIL]", out.getvalue())


if __name__ == "__main__":
    unittest.main()
