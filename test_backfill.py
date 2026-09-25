import contextlib
import io
import tempfile
import unittest
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

import duckdb

import run_models
from dag import TaskState
from run_models import backfill, build, lookback_partitions, main, run_batch
from test_models import ScratchModelsTestCase, read_raw_shipments
from verify_idempotency import compare, snapshot

TASKS = ["staging.stg_fuel_surcharge", "staging.stg_lanes", "staging.stg_shipments",
         "intermediate.int_shipment_lane_costs", "marts.mart_daily_lane_margin"]


def days(n):
    return timedelta(days=n)


def pick_shipments():
    """Find raw shipments with the properties the lookback tests need."""
    by_id = defaultdict(list)
    for r in read_raw_shipments():
        by_id[r["shipment_id"]].append(r)
    single, resent = {}, {}
    for sid, rows in by_id.items():
        pickup = date.fromisoformat(rows[0]["pickup_date"])
        bills = sorted(date.fromisoformat(r["bill_received_date"]) for r in rows)
        if len(rows) == 1:
            single.setdefault((bills[0] - pickup).days, (sid, pickup, bills[0]))
        elif len({tuple(r.values()) for r in rows}) > 1 and bills[0] < bills[-1] \
                and (bills[-1] - pickup).days <= run_models.DEFAULT_LOOKBACK:
            by_bill = {r["bill_received_date"]: r for r in rows}
            resent.setdefault("x", (sid, pickup, bills[0], bills[-1],
                                    by_bill[bills[0].isoformat()]["linehaul_revenue"],
                                    by_bill[bills[-1].isoformat()]["linehaul_revenue"]))
    return single, resent["x"]


SINGLE_BY_LAG, RESENT = pick_shipments()


class LookbackTests(ScratchModelsTestCase):
    def scalar(self, sql, params=()):
        return self.con.execute(sql, list(params)).fetchone()[0]

    def batch(self, as_of, lookback=run_models.DEFAULT_LOOKBACK):
        runs = run_batch(self.con, self.models, self.data, as_of, lookback)
        for _, result in runs:
            result.raise_for_failures()
        return runs

    def shipment_count(self, table, sid):
        return self.scalar(f"select count(*) from {table} where shipment_id = ?", [sid])

    def test_lookback_partitions_oldest_first(self):
        self.assertEqual(lookback_partitions(date(2026, 6, 20), 3),
                         [date(2026, 6, 17), date(2026, 6, 18), date(2026, 6, 19), date(2026, 6, 20)])
        self.assertEqual(lookback_partitions(date(2026, 6, 20), 0), [date(2026, 6, 20)])

    def test_late_bill_corrects_earlier_partition(self):
        sid, pickup, billed = SINGLE_BY_LAG[10]
        mart_total = ("select coalesce(sum(shipment_count), 0) from marts.mart_daily_lane_margin "
                      "where ship_date = ?")

        self.batch(billed - days(1))
        self.assertEqual(self.shipment_count("staging.stg_shipments", sid), 0)
        self.assertEqual(self.shipment_count("intermediate.int_shipment_lane_costs", sid), 0)
        before = self.scalar(mart_total, [pickup])

        # The bill arrives 10 days after pickup; that day's batch reprocesses the pickup partition.
        self.batch(billed)
        self.assertEqual(self.shipment_count("staging.stg_shipments", sid), 1)
        self.assertEqual(self.shipment_count("intermediate.int_shipment_lane_costs", sid), 1)
        after = self.scalar(mart_total, [pickup])
        self.assertGreater(after, before)
        self.assertEqual(after, self.scalar(
            "select count(*) from intermediate.int_shipment_lane_costs where pickup_date = ?", [pickup]))

    def test_resent_bill_replaces_original(self):
        sid, pickup, first_bill, last_bill, first_rev, last_rev = RESENT
        self.assertNotEqual(first_rev, last_rev, "fixture should be a corrected bill")
        revenue = "select linehaul_revenue::varchar from staging.stg_shipments where shipment_id = ?"

        self.batch(first_bill)
        self.assertEqual(self.scalar(revenue, [sid]), first_rev)
        self.batch(last_bill)
        self.assertEqual(self.shipment_count("staging.stg_shipments", sid), 1)
        self.assertEqual(self.scalar(revenue, [sid]), last_rev)

    def test_bill_later_than_lookback_is_missed(self):
        sid, pickup, billed = SINGLE_BY_LAG[5]
        backfill(self.con, self.models, self.data, pickup, billed, lookback=4)
        self.assertEqual(self.shipment_count("staging.stg_shipments", sid), 0)
        backfill(self.con, self.models, self.data, billed, billed, lookback=5)
        self.assertEqual(self.shipment_count("staging.stg_shipments", sid), 1)

    def test_backfill_equals_last_reprocessing_of_each_partition(self):
        """After a backfill, each partition matches one build as of its last batch in the window."""
        start, end, lookback = date(2026, 6, 10), date(2026, 6, 16), 3
        for _, runs in backfill(self.con, self.models, self.data, start, end, lookback):
            for _, result in runs:
                result.raise_for_failures()

        expected = duckdb.connect(":memory:")
        self.addCleanup(expected.close)
        p = start - days(lookback)
        while p <= end:
            build(expected, self.models, self.data, p, min(p + days(lookback), end)).raise_for_failures()
            p += days(1)

        mismatched = [t for t, _, _, ok in compare(snapshot(expected), snapshot(self.con)) if not ok]
        self.assertEqual(mismatched, [])

    def test_rerunning_a_batch_is_idempotent(self):
        self.batch(date(2026, 6, 20), lookback=3)
        first = snapshot(self.con)
        self.batch(date(2026, 6, 20), lookback=3)
        self.assertEqual([t for t, _, _, ok in compare(first, snapshot(self.con)) if not ok], [])


class RunLogTests(ScratchModelsTestCase):
    def log_rows(self, where="true"):
        cur = self.con.execute(f"select * from ops.run_log where {where} order by log_id")
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def test_every_task_start_end_rows_and_status_are_logged(self):
        as_of = date(2026, 6, 20)
        runs = run_batch(self.con, self.models, self.data, as_of, lookback=2)
        rows = self.log_rows()

        self.assertEqual(len(rows), 3 * len(TASKS))
        self.assertEqual({r["batch_id"] for r in rows}, {rows[0]["batch_id"]})
        expected = {(run_date, name): result.results[name].output
                    for run_date, result in runs for name in result.order}
        for r in rows:
            self.assertEqual(r["status"], "success")
            self.assertEqual(r["command"], "run")
            self.assertEqual(r["as_of_date"], as_of)
            self.assertLessEqual(r["started_at"], r["ended_at"])
            self.assertGreaterEqual(r["duration_s"], 0)
            stats = expected[(r["run_date"], r["task_name"])]
            self.assertEqual((r["rows_deleted"], r["rows_inserted"]), (stats.deleted, stats.inserted))
            self.assertIsNone(r["error"])

    def test_backfill_runs_partitions_in_order(self):
        start, end, lookback = date(2026, 6, 10), date(2026, 6, 12), 2
        backfill(self.con, self.models, self.data, start, end, lookback)
        rows = self.log_rows()
        self.assertEqual({r["command"] for r in rows}, {"backfill"})

        # Batches in as_of order; within a batch, partitions oldest first; within a
        # partition, the models in dependency order.
        expected = [(as_of, run_date, task)
                    for as_of in (start, start + days(1), end)
                    for run_date in lookback_partitions(as_of, lookback)
                    for task in TASKS]
        self.assertEqual([(r["as_of_date"], r["run_date"], r["task_name"]) for r in rows], expected)

    def test_start_is_committed_before_the_task_runs(self):
        seen = []
        real = run_models.write_partition

        def spy(con, m, **kwargs):
            seen.append(con.execute("select status from ops.run_log where task_name = ? "
                                    "order by log_id desc limit 1", [m.key]).fetchone()[0])
            return real(con, m, **kwargs)

        with mock.patch.object(run_models, "write_partition", spy):
            build(self.con, self.models, self.data, date(2026, 6, 15))
        self.assertEqual(seen, ["running"] * len(TASKS))
        self.assertEqual({r["status"] for r in self.log_rows()}, {"success"})

    def test_failure_and_skips_are_logged_and_backfill_stops(self):
        self.write_model("staging/stg_fuel_surcharge.sql",
                         "-- depends_on: none\n-- partition_by: rate_date\nselect * from no_such_table")
        batches = backfill(self.con, self.models, self.data, date(2026, 6, 10), date(2026, 6, 12), lookback=2)

        # Stops at the first failed partition of the first batch.
        self.assertEqual(len(batches), 1)
        self.assertEqual(len(batches[0][1]), 1)

        by_task = {r["task_name"]: r for r in self.log_rows()}
        self.assertEqual(by_task["staging.stg_fuel_surcharge"]["status"], "failed")
        self.assertIn("no_such_table", by_task["staging.stg_fuel_surcharge"]["error"])
        self.assertIsNotNone(by_task["staging.stg_fuel_surcharge"]["ended_at"])
        self.assertEqual(by_task["intermediate.int_shipment_lane_costs"]["status"], "skipped")
        self.assertEqual(by_task["intermediate.int_shipment_lane_costs"]["skipped_because"],
                         "staging.stg_fuel_surcharge")
        self.assertEqual(by_task["marts.mart_daily_lane_margin"]["status"], "skipped")
        self.assertEqual(by_task["staging.stg_shipments"]["status"], "success")
        self.assertEqual(self.log_rows("status = 'running'"), [])

    def test_interrupted_task_is_logged_as_failed(self):
        def interrupted(con, m, **kwargs):
            raise KeyboardInterrupt

        with mock.patch.object(run_models, "write_partition", interrupted):
            with self.assertRaises(KeyboardInterrupt):
                build(self.con, self.models, self.data, date(2026, 6, 15))
        rows = self.log_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "failed")
        self.assertIn("KeyboardInterrupt", rows[0]["error"])


class CliTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.db = str(Path(tmp.name) / "cli.duckdb")

    def cli(self, *argv):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            main([*argv, "--db", self.db])
        return out.getvalue()

    def test_backfill_then_log(self):
        out = self.cli("backfill", "--start", "2026-06-10", "--end", "2026-06-11", "--lookback", "1")
        self.assertIn("as_of 2026-06-10  partitions 2026-06-09..2026-06-10", out)
        self.assertIn("Ran 2 batch(es)", out)

        log = self.cli("log")
        self.assertIn("(backfill)", log)
        self.assertIn("20 total: 20 success, 0 failed, 0 skipped, 0 running", log)

    def test_run_uses_date_as_as_of(self):
        self.cli("run", "--date", "2026-06-15", "--lookback", "0")
        with duckdb.connect(self.db, read_only=True) as con:
            self.assertEqual(con.execute("select distinct as_of_date, run_date, command from ops.run_log").fetchall(),
                             [(date(2026, 6, 15), date(2026, 6, 15), "run")])

    def test_end_before_start_is_rejected(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            self.cli("backfill", "--start", "2026-06-11", "--end", "2026-06-10")


if __name__ == "__main__":
    unittest.main()
