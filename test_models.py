import csv
import logging
import os
import shutil
import tempfile
import unittest
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import duckdb

from dag import CycleError, MissingUpstreamError, TaskFailedError, TaskState
from run_models import FAIL_TASK_ENV, ModelError, PartitionError, build, load_models
from verify_idempotency import compare, fingerprint, run_twice, snapshot

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
MODELS_DIR = ROOT / "models"
RUN_DATE = date(2026, 6, 15)

logging.disable(logging.CRITICAL)  # dag.py logs tracebacks for the failures tested below
# A failure switch left set in the shell would fail unrelated tests. test_failure_toggle.py
# sets it explicitly where it's wanted.
os.environ.pop(FAIL_TASK_ENV, None)


def build_dates(con, models_dir, data_dir, start, end):
    """Build every run_date from start to end with all bills visible, failing on any error."""
    d = start
    while d <= end:
        build(con, models_dir, data_dir, d).raise_for_failures()
        d += timedelta(days=1)


def read_raw_shipments(data_dir=DATA_DIR):
    with open(Path(data_dir) / "shipments.csv", newline="") as f:
        return list(csv.DictReader(f))


class ModelTests(unittest.TestCase):
    """Data checks on a full backfill of every pickup date in the raw data."""

    @classmethod
    def setUpClass(cls):
        cls.raw = read_raw_shipments()
        dates = sorted(date.fromisoformat(r["pickup_date"]) for r in cls.raw)
        cls.con = duckdb.connect(":memory:")
        build_dates(cls.con, MODELS_DIR, DATA_DIR, dates[0], dates[-1])

    @classmethod
    def tearDownClass(cls):
        cls.con.close()

    def scalar(self, sql):
        return self.con.execute(sql).fetchone()[0]

    # staging

    def test_shipments_deduplicated(self):
        raw_ids = {r["shipment_id"] for r in self.raw}
        self.assertLess(len(raw_ids), len(self.raw), "raw data should contain duplicates")
        self.assertEqual(self.scalar("select count(*) from staging.stg_shipments"), len(raw_ids))
        self.assertEqual(self.scalar("select count(distinct shipment_id) from staging.stg_shipments"),
                         len(raw_ids))

    def test_resent_bill_keeps_latest(self):
        by_id = defaultdict(list)
        for line, r in enumerate(self.raw, start=1):
            by_id[r["shipment_id"]].append((line, r))
        resent = {sid: rows for sid, rows in by_id.items()
                  if len({tuple(r.values()) for _, r in rows}) > 1}
        self.assertTrue(resent, "raw data should contain re-sent bills")
        for sid, rows in resent.items():
            # Latest bill date wins; on a same-day tie, the row later in the file wins.
            _, latest = max(rows, key=lambda lr: (lr[1]["bill_received_date"], lr[0]))
            got = self.con.execute(
                "select bill_received_date::varchar, linehaul_revenue::varchar "
                "from staging.stg_shipments where shipment_id = ?", [sid]).fetchone()
            self.assertEqual(got, (latest["bill_received_date"], latest["linehaul_revenue"]))

    def test_zip_leading_zeros_preserved(self):
        self.assertEqual(self.scalar(
            "select count(*) from staging.stg_shipments "
            "where length(origin_zip) <> 5 or length(dest_zip) <> 5"), 0)
        self.assertGreater(self.scalar(
            "select count(*) from staging.stg_shipments where origin_zip = '07105'"), 0)

    def test_staging_keys_unique(self):
        self.assertEqual(self.scalar(
            "select count(*) - count(distinct (origin_zip, dest_zip)) from staging.stg_lanes"), 0)
        self.assertEqual(self.scalar(
            "select count(*) - count(distinct rate_date) from staging.stg_fuel_surcharge"), 0)

    # intermediate

    def test_every_shipment_has_lane_and_fuel_rate(self):
        self.assertEqual(self.scalar("select count(*) from intermediate.int_shipment_lane_costs"),
                         self.scalar("select count(*) from staging.stg_shipments"))
        self.assertEqual(self.scalar(
            "select count(*) from intermediate.int_shipment_lane_costs "
            "where lane_id is null or fuel_surcharge_pct is null"), 0)

    def test_fuel_rate_is_on_or_before_pickup(self):
        self.assertEqual(self.scalar(
            "select count(*) from intermediate.int_shipment_lane_costs "
            "where fuel_rate_date > pickup_date"), 0)

    def test_fuel_surcharge_math(self):
        self.assertEqual(self.scalar("""
            select count(*) from intermediate.int_shipment_lane_costs
            where fuel_surcharge_cost <> round(linehaul_cost * fuel_surcharge_pct / 100, 2)
               or total_cost <> linehaul_cost + fuel_surcharge_cost
               or total_revenue <> linehaul_revenue + fuel_surcharge_revenue
        """), 0)

    # marts

    def test_mart_grain_is_unique(self):
        self.assertEqual(self.scalar(
            "select count(*) - count(distinct (ship_date, lane_id)) from marts.mart_daily_lane_margin"), 0)

    def test_mart_reconciles_to_intermediate(self):
        mart = self.con.execute(
            "select sum(shipment_count), sum(total_revenue), sum(total_cost), sum(margin) "
            "from marts.mart_daily_lane_margin").fetchone()
        inter = self.con.execute(
            "select count(*), sum(total_revenue), sum(total_cost), sum(total_revenue) - sum(total_cost) "
            "from intermediate.int_shipment_lane_costs").fetchone()
        self.assertEqual(mart, inter)


class ScratchModelsTestCase(unittest.TestCase):
    """Gives each test its own copy of models/ and data/ plus an empty in-memory database."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.models = Path(tmp.name) / "models"
        self.data = Path(tmp.name) / "data"
        shutil.copytree(MODELS_DIR, self.models)
        shutil.copytree(DATA_DIR, self.data)
        self.con = duckdb.connect(":memory:")
        self.addCleanup(self.con.close)

    def build(self, run_date=RUN_DATE, con=None):
        return build(con or self.con, self.models, self.data, run_date)

    def write_model(self, rel_path, sql):
        """Write a model file, adding '-- partition_by: none' unless the SQL declares one."""
        if "partition_by:" not in sql:
            sql = "-- partition_by: none\n" + sql
        (self.models / rel_path).write_text(sql, encoding="utf-8")

    def count(self, table, where="true"):
        return self.con.execute(f"select count(*) from {table} where {where}").fetchone()[0]

    def tables(self):
        return {f"{s}.{t}" for s, t in self.con.execute(
            "select schema_name, table_name from duckdb_tables() where not temporary").fetchall()}


class DagTests(ScratchModelsTestCase):
    """How run_models turns model files into a DAG."""

    def test_headers_are_parsed(self):
        models = {m.key: m for m in load_models(self.models)}
        self.assertEqual({k: set(m.depends_on) for k, m in models.items()}, {
            "staging.stg_fuel_surcharge": set(),
            "staging.stg_lanes": set(),
            "staging.stg_shipments": set(),
            "intermediate.int_shipment_lane_costs": {
                "staging.stg_shipments", "staging.stg_lanes", "staging.stg_fuel_surcharge"},
            "marts.mart_daily_lane_margin": {"intermediate.int_shipment_lane_costs"},
        })
        self.assertEqual({k: m.partition_by for k, m in models.items()}, {
            "staging.stg_fuel_surcharge": "rate_date",
            "staging.stg_lanes": None,
            "staging.stg_shipments": "pickup_date",
            "intermediate.int_shipment_lane_costs": "pickup_date",
            "marts.mart_daily_lane_margin": "ship_date",
        })

    def test_run_order_is_topological(self):
        order = self.build().order
        self.assertLess(order.index("staging.stg_shipments"),
                        order.index("intermediate.int_shipment_lane_costs"))
        self.assertLess(order.index("intermediate.int_shipment_lane_costs"),
                        order.index("marts.mart_daily_lane_margin"))

    def test_failure_skips_downstream_and_keeps_previous_rows(self):
        self.build()
        before = snapshot(self.con)

        # Break one staging model and add an independent mart that should still build.
        self.write_model("staging/stg_fuel_surcharge.sql",
                         "-- depends_on: none\n-- partition_by: rate_date\nselect * from no_such_table")
        self.write_model("marts/mart_lane_list.sql",
                         "-- depends_on: staging.stg_lanes\nselect lane_id from staging.stg_lanes")
        result = self.build()

        states = {n: r.state for n, r in result.results.items()}
        self.assertEqual(states["staging.stg_fuel_surcharge"], TaskState.FAILED)
        self.assertEqual(states["intermediate.int_shipment_lane_costs"], TaskState.SKIPPED)
        self.assertEqual(states["marts.mart_daily_lane_margin"], TaskState.SKIPPED)
        self.assertEqual(states["staging.stg_shipments"], TaskState.SUCCESS)
        self.assertEqual(states["marts.mart_lane_list"], TaskState.SUCCESS)
        self.assertEqual(result.results["marts.mart_daily_lane_margin"].skipped_because,
                         "intermediate.int_shipment_lane_costs")
        with self.assertRaises(TaskFailedError):
            result.raise_for_failures()

        # Failed and skipped models keep their rows from the last good run.
        after = snapshot(self.con)
        for table in ("staging.stg_fuel_surcharge", "intermediate.int_shipment_lane_costs",
                      "marts.mart_daily_lane_margin"):
            self.assertEqual(after[table], before[table], table)
        self.assertEqual(self.count("marts.mart_lane_list"), 60)

    def test_cycle_fails_before_anything_runs(self):
        self.write_model("intermediate/int_a.sql",
                         "-- depends_on: intermediate.int_b\nselect * from intermediate.int_b")
        self.write_model("intermediate/int_b.sql",
                         "-- depends_on: intermediate.int_a\nselect * from intermediate.int_a")
        with self.assertRaises(CycleError) as ctx:
            self.build()
        self.assertIn("intermediate.int_a", str(ctx.exception))
        self.assertEqual(self.tables(), set())

    def test_unknown_upstream_fails_before_anything_runs(self):
        self.write_model("marts/mart_x.sql",
                         "-- depends_on: intermediate.int_nope\nselect * from intermediate.int_nope")
        with self.assertRaises(MissingUpstreamError):
            self.build()
        self.assertEqual(self.tables(), set())

    def test_undeclared_reference_is_rejected(self):
        self.write_model("marts/mart_x.sql", "-- depends_on: none\nselect * from staging.stg_lanes")
        with self.assertRaisesRegex(ModelError, "does not declare"):
            self.build()

    def test_unused_declaration_is_rejected(self):
        self.write_model("marts/mart_x.sql",
                         "-- depends_on: staging.stg_lanes, staging.stg_shipments\n"
                         "select * from staging.stg_lanes")
        with self.assertRaisesRegex(ModelError, "never reads"):
            self.build()

    def test_comments_are_not_dependencies(self):
        self.write_model("marts/mart_x.sql",
                         "-- depends_on: staging.stg_lanes\n"
                         "-- unlike marts.mart_daily_lane_margin, this is one row per lane\n"
                         "select * from staging.stg_lanes")
        self.assertTrue(self.build().succeeded)

    def test_missing_depends_on_is_rejected(self):
        self.write_model("marts/mart_x.sql", "select 1 as x")
        with self.assertRaisesRegex(ModelError, "missing '-- depends_on:'"):
            self.build()

    def test_missing_partition_by_is_rejected(self):
        (self.models / "marts" / "mart_x.sql").write_text("-- depends_on: none\nselect 1 as x")
        with self.assertRaisesRegex(ModelError, "missing '-- partition_by:'"):
            self.build()

    def test_reading_a_later_layer_is_rejected(self):
        self.write_model("staging/stg_x.sql",
                         "-- depends_on: marts.mart_daily_lane_margin\n"
                         "select * from marts.mart_daily_lane_margin")
        with self.assertRaisesRegex(ModelError, "later layer"):
            self.build()


class IdempotencyTests(ScratchModelsTestCase):
    """Running a run_date again must leave every table exactly as it was."""

    def assertSnapshotsEqual(self, first, second):
        self.assertTrue(first, "snapshot should not be empty")
        mismatched = [t for t, _, _, ok in compare(first, second) if not ok]
        self.assertEqual(mismatched, [], "tables changed between runs")

    def test_same_date_twice_on_empty_database(self):
        first, second = run_twice(self.con, self.models, self.data, RUN_DATE)
        self.assertSnapshotsEqual(first, second)
        self.assertTrue(all(rows > 0 for rows, _ in first.values()))

    def test_second_run_replaces_exactly_what_the_first_inserted(self):
        first = self.build()
        second = self.build()
        for name in second.order:
            self.assertEqual(second.results[name].output.deleted, first.results[name].output.inserted, name)
            self.assertEqual(second.results[name].output.inserted, first.results[name].output.inserted, name)

    def test_rerun_leaves_other_dates_untouched(self):
        build_dates(self.con, self.models, self.data, date(2026, 6, 10), date(2026, 6, 20))
        before = snapshot(self.con)
        self.build(RUN_DATE).raise_for_failures()
        self.assertSnapshotsEqual(before, snapshot(self.con))

    def test_order_of_dates_does_not_matter(self):
        d1, d2 = date(2026, 6, 15), date(2026, 6, 16)
        other = duckdb.connect(":memory:")
        self.addCleanup(other.close)
        for d in (d1, d2):
            self.build(d, con=self.con)
        for d in (d2, d1):
            self.build(d, con=other)
        self.assertSnapshotsEqual(snapshot(self.con), snapshot(other))

    def test_rerun_picks_up_changed_source_data(self):
        self.build()
        before = self.count("staging.stg_shipments")

        # Drop one of run_date's shipments from the raw feed and rerun: replaced, not appended.
        raw = read_raw_shipments(self.data)
        victim = next(r["shipment_id"] for r in raw if r["pickup_date"] == RUN_DATE.isoformat())
        with open(self.data / "shipments.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(raw[0]))
            w.writeheader()
            w.writerows(r for r in raw if r["shipment_id"] != victim)
        self.build().raise_for_failures()

        self.assertEqual(self.count("staging.stg_shipments"), before - 1)
        self.assertEqual(self.count("intermediate.int_shipment_lane_costs", f"shipment_id = '{victim}'"), 0)

    def test_fingerprint_catches_duplicates_and_changes_but_not_row_order(self):
        self.build()
        table = "marts.mart_daily_lane_margin"
        rows, checksum = fingerprint(self.con, table)

        # Same rows in a different physical order: identical fingerprint.
        self.con.execute(f"create table marts.shuffled as select * from {table} order by margin desc")
        self.assertEqual(fingerprint(self.con, "marts.shuffled"), (rows, checksum))

        # A duplicated row, as a non-idempotent append would produce.
        self.con.execute(f"insert into {table} select * from {table} limit 1")
        dup_rows, dup_checksum = fingerprint(self.con, table)
        self.assertEqual(dup_rows, rows + 1)
        self.assertNotEqual(dup_checksum, checksum)

        # A changed value with the row count unchanged.
        self.con.execute("update marts.shuffled set margin = margin + 0.01 "
                         "where lane_id = (select min(lane_id) from marts.shuffled)")
        changed_rows, changed_checksum = fingerprint(self.con, "marts.shuffled")
        self.assertEqual(changed_rows, rows)
        self.assertNotEqual(changed_checksum, checksum)

    def test_model_writing_other_dates_fails_and_writes_nothing(self):
        self.build(date(2026, 6, 14))
        # Forgets to filter on run_date, so it returns every loaded pickup date.
        self.write_model("marts/mart_all_dates.sql",
                         "-- depends_on: intermediate.int_shipment_lane_costs\n"
                         "-- partition_by: pickup_date\n"
                         "select * from intermediate.int_shipment_lane_costs")
        result = self.build(RUN_DATE)

        r = result.results["marts.mart_all_dates"]
        self.assertEqual(r.state, TaskState.FAILED)
        self.assertIsInstance(r.error, PartitionError)
        self.assertIn("other than run_date", str(r.error))
        self.assertNotIn("marts.mart_all_dates", self.tables())  # rolled back, table never created
        self.assertTrue(result.results["marts.mart_daily_lane_margin"].state is TaskState.SUCCESS)

    def test_partition_column_must_be_in_output(self):
        self.write_model("marts/mart_x.sql",
                         "-- depends_on: staging.stg_lanes\n-- partition_by: ship_date\n"
                         "select lane_id from staging.stg_lanes")
        r = self.build().results["marts.mart_x"]
        self.assertEqual(r.state, TaskState.FAILED)
        self.assertIsInstance(r.error, PartitionError)


if __name__ == "__main__":
    unittest.main()
