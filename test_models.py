import csv
import logging
import shutil
import tempfile
import unittest
from collections import defaultdict
from pathlib import Path

import duckdb

from dag import CycleError, MissingUpstreamError, TaskFailedError, TaskState
from run_models import ModelError, build, load_models

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
MODELS_DIR = ROOT / "models"

logging.disable(logging.CRITICAL)  # dag.py logs tracebacks for the failures tested below


class ModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con = duckdb.connect(":memory:")
        result = build(cls.con, MODELS_DIR, DATA_DIR)
        result.raise_for_failures()
        with open(DATA_DIR / "shipments.csv", newline="") as f:
            cls.raw = list(csv.DictReader(f))

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
        for r in self.raw:
            by_id[r["shipment_id"]].append(r)
        resent = {sid: rows for sid, rows in by_id.items()
                  if len({tuple(r.values()) for r in rows}) > 1}
        self.assertTrue(resent, "raw data should contain re-sent bills")
        for sid, rows in resent.items():
            latest = max(rows, key=lambda r: r["bill_received_date"])
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


class DagTests(unittest.TestCase):
    """How run_models turns model files into a DAG, using a scratch copy of models/."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.models = Path(tmp.name) / "models"
        shutil.copytree(MODELS_DIR, self.models)
        self.con = duckdb.connect(":memory:")
        self.addCleanup(self.con.close)

    def write_model(self, rel_path, sql):
        (self.models / rel_path).write_text(sql, encoding="utf-8")

    def count(self, table):
        return self.con.execute(f"select count(*) from {table}").fetchone()[0]

    def tables(self):
        return {f"{s}.{t}" for s, t in self.con.execute(
            "select schema_name, table_name from duckdb_tables()").fetchall()}

    def test_dependencies_come_from_headers(self):
        deps = {m.key: set(m.depends_on) for m in load_models(self.models)}
        self.assertEqual(deps, {
            "staging.stg_fuel_surcharge": set(),
            "staging.stg_lanes": set(),
            "staging.stg_shipments": set(),
            "intermediate.int_shipment_lane_costs": {
                "staging.stg_shipments", "staging.stg_lanes", "staging.stg_fuel_surcharge"},
            "marts.mart_daily_lane_margin": {"intermediate.int_shipment_lane_costs"},
        })

    def test_run_order_is_topological(self):
        order = build(self.con, self.models, DATA_DIR).order
        self.assertLess(order.index("staging.stg_shipments"),
                        order.index("intermediate.int_shipment_lane_costs"))
        self.assertLess(order.index("intermediate.int_shipment_lane_costs"),
                        order.index("marts.mart_daily_lane_margin"))

    def test_failure_skips_downstream_and_keeps_previous_tables(self):
        build(self.con, self.models, DATA_DIR)
        before = self.count("marts.mart_daily_lane_margin")

        # Break one staging model and add an independent mart that should still build.
        self.write_model("staging/stg_fuel_surcharge.sql",
                         "-- depends_on: none\nselect * from no_such_table")
        self.write_model("marts/mart_lane_list.sql",
                         "-- depends_on: staging.stg_lanes\nselect lane_id from staging.stg_lanes")
        result = build(self.con, self.models, DATA_DIR)

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

        # Failed and skipped models keep the tables from the last good build.
        self.assertEqual(self.count("staging.stg_fuel_surcharge"), 110)
        self.assertEqual(self.count("marts.mart_daily_lane_margin"), before)
        self.assertEqual(self.count("marts.mart_lane_list"), 60)

    def test_cycle_fails_before_anything_runs(self):
        self.write_model("intermediate/int_a.sql",
                         "-- depends_on: intermediate.int_b\nselect * from intermediate.int_b")
        self.write_model("intermediate/int_b.sql",
                         "-- depends_on: intermediate.int_a\nselect * from intermediate.int_a")
        with self.assertRaises(CycleError) as ctx:
            build(self.con, self.models, DATA_DIR)
        self.assertIn("intermediate.int_a", str(ctx.exception))
        self.assertEqual(self.tables(), set())

    def test_unknown_upstream_fails_before_anything_runs(self):
        self.write_model("marts/mart_x.sql",
                         "-- depends_on: intermediate.int_nope\nselect * from intermediate.int_nope")
        with self.assertRaises(MissingUpstreamError):
            build(self.con, self.models, DATA_DIR)
        self.assertEqual(self.tables(), set())

    def test_undeclared_reference_is_rejected(self):
        self.write_model("marts/mart_x.sql",
                         "-- depends_on: none\nselect * from staging.stg_lanes")
        with self.assertRaisesRegex(ModelError, "does not declare"):
            build(self.con, self.models, DATA_DIR)

    def test_unused_declaration_is_rejected(self):
        self.write_model("marts/mart_x.sql",
                         "-- depends_on: staging.stg_lanes, staging.stg_shipments\n"
                         "select * from staging.stg_lanes")
        with self.assertRaisesRegex(ModelError, "never reads"):
            build(self.con, self.models, DATA_DIR)

    def test_comments_are_not_dependencies(self):
        self.write_model("marts/mart_x.sql",
                         "-- depends_on: staging.stg_lanes\n"
                         "-- unlike marts.mart_daily_lane_margin, this is one row per lane\n"
                         "select * from staging.stg_lanes")
        self.assertTrue(build(self.con, self.models, DATA_DIR).succeeded)

    def test_missing_header_is_rejected(self):
        self.write_model("marts/mart_x.sql", "select 1 as x")
        with self.assertRaisesRegex(ModelError, "missing '-- depends_on:'"):
            build(self.con, self.models, DATA_DIR)

    def test_reading_a_later_layer_is_rejected(self):
        self.write_model("staging/stg_x.sql",
                         "-- depends_on: marts.mart_daily_lane_margin\n"
                         "select * from marts.mart_daily_lane_margin")
        with self.assertRaisesRegex(ModelError, "later layer"):
            build(self.con, self.models, DATA_DIR)


if __name__ == "__main__":
    unittest.main()
