"""Deduplication rules in staging.stg_shipments, tested on small hand-written feeds.

For each shipment_id the newest visible version wins: latest bill_received_date
first, then the row later in the feed file. The winner is chosen across all
pickup dates before the partition and validity filters are applied.
"""

import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb

from run_models import build, run_batch
from test_models import DATA_DIR, ScratchModelsTestCase

FIELDS = ["shipment_id", "pickup_date", "delivery_date", "origin_zip", "dest_zip",
          "weight_lbs", "linehaul_revenue", "cost", "bill_received_date"]
PICKUP = date(2026, 6, 15)


def bill(sid, bill_received_date, *, pickup=PICKUP.isoformat(), delivery="2026-06-17",
         weight="1200", revenue="100.00", cost="80.00", origin="60607", dest="46204"):
    return {"shipment_id": sid, "pickup_date": pickup, "delivery_date": delivery,
            "origin_zip": origin, "dest_zip": dest, "weight_lbs": weight,
            "linehaul_revenue": revenue, "cost": cost, "bill_received_date": bill_received_date}


class DedupTests(ScratchModelsTestCase):
    def write_feed(self, *rows):
        """Replace the scratch shipments.csv with these rows, in this order."""
        with open(self.data / "shipments.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            w.writeheader()
            w.writerows(rows)

    def staged(self, sid, run_date=PICKUP, as_of=None):
        """Build run_date and return the staged row for sid as a dict, or None."""
        build(self.con, self.models, self.data, run_date, as_of).raise_for_failures()
        cur = self.con.execute("select * from staging.stg_shipments where shipment_id = ?", [sid])
        rows = cur.fetchall()
        self.assertLessEqual(len(rows), 1, f"{sid} staged more than once")
        return dict(zip([d[0] for d in cur.description], rows[0])) if rows else None

    def test_same_day_correction_wins_even_when_it_lowers_revenue(self):
        # The old rule broke ties on revenue desc and kept the $100 original.
        self.write_feed(bill("S1", "2026-06-20", revenue="100.00", cost="80.00"),
                        bill("S1", "2026-06-20", revenue="95.00", cost="76.00"))
        row = self.staged("S1")
        self.assertEqual(str(row["linehaul_revenue"]), "95.00")
        self.assertEqual(str(row["linehaul_cost"]), "76.00")

    def test_full_tie_is_broken_by_file_position_on_every_thread_count(self):
        # Same date, revenue and cost; only weight differs. The old rule picked either one.
        self.write_feed(bill("S1", "2026-06-20", weight="1200"),
                        bill("S1", "2026-06-20", weight="1350"))
        for threads in (1, 4, 8, 16):
            self.con.execute(f"set threads = {threads}")
            self.assertEqual(self.staged("S1")["weight_lbs"], 1350, f"threads={threads}")

    def test_later_bill_date_beats_later_file_position(self):
        self.write_feed(bill("S1", "2026-06-20", revenue="101.00"),
                        bill("S1", "2026-06-18", revenue="99.00"))
        self.assertEqual(str(self.staged("S1")["linehaul_revenue"]), "101.00")

    def test_missing_bill_date_never_beats_a_real_one(self):
        self.write_feed(bill("S1", "2026-06-20", revenue="100.00"),
                        bill("S1", "", revenue="90.00"))
        self.assertEqual(str(self.staged("S1")["linehaul_revenue"]), "100.00")

    def test_exact_copies_collapse_to_one_row(self):
        self.write_feed(bill("S1", "2026-06-20"), bill("S1", "2026-06-20"), bill("S1", "2026-06-20"))
        self.assertIsNotNone(self.staged("S1"))  # staged() asserts at most one row

    def test_version_after_as_of_is_not_visible_yet(self):
        self.write_feed(bill("S1", "2026-06-18", revenue="100.00"),
                        bill("S1", "2026-06-21", revenue="95.00"))
        self.assertEqual(str(self.staged("S1", as_of=date(2026, 6, 20))["linehaul_revenue"]), "100.00")
        self.assertEqual(str(self.staged("S1", as_of=date(2026, 6, 21))["linehaul_revenue"]), "95.00")

    def test_invalid_newest_version_drops_the_shipment(self):
        # The old rule filtered first, so the superseded valid bill came back.
        for label, bad in [("negative cost", {"cost": "-5.00"}),
                           ("unparseable weight", {"weight": "n/a"}),
                           ("delivered before pickup", {"delivery": "2026-06-10"})]:
            with self.subTest(label):
                self.write_feed(bill("S1", "2026-06-18"), bill("S1", "2026-06-20", **bad))
                self.assertIsNone(self.staged("S1"))

    def test_pickup_date_correction_moves_the_shipment_between_partitions(self):
        self.write_feed(bill("S1", "2026-06-18", pickup="2026-06-15"),
                        bill("S1", "2026-06-19", pickup="2026-06-16"))

        # Before the correction arrives, the shipment is in the 06-15 partition.
        run_batch(self.con, self.models, self.data, date(2026, 6, 18), lookback=3)
        partitions = "select pickup_date from staging.stg_shipments where shipment_id = 'S1'"
        self.assertEqual(self.con.execute(partitions).fetchall(), [(date(2026, 6, 15),)])

        # The next day's batch reprocesses both partitions: it leaves 06-15 and appears in 06-16.
        run_batch(self.con, self.models, self.data, date(2026, 6, 19), lookback=4)
        self.assertEqual(self.con.execute(partitions).fetchall(), [(date(2026, 6, 16),)])
        self.assertEqual(self.con.execute(
            "select count(*) from intermediate.int_shipment_lane_costs where shipment_id = 'S1'").fetchone()[0], 1)

    def test_output_schema_has_no_source_line(self):
        self.write_feed(bill("S1", "2026-06-20"))
        self.assertNotIn("source_line", self.staged("S1"))


class SourceLineTests(unittest.TestCase):
    """The tie-breaker relies on row_number() over () following file order, which
    DuckDB does in practice but does not document. Pin it here."""

    def assert_file_order(self, path, key_to_line):
        for threads in (1, 8, 16):
            con = duckdb.connect()
            con.execute(f"set threads = {threads}")
            rows = con.execute(f"select {key_to_line}, source_line from (select *, row_number() over () "
                               f"as source_line from read_csv(?, header = true, all_varchar = true))",
                               [str(path)]).fetchall()
            out_of_order = sum(1 for expected, got in rows if expected != got)
            self.assertEqual(out_of_order, 0, f"threads={threads}")
            con.close()

    def test_matches_line_order_of_the_real_feed(self):
        with open(DATA_DIR / "shipments.csv", newline="") as f:
            order = {i: r for i, r in enumerate(csv.reader(f))}
        # Row i of the file (after the header) must get source_line i. Compare on the full row.
        con = duckdb.connect()
        got = con.execute("select source_line, columns(* exclude (source_line)) from "
                          "(select *, row_number() over () as source_line from "
                          "read_csv(?, header = true, all_varchar = true)) order by source_line",
                          [str(DATA_DIR / "shipments.csv")]).fetchall()
        con.close()
        self.assertEqual(len(got), len(order) - 1)
        for line, *values in got:
            self.assertEqual(list(values), order[line], f"source_line {line}")

    def test_matches_line_order_of_a_file_scanned_in_parallel(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "big.csv"
            with open(path, "w", newline="") as f:
                f.write("k,v\n")
                f.writelines(f"{i},x{i % 97}\n" for i in range(1, 500_001))
            self.assert_file_order(path, "k::bigint")


if __name__ == "__main__":
    unittest.main()
