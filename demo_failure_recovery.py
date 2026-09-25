"""Demonstrate a deliberate task failure and a clean recovery.

Runs the daily batch for --as-of three times:

  1. Baseline: no failure switch. Fingerprints every model table.
  2. Failure: NWF_FAIL_TASK=intermediate.int_shipment_lane_costs. The
     intermediate task raises partway through its write (after deleting the
     partition's rows, before inserting), so:
       - the mart task downstream of it is skipped,
       - the batch stops at that partition,
       - the rollback leaves every table exactly as the baseline left it.
  3. Recovery: switch unset, same batch again. Every task succeeds and every
     table matches the baseline.

Each claim is checked, and the script exits non-zero if any check fails. The
run log rows for all three batches are printed at the end.

The same thing by hand, in PowerShell:
  $env:NWF_FAIL_TASK = "intermediate.int_shipment_lane_costs"
  python run_models.py run --date 2026-07-06          # fails, mart skipped, exit 1
  Remove-Item Env:NWF_FAIL_TASK
  python run_models.py run --date 2026-07-06          # recovers
  python run_models.py log

Examples:
  python demo_failure_recovery.py                    # in-memory database
  python demo_failure_recovery.py --db warehouse.duckdb
"""

import argparse
import logging
import os
from datetime import date
from pathlib import Path

import duckdb

from dag import TaskState
from run_log import TABLE as RUN_LOG_TABLE
from run_log import RunLog
from run_models import DEFAULT_LOOKBACK, FAIL_TASK_ENV, ROOT, InjectedFailure, run_batch
from verify_idempotency import compare, snapshot

FAIL_TASK = "intermediate.int_shipment_lane_costs"
MART_TASK = "marts.mart_daily_lane_margin"


class Demo:
    def __init__(self, con, models_dir, data_dir, as_of, lookback):
        self.con, self.models_dir, self.data_dir = con, models_dir, data_dir
        self.as_of, self.lookback = as_of, lookback
        self.failures = 0
        self.batch_ids = []

    def check(self, ok, message):
        print(f"  [{'PASS' if ok else 'FAIL'}] {message}")
        self.failures += not ok

    def batch(self, fail_task=None):
        run_log = RunLog(self.con, "run")
        self.batch_ids.append(run_log.batch_id)
        previous = os.environ.pop(FAIL_TASK_ENV, None)
        if fail_task:
            os.environ[FAIL_TASK_ENV] = fail_task
        try:
            return run_batch(self.con, self.models_dir, self.data_dir, self.as_of, self.lookback, run_log)
        finally:
            os.environ.pop(FAIL_TASK_ENV, None)
            if previous is not None:
                os.environ[FAIL_TASK_ENV] = previous

    def partition_rows(self, table, column, run_date):
        return self.con.execute(f"select count(*) from {table} where {column} = ?", [run_date]).fetchone()[0]

    def run(self):
        partitions = self.lookback + 1
        print(f"Daily batch as_of {self.as_of}, lookback {self.lookback} ({partitions} partitions)\n")

        print("1. Baseline run, no failure switch")
        runs = self.batch()
        self.check(all(r.succeeded for _, r in runs), f"all {partitions} partitions succeeded")
        baseline = snapshot(self.con)
        first = runs[0][0]
        int_rows = self.partition_rows("intermediate.int_shipment_lane_costs", "pickup_date", first)
        mart_rows = self.partition_rows("marts.mart_daily_lane_margin", "ship_date", first)
        print(f"  partition {first}: {int_rows} intermediate rows, {mart_rows} mart rows\n")

        print(f"2. Failing run, {FAIL_TASK_ENV}={FAIL_TASK}")
        runs = self.batch(fail_task=FAIL_TASK)
        run_date, result = runs[-1]
        states = {n: r.state for n, r in result.results.items()}
        for name in result.order:
            r = result.results[name]
            extra = f" (upstream {r.skipped_because})" if r.skipped_because else ""
            print(f"    {name:<38} {r.state.value.upper()}{extra}")
        self.check(states[FAIL_TASK] is TaskState.FAILED
                   and isinstance(result.results[FAIL_TASK].error, InjectedFailure),
                   f"{FAIL_TASK} raised InjectedFailure")
        self.check(states[MART_TASK] is TaskState.SKIPPED
                   and result.results[MART_TASK].skipped_because == FAIL_TASK,
                   f"{MART_TASK} was skipped because of it")
        self.check(all(s is TaskState.SUCCESS for n, s in states.items() if n.startswith("staging.")),
                   "independent staging tasks still succeeded")
        self.check(len(runs) == 1, f"batch stopped at the first partition ({run_date}); later ones not run")
        unchanged = [t for t, _, _, ok in compare(baseline, snapshot(self.con)) if ok]
        self.check(len(unchanged) == len(baseline),
                   f"all {len(baseline)} tables still match the baseline: the rollback restored the "
                   f"{int_rows} deleted intermediate rows, and the skipped mart kept its {mart_rows}")
        print()

        print("3. Recovery run, switch unset")
        runs = self.batch()
        self.check(all(r.succeeded for _, r in runs), f"all {partitions} partitions succeeded")
        mismatched = [t for t, _, _, ok in compare(baseline, snapshot(self.con)) if not ok]
        self.check(not mismatched, "every table's row count and checksum match the baseline")
        print()

        self.print_run_log()
        return self.failures

    def print_run_log(self):
        print("Run log (ops.run_log), per batch:")
        labels = ["baseline", "failure", "recovery"]
        for label, batch_id in zip(labels, self.batch_ids):
            counts = dict(self.con.execute(
                f"select status, count(*) from {RUN_LOG_TABLE} where batch_id = ? group by 1",
                [batch_id]).fetchall())
            summary = ", ".join(f"{counts[s]} {s}" for s in ("success", "failed", "skipped", "running")
                                if s in counts)
            print(f"  {label:<9} {batch_id}  {summary}")
        rows = self.con.execute(
            f"select run_date, task_name, status, coalesce(error, 'upstream ' || skipped_because) "
            f"from {RUN_LOG_TABLE} where batch_id = ? and status <> 'success' order by log_id",
            [self.batch_ids[1]]).fetchall()
        for run_date, task, status, detail in rows:
            print(f"    {status.upper():<8} run_date {run_date} {task}: {detail}")


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--as-of", type=date.fromisoformat, default=date(2026, 7, 6),
                   help="as_of date of the batch (default 2026-07-06)")
    p.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK,
                   help=f"days of earlier partitions the batch reprocesses (default {DEFAULT_LOOKBACK})")
    p.add_argument("--db", help="DuckDB database file (default: a fresh in-memory database)")
    p.add_argument("--data-dir", type=Path, default=ROOT / "data")
    p.add_argument("--models-dir", type=Path, default=ROOT / "models")
    args = p.parse_args(argv)
    # dag.py logs the injected failure's traceback at ERROR; the demo reports it itself.
    logging.basicConfig(level=logging.CRITICAL)

    with duckdb.connect(args.db or ":memory:") as con:
        failures = Demo(con, args.models_dir, args.data_dir, args.as_of, args.lookback).run()
    if failures:
        raise SystemExit(f"\n{failures} check(s) failed.")
    print("\nAll checks passed: downstream tasks were skipped, nothing was corrupted, and the rerun recovered.")


if __name__ == "__main__":
    main()
