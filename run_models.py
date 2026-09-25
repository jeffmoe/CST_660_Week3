"""Build the SQL models in models/ into a local DuckDB database using the DAG runner.

Each .sql file holds one model: a single SELECT statement. The folder is the
schema and the file name is the table, so models/staging/stg_shipments.sql
becomes the table staging.stg_shipments and the DAG task "staging.stg_shipments".

Every run is for one run_date, and every model is idempotent for that date:
running the same date again leaves the database exactly as the first run did.
Models read the date with getvariable('run_date'). Each model declares two
headers:

    -- depends_on: staging.stg_shipments, staging.stg_lanes
    -- depends_on: none (reads raw CSV)
    -- partition_by: pickup_date
    -- partition_by: none (small dimension, fully replaced every run)

For each model the runner, in one transaction:
  1. runs the SELECT into a temp table,
  2. checks that every row's partition_by column equals run_date (a model that
     returned other dates would duplicate them on every rerun, so it fails),
  3. deletes that run_date's rows from the target (all rows when partition_by
     is none), then
  4. inserts the new rows.
If any step fails, the transaction rolls back and the partition is unchanged.

The runner checks depends_on against the models the SQL actually reads, so a
missing or stale declaration fails before anything runs. A model may read from
its own layer or earlier ones (staging -> intermediate -> marts), never a later one.
Relative CSV paths in read_csv() resolve against --data-dir.

Execution follows dag.py: models run in topological order, a cycle fails
before anything runs, and if a model fails every model downstream of it is
skipped while independent models still build.

Late-arriving bills. Partitions are keyed on pickup date, but a shipment's
bill arrives days later. Each batch has an as_of date and only sees bills
received on or before it (getvariable('as_of')). The daily batch for as_of D
rebuilds partitions D-lookback through D, oldest first, so a bill that arrives
on D corrects its pickup date's partition as long as that date is within the
lookback window. A bill that arrives later than that is not picked up.

Commands:
  run       the daily batch for one as_of date
  backfill  replay the daily batch for every as_of date from --start to --end, in order
  log       summarize a batch from the run log

Every task's start, end, rows deleted and inserted, status, and any error is
written to the DuckDB table ops.run_log (see run_log.py).

Deliberate failures: set NWF_FAIL_TASK to one or more task names (comma
separated) and those tasks raise InjectedFailure partway through their write,
after the delete and before the insert. Their downstream tasks are skipped,
and the transaction rollback leaves the partition as it was. Unset the
variable and rerun to recover. See demo_failure_recovery.py.

Examples:
  python run_models.py run --date 2026-06-15
  python run_models.py backfill --start 2026-06-01 --end 2026-09-15 --lookback 21
  python run_models.py log
"""

import argparse
import logging
import os
import re
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import duckdb

from dag import DAG, DAGError, TaskState
from run_log import SCHEMA as RUN_LOG_SCHEMA
from run_log import TABLE as RUN_LOG_TABLE
from run_log import RunLog

LAYERS = ("staging", "intermediate", "marts")
ROOT = Path(__file__).resolve().parent
# Bills in this feed arrive 1-20 days after pickup, so 21 days of lookback
# lets every one of them land in its pickup-date partition.
DEFAULT_LOOKBACK = 21

DEPENDS_ON = re.compile(r"^--\s*depends_on:(.*)$", re.MULTILINE | re.IGNORECASE)
PARTITION_BY = re.compile(r"^--\s*partition_by:(.*)$", re.MULTILINE | re.IGNORECASE)
LINE_COMMENT = re.compile(r"--[^\n]*")
MODEL_REF = re.compile(r"\b(" + "|".join(LAYERS) + r")\s*\.\s*(\w+)\b", re.IGNORECASE)
IDENTIFIER = re.compile(r"^[A-Za-z_]\w*$")

# Deliberate failure switch for demos and tests: a comma-separated list of task
# names, e.g. NWF_FAIL_TASK=intermediate.int_shipment_lane_costs
FAIL_TASK_ENV = "NWF_FAIL_TASK"
log = logging.getLogger(__name__)


class ModelError(DAGError):
    """A model file is invalid: bad header, or it reads a later layer."""


class PartitionError(Exception):
    """A model returned rows outside the run_date partition it writes."""


class InjectedFailure(Exception):
    """A deliberate failure requested with the NWF_FAIL_TASK environment variable."""


@dataclass
class Model:
    layer: str
    name: str
    path: Path
    sql: str
    depends_on: tuple
    partition_by: str  # column name, or None for a full replace

    @property
    def key(self):
        return f"{self.layer}.{self.name}"

    @property
    def table(self):
        return f'"{self.layer}"."{self.name}"'


@dataclass
class WriteStats:
    deleted: int
    inserted: int


def header_value(pattern, path, sql, name, hint):
    """Return the header's value with any "(note)" removed, or None for "none"."""
    headers = pattern.findall(sql)
    if not headers:
        raise ModelError(f"{path.name}: missing '-- {name}:' header ({hint})")
    if len(headers) > 1:
        raise ModelError(f"{path.name}: more than one '-- {name}:' header")
    value = re.sub(r"\(.*?\)", "", headers[0]).strip()
    return None if value.lower() == "none" else value


def parse_depends_on(path, sql):
    value = header_value(DEPENDS_ON, path, sql, "depends_on",
                         "use '-- depends_on: none' for models that only read CSVs")
    if value is None:
        return ()
    return tuple(dict.fromkeys(d.strip().lower() for d in value.split(",") if d.strip()))


def parse_partition_by(path, sql):
    value = header_value(PARTITION_BY, path, sql, "partition_by",
                         "use '-- partition_by: none' to replace the whole table every run")
    if value is not None and not IDENTIFIER.match(value):
        raise ModelError(f"{path.name}: partition_by must be a single column name, got {value!r}")
    return value


def referenced_models(sql):
    body = LINE_COMMENT.sub("", sql)
    return {f"{layer.lower()}.{name.lower()}" for layer, name in MODEL_REF.findall(body)}


def load_models(models_dir):
    """Read and validate every model under models_dir."""
    models = []
    for layer in LAYERS:
        for path in sorted((Path(models_dir) / layer).glob("*.sql")):
            sql = path.read_text(encoding="utf-8").strip().rstrip(";")
            models.append(Model(layer, path.stem.lower(), path, sql,
                                parse_depends_on(path, sql), parse_partition_by(path, sql)))
    if not models:
        raise ModelError(f"No models found under {Path(models_dir).resolve()}")

    for m in models:
        where = f"{m.layer}/{m.path.name}"
        declared, used = set(m.depends_on), referenced_models(m.sql)
        if used - declared:
            raise ModelError(f"{where} reads {', '.join(sorted(used - declared))} "
                             "but does not declare it in depends_on")
        if declared - used:
            raise ModelError(f"{where} declares {', '.join(sorted(declared - used))} "
                             "in depends_on but never reads it")
        for dep in m.depends_on:
            if LAYERS.index(dep.split(".")[0]) > LAYERS.index(m.layer):
                raise ModelError(f"{where} reads {dep}, which is in a later layer")
    return models


def injected_failures(models):
    """Task names listed in NWF_FAIL_TASK. Unknown names raise ModelError, so a typo can't
    quietly turn the switch off."""
    names = {n.strip().lower() for n in os.environ.get(FAIL_TASK_ENV, "").split(",") if n.strip()}
    unknown = names - {m.key for m in models}
    if unknown:
        raise ModelError(f"{FAIL_TASK_ENV} names unknown task(s): {', '.join(sorted(unknown))}. "
                         f"Tasks: {', '.join(m.key for m in models)}")
    return frozenset(names)


def write_partition(con, m, inject_failure=False):
    """Replace model m's run_date partition in one transaction. Returns WriteStats.

    With inject_failure, raise InjectedFailure after the delete and before the
    insert, so the rollback that restores the partition is exercised too.
    """
    con.execute("begin transaction")
    try:
        con.execute(f"create or replace temp table _model_output as\n{m.sql}\n")
        columns = [d[0] for d in con.execute("select * from _model_output limit 0").description]

        if m.partition_by:
            if m.partition_by not in columns:
                raise PartitionError(f"{m.key}: partition_by column {m.partition_by!r} "
                                     f"is not in the model's output")
            outside = con.execute(
                f'select count(*) from _model_output '
                f'where "{m.partition_by}" is distinct from getvariable(\'run_date\')').fetchone()[0]
            if outside:
                raise PartitionError(f"{m.key}: {outside} rows have {m.partition_by} other than "
                                     f"run_date; filter the model on getvariable('run_date')")
            delete = f'delete from {m.table} where "{m.partition_by}" = getvariable(\'run_date\')'
        else:
            delete = f"delete from {m.table}"

        con.execute(f"create table if not exists {m.table} as select * from _model_output limit 0")
        deleted = con.execute(delete).fetchone()[0]
        if inject_failure:
            raise InjectedFailure(f"deliberate failure because {FAIL_TASK_ENV} includes {m.key}; "
                                  f"the {deleted} rows just deleted are rolled back")
        inserted = con.execute(f"insert into {m.table} by name select * from _model_output").fetchone()[0]
        con.execute("drop table _model_output")
        con.execute("commit")
    except BaseException:
        con.execute("rollback")
        raise
    return WriteStats(deleted, inserted)


def build_dag(con, models, run_date, as_of, run_log, fail_tasks=frozenset()):
    """One DAG task per model. Each task logs its start and end to the run log.
    Tasks named in fail_tasks raise InjectedFailure mid-write."""
    dag = DAG("sql_models")
    for m in models:
        def task(m=m):
            log_id = run_log.start(m.key, run_date, as_of)
            try:
                stats = write_partition(con, m, inject_failure=m.key in fail_tasks)
            except BaseException as exc:
                run_log.finish(log_id, error=exc)
                raise
            run_log.finish(log_id, stats)
            return stats
        dag.add_task(m.key, task, m.depends_on)
    return dag


def build(con, models_dir, data_dir, run_date, as_of=None, run_log=None):
    """Build every model's run_date partition and return the DAGRunResult.

    Only bills received on or before as_of are visible; None makes every bill
    visible. Every task is recorded in ops.run_log. Pass a RunLog to group
    several builds under one batch_id.

    Invalid model files raise ModelError and cycles raise CycleError, in both
    cases before any model runs. Model failures do not raise: check the result,
    or call result.raise_for_failures().
    """
    run_log = run_log or RunLog(con, "build")
    models = load_models(models_dir)
    fail_tasks = injected_failures(models)
    if fail_tasks:
        log.warning("%s is set: %s will fail on purpose", FAIL_TASK_ENV, ", ".join(sorted(fail_tasks)))
    dag = build_dag(con, models, run_date, as_of, run_log, fail_tasks)
    dag.topological_order()  # fail on cycles/unknown models before touching the database

    data_dir = str(Path(data_dir).resolve()).replace("'", "''")
    con.execute(f"set file_search_path = '{data_dir}'")
    con.execute(f"set variable run_date = date '{run_date.isoformat()}'")
    if as_of is None:
        con.execute("set variable as_of = null::date")
    else:
        con.execute(f"set variable as_of = date '{as_of.isoformat()}'")
    for layer in LAYERS:
        con.execute(f'create schema if not exists "{layer}"')
    run_log.ensure_table()

    result = dag.run()
    for name in result.order:
        r = result.results[name]
        if r.state is TaskState.SKIPPED:
            run_log.skipped(name, run_date, as_of, r.skipped_because)
    return result


def lookback_partitions(as_of, lookback):
    """The run_dates a batch as of as_of reprocesses, oldest first."""
    return [as_of - timedelta(days=k) for k in range(lookback, -1, -1)]


def run_batch(con, models_dir, data_dir, as_of, lookback=DEFAULT_LOOKBACK, run_log=None):
    """The daily batch for as_of: rebuild each partition in the lookback window, oldest first.

    Every partition sees the bills received by as_of, so a bill that arrived
    that day corrects its pickup date's partition as long as that date is
    within `lookback` days. Stops at the first partition with a failure.
    Returns [(run_date, DAGRunResult), ...].
    """
    run_log = run_log or RunLog(con, "run")
    runs = []
    for run_date in lookback_partitions(as_of, lookback):
        result = build(con, models_dir, data_dir, run_date, as_of, run_log)
        runs.append((run_date, result))
        if not result.succeeded:
            break
    return runs


def backfill(con, models_dir, data_dir, start, end, lookback=DEFAULT_LOOKBACK, run_log=None):
    """Replay the daily batch for every as_of date from start to end, in order.

    Stops after the first batch with a failure.
    Returns [(as_of, [(run_date, DAGRunResult), ...]), ...].
    """
    if end < start:
        raise ValueError("end must not be before start")
    run_log = run_log or RunLog(con, "backfill")
    batches = []
    as_of = start
    while as_of <= end:
        runs = run_batch(con, models_dir, data_dir, as_of, lookback, run_log)
        batches.append((as_of, runs))
        if not batch_succeeded(runs):
            break
        as_of += timedelta(days=1)
    return batches


def batch_succeeded(runs):
    return all(result.succeeded for _, result in runs)


def print_summary(result):
    width = max(len(n) for n in result.order)
    for name in result.order:
        r = result.results[name]
        if r.state is TaskState.SUCCESS:
            detail = (f"deleted {r.output.deleted:>6,}  inserted {r.output.inserted:>6,}  "
                      f"{r.duration_s:5.2f}s")
        elif r.state is TaskState.FAILED:
            detail = f"{type(r.error).__name__}: {str(r.error).splitlines()[0]}"
        else:
            detail = f"upstream {r.skipped_because} did not succeed"
        print(f"    {name:<{width}}  {r.state.value.upper():<7}  {detail}")


def print_batch(as_of, runs):
    tasks = sum(len(result.order) for _, result in runs)
    inserted = sum(r.output.inserted for _, result in runs
                   for r in result.results.values() if r.state is TaskState.SUCCESS)
    print(f"as_of {as_of}  partitions {runs[0][0]}..{runs[-1][0]}  "
          f"{tasks} tasks  {inserted:>7,} rows inserted")
    for run_date, result in runs:
        if not result.succeeded:
            print(f"  run_date {run_date} FAILED:")
            print_summary(result)


def print_log(con, batch_id):
    """Summarize one batch from the run log: counts by status, then any non-success rows."""
    if not con.execute("select count(*) from duckdb_tables() where schema_name = ? and table_name = 'run_log'",
                       [RUN_LOG_SCHEMA]).fetchone()[0]:
        raise SystemExit("No run log yet: run or backfill first.")
    if batch_id == "latest":
        row = con.execute(f"select batch_id from {RUN_LOG_TABLE} order by log_id desc limit 1").fetchone()
        if not row:
            raise SystemExit("The run log is empty.")
        batch_id = row[0]
    head = con.execute(f"""
        select any_value(command), min(as_of_date), max(as_of_date), min(run_date), max(run_date),
               min(started_at), max(ended_at), count(*),
               count(*) filter (status = 'success'), count(*) filter (status = 'failed'),
               count(*) filter (status = 'skipped'), count(*) filter (status = 'running'),
               coalesce(sum(rows_inserted), 0)
        from {RUN_LOG_TABLE} where batch_id = ?""", [batch_id]).fetchone()
    if not head[7]:
        raise SystemExit(f"No run log rows for batch {batch_id}.")
    (command, as_of_min, as_of_max, rd_min, rd_max, started, ended, total,
     ok, failed, skipped, running, inserted) = head
    print(f"batch    {batch_id}  ({command})")
    print(f"as_of    {as_of_min}..{as_of_max}    run_dates {rd_min}..{rd_max}")
    print(f"time     {started} -> {ended} UTC")
    print(f"tasks    {total:,} total: {ok:,} success, {failed} failed, {skipped} skipped, {running} running")
    print(f"rows     {inserted:,} inserted")
    problems = con.execute(f"""
        select as_of_date, run_date, task_name, status,
               coalesce(error, 'upstream ' || skipped_because, '')
        from {RUN_LOG_TABLE} where batch_id = ? and status <> 'success' order by log_id limit 20""",
        [batch_id]).fetchall()
    for as_of, run_date, task, status, detail in problems:
        print(f"  {status.upper():<8} as_of {as_of} run_date {run_date} {task}: {detail.splitlines()[0]}")


def main(argv=None):
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", type=Path, default=ROOT / "warehouse.duckdb",
                        help="DuckDB database file (default ./warehouse.duckdb)")
    common.add_argument("--data-dir", type=Path, default=ROOT / "data",
                        help="directory holding the raw CSVs (default ./data)")
    common.add_argument("--models-dir", type=Path, default=ROOT / "models",
                        help="directory holding staging/intermediate/marts (default ./models)")
    common.add_argument("-v", "--verbose", action="store_true", help="log each task as it runs")

    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)
    run_p = sub.add_parser("run", parents=[common], help="daily batch for one as_of date")
    run_p.add_argument("--date", type=date.fromisoformat, required=True,
                       help="as_of date of the batch, YYYY-MM-DD")
    bf_p = sub.add_parser("backfill", parents=[common], help="replay the daily batch for a date range")
    bf_p.add_argument("--start", type=date.fromisoformat, required=True, help="first as_of date, YYYY-MM-DD")
    bf_p.add_argument("--end", type=date.fromisoformat, required=True, help="last as_of date, YYYY-MM-DD")
    for sp in (run_p, bf_p):
        sp.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK,
                        help=f"days of earlier partitions each batch reprocesses (default {DEFAULT_LOOKBACK})")
    log_p = sub.add_parser("log", parents=[common], help="summarize a batch from the run log")
    log_p.add_argument("--batch", default="latest", help="batch_id to show (default: the latest)")

    args = p.parse_args(argv)
    if getattr(args, "lookback", 0) < 0:
        p.error("--lookback must be 0 or more")
    if args.command == "backfill" and args.end < args.start:
        p.error("--end must not be before --start")
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(levelname)s %(message)s")

    with duckdb.connect(str(args.db)) as con:
        if args.command == "log":
            print_log(con, args.batch)
            return
        run_log = RunLog(con, args.command)
        try:
            if args.command == "run":
                batches = [(args.date, run_batch(con, args.models_dir, args.data_dir,
                                                 args.date, args.lookback, run_log))]
            else:
                batches = backfill(con, args.models_dir, args.data_dir,
                                   args.start, args.end, args.lookback, run_log)
        except DAGError as exc:
            raise SystemExit(f"Invalid model graph, nothing was built. {exc}")

    for as_of, runs in batches:
        print_batch(as_of, runs)
    print(f"\nbatch_id {run_log.batch_id}  (details: python run_models.py log --batch {run_log.batch_id})")
    if not batch_succeeded(batches[-1][1]):
        raise SystemExit(f"Batch as_of {batches[-1][0]} failed; later batches were not run.")
    print(f"Ran {len(batches)} batch(es) into {args.db.resolve()}")


if __name__ == "__main__":
    main()
