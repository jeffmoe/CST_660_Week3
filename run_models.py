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

Examples:
  python run_models.py --run-date 2026-06-15
  python run_models.py --run-date 2026-06-01 --end-date 2026-08-29   # backfill
"""

import argparse
import logging
import re
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import duckdb

from dag import DAG, DAGError, TaskFailedError, TaskState

LAYERS = ("staging", "intermediate", "marts")
ROOT = Path(__file__).resolve().parent

DEPENDS_ON = re.compile(r"^--\s*depends_on:(.*)$", re.MULTILINE | re.IGNORECASE)
PARTITION_BY = re.compile(r"^--\s*partition_by:(.*)$", re.MULTILINE | re.IGNORECASE)
LINE_COMMENT = re.compile(r"--[^\n]*")
MODEL_REF = re.compile(r"\b(" + "|".join(LAYERS) + r")\s*\.\s*(\w+)\b", re.IGNORECASE)
IDENTIFIER = re.compile(r"^[A-Za-z_]\w*$")


class ModelError(DAGError):
    """A model file is invalid: bad header, or it reads a later layer."""


class PartitionError(Exception):
    """A model returned rows outside the run_date partition it writes."""


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


def write_partition(con, m):
    """Replace model m's run_date partition in one transaction. Returns WriteStats."""
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
        inserted = con.execute(f"insert into {m.table} by name select * from _model_output").fetchone()[0]
        con.execute("drop table _model_output")
        con.execute("commit")
    except BaseException:
        con.execute("rollback")
        raise
    return WriteStats(deleted, inserted)


def build_dag(con, models):
    """One DAG task per model. Unknown upstreams and cycles are reported by the DAG."""
    dag = DAG("sql_models")
    for m in models:
        dag.add_task(m.key, lambda m=m: write_partition(con, m), m.depends_on)
    return dag


def build(con, models_dir, data_dir, run_date):
    """Build every model's run_date partition and return the DAGRunResult.

    Invalid model files raise ModelError and cycles raise CycleError, in both
    cases before any model runs. Model failures do not raise: check the result,
    or call result.raise_for_failures().
    """
    models = load_models(models_dir)
    dag = build_dag(con, models)
    dag.topological_order()  # fail on cycles/unknown models before touching the database

    data_dir = str(Path(data_dir).resolve()).replace("'", "''")
    con.execute(f"set file_search_path = '{data_dir}'")
    con.execute(f"set variable run_date = date '{run_date.isoformat()}'")
    for layer in LAYERS:
        con.execute(f'create schema if not exists "{layer}"')
    return dag.run()


def build_range(con, models_dir, data_dir, start, end):
    """Build each date from start to end inclusive, stopping after the first date with a failure.

    Returns [(run_date, DAGRunResult), ...].
    """
    runs = []
    d = start
    while d <= end:
        result = build(con, models_dir, data_dir, d)
        runs.append((d, result))
        if not result.succeeded:
            break
        d += timedelta(days=1)
    return runs


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
        print(f"  {name:<{width}}  {r.state.value.upper():<7}  {detail}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-date", type=date.fromisoformat, required=True,
                   help="partition date to build, YYYY-MM-DD")
    p.add_argument("--end-date", type=date.fromisoformat,
                   help="also build every date after --run-date up to this one (backfill)")
    p.add_argument("--db", type=Path, default=ROOT / "warehouse.duckdb",
                   help="DuckDB database file (default ./warehouse.duckdb)")
    p.add_argument("--data-dir", type=Path, default=ROOT / "data",
                   help="directory holding the raw CSVs (default ./data)")
    p.add_argument("--models-dir", type=Path, default=ROOT / "models",
                   help="directory holding staging/intermediate/marts (default ./models)")
    p.add_argument("-v", "--verbose", action="store_true", help="log each task as it runs")
    args = p.parse_args()
    end = args.end_date or args.run_date
    if end < args.run_date:
        p.error("--end-date must not be before --run-date")
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(levelname)s %(message)s")

    try:
        with duckdb.connect(str(args.db)) as con:
            runs = build_range(con, args.models_dir, args.data_dir, args.run_date, end)
    except DAGError as exc:
        raise SystemExit(f"Invalid model graph, nothing was built. {exc}")

    for run_date, result in runs:
        if len(runs) == 1 or not result.succeeded:
            print(f"run_date {run_date}")
            print_summary(result)
        else:
            inserted = sum(r.output.inserted for r in result.results.values())
            print(f"run_date {run_date}  {len(result.order)} models  {inserted:>6,} rows inserted")
    try:
        runs[-1][1].raise_for_failures()
    except TaskFailedError as exc:
        raise SystemExit(f"Build for {runs[-1][0]} incomplete; later dates were not run. {exc}")
    print(f"Built {len(runs)} run date(s) into {args.db.resolve()}")


if __name__ == "__main__":
    main()
