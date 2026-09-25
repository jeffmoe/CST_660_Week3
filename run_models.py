"""Build the SQL models in models/ into a local DuckDB database using the DAG runner.

Each .sql file holds one model: a single SELECT statement. The folder is the
schema and the file name is the table, so models/staging/stg_shipments.sql
becomes the table staging.stg_shipments and the DAG task "staging.stg_shipments".

Each model declares its upstream models in a header comment:

    -- depends_on: staging.stg_shipments, staging.stg_lanes
    -- depends_on: none (reads raw CSV)

The runner checks the header against the models the SQL actually reads, so a
missing or stale declaration fails before anything runs. A model may read from
its own layer or earlier ones (staging -> intermediate -> marts), never a later one.
Relative CSV paths in read_csv() resolve against --data-dir.

Execution follows dag.py: models run in topological order, a cycle fails
before anything runs, and if a model fails every model downstream of it is
skipped while independent models still build. Each model commits on its own,
so a failed or skipped model keeps the table from its last successful build.

Example:
  python run_models.py --db warehouse.duckdb --data-dir data
"""

import argparse
import logging
import re
from dataclasses import dataclass
from pathlib import Path

import duckdb

from dag import DAG, DAGError, TaskFailedError, TaskState

LAYERS = ("staging", "intermediate", "marts")
ROOT = Path(__file__).resolve().parent

DEPENDS_ON = re.compile(r"^--\s*depends_on:(.*)$", re.MULTILINE | re.IGNORECASE)
LINE_COMMENT = re.compile(r"--[^\n]*")
MODEL_REF = re.compile(r"\b(" + "|".join(LAYERS) + r")\s*\.\s*(\w+)\b", re.IGNORECASE)


class ModelError(DAGError):
    """A model file is invalid: bad dependency header, or it reads a later layer."""


@dataclass
class Model:
    layer: str
    name: str
    path: Path
    sql: str
    depends_on: tuple

    @property
    def key(self):
        return f"{self.layer}.{self.name}"


def parse_depends_on(path, sql):
    headers = DEPENDS_ON.findall(sql)
    if not headers:
        raise ModelError(f"{path.name}: missing '-- depends_on:' header "
                         "(use '-- depends_on: none' for models that only read CSVs)")
    deps = []
    for value in headers:
        value = re.sub(r"\(.*?\)", "", value).strip()  # allow notes like "(reads raw CSV)"
        if value.lower() != "none":
            deps += [d.strip().lower() for d in value.split(",") if d.strip()]
    return tuple(dict.fromkeys(deps))


def referenced_models(sql):
    body = LINE_COMMENT.sub("", sql)
    return {f"{layer.lower()}.{name.lower()}" for layer, name in MODEL_REF.findall(body)}


def load_models(models_dir):
    """Read and validate every model under models_dir."""
    models = []
    for layer in LAYERS:
        for path in sorted((Path(models_dir) / layer).glob("*.sql")):
            sql = path.read_text(encoding="utf-8").strip().rstrip(";")
            models.append(Model(layer, path.stem.lower(), path, sql, parse_depends_on(path, sql)))
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


def build_dag(con, models):
    """One DAG task per model. Unknown upstreams and cycles are reported by the DAG."""
    dag = DAG("sql_models")
    for m in models:
        def run_model(m=m):
            table = f'"{m.layer}"."{m.name}"'
            con.execute(f"create or replace table {table} as\n{m.sql}\n")
            return con.execute(f"select count(*) from {table}").fetchone()[0]
        dag.add_task(m.key, run_model, m.depends_on)
    return dag


def build(con, models_dir, data_dir):
    """Build every model on an open connection and return the DAGRunResult.

    Invalid model files raise ModelError and cycles raise CycleError, in both
    cases before any model runs. Model failures do not raise: check the result,
    or call result.raise_for_failures().
    """
    models = load_models(models_dir)
    dag = build_dag(con, models)
    dag.topological_order()  # fail on cycles/unknown models before touching the database

    data_dir = str(Path(data_dir).resolve()).replace("'", "''")
    con.execute(f"set file_search_path = '{data_dir}'")
    for layer in LAYERS:
        con.execute(f'create schema if not exists "{layer}"')
    return dag.run()


def print_summary(result):
    width = max(len(n) for n in result.order)
    for name in result.order:
        r = result.results[name]
        if r.state is TaskState.SUCCESS:
            detail = f"{r.output:>8,} rows  {r.duration_s:6.2f}s"
        elif r.state is TaskState.FAILED:
            detail = f"{type(r.error).__name__}: {str(r.error).splitlines()[0]}"
        else:
            detail = f"upstream {r.skipped_because} did not succeed"
        print(f"{name:<{width}}  {r.state.value.upper():<7}  {detail}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", type=Path, default=ROOT / "warehouse.duckdb",
                   help="DuckDB database file (default ./warehouse.duckdb)")
    p.add_argument("--data-dir", type=Path, default=ROOT / "data",
                   help="directory holding the raw CSVs (default ./data)")
    p.add_argument("--models-dir", type=Path, default=ROOT / "models",
                   help="directory holding staging/intermediate/marts (default ./models)")
    p.add_argument("-v", "--verbose", action="store_true", help="log each task as it runs")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(levelname)s %(message)s")

    try:
        with duckdb.connect(str(args.db)) as con:
            result = build(con, args.models_dir, args.data_dir)
    except DAGError as exc:
        raise SystemExit(f"Invalid model graph, nothing was built. {exc}")

    print_summary(result)
    try:
        result.raise_for_failures()
    except TaskFailedError as exc:
        raise SystemExit(f"Build incomplete. {exc}")
    print(f"Built {len(result.order)} models into {args.db.resolve()}")


if __name__ == "__main__":
    main()
