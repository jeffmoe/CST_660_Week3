"""Check that building the same run_date twice leaves every table identical.

Builds --run-date, fingerprints every model table, builds the same date again,
fingerprints again, and compares. A table's fingerprint is its row count plus a
checksum of its contents:

    md5 of the sorted list of per-row md5s, where a row's md5 is
    md5(row::varchar) over all of its columns

Sorting makes the checksum independent of row order, which the database does
not guarantee. Because every row contributes its own hash, a duplicated,
missing or changed row changes the checksum, even when the row count happens
to match.

Every table is compared in full, not just the run_date partition, so the
check also catches a rerun that disturbs other dates' rows.

By default this runs against the real warehouse, so the first build is an
ordinary run for that date. Use --fresh to check against an empty
in-memory database instead.

Examples:
  python verify_idempotency.py --run-date 2026-06-15
  python verify_idempotency.py --run-date 2026-06-15 --fresh
"""

import argparse
import logging
from datetime import date
from pathlib import Path

import duckdb

from run_models import LAYERS, ROOT, build

FINGERPRINT_SQL = """
    select count(*), md5(coalesce(string_agg(row_md5, '' order by row_md5), ''))
    from (select md5(t::varchar) as row_md5 from {table} as t)
"""


def fingerprint(con, table):
    """(row_count, checksum) for a table, independent of row order."""
    return con.execute(FINGERPRINT_SQL.format(table=table)).fetchone()


def snapshot(con):
    """{"schema.table": (row_count, checksum)} for every table in the model layers."""
    placeholders = ", ".join("?" for _ in LAYERS)
    tables = con.execute(
        f"select schema_name, table_name from duckdb_tables() "
        f"where schema_name in ({placeholders}) and not temporary order by 1, 2",
        list(LAYERS)).fetchall()
    return {f"{s}.{t}": fingerprint(con, f'"{s}"."{t}"') for s, t in tables}


def run_twice(con, models_dir, data_dir, run_date):
    """Build run_date twice. Returns (snapshot after run 1, snapshot after run 2)."""
    snapshots = []
    for _ in range(2):
        build(con, models_dir, data_dir, run_date).raise_for_failures()
        snapshots.append(snapshot(con))
    return snapshots


def compare(first, second):
    """[(table, run1, run2, matches)] for every table in either snapshot."""
    missing = (None, None)
    return [(t, first.get(t, missing), second.get(t, missing), first.get(t) == second.get(t))
            for t in sorted(first.keys() | second.keys())]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-date", type=date.fromisoformat, required=True,
                   help="partition date to build twice, YYYY-MM-DD")
    p.add_argument("--db", type=Path, default=ROOT / "warehouse.duckdb",
                   help="DuckDB database file (default ./warehouse.duckdb)")
    p.add_argument("--fresh", action="store_true", help="use an empty in-memory database instead of --db")
    p.add_argument("--data-dir", type=Path, default=ROOT / "data")
    p.add_argument("--models-dir", type=Path, default=ROOT / "models")
    args = p.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

    with duckdb.connect(":memory:" if args.fresh else str(args.db)) as con:
        first, second = run_twice(con, args.models_dir, args.data_dir, args.run_date)

    rows = compare(first, second)
    width = max(len(t) for t, *_ in rows)
    print(f"run_date {args.run_date} built twice into {'an in-memory database' if args.fresh else args.db}\n")
    print(f"{'table':<{width}}  {'rows (1)':>9}  {'rows (2)':>9}  {'checksum (1)':<14}  {'checksum (2)':<14}  match")
    for table, (n1, c1), (n2, c2), ok in rows:
        print(f"{table:<{width}}  {n1 if n1 is not None else '-':>9}  {n2 if n2 is not None else '-':>9}  "
              f"{(c1 or '-')[:12]:<14}  {(c2 or '-')[:12]:<14}  {'yes' if ok else 'NO'}")

    if not all(ok for *_, ok in rows):
        raise SystemExit("\nNOT idempotent: the second run changed the tables marked NO.")
    print(f"\nIdempotent: all {len(rows)} tables have identical row counts and checksums.")


if __name__ == "__main__":
    main()
