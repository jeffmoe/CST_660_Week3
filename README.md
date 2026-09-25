# CST_660_Week3
Repo for Orchistrated Batch Pipeline

## SQL models

Transformations live in `models/`, one model per `.sql` file. Each file is a single `SELECT`, and `run_models.py` builds it into a local DuckDB database (`warehouse.duckdb`). The folder name becomes the schema and the file name becomes the table.

| Layer | Model | What it does |
|---|---|---|
| staging | `stg_shipments` | Casts types, keeps ZIP leading zeros, drops invalid rows, dedupes on `shipment_id` (latest bill wins) |
| staging | `stg_lanes` | Types the lanes CSV; one row per origin/destination ZIP pair |
| staging | `stg_fuel_surcharge` | Types the fuel surcharge CSV; one row per date |
| intermediate | `int_shipment_lane_costs` | Per shipment: its lane, plus the fuel surcharge rate in effect on the pickup date, applied to cost and revenue |
| marts | `mart_daily_lane_margin` | Revenue, cost and margin per lane per pickup date |

```
pip install -r requirements.txt
python generate_freight_data.py        # writes data/*.csv
python run_models.py --run-date 2026-06-15                          # one day (-v logs each task)
python run_models.py --run-date 2026-06-01 --end-date 2026-08-29    # backfill a range
python verify_idempotency.py --run-date 2026-06-15                  # run a day twice and compare
python -m unittest -v test_models test_dag
```

### Dependencies and the DAG runner

Each model is a task in the DAG runner (`dag.py`), named `layer.model`, e.g. `staging.stg_shipments`. A model declares its upstream models in a header comment:

```sql
-- depends_on: staging.stg_shipments, staging.stg_lanes, staging.stg_fuel_surcharge
-- depends_on: none (reads raw CSV)
```

Before anything runs, `run_models.py` checks that:
- every model has a `depends_on` header,
- the header matches the `layer.model` tables the SQL actually reads (comments are ignored),
- no model reads from a later layer (staging -> intermediate -> marts),
- the graph has no cycles and no unknown models.

Any violation stops the build with nothing run. Models then run in topological order. If a model fails, everything downstream of it is skipped, while independent models still build. The script exits non-zero if any model failed. A backfill stops at the first date with a failure.

### Idempotent runs

Every run is for one `run_date`, and running the same date again leaves the database exactly as the first run did. Models read the date with `getvariable('run_date')` and declare which column holds it:

```sql
-- partition_by: pickup_date
-- partition_by: none (small dimension, fully replaced every run)
```

| Model | Partition column |
|---|---|
| `stg_shipments`, `int_shipment_lane_costs` | `pickup_date` |
| `stg_fuel_surcharge` | `rate_date` |
| `mart_daily_lane_margin` | `ship_date` |
| `stg_lanes` | none: the whole table is replaced |

For each model, in one transaction, the runner runs the SELECT into a temp table and checks that every row belongs to `run_date`. It then deletes that date's existing rows and inserts the new ones. A model that returns other dates fails, because rerunning it would duplicate those rows. If any step fails, the transaction rolls back and the partition keeps its previous rows. Rerunning a past date is also how late-arriving bills for that date get picked up.

### Verifying idempotency

`verify_idempotency.py` builds a date, fingerprints every model table, builds the same date again, fingerprints again, and compares. It exits non-zero on any difference. A fingerprint is the row count plus an order-independent checksum of every row:

```sql
select count(*), md5(coalesce(string_agg(row_md5, '' order by row_md5), ''))
from (select md5(t::varchar) as row_md5 from marts.mart_daily_lane_margin as t)
```

Every table is compared in full, not just the `run_date` partition, so a rerun that touched other dates is caught too. Add `--fresh` to check against an empty in-memory database instead of `warehouse.duckdb`.
