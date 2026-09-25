# CST_660_Week3
Repo for Orchistrated Batch Pipeline

## SQL models

Transformations live in `models/`, one model per `.sql` file. Each file is a single `SELECT`, and `run_models.py` builds it into a local DuckDB database (`warehouse.duckdb`). The folder name becomes the schema and the file name becomes the table.

| Layer | Model | What it does |
|---|---|---|
| staging | `stg_shipments` | Casts types and keeps ZIP leading zeros. Keeps the newest version of each `shipment_id` (latest `bill_received_date`, then latest line in the feed), then drops it if invalid |
| staging | `stg_lanes` | Types the lanes CSV; one row per origin/destination ZIP pair |
| staging | `stg_fuel_surcharge` | Types the fuel surcharge CSV; one row per date |
| intermediate | `int_shipment_lane_costs` | Per shipment: its lane, plus the fuel surcharge rate in effect on the pickup date, applied to cost and revenue |
| marts | `mart_daily_lane_margin` | Revenue, cost and margin per lane per pickup date |

```
pip install -r requirements.txt
python generate_freight_data.py        # writes data/*.csv
python run_models.py run --date 2026-06-15                               # daily batch (-v logs each task)
python run_models.py backfill --start 2026-06-01 --end 2026-09-15         # replay daily batches in order
python run_models.py log                                                 # summarize the latest batch
python verify_idempotency.py --run-date 2026-06-15                       # build a day twice and compare
python demo_failure_recovery.py                                          # fail a task on purpose, then recover
python -m unittest -v test_dag test_models test_backfill test_dedup test_failure_toggle
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

Any violation stops the build with nothing run. Models then run in topological order. If a model fails, everything downstream of it is skipped, while independent models still build. The script exits non-zero if any model failed.

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

For each model, in one transaction, the runner runs the SELECT into a temp table and checks that every row belongs to `run_date`. It then deletes that date's existing rows and inserts the new ones. A model that returns other dates fails, because rerunning it would duplicate those rows. If any step fails, the transaction rolls back and the partition keeps its previous rows.

### Verifying idempotency

`verify_idempotency.py` builds a date, fingerprints every model table, builds the same date again, fingerprints again, and compares. It exits non-zero on any difference. A fingerprint is the row count plus an order-independent checksum of every row:

```sql
select count(*), md5(coalesce(string_agg(row_md5, '' order by row_md5), ''))
from (select md5(t::varchar) as row_md5 from marts.mart_daily_lane_margin as t)
```

Every table is compared in full, not just the `run_date` partition, so a rerun that touched other dates is caught too. Add `--fresh` to check against an empty in-memory database instead of `warehouse.duckdb`.

### Late-arriving bills, lookback and backfill

Partitions are keyed on pickup date, but a shipment's bill arrives 1 to 20 days later. Every batch has an `as_of` date, and `stg_shipments` only sees bills received on or before it (`getvariable('as_of')`), just as they would have been on that day.

- `run --date D --lookback N` is the daily batch. It rebuilds partitions `D-N` through `D`, oldest first, all as of `D`. A bill that arrives on `D` corrects its pickup date's partition if that date is within the window, and a re-sent bill replaces the original the same way. A bill that arrives more than `N` days after pickup is never picked up.
- `backfill --start S --end E --lookback N` replays the daily batch for every `as_of` date from `S` to `E`, in order, and stops at the first batch with a failure.

The default lookback is 21 days, which covers every bill in this feed. A full backfill from 2026-06-01 to 2026-09-15 (the last bill date) runs 11,770 tasks in about 4 minutes. It ends with the same row counts and checksums as building every date with all bills visible.

### Run log

Every task writes a row to `ops.run_log` in the warehouse:

| Column | Meaning |
|---|---|
| `batch_id`, `command` | One id per `run` / `backfill` invocation (`build` for direct `build()` calls) |
| `as_of_date`, `run_date` | The batch's bill cutoff, and the partition being written |
| `task_name` | The model, e.g. `staging.stg_shipments` |
| `status` | `running`, `success`, `failed` or `skipped` |
| `started_at`, `ended_at`, `duration_s` | UTC timestamps |
| `rows_deleted`, `rows_inserted` | Rows replaced in the partition |
| `error`, `skipped_because` | The exception for failures; the upstream task for skips |

A task's row is inserted with status `running` and committed before the task starts, then updated when it ends. That way a failed task's log survives its rolled-back write, and a row still marked `running` means the process died mid-task. `python run_models.py log [--batch ID]` summarizes a batch, or query the table directly:

```sql
select run_date, task_name, status, rows_inserted, duration_s, error
from ops.run_log
where batch_id = (select batch_id from ops.run_log order by log_id desc limit 1)
order by log_id;
```

The run log lives in its own `ops` schema, so the idempotency checksums ignore it.

### Deliberate failures

Set `NWF_FAIL_TASK` to one or more task names (comma-separated) and those tasks raise `InjectedFailure` partway through their write: after the partition's rows are deleted, before the new rows are inserted. Unset it to turn the switch off. A name that isn't a task stops the run before anything executes, so a typo can't silently leave the switch off.

In PowerShell:

```powershell
$env:NWF_FAIL_TASK = "intermediate.int_shipment_lane_costs"
python run_models.py run --date 2026-07-06     # intermediate FAILED, mart SKIPPED, exit code 1
Remove-Item Env:NWF_FAIL_TASK
python run_models.py run --date 2026-07-06     # every task succeeds
python run_models.py log                       # latest batch; use --batch <id> for the failed one
```

What happens:
- the failed task's transaction rolls back, so its partition keeps its previous rows;
- every task downstream of it is skipped and keeps its previous rows;
- independent tasks, like the staging models, still run;
- the batch (or backfill) stops at that partition;
- the failure and the skips are recorded in `ops.run_log`.

Rerunning the same date with the switch off rebuilds everything.

`python demo_failure_recovery.py` runs all of this against an in-memory database: a baseline batch, a failing batch, and a recovery batch. It checks that the mart was skipped, that the failure left every table identical to the baseline, and that the recovery run matches the baseline row for row. Add `--db warehouse.duckdb` to run it against the real warehouse.
