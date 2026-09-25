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
python run_models.py                   # builds every model in dependency order (-v logs each task)
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

Any violation stops the build with nothing run. Models then run in topological order. If a model fails, everything downstream of it is skipped, while independent models still build. Each model commits on its own, so a failed or skipped model keeps the table from its last successful build. The script exits non-zero if any model failed.
