"""Run log: one row per task execution, stored in the DuckDB table ops.run_log.

A task's row is inserted with status 'running' before the task starts and is
committed on its own. It is updated to 'success' or 'failed' when the task
ends, so a row still marked 'running' means the process died mid-task.
Tasks skipped because an upstream failed get a row with status 'skipped'.

Timestamps are UTC.
"""

import uuid
from datetime import datetime, timezone

SCHEMA = "ops"
TABLE = f"{SCHEMA}.run_log"

DDL = f"""
create schema if not exists {SCHEMA};
create sequence if not exists {SCHEMA}.run_log_seq;
create table if not exists {TABLE} (
    log_id           bigint default nextval('{SCHEMA}.run_log_seq'),
    batch_id         varchar   not null,  -- one per command invocation
    command          varchar   not null,  -- run, backfill, or build (direct build() calls)
    as_of_date       date,                -- bills received after this date were not visible; null = all
    run_date         date      not null,  -- partition being written
    task_name        varchar   not null,
    status           varchar   not null,  -- running, success, failed, skipped
    started_at       timestamp,
    ended_at         timestamp,
    duration_s       double,
    rows_deleted     bigint,
    rows_inserted    bigint,
    error            varchar,
    skipped_because  varchar
);
"""


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


class RunLog:
    def __init__(self, con, command="build", batch_id=None):
        self.con = con
        self.command = command
        self.batch_id = batch_id or str(uuid.uuid4())
        self._table_ready = False

    def ensure_table(self):
        if not self._table_ready:
            self.con.execute(DDL)
            self._table_ready = True

    def start(self, task_name, run_date, as_of):
        return self.con.execute(
            f"insert into {TABLE} (batch_id, command, as_of_date, run_date, task_name, status, started_at) "
            f"values (?, ?, ?, ?, ?, 'running', ?) returning log_id",
            [self.batch_id, self.command, as_of, run_date, task_name, utc_now()]).fetchone()[0]

    def finish(self, log_id, stats=None, error=None):
        """Mark a started task 'success' (with its WriteStats) or 'failed' (with its exception)."""
        ended = utc_now()
        self.con.execute(
            f"update {TABLE} set status = ?, ended_at = ?, duration_s = epoch(?::timestamp - started_at), "
            f"rows_deleted = ?, rows_inserted = ?, error = ? where log_id = ?",
            [
                "failed" if error is not None else "success",
                ended,
                ended,
                stats.deleted if stats else None,
                stats.inserted if stats else None,
                f"{type(error).__name__}: {error}" if error is not None else None,
                log_id,
            ])

    def skipped(self, task_name, run_date, as_of, skipped_because):
        now = utc_now()
        self.con.execute(
            f"insert into {TABLE} (batch_id, command, as_of_date, run_date, task_name, status, "
            f"started_at, ended_at, duration_s, skipped_because) "
            f"values (?, ?, ?, ?, ?, 'skipped', ?, ?, 0, ?)",
            [self.batch_id, self.command, as_of, run_date, task_name, now, now, skipped_because])
