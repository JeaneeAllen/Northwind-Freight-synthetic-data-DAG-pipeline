#!/usr/bin/env python3
"""Plain-Python DAG runner for the Northwind Freight warehouse, backed by DuckDB.

    raw -> staging_shipments \\
        -> staging_lanes     -> intermediate -> mart -> export
        -> staging_fuel      /

Each task's transform lives in its own file under sql/. Execution order is
derived from DEPENDENCIES (not hard-coded), checked for unknown parents and
cycles. A task whose upstream failed or was blocked is itself blocked, never
executed. Every per-date task replaces only its own date partition inside a
transaction; raw and staging_lanes are global snapshots (not date-scoped)
and use a full CREATE OR REPLACE instead. Successful tasks are checkpointed
in DuckDB, keyed by a hash of their SQL, this file's code, the date, the
as-of cutoff, and their upstream checkpoints, so unchanged work is skipped
and changed work is not. A CSV run log records every task attempt.
"""

import argparse
import csv
import hashlib
import json
import re
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent
SQL_DIR = ROOT / "sql"
DATA_DIR = ROOT / "data"
WORK_DIR = ROOT / "work"
EXPORT_DIR = WORK_DIR / "exports"
DB_PATH = WORK_DIR / "warehouse.duckdb"
RUN_LOG_PATH = WORK_DIR / "run_log.csv"

SHIPMENTS_CSV = DATA_DIR / "shipments.csv"
LANES_CSV = DATA_DIR / "lanes.csv"
FUEL_CSV = DATA_DIR / "fuel.csv"

DEPENDENCIES = {
    "raw": [],
    "staging_shipments": ["raw"],
    "staging_lanes": ["raw"],
    "staging_fuel": ["raw"],
    "intermediate": ["staging_shipments", "staging_lanes", "staging_fuel"],
    "mart": ["intermediate"],
    "export": ["mart"],
}

# Not date-partitioned: checkpointed once per --as-of, shared across every
# date in a run or backfill, instead of once per (date, as-of) pair.
GLOBAL_TASKS = {"raw", "staging_lanes"}

TASK_KIND = {
    "raw": "global_multi",
    "staging_shipments": "partition",
    "staging_lanes": "global_single",
    "staging_fuel": "partition",
    "intermediate": "partition",
    "mart": "partition",
    "export": "export",
}

# task -> (table, DELETE predicate) for tasks that replace one date partition.
PARTITION_TABLE = {
    "staging_shipments": ("staging_shipments", "shipment_date = CAST($date AS DATE)"),
    "staging_fuel": ("staging_fuel", "date = CAST($date AS DATE)"),
    "intermediate": ("intermediate", "shipment_date = CAST($date AS DATE)"),
    "mart": ("mart", "shipment_date = CAST($date AS DATE)"),
}

RUN_LOG_COLUMNS = [
    "run_id", "command", "task", "shipment_date", "as_of",
    "scope_key", "outcome", "checkpoint_hash",
    "started_at", "ended_at", "duration_ms", "message",
]


class InjectedFailure(RuntimeError):
    """Raised to simulate a task failure, for testing rollback/blocking/recovery."""


# ---------------------------------------------------------------- DAG shape

def validate_and_order(dependencies):
    """Return a valid execution order, or raise on an unknown parent or a cycle."""
    for task, parents in dependencies.items():
        for parent in parents:
            if parent not in dependencies:
                raise ValueError(f"task '{task}' depends on unknown task '{parent}'")

    order = []
    state = {}  # task -> 0 unvisited, 1 in progress, 2 done

    def visit(task, stack):
        current = state.get(task, 0)
        if current == 2:
            return
        if current == 1:
            cycle = " -> ".join(stack + [task])
            raise ValueError(f"circular dependency detected: {cycle}")
        state[task] = 1
        for parent in dependencies[task]:
            visit(parent, stack + [task])
        state[task] = 2
        order.append(task)

    for task in dependencies:
        visit(task, [])
    return order


# ------------------------------------------------------------------ hashing

def sha256_file(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sha256_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def scope_key_for(task, target_date, as_of):
    if task in GLOBAL_TASKS:
        return f"asof={as_of.isoformat()}"
    return f"date={target_date.isoformat()}|asof={as_of.isoformat()}"


def checkpoint_hash_for(task, target_date, as_of, upstream_hashes, code_hash):
    payload = {
        "task": task,
        "code_hash": code_hash,
        "sql_hash": sha256_file(SQL_DIR / f"{task}.sql"),
        "date": None if task in GLOBAL_TASKS else target_date.isoformat(),
        "as_of": as_of.isoformat(),
        "upstream": {parent: upstream_hashes[parent] for parent in DEPENDENCIES[task]},
    }
    if task == "raw":
        payload["source_hash"] = "|".join(
            sha256_file(p) for p in (SHIPMENTS_CSV, LANES_CSV, FUEL_CSV)
        )
    return sha256_text(json.dumps(payload, sort_keys=True))


# --------------------------------------------------------------- checkpoints

def get_checkpoint(conn, task, scope_key):
    row = conn.execute(
        "SELECT checkpoint_hash, status FROM pipeline_checkpoints "
        "WHERE task = $task AND scope_key = $scope_key",
        {"task": task, "scope_key": scope_key},
    ).fetchone()
    return None if row is None else {"checkpoint_hash": row[0], "status": row[1]}


def upsert_checkpoint(conn, task, scope_key, checkpoint_hash, status):
    conn.execute(
        """
        INSERT INTO pipeline_checkpoints (task, scope_key, checkpoint_hash, status, updated_at)
        VALUES ($task, $scope_key, $checkpoint_hash, $status, $updated_at)
        ON CONFLICT (task, scope_key) DO UPDATE SET
            checkpoint_hash = EXCLUDED.checkpoint_hash,
            status = EXCLUDED.status,
            updated_at = EXCLUDED.updated_at
        """,
        {
            "task": task,
            "scope_key": scope_key,
            "checkpoint_hash": checkpoint_hash,
            "status": status,
            "updated_at": datetime.now(timezone.utc),
        },
    )


# -------------------------------------------------------------- SQL loading

def load_sql(task):
    return (SQL_DIR / f"{task}.sql").read_text()


def load_statements(task):
    """Split a multi-statement task file (raw.sql) into separate statements.

    DuckDB only accepts bound parameters on the last statement of a multi-
    statement execute() call, so raw.sql's three loads must run separately.
    Comments are stripped first: a ';' inside a comment (e.g. prose like
    "cutoff; lanes and fuel...") is not a statement boundary.
    """
    lines = [ln[: ln.find("--")] if "--" in ln else ln for ln in load_sql(task).splitlines()]
    text = "\n".join(lines)
    return [s.strip() for s in text.split(";") if s.strip()]


def filtered_params(sql_text, full_params):
    """DuckDB rejects a params dict containing keys not referenced in the query."""
    names = set(re.findall(r"\$(\w+)", sql_text))
    return {k: v for k, v in full_params.items() if k in names}


def run(conn, sql_text, full_params):
    params = filtered_params(sql_text, full_params)
    if params:
        conn.execute(sql_text, params)
    else:
        conn.execute(sql_text)


# -------------------------------------------------------------- task bodies

def execute_task(conn, task, target_date, as_of, fail_here):
    """Run one task's transform inside an already-open transaction (BEGIN issued here)."""
    kind = TASK_KIND[task]
    conn.execute("BEGIN")

    if kind == "global_multi":  # raw: full reload of every source file
        params = {
            "shipments_path": str(SHIPMENTS_CSV),
            "lanes_path": str(LANES_CSV),
            "fuel_path": str(FUEL_CSV),
            "as_of": as_of.isoformat(),
        }
        if fail_here:
            raise InjectedFailure(f"injected failure in task '{task}' before ingest")
        for statement in load_statements(task):
            run(conn, statement, params)

    elif kind == "global_single":  # staging_lanes: static reference data
        select_sql = load_sql(task)
        if fail_here:
            raise InjectedFailure(f"injected failure in task '{task}' before replace")
        conn.execute(f"CREATE OR REPLACE TABLE {task} AS {select_sql}")

    elif kind == "partition":  # replace exactly one date partition
        table, predicate = PARTITION_TABLE[task]
        select_sql = load_sql(task)
        params = {"date": target_date.isoformat(), "as_of": as_of.isoformat()}
        run(conn, f"DELETE FROM {table} WHERE {predicate}", params)
        if fail_here:
            raise InjectedFailure(f"injected failure in task '{task}' after DELETE, before INSERT")
        run(conn, f"INSERT INTO {table} {select_sql}", params)

    elif kind == "export":  # write exactly one date's CSV, atomically
        select_sql = load_sql(task)
        params = {"date": target_date.isoformat(), "as_of": as_of.isoformat()}
        EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        target_path = EXPORT_DIR / f"shipments_{target_date.isoformat()}.csv"
        tmp_path = target_path.with_suffix(".csv.tmp")
        if fail_here:
            raise InjectedFailure(f"injected failure in task '{task}' before export write")
        copy_sql = f"COPY ({select_sql}) TO '{tmp_path.as_posix()}' (HEADER, DELIMITER ',')"
        run(conn, copy_sql, params)
        tmp_path.replace(target_path)

    else:
        raise ValueError(f"unknown task kind for '{task}': {kind}")


# ----------------------------------------------------------------- runner

def run_dag_for_date(conn, order, target_date, as_of, force, fail_task, code_hash, log_row):
    outcome = {}
    hashes = {}

    for task in order:
        started_at = datetime.now(timezone.utc)
        scope_key = scope_key_for(task, target_date, as_of)

        blocking_parent = next(
            (p for p in DEPENDENCIES[task] if outcome.get(p) in ("failed", "blocked")),
            None,
        )
        if blocking_parent is not None:
            outcome[task] = "blocked"
            log_row(task, target_date, as_of, scope_key, "blocked", started_at, datetime.now(timezone.utc),
                     None, f"blocked by upstream task '{blocking_parent}'")
            continue

        checkpoint_hash = checkpoint_hash_for(task, target_date, as_of, hashes, code_hash)
        existing = get_checkpoint(conn, task, scope_key)
        already_done = (
            existing is not None
            and existing["status"] == "success"
            and existing["checkpoint_hash"] == checkpoint_hash
        )

        if already_done and not force:
            outcome[task] = "skipped"
            hashes[task] = checkpoint_hash
            log_row(task, target_date, as_of, scope_key, "skipped", started_at, datetime.now(timezone.utc),
                     checkpoint_hash, "checkpoint hit")
            continue

        try:
            execute_task(conn, task, target_date, as_of, fail_here=(fail_task == task))
            upsert_checkpoint(conn, task, scope_key, checkpoint_hash, "success")
            conn.execute("COMMIT")
            outcome[task] = "success"
            hashes[task] = checkpoint_hash
            log_row(task, target_date, as_of, scope_key, "success", started_at, datetime.now(timezone.utc),
                     checkpoint_hash, "")
        except Exception as exc:  # noqa: BLE001 - task body, must roll back and continue the DAG
            conn.execute("ROLLBACK")
            outcome[task] = "failed"
            log_row(task, target_date, as_of, scope_key, "failed", started_at, datetime.now(timezone.utc),
                     None, str(exc))

    return outcome


def daterange(start, end):
    for offset in range((end - start).days + 1):
        yield start + timedelta(days=offset)


# --------------------------------------------------------------------- CLI

def parse_date(value):
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date '{value}', expected YYYY-MM-DD") from exc


def add_common_flags(subparser):
    subparser.add_argument("--force", action="store_true",
                            help="ignore checkpoints and re-execute every task")
    subparser.add_argument("--fail-task", choices=sorted(DEPENDENCIES), default=None,
                            help="inject a failure into this task, after its delete and before "
                                 "its insert, to test rollback/blocking/recovery")


def parse_args(argv):
    parser = argparse.ArgumentParser(prog="pipeline.py", description=__doc__,
                                      formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="process a single shipment date")
    run_p.add_argument("--date", required=True, type=parse_date)
    run_p.add_argument("--as-of", required=True, type=parse_date, dest="as_of")
    add_common_flags(run_p)

    backfill_p = sub.add_parser("backfill", help="process a range of shipment dates")
    backfill_p.add_argument("--start", required=True, type=parse_date)
    backfill_p.add_argument("--end", required=True, type=parse_date)
    backfill_p.add_argument("--as-of", required=True, type=parse_date, dest="as_of")
    add_common_flags(backfill_p)

    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)

    try:
        order = validate_and_order(DEPENDENCIES)
    except ValueError as exc:
        print(f"DAG error: {exc}", file=sys.stderr)
        return 2

    if args.command == "backfill" and args.end < args.start:
        print("error: --end must not be before --start", file=sys.stderr)
        return 2
    dates = [args.date] if args.command == "run" else list(daterange(args.start, args.end))

    WORK_DIR.mkdir(exist_ok=True)
    code_hash = sha256_file(Path(__file__))
    run_id = uuid.uuid4().hex[:12]

    conn = duckdb.connect(str(DB_PATH))
    conn.execute((SQL_DIR / "_schema.sql").read_text())

    log_is_new = not RUN_LOG_PATH.exists()
    with RUN_LOG_PATH.open("a", newline="") as log_file:
        writer = csv.writer(log_file)
        if log_is_new:
            writer.writerow(RUN_LOG_COLUMNS)

        def log_row(task, target_date, as_of, scope_key, outcome, started_at, ended_at,
                     checkpoint_hash, message):
            writer.writerow([
                run_id, args.command, task, target_date.isoformat(), as_of.isoformat(),
                scope_key, outcome, checkpoint_hash or "",
                started_at.isoformat(timespec="milliseconds"),
                ended_at.isoformat(timespec="milliseconds"),
                round((ended_at - started_at).total_seconds() * 1000),
                message,
            ])
            log_file.flush()

        overall_ok = True
        for target_date in dates:
            outcome = run_dag_for_date(conn, order, target_date, args.as_of, args.force,
                                        args.fail_task, code_hash, log_row)
            summary = ", ".join(f"{task}={outcome[task]}" for task in order)
            print(f"[{target_date.isoformat()}] {summary}")
            if any(status in ("failed", "blocked") for status in outcome.values()):
                overall_ok = False

    conn.close()
    return 0 if overall_ok else 1


if __name__ == "__main__":
    sys.exit(main())
