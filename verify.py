#!/usr/bin/env python3
"""Verification suite for pipeline.py.

Every check runs against a fresh copy of generate_data.py, pipeline.py, and
sql/ in its own temporary directory, so results never depend on this
project's own data/ or work/ state, and repeated runs never interfere with
each other.
"""

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable

RESULTS = []


def check(name, condition, detail=""):
    RESULTS.append((name, bool(condition)))
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name}")
    if detail:
        print(f"       {detail}")


def new_workspace(tmp_root, name):
    ws = tmp_root / name
    ws.mkdir()
    shutil.copy2(ROOT / "generate_data.py", ws / "generate_data.py")
    shutil.copy2(ROOT / "pipeline.py", ws / "pipeline.py")
    shutil.copytree(ROOT / "sql", ws / "sql")
    subprocess.run([PYTHON, "generate_data.py"], cwd=ws, check=True, capture_output=True, text=True)
    return ws


def run_pipeline(ws, *args):
    return subprocess.run([PYTHON, "pipeline.py", *args], cwd=ws, capture_output=True, text=True)


def query(ws, sql):
    con = duckdb.connect(str(ws / "work" / "warehouse.duckdb"), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def export_hashes(ws, names=None):
    paths = sorted((ws / "work" / "exports").glob("*.csv"))
    if names is not None:
        paths = [p for p in paths if p.name in names]
    return {p.name: p.read_bytes() for p in paths}


def main():
    with tempfile.TemporaryDirectory(prefix="northwind_verify_") as tmp:
        tmp_root = Path(tmp)

        # --- Test 1: full 90-day run at cutoff March 31 -------------------
        # Expected count: the assignment's checklist says 894 (= 900 - the
        # 6 explicitly late-arriving shipments). That assumes only those 6
        # cross the cutoff. In this data, normal (non-late) lane transit
        # times of 1-3 days ALSO push shipments from the last few days of
        # the quarter past a cutoff set to the quarter's last day: Mar 29
        # loses 5 more, Mar 30 loses 7 more, Mar 31 loses all 10. 900 - 22
        # = 878. See the written review for the full breakdown; this is a
        # mismatch in the checklist's assumption, not a pipeline defect —
        # raw.sql's filter (arrival_date <= as_of) is exactly per spec.
        ws1 = new_workspace(tmp_root, "t1_cutoff_mar31")
        r = run_pipeline(ws1, "backfill", "--start", "2026-01-01", "--end", "2026-03-31", "--as-of", "2026-03-31")
        n = query(ws1, "SELECT COUNT(DISTINCT shipment_id) FROM staging_shipments")[0][0]
        check(
            "90-day run at cutoff 2026-03-31 has a stable, arithmetic-consistent unique count",
            r.returncode == 0 and n == 878,
            f"got {n} unique shipments (checklist assumed 894; correct value given this data's transit times is 878 — see review)",
        )

        # --- Test 2 & 3: intermediate failure blocks mart/export, and the
        # failed transaction leaves zero partial rows -----------------------
        ws2 = new_workspace(tmp_root, "t2_fail_intermediate")
        r = run_pipeline(ws2, "run", "--date", "2026-02-10", "--as-of", "2026-02-17", "--fail-task", "intermediate")
        blocked_ok = (
            r.returncode == 1
            and "intermediate=failed" in r.stdout
            and "mart=blocked" in r.stdout
            and "export=blocked" in r.stdout
        )
        check("an intermediate failure blocks mart and export", blocked_ok, r.stdout.strip())

        rows_after_fail = query(ws2, "SELECT COUNT(*) FROM intermediate WHERE shipment_date = '2026-02-10'")[0][0]
        check(
            "a failure after DELETE rolls back the transaction (zero partial rows)",
            rows_after_fail == 0,
            f"intermediate has {rows_after_fail} rows for 2026-02-10 after the injected failure",
        )

        # --- Test 4: recovery reuses successful upstream work ---------------
        r2 = run_pipeline(ws2, "run", "--date", "2026-02-10", "--as-of", "2026-02-17")
        reused_ok = (
            r2.returncode == 0
            and all(f"{t}=skipped" in r2.stdout for t in
                    ("raw", "staging_shipments", "staging_lanes", "staging_fuel"))
            and "intermediate=success" in r2.stdout
            and "mart=success" in r2.stdout
            and "export=success" in r2.stdout
        )
        check(
            "recovery reuses successful upstream work and retries only failed/blocked tasks",
            reused_ok,
            r2.stdout.strip(),
        )

        # --- Test 5: forced rerun produces byte-identical CSV ---------------
        ws3 = new_workspace(tmp_root, "t3_force_identical")
        run_pipeline(ws3, "run", "--date", "2026-01-10", "--as-of", "2026-01-20")
        csv_path = ws3 / "work" / "exports" / "shipments_2026-01-10.csv"
        before = csv_path.read_bytes()
        r3 = run_pipeline(ws3, "run", "--date", "2026-01-10", "--as-of", "2026-01-20", "--force")
        after = csv_path.read_bytes()
        check(
            "a forced rerun produces a byte-identical CSV",
            r3.returncode == 0 and before == after and len(before) > 0,
        )

        # --- Tests 6, 7, 8: extend an existing backfill's cutoff -------------
        ws4 = new_workspace(tmp_root, "t4_extend_cutoff")
        run_pipeline(ws4, "backfill", "--start", "2026-01-01", "--end", "2026-03-31", "--as-of", "2026-03-31")
        before_hashes = export_hashes(ws4)

        r4 = run_pipeline(ws4, "backfill", "--start", "2026-03-29", "--end", "2026-03-31", "--as-of", "2026-04-07")

        n_unique = query(ws4, "SELECT COUNT(DISTINCT shipment_id) FROM staging_shipments")[0][0]
        n_rows = query(ws4, "SELECT COUNT(*) FROM staging_shipments")[0][0]
        check(
            "backfilling Mar 29-31 at cutoff Apr 7 raises the total to 900 without duplicates",
            r4.returncode == 0 and n_unique == 900 and n_rows == 900,
            f"unique={n_unique} total_rows={n_rows}",
        )

        touched = {"shipments_2026-03-29.csv", "shipments_2026-03-30.csv", "shipments_2026-03-31.csv"}
        untouched_names = set(before_hashes) - touched
        after_hashes = export_hashes(ws4, names=untouched_names)
        all_same = after_hashes == {name: before_hashes[name] for name in untouched_names}
        check(
            f"all {len(untouched_names)} dates outside the backfill remain byte-identical",
            all_same and len(untouched_names) == 87,
            f"{len(untouched_names)} dates compared",
        )

        hashes_before_repeat = export_hashes(ws4)
        r5 = run_pipeline(ws4, "backfill", "--start", "2026-03-29", "--end", "2026-03-31", "--as-of", "2026-04-07")
        hashes_after_repeat = export_hashes(ws4)
        check(
            "repeating the corrected backfill produces identical output",
            r5.returncode == 0 and hashes_before_repeat == hashes_after_repeat,
        )

        # --- Bonus regression: stale shared-partition checkpoint fix --------
        # Not one of the 8 required tests, but direct evidence for a real
        # weakness found in review and then fixed: revisiting a date under
        # its ORIGINAL --as-of, after a different --as-of has overwritten the
        # same physical partition, must recompute rather than silently reuse
        # a checkpoint whose scope no longer matches what's on disk.
        r6 = run_pipeline(ws4, "run", "--date", "2026-03-29", "--as-of", "2026-03-31")
        reverted_csv = (ws4 / "work" / "exports" / "shipments_2026-03-29.csv").read_bytes()
        expected_reverted = before_hashes["shipments_2026-03-29.csv"]
        check(
            "[regression] reverting to an earlier --as-of re-executes instead of serving a stale scope",
            r6.returncode == 0 and "skipped" not in r6.stdout and reverted_csv == expected_reverted,
            r6.stdout.strip(),
        )

    passed = sum(1 for _, ok in RESULTS if ok)
    total = len(RESULTS)
    print(f"\n{passed}/{total} checks passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
