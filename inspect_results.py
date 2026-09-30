#!/usr/bin/env python3
"""Print a quick summary of what's currently in work/warehouse.duckdb.

Read-only: never writes anything. Run pipeline.py first if the database
doesn't exist yet.
"""

import sys
from datetime import date
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "work" / "warehouse.duckdb"


def main():
    if not DB_PATH.exists():
        print(f"No database at {DB_PATH}.")
        print("Run the pipeline first, e.g.:")
        print("  python3 pipeline.py backfill --start 2026-01-01 --end 2026-03-31 --as-of 2026-04-07")
        return 1

    con = duckdb.connect(str(DB_PATH), read_only=True)

    print("=== Total shipment rows ===")
    raw_total = con.execute("SELECT COUNT(*) FROM raw_shipments").fetchone()[0]
    staging_total = con.execute("SELECT COUNT(*) FROM staging_shipments").fetchone()[0]
    staging_unique = con.execute("SELECT COUNT(DISTINCT shipment_id) FROM staging_shipments").fetchone()[0]
    print("(raw_shipments is a shared table: it reflects only the most recently")
    print(" used --as-of cutoff, not every cutoff ever run. staging_shipments is")
    print(" the reliable source — it's kept correctly per date regardless.)")
    print(f"raw_shipments (as currently loaded, includes any duplicate rows): {raw_total}")
    print(f"staging_shipments (deduplicated, one row per shipment_id):        {staging_total}")
    print(f"distinct shipment_id in staging_shipments:                       {staging_unique}")

    print("\n=== Duplicate shipment IDs (found in raw_shipments, before dedup) ===")
    dupes = con.execute(
        """
        SELECT shipment_id, COUNT(*) AS row_count
        FROM raw_shipments
        GROUP BY shipment_id
        HAVING COUNT(*) > 1
        ORDER BY shipment_id
        """
    ).fetchall()
    if dupes:
        for shipment_id, row_count in dupes:
            print(f"  {shipment_id}: {row_count} rows")
        print(f"total duplicate shipment_ids: {len(dupes)}")
    else:
        print("  none")

    print("\n=== Daily shipment counts, March 28-31 (from staging_shipments) ===")
    daily = con.execute(
        """
        SELECT shipment_date, COUNT(*) AS shipment_count
        FROM staging_shipments
        WHERE shipment_date BETWEEN '2026-03-28' AND '2026-03-31'
        GROUP BY shipment_date
        ORDER BY shipment_date
        """
    ).fetchall()
    seen = {d: c for d, c in daily}
    for offset in range(4):
        day = date(2026, 3, 28 + offset)
        print(f"  {day.isoformat()}: {seen.get(day, 0)} shipments")

    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
