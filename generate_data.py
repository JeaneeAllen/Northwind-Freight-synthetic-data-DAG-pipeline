#!/usr/bin/env python3
"""Generate synthetic Northwind Freight data: lanes.csv, fuel.csv, shipments.csv.

Deterministic: every value is derived from the calendar date and a shipment's
position within its day, so re-running produces byte-identical CSVs.
"""

import csv
from datetime import date, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"

START_DATE = date(2026, 1, 1)
END_DATE = date(2026, 3, 31)
SHIPMENTS_PER_DAY = 10

LANES = [
    {
        "lane_id": "LANE-1",
        "origin": "Minneapolis, MN",
        "destination": "Chicago, IL",
        "distance_miles": 409,
        "rate_cents_per_mile": 285,
        "standard_transit_days": 1,
    },
    {
        "lane_id": "LANE-2",
        "origin": "Chicago, IL",
        "destination": "Dallas, TX",
        "distance_miles": 925,
        "rate_cents_per_mile": 210,
        "standard_transit_days": 2,
    },
    {
        "lane_id": "LANE-3",
        "origin": "Minneapolis, MN",
        "destination": "Denver, CO",
        "distance_miles": 693,
        "rate_cents_per_mile": 240,
        "standard_transit_days": 3,
    },
]

FUEL_BASE_RATE_CENTS_PER_MILE = 45
FUEL_RATE_CYCLE = 10  # rate cycles 45-54 cents/mile, keyed off day index

LATE_BILL_DATES = {date(2026, 3, 29), date(2026, 3, 30), date(2026, 3, 31)}
LATE_BILL_SEQS = {0, 1}  # first two of the day's ten shipments
LATE_TRANSIT_DAYS = 7

# 1-based positions (in shipment_date/lane order) to duplicate verbatim.
DUPLICATE_SHIPMENT_INDICES = [1, 100, 200, 300, 400, 500, 600, 700, 800]


def daterange(start, end):
    for offset in range((end - start).days + 1):
        yield start + timedelta(days=offset)


def build_fuel_rates():
    rows = []
    for day_index, day in enumerate(daterange(START_DATE, END_DATE)):
        rate = FUEL_BASE_RATE_CENTS_PER_MILE + (day_index % FUEL_RATE_CYCLE)
        rows.append({"date": day.isoformat(), "fuel_surcharge_cents_per_mile": rate})
    return rows


def build_shipments(fuel_rate_by_date):
    shipments = []
    global_index = 0
    for day in daterange(START_DATE, END_DATE):
        fuel_rate = fuel_rate_by_date[day]
        for seq in range(SHIPMENTS_PER_DAY):
            global_index += 1
            lane = LANES[seq % len(LANES)]

            if day in LATE_BILL_DATES and seq in LATE_BILL_SEQS:
                transit_days = LATE_TRANSIT_DAYS
            else:
                transit_days = lane["standard_transit_days"]

            freight_charge_cents = lane["distance_miles"] * lane["rate_cents_per_mile"]
            fuel_surcharge_cents = lane["distance_miles"] * fuel_rate

            shipments.append({
                "shipment_id": f"SHP{global_index:06d}",
                "shipment_date": day.isoformat(),
                "arrival_date": (day + timedelta(days=transit_days)).isoformat(),
                "lane_id": lane["lane_id"],
                "freight_charge_cents": freight_charge_cents,
                "fuel_surcharge_cents": fuel_surcharge_cents,
                "total_charge_cents": freight_charge_cents + fuel_surcharge_cents,
            })
    return shipments


def add_duplicate_rows(shipments):
    by_position = {i + 1: row for i, row in enumerate(shipments)}
    duplicates = [dict(by_position[i]) for i in DUPLICATE_SHIPMENT_INDICES]
    return shipments + duplicates


def write_csv(path, rows, fieldnames):
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def verify(lanes, fuel_rates, shipments):
    dates = {row["date"] for row in fuel_rates}
    shipment_ids = {row["shipment_id"] for row in shipments}
    late_bills = sum(
        1
        for row in shipments
        if (date.fromisoformat(row["arrival_date"]) - date.fromisoformat(row["shipment_date"])).days
        == LATE_TRANSIT_DAYS
    )

    checks = [
        ("90 dates in fuel.csv", len(dates) == 90),
        ("3 static lanes", len(lanes) == 3),
        ("900 unique shipment IDs", len(shipment_ids) == 900),
        ("909 shipment rows", len(shipments) == 909),
        ("6 late bills (7-day transit)", late_bills == 6),
    ]

    print("\nVerification:")
    all_passed = True
    for label, passed in checks:
        print(f"  [{'PASS' if passed else 'FAIL'}] {label}")
        all_passed = all_passed and passed
    return all_passed


def main():
    DATA_DIR.mkdir(exist_ok=True)

    lanes = list(LANES)
    fuel_rates = build_fuel_rates()
    fuel_rate_by_date = {
        date.fromisoformat(r["date"]): r["fuel_surcharge_cents_per_mile"] for r in fuel_rates
    }

    shipments = build_shipments(fuel_rate_by_date)
    shipments = add_duplicate_rows(shipments)

    write_csv(
        DATA_DIR / "lanes.csv",
        lanes,
        ["lane_id", "origin", "destination", "distance_miles", "rate_cents_per_mile", "standard_transit_days"],
    )
    write_csv(DATA_DIR / "fuel.csv", fuel_rates, ["date", "fuel_surcharge_cents_per_mile"])
    write_csv(
        DATA_DIR / "shipments.csv",
        shipments,
        [
            "shipment_id",
            "shipment_date",
            "arrival_date",
            "lane_id",
            "freight_charge_cents",
            "fuel_surcharge_cents",
            "total_charge_cents",
        ],
    )

    print(f"Wrote {len(lanes)} lanes, {len(fuel_rates)} fuel rates, {len(shipments)} shipment rows to {DATA_DIR}")
    if not verify(lanes, fuel_rates, shipments):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
