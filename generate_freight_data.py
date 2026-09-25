"""Generate synthetic less-than-truckload (LTL) freight data as CSV files.

Outputs (written to --out, default ./data):
  lanes.csv          lane_id, origin_zip, origin_city, origin_state, dest_zip,
                     dest_city, dest_state, distance_miles, base_rate_per_cwt
  fuel_surcharge.csv date, diesel_price_per_gal, fuel_surcharge_pct
  shipments.csv      shipment_id, pickup_date, delivery_date, origin_zip, dest_zip,
                     weight_lbs, linehaul_revenue, cost, bill_received_date

Deliberate data-quality issues for pipeline testing:
  * Late-arriving bills: --late-bills shipments (default 300) have a
    bill_received_date 5-10 days after delivery_date. All other bills arrive
    0-2 days after delivery.
  * Duplicates: --duplicates extra rows (default 20) reuse an existing
    shipment_id. Most are exact copies; about a quarter are "re-sent" bills
    with a later bill_received_date and adjusted revenue/cost, so a naive
    DISTINCT won't remove them.

Uses only the standard library. Output is deterministic for a given --seed.

Example:
  python generate_freight_data.py --start 2026-06-01 --days 90 --out data
"""

import argparse
import csv
import math
import random
from datetime import date, timedelta
from pathlib import Path

# Terminal cities: (zip, city, state, lat, lon)
TERMINALS = [
    ("30303", "Atlanta", "GA", 33.75, -84.39),
    ("60607", "Chicago", "IL", 41.88, -87.63),
    ("75201", "Dallas", "TX", 32.78, -96.80),
    ("80202", "Denver", "CO", 39.74, -104.99),
    ("48226", "Detroit", "MI", 42.33, -83.05),
    ("77002", "Houston", "TX", 29.76, -95.37),
    ("46204", "Indianapolis", "IN", 39.77, -86.16),
    ("64105", "Kansas City", "MO", 39.10, -94.58),
    ("90021", "Los Angeles", "CA", 34.03, -118.24),
    ("38103", "Memphis", "TN", 35.15, -90.05),
    ("55401", "Minneapolis", "MN", 44.98, -93.27),
    ("37203", "Nashville", "TN", 36.16, -86.78),
    ("07105", "Newark", "NJ", 40.72, -74.15),
    ("19104", "Philadelphia", "PA", 39.95, -75.19),
    ("85004", "Phoenix", "AZ", 33.45, -112.07),
    ("97209", "Portland", "OR", 45.53, -122.68),
    ("84101", "Salt Lake City", "UT", 40.76, -111.89),
    ("98108", "Seattle", "WA", 47.55, -122.32),
]

NUM_LANES = 60
LATE_BILL_MIN_DAYS = 5
LATE_BILL_MAX_DAYS = 10


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start", type=date.fromisoformat, default=date(2026, 6, 1),
                   help="first pickup date, YYYY-MM-DD (default 2026-06-01)")
    p.add_argument("--days", type=int, default=90, help="number of days to generate (default 90)")
    p.add_argument("--daily-shipments", type=int, default=75,
                   help="average shipments per weekday (default 75)")
    p.add_argument("--late-bills", type=int, default=300,
                   help="shipments whose bill arrives 5-10 days after delivery (default 300)")
    p.add_argument("--duplicates", type=int, default=20,
                   help="extra rows that repeat an existing shipment_id (default 20)")
    p.add_argument("--seed", type=int, default=42, help="random seed (default 42)")
    p.add_argument("--out", type=Path, default=Path("data"), help="output directory (default ./data)")
    return p.parse_args()


def haversine_miles(lat1, lon1, lat2, lon2):
    r = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def build_lanes(rng):
    pairs = [(o, d) for o in TERMINALS for d in TERMINALS if o[0] != d[0]]
    lanes = []
    for i, (o, d) in enumerate(rng.sample(pairs, NUM_LANES), start=1):
        # Road miles run ~15-25% longer than great-circle distance.
        miles = round(haversine_miles(o[3], o[4], d[3], d[4]) * rng.uniform(1.15, 1.25))
        # Rate per hundredweight rises with distance, with lane-level noise.
        rate = round((18 + miles * 0.035) * rng.uniform(0.85, 1.15), 2)
        lanes.append({
            "lane_id": f"LN{i:03d}",
            "origin_zip": o[0], "origin_city": o[1], "origin_state": o[2],
            "dest_zip": d[0], "dest_city": d[1], "dest_state": d[2],
            "distance_miles": miles,
            "base_rate_per_cwt": rate,
        })
    return lanes


def build_fuel_surcharge(rng, start, days):
    """Daily diesel price as a mean-reverting random walk, plus the derived FSC %."""
    rows = []
    price = 3.85
    for n in range(days + LATE_BILL_MAX_DAYS + 10):  # cover deliveries past the window
        price += rng.gauss(0, 0.015) + (3.85 - price) * 0.03
        price = max(price, 2.50)
        # Common LTL table shape: ~1% of linehaul per $0.10 above a $1.20 peg.
        fsc_pct = round(max(0.0, (price - 1.20) * 10), 1)
        rows.append({
            "date": (start + timedelta(days=n)).isoformat(),
            "diesel_price_per_gal": round(price, 3),
            "fuel_surcharge_pct": fsc_pct,
        })
    return rows


def add_business_days(d, n):
    while n > 0:
        d += timedelta(days=1)
        if d.weekday() < 5:
            n -= 1
    return d


def transit_days(miles, rng):
    base = 1 + int(miles // 500)
    return max(1, base + rng.choice([0, 0, 0, 1, -1 if base > 1 else 0]))


def build_shipments(rng, lanes, start, days, daily_avg):
    # Weight lanes so some are much busier than others.
    lane_weights = [rng.paretovariate(1.5) for _ in lanes]
    shipments = []
    seq = 1
    for n in range(days):
        pickup = start + timedelta(days=n)
        if pickup.weekday() >= 5:
            count = int(rng.gauss(daily_avg * 0.08, 2))  # light weekend volume
        else:
            count = int(rng.gauss(daily_avg, daily_avg * 0.12))
        for _ in range(max(0, count)):
            lane = rng.choices(lanes, weights=lane_weights)[0]
            weight = int(min(max(rng.lognormvariate(math.log(1400), 0.8), 100), 12000))
            revenue = weight / 100 * lane["base_rate_per_cwt"] * rng.uniform(0.9, 1.1)
            revenue = max(revenue, 125.0)  # minimum charge
            cost = revenue * rng.uniform(0.70, 0.98)
            if rng.random() < 0.05:  # a few loss-making shipments
                cost = revenue * rng.uniform(1.0, 1.25)
            delivery = add_business_days(pickup, transit_days(lane["distance_miles"], rng))
            shipments.append({
                "shipment_id": f"NWF{pickup:%y%m%d}{seq:05d}",
                "pickup_date": pickup,
                "delivery_date": delivery,
                "origin_zip": lane["origin_zip"],
                "dest_zip": lane["dest_zip"],
                "weight_lbs": weight,
                "linehaul_revenue": round(revenue, 2),
                "cost": round(cost, 2),
                "bill_received_date": delivery + timedelta(days=rng.randint(0, 2)),
            })
            seq += 1
    return shipments


def inject_late_bills(rng, shipments, n):
    for s in rng.sample(shipments, n):
        s["bill_received_date"] = s["delivery_date"] + timedelta(
            days=rng.randint(LATE_BILL_MIN_DAYS, LATE_BILL_MAX_DAYS))


def inject_duplicates(rng, shipments, n):
    originals = rng.sample(shipments, n)
    rows = list(shipments)
    for i, orig in enumerate(originals):
        dup = dict(orig)
        if i % 4 == 3:
            # Re-sent/corrected bill: same shipment_id, different values.
            # Arrives 1-2 days after the original bill, but only for on-time
            # originals so the late-bill count stays exact.
            if (orig["bill_received_date"] - orig["delivery_date"]).days < LATE_BILL_MIN_DAYS:
                dup["bill_received_date"] = orig["bill_received_date"] + timedelta(days=rng.randint(1, 2))
            factor = rng.uniform(0.95, 1.08)
            dup["linehaul_revenue"] = round(orig["linehaul_revenue"] * factor, 2)
            dup["cost"] = round(orig["cost"] * factor, 2)
        # Place the duplicate somewhere after the original, as a re-feed would.
        idx = rows.index(orig)
        rows.insert(rng.randint(idx + 1, min(len(rows), idx + 400)), dup)
    return rows


def write_csv(path, rows, fields):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: (v.isoformat() if isinstance(v, date) else v) for k, v in r.items()})


def main():
    args = parse_args()
    rng = random.Random(args.seed)
    args.out.mkdir(parents=True, exist_ok=True)

    lanes = build_lanes(rng)
    fuel = build_fuel_surcharge(rng, args.start, args.days)
    shipments = build_shipments(rng, lanes, args.start, args.days, args.daily_shipments)
    if args.late_bills > len(shipments) or args.duplicates > len(shipments):
        raise SystemExit("Not enough shipments for the requested late bills / duplicates.")
    inject_late_bills(rng, shipments, args.late_bills)
    rows = inject_duplicates(rng, shipments, args.duplicates)

    write_csv(args.out / "lanes.csv", lanes, list(lanes[0]))
    write_csv(args.out / "fuel_surcharge.csv", fuel, list(fuel[0]))
    write_csv(args.out / "shipments.csv", rows, list(rows[0]))

    end = args.start + timedelta(days=args.days - 1)
    print(f"Pickup window: {args.start} to {end} ({args.days} days)")
    print(f"lanes.csv:          {len(lanes):>6} rows")
    print(f"fuel_surcharge.csv: {len(fuel):>6} rows")
    print(f"shipments.csv:      {len(rows):>6} rows "
          f"({len(shipments)} unique shipment_ids + {args.duplicates} duplicates)")
    print(f"Late bills (received {LATE_BILL_MIN_DAYS}-{LATE_BILL_MAX_DAYS} days after delivery): "
          f"{args.late_bills}")
    print(f"Output directory: {args.out.resolve()}")


if __name__ == "__main__":
    main()
