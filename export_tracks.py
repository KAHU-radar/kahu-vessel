"""Export KAHU track data from TrackServer to CSV or Excel.

Fetches all routes within a spatial bounding box and time range from
crowdsource.kahu.earth, then writes one row per point.

Usage:
  python export_tracks.py                          # last 24 hours, global bbox
  python export_tracks.py --days 7                 # last 7 days
  python export_tracks.py --out tracks.xlsx        # Excel output
  python export_tracks.py --start 2026-03-01 --end 2026-03-09
  python export_tracks.py --bbox -120,32,-117,35   # lon_min,lat_min,lon_max,lat_max
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE_URL = "https://crowdsource.kahu.earth"


def fetch_geojson(bbox: str, start: str, end: str) -> dict:
    url = f"{BASE_URL}/api/routes/all/{bbox}/{start}/{end}/geojson"
    print(f"Fetching: {url}", file=sys.stderr)
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.load(resp)


def flatten(geojson: dict) -> list[dict]:
    """Convert a FeatureCollection into a flat list of point rows."""
    rows = []
    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        route_uuid = props.get("uuid", "")
        route_start_str = props.get("start", "")
        try:
            route_start = datetime.fromisoformat(route_start_str.replace(" ", "T"))
            if route_start.tzinfo is None:
                route_start = route_start.replace(tzinfo=timezone.utc)
        except (ValueError, AttributeError):
            route_start = None

        coords = feature.get("geometry", {}).get("coordinates", [])
        for i, coord in enumerate(coords):
            lon, lat = coord[0], coord[1]
            offset_s = coord[2] if len(coord) > 2 else 0.0
            if route_start is not None:
                abs_ts = (route_start + timedelta(seconds=offset_s)).isoformat()
            else:
                abs_ts = ""
            rows.append({
                "route_uuid": route_uuid,
                "route_start": route_start_str,
                "point_index": i,
                "lat": lat,
                "lon": lon,
                "time_offset_s": round(offset_s, 3),
                "timestamp": abs_ts,
            })
    return rows


def write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        print("No data returned.", file=sys.stderr)
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {path}", file=sys.stderr)


def write_excel(rows: list[dict], path: Path) -> None:
    try:
        import openpyxl
    except ImportError:
        print("openpyxl not installed — run: pip install openpyxl", file=sys.stderr)
        sys.exit(1)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Tracks"
    if not rows:
        print("No data returned.", file=sys.stderr)
        return
    headers = list(rows[0].keys())
    ws.append(headers)
    for row in rows:
        ws.append([row[h] for h in headers])
    wb.save(path)
    print(f"Wrote {len(rows)} rows to {path}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export KAHU tracks to CSV/Excel")
    parser.add_argument("--bbox", default="-180,-90,180,90",
                        help="lon_min,lat_min,lon_max,lat_max (default: global)")
    parser.add_argument("--start", default=None, help="Start datetime (YYYY-MM-DD or ISO8601)")
    parser.add_argument("--end", default=None, help="End datetime (YYYY-MM-DD or ISO8601)")
    parser.add_argument("--days", type=int, default=1, help="Last N days (default: 1)")
    parser.add_argument("--out", default="tracks.csv", help="Output file (.csv or .xlsx)")
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    end_dt = datetime.fromisoformat(args.end) if args.end else now
    start_dt = datetime.fromisoformat(args.start) if args.start else now - timedelta(days=args.days)

    # Server expects ISO8601 without microseconds
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S%z") if start_dt.tzinfo else start_dt.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_dt.strftime("%Y-%m-%dT%H:%M:%S%z") if end_dt.tzinfo else end_dt.strftime("%Y-%m-%dT%H:%M:%S")

    geojson = fetch_geojson(args.bbox, start_str, end_str)
    n_routes = len(geojson.get("features", []))
    print(f"Received {n_routes} route(s)", file=sys.stderr)

    rows = flatten(geojson)
    out = Path(args.out)
    if out.suffix.lower() == ".xlsx":
        write_excel(rows, out)
    else:
        write_csv(rows, out)


if __name__ == "__main__":
    main()
