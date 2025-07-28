from __future__ import annotations

import csv
import os
from typing import Dict, List, Tuple

from fetch_sheet import fetch_sheet

# Directories for storing fetched data and reports
DATA_DIR = "data"
RAW_FILE = os.path.join(DATA_DIR, "history.csv")
REPORT_FILE = os.path.join(DATA_DIR, "leaderboard.txt")


def save_rows(rows: List[List[str]], path: str) -> None:
    """Save the raw CSV rows to disk."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerows(rows)


def parse_leaderboards(rows: List[List[str]]) -> List[str]:
    """Compute various leaderboards from the data."""
    if not rows:
        return ["No data"]

    header, *data = rows
    # indices of relevant columns
    try:
        idx_track = header.index("賽道")
        idx_driver = header.index("車手")
        idx_vehicle = header.index("車輛")
        idx_time = header.index("時間")
    except ValueError:
        return ["Missing required columns"]

    idx_champ = header.index("錦標賽") if "錦標賽" in header else None

    # Fastest time per track overall (driver)
    fastest_by_track: Dict[str, Tuple[str, float]] = {}
    # Fastest time per track per vehicle
    fastest_by_track_vehicle: Dict[str, Dict[str, Tuple[str, float]]] = {}
    # Driver career best
    driver_best: Dict[str, float] = {}
    # Vehicle best per track
    vehicle_track_best: Dict[str, Dict[str, Tuple[str, float]]] = {}

    championship_rows: List[List[str]] = []

    for row in data:
        if len(row) <= max(idx_track, idx_driver, idx_vehicle, idx_time):
            continue
        track = row[idx_track]
        driver = row[idx_driver]
        vehicle = row[idx_vehicle]
        try:
            t = float(row[idx_time])
        except ValueError:
            continue

        if idx_champ is not None and row[idx_champ]:
            championship_rows.append(row)

        if track not in fastest_by_track or t < fastest_by_track[track][1]:
            fastest_by_track[track] = (driver, t)

        tbv = fastest_by_track_vehicle.setdefault(track, {})
        if vehicle not in tbv or t < tbv[vehicle][1]:
            tbv[vehicle] = (driver, t)

        if driver not in driver_best or t < driver_best[driver]:
            driver_best[driver] = t

        vtb = vehicle_track_best.setdefault(vehicle, {})
        if track not in vtb or t < vtb[track][1]:
            vtb[track] = (driver, t)

    lines: List[str] = []

    lines.append("Fastest driver per track:")
    for track, (driver, t) in fastest_by_track.items():
        lines.append(f"{track}: {driver} {t}")

    lines.append("")
    lines.append("Fastest per vehicle on each track:")
    for track, vehicles in fastest_by_track_vehicle.items():
        lines.append(f"{track}:")
        for vehicle, (driver, t) in vehicles.items():
            lines.append(f"  {vehicle}: {driver} {t}")

    lines.append("")
    lines.append("Driver career best:")
    for driver, t in driver_best.items():
        lines.append(f"{driver}: {t}")

    lines.append("")
    lines.append("Vehicle best per track:")
    for vehicle, tracks in vehicle_track_best.items():
        lines.append(f"{vehicle}:")
        for track, (driver, t) in tracks.items():
            lines.append(f"  {track}: {driver} {t}")

    if championship_rows:
        lines.append("")
        lines.append("Championship entries:")
        for row in championship_rows:
            lines.append(", ".join(row))

    return lines


def main() -> None:
    try:
        rows = fetch_sheet()
    except Exception:
        rows = []

    if rows:
        save_rows(rows, RAW_FILE)

    report_lines = parse_leaderboards(rows)

    os.makedirs(os.path.dirname(REPORT_FILE), exist_ok=True)
    with open(REPORT_FILE, "w", encoding="utf-8") as fh:
        for line in report_lines:
            fh.write(line + "\n")

    print(f"Leaderboard written to {REPORT_FILE}")


if __name__ == "__main__":
    main()
