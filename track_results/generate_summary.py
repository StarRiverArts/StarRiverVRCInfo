from __future__ import annotations

import os
from typing import List

from fetch_sheet import fetch_sheet


OUTPUT_DIR = "report"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "summary.txt")


def summarise(rows: List[List[str]]) -> List[str]:
    if not rows:
        return ["No data fetched."]

    header, *data = rows
    summary = [f"Total entries: {len(data)}"]

    track_idx = header.index("賽道") if "賽道" in header else None
    driver_idx = header.index("車手") if "車手" in header else None
    time_idx = header.index("時間") if "時間" in header else None

    if track_idx is not None and time_idx is not None:
        best_by_track: dict[str, float] = {}
        for row in data:
            if len(row) <= max(track_idx, time_idx):
                continue
            track = row[track_idx]
            try:
                t = float(row[time_idx])
            except ValueError:
                continue
            if track not in best_by_track or t < best_by_track[track]:
                best_by_track[track] = t
        if best_by_track:
            summary.append("")
            summary.append("Fastest per track (sorted):")
            for track, t in sorted(best_by_track.items(), key=lambda item: item[1]):
                summary.append(f"{track}: {t}")

    if driver_idx is not None and time_idx is not None:
        best_by_driver: dict[str, float] = {}
        for row in data:
            if len(row) <= max(driver_idx, time_idx):
                continue
            driver = row[driver_idx]
            try:
                t = float(row[time_idx])
            except ValueError:
                continue
            if driver not in best_by_driver or t < best_by_driver[driver]:
                best_by_driver[driver] = t
        if best_by_driver:
            summary.append("")
            summary.append("Best time per driver:")
            for driver, t in sorted(best_by_driver.items(), key=lambda item: item[1]):
                summary.append(f"{driver}: {t}")

    return summary


def main() -> None:
    try:
        rows = fetch_sheet()
    except RuntimeError as exc:
        print(exc)
        rows = []
    lines = summarise(rows)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        for line in lines:
            fh.write(line + "\n")
    print(f"Summary written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
