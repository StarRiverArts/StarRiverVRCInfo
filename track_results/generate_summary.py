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

    if "賽道" in header and "時間" in header:
        track_idx = header.index("賽道")
        time_idx = header.index("時間")
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
        summary.append("Fastest per track:")
        for track, t in best_by_track.items():
            summary.append(f"{track}: {t}")
    return summary


def main() -> None:
    try:
        rows = fetch_sheet()
    except Exception:
        rows = []
    lines = summarise(rows)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        for line in lines:
            fh.write(line + "\n")
    print(f"Summary written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
