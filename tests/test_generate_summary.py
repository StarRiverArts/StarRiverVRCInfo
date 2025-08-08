import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent / "track_results"))
from generate_summary import summarise  # noqa: E402


def test_summarise_includes_sorted_track_and_driver_best():
    rows = [
        ["賽道", "車手", "車輛", "時間", "錦標賽"],
        ["A", "Driver1", "Car1", "10.5", ""],
        ["A", "Driver2", "Car2", "9.8", ""],
        ["B", "Driver1", "Car3", "8.0", ""],
    ]
    summary = summarise(rows)

    assert summary[0] == "Total entries: 3"

    track_idx = summary.index("Fastest per track (sorted):")
    track_lines = summary[track_idx + 1 : track_idx + 3]
    assert track_lines == ["B: 8.0", "A: 9.8"]

    driver_idx = summary.index("Best time per driver:")
    driver_lines = summary[driver_idx + 1 : driver_idx + 3]
    assert "Driver1: 8.0" in driver_lines
    assert "Driver2: 9.8" in driver_lines

