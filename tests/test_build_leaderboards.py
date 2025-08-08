import csv
import sys
from pathlib import Path


# Allow importing the script as a module
sys.path.append(str(Path(__file__).resolve().parent.parent / "track_results"))
import build_leaderboards as bl  # noqa: E402


def sample_rows():
    """Return sample rows with header for leaderboard generation."""
    return [
        ["賽道", "車手", "車輛", "時間", "錦標賽"],
        ["TrackA", "DriverB", "Car1", "1.5", ""],
        ["TrackA", "DriverA", "Car1", "1.0", ""],
        ["TrackB", "DriverC", "Car2", "2.0", "yes"],
        ["TrackB", "DriverD", "Car2", "1.8", ""],
    ]


def test_parse_leaderboards_sort_and_csv(tmp_path, monkeypatch):
    monkeypatch.setattr(bl, "DATA_DIR", tmp_path.as_posix())

    rows = sample_rows()
    lines = bl.parse_leaderboards(rows, output_csv=True)

    start = lines.index("Driver career best:") + 1
    end = lines.index("", start)
    times = [float(line.split(": ")[1]) for line in lines[start:end]]
    assert times == sorted(times)

    driver_best_csv = tmp_path / "driver_best.csv"
    assert driver_best_csv.exists()
    with driver_best_csv.open(encoding="utf-8") as fh:
        reader = list(csv.reader(fh))
    csv_times = [float(r[1]) for r in reader[1:]]
    assert csv_times == sorted(csv_times)

    championship_csv = tmp_path / "championship_entries.csv"
    assert championship_csv.exists()
