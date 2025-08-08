import datetime as dt
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "world_info"))
import analytics  # type: ignore


def test_calculate_stats_timezone(monkeypatch):
    class DummyDateTime(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return dt.datetime(2024, 1, 2, 1, 0, tzinfo=tz)

    monkeypatch.setattr(analytics.dt, "datetime", DummyDateTime)

    tz = dt.timezone(dt.timedelta(hours=2))
    worlds = [
        {"publicationDate": "2024-01-01T22:30:00Z"},
        {"publicationDate": "2024-01-01T21:00:00Z"},
        {"publicationDate": "2024-01-02T01:30:00Z"},
    ]

    stats = analytics._calculate_stats(worlds, tzinfo=tz)
    assert stats["date"] == "2024/01/02"
    assert stats["total_worlds"] == 3
    assert stats["new_worlds_today"] == 2
