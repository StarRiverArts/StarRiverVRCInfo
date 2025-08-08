import sys
from pathlib import Path
import urllib.error

sys.path.append(str(Path(__file__).resolve().parent.parent / "track_results"))
import fetch_sheet  # noqa: E402


def test_fetch_sheet_retries(monkeypatch):
    calls = {"count": 0}

    def fake_urlopen(url, timeout=None):
        calls["count"] += 1
        raise urllib.error.URLError("timed out")

    monkeypatch.setattr(fetch_sheet.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(fetch_sheet.time, "sleep", lambda _: None)

    result = fetch_sheet.fetch_sheet(timeout=1)

    assert calls["count"] == 3
    assert isinstance(result, str)
    assert "Failed to fetch data" in result
