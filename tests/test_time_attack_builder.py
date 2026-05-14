import json
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from time_attack_tool.build_artifacts import build_artifacts  # noqa: E402
from time_attack_tool.io_utils import normalize_table_source  # noqa: E402
from time_attack_tool.models import format_lap_time_ms, parse_lap_time_to_ms  # noqa: E402


def test_parse_and_format_lap_time():
    assert parse_lap_time_to_ms("1:23.456") == 83_456
    assert parse_lap_time_to_ms("59.001") == 59_001
    assert format_lap_time_ms(83_456) == "1:23.456"
    assert format_lap_time_ms(59_001) == "59.001"


def test_build_artifacts_from_sample_csv(tmp_path):
    approved_records = ROOT / "time_attack_tool" / "examples" / "approved_records.sample.csv"
    submissions = ROOT / "time_attack_tool" / "examples" / "submissions.sample.csv"

    result = build_artifacts(
        approved_records_path=str(approved_records),
        submissions_path=str(submissions),
        output_dir=str(tmp_path),
    )

    assert result["record_count"] == 5

    leaderboards = json.loads((tmp_path / "leaderboards.json").read_text(encoding="utf-8"))
    downhill = leaderboards["by_route"]["route_akina_downhill"]
    uphill = leaderboards["by_route"]["route_akina_uphill"]

    assert downhill["fastest_overall"]["record_id"] == "rec_002"
    assert downhill["top_records"][0]["primary_badge"] == "TR"
    assert downhill["top_records"][1]["primary_badge"] == "PR"
    assert downhill["top_records"][2]["primary_badge"] == "CR"
    assert uphill["fastest_overall"]["record_id"] == "rec_005"
    assert leaderboards["record_count"] == 4

    review_summary = json.loads((tmp_path / "review_summary.json").read_text(encoding="utf-8"))
    assert review_summary["by_status"]["submitted"] == 1
    assert review_summary["by_status"]["needs_info"] == 1
    assert len(review_summary["pending_items"]) == 2


def test_normalize_google_sheet_url_with_gid():
    url = "https://docs.google.com/spreadsheets/d/test-sheet-id/edit#gid=12345"
    normalized = normalize_table_source(url)
    assert normalized == (
        "https://docs.google.com/spreadsheets/d/test-sheet-id/gviz/tq?tqx=out%3Acsv&gid=12345"
    )


def test_build_artifacts_from_google_sheet_url(monkeypatch, tmp_path):
    approved_records_url = "https://docs.google.com/spreadsheets/d/test-sheet-id/edit#gid=111"
    submissions_url = "https://docs.google.com/spreadsheets/d/test-sheet-id/edit#gid=222"
    approved_records = (
        "record_id,submission_id,source_type,review_status,record_tier,racer_id,"
        "racer_display_name,vehicle_id,vehicle_display_name,track_variant_id,"
        "track_variant_name,track_route_id,track_route_name,lap_time_text\n"
        "rec_100,sub_100,google_form,approved,qualified,racer_100,Zed,veh_100,FD3S,"
        "var_test,Test Variant,route_test,Forward,58.321\n"
    )
    submissions = (
        "submission_id,review_status,racer_name_input,track_input,vehicle_input,lap_time_text\n"
        "sub_100,approved,Zed,Test Variant,FD3S,58.321\n"
    )

    class FakeResponse:
        def __init__(self, payload: str):
            self._payload = payload.encode("utf-8")
            self.headers = SimpleNamespace(get=lambda _name, default="": "text/csv")

        def read(self):
            return self._payload

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(url):
        if "gid=111" in url:
            return FakeResponse(approved_records)
        if "gid=222" in url:
            return FakeResponse(submissions)
        raise AssertionError(f"unexpected url: {url}")

    from time_attack_tool import io_utils

    monkeypatch.setattr(io_utils.urllib.request, "urlopen", fake_urlopen)

    result = build_artifacts(
        approved_records_path=approved_records_url,
        submissions_path=submissions_url,
        output_dir=str(tmp_path),
        approved_records_gid="111",
        submissions_gid="222",
    )

    assert result["record_count"] == 1
    leaderboards = json.loads((tmp_path / "leaderboards.json").read_text(encoding="utf-8"))
    assert leaderboards["by_route"]["route_test"]["fastest_overall"]["record_id"] == "rec_100"
