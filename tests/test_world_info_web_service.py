import datetime as dt
import json
import shutil
import uuid
from pathlib import Path

from openpyxl import Workbook

import world_info_web.backend.service as service_module
from world_info_web.backend.service import WorldInfoService


def _write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_workbook(path: Path, rows: list[list[object]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()
    sheet = workbook.active
    for row in rows:
        sheet.append(row)
    workbook.save(path)


def _make_case_dir(name: str) -> Path:
    root = Path.cwd() / ".tmp_pytest" / f"{name}_{uuid.uuid4().hex}"
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    return root


def test_manual_world_search_stores_tag_results(monkeypatch):
    repo_root = _make_case_dir("manual_world_search") / "repo"
    service = WorldInfoService(repo_root=repo_root, app_root=repo_root / "world_info_web")
    calls = []

    def fake_search_worlds_query(**kwargs):
        calls.append(kwargs)
        return [
            {
                "id": "wrld_spacejam",
                "name": "Space Jam Event",
                "authorId": "usr_event",
                "authorName": "VRChat",
                "visits": 100,
                "favorites": 10,
                "heat": 8,
                "popularity": 12,
                "tags": ["admin_spacejam3"],
            }
        ]

    monkeypatch.setattr(service_module, "search_worlds_query", fake_search_worlds_query)

    result = service.search_worlds(
        tags="admin_spacejam3",
        sort="heat",
        active=True,
        limit=25,
        source_name="spacejam3",
    )

    assert result["count"] == 1
    assert result["source"] == "db:manual:world_search:spacejam3"
    assert calls[0]["tags"] == ["admin_spacejam3"]
    assert calls[0]["sort"] == "heat"
    assert calls[0]["active"] is True
    assert calls[0]["limit"] == 25


def test_world_search_job_runs_with_tags(monkeypatch):
    repo_root = _make_case_dir("world_search_job") / "repo"
    app_root = repo_root / "world_info_web"
    _write_json(
        app_root / "config" / "sync_jobs.json",
        {
            "spacejam3": {
                "label": "Space Jam 3",
                "type": "world_search",
                "source_key": "job:spacejam3",
                "tags": ["admin_spacejam3"],
                "sort": "popularity",
                "active": True,
                "limit": 50,
            }
        },
    )
    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    calls = []

    def fake_search_worlds_query(**kwargs):
        calls.append(kwargs)
        return [
            {
                "id": "wrld_event",
                "name": "Event World",
                "authorId": "usr_event",
                "visits": 200,
                "favorites": 20,
                "tags": ["admin_spacejam3"],
            }
        ]

    monkeypatch.setattr(service_module, "search_worlds_query", fake_search_worlds_query)

    result = service.run_job("spacejam3")

    assert result["job_key"] == "spacejam3"
    assert result["count"] == 1
    assert calls[0]["tags"] == ["admin_spacejam3"]
    assert calls[0]["active"] is True
    assert calls[0]["sort"] == "popularity"


def test_world_search_job_does_not_send_null_optional_filters(monkeypatch):
    repo_root = _make_case_dir("world_search_job_nulls") / "repo"
    app_root = repo_root / "world_info_web"
    _write_json(
        app_root / "config" / "sync_jobs.json",
        {
            "spacejam3": {
                "label": "Space Jam 3",
                "type": "world_search",
                "source_key": "job:spacejam3",
                "tags": ["admin_spacejam3"],
                "release_status": None,
                "platform": None,
            }
        },
    )
    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    calls = []

    def fake_search_worlds_query(**kwargs):
        calls.append(kwargs)
        return [{"id": "wrld_event", "name": "Event World", "visits": 1, "tags": ["admin_spacejam3"]}]

    monkeypatch.setattr(service_module, "search_worlds_query", fake_search_worlds_query)

    service.run_job("spacejam3")

    assert calls[0]["release_status"] is None
    assert calls[0]["platform"] is None


def test_topic_crud_and_active_filter():
    repo_root = _make_case_dir("topic_crud") / "repo"
    app_root = repo_root / "world_info_web"
    service = WorldInfoService(repo_root=repo_root, app_root=app_root)

    topic = service.upsert_topic(
        topic_key="event_view",
        label="Event View",
        rules=[{"type": "tag", "value": "admin_spacejam3"}],
    )

    assert topic["topic_key"] == "event_view"
    assert service.list_topics()[0]["topic_key"] == "event_view"

    hidden = service.set_topic_active("event_view", False)
    assert hidden["is_active"] is False
    assert service.list_topics() == []
    assert service.list_topics(include_inactive=True)[0]["topic_key"] == "event_view"

    result = service.delete_topic("event_view")
    assert result["status"] == "deleted"
    assert service.list_topics(include_inactive=True) == []


def test_delete_job_removes_matching_topic_config():
    repo_root = _make_case_dir("delete_job_topic") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    topics_path = app_root / "config" / "topics.json"
    _write_json(
        jobs_path,
        {
            "default": {
                "label": "default",
                "type": "world_search",
                "source_key": "job:default",
                "tags": ["admin_spacejam3"],
            }
        },
    )
    _write_json(
        topics_path,
        {
            "default": {
                "label": "default",
                "rules": [{"type": "source", "value": "db:job:default"}],
            }
        },
    )
    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path, topics_path=topics_path)

    result = service.delete_job("default")

    assert result == {"status": "deleted", "job_key": "default", "topic_deleted": True}
    assert "default" not in json.loads(jobs_path.read_text(encoding="utf-8"))
    assert "default" not in json.loads(topics_path.read_text(encoding="utf-8"))
    assert service.list_topics(include_inactive=True) == []


def test_service_reads_legacy_sources_and_history():
    repo_root = _make_case_dir("service_read") / "repo"
    app_root = repo_root / "world_info_web"

    _write_json(
        repo_root / "world_info" / "scraper" / "raw_worlds.json",
        [
            {
                "id": "wrld_1",
                "name": "Alpha",
                "visits": 100,
                "favorites": 20,
                "updated_at": "2025-08-01T00:00:00Z",
                "publicationDate": "2025-07-01T00:00:00Z",
                "tags": ["featured"],
            },
            {
                "id": "wrld_1",
                "name": "Alpha older",
                "visits": 80,
                "favorites": 10,
            },
        ],
    )
    _write_json(
        repo_root / "world_info" / "scraper" / "history.json",
        {
            "wrld_1": [
                {"timestamp": 1722470400, "name": "Alpha", "visits": 10, "favorites": 2},
                {"timestamp": 1722556800, "name": "Alpha", "visits": 15, "favorites": 3},
            ]
        },
    )
    _write_workbook(
        repo_root / "world_info" / "scraper" / "TaiwanWorlds.xlsx",
        [
            [
                "爬取日期",
                "世界名稱",
                "世界ID",
                "發布日期",
                "最後更新",
                "瀏覽人次",
                "大小",
                "收藏次數",
                "熱度",
                "人氣",
                "實驗室到發布",
                "瀏覽蒐藏比",
                "距離上次更新",
                "已發布",
                "人次發布比",
            ],
            [
                "2025/08/04",
                "Workbook World",
                "wrld_2",
                "2025-08-01T00:00:00Z",
                "2025-08-03T00:00:00Z",
                55,
                24,
                12,
                1,
                5,
                2,
                "21.8%",
                1,
                "public",
                18.3,
            ],
        ],
    )

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)

    legacy_raw = service.load_worlds("legacy-raw")
    legacy_taiwan = service.load_worlds("legacy-taiwan")
    history = service.load_history("wrld_1")["wrld_1"]
    sources = service.list_sources()

    assert len(legacy_raw) == 1
    assert legacy_raw[0]["name"] == "Alpha"
    assert legacy_raw[0]["metrics"]["favorite_rate"] == 20.0
    assert legacy_taiwan[0]["release_status"] == "public"
    assert legacy_taiwan[0]["metrics"]["favorite_rate"] == 21.8
    assert len(history) == 2
    assert any(item["key"] == "legacy-taiwan" and item["count"] == 1 for item in sources)


def test_load_history_can_be_scoped_to_db_source():
    repo_root = _make_case_dir("service_history_source_scope") / "repo"
    app_root = repo_root / "world_info_web"
    service = WorldInfoService(repo_root=repo_root, app_root=app_root)

    alpha_run = service.storage.create_run(
        source_key="job:alpha",
        job_key="alpha",
        trigger_type="manual",
        query_label="Alpha",
        started_at="2026-05-01T00:00:00+00:00",
    )
    service.storage.insert_world_snapshots(
        run_id=alpha_run,
        source_key="job:alpha",
        fetched_at="2026-05-01T00:00:00+00:00",
        worlds=[
            {
                "id": "wrld_1",
                "name": "Scoped World",
                "visits": 100,
                "favorites": 10,
                "tags": [],
            },
            {
                "id": "wrld_1",
                "name": "Scoped World",
                "visits": 120,
                "favorites": 11,
                "fetched_at": "2026-05-03T00:00:00+00:00",
                "tags": [],
            },
        ],
    )

    beta_run = service.storage.create_run(
        source_key="job:beta",
        job_key="beta",
        trigger_type="manual",
        query_label="Beta",
        started_at="2026-05-02T00:00:00+00:00",
    )
    service.storage.insert_world_snapshots(
        run_id=beta_run,
        source_key="job:beta",
        fetched_at="2026-05-02T00:00:00+00:00",
        worlds=[
            {
                "id": "wrld_1",
                "name": "Scoped World",
                "visits": 999,
                "favorites": 77,
                "tags": [],
            }
        ],
    )

    combined = service.load_history("wrld_1")["wrld_1"]
    scoped = service.load_history("wrld_1", source="db:job:alpha")["wrld_1"]

    assert [item["visits"] for item in combined] == [100, 999, 120]
    assert [item["visits"] for item in scoped] == [100, 120]
    assert all(item["favorites"] in {10, 11} for item in scoped)


def test_legacy_workbook_non_status_column_does_not_pollute_release_status():
    repo_root = _make_case_dir("service_legacy_shift") / "repo"
    app_root = repo_root / "world_info_web"

    _write_workbook(
        repo_root / "world_info" / "scraper" / "StarRiverArts.xlsx",
        [
            [
                "fetched",
                "name",
                "world_id",
                "published",
                "updated",
                "visits",
                "capacity",
                "favorites",
                "heat",
                "popularity",
                "labs_days",
                "favorite_rate",
                "days_since_update",
                "days_since_publication",
                "visits_per_day",
            ],
            [
                "2026/04/08",
                "Legacy StarRiver",
                "wrld_legacy",
                "2024-07-08T21:05:28.545Z",
                "2024-07-11T08:38:23.384Z",
                1055,
                8,
                52,
                0,
                5,
                "263天",
                "4.93%",
                "638天",
                "905天",
                1.65,
            ],
        ],
    )

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    legacy = service.load_worlds("legacy-starriver")

    assert legacy[0]["release_status"] is None
    assert legacy[0]["metrics"]["days_since_publication"] == 905


def test_self_check_reports_missing_and_duplicate_data():
    repo_root = _make_case_dir("service_check") / "repo"
    app_root = repo_root / "world_info_web"

    _write_json(
        repo_root / "world_info" / "scraper" / "raw_worlds.json",
        [
            {"id": "wrld_1", "name": "Alpha"},
            {"id": "wrld_1", "name": ""},
        ],
    )

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    result = service.run_self_check()

    assert result["status"] == "warning"
    assert any("duplicate world IDs" in warning for warning in result["warnings"])
    assert any("Missing legacy source" in warning for warning in result["warnings"])
    assert any("Missing local auth headers file" in warning for warning in result["warnings"])


def test_run_job_writes_to_database(monkeypatch):
    repo_root = _make_case_dir("service_job") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    topics_path = app_root / "config" / "topics.json"
    _write_json(
        jobs_path,
        {
            "taiwan": {
                "label": "Taiwan Sync",
                "type": "keywords",
                "source_key": "job:taiwan",
                "keywords": ["Taiwan"],
                "limit_per_keyword": 20,
            }
        },
    )
    _write_json(
        topics_path,
        {
            "taiwan": {
                "label": "Taiwan",
                "sort_order": 10,
                "rules": [{"type": "keyword", "value": "Taiwan"}],
            },
            "racing": {
                "label": "Racing",
                "sort_order": 20,
                "rules": [{"type": "keyword", "value": "Race"}],
            },
        },
    )

    def fake_fetch_worlds(*, keyword=None, user_id=None, limit=20, delay=1.0, headers=None):
        assert keyword == "Taiwan"
        return [
            {
                "id": "wrld_1",
                "name": "Taiwan World",
                "visits": 120,
                "favorites": 24,
                "updated_at": "2025-08-01T00:00:00Z",
                "publicationDate": "2025-07-15T00:00:00Z",
                "tags": ["taiwan"],
            }
        ]

    monkeypatch.setattr(service_module, "fetch_worlds", fake_fetch_worlds)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path, topics_path=topics_path)
    result = service.run_job("taiwan")

    assert result["source"] == "db:job:taiwan"
    assert result["count"] == 1
    assert result["items"][0]["name"] == "Taiwan World"
    assert service.load_worlds("db:job:taiwan")[0]["visits"] == 120
    assert service.list_runs(limit=5)[0]["source"] == "db:job:taiwan"
    assert service.list_topics()[0]["topic_key"] == "taiwan"
    assert service.load_topic_worlds("taiwan")[0]["name"] == "Taiwan World"
    assert service.get_topic_dashboard("taiwan")["summary"]["world_count"] == 1


def test_run_job_accepts_request_auth_inputs(monkeypatch):
    repo_root = _make_case_dir("service_job_auth") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    _write_json(
        jobs_path,
        {
            "starriver": {
                "label": "StarRiver Sync",
                "type": "user",
                "source_key": "job:starriver",
                "user_id": "usr_test",
                "limit": 20,
            }
        },
    )

    def fake_load_headers(cookie=None, username=None, password=None):
        return {"Cookie": cookie or "", "Authorization": f"{username}:{password}" if username and password else ""}

    def fake_fetch_worlds(*, keyword=None, user_id=None, limit=20, delay=1.0, headers=None):
        assert user_id == "usr_test"
        assert headers["Cookie"] == "auth=test"
        return [{"id": "wrld_1", "name": "Auth World", "visits": 1, "favorites": 1}]

    monkeypatch.setattr(service_module, "_load_headers", fake_load_headers)
    monkeypatch.setattr(service_module, "fetch_worlds", fake_fetch_worlds)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    result = service.run_job("starriver", cookie="auth=test")

    assert result["source"] == "db:job:starriver"
    assert result["count"] == 1


def test_search_keyword_reports_missing_visit_warnings(monkeypatch):
    repo_root = _make_case_dir("service_missing_visits") / "repo"
    app_root = repo_root / "world_info_web"

    def fake_fetch_worlds(*, keyword=None, user_id=None, limit=20, delay=1.0, headers=None):
        return [{"id": "wrld_missing", "name": "Missing Visits", "visits": None, "favorites": 2}]

    def fake_enrich_visits(worlds, headers=None, delay=0.5):
        return worlds

    monkeypatch.setattr(service_module, "fetch_worlds", fake_fetch_worlds)
    monkeypatch.setattr(service_module, "enrich_visits", fake_enrich_visits)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    result = service.search_keyword(keyword="Taiwan")

    assert result["count"] == 1
    assert result["meta"]["missing_visits_before_enrich"] == 1
    assert result["meta"]["missing_visits_after_enrich"] == 1
    assert any("still have no visits" in warning for warning in result["warnings"])


def test_check_auth_status_uses_cookie_session_validation(monkeypatch):
    repo_root = _make_case_dir("service_auth_status") / "repo"
    app_root = repo_root / "world_info_web"

    monkeypatch.setattr(
        service_module,
        "vrchat_check_session",
        lambda cookie: {"ok": True, "user": {"id": "usr_test", "displayName": "StarRiver"}},
    )

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    result = service.check_auth_status(cookie="auth=test")

    assert result["status"] == "ok"
    assert result["mode"] == "cookie"
    assert result["user"]["display_name"] == "StarRiver"


def test_persist_and_clear_server_auth_headers():
    repo_root = _make_case_dir("service_persist_server_auth") / "repo"
    app_root = repo_root / "world_info_web"
    service = WorldInfoService(repo_root=repo_root, app_root=app_root)

    saved = service.persist_server_auth(cookie="auth=test; twoFactorAuth=abc")
    headers_path = repo_root / "world_info" / "scraper" / "headers.json"

    assert saved["status"] == "saved"
    assert headers_path.exists()
    payload = json.loads(headers_path.read_text(encoding="utf-8"))
    assert payload["Cookie"] == "auth=test; twoFactorAuth=abc"

    cleared = service.clear_server_auth()

    assert cleared["status"] == "cleared"
    assert headers_path.exists()
    payload = json.loads(headers_path.read_text(encoding="utf-8"))
    assert payload == {}


def test_search_keyword_preserves_missing_visits_as_null(monkeypatch):
    repo_root = _make_case_dir("service_preserve_null_visits") / "repo"
    app_root = repo_root / "world_info_web"

    monkeypatch.setattr(
        service_module,
        "fetch_worlds",
        lambda **kwargs: [{"id": "wrld_null", "name": "Null Visits", "visits": None, "favorites": 4}],
    )
    monkeypatch.setattr(service_module, "enrich_visits", lambda worlds, headers=None, delay=0.0: worlds)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    result = service.search_keyword(keyword="Null Test")

    assert result["items"][0]["visits"] is None
    assert result["items"][0]["favorites"] == 4


def test_fixed_keyword_search_dedupes_overlapping_worlds_before_enrich(monkeypatch):
    repo_root = _make_case_dir("service_fixed_keyword_dedupe") / "repo"
    app_root = repo_root / "world_info_web"
    captured = {}

    def fake_fetch_worlds(*, keyword=None, user_id=None, limit=20, delay=1.0, headers=None):
        payloads = {
            "cvs": [
                {
                    "id": "wrld_overlap",
                    "name": "Project CVS",
                    "visits": None,
                    "favorites": 10,
                    "updated_at": "2026-04-20T00:00:00Z",
                    "tags": ["cvs"],
                }
            ],
            "cvs2": [
                {
                    "id": "wrld_overlap",
                    "name": "Project CVS 2",
                    "visits": 120,
                    "favorites": 12,
                    "updated_at": "2026-04-21T00:00:00Z",
                    "tags": ["cvs2"],
                }
            ],
        }
        return payloads.get(keyword or "", [])

    def fake_enrich_visits(worlds, headers=None, delay=0.0):
        captured["called"] = True
        return worlds

    monkeypatch.setattr(service_module, "fetch_worlds", fake_fetch_worlds)
    monkeypatch.setattr(service_module, "enrich_visits", fake_enrich_visits)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    result = service.search_fixed_keywords(keywords=["cvs", "cvs2"], source_name="Racing")

    assert captured.get("called") is None
    assert len(result["items"]) == 1
    assert result["items"][0]["id"] == "wrld_overlap"
    assert result["items"][0]["visits"] == 120
    assert sorted(result["items"][0]["tags"]) == ["cvs", "cvs2"]
    assert result["meta"]["duplicates_merged_before_enrich"] == 1


def test_update_and_delete_world_record(monkeypatch):
    repo_root = _make_case_dir("service_edit_delete") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    _write_json(
        jobs_path,
        {
            "taiwan": {
                "label": "Taiwan Sync",
                "type": "keywords",
                "source_key": "job:taiwan",
                "keywords": ["Taiwan"],
                "limit_per_keyword": 20,
            }
        },
    )

    monkeypatch.setattr(
        service_module,
        "fetch_worlds",
        lambda **kwargs: [{"id": "wrld_edit", "name": "Editable", "visits": None, "favorites": 3}],
    )
    monkeypatch.setattr(service_module, "enrich_visits", lambda worlds, headers=None, delay=0.0: worlds)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    service.run_job("taiwan")

    updated = service.update_world_record(
        source="db:job:taiwan",
        world_id="wrld_edit",
        changes={"visits": 120, "favorites": 8, "name": "Editable Patched"},
    )
    assert updated["world"]["visits"] == 120
    assert updated["world"]["favorites"] == 8

    deleted = service.delete_world_record(source="db:job:taiwan", world_id="wrld_edit")
    assert deleted["status"] == "deleted"
    assert service.load_worlds("db:job:taiwan") == []


def test_update_world_record_persists_portal_links_property(monkeypatch):
    repo_root = _make_case_dir("service_world_portal_links") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    _write_json(
        jobs_path,
        {
            "starriver": {
                "label": "StarRiver Sync",
                "type": "user",
                "source_key": "job:starriver",
                "user_id": "usr_0673194d-712d-4b5d-8167-1f03ed3233cb",
                "limit": 20,
            }
        },
    )

    monkeypatch.setattr(
        service_module,
        "fetch_worlds",
        lambda **kwargs: [{
            "id": "wrld_edit",
            "name": "Editable",
            "authorId": "usr_0673194d-712d-4b5d-8167-1f03ed3233cb",
            "authorName": "StarRiver Arts",
            "visits": 120,
            "favorites": 8,
        }],
    )
    monkeypatch.setattr(service_module, "enrich_visits", lambda worlds, headers=None, delay=0.0: worlds)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    service.run_job("starriver")

    updated = service.update_world_record(
        source="db:job:starriver",
        world_id="wrld_edit",
        changes={"portal_links": "wrld_a,\nhttps://vrchat.com/home/world/wrld_b"},
    )

    loaded = service.load_worlds("db:job:starriver")
    properties_path = app_root / "config" / "world_properties.json"
    payload = json.loads(properties_path.read_text(encoding="utf-8"))

    assert updated["world"]["portal_links"] == ["wrld_a", "wrld_b"]
    assert updated["portal_links_count"] == 2
    assert updated["portal_links_saved_to"] == "world_info_web/config/world_properties.json"
    assert loaded[0]["portal_links"] == ["wrld_a", "wrld_b"]
    assert payload["wrld_edit"]["portal_links"] == ["wrld_a", "wrld_b"]


def test_build_world_graph_includes_portal_edges(monkeypatch):
    repo_root = _make_case_dir("service_graph_portal_links") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    _write_json(
        jobs_path,
        {
            "starriver": {
                "label": "StarRiver Sync",
                "type": "user",
                "source_key": "job:starriver",
                "user_id": "usr_0673194d-712d-4b5d-8167-1f03ed3233cb",
                "limit": 20,
            }
        },
    )

    monkeypatch.setattr(
        service_module,
        "fetch_worlds",
        lambda **kwargs: [
            {
                "id": "wrld_hub",
                "name": "Hub",
                "authorId": "usr_0673194d-712d-4b5d-8167-1f03ed3233cb",
                "authorName": "StarRiver Arts",
                "visits": 320,
                "favorites": 18,
            },
            {
                "id": "wrld_room",
                "name": "Room",
                "authorId": "usr_0673194d-712d-4b5d-8167-1f03ed3233cb",
                "authorName": "StarRiver Arts",
                "visits": 150,
                "favorites": 9,
            },
        ],
    )
    monkeypatch.setattr(service_module, "enrich_visits", lambda worlds, headers=None, delay=0.0: worlds)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    service.run_job("starriver")
    service.update_world_record(
        source="db:job:starriver",
        world_id="wrld_hub",
        changes={"portal_links": "wrld_room"},
    )

    graph = service.build_world_graph(source="db:job:starriver", edge_types=["portal"], max_nodes=20)
    node_map = {node["id"]: node for node in graph["nodes"]}

    assert node_map["wrld_hub"]["portal_links"] == ["wrld_room"]
    assert any(
        edge["type"] == "portal_link"
        and {edge["source"], edge["target"]} == {"wrld_hub", "wrld_room"}
        for edge in graph["edges"]
    )


def test_build_world_graph_expands_portal_targets_beyond_max_nodes(monkeypatch):
    repo_root = _make_case_dir("service_graph_portal_expand") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    _write_json(
        jobs_path,
        {
            "starriver": {
                "label": "StarRiver Sync",
                "type": "user",
                "source_key": "job:starriver",
                "user_id": "usr_0673194d-712d-4b5d-8167-1f03ed3233cb",
                "limit": 20,
            }
        },
    )

    monkeypatch.setattr(
        service_module,
        "fetch_worlds",
        lambda **kwargs: [
            {
                "id": "wrld_hub",
                "name": "Hub",
                "authorId": "usr_0673194d-712d-4b5d-8167-1f03ed3233cb",
                "authorName": "StarRiver Arts",
                "visits": 500,
                "favorites": 18,
            },
            {
                "id": "wrld_room",
                "name": "Room",
                "authorId": "usr_0673194d-712d-4b5d-8167-1f03ed3233cb",
                "authorName": "StarRiver Arts",
                "visits": 10,
                "favorites": 1,
            },
        ],
    )
    monkeypatch.setattr(service_module, "enrich_visits", lambda worlds, headers=None, delay=0.0: worlds)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    service.run_job("starriver")
    service.update_world_record(
        source="db:job:starriver",
        world_id="wrld_hub",
        changes={"portal_links": "wrld_room"},
    )

    graph = service.build_world_graph(source="db:job:starriver", edge_types=["portal"], max_nodes=1)
    node_ids = {node["id"] for node in graph["nodes"]}

    assert graph["base_node_count"] == 1
    assert graph["portal_expanded_nodes"] == 1
    assert node_ids == {"wrld_hub", "wrld_room"}
    assert any(edge["type"] == "portal_link" for edge in graph["edges"])


def test_job_blacklist_adds_entry_and_removes_taiwan_record(monkeypatch):
    repo_root = _make_case_dir("service_job_blacklist") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    _write_json(
        jobs_path,
        {
            "taiwan": {
                "label": "Taiwan Sync",
                "type": "keywords",
                "source_key": "job:taiwan",
                "keywords": ["Taiwan"],
                "blacklist_file": "world_info/blacklist_taiwan.txt",
                "include_user_ids_file": "world_info/taiwan_creator_whitelist.txt",
                "limit_per_keyword": 20,
            }
        },
    )
    monkeypatch.setattr(
        service_module,
        "fetch_worlds",
        lambda **kwargs: [{"id": "wrld_block_me", "name": "Block Me", "visits": 30, "favorites": 3}],
    )
    monkeypatch.setattr(service_module, "enrich_visits", lambda worlds, headers=None, delay=0.0: worlds)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    service.run_job("taiwan")

    added = service.add_job_blacklist_entry(job_key="taiwan", world_id="wrld_block_me")
    listed = service.list_job_blacklist("taiwan")

    assert added["status"] == "added"
    assert added["removed_from_db"] == 1
    assert "wrld_block_me" in listed["items"]
    assert service.load_worlds("db:job:taiwan") == []


def test_taiwan_keyword_job_includes_creator_whitelist(monkeypatch):
    repo_root = _make_case_dir("service_taiwan_creator_whitelist") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    _write_json(
        jobs_path,
        {
            "taiwan": {
                "label": "Taiwan Sync",
                "type": "keywords",
                "source_key": "job:taiwan",
                "keywords": ["Taiwan"],
                "include_user_ids_file": "world_info/taiwan_creator_whitelist.txt",
                "limit_per_keyword": 20,
            }
        },
    )
    whitelist_path = repo_root / "world_info" / "taiwan_creator_whitelist.txt"
    whitelist_path.parent.mkdir(parents=True, exist_ok=True)
    whitelist_path.write_text("usr_creator\n", encoding="utf-8")

    def fake_fetch_worlds(*, keyword=None, user_id=None, limit=20, delay=1.0, headers=None):
        if keyword == "Taiwan":
            return [{"id": "wrld_keyword", "name": "Taiwan Named", "visits": 20, "favorites": 2}]
        if user_id == "usr_creator":
            return [{"id": "wrld_creator", "name": "Hidden Taiwan World", "authorId": "usr_creator", "visits": 40, "favorites": 6}]
        return []

    monkeypatch.setattr(service_module, "fetch_worlds", fake_fetch_worlds)
    monkeypatch.setattr(service_module, "enrich_visits", lambda worlds, headers=None, delay=0.0: worlds)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    result = service.run_job("taiwan")

    ids = {item["id"] for item in result["items"]}
    assert ids == {"wrld_keyword", "wrld_creator"}


def test_job_display_keeps_keyword_results_when_creator_whitelist_exists(monkeypatch):
    repo_root = _make_case_dir("service_job_display_creator_whitelist") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    _write_json(
        jobs_path,
        {
            "racing": {
                "label": "Racing Sync",
                "type": "keywords",
                "source_key": "job:racing",
                "keywords": ["cvs"],
                "include_user_ids_file": "world_info/racing_creator_whitelist.txt",
                "limit_per_keyword": 20,
            }
        },
    )
    whitelist_path = repo_root / "world_info" / "racing_creator_whitelist.txt"
    whitelist_path.parent.mkdir(parents=True, exist_ok=True)
    whitelist_path.write_text("usr_creator\n", encoding="utf-8")

    def fake_fetch_worlds(*, keyword=None, user_id=None, limit=20, delay=1.0, headers=None):
        if keyword == "cvs":
            return [
                {"id": "wrld_keyword", "name": "CVS Speedway", "authorId": "usr_keyword", "visits": 20, "favorites": 2}
            ]
        if user_id == "usr_creator":
            return [
                {"id": "wrld_creator", "name": "Creator Raceway", "authorId": "usr_creator", "visits": 40, "favorites": 6}
            ]
        return []

    monkeypatch.setattr(service_module, "fetch_worlds", fake_fetch_worlds)
    monkeypatch.setattr(service_module, "enrich_visits", lambda worlds, headers=None, delay=0.0: worlds)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    service.run_job("racing")

    ids = {item["id"] for item in service.load_worlds("db:job:racing")}
    assert ids == {"wrld_keyword", "wrld_creator"}


def test_creator_whitelist_entry_can_be_added_and_removed():
    repo_root = _make_case_dir("service_creator_whitelist_edit") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    _write_json(
        jobs_path,
        {
            "taiwan": {
                "label": "Taiwan Sync",
                "type": "keywords",
                "source_key": "job:taiwan",
                "keywords": ["Taiwan"],
                "include_user_ids_file": "world_info/taiwan_creator_whitelist.txt",
                "limit_per_keyword": 20,
            }
        },
    )

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    added = service.add_job_creator_whitelist_entry(job_key="taiwan", user_id="usr_creator")
    listed = service.list_job_creator_whitelist("taiwan")
    removed = service.remove_job_creator_whitelist_entry(job_key="taiwan", user_id="usr_creator")

    assert added["status"] == "added"
    assert "usr_creator" in listed["items"]
    assert removed["status"] == "removed"
    assert removed["items"] == []


def test_creator_blacklist_entry_can_be_added_and_removed():
    repo_root = _make_case_dir("service_creator_blacklist_edit") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    _write_json(
        jobs_path,
        {
            "taiwan": {
                "label": "Taiwan Sync",
                "type": "keywords",
                "source_key": "job:taiwan",
                "keywords": ["Taiwan"],
                "exclude_author_ids_file": "world_info/blacklist_taiwan_authors_cn.txt",
                "limit_per_keyword": 20,
            }
        },
    )

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    added = service.add_job_creator_blacklist_entry(job_key="taiwan", user_id="usr_creator")
    listed = service.list_job_creator_blacklist("taiwan")
    removed = service.remove_job_creator_blacklist_entry(job_key="taiwan", user_id="usr_creator")

    assert added["status"] == "added"
    assert "usr_creator" in listed["items"]
    assert removed["status"] == "removed"
    assert removed["items"] == []


def test_topic_source_rule_tracks_same_worlds_as_job_source(monkeypatch):
    repo_root = _make_case_dir("service_topic_source_rule") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    topics_path = app_root / "config" / "topics.json"
    _write_json(
        jobs_path,
        {
            "taiwan": {
                "label": "Zh keyword sync",
                "type": "keywords",
                "source_key": "job:taiwan",
                "keywords": ["Taiwan"],
                "limit_per_keyword": 20,
            }
        },
    )
    _write_json(
        topics_path,
        {
            "taiwan": {
                "label": "Zh",
                "sort_order": 30,
                "rules": [{"type": "source", "value": "db:job:taiwan"}],
            }
        },
    )

    monkeypatch.setattr(
        service_module,
        "fetch_worlds",
        lambda **kwargs: [{"id": "wrld_topic", "name": "Zh World", "visits": 50, "favorites": 7}],
    )
    monkeypatch.setattr(service_module, "enrich_visits", lambda worlds, headers=None, delay=0.0: worlds)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path, topics_path=topics_path)
    service.run_job("taiwan")

    topic_worlds = service.load_topic_worlds("taiwan")

    assert len(topic_worlds) == 1
    assert topic_worlds[0]["id"] == "wrld_topic"
    assert topic_worlds[0]["topic_matched_by"] == "source:db:job:taiwan"


def test_view_topic_recent_updated_matches_db_all(monkeypatch):
    repo_root = _make_case_dir("service_recent_updated_topic") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    topics_path = app_root / "config" / "topics.json"
    now = dt.datetime.now(dt.timezone.utc)
    recent_updated = (now - dt.timedelta(days=3)).isoformat()
    stale_updated = (now - dt.timedelta(days=70)).isoformat()
    _write_json(
        jobs_path,
        {
            "taiwan": {
                "label": "Zh keyword sync",
                "type": "keywords",
                "source_key": "job:taiwan",
                "keywords": ["Taiwan"],
                "limit_per_keyword": 20,
            }
        },
    )
    _write_json(
        topics_path,
        {
            "recent_updated": {
                "label": "Recent Updated",
                "topic_type": "view",
                "sort_order": 10,
                "rules": [{"type": "updated_within_days", "value": "30"}],
            }
        },
    )

    monkeypatch.setattr(
        service_module,
        "fetch_worlds",
        lambda **kwargs: [
            {"id": "wrld_recent", "name": "Recent", "visits": 30, "favorites": 5, "updated_at": recent_updated},
            {"id": "wrld_old", "name": "Old", "visits": 60, "favorites": 7, "updated_at": stale_updated},
        ],
    )
    monkeypatch.setattr(service_module, "enrich_visits", lambda worlds, headers=None, delay=0.0: worlds)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path, topics_path=topics_path)
    service.run_job("taiwan")

    topic = service.get_topic("recent_updated")
    topic_worlds = service.load_topic_worlds("recent_updated")

    assert topic["topic_type"] == "view"
    assert [item["id"] for item in topic_worlds] == ["wrld_recent"]


def test_world_matches_rule_supports_quality_thresholds_without_storage():
    service = object.__new__(WorldInfoService)
    world = {
        "id": "wrld_quality",
        "name": "Quality World",
        "visits": 150,
        "favorites": 18,
        "heat": 6,
        "popularity": 4,
        "metrics": {"favorite_rate": 12.0},
        "publication_date": "2026-05-01T00:00:00Z",
    }

    assert service._world_matches_rule(world, "visits_min", "100") is True
    assert service._world_matches_rule(world, "favorites_min", "12") is True
    assert service._world_matches_rule(world, "heat_min", "5") is True
    assert service._world_matches_rule(world, "popularity_min", "4") is True
    assert service._world_matches_rule(world, "favorite_rate_min", "8") is True
    assert service._world_matches_rule(world, "favorite_rate_min", "15") is False
    assert service._world_matches_rule(world, "visits_max", "300") is True
    assert service._world_matches_rule(world, "visits_max", "120") is False
    assert service._world_matches_rule(world, "favorites_max", "20") is True
    assert service._world_matches_rule(world, "favorites_max", "10") is False
    assert service._world_matches_rule(world, "heat_max", "8") is True
    assert service._world_matches_rule(world, "heat_max", "4") is False
    assert service._world_matches_rule(world, "popularity_max", "5") is True
    assert service._world_matches_rule(world, "popularity_max", "3") is False
    assert service._world_matches_rule(world, "favorite_rate_max", "15") is True
    assert service._world_matches_rule(world, "favorite_rate_max", "10") is False


def test_world_matches_rule_can_compute_favorite_rate_from_world_values():
    service = object.__new__(WorldInfoService)
    world = {
        "id": "wrld_computed",
        "name": "Computed Rate World",
        "visits": 200,
        "favorites": 20,
        "metrics": {},
    }

    assert service._world_matches_rule(world, "favorite_rate_min", "9") is True
    assert service._world_matches_rule(world, "favorite_rate_min", "11") is False


def test_load_collection_insights_exposes_monitor_analysis_without_storage():
    service = object.__new__(WorldInfoService)
    now = dt.datetime.now(dt.timezone.utc)
    recent_update = (now - dt.timedelta(days=4)).isoformat()
    old_update = (now - dt.timedelta(days=420)).isoformat()

    worlds = [
        {
            "id": "wrld_rising",
            "name": "Rising World",
            "author_id": "usr_creator_a",
            "author_name": "Creator A",
            "visits": 180,
            "favorites": 16,
            "heat": 7,
            "popularity": 6,
            "publication_date": (now - dt.timedelta(days=5)).isoformat(),
            "updated_at": recent_update,
        },
        {
            "id": "wrld_revive",
            "name": "Revive World",
            "author_id": "usr_creator_a",
            "author_name": "Creator A",
            "visits": 500,
            "favorites": 40,
            "heat": 5,
            "popularity": 4,
            "publication_date": (now - dt.timedelta(days=220)).isoformat(),
            "updated_at": recent_update,
        },
        {
            "id": "wrld_flat",
            "name": "Flat World",
            "author_id": "usr_creator_b",
            "author_name": "Creator B",
            "visits": 140,
            "favorites": 8,
            "heat": 2,
            "popularity": 2,
            "publication_date": (now - dt.timedelta(days=90)).isoformat(),
            "updated_at": (now - dt.timedelta(days=40)).isoformat(),
        },
    ]

    def ts(days_ago: int) -> int:
        return int((now - dt.timedelta(days=days_ago)).timestamp())

    history = {
        "wrld_rising": [
            {"timestamp": ts(14), "visits": 20, "favorites": 2, "updated_at": old_update},
            {"timestamp": ts(7), "visits": 60, "favorites": 5, "updated_at": old_update},
            {"timestamp": ts(1), "visits": 120, "favorites": 10, "updated_at": recent_update},
            {"timestamp": ts(0), "visits": 180, "favorites": 16, "updated_at": recent_update},
        ],
        "wrld_revive": [
            {"timestamp": ts(14), "visits": 400, "favorites": 32, "updated_at": old_update},
            {"timestamp": ts(7), "visits": 405, "favorites": 33, "updated_at": old_update},
            {"timestamp": ts(1), "visits": 430, "favorites": 35, "updated_at": recent_update},
            {"timestamp": ts(0), "visits": 500, "favorites": 40, "updated_at": recent_update},
        ],
        "wrld_flat": [
            {"timestamp": ts(14), "visits": 132, "favorites": 8, "updated_at": (now - dt.timedelta(days=40)).isoformat()},
            {"timestamp": ts(7), "visits": 135, "favorites": 8, "updated_at": (now - dt.timedelta(days=40)).isoformat()},
            {"timestamp": ts(1), "visits": 138, "favorites": 8, "updated_at": (now - dt.timedelta(days=40)).isoformat()},
            {"timestamp": ts(0), "visits": 140, "favorites": 8, "updated_at": (now - dt.timedelta(days=40)).isoformat()},
        ],
    }

    service.load_worlds = lambda source, **kwargs: worlds
    service.load_history = lambda *args, **kwargs: history

    payload = service.load_collection_insights(source="db:all", limit=5)

    assert payload["rising_now_leaderboard"][0]["id"] == "wrld_rising"
    assert payload["dormant_revival_leaderboard"][0]["id"] == "wrld_revive"
    assert payload["creator_momentum"][0]["author_name"] == "Creator A"
    assert payload["creator_momentum"][0]["lead_world_name"] in {"Rising World", "Revive World"}
    assert payload["summary"]["world_count"] == 3
    assert payload["summary"]["tracked_creators"] == 2
    assert payload["briefing"]["new_worlds"][0]["id"] == "wrld_rising"


def test_load_collection_insights_reuses_analysis_cache():
    service = object.__new__(WorldInfoService)
    now = dt.datetime.now(dt.timezone.utc)

    class FakeStorage:
        def __init__(self):
            self.cache = None

        def get_analysis_cache(self, scope_key):
            if self.cache and self.cache["scope_key"] == scope_key:
                return self.cache
            return None

        def upsert_analysis_cache(self, *, scope_key, scope_type, updated_at, payload, source_run_id=None):
            self.cache = {
                "scope_key": scope_key,
                "scope_type": scope_type,
                "updated_at": updated_at,
                "source_run_id": source_run_id,
                "payload": payload,
            }

    storage = FakeStorage()
    worlds = [
        {
            "id": "wrld_cached",
            "name": "Cached World",
            "author_id": "usr_cached",
            "author_name": "Cached Creator",
            "visits": 120,
            "favorites": 12,
            "heat": 4,
            "popularity": 3,
            "publication_date": (now - dt.timedelta(days=3)).isoformat(),
            "updated_at": (now - dt.timedelta(days=1)).isoformat(),
        }
    ]
    history = {
        "wrld_cached": [
            {"timestamp": int((now - dt.timedelta(days=7)).timestamp()), "visits": 40, "favorites": 4},
            {"timestamp": int((now - dt.timedelta(days=1)).timestamp()), "visits": 90, "favorites": 9},
            {"timestamp": int(now.timestamp()), "visits": 120, "favorites": 12},
        ]
    }
    calls = {"load_worlds": 0}

    def fake_load_worlds(source, **kwargs):
        calls["load_worlds"] += 1
        return worlds

    service.storage = storage
    service.load_worlds = fake_load_worlds
    service.load_history = lambda *args, **kwargs: history

    payload = service.load_collection_insights(source="db:all", limit=5, allow_cache=False)
    assert calls["load_worlds"] == 1
    assert storage.cache is not None
    assert payload["growth_leaderboard"][0]["id"] == "wrld_cached"
    assert payload["summary"]["world_count"] == 1

    service.load_worlds = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("cache should be used"))
    cached_payload = service.load_collection_insights(source="db:all", limit=5, allow_cache=True)
    assert cached_payload["growth_leaderboard"][0]["id"] == "wrld_cached"
    assert cached_payload["summary"]["world_count"] == 1


def test_load_collection_insights_uses_source_scoped_history():
    service = object.__new__(WorldInfoService)
    now = dt.datetime.now(dt.timezone.utc)
    calls = {}

    class FakeStorage:
        def get_analysis_cache(self, scope_key):
            return None

        def upsert_analysis_cache(self, **kwargs):
            calls["cache"] = kwargs

    worlds = [
        {
            "id": "wrld_scoped",
            "name": "Scoped World",
            "author_id": "usr_scoped",
            "author_name": "Scoped Creator",
            "visits": 220,
            "favorites": 22,
            "heat": 4,
            "popularity": 3,
            "publication_date": (now - dt.timedelta(days=7)).isoformat(),
            "updated_at": (now - dt.timedelta(days=2)).isoformat(),
        }
    ]
    history = {
        "wrld_scoped": [
            {"timestamp": int((now - dt.timedelta(days=7)).timestamp()), "visits": 100, "favorites": 10},
            {"timestamp": int(now.timestamp()), "visits": 220, "favorites": 22},
        ]
    }

    service.storage = FakeStorage()
    service.load_worlds = lambda source, **kwargs: worlds

    def fake_load_history(world_id=None, source=None):
        calls["history_source"] = source
        return history

    service.load_history = fake_load_history

    payload = service.load_collection_insights(source="db:job:Ch", limit=5, allow_cache=False)

    assert calls["history_source"] == "db:job:Ch"
    assert payload["summary"]["world_count"] == 1
    assert payload["growth_leaderboard"][0]["id"] == "wrld_scoped"


def test_scope_dashboard_payload_uses_source_scoped_history():
    service = object.__new__(WorldInfoService)
    calls = {}
    worlds = [
        {
            "id": "wrld_scoped",
            "name": "Scoped World",
            "visits": 220,
            "favorites": 22,
            "thumbnail_url": "https://example.com/thumb.png",
        }
    ]
    history = {
        "wrld_scoped": [
            {"visits": 100, "favorites": 10},
            {"visits": 220, "favorites": 22},
        ]
    }

    def fake_load_history(world_id=None, source=None):
        calls["history_source"] = source
        return history

    service.load_history = fake_load_history

    payload = service._build_scope_dashboard_payload(
        label="db:job:Ch",
        worlds=worlds,
        history_source="db:job:Ch",
    )

    assert calls["history_source"] == "db:job:Ch"
    assert payload["top_movers"][0]["id"] == "wrld_scoped"
    assert payload["top_movers"][0]["delta"] == 120


def test_load_topic_history_merges_only_source_rule_histories():
    service = object.__new__(WorldInfoService)
    calls = []

    class FakeStorage:
        def list_topic_rules(self, topic_key):
            return [
                {"rule_type": "source", "rule_value": "db:job:Ch", "is_active": 1},
                {"rule_type": "source", "rule_value": "db:job:taiwan", "is_active": 1},
            ]

    def fake_load_history(world_id=None, source=None):
        calls.append(source)
        if source == "db:job:Ch":
            return {"wrld_1": [{"timestamp": 1, "visits": 10, "favorites": 1}]}
        if source == "db:job:taiwan":
            return {"wrld_1": [{"timestamp": 2, "visits": 20, "favorites": 2}]}
        raise AssertionError(f"unexpected source {source}")

    service.storage = FakeStorage()
    service.load_history = fake_load_history

    history = service._load_topic_history("topic_ch")

    assert calls == ["db:job:Ch", "db:job:taiwan"]
    assert [item["visits"] for item in history["wrld_1"]] == [10, 20]


def test_communities_crud_round_trip():
    repo_root = Path.cwd()
    service = WorldInfoService(repo_root=repo_root, app_root=repo_root / "world_info_web")
    group_id = f"grp_test_{uuid.uuid4().hex[:8]}"
    post_id = None
    world_id = "wrld_group_world"
    run_id = service.storage.create_run(
        source_key="job:test",
        job_key="test",
        trigger_type="manual",
        query_label="seed",
        started_at="2026-05-06T00:00:00+00:00",
    )
    service.storage.insert_world_snapshots(
        run_id=run_id,
        source_key="job:test",
        fetched_at="2026-05-06T00:00:00+00:00",
        worlds=[
            {
                "id": world_id,
                "name": "Group World",
                "author_id": "usr_group",
                "author_name": "Group Author",
                "visits": 120,
                "favorites": 12,
                "heat": 6,
                "popularity": 7,
                "updated_at": "2026-05-05T00:00:00+00:00",
                "publication_date": "2026-05-01T00:00:00+00:00",
                "tags": ["tag_a"],
            }
        ],
    )
    try:
        group = service.upsert_group(
            group_id=group_id,
            name="Test Group",
            region="TW",
            category="racing",
            description="community description",
            managed_status="observed",
            external_links=["https://example.com"],
        )
        assert group["group_id"] == group_id
        assert group["external_links"] == ["https://example.com"]

        managed = service.upsert_managed_group(
            group_id=group_id,
            workspace_key="community_racing_tw",
            posting_enabled=True,
            notes="operator notes",
        )
        assert managed["group_id"] == group_id
        assert managed["posting_enabled"] == 1

        scheduled = service.upsert_scheduled_post(
            group_id=group_id,
            content_type="announcement",
            status="pending",
            scheduled_for="2026-05-10T10:00:00+08:00",
            payload={"body": "hello"},
        )
        post_id = scheduled["id"]
        assert scheduled["status"] == "pending"
        assert scheduled["payload"] == {"body": "hello"}

        updated = service.upsert_scheduled_post(
            post_id=post_id,
            group_id=group_id,
            content_type="announcement",
            status="queued",
            scheduled_for="2026-05-10T12:00:00+08:00",
            payload='{"body":"updated"}',
        )
        assert updated["status"] == "queued"
        assert updated["payload"] == {"body": "updated"}

        membership = service.upsert_group_world_membership(
            group_id=group_id,
            world_id=world_id,
            membership_role="flagship",
            source_key="db:job:test",
        )
        assert membership["group_id"] == group_id
        assert membership["world_id"] == world_id
        assert membership["world_name"] == "Group World"

        summary = service.load_communities_workspace()
        assert summary["summary"]["group_count"] >= 1
        assert summary["summary"]["managed_group_count"] >= 1
        assert summary["summary"]["scheduled_post_count"] >= 1
        assert summary["summary"]["group_world_link_count"] >= 1
        assert any(item["group_id"] == group_id for item in summary["publishing"]["managed_groups"])
        assert any(item["world_id"] == world_id for item in summary["worlds"]["items"])
    finally:
        if post_id is not None:
            try:
                service.delete_scheduled_post(post_id)
            except KeyError:
                pass
        try:
            service.delete_group_world_membership(group_id=group_id, world_id=world_id)
        except KeyError:
            pass
        try:
            service.delete_managed_group(group_id)
        except KeyError:
            pass
        try:
            service.delete_group(group_id)
        except KeyError:
            pass


def test_job_diagnostics_include_source_diff(monkeypatch):
    repo_root = _make_case_dir("service_job_diagnostics_diff") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    _write_json(
        jobs_path,
        {
            "taiwan": {
                "label": "Zh keyword sync",
                "type": "keywords",
                "source_key": "job:taiwan",
                "keywords": ["Taiwan"],
                "limit_per_keyword": 20,
            }
        },
    )
    calls = {"count": 0}

    def fake_fetch_worlds(*, keyword=None, user_id=None, limit=20, delay=1.0, headers=None):
        calls["count"] += 1
        if calls["count"] == 1:
            return [
                {"id": "wrld_keep", "name": "Keep", "visits": 100, "favorites": 10},
                {"id": "wrld_drop", "name": "Drop", "visits": 80, "favorites": 8},
            ]
        return [
            {"id": "wrld_keep", "name": "Keep", "visits": 140, "favorites": 15},
            {"id": "wrld_new", "name": "New", "visits": 60, "favorites": 4},
        ]

    monkeypatch.setattr(service_module, "fetch_worlds", fake_fetch_worlds)
    monkeypatch.setattr(service_module, "enrich_visits", lambda worlds, headers=None, delay=0.0: worlds)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    service.run_job("taiwan")
    service.run_job("taiwan")

    diagnostics = service.list_job_diagnostics()
    item = diagnostics[0]

    assert item["current_world_count"] == 2
    assert item["latest_completed_run"] is not None
    assert item["source_diff"]["status"] == "ok"
    assert item["source_diff"]["added_count"] == 1
    assert item["source_diff"]["removed_count"] == 1
    assert any(world["id"] == "wrld_keep" for world in item["source_diff"]["changed_worlds"])


def test_event_feed_includes_spikes_uploads_and_updates(monkeypatch):
    repo_root = _make_case_dir("service_event_feed") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    _write_json(
        jobs_path,
        {
            "taiwan": {
                "label": "Zh keyword sync",
                "type": "keywords",
                "source_key": "job:taiwan",
                "keywords": ["Taiwan"],
                "limit_per_keyword": 20,
            }
        },
    )
    now = dt.datetime.now(dt.timezone.utc)
    recent_publication = (now - dt.timedelta(days=1)).isoformat()
    old_publication = (now - dt.timedelta(days=180)).isoformat()
    old_update = (now - dt.timedelta(days=40)).isoformat()
    recent_update = (now - dt.timedelta(hours=8)).isoformat()
    calls = {"count": 0}

    def fake_fetch_worlds(*, keyword=None, user_id=None, limit=20, delay=1.0, headers=None):
        calls["count"] += 1
        if calls["count"] == 1:
            return [
                {
                    "id": "wrld_keep",
                    "name": "Keep",
                    "authorId": "usr_keep",
                    "authorName": "Keep Author",
                    "visits": 100,
                    "favorites": 10,
                    "heat": 1,
                    "popularity": 1,
                    "updated_at": old_update,
                    "publicationDate": old_publication,
                }
            ]
        return [
            {
                "id": "wrld_keep",
                "name": "Keep",
                "authorId": "usr_keep",
                "authorName": "Keep Author",
                "visits": 180,
                "favorites": 18,
                "heat": 5,
                "popularity": 4,
                "updated_at": recent_update,
                "publicationDate": old_publication,
            },
            {
                "id": "wrld_new",
                "name": "New Upload",
                "authorId": "usr_new",
                "authorName": "New Author",
                "visits": 55,
                "favorites": 6,
                "heat": 2,
                "popularity": 1,
                "updated_at": recent_update,
                "publicationDate": recent_publication,
            },
        ]

    monkeypatch.setattr(service_module, "fetch_worlds", fake_fetch_worlds)
    monkeypatch.setattr(service_module, "enrich_visits", lambda worlds, headers=None, delay=0.0: worlds)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    service.run_job("taiwan")
    service.run_job("taiwan")

    payload = service.list_event_feed(limit=20, recency_days=7)
    types_by_world = {(item["type"], item["world_id"]) for item in payload["items"]}

    assert payload["summary"]["spikes"] >= 1
    assert payload["summary"]["uploads"] >= 1
    assert payload["summary"]["updates"] >= 1
    assert ("traffic_spike", "wrld_keep") in types_by_world
    assert ("new_update", "wrld_keep") in types_by_world
    assert ("new_upload", "wrld_new") in types_by_world


def test_collection_insights_include_growth_authors_and_performance(monkeypatch):
    repo_root = _make_case_dir("service_collection_insights") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    _write_json(
        jobs_path,
        {
            "starriver": {
                "label": "StarRiver Sync",
                "type": "user",
                "source_key": "job:starriver",
                "user_id": "usr_0673194d-712d-4b5d-8167-1f03ed3233cb",
                "limit": 20,
            }
        },
    )

    monkeypatch.setattr(
        service_module,
        "fetch_worlds",
        lambda **kwargs: [
            {
                "id": "wrld_perf",
                "name": "Performance World",
                "authorId": "usr_0673194d-712d-4b5d-8167-1f03ed3233cb",
                "authorName": "StarRiver Arts",
                "visits": 320,
                "favorites": 40,
                "heat": 7,
                "popularity": 5,
                "updated_at": "2026-04-20T00:00:00Z",
                "publicationDate": "2025-01-01T00:00:00Z",
            }
        ],
    )
    monkeypatch.setattr(service_module, "enrich_visits", lambda worlds, headers=None, delay=0.0: worlds)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    service.run_job("starriver")
    service.storage.insert_world_snapshots(
        run_id=service.storage.get_latest_run_for_job("starriver")["id"],
        source_key="job:starriver",
        fetched_at="2026-04-10T00:00:00+00:00",
        worlds=[{
            "id": "wrld_perf",
            "name": "Performance World",
            "author_id": "usr_0673194d-712d-4b5d-8167-1f03ed3233cb",
            "author_name": "StarRiver Arts",
            "visits": 120,
            "favorites": 20,
            "heat": 3,
            "popularity": 2,
            "updated_at": "2025-01-01T00:00:00Z",
            "publication_date": "2025-01-01T00:00:00Z",
            "tags": [],
            "metrics": {},
        }],
    )

    payload = service.load_collection_insights(source="db:job:starriver", limit=10)

    assert payload["growth_leaderboard"][0]["id"] == "wrld_perf"
    assert payload["new_hot_leaderboard"][0]["id"] == "wrld_perf"
    assert payload["worth_watching_leaderboard"][0]["id"] == "wrld_perf"
    assert payload["authors"][0]["author_name"] == "StarRiver Arts"
    assert payload["authors"][0]["top_world_share"] == 100.0
    assert payload["signals"]["summary"]["world_count"] == 1
    assert payload["signals"]["charts"][0]["sample_size"] >= 1
    assert payload["anomalies"]["summary"]["tracked_anomalies"] >= 1
    assert payload["update_effectiveness"]["enabled"] is True
    assert payload["update_effectiveness"]["items"][0]["update_effectiveness_score"] > 0
    assert payload["performance"]["enabled"] is True
    assert payload["world_insights"]["wrld_perf"]["momentum_score"] > 0
    assert payload["world_insights"]["wrld_perf"]["new_hot_score"] > 0


def test_breakout_sort_and_insights_promote_recent_extreme_growth_world(monkeypatch):
    repo_root = _make_case_dir("service_breakout_sort") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    _write_json(
        jobs_path,
        {
            "starriver": {
                "label": "StarRiver Sync",
                "type": "user",
                "source_key": "job:starriver",
                "user_id": "usr_0673194d-712d-4b5d-8167-1f03ed3233cb",
                "limit": 50,
            }
        },
    )

    monkeypatch.setattr(
        service_module,
        "fetch_worlds",
        lambda **kwargs: [
            {
                "id": "wrld_breakout",
                "name": "Breakout World",
                "authorId": "usr_0673194d-712d-4b5d-8167-1f03ed3233cb",
                "authorName": "StarRiver Arts",
                "visits": 180,
                "favorites": 30,
                "heat": 8,
                "popularity": 6,
                "updated_at": "2026-05-04T00:00:00Z",
                "publicationDate": "2026-05-02T00:00:00Z",
            },
            {
                "id": "wrld_legacy_giant",
                "name": "Legacy Giant",
                "authorId": "usr_0673194d-712d-4b5d-8167-1f03ed3233cb",
                "authorName": "StarRiver Arts",
                "visits": 9000,
                "favorites": 300,
                "heat": 9,
                "popularity": 8,
                "updated_at": "2026-04-10T00:00:00Z",
                "publicationDate": "2025-04-01T00:00:00Z",
            },
        ],
    )
    monkeypatch.setattr(service_module, "enrich_visits", lambda worlds, headers=None, delay=0.0: worlds)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    service.run_job("starriver")
    run_id = service.storage.get_latest_run_for_job("starriver")["id"]
    service.storage.insert_world_snapshots(
        run_id=run_id,
        source_key="job:starriver",
        fetched_at="2026-05-03T00:00:00+00:00",
        worlds=[
            {
                "id": "wrld_breakout",
                "name": "Breakout World",
                "author_id": "usr_0673194d-712d-4b5d-8167-1f03ed3233cb",
                "author_name": "StarRiver Arts",
                "visits": 42,
                "favorites": 8,
                "heat": 3,
                "popularity": 2,
                "updated_at": "2026-05-03T00:00:00Z",
                "publication_date": "2026-05-02T00:00:00Z",
                "tags": [],
                "metrics": {},
            },
            {
                "id": "wrld_legacy_giant",
                "name": "Legacy Giant",
                "author_id": "usr_0673194d-712d-4b5d-8167-1f03ed3233cb",
                "author_name": "StarRiver Arts",
                "visits": 8950,
                "favorites": 298,
                "heat": 9,
                "popularity": 8,
                "updated_at": "2026-04-10T00:00:00Z",
                "publication_date": "2025-04-01T00:00:00Z",
                "tags": [],
                "metrics": {},
            },
        ],
    )
    service.storage.insert_world_snapshots(
        run_id=run_id,
        source_key="job:starriver",
        fetched_at="2026-04-27T00:00:00+00:00",
        worlds=[
            {
                "id": "wrld_legacy_giant",
                "name": "Legacy Giant",
                "author_id": "usr_0673194d-712d-4b5d-8167-1f03ed3233cb",
                "author_name": "StarRiver Arts",
                "visits": 8800,
                "favorites": 290,
                "heat": 8,
                "popularity": 7,
                "updated_at": "2026-04-10T00:00:00Z",
                "publication_date": "2025-04-01T00:00:00Z",
                "tags": [],
                "metrics": {},
            },
        ],
    )

    breakout_sorted = service.load_worlds("db:job:starriver", sort="breakout")
    new_hot_sorted = service.load_worlds("db:job:starriver", sort="new_hot")
    payload = service.load_collection_insights(source="db:job:starriver", limit=10)

    assert breakout_sorted[0]["id"] == "wrld_breakout"
    assert new_hot_sorted[0]["id"] == "wrld_breakout"
    assert payload["new_hot_leaderboard"][0]["id"] == "wrld_breakout"
    assert payload["world_insights"]["wrld_breakout"]["breakout_score"] > payload["world_insights"]["wrld_legacy_giant"]["breakout_score"]


def test_signal_efficiency_uses_bucket_percentile_with_confidence_weight():
    repo_root = _make_case_dir("service_signal_efficiency") / "repo"
    app_root = repo_root / "world_info_web"
    service = WorldInfoService(repo_root=repo_root, app_root=app_root)

    signals = service._build_signal_analysis(
        [
            {
                "id": "wrld_low",
                "name": "Low Sample",
                "author_name": "Author A",
                "visits": 20,
                "favorites": 2,
                "heat": 2,
                "popularity": 2,
                "favorite_rate": 10.0,
            },
            {
                "id": "wrld_mid",
                "name": "Mid Sample",
                "author_name": "Author B",
                "visits": 280,
                "favorites": 20,
                "heat": 6,
                "popularity": 5,
                "favorite_rate": 7.14,
            },
            {
                "id": "wrld_high",
                "name": "High Sample",
                "author_name": "Author C",
                "visits": 2400,
                "favorites": 150,
                "heat": 10,
                "popularity": 8,
                "favorite_rate": 6.25,
            },
        ],
        limit=10,
    )

    board = signals["leaderboards"]["signal_efficiency"]

    assert board[0]["id"] == "wrld_high"
    assert board[-1]["id"] == "wrld_low"
    assert board[0]["signal_efficiency_score"] > board[-1]["signal_efficiency_score"]
    assert board[0]["confidence_weight"] > board[-1]["confidence_weight"]


def test_create_job_with_topic_adds_both_configs():
    repo_root = _make_case_dir("service_create_job_topic") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    topics_path = app_root / "config" / "topics.json"
    _write_json(jobs_path, {})
    _write_json(topics_path, {})

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path, topics_path=topics_path)
    result = service.create_job_with_topic(
        job_key="club_night",
        label="Club Night",
        job_type="keywords",
        keywords=["club", "night"],
        limit_per_keyword=40,
    )

    assert result["status"] == "created"
    assert result["job"]["job_key"] == "club_night"
    assert result["topic"]["topic_key"] == "club_night"

    jobs_config = json.loads(jobs_path.read_text(encoding="utf-8"))
    topics_config = json.loads(topics_path.read_text(encoding="utf-8"))
    assert jobs_config["club_night"]["source_key"] == "job:club_night"
    assert topics_config["club_night"]["rules"] == [{"type": "source", "value": "db:job:club_night"}]


def test_verify_vrchat_2fa_accepts_emailotp_alias(monkeypatch):
    repo_root = _make_case_dir("service_emailotp_alias") / "repo"
    app_root = repo_root / "world_info_web"

    monkeypatch.setattr(
        service_module,
        "vrchat_verify_2fa",
        lambda code, method, auth_cookie: {"ok": True, "cookie": f"auth={method}"},
    )
    monkeypatch.setattr(
        service_module,
        "vrchat_check_session",
        lambda cookie: {"ok": True, "user": {"id": "usr_test", "displayName": "Email OTP"}},
    )

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    result = service.verify_vrchat_2fa(code="123456", method="emailOtp", auth_cookie="auth=pending")

    assert result["status"] == "ok"
    assert result["cookie"] == "auth=emailotp"


def test_db_all_merges_rich_legacy_metrics_with_live_creator_sync(monkeypatch):
    repo_root = _make_case_dir("service_merge_sources") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"

    _write_json(
        jobs_path,
        {
            "starriver": {
                "label": "StarRiver Sync",
                "type": "user",
                "source_key": "job:starriver",
                "user_id": "usr_test",
                "limit": 20,
            }
        },
    )
    _write_workbook(
        repo_root / "world_info" / "scraper" / "StarRiverArts.xlsx",
        [
            [
                "fetched",
                "name",
                "world_id",
                "published",
                "updated",
                "visits",
                "capacity",
                "favorites",
                "heat",
                "popularity",
                "labs_days",
                "favorite_rate",
                "days_since_update",
                "days_since_publication",
                "visits_per_day",
            ],
            [
                "2026/04/08",
                "Legacy StarRiver World",
                "wrld_merge",
                "2024-07-08T21:05:28.545Z",
                "2024-07-11T08:38:23.384Z",
                1055,
                8,
                52,
                0,
                5,
                "263天",
                "4.93%",
                "638天",
                "905天",
                1.65,
            ],
        ],
    )

    def fake_fetch_worlds(*, keyword=None, user_id=None, limit=20, delay=1.0, headers=None):
        assert user_id == "usr_test"
        return [
            {
                "id": "wrld_merge",
                "name": "Legacy StarRiver World",
                "authorId": "usr_test",
                "authorName": "StarRiver Arts",
                "visits": 0,
                "favorites": 53,
                "heat": 3,
                "popularity": 5,
                "capacity": 16,
                "updated_at": "2024-07-11T08:38:23.384Z",
                "publicationDate": "2024-07-08T21:05:28.545Z",
                "releaseStatus": "public",
                "tags": ["author_tag_StarRiver", "system_approved"],
            }
        ]

    monkeypatch.setattr(service_module, "fetch_worlds", fake_fetch_worlds)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    service.import_legacy_data()
    service.run_job("starriver")

    merged = service.load_worlds("db:all", query="wrld_merge")[0]

    assert merged["visits"] == 1055
    assert merged["favorites"] == 53
    assert merged["heat"] == 3
    assert merged["capacity"] == 16
    assert merged["release_status"] == "public"
    assert merged["author_name"] == "StarRiver Arts"
    assert merged["tags"] == ["author_tag_StarRiver", "system_approved"]
    assert merged["metrics"]["favorite_rate"] == 5.02
    assert merged["metrics"]["days_since_publication"] == 905


def test_import_legacy_data_moves_snapshots_history_and_daily_stats():
    repo_root = _make_case_dir("service_import") / "repo"
    app_root = repo_root / "world_info_web"

    _write_json(
        repo_root / "world_info" / "scraper" / "raw_worlds.json",
        [
            {
                "id": "wrld_1",
                "name": "Alpha",
                "authorId": "usr_alpha",
                "authorName": "Alpha Maker",
                "visits": 100,
                "favorites": 10,
                "updated_at": "2025-08-02T00:00:00Z",
                "publicationDate": "2025-07-01T00:00:00Z",
                "tags": ["featured", "taiwan"],
            }
        ],
    )
    _write_json(
        repo_root / "world_info" / "scraper" / "history.json",
        {
            "wrld_1": [
                {
                    "timestamp": 1722470400,
                    "name": "Alpha",
                    "visits": 10,
                    "favorites": 2,
                    "updated_at": "2025-08-02T00:00:00Z",
                },
                {
                    "timestamp": 1722556800,
                    "name": "Alpha",
                    "visits": 20,
                    "favorites": 4,
                    "updated_at": "2025-08-02T00:00:00Z",
                },
            ]
        },
    )
    _write_workbook(
        repo_root / "world_info" / "scraper" / "TaiwanWorlds.xlsx",
        [
            ["fetched", "name", "world_id", "published", "updated", "visits", "capacity", "favorites", "heat", "popularity", "labs_days", "favorite_rate", "days_since_update", "status", "visits_per_day"],
            [
                "2025-08-04T00:00:00Z",
                "Taiwan Workbook World",
                "wrld_2",
                "2025-08-01T00:00:00Z",
                "2025-08-03T00:00:00Z",
                55,
                24,
                12,
                1,
                5,
                2,
                "21.8%",
                1,
                "public",
                18.3,
            ],
        ],
    )
    _write_workbook(
        repo_root / "analytics" / "daily_stats_taiwan.xlsx",
        [
            ["date", "total_worlds", "new_worlds_today"],
            ["2025/08/04", 8, 2],
            ["2025/08/05", 9, 1],
        ],
    )

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    result = service.import_legacy_data()

    imported_raw = service.load_worlds("db:import:legacy-raw")
    imported_taiwan = service.load_worlds("db:import:legacy-taiwan")
    history = service.load_history("wrld_1")["wrld_1"]
    daily_stats = service.load_daily_stats()
    sources = service.list_sources()

    assert result["status"] == "completed"
    assert any(item["source"] == "db:import:legacy-raw" and item["count"] == 1 for item in result["sources"])
    assert imported_raw[0]["author_id"] == "usr_alpha"
    assert imported_taiwan[0]["name"] == "Taiwan Workbook World"
    assert len(history) == 4
    assert history[-1]["origin"] == "db"
    assert any(item["source"] == "db:import:legacy-taiwan" for item in daily_stats)
    assert any(item["key"] == "db:import:legacy-raw" for item in sources)
    assert all(item["key"] != "db:history:legacy" for item in sources)
