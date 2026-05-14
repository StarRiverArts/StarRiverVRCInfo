import json
import shutil
import uuid
from datetime import datetime
from pathlib import Path

import world_info_web.backend.service as service_module
from world_info_web.backend.app import create_app
from world_info_web.backend.scheduler import AutoSyncScheduler
from world_info_web.backend.service import WorldInfoService


def _write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_case_dir(name: str) -> Path:
    root = Path.cwd() / ".tmp_pytest" / f"{name}_{uuid.uuid4().hex}"
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    return root


def test_api_worlds_and_self_check_routes():
    repo_root = _make_case_dir("app_routes") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    _write_json(
        repo_root / "world_info" / "scraper" / "raw_worlds.json",
        [
            {
                "id": "wrld_1",
                "name": "Alpha",
                "visits": 100,
                "favorites": 10,
                "tags": ["featured"],
            }
        ],
    )
    _write_json(
        repo_root / "world_info" / "scraper" / "history.json",
        {
            "wrld_1": [
                {"timestamp": 1722470400, "name": "Alpha", "visits": 10, "favorites": 2}
            ]
        },
    )

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()

    worlds_response = client.get("/api/v1/worlds?source=legacy-raw")
    check_response = client.get("/api/v1/review/self-check")

    assert worlds_response.status_code == 200
    assert worlds_response.get_json()["count"] == 1
    assert check_response.status_code == 207
    assert "warnings" in check_response.get_json()


def test_jobs_routes_run_and_list(monkeypatch):
    repo_root = _make_case_dir("app_jobs") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")
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
                "rules": [{"type": "keyword", "value": "Alpha"}],
            }
        },
    )

    def fake_fetch_worlds(*, keyword=None, user_id=None, limit=20, delay=1.0, headers=None):
        assert headers["Cookie"] == "auth=test"
        return [{"id": "wrld_1", "name": "Alpha", "visits": 99, "favorites": 5}]

    monkeypatch.setattr(service_module, "fetch_worlds", fake_fetch_worlds)

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path, topics_path=topics_path)
    app = create_app(service)
    client = app.test_client()

    jobs_response = client.get("/api/v1/jobs")
    run_response = client.post("/api/v1/jobs/taiwan/run", json={"cookie": "auth=test"})
    runs_response = client.get("/api/v1/runs?limit=5")
    topics_response = client.get("/api/v1/topics")
    topic_worlds_response = client.get("/api/v1/topics/taiwan/worlds")

    assert jobs_response.status_code == 200
    assert jobs_response.get_json()["items"][0]["job_key"] == "taiwan"
    assert run_response.status_code == 201
    assert run_response.get_json()["source"] == "db:job:taiwan"
    assert runs_response.status_code == 200
    assert runs_response.get_json()["items"][0]["source"] == "db:job:taiwan"
    assert topics_response.status_code == 200
    assert topics_response.get_json()["items"][0]["topic_key"] == "taiwan"
    assert topic_worlds_response.status_code == 200
    assert topic_worlds_response.get_json()["count"] == 1


def test_topic_crud_routes():
    repo_root = _make_case_dir("app_topic_crud") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()

    create_response = client.post(
        "/api/v1/topics",
        json={
            "topic_key": "event_view",
            "label": "Event View",
            "rules": [{"type": "tag", "value": "admin_spacejam3"}],
        },
    )
    hide_response = client.put("/api/v1/topics/event_view", json={"is_active": False})
    active_response = client.get("/api/v1/topics")
    all_response = client.get("/api/v1/topics?include_inactive=1")
    delete_response = client.delete("/api/v1/topics/event_view")

    assert create_response.status_code == 201
    assert create_response.get_json()["topic_key"] == "event_view"
    assert hide_response.status_code == 200
    assert hide_response.get_json()["is_active"] is False
    assert active_response.get_json()["items"] == []
    assert all_response.get_json()["items"][0]["topic_key"] == "event_view"
    assert delete_response.status_code == 200


def test_delete_job_route_removes_matching_topic():
    repo_root = _make_case_dir("app_delete_job") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")
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
    app = create_app(service)
    client = app.test_client()

    response = client.delete("/api/v1/jobs/default?delete_topic=1")

    assert response.status_code == 200
    assert response.get_json()["topic_deleted"] is True
    assert client.get("/api/v1/jobs").get_json()["items"] == []
    assert client.get("/api/v1/topics?include_inactive=1").get_json()["items"] == []


def test_auto_sync_run_now_route_uses_existing_job_flow(monkeypatch):
    repo_root = _make_case_dir("app_auto_sync_run_now") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()

    captured: dict[str, object] = {}

    def fake_run_job(job_key: str, **kwargs):
        captured["job_key"] = job_key
        captured["kwargs"] = kwargs
        return {"status": "completed", "job_key": job_key, "run_id": 7}

    monkeypatch.setattr(service, "run_job", fake_run_job)

    response = client.post(
        "/api/v1/auto-sync/racing/run-now",
        json={"cookie": "auth=test", "username": "user@example.com", "password": "secret"},
    )

    assert response.status_code == 200
    assert response.get_json()["run_id"] == 7
    assert captured["job_key"] == "racing"
    assert captured["kwargs"] == {
        "trigger_type": "auto_manual",
    }


def test_auto_sync_record_run_route_updates_schedule_file():
    repo_root = _make_case_dir("app_auto_sync_record_run") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()

    response = client.post("/api/v1/auto-sync/racing/record-run")

    assert response.status_code == 200
    assert response.get_json()["status"] == "recorded"
    schedule_path = app_root / "config" / "auto_sync_schedule.json"
    payload = json.loads(schedule_path.read_text(encoding="utf-8"))
    assert "last_auto_run" in payload["racing"]


def test_auto_sync_status_falls_back_to_latest_completed_job_run():
    repo_root = _make_case_dir("app_auto_sync_status_fallback") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    jobs_path = app_root / "config" / "sync_jobs.json"
    _write_json(
        jobs_path,
        {
            "racing": {
                "label": "Racing keyword sync",
                "type": "keywords",
                "source_key": "job:racing",
                "keywords": ["racing"],
                "limit_per_keyword": 20,
            }
        },
    )
    _write_json(app_root / "config" / "auto_sync_schedule.json", {"racing": {"interval": "7d"}})

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    run_id = service.storage.create_run(
        source_key="job:racing",
        job_key="racing",
        trigger_type="job",
        query_label="Racing keyword sync",
        started_at="2026-04-23T08:00:00+00:00",
    )
    service.storage.finish_run(
        run_id,
        status="completed",
        finished_at="2026-04-23T08:02:00+00:00",
        world_count=3,
    )

    app = create_app(service)
    client = app.test_client()

    response = client.get("/api/v1/auto-sync/status")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["racing"]["last_auto_run"] == "2026-04-23T08:02:00+00:00"
    assert payload["racing"]["next_run"] is not None


def test_events_route_forwards_limit_and_days(monkeypatch):
    repo_root = _make_case_dir("app_events_route") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()
    captured: dict[str, object] = {}

    def fake_list_event_feed(*, limit: int = 50, recency_days: int = 7):
        captured["limit"] = limit
        captured["recency_days"] = recency_days
        return {"summary": {"total": 0}, "items": []}

    monkeypatch.setattr(service, "list_event_feed", fake_list_event_feed)

    response = client.get("/api/v1/events?limit=9&days=3")

    assert response.status_code == 200
    assert response.get_json()["items"] == []
    assert captured == {"limit": 9, "recency_days": 3}


def test_auto_sync_status_prefers_latest_completed_run_over_stale_schedule():
    repo_root = _make_case_dir("app_auto_sync_status_latest") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    schedule_path = app_root / "config" / "auto_sync_schedule.json"
    _write_json(
        jobs_path,
        {
            "racing": {
                "label": "Racing keyword sync",
                "type": "keywords",
                "source_key": "job:racing",
                "keywords": ["racing"],
            }
        },
    )
    _write_json(schedule_path, {"racing": {"interval": "1d", "last_auto_run": "2026-04-20T08:00:00+00:00"}})
    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    run_id = service.storage.create_run(
        source_key="job:racing",
        job_key="racing",
        trigger_type="job",
        query_label="Racing keyword sync",
        started_at="2026-04-23T08:00:00+00:00",
    )
    service.storage.finish_run(
        run_id,
        status="completed",
        finished_at="2026-04-23T08:05:00+00:00",
        world_count=3,
    )

    status = AutoSyncScheduler(service, schedule_path).get_status()

    assert status["racing"]["last_auto_run"] == "2026-04-23T08:05:00+00:00"
    assert status["racing"]["last_success_trigger"] == "job"


def test_auto_sync_running_job_is_not_marked_overdue():
    repo_root = _make_case_dir("app_auto_sync_running") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    schedule_path = app_root / "config" / "auto_sync_schedule.json"
    now = service_module.dt.datetime.now(service_module.dt.timezone.utc)
    last_run = (now - service_module.dt.timedelta(hours=2)).isoformat()
    last_attempt = (now - service_module.dt.timedelta(minutes=20)).isoformat()
    _write_json(
        jobs_path,
        {
            "racing": {
                "label": "Racing keyword sync",
                "type": "keywords",
                "source_key": "job:racing",
                "keywords": ["racing"],
            }
        },
    )
    _write_json(
        schedule_path,
        {
            "racing": {
                "interval": "1h",
                "last_auto_run": last_run,
                "last_attempt_at": last_attempt,
                "running": True,
            }
        },
    )
    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)

    status = AutoSyncScheduler(service, schedule_path).get_status()

    assert status["racing"]["running"] is True
    assert status["racing"]["overdue"] is False


def test_scheduler_auto_run_records_auto_trigger(monkeypatch):
    repo_root = _make_case_dir("app_auto_sync_trigger") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    schedule_path = app_root / "config" / "auto_sync_schedule.json"
    _write_json(
        jobs_path,
        {
            "racing": {
                "label": "Racing keyword sync",
                "type": "keywords",
                "source_key": "job:racing",
                "keywords": ["racing"],
            }
        },
    )
    _write_json(schedule_path, {"racing": {"interval": "1h", "last_auto_run": "2026-04-20T08:00:00+00:00"}})
    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    captured = {}

    def fake_run_job(job_key, **kwargs):
        captured["job_key"] = job_key
        captured["kwargs"] = kwargs
        return {"run_id": 1}

    monkeypatch.setattr(service, "run_job", fake_run_job)

    AutoSyncScheduler(service, schedule_path)._tick()

    assert captured == {"job_key": "racing", "kwargs": {"trigger_type": "auto"}}


def test_scheduler_accepts_2d_interval_and_reports_next_run():
    repo_root = _make_case_dir("app_auto_sync_2d") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    schedule_path = app_root / "config" / "auto_sync_schedule.json"
    _write_json(jobs_path, {"racing": {"label": "Racing", "type": "keywords", "source_key": "job:racing", "keywords": ["racing"]}})

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    scheduler = AutoSyncScheduler(service, schedule_path)
    scheduler.set_interval("racing", "2d")

    status = scheduler.get_status()

    assert status["racing"]["interval"] == "2d"
    assert status["racing"]["interval_seconds"] == 172800
    assert status["racing"]["next_run"] is not None


def test_scheduler_rebalances_jobs_with_same_interval():
    repo_root = _make_case_dir("app_auto_sync_rebalance") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    schedule_path = app_root / "config" / "auto_sync_schedule.json"
    _write_json(
        jobs_path,
        {
            "alpha": {"label": "Alpha", "type": "keywords", "source_key": "job:alpha", "keywords": ["alpha"]},
            "beta": {"label": "Beta", "type": "keywords", "source_key": "job:beta", "keywords": ["beta"]},
            "gamma": {"label": "Gamma", "type": "keywords", "source_key": "job:gamma", "keywords": ["gamma"]},
        },
    )

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    scheduler = AutoSyncScheduler(service, schedule_path)
    scheduler.set_interval("alpha", "1d")
    scheduler.set_interval("beta", "1d")
    scheduler.set_interval("gamma", "1d")

    status = scheduler.get_status()
    parsed = [
        datetime.fromisoformat(status[key]["next_run"])
        for key in ("alpha", "beta", "gamma")
        if status[key]["next_run"]
    ]

    assert len(parsed) == 3
    assert len({value.isoformat() for value in parsed}) == 3
    deltas = sorted(
        int((parsed[index + 1] - parsed[index]).total_seconds())
        for index in range(len(parsed) - 1)
    )
    assert deltas[0] >= 25000


def test_scheduler_status_migrates_existing_group_to_staggered_schedule():
    repo_root = _make_case_dir("app_auto_sync_migrate_stagger") / "repo"
    app_root = repo_root / "world_info_web"
    jobs_path = app_root / "config" / "sync_jobs.json"
    schedule_path = app_root / "config" / "auto_sync_schedule.json"
    _write_json(
        jobs_path,
        {
            "alpha": {"label": "Alpha", "type": "keywords", "source_key": "job:alpha", "keywords": ["alpha"]},
            "beta": {"label": "Beta", "type": "keywords", "source_key": "job:beta", "keywords": ["beta"]},
        },
    )
    _write_json(
        schedule_path,
        {
            "alpha": {"interval": "1d", "last_auto_run": "2026-04-20T08:00:00+00:00"},
            "beta": {"interval": "1d", "last_auto_run": "2026-04-20T08:00:00+00:00"},
        },
    )

    service = WorldInfoService(repo_root=repo_root, app_root=app_root, jobs_path=jobs_path)
    scheduler = AutoSyncScheduler(service, schedule_path)

    status = scheduler.get_status()
    payload = json.loads(schedule_path.read_text(encoding="utf-8"))

    assert status["alpha"]["next_run"] != status["beta"]["next_run"]
    assert payload["alpha"]["stagger_interval"] == "1d"
    assert payload["beta"]["stagger_interval"] == "1d"


def test_auth_status_route_returns_session_state(monkeypatch):
    repo_root = _make_case_dir("app_auth_status") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()

    monkeypatch.setattr(
        service,
        "check_auth_status",
        lambda **kwargs: {"status": "ok", "mode": "cookie", "message": "session valid"},
    )

    response = client.post("/api/v1/auth/status", json={"cookie": "auth=test"})

    assert response.status_code == 200
    assert response.get_json()["mode"] == "cookie"


def test_auth_login_and_verify_routes(monkeypatch):
    repo_root = _make_case_dir("app_auth_login") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()

    monkeypatch.setattr(
        service,
        "login_with_vrchat",
        lambda **kwargs: {"status": "requires_2fa", "methods": ["totp"], "auth_cookie": "auth=pending"},
    )
    monkeypatch.setattr(
        service,
        "verify_vrchat_2fa",
        lambda **kwargs: {"status": "ok", "cookie": "auth=done", "message": "2FA verified"},
    )

    login_response = client.post("/api/v1/auth/login", json={"username": "u", "password": "p"})
    verify_response = client.post(
        "/api/v1/auth/verify-2fa",
        json={"code": "123456", "method": "totp", "auth_cookie": "auth=pending"},
    )

    assert login_response.status_code == 200
    assert login_response.get_json()["status"] == "requires_2fa"
    assert verify_response.status_code == 200
    assert verify_response.get_json()["cookie"] == "auth=done"


def test_auth_persist_and_clear_routes(monkeypatch):
    repo_root = _make_case_dir("app_auth_persist") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()

    monkeypatch.setattr(
        service,
        "persist_server_auth",
        lambda **kwargs: {"status": "saved", "mode": "cookie", "path": "world_info/scraper/headers.json"},
    )
    monkeypatch.setattr(
        service,
        "clear_server_auth",
        lambda: {"status": "cleared", "path": "world_info/scraper/headers.json"},
    )

    persist_response = client.post("/api/v1/auth/persist", json={"cookie": "auth=test"})
    clear_response = client.delete("/api/v1/auth/persist")

    assert persist_response.status_code == 200
    assert persist_response.get_json()["status"] == "saved"
    assert clear_response.status_code == 200
    assert clear_response.get_json()["status"] == "cleared"


def test_update_and_delete_world_routes(monkeypatch):
    repo_root = _make_case_dir("app_edit_delete") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()

    monkeypatch.setattr(
        service,
        "update_world_record",
        lambda **kwargs: {"status": "updated", "world": {"id": "wrld_1", "visits": 12}},
    )
    monkeypatch.setattr(
        service,
        "delete_world_record",
        lambda **kwargs: {"status": "deleted", "world_id": "wrld_1"},
    )

    update_response = client.put("/api/v1/worlds/wrld_1", json={"source": "db:job:taiwan", "visits": 12})
    delete_response = client.delete("/api/v1/worlds/wrld_1?source=db%3Ajob%3Ataiwan")

    assert update_response.status_code == 200
    assert update_response.get_json()["status"] == "updated"
    assert delete_response.status_code == 200
    assert delete_response.get_json()["status"] == "deleted"


def test_job_blacklist_routes(monkeypatch):
    repo_root = _make_case_dir("app_blacklist_routes") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()

    monkeypatch.setattr(
        service,
        "list_job_blacklist",
        lambda job_key: {"job_key": job_key, "items": ["wrld_1"], "path": "world_info/blacklist_taiwan.txt"},
    )
    monkeypatch.setattr(
        service,
        "add_job_blacklist_entry",
        lambda **kwargs: {"status": "added", "job_key": kwargs["job_key"], "world_id": kwargs["world_id"], "items": ["wrld_1"]},
    )
    monkeypatch.setattr(
        service,
        "remove_job_blacklist_entry",
        lambda **kwargs: {"status": "removed", "job_key": kwargs["job_key"], "world_id": kwargs["world_id"], "items": []},
    )

    list_response = client.get("/api/v1/jobs/taiwan/blacklist")
    add_response = client.post("/api/v1/jobs/taiwan/blacklist", json={"world_id": "wrld_1"})
    remove_response = client.delete("/api/v1/jobs/taiwan/blacklist/wrld_1")

    assert list_response.status_code == 200
    assert list_response.get_json()["items"] == ["wrld_1"]
    assert add_response.status_code == 201
    assert add_response.get_json()["status"] == "added"
    assert remove_response.status_code == 200
    assert remove_response.get_json()["status"] == "removed"


def test_creator_whitelist_routes(monkeypatch):
    repo_root = _make_case_dir("app_creator_whitelist_routes") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()

    monkeypatch.setattr(
        service,
        "list_job_creator_whitelist",
        lambda job_key: {"job_key": job_key, "items": ["usr_1"], "path": "world_info/taiwan_creator_whitelist.txt"},
    )
    monkeypatch.setattr(
        service,
        "add_job_creator_whitelist_entry",
        lambda **kwargs: {"status": "added", "job_key": kwargs["job_key"], "user_id": kwargs["user_id"], "items": ["usr_1"]},
    )
    monkeypatch.setattr(
        service,
        "remove_job_creator_whitelist_entry",
        lambda **kwargs: {"status": "removed", "job_key": kwargs["job_key"], "user_id": kwargs["user_id"], "items": []},
    )

    list_response = client.get("/api/v1/jobs/taiwan/creator-whitelist")
    add_response = client.post("/api/v1/jobs/taiwan/creator-whitelist", json={"user_id": "usr_1"})
    remove_response = client.delete("/api/v1/jobs/taiwan/creator-whitelist/usr_1")

    assert list_response.status_code == 200
    assert list_response.get_json()["items"] == ["usr_1"]
    assert add_response.status_code == 201
    assert add_response.get_json()["status"] == "added"
    assert remove_response.status_code == 200
    assert remove_response.get_json()["status"] == "removed"


def test_creator_blacklist_routes(monkeypatch):
    repo_root = _make_case_dir("app_creator_blacklist_routes") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()

    monkeypatch.setattr(
        service,
        "list_job_creator_blacklist",
        lambda job_key: {"job_key": job_key, "items": ["usr_1"], "path": "world_info/blacklist_taiwan_authors_cn.txt"},
    )
    monkeypatch.setattr(
        service,
        "add_job_creator_blacklist_entry",
        lambda **kwargs: {"status": "added", "job_key": kwargs["job_key"], "user_id": kwargs["user_id"], "items": ["usr_1"]},
    )
    monkeypatch.setattr(
        service,
        "remove_job_creator_blacklist_entry",
        lambda **kwargs: {"status": "removed", "job_key": kwargs["job_key"], "user_id": kwargs["user_id"], "items": []},
    )

    list_response = client.get("/api/v1/jobs/taiwan/creator-blacklist")
    add_response = client.post("/api/v1/jobs/taiwan/creator-blacklist", json={"user_id": "usr_1"})
    remove_response = client.delete("/api/v1/jobs/taiwan/creator-blacklist/usr_1")

    assert list_response.status_code == 200
    assert list_response.get_json()["items"] == ["usr_1"]
    assert add_response.status_code == 201
    assert add_response.get_json()["status"] == "added"
    assert remove_response.status_code == 200
    assert remove_response.get_json()["status"] == "removed"


def test_create_job_route():
    repo_root = _make_case_dir("app_create_job_route") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()

    response = client.post(
        "/api/v1/jobs",
        json={
            "job_key": "club_night",
            "label": "Club Night",
            "job_type": "keywords",
            "keywords": "club,night",
            "limit_per_keyword": 40,
        },
    )

    assert response.status_code == 201
    payload = response.get_json()
    assert payload["job"]["job_key"] == "club_night"
    assert payload["topic"]["topic_key"] == "club_night"


def test_job_diagnostics_route(monkeypatch):
    repo_root = _make_case_dir("app_job_diagnostics_route") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()

    monkeypatch.setattr(
        service,
        "list_job_diagnostics",
        lambda: [
            {
                "job_key": "taiwan",
                "label": "Zh keyword sync",
                "source_diff": {"status": "insufficient_history"},
            }
        ],
    )

    response = client.get("/api/v1/jobs/diagnostics")

    assert response.status_code == 200
    assert response.get_json()["items"][0]["job_key"] == "taiwan"


def test_insights_route(monkeypatch):
    repo_root = _make_case_dir("app_insights_route") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()

    monkeypatch.setattr(
        service,
        "load_collection_insights",
        lambda **kwargs: {
            "label": "db:all",
            "growth_leaderboard": [],
            "authors": [],
            "signals": {"summary": {"world_count": 0}, "correlations": [], "charts": [], "leaderboards": {}},
            "performance": {"enabled": False, "items": []},
        },
    )

    response = client.get("/api/v1/insights?source=db:all")

    assert response.status_code == 200
    assert response.get_json()["label"] == "db:all"
    assert "signals" in response.get_json()


def test_communities_crud_routes():
    repo_root = Path.cwd()
    app_root = repo_root / "world_info_web"
    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()
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
        group_response = client.post(
            "/api/v1/groups",
            json={
                "group_id": group_id,
                "name": "Test Group",
                "region": "TW",
                "category": "racing",
                "description": "community description",
                "managed_status": "observed",
                "external_links": ["https://example.com"],
            },
        )
        assert group_response.status_code == 201
        assert group_response.get_json()["group_id"] == group_id

        managed_response = client.post(
            "/api/v1/managed-groups",
            json={
                "group_id": group_id,
                "workspace_key": "community_racing_tw",
                "posting_enabled": True,
                "notes": "ops",
            },
        )
        assert managed_response.status_code == 201
        assert managed_response.get_json()["posting_enabled"] == 1

        scheduled_response = client.post(
            "/api/v1/scheduled-posts",
            json={
                "group_id": group_id,
                "content_type": "announcement",
                "status": "pending",
                "scheduled_for": "2026-05-10T10:00:00+08:00",
                "payload": {"body": "hello"},
            },
        )
        assert scheduled_response.status_code == 201
        post_id = scheduled_response.get_json()["id"]

        update_response = client.put(
            f"/api/v1/scheduled-posts/{post_id}",
            json={
                "group_id": group_id,
                "content_type": "announcement",
                "status": "queued",
                "scheduled_for": "2026-05-10T12:00:00+08:00",
                "payload": {"body": "updated"},
            },
        )
        assert update_response.status_code == 200
        assert update_response.get_json()["status"] == "queued"

        membership_response = client.post(
            "/api/v1/group-world-memberships",
            json={
                "group_id": group_id,
                "world_id": world_id,
                "membership_role": "flagship",
                "source_key": "db:job:test",
            },
        )
        assert membership_response.status_code == 201
        assert membership_response.get_json()["world_id"] == world_id

        summary_response = client.get("/api/v1/communities/summary")
        assert summary_response.status_code == 200
        assert summary_response.get_json()["summary"]["group_count"] >= 1
        assert summary_response.get_json()["summary"]["group_world_link_count"] >= 1
    finally:
        client.delete(f"/api/v1/group-world-memberships/{group_id}/{world_id}")
        if post_id is not None:
            client.delete(f"/api/v1/scheduled-posts/{post_id}")
        client.delete(f"/api/v1/managed-groups/{group_id}")
        client.delete(f"/api/v1/groups/{group_id}")


def test_worlds_route_defaults_to_new_hot_sort(monkeypatch):
    repo_root = _make_case_dir("app_worlds_default_sort") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()
    calls = []

    def fake_load_worlds(source, **kwargs):
        calls.append((source, kwargs))
        return []

    monkeypatch.setattr(service, "load_worlds", fake_load_worlds)
    monkeypatch.setattr(service, "collect_tags", lambda items: [])

    response = client.get("/api/v1/worlds?source=db:all")

    assert response.status_code == 200
    assert calls[0][0] == "db:all"
    assert calls[0][1]["sort"] == "new_hot"
    assert calls[0][1]["direction"] == "desc"


def test_topic_worlds_route_defaults_to_new_hot_sort(monkeypatch):
    repo_root = _make_case_dir("app_topic_worlds_default_sort") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()
    calls = []

    monkeypatch.setattr(service, "get_topic", lambda topic_key: {"topic_key": topic_key, "label": "Taiwan"})
    monkeypatch.setattr(
        service,
        "load_topic_worlds",
        lambda topic_key, **kwargs: calls.append((topic_key, kwargs)) or [],
    )
    monkeypatch.setattr(service, "collect_tags", lambda items: [])

    response = client.get("/api/v1/topics/taiwan/worlds")

    assert response.status_code == 200
    assert calls[0][0] == "taiwan"
    assert calls[0][1]["sort"] == "new_hot"
    assert calls[0][1]["direction"] == "desc"


def test_history_route_passes_source_scope(monkeypatch):
    repo_root = _make_case_dir("app_history_source_scope") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()
    calls = []

    def fake_load_history(world_id=None, source=None):
        calls.append((world_id, source))
        return {world_id: [{"world_id": world_id, "visits": 1, "favorites": 2}]}

    monkeypatch.setattr(service, "load_history", fake_load_history)

    response = client.get("/api/v1/history/wrld_1?source=db:job:Ch")

    assert response.status_code == 200
    assert calls == [("wrld_1", "db:job:Ch")]
    assert response.get_json()["items"][0]["favorites"] == 2


def test_import_legacy_route_moves_data_into_db():
    repo_root = _make_case_dir("app_import") / "repo"
    app_root = repo_root / "world_info_web"
    frontend_dir = app_root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")

    _write_json(
        repo_root / "world_info" / "scraper" / "raw_worlds.json",
        [
            {
                "id": "wrld_legacy",
                "name": "Legacy Alpha",
                "visits": 100,
                "favorites": 20,
                "authorId": "usr_legacy",
                "publicationDate": "2025-07-01T00:00:00Z",
            }
        ],
    )
    _write_json(
        repo_root / "world_info" / "scraper" / "history.json",
        {
            "wrld_legacy": [
                {"timestamp": 1722470400, "name": "Legacy Alpha", "visits": 10, "favorites": 1}
            ]
        },
    )

    service = WorldInfoService(repo_root=repo_root, app_root=app_root)
    app = create_app(service)
    client = app.test_client()

    import_response = client.post("/api/v1/import/legacy")
    worlds_response = client.get("/api/v1/worlds?source=db:import:legacy-raw")
    runs_response = client.get("/api/v1/runs?limit=8")

    assert import_response.status_code == 201
    assert import_response.get_json()["status"] == "completed"
    assert worlds_response.status_code == 200
    assert worlds_response.get_json()["count"] == 1
    assert worlds_response.get_json()["items"][0]["name"] == "Legacy Alpha"
    assert any(item["source"] == "db:import:legacy-raw" for item in runs_response.get_json()["items"])
