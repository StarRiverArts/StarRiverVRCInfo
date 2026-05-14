from __future__ import annotations

import contextlib
import json
import os
import threading
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

playwright = pytest.importorskip("playwright.sync_api")
from playwright.sync_api import expect, sync_playwright


REPO_ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIR = REPO_ROOT / "world_info_web" / "frontend"


@contextlib.contextmanager
def frontend_server():
    handler = partial(SimpleHTTPRequestHandler, directory=str(FRONTEND_DIR))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def mock_api_payload(path: str, query: dict[str, list[str]]) -> dict:
    if path == "/api/v1/sources":
        return {
            "default_source": "db:all",
            "items": [
                {"key": "db:all", "label": "db:all", "count": 2, "source": "db:all"},
                {"key": "db:job:taiwan", "label": "Zh (taiwan)", "count": 2, "source": "db:job:taiwan"},
            ],
        }
    if path == "/api/v1/insights":
        return {
            "label": query.get("source", ["db:all"])[0],
            "summary": {
                "world_count": 2,
                "tracked_creators": 1,
                "new_worlds_14d": 1,
                "updated_worlds_30d": 1,
                "last_seen_at": "2026-05-09T09:00:00Z",
            },
            "new_hot_leaderboard": [
                {
                    "id": "wrld_alpha",
                    "name": "Alpha Plaza",
                    "author_name": "Creator One",
                    "author_id": "usr_1",
                    "days_since_publication": 3,
                    "visits_delta_1d": 20,
                    "visits_delta_7d": 100,
                    "publication_visits_per_day": 14,
                    "visits_growth_1d": 0.5,
                    "new_hot_score": 91,
                    "world_url": "https://example.com/world/alpha",
                }
            ],
            "worth_watching_leaderboard": [
                {
                    "id": "wrld_beta",
                    "name": "Beta Hall",
                    "author_name": "Creator One",
                    "author_id": "usr_1",
                    "favorite_rate": 8.3,
                    "heat": 11,
                    "popularity": 7,
                    "favorites_delta_7d": 6,
                    "visits_delta_7d": 44,
                    "since_update_visits_per_day": 9,
                    "worth_watching_score": 77,
                    "discovery_reason": "momentum",
                }
            ],
            "rising_now_leaderboard": [],
            "growth_leaderboard": [
                {
                    "id": "wrld_alpha",
                    "name": "Alpha Plaza",
                    "author_name": "Creator One",
                    "author_id": "usr_1",
                    "visits_delta_7d": 100,
                    "visits_delta_30d": 200,
                    "favorites_delta_7d": 10,
                    "favorite_rate": 8.3,
                    "visits_growth_7d": 0.35,
                    "momentum_score": 73,
                }
            ],
            "dormant_revival_leaderboard": [],
            "creator_momentum": [
                {
                    "author_name": "Creator One",
                    "author_id": "usr_1",
                    "creator_momentum_score": 70,
                    "recent_visits_delta_7d": 90,
                    "recent_visits_delta_30d": 180,
                    "active_worlds_30d": 1,
                    "breakout_worlds": 1,
                    "rising_worlds": 1,
                    "worth_watching_worlds": 1,
                    "average_favorite_rate": 8.3,
                    "lead_world_name": "Alpha Plaza",
                }
            ],
            "authors": [
                {
                    "author_name": "Creator One",
                    "author_id": "usr_1",
                    "world_count": 2,
                    "total_visits": 400,
                    "total_favorites": 30,
                    "recent_visits_delta_30d": 180,
                    "active_worlds_30d": 1,
                    "average_favorite_rate": 8.3,
                    "top_world_name": "Alpha Plaza",
                    "top_world_share": 70,
                }
            ],
            "anomalies": {"summary": {}, "items": []},
            "update_effectiveness": {"summary": {}, "items": []},
            "signals": {"summary": {}, "correlations": [], "charts": [], "leaderboards": {}},
            "performance": {"summary": {}, "items": []},
        }
    if path == "/api/v1/health":
        return {"status": "ok"}
    if path == "/api/v1/review/self-check":
        return {"status": "ok", "warnings": []}
    if path == "/api/v1/analytics/daily-stats":
        return {"items": []}
    if path == "/api/v1/jobs":
        return {
            "items": [
                {
                    "job_key": "taiwan",
                    "label": "Zh (taiwan)",
                    "type": "keyword",
                    "source": "db:job:taiwan",
                    "ready": True,
                    "reason": "config ok",
                    "latest_run": None,
                }
            ]
        }
    if path == "/api/v1/runs":
        return {"items": []}
    if path == "/api/v1/query-analytics":
        return {"summary": {}, "items": []}
    if path == "/api/v1/rate-limits":
        return {"summary": {}, "items": []}
    if path == "/api/v1/topics":
        return {"items": []}
    if path == "/api/v1/jobs/diagnostics":
        return {"items": []}
    if path == "/api/v1/jobs/taiwan/blacklist":
        return {"items": []}
    if path == "/api/v1/jobs/taiwan/creator-whitelist":
        return {"items": []}
    if path == "/api/v1/jobs/taiwan/creator-blacklist":
        return {"items": []}
    if path == "/api/v1/auto-sync/status":
        return {
            "jobs": {
                "taiwan": {
                    "job_key": "taiwan",
                    "label": "Zh (taiwan)",
                    "interval": "1d",
                    "next_run": None,
                    "last_auto_run": None,
                    "last_attempt_at": None,
                    "last_success_trigger": None,
                    "overdue": False,
                    "running": False,
                    "last_error": None,
                }
            },
            "rate_limit": {},
        }
    raise KeyError(path)


def install_api_mocks(page):
    unhandled: list[str] = []

    def handle(route):
        parsed = urlparse(route.request.url)
        try:
            payload = mock_api_payload(parsed.path, parse_qs(parsed.query))
        except KeyError:
            unhandled.append(parsed.path)
            route.fulfill(
                status=404,
                content_type="application/json",
                body=json.dumps({"error": f"Unhandled mock route: {parsed.path}"}),
            )
            return
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(payload),
        )

    page.route("**/api/v1/**", handle)
    return unhandled


def test_frontend_page_isolation_with_mocked_api():
    if os.environ.get("RUN_PLAYWRIGHT_SMOKE") != "1":
        pytest.skip("Set RUN_PLAYWRIGHT_SMOKE=1 to run Playwright smoke tests.")

    with frontend_server() as base_url:
        try:
            playwright_manager = sync_playwright().start()
        except Exception as exc:  # pragma: no cover - environment dependent
            pytest.skip(f"Playwright runtime is unavailable in this environment: {exc}")

        try:
            browser = playwright_manager.chromium.launch()
        except Exception as exc:  # pragma: no cover - environment dependent
            playwright_manager.stop()
            pytest.skip(f"Chromium is not available for Playwright smoke tests: {exc}")

        page_errors: list[str] = []
        context = browser.new_context()
        page = context.new_page()
        unhandled_routes = install_api_mocks(page)
        page.on("pageerror", lambda error: page_errors.append(str(error)))

        page.goto(base_url, wait_until="domcontentloaded")
        expect(page.locator("#current-source-label")).to_have_text("db:all")
        expect(page.locator(".dashboard-section[data-dashboard-section='briefing']")).to_be_visible()
        expect(page.locator(".debug-card")).to_be_hidden()

        page.get_by_role("button", name="Debug").click()
        expect(page.locator(".debug-card")).to_be_visible()
        expect(page.locator("#debug-panel-status-body tr").first).to_be_visible()
        expect(page.locator(".dashboard-section[data-dashboard-section='briefing']")).to_be_hidden()
        expect(page.locator(".auth-card")).to_be_hidden()

        page.get_by_role("button", name="Operations").click()
        page.locator("[data-operations-tab='diagnostics']").click()
        expect(page.locator(".review-card")).to_be_visible()
        expect(page.locator(".diagnostics-card").first).to_be_visible()
        expect(page.locator(".auth-card")).to_be_hidden()
        expect(page.locator(".auto-sync-card")).to_be_hidden()

        context.close()
        browser.close()
        playwright_manager.stop()

    assert not page_errors
    assert not unhandled_routes
