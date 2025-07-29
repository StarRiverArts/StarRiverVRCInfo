
"""Retrieve world data from the VRChat API.

This script queries the unofficial VRChat API to fetch world information and
stores the results as a JSON list.  Searching by keyword uses the HTTP API.
To list a creator's worlds we scrape the website using Playwright because there
is no stable API endpoint.  Authentication headers can be supplied in
``headers.json`` or via command line options.  The credentials file is ignored
by git so your secrets remain local.
"""

from __future__ import annotations

import base64
import json
import csv
import datetime as dt
from pathlib import Path
from typing import Dict, List, Optional
import time

try:
    from openpyxl import Workbook, load_workbook  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    Workbook = None  # type: ignore
    load_workbook = None  # type: ignore

try:

    from playwright.sync_api import sync_playwright  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    sync_playwright = None  # type: ignore

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    requests = None  # type: ignore

BASE = Path(__file__).parent
HEADERS_FILE = BASE / "headers.json"
HISTORY_FILE = BASE / "history.json"
HISTORY_TABLE = BASE / "history_table.csv"
EXCEL_FILE = BASE / "worlds.xlsx"


def _load_headers(cookie: Optional[str] = None,
                  username: Optional[str] = None,
                  password: Optional[str] = None) -> Dict[str, str]:
    """Load HTTP headers from ``headers.json`` and command line options."""

    headers = {"User-Agent": "Mozilla/5.0"}

    if HEADERS_FILE.exists():
        with open(HEADERS_FILE, "r", encoding="utf-8") as f:
            try:
                headers.update(json.load(f))
            except json.JSONDecodeError:
                pass

    if cookie:
        headers["Cookie"] = cookie

    if username and password:
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"

    return headers


HEADERS: Dict[str, str] = _load_headers()


def record_row(world: dict, now: Optional[int] = None) -> List[object]:
    """Return a metrics row for history tables and Excel."""
    ts_now = dt.datetime.fromtimestamp(now, dt.timezone.utc) if isinstance(now, int) else dt.datetime.now(dt.timezone.utc)

    pub = _parse_date(world.get("publicationDate"))
    updated = _parse_date(world.get("updated_at"))
    labs = _parse_date(world.get("labsPublicationDate"))

    days_labs_to_pub = (pub - labs).days if pub and labs else ""
    visits = world.get("visits") or 0
    favs = world.get("favorites") or 0
    ratio_vf = round(visits / favs, 2) if favs else ""
    since_update = (ts_now - updated).days if updated else ""
    since_pub = (ts_now - pub).days if pub else 0
    visits_per_day = round(visits / since_pub, 2) if since_pub > 0 else ""

    return [
        world.get("name"),
        world.get("id"),
        world.get("publicationDate"),
        world.get("updated_at"),
        visits,
        world.get("capacity"),
        favs,
        world.get("heat"),
        world.get("popularity"),
        days_labs_to_pub,
        ratio_vf,
        since_update,
        world.get("releaseStatus"),
        visits_per_day,
    ]


def _parse_date(value: Optional[str]) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt_obj = dt.datetime.fromisoformat(value)
        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
        return dt_obj
    except Exception:
        return None


def load_history() -> Dict[str, List[dict]]:
    """Load the long-term history file if present."""
    if HISTORY_FILE.exists():
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}


def update_history(worlds: List[dict], threshold: int = 3600) -> Dict[str, List[dict]]:
    """Append new stats to ``history.json`` unless recorded recently."""
    history = load_history()
    now = int(time.time())
    appended = False
    for w in worlds:
        wid = w.get("id") or w.get("worldId")
        if not wid:
            continue
        recs = history.setdefault(wid, [])
        if recs and now - recs[-1].get("timestamp", 0) < threshold:
            continue
        rec = {
            "timestamp": now,
            "visits": w.get("visits"),
            "favorites": w.get("favorites"),
            "heat": w.get("heat"),
            "popularity": w.get("popularity"),
            "updated_at": w.get("updated_at"),
            "publicationDate": w.get("publicationDate"),
            "labsPublicationDate": w.get("labsPublicationDate"),
        }
        recs.append(rec)
        row = record_row(w, now)
        _append_history_table(row)
        _append_excel_row(row)
        appended = True
    if appended:
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    return history


def _append_history_table(row: List[object]) -> None:
    """Append a metrics row to ``history_table.csv``."""
    if not HISTORY_TABLE.exists():
        with open(HISTORY_TABLE, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
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
            ])

    with open(HISTORY_TABLE, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)

    with open(HISTORY_TABLE, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)

def _append_excel_row(row: List[object]) -> None:
    """Append a metrics row to ``worlds.xlsx``."""
    if Workbook is None or load_workbook is None:
        return

    if EXCEL_FILE.exists():
        wb = load_workbook(EXCEL_FILE)
        ws = wb.active
    else:
        wb = Workbook()
        ws = wb.active
        ws.append([
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
        ])
    ws.append(row)
    wb.save(EXCEL_FILE)


def _fetch_paginated(base_url: str, limit: int, delay: float,
                     headers: Optional[Dict[str, str]] = None) -> List[dict]:
    """Fetch up to ``limit`` worlds from ``base_url`` using pagination."""
    if requests is None:
        raise RuntimeError("requests package is required")
    results: List[dict] = []
    offset = 0
    while len(results) < limit:
        remaining = min(60, limit - len(results))
        sep = '&' if '?' in base_url else '?'  # handle URLs with no query yet
        url = f"{base_url}{sep}n={remaining}&offset={offset}"
        try:
            r = requests.get(url, headers=headers or HEADERS, timeout=30)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:  # pragma: no cover - runtime only
            if e.response is not None and e.response.status_code == 403:
                raise RuntimeError(
                    "403 Forbidden: check your cookie or login credentials"
                ) from e
            raise
        chunk = r.json()
        if not isinstance(chunk, list):
            break
        results.extend(chunk)
        if len(chunk) < remaining:
            break
        offset += len(chunk)
        time.sleep(delay)
    return results[:limit]


def search_worlds(keyword: str, limit: int = 20, delay: float = 1.0,
                  headers: Optional[Dict[str, str]] = None) -> List[dict]:
    if requests is None:
        raise RuntimeError("requests package is required")

    base = f"https://api.vrchat.cloud/api/1/worlds?search={keyword}"
    return _fetch_paginated(base, limit, delay, headers)


def _cookie_to_playwright(cookie_str: str) -> List[Dict[str, str]]:
    """Convert a standard cookie header string into Playwright cookie dicts."""
    cookies: List[Dict[str, str]] = []
    for part in cookie_str.split(";"):
        if "=" in part:
            name, value = part.strip().split("=", 1)
            cookies.append({"name": name, "value": value, "url": "https://vrchat.com"})
    return cookies


def get_user_worlds(user_id: str, limit: int = 20, delay: float = 1.0,
                    headers: Optional[Dict[str, str]] = None) -> List[dict]:
    """Fetch worlds created by the given user ID.

    VRChat does not expose an official endpoint for this, so we load the
    user's page using Playwright and parse the world cards from the HTML.
    """

    if sync_playwright is None:
        raise RuntimeError("playwright is required for user world scraping")
    if requests is None:
        raise RuntimeError("requests package is required")

    headers = headers or HEADERS
    cookie_str = headers.get("Cookie", "")

    url = f"https://vrchat.com/home/user/{user_id}"
    results: List[dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        if cookie_str:
            context.add_cookies(_cookie_to_playwright(cookie_str))
        page = context.new_page()
        page.goto(url)
        page.wait_for_timeout(int(delay * 1000))

        while len(results) < limit:
            cards = page.query_selector_all("a[href^='/home/world/wrld_']")
            for card in cards[len(results):]:
                name = (card.inner_text() or "").strip()
                link = card.get_attribute("href") or ""
                world_id = link.split("/")[-1]
                results.append({"name": name, "id": world_id})
                if len(results) >= limit:
                    break

            if len(results) >= limit:
                break
            show_more = page.query_selector("button:has-text('Show More')")
            if show_more:
                show_more.click()
                page.wait_for_timeout(int(delay * 1000))
            else:
                break

        browser.close()

    # For each world ID we can fetch detailed info via the official world endpoint
    details: List[dict] = []
    for r in results:
        try:
            info = requests.get(
                f"https://api.vrchat.cloud/api/1/worlds/{r['id']}",
                headers=headers,
                timeout=30,
            )
            info.raise_for_status()
            details.append(info.json())
        except requests.HTTPError:
            continue

    return details[:limit]


def extract_info(world: dict) -> Dict[str, object]:
    return {
        "worldId": world.get("id"),
        "世界名稱": world.get("name"),
        "世界ID": world.get("id"),
        "世界大小": world.get("capacity"),
        "上傳日期": world.get("created_at"),
        "更新日期": world.get("updated_at"),
        "實驗室日期": world.get("labsPublicationDate"),
        "發布日期": world.get("publicationDate"),
        "瀏覽人次": world.get("visits"),
        "收藏人次": world.get("favorites"),
        "世界熱度": world.get("heat"),
        "世界熱門度": world.get("popularity"),
        "Tag": world.get("tags"),
        "世界連結": f"https://vrchat.com/home/world/{world.get('id')}",
    }


def fetch_worlds(*,
                 keyword: Optional[str] = None,
                 user_id: Optional[str] = None,
                 limit: int = 20,
                 delay: float = 1.0,
                 headers: Optional[Dict[str, str]] = None) -> List[dict]:
    """High level helper to fetch worlds by keyword or user ID."""
    if keyword:
        return search_worlds(keyword, limit, delay, headers)
    if user_id:
        return get_user_worlds(user_id, limit, delay, headers)
    raise ValueError("keyword or user_id required")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Query VRChat worlds")
    parser.add_argument("--keyword", help="search keyword")
    parser.add_argument("--user", help="creator userId")
    parser.add_argument("--limit", type=int, default=20, help="maximum worlds")
    parser.add_argument("--delay", type=float, default=1.0, help="seconds between requests")
    parser.add_argument("--out", type=Path, default=BASE / "raw_worlds.json", help="output JSON path")
    parser.add_argument("--cookie", help="authentication cookie string")
    parser.add_argument("--username", help="basic auth username")
    parser.add_argument("--password", help="basic auth password")
    args = parser.parse_args()

    global HEADERS
    HEADERS = _load_headers(args.cookie, args.username, args.password)

    if args.keyword:
        worlds = search_worlds(args.keyword, args.limit, args.delay, HEADERS)
    elif args.user:
        worlds = get_user_worlds(args.user, args.limit, args.delay, HEADERS)
    else:
        parser.error("--keyword or --user is required")

    parsed = [extract_info(w) for w in worlds]
    update_history(worlds)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(parsed, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(parsed)} worlds to {args.out}")


if __name__ == "__main__":
    main()

