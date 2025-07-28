
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
from pathlib import Path
from typing import Dict, List, Optional
import time

try:
    from playwright.sync_api import sync_playwright  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    sync_playwright = None  # type: ignore

import requests

BASE = Path(__file__).parent
HEADERS_FILE = BASE / "headers.json"


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


def _fetch_paginated(base_url: str, limit: int, delay: float,
                     headers: Optional[Dict[str, str]] = None) -> List[dict]:
    """Fetch up to ``limit`` worlds from ``base_url`` using pagination."""
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
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(parsed, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(parsed)} worlds to {args.out}")


if __name__ == "__main__":
    main()

