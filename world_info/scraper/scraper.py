
"""Retrieve world data from the VRChat API.

This script queries the unofficial VRChat API to fetch world information
and stores the results as a JSON list.  Authentication headers such as
cookies should be provided in ``headers.json``.  The file is ignored by
git so your credentials remain local.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List
import time

import requests

BASE = Path(__file__).parent
HEADERS_FILE = BASE / "headers.json"


def _load_headers() -> Dict[str, str]:
    """Load HTTP headers from ``headers.json`` if it exists."""
    if HEADERS_FILE.exists():
        with open(HEADERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"User-Agent": "Mozilla/5.0"}


HEADERS = _load_headers()


def _fetch_paginated(base_url: str, limit: int, delay: float) -> List[dict]:
    """Fetch up to ``limit`` worlds from ``base_url`` using pagination."""
    results: List[dict] = []
    offset = 0
    while len(results) < limit:
        remaining = min(60, limit - len(results))
        url = f"{base_url}&n={remaining}&offset={offset}"
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        chunk = r.json()
        if not isinstance(chunk, list):
            break
        results.extend(chunk)
        if len(chunk) < remaining:
            break
        offset += len(chunk)
        time.sleep(delay)
    return results[:limit]


def search_worlds(keyword: str, limit: int = 20, delay: float = 1.0) -> List[dict]:
    base = f"https://api.vrchat.cloud/api/1/worlds?search={keyword}"
    return _fetch_paginated(base, limit, delay)


def get_user_worlds(user_id: str, limit: int = 20, delay: float = 1.0) -> List[dict]:
    base = f"https://api.vrchat.cloud/api/1/users/{user_id}/worlds?"
    return _fetch_paginated(base, limit, delay)


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


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Query VRChat worlds")
    parser.add_argument("--keyword", help="search keyword")
    parser.add_argument("--user", help="creator userId")
    parser.add_argument("--limit", type=int, default=20, help="maximum worlds")
    parser.add_argument("--delay", type=float, default=1.0, help="seconds between requests")
    parser.add_argument("--out", type=Path, default=BASE / "raw_worlds.json", help="output JSON path")
    args = parser.parse_args()

    if args.keyword:
        worlds = search_worlds(args.keyword, args.limit, args.delay)
    elif args.user:
        worlds = get_user_worlds(args.user, args.limit, args.delay)
    else:
        parser.error("--keyword or --user is required")

    parsed = [extract_info(w) for w in worlds]
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(parsed, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(parsed)} worlds to {args.out}")


if __name__ == "__main__":
    main()

