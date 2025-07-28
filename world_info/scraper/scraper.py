
"""Retrieve world data from the VRChat API."""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Dict
import requests

BASE = Path(__file__).parent
HEADERS_FILE = BASE / "headers.json"


def _load_headers() -> Dict[str, str]:
    """Load HTTP headers (e.g. cookies) from headers.json."""
    if HEADERS_FILE.exists():
        with open(HEADERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"User-Agent": "Mozilla/5.0"}


HEADERS = _load_headers()


def search_worlds(keyword: str, n: int = 20) -> List[dict]:
    url = f"https://api.vrchat.cloud/api/1/worlds?search={keyword}&n={n}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def get_user_worlds(user_id: str, n: int = 20) -> List[dict]:
    url = f"https://api.vrchat.cloud/api/1/users/{user_id}/worlds?n={n}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


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
    parser.add_argument("--keyword", help="keyword to search", default=None)
    parser.add_argument("--user", help="creator userId", default=None)
    parser.add_argument("--n", type=int, default=20, help="number of worlds")
    args = parser.parse_args()

    if args.keyword:
        worlds = search_worlds(args.keyword, args.n)
    elif args.user:
        worlds = get_user_worlds(args.user, args.n)
    else:
        parser.error("--keyword or --user is required")

    parsed = [extract_info(w) for w in worlds]
    out = BASE / "raw_worlds.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(parsed, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(parsed)} worlds to {out}")


if __name__ == "__main__":
    main()

