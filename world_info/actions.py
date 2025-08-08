from __future__ import annotations

from pathlib import Path
from typing import Optional

from scraper.scraper import (
    fetch_worlds,
    _load_headers,
    record_row,
)

try:  # optional dependency
    from openpyxl import load_workbook, Workbook  # type: ignore
except Exception:  # pragma: no cover - optional
    load_workbook = None  # type: ignore
    Workbook = None  # type: ignore

BASE = Path(__file__).resolve().parent
RAW_FILE = BASE / "scraper" / "raw_worlds.json"
USER_FILE = BASE / "scraper" / "user_worlds.json"
PERSONAL_FILE = BASE / "scraper" / "StarRiverArts.xlsx"
TAIWAN_FILE = BASE / "scraper" / "taiwan_worlds.xlsx"


def load_auth_headers(cookie: Optional[str], user: Optional[str], pw: Optional[str]) -> dict:
    """Return headers for authenticated requests."""
    return _load_headers(cookie, user, pw)


def search_keyword(keyword: str, headers: dict, limit: int = 50) -> list[dict]:
    """Fetch worlds by keyword."""
    return fetch_worlds(keyword=keyword, limit=limit, headers=headers)


def search_user(user_id: str, headers: dict, limit: int = 50) -> list[dict]:
    """Fetch worlds created by a user."""
    return fetch_worlds(user_id=user_id, limit=limit, headers=headers)


def search_fixed(keywords: str, headers: dict, blacklist: set[str]) -> list[dict]:
    """Fetch worlds for a comma-separated keyword list, skipping blacklist."""
    kw_list = [k.strip() for k in keywords.split(",") if k.strip()]
    all_worlds: list[dict] = []
    for kw in kw_list:
        if kw in blacklist:
            continue
        worlds = fetch_worlds(keyword=kw, limit=50, headers=headers)
        all_worlds.extend(worlds)
    return all_worlds


def save_worlds(worlds: list[dict], file: Path) -> None:
    """Save world records to an Excel file."""
    if Workbook is None or load_workbook is None:
        return
    headers = [
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
    ]
    if file.exists():
        wb = load_workbook(file)
        ws = wb.active
    else:
        wb = Workbook()
        ws = wb.active
        ws.append(headers)
    for w in worlds:
        ws.append(record_row(w))
    wb.save(file)
