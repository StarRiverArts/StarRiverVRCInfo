from __future__ import annotations

from pathlib import Path
from typing import Optional
import logging

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

try:
    from world_info.constants import (
        BASE,
        RAW_FILE,
        USER_FILE,
        STAR_RIVER_FILE,
        TAIWAN_FILE,
        METRIC_COLS,
    )
except ModuleNotFoundError:  # pragma: no cover - package path
    from constants import (
        BASE,
        RAW_FILE,
        USER_FILE,
        STAR_RIVER_FILE,
        TAIWAN_FILE,
        METRIC_COLS,
    )

logger = logging.getLogger(__name__)


def load_auth_headers(cookie: Optional[str], user: Optional[str], pw: Optional[str]) -> dict:
    """Return headers for authenticated requests."""
    return _load_headers(cookie, user, pw)


def search_keyword(keyword: str, headers: dict, limit: int = 50) -> list[dict]:
    """Fetch worlds by keyword."""
    worlds = fetch_worlds(keyword=keyword, limit=limit, headers=headers)
    logger.info("Fetched %d worlds for keyword '%s'", len(worlds), keyword)
    return worlds


def search_user(user_id: str, headers: dict, limit: int = 50) -> list[dict]:
    """Fetch worlds created by a user."""
    worlds = fetch_worlds(user_id=user_id, limit=limit, headers=headers)
    logger.info("Fetched %d worlds for user '%s'", len(worlds), user_id)
    return worlds


def search_fixed(keywords: str, headers: dict, blacklist: set[str]) -> list[dict]:
    """Fetch worlds for a comma-separated keyword list, skipping blacklist.

    Duplicate worlds across keywords are removed based on their ID.
    """
    kw_list = [k.strip() for k in keywords.split(",") if k.strip()]
    all_worlds: list[dict] = []
    seen: set[str] = set()
    for kw in kw_list:
        if kw in blacklist:
            continue
        worlds = fetch_worlds(keyword=kw, limit=50, headers=headers)
        for w in worlds:
            wid = w.get("id") or w.get("worldId")
            if wid and wid in seen:
                continue
            if wid:
                seen.add(wid)
            all_worlds.append(w)
    logger.info("Fetched %d worlds for keywords %s", len(all_worlds), kw_list)
    return all_worlds


def save_worlds(worlds: list[dict], file: Path) -> None:
    """Save world records to an Excel file."""
    if Workbook is None or load_workbook is None:
        logger.error("openpyxl not available; cannot save %s", file)
        return
    headers = ["爬取日期"] + METRIC_COLS
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
    logger.info("Saved %d worlds to %s", len(worlds), file)
