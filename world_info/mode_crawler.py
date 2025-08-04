from __future__ import annotations

import json
from pathlib import Path
from typing import List, Dict

from analytics import update_daily_stats
from scraper.scraper import (
    search_worlds,
    get_user_worlds,
    update_history,
    record_row,
)

try:
    from openpyxl import Workbook, load_workbook  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    Workbook = None  # type: ignore
    load_workbook = None  # type: ignore

BASE = Path(__file__).resolve().parent
CONFIG_FILE = BASE / "config" / "search_modes.json"
ANALYTICS_DIR = BASE.parent / "analytics"

# Columns used when saving results to Excel
METRIC_COLS = [
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


def _save_worlds(worlds: List[dict], file_path: Path) -> None:
    """Append ``worlds`` to the Excel sheet at ``file_path``."""
    if Workbook is None or load_workbook is None:
        return
    if file_path.exists():
        wb = load_workbook(file_path)
        ws = wb.active
    else:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        wb = Workbook()
        ws = wb.active
        ws.append(METRIC_COLS)
    for w in worlds:
        ws.append(record_row(w))
    wb.save(file_path)


def _run_mode(name: str, cfg: Dict[str, object]) -> None:
    mode_type = cfg.get("type")
    worlds: List[dict] = []
    if mode_type == "keyword":
        for kw in cfg.get("keywords", []):
            worlds.extend(search_worlds(str(kw), limit=50))
    elif mode_type == "user":
        user_id = str(cfg.get("user_id", ""))
        if user_id:
            worlds = get_user_worlds(user_id, limit=50)
    else:
        return
    if not worlds:
        return
    update_history(worlds)
    sheet_file = ANALYTICS_DIR / f"{name}WorldSheet.xlsx"
    _save_worlds(worlds, sheet_file)
    stats_path = cfg.get("stats")
    if stats_path:
        stats_path = Path(str(stats_path))
        if not stats_path.is_absolute():
            stats_path = BASE.parent / stats_path
    update_daily_stats(name, worlds, stats_path)


def main() -> None:
    if not CONFIG_FILE.exists():
        raise SystemExit(f"Config file not found: {CONFIG_FILE}")
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        modes = json.load(f)
    for name, cfg in modes.items():
        if isinstance(cfg, dict):
            _run_mode(name, cfg)


if __name__ == "__main__":
    main()
