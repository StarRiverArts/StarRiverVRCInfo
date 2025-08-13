from pathlib import Path

BASE = Path(__file__).resolve().parent
SCRAPER_DIR = BASE / "scraper"
RAW_FILE = SCRAPER_DIR / "raw_worlds.json"
USER_FILE = SCRAPER_DIR / "user_worlds.json"
STAR_RIVER_FILE = SCRAPER_DIR / "StarRiverArts.xlsx"
TAIWAN_FILE = SCRAPER_DIR / "taiwan_worlds.xlsx"

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

LEGEND_TEXT = "藍:人次 綠:收藏 紅:熱度 紫:熱門度 橘:實驗室 黑:公開 灰:更新"
