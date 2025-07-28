import json
from pathlib import Path

BASE = Path(__file__).parent
RAW_FILE = BASE / "raw_worlds.json"
REVIEW_FILE = BASE / "reviewed_worlds.json"
OUTPUT_FILE = BASE / "approved_export.json"


def main():
    if not RAW_FILE.exists() or not REVIEW_FILE.exists():
        print("Missing input files")
        return

    with open(RAW_FILE, "r", encoding="utf-8") as f:
        worlds = json.load(f)
    with open(REVIEW_FILE, "r", encoding="utf-8") as f:
        reviews = json.load(f)

    approved = []
    for w in worlds:
        if reviews.get(w["worldId"]) == "approved":
            approved.append({
                "worldId": w.get("世界ID") or w.get("worldId"),
                "name": w.get("世界名稱") or w.get("name"),
                "author": w.get("author"),
                "description": w.get("description"),
                "imageUrl": w.get("imageUrl"),
                "tags": w.get("Tag") or w.get("tags"),
                "visits": w.get("瀏覽人次") or w.get("visits"),
                "capacity": w.get("世界大小"),
                "created_at": w.get("上傳日期"),
                "updated_at": w.get("更新日期"),
                "labsPublicationDate": w.get("實驗室日期"),
                "publicationDate": w.get("發布日期"),
                "favorites": w.get("收藏人次"),
                "heat": w.get("世界熱度"),
                "popularity": w.get("世界熱門度"),
                "worldUrl": w.get("世界連結"),
            })

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(approved, f, ensure_ascii=False, indent=2)
    print(f"Exported {len(approved)} worlds to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
