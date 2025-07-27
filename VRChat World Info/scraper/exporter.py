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
                "worldId": w["worldId"],
                "name": w["name"],
                "author": w["author"],
                "description": w["description"],
                "imageUrl": w["imageUrl"],
                "tags": w["tags"],
                "visits": w["visits"],
            })

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(approved, f, ensure_ascii=False, indent=2)
    print(f"Exported {len(approved)} worlds to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
