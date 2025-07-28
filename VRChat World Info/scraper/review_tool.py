import json
from pathlib import Path

BASE = Path(__file__).parent
RAW_FILE = BASE / "raw_worlds.json"
REVIEW_FILE = BASE / "reviewed_worlds.json"


def main():
    if not RAW_FILE.exists():
        print(f"Missing {RAW_FILE}")
        return
    with open(RAW_FILE, "r", encoding="utf-8") as f:
        worlds = json.load(f)

    reviewed = {w["worldId"]: "approved" for w in worlds}

    with open(REVIEW_FILE, "w", encoding="utf-8") as f:
        json.dump(reviewed, f, ensure_ascii=False, indent=2)
    print(f"Wrote reviews for {len(reviewed)} worlds to {REVIEW_FILE}")


if __name__ == "__main__":
    main()
