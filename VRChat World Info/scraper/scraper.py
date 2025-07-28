import json
from pathlib import Path

SAMPLE_WORLDS = [
    {
        "worldId": "wrld_aaa111",
        "name": "Taiwan Temple",
        "author": "TaiwanDev",
        "description": "A temple from Taiwan",
        "imageUrl": "https://example.com/image1.jpg",
        "tags": ["Taiwan"],
        "visits": 3000
    },
    {
        "worldId": "wrld_bbb222",
        "name": "Racing Track",
        "author": "Racer",
        "description": "Speed course",
        "imageUrl": "https://example.com/image2.jpg",
        "tags": ["Racing", "Taiwan"],
        "visits": 5000
    }
]


def main(output="raw_worlds.json"):
    """Write sample world data to a JSON file."""
    out_path = Path(__file__).parent / output
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(SAMPLE_WORLDS, f, ensure_ascii=False, indent=2)
    print(f"Wrote {len(SAMPLE_WORLDS)} worlds to {out_path}")


if __name__ == "__main__":
    main()
