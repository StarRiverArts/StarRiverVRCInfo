from __future__ import annotations

import os
from jinja2 import Environment, FileSystemLoader

LEADERBOARD_FILE = os.path.join("data", "leaderboard.txt")
OUTPUT_DIR = "site"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "index.html")
TEMPLATE_DIR = "templates"
TEMPLATE_FILE = "index.html"


def build_page() -> None:
    if os.path.exists(LEADERBOARD_FILE):
        with open(LEADERBOARD_FILE, "r", encoding="utf-8") as fh:
            content = fh.read()
    else:
        content = "No leaderboard data available."

    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
    template = env.get_template(TEMPLATE_FILE)
    rendered = template.render(content=content)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        fh.write(rendered)
    print(f"Page written to {OUTPUT_FILE}")


if __name__ == "__main__":
    build_page()

