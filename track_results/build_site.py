from __future__ import annotations

import os

LEADERBOARD_FILE = os.path.join("data", "leaderboard.txt")
OUTPUT_DIR = "site"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "index.html")


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang=\"en\">
<head>
    <meta charset=\"UTF-8\">
    <title>VR Racing Club Leaderboard</title>
    <style>
        body {{font-family: sans-serif; padding: 20px;}}
        pre {{background: #f4f4f4; padding: 10px; white-space: pre-wrap;}}
    </style>
</head>
<body>
<h1>VR Racing Club Leaderboard</h1>
<pre>{content}</pre>
</body>
</html>"""


def build_page() -> None:
    if os.path.exists(LEADERBOARD_FILE):
        with open(LEADERBOARD_FILE, "r", encoding="utf-8") as fh:
            content = fh.read()
    else:
        content = "No leaderboard data available."

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        fh.write(HTML_TEMPLATE.format(content=content))
    print(f"Page written to {OUTPUT_FILE}")


if __name__ == "__main__":
    build_page()
