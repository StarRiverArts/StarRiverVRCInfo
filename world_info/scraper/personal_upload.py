from __future__ import annotations

"""Upload personal world stats to a remote endpoint.

This script loads the configured player ID from ``settings.json`` and retrieves
that user's worlds via the VRChat API.  The results are appended to the local
history and then uploaded to a cloud service for further processing.
"""

import json
import os
from pathlib import Path
from typing import Any, Optional, Tuple

import requests

if __package__ is None or __package__ == "":
    import sys

    # Allow ``import world_info.actions`` when executed directly.
    sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
    from world_info.actions import load_auth_headers, search_user  # type: ignore
    from world_info.scraper.scraper import update_history  # type: ignore
else:  # pragma: no cover - package execution
    from ...actions import load_auth_headers, search_user
    from .scraper import update_history

BASE = Path(__file__).parent
SETTINGS_FILE = BASE / "settings.json"
DEFAULT_PLAYER_ID = "usr_example"
ENDPOINT_ENV = "UPLOAD_ENDPOINT"
USER_ENV = "UPLOAD_USER"
PASS_ENV = "UPLOAD_PASS"


def _load_settings() -> dict[str, Any]:
    if SETTINGS_FILE.exists():
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}


def _get_auth(settings: dict[str, Any]) -> Optional[Tuple[str, str]]:
    user = settings.get("upload_user") or os.getenv(USER_ENV)
    pw = settings.get("upload_pass") or os.getenv(PASS_ENV)
    if user and pw:
        return user, pw
    return None


def main() -> None:
    settings = _load_settings()
    player_id = settings.get("player_id", DEFAULT_PLAYER_ID)
    cookie = settings.get("cookie")
    headers = load_auth_headers(cookie, None, None)
    worlds = search_user(player_id, headers)
    update_history(worlds)

    url = settings.get("cloud_endpoint") or os.getenv(ENDPOINT_ENV)
    if not url:
        raise RuntimeError("No upload endpoint configured")

    auth = _get_auth(settings)
    response = requests.post(url, json=worlds, auth=auth, timeout=10)
    response.raise_for_status()


if __name__ == "__main__":
    main()
