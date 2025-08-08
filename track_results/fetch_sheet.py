"""Download racing data from a public Google Spreadsheet.

This module exposes a :func:`fetch_sheet` function returning the CSV rows so
other scripts can easily consume the data.
"""

from __future__ import annotations

import csv
import io
import time
import urllib.parse
import urllib.request
from typing import List


SHEET_ID = "1ifyJiZfDAJD4kf-67puKALA2ikEHCSrnw02dvewdFO0"
"""Spreadsheet ID containing racing data."""

SHEET_NAME = "歷史紀錄"
"""Worksheet name with the historical records."""


def fetch_sheet(
    sheet_id: str = SHEET_ID,
    sheet_name: str = SHEET_NAME,
    timeout: float | None = None,
    retries: int = 3,
) -> List[List[str]] | str:
    """Return the worksheet contents as a list of rows.

    Parameters
    ----------
    sheet_id:
        The spreadsheet identifier.
    sheet_name:
        The specific worksheet name to download.
    timeout:
        Seconds to wait for a response before raising a timeout error.
    retries:
        Maximum number of attempts before giving up.
    """

    sheet_name_encoded = urllib.parse.quote(sheet_name)
    url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet="
        f"{sheet_name_encoded}"
    )

    print(f"Fetching data from: {url}")

    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                csv_data = response.read().decode("utf-8")
            break
        except Exception as e:  # pragma: no cover - network errors are environment specific
            print(f"Attempt {attempt} failed: {e}")
            last_exc = e
            if attempt == retries:
                return f"Failed to fetch data after {retries} attempts: {e}"
            time.sleep(attempt)
    else:
        # Should not reach here since loop either breaks or returns
        return "Failed to fetch data: unknown error"

    reader = csv.reader(io.StringIO(csv_data))
    return list(reader)


if __name__ == "__main__":
    for row in fetch_sheet():
        print(row)
