"""Download racing data from a public Google Spreadsheet.

This module exposes a :func:`fetch_sheet` function returning the CSV rows so
other scripts can easily consume the data.
"""

from __future__ import annotations

import csv
import io
import urllib.parse
import urllib.request
from typing import List


SHEET_ID = "1ifyJiZfDAJD4kf-67puKALA2ikEHCSrnw02dvewdFO0"
"""Spreadsheet ID containing racing data."""

SHEET_NAME = "歷史紀錄"
"""Worksheet name with the historical records."""


def fetch_sheet(sheet_id: str = SHEET_ID, sheet_name: str = SHEET_NAME) -> List[List[str]]:
    """Return the worksheet contents as a list of rows.

    Parameters
    ----------
    sheet_id:
        The spreadsheet identifier.
    sheet_name:
        The specific worksheet name to download.
    """

    sheet_name_encoded = urllib.parse.quote(sheet_name)
    url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet="
        f"{sheet_name_encoded}"
    )

    print(f"Fetching data from: {url}")

    try:
        with urllib.request.urlopen(url) as response:
            csv_data = response.read().decode("utf-8")
    except Exception as e:  # pragma: no cover - network errors are environment specific
        print("Failed to fetch data:", e)
        raise

    reader = csv.reader(io.StringIO(csv_data))
    return list(reader)


if __name__ == "__main__":
    for row in fetch_sheet():
        print(row)
