# VR_RacingClubTW

This repository contains utilities for reading racing data from a public
Google Spreadsheet and showing the processed results inside VRChat.

## fetch_sheet.py

`fetch_sheet.py` downloads the `歷史紀錄` worksheet from the public Google
Spreadsheet identified by the ID `1ifyJiZfDAJD4kf-67puKALA2ikEHCSrnw02dvewdFO0`.
The script exports the sheet as CSV and prints each row. It requires Python 3 and
internet access. Run it with:

```bash
python3 fetch_sheet.py
```

If the environment blocks outbound network requests the script will fail with a
`403 Forbidden` error.

## generate_summary.py

`generate_summary.py` builds a small text report from the spreadsheet data.
The file `report/summary.txt` will be created containing a list of statistics.

```bash
python3 generate_summary.py
```

The script relies on `fetch_sheet.py` and therefore also requires internet
access.

## prefab/TextDisplay.cs

`prefab/TextDisplay.cs` is a simple Unity component that downloads a text file
from a URL and displays it in a `Text` UI element. Attach it to a prefab and set
the `url` and `targetText` fields in the Inspector.
