# VRChat WorldInfo by StarRiver

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

## build_leaderboards.py

`build_leaderboards.py` combines fetching the sheet, storing the raw CSV under
`data/history.csv` and producing a simple text leaderboard at
`data/leaderboard.txt`. The leaderboard lists the fastest driver for each
track, best times per vehicle, each driver's career best and more. If a row is
marked as belonging to a championship, it will be listed separately.

Run it with:

```bash
python3 build_leaderboards.py [--output-csv]
```

Use `--output-csv` to additionally write each leaderboard as a CSV file under
`data/`.

## prefab/TextDisplay.cs

`prefab/TextDisplay.cs` is a simple Unity component that downloads a text file
from a URL and displays it in a `Text` UI element. Attach it to a prefab and set
the `url` and `targetText` fields in the Inspector.

## build_site.py

`build_site.py` reads the generated leaderboard text and renders a static
`site/index.html` page using the Jinja2 template located at `templates/index.html`.
Customize the look of the leaderboard by editing that template or swapping in
your own.

```bash
python3 build_site.py
```

After committing the contents of the `site` directory you can configure GitHub
Pages to publish from that folder.

## ui.py

`ui.py` offers a small Tkinter interface. It lets you enter a Google Sheet URL
or a local CSV path, then view various leaderboards across several tabs. One tab
allows manual record entry while another provides a simple championship bracket
generator. The final tab embeds the VRChat world review tool so you can approve
world data directly inside the same window.

For Traditional Chinese instructions, see
[`README.zh_TW.md`](README.zh_TW.md).

