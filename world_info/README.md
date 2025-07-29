# VRChat World Info

This tool collects information about VRChat worlds, lets you review the entries
and exports a filtered JSON file for use on a website or in Unity.

```
world_info/
├─ scraper/
│  ├─ scraper.py          # query the VRChat API for world info
│  ├─ review_tool.py      # mark worlds as approved
│  ├─ exporter.py         # create approved_export.json
│  └─ raw_worlds.json     # generated sample data
├─ ui.py                  # Tkinter interface for login and world search
├─ docs/
│  ├─ index.html          # page listing approved worlds with filters
│  └─ approved_export.json
└─ unity_prefab_generator/
   └─ GenerateWorldCards.cs
```

Install the required Python packages with::

  pip install -r requirements.txt

If you plan to fetch a creator's worlds, also run::

  playwright install

Create ``scraper/headers.json`` with your login cookie::

  {"Cookie": "auth=...; twoFactorAuth=...; machineId=..."}

Run the tools in order:

1. ``python3 scraper/scraper.py --keyword Taiwan --limit 50`` to search worlds.
   To collect a creator's worlds, use ``--user usr_abc123`` which will launch a
   headless browser via Playwright to scrape ``https://vrchat.com/home/user`` and
   then query each world ID.  Add ``--cookie``, ``--username`` or ``--password``
   to supply authentication headers. Results are written to ``raw_worlds.json``.
2. ``python3 scraper/review_tool.py`` (optional) or run ``python3 ui.py`` for
   an interface that lets you log in, fetch worlds and apply filters. The world
   list tab now shows results in a sortable table. A new "History" tab tracks
   visits, favorites and heat over time with a simple line chart. Each fetch
   also appends a row to ``scraper/history_table.xlsx`` and ``scraper/worlds.xlsx``
   with additional metrics like visit/favorite ratio and days since last update.
   ``worlds.xlsx`` can be opened in Excel to edit or import existing data.
3. ``python3 scraper/exporter.py``

Fetching a creator's worlds requires the ``playwright`` package.  Install it and
run ``playwright install`` before using the ``--user`` option.  If the package
is missing, the UI will still run but the creator-world feature will be
disabled.

Copy `scraper/approved_export.json` into `docs/` to update the website or load
it inside Unity using the `GenerateWorldCards` editor script.

For a Traditional Chinese version of these instructions, see
[`README.zh_TW.md`](README.zh_TW.md).

More background details are provided in
[`complete_guide.zh_TW.md`](complete_guide.zh_TW.md).

