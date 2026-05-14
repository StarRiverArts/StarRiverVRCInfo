# VRChat World Info

> Legacy notice: `world_info/` is now a maintenance-only local toolchain.
> New feature work should move to `world_info_web/` unless there is a clear legacy-only reason.

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

Create ``scraper/headers.json`` with your login cookie::

  {"Cookie": "auth=...; twoFactorAuth=...; machineId=..."}

Run the tools in order:

1. ``python3 scraper/scraper.py --keyword Taiwan --limit 50`` to search worlds.
   To collect a creator's worlds, use ``--user usr_abc123`` which now queries
   the worlds API with a ``userId`` filter instead of scraping the website.
   Add ``--cookie``, ``--username`` or ``--password`` only when you need
   authenticated access. Results are written to ``raw_worlds.json``.
2. ``python3 scraper/review_tool.py`` (optional) or run ``python3 ui.py`` for
   an interface that lets you log in, fetch worlds and apply filters. The world
   list tab now shows results in a sortable table. A new "History" tab tracks
   visits, favorites and heat over time with a simple line chart. A "Log" tab
   displays execution messages and errors. Each fetch
   also appends a row to ``scraper/history_table.xlsx`` and ``scraper/TaiwanWorlds.xlsx``
   with additional metrics like visit/favorite ratio, days since last update and
   the fetch date (``YYYY/MM/DD``) so you know when the data was retrieved.
   These Excel files require the ``openpyxl`` package and can be edited directly
   in spreadsheet software.
3. ``python3 scraper/exporter.py``

Copy `scraper/approved_export.json` into `docs/` to update the website or load
it inside Unity using the `GenerateWorldCards` editor script.

For a Traditional Chinese version of these instructions, see
[`README.zh_TW.md`](README.zh_TW.md).

More background details are provided in
[`complete_guide.zh_TW.md`](complete_guide.zh_TW.md).

