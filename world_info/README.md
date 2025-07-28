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
├─ ui.py                  # simple tkinter review interface
├─ docs/
│  ├─ index.html          # page listing approved worlds with filters
│  └─ approved_export.json
└─ unity_prefab_generator/
   └─ GenerateWorldCards.cs
```

Run the scripts in order:

1. `python3 scraper/scraper.py --keyword Taiwan` to search worlds by keyword
   or `python3 scraper/scraper.py --user usr_abc123` to fetch worlds from a creator.
2. `python3 scraper/review_tool.py` (or run `python3 ui.py` for a GUI)
3. `python3 scraper/exporter.py`

Copy `scraper/approved_export.json` into `docs/` to update the website or load
it inside Unity using the `GenerateWorldCards` editor script.

For a Traditional Chinese version of these instructions, see
[`README.zh_TW.md`](README.zh_TW.md).

More background details are provided in
[`complete_guide.zh_TW.md`](complete_guide.zh_TW.md).

