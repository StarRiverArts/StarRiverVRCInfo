# VRChat World Info

This tool collects information about VRChat worlds, lets you review the entries
and exports a filtered JSON file for use on a website or in Unity.

```
VRChat World Info/
├─ scraper/
│  ├─ scraper.py          # fetch world data (sample implementation)
│  ├─ review_tool.py      # mark worlds as approved
│  ├─ exporter.py         # create approved_export.json
│  └─ raw_worlds.json     # generated sample data
├─ docs/
│  ├─ index.html          # simple page listing approved worlds
│  └─ approved_export.json
└─ unity_prefab_generator/
   └─ GenerateWorldCards.cs
```

Run the scripts in order:

1. `python3 scraper/scraper.py`
2. `python3 scraper/review_tool.py`
3. `python3 scraper/exporter.py`

Copy `scraper/approved_export.json` into `docs/` to update the website or load
it inside Unity using the `GenerateWorldCards` editor script.
