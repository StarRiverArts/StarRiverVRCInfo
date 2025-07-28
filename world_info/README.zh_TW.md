# VRChat 世界資訊工具

此工具可收集 VRChat 世界的基本資訊，讓使用者進行人工審核，
並匯出篩選後的 JSON 檔，可用於網站或 Unity 專案。

```
world_info/
├─ scraper/
│  ├─ scraper.py          # 從 VRChat API 取得世界資料
│  ├─ review_tool.py      # 標記世界是否核可
│  ├─ exporter.py         # 產生 approved_export.json
│  └─ raw_worlds.json     # 產生的範例資料
├─ ui.py                  # Tkinter 介面，可登入並搜尋世界
├─ docs/
│  ├─ index.html          # 提供篩選功能的世界清單頁面
│  └─ approved_export.json
└─ unity_prefab_generator/
   └─ GenerateWorldCards.cs
```

請在 ``scraper/headers.json`` 中輸入登入後取得的 Cookie，例如：

```
{"Cookie": "auth=...; twoFactorAuth=...; machineId=..."}
```

執行流程：

1. `python3 scraper/scraper.py --keyword Taiwan --limit 50` 以關鍵字搜尋世界。
   若要抓取某作者世界，使用 `--user usr_abc123`，會透過 Playwright 在
   `https://vrchat.com/home/user/` 頁面爬取 worldId，再查詢詳細資料。
   可額外加入 `--cookie`、`--username` 或 `--password` 提供驗證資訊，
   結果會輸出到 `raw_worlds.json`。
2. `python3 scraper/review_tool.py`（可選）或執行 `python3 ui.py`，
   透過圖形介面登入並搜尋、篩選世界。世界列表頁以表格方式呈現資料，方便檢視。
3. `python3 scraper/exporter.py`

若要抓取作者世界，需先安裝 `playwright` 套件並執行 `playwright install`。
若未安裝此套件，圖形介面仍可使用，但無法取得作者創作世界。

完成後，將 `scraper/approved_export.json` 複製到 `docs/` 以更新網站，
或在 Unity 中使用 `GenerateWorldCards` 編輯器腳本載入。

更多背景與架構說明請參考
[`complete_guide.zh_TW.md`](complete_guide.zh_TW.md)。
