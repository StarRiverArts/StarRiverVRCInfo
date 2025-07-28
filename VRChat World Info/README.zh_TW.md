# VRChat 世界資訊工具

此工具可收集 VRChat 世界的基本資訊，讓使用者進行人工審核，
並匯出篩選後的 JSON 檔，可用於網站或 Unity 專案。

```
VRChat World Info/
├─ scraper/
│  ├─ scraper.py          # 爬取世界資料（範例實作）
│  ├─ review_tool.py      # 標記世界是否核可
│  ├─ exporter.py         # 產生 approved_export.json
│  └─ raw_worlds.json     # 產生的範例資料
├─ docs/
│  ├─ index.html          # 簡易清單頁面
│  └─ approved_export.json
└─ unity_prefab_generator/
   └─ GenerateWorldCards.cs
```

執行順序：

1. `python3 scraper/scraper.py`
2. `python3 scraper/review_tool.py`
3. `python3 scraper/exporter.py`

完成後，將 `scraper/approved_export.json` 複製到 `docs/` 以更新網站，
或在 Unity 中使用 `GenerateWorldCards` 編輯器腳本載入。
