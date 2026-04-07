# VRChat 世界資料整理與展示系統使用指南

以下內容概述了整個專案的目的、結構、環境與資料處理流程，取自先前對話中給出的詳細說明。

## 專案目的

- 建立一套針對 VRChat 世界的整理與展示系統。
- 透過本地爬蟲與人工審核流程，過濾符合特定主題的世界資訊。
- 產生 JSON 供 GitHub Pages 或 Unity Prefab 使用。

## 環境與語言

| 模組 | 技術 | 說明 |
| --- | --- | --- |
| 本地爬蟲 | Python 3.x + Playwright | 爬取並產出原始資料 |
| 人工審核工具 | Python CLI 或 tkinter GUI | 逐筆分類資料 |
| 網頁前端 | HTML + JavaScript + GitHub Pages | 靜態網站展示 |
| 資料同步 | Git + GitHub | 版本控制與同步 |
| Unity Prefab | Unity 2022.3 + C# | 自動生成 UI Prefab |

## 目錄結構

```text
/my-vrchat-worlds/
├─ scraper/
│  ├─ scraper.py
│  ├─ review_tool.py
│  ├─ exporter.py
│  ├─ personal_upload.py
│  ├─ raw_worlds.json
│  ├─ reviewed_worlds.json
│  └─ approved_export.json
├─ docs/
│  ├─ index.html
│  ├─ script.js
│  ├─ style.css
│  └─ approved_export.json
├─ unity_prefab_generator/
│  ├─ GenerateWorldCards.cs
│  └─ WorldCardTemplate.prefab
```

## 資料處理流程

1. `scraper.py`：登入 VRChat、搜尋關鍵字取得世界資料並輸出 `raw_worlds.json`。
2. `review_tool.py`：讀取 raw 資料，人工標記為 approved 或 rejected，結果存入 `reviewed_worlds.json`。
3. `exporter.py`：濾除未核可的世界，產生 `approved_export.json`。
4. `personal_upload.py`：把特定玩家的世界統計上傳到雲端。
5. `docs/index.html`：於 GitHub Pages 顯示世界清單。
6. `GenerateWorldCards.cs`：在 Unity 中依 `WorldCardTemplate.prefab` 建立卡片，輸出可在 VRChat 內作為螢幕的 Prefab。

## JSON 範例

```json
[
  {
    "worldId": "wrld_aaa111",
    "name": "Taiwan Temple",
    "author": "TaiwanDev",
    "description": "A temple from Taiwan",
    "imageUrl": "https://...",
    "tags": ["Taiwan"],
    "visits": 3000
  }
]
```

更多細節請參考專案其他 README 檔案。
