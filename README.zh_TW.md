# VRChat WorldInfo by StarRiver

本專案包含兩個獨立的工具集：

## Racing Club
用於從 Google 試算表下載賽車紀錄並產生文字或 HTML 形式的排行榜。詳盡使用方式請參考 [`Racing Club/README.zh_TW.md`](Racing%20Club/README.zh_TW.md)。

## VRChat World Info
用於收集 VRChat 世界資訊、人工審核並匯出可在網站或 Unity 中使用的 JSON 檔案。詳細流程請參考 [`VRChat World Info/README.zh_TW.md`](VRChat%20World%20Info/README.zh_TW.md)。

---

## 檔案結構

```
VRChat WorldInfo by StarRiver/
├─ README.md                 # 英文說明
├─ README.zh_TW.md           # 本文件，繁體中文說明
├─ Racing Club/              # 賽車紀錄相關腳本與資源
└─ VRChat World Info/        # VRChat 世界資訊工具
```

### Racing Club 子目錄

```
Racing Club/
├─ fetch_sheet.py       # 下載試算表中的「歷史紀錄」工作表
├─ generate_summary.py  # 產生統計摘要報告
├─ build_leaderboards.py# 建立綜合排行榜文字檔
├─ build_site.py        # 產生可放到 GitHub Pages 的網站
├─ prefab/TextDisplay.cs# Unity 組件，用來載入並顯示文字檔
├─ ui.py                # Tkinter 圖形介面
└─ README.zh_TW.md      # 詳細說明
```

### VRChat World Info 子目錄

```
VRChat World Info/
├─ scraper/             # 爬蟲與審核腳本
├─ docs/                # 可放到 GitHub Pages 的簡易網站
├─ unity_prefab_generator/
└─ README.zh_TW.md      # 詳細說明
```

---

## 快速開始

1. **安裝 Python 3**：兩個工具集都需要 Python 3 環境。
2. **（選用）啟用網路**：若環境無法連線到 Google，`fetch_sheet.py` 等腳本會無法下載試算表資料。
3. **依序執行腳本**：可根據 `Racing Club/README.zh_TW.md` 及 `VRChat World Info/README.zh_TW.md` 的指示操作。
4. **更新網站或 Unity 專案**：產生的 `site/` 或 `docs/` 內容可以直接部署到 GitHub Pages；JSON 檔也可在 Unity 中載入。

---

## 注意事項

- 本倉庫的 `Racing Club` 腳本須能連線到 Google 服務方可正常運作。
- `VRChat World Info` 相關檔案會產生 JSON 於本地端，預設不會提交到版本控制。

