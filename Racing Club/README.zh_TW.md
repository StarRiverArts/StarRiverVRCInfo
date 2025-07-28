# VRChat WorldInfo by StarRiver

本資料夾提供數個腳本，用來讀取公開 Google 試算表中的賽車紀錄，並產生排行榜或在 VRChat 中顯示。

## fetch_sheet.py

`fetch_sheet.py` 會從 ID `1ifyJiZfDAJD4kf-67puKALA2ikEHCSrnw02dvewdFO0` 的 Google 試算表下載「歷史紀錄」工作表。腳本將資料匯出成 CSV 並逐行印出，需要 Python 3 與網路連線。

```bash
python3 fetch_sheet.py
```

若環境封鎖外部連線，將出現 `403 Forbidden` 錯誤。

## generate_summary.py

`generate_summary.py` 會根據試算表建立簡易文字報告，輸出在 `report/summary.txt`。

```bash
python3 generate_summary.py
```

此腳本依賴 `fetch_sheet.py`，因此同樣需要網路。

## build_leaderboards.py

`build_leaderboards.py` 整合下載與分析流程，先將原始 CSV 存到 `data/history.csv`，再於 `data/leaderboard.txt` 產生排行榜。內容包含每條賽道最快車手、各車種最佳紀錄、車手生涯最佳等，若標記為錦標賽則另行列出。

```bash
python3 build_leaderboards.py
```

## prefab/TextDisplay.cs

`prefab/TextDisplay.cs` 是 Unity 組件，可從指定 URL 下載文字檔並顯示到 `Text` UI 元件。將其掛載於 Prefab 並在 Inspector 設定 `url` 與 `targetText`。

## build_site.py

`build_site.py` 讀取排行榜文字後建立靜態的 `site/index.html`，可透過 GitHub Pages 部署讓玩家瀏覽最新成績。

```bash
python3 build_site.py
```

提交 `site` 內的檔案後即可在 GitHub Pages 發佈。

## ui.py

`ui.py` 提供簡易的圖形介面，讓你輸入 Google 試算表連結或本地檔案路徑並瀏覽各式排行榜。介面含有多個分頁，可手動輸入成績、查看依賽道或車輛的最速紀錄、統計車手生涯最佳，以及簡易的錦標賽規劃工具。
