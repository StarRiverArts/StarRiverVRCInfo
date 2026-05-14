# Time Attack Tool v2

這個目錄是新一代計時賽工具的起點，定位為「正式資料層 + 靜態輸出 builder」，不取代舊版 `track_results/` 的即時使用流程。

目前支援的輸入來源有兩種：

- 本機 `CSV / XLSX`
- 公開可讀的 Google Sheet 網址或 CSV 匯出網址

## 目前完成範圍

- 定義新版正式紀錄的標準欄位
- 支援從 `approved_records` CSV / XLSX 匯入正式紀錄
- 支援從 `submissions` CSV / XLSX 產生審核摘要
- 以 `track_variant` / `track_route` 為核心生成 leaderboard JSON
- 依 route 規則自動標示 `TR` / `CR` / `PR`
- 產出可供網站、圖片 builder、VRChat 顯示端共用的靜態 JSON

## 為什麼先做這層

上一版 `track_results/` 直接抓 Google Sheet 並生成文字榜單，能快速使用，但缺少：

- 投稿與審核分層
- 正式紀錄與草稿資料分層
- 給網站 / 圖片 / VRChat 共用的穩定輸出格式
- `track_variant` / `track_route` 層級

這一版先補 builder，後續才能安全接：

- Google Form / Google Apps Script
- GitHub Pages 新網站
- baked image 輸出
- Unity / VRChat 專用 feed

## 輸入資料契約

### `approved_records`

至少需要以下欄位：

- `record_id`
- `racer_id`
- `racer_display_name`
- `vehicle_id`
- `vehicle_display_name`
- `track_variant_id` 或 `track_id`
- `track_variant_name` 或 `track_display_name`
- `lap_time_ms` 或 `lap_time_text`

可選欄位：

- `track_family_id`
- `track_family_name`
- `track_route_id`
- `track_route_name`
- `submission_id`
- `source_type`
- `review_status`
- `record_tier`
- `platform`
- `fps_value`
- `recorded_at`
- `approved_at`
- `event_id`
- `notes`

如果沒有提供 `track_route_id` / `track_route_name`，builder 會自動建立：

- `track_route_id = {track_variant_id}:default`
- `track_route_name = Default`

### `submissions`

目前只用來生成審核摘要，至少建議有：

- `submission_id`
- `review_status`
- `racer_name_input`
- `track_input`
- `vehicle_input`
- `lap_time_text`

## 輸出檔案

執行後會產生：

- `records.json`
- `leaderboards.json`
- `manifest.json`
- `review_summary.json`（若有提供 submissions）

## CLI

```bash
python time_attack_tool/build_artifacts.py \
  --approved-records time_attack_tool/examples/approved_records.sample.csv \
  --submissions time_attack_tool/examples/submissions.sample.csv \
  --output-dir time_attack_tool/out
```

### 直接吃 Google Sheet

如果你提供的是一般 Google Sheet 分享網址，builder 會自動轉成 CSV 匯出網址。

```bash
python time_attack_tool/build_artifacts.py \
  --approved-records "https://docs.google.com/spreadsheets/d/your-sheet-id/edit#gid=123456789" \
  --approved-records-gid 123456789 \
  --submissions "https://docs.google.com/spreadsheets/d/your-sheet-id/edit#gid=987654321" \
  --submissions-gid 987654321 \
  --output-dir time_attack_tool/out
```

如果你知道工作表名稱，也可以直接指定：

```bash
python time_attack_tool/build_artifacts.py \
  --approved-records "https://docs.google.com/spreadsheets/d/your-sheet-id/edit" \
  --approved-records-sheet approved_records \
  --submissions "https://docs.google.com/spreadsheets/d/your-sheet-id/edit" \
  --submissions-sheet submissions \
  --output-dir time_attack_tool/out
```

注意：

- 這一版預設讀的是「公開可讀」的 Google Sheet
- 若表單整合表不是公開的，之後需要再補 Google API / Apps Script 取數
- 你現在規劃的流程很適合先用「整合用 Google Sheet -> 現有正式 Google Sheet -> 本機 builder」這種半自動方式

## 第一階段刻意不做的事

- 直接連 Google Form / Google API
- 自動寫回 `approved_records`
- baked image 產生
- 前端網站頁面
- Unity / VRChat 接收端
- 活動 / 賽季榜

這些都依賴先有穩定正式輸出。
