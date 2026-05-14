# Time Attack Tool 開發紀錄

## 2026-05-13

### 本次建立

- 新增 `time_attack_tool/`，作為新版計時賽工具工作區
- 定義 `NormalizedRecord` 與時間格式解析工具
- 實作 CSV / XLSX 匯入 `approved_records` 的標準化流程
- 實作 `submissions` 審核摘要輸出
- 實作以 `track_variant` / `track_route` 為核心的 leaderboard builder
- 實作 `TR` / `CR` / `PR` 自動標記規則
- 實作 CLI：`build_artifacts.py`
- 新增範例資料與測試

### 本次追加

- 補上公開 Google Sheet 網址讀取能力
- 支援一般分享網址自動轉換為 CSV 匯出網址
- 支援用 `sheet name` 或 `gid` 指定工作表
- 維持原本本機 `CSV / XLSX` 輸入模式不變

### 目前定位

這一版先處理「正式資料進 builder 後的整理與輸出」，不碰：

- Google Apps Script 核准流程
- 自動圖片
- GitHub Pages 新 UI
- VRChat 顯示端

### 後續建議順序

1. 補一份正式 `approved_records` / `submissions` 欄位規格文件
2. 補 Google Sheet 匯出樣本，驗證真實資料欄位差異
3. 增加 `track_family` / `track_variant` / `track_route` 草稿轉正流程
4. 加入 baked image builder
5. 再接新網站與 VRChat feed
