# VRChat 工具集

VRChat 工具集是一個互動式工具組，包含賽道成績 (Track Results) 與世界資訊 (World Info) 兩部分。它能從 Google 試算表與 VRChat API 取得資料，產生排行榜、歷史紀錄並匯出 JSON 或靜態網站。介面採用 Tkinter，方便在桌面環境操作審核流程。

本專案由星河 StarRiver 與 Codex AI 協作開發。

## 功能簡介
- **Track Results**：下載賽車紀錄並建立文字或 HTML 排行榜，並可產生包含總筆數、各賽道最快與車手最佳時間的統計摘要。詳見 [`track_results/README.md`](track_results/README.md)。
- **World Info**：收集世界資訊、維護歷史資料並提供圖形審核介面；執行爬蟲時會顯示各模式進度並於結束後輸出抓取的世界總數。詳見 [`world_info/README.md`](world_info/README.md)。

## 資料來源
- Google 試算表
- VRChat 官方 API

## CREDIT
星河 StarRiver、Codex AI

## 版本歷史
1.3.2: 2025-08-06  
- 同步最新世界與賽道資料

1.3.1: 2025-08-05  
- 更新資料檔  
- 文件加入近期 UI 改進說明

1.3.0: 2025-08-05  
- 新增長內容捲軸  
- 儀表板圖表支援視窗大小調整  
- 啟動錯誤顯示 traceback 以利除錯

## 快速開始
安裝相依套件：
```bash
pip install -r requirements.txt
```
若需爬取作者世界資料，請於安裝後執行：
```bash
playwright install
```

更多繁體中文說明請參見 [`README.zh_TW.md`](README.zh_TW.md)。
