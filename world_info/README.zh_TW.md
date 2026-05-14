# VRChat World Info

> 維護模式說明：`world_info/` 現在視為 legacy / 維護用工具鏈。  
> 新功能開發原則上應移到 `world_info_web/`，除非有明確的舊版相容需求。

這個工具用來蒐集 VRChat 世界資料、檢視結果，並輸出可供網站或 Unity 使用的 JSON。

## 安裝

```bash
pip install -r requirements.txt
```

如需驗證，請在 `world_info/scraper/headers.json` 放入本機登入後的 Cookie：

```json
{"Cookie": "auth=...; twoFactorAuth=...; machineId=..."}
```

## 使用方式

1. 依關鍵字抓世界：

```bash
python3 world_info/scraper/scraper.py --keyword Taiwan --limit 50
```

2. 依 creator user ID 抓世界：

```bash
python3 world_info/scraper/scraper.py --user usr_abc123 --limit 50
```

現在作者世界抓取會直接走 worlds API 的 `userId` 篩選，不再用 Playwright 去爬 `vrchat.com` 的使用者頁面。

3. 如需桌面介面：

```bash
python3 world_info/ui.py
```

4. 匯出網站／Unity 用 JSON：

```bash
python3 world_info/scraper/exporter.py
```

## 目前建議

- 抓取層盡量只走 API，不再依賴 HTML scraping。
- 驗證資訊保留在本機，不要透過網頁介面輸入。
- Excel 比較適合當人工檢查輸出，不適合當系統主資料來源。
