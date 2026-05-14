# world_info_web 架構草案

更新日期：2026-05-05

本文件用來固定目前對 `world_info_web` 的產品定位、資訊架構、資料架構、流程邊界與未來拆分方向，避免後續討論只存在對話記憶中。

## 1. 產品定位

`world_info_web` 不再只是一個世界資料抓取與展示工具，而是朝以下方向演進：

- 主動式 VRChat 社群情報協作台
- 一站式 VRChat 社群動態監視器 / 後台
- 以「先整理、先篩選、先提醒，再交給人判斷」為核心

這和 VRCX 類型的工具不同：

- VRCX 偏向使用者主動打開工具、手動查詢資訊的 companion app
- `world_info_web` 偏向主動產出 briefing、watchlists、alerts、review queue 的 intelligence console

## 2. 核心需求

系統應能主動回答以下問題：

- 今天有哪些新世界值得看？
- 哪些世界正在成長，但還沒被官方搜尋系統推到頂端？
- 哪些創作者最近值得追？
- 哪些群組正在成長？
- 哪些更新真的有帶來流量？
- 哪些異常值得人工 review？
- 哪些群組需要更新資料、安排貼文、追蹤後續？

## 3. 產品邊界

本專案長期應覆蓋四種能力：

1. 情報監看
   - worlds
   - creators
   - groups
   - events
   - anomalies
2. 探索分析
   - new worlds
   - potential worlds
   - regional discovery
   - compare / history / graph
3. 協作治理
   - review
   - whitelist / blacklist
   - view management
   - diagnostics
4. 營運發布
   - managed groups
   - group profile maintenance
   - scheduled posts
   - future bot / webhook delivery

## 4. 主分頁與子分頁

建議的頂層資訊架構如下：

### Dashboard

用途：30 秒內了解今天的重點。

子分頁：

- Briefing
- Highlights
- Health

只顯示精選結果，不顯示完整 worlds table。

### Monitor

用途：持續監看現在要盯的訊號。

子分頁：

- Worlds
- Creators
- Groups
- Events

### Discover

用途：往下深挖與探索。

子分頁：

- New
- Potential
- Regional
- Search
- Compare
- History

### Communities

用途：群組相關監看與管理。

子分頁：

- Group Directory
- Group Growth
- Group Worlds
- Publishing

### Review

用途：人工判斷、審核與標記。

子分頁：

- Creator Review
- World Review
- Flags

### Operations

用途：後台工作流與系統維護。

子分頁：

- Sync
- Views
- Scheduler
- Settings
- Debug

`Settings` 不應獨立為頂層頁，而應納入 `Operations`。

## 5. 領域模型

目前最重要的概念拆分如下：

### Source

代表資料從哪裡來，和爬取排程有關。

例：

- `db:job:taiwan`
- `db:job:JapanTop`
- `keyword:Taiwan`
- `user:usr_xxx`
- future: `group:grp_xxx`

### View

代表怎麼看既有資料，是分析視角，不應直接等同爬取任務。

例：

- 台灣新世界
- 中文圈潛力股
- Zh/JP Emerging
- Worth Watching

### Watchlist

代表持續監看的精選榜單或提醒集合。

例：

- New Worlds
- Rising Now
- Dormant Revival
- Group Growth

### Workspace

代表實際要管理的單位，會和 review、scheduler、publishing 相連。

例：

- 某個群組
- 某個創作者清單
- 某個社群營運範圍

## 6. Source / View / Watchlist / Workspace 流程原則

### 新增 source

只有在以下情況才需要新增 source：

- 現有資料覆蓋不到
- 需要新的 keyword / creator / group 來源
- 需要不同的 endpoint 或不同頻率

新增 source 代表：

- 新 sync job
- 新爬取排程
- 新資料保留策略

### 新增 view

新增 view 不應新增 crawler。

新增 view 代表：

- 在既有 corpus 上定義新的篩選規則
- 物化新的 leaderboard 或 query cache
- 提供新的 discover / dashboard 視角

### 新增 watchlist

新增 watchlist 不應新增 crawler。

新增 watchlist 代表：

- 在現有分析結果中挑出新的 briefing / monitor 區塊

### 新增 workspace

新增 workspace 代表：

- 新的管理單位
- 可掛接 source / view / watchlist / publishing 設定

### 停用與刪除

應明確區分：

- disable view：停用視角，但保留歷史
- archive source：停止排程，但保留資料
- purge source：明確要求才刪除資料

## 7. 對目前資料庫的評估

目前 SQLite schema 對第一階段「抓取 + 展示 + 初步分析」是夠的，但對第二階段「公開服務化 + 群組管理 + 發文排程 + 未來拆分」還不夠。

### 目前已具備的優點

- `sync_runs` 保存抓取 provenance
- `world_snapshots` 保存歷史快照
- `run_queries` / `run_query_hits` 可回溯搜尋來源
- `topics` / `topic_rules` / `topic_memberships` 已有 reusable view 雛形
- `analysis_cache` 已開始承擔精選分析快取

### 目前的結構限制

- 沒有 `worlds` 主表
- 沒有 `creators` 實體表
- 沒有 `groups` 相關資料表
- `analysis_cache` 目前是 JSON blob，不利於查詢與公開 API 穩定化
- 沒有物化 `events` 表
- 沒有 `publishing / scheduler` 相關資料表

## 8. 目標資料架構

建議從 snapshot-only 架構，升級成「核心表 + 快照表 + 物化分析表 + 協作表」。

### A. 核心實體表

- `worlds`
- `creators`
- `groups`
- `sources`
- `jobs`
- `views`
- `watchlists`
- `workspaces`

用途：

- 存放最新狀態
- 給 API 與 UI 穩定讀取
- 降低前端依賴 `world_snapshots + raw_json`

### B. 歷史快照表

- `world_snapshots`
- `creator_snapshots`
- `group_snapshots`
- `sync_runs`
- `run_queries`
- `run_query_hits`
- `rate_limit_events`

用途：

- 趨勢分析
- 事件回溯
- 重算分析

### C. 物化分析表

建議新增：

- `analysis_runs`
- `analysis_leaderboards`
- `analysis_leaderboard_items`
- `event_feed_items`

用途：

- Dashboard / Monitor 不掃全量資料
- 用於 briefing、watchlists、alerts
- 讓公開 API 可穩定分頁與排序

### D. 協作與發布表

建議新增：

- `managed_groups`
- `group_profiles`
- `content_plans`
- `scheduled_posts`
- `post_deliveries`
- `review_flags`
- `review_decisions`

用途：

- 管理中的群組與資訊
- 群組貼文排程
- 人工審核與處理紀錄

## 9. 目前應保留的現有表

以下現有表應保留並繼續使用：

- `sync_runs`
- `world_snapshots`
- `daily_stats`
- `run_queries`
- `run_query_hits`
- `rate_limit_events`
- `topics`
- `topic_rules`
- `topic_memberships`

`analysis_cache` 短期內也應保留，作為從現況平滑過渡到物化分析表的中介層。

## 10. analysis_cache 的定位

`analysis_cache` 是正確方向，但不應是最終型態。

短期角色：

- 首頁與 monitor 的輕量資料來源
- 降低每次頁面載入時的全庫掃描成本

中期演進：

- 保留 `analysis_cache` 作為 summary cache
- 新增 `analysis_leaderboards` / `analysis_leaderboard_items`

長期角色：

- cache 只放 brief summary
- leaderboard item 級資料改由可查詢資料表提供

## 11. 首頁與 Monitor 的資料策略

### Dashboard

只讀：

- briefing snapshot
- selected leaderboards
- health snapshot

不讀：

- 全量 worlds table

### Monitor

只讀：

- event feed
- anomalies
- update effectiveness
- creator / group momentum

### Discover

才讀完整 worlds collection。

## 12. 群組能力預留

未來至少要區分兩種 group：

### Observed group

只是監看對象，不一定是自己管理。

### Managed group

自己會維護、更新、排程貼文、追蹤結果的對象。

因此應分成：

- `groups`
- `managed_groups`

不要只用一個欄位混在同一張表裡。

## 13. 公開發布與未來拆分策略

目前不一定要拆 repo，但應先拆 bounded contexts。

建議的邏輯子系統如下：

### Ingestion

- sync jobs
- source crawling
- rate limit handling
- raw snapshots

### Intelligence

- world / creator / group analysis
- leaderboards
- anomaly detection
- event generation
- briefing generation

### Console

- dashboard
- monitor
- discover
- review
- operations

### Publishing

- managed group profiles
- scheduled posts
- delivery logs

未來公開化時，可考慮：

- 對外公開 `Console + Intelligence API`
- `Ingestion` 保持內部
- `Publishing` 視需求另拆服務

## 14. 專案定位的階段演進

### Phase 1

VRChat World / Creator Intelligence Console

### Phase 2

VRChat Community Intelligence Console

### Phase 3

VRChat Community Operations Console

這個順序能避免產品一開始承諾過大，也能保留從個人工具演進為公開服務的可能。

## 15. 接下來的實作優先順序

### 第一階段

- 將 Dashboard / Monitor 完全從全量 worlds 拆開
- 首頁只讀 analysis cache / briefing 資料
- 清理前端責任邊界

### 第二階段

- 新增 `worlds`
- 新增 `creators`
- 建立 current-state 層

### 第三階段

- 新增 `analysis_runs`
- 新增 `analysis_leaderboards`
- 新增 `analysis_leaderboard_items`
- 將首頁與 monitor 改讀物化分析表

### 第四階段

- 新增 `groups`
- 新增 `group_world_memberships`
- 新增 `group_snapshots`
- 新增 group growth watchlists

### 第五階段

- 新增 `managed_groups`
- 新增 `scheduled_posts`
- 新增 `post_deliveries`

## 16. 現階段的核心結論

- `world_info_web` 的定位應升級為主動式情報協作台，而不是單純資料展示頁
- `topic` 概念需要拆成 `source / view / watchlist / workspace`
- `analysis_cache` 是正確方向，但需要演進成物化分析表
- 目前資料庫可以繼續沿用，但必須逐步補齊 `world current / creator / group / publishing` 幾層
- 現在應優先做的是資料與頁面責任切分，而不是再堆更多混合功能

## 17. 啟動與驗證規則

為了避免再次卡在同一個階段，`world_info_web` 的啟動驗證流程固定如下：

- 不要把 `python -m world_info_web.backend.app` 當成一次性測試命令直接前景執行。
- 這個命令是長駐 server，沒有自然結束點；直接執行只會停在「服務持續運行」。
- 驗證必須使用「背景啟動 + health check + 明確收尾」。

標準流程：

1. 使用 `python -m world_info_web.launcher` 或等效背景啟動流程。
2. 輪詢 `GET /api/v1/health`。
3. 在限定時間內判定：
   - `200 OK` 代表啟動成功
   - 啟動逾時則視為失敗，必須抓 log / stderr
4. 驗證結束後，要明確說明：
   - 服務已在執行
   - 或啟動失敗原因
   - 或既有服務無法停止但仍可用

補充規則：

- `start_world_info_web.bat` / `world_info_web.launcher` 應被視為「可終止的驗證入口」。
- 若要檢查頁面是否可用，至少要驗證：
  - `/`
  - `/app.js`
  - `/styles.css`
  - `/api/v1/health`
  - 首頁 boot 依賴的主要 API，例如 `/api/v1/sources`、`/api/v1/insights`
- 若啟動卡住，先區分是：
  - server 沒起來
  - server 已起來但 boot API 卡住
  - launcher 的 restart / stop 流程失敗

## 18. 實作狀態盤點

### 已正式實作

- `Dashboard / Monitor / Discover / Operations / Review / Debug / Graph` 主頁已存在。
- `Dashboard` 已改成 briefing-first，不再依賴首頁先載完整 worlds collection。
- `Dashboard` 已拆成 `Briefing / Highlights / Health` 子分頁架構。
- `Monitor` 已拆成 `Events / Worlds / Creators / Groups / Health` 子分頁架構。
- `Discover` 已拆成 `New / Potential / Regional / Search / Compare / History / Signals` 子分頁架構。
- `Operations` 已拆成 `Sync / Views / Scheduler / Records / Diagnostics / Settings` 子分頁架構。
- `Communities` 主頁已存在，並有 `Directory / Growth / Group Worlds / Publishing` 子分頁架構。
- `/api/v1/insights` 已成為 dashboard / monitor 的主要摘要來源。
- `/api/v1/communities/summary` 已存在，前端 communities 頁已接上。
- `analysis_cache` 已存在，首頁與 monitor 類摘要可重用快取。
- `groups / group_world_memberships / managed_groups / scheduled_posts` 已進入 SQLite schema。
- `groups / managed_groups / scheduled_posts` 已有正式 CRUD service、API 與前端表單/列表操作。
- `group_world_memberships` 已有正式 CRUD service、API 與前端表單/列表操作。

### 半實作

- `Communities` 已有真實 API 與前端渲染，`Directory` 與 `Publishing` 已可做基本 CRUD，但整體仍不是完整 group workflow。
- `Communities > Group Worlds` 已可維護 memberships，並顯示 group-linked worlds 與 per-group slices。
- `topic` 在 UI 命名上已逐步收斂成 `view`，但底層資料模型仍沿用 `topics / topic_rules / topic_memberships`。
- `Discover` 子分頁已經切開，並會套用不同排序偏好，但資料仍多數來自既有 world insights，而非獨立資料產品。
- `Monitor > Groups` 已開始吃 communities summary / managed group / publishing proxy，但仍是過渡區，等待 group snapshots 與 group-local analysis。
- `creators` 表已進 schema，但 creator current-state 還沒成為主驅動資料層。

### 目前仍是骨架 / 佔位

- `Communities > Growth` 還沒有真實的 group growth time series。
- `Communities > Publishing` 已有基本 post queue editor，但還沒有 delivery log workflow。
- `Monitor > Groups` 還沒有 group watchlists、group breakout、group health 真資料。
- `managed_groups` / `scheduled_posts` 已可 CRUD，但還沒有完整 approval / delivery / retry lifecycle。
- `Publishing` bounded context 尚未成形，還沒有獨立的 posting lifecycle。

### 尚未開始或未正式切換

- `source / view / watchlist / workspace` 四分模型還沒有正式取代現有 `topic` 主模型。
- `analysis_runs / analysis_leaderboards / analysis_leaderboard_items` 尚未落地。
- `world current / creator current / group current` 這批 current-state 表尚未落地。
- `group_snapshots` 與 group growth 分析尚未落地。
- `post_deliveries`、delivery retry、publishing history 尚未落地。
- SQLite 在部分臨時測試目錄仍可能觸發 `disk I/O error`；目前以真庫唯一 ID smoke test 驗證 Communities CRUD，後續需要再收斂測試 DB 初始化策略。

## 19. 大量實作順序

### Wave 1：資料與 API 正規化

- 完成 `groups` CRUD API
- 完成 `managed_groups` CRUD API
- 完成 `scheduled_posts` CRUD API
- 完成 `group_world_memberships` 寫入與讀取 API
- 把 communities summary 拆成可單獨使用的細分 API

### Wave 2：分析層正式化

- 導入 `analysis_runs`
- 導入 `analysis_leaderboards`
- 導入 `analysis_leaderboard_items`
- 把 dashboard / monitor / communities 的摘要逐步轉為物化分析表驅動

### Wave 3：模型切分

- 將 `topic` 概念逐步拆成 `source / view / watchlist / workspace`
- `View Manager` 對應正式 `views`
- `Monitor` 對應正式 `watchlists`
- `Communities` 對應正式 `workspaces / managed_groups`

### Wave 4：群組情報與營運

- group snapshots
- group growth watchlists
- managed groups
- publishing queue
- delivery logs

### Wave 5：整體收尾

- 把頁面命名、API 命名、資料模型命名從 `topic` 全面收斂到新詞彙
- 對 communities / publishing 補完整驗證流程
- 對 dashboard / monitor / discover / operations 補更穩定的 smoke checks
