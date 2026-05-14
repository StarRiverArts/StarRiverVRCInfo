# Unity 顯示端接收需求草案（暫不實作）

> 狀態：草案  
> 備註：**不立刻實行，仍需進一步討論**。本文只針對遊戲內顯示端、VRChat 取用方式與資料接收策略整理，不代表已定案。

## 目的

規劃一套 Unity 內的接收顯示端，用來讀取計時賽網站輸出的資料，並以 prefab / 看板方式在遊戲內展示。

這份文件只關心：

- Unity 端要怎麼選資料
- Unity 端要怎麼顯示資料
- Unity 端需要哪些輸出格式
- Unity 端如何處理即時性、快取與 VRChat 限制

## 核心原則

### 1. 選取單位是 `track_variant`

- Unity 端的主要選取單位應是 `track_variant`
- 不應直接以 `track_family` 作為主要選取單位
- 也不應以單一路線 `track_route` 作為主要選取入口

原因：

- family 會混入其他作者 / 其他版本
- route 太細，會失去同 variant 下其他路線的上下文
- 實務上通常要一次顯示某個 variant 下所有 routes

### 2. 顯示展開單位是 `route`

當 Unity 選到某個 variant 時，應自動展開該 variant 底下所有 routes：

- 有紀錄的 route
- 無紀錄的 route

都應在同一個 UI 卡內顯示。

### 3. 大總表可相容

即使是大總表模式，也可以先拿 `variant list`：

- 再依 family 排序或群組
- 再把每個 variant 展開成 routes

## 主要使用情境

### 1. 世界內即時排行榜 UI

用途：

- 放在特定賽道世界內
- 直接告訴玩家是否破紀錄
- 固定綁 `track_variant_id`

需求：

- 顯示簡版主榜
- 可有詳版變體
- 只用寫好的 variant ID

### 2. 搜尋紀錄 UI

這應該是一個統一框架，不建議拆成很多分散 UI。

同一個框架內支援：

- 個人戰績查詢
- 車輛榜查詢
- 賽道紀錄查詢

用途：

- 給玩家查自己紀錄
- 給玩家秀資料
- 給聚會場所使用

### 3. 分析表

用途：

- 給車隊 / 賽車俱樂部分析用

可能的分析內容：

- 賽道破紀錄頻率
- 某 route 活躍度
- 某 variant 更新頻率
- 玩家常用車輛
- 玩家擅長賽道類型
- sector 差距分析

### 4. 總表 / Overview

用途：

- 一次看到大量甚至全部賽道的最快幾筆紀錄

特性：

- 側重掃描
- 不做太深的閱讀
- 以 variant 為卡片，再展開 routes

### 5. 新紀錄公告板

用途：

- 顯示最近幾筆新紀錄
- 刺激玩家跟跑

### 6. 俱樂部 / 車隊內榜

用途：

- 只看某群組的成績
- 連動玩家屬性資料

### 7. 活動 / 賽季榜

保留為後續方向，前提是網站端先具備活動 / 賽季資料層。

### 8. 場館輪播

保留。

之後可考慮輪播：

- 最新紀錄
- 總表摘要
- 單一 variant 榜
- 活動榜

## 顯示層級

### `variant card`

每個 `track_variant` 對應一個主要 UI 框。

卡片中應包含：

- variant 名稱
- family / 分組資訊
- 多個 route block

### `route block`

每個 route block 只顯示最基本資訊：

- 時間
- 車輛
- 玩家
- 平台

並且：

- 必須顯示無紀錄 route 的空白佔位
- route 多寡會影響 variant card 的尺寸

### 詳版變體

單一賽道詳細榜可視為即時排行榜 UI 的詳版變體，不另拆成完全獨立的大類。

## 紀錄標示規則

以 route 為單位標示：

- `TR`
- `CR`
- `PR`

優先級：

- `TR > CR > PR`

規則：

- 若已達成 `TR`，只顯示 `TR`
- 不同時再疊出 `CR`、`PR`

## `sector time` 支援

部分賽道有 `sector time`，Unity 端應預留支援，但不強制所有面板都顯示。

建議：

- 即時榜與總表預設不展開 sector
- 詳版榜 / 搜尋結果 / 分析表可選擇顯示 sector
- 若 route 無 sector 定義，就自動退化為單圈時間顯示

## VRChat 讀取模式

### 保守模式

優先使用：

- GitHub Pages 上的靜態 JSON / TXT / PNG
- allowlisted 可讀網址
- revision 檢查後再更新

這是最穩、最容易推廣給其他創作者的模式。

### 進階模式

可選擇支援：

- 不受信任網址
- 少量進階查詢
- selection / 特殊榜單輸出

但這不應是核心依賴，因為：

- 並非每個玩家都開 `Allow Untrusted URLs`
- 作者採用門檻較高
- 外部依賴與風險增加

## 即時性與快取策略

世界內 UI 的核心問題不是能不能顯示，而是：

- 需要有更新時能快點反應
- 也需要在讀取失敗時穩定退回舊資料

### 原則：更新檢查優先，完整抓取次之

#### 輕量檢查

先檢查：

- `updated_at`
- `revision`
- `latest_record_id`
- `etag / hash`

若沒有更新：

- 直接使用本地快取

若有更新：

- 再抓完整資料包

#### 本地快取

Unity 端應保留最近一次成功資料，用於：

- 網路失敗 fallback
- 降低頻繁抓取
- 進場後快速顯示

#### 顯示狀態

可考慮顯示：

- `Live`
- `Cached`
- `Last updated xx min ago`

## Unity / VRChat 效能限制

這不是單純優化問題，而是設計前提。

### 世界內不要當資料分析器

重計算留在網站端 / builder 端做。

Unity 端應盡量只拿：

- 已整理好的少量資料
- 已排序好的結果
- 已算好的 badge / gap / rank

### 不要全抓

Unity 端應只抓：

- 指定 variant
- 指定 selection
- 指定榜單模式
- 指定最新 N 筆

不應一開就抓：

- 全部賽道
- 全部玩家
- 全部歷史紀錄

### 每個面板都要有資料量上限

例如：

- 每個 variant 最多顯示前 3 或前 5
- 公告板最多顯示 10 到 20 筆
- 總表一次最多顯示 N 個 variant

### 多個 prefab 不應重複抓同一份資料

建議世界內有共用 data manager 或至少共用快取，避免：

- 各面板各自高頻輪詢
- 重複 parse 相同 JSON
- 重複下載相同圖片

## Unity 端輸入方式

建議支援：

### 1. `feed_url`

- 最簡單
- 適合固定面板

### 2. `track_variant_ids`

- 由 Unity 端決定要顯示哪些 variant
- 系統再回傳完整資料包

### 3. `selection_id`

- 外部先建立一組 selection
- Unity 端只負責讀取

## Prefab 需要能設定的參數

- 顯示模式
- `feed_url`
- `track_variant_ids`
- `selection_id`
- 每頁最多顯示幾個 variant
- 是否輪播
- 刷新間隔
- 是否啟用快取
- 排序方式
- 是否顯示空白 route
- 是否顯示 family 標頭
- 是否顯示 sector

## 輸出端格式需求

### 1. `dashboard feed`

用途：

- Unity curated dashboard
- 總表看板
- 最新紀錄摘要

需求：

- 支援 variant subset
- 回傳 route 摘要
- 回傳最新幾筆紀錄

### 2. `track board feed`

用途：

- 單一 variant 完整榜

### 3. `player / vehicle feed`

用途：

- 搜尋紀錄 UI
- 主題榜

### 4. `selection feed`

用途：

- 讀取一組預先選好的 variants

### 5. `lightweight update feed`

用途：

- 只檢查是否有更新
- 不直接下載整包榜單

### 6. `baked image output`

用途：

- 總表圖片
- 公告板圖片
- 單一 variant 榜單圖片

適合：

- 純展示用面板
- 不需要互動的場景
- 想降低世界內排版負擔的情況

## GitHub Pages 與 VRChat 專用輸出

### 推薦模式

網站端應將 VRChat 專用輸出發到 GitHub Pages。

原因：

- 適合靜態託管
- 可和主網站共存
- 適合 revision 與 artifact 快取策略
- 也較符合 VRChat 對靜態外部內容的使用方式

### 不建議的核心依賴

不建議把世界內核心功能完全建立在：

- 自訂動態 API
- 高頻即時查詢
- 不受信任網址

## 兩種 Dashboard 模式

### `Curated Dashboard Mode`

- 由 Unity 指定 `track_variant_ids`
- 只抓這些 variant 與其 routes

### `Selection Analysis Mode`

- 先從總表挑出幾個 variant
- 再做討論、分析與展示

## 目前不建議立刻做的事

- 一開始就做很重的互動式 UI
- 在輸出格式未定前就寫死 Prefab 邏輯
- 一開始就把所有圖表都塞進世界內 UI
- 沒有快取策略就直接追求即時刷新

## 待討論問題

- Curated mode 的 variant 清單要放在 Unity 還是後台？
- 搜尋紀錄 UI 要即時查 Web，還是預抓常用資料？
- 快取粒度要到 variant、route，還是整個 feed？
- sector 資料要在 Unity 端計算顯示，還是直接由 API 提供？
- 活動 / 賽季榜要不要在 Unity 內常駐，還是只作特殊場景使用？
