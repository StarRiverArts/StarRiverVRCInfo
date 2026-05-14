# 計時賽 Web 系統草案（暫不實作）

> 狀態：草案  
> 備註：**不立刻實行，仍需進一步討論**。本文僅整理需求、資料模型、網站角色與輸出策略，不代表已定案。

## 目的

建立一套可部署在 GitHub Pages 上的計時賽網站，用來管理：

- 賽道資料
- 路線資料
- 玩家資料
- 車輛資料
- 計時紀錄
- 分析與展示輸出

這套系統的定位不是單純排行榜，而是可持續維護的賽道與紀錄資料平台，後續可同時服務：

- Web 大總表
- Web 查詢與分析頁
- Unity / VRChat 內的看板與 prefab
- 車隊 / 俱樂部內部查詢

## 核心原則

### 1. 輸入要快

- 使用者應能在輸入紀錄的同一個地方補入新的賽道資訊
- 找不到對應賽道時，不要中斷流程
- 允許先建立 draft，後續再補完與整理

### 2. 正式主檔不能被 typo 汙染

- typo 不應直接變成正式賽道
- 需要 draft、別名、相似名稱比對與合併流程

### 3. 顯示單位與計時單位要分清楚

- 同一概念賽道可能有不同作者 / 版本 / 變體
- 實務上應分開計時、分開排行
- 但瀏覽時仍希望放在鄰近位置

### 4. 網站與 VRChat 顯示共用同一套輸出邏輯

- 不做兩套互相分離的資料來源
- 後端 / builder 算好資料，再輸出成靜態產物

### 5. 進階資料允許擴充，但不強制

系統先支援核心計時資料，再預留進階欄位，例如：

- 差距秒數
- 基準時間
- 分段時間 `sector_times`
- 分梯 / 分群資訊

這些作為 optional property，不要求所有紀錄都必填。

## 資料模型

### `track_family`

代表一組相近的賽道概念，用於：

- 瀏覽分群
- 相鄰排序
- 視覺整理

例如：

- 同一條路線概念
- 同系列地圖
- 同主題下的多作者版本

### `track_variant`

代表真正拿來被選取、顯示、排行與輸出的賽道變體。

這是最重要的單位，因為：

- 不同作者 / 不同版本通常要分開當賽道處理
- Unity 端選取單位應是 `track_variant`
- Web 大總表也可先以 `variant list` 作為基底

### `track_route`

代表 variant 底下的路線。

例如：

- 上山
- 下山
- 單圈
- 逆走

正式紀錄應綁在 `track_route`，因為：

- `TR / CR / PR` 需以路線為單位計算
- 同一個 variant 下不同 route 不能直接混榜

### 其他核心實體

- `players`
- `vehicles`
- `records`
- `track_aliases`
- `track_variant_drafts`
- `track_route_drafts`
- `record_reviews`

### 活動與賽季資料層

這層要額外獨立出來，不應假設會自然長在一般計時資料裡。

建議預留：

- `events`
- `seasons`
- `event_entries`
- `season_results`

用途：

- 活動榜
- 賽季榜
- 歷屆賽事成績

## 為什麼 Unity 選取單位是 `track_variant`

這條先定義清楚：

- Unity 不應以 `track_family` 作為主要選取單位
- Unity 也不應以單一路線 `track_route` 作為主要選取入口

原因：

- 用 family 選取時，容易混入不需要的其他作者或其他版本
- 用 route 選取時，又會失去同一 variant 下其他路線的上下文

建議規則：

- `Unity curated dashboard selection unit = track_variant`
- `Unity display expansion unit = all routes under selected variant`
- `track_family` 僅作為分群、排序、鄰近顯示用 metadata

## 賽道新增與去重

### 目標

使用感應該像這樣：

- 在輸入紀錄的同一個地方輸入新的賽道資訊
- 若系統找不到對應 variant，先建立 draft
- 後續再補正式資訊

### 建議流程

1. 使用者輸入紀錄
2. 輸入 variant 名稱與必要資訊
3. 系統即時搜尋既有 variant
4. 若有高度相似項目，優先提示選用
5. 若沒有，允許建立 `track_variant draft`
6. route 若不存在，也允許建立 `track_route draft`
7. 紀錄先綁到 draft 或暫存映射
8. 後續再補：
   - family
   - variant 正式名稱
   - route 集合
   - 作者資訊
   - 備註 / 別名 / 狀態

### 避免 typo 的方法

- 完全同名攔截
- 標準化比對
  - 忽略大小寫
  - 忽略空白
  - 忽略部分符號
- 相似名稱提示
- 別名表
- draft 審核列表
- draft 合併到正式 variant

## 紀錄模型

### 每筆紀錄至少綁定

- `track_route_id`
- `track_variant_id`
- `player_id`
- `vehicle_id`
- `lap_time`
- `platform`
- `recorded_at`

### 建議擴充欄位

- `gap_to_tr`
- `gap_to_cr`
- `gap_to_pr`
- `sector_times`
- `baseline_tier`
- `notes`

### `sector_times`

部分賽道有 sector time，這點不能忽略。

建議原則：

- `sector_times` 為 optional
- 不強制每條 route 都有 sector
- 不強制每筆紀錄都填 sector
- 若有 sector，應與 route 綁定，避免不同路線誤用同一組 sector 結構

可行方向：

- route 定義中可描述 sector 數量與名稱
- record 中可填對應 sector time

用途：

- 顯示分段表現
- 支援未來更細的分析
- 支援基準時間與分梯資料的延伸

## 紀錄標示規則

以 route 為單位標示：

- `TR`：該路線全體最快
- `CR`：該車在該路線最快
- `PR`：該玩家在該路線最快

優先級：

- `TR > CR > PR`

規則：

- 若已達成 `TR`，只顯示 `TR`
- 不同時再疊出 `CR`、`PR`

## 頁面與產品層級

### 1. Dashboard

目標是一次看到大量賽道資訊，但只顯示基本內容：

- 時間
- 車輛
- 玩家
- 平台

UI 層級建議：

- `track_variant card`
  - 底下展開多個 `track_route row / block`

每個 variant 卡片應：

- 顯示該 variant 下所有 routes
- 包含有紀錄與無紀錄的 route
- route 多寡可影響卡片尺寸

### 2. Analytics Page

Dashboard 與圖表頁應分開。

分析頁可處理：

- 賽道熱度
- 刷新頻率
- 紀錄活躍度
- 分段資料分析

### 3. Search Records UI

這是一個統一框架，不應拆成太多獨立 UI。

同一框架內支援：

- 個人戰績查詢
- 車輛榜查詢
- 賽道紀錄查詢

### 4. New Record Board

保留。

用途：

- 顯示最近幾筆新紀錄
- 刺激玩家跟跑
- 適合公共空間、聚會場所、入口處

### 5. Club / Team Board

保留。

與玩家資料擴充直接相關，可顯示：

- 團隊內榜
- 團隊常用車
- 團隊活躍賽道

### 6. Activity / Season Views

保留為後續方向，但需依賴新資料層。

### 7. Venue Rotation

保留。

可作為展示策略，例如輪播：

- 最新紀錄
- 總表摘要
- 單一 variant 榜
- 活動榜

## 玩家資料擴充

玩家資料不應只有名稱。

建議擴充為：

- 顯示名稱
- 玩家唯一 ID
- 更名紀錄
- 所屬車隊 / 團體
- 歷年紀錄
- 歷屆賽事成績
- 常用車輛
- 使用車輛頻率
- 常跑賽道類型
- 擅長賽道類型

### 哪些可直接由總表推導

- 往年紀錄
- 常用車輛
- 使用頻率
- 常跑 route / variant / 類型

### 哪些需要新資料層

- 賽事成績
- 活動成績
- 賽季排名
- 車隊 / 團體關聯

## GitHub Pages 託管策略

### 定位

`GitHub Pages` 應作為：

- 公開網站層
- 靜態資料發布層
- VRChat 專用輸出層

而不是：

- 即時後端
- 動態寫入 API
- 多人在線提交服務

### 建議分層

#### 1. Public Site Layer

放在 GitHub Pages：

- 前端網站
- 公開查詢頁
- 公開圖表頁
- 靜態 JSON / TXT / PNG
- manifest / revision 檔

#### 2. Builder / Publisher Layer

放在本機或私有環境：

- 整理原始紀錄
- 驗證資料
- 產生統計
- 產生 feed
- bake 圖片
- 輸出成靜態檔後再 push 到 Pages

### 第一版建議

第一版應接受：

- 公開站是靜態
- 管理端先離線
- 所有 VRChat 顯示都讀靜態產物

## VRChat 專用輸出層

### 目的

給 Unity / VRChat 讀取的內容不應直接等於主網站內部資料，而應是經過整理的專用 artifact。

### 建議輸出類型

- `dashboard feed`
- `track board feed`
- `player / vehicle feed`
- `selection feed`
- `full summary feed`
- `lightweight update feed`
- `baked leaderboard image`

### 為什麼要獨立輸出層

- Unity / VRChat 效能限制較嚴格
- 需要固定格式、少量欄位、可快取內容
- 需要和一般 web 分析資料分流

## Artifact 生成與快取策略

### 原則

VRChat 輸出不應隨時重算，而應做成：

- 按需生成
- 相同要求共用同一份輸出
- revision 沒變就不重建

### Artifact Key

應把請求正規化成 key，例如：

- `variant:{id}:top5`
- `selection:{id}:summary`
- `variant:{id}:image:v2`
- `dashboard:{selection_hash}`

### 快取規則

- 同 key 同 revision 只生成一次
- 同時間相同要求共用同一份結果
- 內容沒變不重產
- 輸出檔名可帶 revision 或 hash

### 建議分層

- `hot artifacts`
  - 常用賽道、總表摘要、最新紀錄板
- `warm artifacts`
  - 有要求才生成，生成後保留
- `cold artifacts`
  - 特殊用途，較少生成

## Baked Image 策略

如果納入 baked image，網站端需要負責生成賽道排行榜圖片。

適合用途：

- 總表看板
- 新紀錄公告板
- 單一 variant 榜
- 固定展示板

優點：

- Unity 端排版壓力小
- 載入簡單
- 適合公開展示

缺點：

- 互動性低
- 需要 builder 支援圖像輸出

## 不建議第一階段就做的事

- GitHub Pages 直接承擔動態寫入
- 在 family / variant / route 還沒定清楚前就直接寫大量 UI
- 沒先定義輸出層就直接做 Unity 接收端
- 在活動 / 賽季資料層還沒定義前就先做賽季榜

## 待討論問題

- draft 紀錄是否允許綁在未轉正的 variant / route 上？
- sector 定義是由 route 固定，還是可針對版本調整？
- family 是否要做統計聚合，還是只做瀏覽分群？
- 玩家 / 車隊資料是由人工管理，還是部分可由匯入整合？
- 差距秒數是即時計算，還是儲存成欄位？
