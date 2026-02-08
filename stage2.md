# 阶段二：Polymarket 链上市场与交易索引器

## 学习目标

通过实战构建一个针对 Polymarket 市场和交易的链上数据索引器，理解从链上原始数据到业务语义层数据对齐的完整流程。具体目标：

- 理解链上日志数据如何与 Polymarket 应用层的市场概念对应，并设计流程定期同步链上数据。
- 实现一个最小可用的索引服务，能够扫描 Polygon 上 Polymarket 合约的交易数据，将市场和交易信息存储到本地数据库，并提供查询接口。
- 培养工程上的健壮性考虑，包括数据一致性校验、错误重试、断点续传、幂等写入等机制，确保索引器长期稳定运行。

---

## 系统架构概览

一个 Polymarket 数据索引系统大体上由**数据源**、**处理管道**、和**数据库/接口**三部分组成。

### 数据源 (Data Sources)

#### 链上数据 (Primary)

主要来源是区块链节点的 RPC 接口或区块链事件订阅。我们需要从 Polygon 链上获取与 Polymarket 相关的合约事件，例如：

- `OrderFilled`, `OrdersMatched`（来自交易撮合合约）
- `ConditionPreparation`, `PositionSplit`, `PositionsMerge`, `PayoutRedemption` 等（来自 ConditionalTokens 或适配器合约）

这些链上事件是权威的交易和状态记录，构成最终事实依据。

#### 链下数据 (Secondary)

Polymarket 提供的 **Gamma API**。这属于中心化服务，提供了市场的元数据（例如市场的 slug、标题描述、分类标签、是否负风险、状态等）和某些链上参数的缓存（如 `conditionId`、`questionId`、`clobTokenIds` 等）。虽然这些数据在链上有对应来源，但通过 API 获取可以简化解析过程，并提供丰富的语义信息（例如市场问题的文本描述、截止时间等）。Secondary 数据用于补充说明和验证，但应与 Primary 数据交叉核对，以确保一致。

---

## 索引流程 (Pipeline)

索引器需要持续地发现新市场、同步交易，并提供查询服务。可以将流程分解为几个阶段：

### 1. 市场发现 (Market Discovery)

定期或在启动时，从 Gamma API 拉取事件列表和市场列表。由于 Gamma 将 Market 组织在 Event 下，我们可以选取感兴趣的 Event（例如通过 slug 明确指定）或者通过 Gamma 的 `/markets` 列表端点获取所有当前活动的市场。

对于每个 Market 数据，提取其中的关键信息：

- `slug`：市场短标识符，用于友好地标记市场。
- `conditionId`：链上条件 ID。
- `questionId`：链上问题 ID（对应 UMA Oracle 的问题哈希）。
- `oracle`：预言机合约地址（通常 UMA Adapter 合约地址）。
- `enableOrderBook` / `status` 等：指示该市场是否开启交易，以及市场状态（未结算/已结算等）。
- `clobTokenIds`：这通常是一个包含两个 tokenId 的列表，分别对应市场的 Yes 和 No 头寸。在 Gamma API 文档中，提到 Market 映射到"一对 CLOB token ids"。

**验证**：对于拿到的 `conditionId` 和 `clobTokenIds`，可以本地计算一遍（参考阶段一任务 B 中 MarketDecoder）以确保 Gamma 数据可靠。即使用 `conditionId` 和 collateral USDC 地址计算 yes/noTokenId，核对是否和 `clobTokenIds` 吻合。如果不符，需谨慎处理（可能是数据错误或版本差异）。

将新的市场记录插入数据库的 `markets` 表（结构见下文），或更新已有市场的信息（例如市场状态改变）。

### 2. 历史数据同步 (Backfill/Sync)

针对数据库中已登记的市场，抓取其历史和实时交易事件。实现上可以有两种策略：

**按区块高度顺序全局扫描**：如果 Polymarket 的交易主要发生在固定的几个合约（如前述两个 Exchange 合约地址），我们可以针对这些合约地址，用 `eth_getLogs` 按区块范围批量获取日志。例如每次获取 Exchange 合约在某区块范围内的所有 `OrderFilled` 事件。这样效率较高且实现简单。由于 Polymarket 两个主要撮合合约地址已知，我们可以通过 topics 过滤指定 `OrderFilled` 事件的签名和这些地址来抓取所有成交记录。

**按市场逐个扫描**：(不太必要) 也可依据每个市场的 tokenId 过滤 Transfer 或 PositionSplit，但 Polymarket 没有单独的市场合约，所以一般不这么做。而是直接扫描交易所合约的事件即可拿到全部交易，再按 tokenId 分类归属市场。

实际工程中，会设置一个起始区块 `fromBlock`（比如 Polymarket 上线 Polygon 的开始区块或上次中断的区块），以及一个每次处理的 `toBlock` 批次范围。循环调用 `getLogs` 获取这些区块间的所有相关事件日志。需要考虑限流和响应大小：可以按比如 10000 区块为一批，逐段扫描。同时，维护一个全局的同步状态（如 `sync_state` 表或本地文件）记录当前已处理到的最新区块高度，以支持断点续传。

### 3. 解码与处理 (Decoding & Processing)

对于获取到的 `OrderFilled` 日志列表，逐条解析（利用阶段一编写的 TradeDecoder）。解析出交易详情后，需要将其归类到对应市场：

- 日志中的 `makerAssetId` 或 `takerAssetId` 非零值，就是交易涉及的头寸 TokenId（例如 `tokenId = 0x...1234`）。通过查找数据库 `markets` 表，找到记录中 `yes_token_id` 或 `no_token_id` 等于该 TokenId 的市场。这样即可确定此交易属于哪个市场。
- 获取该市场的主键 `market_id`，连同交易信息一起准备写入 `trades` 表。
- 同时可以确定交易的具体 outcome（是 Yes 还是 No）。例如如果匹配的是 `yes_token_id`，可以在需要时标注这笔交易在买卖 "YES" 头寸；匹配 `no_token_id` 则是针对 "NO" 头寸。（在基本要求中，可以不特别标明，但在扩展应用中可能有用，比如区分买卖的到底是哪一边结果）。

**注意处理重复数据**：由于链上日志唯一由 `(tx_hash, log_index)` 标识，我们可以在插入数据库时设置该组合为唯一键，避免重复插入同一条链上记录。解析过程中也可以自行去重（比如用集合暂存已处理过的 tx+index）。

此外，需要处理其他可能的日志类型：

- `PositionsSplit` / `PositionsMerge`：虽然我们的重点是交易，但这些日志可以帮助确认市场创建和销毁情况。尤其 `PositionSplit` 可以作为市场存在和初始流动的证据，可选地记录下来。
- `OrdersMatched`：此事件主要将一系列 `OrderFilled` 关联起来（提供撮合订单哈希等信息），一般可用于统计撮合次数，但对成交列表本身影响不大，可暂时忽略或者简单记录。
- 如果考虑负风险市场的转换，还可能有 `PositionsConverted` 事件。初期索引器可以不深入解析转换事件，只要记录有交易发生即可。

### 4. 存储 (Storage)

将解析好的交易数据批量写入数据库中的 `trades` 表，以及相应更新市场状态：

- 对每条交易，插入一行包含交易哈希、市场 ID、价格、数量、方向、时间戳等信息的记录。
- 时间戳可以通过交易所在区块的时间获得（可从区块缓存或通过 RPC 获取区块信息）。有的日志提供 `blockTimestamp`（如果 RPC 支持 `eth_getLogs` 返回），否则需要额外查询区块。
- 市场表可能也需要更新一些信息，比如最后交易时间、累计交易量等，可在此阶段一起完成（或由查询时计算）。
- 更新 `sync_state` 存储新的 `last_block`，高度至少到 `toBlock`，下次扫描从那里开始。这样即使程序中断，也能从上次中止处继续，不会漏掉或重复。

整个流程运行后，就建立了链上数据和本地语义数据的索引映射：每个市场知道了自己的 tokenId，预言机等信息，交易表记录了所有和这些市场相关的成交历史。这为后续提供查询和分析服务奠定了基础。

---

## 数据库设计 (Schema)

为了存储上述数据，我们需要设计关系型表结构来高效查询。根据任务描述，建议的表结构如下：

### `markets` 表

市场基本信息，每条记录对应 Polymarket 的一个市场（通常一个问题的 Yes/No 预测）：

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PRIMARY KEY | 自增主键 |
| `slug` | VARCHAR | 市场短标识符 |
| `condition_id` | VARCHAR | 链上条件 ID |
| `question_id` | VARCHAR | 链上问题 ID |
| `oracle` | VARCHAR | 预言机合约地址 |
| `collateral_token` | VARCHAR | 抵押品代币地址 |
| `yes_token_id` | VARCHAR | YES 头寸 Token ID |
| `no_token_id` | VARCHAR | NO 头寸 Token ID |
| `enable_neg_risk` | BOOLEAN | 是否为负风险市场 |
| `status` | VARCHAR | 市场状态 |
| `created_at` | TIMESTAMP | 创建时间 |

（实际实现中，字符串长度可根据需要调整为定长或 TEXT。）

### `trades` 表

交易记录，每条记录对应链上一笔 `OrderFilled` 事件（可能是部分成交，也可能是完整撮合的一部分）：

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PRIMARY KEY | 自增主键 |
| `market_id` | INTEGER FOREIGN KEY | 关联的市场 ID |
| `tx_hash` | VARCHAR | 交易哈希 |
| `log_index` | INTEGER | 日志索引 |
| `maker` | VARCHAR | 挂单方地址 |
| `taker` | VARCHAR | 吃单方地址 |
| `side` | VARCHAR | 买卖方向 (BUY/SELL) |
| `outcome` | VARCHAR | 结果类型 (YES/NO) |
| `price` | DECIMAL | 成交价格 |
| `size` | DECIMAL | 成交数量 |
| `timestamp` | TIMESTAMP | 成交时间 |

**唯一索引**：`(tx_hash, log_index)` 以确保重复插入时违反唯一性，从而实现幂等。

### `sync_state` 表

存储同步进度等信息：

| 字段 | 类型 | 说明 |
|------|------|------|
| `key` | VARCHAR PRIMARY KEY | 状态键名 |
| `last_block` | INTEGER | 最后处理的区块高度 |
| `updated_at` | TIMESTAMP | 更新时间 |

可以将 key 设成 `'market_sync'` 和 `'trade_sync'` 分别跟踪市场和交易的同步进度，或者简单用 `'global_indexer'` 一个 key 记录整体同步进度。

---

## 任务拆解

### 任务 A: Market Discovery Service 实现

**需求**：编写一个脚本或服务，负责定期发现新的市场并存储市场信息。

#### 实现步骤

**1. 获取市场列表**

利用 Polymarket Gamma API 提供的接口获取市场数据。Gamma 提供了按事件获取市场和直接获取所有市场等方式。例如，可以调用 `GET /markets` 或基于给定的 Event slug 调用 `GET /events/{slug}/markets`。假设我们有事件 slug，如 `"will-there-be-another-us-government-shutdown-by-january-31"`，就获取该事件下的所有市场列表。

**2. 解析市场数据**

对于每个市场条目，提取我们关心的信息：
- `slug`（有的市场 Slug 可能和事件 slug 相关或带序号，如 `fed-rate-jan2024-unchanged` 之类）
- `conditionId`, `questionId`, `oracleAddress`, `outcomeSlotCount` 等链上参数
- `collateralToken`（一般应为 USDC 地址，可默认为 USDC，如果 API 未提供就填默认值）
- `clobTokenIds` 列表（大小为 2 的数组，对应 Yes/No tokenId）。有些 API 输出可能标记为 token0/token1 或类似字段，要对照文档理解
- 市场描述、状态（active/inactive/resolved）、截至时间等额外信息也可获取保存

**3. 校验数据**

用链上公式校验 `clobTokenIds` 是否正确：
- 根据拿到的 `conditionId`，使用我们在阶段一编写的算法重新计算 yes/noTokenId，确保与 API 给出值吻合。如果不一致，需打印警告或暂不记录该市场，避免错误数据。
- 确认 `collateralToken` 是预期的 USDC 地址。

**4. 存储到数据库**

将市场信息插入或更新到 `markets` 表：
- 如果表中已存在相同的 `conditionId`（或 slug），则更新其信息（可能是状态变化或补充标题等）。
- 如不存在，则插入新记录，生成新的 id。
- 特别地，记录 `enable_neg_risk`（是否负风险事件，可从 Gamma 的 `event.negRisk` 字段或市场数量判断：如果同一事件有多市场则为 true）。
- 记录 `created_at`（可取 Gamma 提供的创建时间，或通过链上 `ConditionPreparation` 的区块时间，但前者获取方便）。

**5. 定期运行**

将上述流程设置为一个周期任务，每隔一段时间（如每小时）调用，以捕获新上线的市场。或者在启动时拉取一次全量市场。也可以根据 Gamma 提供的"最新市场 ID"增量拉取。

通过 Market Discovery，我们确保索引器掌握最新的市场清单，为后续同步交易做准备。

---

### 任务 B: Trades Indexer 实现

**需求**：实现一个函数如 `run_indexer(from_block, to_block)`，扫描指定区块范围内 Polymarket 的交易日志，并将交易存入数据库。

#### 实现要点

**1. 获取日志**

使用 RPC 的 `eth_getLogs` 接口，构造过滤参数：
- `address`: 设置为 Polymarket Exchange 合约地址列表（可以两个地址都填上，以同时获取 Binary 和 NegRisk 交易所的事件）。
- `topics[0]`: 设置为 `OrderFilled` 事件的事件签名哈希（可以在合约 ABI 中找到，或根据已知字段计算）。这将仅返回我们需要的成交事件。
- `fromBlock` 和 `toBlock`: 设置为函数参数，或者从上次同步位置读取。本次要处理的区块区间。注意不要一次跨度太大以免超时，根据需要拆分多次调用。

示例：

```javascript
getLogs({
  address: [exch1, exch2],
  topics: [ORDER_FILLED_TOPIC],
  fromBlock: 40000000,
  toBlock: 40010000
})
```

**2. 解析日志列表**

对返回的每条日志，应用 TradeDecoder（阶段一任务 A）提取结构化数据：

- 解出 `tx_hash`, `log_index`, `maker`, `taker`, `makerAssetId`, `takerAssetId`, `amounts`, `price`, `tokenId`, `side` 等。
- 通过 `tokenId` 找到所属市场的 `market_id`（查询数据库 `markets` 表）。如未找到匹配市场，可能意味着出现了尚未记录的新市场：
  - 对于这种情况，可调用 Market Discovery 流程补充该市场（例如某些市场是在我们上次获取后新创建但 Gamma 未及时提供，或者我们的市场列表滞后）。这相当于动态市场发现：索引交易时发现未知 tokenId 则立即去 Gamma API 获取它的市场信息并入库。
  - 如果仍无法匹配，需记录异常日志以便调查。
- 确定 outcome 类型：若 `tokenId` 等于 `market.yes_token_id`，则 `outcome = "YES"`，若等于 `no_token_id` 则为 `"NO"`。
- 整理 price 和 size：price 已有，size 则取成交的头寸数量（记得换算为实际单位，一般 `size = takerAmountFilled / 1e6` 如果 `takerAssetId` 是头寸，或 `makerAmountFilled / 1e6` 如果 `makerAssetId` 是头寸，总之就是 OutcomeToken 的实际张数）。
- 时间戳：日志本身不含时间，需要查询该日志所在区块时间。可以在获取日志时顺便获取 `blockNumber`，然后批量查区块时间，或者利用本地缓存。也可以在事后通过交易哈希调用 `eth_getTransactionReceipt` 再取 blockNumber + 再查时间。但为了效率，推荐批量区块查询。常用方法是：维护一个简单缓存字典，遇到新 blockNumber 用 `eth_getBlockByNumber` 查询一次时间戳，存入缓存。

**3. 写入数据库**

采用批量插入或逐条插入 `trades` 表：
- 插入前可以先按照 `(tx_hash, log_index)` 排序数据，确保写入顺序一致（可选）。
- 利用数据库的唯一键防止重复插入。如果数据库支持 UPSERT，可以直接 UPSERT，否则捕获重复错误后忽略。
- 每个插入包括所有解析出的字段。Decimal 类型字段注意以合适类型写（或转为字符串后由 SQL 转换）。
- 建议在同一事务中完成一批区块范围的插入，确保原子性。

**4. 更新同步点**

当该批次 `toBlock` 的日志处理完毕，更新 `sync_state.last_block` 为 `toBlock` 或 `toBlock_processed`。这样即使程序停止，下次启动知道从哪里继续。更新时也记录当前时间。

**5. 循环运行**

可以将上述流程放在一个循环或调度中，不断向前推进块高，直至最新区块。同步初期可能需要从 Polymarket 上线以来的早期区块开始 backfill，完成历史数据入库。之后进入实时同步模式，可每隔几秒/块查询一次新日志。

> **注意**：使用 `eth_getLogs` 批量获取历史数据时，要小心 RPC 提供商限频和数据量。可以加上指数退避重试，以及对 `fromBlock` - `toBlock` 区间根据返回数据量动态调整大小。如果链上交易频繁，一个区间日志很多，可以缩短区间，以避免单次返回过大数据。Polymarket 每日交易量相对适中，但遇到热点事件可能有大量成交，应考虑性能。

---

### 任务 C: 查询 API 服务

最后，为了提供方便的数据查询接口，我们可以构建一个简单的 REST API（使用 FastAPI、Flask 等皆可）来查询数据库内容。两个基础接口：

#### `GET /markets/{slug}`

输入市场 slug，返回该市场的详细信息（对应 `markets` 表的内容）。包括 `conditionId`, `questionId`, `tokenIds`，以及市场的文本描述、状态等。

示例返回：

```json
{
  "slug": "fed-rate-jan2024-unchanged",
  "title": "美联储1月是否维持利率不变？",
  "conditionId": "0xabc...123",
  "oracle": "0xUMAAdapterV2...",
  "yesTokenId": "0xYYY...",
  "noTokenId": "0xZZZ...",
  "status": "active",
  "created_at": "2024-01-01T00:00:00Z"
}
```

如果找不到该 slug，返回 404 或空结果。

#### `GET /markets/{slug}/trades?limit=100&offset=0`

分页查询该市场的历史交易记录。可以通过 join `trades` 表获取数据，或在 `trades` 表直接筛选 `market_id` 来获取。返回 JSON 列表，每项包含交易详情（时间戳、价格、数量、买卖方等）。

示例返回：

```json
[
  {
    "timestamp": "2024-01-05T12:00:00Z",
    "side": "BUY",
    "outcome": "YES",
    "price": "0.45",
    "size": "100.0",
    "maker": "0xMaker...",
    "taker": "0xTaker..."
  },
  { "..." }
]
```

列表默认按时间排序（可以按 `tx_hash` 顺序近似认为按时间，因为同区块内顺序可忽略）。可提供参数控制排序或过滤（比如按地址过滤该用户的交易）。

#### 实现提示

- 使用一个轻量的 web 框架建立路由，对接数据库查询即可。对于频繁查询，可以添加简单的缓存。
- 注意对 slug 或输入做校验，防止 SQL 注入（如果直接拼接 SQL 的话）。
- 若性能要求高，可在 `trades` 表建立 `market_id`, `timestamp` 的索引，加速按市场查询排序。

通过这些 API，前端或用户就可以方便地按市场获取链上的交易数据，验证我们的索引结果。

---

## 工程实现关键点

### 断点续传

务必确保索引器可以随时中断和重启而不造成数据丢失或重复。`sync_state` 的 `last_block` 是核心。实现时，可以在启动时读取 `last_block`，从 `last_block + 1` 开始继续扫描（因为 `last_block` 可能已处理完）。在处理过程中，先不立即更新 `last_block`，待确认该批次完整写入成功后再更新，以免中途失败导致进度丢失。对于市场列表，同样可以有类似机制（记录最后同步的市场 ID 等）。但由于 Gamma API 可以直接获取全量市场，简单起见每次都全量拉取更新也未尝不可。

### 错误重试

链上 RPC 有时会超时或失败。应对每次 `getLogs` 和 `getBlock` 请求做好异常处理和重试策略。例如，封装一个带有**指数退避 (exponential backoff)** 的重试，出现网络错误或服务端错误时等待一段时间再重试，最多重试 N 次。在批量同步初期，大量请求可能触发速率限制，合理安排请求节奏和并发。

### 数据一致性

由于我们结合链上和链下数据，必须注意两者的同步。**永远以链上数据为准**：即使 Gamma API 提供的市场列表可能有延迟或错误，我们也不能漏掉链上实际发生的交易。因此，才需要在解析交易时动态发现未知市场并补录。同时，当市场结算时，Gamma API 会更新其状态为 resolved，我们也应从链上检测到 `ConditionResolution` 或通过预言机事件知道市场结束，从而更新数据库状态。这两种来源可以互相验证。如出现冲突（链上显示结算但 Gamma 未更新或相反），应倾向相信链上并进行记录。

### 幂等写入

如前所述，插入数据库时利用唯一键避免重复。对于市场数据，也可用 `conditionId` 作为唯一键，后插入的相同 `conditionId` 更新已有记录而不是新增。幂等保证即使某段区块重复处理（可能因为重启时 `last_block` 没更新好，或者手工重新跑某范围），不会产生重复交易记录。

### 性能和扩展

随着 Polymarket 市场数量和交易量增长，注意数据库索引和查询效率。例如 `trades` 表按 `market_id` 查询需要索引支撑。批量插入可以提升写入性能。可以考虑使用异步方式处理 I/O（如使用 asyncio 或多线程）来同时获取链上数据和 Gamma 数据。进一步扩展可以引入消息队列，分离出获取和处理的阶段，但在本任务范围内简单顺序流程即可。

---

## 进阶思考

### 链上重组 (Reorg) 处理

在罕见情况下，Polygon 区块链可能发生区块重组，使某些已确认的交易被替换或日志被回滚。如果索引器只按照区块高度推进，一旦遇到重组，可能会记录一些幽灵交易或漏掉重组后的新交易。

一个稳健的方案是在同步最新数据时**延后几个区块确认**：比如当前区块高度是 N，索引器只处理到 N-5 或 N-10，然后等待。当检测到链上出现重组（可通过比较已处理区块的哈希变化），需要删除回滚的那部分数据并重跑。因此，维护每条交易的 `blockNumber` 以及 `blockHash` 在数据库也有帮助，用于比对。当然，在 Polygon 上大规模 reorg 极少，但小的临时 reorg 是可能的，索引器应有相应设计。

### 复杂交易拆解

在负风险市场中，一个用户的下单可能触发更复杂的撮合场景。例如，一笔 Order 可能同时与多个对手单成交，或者通过 NegRiskAdapter 将一种头寸转换为多种头寸。链上体现为多条 `OrderFilled` 和 `PositionsConverted` 等事件。

在我们的模型中，我们将每个 `OrderFilled` 都记录为独立交易记录。这对于统计成交量等已足够。但如果要还原更高层的意图（比如用户一次操作的整体情况），需要结合多条日志分析，这属于更高级的解析，不在当前索引范围内。初期索引器可以忽略这种拆解需求，只聚焦于逐笔链上成交记录的简单、完整记录。

---

## 总结

通过完成阶段二的索引器构建，我们能够将阶段一学到的 Polymarket 链上数据解析知识应用到实际工程中，搭建起从链上数据到业务数据库的桥梁。在确保准确性、一致性的基础上，索引器可以支持丰富的应用，比如实时行情、历史数据分析、用户盈亏计算等，为 Polymarket 生态提供重要的数据基础设施。

---

## 参考实现示例

以下是基于本项目代码的具体示例，供学习和参考。

### 环境配置

1. 复制 `.env.example` 为 `.env` 并填入必要配置：

```bash
cp .env.example .env
```

2. 安装依赖：

```bash
pip install -r requirements.txt
```

3. 配置必要的环境变量（`.env` 文件）：

```
RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YOUR_API_KEY
DB_PATH=./data/indexer.db
```

### 示例数据

项目中使用的示例数据：

- **示例交易哈希**：`0x916cad96dd5c219997638133512fd17fe7c1ce72b830157e4fd5323cf4f19946`
- **示例事件 Slug**：`will-there-be-another-us-government-shutdown-by-january-31`

### 数据库 Schema 实现

`src/db/schema.py` 定义了数据库表结构，使用 SQLite：

```python
# 初始化数据库
conn = init_db("./data/indexer.db")
```

表结构包括：
- `events` - 事件信息表
- `markets` - 市场信息表（包含 `yes_token_id`, `no_token_id` 等）
- `trades` - 交易记录表（唯一索引 `tx_hash + log_index`）
- `sync_state` - 同步状态表

### 数据存储实现

`src/db/store.py` 提供了数据访问层函数：

```python
# 保存市场信息
upsert_market(conn, market_dict)

# 保存交易记录
insert_trades(conn, trades_list)

# 查询市场
market = fetch_market_by_slug(conn, "market-slug")

# 查询交易
trades = fetch_trades_for_market(conn, market_id, limit=100, offset=0)
```

### 索引器核心实现

`src/indexer/run.py` 中的 `run_indexer` 函数是核心入口：

```python
results = run_indexer(
    w3=w3,
    conn=conn,
    settings=settings,
    from_block=from_block,
    to_block=to_block,
    exchange_address=exchange_address,
    neg_risk_exchange=neg_risk_exchange,
    ctf_address=ctf_address,
    exchange_abi=exchange_abi,
    ctf_abi=ctf_abi,
    include_ctf=False,
    include_exchange=True,
    include_neg_risk=True,
    event_slug="will-there-be-another-us-government-shutdown-by-january-31",
)
```

### API Server 实现

`src/api/server.py` 提供了 HTTP API 服务，支持以下端点：

| 端点 | 说明 |
|------|------|
| `GET /events/{slug}` | 获取事件详情 |
| `GET /events/{slug}/markets` | 获取事件下的所有市场 |
| `GET /markets/{slug}` | 获取市场详情 |
| `GET /markets/{slug}/trades` | 获取市场交易记录（支持分页） |
| `GET /tokens/{token_id}/trades` | 按 TokenId 获取交易记录 |

查询参数：
- `limit` - 返回条数限制（默认 100）
- `cursor` - 分页偏移量
- `fromBlock` / `toBlock` - 区块范围过滤

---

## 验证命令规范

> **重要提示**：任务验收将**严格按照以下规范**进行。请确保你的实现能够通过下述所有验证命令，并产出符合规定格式的输出。**不符合规范的提交将无法通过验收。**

完成任务后，请使用以下统一命令进行验证。所有命令均在 `stage2/` 目录下执行。

### 前置检查

```bash
# 确保环境配置正确
cp .env.example .env
# 编辑 .env 填入有效的 RPC_URL

# 安装依赖
pip install -r requirements.txt
```

### 任务 A：Market Discovery 验证

Market Discovery 功能集成在 `run_indexer` 中，会自动从 Gamma API 获取并保存市场信息。

```bash
# 运行 demo 时会自动执行 Market Discovery
python -m src.demo \
    --tx-hash 0x916cad96dd5c219997638133512fd17fe7c1ce72b830157e4fd5323cf4f19946 \
    --event-slug will-there-be-another-us-government-shutdown-by-january-31 \
    --reset-db
```

验证数据库中是否有市场数据：

```bash
sqlite3 ./data/demo_indexer.db "SELECT slug, condition_id, yes_token_id, no_token_id FROM markets LIMIT 5;"
```

### 任务 B：Trades Indexer 验证

```bash
# 基础用法：索引单个区块（包含示例交易的区块）
python -m src.demo \
    --tx-hash 0x916cad96dd5c219997638133512fd17fe7c1ce72b830157e4fd5323cf4f19946 \
    --event-slug will-there-be-another-us-government-shutdown-by-january-31 \
    --output ./data/demo_output.json

# 指定区块范围索引
python -m src.demo \
    --from-block 66000000 \
    --to-block 66001000 \
    --event-slug will-there-be-another-us-government-shutdown-by-january-31 \
    --db ./data/indexer.db

# 重置数据库后重新索引
python -m src.demo \
    --tx-hash 0x916cad96dd5c219997638133512fd17fe7c1ce72b830157e4fd5323cf4f19946 \
    --event-slug will-there-be-another-us-government-shutdown-by-january-31 \
    --reset-db \
    --output ./data/demo_output.json
```

**预期输出格式**：

```json
{
  "stage2": {
    "from_block": 66000000,
    "to_block": 66000000,
    "inserted_trades": 5,
    "market_slug": "will-there-be-another-us-government-shutdown-by-january-31",
    "market_id": 1,
    "sample_trades": [
      {
        "tx_hash": "0x...",
        "log_index": 123,
        "block_number": 66000000,
        "timestamp": "2024-01-15T12:00:00",
        "side": "BUY",
        "outcome": "YES",
        "price": "0.45",
        "size": "100.0",
        "token_id": "12345..."
      }
    ],
    "db_path": "./data/demo_indexer.db"
  }
}
```

验证数据库中的交易数据：

```bash
sqlite3 ./data/demo_indexer.db "SELECT tx_hash, side, outcome, price, size FROM trades LIMIT 10;"
```

### 任务 C：API Server 验证

```bash
# 启动 API 服务器
python -m src.api.server --db ./data/demo_indexer.db --port 8000
```

在另一个终端中测试 API 端点：

```bash
# 获取事件信息
curl http://127.0.0.1:8000/events/will-there-be-another-us-government-shutdown-by-january-31

# 获取市场信息
curl http://127.0.0.1:8000/markets/will-there-be-another-us-government-shutdown-by-january-31

# 获取市场交易记录（带分页）
curl "http://127.0.0.1:8000/markets/will-there-be-another-us-government-shutdown-by-january-31/trades?limit=10&cursor=0"

# 按 TokenId 获取交易
curl "http://127.0.0.1:8000/tokens/<token_id>/trades?limit=10"
```

**API 响应格式示例**：

`GET /markets/{slug}`:

```json
{
  "market_id": 1,
  "slug": "will-there-be-another-us-government-shutdown-by-january-31",
  "condition_id": "0xabc...123",
  "question_id": "0xdef...456",
  "oracle": "0x157Ce2d672854c848c9b79C49a8Cc6cc89176a49",
  "collateral_token": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
  "yes_token_id": "12345...",
  "no_token_id": "67890...",
  "status": "active"
}
```

`GET /markets/{slug}/trades`:

```json
[
  {
    "trade_id": 1,
    "market_id": 1,
    "tx_hash": "0x...",
    "log_index": 123,
    "block_number": 66000000,
    "timestamp": "2024-01-15T12:00:00",
    "maker": "0x...",
    "taker": "0x...",
    "side": "BUY",
    "outcome": "YES",
    "price": "0.45",
    "size": "100.0"
  }
]
```

### 综合验证流程

```bash
# 1. 初始化并索引数据
python -m src.demo \
    --tx-hash 0x916cad96dd5c219997638133512fd17fe7c1ce72b830157e4fd5323cf4f19946 \
    --event-slug will-there-be-another-us-government-shutdown-by-january-31 \
    --reset-db \
    --db ./data/demo_indexer.db \
    --output ./data/demo_output.json

# 2. 检查输出文件
cat ./data/demo_output.json

# 3. 验证数据库内容
sqlite3 ./data/demo_indexer.db "SELECT COUNT(*) FROM markets;"
sqlite3 ./data/demo_indexer.db "SELECT COUNT(*) FROM trades;"

# 4. 启动 API 服务并测试（在后台运行）
python -m src.api.server --db ./data/demo_indexer.db --port 8000 &

# 5. 测试 API
curl http://127.0.0.1:8000/markets/will-there-be-another-us-government-shutdown-by-january-31
curl "http://127.0.0.1:8000/markets/will-there-be-another-us-government-shutdown-by-january-31/trades?limit=5"

# 6. 停止 API 服务
kill %1
```

### 验证清单（必须全部通过）

以下所有检查项均为**必须通过**的验收标准：

- [ ] 数据库正确初始化，包含 `events`、`markets`、`trades`、`sync_state` 表
- [ ] Market Discovery 能从 Gamma API 获取市场并存入数据库
- [ ] 市场数据包含正确的 `yes_token_id` 和 `no_token_id`
- [ ] Trades Indexer 能扫描指定区块范围的 `OrderFilled` 事件
- [ ] 交易记录正确关联到对应的市场（通过 TokenId 匹配）
- [ ] 交易记录包含正确的 `outcome`（YES/NO）
- [ ] 重复插入相同交易不会产生重复数据（幂等性）
- [ ] `sync_state` 正确记录最后处理的区块高度
- [ ] API 服务正常启动并响应请求
- [ ] `GET /markets/{slug}` 返回正确的市场信息
- [ ] `GET /markets/{slug}/trades` 返回分页的交易记录
- [ ] API 支持 `limit`、`cursor`、`fromBlock`、`toBlock` 查询参数

### 验收标准说明

1. **命令格式**：验收时将使用上述规定的命令格式运行你的代码，请确保命令行参数与规范一致。
2. **输出格式**：JSON 输出必须包含规定的所有字段，字段名称必须与示例完全一致（区分大小写）。
3. **数据正确性**：使用示例交易哈希 `0x916cad96dd5c219997638133512fd17fe7c1ce72b830157e4fd5323cf4f19946` 和示例事件 Slug `will-there-be-another-us-government-shutdown-by-january-31` 进行验证。
4. **数据库结构**：数据库表结构必须与文档中定义的 Schema 一致，包括字段名称和类型。
5. **API 规范**：API 端点路径和响应格式必须与文档规定一致，评审将使用 `curl` 命令进行验证。
6. **代码可运行**：提交的代码必须在配置好 `.env` 后能够直接运行，不能有额外的手动配置步骤。