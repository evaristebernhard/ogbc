# Stage2 Polymarket Indexer 使用说明

本仓库已实现 `stage2.md` 的任务 A/B/C：

- A: Market Discovery（从 Gamma API 发现市场并入库）
- B: Trades Indexer（扫描 Polygon 上 `OrderFilled` 事件并入库）
- C: Query API（按事件/市场/Token 查询）

代码目录：`stage2/`

## 1. 环境准备

1. 进入项目目录

```bash
cd stage2
```

2. 配置环境变量

```bash
cp .env.example .env
```

如果你在 Windows PowerShell，没有 `cp` 可用：

```powershell
Copy-Item .env.example .env
```

3. 安装依赖

```bash
pip install -r requirements.txt
```

## 2. 运行任务 A+B（索引入口）

最常用命令（按示例交易哈希自动定位区块）：

```bash
python -m src.demo \
  --tx-hash 0x916cad96dd5c219997638133512fd17fe7c1ce72b830157e4fd5323cf4f19946 \
  --event-slug will-there-be-another-us-government-shutdown-by-january-31 \
  --reset-db \
  --db ./data/demo_indexer.db \
  --output ./data/demo_output.json
```

输出是标准 JSON，顶层字段为 `stage2`，包含：

- `from_block` / `to_block`
- `inserted_trades`
- `market_slug` / `market_id`
- `sample_trades`
- `db_path`

按区块范围索引：

```bash
python -m src.demo \
  --from-block 66000000 \
  --to-block 66001000 \
  --event-slug will-there-be-another-us-government-shutdown-by-january-31 \
  --db ./data/indexer.db
```

## 3. 数据库检查

```bash
sqlite3 ./data/demo_indexer.db "SELECT COUNT(*) FROM events;"
sqlite3 ./data/demo_indexer.db "SELECT COUNT(*) FROM markets;"
sqlite3 ./data/demo_indexer.db "SELECT COUNT(*) FROM trades;"
sqlite3 ./data/demo_indexer.db "SELECT key,last_block FROM sync_state;"
```

查看市场样例：

```bash
sqlite3 ./data/demo_indexer.db "SELECT slug,condition_id,yes_token_id,no_token_id FROM markets LIMIT 5;"
```

## 4. 运行任务 C（API 服务）

启动 API：

```bash
python -m src.api.server --db ./data/demo_indexer.db --port 8000
```

另开一个终端测试：

```bash
curl http://127.0.0.1:8000/events/will-there-be-another-us-government-shutdown-by-january-31
curl http://127.0.0.1:8000/events/will-there-be-another-us-government-shutdown-by-january-31/markets
curl http://127.0.0.1:8000/markets/will-there-be-another-us-government-shutdown-by-january-31
curl "http://127.0.0.1:8000/markets/will-there-be-another-us-government-shutdown-by-january-31/trades?limit=10&cursor=0"
curl "http://127.0.0.1:8000/tokens/<token_id>/trades?limit=10"
```

## 5. 可用命令参数

`python -m src.demo --help`

- `--tx-hash`: 用交易哈希自动推断区块
- `--event-slug`: 指定 Gamma 事件 slug（建议必填）
- `--from-block --to-block`: 手动区块范围
- `--db`: 数据库路径
- `--output`: 输出 JSON 文件路径
- `--reset-db`: 运行前删除旧库

`python -m src.api.server --help`

- `--db`: SQLite 路径（必填）
- `--host`: 默认 `127.0.0.1`
- `--port`: 默认 `8000`

## 6. 常见问题

- `Missing dependency`：先执行 `pip install -r requirements.txt`
- `Unable to connect RPC`：检查 `.env` 里的 `RPC_URL` 是否可访问
- 查询为空：先确认索引命令执行成功且 `inserted_trades > 0`
