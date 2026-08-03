# ARCHITECTURE.md

## 阶段 4 Gate A 目标

阶段 4 Gate A 在已完成的 Fixture/Replay、Paper/Shadow 生命周期和 Binance Web3 官方只读适配之上增加实时只读数据、Dashboard 和运维闭环。真实执行能力仍延后。

## 目标分层

Fixture / Replay / Binance Web3 SignalSource
        |
        v
Binance Dynamic + Solana RPC/WSS + Pump/PumpSwap state + Jupiter Quote GET
        |
        v
Domain Models -> Strategy Registry -> Candidate Ledger
        |
    +--> Candidate Evaluation
             |
             +--> Paper Lifecycle -> Paper runtime.db
             |
             +--> Shadow Lifecycle -> Shadow runtime.db
        |
        v
SQLite WAL / JSONL Audit / CSV-Parquet Export / Dashboard / Telegram safe controls

## 安全边界

- 当前仅允许 Paper + Shadow。
- SafetyConfig.validate() 在应用启动前拒绝任何危险开关。
- 阶段 4 不包含签名、广播、钱包或链上写入模块。
- Binance Web3 client 只允许官方公开只读 endpoint，当前鉴权模式为 `none`。
- Smart Money adapter 是 Shadow-only，不能改变 Paper 入口或退出。
- Solana 仅使用 Jupiter Quote GET；Pump/PumpSwap 仅状态读取；BSC Meme Rush 只负责发现，Paper/Shadow 使用实际 bonding curve 或 PancakeSwap Router 的只读可执行报价，Binance 仅作 Dashboard 参考，BSC Live 仍按独立安全边界处理。

## 数据隔离

Solana Paper：data/solana/paper/runtime.db
Solana Shadow：data/solana/shadow/runtime.db
BSC Paper：data/bsc/paper/runtime.db
BSC Shadow：data/bsc/shadow/runtime.db

两种模式使用独立数据库和独立生命周期 ID。所有候选、持仓、退出和影子结果保留策略身份与配置版本。

## 未来 Protocol 边界

- SignalSource：产生标准化信号。
- MarketDataAdapter：提供只读市场快照。
- QuoteProvider：提供可执行报价，不等于执行。
- BscAdapterProtocol：定义 BSC 只读候选边界，不包含钱包、签名、广播或 Live。
- LedgerQueries：为 Dashboard 提供只读、最新优先的数据结构；Dashboard 的唯一写操作是安全暂停标志。

## Binance Web3 边界

- `BinanceWeb3SignalSource`：Meme Rush `rankType` 10/20/30，支持 Solana `CT_501` 和 BSC `56`。
- `BinanceWeb3SmartMoneyAdapter`：Smart Money 观察记录，固定 `trigger_entry=false`。
- `BinanceWeb3MarketDataAdapter`：Token Dynamic 指示性市场字段，不声称可执行报价。
- `BinanceWeb3KlineAdapter`：Kline candles，只读历史/短窗数据。
- `run_source_probe.py`：一次请求或显式上限内的有限探测，不写数据库。

## Gate A 运行组件

- `run_realtime.py` / `run_paper.py` / `run_shadow.py`：显式启动、可停止、单实例锁。
- `run_dashboard.py`：默认 `127.0.0.1:8788`；GET 查询与仅 Paper/Shadow 暂停控制。
- `src/meme_system/realtime.py`：一周期协调与确定性引擎复用。
- `src/meme_system/dashboard_server.py`：最新优先 LedgerQueries API。
- `src/meme_system/telegram_control.py`：默认关闭的 Paper/Shadow 控制。

## 禁止自动进入 Gate B

Gate A 完成后停止；不创建 Live Engine、Wallet、PrivateKey、Keypair、Signer、交易构建或广播路径。
