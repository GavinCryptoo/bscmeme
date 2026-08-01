# ARCHITECTURE.md

## 阶段 3 目标

阶段 3 在已完成的 Fixture/Replay、Paper/Shadow 生命周期之上增加 Binance Web3 官方只读适配。真实执行能力、Dashboard HTTP 服务和其它链数据源仍延后。

## 目标分层

Fixture / Replay / Binance Web3 SignalSource
        |
        v
MarketData / QuoteProvider
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
JSONL Audit / CSV Export / Dashboard Read Model

## 安全边界

- 当前仅允许 Paper + Shadow。
- SafetyConfig.validate() 在应用启动前拒绝任何危险开关。
- 阶段 3 不包含签名、广播、钱包或链上写入模块。
- Binance Web3 client 只允许官方公开只读 endpoint，当前鉴权模式为 `none`。
- Smart Money adapter 是 Shadow-only，不能改变 Paper 入口或退出。
- 真实 RPC/WSS、Jupiter、Pump/PumpSwap 和 BSC 适配器不属于本阶段。

## 数据隔离

Paper：data/solana/paper/runtime.db
Shadow：data/solana/shadow/runtime.db

两种模式使用独立数据库和独立生命周期 ID。所有候选、持仓、退出和影子结果保留策略身份与配置版本。

## 未来 Protocol 边界

- SignalSource：产生标准化信号。
- MarketDataAdapter：提供只读市场快照。
- QuoteProvider：提供可执行报价，不等于执行。
- BscAdapterProtocol：仅定义未来链适配边界，不连接 BSC。
- LedgerQueries：为未来 Dashboard 提供只读、最新优先的数据结构，不启动 HTTP 服务。

## Binance Web3 边界

- `BinanceWeb3SignalSource`：Meme Rush `rankType` 10/20/30，Solana `CT_501`。
- `BinanceWeb3SmartMoneyAdapter`：Smart Money 观察记录，固定 `trigger_entry=false`。
- `BinanceWeb3MarketDataAdapter`：Token Dynamic 指示性市场字段，不声称可执行报价。
- `BinanceWeb3KlineAdapter`：Kline candles，只读历史/短窗数据。
- `run_source_probe.py`：一次请求或显式上限内的有限探测，不写数据库。
