# ARCHITECTURE.md

## 阶段 2 目标

阶段 2 完成标准化 Fixture/Replay 输入上的策略评估、Paper/Shadow 完整虚拟生命周期、MFE/MAE、SQLite 恢复和只读查询契约。真实数据源、Dashboard HTTP 服务和所有链上执行能力延后。

## 目标分层

SignalSource / Replay
        |
        v
MarketData / QuoteProvider
        |
        v
Domain Models -> Strategy Registry -> Candidate Ledger
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
- 阶段 2 不包含签名、广播、钱包或链上写入模块。
- BINANCE_WEB3_API_AUDIT.md 缺失时，不实现或猜测 Binance API 适配器。
- 真实 RPC/WSS/Jupiter 只读适配器必须等接口调查和确认。

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
