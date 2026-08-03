# meme0801

Solana + BSC `Paper + Shadow` 策略系统，并提供一条完全隔离、显式配置的 BSC 小额 Live 路径。Solana 仍不进入 Live；BSC Live 使用 PancakeSwap 官方 Smart Router 实时报价和 calldata，不使用 Binance indicative price 作为成交价。

## 当前边界

- 默认 `DATA_SOURCE=fixture`；支持 `fixture`、`replay`、`binance_web3`。
- Binance Web3 启用 Solana `CT_501`，以及 BSC `56` 的 Meme Rush 只读候选。
- Meme Rush 可作为只读候选观察源；Smart Money 固定为 Shadow-only，不能触发入场。
- Token Dynamic 和 Kline 是指示性市场数据，不是可执行报价。
- Solana 只允许官方 Jupiter Quote GET；BSC Paper/Shadow 在没有 Quote Provider 时使用 Binance Web3 当前价格进行指示价模拟，并明确标记不可执行、PnL 为估算值；BSC Live 使用 PancakeSwap Smart Router 的链上报价。Dashboard 和长期 runner 需显式启动。

Paper/Shadow 安全默认保持：`PAPER_ONLY=true`、`LIVE_TRADING=false`、`WALLET_ENABLED=false`、`SIGNING_ENABLED=false`、`BROADCAST_ENABLED=false`、`TELEGRAM_ENABLED=false`。BSC Live 另需显式设置 `LIVE_TRADING=true`、`BSC_LIVE_ENABLED=true`、`BSC_TRADE_AMOUNT_BNB`、`BSC_MAX_POSITIONS`、`BSC_SLIPPAGE_BPS`，以及本地 `.env` 中的 `BSC_PRIVATE_KEY` 和 `BSC_RPC_URL`。

## 本地验证

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_*.py' -v
PYTHONPATH=src python3 -m compileall -q src
PYTHONPATH=src python3 run_source_probe.py --endpoint meme --once
# Explicit Paper/Shadow runner; DATA_SOURCE=fixture remains the safe default.
DATA_SOURCE=binance_web3 PYTHONPATH=src python3 run_realtime.py --mode both --once
PYTHONPATH=src python3 run_dashboard.py
```

BSC Live 仅在确认 `.env` 配置、依赖和首次交易限制后显式启动：

```bash
DATA_SOURCE=binance_web3 PYTHONPATH=src python3 run_realtime.py --chain bsc --mode live
```

最后一个命令是有限、只读、可终止的探测；它不创建数据库、不启动 Paper/Shadow、不写链。默认没有真实 API 凭据要求；公开 Binance endpoint 的鉴权模式冻结为 `none`。

## 主要目录

```text
src/meme_system/domain/                 基础领域模型与策略生命周期
src/meme_system/adapters/               Fixture/Replay 与 Binance Web3 适配器
src/meme_system/config/                 安全与数据源配置
src/meme_system/storage/                SQLite WAL 和只读 ledger 查询
tests/fixtures/                         确定性 fixture；Binance fixture 标记来源
run_source_probe.py                     有界的 Binance Web3 只读探测
docs/                                   决策、字段覆盖、审计和验收文档
```

阶段边界和字段可用性以 [`CURRENT_DECISIONS.md`](CURRENT_DECISIONS.md) 和 [`docs/BINANCE_WEB3_API_AUDIT.md`](docs/BINANCE_WEB3_API_AUDIT.md) 为准。

Paper/Shadow runner 只记录 Binance/Jupiter 只读结果和虚拟生命周期；BSC 指示价模拟明确记录 `pricing_mode=binance_indicative`、`executable_quote=false`、`net_pnl_is_estimated=true`。BSC Live 的数据库、审计日志、控制文件和锁目录均在 `data/bsc/live/` 下；Live 不会由 Paper/Shadow、Dashboard 或 Telegram 启动。显式启动后，Telegram 只提供买入/卖出/失败通知、状态查看和暂停/恢复新开仓，不提供下单、卖出、钱包或进程控制。
