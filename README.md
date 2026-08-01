# meme0801

Solana `Paper + Shadow` 策略系统。当前阶段是 Gate A：增加 Binance Web3、Solana RPC/WSS、Pump/PumpSwap 和 Jupiter Quote 的只读适配，并提供实时虚拟生命周期与本地 Dashboard；不包含任何钱包、签名、广播、Live 或链上写入能力。

## 当前边界

- 默认 `DATA_SOURCE=fixture`；支持 `fixture`、`replay`、`binance_web3`。
- Binance Web3 只启用 Solana `CT_501`。
- Meme Rush 可作为只读候选观察源；Smart Money 固定为 Shadow-only，不能触发入场。
- Token Dynamic 和 Kline 是指示性市场数据，不是可执行报价。
- Jupiter 只允许官方 Quote GET；Dashboard 和长期 runner 需显式启动，BSC 只保留未来 adapter 接口。

安全开关必须保持：`PAPER_ONLY=true`、`LIVE_TRADING=false`、`WALLET_ENABLED=false`、`SIGNING_ENABLED=false`、`BROADCAST_ENABLED=false`、`TELEGRAM_ENABLED=false`。

## 本地验证

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_*.py' -v
PYTHONPATH=src python3 -m compileall -q src
PYTHONPATH=src python3 run_source_probe.py --endpoint meme --once
# Explicit Gate A runner; DATA_SOURCE=fixture remains the safe default.
DATA_SOURCE=binance_web3 PYTHONPATH=src python3 run_realtime.py --mode both --once
PYTHONPATH=src python3 run_dashboard.py
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

Gate A 的实时 runner 只会记录 Binance/Jupiter 只读结果和虚拟生命周期；缺少 Binance 硬字段、Jupiter API key、Token decimals 或未确认的 price impact 单位时，候选保持拒绝，不会用估算值成交。Gate A 完成后不自动创建或进入 Live。
