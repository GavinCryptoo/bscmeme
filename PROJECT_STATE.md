# PROJECT_STATE

> 当前有效状态快照，不是修改流水账。新任务先读 `AGENTS.md`，再读本文件；发生运行、执行或配置变化时应重新核验。

## Current Status

- 项目重点策略：BSC Balanced Survivor；入口为 `run_bsc_balanced_paper.py` / `run_realtime.py`，数据根为 `data/bsc-balanced/`。
- 2026-08-16：BSC Balanced GMGN Live 使用独立 `data/bsc-balanced/gmgn-live/` runtime；Factory V2/V3 WSS、Binance discovery、主循环和 GMGN 路由 preflight 应以当前 PID/健康文件核验。Dashboard 进程与真实交易状态无等价关系。
- BSC Balanced GMGN Live 的 Discovery 同时接收 Binance Meme Rush 与 OKX Wallet BSC Signal（Smart Money/KOL/Whales）；OKX 仅每 30 秒发现和记录信号元数据，失败仅降级该源，绝不承担价格、报价或执行职责。
- BSC Balanced Paper 仍独立运行，其 launcher 显式覆盖为 `LIVE_TRADING=false`、`BSC_LIVE_ENABLED=false`、`WALLET_ENABLED=false`、`SIGNING_ENABLED=false`、`BROADCAST_ENABLED=false`。运行模式必须以 PID 的有效环境和目标 runtime DB 为准。
- 当前 BSC Balanced Live 执行/路由：`GMGN_CLI`；有效交易金额与仓位上限必须读取当前 PID 的 self-check，而不能采用本文档中的历史数字。GMGN CLI 负责双向报价、swap 提交、订单状态和余额读取，异步 bridge 负责隔离主循环。真实 BUY 必须先在 owner/main loop 持久化容量 Reservation，`OPEN + active BUY reservation` 才是新入场容量；超时/未知订单在对账明确前继续占位。Bitget 与直接 BSC executor 保留为其他路径的已有代码，未作为本次 GMGN Live 的执行器。
- Live 价格职责已分离：Binance Meme Rush/Token USD 只保存为发现与诊断参考；策略规范价只能来自 Flap Portal（含 Quote Asset USD 转换）或迁移后 Pool/WSS。GMGN 买卖报价和已确认成交价分别记录，不可与策略规范价混用。OPEN 仓位另行持久化 Position Mark：优先 Venue/WSS，低频 GMGN SELL 可执行报价兜底；Dashboard 的持仓估值只读该 Mark，不再借用 Candidate 价格。
- Live 只对当前 OPEN Token 做钱包余额对账。无本机 SELL pending/unknown 时，真实余额归零会收为 `MANUAL_EXTERNAL_EXIT`，部分减少则保留 OPEN 并标为外部部分退出；未知外部成交价格时不伪造 exit price 或 PnL。
- 2026-08-16：GMGN Live 从启用后新确认的真实 BUY 起，保存不可变 `ENTRY_SNAPSHOT`，并以 Position Mark 维护 1/3/5/15/30 分钟收益、MFE/MAE、TP 命中及最终真实成交结果；累计 20 笔已结束样本后只生成一次探索性描述报告，不参与任何 Entry/Exit 决策。

## Working

- BSC Balanced Paper 独立运行、SQLite runtime 隔离、Binance Web3 discovery、BSC WSS/Factory、Venue/Pool registry、报价桥和本地 Dashboard 代码均已存在。
- BSC 路由和退出设计要求双向报价、异步 bridge、`EXIT_INTENT`、状态对账与单一 SQLite writer；GMGN Live 与 Paper/Bitget Live 数据路径独立。
- 近期基准代码覆盖多类 BSC Token 的 Provider/roundtrip 评估；其结果只可用于路由决策，不能替代真实成交或余额对账。
- 2026-08-14：GMGN CLI 在 `NODE_USE_ENV_PROXY=1` 下完成过一次受限 BSC 往返：`0.001 BNB` 买入指定 Token、订单 confirmed、按钱包真实余额 100% 卖出、订单 confirmed，最终 Token 余额为零；随后已接入 Balanced Live runtime 并完成只读 preflight。真实策略信号的自动成交仍须以订单、receipt 和余额对账为准。

## Known Issues

- `CURRENT_DECISIONS.md` 中仍有早期 Capability Router/Bitget 描述，与当前 GMGN Live 注入路径不完全一致；若未来继续扩展 Provider，必须先统一决策文档、实际 runner 和可执行 Provider。
- GMGN executor 已有独立订单 journal，用于在超时/重启后恢复待对账订单并阻止同 token/side 的重复提交。GMGN 对不同 Venue 的覆盖率和长期故障恢复仍需继续以实际订单、receipt 和余额对账验证。任何 GMGN runtime 必须显式继承 `NODE_USE_ENV_PROXY=1`，否则网络可能在 TLS 握手前失败。
- 部分旧健康文件和旧说明文档可能滞后；必须以当前 PID、有效环境、短日志和实时健康推进复核，不得直接采用旧 Dashboard 或历史 DB 的结论。

## Safety-Critical Facts

- 真实执行能力位于 BSC Live 入口、Bitget executor、直接 BSC executor 和可能执行 swap 的外部 CLI 路径。未经用户就当前具体操作的明确授权，不得启动 Live、签名、approve、广播、swap、充值、提现或停止现有 runtime。
- `.env`、本地钱包配置、GMGN/Provider 配置和认证存储均为敏感文件；不得输出、提交、复制或写入本项目 Markdown。
- GMGN CLI 的 `swap` 有独立代码级人工交易保护：headless 自动提交必须由操作者在其自身环境显式启用 `GMGN_ALLOW_AUTOMATED_TRADES=1` 并传入 `--yes`。Agent 不得自行设置或绕过该保护。
- GMGN Live 必须使用单独 `gmgn-live` DB、控制文件、审计日志、锁和 executor journal；不得复用旧 Bitget Live runtime 或其持仓状态。
- `PAPER_ONLY=true` 或 `.env` 中的 Live 开关本身都不足以证明运行模式；始终先检查实际进程的有效环境和目标 runtime DB。
- Dashboard 只应使用独立只读连接；worker 不得写 runtime DB，主循环/owner 是唯一写入者。

## Current Priority

1. 观察 GMGN Live 的首批策略订单：双向报价、订单确认、receipt/余额对账、退出与订单 journal 恢复。
2. 如继续扩展其它 Provider，先统一 `CURRENT_DECISIONS.md`、实际 runner 和执行器选择，且不得影响已隔离的 GMGN Live。
