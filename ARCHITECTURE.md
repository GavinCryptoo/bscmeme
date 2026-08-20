# Architecture

> 当前 BSC Balanced Survivor 的项目地图。运行状态、是否 Live 及当前问题以 `PROJECT_STATE.md` 为准；策略规则以 `CURRENT_DECISIONS.md` 和有效配置为准。

## 运行入口与隔离

- `run_realtime.py`：统一 realtime runner，支持链、模式与 `high` / `balanced` profile。
- `run_bsc_balanced_paper.py`：BSC Balanced Paper 的显式入口，转入 `run_realtime.py --chain bsc --mode paper --strategy-profile balanced`。
- Balanced 使用 `data/bsc-balanced/`；Paper、GMGN Live、旧 Bitget Live 执行记录及 Dashboard 查询路径分离。
- `run_dashboard.py` 与 `run_bsc_balanced_live_dashboard.py` 只提供本地监控界面；页面存在不代表真实交易正在运行。

## 核心模块

| 模块 | 职责 |
| --- | --- |
| `adapters/binance_web3.py` | Binance Web3 Meme Rush 等只读信号和市场字段。 |
| `adapters/bsc_wss.py` / Pool、Venue 适配器 | BSC Pool/Venue 识别、Factory/市场 WSS、实时价格和 Flow 输入。 |
| `strategies/survivor_reversal.py` | Balanced Candidate、持仓、入场和退出状态机。 |
| `adapters/bsc_quote.py` | BSC 只读可执行报价与直接 Venue/DEX 诊断。 |
| `adapters/gmgn_openapi.py` | GMGN CLI 双向报价与显式 gated 的 BSC Live executor；独立订单 journal 防重复和重启恢复。 |
| `adapters/bitget_wallet.py` | Bitget 路由报价与 BSC Live executor，保留为非本次 GMGN Live 的既有路径。 |
| `adapters/universal_execution_router.py` | Capability Router / Provider race 的实现与验证层，尚未成为 realtime launcher 的默认执行注入。 |
| `storage/`、runtime/ledger | SQLite WAL runtime 状态、审计、健康指标；owner/main loop 单一写入。 |
| Dashboard 模块 | 独立只读连接查询当前 runtime DB。 |

## 关键数据流

```text
Binance Web3 discovery ─┐
BSC Factory / market WSS ├─> 标准化 Token / Venue / Flow 结果队列
链上只读 RPC / Quote ───┘                │
                                         v
                          Balanced 主循环（唯一 runtime DB writer）
                                         │
                   Candidate / ATH / Position / EXIT_INTENT 状态机
                         │                         │
                         v                         v
                 双向 executable quote        Exit quote / route retry
                         │                         │
                   Paper fill 或显式 Live executor ─> receipt / 余额对账
                                         │
                                         v
                          SQLite runtime DB ─> 本地只读 Dashboard
```

网络、RPC、WSS、指纹与报价工作应在 worker/bridge 完成；主循环只消费小型结果、执行状态转换和数据库写入，避免阻塞 Candidate 与 Position。

价格按职责分离：Binance Meme Rush/Token USD 仅提供 discovery reference；Flap 未迁移 Token 的规范策略价来自 Portal 实时成交价及 Quote Asset USD 换算，迁移后 Token 来自已验证 Pool/WSS。GMGN 只提供双向可执行报价与真实成交对账；策略规范价、报价价、实际 entry/exit 成交价分别保存，不能互相替代。

Live Position 的估值是独立的数据流：Venue/WSS 先写入 Position Mark，GMGN SELL quote 低频异步兜底；owner/main loop 持久化 mark、来源与新鲜度，Dashboard 只读该 mark 计算未实现盈亏。另有仅覆盖 OPEN Token 的异步钱包余额对账；外部余额变化在没有本机 pending/unknown SELL 时才收敛为外部全量/部分退出，外部成交价不可验证时保持 PnL 未知。

## 外部依赖与执行边界

- 只读输入：Binance Web3、BSC HTTP RPC、独立 Logs RPC、BSC WSS、Pancake/链上合约只读调用。
- 当前 BSC Balanced Live 路由/执行：GMGN CLI；Live entry 必须通过同 Provider 的双向报价，再经独立 swap、订单状态和余额对账。GMGN CLI 认证和自动化 gate 仅来自本地 CLI 配置和操作者环境。
- Bitget Wallet Order Mode；Universal Router 中的 Bitget、Velora、Kyber、LI.FI、0x 与协议原生适配保留为能力层，不应假定已经接入当前 GMGN runner。
- Paper 使用报价进行模拟；Live 只有 `--mode live` 加完整显式安全配置、授权和执行校验后才可能签名/广播。报价成功不等于成交成功。
- Live 下单、退出、pending 对账与 `EXIT_INTENT` 必须共用 nonce、去重、receipt/余额对账和钱包级执行锁；缺少可靠状态时 fail closed。
