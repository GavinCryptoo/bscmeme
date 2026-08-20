# CURRENT_DECISIONS.md

版本：0.4.2
状态：当前实现最高优先级；BSC Live 已获明确授权，Solana 保持只读与 Paper/Shadow

优先级顺序：

1. CURRENT_DECISIONS.md
2. MEME_STRATEGY_REBUILD_SPEC.md
3. 历史上下文

## 项目边界

- 项目：meme0801
- 当前范围链：Solana + BSC
- 第一阶段模式：Paper + Shadow；BSC 另有独立小额 Live 模式
- Dashboard：实现，默认 127.0.0.1:8788
- Telegram 默认关闭；开启后可用于 Paper/Shadow 通知与暂停/恢复新入场；BSC Live 仅允许通知、状态和暂停/恢复新入场
- Solana Live、Solana 钱包、Solana 私钥读取、Solana 签名、Solana 广播和 Solana 链上写入：不实现
- BSC Live 允许，但必须使用独立 Live 进程、数据库、审计日志、锁和配置；不得复用 Paper/Shadow 状态
- BSC Balanced Live 执行层采用 Capability Router：已验证且未迁移的 Flap 走 `FLAP_DIRECT`；FourMeme 只有官方 ABI、双向链上预览与 calldata 均验证后才允许 `FOURMEME_DIRECT`，否则 fail closed；迁移后/DEX Token 并行竞价 Bitget、Velora、KyberSwap、LI.FI 和已配置的 0x，按双向 Roundtrip 与扣除明确 Gas/Provider Fee 后的净到账选择；所有路径共用同一本地 BSC EOA、Nonce Manager、Receipt Reconciliation、EXIT_INTENT 和防重复机制。Binance Agentic Wallet 仅保留为 LEGACY 资产边界，不进入新交易路径；不得使用 Binance indicative price 作为成交价。Universal Benchmark 验收前真实交易保持停止
- BSC Live 启动时只允许启动后复合策略产生的新代币入场；启动首轮现有列表及 Live 数据库已见代币只观察、不买入，且不改变既有策略参数
- BSC Live 的交易执行必须 fail-closed：chainId=56、余额、nonce、decimals、allowance、实时报价、estimateGas、非零最低到账和 deadline 任一检查失败即拒绝发送
- 当前工作区按全新项目重建
- 不恢复历史 V2、V2.1、V2.2、V4 为可运行策略
- 只建立一个新的基线策略
- 当前实现阶段：BSC Live 最小实现与 Paper/Shadow 并行维护；Solana 不进入 Live

## 基线策略身份

strategy_name: sol_ultra_early_baseline
ruleset_name: ultra_early_minimal
ruleset_version: 0.1.2

每个候选、持仓、退出和影子结果必须记录：

- strategy_name
- ruleset_name
- ruleset_version
- config_version

同一 Mint + strategy_name + ruleset_version 只能存在一个活跃生命周期。

## 当前真实硬条件

token_age_min_sec: 5
token_age_max_sec: 120
unique_buyers_15s_min: 6
buy_sell_count_ratio_15s_min: 1.8
net_buy_15s > 0
require_two_non_negative_flow_windows: true
require_executable_buy_route: true
require_executable_sell_route: true
creator_confirmed_sold_at_entry: false
max_buy_price_impact_pct: 10
max_immediate_exit_impact_pct: 15
one_trade_per_mint: true
same_name_cooldown_sec: 900
max_open_positions: 2

以下指标当前只记录、分组和影子评估，不得拒绝 Paper 交易：holders、市值、Token 单价、固定美元流动性、Bundler 比例、Top holders 集中度、创建者历史、钱包资金来源、社交数据、最大钱包潜在抛压、同名代币历史表现。

## Solana Paper/Shadow 退出规则

take_profit_1_pct: 20
take_profit_1_sell_pct: 50（卖出当时剩余仓位的一半）
take_profit_2_pct: 30
take_profit_2_sell_pct: 50（再次卖出当时剩余仓位的一半）
tp2_breakeven_exit_enabled: true（TP2 后价格回到买入价即清仓）
stop_loss_trigger_pct: -30
stop_loss_sell_pct: 100
max_hold_sec: 600
moving_stop_enabled: true（仅 TP2 后成本价保护）
cliff_guard_enabled: false
partial_take_profit_enabled: true
weak_demand_exit_enabled: false

## Shadow 规则

Shadow 建立独立虚拟生命周期和持仓，不修改 Paper 仓位、PnL、最大持仓或熔断。

- shadow_defense_v1：收益率 <= -8%、最近短窗口净流量为负、独立买家增长停止时触发。
- shadow_creator_sell：高置信识别创建者或明确关联地址卖出时触发。
- shadow_time_exit：持仓 >=120s、MFE <5%、买家增长和净流量同时放缓时触发。
- Solana Shadow 与 Solana Paper 使用本节同一组 TP1、TP2、TP2 后成本价保护、
  止损和最长持仓规则，同时保留以下结构性提前退出规则。
- BSC Shadow 继续继承 BSC Paper 的独立 `take_profit`、`stop_loss` 和
  `max_hold_timeout` 规则；不使用 Solana 的分批止盈配置。
- 触发后记录 5s、15s、30s、60s、120s 收益、是否达到 Paper TP、避免损失和错过利润。

## Paper 参数

initial_virtual_balance_sol: 1
position_size_sol: 0.001
max_open_positions: 2
assume_position_can_lose_100_percent: true
daily_full_loss_sol_limit: 0.01
pause_new_entries_after_large_losses: 5
large_loss_threshold_pct: -40

暂停新开仓后，已有持仓继续监控和退出。

## 数据源与报价

- Fixture/Replay 仍是默认数据源和确定性测试事实来源；`DATA_SOURCE=fixture` 为默认值。
- 当前允许 Binance Web3 官方只读接口：Solana `CT_501` 的 Meme Rush、Smart Money、Token Dynamic 和 Kline，以及 BSC `56` 的 Meme Rush；允许 Solana 主网 RPC/WSS 只读订阅、Pump/PumpSwap 状态读取，以及 Jupiter 当前官方 Quote GET。
- Binance Web3 当前冻结为公开 `auth_mode=none`；适配器不读取或发送钱包、Jupiter、API Key、Cookie、Session 或签名材料。
- Binance Smart Money 只作为 Shadow 观察信号，`trigger_entry=false`，不得触发 Paper 入场或退出。
- Meme Rush 的 `createTime`/`migrateTime` 单位未被官方参考明确为毫秒，因此在可证实前保持 unavailable。
- Binance 不能提供的 15 秒独立买家、15 秒买卖比、15 秒净买、price impact 和可靠创建者卖出确认，不得用其它字段填补；这些字段保持 unavailable。BSC 没有 Quote Provider 时，`buy_quote_unavailable`/`sell_quote_unavailable` 不再单独阻断 BSC Paper/Shadow。
- Jupiter 只允许 Quote endpoint；没有 `/swap`、`/swap-instructions`、构建交易、签名或发送交易。`priceImpactPct` 的单位未在当前官方接口契约中冻结，默认保持 unavailable，不得自行换算。
- Solana Paper/Shadow 继续必须使用 Jupiter Quote；无有效 Jupiter 可执行报价不得模拟成交。
- BSC read-only + Paper/Shadow is allowed. BSC Live remains disabled. BSC Paper/Shadow 入场必须同时取得非零的链上只读买入和即时卖出报价；bonding curve 使用协议真实状态和计算，已迁移代币使用 PancakeSwap Router。缺少报价、route、`quoted_at` 或 price impact 时拒绝，不使用 Binance 指示价开仓。持仓和平仓使用最新链上卖出报价计算 PnL；无有效卖出报价的退出记录为 `pnl_status=unknown`，不得伪造价格。记录 `pricing_mode=bsc_executable_quote`、`executable_quote=true`、`quote_source`、route、输入输出和时间。Binance 仅作 Dashboard 对照，标记 `pricing_mode=binance_indicative_reference`、`executable_quote=false`，不参与 PnL；旧 BSC 指示价记录标记 `legacy_binance_indicative`，默认从正式统计排除。

## 报价、成交和成本

- Paper 和 Shadow 不得使用中间价或页面展示价冒充成交价。
- 买入按 position_size_sol 获取可执行买入报价，记录输出数量、路由费、price impact 和 quote age。
- 卖出按当前实际虚拟持仓数量重新获取可执行卖出报价，使用真实可执行输出计算收益。
- 分别记录 gross_pnl、route_fee、estimated_network_fee、estimated_priority_fee 和 net_pnl_estimated。
- 如果网络费不能可靠获取，estimated_network_fee 可以为空或配置值，并且必须标记 net_pnl_is_estimated=true。

## Dashboard 最低范围

第一版只读，不提供启动 Live、钱包、签名或广播入口，至少包含：

1. 运行状态
2. 信号
3. 候选与过滤原因
4. Paper 当前持仓
5. Shadow 当前持仓
6. 历史交易
7. 退出执行详情
8. 影子规则对比
9. 延迟审计
10. 数据源健康状态
11. 策略版本和配置版本
12. 异常事件

## Dashboard 与持久化

- 默认绑定 127.0.0.1:8788。
- Dashboard 默认本地只读；允许的写操作仅是 Paper/Shadow 新入场暂停/恢复，不提供 Live、钱包、签名或广播入口。
- Paper 和 Shadow 必须明显分区，列表默认最新在上。
- SQLite WAL 是运行事实来源。
- JSONL 是不可变审计日志；CSV 仅导出；Parquet 延后。
- latest_status.json 仅为 Dashboard 快照，不是事实账本。
- Solana Paper：data/solana/paper/runtime.db
- Solana Shadow：data/solana/shadow/runtime.db
- BSC Paper：data/bsc/paper/runtime.db
- BSC Shadow：data/bsc/shadow/runtime.db
- 每条链的 Paper/Shadow runner 不得写同一数据库或状态目录；Solana 与 BSC 必须隔离。

## 观察与历史数据

- 20–30 个完整生命周期：工程和状态机检查。
- 100 个完整生命周期：初步评估影子过滤和影子退出。
- 300 个完整生命周期：评估策略期望值和主要参数调整。
- 最短连续观察时间：7 天。
- 第一阶段不导入历史交易和旧状态，但预留 Replay Importer 接口。
- Replay Importer 未来只允许导入经过校验的 signals、quotes、pool events、trades 和 positions。

## 旧进程检查

编码前允许只读检查 ps、pgrep、screen -ls、launchctl list 和 lsof。

- 只报告，不终止任何进程。
- 不修改 launchd。
- 不删除文件。
- 不读取或输出密钥。
- 如发现疑似旧交易进程，报告完整命令、PID、父进程和工作目录，等待确认。

## 强制安全开关

必须存在并强制验证：

PAPER_ONLY=true（Paper/Shadow 默认值；BSC Live 必须显式设置 LIVE_TRADING=true）
LIVE_TRADING=false（BSC Live 启动时必须显式为 true）
WALLET_ENABLED=false
SIGNING_ENABLED=false
BROADCAST_ENABLED=false（Paper/Shadow 默认值；BSC Live 执行路径由 LIVE_TRADING+BSC_LIVE_ENABLED 显式开启）
TELEGRAM_ENABLED=false

BSC_LIVE_ENABLED=false

Telegram 默认保持 `TELEGRAM_ENABLED=false`；BSC Live 开启 Telegram 后只接受状态查看和暂停/恢复新开仓，不接受下单、卖出、金额、滑点、钱包或进程控制。

Paper/Shadow 代码中不得存在可到达的签名和广播实现。BSC Live 仅在显式 Live 配置完整时创建本地签名账户；私钥只从本地 `.env` 读取，不写入日志、数据库、审计文件或提交。

## 当前授权范围

允许：实现已审计的 Binance Web3（Solana 与 BSC Meme Rush）、Solana RPC/WSS、Pump/PumpSwap 和 Jupiter Quote 只读适配；实现实时 Paper/Shadow 协调器、Bitget Wallet Order Mode BSC Live 独立持仓生命周期、重启恢复、Dashboard、健康、锁、导出、Telegram 通知和仅新开仓暂停/恢复控制；执行有限、可终止的只读探测。BSC Live 代码必须先完成测试，并且仅在用户明确授权后启动。

禁止：Solana 写链、Jupiter 执行接口、Solana 交易构建、Solana 钱包/私钥/签名/广播、BSC 未确认字段或 endpoint、Paper/Shadow 触发 Live、Live 无限重试、`amountOutMin=0`、绕过 chainId/余额/报价/到账核对。

BSC read-only + Paper/Shadow + explicitly configured isolated BSC Live is allowed. Solana Live remains disabled.

## BSC Paper/Shadow 数据回测覆盖（2026-08-02）

基于当前 BSC Paper 已平仓的 92 个独立 Mint 做最小参数调整，仅作用于 BSC
Paper/Shadow，不改变 Solana 基线、不改变市值、流动性、观察期和 Binance
指示价要求：

- strategy_name: bsc_binance_indicative
- ruleset_name: ultra_early_selective_bsc
- ruleset_version: 0.1.5
- config_version: 0.1.5
- min_holders: 100
- min_holders_inclusive: true
- stop_loss_trigger_pct: -10
- take_profit_pct: 10（保持）
- max_hold_sec: 600（保持）

BSC 新增入场观察门槛：

- 观察期为 60 秒。
- 首次发现时记录持币地址数；观察结束时当前持币地址数必须不低于首次值。
- 首次或观察结束的持币地址数缺失时拒绝入场，不填默认值。
- 该门槛只作用于 BSC Paper/Shadow；Solana 观察期、Jupiter Quote 和原有规则不变。

BSC Shadow 新增影子提前退出观察：

- 开仓后持币地址数较入场下降超过 10%，记录一次 `shadow_holders_drop_over_10pct`。
- 开仓后流动性较入场下降超过 15%，记录一次 `shadow_liquidity_drop_over_15pct`。
- 以上规则只作用于 BSC Shadow 的结构性提前退出；BSC Shadow 同时按 BSC Paper
  执行止盈、止损和最长持仓退出，不改变 BSC Paper 的阈值、全部退出比例或 Solana
  Shadow 规则。
- 当前地址数或流动性不可用时不触发该规则；不填默认值。
- 每次 BSC Shadow 退出评估记录入场值、当前值、降幅和是否触发，持续用于比较 `-50%` 以上极端亏损的减少效果；缺少后续窗口时标记为 `pending_follow_up`。

BSC Shadow 观察期新增流动性门槛：

- 观察期结束时，当前 liquidity 必须不低于首次发现时的 liquidity；如果已经下降，跳过 Shadow 入场并记录 `liquidity_below_first_discovery_after_observation`。
- 该门槛只作用于 BSC Shadow；BSC Paper、Solana、其他市值/流动性/持币地址数/观察期和退出规则保持不变。
- 首次或观察结束的 liquidity 不可用时，不填默认值，按 `observation_liquidity_unavailable` 拒绝 Shadow 入场。

## BSC Paper/Shadow 资本统计会话（2026-08-03）

- 从下一笔 BSC Paper/Shadow 交易开始重新计算当前 Dashboard 的盈亏额与盈亏率。
- 初始虚拟资金：`0.1 BNB`。
- 单笔虚拟仓位：`0.01 BNB`。
- 历史 SQLite 账本保留，不删除、不重算、不篡改；历史记录仍可通过原始数据库审计。
- 本次资本统计会话只作用于 BSC Paper/Shadow；Solana 继续使用 `1 SOL` 初始虚拟资金与 `0.001 SOL` 单笔仓位。

该调整只用于 Paper/Shadow 验证；不改变 BSC Live 使用独立 Bitget Wallet Order Mode 执行层
报价、独立状态和显式小额配置的边界。

## 历史阶段记录（不覆盖当前 BSC Live 授权）

以下阶段 0 约束只记录早期重建过程，不覆盖本版本已明确的 BSC Live 最小实现授权。

允许：只读残留进程检查、Git 初始化、文档、目录、pyproject.toml、.gitignore、.env.example、最小 Python package、安全 schema、fail-closed 测试、空 adapter Protocol、基础 domain model、SQLite migration 框架、Fixture/Replay 测试骨架。

禁止：真实 RPC/WSS、Jupiter 接入、真实信号采集、长期 runner、钱包、签名、广播、Live、完整策略交易循环。

## 阶段 0 验收

阶段 0 完成后必须报告：

1. 新建和修改的文件清单
2. 项目目录树
3. 安全边界实现方式
4. 所有测试命令
5. 测试完整输出
6. 编译或类型检查结果
7. Git 状态
8. 未完成事项
9. 下一阶段建议
10. 明确确认没有启动长期进程
