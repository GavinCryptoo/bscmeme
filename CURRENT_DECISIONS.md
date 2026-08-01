# CURRENT_DECISIONS.md

版本：0.2.0
状态：冻结，作为当前实现最高优先级

优先级顺序：

1. CURRENT_DECISIONS.md
2. MEME_STRATEGY_REBUILD_SPEC.md
3. 历史上下文

## 项目边界

- 项目：meme0801
- 第一阶段链：Solana
- 第一阶段模式：Paper + Shadow
- Dashboard：实现，默认 127.0.0.1:8788
- Telegram：Gate A 默认关闭；开启后仅允许 Paper/Shadow 通知与暂停/恢复新入场
- Live、钱包、私钥读取、签名、广播、链上写入：不实现
- BSC：本阶段不实现，只保留最小 Protocol 接口
- 当前工作区按全新项目重建
- 不恢复历史 V2、V2.1、V2.2、V4 为可运行策略
- 只建立一个新的基线策略
- 当前实现阶段为阶段 4 Gate A：实时只读数据源、Paper/Shadow、Dashboard 与运行运维
- Gate A 完成后必须停止，等待单独的 Gate B Live 授权

## 基线策略身份

strategy_name: sol_ultra_early_baseline
ruleset_name: ultra_early_minimal
ruleset_version: 0.1.0

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
max_buy_price_impact_pct: 3
max_immediate_exit_impact_pct: 8
one_trade_per_mint: true
same_name_cooldown_sec: 900
max_open_positions: 2

以下指标当前只记录、分组和影子评估，不得拒绝 Paper 交易：holders、市值、Token 单价、固定美元流动性、Bundler 比例、Top holders 集中度、创建者历史、钱包资金来源、社交数据、最大钱包潜在抛压、同名代币历史表现。

## Paper 退出规则

take_profit_pct: 10
take_profit_sell_pct: 100
stop_loss_trigger_pct: -20
stop_loss_sell_pct: 100
max_hold_sec: 600
moving_stop_enabled: false
cliff_guard_enabled: false
partial_take_profit_enabled: false
weak_demand_exit_enabled: false

## Shadow 规则

Shadow 建立独立虚拟生命周期和持仓，不修改 Paper 仓位、PnL、最大持仓或熔断。

- shadow_defense_v1：收益率 <= -8%、最近短窗口净流量为负、独立买家增长停止时触发。
- shadow_creator_sell：高置信识别创建者或明确关联地址卖出时触发。
- shadow_time_exit：持仓 >=120s、MFE <5%、买家增长和净流量同时放缓时触发。
- 触发后记录 5s、15s、30s、60s、120s 收益、是否达到 Paper TP、避免损失和错过利润。

## Paper 参数

initial_virtual_balance_sol: 10
position_size_sol: 0.001
max_open_positions: 2
assume_position_can_lose_100_percent: true
daily_full_loss_units_limit: 5
pause_new_entries_after_large_losses: 3
large_loss_threshold_pct: -40

暂停新开仓后，已有持仓继续监控和退出。

## 数据源与报价

- Fixture/Replay 仍是默认数据源和确定性测试事实来源；`DATA_SOURCE=fixture` 为默认值。
- 阶段 4 Gate A 允许 Binance Web3 官方只读接口：Solana `CT_501` 的 Meme Rush、Smart Money、Token Dynamic 和 Kline；允许 Solana 主网 RPC/WSS 只读订阅、Pump/PumpSwap 状态读取，以及 Jupiter 当前官方 Quote GET。
- Binance Web3 当前冻结为公开 `auth_mode=none`；适配器不读取或发送钱包、Jupiter、API Key、Cookie、Session 或签名材料。
- Binance Smart Money 只作为 Shadow 观察信号，`trigger_entry=false`，不得触发 Paper 入场或退出。
- Meme Rush 的 `createTime`/`migrateTime` 单位未被官方参考明确为毫秒，因此在可证实前保持 unavailable。
- Binance 不能提供的 15 秒独立买家、15 秒买卖比、15 秒净买、可执行买卖路由、price impact 和可靠创建者卖出确认，不得用其它字段填补，也不得触发 Paper。
- Jupiter 只允许 Quote endpoint；没有 `/swap`、`/swap-instructions`、构建交易、签名或发送交易。`priceImpactPct` 的单位未在当前官方接口契约中冻结，默认保持 unavailable，不得自行换算。
- 无有效可执行报价不得模拟成交；Token Dynamic、Kline、Pump 曲线指示价不得冒充 Jupiter 可执行报价。

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
- Paper：data/solana/paper/runtime.db
- Shadow：data/solana/shadow/runtime.db
- 两个 runner 不得写同一数据库或状态目录。

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

PAPER_ONLY=true
LIVE_TRADING=false
WALLET_ENABLED=false
SIGNING_ENABLED=false
BROADCAST_ENABLED=false
TELEGRAM_ENABLED=false

Gate A 默认保持 `TELEGRAM_ENABLED=false`；如需开启，只能在以上 Paper/Shadow 执行安全开关不变时用于通知和暂停/恢复新入场。

MVP 代码中不得存在可到达的签名和广播实现；不得创建 PrivateKey、Keypair 或 Wallet 类实例；不得安装非必要的钱包执行依赖。

## 阶段四 Gate A 当前授权范围

允许：实现已审计的 Binance Web3、Solana RPC/WSS、Pump/PumpSwap 和 Jupiter Quote 只读适配；实现实时 Paper/Shadow 协调器、持仓生命周期、重启恢复、Dashboard、Telegram Paper/Shadow 控制、健康、锁、导出和文档；执行有限、可终止的只读探测。

禁止：任何写链、Jupiter 执行接口、交易构建、钱包、私钥、签名、广播、Live、BSC、未确认字段或 endpoint、自动进入 Gate B。

## Gate B 单独授权边界

Gate B Live 只有在用户单独明确授权后才可审计和设计；在此之前不得创建 Live Engine、钱包适配、签名器、交易构建器或广播路径。

## 阶段 0 授权范围

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
