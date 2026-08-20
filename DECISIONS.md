# DECISIONS

> 只记录仍影响后续开发的技术取舍；不是参数表或 CHANGELOG。当前策略规则仍以 `CURRENT_DECISIONS.md` 和有效运行配置为准。

### 2026-08-13 / 运行模式以有效进程环境为准

**Decision**  Paper、Live 与 Dashboard 的状态由当前 PID、有效环境和 runtime DB 共同判定，不以 `.env` 或页面单独判定。

**Reason**  本地 `.env` 可以保留 Live-capable 配置，而 Paper launcher 会显式覆盖交易、钱包、签名和广播开关。

**Revisit when**  启动器或配置加载优先级发生实质变化时。

### 2026-08-13 / BSC Balanced Live 保持 fail-closed

**Decision**  未经针对当前操作的明确授权及完整 preflight，不启动或恢复 BSC Live；报价、路由或 benchmark 成功不等于成交资格。

**Reason**  真实执行涉及签名、广播、nonce、gas、receipt 和余额对账，且当前 Capability Router 目标与 realtime launcher 注入路径尚未统一。

**Revisit when**  统一执行器完成 runtime 接入，并通过独立的真实安全验收且用户明确授权 Live 时。

### 2026-08-13 / 执行能力按 Venue / Route capability 分层

**Decision**  Venue 发现、行情、Flow、双向 quote 与真实执行分别判定；未知或不支持 Venue 不能被误标为不存在，也不能绕过可执行性检查。

**Reason**  BSC Meme Token 同时包含预迁移 Launchpad 和迁移后 DEX 场所；单一 Provider 或展示价格无法覆盖所有路线，也不能作为可成交价格。

**Revisit when**  有新的、经真实双向报价与安全验证的 Provider 或协议适配器进入 runtime 执行链时。

### 2026-08-14 / BSC Balanced Live 使用 GMGN CLI

**Decision**  当前 BSC Balanced Live 使用 `GMGN_CLI` 作为路由与执行边界；每笔 `0.001 BNB`、最多 2 个同时仓位，并使用独立 GMGN Live runtime 与订单 journal。

**Reason**  GMGN CLI 已完成受限真实 BSC 买卖往返验证，并能提供同一边界内的双向报价、swap、订单状态和钱包余额；runtime 通过异步 bridge 维持主循环与 SQLite owner 的隔离。

**Revisit when**  实际订单对账显示覆盖率、故障恢复或成本不满足要求，或另一个 Provider 完成同等安全验证并被明确选择时。
