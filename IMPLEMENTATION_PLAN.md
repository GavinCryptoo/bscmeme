# IMPLEMENTATION_PLAN.md

当前阶段：阶段 4 Gate A / 实时 Paper + Shadow 完整集成
当前状态：Gate A 完成后停止，等待单独的 Gate B Live 授权

## 阶段 0：项目冻结与骨架

范围：

- 固化 CURRENT_DECISIONS.md。
- 创建安全边界、基础 domain model、空 Protocol 和 SQLite migration 框架。
- 创建 Fixture/Replay 测试骨架。
- 只读检查旧进程，不终止任何进程。

不包含真实数据源、完整策略循环、Dashboard HTTP 服务、长期 runner 或任何链上执行能力。

验收：

- fail-closed 测试通过。
- 单元测试通过。
- Python 编译检查通过。
- Git 状态可审计。
- 没有长期进程启动。

## 阶段 1：领域模型与确定性 Paper/Shadow 核心

- 实现已冻结的基线身份、入场条件、Paper 退出和 Shadow 退出。
- 接入 Fixture/Replay 数据，不连接真实网络。
- 完成独立 Paper/Shadow 生命周期、报价和成本记录。

当前阶段不启动 runner，不启动 Dashboard HTTP 服务，不连接真实网络。

## 阶段 2 当前完成范围

- 基线策略评估和逐项过滤原因。
- Paper/Shadow 独立虚拟生命周期。
- MFE、MAE、可执行卖出报价观察。
- SQLite WAL 版本迁移、生命周期事件和重启恢复。
- 确定性 ReplayRunner 和只读 LedgerQueries。

阶段二已经在本地 commit `2de0709` 完成，并标记 `phase-2-complete`。Dashboard HTTP 服务仍未启动，也不属于阶段三范围。

## 阶段 3：Binance Web3 官方只读适配

- 固化官方 endpoint、Host、Method、Solana chain ID、可确认字段、错误码和未确认字段。
- 实现 `binance_web3` client、auth mode、models、normalizer、signal source、market data、Kline、Smart Money、rate limit 和 redaction。
- `DATA_SOURCE` 支持 `fixture`、`replay`、`binance_web3`，默认 `fixture`。
- 只做有限、可终止的单次或短时探测；不启动长期 runner，不启动 Dashboard。
- Binance Smart Money 仅 Shadow；历史 bootstrap 只标记，不导入 Paper。
- 用官方 schema 样例构造脱敏 fixture；若网络受阻，不伪造真实 live capture。

阶段三验收：审计文档、字段覆盖、错误目录、有限探测报告、适配器单测、Phase 2 回归测试和本地 commit 均可审计；钱包、签名、广播、Live、RPC/WSS、Jupiter、BSC 和 Dashboard 均未进入。

## 阶段 4 Gate A：实时只读数据与 Paper/Shadow 运行

- 使用已审计 Binance Web3 Solana `CT_501` 信号源。
- 使用 Solana RPC/WSS、Pump/PumpSwap 只读状态模型；未知布局 fail-closed。
- 使用 Jupiter 当前官方 Quote GET，禁止 Swap、交易构建、签名和广播。
- 通过冻结基线策略评估候选，记录逐项过滤原因。
- 运行独立 Paper/Shadow SQLite WAL、JSONL、MFE/MAE、退出和重启恢复。
- 提供本地 Dashboard、有限 Telegram Paper/Shadow 控制、健康检查、单实例锁和导出。
- 运行有限验收和确定性 Fixture/Replay；不启动 Live，不自动进入 Gate B。

验收：阶段一至三回归测试及 Gate A 新增测试通过；compileall 通过；只读安全审计通过；Dashboard 查询与控制接口通过；Paper/Shadow 数据目录隔离；未创建钱包、签名、广播、Live 文件。

## 阶段 5：观察、回放与评估

- 先完成 20–30 个完整生命周期工程检查。
- 达到 100 个完整生命周期前，不把 Shadow Filter 变成硬过滤。
- 按 7 天连续观察和 100/300 生命周期门槛进行评估。

## 永不自动进入的阶段

Live、钱包、私钥、签名、广播、链上写入和 BSC 实现不在本计划自动范围内，必须由用户另行授权。
