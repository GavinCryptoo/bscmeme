# IMPLEMENTATION_PLAN.md

当前阶段：阶段 2 / 基线策略冻结与 Paper/Shadow 完整生命周期  
当前状态：执行中，完成后停止等待确认

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

## 阶段 2：Dashboard

- 实现只读 API 和 127.0.0.1:8788 Dashboard。
- 展示运行状态、信号、候选、Paper 持仓、Shadow 持仓、交易、退出详情、Shadow 对比、延迟、数据源健康和版本信息。

## 阶段 2 当前完成范围

- 基线策略评估和逐项过滤原因。
- Paper/Shadow 独立虚拟生命周期。
- MFE、MAE、可执行卖出报价观察。
- SQLite WAL 版本迁移、生命周期事件和重启恢复。
- 确定性 ReplayRunner 和只读 LedgerQueries。

BINANCE_WEB3_API_AUDIT.md 当前缺失，因此本阶段不冻结、不实现任何 Binance API-specific 字段或适配器；该项必须在真实数据源阶段补齐审计后处理。

当前阶段不启动 runner，不启动 Dashboard HTTP 服务，不连接真实网络。

## 阶段 3：真实只读数据源

- 先提交官方文档与可靠开源实现调查报告。
- 确认 API、鉴权、配额、字段和错误语义后，再实现 Solana RPC/WSS、Jupiter QuoteProvider、Pump/PumpSwap 适配器。
- Provider 全部由环境变量配置。

## 阶段 4：观察、回放与评估

- 先完成 20–30 个完整生命周期工程检查。
- 达到 100 个完整生命周期前，不把 Shadow Filter 变成硬过滤。
- 按 7 天连续观察和 100/300 生命周期门槛进行评估。

## 永不自动进入的阶段

Live、钱包、私钥、签名、广播、链上写入和 BSC 实现不在本计划自动范围内，必须由用户另行授权。
