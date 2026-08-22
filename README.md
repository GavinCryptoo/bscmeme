# meme0801

Solana + BSC 的 Meme 策略研究与运行项目。项目把信号发现、链上行情、策略状态机、Paper/Shadow 账本和可选的 BSC Live 执行路径分开，便于回放、验证和安全运行。

## Development Status

项目当前处于持续开发与验证阶段：Paper/Shadow 基线、BSC 数据链路和隔离的 BSC Live 路径均在维护中。执行器、数据源和运行状态应以实际配置、PID、健康文件及最新项目文档为准；涉及真实资金的改动必须先完成针对性验证并获得明确授权。

## 项目背景

这个项目用于评估超早期 Meme Token 的发现、行情跟踪和退出规则。外部数据源只负责提供其能够可靠证明的字段；发现价格、策略规范价、可执行报价和真实成交价不会混为一谈。

当前范围包括：

- Solana：只读链上数据、Jupiter Quote、Paper/Shadow；不进入 Solana Live。
- BSC：Binance Meme Rush 只读发现、BSC RPC/WSS、Pool/Venue 识别、Paper/Shadow，以及独立且必须显式授权的 Live 路径。
- Dashboard：本地只读监控与 Paper/Shadow 新开仓暂停控制，不提供钱包、签名、广播或 Live 下单入口。

当前有效策略身份、参数和运行优先级以 [`CURRENT_DECISIONS.md`](CURRENT_DECISIONS.md)、[`PROJECT_STATE.md`](PROJECT_STATE.md) 及实际 PID 的有效环境为准；旧历史说明不自动覆盖当前运行配置。

## 策略架构

核心数据流如下：

```text
Discovery / Signal
        ↓
标准化 Token、Venue、Flow 结果
        ↓
实时行情 / 只读 Quote / 数据质量检查
        ↓
策略 Candidate、Position、Exit 状态机
        ↓
Paper/Shadow 账本，或显式配置的 BSC Live Executor
        ↓
SQLite WAL、审计日志、Dashboard
```

网络请求、RPC/WSS、指纹识别和报价在 worker/bridge 中完成；运行时主循环负责消费小结果、推进状态并写入 runtime DB。Dashboard 使用独立只读连接。

价格职责保持分离：Binance 价格是发现/诊断参考；策略规范价来自已验证的链上 Venue/WSS 或协议实时数据；Quote 是可执行性检查；真实 Entry/Exit 价格只能来自已确认订单、receipt 和余额对账。

详细模块职责见 [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)。
运行日志格式与保留规则见 [`docs/RUNTIME_LOGGING.md`](docs/RUNTIME_LOGGING.md)。
开发环境、测试、Dashboard 和数据源扩展见 [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md)。

## Paper / Shadow / Live

| 模式 | 用途 | 钱包/签名/广播 | 数据与风险边界 |
| --- | --- | --- | --- |
| Paper | 虚拟交易和策略回归 | 禁止 | 独立虚拟余额、Position、PnL 和 SQLite；只接受合格报价，不伪造成交 |
| Shadow | 独立的对照与风险观察 | 禁止 | 独立生命周期和账本，不修改 Paper 的仓位、PnL、容量或熔断 |
| Live | BSC 小额真实执行（需明确授权） | 仅在显式安全开关和执行器预检通过时允许 | 独立 DB、审计、锁和执行器；必须经过报价、提交、状态查询、receipt/余额对账 |

默认运行保持 Paper/Shadow 安全开关。Solana Live 不在项目范围内。BSC Live 的具体 Provider、金额、容量和认证状态必须从当前 runner 与进程环境核验，不能只看 `.env` 或 Dashboard。

## 风险控制

- **Fail-closed**：chainId、余额、nonce、decimals、allowance、双向报价、报价新鲜度、gas、最低到账量、deadline 或状态对账任一关键项不可靠时拒绝执行。
- **容量与去重**：同一 Token 的活跃生命周期、同名冷却、最大持仓和未完成买入 Reservation 必须计入容量；请求超时或订单未知时，先对账再重试，禁止盲目重复下单。
- **退出安全**：退出意图持久化，TP/SL/超时和手动/外部退出分开记录；真实成交以 receipt 与钱包余额为准。
- **数据隔离**：Paper、Shadow、BSC Live 使用不同 runtime DB、日志、锁和状态目录；后台 worker 不直接写 runtime DB。
- **凭据边界**：私钥、助记词、API Secret、认证会话和 `.env` 只允许本地运行时读取，不进入 Git、日志、数据库或 Dashboard。

## 当前限制

- Binance Web3 的部分时间窗口、独立买家、可靠创建者卖出和 price impact 字段可能不可用；不可用字段保持 unavailable，不用其它字段猜测替代。
- Binance 页面/Token Dynamic/Kline 价格不能替代策略规范价、可执行 Quote 或真实成交价。
- 不同 Launchpad、Pool/Venue 和 Provider 的能力并不等价；未知协议必须保留为 unknown/unsupported，不能因为缺少 Adapter 就伪造流动性或报价。
- Paper/Shadow 的网络成本和部分 PnL 可能是估算值，必须显式标记；无可靠卖出报价时不得伪造退出价格。
- Live 是高风险、独立、显式授权的 BSC 路径；任何文档、测试或 Quote Benchmark 都不等于真实交易链路已经验证。
- 运行状态、当前执行器和配置可能随部署变化；以 PID、健康文件、短日志和目标 runtime DB 为最终事实来源。

## 快速开始

### 安全的 Paper / Shadow 检查

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_*.py' -v
PYTHONPATH=src python3 -m compileall -q src
DATA_SOURCE=binance_web3 PYTHONPATH=src python3 run_realtime.py --mode both --once
PYTHONPATH=src python3 run_dashboard.py
```

`DATA_SOURCE=fixture` 是默认、确定性的测试数据源。长期 runner 需要由用户或外部 supervisor 显式启动。

### BSC Balanced Paper

```bash
PYTHONPATH=src python3 run_bsc_balanced_paper.py --env-file .env
```

### BSC Live（仅在明确授权后）

```bash
DATA_SOURCE=binance_web3 PYTHONPATH=src \
  python3 run_realtime.py --chain bsc --mode live \
  --strategy-profile balanced --env-file .env
```

启动前必须确认独立 Live 配置、执行器预检、余额、锁、目标 DB 和实际环境。不要把上面的命令用于未经授权的真实交易。

## 目录导航

```text
src/meme_system/domain/                 领域模型与策略生命周期
src/meme_system/strategies/             Candidate、Position、Exit 状态机
src/meme_system/adapters/               Binance、BSC、Quote、执行器等适配器
src/meme_system/config/                 运行与安全配置
src/meme_system/storage/                SQLite WAL、迁移和账本查询
src/meme_system/static/                 Dashboard 静态资源
tests/                                  单元与回归测试
docs/                                   架构、运行、数据和验收文档
run_realtime.py                         统一 realtime 入口
run_bsc_balanced_paper.py               BSC Balanced Paper 入口
run_dashboard.py                        本地 Dashboard 入口
```

更多运行和数据边界：

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- [`docs/RUNTIME_OPERATIONS.md`](docs/RUNTIME_OPERATIONS.md)
- [`docs/PAPER_EXECUTION_MODEL.md`](docs/PAPER_EXECUTION_MODEL.md)
- [`docs/SHADOW_SEMANTICS.md`](docs/SHADOW_SEMANTICS.md)
- [`CURRENT_DECISIONS.md`](CURRENT_DECISIONS.md)
- [`PROJECT_STATE.md`](PROJECT_STATE.md)
