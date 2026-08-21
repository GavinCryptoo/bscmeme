# 项目架构

本文档是面向开发和运维的简明地图；当前运行版本、有效 Provider 和策略参数以 `PROJECT_STATE.md`、`CURRENT_DECISIONS.md`、有效配置和实际 PID 为准。

## 模块职责

| 层 | 主要职责 |
| --- | --- |
| Discovery / Signal | 读取 Binance Meme Rush、OKX 只读信号或 Fixture/Replay，并标准化候选字段 |
| Market Data | 读取 BSC RPC/WSS、Factory/Pool/Venue、Flap 等实时行情与 Flow |
| Quote | 获取只读、可执行方向的买卖报价，检查 route、TTL、流动性、price impact 和费用 |
| Strategy | 维护 Candidate、Position、ATH/MFE/MAE、容量、TP/SL/超时与退出意图 |
| Execution | Paper/Shadow 虚拟成交，或在 BSC Live 中执行经过显式授权的真实订单 |
| Storage | 由 runtime owner/main loop 写 SQLite WAL 和不可变审计；保存迁移、状态和对账结果 |
| Dashboard | 用独立只读连接展示状态、候选、持仓、历史交易、健康和异常 |

## 关键调用关系

```text
外部 Discovery / RPC / WSS
          │
          ▼
worker / bridge（网络、解析、报价）
          │  小型结果队列
          ▼
runtime owner / main loop（唯一 runtime DB writer）
          │
          ├─ Candidate / Gate
          ├─ Position / Mark / MFE / MAE
          ├─ EXIT_INTENT / Capacity Reservation
          └─ Paper、Shadow 或 BSC Live 执行桥
                         │
                         ▼
             order status → receipt → wallet balance
                         │
                         ▼
                    SQLite / Dashboard
```

后台任务不得直接提交 runtime DB；Dashboard 不参与策略判断、签名或广播。

## 从信号到交易

1. Discovery 产生标准化 Token，记录来源和字段可用性。
2. Market Data/Quote 层分别取得策略规范价和可执行双向报价；参考价不能冒充成交价。
3. Strategy 评估 Candidate 和风险条件，满足条件后先占用容量，再请求买入执行。
4. Paper/Shadow 只写虚拟账本；BSC Live 还必须通过链、余额、nonce、decimals、allowance、gas、最低到账和 deadline 等安全检查。
5. 真实 Live 订单必须区分 `submitted`、`pending`、`confirmed`、`failed`、`unknown`，并用 receipt 与余额对账后才建立或关闭真实 Position。
6. Exit 由现有 TP/SL/超时/手动或外部余额变化触发；`EXIT_INTENT` 在报价或进程异常时仍保留，直到完成对账。

## 三种运行模式

- **Paper**：独立虚拟余额、Position、PnL 和 runtime DB；不连接钱包，不签名，不广播。
- **Shadow**：独立虚拟生命周期和数据库，用于对照和风险观察；不修改 Paper 的仓位、PnL、容量或熔断。
- **Live**：仅 BSC，独立 DB、审计、锁、钱包和执行器；必须由用户明确授权并显式启用，当前执行器和 Provider 以有效 runner 配置为准。Solana 不进入 Live。

## 外部依赖与边界

- Binance Web3：发现和有限只读市场字段。
- BSC HTTP RPC / WSS / Logs RPC：链上只读、Pool/Venue、价格和 Flow。
- Quote/Execution Provider：Quote、交易构建或经授权的 Live 执行；Provider 失败不能被当作成交成功。
- SQLite WAL：运行事实来源；JSONL 为追加审计，Dashboard 只读。

## 安全边界

任何关键数据缺失或不可信时应 fail-closed。Paper/Shadow 禁止到达签名和广播路径；Live 不能复用 Paper/Shadow 数据或状态。`.env`、钱包和认证存储不属于项目文档或 Git 提交内容。
