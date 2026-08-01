# 规则集字段映射

本表把当前基线规则映射到实际可用字段。`unavailable` 不得用 0、当前值或其它字段替代。

| 基线字段/规则 | Fixture/Replay | Binance Web3 当前状态 | 决策 |
|---|---|---|---|
| `token_age_sec` | 可由 fixture 的已确认时间计算 | `createTime` 单位未在官方参考中明确 | Binance 下 unavailable，不触发 Paper |
| `unique_buyers_15s` | 由 fixture 事件聚合 | 未确认提供 | unavailable，不触发 Paper |
| `buy_sell_count_ratio_15s` | 由 fixture 事件聚合 | Meme Rush `countBuy`/`countSell` 是 24h 计数 | 不映射为 15s 比值 |
| `net_buy_15s` | 由 fixture 事件聚合 | 未确认提供 15s 净买字段 | unavailable |
| 两个非负短窗净流量 | 由 fixture 事件聚合 | 未确认提供对应窗口字段 | unavailable |
| 可执行买入路由 | Fixture quote provider | Binance Token Dynamic/Kline 不提供路由报价 | unavailable，不模拟成交 |
| 可执行卖出路由 | Fixture quote provider | 未确认提供 | unavailable |
| 买入/卖出 price impact | Fixture quote provider | 未确认提供 | unavailable |
| `creator_confirmed_sold` | Fixture pool/wallet event | Meme Rush `devSellPercent` 不等于确认事件语义 | 不替代 |
| `holders` | Fixture 字段 | Meme Rush / Dynamic 可提供 | 只记录、分组和 Shadow 观察 |
| `market_cap_usd` | Fixture 字段 | Meme Rush 可提供 `marketCap`；Dynamic 的对应字段未作为本阶段必需字段 | 可记录，不作为缺失硬条件的替代 |
| `liquidity_usd` | Fixture 字段 | Meme Rush / Dynamic 可提供 | 指示性记录，不是可执行流动性 |
| 退出 TP/SL/timeout | Fixture/Replay 可计算 | Binance 本身不提供可执行卖价 | 继续只对有合格报价的 Paper/Shadow 生命周期计算 |

当前基线硬条件仍保留在策略身份中，但 Binance Web3 缺失必要条件时必须给出逐项过滤原因，不能把候选强行送入 Paper。
