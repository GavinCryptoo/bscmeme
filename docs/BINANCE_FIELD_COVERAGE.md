# Binance 字段覆盖

## 口径

本阶段已在 `2026-08-01` 取得各 endpoint 的 HTTP 200 live response，并保存了经过字段裁剪的真实响应子集。以下 live 覆盖率只表示本次样本观察结果，不代表每次线上响应都会提供可选字段。

| endpoint | live sample count | schema fixture | network probe |
|---|---:|---|---|
| Meme Rush | 1 response / 3-row probe | 1 synthetic + 1 live subset | success, HTTP 200 |
| Smart Money | 1 response / 3-row probe | 1 synthetic + 1 live subset | success, HTTP 200 |
| Token Dynamic | 1 response | 1 synthetic + 1 live subset | success, HTTP 200 |
| Kline | 1 response | 1 synthetic + 1 live subset | success, HTTP 200 |

## 归一化字段

| 归一化字段 | 来源字段 | fixture | live | 可否用于基线硬条件 |
|---|---|---:|---:|---|
| `mint` | `contractAddress` | available | available in sample | 可作为身份字段 |
| `symbol` / `name` | `symbol` / `name` | available | available in sample | 仅记录 |
| `price_usd` | Meme `price` | available | available in sample | 指示性，不是执行价 |
| `market_cap_usd` | `marketCap` | available | available in sample | 记录/分组 |
| `liquidity_usd` | `liquidity` | available | available in sample | 指示性，不是可执行流动性 |
| `volume_24h_usd` | `volume` | available | available in sample | 记录/分组 |
| `holders` | `holders` | available | available in sample | 不能代替独立买家 |
| `count_24h` | `count` | available | available in sample | 不能代替 15s 事件数 |
| `count_buy_24h` / `count_sell_24h` | `countBuy` / `countSell` | available | available in sample | 不能代替 15s 比值 |
| `progress_pct` | `progress` | available | available in sample | 仅观察 |
| `token_created_at` | `createTime` | unavailable: unit unknown | unavailable | 不能计算 token age |
| `migrate_time` | `migrateTime` | unavailable: unit unknown | unavailable | 不能作为确认时间 |
| `dev_sold_percent` | `devSellPercent` | available | available in sample | 不是创建者卖出确认事件 |
| `signal_timestamp` | Smart `signalTriggerTime` | available, ms | available, ms | 仅 Smart Money 观察时间 |
| `max_gain` | Smart `maxGain` | available, decimal ratio | available, decimal ratio | Shadow 观察 |
| `exit_rate` | Smart `exitRate` | available | available in sample | Shadow 观察 |
| `smart_money_count` | Smart `smartMoneyCount` | available | available in sample | Shadow 观察 |
| `open/high/low/close` | Kline row | available | available in sample | 不能生成可执行路由 |

## 基线条件覆盖

| 条件 | Binance Web3 状态 | 处理 |
|---|---|---|
| token age 5–120s | unavailable | 候选拒绝并记录原因 |
| 15s unique buyers >= 6 | unavailable | 候选拒绝并记录原因 |
| 15s buy/sell ratio >= 1.8 | unavailable；只有 24h counts | 候选拒绝并记录原因 |
| 15s net buy > 0 | unavailable | 候选拒绝并记录原因 |
| 两个非负流量窗口 | unavailable | 候选拒绝并记录原因 |
| executable buy/sell route | unavailable | 不模拟成交 |
| buy/immediate-exit impact | unavailable | 不模拟成交 |
| creator confirmed sold | unavailable | 不用 `devSellPercent` 替代 |
