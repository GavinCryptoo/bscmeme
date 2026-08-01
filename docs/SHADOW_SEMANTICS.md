# Shadow 语义

Shadow 使用独立虚拟生命周期、独立数据库和独立持仓指标。它不得修改 Paper 仓位、PnL、最大持仓、熔断或入场计数。

## Smart Money

Binance Smart Money 适配器固定：

```text
trigger_entry: false
shadow_feature_only: true
```

它只用于记录信号、观察时点、方向、当前价格/市值、`maxGain`、`exitRate` 和智能资金数量，并参与 Shadow 对比；不得单独触发 Paper 入场或退出。

## Shadow 退出规则

- `shadow_defense_v1`：收益率 <= -8%、最近短窗口净流量为负、独立买家增长停止。
- `shadow_creator_sell`：只有高置信创建者或明确关联地址卖出事件才触发；`devSellPercent` 不自动等同于该事件。
- `shadow_time_exit`：持仓 >=120s、MFE <5%、买家增长和净流量同时放缓。

触发后记录 5s、15s、30s、60s、120s 观察收益、是否达到 Paper TP、避免损失和错过利润。无法获得字段时保留 unavailable，不回填。
