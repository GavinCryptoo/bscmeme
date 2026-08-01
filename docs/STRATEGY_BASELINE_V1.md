# Solana 超前期基线策略 v1

## 身份

```text
strategy_name: sol_ultra_early_baseline
ruleset_name: ultra_early_minimal
ruleset_version: 0.1.0
```

本阶段只建立一个新基线，不恢复历史 V2、V2.1、V2.2、V4。

## 入场硬条件

```text
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
```

Binance Web3 当前没有被确认能提供上述全部时间窗口、路由和 price impact 字段。因此这些条件在 Binance 候选评估中保持 `unavailable`，过滤结果必须说明缺失字段；不能以 holders、24h 买卖次数、页面价格或 Kline 替代。

## 退出

```text
take_profit_pct: 10
take_profit_sell_pct: 100
stop_loss_trigger_pct: -20
stop_loss_sell_pct: 100
max_hold_sec: 600
moving_stop_enabled: false
cliff_guard_enabled: false
partial_take_profit_enabled: false
weak_demand_exit_enabled: false
```

退出只基于 Paper/Shadow 生命周期中合格的虚拟报价；Binance 指示性价格不能被称为可执行成交价。
