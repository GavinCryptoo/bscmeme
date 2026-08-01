# Paper 执行模型

Paper 是虚拟交易账本，不连接钱包，不签名，不广播，不写链。

## 生命周期

```text
candidate -> evaluated -> accepted -> entry_quoted -> open
         -> exit_requested -> closed
```

每个生命周期必须携带 `strategy_name`、`ruleset_name`、`ruleset_version`、`config_version`，并以 `mint + strategy_name + ruleset_version` 防止重复活跃生命周期。

## 报价与成交

- Fixture/Replay 的 QuoteProvider 必须给出方向、数量、输出数量、报价时间、quote age、路由费和 price impact。
- 无报价、报价过期、无路由或无流动性时，Paper 不得开仓或伪造退出成交。
- Binance Token Dynamic/Kline 只能提供观察数据，不能直接驱动可执行成交。
- 成本字段分开保存：`gross_pnl`、`route_fee`、`estimated_network_fee`、`estimated_priority_fee`、`net_pnl_estimated`。
- 无法可靠取得网络费时保持空值并设置 `net_pnl_is_estimated=true`。

## 退出与指标

支持 TP、SL、最大持仓超时；每个持仓更新 MFE、MAE，并记录退出原因、触发报价和审计事件。重启从独立 Paper SQLite WAL 恢复，重复事件不得重复开仓或重复关闭。
