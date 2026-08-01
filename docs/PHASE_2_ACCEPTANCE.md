# 阶段二验收记录

阶段二目标为基线策略评估、Paper/Shadow 独立生命周期、持仓 MFE/MAE、SQLite WAL 恢复、重复生命周期保护和确定性 Replay。

## 已完成检查点

- Stage 2 全量测试：25 tests，`OK`。
- 本地 commit：`2de0709 phase-2: complete fixture replay paper shadow lifecycle`。
- 本地 tag：`phase-2-complete`。
- 未 push；没有启动长期 runner、Dashboard HTTP 服务或真实网络接入。

## 保留边界

阶段二的事实输入是 Fixture/Replay。Paper/Shadow 的可执行报价语义不能由 Binance Web3 的页面价格、Token Dynamic 或 Kline 自动替代。
