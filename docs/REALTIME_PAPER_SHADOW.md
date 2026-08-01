# Gate A 实时 Paper / Shadow

## 数据流

`Binance Meme Rush -> normalized signal -> field availability audit -> Jupiter Quote GET -> frozen baseline -> independent Paper/Shadow ledger`。

`run_realtime.py` 默认不会运行；必须显式使用 `DATA_SOURCE=binance_web3` 启动。第一轮 Binance 返回的数据标记为 `historical_bootstrap`，只观察、不追加入场，避免重启追历史。

## 入场

基线身份固定为 `sol_ultra_early_baseline / ultra_early_minimal / 0.1.0`。币龄、15 秒独立买家、15 秒买卖比、15 秒净买、两个短窗流量和创建者卖出确认任一不可用，都会产生明确过滤原因并拒绝入场。Binance 已确认的 24h/5m 字段不会被转换成这些字段。

Paper 与 Shadow 使用两个 `SimulationLedger` 和两个 SQLite WAL 文件。Shadow 不共享 Paper 的持仓、余额、熔断、MFE/MAE 或生命周期状态。

## 退出和恢复

每次重新获取当前虚拟持仓数量的卖出 Quote；只有路由、流动性、数量、TTL 和 price impact 均可用时才计算虚拟成交。Paper 记录止盈、止损、超时、route fee 和估算网络成本；Shadow 额外记录防守、创建者卖出、时间退出及 5/15/30/60/120 秒跟踪窗口。SQLite 重启时从 `virtual_positions` 和执行事实恢复活动生命周期。

## 安全

本阶段没有钱包、私钥、Keypair、Signer、交易构建、Jupiter Swap 或广播代码。Dashboard/Telegram 只可暂停或恢复 Paper/Shadow 新入场。
