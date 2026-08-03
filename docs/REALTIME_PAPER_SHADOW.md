# Gate A 实时 Paper / Shadow

## 数据流

`Binance Meme Rush -> normalized signal -> field availability audit -> BSC bonding curve / PancakeSwap read-only quote or Solana Jupiter Quote -> frozen baseline -> independent Paper/Shadow ledger`。

`run_realtime.py` 默认不会运行；必须显式使用 `DATA_SOURCE=binance_web3` 启动。第一轮 Binance 返回的数据标记为 `historical_bootstrap`，只观察、不追加入场，避免重启追历史。

## 入场

基线身份固定为 `sol_ultra_early_baseline / ultra_early_minimal / 0.1.0`。币龄、15 秒独立买家、15 秒买卖比、15 秒净买、两个短窗流量和创建者卖出确认任一不可用，都会产生明确过滤原因并拒绝入场。Binance 已确认的 24h/5m 字段不会被转换成这些字段。

Paper 与 Shadow 使用两个 `SimulationLedger` 和两个 SQLite WAL 文件。Shadow 不共享 Paper 的持仓、余额、熔断、MFE/MAE 或生命周期状态。

## 退出和恢复

每次重新获取当前虚拟持仓数量的卖出 Quote；只有路由、流动性、数量、TTL 和 price impact 均可用时才计算虚拟成交。Paper 记录止盈、止损、超时、route fee 和估算网络成本；BSC Shadow 在保留结构性提前退出的同时继承 BSC Paper 的止盈、止损和超时规则，Solana Shadow 额外记录防守、创建者卖出、时间退出及 5/15/30/60/120 秒跟踪窗口。SQLite 重启时从 `virtual_positions` 和执行事实恢复活动生命周期。

## 安全

本阶段没有钱包、私钥、Keypair、Signer、交易构建、Jupiter Swap 或广播代码。Dashboard/Telegram 只可暂停或恢复 Paper/Shadow 新入场。

## BSC 持仓刷新

BSC 持仓优先使用已确认的实际交易池或 bonding curve 合约订阅 `eth_subscribe`
日志。日志只触发重新获取实际卖出 Quote，不作为成交价；每 2 秒继续使用实际只读
Quote 作为兜底。WSS 未配置、池地址不可用、依赖缺失或连接失败时，不阻塞持仓，
继续使用 2 秒 Quote 轮询。BSC 的 `pricing_mode=bsc_executable_quote`，Binance
`binance_indicative_reference` 只用于 Dashboard 对照，不参与 PnL；无法取得真实
route、amountOut、quote_at 或 price impact 时拒绝入场。Solana 持仓监控和 Jupiter
Quote 路径不变。
