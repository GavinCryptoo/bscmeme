# KNOWN_FAILURES.md

当前实现必须避免以下已知失败：

1. 不把历史策略 V2、V2.1、V2.2、V4 当作当前可运行策略。
2. 不把中间价、页面展示价或本地指示价冒充可执行成交价。
3. 无有效报价、报价过期、无路由或无流动性时不得模拟成交。
4. 不使用固定 1 秒节流掩盖事件循环或执行链路阻塞。
5. 不让 Shadow 修改 Paper 仓位、PnL、最大持仓或熔断。
6. 不让两个 runner 写同一数据库或状态目录。
7. 不把 latest_status.json 当作运行事实账本。
8. 不用当前值填补缺失历史字段；缺失值必须为 — 或 N/A。
9. 不把 Bitget 聚合行情当作原生 Solana WSS，也不让它触发 Paper 或退出。
10. 不在没有官方文档和当前接口确认时猜测 RPC、WSS 或 Jupiter 字段。
11. 不把旧 Order/Execute 字段当成当前 Jupiter 事实。
12. 不安装非必要的钱包执行依赖。
13. 不创建 PrivateKey、Keypair 或 Wallet 实例。
14. 不允许环境变量开启签名、广播、钱包或 Live 能力后继续启动。
15. 不自动启动长期进程，不终止旧进程，不修改 launchd。
16. BINANCE_WEB3_API_AUDIT.md 缺失时，不得根据历史字段名称猜测 Binance API 能力。
17. 不把 Binance Web3 官方 schema fixture 当作真实网络 capture；必须标记 fixture 来源和网络探测状态。
18. 不把 Binance Meme Rush 的 `createTime` 或 `migrateTime` 在官方未确认单位时当作可计算 token age。
19. 不把 Binance 的 24h `countBuy`/`countSell` 当作 15 秒买卖比，不把 `holders` 当作 15 秒独立买家。
20. 不把 Token Dynamic 或 Kline 指示性数据当作可执行买卖报价、路由或 price impact。
21. 不让 Smart Money 信号触发 Paper 入场；它只能写入独立 Shadow 观察结果。
22. Binance Web3 client 必须有有限超时、重试、响应大小上限和安全错误输出；禁止无界重试。
23. 本地网络探测若为 connect timeout，只能记录为 blocked/unavailable，不得用成功响应替代。
