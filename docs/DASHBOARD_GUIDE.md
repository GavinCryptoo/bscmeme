# Dashboard 使用说明

显式启动：

```bash
PYTHONPATH=src python3 run_dashboard.py
```

默认地址是 `http://127.0.0.1:8788/`。现在 Solana 与 BSC 共用同一个页面，使用页面顶部链切换；接口包括：`/api/status`、`/api/health`、`/api/signals`、`/api/candidates`、`/api/positions`、`/api/executions`、`/api/events`、`/api/shadow-outcomes`、`/api/config` 和 `/api/analytics`。列表默认按时间倒序。

`/api/analytics` 支持 `chain=solana|bsc`、`mode=paper|shadow`、`strategy`、`token`、`outcome=all|profit|loss`、`window=24h|7d|all` 和趋势专用的 `trend_window=24h|3d|7d|1m|all`。它按策略分别计算已实现盈亏，返回亏损原因占比、拒绝原因占比以及可切换时间范围的盈亏趋势；BSC 数值继续标记为基于 Binance 指示价的估算值。

唯一允许的 POST 是 `/api/control`，JSON 为 `{ "mode": "paper"|"shadow", "paused": true|false }`。它只改变新入场暂停标志，已有持仓仍继续监控；没有 Live、钱包、签名或广播控制。

Dashboard 读取 SQLite WAL 与健康快照；JSONL 不作为事实账本替代。公网绑定不属于默认配置，非本地绑定必须另行实现认证和审计。
