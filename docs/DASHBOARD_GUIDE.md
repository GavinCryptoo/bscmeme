# Dashboard 使用说明

显式启动：

```bash
PYTHONPATH=src python3 run_dashboard.py
```

默认地址是 `http://127.0.0.1:8788/`。接口包括：`/api/status`、`/api/health`、`/api/signals`、`/api/candidates`、`/api/positions`、`/api/executions`、`/api/events`、`/api/shadow-outcomes` 和 `/api/config`。列表默认按时间倒序。

唯一允许的 POST 是 `/api/control`，JSON 为 `{ "mode": "paper"|"shadow", "paused": true|false }`。它只改变新入场暂停标志，已有持仓仍继续监控；没有 Live、钱包、签名或广播控制。

Dashboard 读取 SQLite WAL 与健康快照；JSONL 不作为事实账本替代。公网绑定不属于默认配置，非本地绑定必须另行实现认证和审计。
