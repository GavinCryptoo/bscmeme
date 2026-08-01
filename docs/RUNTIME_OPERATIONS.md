# Gate A 运行运维

## 启动与单实例

```bash
DATA_SOURCE=binance_web3 PYTHONPATH=src python3 run_realtime.py --mode both --once
DATA_SOURCE=binance_web3 PYTHONPATH=src python3 run_realtime.py --mode both --poll-sec 10 --duration 900
```

`--once` 用于有限探测；长期运行必须由用户或外部 supervisor 显式启动。`data/solana/locks/` 下的 O_EXCL 锁阻止同一模式重复启动；锁不会静默删除，残留锁需要人工核对 PID、工作目录和进程状态。

## 健康与故障

健康快照分别写入 Paper/Shadow 目录，SQLite `health_events` 和 `latency_events` 保留事实。Binance、Jupiter、RPC、WSS 和 Telegram 失败都 fail-closed；状态为 unavailable/stale 时不产生虚拟成交。

## 关闭与恢复

使用运行终端的 SIGINT/SIGTERM 进行优雅停止；不要用强制 kill 绕过 SQLite 提交。重启后从 `virtual_positions`、`executions` 和 `lifecycle_events` 恢复，重复 signal/candidate 使用稳定 ID 保护。
