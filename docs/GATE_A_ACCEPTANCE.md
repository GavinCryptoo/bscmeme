# Gate A 验收

## 已自动验证

- Paper/Shadow 独立数据库、候选、持仓、执行、MFE/MAE、退出和重启恢复。
- Binance Web3 适配器回归、有限重试、历史 bootstrap、Smart Money Shadow-only。
- Solana RPC/WSS 只读 allow-list、Pump/PumpSwap 解码 fail-closed、Jupiter Quote schema 和未知 price impact 单位保护。
- Dashboard 查询/控制、Telegram 默认关闭、单实例锁、运行状态和导出接口。

## 本地命令

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_*.py' -v
PYTHONPATH=src python3 -m compileall -q .
```

## 受控网络验收

真实 Binance、Solana RPC/WSS、Jupiter Quote 和 Telegram 需要用户提供运行时环境变量。未配置时必须报告 unavailable，不得放入凭据或伪造成功。Gate A 需要分别记录一次启动、15 分钟稳定运行、2 小时稳定运行以及 24–48 小时 soak 的日志、健康、延迟、磁盘和重复生命周期结果；这些长时验收不在编码时自动启动。

## Gate B 停止条件

Gate A 通过后停止。没有用户另行明确授权，不创建 Live Engine、钱包、私钥、签名、交易构建或广播路径。
