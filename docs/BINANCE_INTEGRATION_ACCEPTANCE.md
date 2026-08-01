# Binance Web3 集成验收

## 必须成立

- [x] 只启用 Solana `CT_501`。
- [x] 公开 endpoint 使用 `auth_mode=none`，没有 API Key/Cookie/Wallet 注入。
- [x] `DATA_SOURCE` 显式支持 `fixture`、`replay`、`binance_web3`，默认 `fixture`。
- [x] client 有超时、最大响应大小、有限重试、request id、延迟和安全错误分类。
- [x] Meme Rush、Smart Money、Token Dynamic、Kline 均有独立 adapter/normalizer 边界，并已取得 HTTP 200 真实响应子集。
- [x] Smart Money 固定 Shadow-only，不能触发入场。
- [x] 缺失字段保持 unavailable；没有用 24h 字段替代 15s 规则。
- [x] Phase 2 Paper/Shadow 独立数据库和重启/重复生命周期保护未被改写。
- [x] 没有启动 Dashboard、长期 runner、RPC/WSS、Jupiter、钱包、签名、广播或 Live。

## 验证命令

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_*.py' -v
PYTHONPATH=src python3 -m compileall -q src
PYTHONPATH=src python3 run_source_probe.py --endpoint meme --once
```

阶段二冻结前回归：25 tests，`OK`；commit `2de0709`，tag `phase-2-complete`。

阶段三最终回归：41 tests，`OK`；`compileall` 通过。2026-08-01 的有限探测已成功；live fixture 是经字段裁剪的公开响应子集，不等于稳定历史数据集。

## 不属于本阶段的验收项

- Dashboard HTTP 页面/API。
- Jupiter quote、可执行路由和 price impact。
- Solana RPC/WSS、Pump/PumpSwap。
- 任何钱包、私钥、签名、广播、订单执行或 Live。
- BSC adapter 的真实连接。
