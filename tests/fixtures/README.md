# Fixture/Replay fixtures

阶段 2 只使用标准化确定性 Fixture/Replay 输入。阶段 3 另外保留两类 Binance Web3 样例：

- `*_normal.json`：根据官方 schema 编写的合成测试 fixture，稳定、脱敏，不代表线上捕获。
- `*_live.json`：2026-08-01 通过官方公开只读 endpoint 获取的真实响应子集，经过字段裁剪和脱敏，仅用于 schema/normalizer 回归，不用于交易决策。

所有 `.meta.json` 都记录样例来源、捕获时间和网络探测状态；没有凭据字段。
