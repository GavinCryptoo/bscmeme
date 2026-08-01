# Pump / PumpSwap 只读迁移模型

## Pump bonding curve

官方 Pump 文档与 IDL：

- [Pump program README](https://github.com/pump-fun/pump-public-docs/blob/main/docs/PUMP_PROGRAM_README.md)
- [Pump IDL](https://raw.githubusercontent.com/pump-fun/pump-public-docs/main/idl/pump.json)

已冻结的只读状态包括 virtual/real token 和 SOL reserves、total supply、`complete`、观察 slot。bonding curve PDA 使用官方 `bonding-curve` seed；无法完成经过验证的 PDA 或账户布局不足时返回 `UNKNOWN`。

`complete=true` 且 real token reserves 为零时分类为 `MIGRATION_PENDING`。这只是状态，不触发迁移。

## PumpSwap

官方 IDL：[PumpSwap IDL](https://raw.githubusercontent.com/pump-fun/pump-public-docs/main/idl/pump_amm.json)。Pool 的 `base_mint` 通过官方布局的 `getProgramAccounts` memcmp 读取；由于 pool index、creator 和 quote mint 参与地址结构，不能只凭 mint 猜 PDA。Pool 布局变化、多个冲突账户或 vault 余额不可确认时不产生可执行报价。

本模型不会调用 `migrate`、买卖指令、交易构建或广播。Jupiter Quote 是迁移后首选的可执行样式来源，但仍受其独立的 TTL、路由和 price impact 审计约束。
