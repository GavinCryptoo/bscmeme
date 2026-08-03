# meme0801 工作规则

## 当前安全边界

- 默认 Paper/Shadow 必须独立运行；BSC Live 仅在显式完整配置下独立运行。
- Solana Live、Solana 钱包、私钥、签名、广播和链上写入仍不属于当前项目；BSC Live 私钥只从本地 `.env` 读取，不输出、不记录、不提交。
- BSC Live 必须显式设置 `LIVE_TRADING=true`、`BSC_LIVE_ENABLED=true`、交易金额、最大持仓、滑点；Paper/Shadow 的安全默认值保持不变。
- 不安装非必要的钱包或交易执行依赖；BSC Live 仅使用官方 PancakeSwap Smart Router SDK 与受控的 web3 依赖。
- Telegram Token/Chat ID 使用本地 `.env` 配置，不输出、不提交；Telegram 仅允许通知、状态和暂停/恢复新开仓，不允许下单、卖出、钱包或进程控制。
- 不输出密钥、Token、API Secret 或私钥内容。

## 开发规则

- 当前最高优先级：CURRENT_DECISIONS.md，其次是规格文档，最后才是历史上下文。
- 写入真实数据源前，先检索官方 SDK 文档和可靠 GitHub 开源实现。
- 一次只实施一个最小变更；完成后运行测试并停止，不自动启动真实交易。
- 不猜测未知接口、URL、字段、配额或鉴权方式。
- 不自动启动长期进程，不自动终止现有进程，不修改 launchd。
- 不未经确认扩大任务范围。
- 所有主要改动必须有版本标识和回滚方式。
- 临时诊断、回放和对账任务完成后必须确认没有残留子进程。

## 当前阶段

当前允许 BSC 独立小额 Live 最小实现；Solana 仍只允许只读数据与 Paper/Shadow。BSC Live 禁止使用 Binance indicative price 作为成交价，必须先通过官方 PancakeSwap Smart Router 生成实时报价和 calldata，并完成余额、nonce、decimals、allowance、estimateGas、最低到账和 deadline 检查。Binance Web3 的公开接口鉴权模式仍为 `none`。
