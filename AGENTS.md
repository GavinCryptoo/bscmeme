# meme0801 工作规则

## 当前安全边界

- 默认只允许 Paper；Shadow 必须独立运行。
- Live、钱包、私钥读取、Keypair/PrivateKey/Wallet 实例、签名、广播和链上写入均不属于当前项目。
- PAPER_ONLY=true、LIVE_TRADING=false、WALLET_ENABLED=false、SIGNING_ENABLED=false、BROADCAST_ENABLED=false、TELEGRAM_ENABLED=false 必须强制校验。
- 不安装非必要的钱包或交易执行依赖。
- 不输出密钥、Token、API Secret 或私钥内容。

## 开发规则

- 当前最高优先级：CURRENT_DECISIONS.md，其次是规格文档，最后才是历史上下文。
- 写入真实数据源前，先检索官方 SDK 文档和可靠 GitHub 开源实现。
- 一次只实施一个阶段；每个阶段完成后运行测试并停止，等待确认。
- 不猜测未知接口、URL、字段、配额或鉴权方式。
- 不自动启动长期进程，不自动终止现有进程，不修改 launchd。
- 不未经确认扩大任务范围。
- 所有主要改动必须有版本标识和回滚方式。
- 临时诊断、回放和对账任务完成后必须确认没有残留子进程。

## 当前阶段

当前已获确认进入阶段 3：只允许接入已审计的 Binance Web3 官方只读接口，范围限定 Solana `CT_501`、有限探测、Fixture/Replay 和 Paper/Shadow 的数据适配，不进入 Dashboard 实现。

阶段 3 仍禁止真实 Solana RPC/WSS、Jupiter、Pump/PumpSwap、钱包、私钥、Keypair/PrivateKey/Wallet、签名、广播、链上写入、Live、BSC、Telegram 和长期 runner。Binance Web3 的当前公开接口鉴权模式冻结为 `none`；不得自行添加 API Key、Cookie、Session 或 Jupiter 凭据头。
