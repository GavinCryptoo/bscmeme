# Telegram Paper / Shadow 控制

默认 `TELEGRAM_ENABLED=false`。显式开启时需要 `TELEGRAM_BOT_TOKEN` 和 `TELEGRAM_CHAT_ID`，令牌只在请求 URL 中使用，不出现在 status、日志或异常内容中。

允许命令：`/paper_pause`、`/paper_resume`、`/shadow_pause`、`/shadow_resume`、`/status`。只有配置的 chat id 被接受。暂停只禁止新入场，既有 Paper/Shadow 持仓继续运行；不存在 Live 命令。

Telegram 不是行情、报价、签名或广播通道。网络错误会被分类为 `telegram_connection_error`，不会改变交易安全开关或自动重试到无界。
