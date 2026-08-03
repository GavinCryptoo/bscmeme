# Solana 只读监控

## 允许的方法

`SolanaRpcClient` 仅允许显式读取方法：`getAccountInfo`、`getSlot`、`getSignaturesForAddress`、`getTransaction`、`getProgramAccounts` 及其它白名单读取。未知方法直接返回 `solana_write_method_blocked`。

`SolanaWssMonitor` 仅允许 `accountSubscribe`、`logsSubscribe`、`programSubscribe`、`slotSubscribe`。它有超时、心跳、重连退避和 `STALE` 状态；断线不会产生任何写操作。

## 来源与限制

- [Solana getAccountInfo](https://solana.com/docs/rpc/http/getaccountinfo)
- [Solana getSlot](https://solana.com/docs/rpc/http/getslot)
- [Solana getSignaturesForAddress](https://solana.com/docs/rpc/http/getsignaturesforaddress)
- [Solana accountSubscribe](https://solana.com/docs/rpc/websocket/accountsubscribe)
- [Solana logsSubscribe](https://solana.com/docs/rpc/websocket/logssubscribe)

## 多源主备

HTTP RPC 与 WSS 是两条独立的只读通道，分别按环境变量中的顺序运行：

- `SOLANA_RPC_URL` → `SOLANA_BACKUP_RPC_URL` → `SOLANA_RPC_BACKUP_URLS`
- `SOLANA_WS_URL` → `SOLANA_BACKUP_WS_URL` → `SOLANA_WS_BACKUP_URLS`

每次 RPC 调用从当前健康端点开始，失败后依次尝试备用端点，并记住成功端点；WSS 连接或订阅循环断开后按相同顺序切换。状态只记录配置端点数量、当前索引、切换次数和错误类别，不记录 URL。

RPC/WSS URL 只从环境读取，不打印。响应异常、未知布局、缺少依赖和连接超时均为 unavailable；不得用 RPC 余额或交易历史推导未确认的策略字段。

## Token decimals

Jupiter Quote 所需的 Token decimals 按以下优先级取得：`TOKEN_DECIMALS_JSON` 可选覆盖 → `TOKEN_DECIMALS_CACHE_PATH` 本地缓存 → Solana RPC `getTokenSupply` 的 `result.value.decimals`。缓存只保存 Mint 与 decimals，不保存 API Key、Token、钱包或交易数据。
