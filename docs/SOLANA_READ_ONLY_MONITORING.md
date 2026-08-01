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

RPC/WSS URL 只从环境读取，不打印。响应异常、未知布局、缺少依赖和连接超时均为 unavailable；不得用 RPC 余额或交易历史推导未确认的策略字段。
