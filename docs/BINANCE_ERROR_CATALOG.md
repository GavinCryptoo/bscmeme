# Binance Web3 错误目录

所有错误必须输出安全的 `error_class`、endpoint、request id、HTTP status（如有）、retry count；不得输出请求头、Cookie、API Key、Wallet、私钥或完整响应。

| error_class | 触发条件 | 是否重试 | 处理 |
|---|---|---|---|
| `binance_auth_missing` | 预留分类；当前公开 endpoint 不要求凭据 | 否 | 保持 auth mode none，不自行加凭据 |
| `binance_auth_rejected` | HTTP 401/403 | 否 | 标记源不可用 |
| `binance_rate_limited` | HTTP 429 或官方业务码 100004 | 有限 | 遵守有限 retry policy，耗尽后停止 |
| `binance_timeout` | 请求超时或请求预算耗尽 | 有限 | bounded retry，之后 blocked |
| `binance_connection_error` | DNS/TCP/TLS/连接失败 | 有限 | bounded retry，之后 blocked |
| `binance_http_4xx` | 其它 HTTP 4xx | 否 | 记录参数/接口错误 |
| `binance_http_5xx` | HTTP 5xx | 有限 | bounded retry，之后停止 |
| `binance_invalid_json` | 成功 HTTP 响应不是 JSON | 否 | 标记 schema/供应商异常 |
| `binance_schema_changed` | envelope、list、candle row 形状不符合已审计结构 | 否 | 不归一化、不入账 |
| `binance_missing_required_field` | 缺少 `contractAddress` 等身份字段 | 否 | 丢弃该记录并记录原因 |
| `binance_invalid_timestamp` | 已声明毫秒字段非法；或未确认单位被误用 | 否 | 字段 unavailable |
| `binance_invalid_decimal` | 数值无法严格解析为 Decimal | 否 | 字段 unavailable/记录错误 |
| `binance_empty_response` | 空 envelope 或无可处理列表 | 否 | 保持无信号，不补造记录 |
| `binance_pagination_error` | page/pageSize 形状无法解释 | 否 | 停止该页 |
| `binance_duplicate_signal` | 同一稳定 signal id 重复出现 | 否 | 去重，不重复生命周期 |
| `binance_stale_signal` | 数据年龄超过消费方允许窗口 | 否 | 不触发 Paper |
| `binance_unsupported_chain` | 非已审计 `CT_501` 或 `56` | 否 | fail closed |
| `unsupported_auth_method` | 配置要求 API key/cookie/wallet 等未审计模式 | 否 | fail closed |
| `binance_response_too_large` | 响应超过配置上限 | 否 | 丢弃响应 |
| `binance_business_error` | 业务码非 `000000` | 视码而定 | 100004 限流，其它不猜测 |
| `binance_kline_duplicate` | Kline 时间戳重复 | 否 | 拒绝该结果，避免错误序列 |

## 已确认业务码

| code | 含义 | 本地处理 |
|---|---|---|
| `000000` | success | 继续 schema 校验 |
| `100004` | rate limited | 映射 `binance_rate_limited` |
| `100002` | bad parameter | 映射 `binance_business_error`，不重试 |
| `000400` | token not found/unsupported chain | 映射 `binance_business_error` 或链不支持 |
