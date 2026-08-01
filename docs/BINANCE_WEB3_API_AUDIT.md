# Binance Web3 API 审计

审计时间：`2026-08-01T05:56:03Z`。审计依据为 Binance Skills Hub 官方页面、官方 GitHub 仓库源码和本机有限只读探测。本文只冻结已被官方资料和当前样本确认的 endpoint、字段和错误语义；live fixture 是经过字段裁剪的公开响应子集，不含凭据。

## 结论

- 当前链：Solana，`chainId=CT_501`。
- 当前公开 endpoint 鉴权：`auth_mode=none`。官方 skill 源码只设置 User-Agent 和 `Accept-Encoding: identity`，本实现不发送 API Key、Cookie、Session、钱包或 Jupiter 凭据。
- 当前允许：Meme Rush、Smart Money、Token Dynamic、Kline 的有限只读请求。
- 当前不允许：`baw` 私有信号列表、交易/Execute、签名、广播、钱包、RPC/WSS、Jupiter、BSC。
- 配额窗口、精确 QPS、完整历史回放能力和部分时间单位未被本阶段资料确认，保持 `unknown` 或 `unavailable`。

## Endpoint 清单

| endpoint | Host + Path | Method | Solana 参数 | 状态 |
|---|---|---|---|---|
| Meme Rush | `https://web3.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/market/token/pulse/rank/list/ai` | POST | `chainId=CT_501`、`rankType=10/20/30`、`limit<=200` | `verified` |
| Smart Money | `https://web3.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/web/signal/smart-money/ai` | POST | `chainId=CT_501`、`page`、`pageSize` | `verified` |
| Token Dynamic | `https://web3.binance.com/bapi/defi/v4/public/wallet-direct/buw/wallet/market/token/dynamic/info/ai?chainId=CT_501&contractAddress=...` | GET | `chainId`、`contractAddress` | `verified` |
| Kline | `https://dquery.sintral.io/u-kline/v1/k-line/candles?platform=solana&address=...&interval=...` | GET | `platform=solana`、`address`、`interval`、`limit<=500` | `verified` |

`verified` 的含义是：官方文档/官方源码确认了路径和请求形状，并在 `2026-08-01` 取得了 HTTP 200 的真实公开响应样本。它不代表已确认稳定配额，也不代表所有可选字段在每次响应都出现。

## 官方字段语义

### Meme Rush

已确认字段包括 `contractAddress`、`symbol`、`name`、`decimals`、`price`、`priceChange`、`marketCap`、`liquidity`、`volume`、`holders`、`progress`、`count`、`countBuy`、`countSell`、`devSellPercent`、`devPosition`、`migrateStatus`、`migrateTime`、`createTime`。市场数值以字符串形式出现；计数文档语义为 24h。`createTime` 和 `migrateTime` 虽被描述为 long，但单位在官方参考中没有被确认，适配器不计算 token age。

### Smart Money

已确认字段包括 `signalTriggerTime`（官方明确为毫秒）、`alertPrice`、`currentPrice`、`currentMarketCap`、`maxGain`（小数比例）、`exitRate`、`smartMoneyCount`、`direction` 和 `status`。该源固定 Shadow-only。

### Token Dynamic

已确认字段模式包括 `price`、`nativeTokenPrice`、`volume5m/1h/4h/24h`、各窗口 Buy/Sell、NetBuy、Binance、NetBinance、`holders`、`liquidity`。线上样本还包含 `marketCap`、`launchTime`、各窗口 count 等扩展字段；本阶段只归一化已审计字段。数值按字符串小数解析；缺失字段保持 unavailable。

### Kline

返回行是 `[open, high, low, close, volume, timestamp_ms, count]`，时间戳为毫秒；请求参数使用 `platform=solana` 和 `address`，本实现校验 Decimal、毫秒时间戳和重复 candle 时间。

## 错误和限流

官方参考确认的业务码包括：`000000` 成功、`100004` rate limited、`100002` bad parameter、`000400` token not found/unsupported chain。精确限流窗口未确认；client 只做有限重试和请求预算，不宣称配额值。

## 本地实现文件

- `src/meme_system/adapters/binance_web3/client.py`
- `src/meme_system/adapters/binance_web3/auth.py`
- `src/meme_system/adapters/binance_web3/models.py`
- `src/meme_system/adapters/binance_web3/normalizer.py`
- `src/meme_system/adapters/binance_web3/signal_source.py`
- `src/meme_system/adapters/binance_web3/market_data.py`
- `src/meme_system/adapters/binance_web3/kline.py`
- `src/meme_system/adapters/binance_web3/smart_money.py`

## 官方来源

- [Meme Rush 官方 skill](https://www.binance.com/en/skills/detail/binance-web3/meme-rush)
- [Query Token Info 官方 skill](https://www.binance.com/en/skills/detail/binance-web3/query-token-info)
- [Binance Trading Signal 官方 skill](https://www.binance.com/en/skills/detail/binance-web3/binance-trading-signal)
- [官方 Meme Rush CLI 源码](https://raw.githubusercontent.com/binance/binance-skills-hub/main/skills/binance-web3/meme-rush/scripts/cli.mjs)
- [官方 Smart Money CLI 源码](https://raw.githubusercontent.com/binance/binance-skills-hub/main/skills/binance-web3/binance-trading-signal/scripts/cli.mjs)
- [官方 Query Token Info CLI 源码](https://raw.githubusercontent.com/binance/binance-skills-hub/main/skills/binance-web3/query-token-info/scripts/cli.mjs)
