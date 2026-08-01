# Jupiter Quote 审计

Gate A 只实现当前官方 Quote GET：`https://api.jup.ag/swap/v1/quote`。实现读取 `inputMint`、`outputMint`、原始 `amount`、`slippageBps`、`restrictIntermediateTokens`、`instructionVersion` 和官方响应中的数量、route、`contextSlot`、`timeTaken` 等字段。

官方参考：[Jupiter Get Quote](https://developers.jup.ag/docs/swap/v1/get-quote)。当前代码不实现 `/swap`、`/swap-instructions`、交易构建、钱包、签名或发送。

## 未冻结字段

官方响应有 `priceImpactPct`，但本项目当前审计没有把它的单位冻结成 ratio 或 percent。因此默认 `JUPITER_PRICE_IMPACT_UNIT` 为空，归一化后的 `price_impact_pct` 保持 unavailable，基线策略拒绝使用该报价。只有用户后续确认单位并显式配置 `ratio` 或 `percent` 后，才允许转换。

Token decimals 也必须来自已确认配置；缺少 decimals、API key、路由、流动性、数量或 TTL 时，Quote 是 unavailable，不会降级成中间价。
