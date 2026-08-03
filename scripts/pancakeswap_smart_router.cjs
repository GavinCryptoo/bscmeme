#!/usr/bin/env node

// Official PancakeSwap Smart Router SDK boundary. The process receives only
// public swap parameters; the Python parent never passes BSC_PRIVATE_KEY here.

const fs = require('node:fs');
const { createPublicClient, http, hexToBigInt } = require('viem');
const { bsc } = require('viem/chains');
const { Native, Token, CurrencyAmount, TradeType, Percent } = require('@pancakeswap/sdk');
const { ChainId } = require('@pancakeswap/chains');
const { SmartRouter, SwapRouter, SMART_ROUTER_ADDRESSES } = require('@pancakeswap/smart-router/evm');

function fail(errorClass) {
  process.stdout.write(JSON.stringify({ status: 'error', error_class: errorClass }));
  process.exitCode = 1;
}

async function main() {
  const input = JSON.parse(fs.readFileSync(0, 'utf8'));
  if (!input || input.operation !== 'quote' && input.operation !== 'build') throw new Error('invalid_operation');
  if (input.side !== 'buy' && input.side !== 'sell') throw new Error('invalid_side');
  if (!input.rpcUrl || !input.token || (input.operation === 'build' && !input.recipient)) throw new Error('missing_public_swap_input');

  const chainId = ChainId.BSC;
  const publicClient = createPublicClient({
    chain: bsc,
    transport: http(input.rpcUrl, { timeout: 15000 }),
    batch: { multicall: { batchSize: 1024 * 200 } },
  });
  const token = new Token(chainId, input.token, Number(input.tokenDecimals), 'TOKEN');
  const native = Native.onChain(chainId);
  const inputCurrency = input.side === 'buy' ? native : token;
  const outputCurrency = input.side === 'buy' ? token : native;
  const amount = CurrencyAmount.fromRawAmount(inputCurrency, BigInt(input.amountRaw));

  const [v2Pools, v3Pools] = await Promise.all([
    SmartRouter.getV2CandidatePools({
      onChainProvider: () => publicClient,
      currencyA: inputCurrency,
      currencyB: outputCurrency,
    }),
    SmartRouter.getV3CandidatePools({
      onChainProvider: () => publicClient,
      // Keep this bounded to on-chain discovery plus the SDK's static fallback.
      // No guessed or stale third-party subgraph endpoint is used.
      subgraphFallback: false,
      currencyA: inputCurrency,
      currencyB: outputCurrency,
    }),
  ]);
  const pools = [...v2Pools, ...v3Pools];
  if (pools.length === 0) throw new Error('no_pancakeswap_candidate_pools');
  const quoteProvider = SmartRouter.createQuoteProvider({ onChainProvider: () => publicClient });
  const trade = await SmartRouter.getBestTrade(
    amount,
    outputCurrency,
    TradeType.EXACT_INPUT,
    {
      gasPriceWei: () => publicClient.getGasPrice(),
      maxHops: 3,
      maxSplits: 4,
      poolProvider: SmartRouter.createStaticPoolProvider(pools),
      quoteProvider,
      quoterOptimization: true,
    },
  );
  if (!trade) throw new Error('no_pancakeswap_trade');
  const outputRaw = trade.outputAmount.quotient;
  const priceImpact = SmartRouter.getPriceImpact(trade);
  if (!priceImpact) throw new Error('price_impact_unavailable');
  const route = trade.routes.map((routeItem) => ({
    type: String(routeItem.type),
    percent: String(routeItem.percent ?? 100),
    pools: routeItem.pools.map((pool) => SmartRouter.getPoolAddress(pool)),
  }));
  if (!route.length || route.some((item) => !item.pools.length || item.pools.some((pool) => !pool))) {
    throw new Error('route_unavailable');
  }
  const slippageBps = Number(input.slippageBps);
  if (!Number.isInteger(slippageBps) || slippageBps < 1 || slippageBps > 5000) throw new Error('invalid_slippage');
  const minimumOutputRaw = (outputRaw * BigInt(10000 - slippageBps)) / 10000n;
  if (minimumOutputRaw <= 0n) throw new Error('minimum_output_is_zero');
  const routerAddress = SMART_ROUTER_ADDRESSES[chainId];
  const response = {
    status: 'ok',
    routerAddress,
    inputRaw: trade.inputAmount.quotient.toString(),
    outputRaw: outputRaw.toString(),
    priceImpactPct: priceImpact.toSignificant(8),
    route,
    minimumOutputRaw: minimumOutputRaw.toString(),
    deadline: Number(input.deadline),
  };
  if (input.operation === 'build') {
    const parameters = SwapRouter.swapCallParameters(trade, {
      recipient: input.recipient,
      slippageTolerance: new Percent(slippageBps, 10000),
      deadlineOrPreviousBlockhash: Number(input.deadline),
    });
    response.to = routerAddress;
    response.data = parameters.calldata;
    response.value = parameters.value;
  }
  process.stdout.write(JSON.stringify(response));
}

main().catch((error) => fail(error && error.name ? error.name : 'smart_router_error'));
