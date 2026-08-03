#!/usr/bin/env node

// Official PancakeSwap Smart Router SDK boundary. The process receives only
// public read-only swap parameters; the Python parent never passes a wallet,
// key, signature, or transaction request here.

const fs = require('node:fs');
const readline = require('node:readline');
const { createPublicClient, http } = require('viem');
const { bsc } = require('viem/chains');
const { Native, Token, CurrencyAmount, TradeType, Percent } = require('@pancakeswap/sdk');
const { ChainId } = require('@pancakeswap/chains');
const { SmartRouter, SwapRouter, SMART_ROUTER_ADDRESSES } = require('@pancakeswap/smart-router/evm');

function errorResponse(error) {
  return {
    status: 'error',
    error_class: error && error.name ? error.name : 'smart_router_error',
    error_message: error && error.message ? String(error.message).slice(0, 160) : null,
  };
}

function currencyFrom(input, prefix, native) {
  const isNative = input[`${prefix}Native`];
  if (isNative === true) return native;
  const token = input[`${prefix}Token`];
  const decimals = Number(input[`${prefix}Decimals`]);
  if (!token || !Number.isInteger(decimals) || decimals < 0 || decimals > 255) {
    throw new Error(`invalid_${prefix.toLowerCase()}_currency`);
  }
  return new Token(ChainId.BSC, token, decimals, prefix.toUpperCase());
}

function normalizeCurrencies(input, native) {
  // Keep the pre-existing live build payload compatible. New quote callers
  // specify input/output explicitly so a stablecoin fundraising asset is not
  // silently treated as native BNB.
  if (Object.prototype.hasOwnProperty.call(input, 'inputNative')) {
    return {
      inputCurrency: currencyFrom(input, 'input', native),
      outputCurrency: currencyFrom(input, 'output', native),
    };
  }
  if (input.side !== 'buy' && input.side !== 'sell') throw new Error('invalid_side');
  if (!input.token) throw new Error('missing_public_swap_input');
  const token = new Token(ChainId.BSC, input.token, Number(input.tokenDecimals), 'TOKEN');
  return {
    inputCurrency: input.side === 'buy' ? native : token,
    outputCurrency: input.side === 'buy' ? token : native,
  };
}

async function handle(input) {
  if (!input || typeof input !== 'object') throw new Error('invalid_input');
  if (input.operation === 'health') return { status: 'ok', ready: true };
  if (input.operation !== 'quote' && input.operation !== 'build') throw new Error('invalid_operation');
  if (!input.rpcUrl || (input.operation === 'build' && !input.recipient)) throw new Error('missing_public_swap_input');

  const chainId = ChainId.BSC;
  const publicClient = createPublicClient({
    chain: bsc,
    transport: http(input.rpcUrl, { timeout: 15000 }),
    batch: { multicall: { batchSize: 1024 * 200 } },
  });
  const native = Native.onChain(chainId);
  const { inputCurrency, outputCurrency } = normalizeCurrencies(input, native);
  const amount = CurrencyAmount.fromRawAmount(inputCurrency, BigInt(input.amountRaw));

  const [v2Pools, v3Pools] = await Promise.all([
    SmartRouter.getV2CandidatePools({
      onChainProvider: () => publicClient,
      currencyA: inputCurrency,
      currencyB: outputCurrency,
    }),
    SmartRouter.getV3CandidatePools({
      onChainProvider: () => publicClient,
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
  return response;
}

async function emit(input) {
  try {
    process.stdout.write(`${JSON.stringify(await handle(input))}\n`);
  } catch (error) {
    process.stdout.write(`${JSON.stringify(errorResponse(error))}\n`);
  }
}

async function main() {
  if (!process.argv.includes('--server')) {
    const input = JSON.parse(fs.readFileSync(0, 'utf8'));
    await emit(input);
    return;
  }
  const lines = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  for await (const line of lines) {
    if (!line.trim()) continue;
    try {
      await emit(JSON.parse(line));
    } catch (error) {
      process.stdout.write(`${JSON.stringify(errorResponse(error))}\n`);
    }
  }
}

main().catch((error) => {
  process.stdout.write(`${JSON.stringify(errorResponse(error))}\n`);
  process.exitCode = 1;
});
