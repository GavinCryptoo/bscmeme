"""BSC capability router for quote/build-only execution discovery.

Providers in this module never own a wallet and never sign or broadcast.  They
only return normalized quotes plus unsigned transaction requests for the one
local BSC EOA.  The existing live nonce/journal/receipt layer remains the sole
execution boundary when live trading is explicitly enabled in a later task.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence

import requests
from eth_abi import encode as abi_encode
from eth_utils import keccak

from meme_system.adapters.bitget_wallet import (
    BITGET_CHAIN,
    BITGET_CHAIN_ID,
    BitgetWalletApiClient,
    BitgetWalletError,
    classify_bitget_failure,
)
from meme_system.adapters.bsc_quote import (
    BscQuoteUnavailable,
    BscReadOnlyQuoteProvider,
    FlapContext,
    FourMemeContext,
    ZERO_ADDRESS,
)
from meme_system.adapters.bsc_wss import normalize_bsc_address
from meme_system.adapters.protocols import ExecutableQuote


NATIVE_EVM = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
FOUR_MEME_TOKEN_MANAGER2 = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"
_FLAP_SWAP_EXACT_INPUT_SELECTOR = "0x" + keccak(
    text="swapExactInput((address,address,uint256,uint256,bytes))"
)[:4].hex()
_FOUR_BUY_AMAP_SELECTOR = "0x" + keccak(
    text="buyTokenAMAP(address,uint256,uint256)"
)[:4].hex()
_FOUR_SELL_MIN_SELECTOR = "0x" + keccak(
    text="sellToken(uint256,address,uint256,uint256)"
)[:4].hex()
_APPROVE_SELECTOR = "0x" + keccak(text="approve(address,uint256)")[:4].hex()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _decimal(value: Any) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _raw(quantity: Decimal, decimals: int) -> int:
    return int(quantity * (Decimal(10) ** decimals))


def _human(quantity: Any, decimals: int) -> Decimal:
    return Decimal(str(quantity)) / (Decimal(10) ** decimals)


def _integer(value: Any) -> int:
    if value in (None, ""):
        return 0
    text = str(value)
    return int(text, 16) if text.lower().startswith("0x") else int(text)


def _native_value(value: Any) -> int:
    text = str(value or "0")
    return int(Decimal(text) * (Decimal(10) ** 18)) if "." in text else _integer(text)


def _failure(response: requests.Response) -> str:
    if response.status_code == 429:
        return "RATE_LIMIT"
    if response.status_code in {408, 504}:
        return "REQUEST_TIMEOUT"
    if response.status_code >= 500:
        return "PROVIDER_UNAVAILABLE"
    try:
        text = json.dumps(response.json(), ensure_ascii=False).lower()
    except ValueError:
        text = response.text.lower()
    if "no route" in text or "unable to find" in text or "no routes" in text:
        return "NO_ROUTE"
    if "liquidity" in text:
        return "INSUFFICIENT_LIQUIDITY"
    if "token" in text and ("unsupported" in text or "invalid" in text):
        return "TOKEN_UNSUPPORTED"
    return f"HTTP_{response.status_code}"


@dataclass(frozen=True)
class UnsignedTransaction:
    to: str
    data: str
    value: int
    gas: int | None = None
    gas_price: int | None = None
    chain_id: int = 56
    purpose: str = "swap"


@dataclass(frozen=True)
class RouteResult:
    provider: str
    token: str
    side: str
    input_quantity: Decimal
    output_quantity: Decimal | None
    latency_ms: int
    transaction: UnsignedTransaction | None = None
    approval: UnsignedTransaction | None = None
    gas_hint: int | None = None
    gas_fee_usd: Decimal | None = None
    provider_fee_usd: Decimal | None = None
    lp_fee_usd: Decimal | None = None
    fee_detail: str | None = None
    price_impact_pct: Decimal | None = None
    route: tuple[str, ...] = ()
    quoted_at: datetime | None = None
    failure_reason: str | None = None
    raw_response_hash: str | None = None
    build_failure_reason: str | None = None

    @property
    def ok(self) -> bool:
        return self.output_quantity is not None and self.output_quantity > 0 and self.failure_reason is None


@dataclass(frozen=True)
class RoundtripResult:
    provider: str
    token: str
    buy: RouteResult
    sell: RouteResult | None

    @property
    def ok(self) -> bool:
        return self.buy.ok and self.sell is not None and self.sell.ok


class RpcTokenMetadata:
    def __init__(self, web3: Any) -> None:
        self.web3 = web3
        self._decimals: dict[str, int] = {}

    def decimals(self, token: str) -> int:
        normalized = normalize_bsc_address(token)
        if normalized is None:
            raise ValueError("INVALID_TOKEN")
        if normalized not in self._decimals:
            abi = [{"inputs": [], "name": "decimals", "outputs": [{"type": "uint8"}], "stateMutability": "view", "type": "function"}]
            value = int(self.web3.eth.contract(address=self.web3.to_checksum_address(normalized), abi=abi).functions.decimals().call())
            if value < 0 or value > 36:
                raise ValueError("INVALID_DECIMALS")
            self._decimals[normalized] = value
        return self._decimals[normalized]


class HttpRouteProvider:
    provider = "UNKNOWN"
    leg_delay_sec = 0.0

    def __init__(self, wallet: str, metadata: RpcTokenMetadata, *, timeout_sec: float = 15.0) -> None:
        normalized = normalize_bsc_address(wallet)
        if normalized is None:
            raise ValueError("INVALID_WALLET")
        self.wallet = normalized
        self.metadata = metadata
        self.timeout_sec = timeout_sec
        self.session = requests.Session()

    def quote_build(self, token: str, side: str, quantity: Decimal, *, build: bool = True) -> RouteResult:
        raise NotImplementedError

    def roundtrip(self, token: str, amount_bnb: Decimal, *, build: bool = False) -> RoundtripResult:
        buy = self.quote_build(token, "buy", amount_bnb, build=build)
        if buy.ok and self.leg_delay_sec > 0:
            time.sleep(self.leg_delay_sec)
        sell = self.quote_build(token, "sell", buy.output_quantity, build=build) if buy.ok and buy.output_quantity else None
        return RoundtripResult(self.provider, token, buy, sell)

    def _error(self, token: str, side: str, quantity: Decimal, started: float, reason: str) -> RouteResult:
        return RouteResult(self.provider, token, side, quantity, None, round((time.monotonic() - started) * 1000), failure_reason=reason)


class VeloraRouteProvider(HttpRouteProvider):
    provider = "VELORA"
    endpoint = "https://api.paraswap.io/swap"

    def quote_build(self, token: str, side: str, quantity: Decimal, *, build: bool = True) -> RouteResult:
        started = time.monotonic()
        try:
            decimals = self.metadata.decimals(token)
            src, dst = (NATIVE_EVM, token) if side == "buy" else (token, NATIVE_EVM)
            src_decimals, dst_decimals = (18, decimals) if side == "buy" else (decimals, 18)
            response = self.session.get(self.endpoint, params={
                "srcToken": src, "destToken": dst, "srcDecimals": src_decimals,
                "destDecimals": dst_decimals, "amount": str(_raw(quantity, src_decimals)),
                "side": "SELL", "network": 56, "version": "6.2",
                "userAddress": self.wallet, "slippage": 100,
            }, timeout=self.timeout_sec)
            if response.status_code != 200:
                return self._error(token, side, quantity, started, _failure(response))
            payload = response.json(); price = payload.get("priceRoute") or {}; tx = payload.get("txParams") or {}
            output_raw = int(price.get("destAmount") or 0)
            if output_raw <= 0:
                return self._error(token, side, quantity, started, "NO_ROUTE")
            transaction = None
            if build:
                transaction = UnsignedTransaction(
                    to=str(tx.get("to") or ""), data=str(tx.get("data") or ""), value=int(tx.get("value") or 0),
                    gas=int(tx["gas"]) if tx.get("gas") else None, gas_price=int(tx["gasPrice"]) if tx.get("gasPrice") else None,
                    chain_id=int(tx.get("chainId") or 56),
                )
            partner_fee = _decimal(price.get("partnerFee")) or Decimal(0)
            src_usd, dst_usd = _decimal(price.get("srcUSD")), _decimal(price.get("destUSD"))
            impact = ((src_usd - dst_usd) / src_usd * 100) if src_usd and dst_usd and src_usd > 0 else None
            return RouteResult(
                self.provider, token, side, quantity, _human(output_raw, dst_decimals),
                round((time.monotonic() - started) * 1000), transaction=transaction,
                gas_hint=int(price["gasCost"]) if price.get("gasCost") else None,
                gas_fee_usd=_decimal(price.get("gasCostUSD")), provider_fee_usd=Decimal(0) if partner_fee == 0 else None,
                fee_detail=f"partnerFee={partner_fee}",
                price_impact_pct=impact, route=tuple(
                    str(exchange.get("exchange")) for leg in price.get("bestRoute", [])
                    for swap in leg.get("swaps", []) for exchange in swap.get("swapExchanges", [])
                    if exchange.get("exchange")
                ), quoted_at=_now(), raw_response_hash=hashlib.sha256(response.content).hexdigest(),
            )
        except requests.Timeout:
            return self._error(token, side, quantity, started, "REQUEST_TIMEOUT")
        except requests.RequestException:
            return self._error(token, side, quantity, started, "PROVIDER_UNAVAILABLE")
        except Exception:
            return self._error(token, side, quantity, started, "INVALID_RESPONSE")


class KyberSwapRouteProvider(HttpRouteProvider):
    provider = "KYBERSWAP"
    base = "https://aggregator-api.kyberswap.com/bsc/api/v1"

    def quote_build(self, token: str, side: str, quantity: Decimal, *, build: bool = True) -> RouteResult:
        started = time.monotonic()
        try:
            decimals = self.metadata.decimals(token)
            token_in, token_out = (NATIVE_EVM, token) if side == "buy" else (token, NATIVE_EVM)
            input_decimals, output_decimals = (18, decimals) if side == "buy" else (decimals, 18)
            headers = {"x-client-id": "meme0801-universal-router"}
            response = self.session.get(self.base + "/routes", headers=headers, params={
                "tokenIn": token_in, "tokenOut": token_out,
                "amountIn": str(_raw(quantity, input_decimals)), "gasInclude": "true",
            }, timeout=self.timeout_sec)
            if response.status_code != 200:
                return self._error(token, side, quantity, started, _failure(response))
            data = response.json().get("data") or {}; summary = data.get("routeSummary") or {}
            output_raw = int(summary.get("amountOut") or 0)
            if output_raw <= 0:
                return self._error(token, side, quantity, started, "NO_ROUTE")
            transaction = None; build_data: Mapping[str, Any] = {}
            if build:
                built = self.session.post(self.base + "/route/build", headers={**headers, "Content-Type": "application/json"}, json={
                    "routeSummary": summary, "sender": self.wallet, "recipient": self.wallet,
                    "slippageTolerance": 100, "deadline": int(time.time()) + 300,
                    "source": "meme0801",
                }, timeout=self.timeout_sec)
                if built.status_code != 200:
                    return self._error(token, side, quantity, started, _failure(built))
                build_data = built.json().get("data") or {}
                transaction = UnsignedTransaction(
                    to=str(data.get("routerAddress") or build_data.get("routerAddress") or ""),
                    data=str(build_data.get("data") or ""), value=int(build_data.get("value") or (str(_raw(quantity, 18)) if side == "buy" else 0)),
                    gas=int(build_data["gas"]) if build_data.get("gas") else None,
                    gas_price=int(summary["gasPrice"]) if summary.get("gasPrice") else None,
                )
            route = tuple(str(part.get("exchange")) for path in summary.get("route", []) for part in path if part.get("exchange"))
            extra_fee = summary.get("extraFee") or {}
            return RouteResult(
                self.provider, token, side, quantity, _human(output_raw, output_decimals),
                round((time.monotonic() - started) * 1000), transaction=transaction,
                gas_hint=int(summary["gas"]) if summary.get("gas") else None,
                gas_fee_usd=_decimal(summary.get("gasUsd")), provider_fee_usd=Decimal(0) if not extra_fee or _decimal(extra_fee.get("feeAmount")) in {None, Decimal(0)} else None,
                fee_detail=json.dumps(extra_fee, sort_keys=True) if extra_fee else "extraFee=0",
                price_impact_pct=None, route=route, quoted_at=_now(),
                raw_response_hash=hashlib.sha256(response.content).hexdigest(),
            )
        except requests.Timeout:
            return self._error(token, side, quantity, started, "REQUEST_TIMEOUT")
        except requests.RequestException:
            return self._error(token, side, quantity, started, "PROVIDER_UNAVAILABLE")
        except Exception:
            return self._error(token, side, quantity, started, "INVALID_RESPONSE")


class LifiRouteProvider(HttpRouteProvider):
    provider = "LIFI"
    leg_delay_sec = 0.35
    endpoint = "https://li.quest/v1/quote"

    def quote_build(self, token: str, side: str, quantity: Decimal, *, build: bool = True) -> RouteResult:
        started = time.monotonic()
        try:
            decimals = self.metadata.decimals(token)
            source, target = (NATIVE_EVM, token) if side == "buy" else (token, NATIVE_EVM)
            input_decimals, output_decimals = (18, decimals) if side == "buy" else (decimals, 18)
            response = self.session.get(self.endpoint, params={
                "fromChain": 56, "toChain": 56, "fromToken": source, "toToken": target,
                "fromAmount": str(_raw(quantity, input_decimals)), "fromAddress": self.wallet,
                "toAddress": self.wallet, "slippage": "0.01", "order": "CHEAPEST",
            }, timeout=self.timeout_sec)
            if response.status_code != 200:
                return self._error(token, side, quantity, started, _failure(response))
            payload = response.json(); estimate = payload.get("estimate") or {}; tx = payload.get("transactionRequest") or {}
            output_raw = int(estimate.get("toAmount") or 0)
            if output_raw <= 0:
                return self._error(token, side, quantity, started, "NO_ROUTE")
            transaction = UnsignedTransaction(
                to=str(tx.get("to") or ""), data=str(tx.get("data") or ""), value=_integer(tx.get("value")),
                gas=_integer(tx["gasLimit"]) if tx.get("gasLimit") else None,
                gas_price=_integer(tx["gasPrice"]) if tx.get("gasPrice") else None,
                chain_id=_integer(tx.get("chainId") or 56),
            ) if build else None
            gas_costs = estimate.get("gasCosts") or []; fee_costs = estimate.get("feeCosts") or []
            gas_usd = sum((_decimal(item.get("amountUSD")) or Decimal(0) for item in gas_costs), Decimal(0))
            provider_fee = sum((_decimal(item.get("amountUSD")) or Decimal(0) for item in fee_costs), Decimal(0))
            route = tuple(str(step.get("tool")) for step in payload.get("includedSteps", []) if step.get("tool")) or (str(payload.get("tool") or "lifi"),)
            return RouteResult(
                self.provider, token, side, quantity, _human(output_raw, output_decimals),
                round((time.monotonic() - started) * 1000), transaction=transaction,
                gas_hint=sum((int(item.get("estimate") or 0) for item in gas_costs), 0) or None,
                gas_fee_usd=gas_usd, provider_fee_usd=provider_fee,
                fee_detail=json.dumps(fee_costs, sort_keys=True) if fee_costs else "feeCosts=0",
                price_impact_pct=None, route=route, quoted_at=_now(),
                raw_response_hash=hashlib.sha256(response.content).hexdigest(),
            )
        except requests.Timeout:
            return self._error(token, side, quantity, started, "REQUEST_TIMEOUT")
        except requests.RequestException:
            return self._error(token, side, quantity, started, "PROVIDER_UNAVAILABLE")
        except Exception:
            return self._error(token, side, quantity, started, "INVALID_RESPONSE")


class ZeroXRouteProvider(HttpRouteProvider):
    provider = "0X"
    endpoint = "https://api.0x.org/swap/allowance-holder/quote"

    def __init__(self, wallet: str, metadata: RpcTokenMetadata, api_key: str, **kwargs: Any) -> None:
        if not api_key:
            raise ValueError("ZEROX_API_KEY_MISSING")
        super().__init__(wallet, metadata, **kwargs)
        self.api_key = api_key

    def quote_build(self, token: str, side: str, quantity: Decimal, *, build: bool = True) -> RouteResult:
        started = time.monotonic()
        try:
            decimals = self.metadata.decimals(token)
            source, target = (NATIVE_EVM, token) if side == "buy" else (token, NATIVE_EVM)
            input_decimals, output_decimals = (18, decimals) if side == "buy" else (decimals, 18)
            response = self.session.get(self.endpoint, headers={"0x-api-key": self.api_key, "0x-version": "v2"}, params={
                "chainId": 56, "sellToken": source, "buyToken": target,
                "sellAmount": str(_raw(quantity, input_decimals)), "taker": self.wallet, "slippageBps": 100,
            }, timeout=self.timeout_sec)
            if response.status_code != 200:
                return self._error(token, side, quantity, started, _failure(response))
            payload = response.json(); output_raw = int(payload.get("buyAmount") or 0); tx = payload.get("transaction") or {}
            if not payload.get("liquidityAvailable") or output_raw <= 0:
                return self._error(token, side, quantity, started, "NO_ROUTE")
            issues = payload.get("issues") or {}
            if issues.get("simulationIncomplete"):
                return self._error(token, side, quantity, started, "SAFETY_CHECK_FAILED")
            transaction = UnsignedTransaction(
                to=str(tx.get("to") or ""), data=str(tx.get("data") or ""), value=int(tx.get("value") or 0),
                gas=int(tx["gas"]) if tx.get("gas") else None, gas_price=int(tx["gasPrice"]) if tx.get("gasPrice") else None,
            ) if build else None
            fees = payload.get("fees") or {}
            fee_items = {name: fees.get(name) for name in ("integratorFee", "zeroExFee", "gasFee") if fees.get(name)}
            route = tuple(str(item.get("source")) for item in (payload.get("route") or {}).get("fills", []) if item.get("source"))
            return RouteResult(
                self.provider, token, side, quantity, _human(output_raw, output_decimals),
                round((time.monotonic() - started) * 1000), transaction=transaction,
                gas_hint=int(tx["gas"]) if tx.get("gas") else None,
                gas_fee_usd=None, provider_fee_usd=None, fee_detail=json.dumps(fee_items, sort_keys=True), price_impact_pct=None,
                route=route, quoted_at=_now(), raw_response_hash=hashlib.sha256(response.content).hexdigest(),
            )
        except requests.Timeout:
            return self._error(token, side, quantity, started, "REQUEST_TIMEOUT")
        except requests.RequestException:
            return self._error(token, side, quantity, started, "PROVIDER_UNAVAILABLE")
        except Exception:
            return self._error(token, side, quantity, started, "INVALID_RESPONSE")


class BitgetAggregateRouteProvider(HttpRouteProvider):
    provider = "BITGET"
    leg_delay_sec = 1.1

    def __init__(self, wallet: str, metadata: RpcTokenMetadata, client: BitgetWalletApiClient) -> None:
        super().__init__(wallet, metadata)
        self.client = client

    def quote_build(self, token: str, side: str, quantity: Decimal, *, build: bool = True) -> RouteResult:
        started = time.monotonic()
        try:
            normalized = normalize_bsc_address(token)
            if normalized is None:
                raise BitgetWalletError("INVALID_TOKEN")
            source, target = (("", normalized) if side == "buy" else (normalized, ""))
            body = {
                "fromAddress": self.wallet, "fromAmount": format(quantity, "f"),
                "fromChain": BITGET_CHAIN, "fromContract": source,
                "toAddress": self.wallet, "toChain": BITGET_CHAIN, "toContract": target,
            }
            response = self.client.quote(body); data = response.payload.get("data") or {}
            output = _decimal(data.get("toAmount"))
            if output is None or output <= 0 or not data.get("market"):
                raise BitgetWalletError("NO_ROUTE")
            fee = data.get("fee") or {}; transaction = approval = None
            build_failure = None
            if build:
                try:
                    order_body = {**body, "market": data["market"], "slippage": str(data.get("slippage") or "0.03")}
                    if data.get("toMinAmount"):
                        order_body["toMinAmount"] = data["toMinAmount"]
                    made = self.client.make_order(order_body); order = made.payload.get("data") or {}
                    txs = order.get("txs") or []
                    parsed: list[UnsignedTransaction] = []
                    for item in txs:
                        details = item.get("data") or {}
                        parsed.append(UnsignedTransaction(
                            to=str(details.get("to") or ""), data=str(details.get("data") or details.get("calldata") or ""), value=_native_value(details.get("value")),
                            gas=_integer(details["gasLimit"]) if details.get("gasLimit") else None,
                            gas_price=_integer(details["gasPrice"]) if details.get("gasPrice") else None,
                            chain_id=_integer(item.get("chainId") or details.get("chainId") or 56),
                            purpose=str(item.get("purpose") or item.get("type") or "swap"),
                        ))
                    if parsed:
                        transaction = parsed[-1]
                        approval = parsed[0] if len(parsed) > 1 else None
                    else:
                        build_failure = "BITGET_ORDER_TXS_MISSING"
                except Exception as build_exc:
                    build_failure = classify_bitget_failure(build_exc) if isinstance(build_exc, BitgetWalletError) else f"INVALID_RESPONSE_{type(build_exc).__name__.upper()}"
            gas = fee.get("gasFee") or {}; lp = fee.get("lpFee") or {}; app = fee.get("appFee") or {}; platform = fee.get("platformFee") or {}; swap = fee.get("swapFee") or {}
            provider_fee = sum((_decimal(x.get("amountInUsd")) or Decimal(0) for x in (app, platform, swap)), Decimal(0))
            return RouteResult(
                self.provider, token, side, quantity, output,
                round((time.monotonic() - started) * 1000), transaction=transaction, approval=approval,
                gas_hint=transaction.gas if transaction else None,
                gas_fee_usd=_decimal(gas.get("amountInUsd")), provider_fee_usd=provider_fee,
                lp_fee_usd=_decimal(lp.get("amountInUsd")), price_impact_pct=_decimal(data.get("priceImpact")),
                fee_detail=json.dumps({"appFee": app, "platformFee": platform, "swapFee": swap, "lpFee": lp, "gasFee": gas}, sort_keys=True),
                route=(str(data.get("market")),), quoted_at=_now(),
                raw_response_hash=hashlib.sha256(json.dumps(response.payload, sort_keys=True).encode()).hexdigest(),
                build_failure_reason=build_failure,
            )
        except Exception as exc:
            reason = classify_bitget_failure(exc) if isinstance(exc, BitgetWalletError) else f"INVALID_RESPONSE_{type(exc).__name__.upper()}"
            return self._error(token, side, quantity, started, reason)


class LaunchpadDirectRouteProvider(HttpRouteProvider):
    """Verified Flap and FourMeme V2 quote/build adapter; never broadcasts."""

    provider = "LAUNCHPAD_DIRECT"

    def __init__(self, wallet: str, metadata: RpcTokenMetadata, quote_provider: BscReadOnlyQuoteProvider, *, slippage_bps: int = 100) -> None:
        super().__init__(wallet, metadata)
        self.quote_provider = quote_provider
        self.slippage_bps = slippage_bps

    def _plan(self, context: FlapContext | FourMemeContext, quote: ExecutableQuote, side: str) -> tuple[UnsignedTransaction, UnsignedTransaction | None]:
        if isinstance(context, FlapContext):
            if context.migrated or context.status != 1:
                raise ValueError("FLAP_NOT_PREMIGRATION")
            input_decimals = 18 if side == "buy" else context.token_decimals
            output_decimals = context.token_decimals if side == "buy" else 18
            amount_raw = _raw(quote.input_quantity, input_decimals)
            minimum = _raw(quote.output_quantity, output_decimals) * (10_000 - self.slippage_bps) // 10_000
            data = _FLAP_SWAP_EXACT_INPUT_SELECTOR + abi_encode(
                ["(address,address,uint256,uint256,bytes)"],
                [(ZERO_ADDRESS if side == "buy" else context.mint, context.mint if side == "buy" else ZERO_ADDRESS, amount_raw, minimum, b"")],
            ).hex()
            swap = UnsignedTransaction(context.launchpad, data, amount_raw if side == "buy" else 0)
            approval = None if side == "buy" else UnsignedTransaction(
                context.mint, _APPROVE_SELECTOR + abi_encode(["address", "uint256"], [context.launchpad, amount_raw]).hex(), 0, purpose="approve"
            )
            return swap, approval
        if context.migrated or context.version != 2 or context.launchpad.lower() != FOUR_MEME_TOKEN_MANAGER2:
            raise ValueError("FOURMEME_PREMIGRATION_NOT_VERIFIED")
        if not context.fundraising_is_native:
            raise ValueError("FOURMEME_NON_NATIVE_QUOTE_NOT_VERIFIED")
        if side == "buy":
            funds = _raw(quote.input_quantity, 18); minimum = _raw(quote.output_quantity, context.token_decimals) * (10_000 - self.slippage_bps) // 10_000
            data = _FOUR_BUY_AMAP_SELECTOR + abi_encode(["address", "uint256", "uint256"], [context.mint, funds, minimum]).hex()
            return UnsignedTransaction(context.launchpad, data, funds), None
        amount = _raw(quote.input_quantity, context.token_decimals); minimum = _raw(quote.output_quantity, 18) * (10_000 - self.slippage_bps) // 10_000
        data = _FOUR_SELL_MIN_SELECTOR + abi_encode(["uint256", "address", "uint256", "uint256"], [0, context.mint, amount, minimum]).hex()
        approval = UnsignedTransaction(context.mint, _APPROVE_SELECTOR + abi_encode(["address", "uint256"], [context.launchpad, amount]).hex(), 0, purpose="approve")
        return UnsignedTransaction(context.launchpad, data, 0), approval

    def quote_build(self, token: str, side: str, quantity: Decimal, *, build: bool = True) -> RouteResult:
        started = time.monotonic()
        try:
            quote, context = self.quote_provider.quote_with_venue(token, side, quantity)
            if isinstance(context, FlapContext) and not context.migrated:
                provider = "FLAP_DIRECT"
            elif isinstance(context, FourMemeContext) and not context.migrated and context.version == 2:
                provider = "FOURMEME_DIRECT"
            else:
                return self._error(token, side, quantity, started, "NOT_PREMIGRATION")
            transaction = approval = None
            if build:
                transaction, approval = self._plan(context, quote, side)
            return RouteResult(
                provider, token, side, quantity, quote.output_quantity,
                round((time.monotonic() - started) * 1000), transaction=transaction, approval=approval,
                gas_hint=None, gas_fee_usd=None, provider_fee_usd=Decimal(0),
                price_impact_pct=quote.price_impact_pct, route=quote.route,
                quoted_at=quote.quoted_at, raw_response_hash=quote.raw_response_hash,
            )
        except BscQuoteUnavailable as exc:
            return self._error(token, side, quantity, started, str(exc))
        except Exception as exc:
            return self._error(token, side, quantity, started, str(exc)[:80] or "DIRECT_ROUTE_FAILED")


class UniversalExecutionRouter:
    """Route by venue capability, with a parallel aggregator race for DEXs."""

    def __init__(
        self,
        wallet: str,
        aggregators: Sequence[HttpRouteProvider],
        direct: LaunchpadDirectRouteProvider,
        *,
        bnb_usd: Decimal | None = None,
        max_quote_age_sec: int = 30,
    ) -> None:
        self.wallet = wallet
        self.aggregators = tuple(aggregators)
        self.direct = direct
        self.bnb_usd = bnb_usd
        self.max_quote_age_sec = max_quote_age_sec

    def _fresh(self, result: RoundtripResult) -> bool:
        cutoff = _now() - timedelta(seconds=self.max_quote_age_sec)
        return all(
            leg is not None and (leg.quoted_at is None or leg.quoted_at >= cutoff)
            for leg in (result.buy, result.sell)
        )

    def _net_output(self, result: RoundtripResult) -> Decimal:
        """Expected BNB returned after explicit provider/router and gas costs.

        LP price impact is already reflected in ``output_quantity`` and is not
        subtracted a second time.  When no current BNB/USD mark is available,
        the router deliberately falls back to quoted BNB output instead of
        inventing a conversion.
        """
        assert result.sell and result.sell.output_quantity
        output = result.sell.output_quantity
        if not self.bnb_usd or self.bnb_usd <= 0:
            return output
        explicit_cost_usd = sum(
            (leg.gas_fee_usd or Decimal(0)) + (leg.provider_fee_usd or Decimal(0))
            for leg in (result.buy, result.sell)
            if leg is not None
        )
        return output - (explicit_cost_usd / self.bnb_usd)

    def roundtrip(self, token: str, amount_bnb: Decimal, *, migrate_status: int, protocol_family: str = "") -> tuple[RoundtripResult | None, tuple[RoundtripResult, ...]]:
        family = protocol_family.upper()
        if int(migrate_status) == 0:
            result = self.direct.roundtrip(token, amount_bnb, build=True)
            if result.ok:
                return result, (result,)
            # A recognized bonding-curve venue must fail closed in its native
            # adapter.  It must never leak into generic aggregators merely
            # because its SELL preview or ABI validation failed.
            recognized_direct = result.buy.provider in {"FLAP_DIRECT", "FOURMEME_DIRECT"}
            recognized_family = "FLAP" in family or "FOURMEME" in family
            if recognized_direct or recognized_family:
                return None, (result,)
            # An unrecognized pre-migration token may already expose a DEX
            # route, so it is still eligible for the aggregator race.
        with ThreadPoolExecutor(max_workers=max(1, len(self.aggregators))) as executor:
            futures = [executor.submit(provider.roundtrip, token, amount_bnb, build=True) for provider in self.aggregators]
            results = tuple(future.result() for future in as_completed(futures))
        valid = [item for item in results if item.ok and item.sell is not None and self._fresh(item)]
        if not valid:
            return None, results
        return max(valid, key=self._net_output), results


def providers_from_env(wallet: str, web3: Any) -> tuple[list[HttpRouteProvider], LaunchpadDirectRouteProvider]:
    metadata = RpcTokenMetadata(web3)
    providers: list[HttpRouteProvider] = [
        BitgetAggregateRouteProvider(wallet, metadata, BitgetWalletApiClient.from_env()),
        VeloraRouteProvider(wallet, metadata), KyberSwapRouteProvider(wallet, metadata), LifiRouteProvider(wallet, metadata),
    ]
    zero_key = os.environ.get("ZEROX_API_KEY", "").strip()
    if zero_key:
        providers.append(ZeroXRouteProvider(wallet, metadata, zero_key))
    direct = LaunchpadDirectRouteProvider(wallet, metadata, BscReadOnlyQuoteProvider.from_env())
    return providers, direct
