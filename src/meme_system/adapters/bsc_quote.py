"""Read-only BSC Paper/Shadow quotes.

Four.meme venue selection is derived from Four's on-chain Helper3 contract,
never from Binance Meme Rush metadata.  The module deliberately contains no
wallet, signer, approval, transaction, or broadcast capability.
"""

from __future__ import annotations

import hashlib
import json
import os
import select
import subprocess
import threading
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping, Sequence

from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode
from eth_utils import keccak

from meme_system.adapters.bsc_wss import BscRpcClient, normalize_bsc_address
from meme_system.adapters.protocols import ExecutableQuote


# Four.meme publishes the Helper3 ABI in its Protocol Integration documents.
# The helper returns the actual token manager, quote asset and migration flag.
FOUR_MEME_HELPER3 = "0xf251f83e40a78868fcfa3fa4599dad6494e46034"
WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
DEFAULT_BSC_RPC_URLS = (
    "https://bsc-dataseed.bnbchain.org",
    "https://bsc-dataseed-public.bnbchain.org",
)

_TOKEN_DECIMALS_SELECTOR = "0x313ce567"
_GET_TOKEN_INFO_SELECTOR = "0x" + keccak(text="getTokenInfo(address)")[:4].hex()
_GET_PANCAKE_PAIR_SELECTOR = "0x" + keccak(text="getPancakePair(address)")[:4].hex()
_TRY_BUY_SELECTOR = "0x" + keccak(text="tryBuy(address,uint256,uint256)")[:4].hex()
_TRY_SELL_SELECTOR = "0x" + keccak(text="trySell(address,uint256)")[:4].hex()
_FOUR_INFO_TYPES = (
    "uint256", "address", "address", "uint256", "uint256", "uint256",
    "uint256", "uint256", "uint256", "uint256", "uint256", "bool",
)
_TRY_BUY_TYPES = ("address", "address", "uint256", "uint256", "uint256", "uint256", "uint256", "uint256")
_TRY_SELL_TYPES = ("address", "address", "uint256", "uint256")


class BscQuoteUnavailable(RuntimeError):
    """A read-only quote cannot be verified for this candidate."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _raw_to_decimal(value: int, decimals: int) -> Decimal:
    return Decimal(value) / (Decimal(10) ** decimals)


def _decimal_to_raw(value: Decimal, decimals: int) -> int:
    raw = int(value * (Decimal(10) ** decimals))
    if raw <= 0:
        raise BscQuoteUnavailable("quote_input_is_zero")
    return raw


def _valid_decimals(value: int | None) -> bool:
    return value is not None and 0 <= value <= 36


@dataclass(frozen=True)
class FourMemeContext:
    mint: str
    token_proxy: str
    token_implementation: str | None
    launchpad: str
    fundraising_currency: str | None
    fundraising_decimals: int
    token_decimals: int
    version: int
    migrated: bool
    pancake_pair: str | None
    last_price_raw: int

    @property
    def fundraising_is_native(self) -> bool:
        return self.fundraising_currency is None


class _PancakeRouterClient:
    """One bounded Smart Router process per BSC runner.

    The process uses an NDJSON request protocol.  A timeout or malformed
    response causes one restart and one retry; it never loops indefinitely.
    """

    def __init__(self, node_binary: str, helper_path: Path) -> None:
        self.node_binary = node_binary
        self.helper_path = helper_path
        self._process: subprocess.Popen[str] | None = None
        self._lock = threading.Lock()

    def health_check(self) -> bool:
        try:
            result = self.request({"operation": "health"})
        except BscQuoteUnavailable:
            return False
        return result.get("ready") is True

    def request(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        with self._lock:
            for attempt in range(2):
                try:
                    return self._request_once(payload)
                except BscQuoteUnavailable:
                    self._stop_locked()
                    if attempt:
                        raise
            raise BscQuoteUnavailable("pancakeswap_quote_unavailable")

    def close(self) -> None:
        with self._lock:
            self._stop_locked()

    def _start_locked(self) -> subprocess.Popen[str]:
        if not self.helper_path.is_file():
            raise BscQuoteUnavailable("pancakeswap_quote_unavailable")
        try:
            process = subprocess.Popen(
                [self.node_binary, str(self.helper_path), "--server"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                bufsize=1,
            )
        except OSError as exc:
            raise BscQuoteUnavailable("pancakeswap_quote_unavailable") from exc
        self._process = process
        return process

    def _request_once(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        process = self._process
        if process is None or process.poll() is not None:
            process = self._start_locked()
        if process.stdin is None or process.stdout is None:
            raise BscQuoteUnavailable("pancakeswap_quote_unavailable")
        try:
            process.stdin.write(json.dumps(dict(payload), separators=(",", ":")) + "\n")
            process.stdin.flush()
            ready, _, _ = select.select([process.stdout], [], [], 25)
            if not ready:
                raise BscQuoteUnavailable("pancakeswap_quote_unavailable")
            line = process.stdout.readline()
            result = json.loads(line)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise BscQuoteUnavailable("pancakeswap_quote_unavailable") from exc
        if not isinstance(result, Mapping) or result.get("status") != "ok":
            raise BscQuoteUnavailable("pancakeswap_quote_unavailable")
        return result

    def _stop_locked(self) -> None:
        process = self._process
        self._process = None
        if process is None:
            return
        try:
            process.terminate()
            process.wait(timeout=2)
        except (OSError, subprocess.SubprocessError):
            try:
                process.kill()
            except OSError:
                pass


class BscReadOnlyQuoteProvider:
    """Read-only Four.meme/PancakeSwap quote provider for Paper and Shadow."""

    def __init__(
        self,
        *,
        rpc: BscRpcClient | None = None,
        rpc_url: str | None = None,
        helper_path: Path | None = None,
        node_binary: str | None = None,
        slippage_bps: int = 100,
        quote_ttl_ms: int = 1800,
        deadline_sec: int = 60,
        four_helper_address: str = FOUR_MEME_HELPER3,
        router_client: _PancakeRouterClient | None = None,
    ) -> None:
        urls = (rpc_url,) if rpc_url else ()
        self.rpc = rpc or BscRpcClient(urls or DEFAULT_BSC_RPC_URLS)
        self.helper_path = Path(helper_path) if helper_path is not None else Path("scripts/pancakeswap_smart_router.cjs")
        self.node_binary = node_binary or os.environ.get("NODE_BINARY", "node")
        self.slippage_bps = max(1, min(5000, int(slippage_bps)))
        self.quote_ttl_ms = max(250, min(10000, int(quote_ttl_ms)))
        self.deadline_sec = max(15, min(300, int(deadline_sec)))
        self.four_helper_address = normalize_bsc_address(four_helper_address) or FOUR_MEME_HELPER3
        self._contexts: dict[str, FourMemeContext | str] = {}
        self._context_lock = threading.Lock()
        self._router = router_client or _PancakeRouterClient(self.node_binary, self.helper_path)
        self._metrics_lock = threading.Lock()
        self._metrics = {
            "quote_requests": 0,
            "context_invalid": 0,
            "bonding_curve_quote_success": 0,
            "pancakeswap_quote_success": 0,
        }

    @classmethod
    def from_env(cls, *, project_root: Path | None = None) -> "BscReadOnlyQuoteProvider":
        helper = Path(os.environ.get("PANCAKESWAP_SMART_ROUTER_HELPER", "scripts/pancakeswap_smart_router.cjs"))
        if not helper.is_absolute() and project_root is not None:
            helper = project_root / helper
        return cls(
            rpc_url=os.environ.get("BSC_RPC_URL", "").strip() or None,
            helper_path=helper,
            node_binary=os.environ.get("NODE_BINARY", "node"),
            slippage_bps=int(os.environ.get("BSC_SLIPPAGE_BPS", "100")),
            quote_ttl_ms=int(os.environ.get("BSC_QUOTE_TTL_MS", "1800")),
            deadline_sec=int(os.environ.get("BSC_TRADE_DEADLINE_SEC", "60")),
            four_helper_address=os.environ.get("FOUR_MEME_HELPER3_ADDRESS", FOUR_MEME_HELPER3),
        )

    @property
    def configured(self) -> bool:
        return self.rpc.configured and self.helper_path.is_file()

    def router_health_check(self) -> bool:
        return self._router.health_check()

    def close(self) -> None:
        self._router.close()

    def metrics(self) -> dict[str, int]:
        with self._metrics_lock:
            return dict(self._metrics)

    def remember_candidate(self, mint: str, _fields: Mapping[str, object] | None = None) -> None:
        """Compatibility hook; context is resolved lazily from the chain."""
        self._context_for(mint)

    def cached_context(self, mint: str) -> FourMemeContext | None:
        """Return already-resolved venue data without triggering chain I/O."""

        token = normalize_bsc_address(mint)
        if token is None:
            return None
        with self._context_lock:
            cached = self._contexts.get(token)
        return cached if isinstance(cached, FourMemeContext) else None

    def quote_candidate(
        self,
        mint: str,
        amount_bnb: Decimal,
        _fields: Mapping[str, object] | None = None,
    ) -> tuple[ExecutableQuote | None, ExecutableQuote | None, str | None]:
        """Quote native BNB entry plus immediate all-token exit, fail-closed."""
        self._increment_metric("quote_requests")
        try:
            context = self._context_for(mint)
            if context.migrated:
                buy = self._pancake_quote(mint, "buy", amount_bnb, context)
            elif context.fundraising_is_native:
                buy = self._four_native_quote(context, "buy", amount_bnb)
            else:
                buy = self._four_stable_quote(context, "buy", amount_bnb)
            if buy.output_quantity <= 0:
                raise BscQuoteUnavailable("bonding_curve_buy_quote_unavailable")
            sell = self._quote_with_context(context, "sell", buy.output_quantity)
            if sell.output_quantity <= 0:
                raise BscQuoteUnavailable("bonding_curve_sell_quote_unavailable")
            if context.migrated:
                self._increment_metric("pancakeswap_quote_success")
            else:
                self._increment_metric("bonding_curve_quote_success")
            return buy, sell, None
        except BscQuoteUnavailable as exc:
            if str(exc) in {"fourmeme_context_unavailable", "unsupported_fundraising_asset"}:
                self._increment_metric("context_invalid")
            return None, None, str(exc)
        except Exception:
            self._increment_metric("context_invalid")
            return None, None, "fourmeme_context_unavailable"

    def _increment_metric(self, name: str) -> None:
        with self._metrics_lock:
            self._metrics[name] = self._metrics.get(name, 0) + 1

    def quote(self, mint: str, side: str, input_quantity: Decimal) -> ExecutableQuote | None:
        if side not in {"buy", "sell"} or input_quantity <= 0:
            return None
        try:
            return self._quote_with_context(self._context_for(mint), side, input_quantity)
        except BscQuoteUnavailable:
            return None

    def _quote_with_context(self, context: FourMemeContext, side: str, input_quantity: Decimal) -> ExecutableQuote:
        if context.migrated:
            return self._pancake_quote(context.mint, side, input_quantity, context)
        if context.fundraising_is_native:
            return self._four_native_quote(context, side, input_quantity)
        return self._four_stable_quote(context, side, input_quantity)

    def _context_for(self, mint: str) -> FourMemeContext:
        token = normalize_bsc_address(mint)
        if token is None:
            raise BscQuoteUnavailable("fourmeme_context_unavailable")
        with self._context_lock:
            cached = self._contexts.get(token)
        if isinstance(cached, FourMemeContext):
            return cached
        if isinstance(cached, str):
            raise BscQuoteUnavailable(cached)
        try:
            context = self._read_four_context(token)
        except BscQuoteUnavailable as exc:
            with self._context_lock:
                self._contexts[token] = str(exc)
            raise
        with self._context_lock:
            self._contexts[token] = context
        return context

    def _read_four_context(self, token: str) -> FourMemeContext:
        raw = self.rpc.call_hex(
            self.four_helper_address,
            _GET_TOKEN_INFO_SELECTOR + abi_encode(["address"], [token]).hex(),
        )
        if raw in {None, "0x"}:
            raise BscQuoteUnavailable("fourmeme_context_unavailable")
        try:
            values = abi_decode(list(_FOUR_INFO_TYPES), bytes.fromhex(raw[2:]))
            version, manager, quote, last_price, _fee_rate, _min_fee, _launch, _offers, _max_offers, _funds, _max_funds, liquidity_added = values
        except Exception as exc:
            raise BscQuoteUnavailable("fourmeme_context_unavailable") from exc
        launchpad = normalize_bsc_address(manager)
        quote_address = normalize_bsc_address(quote)
        if not isinstance(version, int) or version <= 0 or launchpad is None:
            raise BscQuoteUnavailable("fourmeme_context_unavailable")
        token_decimals = self.rpc.call_uint(token, _TOKEN_DECIMALS_SELECTOR)
        if not _valid_decimals(token_decimals):
            raise BscQuoteUnavailable("fourmeme_context_unavailable")
        fundraising_decimals = 18
        if quote_address is not None and quote_address != WBNB:
            fundraising_decimals = self.rpc.call_uint(quote_address, _TOKEN_DECIMALS_SELECTOR)
            if not _valid_decimals(fundraising_decimals):
                raise BscQuoteUnavailable("unsupported_fundraising_asset")
        pair = self.rpc.call_address(
            self.four_helper_address,
            _GET_PANCAKE_PAIR_SELECTOR + abi_encode(["address"], [token]).hex(),
        )
        migrated = bool(liquidity_added) or pair is not None
        code = self.rpc.call("eth_getCode", [token, "latest"])
        implementation = self._minimal_proxy_implementation(code)
        return FourMemeContext(
            mint=token,
            token_proxy=token,
            token_implementation=implementation,
            launchpad=launchpad,
            fundraising_currency=None if quote_address in {None, WBNB} else quote_address,
            fundraising_decimals=int(fundraising_decimals),
            token_decimals=int(token_decimals),
            version=int(version),
            migrated=migrated,
            pancake_pair=pair,
            last_price_raw=int(last_price),
        )

    def _four_native_quote(self, context: FourMemeContext, side: str, quantity: Decimal) -> ExecutableQuote:
        if side == "buy":
            funds_raw = _decimal_to_raw(quantity, 18)
            buy = self._try_buy(context, funds_raw)
            token_raw = buy[2]
            if token_raw <= 0:
                raise BscQuoteUnavailable("bonding_curve_buy_quote_unavailable")
            return self._quote(
                "bonding_curve_quote", context, side, quantity,
                _raw_to_decimal(token_raw, context.token_decimals),
                route=("fourmeme_helper3", self.four_helper_address, f"manager:{context.launchpad}", "fundraising:native"),
            )
        token_raw = _decimal_to_raw(quantity, context.token_decimals)
        sell = self._try_sell(context, token_raw)
        proceeds_raw = sell[2] - sell[3]
        if proceeds_raw <= 0:
            raise BscQuoteUnavailable("bonding_curve_sell_quote_unavailable")
        return self._quote(
            "bonding_curve_quote", context, side, quantity, _raw_to_decimal(proceeds_raw, 18),
            route=("fourmeme_helper3", self.four_helper_address, f"manager:{context.launchpad}", "fundraising:native"),
        )

    def _four_stable_quote(self, context: FourMemeContext, side: str, quantity: Decimal) -> ExecutableQuote:
        assert context.fundraising_currency is not None
        if side == "buy":
            funding_plan = self._router_request(
                input_token=None, input_decimals=18,
                output_token=context.fundraising_currency, output_decimals=context.fundraising_decimals,
                amount_raw=_decimal_to_raw(quantity, 18),
            )
            funds_raw = self._router_output_raw(funding_plan)
            buy = self._try_buy(context, funds_raw)
            token_raw = buy[2]
            if token_raw <= 0:
                raise BscQuoteUnavailable("bonding_curve_buy_quote_unavailable")
            return self._quote(
                "bonding_curve_quote", context, side, quantity,
                _raw_to_decimal(token_raw, context.token_decimals),
                route=(*self._router_route(funding_plan), "fourmeme_helper3", f"manager:{context.launchpad}", f"fundraising:{context.fundraising_currency}"),
            )
        token_raw = _decimal_to_raw(quantity, context.token_decimals)
        sell = self._try_sell(context, token_raw)
        funds_raw = sell[2] - sell[3]
        if funds_raw <= 0:
            raise BscQuoteUnavailable("bonding_curve_sell_quote_unavailable")
        settlement_plan = self._router_request(
            input_token=context.fundraising_currency, input_decimals=context.fundraising_decimals,
            output_token=None, output_decimals=18, amount_raw=funds_raw,
        )
        return self._quote(
            "bonding_curve_quote", context, side, quantity,
            _raw_to_decimal(self._router_output_raw(settlement_plan), 18),
            route=("fourmeme_helper3", f"manager:{context.launchpad}", f"fundraising:{context.fundraising_currency}", *self._router_route(settlement_plan)),
        )

    def _pancake_quote(self, mint: str, side: str, input_quantity: Decimal, context: FourMemeContext) -> ExecutableQuote:
        amount_raw = _decimal_to_raw(input_quantity, 18 if side == "buy" else context.token_decimals)
        plan = self._router_request(
            input_token=None if side == "buy" else mint,
            input_decimals=18 if side == "buy" else context.token_decimals,
            output_token=mint if side == "buy" else None,
            output_decimals=context.token_decimals if side == "buy" else 18,
            amount_raw=amount_raw,
        )
        output = _raw_to_decimal(self._router_output_raw(plan), context.token_decimals if side == "buy" else 18)
        if output <= 0:
            raise BscQuoteUnavailable("pancakeswap_quote_unavailable")
        route = self._router_route(plan)
        self._cache_pancake_pair(context, route)
        return self._quote("pancakeswap_quote", context, side, input_quantity, output, route=route)

    def _cache_pancake_pair(self, context: FourMemeContext, route: Sequence[str]) -> None:
        if context.pancake_pair is not None:
            return
        pair = next(
            (normalize_bsc_address(item.removeprefix("pool:")) for item in route if item.startswith("pool:")),
            None,
        )
        if pair is None:
            return
        with self._context_lock:
            cached = self._contexts.get(context.mint)
            if isinstance(cached, FourMemeContext) and cached.pancake_pair is None:
                self._contexts[context.mint] = replace(cached, pancake_pair=pair)

    def _try_buy(self, context: FourMemeContext, funds_raw: int) -> tuple[object, ...]:
        raw = self.rpc.call_hex(
            self.four_helper_address,
            _TRY_BUY_SELECTOR + abi_encode(["address", "uint256", "uint256"], [context.mint, 0, funds_raw]).hex(),
        )
        if raw in {None, "0x"}:
            raise BscQuoteUnavailable("bonding_curve_buy_quote_unavailable")
        try:
            values = abi_decode(list(_TRY_BUY_TYPES), bytes.fromhex(raw[2:]))
        except Exception as exc:
            raise BscQuoteUnavailable("bonding_curve_buy_quote_unavailable") from exc
        if normalize_bsc_address(values[0]) != context.launchpad:
            raise BscQuoteUnavailable("bonding_curve_buy_quote_unavailable")
        return values

    def _try_sell(self, context: FourMemeContext, token_raw: int) -> tuple[object, ...]:
        raw = self.rpc.call_hex(
            self.four_helper_address,
            _TRY_SELL_SELECTOR + abi_encode(["address", "uint256"], [context.mint, token_raw]).hex(),
        )
        if raw in {None, "0x"}:
            raise BscQuoteUnavailable("bonding_curve_sell_quote_unavailable")
        try:
            values = abi_decode(list(_TRY_SELL_TYPES), bytes.fromhex(raw[2:]))
        except Exception as exc:
            raise BscQuoteUnavailable("bonding_curve_sell_quote_unavailable") from exc
        if normalize_bsc_address(values[0]) != context.launchpad:
            raise BscQuoteUnavailable("bonding_curve_sell_quote_unavailable")
        return values

    def _router_request(self, *, input_token: str | None, input_decimals: int, output_token: str | None, output_decimals: int, amount_raw: int) -> Mapping[str, object]:
        if not self.rpc.urls:
            raise BscQuoteUnavailable("pancakeswap_quote_unavailable")
        for rpc_url in self.rpc.urls:
            try:
                return self._router.request({
                    "operation": "quote", "rpcUrl": rpc_url,
                    "inputNative": input_token is None, "inputToken": input_token,
                    "inputDecimals": input_decimals, "outputNative": output_token is None,
                    "outputToken": output_token, "outputDecimals": output_decimals,
                    "amountRaw": str(amount_raw), "slippageBps": self.slippage_bps,
                    "deadline": int(_utc_now().timestamp()) + self.deadline_sec,
                })
            except BscQuoteUnavailable:
                continue
        raise BscQuoteUnavailable("pancakeswap_quote_unavailable")

    def _quote(self, source: str, context: FourMemeContext, side: str, input_quantity: Decimal, output_quantity: Decimal, *, route: Sequence[str]) -> ExecutableQuote:
        now = _utc_now()
        return ExecutableQuote(
            quote_id=self._quote_id(source, context.mint, side, input_quantity, output_quantity, now),
            mint=context.mint, side=side, input_quantity=input_quantity, output_quantity=output_quantity,
            route_fee=None, price_impact_pct=None, quoted_at=now, age_ms=0,
            expires_at=now + timedelta(milliseconds=self.quote_ttl_ms), provider=source,
            route=tuple(route), executable_style=True, confidence="verified",
            requested_at=now, received_at=now, quote_source=source,
        )

    @staticmethod
    def _router_output_raw(plan: Mapping[str, object]) -> int:
        try:
            value = int(str(plan["outputRaw"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise BscQuoteUnavailable("pancakeswap_quote_unavailable") from exc
        if value <= 0:
            raise BscQuoteUnavailable("pancakeswap_quote_unavailable")
        return value

    @staticmethod
    def _router_route(plan: Mapping[str, object]) -> tuple[str, ...]:
        router = normalize_bsc_address(plan.get("routerAddress"))
        items = plan.get("route")
        if router is None or not isinstance(items, list) or not items:
            raise BscQuoteUnavailable("pancakeswap_quote_unavailable")
        route = [f"router:{router}"]
        for item in items:
            if not isinstance(item, Mapping) or not isinstance(item.get("pools"), list):
                raise BscQuoteUnavailable("pancakeswap_quote_unavailable")
            pools = [normalize_bsc_address(pool) for pool in item["pools"]]
            if not pools or any(pool is None for pool in pools):
                raise BscQuoteUnavailable("pancakeswap_quote_unavailable")
            route.append(f"route_type:{item.get('type', 'unknown')}")
            route.extend(f"pool:{pool}" for pool in pools if pool is not None)
        return tuple(route)

    @staticmethod
    def _minimal_proxy_implementation(code: object) -> str | None:
        if not isinstance(code, str):
            return None
        marker = "363d3d373d3d3d363d73"
        value = code.lower()
        if not value.startswith("0x" + marker) or len(value) < 2 + len(marker) + 40:
            return None
        return normalize_bsc_address("0x" + value[2 + len(marker):2 + len(marker) + 40])

    @staticmethod
    def _quote_id(source: str, mint: str, side: str, input_quantity: Decimal, output_quantity: Decimal, now: datetime) -> str:
        digest = hashlib.sha256(f"{source}|{mint}|{side}|{input_quantity}|{output_quantity}|{now.isoformat()}".encode()).hexdigest()[:20]
        return f"bsc-quote:{source}:{side}:{digest}"
