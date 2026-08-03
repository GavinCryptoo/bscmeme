"""Read-only executable BSC quotes for Paper/Shadow.

This module is deliberately separate from :mod:`bsc_live`.  It can discover
and quote a public route, but it has no account, signing, allowance,
transaction, or broadcast code.

Binance Meme Rush is not consulted here.  It is only used by the discovery
adapter and by the Dashboard's optional indicative reference fields.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping, Sequence

from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode
from eth_utils import keccak

from meme_system.adapters.bsc_wss import BscRpcClient, normalize_bsc_address
from meme_system.adapters.protocols import ExecutableQuote


FOUR_MEME_TOKEN_MANAGER = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"
FOUR_MEME_RUSH_TOKEN_MANAGER = "0x87fd30d61a8dce9150e173be9bf53e7f1c55dff8"
FOUR_MEME_PROTOCOL = 2002
FOUR_MEME_BONDING_CURVE_ROUTE = "fourmeme_token_manager2"
FOUR_MEME_RUSH_BONDING_CURVE_ROUTE = "fourmeme_token_manager_rush"
DEFAULT_BSC_RPC_URLS = (
    "https://bsc-dataseed.bnbchain.org",
    "https://bsc-dataseed-public.bnbchain.org",
)

_TOKEN_DECIMALS_SELECTOR = "0x313ce567"
_TOKEN_INFOS_TYPES = (
    "address", "address", "uint256", "uint256", "uint256", "uint256",
    "uint256", "uint256", "uint256", "uint256", "uint256", "uint256",
    "uint256",
)
_RUSH_TOKEN_INFOS_TYPES = (
    "address", "address", "uint256", "uint256", "uint256", "uint256",
    "uint256", "uint256", "uint256", "uint256", "uint256", "uint256",
)
_TOKEN_INFOS_SELECTOR = "0x" + keccak(text="_tokenInfos(address)")[:4].hex()
_CALC_TRADING_FEE_SELECTOR = "0x" + keccak(
    text="calcTradingFee((address,address,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256),uint256)"
)[:4].hex()
_CALC_BUY_AMOUNT_SELECTOR = "0x" + keccak(
    text="calcBuyAmount((address,address,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256),uint256)"
)[:4].hex()
_CALC_SELL_COST_SELECTOR = "0x" + keccak(
    text="calcSellCost((address,address,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256),uint256)"
)[:4].hex()
_RUSH_CALC_BUY_AMOUNT_SELECTOR = "0x" + keccak(
    text="calcBuyAmount((address,address,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256),uint256)"
)[:4].hex()
_RUSH_CALC_SELL_COST_SELECTOR = "0x" + keccak(
    text="calcSellCost((address,address,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256),uint256)"
)[:4].hex()
_RUSH_CALC_TRADING_FEE_SELECTOR = "0x" + keccak(text="calcTradingFee(uint256)")[:4].hex()
_NATIVE_QUOTE_ADDRESSES = frozenset({
    "0x0000000000000000000000000000000000000000",
    "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
})


class BscQuoteUnavailable(RuntimeError):
    """A candidate cannot be valued with a current executable quote."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _positive_decimal(value: object) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


def _raw_to_decimal(value: int, decimals: int) -> Decimal:
    return Decimal(value) / (Decimal(10) ** decimals)


def _decimal_to_raw(value: Decimal, decimals: int) -> int:
    scale = Decimal(10) ** decimals
    raw = int(value * scale)
    if raw <= 0:
        raise BscQuoteUnavailable("quote_input_is_zero")
    return raw


def _quote_error(exc: BaseException) -> str:
    value = str(exc).strip().splitlines()[0] if str(exc).strip() else "quote_unavailable"
    return value[:120].replace(" ", "_")


@dataclass(frozen=True)
class BscMarketHint:
    pair_address: str | None = None
    bonding_curve_address: str | None = None
    migrate_status: int | None = None
    protocol: int | None = None
    token_version: int | None = None
    token_decimals: int | None = None

    @property
    def is_four_bonding_curve(self) -> bool:
        # A confirmed pair takes precedence over stale migration metadata.
        return (
            self.protocol == FOUR_MEME_PROTOCOL
            and self.migrate_status == 0
            and self.pair_address is None
        )

    @property
    def venue(self) -> str:
        return "bonding_curve" if self.is_four_bonding_curve else "pancakeswap"


@dataclass(frozen=True)
class _FourCurveSpec:
    manager: str
    route_name: str
    token_info_types: tuple[str, ...]
    last_price_index: int
    fee_selector: str
    buy_selector: str
    sell_selector: str
    fee_uses_token_info: bool


class BscReadOnlyQuoteProvider:
    """Fetch current BSC quotes through a Router or verified curve view calls."""

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
    ) -> None:
        urls = (rpc_url,) if rpc_url else ()
        self.rpc = rpc or BscRpcClient(urls or DEFAULT_BSC_RPC_URLS)
        self.helper_path = helper_path or Path("scripts/pancakeswap_smart_router.cjs")
        self.node_binary = node_binary or os.environ.get("NODE_BINARY", "node")
        self.slippage_bps = max(1, min(5000, int(slippage_bps)))
        self.quote_ttl_ms = max(250, min(10000, int(quote_ttl_ms)))
        self.deadline_sec = max(15, min(300, int(deadline_sec)))
        self._hints: dict[str, BscMarketHint] = {}

    @classmethod
    def from_env(cls, *, project_root: Path | None = None) -> "BscReadOnlyQuoteProvider":
        helper = Path(os.environ.get("PANCAKESWAP_SMART_ROUTER_HELPER", "scripts/pancakeswap_smart_router.cjs"))
        if not helper.is_absolute() and project_root is not None:
            helper = project_root / helper
        rpc_url = os.environ.get("BSC_RPC_URL", "").strip() or None
        return cls(
            rpc_url=rpc_url,
            helper_path=helper,
            node_binary=os.environ.get("NODE_BINARY", "node"),
            slippage_bps=int(os.environ.get("BSC_SLIPPAGE_BPS", "100")),
            quote_ttl_ms=int(os.environ.get("BSC_QUOTE_TTL_MS", "1800")),
            deadline_sec=int(os.environ.get("BSC_TRADE_DEADLINE_SEC", "60")),
        )

    @property
    def configured(self) -> bool:
        return self.rpc.configured and self.helper_path.is_file()

    def remember_candidate(self, mint: str, fields: Mapping[str, object]) -> BscMarketHint:
        hint = BscMarketHint(
            pair_address=normalize_bsc_address(fields.get("pair_address")),
            bonding_curve_address=normalize_bsc_address(fields.get("bonding_curve_address")),
            migrate_status=self._int_or_none(fields.get("migrate_status")),
            protocol=self._int_or_none(fields.get("protocol")),
            token_version=self._int_or_none(fields.get("token_version")),
            token_decimals=self._int_or_none(fields.get("token_decimals")),
        )
        self._hints[mint.lower()] = hint
        return hint

    def quote_candidate(
        self,
        mint: str,
        amount_bnb: Decimal,
        fields: Mapping[str, object],
    ) -> tuple[ExecutableQuote | None, ExecutableQuote | None, str | None]:
        """Return buy plus immediate full-size sell quotes, fail-closed."""

        hint = self.remember_candidate(mint, fields)
        try:
            if hint.venue == "bonding_curve":
                buy = self._four_quote(mint, "buy", amount_bnb, hint)
            else:
                buy = self._pancake_quote(mint, "buy", amount_bnb, hint)
            if buy is None or buy.output_quantity <= 0:
                raise BscQuoteUnavailable("buy_quote_unavailable")
            sell = self.quote(mint, "sell", buy.output_quantity)
            if sell is None or sell.output_quantity <= 0:
                raise BscQuoteUnavailable("sell_quote_unavailable")
            return buy, sell, None
        except BscQuoteUnavailable as exc:
            return None, None, _quote_error(exc)
        except Exception as exc:
            return None, None, _quote_error(exc)

    def quote(self, mint: str, side: str, input_quantity: Decimal) -> ExecutableQuote | None:
        if side not in {"buy", "sell"} or input_quantity <= 0:
            return None
        hint = self._hints.get(mint.lower(), BscMarketHint())
        try:
            if hint.venue == "bonding_curve":
                return self._four_quote(mint, side, input_quantity, hint)
            return self._pancake_quote(mint, side, input_quantity, hint)
        except Exception:
            return None

    def _pancake_quote(
        self,
        mint: str,
        side: str,
        input_quantity: Decimal,
        hint: BscMarketHint,
    ) -> ExecutableQuote:
        decimals = hint.token_decimals
        if decimals is None:
            decimals = self.rpc.call_uint(mint, _TOKEN_DECIMALS_SELECTOR)
        if decimals is None or decimals < 0 or decimals > 36:
            raise BscQuoteUnavailable("token_decimals_unavailable")
        amount_raw = _decimal_to_raw(input_quantity, 18 if side == "buy" else decimals)
        plan = self._router_request(mint, side, amount_raw, decimals)
        try:
            output_raw = int(str(plan["outputRaw"]))
            impact = Decimal(str(plan["priceImpactPct"]))
            router = normalize_bsc_address(plan.get("routerAddress"))
            route_items = plan["route"]
        except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
            raise BscQuoteUnavailable("router_quote_metadata_invalid") from exc
        if output_raw <= 0 or router is None or not isinstance(route_items, list) or not route_items:
            raise BscQuoteUnavailable("router_quote_invalid")
        if not impact.is_finite() or impact < 0:
            raise BscQuoteUnavailable("price_impact_unavailable")
        route = [f"router:{router}"]
        for item in route_items:
            if not isinstance(item, Mapping):
                raise BscQuoteUnavailable("route_unavailable")
            route_type = str(item.get("type", "unknown"))
            pools = item.get("pools")
            if not isinstance(pools, list) or not pools or any(not normalize_bsc_address(p) for p in pools):
                raise BscQuoteUnavailable("route_unavailable")
            route.append(f"route_type:{route_type}")
            route.extend(f"pool:{normalize_bsc_address(pool)}" for pool in pools)
        now = _utc_now()
        output_decimals = decimals if side == "buy" else 18
        output_quantity = _raw_to_decimal(output_raw, output_decimals)
        return ExecutableQuote(
            quote_id=self._quote_id("pancakeswap", mint, side, input_quantity, output_quantity, now),
            mint=mint,
            side=side,
            input_quantity=input_quantity,
            output_quantity=output_quantity,
            route_fee=None,
            price_impact_pct=impact,
            quoted_at=now,
            age_ms=0,
            expires_at=now + timedelta(milliseconds=self.quote_ttl_ms),
            provider="pancakeswap_router",
            route=tuple(route),
            executable_style=True,
            confidence="verified",
            requested_at=now,
            received_at=now,
            quote_source="pancakeswap_router",
        )

    def _four_quote(
        self,
        mint: str,
        side: str,
        input_quantity: Decimal,
        hint: BscMarketHint,
    ) -> ExecutableQuote:
        decimals = hint.token_decimals
        if decimals is None:
            decimals = self.rpc.call_uint(mint, _TOKEN_DECIMALS_SELECTOR)
        if decimals is None or decimals < 0 or decimals > 36:
            raise BscQuoteUnavailable("token_decimals_unavailable")
        curve = self._four_curve_spec(hint)
        info = self._four_token_info(mint, curve)
        if info[0].lower() != mint.lower() or info[1].lower() not in _NATIVE_QUOTE_ADDRESSES:
            # The curve quote must be for the actual token and native quote.
            raise BscQuoteUnavailable("bonding_curve_quote_context_invalid")
        input_raw = _decimal_to_raw(input_quantity, 18 if side == "buy" else decimals)
        if side == "buy":
            fee = self._four_fee(curve, info, input_raw)
            if fee is None or fee >= input_raw:
                raise BscQuoteUnavailable("bonding_curve_fee_unavailable")
            output_raw = self._four_call_uint(curve, curve.buy_selector, info, input_raw - fee)
            if output_raw is None or output_raw <= 0:
                raise BscQuoteUnavailable("bonding_curve_exact_buy_unavailable")
        else:
            output_raw = self._four_call_uint(curve, curve.sell_selector, info, input_raw)
            if output_raw is None or output_raw <= 0:
                raise BscQuoteUnavailable("bonding_curve_exact_sell_unavailable")
        last_price_raw = info[curve.last_price_index]
        if last_price_raw <= 0:
            raise BscQuoteUnavailable("bonding_curve_price_unavailable")
        output_decimals = decimals if side == "buy" else 18
        output_quantity = _raw_to_decimal(output_raw, output_decimals)
        if output_quantity <= 0:
            raise BscQuoteUnavailable("bonding_curve_amount_out_zero")
        # Impact is calculated from the contract's current marginal lastPrice
        # and the exact view-call output, never from Binance data.
        if side == "buy":
            execution_price = Decimal(input_raw) / Decimal(output_raw)
        else:
            execution_price = Decimal(output_raw) / Decimal(input_raw)
        marginal_price = Decimal(last_price_raw) / (Decimal(10) ** 18)
        if side == "buy":
            marginal_inverse = Decimal(1) / marginal_price
            impact = max(Decimal(0), (Decimal(1) - execution_price / marginal_inverse) * Decimal(100))
        else:
            impact = max(Decimal(0), (Decimal(1) - execution_price / marginal_price) * Decimal(100))
        now = _utc_now()
        return ExecutableQuote(
            quote_id=self._quote_id("bonding_curve", mint, side, input_quantity, output_quantity, now),
            mint=mint,
            side=side,
            input_quantity=input_quantity,
            output_quantity=output_quantity,
            route_fee=None,
            price_impact_pct=impact,
            quoted_at=now,
            age_ms=0,
            expires_at=now + timedelta(milliseconds=self.quote_ttl_ms),
            provider="bonding_curve",
            route=tuple(
                item for item in (
                    curve.route_name,
                    curve.manager,
                    hint.bonding_curve_address,
                ) if item
            ),
            executable_style=True,
            confidence="verified",
            requested_at=now,
            received_at=now,
            quote_source="bonding_curve",
        )

    @staticmethod
    def _four_curve_spec(hint: BscMarketHint) -> _FourCurveSpec:
        if hint.token_version == 4:
            return _FourCurveSpec(
                manager=FOUR_MEME_RUSH_TOKEN_MANAGER,
                route_name=FOUR_MEME_RUSH_BONDING_CURVE_ROUTE,
                token_info_types=_RUSH_TOKEN_INFOS_TYPES,
                last_price_index=5,
                fee_selector=_RUSH_CALC_TRADING_FEE_SELECTOR,
                buy_selector=_RUSH_CALC_BUY_AMOUNT_SELECTOR,
                sell_selector=_RUSH_CALC_SELL_COST_SELECTOR,
                fee_uses_token_info=False,
            )
        return _FourCurveSpec(
            manager=FOUR_MEME_TOKEN_MANAGER,
            route_name=FOUR_MEME_BONDING_CURVE_ROUTE,
            token_info_types=_TOKEN_INFOS_TYPES,
            last_price_index=9,
            fee_selector=_CALC_TRADING_FEE_SELECTOR,
            buy_selector=_CALC_BUY_AMOUNT_SELECTOR,
            sell_selector=_CALC_SELL_COST_SELECTOR,
            fee_uses_token_info=True,
        )

    def _four_token_info(self, mint: str, curve: _FourCurveSpec) -> tuple[object, ...]:
        token = normalize_bsc_address(mint)
        if token is None:
            raise BscQuoteUnavailable("invalid_token_address")
        data = _TOKEN_INFOS_SELECTOR + abi_encode(["address"], [token]).hex()
        raw = self.rpc.call_hex(curve.manager, data)
        if raw is None:
            raise BscQuoteUnavailable("bonding_curve_state_unavailable")
        try:
            values = abi_decode(list(curve.token_info_types), bytes.fromhex(raw[2:]))
        except Exception as exc:
            raise BscQuoteUnavailable("bonding_curve_state_invalid") from exc
        if len(values) != len(curve.token_info_types):
            raise BscQuoteUnavailable("bonding_curve_state_invalid")
        return values

    def _four_fee(self, curve: _FourCurveSpec, info: tuple[object, ...], amount_raw: int) -> int | None:
        if curve.fee_uses_token_info:
            return self._four_call_uint(curve, curve.fee_selector, info, amount_raw)
        raw = self.rpc.call_hex(curve.manager, curve.fee_selector + abi_encode(["uint256"], [amount_raw]).hex())
        try:
            return int(raw, 16) if raw is not None else None
        except ValueError:
            return None

    def _four_call_uint(
        self,
        curve: _FourCurveSpec,
        selector: str,
        info: tuple[object, ...],
        amount_raw: int,
    ) -> int | None:
        tuple_type = f"({curve.token_info_types[0]},{','.join(curve.token_info_types[1:])})"
        data = selector + abi_encode([tuple_type, "uint256"], [info, amount_raw]).hex()
        raw = self.rpc.call_hex(curve.manager, data)
        if raw is None:
            return None
        try:
            return int(raw, 16)
        except ValueError:
            return None

    def _router_request(self, mint: str, side: str, amount_raw: int, decimals: int) -> Mapping[str, object]:
        if not self.helper_path.is_file():
            raise BscQuoteUnavailable("smart_router_helper_missing")
        if not self.rpc.urls:
            raise BscQuoteUnavailable("bsc_rpc_unavailable")
        last_error = "smart_router_quote_unavailable"
        # Endpoint support for Router multicalls differs.  Each configured
        # endpoint gets one bounded, read-only request before the route is
        # declared unavailable.
        for rpc_url in self.rpc.urls:
            payload = {
                "operation": "quote",
                "rpcUrl": rpc_url,
                "side": side,
                "token": mint,
                "tokenDecimals": decimals,
                "amountRaw": str(amount_raw),
                "slippageBps": self.slippage_bps,
                "deadline": int(_utc_now().timestamp()) + self.deadline_sec,
            }
            try:
                completed = subprocess.run(
                    [self.node_binary, str(self.helper_path)],
                    input=json.dumps(payload),
                    text=True,
                    capture_output=True,
                    timeout=25,
                    check=False,
                )
            except (OSError, subprocess.SubprocessError) as exc:
                raise BscQuoteUnavailable("smart_router_process_unavailable") from exc
            try:
                result = json.loads(completed.stdout or "{}")
            except json.JSONDecodeError:
                last_error = "smart_router_response_invalid"
                continue
            if completed.returncode == 0 and isinstance(result, Mapping) and result.get("status") == "ok":
                return result
            if isinstance(result, Mapping):
                last_error = str(result.get("error_class") or last_error)
        raise BscQuoteUnavailable(last_error)

    @staticmethod
    def _quote_id(source: str, mint: str, side: str, input_quantity: Decimal, output_quantity: Decimal, now: datetime) -> str:
        digest = hashlib.sha256(
            f"{source}|{mint.lower()}|{side}|{input_quantity}|{output_quantity}|{now.isoformat()}".encode()
        ).hexdigest()[:20]
        return f"bsc-quote:{source}:{side}:{digest}"

    @staticmethod
    def _int_or_none(value: object) -> int | None:
        if isinstance(value, bool) or value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
