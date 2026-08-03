"""Read-only Pump bonding-curve and PumpSwap state adapters.

Account offsets and PDA seeds in this module are taken from the official Pump
program/PumpSwap IDLs. The module never builds an instruction or sends a
transaction. If an account layout is shorter or unknown, the result is
``UNKNOWN`` rather than a guessed state.
"""

from __future__ import annotations

import base64
import struct
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, Mapping

from meme_system.adapters.protocols import ExecutableQuote
from meme_system.adapters.solana_readonly import SolanaReadOnlyError, SolanaRpcClient


PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMPSWAP_PROGRAM_ID = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
SOL_MINT = "So11111111111111111111111111111111111111112"


class PumpReadOnlyError(RuntimeError):
    def __init__(self, message: str, *, error_class: str) -> None:
        self.error_class = error_class
        super().__init__(message[:500])


@dataclass(frozen=True)
class PumpCurveState:
    mint: str
    account_address: str | None
    status: str
    virtual_token_reserves: int | None
    virtual_sol_reserves: int | None
    real_token_reserves: int | None
    real_sol_reserves: int | None
    token_total_supply: int | None
    complete: bool | None
    observed_slot: int | None
    error_class: str | None = None


@dataclass(frozen=True)
class PumpSwapPoolState:
    mint: str
    pool_address: str | None
    quote_mint: str | None
    pool_base_token_account: str | None
    pool_quote_token_account: str | None
    lp_supply: int | None
    virtual_quote_reserves: int | None
    observed_slot: int | None
    status: str
    error_class: str | None = None


@dataclass(frozen=True)
class PumpMarketState:
    mint: str
    state: str
    bonding_curve: PumpCurveState | None
    pumpswap_pool: PumpSwapPoolState | None
    observed_slot: int | None


class PumpSwapReadOnlyAdapter:
    """Find and decode canonical PumpSwap pools by the official Pool layout."""

    POOL_BASE_MINT_OFFSET = 43  # discriminator 8 + bump 1 + index 2 + creator 32
    MIN_POOL_DATA_BYTES = 261  # through Pool.virtual_quote_reserves

    def __init__(self, rpc: SolanaRpcClient) -> None:
        self.rpc = rpc

    def find_pool(self, mint: str) -> PumpSwapPoolState | None:
        try:
            accounts = self.rpc.get_program_accounts(
                PUMPSWAP_PROGRAM_ID,
                filters=[{"memcmp": {"offset": self.POOL_BASE_MINT_OFFSET, "bytes": mint}}],
            )
        except SolanaReadOnlyError as exc:
            return PumpSwapPoolState(mint, None, None, None, None, None, None, None, "UNKNOWN", exc.error_class)
        for account in accounts:
            address = account.get("pubkey")
            data = _account_bytes(account)
            if not isinstance(address, str) or data is None or len(data) < self.MIN_POOL_DATA_BYTES:
                continue
            try:
                return _decode_pool(mint, address, data, _context_slot(account))
            except (ValueError, struct.error):
                continue
        return None


class PumpReadOnlyAdapter:
    """Inspect a Pump curve and classify migration without any write path."""

    MIN_CURVE_DATA_BYTES = 49  # discriminator + 5 u64 + complete bool

    def __init__(self, rpc: SolanaRpcClient, *, pumpswap: PumpSwapReadOnlyAdapter | None = None) -> None:
        self.rpc = rpc
        self.pumpswap = pumpswap or PumpSwapReadOnlyAdapter(rpc)

    def inspect(self, mint: str, *, bonding_curve_address: str | None = None) -> PumpMarketState:
        try:
            curve_address = bonding_curve_address or _find_pda([b"bonding-curve", _b58decode(mint)], PUMP_PROGRAM_ID)[0]
        except (ValueError, PumpReadOnlyError) as exc:
            error_class = exc.error_class if isinstance(exc, PumpReadOnlyError) else "pump_invalid_mint"
            return PumpMarketState(mint, "UNKNOWN", None, None, None)
        try:
            account = self.rpc.get_account_info(curve_address, encoding="base64")
        except SolanaReadOnlyError as exc:
            curve = PumpCurveState(mint, curve_address, "UNKNOWN", None, None, None, None, None, None, None, exc.error_class)
            return PumpMarketState(mint, "UNKNOWN", curve, None, None)
        if account is None:
            curve = PumpCurveState(mint, curve_address, "ROUTE_UNAVAILABLE", None, None, None, None, None, None, None, "pump_curve_missing")
            return PumpMarketState(mint, "ROUTE_UNAVAILABLE", curve, None, None)
        try:
            data = _account_bytes(account)
            if data is None or len(data) < self.MIN_CURVE_DATA_BYTES:
                raise PumpReadOnlyError("Pump curve account layout is unavailable", error_class="pump_schema_changed")
            values = struct.unpack_from("<QQQQQ?", data, 8)
            virtual_token, virtual_sol, real_token, real_sol, supply, complete = values
            slot = _context_slot(account)
            curve_status = "MIGRATION_PENDING" if complete and real_token == 0 else "PUMP_BONDING_CURVE"
            curve = PumpCurveState(mint, curve_address, curve_status, virtual_token, virtual_sol, real_token, real_sol, supply, complete, slot)
        except (PumpReadOnlyError, struct.error, ValueError) as exc:
            error_class = exc.error_class if isinstance(exc, PumpReadOnlyError) else "pump_schema_changed"
            curve = PumpCurveState(mint, curve_address, "UNKNOWN", None, None, None, None, None, None, _context_slot(account), error_class)
            return PumpMarketState(mint, "UNKNOWN", curve, None, curve.observed_slot)
        if curve.status == "MIGRATION_PENDING":
            pool = self.pumpswap.find_pool(mint)
            if pool is not None and pool.status == "PUMPSWAP_READY":
                return PumpMarketState(mint, "PUMPSWAP_READY", curve, pool, pool.observed_slot or curve.observed_slot)
            return PumpMarketState(mint, "MIGRATION_PENDING", curve, pool, curve.observed_slot)
        return PumpMarketState(mint, curve.status, curve, None, curve.observed_slot)


class PumpProtocolReadOnlyQuoteProvider:
    """Quote a live Pump curve with the documented constant-product formula.

    PumpSwap quotes intentionally remain unavailable here until vault balances
    are supplied by a verified account-balance reader; Jupiter is the preferred
    executable-style provider after migration.
    """

    def __init__(
        self,
        state_adapter: PumpReadOnlyAdapter,
        *,
        token_decimals: Mapping[str, int] | None = None,
        decimals_resolver: Callable[[str], int | None] | None = None,
        fee_bps: int = 100,
    ) -> None:
        self.state_adapter = state_adapter
        self.token_decimals = dict(token_decimals or {})
        self.decimals_resolver = decimals_resolver
        self.fee_bps = max(0, min(10_000, fee_bps))

    def quote(self, mint: str, side: str, input_quantity: Decimal) -> ExecutableQuote:
        state = self.state_adapter.inspect(mint)
        return self.quote_state(state, side, input_quantity)

    def quote_state(
        self,
        state: PumpMarketState,
        side: str,
        input_quantity: Decimal,
    ) -> ExecutableQuote:
        """Quote the exact state already inspected by the routing decision."""
        mint = state.mint
        if state.state != "PUMP_BONDING_CURVE" or state.bonding_curve is None:
            return _unavailable_quote(mint, side, input_quantity, "pump_route_unavailable")
        curve = state.bonding_curve
        if curve.virtual_token_reserves is None or curve.virtual_sol_reserves is None:
            return _unavailable_quote(mint, side, input_quantity, "pump_curve_reserves_unavailable")
        decimals = self.token_decimals.get(mint)
        if decimals is None and self.decimals_resolver is not None:
            try:
                decimals = self.decimals_resolver(mint)
            except Exception:
                decimals = None
        if decimals is None:
            return _unavailable_quote(mint, side, input_quantity, "pump_token_decimals_missing")
        fee_factor = Decimal(10_000 - self.fee_bps) / Decimal(10_000)
        if side == "buy":
            input_raw = int(input_quantity * Decimal(10**9))
            net = Decimal(input_raw) * fee_factor
            output_raw = (Decimal(curve.virtual_token_reserves) * net / (Decimal(curve.virtual_sol_reserves) + net)).to_integral_value()
            input_display = Decimal(input_raw) / Decimal(10**9)
            output_display = Decimal(output_raw) / Decimal(10**decimals)
        elif side == "sell":
            input_raw = int(input_quantity * Decimal(10**decimals))
            net = Decimal(input_raw) * fee_factor
            output_raw = (Decimal(curve.virtual_sol_reserves) * net / (Decimal(curve.virtual_token_reserves) + net)).to_integral_value()
            input_display = Decimal(input_raw) / Decimal(10**decimals)
            output_display = Decimal(output_raw) / Decimal(10**9)
        else:
            raise ValueError("side must be buy or sell")
        now = datetime.now(timezone.utc)
        return ExecutableQuote(
            quote_id=f"pump:{mint}:{side}:{state.observed_slot}",
            mint=mint,
            side=side,
            input_quantity=input_display,
            output_quantity=output_display,
            route_fee=None,
            price_impact_pct=None,
            quoted_at=now,
            age_ms=0,
            provider="pump_bonding_curve_quote",
            route=("pump_bonding_curve",),
            quote_context_slot=state.observed_slot,
            requested_at=now,
            received_at=now,
            latency_ms=getattr(getattr(self.state_adapter, "rpc", None), "last_latency_ms", None),
            executable_style=True,
            confidence="verified",
            quote_source="pump_bonding_curve_quote",
        )


def _decode_pool(mint: str, address: str, data: bytes, slot: int | None) -> PumpSwapPoolState:
    base_mint = _b58encode(data[43:75])
    if base_mint != mint:
        raise ValueError("Pool base mint mismatch")
    quote_mint = _b58encode(data[75:107])
    pool_base = _b58encode(data[139:171])
    pool_quote = _b58encode(data[171:203])
    lp_supply = struct.unpack_from("<Q", data, 203)[0]
    virtual_quote = int.from_bytes(data[245:261], "little", signed=True)
    return PumpSwapPoolState(mint, address, quote_mint, pool_base, pool_quote, lp_supply, virtual_quote, slot, "PUMPSWAP_READY")


def _account_bytes(account: Mapping[str, object]) -> bytes | None:
    value: object = account.get("data")
    if value is None and isinstance(account.get("value"), Mapping):
        value = account["value"].get("data")
    if value is None and isinstance(account.get("account"), Mapping):
        value = account["account"].get("data")
    if isinstance(value, list) and value and isinstance(value[0], str):
        try:
            return base64.b64decode(value[0], validate=True)
        except (ValueError, base64.binascii.Error):
            return None
    if isinstance(value, str):
        try:
            return base64.b64decode(value, validate=True)
        except (ValueError, base64.binascii.Error):
            return None
    return None


def _context_slot(account: Mapping[str, object]) -> int | None:
    context = account.get("context")
    if isinstance(context, Mapping) and isinstance(context.get("slot"), int):
        return context["slot"]
    return None


def _unavailable_quote(mint: str, side: str, input_quantity: Decimal, error_class: str) -> ExecutableQuote:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    return ExecutableQuote(
        quote_id=f"pump:unavailable:{mint}:{side}",
        mint=mint,
        side=side,
        input_quantity=input_quantity,
        output_quantity=Decimal("0"),
        route_fee=None,
        price_impact_pct=None,
        quoted_at=now,
        age_ms=0,
        expires_at=now,
        route_available=False,
        liquidity_available=False,
        provider="pump_protocol",
        executable_style=False,
        confidence="unavailable",
        error_class=error_class,
    )


def _find_pda(seeds: list[bytes], program_id: str) -> tuple[str, int]:
    try:
        from solders.pubkey import Pubkey  # type: ignore

        address, bump = Pubkey.find_program_address(seeds, Pubkey.from_string(program_id))
        return str(address), int(bump)
    except ImportError:
        raise PumpReadOnlyError("solders is required for verified PDA derivation", error_class="pump_pda_dependency_missing")


def _b58decode(value: str) -> bytes:
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    number = 0
    for char in value:
        if char not in alphabet:
            raise ValueError("invalid Base58 value")
        number = number * 58 + alphabet.index(char)
    raw = number.to_bytes((number.bit_length() + 7) // 8, "big") if number else b""
    return b"\x00" * (len(value) - len(value.lstrip("1"))) + raw


def _b58encode(value: bytes) -> str:
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    number = int.from_bytes(value, "big")
    chars = ""
    while number:
        number, remainder = divmod(number, 58)
        chars = alphabet[remainder] + chars
    return alphabet[0] * (len(value) - len(value.lstrip(b"\x00"))) + chars
