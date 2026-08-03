"""Read-only Solana pool/curve price observations for Paper/Shadow.

This module only decodes account data received through Solana RPC/WSS.  It
does not build instructions, request a transaction, sign, or broadcast.
"""

from __future__ import annotations

import base64
import struct
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from threading import RLock
from typing import Mapping

from meme_system.adapters.pump_readonly import (
    PumpCurveState,
    PumpMarketState,
    PumpReadOnlyAdapter,
    PumpSwapPoolState,
    _account_bytes,
    _context_slot,
    _decode_pool,
)


LAMPORTS_PER_SOL = Decimal("1000000000")


@dataclass(frozen=True)
class SolanaObservedPrice:
    mint: str
    observed_at: datetime
    price_sol_per_token: Decimal
    source: str
    stage: str
    account_address: str
    observed_slot: int | None
    base_reserves_raw: int | None = None
    quote_reserves_raw: int | None = None


@dataclass(frozen=True)
class SolanaPriceBinding:
    mint: str
    stage: str
    primary_account: str
    account_addresses: tuple[str, ...]
    market_state: PumpMarketState


@dataclass
class _ObservedMarket:
    binding: SolanaPriceBinding
    curve: PumpCurveState | None = None
    pool: PumpSwapPoolState | None = None
    vault_amounts: dict[str, int] | None = None


class SolanaPriceMonitor:
    """Maintain one read-only account binding and latest price per active Mint."""

    def __init__(self, pump_adapter: PumpReadOnlyAdapter, decimals_resolver) -> None:
        self.pump_adapter = pump_adapter
        self.decimals_resolver = decimals_resolver
        self._lock = RLock()
        self._markets: dict[str, _ObservedMarket] = {}

    def register_position(self, mint: str, bonding_curve_address: str | None = None) -> SolanaPriceBinding | None:
        with self._lock:
            state = self.pump_adapter.inspect(mint, bonding_curve_address=bonding_curve_address)
            binding = self._binding_from_state(state)
            if binding is None:
                self._markets.pop(mint, None)
                return None
            market = _ObservedMarket(binding=binding, curve=state.bonding_curve, pool=state.pumpswap_pool, vault_amounts={})
            if binding.stage == "pump_bonding_curve":
                self._load_account(market, binding.primary_account)
            else:
                for address in binding.account_addresses:
                    self._load_account(market, address)
            self._markets[mint] = market
            return binding

    def unregister_missing(self, mints: set[str]) -> None:
        with self._lock:
            for mint in tuple(self._markets):
                if mint not in mints:
                    self._markets.pop(mint, None)

    def bindings(self) -> tuple[SolanaPriceBinding, ...]:
        with self._lock:
            return tuple(market.binding for market in self._markets.values())

    def subscriptions(self) -> tuple[tuple[str, list[object]], ...]:
        with self._lock:
            result: list[tuple[str, list[object]]] = []
            for market in self._markets.values():
                for address in market.binding.account_addresses:
                    result.append((
                        "accountSubscribe",
                        [address, {"encoding": "base64", "commitment": "processed"}],
                    ))
            return tuple(result)

    def process_wss_event(self, event: Mapping[str, object]) -> SolanaObservedPrice | None:
        if event.get("method") != "accountNotification":
            return None
        with self._lock:
            return self._process_wss_event_locked(event)

    def _process_wss_event_locked(self, event: Mapping[str, object]) -> SolanaObservedPrice | None:
        metadata = event.get("_subscription_params")
        if not isinstance(metadata, list) or not metadata or not isinstance(metadata[0], str):
            return None
        address = metadata[0]
        params = event.get("params")
        if not isinstance(params, Mapping):
            return None
        result = params.get("result")
        if not isinstance(result, Mapping):
            return None
        value = result.get("value")
        if not isinstance(value, Mapping):
            return None
        encoded = value.get("data")
        data = self._decode_data(encoded)
        if data is None:
            return None
        context = result.get("context")
        slot = context.get("slot") if isinstance(context, Mapping) and isinstance(context.get("slot"), int) else None
        for market in tuple(self._markets.values()):
            if address not in market.binding.account_addresses:
                continue
            now = datetime.now(timezone.utc)
            if market.binding.stage == "pump_bonding_curve":
                try:
                    values = struct.unpack_from("<QQQQQ?", data, 8)
                    market.curve = PumpCurveState(market.binding.mint, address, "PUMP_BONDING_CURVE", *values[:5], values[5], slot)
                except (ValueError, IndexError, struct.error):
                    return None
                if market.curve.complete and market.curve.real_token_reserves == 0:
                    self.register_position(market.binding.mint, bonding_curve_address=address)
                    return None
                price = self._curve_price(market.curve)
                if price is None:
                    return None
                return SolanaObservedPrice(market.binding.mint, now, price, "pool_wss_indicative", "pump_bonding_curve", address, slot, market.curve.virtual_token_reserves, market.curve.virtual_sol_reserves)
            if market.pool is not None and address == market.binding.primary_account:
                try:
                    market.pool = _decode_pool(market.binding.mint, address, data, slot)
                except (ValueError, struct.error):
                    return None
            else:
                amount = self._token_amount(data)
                if amount is None:
                    return None
                market.vault_amounts[address] = amount
            return self._pool_price(market, now, slot)
        return None

    def latest(self, mint: str) -> SolanaObservedPrice | None:
        with self._lock:
            market = self._markets.get(mint)
            if market is None:
                return None
            now = datetime.now(timezone.utc)
            if market.binding.stage == "pump_bonding_curve" and market.curve is not None:
                price = self._curve_price(market.curve)
                if price is not None:
                    return SolanaObservedPrice(mint, now, price, "pool_wss_indicative", market.binding.stage, market.binding.primary_account, market.curve.observed_slot, market.curve.virtual_token_reserves, market.curve.virtual_sol_reserves)
            return self._pool_price(market, now, None)

    def _binding_from_state(self, state: PumpMarketState) -> SolanaPriceBinding | None:
        if state.state == "PUMP_BONDING_CURVE" and state.bonding_curve and state.bonding_curve.account_address:
            address = state.bonding_curve.account_address
            return SolanaPriceBinding(state.mint, "pump_bonding_curve", address, (address,), state)
        pool = state.pumpswap_pool
        if state.state == "PUMPSWAP_READY" and pool and pool.pool_address and pool.pool_base_token_account and pool.pool_quote_token_account:
            addresses = (pool.pool_address, pool.pool_base_token_account, pool.pool_quote_token_account)
            return SolanaPriceBinding(state.mint, "pumpswap_pool", pool.pool_address, addresses, state)
        return None

    def _load_account(self, market: _ObservedMarket, address: str) -> None:
        try:
            account = self.pump_adapter.rpc.get_account_info(address, encoding="base64", commitment="processed")
        except Exception:
            return
        if account is None:
            return
        data = _account_bytes(account)
        if data is None:
            return
        slot = _context_slot(account)
        if market.binding.stage == "pump_bonding_curve":
            try:
                values = struct.unpack_from("<QQQQQ?", data, 8)
                market.curve = PumpCurveState(market.binding.mint, address, "PUMP_BONDING_CURVE", *values[:5], values[5], slot)
            except (struct.error, ValueError):
                return
        elif address == market.binding.primary_account:
            try:
                market.pool = _decode_pool(market.binding.mint, address, data, slot)
            except (struct.error, ValueError):
                return
        else:
            amount = self._token_amount(data)
            if amount is not None:
                market.vault_amounts = market.vault_amounts or {}
                market.vault_amounts[address] = amount

    def _curve_price(self, curve: PumpCurveState) -> Decimal | None:
        if curve.virtual_token_reserves is None or curve.virtual_sol_reserves is None or curve.virtual_token_reserves <= 0:
            return None
        decimals = self._decimals(curve.mint)
        if decimals is None:
            return None
        return (Decimal(curve.virtual_sol_reserves) / LAMPORTS_PER_SOL) / (Decimal(curve.virtual_token_reserves) / Decimal(10) ** decimals)

    def _pool_price(self, market: _ObservedMarket, observed_at: datetime, slot: int | None) -> SolanaObservedPrice | None:
        pool = market.pool
        amounts = market.vault_amounts or {}
        if pool is None or pool.pool_base_token_account is None or pool.pool_quote_token_account is None:
            return None
        base_raw = amounts.get(pool.pool_base_token_account)
        quote_raw = amounts.get(pool.pool_quote_token_account)
        if base_raw is None or quote_raw is None or base_raw <= 0:
            return None
        decimals = self._decimals(market.binding.mint)
        if decimals is None:
            return None
        virtual_quote = pool.virtual_quote_reserves or 0
        effective_quote = quote_raw + virtual_quote
        if effective_quote <= 0:
            return None
        price = (Decimal(effective_quote) / LAMPORTS_PER_SOL) / (Decimal(base_raw) / Decimal(10) ** decimals)
        return SolanaObservedPrice(market.binding.mint, observed_at, price, "pool_wss_indicative", "pumpswap_pool", market.binding.primary_account, slot or pool.observed_slot, base_raw, effective_quote)

    def _decimals(self, mint: str) -> int | None:
        try:
            value = self.decimals_resolver(mint)
            return int(value) if 0 <= int(value) <= 18 else None
        except Exception:
            return None

    @staticmethod
    def _token_amount(data: bytes) -> int | None:
        if len(data) < 72:
            return None
        return int.from_bytes(data[64:72], "little")

    @staticmethod
    def _decode_data(value: object) -> bytes | None:
        if isinstance(value, list) and value and isinstance(value[0], str):
            try:
                return base64.b64decode(value[0], validate=True)
            except Exception:
                return None
        if isinstance(value, str):
            try:
                return base64.b64decode(value, validate=True)
            except Exception:
                return None
        return None
