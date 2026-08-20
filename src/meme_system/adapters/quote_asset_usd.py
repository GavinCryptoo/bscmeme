"""Read-only USD marks for BSC ERC-20 quote assets.

The resolver is deliberately independent from any target meme token.  It
prices the *fundraising asset* used by a venue, so one fresh result can be
reused for every Flap token that settles in that asset.  It only asks for
quotes; it has no signing, approval, or swap capability.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import requests
from eth_abi import decode as abi_decode

from meme_system.adapters.bsc_wss import BSC_USDC_ADDRESS, BSC_USDT_ADDRESS, BSC_WBNB_ADDRESS, BscRpcClient, normalize_bsc_address
from meme_system.adapters.gmgn_openapi import BSC_NATIVE


_DECIMALS_SELECTOR = "0x313ce567"
_SYMBOL_SELECTOR = "0x95d89b41"
_STABLE_ASSETS = (BSC_USDT_ADDRESS, BSC_USDC_ADDRESS)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _human(raw: int, decimals: int) -> Decimal:
    return Decimal(raw) / (Decimal(10) ** decimals)


def _symbol(raw: str | None) -> str | None:
    if raw in {None, "0x"}:
        return None
    try:
        payload = bytes.fromhex(str(raw)[2:])
        # Standard ERC-20 string ABI.  Some older tokens return bytes32.
        if len(payload) >= 96:
            value = abi_decode(["string"], payload)[0]
            return str(value)[:64] or None
        return payload[:32].rstrip(b"\x00").decode("utf-8", errors="ignore")[:64] or None
    except Exception:
        return None


@dataclass(frozen=True)
class QuoteAssetUsdResolution:
    asset: str
    decimals: int | None
    symbol: str | None
    price_usd: Decimal | None
    source: str | None
    updated_at: datetime
    fresh: bool
    failure_reason: str | None = None


class QuoteAssetUsdResolver:
    """Bounded quote-only resolver for a BSC venue's ERC-20 funding asset."""

    kyber_endpoint = "https://aggregator-api.kyberswap.com/bsc/api/v1/routes"

    def __init__(
        self,
        rpc: BscRpcClient,
        *,
        gmgn_provider: Any | None = None,
        timeout_sec: float = 8.0,
    ) -> None:
        self.rpc = rpc
        self.gmgn_provider = gmgn_provider
        self.timeout_sec = max(1.0, min(20.0, float(timeout_sec)))

    def _metadata(self, asset: str) -> tuple[int | None, str | None]:
        decimals = self.rpc.call_uint(asset, _DECIMALS_SELECTOR)
        if decimals is not None and not 0 <= decimals <= 36:
            decimals = None
        symbol = _symbol(self.rpc.call_hex(asset, _SYMBOL_SELECTOR))
        if decimals is None:
            # GMGN's token-info endpoint is already a trusted metadata
            # fallback in the execution adapter.  Reuse it here instead of
            # treating a transient ERC-20 eth_call failure as an unknown
            # token or guessing a conventional 18-decimal scale.
            metadata_decimals = getattr(self.gmgn_provider, "_token_decimals", None)
            if callable(metadata_decimals):
                try:
                    value = metadata_decimals(asset)
                    decimals = int(value) if value is not None else None
                except (TypeError, ValueError):
                    decimals = None
            if decimals is not None and not 0 <= decimals <= 36:
                decimals = None
        return decimals, symbol

    def _gmgn_quote(self, asset: str, output: str, raw_amount: int) -> int | None:
        quote = getattr(self.gmgn_provider, "quote", None)
        if not callable(quote):
            return None
        try:
            result = quote(asset, output, raw_amount)
            amount = getattr(result, "output_amount", None)
            return int(amount) if getattr(result, "success", False) and amount is not None and int(amount) > 0 else None
        except Exception:
            return None

    def _gmgn_native_price(self, asset: str, raw_amount: int, native_usd: Decimal) -> tuple[Decimal | None, str | None]:
        # Prefer an ERC-20 stable settlement.  Its own USD mark is obtained
        # dynamically through WBNB, so it is never permanently assumed to be
        # $1 and a depeg remains visible/fail-closed.
        for stable in _STABLE_ASSETS:
            stable_decimals, _ = self._metadata(stable)
            if stable_decimals is None:
                continue
            stable_raw = self._gmgn_quote(asset, stable, raw_amount)
            stable_to_bnb = self._gmgn_quote(stable, BSC_NATIVE, 10 ** stable_decimals)
            if stable_raw is None or stable_to_bnb is None:
                continue
            stable_usd = _human(stable_to_bnb, 18) * native_usd
            # A quote that says a known USD settlement is materially depegged
            # is not a reliable canonical mark for an entry gate.
            if stable_usd <= 0 or not Decimal("0.90") <= stable_usd <= Decimal("1.10"):
                continue
            stable_symbol = "USDT" if stable == BSC_USDT_ADDRESS else "USDC"
            return _human(stable_raw, stable_decimals) * stable_usd, f"GMGN_{stable_symbol}_USD"
        native_raw = self._gmgn_quote(asset, BSC_NATIVE, raw_amount)
        if native_raw is not None:
            return _human(native_raw, 18) * native_usd, "GMGN_WBNB_QUOTE"
        return None, None

    def _kyber_native_price(self, asset: str, raw_amount: int, native_usd: Decimal) -> Decimal | None:
        try:
            response = requests.get(
                self.kyber_endpoint,
                headers={"x-client-id": "meme0801-universal-router"},
                params={"tokenIn": asset, "tokenOut": BSC_WBNB_ADDRESS, "amountIn": str(raw_amount), "gasInclude": "true"},
                timeout=self.timeout_sec,
            )
            if response.status_code != 200:
                return None
            summary = (response.json().get("data") or {}).get("routeSummary") or {}
            raw = int(summary.get("amountOut") or 0)
            return _human(raw, 18) * native_usd if raw > 0 else None
        except (requests.RequestException, ValueError, TypeError):
            return None

    def resolve_native_usd(self, chain_id: int) -> QuoteAssetUsdResolution:
        """Resolve BNB/USD through an executable BNB-to-USD-stable quote.

        This is the same read-only GMGN/Kyber quote path used for other quote
        assets.  It exists for periods where Meme Rush omits nativeTokenPrice;
        it does not use a target-token Binance display price.
        """
        now = _now()
        if chain_id != 56:
            return QuoteAssetUsdResolution(BSC_WBNB_ADDRESS, 18, "WBNB", None, None, now, False, "INVALID_CHAIN")
        for stable in _STABLE_ASSETS:
            decimals, symbol = self._metadata(stable)
            if decimals is None:
                continue
            raw = self._gmgn_quote(BSC_NATIVE, stable, 10 ** 18)
            if raw is not None and raw > 0:
                return QuoteAssetUsdResolution(
                    BSC_WBNB_ADDRESS, 18, "WBNB", _human(raw, decimals),
                    f"GMGN_WBNB_{symbol or 'USD_STABLE'}", now, True,
                )
        try:
            response = requests.get(
                self.kyber_endpoint,
                headers={"x-client-id": "meme0801-universal-router"},
                params={"tokenIn": BSC_WBNB_ADDRESS, "tokenOut": BSC_USDT_ADDRESS, "amountIn": str(10 ** 18), "gasInclude": "true"},
                timeout=self.timeout_sec,
            )
            summary = (response.json().get("data") or {}).get("routeSummary") or {} if response.status_code == 200 else {}
            raw = int(summary.get("amountOut") or 0)
            if raw > 0:
                decimals, _ = self._metadata(BSC_USDT_ADDRESS)
                if decimals is not None:
                    return QuoteAssetUsdResolution(BSC_WBNB_ADDRESS, 18, "WBNB", _human(raw, decimals), "KYBERSWAP_WBNB_USDT", now, True)
        except (requests.RequestException, ValueError, TypeError):
            pass
        return QuoteAssetUsdResolution(BSC_WBNB_ADDRESS, 18, "WBNB", None, None, now, False, "BNB_USD_UNAVAILABLE")

    def resolve(self, chain_id: int, quote_asset: str, *, native_usd: Decimal | None) -> QuoteAssetUsdResolution:
        now = _now()
        asset = normalize_bsc_address(quote_asset)
        if chain_id != 56 or asset is None:
            return QuoteAssetUsdResolution(str(quote_asset), None, None, None, None, now, False, "INVALID_ASSET")
        if native_usd is None or native_usd <= 0:
            return QuoteAssetUsdResolution(asset, None, None, None, None, now, False, "BNB_USD_UNAVAILABLE")
        decimals, symbol = self._metadata(asset)
        if decimals is None:
            return QuoteAssetUsdResolution(asset, None, symbol, None, None, now, False, "TOKEN_DECIMALS_UNAVAILABLE")
        raw_amount = 10 ** decimals
        price, source = self._gmgn_native_price(asset, raw_amount, native_usd)
        if price is not None and price > 0:
            return QuoteAssetUsdResolution(asset, decimals, symbol, price, source, now, True)
        price = self._kyber_native_price(asset, raw_amount, native_usd)
        if price is not None and price > 0:
            return QuoteAssetUsdResolution(asset, decimals, symbol, price, "KYBERSWAP_WBNB_QUOTE", now, True)
        return QuoteAssetUsdResolution(asset, decimals, symbol, None, None, now, False, "QUOTE_ASSET_USD_UNAVAILABLE")
