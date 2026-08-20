"""Verified PumpSwap direction from transaction balance deltas.

The parser is deliberately protocol- and account-specific. It never infers a
direction from log text. Unknown layouts, unsupported quote assets, missing
vault balances, or non-opposing deltas remain UNKNOWN.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping, Sequence

from meme_system.adapters.pump_readonly import SOL_MINT
from meme_system.adapters.solana_price import SolanaPriceBinding


USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
SUPPORTED_QUOTE_MINTS = frozenset({SOL_MINT, USDC_MINT, USDT_MINT})


@dataclass(frozen=True)
class ParsedSolanaSwap:
    signature: str
    mint: str
    protocol: str
    quote_mint: str | None
    pool_address: str | None
    base_vault: str | None
    quote_vault: str | None
    candidate_delta: Decimal | None
    quote_delta: Decimal | None
    direction: str
    confidence: str
    slot: int | None
    detected_at: datetime
    tx_fetched_at: datetime
    parse_finished_at: datetime
    latency_ms: int
    error_class: str | None = None
    rpc_latency_ms: int | None = None
    rpc_failed: bool = False
    rpc_http_429: bool = False

    @property
    def quote_volume_native(self) -> Decimal | None:
        return abs(self.quote_delta) if self.quote_delta is not None else None


def parse_pumpswap_transaction(
    transaction: Mapping[str, object],
    *,
    signature: str,
    binding: SolanaPriceBinding,
    detected_at: datetime,
    tx_fetched_at: datetime,
    parse_finished_at: datetime,
) -> ParsedSolanaSwap:
    pool = binding.market_state.pumpswap_pool
    quote_mint = pool.quote_mint if pool is not None else None
    base_vault = pool.pool_base_token_account if pool is not None else None
    quote_vault = pool.pool_quote_token_account if pool is not None else None
    slot = transaction.get("slot") if isinstance(transaction.get("slot"), int) else None
    latency_ms = max(0, int((parse_finished_at - detected_at).total_seconds() * 1000))

    def result(direction: str, confidence: str, candidate_delta: Decimal | None = None, quote_delta: Decimal | None = None, error: str | None = None) -> ParsedSolanaSwap:
        return ParsedSolanaSwap(
            signature, binding.mint, "pumpswap", quote_mint,
            pool.pool_address if pool is not None else None,
            base_vault, quote_vault, candidate_delta, quote_delta,
            direction, confidence, slot, detected_at, tx_fetched_at,
            parse_finished_at, latency_ms, error,
        )

    if binding.stage != "pumpswap_pool" or pool is None or not base_vault or not quote_vault:
        return result("UNKNOWN", "NONE", error="PUMPSWAP_BINDING_INCOMPLETE")
    if quote_mint not in SUPPORTED_QUOTE_MINTS:
        return result("UNKNOWN", "NONE", error="QUOTE_MINT_UNSUPPORTED")
    meta = transaction.get("meta")
    tx = transaction.get("transaction")
    if not isinstance(meta, Mapping) or not isinstance(tx, Mapping):
        return result("UNKNOWN", "NONE", error="TRANSACTION_META_MISSING")
    if meta.get("err") is not None:
        return result("UNKNOWN", "NONE", error="TRANSACTION_FAILED")
    message = tx.get("message")
    if not isinstance(message, Mapping):
        return result("UNKNOWN", "NONE", error="TRANSACTION_MESSAGE_MISSING")
    keys = _account_keys(message.get("accountKeys"))
    try:
        base_index = keys.index(base_vault)
        quote_index = keys.index(quote_vault)
    except ValueError:
        return result("UNKNOWN", "NONE", error="POOL_VAULT_INDEX_MISSING")
    pre = _token_balance_map(meta.get("preTokenBalances"))
    post = _token_balance_map(meta.get("postTokenBalances"))
    base_pre = pre.get(base_index)
    base_post = post.get(base_index)
    quote_pre = pre.get(quote_index)
    quote_post = post.get(quote_index)
    if not base_pre or not base_post or not quote_pre or not quote_post:
        return result("UNKNOWN", "NONE", error="POOL_VAULT_BALANCE_MISSING")
    if base_pre[0] != binding.mint or base_post[0] != binding.mint:
        return result("UNKNOWN", "NONE", error="CANDIDATE_MINT_MISMATCH")
    if quote_pre[0] != quote_mint or quote_post[0] != quote_mint:
        return result("UNKNOWN", "NONE", error="QUOTE_MINT_MISMATCH")
    pool_candidate_delta = base_post[1] - base_pre[1]
    pool_quote_delta = quote_post[1] - quote_pre[1]
    user_candidate_delta = -pool_candidate_delta
    user_quote_delta = -pool_quote_delta
    if user_candidate_delta > 0 and user_quote_delta < 0:
        return result("BUY", "HIGH", user_candidate_delta, user_quote_delta)
    if user_candidate_delta < 0 and user_quote_delta > 0:
        return result("SELL", "HIGH", user_candidate_delta, user_quote_delta)
    return result("UNKNOWN", "NONE", user_candidate_delta, user_quote_delta, "NON_OPPOSING_BALANCE_DELTAS")


def _account_keys(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    keys: list[str] = []
    for item in value:
        if isinstance(item, str):
            keys.append(item)
        elif isinstance(item, Mapping) and isinstance(item.get("pubkey"), str):
            keys.append(str(item["pubkey"]))
        else:
            keys.append("")
    return keys


def _token_balance_map(value: object) -> dict[int, tuple[str, Decimal]]:
    result: dict[int, tuple[str, Decimal]] = {}
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return result
    for item in value:
        if not isinstance(item, Mapping) or not isinstance(item.get("accountIndex"), int) or not isinstance(item.get("mint"), str):
            continue
        ui = item.get("uiTokenAmount")
        if not isinstance(ui, Mapping):
            continue
        raw = ui.get("amount")
        decimals = ui.get("decimals")
        try:
            if not isinstance(decimals, int) or isinstance(decimals, bool):
                continue
            amount = Decimal(str(raw)) / (Decimal(10) ** decimals)
        except Exception:
            continue
        result[int(item["accountIndex"])] = (str(item["mint"]), amount)
    return result
