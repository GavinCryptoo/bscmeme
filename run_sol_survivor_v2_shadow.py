#!/usr/bin/env python3
"""Isolated Paper-only SOL Survivor V2 Shadow runtime.

The existing Survivor strategy engine remains the only SQLite writer. All
network reads are scheduled on bounded I/O lanes and returned through queues.
No wallet, signing, transaction construction, swap or broadcast path exists.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import signal
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from queue import Empty, Queue
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from run_realtime import _load_env, _token_decimals
from meme_system.adapters.binance_web3.client import BinanceWeb3Client
from meme_system.adapters.binance_web3.signal_source import BinanceWeb3SignalSource
from meme_system.adapters.jupiter import JupiterReadOnlyQuoteProvider, TokenDecimalsCache
from meme_system.adapters.pump_readonly import PumpProtocolReadOnlyQuoteProvider, PumpReadOnlyAdapter
from meme_system.adapters.solana_agentic_wallet import SolanaAgenticWalletQuoteProvider
from meme_system.adapters.solana_price import SolanaPriceMonitor, _ObservedMarket
from meme_system.adapters.solana_readonly import SolanaRpcClient, SolanaWssMonitor
from meme_system.runtime_ops import HealthRegistry, JsonlAuditWriter, RuntimeControl, SingleInstanceLock
from meme_system.storage.database import initialize_database
from meme_system.storage.runtime_store import RuntimeStore
from meme_system.strategies.sol_survivor_reversal import SolSurvivorReversalConfig, SolSurvivorReversalEngine
from meme_system.strategies.survivor_reversal import SurvivorReversalEngine


IDENTITY = "MEME_SURVIVOR_REVERSAL_SOL_V2_SHADOW"
HELIUS_SWITCH_ID = "HELIUS_REALTIME_PRIMARY_V1"


def helius_endpoints() -> tuple[str | None, str | None]:
    key = os.environ.get("HELIUS_API_KEY", "").strip()
    if not key:
        return None, None
    return (
        f"https://mainnet.helius-rpc.com/?api-key={key}",
        f"wss://mainnet.helius-rpc.com/?api-key={key}",
    )


def classify_helius_transaction_reply(reply: Any) -> tuple[str, str | None]:
    if not isinstance(reply, dict):
        return "DEGRADED_FALLBACK_ACTIVE", "INVALID_RESPONSE"
    if isinstance(reply.get("result"), int):
        return "SUPPORTED", None
    error = reply.get("error")
    if not isinstance(error, dict):
        return "DEGRADED_FALLBACK_ACTIVE", "INVALID_RESPONSE"
    message = str(error.get("message") or error.get("code") or "HELIUS_CAPABILITY_ERROR")[:240]
    lowered = message.lower()
    if any(word in lowered for word in ("free plan", "permission", "plan", "not available", "unsupported")):
        return "UNSUPPORTED_FALLBACK_ACTIVE", message
    return "DEGRADED_FALLBACK_ACTIVE", message


def quantize_exit_quantity(
    quantity: Decimal,
    reference_quantity: Decimal,
    mint: str,
    decimals_resolver: Any,
) -> Decimal:
    try:
        decimals = int(decimals_resolver(mint))
    except Exception:
        # The entry quantity came from an executable quote and is already
        # expressed in token base units. Preserve that known precision if a
        # transient RPC decimals lookup fails during an exit retry.
        decimals = max(0, min(18, -int(reference_quantity.as_tuple().exponent)))
    return quantity.quantize(Decimal("1").scaleb(-decimals), rounding=ROUND_DOWN)


class V2PriceMonitor(SolanaPriceMonitor):
    """Resolve each market outside the shared state lock."""

    def register_position(self, mint: str, bonding_curve_address: str | None = None) -> Any:
        if threading.current_thread() is threading.main_thread():
            with self._lock:
                existing = self._markets.get(mint)
                return existing.binding if existing is not None else None
        state = self.pump_adapter.inspect(mint, bonding_curve_address=bonding_curve_address)
        binding = self._binding_from_state(state)
        if binding is None:
            with self._lock:
                existing = self._markets.get(mint)
                return existing.binding if existing is not None else None
        market = _ObservedMarket(binding=binding, curve=state.bonding_curve, pool=state.pumpswap_pool, vault_amounts={})
        if binding.stage == "pump_bonding_curve":
            self._load_account(market, binding.primary_account)
        else:
            for address in binding.account_addresses:
                self._load_account(market, address)
        with self._lock:
            self._markets[mint] = market
        return binding


class V2RpcMetricsProxy:
    def __init__(self, rpc: Any) -> None:
        self.rpc = rpc
        self._lock = threading.Lock()
        self._block_times: dict[str, int] = {}

    def __getattr__(self, name: str) -> Any:
        return getattr(self.rpc, name)

    def get_transaction(self, signature: str, **kwargs: Any) -> Any:
        transaction = self.rpc.get_transaction(signature, **kwargs)
        if isinstance(transaction, dict) and isinstance(transaction.get("blockTime"), int):
            with self._lock:
                self._block_times[signature] = int(transaction["blockTime"])
                if len(self._block_times) > 4096:
                    self._block_times.pop(next(iter(self._block_times)))
        return transaction

    def block_time(self, signature: str) -> int | None:
        with self._lock:
            return self._block_times.get(signature)


class V2TransactionMonitor:
    def __init__(self, urls: tuple[str, ...], events: Queue[dict[str, Any]]) -> None:
        self.urls = urls
        self.events = events
        self.stop = threading.Event()
        self._lock = threading.Lock()
        self._accounts: tuple[str, ...] = ()
        self._generation = 0
        self.enabled = False
        self.state = "DISABLED"
        self.subscription_id: int | None = None
        self.ack_count = 0
        self.event_count = 0
        self.reconnect_count = 0
        self.disconnect_count = 0
        self.last_event_at: datetime | None = None
        self.last_error: str | None = None

    def configure(self, accounts: set[str], enabled: bool) -> None:
        with self._lock:
            normalized = tuple(sorted(accounts))
            if normalized != self._accounts or enabled != self.enabled:
                self._accounts = normalized
                self.enabled = enabled
                self._generation += 1

    def snapshot(self) -> tuple[tuple[str, ...], int, bool]:
        with self._lock:
            return self._accounts, self._generation, self.enabled

    def run(self) -> None:
        asyncio.run(self._run())

    async def _run(self) -> None:
        import websockets
        while not self.stop.is_set():
            accounts, generation, enabled = self.snapshot()
            if not enabled or not accounts or not self.urls:
                self.state = "DISABLED" if not enabled else "WAITING_ACCOUNTS"
                await asyncio.sleep(.25)
                continue
            try:
                self.state = "CONNECTING"
                async with websockets.connect(self.urls[0], ping_interval=20, ping_timeout=20, close_timeout=2) as socket:
                    request = {"jsonrpc":"2.0","id":420,"method":"transactionSubscribe","params":[
                        {"vote":False,"failed":False,"accountInclude":list(accounts)},
                        {"commitment":"confirmed","encoding":"jsonParsed","transactionDetails":"full","showRewards":False,"maxSupportedTransactionVersion":0},
                    ]}
                    await socket.send(json.dumps(request, separators=(",",":")))
                    self.reconnect_count += 1
                    self.state = "PENDING_ACK"
                    while not self.stop.is_set():
                        if self.snapshot()[1] != generation:
                            break
                        try:
                            raw = await asyncio.wait_for(socket.recv(), timeout=.5)
                        except asyncio.TimeoutError:
                            continue
                        event = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
                        if isinstance(event, dict) and event.get("id") == 420 and isinstance(event.get("result"), int):
                            self.subscription_id = int(event["result"])
                            self.ack_count += 1
                            self.last_error = None
                            self.state = "HEALTHY"
                            self.events.put_nowait({"method":"v2TransactionAck","subscription_id":self.subscription_id})
                        elif isinstance(event, dict) and event.get("id") == 420 and isinstance(event.get("error"), dict):
                            with self._lock:
                                self.enabled = False
                                self._generation += 1
                            self.state = "UNSUPPORTED_FALLBACK_ACTIVE"
                            self.last_error = str(event["error"].get("message") or event["error"].get("code") or "UNSUPPORTED")[:240]
                            self.events.put_nowait({"method":"v2TransactionUnsupported","error_class":self.last_error})
                            break
                        elif isinstance(event, dict) and event.get("method") == "transactionNotification":
                            self.last_event_at = utc_now()
                            self.event_count += 1
                            self.state = "HEALTHY"
                            self.events.put_nowait({"method":"v2TransactionNotification","payload":event})
            except Exception:
                self.disconnect_count += 1
                self.state = "DEGRADED"
                await asyncio.sleep(.5)

    def close(self) -> None:
        self.stop.set()


class V2QuoteRouter:
    """Non-blocking dual-lane provider race with isolated provider circuits."""

    def __init__(self, direct: Any, jupiter: Any, baw: Any, config: SolSurvivorReversalConfig, policy: str) -> None:
        ordered = (
            (("binance_agentic_wallet", baw), ("jupiter", jupiter), ("direct_pump", direct))
            if policy == "BINANCE_PRIMARY"
            else (("jupiter", jupiter), ("binance_agentic_wallet", baw), ("direct_pump", direct))
            if policy == "HYBRID"
            else (("jupiter", jupiter), ("direct_pump", direct), ("binance_agentic_wallet", baw))
        )
        self.providers = dict(ordered)
        self.policy = policy
        self.config = config
        self.exit_io = ThreadPoolExecutor(max_workers=5, thread_name_prefix="sol-v2-exit-quote")
        self.normal_io = ThreadPoolExecutor(max_workers=4, thread_name_prefix="sol-v2-normal-quote")
        self._lock = threading.Lock()
        self._inflight: set[tuple[str, str, Decimal]] = set()
        self._results: dict[tuple[str, str, Decimal], Any] = {}
        self._roundtrips: dict[tuple[str, Decimal], tuple[Any, float]] = {}
        self._accepted_roundtrips: dict[str, tuple[Any, Any]] = {}
        self._delivered: dict[tuple[str, str, Decimal], Any] = {}
        self._attempt_events: Queue[dict[str, Any]] = Queue(maxsize=10000)
        self._stats: dict[str, dict[str, Any]] = {
            name: {"state": "RECOVERING", "calls": 0, "success": 0, "timeout": 0,
                   "failures": 0, "circuit_until": 0.0, "latencies_ms": deque(maxlen=1000),
                   "last_failure_reason": None}
            for name in self.providers
        }
        self.last_error: str | None = None
        self.last_provider = "NOT_REQUESTED"
        if policy == "JUPITER_DIRECT_PRIMARY" and "binance_agentic_wallet" in self._stats:
            self._stats["binance_agentic_wallet"]["state"] = "OPEN_CIRCUIT"
            self._stats["binance_agentic_wallet"]["circuit_until"] = time.monotonic() + 30.0

    @staticmethod
    def _failure(exc: BaseException | None, quote: Any = None) -> str:
        if exc is not None:
            name = type(exc).__name__.upper()
            message = str(exc).upper()
            if "429" in message:
                return "HTTP_429"
            if "SSL" in name or "TLS" in message:
                return "TLS_HANDSHAKE_TIMEOUT"
            if "TIMEOUT" in name or "TIMED OUT" in message:
                return "READ_TIMEOUT"
            if "DNS" in message or "NAME OR SERVICE" in message:
                return "DNS_ERROR"
            return name
        return getattr(quote, "error_class", None) or "NO_ROUTE"

    def _race(self, key: tuple[str, str, Decimal]) -> None:
        mint, side, amount = key
        answers: Queue[tuple[str, Any, BaseException | None, int]] = Queue()
        launched = 0
        launched_names: set[str] = set()
        now = time.monotonic()

        def provider_call(name: str, provider: Any) -> None:
            started = time.monotonic()
            requested_at = utc_now()
            try:
                quote, error = provider.quote(mint, side, amount), None
            except BaseException as exc:  # isolate provider code, including TLS stacks
                quote, error = None, exc
            try:
                latency = int((time.monotonic() - started) * 1000)
                answers.put_nowait((name, quote, error, latency))
                failure = None if error is None and self._usable(quote) else self._failure(error, quote)
                self._attempt_events.put_nowait({
                    "mint": mint, "side": side, "input_quantity": str(amount), "provider": name,
                    "requested_at": requested_at.isoformat(),
                    "quoted_at": quote.quoted_at.isoformat() if quote is not None and getattr(quote, "quoted_at", None) else None,
                    "latency_ms": latency, "success": failure is None, "failure_reason": failure,
                    "price_impact_pct": str(quote.price_impact_pct) if quote is not None and quote.price_impact_pct is not None else None,
                    "route": list(quote.route) if quote is not None else [],
                })
            except Exception:
                pass

        for name, provider in self.providers.items():
            stat = self._stats[name]
            if provider is None or now < float(stat["circuit_until"]):
                continue
            launched += 1
            launched_names.add(name)
            stat["calls"] += 1
            # A provider handshake may hang below Python's timeout machinery.
            # Its daemon thread is abandoned after the race deadline and can
            # never occupy either bounded scheduling lane.
            threading.Thread(target=provider_call, args=(name, provider), daemon=True,
                             name=f"sol-v2-provider-{name}").start()

        winner = None
        deadline = time.monotonic() + max(0.25, float(os.environ.get("SOL_V2_QUOTE_TOTAL_TIMEOUT_SEC", "2.5")))
        received: set[str] = set()
        while len(received) < launched and time.monotonic() < deadline:
            try:
                name, quote, error, latency = answers.get(timeout=min(0.05, max(0.001, deadline - time.monotonic())))
            except Empty:
                continue
            received.add(name)
            stat = self._stats[name]
            stat["latencies_ms"].append(latency)
            if error is None and self._usable(quote):
                stat["success"] += 1
                stat["failures"] = 0
                stat["state"] = "HEALTHY"
                stat["last_failure_reason"] = None
                if winner is None:
                    winner = quote
                    self.last_provider = name
                    break
            else:
                reason = self._failure(error, quote)
                stat["failures"] += 1
                stat["last_failure_reason"] = reason
                stat["state"] = "DEGRADED"
                if stat["failures"] >= 3:
                    stat["state"] = "OPEN_CIRCUIT"
                    stat["circuit_until"] = time.monotonic() + 5.0
        # Once a usable winner arrives the remaining calls are race losers, not
        # timeouts. Count a timeout only when the whole race deadline expires
        # without any executable route.
        if winner is None:
            for name in launched_names - received:
                stat = self._stats[name]
                stat["timeout"] += 1
                stat["failures"] += 1
                stat["last_failure_reason"] = "TOTAL_TIMEOUT"
                stat["state"] = "OPEN_CIRCUIT" if stat["failures"] >= 3 else "DEGRADED"
                if stat["failures"] >= 3:
                    stat["circuit_until"] = time.monotonic() + 5.0
        with self._lock:
            self._inflight.discard(key)
            self._results[key] = winner
            self.last_error = None if winner is not None else "SOL_EXIT_ROUTE_UNAVAILABLE" if side == "sell" else "SOL_QUOTE_UNAVAILABLE"

    def _usable(self, quote: Any) -> bool:
        if quote is None or quote.output_quantity <= 0 or quote.input_quantity <= 0:
            return False
        now = utc_now()
        if quote.unusable_reason(now):
            return False
        age_ms = max(int(getattr(quote, "age_ms", 0)), int((now - quote.quoted_at).total_seconds() * 1000))
        if age_ms > self.config.max_quote_age_ms or quote.price_impact_pct is None:
            return False
        maximum = self.config.max_buy_price_impact if quote.side == "buy" else self.config.max_sell_price_impact
        return abs(quote.price_impact_pct) / Decimal("100") <= maximum

    def quote(self, mint: str, side: str, amount: Decimal) -> Any:
        key = (mint, side, amount)
        with self._lock:
            if key in self._results:
                result = self._results.pop(key)
                if result is not None:
                    self._delivered[key] = result
                else:
                    self.last_error = "SOL_EXIT_ROUTE_UNAVAILABLE" if side == "sell" else "SOL_QUOTE_UNAVAILABLE"
                return result
            if key in self._inflight:
                return None
            self._inflight.add(key)
        (self.exit_io if side == "sell" else self.normal_io).submit(self._race, key)
        self.last_error = "SOL_EXIT_ROUTE_PENDING" if side == "sell" else "SOL_QUOTE_PENDING"
        return None

    def quote_candidate(self, mint: str, input_sol: Decimal, _: Any) -> tuple[Any, Any, str | None]:
        now = utc_now()
        roundtrip_key = (mint, input_sol)
        state = self._roundtrips.get(roundtrip_key)
        buy = state[0] if state is not None else self.quote(mint, "buy", input_sol)
        if buy is None or buy.output_quantity <= 0:
            return buy, None, self.last_error or "NO_BUY_ROUTE"
        if state is None:
            self._roundtrips[roundtrip_key] = (buy, time.monotonic())
        elif time.monotonic() - state[1] > max(3.0, self.config.max_quote_age_ms / 1000):
            self._roundtrips.pop(roundtrip_key, None)
            return None, None, "PRICE_MOVED"
        sell = self.quote(mint, "sell", buy.output_quantity)
        if sell is None:
            pending = self.last_error and "PENDING" in self.last_error
            return buy, None, "SELL_QUOTE_PENDING" if pending else "NO_SELL_ROUTE"
        for quote, maximum, label in ((buy, self.config.max_buy_price_impact, "BUY"),
                                      (sell, self.config.max_sell_price_impact, "SELL")):
            if quote is None or quote.output_quantity <= 0 or quote.unusable_reason(now):
                return buy, sell, f"{label}_QUOTE_UNAVAILABLE"
            if quote.price_impact_pct is None or abs(quote.price_impact_pct) / Decimal("100") > maximum:
                return buy, sell, f"{label}_PRICE_IMPACT_TOO_HIGH"
        self._roundtrips.pop(roundtrip_key, None)
        self._accepted_roundtrips[mint] = (buy, sell)
        return buy, sell, None

    def consume_roundtrip(self, mint: str) -> tuple[Any, Any] | None:
        return self._accepted_roundtrips.pop(mint, None)

    def consume_delivered(self, mint: str, side: str, amount: Decimal) -> Any:
        return self._delivered.pop((mint, side, amount), None)

    def drain_attempt_events(self, limit: int = 1000) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for _ in range(limit):
            try:
                events.append(self._attempt_events.get_nowait())
            except Empty:
                break
        return events

    def status(self) -> dict[str, Any]:
        return {"policy": self.policy, "providers": {name: {"state": stat["state"], "calls": stat["calls"], "success": stat["success"],
                       "timeout": stat["timeout"], "last_failure_reason": stat["last_failure_reason"],
                       "p50_ms": percentile(stat["latencies_ms"], .5), "p95_ms": percentile(stat["latencies_ms"], .95),
                       "p99_ms": percentile(stat["latencies_ms"], .99)} for name, stat in self._stats.items()}}

    def shutdown(self) -> None:
        self.exit_io.shutdown(wait=False, cancel_futures=True)
        self.normal_io.shutdown(wait=False, cancel_futures=True)


class V2ShadowEngine(SolSurvivorReversalEngine):
    """Existing strategy with durable, monotonic exit intents."""

    def __init__(self, **kwargs: Any) -> None:
        self._v2_tx_retry: dict[tuple[str, str], tuple[float, Any]] = {}
        self._v2_retry_meta: dict[tuple[str, str], dict[str, int]] = {}
        self.v2_latency_records: Queue[dict[str, Any]] = Queue(maxsize=4096)
        self.transaction_not_available_yet = 0
        self.wss_receive_lag_ms: deque[int] = deque(maxlen=1000)
        self.transaction_fetch_lag_ms: deque[int] = deque(maxlen=1000)
        self.processing_lag_ms: deque[int] = deque(maxlen=1000)
        self.total_price_update_lag_ms: deque[int] = deque(maxlen=1000)
        self.v2_position_evaluate_ms = 0
        self._v2_candidate_cursor = 0
        super().__init__(**kwargs)
        self._v2_active_opportunities = {
            str(row[0]): str(row[1]) for row in self.connection.execute(
                "SELECT mint,opportunity_id FROM sol_v2_entry_opportunities WHERE state='PENDING' ORDER BY signal_at"
            )
        }
        self._reconcile_completed_tp3_dust()

    def _reconcile_completed_tp3_dust(self) -> None:
        """Close only sub-base-unit TP3 dust without inventing proceeds."""
        now = utc_now()
        for position in self._positions.values():
            if position.status != "OPEN" or not position.tp2 or not position.trailing_active:
                continue
            try:
                decimals = int(self.price_monitor.decimals_resolver(position.mint))
                quantum = Decimal("1").scaleb(-decimals)
            except Exception:
                continue
            if position.remaining_quantity_token <= quantum * 2:
                position.remaining_quantity_token = Decimal("0")
                position.status = "CLOSED"
                self._persist_position(position, now, reason="TP3_COMPLETED_ATOMIC_DUST")
                candidate = self._candidates.get(position.mint)
                if candidate is not None:
                    candidate.state = "POSITION_CLOSED"
                self._audit_event("SOL_SURVIVOR_ATOMIC_DUST_CLOSED", candidate, {
                    "position_id": position.position_id, "proceeds_added": "0", "executable_quote_fabricated": False,
                })
        self.connection.commit()

    def _sync_bindings(self) -> None:
        # The inherited implementation performs synchronous RPC reads. V2's
        # five-second binding scheduler owns those reads off-loop.
        self._v2_binding_refresh_requested = True

    def on_records(self, records: Any, now: datetime | None = None) -> None:
        # Preserve the SOL post-processing but scope it to the records in this
        # bounded P4 batch; the V1 method re-walks every historical candidate.
        SurvivorReversalEngine.on_records(self, records, now)
        observed_at = now or self.clock()
        self._refresh_smart_money(observed_at)
        touched = {
            record.signal.mint for record in records
            if getattr(getattr(record, "signal", None), "mint", None)
        }
        with self._lock:
            for mint in touched:
                candidate = self._candidates.get(mint)
                if candidate is None:
                    continue
                candidate.data_quality_cohort = self._classify_cohort(candidate)
                if self._exclude_if_first_discovery_price_above_cap(candidate, observed_at):
                    continue
                self._record_holder_observation(candidate, observed_at)
                if candidate.current_price_usd and candidate.native_token_price_usd and candidate.native_token_price_usd > 0 and candidate.current_price_native is None:
                    candidate.current_price_native = candidate.current_price_usd / candidate.native_token_price_usd
                self._apply_price_cap(candidate)
                lifecycle = candidate.latest_lifecycle or "UNKNOWN"
                candidate.source_status["protocol_state"] = "VALID"
                if candidate.source_status.get("swap_direction") != "VALID":
                    candidate.source_status["swap_direction"] = "SOURCE_UNAVAILABLE"
                setattr(candidate, "protocol_state", self._protocol_state(lifecycle))
                self._persist_sol_fields(candidate)
            self.connection.commit()
            self._publish(observed_at)

    def poll_swap_rpc_work(self, *, max_accounts: int = 1, max_transactions: int = 4) -> None:
        current = time.monotonic()
        with self._lock:
            for key, (due_at, parsed) in tuple(self._v2_tx_retry.items()):
                if due_at > current:
                    continue
                self._v2_tx_retry.pop(key, None)
                self._seen_signatures.pop(key, None)
                self._signature_queue.append((parsed.detected_at, parsed.signature, parsed.mint, parsed.slot))
                self._queued_signatures.add(key)
        super().poll_swap_rpc_work(max_accounts=max_accounts, max_transactions=max_transactions)
        with self._lock:
            retained = deque(maxlen=self._parsed_swap_queue.maxlen)
            while self._parsed_swap_queue:
                parsed = self._parsed_swap_queue.popleft()
                key = (parsed.signature, parsed.mint)
                block_time = self.swap_rpc.block_time(parsed.signature) if hasattr(self.swap_rpc, "block_time") else None
                if block_time is not None:
                    chain_at = datetime.fromtimestamp(block_time, timezone.utc)
                    self.wss_receive_lag_ms.append(max(0, int((parsed.detected_at - chain_at).total_seconds() * 1000)))
                    self.total_price_update_lag_ms.append(max(0, int((parsed.parse_finished_at - chain_at).total_seconds() * 1000)))
                self.transaction_fetch_lag_ms.append(max(0, int((parsed.tx_fetched_at - parsed.detected_at).total_seconds() * 1000)))
                self.processing_lag_ms.append(max(0, int((parsed.parse_finished_at - parsed.tx_fetched_at).total_seconds() * 1000)))
                retryable = parsed.error_class == "TRANSACTION_NOT_AVAILABLE" or bool(parsed.rpc_http_429)
                if not retryable:
                    self._queue_latency_record(parsed, key)
                    retained.append(parsed)
                    continue
                meta = self._v2_retry_meta.setdefault(key, {"count": 0, "delay_ms": 0, "http_429": 0})
                meta["count"] += 1
                meta["http_429"] += int(bool(parsed.rpc_http_429))
                if meta["count"] <= 3:
                    delay = (0.25, 0.75, 2.0)[meta["count"] - 1]
                    meta["delay_ms"] += int(delay * 1000)
                    self._v2_tx_retry[key] = (time.monotonic() + delay, parsed)
                    self._seen_signatures.pop(key, None)
                    if parsed.error_class == "TRANSACTION_NOT_AVAILABLE":
                        self.transaction_not_available_yet += 1
                else:
                    self._queue_latency_record(parsed, key)
                    retained.append(parsed)
            self._parsed_swap_queue.extend(retained)

    def _queue_latency_record(self, parsed: Any, key: tuple[str, str]) -> None:
        meta = self._v2_retry_meta.pop(key, {"count": 0, "delay_ms": 0, "http_429": 0})
        block_time = self.swap_rpc.block_time(parsed.signature) if hasattr(self.swap_rpc, "block_time") else None
        chain_at = datetime.fromtimestamp(block_time, timezone.utc) if block_time is not None else None
        retry_ms = int(meta.get("delay_ms", 0))
        wss_to_tx = max(0, int((parsed.tx_fetched_at - parsed.detected_at).total_seconds() * 1000) - retry_ms)
        try:
            self.v2_latency_records.put_nowait({
                "signature": parsed.signature, "mint": parsed.mint,
                "chain_block_at": chain_at.isoformat() if chain_at else None,
                "wss_received_at": parsed.detected_at.isoformat(),
                "transaction_available_at": parsed.tx_fetched_at.isoformat(),
                "parsed_at": parsed.parse_finished_at.isoformat(),
                "chain_to_wss_ms": max(0, int((parsed.detected_at - chain_at).total_seconds() * 1000)) if chain_at else None,
                "wss_to_get_transaction_ms": wss_to_tx, "get_transaction_retry_ms": retry_ms,
                "parsing_ms": max(0, int((parsed.parse_finished_at - parsed.tx_fetched_at).total_seconds() * 1000)),
                "retry_count": int(meta.get("count", 0)), "rpc_http_429_count": int(meta.get("http_429", 0)),
            })
        except Exception:
            pass

    def _intent(self, position: Any, reason: str, now: datetime, pct: Decimal | None) -> None:
        self.connection.execute(
            "INSERT INTO sol_v2_exit_intents(position_id,exit_reason,exit_triggered_at,trigger_reference_price,requested_initial_pct,status,updated_at) "
            "VALUES(?,?,?,?,?,'PENDING_ROUTE',?) ON CONFLICT(position_id,exit_reason) DO UPDATE SET updated_at=excluded.updated_at",
            (position.position_id, reason, now.isoformat(), str(position.current_price_native) if position.current_price_native else None,
             str(pct) if pct is not None else None, now.isoformat()),
        )
        self.connection.commit()

    @staticmethod
    def _entry_skip_reason(reason: str | None) -> str:
        value = str(reason or "OTHER").upper()
        if "PENDING" in value:
            return "QUOTE_PENDING"
        if "TIMEOUT" in value:
            return "QUOTE_TIMEOUT"
        if "SELL" in value or "ROUNDTRIP" in value:
            return "NO_SELL_ROUTE" if "UNAVAILABLE" in value else "ROUNDTRIP_FAILED"
        if "BUY" in value or "QUOTE" in value or "ROUTE" in value:
            return "NO_BUY_ROUTE"
        if "IMPACT" in value or "PRICE_MOVED" in value or "PRICE_" in value or "EXPIRED" in value:
            return "PRICE_MOVED"
        if "LIQUID" in value:
            return "LIQUIDITY_INSUFFICIENT"
        if "AUDIT" in value:
            return "AUDIT_FAILED"
        if "FLOW" in value:
            return "FLOW_FAILED"
        return "OTHER"

    def _record_trade_execution(
        self,
        *,
        position: Any,
        side: str,
        reason: str,
        reference_at: datetime,
        reference_price: Decimal | None,
        quote: Any,
        fill_at: datetime,
    ) -> None:
        if quote is None or quote.input_quantity <= 0 or quote.output_quantity <= 0:
            return
        fill_price = quote.input_quantity / quote.output_quantity if side == "BUY" else quote.output_quantity / quote.input_quantity
        latency_ms = max(0, int((fill_at - reference_at).total_seconds() * 1000))
        slippage = None
        if reference_price is not None and reference_price > 0:
            slippage = (fill_price / reference_price - Decimal("1")) * Decimal("100")
        execution_id = f"{position.position_id}:{side}:{reason}"
        self.connection.execute(
            "INSERT OR IGNORE INTO sol_v2_trade_executions(execution_id,position_id,mint,side,reason,reference_at,reference_price,quote_request_at,quote_at,sim_fill_at,quote_price,sim_fill_price,provider,quote_latency_ms,price_impact_pct,route_json,execution_latency_ms,slippage_vs_reference_pct) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                execution_id, position.position_id, position.mint, side, reason, reference_at.isoformat(),
                str(reference_price) if reference_price is not None else None,
                quote.requested_at.isoformat() if quote.requested_at else None,
                quote.quoted_at.isoformat(), fill_at.isoformat(), str(fill_price), str(fill_price),
                quote.quote_source or quote.provider, quote.latency_ms,
                str(quote.price_impact_pct) if quote.price_impact_pct is not None else None,
                json.dumps(list(quote.route), ensure_ascii=False), latency_ms,
                str(slippage) if slippage is not None else None,
            ),
        )
        self.connection.commit()

    def _try_buy(self, candidate: Any, now: datetime) -> None:
        # A V2 execution opportunity starts only after the unchanged strategy
        # controls have admitted an entry.  In particular, an already-held or
        # 24-hour-cooldown mint is not a new signal and must not inflate the
        # missed-execution denominator on every engine tick.
        if self.controls.paused("paper"):
            return
        if sum(position.status == "OPEN" for position in self._positions.values()) >= self.config.max_open_positions:
            return
        cutoff = (now - timedelta(hours=24)).isoformat()
        if self.connection.execute(
            "SELECT 1 FROM survivor_positions WHERE mint=? AND opened_at>=? LIMIT 1",
            (candidate.mint, cutoff),
        ).fetchone() is not None:
            return
        opportunity_id = self._v2_active_opportunities.get(candidate.mint)
        if opportunity_id is None:
            opportunity_id = f"entry:{candidate.mint}:{now.isoformat()}"
            self._v2_active_opportunities[candidate.mint] = opportunity_id
            self.connection.execute(
                "INSERT INTO sol_v2_entry_opportunities(opportunity_id,mint,symbol,signal_at,signal_price,state,updated_at) VALUES(?,?,?,?,?,'PENDING',?)",
                (opportunity_id, candidate.mint, candidate.symbol, now.isoformat(),
                 str(candidate.current_price_native) if candidate.current_price_native is not None else None, now.isoformat()),
            )
        before = set(self._positions)
        super()._try_buy(candidate, now)
        opened = [position for position_id, position in self._positions.items() if position_id not in before and position.mint == candidate.mint]
        if opened:
            position = opened[-1]
            pair = self.quote_provider.consume_roundtrip(candidate.mint) if hasattr(self.quote_provider, "consume_roundtrip") else None
            buy = pair[0] if pair else None
            signal = self.connection.execute(
                "SELECT signal_at,signal_price FROM sol_v2_entry_opportunities WHERE opportunity_id=?", (opportunity_id,)
            ).fetchone()
            signal_at = datetime.fromisoformat(signal[0]) if signal and signal[0] else now
            signal_price = Decimal(str(signal[1])) if signal and signal[1] is not None else candidate.current_price_native
            self._record_trade_execution(position=position, side="BUY", reason="ENTRY", reference_at=signal_at,
                                         reference_price=signal_price, quote=buy, fill_at=now)
            self.connection.execute(
                "UPDATE sol_v2_entry_opportunities SET state='FILLED',position_id=?,skipped_reason=NULL,updated_at=? WHERE opportunity_id=?",
                (position.position_id, now.isoformat(), opportunity_id),
            )
            self._v2_active_opportunities.pop(candidate.mint, None)
        else:
            skipped = self._entry_skip_reason(candidate.last_rejection)
            self.connection.execute(
                "UPDATE sol_v2_entry_opportunities SET state='PENDING',skipped_reason=?,updated_at=? WHERE opportunity_id=?",
                (skipped, now.isoformat(), opportunity_id),
            )
        self.connection.commit()

    def _evaluate_candidate(self, candidate: Any, now: datetime) -> None:
        super()._evaluate_candidate(candidate, now)
        if candidate.state not in {"EXPIRED", "PERMANENTLY_EXCLUDED"}:
            return
        opportunity_id = self._v2_active_opportunities.pop(candidate.mint, None)
        if opportunity_id is None:
            return
        reason = self._entry_skip_reason(candidate.last_rejection)
        self.connection.execute(
            "UPDATE sol_v2_entry_opportunities SET state='SKIPPED',skipped_reason=COALESCE(skipped_reason,?),updated_at=? WHERE opportunity_id=? AND state='PENDING'",
            (reason, now.isoformat(), opportunity_id),
        )
        self.connection.execute(
            "DELETE FROM sol_v2_entry_opportunities WHERE state='SKIPPED' AND rowid NOT IN (SELECT rowid FROM sol_v2_entry_opportunities WHERE state='SKIPPED' ORDER BY signal_at DESC LIMIT 50000)"
        )
        self.connection.commit()

    def _settled(self, position: Any, reason: str, now: datetime) -> None:
        self.connection.execute("UPDATE sol_v2_exit_intents SET status='PAPER_EXITED',updated_at=? WHERE position_id=? AND exit_reason=?",
                                (now.isoformat(), position.position_id, reason))
        self.connection.commit()

    def _route_attempted(self, position: Any, now: datetime, success: bool) -> None:
        self.connection.execute(
            "UPDATE sol_v2_position_freshness SET last_exit_quote_at=?,last_successful_exit_route_at=CASE WHEN ? THEN ? ELSE last_successful_exit_route_at END,updated_at=? WHERE position_id=?",
            (now.isoformat(), int(success), now.isoformat(), now.isoformat(), position.position_id),
        )
        self.connection.commit()

    def _partial_exit_exact(self, position: Any, quantity: Decimal, reason: str, now: datetime) -> bool:
        """Apply a V2 partial exit without converting base units through a ratio."""
        provider = getattr(self, "exit_quote_provider", self.quote_provider)
        if hasattr(provider, "sell_quote"):
            quote, error = provider.sell_quote(position.mint, quantity)
        else:
            quote, error = (provider.quote(position.mint, "sell", quantity) if provider is not None else None), None
        if quote is None or quote.output_quantity <= 0:
            self._set_exit_intent(position, reason, quantity, now, error)
            self._audit_event(
                "SURVIVOR_EXIT_QUOTE_UNAVAILABLE",
                self._candidates.get(position.mint),
                {"position_id": position.position_id, "reason": reason},
            )
            return False
        position.remaining_quantity_token -= quantity
        position.realized_bnb += quote.output_quantity
        if reason.startswith("TP1"):
            position.tp1 = True
        elif reason.startswith("TP2"):
            position.tp2 = True
            position.trailing_active = True
            position.high_after_tp2 = position.current_price_native
        self._persist_position(position, now, reason=None)
        self._clear_exit_intent(position)
        self._audit_event(
            "SURVIVOR_PARTIAL_EXIT",
            self._candidates.get(position.mint),
            {
                "position_id": position.position_id,
                "reason": reason,
                "sell_quote_id": quote.quote_id,
                "sell_input_quantity": str(quote.input_quantity),
                "sell_output_bnb": str(quote.output_quantity),
                "quote_source": quote.quote_source or quote.provider,
                "executable_quote": True,
            },
        )
        return True

    def _partial_exit_of_initial(self, position: Any, initial_pct: Decimal, reason: str, now: datetime) -> bool:
        reference_price = position.current_price_native
        quantity = min(position.remaining_quantity_token, position.quantity_token * initial_pct / Decimal("100"))
        if reason.startswith("TP3"):
            quantity = position.remaining_quantity_token
        quantity = quantize_exit_quantity(
            quantity, position.quantity_token, position.mint, self.price_monitor.decimals_resolver
        )
        if quantity <= 0:
            return False
        self._intent(position, reason, now, initial_pct)
        intent = self.connection.execute(
            "SELECT exit_triggered_at,trigger_reference_price FROM sol_v2_exit_intents WHERE position_id=? AND exit_reason=?",
            (position.position_id, reason),
        ).fetchone()
        reference_at = datetime.fromisoformat(intent[0]) if intent and intent[0] else now
        if intent and intent[1] is not None:
            reference_price = Decimal(str(intent[1]))
        tolerance = max(Decimal("1"), position.quantity_token) * Decimal("1e-18")
        if position.remaining_quantity_token - quantity <= tolerance:
            super()._close_position(position, reason, now)
            result = position.status == "CLOSED"
        else:
            result = self._partial_exit_exact(position, quantity, reason, now)
        self._route_attempted(position, now, result)
        if result:
            quote = self.quote_provider.consume_delivered(position.mint, "sell", quantity) if hasattr(self.quote_provider, "consume_delivered") else None
            self._record_trade_execution(position=position, side="SELL", reason=reason, reference_at=reference_at,
                                         reference_price=reference_price, quote=quote, fill_at=now)
            self._settled(position, reason, now)
        return result

    def _close_position(self, position: Any, reason: str, now: datetime) -> None:
        reference_price = position.current_price_native
        quantity = position.remaining_quantity_token
        self._intent(position, reason, now, None)
        intent = self.connection.execute(
            "SELECT exit_triggered_at,trigger_reference_price FROM sol_v2_exit_intents WHERE position_id=? AND exit_reason=?",
            (position.position_id, reason),
        ).fetchone()
        reference_at = datetime.fromisoformat(intent[0]) if intent and intent[0] else now
        if intent and intent[1] is not None:
            reference_price = Decimal(str(intent[1]))
        super()._close_position(position, reason, now)
        self._route_attempted(position, now, position.status == "CLOSED")
        if position.status == "CLOSED":
            quote = self.quote_provider.consume_delivered(position.mint, "sell", quantity) if hasattr(self.quote_provider, "consume_delivered") else None
            self._record_trade_execution(position=position, side="SELL", reason=reason, reference_at=reference_at,
                                         reference_price=reference_price, quote=quote, fill_at=now)
            self._settled(position, reason, now)

    def _resume_exit_intents(self, now: datetime) -> None:
        rows = self.connection.execute(
            "SELECT position_id,exit_reason,requested_initial_pct FROM sol_v2_exit_intents WHERE status='PENDING_ROUTE' ORDER BY exit_triggered_at"
        ).fetchall()
        for position_id, reason, pct in rows:
            position = self._positions.get(position_id)
            if position is None or position.status != "OPEN":
                self._settled(type("P", (), {"position_id": position_id})(), reason, now)
                continue
            if pct is None:
                self._close_position(position, reason, now)
            else:
                result = self._partial_exit_of_initial(position, Decimal(str(pct)), reason, now)
                if result and reason.startswith("TP2") and position.status == "OPEN":
                    position.trailing_active = False
                    self._persist_position(position, now, reason=None)
                elif result and reason.startswith("TP3") and position.status == "OPEN":
                    position.trailing_active = True
                    self._persist_position(position, now, reason=None)

    def evaluate(self, now: datetime | None = None) -> None:
        current = now or self.clock()
        started = time.monotonic()
        with self._lock:
            self._resume_exit_intents(current)
            position_started = time.monotonic()
            self._evaluate_positions(current)
            self.v2_position_evaluate_ms = int((time.monotonic() - position_started) * 1000)
            self._drain_audit_prefetch_results(current)
            while self._pending_events:
                self._apply_event(self._pending_events.popleft())
            if self._pending_gap_events and not self._pending_events:
                self._apply_event(self._pending_gap_events.popleft())
            self._run_factory_gap_fill(current)
            self._drain_pre_migration_venue_results(current)
            if self._factory_wss_seen_healthy and not self._pending_events:
                self._activate_one_fourmeme_bonding_candidate(current)
            if not self._pending_events:
                self._activate_one_pre_migration_candidate(current)
            if self._venue_history_backfill_enabled and self._factory_wss_seen_healthy and not self._pending_events:
                self._hydrate_legacy_venue_registry(current)
            active = [candidate for candidate in self._candidates.values() if candidate.active_candidate]
            background = [candidate for candidate in self._candidates.values() if not candidate.active_candidate]
            batch: list[Any] = []
            if background:
                start = self._v2_candidate_cursor % len(background)
                batch = (background + background)[start:start + min(20, len(background))]
                self._v2_candidate_cursor = (start + len(batch)) % len(background)
            for candidate in active + batch:
                self._evaluate_candidate(candidate, current)
                if candidate.active_candidate:
                    self._persist_candidate(candidate, current)
            self._publish(current)
        self._main_loop_durations_ms.append(int((time.monotonic() - started) * 1000))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def percentile(values: deque[int], fraction: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]


def import_v1_open_positions(target: sqlite3.Connection, source_path: Path) -> int:
    """Read-only snapshot import; never modifies the V1 database."""
    if not source_path.exists():
        return 0
    source = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
    source.row_factory = sqlite3.Row
    imported = 0
    try:
        target_cols = {row[1] for row in target.execute("PRAGMA table_info(survivor_positions)")}
        source_cols = {row[1] for row in source.execute("PRAGMA table_info(survivor_positions)")}
        cols = sorted(target_cols & source_cols)
        placeholders = ",".join("?" for _ in cols)
        for row in source.execute("SELECT * FROM survivor_positions WHERE status='OPEN'"):
            target.execute(
                f"INSERT OR IGNORE INTO survivor_positions({','.join(cols)}) VALUES({placeholders})",
                tuple(row[col] for col in cols),
            )
            imported += int(target.execute("SELECT changes()").fetchone()[0] > 0)
        candidate_target = {row[1] for row in target.execute("PRAGMA table_info(survivor_candidates)")}
        candidate_source = {row[1] for row in source.execute("PRAGMA table_info(survivor_candidates)")}
        candidate_cols = sorted(candidate_target & candidate_source)
        for row in source.execute("SELECT c.* FROM survivor_candidates c JOIN survivor_positions p ON p.mint=c.mint WHERE p.status='OPEN'"):
            target.execute(
                f"INSERT OR IGNORE INTO survivor_candidates({','.join(candidate_cols)}) VALUES({','.join('?' for _ in candidate_cols)})",
                tuple(row[col] for col in candidate_cols),
            )
        target.execute("CREATE TABLE IF NOT EXISTS sol_v2_imports(position_id TEXT PRIMARY KEY,source_runtime TEXT NOT NULL,source_position_id TEXT NOT NULL,imported_at TEXT NOT NULL)")
        source_open_ids = tuple(row[0] for row in source.execute("SELECT position_id FROM survivor_positions WHERE status='OPEN'"))
        for position_id in source_open_ids:
            target.execute("INSERT OR IGNORE INTO sol_v2_imports VALUES(?,?,?,?)", (position_id, "V1", position_id, utc_now().isoformat()))
        imported_ids = tuple(row[0] for row in target.execute("SELECT position_id FROM sol_v2_imports WHERE source_runtime='V1'"))
        if imported_ids:
            marks = ",".join("?" for _ in imported_ids)
            target.execute(f"UPDATE survivor_positions SET current_price_native=NULL WHERE position_id IN ({marks})", imported_ids)
            imported_mints = tuple(row[0] for row in target.execute(
                f"SELECT DISTINCT mint FROM survivor_positions WHERE position_id IN ({marks})", imported_ids
            ))
            if imported_mints:
                mint_marks = ",".join("?" for _ in imported_mints)
                target.execute(
                    f"UPDATE survivor_candidates SET current_price_native=NULL,current_price_usd=NULL,price_updated_at=NULL,price_age_ms=NULL,price_status='PENDING' WHERE mint IN ({mint_marks})",
                    imported_mints,
                )
        target.commit()
    finally:
        source.close()
    return imported


class V2Runtime:
    def __init__(self, root: Path, v1_db: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = initialize_database(root / "runtime.db")
        import_v1_open_positions(self.db, v1_db)
        self.db.execute("CREATE TABLE IF NOT EXISTS sol_v2_exit_intents(position_id TEXT NOT NULL,exit_reason TEXT NOT NULL,exit_triggered_at TEXT NOT NULL,trigger_reference_price TEXT,requested_initial_pct TEXT,status TEXT NOT NULL,updated_at TEXT NOT NULL,PRIMARY KEY(position_id,exit_reason))")
        self.db.execute("CREATE TABLE IF NOT EXISTS sol_v2_position_freshness(position_id TEXT PRIMARY KEY,last_position_evaluate_at TEXT,last_wss_price_at TEXT,last_chain_activity_at TEXT,last_exit_quote_at TEXT,last_successful_exit_route_at TEXT,state TEXT NOT NULL,updated_at TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS sol_v2_subscriptions(token TEXT NOT NULL,pool_or_program TEXT NOT NULL,subscription_id INTEGER,subscription_type TEXT NOT NULL,ack INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL,last_event_at TEXT,last_event_slot INTEGER,status TEXT NOT NULL,PRIMARY KEY(token,pool_or_program,subscription_type))")
        self.db.execute("CREATE TABLE IF NOT EXISTS sol_v2_gap_cursors(token TEXT PRIMARY KEY,last_processed_slot INTEGER,last_processed_signature TEXT,updated_at TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS sol_v2_entry_opportunities(opportunity_id TEXT PRIMARY KEY,mint TEXT NOT NULL,symbol TEXT,signal_at TEXT NOT NULL,signal_price TEXT,state TEXT NOT NULL,skipped_reason TEXT,position_id TEXT,updated_at TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS sol_v2_trade_executions(execution_id TEXT PRIMARY KEY,position_id TEXT NOT NULL,mint TEXT NOT NULL,side TEXT NOT NULL,reason TEXT NOT NULL,reference_at TEXT NOT NULL,reference_price TEXT,quote_request_at TEXT,quote_at TEXT NOT NULL,sim_fill_at TEXT NOT NULL,quote_price TEXT NOT NULL,sim_fill_price TEXT NOT NULL,provider TEXT NOT NULL,quote_latency_ms INTEGER,price_impact_pct TEXT,route_json TEXT NOT NULL,execution_latency_ms INTEGER NOT NULL,slippage_vs_reference_pct TEXT)")
        self.db.execute("CREATE TABLE IF NOT EXISTS sol_v2_quote_attempts(attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,mint TEXT NOT NULL,side TEXT NOT NULL,input_quantity TEXT NOT NULL,provider TEXT NOT NULL,requested_at TEXT NOT NULL,quoted_at TEXT,latency_ms INTEGER,success INTEGER NOT NULL,failure_reason TEXT,price_impact_pct TEXT,route_json TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS sol_v2_pipeline_latency(signature TEXT NOT NULL,mint TEXT NOT NULL,chain_block_at TEXT,wss_received_at TEXT NOT NULL,transaction_available_at TEXT NOT NULL,parsed_at TEXT NOT NULL,strategy_evaluated_at TEXT NOT NULL,chain_to_wss_ms INTEGER,wss_to_get_transaction_ms INTEGER NOT NULL,get_transaction_retry_ms INTEGER NOT NULL,parsing_ms INTEGER NOT NULL,queue_wait_ms INTEGER NOT NULL,strategy_evaluate_ms INTEGER NOT NULL,retry_count INTEGER NOT NULL,rpc_http_429_count INTEGER NOT NULL,PRIMARY KEY(signature,mint))")
        self.db.execute("CREATE TABLE IF NOT EXISTS sol_v2_realtime_switch(switch_id TEXT PRIMARY KEY,started_at TEXT NOT NULL,before_health_json TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS sol_v2_realtime_capability(provider TEXT PRIMARY KEY,probed_at TEXT NOT NULL,api_available INTEGER NOT NULL,slot_supported INTEGER NOT NULL,transaction_state TEXT NOT NULL,error_message TEXT)")
        self.db.execute("CREATE TABLE IF NOT EXISTS sol_v2_realtime_pipeline(signature TEXT NOT NULL,mint TEXT NOT NULL,source_mode TEXT NOT NULL,chain_block_at TEXT,helius_received_at TEXT NOT NULL,parsed_at TEXT NOT NULL,strategy_evaluated_at TEXT NOT NULL,price_updated_at TEXT,get_transaction_requested_at TEXT,get_transaction_received_at TEXT,chain_to_helius_ms INTEGER,helius_to_parse_ms INTEGER,parse_to_strategy_ms INTEGER,strategy_to_price_update_ms INTEGER,chain_to_price_update_ms INTEGER,PRIMARY KEY(signature,mint,source_mode))")
        switch = self.db.execute("SELECT started_at,before_health_json FROM sol_v2_realtime_switch WHERE switch_id=?", (HELIUS_SWITCH_ID,)).fetchone()
        if switch is None:
            try:
                before_health = json.loads((root / "health.json").read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                before_health = {}
            switch_started_at = utc_now().isoformat()
            self.db.execute(
                "INSERT INTO sol_v2_realtime_switch(switch_id,started_at,before_health_json) VALUES(?,?,?)",
                (HELIUS_SWITCH_ID, switch_started_at, json.dumps(before_health, ensure_ascii=False, sort_keys=True)),
            )
            self.helius_switch_started_at = datetime.fromisoformat(switch_started_at)
            self.helius_before_health = before_health
        else:
            self.helius_switch_started_at = datetime.fromisoformat(str(switch[0]))
            try:
                self.helius_before_health = json.loads(str(switch[1]))
            except json.JSONDecodeError:
                self.helius_before_health = {}
        self.db.execute("UPDATE sol_v2_entry_opportunities SET state='FILLED',skipped_reason=NULL WHERE position_id IS NOT NULL AND state!='FILLED'")
        # Remove historical rows produced by the pre-fix instrumentation when
        # a mint already bought within 24 hours was repeatedly counted as a
        # fresh execution opportunity.  Filled and genuine route failures are
        # preserved.
        self.db.execute(
            "DELETE FROM sol_v2_entry_opportunities AS opportunity "
            "WHERE state='SKIPPED' AND skipped_reason='OTHER' AND EXISTS ("
            "SELECT 1 FROM survivor_positions AS position WHERE position.mint=opportunity.mint "
            "AND julianday(position.opened_at)<=julianday(opportunity.signal_at) "
            "AND julianday(position.opened_at)>=julianday(opportunity.signal_at)-1)"
        )
        self.db.execute(
            "DELETE FROM sol_v2_entry_opportunities AS opportunity WHERE state='SKIPPED' AND EXISTS ("
            "SELECT 1 FROM sol_v2_entry_opportunities AS filled "
            "WHERE filled.mint=opportunity.mint AND filled.state='FILLED')"
        )
        self.db.execute("INSERT OR IGNORE INTO sol_v2_subscriptions(token,pool_or_program,subscription_type,created_at,status) VALUES('__RUNTIME__','slot','slotSubscribe',?,'PENDING_ACK')", (utc_now().isoformat(),))
        self.db.commit()
        self.imported = int(self.db.execute("SELECT COUNT(*) FROM sol_v2_imports").fetchone()[0])
        self.health = HealthRegistry(root / "engine_health.json")
        self.audit = JsonlAuditWriter(root / "audit.jsonl")
        self.control = RuntimeControl(root / "runtime_control.json")
        self.config = SolSurvivorReversalConfig.from_env()
        self._verify_safety_and_config()
        helius_http, self.helius_wss_url = helius_endpoints()
        fallback_rpc = SolanaRpcClient.from_env()
        rpc_urls = tuple(url for url in ((helius_http,) + fallback_rpc.urls) if url)
        self.rpc = SolanaRpcClient(
            urls=rpc_urls,
            timeout_sec=fallback_rpc.timeout_sec,
            max_retries=fallback_rpc.max_retries,
        )
        self.swap_rpc = V2RpcMetricsProxy(self.rpc)
        self.decimals = TokenDecimalsCache.from_env(rpc=self.rpc, overrides=_token_decimals())
        self.price_monitor = V2PriceMonitor(PumpReadOnlyAdapter(self.rpc), self.decimals.resolve)
        self.jupiter = JupiterReadOnlyQuoteProvider.from_env(
            decimals_resolver=self.decimals.resolve,
            swap_v2_price_impact=True,
        )
        self.direct = PumpProtocolReadOnlyQuoteProvider(self.price_monitor.pump_adapter, decimals_resolver=self.decimals.resolve)
        benchmark_path = Path(os.environ.get("SOL_V2_BINANCE_BENCHMARK_PATH", "reports/sol_survivor_v2/binance_agentic_wallet_sol_benchmark.json"))
        try:
            benchmark = json.loads(benchmark_path.read_text(encoding="utf-8"))
            self.route_policy = str(benchmark.get("selected_route_policy") or "JUPITER_DIRECT_PRIMARY")
            self.baw_benchmark = benchmark
        except (OSError, json.JSONDecodeError):
            self.route_policy = "JUPITER_DIRECT_PRIMARY"
            self.baw_benchmark = {}
        if self.route_policy not in {"BINANCE_PRIMARY", "HYBRID", "JUPITER_DIRECT_PRIMARY"}:
            raise RuntimeError("INVALID_SOL_V2_ROUTE_POLICY")
        self.baw = SolanaAgenticWalletQuoteProvider(timeout_sec=float(os.environ.get("SOL_V2_BAW_TIMEOUT_SEC", "8"))) if shutil.which("baw") else None
        self.router = V2QuoteRouter(self.direct, self.jupiter, self.baw, self.config, self.route_policy)
        self.engine = V2ShadowEngine(
            connection=self.db, store=RuntimeStore(self.db, "paper"), health=self.health,
            audit=self.audit, controls=self.control, config=self.config,
            quote_provider=self.router, price_monitor=self.price_monitor, smart_money=None,
            swap_rpc=self.swap_rpc,
        )
        self.source = BinanceWeb3SignalSource(BinanceWeb3Client.from_env(), chain_id="CT_501", rank_types=(10, 20, 30), limit=200)
        self.exit_slots = threading.BoundedSemaphore(2)
        self.normal_slots = threading.BoundedSemaphore(4)
        self.results: Queue[tuple[str, Any, int, str | None]] = Queue(maxsize=512)
        self.events: Queue[dict[str, Any]] = Queue(maxsize=4096)
        self._event_coalesce_lock = threading.Lock()
        self._position_event_latest: dict[str, dict[str, Any]] = {}
        self._candidate_event_latest: dict[str, dict[str, Any]] = {}
        self.helius_standard_event_replaced = 0
        self.helius_position_events_processed = 0
        self.helius_candidate_events_processed = 0
        self.acks: Queue[dict[str, Any]] = Queue(maxsize=512)
        self.pending_discovery: deque[Any] = deque(maxlen=5000)
        self.stop = threading.Event()
        self.discovery_inflight = False
        self.binding_inflight = False
        self.swap_rpc_inflight = False
        self.tick_ms: deque[int] = deque(maxlen=4096)
        self.position_lag_ms: deque[int] = deque(maxlen=4096)
        self.max_stall_ms = 0
        self.stage_max_ms = {name: 0 for name in ("results","wss_events","engine","discovery_apply","freshness","scheduling","health")}
        self.last_tick_at: datetime | None = None
        self.last_slot: int | None = None
        self.last_slot_at: datetime | None = None
        self.last_reconnect_count = 0
        self.gap_fill_runs = 0
        self.gap_fill_success = 0
        self.transaction_subscribe_capability = "PENDING_PROBE"
        self.transaction_subscribe_error: str | None = None
        self.helius_api_available = False
        self.helius_slot_supported = False
        self.helius_standard_event_count = 0
        self._open_position_mints: set[str] = set()
        self.capability_probe_inflight = False
        self.baw_state = "PENDING_PROBE" if shutil.which("baw") else "NOT_CONFIGURED"
        self.baw_error: str | None = None
        self.baw_probe_inflight = False
        self.last_freshness_write = 0.0
        self.last_gap_fill_request = 0.0
        fallback_wss = SolanaWssMonitor(stale_after_sec=30)
        wss_urls = tuple(url for url in ((self.helius_wss_url,) + fallback_wss.urls) if url)
        self.wss = SolanaWssMonitor(urls=wss_urls, stale_after_sec=30)
        self.wss.replace_subscriptions((("slotSubscribe", []),))
        enhanced_urls = (self.helius_wss_url,) if self.helius_wss_url else ()
        self.transaction_wss = V2TransactionMonitor(enhanced_urls, self.events)
        self.transaction_wss_thread = threading.Thread(target=self.transaction_wss.run, name="sol-v2-transaction-wss", daemon=True)
        self.wss_loop: asyncio.AbstractEventLoop | None = None
        self.wss_stop: asyncio.Event | None = None
        self.wss_thread = threading.Thread(target=self._run_wss, name="sol-v2-wss", daemon=True)

    def _verify_safety_and_config(self) -> None:
        required = {"PAPER_ONLY": "true", "ALLOW_LIVE_TRADING": "false", "LIVE_TRADING": "false", "SIGNING_ENABLED": "false", "BROADCAST_ENABLED": "false", "SOL_SIGNING_ENABLED": "false", "SOL_BROADCAST_ENABLED": "false"}
        for key, expected in required.items():
            if os.environ.get(key, expected).strip().lower() != expected:
                raise RuntimeError(f"V2 safety violation: {key}")
        effective = self.config.effective_values()
        digest = hashlib.sha256(json.dumps(effective, sort_keys=True, default=str).encode()).hexdigest()
        snapshot = self.root / "strategy_config.sha256"
        if snapshot.exists() and snapshot.read_text().strip() != digest:
            raise RuntimeError("CONFIG_DRIFT")
        if not snapshot.exists():
            snapshot.write_text(digest + "\n")

    def _run_wss(self) -> None:
        loop = asyncio.new_event_loop(); self.wss_loop = loop; asyncio.set_event_loop(loop)
        stop = asyncio.Event(); self.wss_stop = stop
        async def callback(event: dict[str, Any]) -> None:
            if event.get("method") == "slotNotification":
                params = event.get("params")
                result = params.get("result") if isinstance(params, dict) else None
                value = result.get("slot") if isinstance(result, dict) else None
                if isinstance(value, int):
                    self.last_slot, self.last_slot_at = value, utc_now()
                return
            if isinstance(event.get("id"), int) and isinstance(event.get("result"), int):
                try: self.acks.put_nowait(dict(event))
                except Exception: pass
                return
            event_copy = dict(event)
            if event.get("method") == "accountNotification":
                self.helius_standard_event_count += 1
            subscription_params = event.get("_subscription_params")
            address = subscription_params[0] if isinstance(subscription_params, list) and subscription_params else None
            binding = self.price_monitor.binding_for_account(address) if isinstance(address, str) else None
            if binding is not None and isinstance(address, str):
                with self._event_coalesce_lock:
                    target = self._position_event_latest if binding.mint in self._open_position_mints else self._candidate_event_latest
                    self.helius_standard_event_replaced += int(address in target)
                    target[address] = event_copy
                return
            try: self.events.put_nowait(event_copy)
            except Exception: pass
        loop.run_until_complete(self.wss.run(callback, stop))

    def _submit(self, lane: str, kind: str, fn: Any) -> bool:
        slots = self.exit_slots if lane == "exit" else self.normal_slots
        if not slots.acquire(blocking=False):
            return False
        started = time.monotonic()
        def work() -> None:
            try:
                try: value, error = fn(), None
                except Exception as exc: value, error = None, getattr(exc, "error_class", type(exc).__name__)
                elapsed = int((time.monotonic() - started) * 1000)
                try: self.results.put_nowait((kind, value, elapsed, error))
                except Exception: pass
            finally:
                slots.release()
        threading.Thread(target=work, daemon=True, name=f"sol-v2-{lane}-{kind}").start()
        return True

    def _schedule_discovery(self) -> None:
        if self.discovery_inflight: return
        self.discovery_inflight = True
        if not self._submit("normal", "discovery", self.source.fetch_once):
            self.discovery_inflight = False

    def _schedule_bindings(self) -> None:
        if self.binding_inflight: return
        self.binding_inflight = True
        mints = tuple(sorted(
            {p.mint for p in self.engine._positions.values() if p.status == "OPEN"}
            | {c.mint for c in self.engine._candidates.values() if c.active_candidate}
        ))
        def bind() -> tuple[tuple[tuple[str, list[object]], ...], tuple[Any, ...]]:
            completed: Queue[str] = Queue()
            def one(mint: str) -> None:
                try:
                    self.price_monitor.register_position(mint)
                except Exception:
                    pass
                finally:
                    completed.put(mint)
            for mint in mints:
                threading.Thread(target=one, args=(mint,), daemon=True, name="sol-v2-binding").start()
            deadline = time.monotonic() + 5.0
            seen = 0
            while seen < len(mints) and time.monotonic() < deadline:
                try:
                    completed.get(timeout=min(.05, max(.001, deadline - time.monotonic())))
                    seen += 1
                except Empty:
                    pass
            self.price_monitor.unregister_missing(set(mints))
            return self.price_monitor.subscriptions(), self.price_monitor.bindings()
        if not self._submit("normal", "bindings", bind):
            self.binding_inflight = False

    def _schedule_swap_rpc(self) -> None:
        if self.swap_rpc_inflight:
            return
        self.swap_rpc_inflight = True
        if not self._submit("normal", "swap_rpc", lambda: self.engine.poll_swap_rpc_work(max_accounts=4, max_transactions=8)):
            self.swap_rpc_inflight = False

    def _schedule_capability_probe(self) -> None:
        if self.capability_probe_inflight or self.transaction_subscribe_capability != "PENDING_PROBE":
            return
        self.capability_probe_inflight = True
        def probe() -> dict[str, Any]:
            if not self.helius_wss_url:
                return {"api_available": False, "slot_supported": False, "state": "NOT_CONFIGURED", "error": "HELIUS_API_KEY_MISSING"}
            async def request() -> dict[str, Any]:
                import websockets
                async with websockets.connect(self.helius_wss_url, ping_interval=20, ping_timeout=20, close_timeout=1) as socket:
                    await socket.send(json.dumps({"jsonrpc":"2.0","id":990,"method":"slotSubscribe","params":[]}))
                    slot_reply = json.loads(await asyncio.wait_for(socket.recv(), timeout=3.0))
                    slot_supported = isinstance(slot_reply, dict) and isinstance(slot_reply.get("result"), int)
                    await socket.send(json.dumps({"jsonrpc":"2.0","id":991,"method":"transactionSubscribe","params":[
                        {"vote":False,"failed":False,"accountInclude":["6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"]},
                        {"commitment":"confirmed","encoding":"jsonParsed","transactionDetails":"full","showRewards":False,"maxSupportedTransactionVersion":0},
                    ]}))
                    deadline = time.monotonic() + 3.0
                    transaction_reply = None
                    while time.monotonic() < deadline:
                        reply = json.loads(await asyncio.wait_for(socket.recv(), timeout=max(.1, deadline-time.monotonic())))
                        if isinstance(reply, dict) and reply.get("id") == 991:
                            transaction_reply = reply
                            break
                    state, error = classify_helius_transaction_reply(transaction_reply)
                    return {"api_available": True, "slot_supported": slot_supported, "state": state, "error": error}
            try:
                return asyncio.run(request())
            except Exception as exc:
                return {"api_available": False, "slot_supported": False, "state": "DEGRADED_FALLBACK_ACTIVE", "error": type(exc).__name__}
        if not self._submit("normal", "capability_probe", probe):
            self.capability_probe_inflight = False

    def _schedule_baw_probe(self) -> None:
        if self.baw_probe_inflight or self.baw_state != "PENDING_PROBE":
            return
        self.baw_probe_inflight = True
        def probe() -> tuple[str, str | None]:
            try:
                completed = subprocess.run(["baw", "wallet", "status", "--json"], capture_output=True, text=True, timeout=3, check=False)
                payload = json.loads(completed.stdout or "{}")
                if completed.returncode != 0 or not isinstance(payload, dict) or payload.get("success") is not True:
                    return "NOT_AUTHENTICATED", "WALLET_STATUS_FAILED"
                # The reusable adapter in this repository is BSC-only. Do not
                # guess a Solana CLI command or allow it into provider races.
                return "DEGRADED", "SOLANA_QUOTE_ADAPTER_UNAVAILABLE"
            except subprocess.TimeoutExpired:
                return "DEGRADED", "STATUS_TIMEOUT"
            except Exception as exc:
                return "DEGRADED", type(exc).__name__
        if not self._submit("normal", "baw_probe", probe):
            self.baw_probe_inflight = False

    def _request_gap_fill(self) -> None:
        if time.monotonic() - self.last_gap_fill_request < .5:
            return
        self.last_gap_fill_request = time.monotonic()
        now = utc_now()
        queued = 0
        with self.engine._lock:
            for binding in self.price_monitor.bindings():
                for address in binding.account_addresses:
                    if address in self.engine._queued_trigger_accounts:
                        continue
                    self.engine._swap_trigger_queue.append((now, binding.mint, address, self.last_slot))
                    self.engine._queued_trigger_accounts.add(address)
                    queued += 1
        self.gap_fill_runs += 1
        if queued:
            self.gap_fill_success += 1

    def _write_freshness(self, now: datetime) -> None:
        if time.monotonic() - self.last_freshness_write < 1.0:
            return
        self.last_freshness_write = time.monotonic()
        pending = {row[0] for row in self.db.execute("SELECT position_id FROM sol_v2_exit_intents WHERE status='PENDING_ROUTE'")}
        open_ids = tuple(position.position_id for position in self.engine._positions.values() if position.status == "OPEN")
        self._open_position_mints = {position.mint for position in self.engine._positions.values() if position.status == "OPEN"}
        if open_ids:
            marks = ",".join("?" for _ in open_ids)
            self.db.execute(f"DELETE FROM sol_v2_position_freshness WHERE position_id NOT IN ({marks})", open_ids)
        else:
            self.db.execute("DELETE FROM sol_v2_position_freshness")
        for position in self.engine._positions.values():
            if position.status != "OPEN":
                continue
            candidate = self.engine._candidates.get(position.mint)
            price_at = candidate.price_updated_at if candidate is not None else None
            if position.position_id in pending:
                state = "EXIT_TRIGGERED_WAITING_ROUTE"
            elif price_at is None:
                state = "NO_NEW_CHAIN_ACTIVITY"
            elif (now - price_at).total_seconds() > self.config.idle_ttl_sec:
                state = "PRICE_STALE"
            else:
                state = "LIVE"
            self.db.execute(
                "INSERT INTO sol_v2_position_freshness(position_id,last_position_evaluate_at,last_wss_price_at,last_chain_activity_at,last_exit_quote_at,last_successful_exit_route_at,state,updated_at) VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(position_id) DO UPDATE SET last_position_evaluate_at=excluded.last_position_evaluate_at,last_wss_price_at=excluded.last_wss_price_at,last_chain_activity_at=excluded.last_chain_activity_at,state=excluded.state,updated_at=excluded.updated_at",
                (position.position_id, now.isoformat(), price_at.isoformat() if price_at else None,
                 price_at.isoformat() if price_at else None, None, None, state, now.isoformat()),
            )
        tracked = {p.mint for p in self.engine._positions.values() if p.status == "OPEN"} | {
            c.mint for c in self.engine._candidates.values() if c.active_candidate
        }
        for mint in tracked:
            latest = self.db.execute(
                "SELECT slot,signature FROM sol_survivor_swap_events WHERE mint=? ORDER BY parse_finished_at DESC LIMIT 1", (mint,)
            ).fetchone()
            self.db.execute(
                "INSERT INTO sol_v2_gap_cursors(token,last_processed_slot,last_processed_signature,updated_at) VALUES(?,?,?,?) "
                "ON CONFLICT(token) DO UPDATE SET last_processed_slot=COALESCE(excluded.last_processed_slot,last_processed_slot),last_processed_signature=COALESCE(excluded.last_processed_signature,last_processed_signature),updated_at=excluded.updated_at",
                (mint, latest[0] if latest else None, latest[1] if latest else None, now.isoformat()),
            )
        self.db.commit()

    def _drain_results(self) -> None:
        for _ in range(128):
            try: kind, value, _elapsed, error = self.results.get_nowait()
            except Empty: break
            if kind == "discovery":
                self.discovery_inflight = False
                if error is None and value:
                    self.pending_discovery.extend(value)
            elif kind == "bindings":
                self.binding_inflight = False
                if error is None:
                    subscriptions, bindings = value
                    self.engine._bindings = {binding.mint: binding for binding in bindings}
                    transaction_accounts = {binding.mint for binding in bindings} | {
                        address for binding in bindings for address in binding.account_addresses
                    }
                    self.transaction_wss.configure(transaction_accounts, self.transaction_subscribe_capability.startswith("SUPPORTED"))
                    self.wss.replace_subscriptions((("slotSubscribe", []),) + tuple(subscriptions))
                    created = utc_now().isoformat()
                    desired_addresses = {str(binding_address) for binding in bindings for binding_address in binding.account_addresses}
                    if desired_addresses:
                        marks = ",".join("?" for _ in desired_addresses)
                        self.db.execute(f"DELETE FROM sol_v2_subscriptions WHERE subscription_type='accountSubscribe' AND pool_or_program NOT IN ({marks})", tuple(desired_addresses))
                    else:
                        self.db.execute("DELETE FROM sol_v2_subscriptions WHERE subscription_type='accountSubscribe'")
                    for binding in bindings:
                        for address in binding.account_addresses:
                            self.db.execute("INSERT OR IGNORE INTO sol_v2_subscriptions(token,pool_or_program,subscription_type,created_at,status) VALUES(?,?,?,?,'PENDING_ACK')",
                                            (binding.mint, address, "accountSubscribe", created))
                    self.db.commit()
            elif kind == "swap_rpc":
                self.swap_rpc_inflight = False
                if error is None:
                    self.engine.drain_swap_results(max_results=1)
                    self._drain_pipeline_latency()
            elif kind == "capability_probe":
                self.capability_probe_inflight = False
                if error is None and isinstance(value, dict):
                    self.helius_api_available = bool(value.get("api_available"))
                    self.helius_slot_supported = bool(value.get("slot_supported"))
                    self.transaction_subscribe_capability = str(value.get("state") or "DEGRADED_FALLBACK_ACTIVE")
                    self.transaction_subscribe_error = str(value.get("error"))[:240] if value.get("error") else None
                    self.db.execute(
                        "INSERT INTO sol_v2_realtime_capability(provider,probed_at,api_available,slot_supported,transaction_state,error_message) VALUES('HELIUS',?,?,?,?,?) ON CONFLICT(provider) DO UPDATE SET probed_at=excluded.probed_at,api_available=excluded.api_available,slot_supported=excluded.slot_supported,transaction_state=excluded.transaction_state,error_message=excluded.error_message",
                        (utc_now().isoformat(), int(self.helius_api_available), int(self.helius_slot_supported), self.transaction_subscribe_capability, self.transaction_subscribe_error),
                    )
                    self.db.commit()
                    accounts = {binding.mint for binding in self.price_monitor.bindings()} | {
                        address for binding in self.price_monitor.bindings() for address in binding.account_addresses
                    }
                    self.transaction_wss.configure(accounts, self.transaction_subscribe_capability.startswith("SUPPORTED"))
                else:
                    self.transaction_subscribe_capability, self.transaction_subscribe_error = "DEGRADED_FALLBACK_ACTIVE", error
            elif kind == "baw_probe":
                self.baw_probe_inflight = False
                if error is None and value:
                    self.baw_state, self.baw_error = value
                else:
                    self.baw_state, self.baw_error = "DEGRADED", error

    def _drain_quote_attempts(self) -> None:
        events = self.router.drain_attempt_events()
        if not events:
            return
        self.db.executemany(
            "INSERT INTO sol_v2_quote_attempts(mint,side,input_quantity,provider,requested_at,quoted_at,latency_ms,success,failure_reason,price_impact_pct,route_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            [(
                event["mint"], event["side"], event["input_quantity"], event["provider"], event["requested_at"],
                event["quoted_at"], event["latency_ms"], int(bool(event["success"])), event["failure_reason"],
                event["price_impact_pct"], json.dumps(event["route"], ensure_ascii=False),
            ) for event in events],
        )
        self.db.execute("DELETE FROM sol_v2_quote_attempts WHERE attempt_id <= COALESCE((SELECT MAX(attempt_id)-50000 FROM sol_v2_quote_attempts),-1)")
        self.db.commit()

    def _drain_pipeline_latency(self) -> None:
        records: list[dict[str, Any]] = []
        for _ in range(1000):
            try:
                records.append(self.engine.v2_latency_records.get_nowait())
            except Empty:
                break
        if not records:
            return
        evaluated_at = utc_now()
        for record in records:
            parsed_at = datetime.fromisoformat(record["parsed_at"])
            queue_wait_ms = max(0, int((evaluated_at - parsed_at).total_seconds() * 1000))
            self.db.execute(
                "INSERT OR REPLACE INTO sol_v2_pipeline_latency(signature,mint,chain_block_at,wss_received_at,transaction_available_at,parsed_at,strategy_evaluated_at,chain_to_wss_ms,wss_to_get_transaction_ms,get_transaction_retry_ms,parsing_ms,queue_wait_ms,strategy_evaluate_ms,retry_count,rpc_http_429_count) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    record["signature"], record["mint"], record["chain_block_at"], record["wss_received_at"],
                    record["transaction_available_at"], record["parsed_at"], evaluated_at.isoformat(), record["chain_to_wss_ms"],
                    record["wss_to_get_transaction_ms"], record["get_transaction_retry_ms"], record["parsing_ms"],
                    queue_wait_ms, self.engine.v2_position_evaluate_ms, record["retry_count"], record["rpc_http_429_count"],
                ),
            )
            if parsed_at >= self.helius_switch_started_at:
                chain_at = datetime.fromisoformat(record["chain_block_at"]) if record.get("chain_block_at") else None
                received_at = datetime.fromisoformat(record["wss_received_at"])
                candidate = self.engine._candidates.get(record["mint"])
                price_at = candidate.price_updated_at if candidate is not None else None
                if price_at is not None and price_at < received_at:
                    price_at = None
                self.db.execute(
                    "INSERT OR REPLACE INTO sol_v2_realtime_pipeline(signature,mint,source_mode,chain_block_at,helius_received_at,parsed_at,strategy_evaluated_at,price_updated_at,get_transaction_requested_at,get_transaction_received_at,chain_to_helius_ms,helius_to_parse_ms,parse_to_strategy_ms,strategy_to_price_update_ms,chain_to_price_update_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        record["signature"], record["mint"], "HELIUS_STANDARD_POST_SWITCH",
                        record.get("chain_block_at"), record["wss_received_at"], record["parsed_at"], evaluated_at.isoformat(),
                        price_at.isoformat() if price_at else None, record["wss_received_at"], record["transaction_available_at"],
                        max(0, int((received_at-chain_at).total_seconds()*1000)) if chain_at else None,
                        max(0, int((parsed_at-received_at).total_seconds()*1000)),
                        max(0, int((evaluated_at-parsed_at).total_seconds()*1000)),
                        max(0, int((price_at-evaluated_at).total_seconds()*1000)) if price_at and price_at >= evaluated_at else None,
                        max(0, int((price_at-chain_at).total_seconds()*1000)) if price_at and chain_at else None,
                    ),
                )
        self.db.execute("DELETE FROM sol_v2_pipeline_latency WHERE rowid NOT IN (SELECT rowid FROM sol_v2_pipeline_latency ORDER BY parsed_at DESC LIMIT 50000)")
        self.db.execute("DELETE FROM sol_v2_realtime_pipeline WHERE rowid NOT IN (SELECT rowid FROM sol_v2_realtime_pipeline ORDER BY parsed_at DESC LIMIT 50000)")
        self.db.commit()

    def _drain_discovery_batch(self, now: datetime) -> None:
        batch: list[Any] = []
        while self.pending_discovery and len(batch) < 5:
            batch.append(self.pending_discovery.popleft())
        if batch:
            self.engine.on_records(tuple(batch), now)

    def _drain_wss(self) -> None:
        for _ in range(128):
            try: ack = self.acks.get_nowait()
            except Empty: break
            self._apply_wss_ack(ack)
        for priority, limit in (("position", 4), ("candidate", 1)):
            for _ in range(limit):
                with self._event_coalesce_lock:
                    source = self._position_event_latest if priority == "position" else self._candidate_event_latest
                    address = next(iter(source), None)
                    event = source.pop(address) if address is not None else None
                if event is None:
                    break
                self._process_wss_event(event)
                if priority == "position":
                    self.helius_position_events_processed += 1
                else:
                    self.helius_candidate_events_processed += 1
        try: event = self.events.get_nowait()
        except Empty: event = None
        if event is not None:
            self._process_wss_event(event)

    def _process_wss_event(self, event: dict[str, Any]) -> None:
        request_id, result_id = event.get("id"), event.get("result")
        if event.get("method") == "v2TransactionAck":
            self.db.execute("INSERT INTO sol_v2_subscriptions(token,pool_or_program,subscription_id,subscription_type,ack,created_at,last_event_at,status) VALUES('__TRACKED__','tracked_accounts',?,'transactionSubscribe',1,?,?,'ACTIVE') ON CONFLICT(token,pool_or_program,subscription_type) DO UPDATE SET subscription_id=excluded.subscription_id,ack=1,last_event_at=excluded.last_event_at,status='ACTIVE'",
                            (event.get("subscription_id"), utc_now().isoformat(), utc_now().isoformat()))
            return
        if event.get("method") == "v2TransactionUnsupported":
            self.transaction_subscribe_capability = "UNSUPPORTED_FALLBACK_ACTIVE"
            self.transaction_subscribe_error = str(event.get("error_class") or "UNSUPPORTED")[:240]
            return
        if event.get("method") == "v2TransactionNotification":
            self._request_gap_fill()
            return
        if isinstance(request_id, int) and isinstance(result_id, int):
            self._apply_wss_ack(event)
            return
        if event.get("method") == "slotNotification":
            value = event.get("params", {}).get("result", {}).get("slot") if isinstance(event.get("params"), dict) else None
            if isinstance(value, int): self.last_slot, self.last_slot_at = value, utc_now()
            return
        subscription_id = event.get("_subscription_id")
        subscription_params = event.get("_subscription_params")
        if isinstance(subscription_id, int) and isinstance(subscription_params, list) and subscription_params:
            params = event.get("params", {})
            result = params.get("result", {}) if isinstance(params, dict) else {}
            context = result.get("context", {}) if isinstance(result, dict) else {}
            slot = context.get("slot") if isinstance(context, dict) else None
            self.db.execute("UPDATE sol_v2_subscriptions SET subscription_id=?,ack=1,last_event_at=?,last_event_slot=?,status='ACTIVE' WHERE pool_or_program=?",
                            (subscription_id, utc_now().isoformat(), slot, str(subscription_params[0])))
        observed = self.price_monitor.process_wss_event(event)
        if observed is not None: self.engine.on_solana_price(observed)
        self.engine.on_solana_account_event(event)

    def _apply_wss_ack(self, event: dict[str, Any]) -> None:
        request_id, result_id = event.get("id"), event.get("result")
        snapshot = self.wss.subscription_snapshot()
        if isinstance(request_id, int) and isinstance(result_id, int) and 0 < request_id <= len(snapshot):
            method, params = snapshot[request_id - 1]
            address = str(params[0]) if params else "slot"
            self.db.execute(
                "UPDATE sol_v2_subscriptions SET subscription_id=?,ack=1,last_event_at=?,status='ACTIVE' WHERE pool_or_program=? AND subscription_type=?",
                (result_id, utc_now().isoformat(), address, method),
            )

    def _write_health(self, duration_ms: int) -> None:
        wss = self.wss.health(); rpc = self.rpc.health()
        open_positions = [p for p in self.engine._positions.values() if p.status == "OPEN"]
        rpc_latencies = self.engine._get_transaction_latency_ms
        swap_latencies = deque(
            (int(row[0]) for row in self.db.execute("SELECT latency_ms FROM sol_survivor_swap_events WHERE latency_ms IS NOT NULL ORDER BY parse_finished_at DESC LIMIT 1000")),
            maxlen=1000,
        )
        pipeline_rows = self.db.execute(
            "SELECT chain_to_wss_ms,wss_to_get_transaction_ms,get_transaction_retry_ms,parsing_ms,queue_wait_ms,strategy_evaluate_ms,retry_count,rpc_http_429_count,"
            "(julianday(parsed_at)-julianday(wss_received_at))*86400000 FROM sol_v2_pipeline_latency ORDER BY parsed_at DESC LIMIT 1000"
        ).fetchall()
        def pipeline_metric(index: int) -> dict[str, Any]:
            values = deque((max(0, int(row[index])) for row in pipeline_rows if row[index] is not None), maxlen=1000)
            return {"samples": len(values), "p50_ms": percentile(values,.5), "p95_ms": percentile(values,.95), "p99_ms": percentile(values,.99)}
        helius_rows = self.db.execute(
            "SELECT chain_to_helius_ms,helius_to_parse_ms,parse_to_strategy_ms,strategy_to_price_update_ms,chain_to_price_update_ms FROM sol_v2_realtime_pipeline ORDER BY parsed_at DESC LIMIT 1000"
        ).fetchall()
        def helius_metric(index: int) -> dict[str, Any]:
            values = deque((max(0, int(row[index])) for row in helius_rows if row[index] is not None), maxlen=1000)
            return {"samples": len(values), "p50_ms": percentile(values,.5), "p95_ms": percentile(values,.95), "p99_ms": percentile(values,.99)}
        before_gettx = self.helius_before_health.get("get_transaction", {}) if isinstance(self.helius_before_health, dict) else {}
        before_latency = self.helius_before_health.get("latency_breakdown", {}) if isinstance(self.helius_before_health, dict) else {}
        before_calls = int(before_gettx.get("calls") or 0) if isinstance(before_gettx, dict) else 0
        before_events = int((before_latency.get("WSS_TO_PARSED") or {}).get("samples") or 0) if isinstance(before_latency, dict) else 0
        after_calls = max(0, self.engine._get_transaction_calls - before_calls)
        post_events = len(helius_rows)
        quote_attempts = self.db.execute(
            "SELECT side,COUNT(*),SUM(success),SUM(CASE WHEN failure_reason IN ('QUOTE_TIMEOUT','READ_TIMEOUT','TOTAL_TIMEOUT','TLS_HANDSHAKE_TIMEOUT') THEN 1 ELSE 0 END) FROM (SELECT * FROM sol_v2_quote_attempts ORDER BY attempt_id DESC LIMIT 10000) GROUP BY side"
        ).fetchall()
        quote_summary = {str(row[0]): {"calls": int(row[1]), "success": int(row[2] or 0), "timeout": int(row[3] or 0)} for row in quote_attempts}
        payload = {
            "runtime": IDENTITY, "pid": os.getpid(), "paper_only": True,
            "main_loop_last_tick_at": self.last_tick_at.isoformat() if self.last_tick_at else None,
            "main_loop_tick_duration_ms": duration_ms, "main_loop_p50_ms": percentile(self.tick_ms,.5),
            "main_loop_p95_ms": percentile(self.tick_ms,.95), "main_loop_p99_ms": percentile(self.tick_ms,.99),
            "main_loop_max_stall_ms": self.max_stall_ms,
            "main_loop_stage_max_ms": dict(self.stage_max_ms),
            "wss": {"state": wss.state, "provider": "helius_standard_wss" if self.helius_wss_url else "configured_solana_wss", "latest_slot": self.last_slot, "slot_heartbeat_at": self.last_slot_at.isoformat() if self.last_slot_at else None, "subscriptions": wss.subscriptions, "reconnect_count": wss.reconnect_count, "disconnect_count": wss.disconnect_count},
            "transaction_subscribe": {"state": self.transaction_subscribe_capability, "error_class": self.transaction_subscribe_error, "fallback": "logs/account + getTransaction"},
            "transaction_wss": {"state": self.transaction_wss.state, "subscription_id_ack": self.transaction_wss.subscription_id is not None,
                                "reconnect_count": self.transaction_wss.reconnect_count, "disconnect_count": self.transaction_wss.disconnect_count,
                                "last_event_at": self.transaction_wss.last_event_at.isoformat() if self.transaction_wss.last_event_at else None},
            "helius_realtime": {
                "api_available": self.helius_api_available,
                "slot_subscribe_supported": self.helius_slot_supported,
                "primary": "HELIUS_TRANSACTION_SUBSCRIBE" if self.transaction_subscribe_capability == "SUPPORTED" else "HELIUS_STANDARD_WSS",
                "fallback": "HELIUS_STANDARD_WSS",
                "recovery": "HELIUS_HTTP_RPC_GET_SIGNATURES_AND_TRANSACTION",
                "enhanced_state": self.transaction_subscribe_capability,
                "enhanced_error": self.transaction_subscribe_error,
                "enhanced_accounts": len(self.transaction_wss.snapshot()[0]),
                "enhanced_ack_count": self.transaction_wss.ack_count,
                "enhanced_event_count": self.transaction_wss.event_count,
                "standard_event_count": self.helius_standard_event_count,
                "standard_event_replaced_by_newer": self.helius_standard_event_replaced,
                "position_events_processed": self.helius_position_events_processed,
                "candidate_events_processed": self.helius_candidate_events_processed,
                "pending_position_accounts": len(self._position_event_latest),
                "pending_candidate_accounts": len(self._candidate_event_latest),
                "post_switch_started_at": self.helius_switch_started_at.isoformat(),
                "latency": {
                    "CHAIN_TO_HELIUS": helius_metric(0), "HELIUS_TO_PARSE": helius_metric(1),
                    "PARSE_TO_STRATEGY": helius_metric(2), "STRATEGY_TO_PRICE_UPDATE": helius_metric(3),
                    "CHAIN_TO_PRICE_UPDATE": helius_metric(4),
                },
                "get_transaction_rate": {
                    "before_calls": before_calls, "before_events": before_events,
                    "before_calls_per_event": round(before_calls/before_events, 4) if before_events else None,
                    "after_calls": after_calls, "after_events": post_events,
                    "after_calls_per_event": round(after_calls/post_events, 4) if post_events else None,
                },
            },
            "http_rpc": {"state": rpc.state, "last_error_class": rpc.last_error_class, "latency_ms": rpc.latency_ms},
            "get_transaction": {"calls": self.engine._get_transaction_calls, "failures": self.engine._get_transaction_failures,
                                "http_429": self.engine._get_transaction_429, "not_available_yet": self.engine.transaction_not_available_yet,
                                "http_429_ratio_pct": round(self.engine._get_transaction_429 * 100 / self.engine._get_transaction_calls, 4) if self.engine._get_transaction_calls else 0,
                                "retry_count": sum(int(row[6] or 0) for row in pipeline_rows),
                                "retry_extra_delay_ms": sum(int(row[2] or 0) for row in pipeline_rows),
                                "http_429_extra_delay_ms": sum(int(row[2] or 0) for row in pipeline_rows if int(row[7] or 0) > 0),
                                "p50_ms": percentile(rpc_latencies,.5), "p95_ms": percentile(rpc_latencies,.95), "p99_ms": percentile(rpc_latencies,.99)},
            "wss_transaction_lag": {"samples": len(swap_latencies), "p50_ms": percentile(swap_latencies,.5),
                                    "p95_ms": percentile(swap_latencies,.95), "p99_ms": percentile(swap_latencies,.99)},
            "latency_breakdown": {
                "CHAIN_TO_WSS": pipeline_metric(0), "WSS_TO_GET_TRANSACTION": pipeline_metric(1),
                "GET_TRANSACTION_RETRY": pipeline_metric(2), "PARSING": pipeline_metric(3),
                "QUEUE_WAIT": pipeline_metric(4), "STRATEGY_EVALUATE": pipeline_metric(5),
                "WSS_TO_PARSED": pipeline_metric(8),
            },
            "quote": self.router.status(),
            "quote_execution_summary": quote_summary,
            "binance_agentic_wallet_benchmark": {
                "sample_count": self.baw_benchmark.get("sample_count"),
                "buy_coverage_pct": self.baw_benchmark.get("buy", {}).get("coverage_pct") if isinstance(self.baw_benchmark.get("buy"), dict) else None,
                "sell_coverage_pct": self.baw_benchmark.get("sell", {}).get("coverage_pct") if isinstance(self.baw_benchmark.get("sell"), dict) else None,
                "roundtrip_coverage_pct": self.baw_benchmark.get("roundtrip", {}).get("coverage_pct") if isinstance(self.baw_benchmark.get("roundtrip"), dict) else None,
            },
            "positions": {"open": len(open_positions), "imported": self.imported, "max_evaluate_lag_ms": max(self.position_lag_ms, default=0)},
            "discovery": {"pending_records": len(self.pending_discovery)},
            "exit_intents": {"total": self.db.execute("SELECT COUNT(*) FROM sol_v2_exit_intents").fetchone()[0], "pending": self.db.execute("SELECT COUNT(*) FROM sol_v2_exit_intents WHERE status='PENDING_ROUTE'").fetchone()[0], "paper_exited": self.db.execute("SELECT COUNT(*) FROM sol_v2_exit_intents WHERE status='PAPER_EXITED'").fetchone()[0]},
            "gap_fill": {"runs": self.gap_fill_runs, "successful_schedules": self.gap_fill_success},
            "subscriptions": {"desired": self.db.execute("SELECT COUNT(*) FROM sol_v2_subscriptions").fetchone()[0], "ack": self.db.execute("SELECT COUNT(*) FROM sol_v2_subscriptions WHERE ack=1").fetchone()[0]},
            "paper_execution": {
                "entry_signals": self.db.execute("SELECT COUNT(*) FROM sol_v2_entry_opportunities").fetchone()[0],
                "entry_fills": self.db.execute("SELECT COUNT(*) FROM sol_v2_trade_executions WHERE side='BUY'").fetchone()[0],
                "exit_fills": self.db.execute("SELECT COUNT(*) FROM sol_v2_trade_executions WHERE side='SELL'").fetchone()[0],
                "missed_entries": self.db.execute("SELECT COUNT(*) FROM sol_v2_entry_opportunities WHERE state='SKIPPED'").fetchone()[0],
            },
            "sqlite_single_writer": True, "config_drift": False,
        }
        temporary = self.root / "health.json.tmp"
        temporary.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str))
        temporary.replace(self.root / "health.json")

    def run(self, duration: float = 0) -> None:
        self.wss_thread.start(); self.transaction_wss_thread.start(); started = time.monotonic(); next_discovery = 0.0; next_bind = 0.0
        while not self.stop.is_set() and (duration <= 0 or time.monotonic() - started < duration):
            tick_started = time.monotonic(); now = utc_now()
            stage = time.monotonic(); self._drain_results(); self._drain_quote_attempts(); self.stage_max_ms["results"] = max(self.stage_max_ms["results"], int((time.monotonic()-stage)*1000))
            stage = time.monotonic(); self._drain_wss(); self.stage_max_ms["wss_events"] = max(self.stage_max_ms["wss_events"], int((time.monotonic()-stage)*1000))
            current_reconnects = self.wss.health().reconnect_count
            if current_reconnects > self.last_reconnect_count:
                self.last_reconnect_count = current_reconnects
                self.db.execute("UPDATE sol_v2_subscriptions SET subscription_id=NULL,ack=0,status='PENDING_ACK'")
                self.db.commit()
                self._request_gap_fill()
            stage = time.monotonic(); self.engine.evaluate(now); self.stage_max_ms["engine"] = max(self.stage_max_ms["engine"], int((time.monotonic()-stage)*1000))
            self.position_lag_ms.append(self.engine.v2_position_evaluate_ms)
            stage = time.monotonic(); self._drain_discovery_batch(now); self.stage_max_ms["discovery_apply"] = max(self.stage_max_ms["discovery_apply"], int((time.monotonic()-stage)*1000))
            stage = time.monotonic(); self._write_freshness(now); self.stage_max_ms["freshness"] = max(self.stage_max_ms["freshness"], int((time.monotonic()-stage)*1000))
            stage = time.monotonic()
            self._schedule_capability_probe()
            self._schedule_baw_probe()
            self._schedule_swap_rpc()
            if time.monotonic() >= next_bind: self._schedule_bindings(); next_bind = time.monotonic() + 5
            if time.monotonic() >= next_discovery: self._schedule_discovery(); next_discovery = time.monotonic() + 4
            self.stage_max_ms["scheduling"] = max(self.stage_max_ms["scheduling"], int((time.monotonic()-stage)*1000))
            duration_ms = int((time.monotonic() - tick_started) * 1000); self.tick_ms.append(duration_ms)
            self.max_stall_ms = max(self.max_stall_ms, duration_ms); self.last_tick_at = utc_now(); stage = time.monotonic(); self._write_health(duration_ms); self.stage_max_ms["health"] = max(self.stage_max_ms["health"], int((time.monotonic()-stage)*1000))
            self.stop.wait(max(0.01, .2 - (time.monotonic() - tick_started)))

    def close(self) -> None:
        self.stop.set()
        self.transaction_wss.close()
        if self.wss_loop and self.wss_stop: self.wss_loop.call_soon_threadsafe(self.wss_stop.set)
        self.router.shutdown(); self.db.close()


def main() -> int:
    parser = argparse.ArgumentParser(); parser.add_argument("--env-file", type=Path, default=Path(".env")); parser.add_argument("--strategy-env-file", type=Path, default=Path("config/sol_survivor_v1.env")); parser.add_argument("--root", type=Path, default=Path("data/solana/survivor_v2_shadow")); parser.add_argument("--v1-db", type=Path, default=Path("data/solana/survivor_v1/paper/runtime.db")); parser.add_argument("--duration", type=float, default=0)
    args = parser.parse_args(); _load_env(args.env_file); _load_env(args.strategy_env_file)
    # This process has an independent, fail-closed safety envelope even when
    # the shared .env contains an explicitly configured BSC live runtime.
    os.environ.update({"PAPER_ONLY": "true", "ALLOW_LIVE_TRADING": "false", "LIVE_TRADING": "false",
                       "SIGNING_ENABLED": "false", "BROADCAST_ENABLED": "false",
                       "SOL_SIGNING_ENABLED": "false", "SOL_BROADCAST_ENABLED": "false"})
    runtime = V2Runtime(args.root, args.v1_db); signal.signal(signal.SIGTERM, lambda *_: runtime.stop.set()); signal.signal(signal.SIGINT, lambda *_: runtime.stop.set())
    lock = SingleInstanceLock(args.root / "runtime.lock", name="sol-survivor-v2-shadow")
    try:
        with lock: runtime.run(args.duration)
    finally: runtime.close()
    return 0


if __name__ == "__main__": raise SystemExit(main())
