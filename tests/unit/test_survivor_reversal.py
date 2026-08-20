from __future__ import annotations

import unittest
import sqlite3
import json
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from eth_abi import encode as abi_encode
from pathlib import Path
from types import SimpleNamespace
from queue import Queue
from tempfile import TemporaryDirectory

from meme_system.adapters.bsc_wss import (
    BONDING_CURVE_POOL_TYPE, BSC_WBNB_ADDRESS, FLAP_PORTAL_ADDRESS, FLAP_PORTAL_EVENT_TOPICS,
    FLAP_PORTAL_POOL_TYPE, FLAP_TOKEN_BOUGHT_TOPIC, FOUR_MEME_TOKEN_MANAGER,
    FOUR_TOKEN_PURCHASE_TOPIC, FOUR_TOKEN_SALE_TOPIC, PANCAKE_V2_FACTORY, BscPairEvent, BscPoolDescriptor,
    BscPoolResolution, BscPoolResolver, BscRpcClient, BscVenueInspection, V2_POOL_TYPE, SWAP_EVENT_TOPIC,
)
from meme_system.adapters.bsc_quote import FlapContext
from meme_system.adapters.quote_asset_usd import QuoteAssetUsdResolution
from meme_system.adapters.protocols import ExecutableQuote
from meme_system.adapters.binance_agentic_wallet import LiveExecutionEvent
from meme_system.runtime_ops import HealthRegistry, JsonlAuditWriter, RuntimeControl
from meme_system.storage.database import initialize_database
from meme_system.storage.runtime_store import RuntimeStore
from meme_system.strategies.survivor_reversal import (
    FlowSample,
    BalancedSurvivorConfig,
    SurvivorCandidate,
    SurvivorPosition,
    SurvivorReversalConfig,
    SurvivorReversalEngine,
    BscBalancedLiveEngine,
    FactoryPoolResolutionJob,
    FactoryPoolResolutionResult,
    FlowReconciliationJob,
    FlowReconciliationResult,
    parse_v2_swap_flow,
)


def _data(*words: int) -> str:
    return "0x" + "".join(f"{word:064x}" for word in words)


class SurvivorReversalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = datetime(2026, 8, 8, tzinfo=timezone.utc)
        self.config = SurvivorReversalConfig()
        self.logic = object.__new__(SurvivorReversalEngine)
        self.logic.config = self.config

    def test_identity_and_paper_default(self) -> None:
        self.assertEqual(self.config.execution_provider, "paper")
        self.assertEqual(self.config.active_max, 150)

    def test_config_rejects_non_paper(self) -> None:
        with self.assertRaises(ValueError):
            SurvivorReversalConfig(execution_provider="live").validate()

    def test_balanced_profile_has_independent_entry_rules(self) -> None:
        config = BalancedSurvivorConfig()
        self.assertFalse(config.require_pullback)
        self.assertTrue(config.candidate_is_entry)
        self.assertFalse(config.one_trade_per_day)
        self.assertEqual(config.min_age_sec, 0)
        self.assertEqual(config.max_age_sec, 1800)
        self.assertEqual(config.min_active_mc_usd, Decimal("5000"))
        self.assertEqual(config.min_active_liquidity_usd, Decimal("1000"))
        self.assertEqual(config.min_active_holders, 30)
        self.assertEqual(config.universe_min_mc_usd, Decimal("5000"))
        self.assertIsNone(config.universe_max_mc_usd)
        self.assertEqual(config.universe_min_liquidity_usd, Decimal("1000"))
        self.assertEqual(config.universe_min_holders, 30)
        self.assertEqual(config.min_entry_price_usd, Decimal("0.000005"))
        self.assertEqual(config.max_entry_price_usd, Decimal("0.00008"))
        self.assertFalse(config.require_audit_fields)
        self.assertEqual(config.same_token_cooldown_sec, 24 * 60 * 60)
        self.assertEqual(config.max_open_positions, 30)
        self.assertEqual(config.hard_stop_pct, Decimal("30"))
        self.assertEqual(config.no_trade_exit_sec, 120)
        self.assertEqual(config.time_stop_sec, 30 * 60)
        self.assertEqual(config.identity.strategy_name, "MEME_SURVIVOR_BALANCED_V1")

    def test_balanced_profile_candidate_is_direct_entry_and_skips_secondary_gates(self) -> None:
        candidate = SurvivorCandidate(
            "0x1", "X", self.now - timedelta(minutes=3), self.now,
            state="ACTIVE_CANDIDATE", candidate_eligible=True,
            current_price_usd=Decimal("0.00002"), ath_price_usd=Decimal("0.00002"),
            price_status="VALID", price_updated_at=self.now, price_history_status="VALID",
        )
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = BalancedSurvivorConfig()
        engine._wss_ready_for_candidate = lambda *_: (_ for _ in ()).throw(AssertionError("WSS must not gate Balanced Candidate entry"))
        engine._request_audit = lambda *_: (_ for _ in ()).throw(AssertionError("Audit must not gate Balanced Candidate entry"))
        engine._flow_1m = lambda *_: (_ for _ in ()).throw(AssertionError("Flow must not gate Balanced Candidate entry"))
        engine._try_buy = lambda *_: None
        engine._evaluate_candidate(candidate, self.now)
        self.assertEqual(candidate.state, "READY_TO_BUY")

    def _tp_position(self) -> SurvivorPosition:
        return SurvivorPosition(
            position_id="survivor:tp", mint="0xtp", symbol="TP",
            opened_at=self.now, entry_price_native=Decimal("1"),
            current_price_native=Decimal("1"), quantity_token=Decimal("10"),
            remaining_quantity_token=Decimal("10"), invested_bnb=Decimal("10"),
        )

    def test_tp_ladder_latches_all_crossings_independent_of_fill(self) -> None:
        position = self._tp_position()
        position.current_price_native = Decimal("1.55")
        self.assertTrue(self.logic._latch_tp_triggers(position, self.now, Decimal("55")))
        self.assertTrue(position.tp1_triggered)
        self.assertFalse(position.tp2_triggered)
        self.assertEqual(self.logic._next_latched_tp(position), ("TP1_PLUS_50", Decimal("0.30")))

        # TP1 is still pending, but a fresh +120% mark must latch TP2.
        position.current_price_native = Decimal("2.20")
        self.assertTrue(self.logic._latch_tp_triggers(position, self.now + timedelta(seconds=1), Decimal("120")))
        self.assertTrue(position.tp2_triggered)
        self.assertEqual(self.logic._next_latched_tp(position), ("TP1_PLUS_50", Decimal("0.30")))
        self.logic._mark_tp_filled(position, "TP1_PLUS_50", self.now + timedelta(seconds=2))
        self.assertEqual(self.logic._next_latched_tp(position), ("TP2_PLUS_100", Decimal("0.30")))

        # A retrace never clears the recorded TP2 crossing.
        position.current_price_native = Decimal("1.20")
        self.assertTrue(position.tp2_triggered)
        self.assertFalse(position.tp2_filled)

    def test_tp_ladder_jump_latches_and_serializes_all_stages(self) -> None:
        position = self._tp_position()
        position.current_price_native = Decimal("3.20")
        self.logic._latch_tp_triggers(position, self.now, Decimal("220"))
        self.assertTrue(position.tp1_triggered)
        self.assertTrue(position.tp2_triggered)
        self.assertTrue(position.tp3_triggered)
        self.assertEqual(self.logic._next_latched_tp(position)[0], "TP1_PLUS_50")
        self.logic._mark_tp_filled(position, "TP1_PLUS_50", self.now)
        self.assertEqual(self.logic._next_latched_tp(position)[0], "TP2_PLUS_100")
        self.logic._mark_tp_filled(position, "TP2_PLUS_100", self.now)
        self.assertEqual(self.logic._next_latched_tp(position)[0], "TP3_PLUS_200")
        self.logic._mark_tp_filled(position, "TP3_PLUS_200", self.now)
        self.assertIsNone(self.logic._next_latched_tp(position))

    def test_pending_tp1_still_consumes_fresh_mark_and_latches_tp2(self) -> None:
        position = self._tp_position()
        position.current_price_native = Decimal("2.20")
        position.tp1_triggered = True
        position.exit_intent_reason = "TP1_PLUS_50"
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = SurvivorReversalConfig(time_stop_sec=10_000)
        engine._positions = {position.position_id: position}
        engine._candidates = {}
        engine._position_mark_dirty = {position.position_id}
        engine._wss_trade_markers = {}
        engine._wss_trade_markers_lock = threading.Lock()
        engine._no_trade_monitoring_ready = lambda *_: False
        engine._persist_position = lambda *_, **__: None
        engine._record_exit_trigger = lambda *_args, **_kwargs: None
        submits: list[str] = []
        engine._partial_exit = lambda _p, _fraction, reason, _now: submits.append(reason) or False
        engine._close_position = lambda _p, reason, _now: submits.append(reason)
        engine._evaluate_positions(self.now)
        self.assertTrue(position.tp2_triggered)
        self.assertEqual(submits, ["TP1_PLUS_50"])

    def test_tp_trigger_state_survives_sqlite_restart(self) -> None:
        with TemporaryDirectory() as td:
            path = Path(td) / "runtime.db"
            connection = initialize_database(path)
            columns = {row[1] for row in connection.execute("PRAGMA table_info(survivor_positions)")}
            self.assertTrue({"tp1_triggered", "tp2_triggered", "tp3_triggered", "tp1_filled", "tp2_filled", "tp3_filled", "high_since_tp1_native"}.issubset(columns))
            connection.close()

    def test_tp1_to_tp2_high_retrace_closes_remaining_position(self) -> None:
        position = self._tp_position()
        position.tp1 = True
        position.tp1_filled = True
        position.high_since_tp1_native = Decimal("2")
        position.current_price_native = Decimal("1.30")  # exactly -35% from the TP1-stage high
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = SurvivorReversalConfig(time_stop_sec=10_000)
        engine._positions = {position.position_id: position}
        engine._candidates = {}
        engine._position_mark_dirty = set()
        engine._wss_trade_markers = {}
        engine._wss_trade_markers_lock = threading.Lock()
        engine._no_trade_monitoring_ready = lambda *_: False
        engine._record_exit_trigger = lambda *_args, **_kwargs: None
        engine._persist_position = lambda *_, **__: None
        exits: list[str] = []
        engine._close_position = lambda _position, reason, _now: exits.append(reason)
        engine._partial_exit = lambda *_args, **_kwargs: self.fail("TP1 retrace must close, not partial sell")
        engine._evaluate_positions(self.now)
        self.assertEqual(exits, ["TP1_HIGH_RETRACE_35_PERCENT"])

    def test_tp1_stage_high_only_updates_before_tp2_fill(self) -> None:
        position = self._tp_position()
        position.tp1 = True
        position.tp1_filled = True
        position.high_since_tp1_native = Decimal("1.5")
        position.current_price_native = Decimal("1.8")
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = SurvivorReversalConfig(time_stop_sec=10_000)
        engine._positions = {position.position_id: position}
        engine._candidates = {}
        engine._position_mark_dirty = set()
        engine._wss_trade_markers = {}
        engine._wss_trade_markers_lock = threading.Lock()
        engine._no_trade_monitoring_ready = lambda *_: False
        engine._persist_position = lambda *_, **__: None
        engine._record_exit_trigger = lambda *_args, **_kwargs: None
        engine._partial_exit = lambda *_args, **_kwargs: False
        engine._close_position = lambda *_args, **_kwargs: None
        engine._evaluate_positions(self.now)
        self.assertEqual(position.high_since_tp1_native, Decimal("1.8"))
        position.tp2 = True
        position.tp2_filled = True
        position.current_price_native = Decimal("2.0")
        engine._evaluate_positions(self.now + timedelta(seconds=1))
        self.assertEqual(position.high_since_tp1_native, Decimal("1.8"))

    def test_balanced_profile_does_not_apply_legacy_daily_entry_cap(self) -> None:
        connection = sqlite3.connect(":memory:")
        connection.execute("CREATE TABLE survivor_positions(opened_at TEXT,mint TEXT,status TEXT,closed_at TEXT)")
        connection.execute("INSERT INTO survivor_positions(opened_at) VALUES(?)", (self.now.isoformat(),))
        candidate = SurvivorCandidate("0x1", "X", self.now, self.now)
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = BalancedSurvivorConfig()
        engine.connection = connection
        engine.controls = SimpleNamespace(paused=lambda _: False)
        engine._positions = {}
        engine.quote_provider = None
        engine._try_buy(candidate, self.now)
        self.assertEqual(candidate.last_rejection, "QUOTE_GATE_FAILED")
        self.assertNotEqual(candidate.last_rejection, "ONE_TRADE_PER_DAY")

    def test_balanced_same_token_cooldown_blocks_reentry_for_24_hours_after_close(self) -> None:
        connection = sqlite3.connect(":memory:")
        connection.execute("CREATE TABLE survivor_positions(mint TEXT,status TEXT,closed_at TEXT)")
        closed_at = self.now - timedelta(hours=1)
        connection.execute(
            "INSERT INTO survivor_positions(mint,status,closed_at) VALUES(?,?,?)",
            ("0x1", "CLOSED", closed_at.isoformat()),
        )
        candidate = SurvivorCandidate("0x1", "X", self.now, self.now)
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = BalancedSurvivorConfig()
        engine.connection = connection
        engine.controls = SimpleNamespace(paused=lambda _: False)
        engine._positions = {}
        engine.quote_provider = None
        engine._try_buy(candidate, self.now)
        self.assertEqual(candidate.last_rejection, "SAME_TOKEN_COOLDOWN")
        self.assertIn("same_token_cooldown_until", candidate.source_status)

    def test_live_same_token_cooldown_blocks_reentry_before_any_quote(self) -> None:
        """Live must apply the same closed-position cooldown as Paper."""
        with TemporaryDirectory() as temporary:
            connection = initialize_database(Path(temporary) / "runtime.db")
            closed_at = self.now - timedelta(minutes=1)
            mint = "0x" + "c" * 40
            connection.execute(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,closed_at,status,entry_price_native,"
                "current_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("closed", mint, "X", closed_at.isoformat(), closed_at.isoformat(), "CLOSED", "1", "1", "1", "0", "1", "0", closed_at.isoformat()),
            )
            connection.commit()
            engine = self._live_capacity_engine(connection)
            engine.controls = SimpleNamespace(paused=lambda _mode: False)
            engine.live_entry_funds_available = True
            engine.reconcile_open_positions_with_wallet = lambda *_args, **_kwargs: None
            engine._live_buy_context = {}
            engine._live_orders = {}
            engine.entry_quote_provider = SimpleNamespace(
                quote_candidate=lambda *_args: self.fail("cooldown must reject before quote")
            )
            candidate = SurvivorCandidate(mint, "X", self.now, self.now)
            engine._try_buy(candidate, self.now)
            self.assertEqual(candidate.last_rejection, "SAME_TOKEN_COOLDOWN")

    def test_live_non_bnb_candidate_raw_price_cannot_latch_tp(self) -> None:
        """A QQQB/token venue mark must not be treated as BNB/token PnL."""
        engine = object.__new__(BscBalancedLiveEngine)
        engine._entry_outcome_mark_dirty = set()
        engine._position_mark_dirty = set()
        engine.config = replace(BalancedSurvivorConfig(), time_stop_sec=10_000)
        engine._positions = {}
        engine._candidates = {}
        engine._wss_trade_markers = {}
        engine._wss_trade_markers_lock = threading.Lock()
        engine._no_trade_monitoring_ready = lambda *_args: False
        engine._persist_position = lambda *_args, **_kwargs: None
        engine._record_exit_trigger = lambda *_args, **_kwargs: None
        engine._partial_exit = lambda *_args, **_kwargs: self.fail("false TP must not submit a sell")
        engine._close_position = lambda *_args, **_kwargs: self.fail("false TP must not close")
        position = self._tp_position()
        candidate = SurvivorCandidate(
            position.mint, "TP", self.now, self.now,
            current_price_native=Decimal("2.20"),
            native_token_price_usd=Decimal("600"),
            source_status={"venue": "Flap", "fundraising_quote_asset": "0x205812cdbed920aff76c6580abd681a46d11efc7"},
        )
        engine._positions[position.position_id] = position
        engine._candidates[position.mint] = candidate
        engine._evaluate_positions(self.now)
        self.assertEqual(position.current_price_native, Decimal("1"))
        self.assertFalse(position.tp1_triggered)
        self.assertFalse(position.tp2_triggered)

    def test_live_canonical_wss_mark_wakes_hard_stop_quote_without_direct_exit(self) -> None:
        """A WSS drawdown wakes GMGN confirmation; it cannot lock a stop alone."""
        engine = object.__new__(BscBalancedLiveEngine)
        engine._entry_outcome_mark_dirty = set()
        engine._position_mark_dirty = set()
        engine._position_mark_generation = {}
        engine._hard_stop_quote_context = {}
        engine.config = replace(BalancedSurvivorConfig(), hard_stop_pct=Decimal("30"), time_stop_sec=10_000)
        engine.clock = lambda: self.now
        engine._positions = {}
        engine._candidates = {}
        engine._wss_trade_markers = {}
        engine._wss_trade_markers_lock = threading.Lock()
        engine._no_trade_monitoring_ready = lambda *_args: False
        engine._persist_position = lambda *_args, **_kwargs: None
        triggers: list[str] = []
        exits: list[str] = []
        engine._record_exit_trigger = lambda _position, reason, *_args, **_kwargs: triggers.append(reason)
        engine._close_position = lambda _position, reason, _now: exits.append(reason)
        engine._partial_exit = lambda *_args, **_kwargs: self.fail("hard stop must fully exit")
        submitted: list[tuple[str, str]] = []
        engine.live_bridge = SimpleNamespace(submit=lambda action, key, *_args: submitted.append((action, key)) or "accepted")
        position = self._tp_position()
        candidate = SurvivorCandidate(
            position.mint, "TP", self.now, self.now,
            current_price_native=Decimal("2.20"),  # raw QQQB/token: not used as BNB/token
            current_price_usd=Decimal("65"),
            native_token_price_usd=Decimal("100"),
            price_status="VALID",
            price_updated_at=self.now,
            source_status={"venue": "Flap", "fundraising_quote_asset": "0x205812cdbed920aff76c6580abd681a46d11efc7"},
        )
        engine._positions[position.position_id] = position
        engine._candidates[position.mint] = candidate
        engine._evaluate_positions(self.now)
        self.assertEqual(position.current_price_native, Decimal("0.65"))
        self.assertEqual(triggers, [])
        self.assertEqual(exits, [])
        self.assertEqual(submitted, [("quote_sell", f"hard-stop-quote:{position.position_id}")])
        self.assertIn(f"hard-stop-quote:{position.position_id}", engine._hard_stop_quote_context)

    def test_live_hard_stop_requires_negative_executable_sell_quote(self) -> None:
        """A recovered executable SELL quote must clear a WSS-only stop candidate."""
        engine = object.__new__(BscBalancedLiveEngine)
        engine._entry_outcome_mark_dirty = set()
        engine._position_mark_dirty = set()
        engine._position_mark_generation = {}
        engine._position_mark_quote_context = {}
        engine._hard_stop_quote_context = {}
        engine.config = replace(BalancedSurvivorConfig(), hard_stop_pct=Decimal("30"))
        engine.live_provider = "GMGN_CLI"
        position = self._tp_position()
        candidate = SurvivorCandidate(position.mint, "TP", self.now, self.now)
        key = f"hard-stop-quote:{position.position_id}"
        engine._positions = {position.position_id: position}
        engine._candidates = {position.mint: candidate}
        engine._hard_stop_quote_context[key] = {
            "position": position,
            "generation": 1,
            "triggered_at": self.now,
            "trigger_mark_price": Decimal("0.65"),
            "trigger_mark_return": Decimal("-35"),
        }
        quote = ExecutableQuote(
            quote_id="recovered", mint=position.mint, side="sell",
            input_quantity=position.remaining_quantity_token, output_quantity=Decimal("12"),
            route_fee=None, price_impact_pct=None, quoted_at=self.now, age_ms=0,
            provider="GMGN_CLI", executable_style=True,
        )
        event = LiveExecutionEvent(
            request_id="request", action="quote_sell", key=key, token=position.mint,
            quantity=position.remaining_quantity_token, result=(quote, None),
            requested_at=self.now, completed_at=self.now + timedelta(seconds=1),
        )
        engine.live_bridge = SimpleNamespace(poll=lambda _limit: (event,))
        submitted: list[tuple[str, ExecutableQuote | None]] = []
        engine._submit_live_sell = lambda _position, reason, _quantity, _now, **kwargs: submitted.append((reason, kwargs.get("confirmed_quote")))
        engine._drain_live_events(self.now + timedelta(seconds=1))
        self.assertEqual(submitted, [])
        self.assertEqual(position.position_mark_source, "GMGN_SELL_QUOTE")
        self.assertGreater(engine._current_mark_pnl_pct(position), Decimal("0"))

        engine._hard_stop_quote_context[key] = {
            "position": position,
            "generation": 2,
            "triggered_at": self.now,
            "trigger_mark_price": Decimal("0.65"),
            "trigger_mark_return": Decimal("-35"),
        }
        loss_quote = replace(quote, quote_id="loss", output_quantity=Decimal("6"))
        loss_event = replace(event, result=(loss_quote, None), completed_at=self.now + timedelta(seconds=2))
        engine.live_bridge = SimpleNamespace(poll=lambda _limit: (loss_event,))
        engine._record_exit_trigger = lambda *_args, **_kwargs: None
        engine._drain_live_events(self.now + timedelta(seconds=2))
        self.assertEqual(submitted, [("HARD_STOP_PNL", loss_quote)])

    def test_live_stale_hard_stop_quote_cannot_replace_existing_tp_intent(self) -> None:
        engine = object.__new__(BscBalancedLiveEngine)
        engine._entry_outcome_mark_dirty = set()
        engine._position_mark_dirty = set()
        engine._position_mark_generation = {}
        engine._position_mark_quote_context = {}
        engine._hard_stop_quote_context = {}
        engine.config = replace(BalancedSurvivorConfig(), hard_stop_pct=Decimal("30"))
        engine.live_provider = "GMGN_CLI"
        position = self._tp_position()
        position.exit_intent_reason = "TP1_PLUS_50"
        candidate = SurvivorCandidate(position.mint, "TP", self.now, self.now)
        key = f"hard-stop-quote:{position.position_id}"
        engine._positions = {position.position_id: position}
        engine._candidates = {position.mint: candidate}
        engine._hard_stop_quote_context[key] = {"position": position, "generation": 1}
        quote = ExecutableQuote(
            quote_id="late-stop", mint=position.mint, side="sell",
            input_quantity=position.remaining_quantity_token, output_quantity=Decimal("6"),
            route_fee=None, price_impact_pct=None, quoted_at=self.now, age_ms=0,
            provider="GMGN_CLI", executable_style=True,
        )
        event = LiveExecutionEvent(
            request_id="request", action="quote_sell", key=key, token=position.mint,
            quantity=position.remaining_quantity_token, result=(quote, None),
            requested_at=self.now, completed_at=self.now + timedelta(seconds=1),
        )
        engine.live_bridge = SimpleNamespace(poll=lambda _limit: (event,))
        submitted: list[str] = []
        engine._submit_live_sell = lambda *_args, **_kwargs: submitted.append("HARD_STOP")
        engine._drain_live_events(self.now + timedelta(seconds=1))
        self.assertEqual(submitted, [])
        self.assertEqual(position.exit_intent_reason, "TP1_PLUS_50")

    def test_live_wss_price_update_applies_canonical_mark_in_same_owner_tick(self) -> None:
        """The WSS update path itself must publish the stop-relevant mark."""
        engine = object.__new__(BscBalancedLiveEngine)
        engine._entry_outcome_mark_dirty = set()
        engine._position_mark_dirty = set()
        engine._position_mark_generation = {}
        engine._hard_stop_quote_context = {}
        engine.config = BalancedSurvivorConfig()
        engine.live_bridge = SimpleNamespace(submit=lambda *_args, **_kwargs: "accepted")
        engine._positions = {}
        engine._candidates = {}
        engine.clock = lambda: self.now
        position = self._tp_position()
        candidate = SurvivorCandidate(
            position.mint, "TP", self.now, self.now,
            native_token_price_usd=Decimal("100"),
            source_status={"venue": "Flap", "fundraising_quote_asset": "0x205812cdbed920aff76c6580abd681a46d11efc7"},
        )
        engine._positions[position.position_id] = position
        engine._update_price(
            candidate,
            Decimal("65"),
            self.now,
            "FLAP_CANONICAL_PRICE",
            native_price=Decimal("2.20"),
            record_sample=False,
            persist_snapshot=False,
        )
        self.assertEqual(position.current_price_native, Decimal("0.65"))
        self.assertEqual(position.position_mark_source, "FLAP_CANONICAL_PRICE")

    def test_balanced_same_token_open_position_blocks_duplicate_entry(self) -> None:
        connection = sqlite3.connect(":memory:")
        connection.execute("CREATE TABLE survivor_positions(mint TEXT,status TEXT,closed_at TEXT)")
        candidate = SurvivorCandidate("0xAbC", "X", self.now, self.now)
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = BalancedSurvivorConfig()
        engine.connection = connection
        engine.controls = SimpleNamespace(paused=lambda _: False)
        engine._positions = {"open": SimpleNamespace(mint="0xabc", status="OPEN")}
        engine.quote_provider = None
        engine._try_buy(candidate, self.now)
        self.assertEqual(candidate.last_rejection, "SAME_TOKEN_ALREADY_OPEN")
        self.assertTrue(candidate.source_status["same_token_open"])

    def test_balanced_allows_thirty_open_positions_and_blocks_the_thirty_first(self) -> None:
        connection = sqlite3.connect(":memory:")
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = BalancedSurvivorConfig()
        engine.connection = connection
        engine.controls = SimpleNamespace(paused=lambda _: False)
        engine.quote_provider = None
        engine._positions = {str(index): SimpleNamespace(status="OPEN") for index in range(30)}
        candidate = SurvivorCandidate("0xnew", "X", self.now, self.now)
        engine._try_buy(candidate, self.now)
        self.assertEqual(candidate.last_rejection, "MAX_OPEN_POSITIONS_REACHED")
        engine._positions.pop("29")
        candidate = SurvivorCandidate("0xnew2", "Y", self.now, self.now)
        engine._try_buy(candidate, self.now)
        self.assertEqual(candidate.last_rejection, "QUOTE_GATE_FAILED")

    def _live_capacity_engine(self, connection: sqlite3.Connection, *, open_positions: int = 0, max_open_positions: int = 2) -> BscBalancedLiveEngine:
        """Small owner-loop harness: no worker or provider touches SQLite."""
        engine = object.__new__(BscBalancedLiveEngine)
        engine.mode = "live"
        engine.connection = connection
        engine.config = replace(BalancedSurvivorConfig(), max_open_positions=max_open_positions)
        engine.clock = lambda: self.now
        engine._positions = {
            f"open-{index}": SimpleNamespace(status="OPEN", mint=f"0xopen{index}")
            for index in range(open_positions)
        }
        engine._entry_reservations = {}
        engine._wallet_reconcile_next = {}
        engine._wallet_reconcile_pending = set()
        engine._last_db_write_at = None
        engine._db_write_error_count = 0
        return engine

    def test_live_capacity_reserves_only_two_slots_for_ten_ready_candidates(self) -> None:
        """A single owner-loop tick must not submit BUY #3 through #10."""
        with TemporaryDirectory() as temporary:
            connection = initialize_database(Path(temporary) / "runtime.db")
            engine = self._live_capacity_engine(connection)
            submits: list[str] = []

            class Provider:
                def quote_candidate(self, mint, amount, _context):
                    buy = SimpleNamespace(output_quantity=Decimal("100"), input_quantity=amount, provider="GMGN_CLI")
                    sell = SimpleNamespace(output_quantity=Decimal("0.001"), provider="GMGN_CLI")
                    return buy, sell, None

            class Bridge:
                def submit(self, action, _key, token=None, _quantity=None):
                    if action == "buy":
                        submits.append(str(token))
                    return "accepted"

            engine.controls = SimpleNamespace(paused=lambda _mode: False)
            engine.live_entry_funds_available = True
            engine.entry_quote_provider = Provider()
            engine.live_provider = "GMGN_CLI"
            engine.live_amount_bnb = Decimal("0.001")
            engine.live_bridge = Bridge()
            engine._live_buy_context = {}
            engine._live_orders = {}
            candidates = [SurvivorCandidate(f"0x{index:040x}", str(index), self.now, self.now) for index in range(10)]
            for candidate in candidates:
                engine._try_buy(candidate, self.now)
            self.assertEqual(len(submits), 2)
            self.assertEqual(engine._entry_capacity()["used_slots"], 2)
            self.assertTrue(all(candidate.last_rejection == "ENTRY_CAPACITY_FULL" for candidate in candidates[2:]))

    def test_live_capacity_reserves_only_four_slots_when_live_limit_is_four(self) -> None:
        with TemporaryDirectory() as temporary:
            connection = initialize_database(Path(temporary) / "runtime.db")
            engine = self._live_capacity_engine(connection, max_open_positions=4)
            candidates = [SurvivorCandidate(f"0x{index:040x}", str(index), self.now, self.now) for index in range(5)]
            for candidate in candidates[:4]:
                self.assertIsNotNone(engine._reserve_entry_slot(candidate, self.now))
            self.assertEqual(engine._entry_capacity(), {
                "max_open_positions": 4,
                "open_positions": 0,
                "buy_reserved": 4,
                "used_slots": 4,
                "available_slots": 0,
            })
            self.assertIsNone(engine._reserve_entry_slot(candidates[-1], self.now))
            self.assertEqual(candidates[-1].last_rejection, "ENTRY_CAPACITY_FULL")

    def test_live_capacity_open_and_pending_unknown_reservations_hold_slots_and_restart(self) -> None:
        with TemporaryDirectory() as temporary:
            database = Path(temporary) / "runtime.db"
            connection = initialize_database(database)
            engine = self._live_capacity_engine(connection, open_positions=1)
            first = SurvivorCandidate("0x" + "1" * 40, "FIRST", self.now, self.now)
            self.assertIsNotNone(engine._reserve_entry_slot(first, self.now))
            engine._set_entry_reservation_status(first.mint, "SWAP_UNKNOWN", self.now, error_code="REQUEST_TIMEOUT")
            self.assertEqual(engine._entry_capacity()["available_slots"], 0)
            second = SurvivorCandidate("0x" + "2" * 40, "SECOND", self.now, self.now)
            self.assertIsNone(engine._reserve_entry_slot(second, self.now))
            self.assertEqual(second.last_rejection, "ENTRY_CAPACITY_FULL")

            restarted = self._live_capacity_engine(connection, open_positions=1)
            restarted._restore_entry_reservations()
            self.assertEqual(restarted._entry_capacity()["buy_reserved"], 1)
            self.assertEqual(restarted._entry_capacity()["available_slots"], 0)

    def test_live_capacity_releases_only_definitive_failure_and_blocks_when_legacy_open_exceeds_cap(self) -> None:
        with TemporaryDirectory() as temporary:
            connection = initialize_database(Path(temporary) / "runtime.db")
            engine = self._live_capacity_engine(connection)
            candidate = SurvivorCandidate("0x" + "3" * 40, "X", self.now, self.now)
            self.assertIsNotNone(engine._reserve_entry_slot(candidate, self.now))
            engine._set_entry_reservation_status(candidate.mint, "SWAP_UNKNOWN", self.now, error_code="REQUEST_TIMEOUT")
            self.assertEqual(engine._entry_capacity()["buy_reserved"], 1)
            engine._release_entry_reservation(candidate.mint, self.now, "GAS_RESERVE_INSUFFICIENT")
            self.assertEqual(engine._entry_capacity()["buy_reserved"], 0)

            over = self._live_capacity_engine(connection, open_positions=3)
            blocked = SurvivorCandidate("0x" + "4" * 40, "OVER", self.now, self.now)
            self.assertIsNone(over._reserve_entry_slot(blocked, self.now))
            self.assertEqual(blocked.last_rejection, "ENTRY_CAPACITY_FULL")

    def test_live_wallet_reconciliation_closes_external_full_exit_without_fabricating_pnl(self) -> None:
        with TemporaryDirectory() as temporary:
            connection = initialize_database(Path(temporary) / "runtime.db")
            engine = self._live_capacity_engine(connection)
            engine.store = RuntimeStore(connection, "live")
            engine._candidates = {}
            engine._live_sell_context = {}
            engine._audit_event = lambda *_args, **_kwargs: None
            position = SurvivorPosition(
                position_id="external-full", mint="0x" + "a" * 40, symbol="X",
                opened_at=self.now, entry_price_native=Decimal("0.00001"),
                current_price_native=Decimal("0.00002"), quantity_token=Decimal("100"),
                remaining_quantity_token=Decimal("100"), invested_bnb=Decimal("0.001"),
            )
            engine._positions[position.position_id] = position
            connection.execute(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,current_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (position.position_id, position.mint, position.symbol, self.now.isoformat(), "OPEN", "0.00001", "0.00002", "100", "100", "0.001", "0", self.now.isoformat()),
            )
            connection.commit()
            engine.reconcile_open_positions_with_wallet(
                self.now,
                completed_position_id=position.position_id,
                completed_balance=Decimal("0"),
            )
            row = connection.execute(
                "SELECT status,exit_reason,remaining_quantity_token,pnl_pct,external_exit_unpriced FROM survivor_positions WHERE position_id=?",
                (position.position_id,),
            ).fetchone()
            self.assertEqual(tuple(row[:3]), ("CLOSED", "EXTERNAL_EXIT", "0"))
            self.assertIsNone(row[3])
            self.assertEqual(row[4], 1)
            self.assertNotIn(position.position_id, engine._positions)

    def test_live_wallet_reconciliation_keeps_partial_external_exit_open_and_unpriced(self) -> None:
        with TemporaryDirectory() as temporary:
            connection = initialize_database(Path(temporary) / "runtime.db")
            engine = self._live_capacity_engine(connection)
            engine.store = RuntimeStore(connection, "live")
            engine._candidates = {}
            engine._live_sell_context = {}
            engine._audit_event = lambda *_args, **_kwargs: None
            position = SurvivorPosition(
                position_id="external-partial", mint="0x" + "b" * 40, symbol="X",
                opened_at=self.now, entry_price_native=Decimal("0.00001"),
                current_price_native=Decimal("0.00002"), quantity_token=Decimal("100"),
                remaining_quantity_token=Decimal("100"), invested_bnb=Decimal("0.001"),
            )
            engine._positions[position.position_id] = position
            connection.execute(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,current_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (position.position_id, position.mint, position.symbol, self.now.isoformat(), "OPEN", "0.00001", "0.00002", "100", "100", "0.001", "0", self.now.isoformat()),
            )
            connection.commit()
            engine.reconcile_open_positions_with_wallet(
                self.now,
                completed_position_id=position.position_id,
                completed_balance=Decimal("40"),
            )
            row = connection.execute(
                "SELECT status,remaining_quantity_token,pnl_pct,external_exit_unpriced FROM survivor_positions WHERE position_id=?",
                (position.position_id,),
            ).fetchone()
            self.assertEqual(tuple(row[:2]), ("OPEN", "40"))
            self.assertIsNone(row[2])
            self.assertEqual(row[3], 1)
            self.assertEqual(position.remaining_quantity_token, Decimal("40"))

    def test_live_wallet_reconciliation_releases_slot_when_own_sell_balance_reaches_zero(self) -> None:
        with TemporaryDirectory() as temporary:
            connection = initialize_database(Path(temporary) / "runtime.db")
            engine = self._live_capacity_engine(connection)
            engine.store = RuntimeStore(connection, "live")
            engine._candidates = {}
            engine._live_sell_context = {}
            engine._audit_event = lambda *_args, **_kwargs: None
            position = SurvivorPosition(
                position_id="own-pending-sell", mint="0x" + "c" * 40, symbol="X",
                opened_at=self.now, entry_price_native=Decimal("0.00001"),
                current_price_native=Decimal("0.00002"), quantity_token=Decimal("100"),
                remaining_quantity_token=Decimal("100"), invested_bnb=Decimal("0.001"),
            )
            engine._positions[position.position_id] = position
            connection.execute(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,current_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,exit_swap_status,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (position.position_id, position.mint, position.symbol, self.now.isoformat(), "OPEN", "0.00001", "0.00002", "100", "100", "0.001", "0", "SWAP_PENDING", self.now.isoformat()),
            )
            connection.commit()
            engine._live_sell_context[position.position_id] = {"position": position}
            engine.reconcile_open_positions_with_wallet(
                self.now,
                completed_position_id=position.position_id,
                completed_balance=Decimal("0"),
            )
            row = connection.execute(
                "SELECT status,remaining_quantity_token,exit_reason FROM survivor_positions WHERE position_id=?",
                (position.position_id,),
            ).fetchone()
            self.assertEqual(tuple(row), ("CLOSED", "0", None))
            self.assertNotIn(position.position_id, engine._positions)

    def test_live_wallet_reconciliation_rpc_failure_never_releases_slot(self) -> None:
        with TemporaryDirectory() as temporary:
            connection = initialize_database(Path(temporary) / "runtime.db")
            engine = self._live_capacity_engine(connection)
            engine._candidates = {}
            position = SurvivorPosition(
                position_id="rpc-fail", mint="0x" + "d" * 40, symbol="X",
                opened_at=self.now, entry_price_native=Decimal("0.00001"),
                current_price_native=Decimal("0.00002"), quantity_token=Decimal("100"),
                remaining_quantity_token=Decimal("100"), invested_bnb=Decimal("0.001"),
            )
            engine._positions[position.position_id] = position
            connection.execute(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,current_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (position.position_id, position.mint, position.symbol, self.now.isoformat(), "OPEN", "0.00001", "0.00002", "100", "100", "0.001", "0", self.now.isoformat()),
            )
            connection.commit()
            engine.reconcile_open_positions_with_wallet(
                self.now,
                completed_position_id=position.position_id,
                completed_balance=None,
            )
            self.assertEqual(connection.execute("SELECT status FROM survivor_positions WHERE position_id=?", (position.position_id,)).fetchone()[0], "OPEN")
            self.assertEqual(engine._entry_capacity()["used_slots"], 1)

    def test_live_buy_forces_wallet_reconciliation_before_capacity_check(self) -> None:
        with TemporaryDirectory() as temporary:
            connection = initialize_database(Path(temporary) / "runtime.db")
            engine = self._live_capacity_engine(connection, open_positions=0, max_open_positions=1)
            old = SurvivorPosition(
                position_id="old", mint="0x" + "e" * 40, symbol="OLD",
                opened_at=self.now, entry_price_native=Decimal("1"), current_price_native=Decimal("1"),
                quantity_token=Decimal("10"), remaining_quantity_token=Decimal("10"), invested_bnb=Decimal("0.001"),
            )
            engine._positions[old.position_id] = old
            connection.execute(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,current_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (old.position_id, old.mint, old.symbol, self.now.isoformat(), "OPEN", "1", "1", "10", "10", "0.001", "0", self.now.isoformat()),
            )
            connection.commit()
            engine._candidates = {}
            engine.resolver = SimpleNamespace(erc20_balance_of=lambda _token, _owner: Decimal("0"))
            engine.live_executor = SimpleNamespace(wallet="0x" + "1" * 40)
            engine.controls = SimpleNamespace(paused=lambda _mode: False)
            engine.live_entry_funds_available = True
            engine.live_provider = "GMGN_CLI"
            engine.live_amount_bnb = Decimal("0.001")
            engine._live_buy_context = {}
            engine._live_orders = {}
            engine._persist_candidate = lambda *_args, **_kwargs: None
            engine.entry_quote_provider = SimpleNamespace(
                quote_candidate=lambda _mint, amount, _context: (
                    SimpleNamespace(output_quantity=Decimal("100"), input_quantity=amount, provider="GMGN_CLI"),
                    SimpleNamespace(output_quantity=Decimal("0.001"), provider="GMGN_CLI"),
                    None,
                )
            )
            submitted: list[str] = []
            engine.live_bridge = SimpleNamespace(submit=lambda action, _key, token=None, _quantity=None: submitted.append(str(token)) or "accepted")
            candidate = SurvivorCandidate("0x" + "f" * 40, "NEW", self.now, self.now)
            engine._try_buy(candidate, self.now)
            self.assertEqual(submitted, [candidate.mint])
            self.assertEqual(engine._entry_capacity()["used_slots"], 1)

    def test_exit_intent_survives_all_quote_failures_and_recovers_without_retrigger(self) -> None:
        """Fault-injection integration: SQLite intent is the retry source of truth."""

        class ExitProvider:
            def __init__(self):
                self.available = False

            def sell_quote(self, mint, quantity):
                if not self.available:
                    return None, "REQUEST_TIMEOUT"
                return ExecutableQuote(
                    quote_id="recovered-sell", mint=mint, side="sell",
                    input_quantity=quantity, output_quantity=Decimal("0.009"),
                    route_fee=None, price_impact_pct=None, quoted_at=self_now,
                    age_ms=0, provider="DIRECT", executable_style=True,
                ), None

        with TemporaryDirectory() as temporary:
            connection = initialize_database(Path(temporary) / "runtime.db")
            self_now = self.now
            provider = ExitProvider()
            engine = SurvivorReversalEngine(
                connection=connection, store=RuntimeStore(connection, "paper"),
                health=HealthRegistry(), audit=JsonlAuditWriter(Path(temporary) / "events.jsonl"),
                quote_provider=None, exit_quote_provider=provider, resolver=None,
                controls=RuntimeControl(Path(temporary) / "controls.json"),
                config=BalancedSurvivorConfig(), clock=lambda: self_now,
            )
            position = SurvivorPosition(
                position_id="exit-e2e", mint="0x1111111111111111111111111111111111111111",
                symbol="EXIT", opened_at=self_now, entry_price_native=Decimal("0.00001"),
                current_price_native=Decimal("0.00001"), quantity_token=Decimal("10"),
                remaining_quantity_token=Decimal("10"), invested_bnb=Decimal("0.01"),
            )
            engine._positions[position.position_id] = position
            connection.execute(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,current_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,last_trade_at,quote_source,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (position.position_id, position.mint, position.symbol, self_now.isoformat(), "OPEN", "0.00001", "0.00001", "10", "10", "0.01", "0", self_now.isoformat(), "TEST", self_now.isoformat()),
            )
            connection.commit()
            engine._close_position(position, "E2E_EXIT", self_now)
            intent = connection.execute("SELECT value_json FROM runtime_state WHERE state_key='exit_intent:exit-e2e'").fetchone()
            self.assertEqual(position.status, "OPEN")
            self.assertIsNotNone(intent)
            self.assertIn("EXIT_TRIGGERED_WAITING_ROUTE", intent[0])
            provider.available = True
            engine._evaluate_positions(self_now + timedelta(seconds=1))
            state = connection.execute("SELECT status FROM survivor_positions WHERE position_id='exit-e2e'").fetchone()[0]
            intent = connection.execute("SELECT value_json FROM runtime_state WHERE state_key='exit_intent:exit-e2e'").fetchone()[0]
            self.assertEqual(state, "CLOSED")
            self.assertIn("COMPLETED", intent)
            connection.close()

    def test_balanced_paper_positions_freeze_entry_and_exit_market_snapshots(self) -> None:
        class EntryProvider:
            def quote_candidate(self, mint, amount, _context):
                buy = ExecutableQuote(
                    quote_id="snapshot-buy", mint=mint, side="buy", input_quantity=amount,
                    output_quantity=Decimal("1000"), route_fee=None, price_impact_pct=None,
                    quoted_at=self_now, age_ms=0, provider="TEST", executable_style=True,
                )
                sell = ExecutableQuote(
                    quote_id="snapshot-roundtrip", mint=mint, side="sell", input_quantity=Decimal("1000"),
                    output_quantity=Decimal("0.009"), route_fee=None, price_impact_pct=None,
                    quoted_at=self_now, age_ms=0, provider="TEST", executable_style=True,
                )
                return buy, sell, None

        class ExitProvider:
            def sell_quote(self, mint, quantity):
                return ExecutableQuote(
                    quote_id="snapshot-exit", mint=mint, side="sell", input_quantity=quantity,
                    output_quantity=Decimal("0.012"), route_fee=None, price_impact_pct=None,
                    quoted_at=self_now, age_ms=0, provider="TEST", executable_style=True,
                ), None

        with TemporaryDirectory() as temporary:
            self_now = self.now
            connection = initialize_database(Path(temporary) / "runtime.db")
            engine = SurvivorReversalEngine(
                connection=connection, store=RuntimeStore(connection, "paper"),
                health=HealthRegistry(), audit=JsonlAuditWriter(Path(temporary) / "events.jsonl"),
                quote_provider=None, entry_quote_provider=EntryProvider(), exit_quote_provider=ExitProvider(),
                resolver=None, controls=RuntimeControl(Path(temporary) / "controls.json"),
                config=BalancedSurvivorConfig(), clock=lambda: self_now,
            )
            candidate = SurvivorCandidate(
                "0x1111111111111111111111111111111111111111", "SNAP", self_now, self_now,
                current_price_native=Decimal("0.00001"), current_price_usd=Decimal("0.00002"),
                holders=31, market_cap_usd=Decimal("5100"), liquidity_usd=Decimal("1100"),
            )
            engine._candidates[candidate.mint] = candidate
            engine._try_buy(candidate, self_now)
            position = next(iter(engine._positions.values()))
            candidate.current_price_usd = Decimal("0.00003")
            candidate.holders = 42
            candidate.market_cap_usd = Decimal("6200")
            candidate.liquidity_usd = Decimal("1300")
            engine._close_position(position, "TEST_EXIT", self_now + timedelta(seconds=1))
            row = connection.execute(
                "SELECT entry_price_usd,entry_holders,entry_market_cap_usd,entry_liquidity_usd,"
                "exit_price_native,exit_price_usd,exit_holders,exit_market_cap_usd,exit_liquidity_usd "
                "FROM survivor_positions WHERE position_id=?",
                (position.position_id,),
            ).fetchone()
            self.assertEqual(tuple(row), ("0.00002", 31, "5100", "1100", "0.000012", "0.00003", 42, "6200", "1300"))
            connection.close()

    def test_entry_quote_pending_does_not_block_or_create_paper_position(self) -> None:
        connection = sqlite3.connect(":memory:")
        candidate = SurvivorCandidate("0x1", "X", self.now, self.now)
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = BalancedSurvivorConfig()
        engine.connection = connection
        engine.controls = SimpleNamespace(paused=lambda _: False)
        engine._positions = {}
        engine.quote_provider = None
        engine.entry_quote_provider = SimpleNamespace(quote_candidate=lambda *_: (None, None, "QUOTE_PENDING"))
        engine._quote_requested_total = 0
        engine._try_buy(candidate, self.now)
        self.assertEqual(candidate.quote_state, "PENDING")
        self.assertEqual(candidate.last_rejection, "QUOTE_PENDING")
        self.assertFalse(candidate.paper_buy)

    def test_buy_only_quote_is_never_allowed_to_create_a_paper_position(self) -> None:
        connection = sqlite3.connect(":memory:")
        connection.execute(
            "CREATE TABLE survivor_positions(position_id TEXT,mint TEXT,symbol TEXT,opened_at TEXT,status TEXT,entry_price_native TEXT,current_price_native TEXT,quantity_token TEXT,remaining_quantity_token TEXT,invested_bnb TEXT,realized_bnb TEXT,last_trade_at TEXT,quote_source TEXT,updated_at TEXT)"
        )
        candidate = SurvivorCandidate(
            "0x1111111111111111111111111111111111111111", "X", self.now, self.now,
            current_price_native=Decimal("0.00001"), current_price_usd=Decimal("0.00001"),
        )
        buy = ExecutableQuote(
            quote_id="buy-only", mint=candidate.mint, side="buy", input_quantity=Decimal("0.01"),
            output_quantity=Decimal("1000"), route_fee=None, price_impact_pct=None,
            quoted_at=self.now, age_ms=0, provider="BINANCE_AGENTIC_WALLET", executable_style=True,
        )
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = BalancedSurvivorConfig()
        engine.connection = connection
        engine.controls = SimpleNamespace(paused=lambda _: False)
        engine._positions = {}
        engine.quote_provider = None
        engine.entry_quote_provider = SimpleNamespace(quote_candidate=lambda *_: (buy, None, "INSUFFICIENT_LIQUIDITY"))
        engine._quote_requested_total = 0
        engine._audit_event = lambda *_: None
        engine._try_buy(candidate, self.now)
        self.assertEqual(candidate.last_rejection, "QUOTE_GATE_FAILED")
        self.assertFalse(candidate.paper_buy)
        self.assertEqual(connection.execute("SELECT count(*) FROM survivor_positions").fetchone()[0], 0)

    def test_pre_migration_venue_jobs_do_not_block_candidate_scheduler(self) -> None:
        release = threading.Event()

        def blocking_resolve(_: str) -> None:
            release.wait(timeout=1)
            return None

        engine = object.__new__(SurvivorReversalEngine)
        engine.clock = lambda: self.now
        engine.quote_provider = SimpleNamespace(cached_context=lambda _: None, resolve_venue=blocking_resolve)
        engine.resolver = None
        engine._venue_resolution_executor = ThreadPoolExecutor(max_workers=2)
        engine._venue_resolution_results = Queue()
        engine._venue_resolution_jobs = {}
        engine._venue_resolution_generation = defaultdict(int)
        engine._venue_resolution_completed = 0
        engine._venue_resolution_failed = 0
        engine._venue_resolution_durations_ms = deque(maxlen=500)
        engine._candidates = {}
        engine._persist_candidate = lambda *_: None
        candidates = [SurvivorCandidate(f"0x{i:040x}", f"X{i}", self.now, self.now, latest_migrate_status=0) for i in range(20)]
        engine._candidates = {candidate.mint: candidate for candidate in candidates}
        started = time.monotonic()
        for candidate in candidates:
            engine._schedule_pre_migration_venue_resolution(candidate, protocol=None, migrate_status=0, now=self.now)
        self.assertLess(time.monotonic() - started, 0.2)
        self.assertEqual(len(engine._venue_resolution_jobs), 20)
        release.set()
        result = engine._venue_resolution_results.get(timeout=2)
        engine._venue_resolution_results.put(result)
        engine._drain_pre_migration_venue_results(self.now)
        self.assertGreaterEqual(engine._venue_resolution_completed, 1)
        engine._venue_resolution_executor.shutdown(wait=True)

    def test_balanced_candidate_gate_includes_its_price_window(self) -> None:
        self.logic.config = BalancedSurvivorConfig()
        candidate = SurvivorCandidate(
            "0x1", "X", self.now - timedelta(minutes=3), self.now,
            market_cap_usd=Decimal("5000"), liquidity_usd=Decimal("1000"), holders=30,
            price_status="VALID", current_price_usd=Decimal("0.000004"),
        )
        self.assertFalse(self.logic._candidate_eligible(candidate, self.now))
        candidate.current_price_usd = Decimal("0.000005")
        self.assertTrue(self.logic._candidate_eligible(candidate, self.now))

    def test_balanced_profile_does_not_reject_a_price_for_drawdown(self) -> None:
        candidate = SurvivorCandidate("0x1", "X", self.now - timedelta(seconds=180), self.now, state="ACTIVE_CANDIDATE", candidate_eligible=True, first_seen_price_usd=Decimal("100"), ath_price_usd=Decimal("100"), ath_price_native=Decimal("100"), price_history_status="VALID")
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = BalancedSurvivorConfig()
        engine._update_price(candidate, Decimal("1"), self.now)
        self.assertEqual(candidate.state, "ACTIVE_CANDIDATE")
        self.assertEqual(candidate.drawdown_pct, Decimal("-99.00"))

    def test_balanced_profile_enforces_holder_and_price_window(self) -> None:
        self.logic.config = BalancedSurvivorConfig()
        candidate = SurvivorCandidate("0x1", "X", self.now - timedelta(minutes=3), self.now, market_cap_usd=Decimal("100000"), liquidity_usd=Decimal("20000"), holders=29, current_price_usd=Decimal("0.00002"))
        self.assertEqual(self.logic._universe_reason(candidate, self.now), "HOLDERS_BELOW_30")
        candidate.holders = 30
        candidate.current_price_usd = Decimal("0.000004")
        self.assertEqual(self.logic._universe_reason(candidate, self.now), "PRICE_BELOW_0_000005")
        candidate.holders = 301
        candidate.current_price_usd = Decimal("0.00002")
        self.assertEqual(self.logic._universe_reason(candidate, self.now), "HOLDERS_ABOVE_300")
        candidate.price_status = "VALID"
        self.assertFalse(self.logic._candidate_eligible(candidate, self.now))
        candidate.holders = 30
        candidate.current_price_usd = Decimal("0.000081")
        self.assertEqual(self.logic._universe_reason(candidate, self.now), "PRICE_ABOVE_0_00008")
        candidate.current_price_usd = Decimal("0.00002")
        self.assertIsNone(self.logic._universe_reason(candidate, self.now))

    def test_balanced_price_above_entry_ceiling_is_terminal(self) -> None:
        self.logic.config = BalancedSurvivorConfig()
        candidate = SurvivorCandidate(
            "0x1", "X", self.now - timedelta(minutes=3), self.now,
            state="ACTIVE_CANDIDATE", candidate_eligible=True,
            active_candidate=True, current_price_usd=Decimal("0.000081"),
        )
        self.assertTrue(self.logic._is_balanced_price_ceiling_terminal(candidate))
        self.logic._reject_balanced_price_ceiling(candidate, self.now)
        self.assertEqual(candidate.state, "REJECTED")
        self.assertFalse(candidate.candidate_eligible)
        self.assertFalse(candidate.active_candidate)
        self.assertEqual(candidate.last_rejection, "PRICE_ABOVE_0_00008")
        self.assertEqual(candidate.source_status["price_ceiling_terminal"], "TRUE")

    def test_balanced_price_ceiling_rechecks_a_stale_marker_after_ceiling_change(self) -> None:
        self.logic.config = BalancedSurvivorConfig()
        candidate = SurvivorCandidate(
            "0x1", "X", self.now - timedelta(minutes=3), self.now,
            current_price_usd=Decimal("0.00006"),
            source_status={"price_ceiling_terminal": "TRUE"},
        )
        self.assertFalse(self.logic._is_balanced_price_ceiling_terminal(candidate))
        self.assertNotIn("price_ceiling_terminal", candidate.source_status)

    def test_balanced_profile_enforces_thirty_minute_age_ceiling(self) -> None:
        self.logic.config = BalancedSurvivorConfig()
        candidate = SurvivorCandidate(
            "0x1", "X", self.now - timedelta(seconds=1801), self.now,
            market_cap_usd=Decimal("5000"), liquidity_usd=Decimal("1000"), holders=30,
            price_status="VALID", current_price_usd=Decimal("0.00002"),
        )
        self.assertFalse(self.logic._candidate_eligible(candidate, self.now))
        self.assertEqual(self.logic._universe_reason(candidate, self.now), "AGE_ABOVE_1800S")

    def test_balanced_profile_enforces_new_market_cap_and_liquidity_ranges(self) -> None:
        self.logic.config = BalancedSurvivorConfig()
        candidate = SurvivorCandidate(
            "0x1", "X", self.now - timedelta(minutes=3), self.now,
            market_cap_usd=Decimal("4999"), liquidity_usd=Decimal("1000"),
            holders=150, current_price_usd=Decimal("0.00002"),
        )
        self.assertEqual(self.logic._universe_reason(candidate, self.now), "MC_BELOW_5K")
        candidate.market_cap_usd = Decimal("2000000")
        candidate.liquidity_usd = Decimal("999")
        self.assertEqual(self.logic._universe_reason(candidate, self.now), "LIQUIDITY_BELOW_1K")
        candidate.liquidity_usd = Decimal("160000")
        self.assertIsNone(self.logic._universe_reason(candidate, self.now))

    def test_discovery_filter_labels_follow_the_active_config_not_legacy_values(self) -> None:
        self.assertEqual(SurvivorReversalEngine._usd_gate_label(Decimal("5000")), "5K")
        self.assertEqual(SurvivorReversalEngine._usd_gate_label(Decimal("1000")), "1K")
        self.assertEqual(SurvivorReversalEngine._usd_gate_label(Decimal("250000")), "250K")

    def test_fourmeme_unmigrated_candidate_uses_bonding_curve_without_pancake_discovery(self) -> None:
        class Resolver:
            def resolve_pancake_v2(self, *_args, **_kwargs):
                raise AssertionError("unmigrated FourMeme must not query Pancake")

        candidate = SurvivorCandidate(
            "0x" + "2" * 40, "FOUR", self.now - timedelta(minutes=5), self.now,
            candidate_eligible=True, latest_migrate_status=0,
        )
        engine = object.__new__(SurvivorReversalEngine)
        engine.resolver = Resolver()
        engine.quote_provider = None
        engine.clock = lambda: self.now
        engine._resolve_candidate_pool(candidate, protocol="FourMeme", migrate_status=0, now=self.now)
        self.assertIsNotNone(candidate.descriptor)
        assert candidate.descriptor is not None
        self.assertEqual(candidate.descriptor.pool_type, BONDING_CURVE_POOL_TYPE)
        self.assertEqual(candidate.source_status["venue_state"], "FOURMEME_BONDING_CURVE")
        self.assertEqual(candidate.source_status["pool_status"], "VALID")
        self.assertEqual(candidate.source_status["flow_status"], "UNSUPPORTED")

    def test_fourmeme_migrated_candidate_discovers_pancake_pool(self) -> None:
        mint = "0x" + "2" * 40
        descriptor = BscPoolDescriptor(
            address="0x" + "3" * 40, pool_type=V2_POOL_TYPE, mint=mint,
            token0=mint, token1=BSC_WBNB_ADDRESS,
        )
        class Resolver:
            def resolve_pancake_v2(self, *_args, **_kwargs):
                return BscPoolResolution(descriptor, "VALID", "PANCAKE_V2_FACTORY", reserves=(1, 2), quote_asset=BSC_WBNB_ADDRESS)

        candidate = SurvivorCandidate(mint, "FOUR", self.now - timedelta(minutes=5), self.now, candidate_eligible=True, latest_migrate_status=1)
        engine = object.__new__(SurvivorReversalEngine)
        engine.resolver = Resolver()
        engine.quote_provider = None
        engine.clock = lambda: self.now
        engine._resolve_candidate_pool(candidate, protocol="FourMeme", migrate_status=1, now=self.now)
        self.assertEqual(candidate.descriptor, descriptor)
        self.assertEqual(candidate.source_status["venue_state"], "MIGRATED_TO_PANCAKE")
        self.assertEqual(candidate.source_status["pool_status"], "VALID")

    def test_flap_pre_migration_context_is_registry_first_and_fail_closed(self) -> None:
        mint = "0x" + "2" * 40
        portal = "0x" + "3" * 40
        implementation = "0x" + "4" * 40
        token_inspection = BscVenueInspection(
            address=mint, chain="bsc:56", is_contract=True, code_size=45,
            bytecode_hash="sha256:token", implementation=implementation,
            selector_bitmap="factory=0;token0=0", protocol_fingerprint="sha256:token",
            protocol_family="UNKNOWN_FAMILY_token", capabilities=(("DISCOVERY", "SUPPORTED"),),
        )
        portal_inspection = BscVenueInspection(
            address=portal, chain="bsc:56", is_contract=True, code_size=99,
            bytecode_hash="sha256:portal", selector_bitmap="flap_portal_context=1",
            protocol_fingerprint="FLAP_PORTAL_CONTEXT_V1", protocol_family="FLAP_CONTEXT",
            capabilities=(
                ("DISCOVERY", "SUPPORTED"), ("PRICE", "SUPPORTED"), ("LIQUIDITY", "SUPPORTED"),
                ("WSS", "SUPPORTED"), ("FLOW", "SUPPORTED"), ("BUY_QUOTE", "SUPPORTED"),
                ("SELL_QUOTE", "SUPPORTED"), ("PAPER_FILL", "UNSUPPORTED"),
            ),
        )

        class Resolver:
            def inspect_venue(self, address):
                return token_inspection if address == mint else None

            def inspect_flap_context(self, address):
                return portal_inspection if address == portal else None

            def resolve_pancake_v2(self, *_args, **_kwargs):
                raise AssertionError("pre-migration Flap must not query Pancake")

        class QuoteProvider:
            def resolve_venue(self, _mint):
                return FlapContext(
                    mint=mint, token_proxy=mint, token_implementation=implementation,
                    launchpad=portal, fundraising_currency=BSC_WBNB_ADDRESS,
                    fundraising_decimals=18, token_decimals=18, status=1, migrated=False,
                    pancake_pair=None, native_to_quote_swap_enabled=True, tax_rate_bps=100, progress=1,
                )

        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            engine.resolver = Resolver()
            engine.quote_provider = QuoteProvider()
            candidate = SurvivorCandidate(mint, "FLAP", self.now, self.now, candidate_eligible=True, latest_migrate_status=0)
            engine._resolve_candidate_pool(candidate, migrate_status=0, now=self.now)
            row = engine.connection.execute(
                "SELECT protocol_family,strategy_support FROM bsc_venue_registry WHERE token_address=? AND venue_address=?",
                (mint, portal),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["protocol_family"], "FLAP_CONTEXT")
            self.assertEqual(row["strategy_support"], "SUPPORTED_ADAPTER")
            self.assertIsNone(candidate.descriptor)
            self.assertEqual(candidate.source_status["venue_state"], "PRE_MIGRATION")
            self.assertEqual(candidate.source_status["protocol_family"], "FLAP_CONTEXT")
            self.assertEqual(candidate.source_status["paper_fill_capability"], "UNSUPPORTED")
            self.assertEqual(candidate.source_status["wss_subscription_status"], "READY")

    def test_flap_curve_liquidity_uses_verified_reserve_not_binance_indicator(self) -> None:
        engine = object.__new__(SurvivorReversalEngine)
        engine._latest_native_token_price_usd = Decimal("612")
        candidate = SurvivorCandidate(
            "0x" + "2" * 40, "FLAP", self.now, self.now,
            liquidity_usd=Decimal("9454.61"),
        )
        context = SimpleNamespace(curve_reserve_raw=165_000_000_000_000_000)
        engine._apply_flap_curve_liquidity(candidate, context)
        self.assertEqual(candidate.source_status["liquidity_source"], "FLAP_CURVE_ONCHAIN")
        self.assertEqual(candidate.source_status["binance_liquidity_usd"], "9454.61")
        self.assertEqual(candidate.liquidity_usd, Decimal("201.96"))
        self.assertEqual(candidate.liquidity_current_usd, Decimal("201.96"))

    def test_flap_portal_subscription_is_shared_and_trade_event_updates_flow_price(self) -> None:
        mint = "0x" + "3" * 40
        candidate = SurvivorCandidate(
            mint, "FLAP", self.now - timedelta(minutes=1), self.now,
            state="ACTIVE_CANDIDATE", candidate_eligible=True,
            source_status={
                "venue": "Flap", "protocol": "Flap", "protocol_family": "FLAP_CONTEXT",
                "flap_token_decimals": "18", "fundraising_quote_decimals": "18",
                "fundraising_quote_asset": "NATIVE_BNB", "flow_status": "SUPPORTED",
            },
            native_token_price_usd=Decimal("600"),
        )
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = SurvivorReversalConfig()
        engine._lock = threading.RLock()
        engine.clock = lambda: self.now
        engine._candidates = {mint: candidate}
        engine._positions = {}
        engine._wss_provider_state = "HEALTHY"
        engine._wss_subscribed_addresses = {FLAP_PORTAL_ADDRESS}
        engine.store = SimpleNamespace(set_state=lambda *_args, **_kwargs: None)
        engine._flap_portal_cursor_block = None
        engine._flap_event_keys = deque(maxlen=8192)
        engine._flap_event_key_set = set()
        engine._persist_flow = lambda *_args: None
        engine._persist_candidate = lambda *_args: None
        engine._update_rollups = lambda *_args: None
        engine._update_price = lambda c, price, observed, source, **kwargs: setattr(c, "current_price_usd", price)
        details = engine.subscription_details()
        portal = [item for item in details if item["pool_address"] == FLAP_PORTAL_ADDRESS]
        self.assertEqual(len(portal), 1)
        self.assertEqual(portal[0]["descriptor"].pool_type, FLAP_PORTAL_POOL_TYPE)
        data = "0x" + abi_encode(
            ["uint256", "address", "address", "uint256", "uint256", "uint256", "uint256"],
            [int(self.now.timestamp()), mint, "0x" + "4" * 40, 2 * 10**18, 10**17, 10**15, 2 * 10**15],
        ).hex()
        event = BscPairEvent(
            FLAP_PORTAL_ADDRESS, "flap_token_bought", 100, "0x" + "a" * 64, 1,
            self.now, pool_type=FLAP_PORTAL_POOL_TYPE, data=data,
            topics=(FLAP_TOKEN_BOUGHT_TOPIC,),
        )
        engine._apply_event(event)
        self.assertEqual(candidate.buy_count_1m, 0)  # rollup is intentionally delegated in this fixture
        self.assertEqual(candidate.source_status["price_source"], "FLAP_PORTAL_EVENT")
        self.assertEqual(candidate.source_status["flow_status"], "SUPPORTED")
        position = SurvivorPosition(
            position_id="new-position", mint=mint, symbol="FLAP", opened_at=self.now,
            entry_price_native=Decimal("0.00001"), current_price_native=Decimal("0.00001"),
            quantity_token=Decimal("1"), remaining_quantity_token=Decimal("1"),
            invested_bnb=Decimal("0.001"), last_trade_at=self.now,
        )
        engine._positions = {position.position_id: position}
        engine._persist_trade_activity = lambda *_args: self.fail("pre-entry event must not update position activity")
        older = self.now - timedelta(minutes=3)
        historical_data = "0x" + abi_encode(
            ["uint256", "address", "address", "uint256", "uint256", "uint256", "uint256"],
            [int(older.timestamp()), mint, "0x" + "4" * 40, 2 * 10**18, 10**17, 10**15, 2 * 10**15],
        ).hex()
        engine._apply_event(BscPairEvent(
            FLAP_PORTAL_ADDRESS, "flap_token_bought", 101, "0x" + "b" * 64, 1,
            older, pool_type=FLAP_PORTAL_POOL_TYPE, data=historical_data,
            topics=(FLAP_TOKEN_BOUGHT_TOPIC,),
        ))
        self.assertEqual(position.last_trade_at, self.now)
        self.assertEqual(candidate.flows[-1].buy_count, 1)
        self.assertEqual(candidate.flows[-1].quote_amount, Decimal("0.1"))

    def test_flap_portal_gap_fill_uses_logs_rpc_and_advances_cursor_after_success(self) -> None:
        calls = []
        class Logs:
            def call(self, method, params):
                calls.append((method, params))
                return [{
                    "address": FLAP_PORTAL_ADDRESS,
                    "topics": [FLAP_TOKEN_BOUGHT_TOPIC],
                    "data": "0x" + "00" * 32 * 7,
                    "blockNumber": hex(101), "logIndex": hex(2),
                    "transactionHash": "0x" + "b" * 64,
                }]
        engine = object.__new__(SurvivorReversalEngine)
        engine.resolver = SimpleNamespace(logs_rpc=Logs())
        engine._flap_portal_gap_required = True
        engine._flap_portal_gap_target = 101
        engine._flap_portal_cursor_block = 100
        engine._pending_gap_events = deque(maxlen=5000)
        states = []
        engine.store = SimpleNamespace(set_state=lambda key, value: states.append((key, value)))
        engine._run_flap_portal_gap_fill(self.now)
        self.assertEqual(calls[0][0], "eth_getLogs")
        self.assertEqual(calls[0][1][0]["address"], FLAP_PORTAL_ADDRESS)
        self.assertEqual(calls[0][1][0]["fromBlock"], hex(101))
        self.assertEqual(calls[0][1][0]["toBlock"], hex(101))
        self.assertEqual(engine._flap_portal_cursor_block, 101)
        self.assertFalse(engine._flap_portal_gap_required)
        self.assertEqual(engine._pending_gap_events[0].event_type, "flap_token_bought")
        self.assertEqual(states[-1][1]["state"], "HEALTHY")

    def test_flow_reconciliation_recovers_wss_silent_miss_without_duplicate_delivery(self) -> None:
        """Logs RPC is authoritative when a healthy WSS delivered nothing."""

        mint = "0x" + "5" * 40
        pool = "0x" + "6" * 40

        class Logs:
            configured = True
            get_logs_call_count = 0
            get_logs_429_count = 0

            def call(self, method, params):
                if method == "eth_blockNumber":
                    return hex(110)
                self.get_logs_call_count += 1
                return [{
                    "address": pool,
                    "topics": [SWAP_EVENT_TOPIC],
                    "data": _data(1, 2, 3, 4),
                    "blockNumber": hex(108),
                    "logIndex": hex(1),
                    "transactionHash": "0x" + "7" * 64,
                }]

        candidate = SurvivorCandidate(
            mint, "MISS", self.now - timedelta(minutes=2), self.now,
            state="ACTIVE_CANDIDATE", candidate_eligible=True,
            descriptor=BscPoolDescriptor(pool, V2_POOL_TYPE, mint=mint,
                                          token0=BSC_WBNB_ADDRESS, token1=mint),
        )
        engine = object.__new__(SurvivorReversalEngine)
        engine.clock = lambda: self.now
        engine.resolver = SimpleNamespace(logs_rpc=Logs())
        engine._candidates = {mint: candidate}
        engine._positions = {}
        engine._wss_provider_state = "HEALTHY"
        engine._flow_reconciliation_state = {}
        engine._flow_reconciliation_results = Queue()
        engine._flow_reconciliation_pending = set()
        engine._flow_reconciliation_misses_total = 0
        engine._flow_reconciliation_recovered_total = 0
        engine._flow_reconciliation_rpc_calls = 0
        engine._flow_reconciliation_rpc_429 = 0
        engine._flow_reconciliation_resubscribe_requested = False
        engine._flow_reconciliation_resubscribe_callback = None
        engine._wss_flow_event_key_set = set()
        engine._processed_flow_event_key_set = set()
        engine._processed_flow_event_keys = deque(maxlen=50000)
        engine._pending_gap_events = deque(maxlen=5000)
        engine.store = SimpleNamespace(set_state=lambda *_args, **_kwargs: None)
        engine._persist_candidate = lambda *_args, **_kwargs: None
        engine._audit_event = lambda *_args, **_kwargs: None
        engine._run_flow_reconciliation_job(FlowReconciliationJob(
            f"{V2_POOL_TYPE}:{pool}", pool, V2_POOL_TYPE, (SWAP_EVENT_TOPIC,),
            108, -1, self.now,
        ))
        result = engine._flow_reconciliation_results.get_nowait()
        self.assertEqual(len(result.events), 1)
        self.assertEqual(result.events[0].source, "RPC_RECONCILIATION")
        engine._flow_reconciliation_results.put(result)
        engine._drain_flow_reconciliation_results(self.now)
        self.assertEqual(engine._flow_reconciliation_misses_total, 1)
        self.assertEqual(engine._flow_reconciliation_recovered_total, 1)
        self.assertEqual(len(engine._pending_gap_events), 1)
        self.assertEqual(engine._pending_gap_events[0].transaction_hash, "0x" + "7" * 64)
        # The next periodic pass sees the recovered chain key and must not
        # report or enqueue the same silent miss again.
        engine._flow_reconciliation_results.put(result)
        engine._drain_flow_reconciliation_results(self.now)
        self.assertEqual(engine._flow_reconciliation_misses_total, 1)
        self.assertEqual(len(engine._pending_gap_events), 1)

    def test_no_trade_remains_open_when_reconciliation_rpc_fails(self) -> None:
        mint = "0x" + "8" * 40
        candidate = SurvivorCandidate(
            mint, "RPCFAIL", self.now - timedelta(minutes=2), self.now,
            state="ACTIVE_CANDIDATE", candidate_eligible=True,
            descriptor=BscPoolDescriptor("0x" + "9" * 40, V2_POOL_TYPE, mint=mint,
                                          token0=BSC_WBNB_ADDRESS, token1=mint),
        )
        position = SurvivorPosition(
            position_id="survivor:rpc-fail", mint=mint, symbol="RPCFAIL",
            opened_at=self.now - timedelta(seconds=45), entry_price_native=Decimal("1"),
            current_price_native=Decimal("1"), quantity_token=Decimal("1"),
            remaining_quantity_token=Decimal("1"), invested_bnb=Decimal("0.01"),
            last_trade_at=self.now - timedelta(seconds=45),
        )
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = SurvivorReversalConfig(no_trade_exit_sec=40, time_stop_sec=10_000)
        engine._candidates = {mint: candidate}
        engine._positions = {position.position_id: position}
        engine._flow_reconciliation_state = {
            f"{V2_POOL_TYPE}:{candidate.descriptor.address}": {
                "data_completeness_health": "DEGRADED",
                "window_40s_complete": False,
            }
        }
        engine._wss_trade_markers = {}
        engine._wss_trade_markers_lock = threading.Lock()
        engine._persist_trade_activity = lambda *_args, **_kwargs: None
        engine._persist_position = lambda *_args, **_kwargs: None
        engine._close_position = lambda *_args, **_kwargs: self.fail("unverified flow must not close")
        engine._no_trade_monitoring_ready = lambda *_args: True
        engine._flow_state = SurvivorReversalEngine._flow_state.__get__(engine)
        engine._evaluate_positions(self.now)
        self.assertEqual(position.status, "OPEN")
        self.assertEqual(candidate.source_status["no_trade_state"], "NO_TRADE_40S_UNVERIFIED")

    def test_no_trade_closes_only_after_complete_recent_reconciliation(self) -> None:
        mint = "0x" + "a" * 40
        pool = "0x" + "b" * 40
        candidate = SurvivorCandidate(
            mint, "QUIET", self.now - timedelta(minutes=2), self.now,
            state="ACTIVE_CANDIDATE", candidate_eligible=True,
            descriptor=BscPoolDescriptor(pool, V2_POOL_TYPE, mint=mint,
                                          token0=BSC_WBNB_ADDRESS, token1=mint),
            source_status={"flow_status": "SUPPORTED"},
        )
        position = SurvivorPosition(
            position_id="survivor:quiet", mint=mint, symbol="QUIET",
            opened_at=self.now - timedelta(seconds=45), entry_price_native=Decimal("1"),
            current_price_native=Decimal("1"), quantity_token=Decimal("1"),
            remaining_quantity_token=Decimal("1"), invested_bnb=Decimal("0.01"),
            last_trade_at=self.now - timedelta(seconds=45),
        )
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = SurvivorReversalConfig(no_trade_exit_sec=40, time_stop_sec=10_000)
        engine._candidates = {mint: candidate}
        engine._positions = {position.position_id: position}
        engine._flow_reconciliation_state = {
            f"{V2_POOL_TYPE}:{pool}": {
                "data_completeness_health": "HEALTHY",
                "window_40s_complete": True,
                "last_reconciled_at": self.now.isoformat(),
                "reconciliation_lag_blocks": 2,
            }
        }
        engine._wss_trade_markers = {}
        engine._wss_trade_markers_lock = threading.Lock()
        engine._persist_trade_activity = lambda *_args, **_kwargs: None
        engine._persist_position = lambda *_args, **_kwargs: None
        reasons = []
        engine._record_exit_trigger = lambda *_args, **_kwargs: None
        engine._close_position = lambda _position, reason, _now: reasons.append(reason)
        engine._no_trade_monitoring_ready = lambda *_args: True
        engine._flow_state = SurvivorReversalEngine._flow_state.__get__(engine)
        engine._evaluate_positions(self.now)
        self.assertEqual(reasons, ["NO_TRADE_40S_PNL_LT_10"])

    def test_no_trade_after_forty_seconds_with_profit_sells_half_once(self) -> None:
        mint = "0x" + "c" * 40
        pool = "0x" + "d" * 40
        candidate = SurvivorCandidate(
            mint, "PROFIT", self.now - timedelta(minutes=2), self.now,
            state="ACTIVE_CANDIDATE", candidate_eligible=True,
            descriptor=BscPoolDescriptor(pool, V2_POOL_TYPE, mint=mint,
                                          token0=BSC_WBNB_ADDRESS, token1=mint),
            source_status={"flow_status": "SUPPORTED"},
        )
        position = SurvivorPosition(
            position_id="survivor:profit-quiet", mint=mint, symbol="PROFIT",
            opened_at=self.now - timedelta(seconds=45), entry_price_native=Decimal("1"),
            current_price_native=Decimal("1.11"), quantity_token=Decimal("10"),
            remaining_quantity_token=Decimal("10"), invested_bnb=Decimal("0.01"),
            last_trade_at=self.now - timedelta(seconds=45),
        )
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = SurvivorReversalConfig(no_trade_exit_sec=40, time_stop_sec=10_000)
        engine._candidates = {mint: candidate}
        engine._positions = {position.position_id: position}
        engine._wss_trade_markers = {}
        engine._wss_trade_markers_lock = threading.Lock()
        engine._persist_trade_activity = lambda *_args, **_kwargs: None
        engine._persist_position = lambda *_args, **_kwargs: None
        engine._record_exit_trigger = lambda *_args, **_kwargs: None
        engine._no_trade_monitoring_ready = lambda *_args: True
        engine._flow_data_completeness_ready = lambda *_args: True
        partials: list[tuple[Decimal, str]] = []

        def partial(_position, fraction, reason, _now):
            partials.append((fraction, reason))
            _position.no_trade_profit_partial_done = True
            return True

        engine._partial_exit = partial
        engine._evaluate_positions(self.now)
        self.assertEqual(partials, [(Decimal("0.50"), "NO_TRADE_40S_PNL_GT_10_PARTIAL")])
        engine._evaluate_positions(self.now + timedelta(seconds=1))
        self.assertEqual(len(partials), 1)
        self.assertEqual(candidate.source_status["no_trade_state"], "NO_TRADE_40S_PNL_GT_10_PARTIAL_DONE")

    def test_no_trade_at_exactly_ten_percent_does_not_trigger_no_trade_exit(self) -> None:
        mint = "0x" + "e" * 40
        candidate = SurvivorCandidate(mint, "TEN", self.now - timedelta(minutes=2), self.now)
        position = SurvivorPosition(
            position_id="survivor:ten-quiet", mint=mint, symbol="TEN",
            opened_at=self.now - timedelta(seconds=45), entry_price_native=Decimal("1"),
            current_price_native=Decimal("1.10"), quantity_token=Decimal("10"),
            remaining_quantity_token=Decimal("10"), invested_bnb=Decimal("0.01"),
            last_trade_at=self.now - timedelta(seconds=45),
        )
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = SurvivorReversalConfig(no_trade_exit_sec=40, time_stop_sec=10_000)
        engine._candidates = {mint: candidate}
        engine._positions = {position.position_id: position}
        engine._wss_trade_markers = {}
        engine._wss_trade_markers_lock = threading.Lock()
        engine._persist_trade_activity = lambda *_args, **_kwargs: None
        engine._persist_position = lambda *_args, **_kwargs: None
        engine._no_trade_monitoring_ready = lambda *_args: True
        engine._flow_data_completeness_ready = lambda *_args: True
        engine._close_position = lambda *_args, **_kwargs: self.fail("exact +10% must not use no-trade exit")
        engine._partial_exit = lambda *_args, **_kwargs: self.fail("exact +10% must not use no-trade partial exit")
        engine._evaluate_positions(self.now)
        self.assertEqual(candidate.source_status["no_trade_state"], "NO_TRADE_40S_PNL_EQ_10_HOLD")

    def test_three_silent_miss_ranges_request_resubscribe(self) -> None:
        engine = object.__new__(SurvivorReversalEngine)
        engine._flow_reconciliation_state = {}
        engine._flow_reconciliation_pending = set()
        engine._flow_reconciliation_results = Queue()
        engine._flow_reconciliation_misses_total = 0
        engine._flow_reconciliation_recovered_total = 0
        engine._flow_reconciliation_rpc_calls = 0
        engine._flow_reconciliation_rpc_429 = 0
        engine._flow_reconciliation_resubscribe_requested = False
        engine._flow_reconciliation_resubscribe_callback = None
        engine._wss_flow_event_key_set = set()
        engine._pending_gap_events = deque(maxlen=5000)
        engine._persist_candidate = lambda *_args, **_kwargs: None
        engine._persist_flow_state = lambda *_args, **_kwargs: None
        engine._apply_flow_state_to_candidates = lambda *_args, **_kwargs: None
        engine._audit_event = lambda *_args, **_kwargs: None
        engine._reconciled_flow_event_keys = deque(maxlen=50000)
        engine._reconciled_flow_event_key_set = set()
        source = "flap_portal:0x" + "1" * 40
        calls = []
        engine._flow_reconciliation_resubscribe_callback = lambda: calls.append("resubscribe")
        for index in range(3):
            event = BscPairEvent(
                "0x" + "1" * 40, "flap_token_bought", 100 + index,
                "0x" + str(index + 1) * 64, index, self.now,
                pool_type=FLAP_PORTAL_POOL_TYPE, source="RPC_RECONCILIATION",
            )
            job = FlowReconciliationJob(source, event.pair_address, FLAP_PORTAL_POOL_TYPE,
                                        (FLAP_TOKEN_BOUGHT_TOPIC,), 100 + index, 100 + index, self.now)
            engine._flow_reconciliation_results.put(FlowReconciliationResult(
                job, (event,), (SurvivorReversalEngine._flow_event_key(event),),
                110 + index, 108 + index, self.now,
            ))
            engine._drain_flow_reconciliation_results(self.now)
        self.assertEqual(calls, ["resubscribe"])

    def test_rpc_timeout_marks_unverified_and_does_not_confirm_no_trade(self) -> None:
        engine = object.__new__(SurvivorReversalEngine)
        engine._flow_reconciliation_state = {}
        engine._flow_reconciliation_pending = set()
        engine._flow_reconciliation_results = Queue()
        engine._flow_reconciliation_misses_total = 0
        engine._flow_reconciliation_recovered_total = 0
        engine._flow_reconciliation_rpc_calls = 0
        engine._flow_reconciliation_rpc_429 = 0
        engine._flow_reconciliation_resubscribe_requested = False
        engine._flow_reconciliation_resubscribe_callback = None
        engine._wss_flow_event_key_set = set()
        engine._pending_gap_events = deque(maxlen=5000)
        engine._persist_candidate = lambda *_args, **_kwargs: None
        engine._persist_flow_state = lambda *_args, **_kwargs: None
        engine._apply_flow_state_to_candidates = lambda *_args, **_kwargs: None
        engine._audit_event = lambda *_args, **_kwargs: None
        job = FlowReconciliationJob("v2:0x" + "2" * 40, "0x" + "2" * 40, V2_POOL_TYPE,
                                    (SWAP_EVENT_TOPIC,), 100, 110, self.now)
        engine._flow_reconciliation_results.put(FlowReconciliationResult(
            job, (), (), 112, 110, self.now, "RPC_READ_FAILED", "timeout", rpc_calls=1,
        ))
        engine._drain_flow_reconciliation_results(self.now)
        state = engine._flow_reconciliation_state[job.source_key]
        self.assertEqual(state["data_completeness_health"], "DEGRADED")
        self.assertFalse(state["window_40s_complete"])

    def test_unknown_pre_migration_does_not_become_pancake_unresolved(self) -> None:
        mint = "0x" + "2" * 40

        class QuoteProvider:
            def resolve_venue(self, _mint):
                raise RuntimeError("venue_unrecognized")

        engine = object.__new__(SurvivorReversalEngine)
        engine.resolver = object()
        engine.quote_provider = QuoteProvider()
        engine.clock = lambda: self.now
        candidate = SurvivorCandidate(mint, "UNKNOWN", self.now, self.now, candidate_eligible=True, latest_migrate_status=0)
        engine._resolve_candidate_pool(candidate, migrate_status=0, now=self.now)
        self.assertEqual(candidate.source_status["venue_state"], "PRE_MIGRATION_VENUE_RESOLUTION")
        self.assertEqual(candidate.source_status["pool_status"], "VENUE_UNKNOWN")
        self.assertEqual(candidate.source_status["wss_subscription_status"], "NOT_REQUESTED")

    def test_queued_pre_migration_resolution_is_explicit_not_unknown_or_fourmeme(self) -> None:
        candidate = SurvivorCandidate("0x" + "2" * 40, "QUEUED", self.now, self.now, latest_migrate_status=0)
        SurvivorReversalEngine._mark_pre_migration_resolution_pending(candidate)
        self.assertEqual(candidate.source_status["venue"], "PENDING")
        self.assertEqual(candidate.source_status["venue_state"], "PRE_MIGRATION_VENUE_RESOLUTION")
        self.assertEqual(candidate.source_status["pool_status"], "PENDING")
        self.assertEqual(candidate.source_status["protocol_family"], "UNKNOWN_PREMIGRATION_FAMILY_PENDING")
        self.assertEqual(candidate.source_status["wss_subscription_status"], "NOT_REQUESTED")

    def test_unknown_contract_is_fingerprinted_as_venue_not_invalid_pool(self) -> None:
        class Rpc(BscRpcClient):
            configured = True

            def __init__(self):
                pass

            def call_batch(self, _calls):
                return (
                    "0x60006000", "0x" + "0" * 64,
                    None, None, None, None, None, None, None,
                )

            def get_code(self, _address):
                return "0x60006000"

            def get_storage_at(self, _address, _slot):
                return "0x" + "0" * 64

            def call_address(self, _address, _selector):
                return None

            def call_hex(self, _address, _selector):
                return None

            def call_uint(self, _address, _selector):
                return None

        inspection = BscPoolResolver(Rpc(), Rpc()).inspect_venue("0x" + "7" * 40)
        self.assertIsNotNone(inspection)
        assert inspection is not None
        self.assertTrue(inspection.is_contract)
        self.assertTrue(inspection.protocol_family.startswith("UNKNOWN_FAMILY_"))
        self.assertEqual(inspection.capability("DISCOVERY"), "SUPPORTED")
        self.assertEqual(inspection.capability("PAPER_FILL"), "UNSUPPORTED")

    def test_registry_first_preserves_unknown_venue_without_buy_capability(self) -> None:
        mint = "0x" + "2" * 40
        venue = "0x" + "4" * 40
        inspection = BscVenueInspection(
            address=venue, chain="bsc:56", is_contract=True, code_size=10,
            bytecode_hash="sha256:family", selector_bitmap="token0=0;token1=0",
            protocol_fingerprint="sha256:family", protocol_family="UNKNOWN_FAMILY_family",
            capabilities=(("DISCOVERY", "SUPPORTED"), ("FLOW", "UNSUPPORTED"), ("PAPER_FILL", "UNSUPPORTED")),
        )

        class Resolver:
            def inspect_venue(self, address):
                return inspection if address == venue else None

            def resolve_pancake_v2(self, *_args, **_kwargs):
                return BscPoolResolution(None, "UNSUPPORTED_POOL_TYPE", "VALIDATION", venue)

        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            engine.resolver = Resolver()
            candidate = SurvivorCandidate(mint, "UNKNOWN", self.now, self.now, candidate_eligible=True)
            engine._resolve_candidate_pool(candidate, pair=venue, protocol="Flap", migrate_status=1, now=self.now)
            row = engine.connection.execute(
                "SELECT protocol_family,strategy_support FROM bsc_venue_registry WHERE token_address=? AND venue_address=?",
                (mint, venue),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["protocol_family"], "UNKNOWN_FAMILY_family")
            self.assertEqual(row["strategy_support"], "UNSUPPORTED_PROTOCOL")
            self.assertEqual(candidate.source_status["pool_status"], "VALID_UNKNOWN_PROTOCOL")
            self.assertEqual(candidate.source_status["venue_state"], "POOL_FOUND_BUT_UNSUPPORTED")
            self.assertEqual(candidate.source_status["wss_subscription_status"], "NOT_REQUESTED")

    def test_v2_buy_flow_token1(self) -> None:
        descriptor = BscPoolDescriptor(
            address="0x" + "1" * 40, pool_type=V2_POOL_TYPE,
            mint="0x" + "2" * 40, token0=BSC_WBNB_ADDRESS,
            token1="0x" + "3" * 40, token0_decimals=18, token1_decimals=18,
        )
        event = BscPairEvent(descriptor.address, "swap", 1, None, 1, self.now, "v2", _data(2 * 10**18, 0, 0, 1000))
        sample = parse_v2_swap_flow(descriptor, event)
        self.assertIsNotNone(sample)
        assert sample is not None
        self.assertEqual(sample.buy_volume_bnb, Decimal("2"))
        self.assertEqual(sample.sell_volume_bnb, Decimal("0"))

    def test_v2_sell_flow_token1(self) -> None:
        descriptor = BscPoolDescriptor(
            address="0x" + "1" * 40, pool_type=V2_POOL_TYPE,
            mint="0x" + "2" * 40, token0=BSC_WBNB_ADDRESS,
            token1="0x" + "3" * 40, token0_decimals=18, token1_decimals=18,
        )
        event = BscPairEvent(descriptor.address, "swap", 1, None, 1, self.now, "v2", _data(0, 1000, 2 * 10**18, 0))
        sample = parse_v2_swap_flow(descriptor, event)
        self.assertIsNotNone(sample)
        assert sample is not None
        self.assertEqual(sample.sell_volume_bnb, Decimal("2"))

    def test_v2_buy_flow_token0(self) -> None:
        descriptor = BscPoolDescriptor(
            address="0x" + "1" * 40, pool_type=V2_POOL_TYPE,
            mint="0x" + "2" * 40, token0="0x" + "3" * 40,
            token1=BSC_WBNB_ADDRESS, token0_decimals=18, token1_decimals=18,
        )
        event = BscPairEvent(descriptor.address, "swap", 1, None, 1, self.now, "v2", _data(0, 2 * 10**18, 1000, 0))
        sample = parse_v2_swap_flow(descriptor, event)
        self.assertIsNotNone(sample)
        assert sample is not None
        self.assertEqual(sample.buy_volume_bnb, Decimal("2"))

    def test_non_native_pair_has_no_flow(self) -> None:
        descriptor = BscPoolDescriptor(
            address="0x" + "1" * 40, pool_type=V2_POOL_TYPE,
            mint="0x" + "2" * 40, token0="0x" + "3" * 40,
            token1="0x" + "4" * 40, token0_decimals=18, token1_decimals=18,
        )
        event = BscPairEvent(descriptor.address, "swap", 1, None, 1, self.now, "v2", _data(1, 1, 1, 1))
        self.assertIsNone(parse_v2_swap_flow(descriptor, event))

    def test_sync_event_is_not_swap_flow(self) -> None:
        descriptor = BscPoolDescriptor(address="0x" + "1" * 40, pool_type=V2_POOL_TYPE, token0=BSC_WBNB_ADDRESS, token1="0x" + "3" * 40)
        event = BscPairEvent(descriptor.address, "sync", 1, None, 1, self.now, "v2", _data(1, 2))
        self.assertIsNone(parse_v2_swap_flow(descriptor, event))

    def test_active_age_gate(self) -> None:
        candidate = SurvivorCandidate("0x1", "X", self.now - timedelta(seconds=179), self.now)
        candidate.market_cap_usd = Decimal("500000")
        candidate.liquidity_usd = Decimal("50000")
        candidate.holders = 500
        self.assertEqual(self.logic._discovery_state(candidate, self.now), "LIGHT_TRACKING")

    def test_active_market_cap_gate(self) -> None:
        candidate = SurvivorCandidate("0x1", "X", self.now - timedelta(minutes=3), self.now)
        candidate.market_cap_usd = Decimal("9999")
        candidate.liquidity_usd = Decimal("50000")
        candidate.holders = 500
        self.assertEqual(self.logic._discovery_state(candidate, self.now), "LIGHT_TRACKING")

    def test_active_liquidity_gate(self) -> None:
        candidate = SurvivorCandidate("0x1", "X", self.now - timedelta(minutes=3), self.now)
        candidate.market_cap_usd = Decimal("500000")
        candidate.liquidity_usd = Decimal("29999")
        candidate.holders = 500
        self.assertEqual(self.logic._discovery_state(candidate, self.now), "LIGHT_TRACKING")

    def test_active_holder_gate(self) -> None:
        candidate = SurvivorCandidate("0x1", "X", self.now - timedelta(minutes=3), self.now)
        candidate.market_cap_usd = Decimal("500000")
        candidate.liquidity_usd = Decimal("50000")
        candidate.holders = 149
        self.assertEqual(self.logic._discovery_state(candidate, self.now), "LIGHT_TRACKING")

    def test_missing_price_cannot_promote_to_active_candidate(self) -> None:
        self.logic.config = BalancedSurvivorConfig()
        candidate = SurvivorCandidate(
            "0x1", "X", self.now - timedelta(minutes=3), self.now,
            market_cap_usd=Decimal("5000"), liquidity_usd=Decimal("1000"), holders=30,
        )
        self.assertFalse(self.logic._candidate_eligible(candidate, self.now))
        self.assertEqual(self.logic._discovery_state(candidate, self.now), "PRE_CANDIDATE_PRICE_PENDING")
        self.assertFalse(candidate.active_candidate)
        self.assertEqual(candidate.last_rejection, "PRICE_PENDING")

    def test_valid_price_can_promote_after_price_pending(self) -> None:
        self.logic.config = BalancedSurvivorConfig()
        candidate = SurvivorCandidate(
            "0x1", "X", self.now - timedelta(minutes=3), self.now,
            market_cap_usd=Decimal("5000"), liquidity_usd=Decimal("1000"), holders=30,
        )
        self.logic._update_price(candidate, Decimal("0.00002"), self.now, "TOKEN_INFO", persist_snapshot=False)
        self.assertTrue(self.logic._candidate_eligible(candidate, self.now))
        self.assertEqual(self.logic._discovery_state(candidate, self.now), "ACTIVE_CANDIDATE")

    def test_flap_quote_asset_conversion_recovers_price_pending_candidate(self) -> None:
        """A Flap quote mark must re-enter the normal USD Candidate gate."""
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = BalancedSurvivorConfig()
        engine._latest_native_token_price_usd = Decimal("600")
        engine._latest_native_token_price_observed_at = self.now
        candidate = SurvivorCandidate(
            "0x1", "FLAP", self.now - timedelta(minutes=3), self.now,
            market_cap_usd=Decimal("5000"), liquidity_usd=Decimal("1000"), holders=30,
            price_status="PRICE_CONVERSION_PENDING",
            source_status={
                "protocol_family": "FLAP_CONTEXT",
                "flap_last_post_price": "0.000000045",
                "flap_last_post_price_at": self.now.isoformat(),
                "fundraising_quote_asset": "0x" + "3" * 40,
                "flap_quote_asset_per_bnb": "1",
            },
        )

        def apply_price(item, price, observed, source, **_kwargs):
            item.current_price_usd = price
            item.price_status = "VALID"
            item.price_source = source
            item.price_updated_at = observed

        engine._update_price = apply_price
        self.assertTrue(engine._recompute_flap_canonical_price(candidate, self.now))
        self.assertEqual(candidate.current_price_usd, Decimal("0.000027000"))
        self.assertEqual(candidate.price_status, "VALID")
        self.assertTrue(engine._candidate_eligible(candidate, self.now))
        self.assertEqual(engine._discovery_state(candidate, self.now), "ACTIVE_CANDIDATE")

    def test_flap_erc20_quote_asset_cache_builds_usd_price_without_assuming_one_dollar(self) -> None:
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = BalancedSurvivorConfig()
        engine._latest_native_token_price_usd = Decimal("600")
        engine._latest_native_token_price_observed_at = self.now
        engine._quote_asset_usd_refresh_seconds = 60
        quote_asset = "0x" + "4" * 40
        engine._quote_asset_usd_cache = {
            (56, quote_asset): QuoteAssetUsdResolution(
                quote_asset, 18, "Q", Decimal("160"), "GMGN_WBNB_QUOTE", self.now, True,
            ),
        }
        candidate = SurvivorCandidate(
            "0x1", "FLAP", self.now - timedelta(minutes=3), self.now,
            market_cap_usd=Decimal("5000"), liquidity_usd=Decimal("1000"), holders=30,
            price_status="PRICE_CONVERSION_PENDING",
            source_status={
                "protocol_family": "FLAP_CONTEXT",
                "flap_last_post_price": "0.0000003",
                "flap_last_post_price_at": self.now.isoformat(),
                "fundraising_quote_asset": quote_asset,
            },
        )

        def apply_price(item, price, observed, source, **_kwargs):
            item.current_price_usd = price
            item.price_status = "VALID"
            item.price_source = source
            item.price_updated_at = observed

        engine._update_price = apply_price
        self.assertTrue(engine._recompute_flap_canonical_price(candidate, self.now))
        self.assertEqual(candidate.current_price_usd, Decimal("0.0000480"))
        self.assertEqual(candidate.source_status["price_conversion_source"], "GMGN_WBNB_QUOTE")

    def test_pool_erc20_quote_asset_cache_builds_canonical_price(self) -> None:
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = BalancedSurvivorConfig()
        engine._latest_native_token_price_usd = Decimal("600")
        engine._latest_native_token_price_observed_at = self.now
        engine._quote_asset_usd_refresh_seconds = 60
        quote_asset = "0x" + "4" * 40
        engine._quote_asset_usd_cache = {
            (56, quote_asset): QuoteAssetUsdResolution(
                quote_asset, 18, "Q", Decimal("160"), "GMGN_WBNB_QUOTE", self.now, True,
            ),
        }
        candidate = SurvivorCandidate(
            "0x1", "POOL", self.now - timedelta(minutes=3), self.now,
            market_cap_usd=Decimal("5000"), liquidity_usd=Decimal("1000"), holders=30,
            source_status={"pool_quote_asset": quote_asset, "pool_last_raw_price": "0.0000003"},
        )

        def apply_price(item, price, observed, source, **kwargs):
            item.current_price_usd = price
            item.price_status = "VALID"
            item.price_source = source
            item.price_updated_at = observed
            item.current_price_native = kwargs.get("native_price")

        engine._update_price = apply_price
        engine._schedule_quote_asset_usd_resolution = lambda *_args: None
        self.assertTrue(engine._recompute_pool_canonical_price(candidate, self.now))
        self.assertEqual(candidate.current_price_usd, Decimal("0.0000480"))
        self.assertEqual(candidate.current_price_native, Decimal("0.00000008"))
        self.assertEqual(candidate.source_status["price_conversion_source"], "GMGN_WBNB_QUOTE")

    def test_missing_price_is_enriched_before_candidate_promotion(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            engine.config = BalancedSurvivorConfig()

            class MarketData:
                @staticmethod
                def snapshot(_mint: str) -> dict[str, str]:
                    return {"price_usd": "0.00002"}

            candidate = SurvivorCandidate(
                "0x1", "X", self.now - timedelta(minutes=3), self.now,
                market_cap_usd=Decimal("5000"), liquidity_usd=Decimal("1000"), holders=30,
            )
            engine.market_data = MarketData()
            engine._candidates[candidate.mint] = candidate
            engine.on_records([], self.now)

            self.assertEqual(candidate.price_status, "VALID")
            self.assertEqual(candidate.first_seen_price_usd, Decimal("0.00002"))
            self.assertEqual(candidate.ath_price_usd, Decimal("0.00002"))
            self.assertTrue(candidate.candidate_eligible)
            self.assertEqual(candidate.state, "ACTIVE_CANDIDATE")

    def test_live_balanced_keeps_binance_price_as_reference_only(self) -> None:
        engine = object.__new__(SurvivorReversalEngine)
        engine.mode = "live"
        engine.config = BalancedSurvivorConfig()
        candidate = SurvivorCandidate("0x1", "X", self.now, self.now)

        engine._update_price(candidate, Decimal("0.00002"), self.now, "MEME_RUSH")

        self.assertIsNone(candidate.current_price_usd)
        self.assertEqual(candidate.price_status, "CANONICAL_PRICE_PENDING")
        self.assertEqual(candidate.source_status["discovery_reference_price"], "0.00002")
        self.assertEqual(candidate.source_status["price_source_priority"], "CANONICAL_VENUE_PRICE")

    def test_missing_holders_can_be_active_but_not_buyable(self) -> None:
        candidate = SurvivorCandidate("0x1", "X", self.now - timedelta(minutes=3), self.now)
        candidate.market_cap_usd = Decimal("300000")
        candidate.liquidity_usd = Decimal("50000")
        self.assertEqual(self.logic._discovery_state(candidate, self.now), "LIGHT_TRACKING")
        self.assertEqual(self.logic._universe_reason(candidate, self.now), "HOLDERS_REQUIRED_BEFORE_BUY")

    def test_universe_mc_upper_bound(self) -> None:
        candidate = SurvivorCandidate("0x1", "X", self.now - timedelta(minutes=3), self.now, market_cap_usd=Decimal("2000001"), liquidity_usd=Decimal("200000"), holders=500)
        self.assertEqual(self.logic._universe_reason(candidate, self.now), "MC_OUTSIDE_300K_2M")

    def test_universe_lp_mc_gate(self) -> None:
        self.logic.config = SurvivorReversalConfig(universe_max_mc_usd=Decimal("1000000"))
        candidate = SurvivorCandidate("0x1", "X", self.now - timedelta(minutes=3), self.now, market_cap_usd=Decimal("1000000"), liquidity_usd=Decimal("79999"), holders=500)
        self.assertEqual(self.logic._universe_reason(candidate, self.now), "LP_MC_BELOW_8_PERCENT")

    def test_universe_holder_gate(self) -> None:
        candidate = SurvivorCandidate("0x1", "X", self.now - timedelta(minutes=3), self.now, market_cap_usd=Decimal("300000"), liquidity_usd=Decimal("100000"), holders=299)
        self.assertEqual(self.logic._universe_reason(candidate, self.now), "HOLDERS_BELOW_350")

    def test_flow_window_aggregates_only_last_minute(self) -> None:
        candidate = SurvivorCandidate("0x1", "X", self.now, self.now)
        candidate.flows.extend([
            FlowSample(self.now - timedelta(seconds=61), Decimal("9"), Decimal("1"), 9, 1),
            FlowSample(self.now - timedelta(seconds=30), Decimal("3"), Decimal("2"), 3, 2),
        ])
        buy, sell, buy_count, sell_count = self.logic._flow_1m(candidate, self.now)
        self.assertEqual((buy, sell, buy_count, sell_count), (Decimal("3"), Decimal("2"), 3, 2))

    def test_candidate_at_price_is_in_first_seen_ath_tracking(self) -> None:
        candidate_at = self.now + timedelta(minutes=20)
        candidate = SurvivorCandidate("0x1", "X", self.now, candidate_at)
        samples = [
            FlowSample(self.now + timedelta(seconds=index * 60), price_usd=Decimal(str(100 + index)))
            for index in range(20)
        ]
        samples[-1].observed_at = candidate_at
        engine = object.__new__(SurvivorReversalEngine)
        engine._freeze_candidate_history(candidate, samples, candidate_at)
        self.assertEqual(candidate.price_history_status, "FIRST_SEEN_TRACKING")
        self.assertEqual(candidate.price_history_quality, "FIRST_SEEN")
        self.assertTrue(candidate.ath_before_candidate)
        self.assertEqual(candidate.ath_before_candidate_price_usd, Decimal("119"))
        self.assertEqual(candidate.ath_before_candidate_at, candidate_at)

    def test_sparse_history_is_diagnostic_only_and_allows_pullback(self) -> None:
        candidate_at = self.now + timedelta(minutes=30)
        candidate = SurvivorCandidate("0x1", "X", self.now, candidate_at)
        samples = [
            FlowSample(self.now + timedelta(seconds=index * 60), price_usd=Decimal(str(100 + index)))
            for index in range(10)
        ]
        samples += [
            FlowSample(self.now + timedelta(seconds=1200 + index * 60), price_usd=Decimal(str(110 + index)))
            for index in range(9)
        ]
        samples.append(FlowSample(candidate_at, price_usd=Decimal("120")))
        engine = object.__new__(SurvivorReversalEngine)
        engine._freeze_candidate_history(candidate, samples, candidate_at)
        self.assertEqual(candidate.price_history_status, "FIRST_SEEN_TRACKING")
        self.assertEqual(candidate.price_history_quality, "FIRST_SEEN")
        candidate.price_status = "VALID"
        candidate.first_seen_price_usd = Decimal("100")
        candidate.first_price_at = self.now
        candidate.ath_price_usd = Decimal("120")
        self.assertTrue(engine._history_allows_pullback(candidate))

    def test_one_first_seen_price_is_enough_for_pullback_tracking(self) -> None:
        candidate = SurvivorCandidate(
            "0x1", "X", self.now, self.now,
            candidate_eligible=True,
            first_seen_price_usd=Decimal("100"),
            first_price_at=self.now,
            ath_price_usd=Decimal("100"),
            price_status="VALID",
            price_history_status="PRICE_HISTORY_INCOMPLETE",
            price_history_quality="INSUFFICIENT",
        )
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = self.config
        self.assertTrue(engine._history_allows_pullback(candidate))
        self.assertIsNone(engine._history_block_reason(candidate))

    def test_missing_audit_is_unknown(self) -> None:
        candidate = SurvivorCandidate("0x1", "X", self.now, self.now)
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            self.assertEqual(engine._audit_result(candidate)[0], "UNKNOWN")

    def test_pullback_starts_at_thirty_percent(self) -> None:
        candidate = SurvivorCandidate("0x1", "X", self.now - timedelta(seconds=180), self.now, state="ACTIVE_CANDIDATE", candidate_eligible=True, first_seen_price_usd=Decimal("100"), ath_price_usd=Decimal("100"), ath_price_native=Decimal("100"), price_history_status="VALID")
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = self.config
        engine._update_price(candidate, Decimal("70"), self.now)
        self.assertEqual(candidate.state, "PULLBACK_ZONE")
        self.assertEqual(candidate.local_low_native, Decimal("70"))

    def test_below_fifty_percent_is_rejected(self) -> None:
        candidate = SurvivorCandidate("0x1", "X", self.now - timedelta(seconds=180), self.now, state="ACTIVE_CANDIDATE", candidate_eligible=True, first_seen_price_usd=Decimal("100"), ath_price_usd=Decimal("100"), ath_price_native=Decimal("100"), price_history_status="VALID")
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = self.config
        engine._update_price(candidate, Decimal("49"), self.now)
        self.assertEqual(candidate.state, "REJECTED")

    def test_new_ath_resets_pullback(self) -> None:
        candidate = SurvivorCandidate("0x1", "X", self.now - timedelta(seconds=180), self.now, state="STOP_CONFIRMED", candidate_eligible=True, first_seen_price_usd=Decimal("100"), ath_price_usd=Decimal("100"), ath_price_native=Decimal("100"), local_low_native=Decimal("60"), price_history_status="VALID")
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = self.config
        engine._update_price(candidate, Decimal("101"), self.now)
        self.assertEqual(candidate.ath_price_native, Decimal("101"))
        self.assertIsNone(candidate.local_low_native)

    def test_rebound_ten_to_twenty_is_allowed(self) -> None:
        candidate = SurvivorCandidate("0x1", "X", self.now, self.now, state="STOP_CONFIRMED", local_low_native=Decimal("100"), current_price_native=Decimal("110"), ath_price_native=Decimal("150"))
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = self.config
        engine._universe_reason = lambda *_: None
        candidate.current_price_usd = Decimal("110")
        candidate.ath_price_usd = Decimal("150")
        candidate.drawdown_pct = Decimal("-33.33")
        candidate.price_status = "VALID"
        engine._request_audit = lambda *_: ("PASS", "AUDIT_PASS")
        engine._flow_1m = lambda *_: (Decimal("2"), Decimal("1"), 4, 2)
        engine._try_buy = lambda *_: None
        engine._audit_event = lambda *_: None
        engine._evaluate_candidate(candidate, self.now)
        self.assertEqual(candidate.state, "READY_TO_BUY")

    def test_rebound_above_twenty_rejects_chase(self) -> None:
        candidate = SurvivorCandidate("0x1", "X", self.now, self.now, state="STOP_CONFIRMED", local_low_native=Decimal("100"), current_price_native=Decimal("121"), current_price_usd=Decimal("121"), ath_price_native=Decimal("150"), ath_price_usd=Decimal("150"), price_status="VALID")
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = self.config
        engine._evaluate_candidate(candidate, self.now)
        self.assertEqual(candidate.state, "REJECTED")

    def test_idle_candidate_expires(self) -> None:
        candidate = SurvivorCandidate("0x1", "X", self.now - timedelta(hours=1), self.now - timedelta(minutes=31))
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = self.config
        engine._persist_candidate = lambda *_: None
        engine._evaluate_candidate(candidate, self.now)
        self.assertEqual(candidate.state, "EXPIRED")

    def test_idle_candidate_near_pullback_is_retained(self) -> None:
        candidate = SurvivorCandidate(
            "0x1", "X", self.now - timedelta(hours=1), self.now - timedelta(minutes=31),
            state="ACTIVE_CANDIDATE", candidate_eligible=True,
            price_status="VALID", price_history_status="VALID",
            first_seen_price_usd=Decimal("100"), first_price_at=self.now - timedelta(hours=1),
            ath_price_usd=Decimal("100"),
            drawdown_pct=Decimal("-25.285"),
        )
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = self.config
        self.assertTrue(engine._should_retain_candidate(candidate, self.now))

    def test_audit_prefetch_is_recorded_without_blocking_call(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            candidate = SurvivorCandidate(
                "0x1", "X", self.now - timedelta(minutes=5), self.now,
                candidate_eligible=True, price_status="VALID",
                price_history_status="VALID", first_seen_price_usd=Decimal("100"),
                first_price_at=self.now - timedelta(minutes=5), ath_price_usd=Decimal("100"),
                drawdown_pct=Decimal("-25"),
            )
            engine._candidates[candidate.mint] = candidate
            engine._maybe_schedule_audit_prefetch(candidate, self.now)
            self.assertTrue(candidate.audit_requested)
            self.assertIn(candidate.audit_state, {"PENDING", "SOURCE_UNAVAILABLE", "INVALID", "VALID"})
            engine._audit_prefetch_executor.shutdown(wait=True)
            engine._drain_audit_prefetch_results(self.now)
            engine.connection.commit()
            self.assertIn(candidate.audit_state, {"SOURCE_UNAVAILABLE", "INVALID", "VALID"})
            self.assertIsNotNone(candidate.source_status.get("audit_prefetch_at"))

    def test_balanced_audit_prefetch_1000_results_have_no_owner_write_loss(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            engine.config = BalancedSurvivorConfig()
            candidates = []
            for index in range(1000):
                candidate = SurvivorCandidate(
                    f"0x{index:040x}", f"T{index}", self.now - timedelta(minutes=5), self.now,
                    candidate_eligible=True, price_status="VALID", price_history_status="VALID",
                    drawdown_pct=Decimal("-25"),
                )
                engine._candidates[candidate.mint] = candidate
                engine._audit_prefetch_pending.add(candidate.mint)
                candidates.append(candidate)
            commit_errors = locked_errors = write_errors = 0
            with ThreadPoolExecutor(max_workers=4) as workers:
                futures = [workers.submit(lambda: ("PASS", "AUDIT_OK")) for _ in candidates]
                for candidate, future in zip(candidates, futures):
                    future.add_done_callback(
                        lambda completed, mint=candidate.mint: engine._complete_audit_prefetch(mint, completed)
                    )
                for index in range(1000):
                    try:
                        engine.store.set_state("audit_prefetch_stress", {"sequence": index})
                        engine._drain_audit_prefetch_results(self.now)
                    except Exception as exc:
                        write_errors += 1
                        if "commit" in str(exc).lower():
                            commit_errors += 1
                        if "locked" in str(exc).lower():
                            locked_errors += 1
            engine._drain_audit_prefetch_results(self.now)
            self.assertEqual(commit_errors, 0)
            self.assertEqual(locked_errors, 0)
            self.assertEqual(write_errors, 0)
            self.assertEqual(sum(1 for item in candidates if item.audit_state == "VALID"), 1000)
            self.assertEqual(len(engine._audit_prefetch_pending), 0)

    def test_status_is_paper_only(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            status = engine.status()
            self.assertTrue(status["paper_only"])
            self.assertEqual(status["execution_provider"], "paper")

    def test_no_trade_exit_after_forty_seconds(self) -> None:
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = SurvivorReversalConfig(no_trade_exit_sec=40)
        engine._positions = {}
        engine._candidates = {}
        events: list[tuple[str, str]] = []
        engine._audit_event = lambda event_type, candidate, payload: events.append((event_type, str(payload["reason"])))
        engine._persist_position = lambda position, now, reason: None

        class QuoteProvider:
            def quote(self, mint: str, side: str, input_quantity: Decimal) -> ExecutableQuote:
                return ExecutableQuote(
                    quote_id="test-quote", mint=mint, side=side,
                    input_quantity=input_quantity, output_quantity=Decimal("0.009"),
                    route_fee=None, price_impact_pct=None, quoted_at=self.now,
                    age_ms=0,
                )

        quote_provider = QuoteProvider()
        quote_provider.now = self.now
        engine.quote_provider = quote_provider
        old_position = SurvivorPosition(
            position_id="survivor:0x1:test", mint="0x1", symbol="X",
            opened_at=self.now - timedelta(seconds=41),
            entry_price_native=Decimal("1"), current_price_native=Decimal("1"),
            quantity_token=Decimal("10"), remaining_quantity_token=Decimal("10"),
            invested_bnb=Decimal("0.01"), last_trade_at=self.now - timedelta(seconds=41),
        )
        engine._positions[old_position.position_id] = old_position
        # Without a verified venue subscription this position must not be
        # falsely classified as a 40-second no-trade exit.  Binance Meme Rush
        # activity is not a substitute for a real parsed venue event stream.
        engine._evaluate_positions(self.now)
        self.assertEqual(old_position.status, "OPEN")

        monitored = SurvivorCandidate(
            "0x1", "X", self.now - timedelta(seconds=41), self.now,
            descriptor=BscPoolDescriptor(
                address="0x" + "a" * 40, pool_type=V2_POOL_TYPE, mint="0x1",
                token0=BSC_WBNB_ADDRESS, token1="0x" + "b" * 40,
            ),
            source_status={"wss_subscription_status": "SUBSCRIBED", "flow_status": "SUPPORTED"},
        )
        engine._candidates = {monitored.mint: monitored}
        old_position.status = "OPEN"
        engine._evaluate_positions(self.now)
        self.assertEqual(old_position.status, "OPEN")
        self.assertEqual(events, [])

        # A callback may enqueue a real Swap immediately after the owner-loop
        # drains its queue.  The in-memory marker must still refresh
        # last_trade_at before the 40-second check, without a worker commit.
        engine._wss_provider_state = "HEALTHY"
        engine._wss_subscribed_addresses = {monitored.descriptor.address}
        engine.config = replace(engine.config, time_stop_sec=10_000)
        engine._wss_trade_markers = {}
        engine._wss_trade_markers_lock = threading.Lock()
        engine._pending_events = deque()
        old_position.status = "OPEN"
        old_position.current_price_native = Decimal("1")
        old_position.last_trade_at = self.now - timedelta(seconds=41)
        engine._persist_trade_activity = lambda position, observed_at: None
        closed_reasons: list[str] = []
        engine._close_position = lambda position, reason, now: closed_reasons.append(str(reason))
        engine.on_wss_event(BscPairEvent(
            monitored.descriptor.address, "swap", 2, None, 1, self.now,
            "v2", _data(2 * 10**18, 0, 0, 1000),
        ))
        engine._evaluate_positions(self.now)
        self.assertEqual(old_position.status, "OPEN")
        self.assertEqual(old_position.last_trade_at, self.now)
        self.assertEqual(closed_reasons, [])

        recent_position = SurvivorPosition(
            position_id="survivor:0x2:test", mint="0x2", symbol="Y",
            opened_at=self.now - timedelta(seconds=41),
            entry_price_native=Decimal("1"), current_price_native=Decimal("1"),
            quantity_token=Decimal("10"), remaining_quantity_token=Decimal("10"),
            invested_bnb=Decimal("0.01"), last_trade_at=self.now - timedelta(seconds=39),
        )
        engine._positions = {recent_position.position_id: recent_position}
        engine._evaluate_positions(self.now)
        self.assertEqual(recent_position.status, "OPEN")

    def test_flap_portal_activity_marker_is_not_shared_between_tokens(self) -> None:
        engine = object.__new__(SurvivorReversalEngine)
        engine.config = SurvivorReversalConfig(no_trade_exit_sec=40, time_stop_sec=10_000)
        engine._positions = {}
        engine._candidates = {}
        engine._wss_provider_state = "HEALTHY"
        engine._wss_subscribed_addresses = {FLAP_PORTAL_ADDRESS}
        engine._wss_trade_markers = {FLAP_PORTAL_ADDRESS: self.now}
        engine._wss_trade_markers_lock = threading.Lock()
        engine._persist_trade_activity = lambda *_args: None
        engine._persist_position = lambda *_args, **_kwargs: None
        reasons: list[str] = []
        engine._close_position = lambda _position, reason, _now: reasons.append(str(reason))

        candidate = SurvivorCandidate(
            "0x1", "FLAP", self.now - timedelta(minutes=2), self.now,
            latest_migrate_status=0,
            current_price_native=Decimal("1"),
            source_status={
                "venue": "Flap",
                "protocol_family": "FLAP_CONTEXT",
                "flow_status": "SUPPORTED",
                "wss_subscription_status": "SUBSCRIBED",
            },
        )
        position = SurvivorPosition(
            position_id="survivor:flap:test", mint="0x1", symbol="FLAP",
            opened_at=self.now - timedelta(seconds=41),
            entry_price_native=Decimal("1"), current_price_native=Decimal("1"),
            quantity_token=Decimal("10"), remaining_quantity_token=Decimal("10"),
            invested_bnb=Decimal("0.01"), last_trade_at=self.now - timedelta(seconds=41),
        )
        engine._candidates[candidate.mint] = candidate
        engine._positions[position.position_id] = position

        # The shared Portal had an unrelated event.  It must not refresh this
        # token's timer or suppress its own 40-second no-trade decision.
        engine._evaluate_positions(self.now)
        self.assertEqual(reasons, [])
        self.assertEqual(position.status, "OPEN")
        self.assertEqual(position.last_trade_at, self.now - timedelta(seconds=41))

    def test_open_position_rehydrates_persisted_v2_descriptor_for_wss(self) -> None:
        engine = object.__new__(SurvivorReversalEngine)
        engine._lock = threading.RLock()
        engine.clock = lambda: self.now
        engine.config = SurvivorReversalConfig()
        engine._candidates = {}
        engine._positions = {}
        mint = "0x" + "1" * 40
        pool = "0x" + "2" * 40
        candidate = SurvivorCandidate(
            mint, "X", self.now - timedelta(minutes=2), self.now,
            pair_address=pool,
            pool_type=V2_POOL_TYPE,
            source_status={
                "pool_token0": BSC_WBNB_ADDRESS,
                "pool_token1": mint,
            },
        )
        position = SurvivorPosition(
            position_id="survivor:test:open", mint=mint, symbol="X",
            opened_at=self.now - timedelta(minutes=1), entry_price_native=Decimal("1"),
            current_price_native=Decimal("1"), quantity_token=Decimal("1"),
            remaining_quantity_token=Decimal("1"), invested_bnb=Decimal("0.01"),
        )
        engine._candidates[mint] = candidate
        engine._positions[position.position_id] = position
        details = engine.subscription_details()
        self.assertEqual(len(details), 1)
        self.assertEqual(details[0]["reason"], "OPEN_POSITION")
        self.assertEqual(details[0]["descriptor"].address, pool)
        self.assertEqual(candidate.descriptor.address, pool)

    def test_open_position_rehydrates_fourmeme_manager_descriptor_for_wss(self) -> None:
        engine = object.__new__(SurvivorReversalEngine)
        engine._lock = threading.RLock()
        engine.clock = lambda: self.now
        engine.config = SurvivorReversalConfig()
        engine._candidates = {}
        engine._positions = {}
        mint = "0x" + "3" * 40
        candidate = SurvivorCandidate(
            mint, "FOUR", self.now - timedelta(minutes=2), self.now,
            pair_address=FOUR_MEME_TOKEN_MANAGER,
            pool_type=BONDING_CURVE_POOL_TYPE,
            source_status={
                "protocol": "FourMeme",
                "pool_source": "FOURMEME_TOKEN_MANAGER",
            },
        )
        position = SurvivorPosition(
            position_id="survivor:test:fourmeme-open", mint=mint, symbol="FOUR",
            opened_at=self.now - timedelta(minutes=1), entry_price_native=Decimal("1"),
            current_price_native=Decimal("1"), quantity_token=Decimal("1"),
            remaining_quantity_token=Decimal("1"), invested_bnb=Decimal("0.01"),
        )
        engine._candidates[mint] = candidate
        engine._positions[position.position_id] = position
        details = engine.subscription_details()
        self.assertEqual(len(details), 1)
        self.assertEqual(details[0]["reason"], "OPEN_POSITION")
        self.assertEqual(details[0]["descriptor"].address, FOUR_MEME_TOKEN_MANAGER)
        self.assertEqual(details[0]["descriptor"].pool_type, BONDING_CURVE_POOL_TYPE)
        self.assertEqual(candidate.descriptor.event_topics, (FOUR_TOKEN_PURCHASE_TOPIC, FOUR_TOKEN_SALE_TOPIC))

    def test_closed_position_pnl_uses_realized_executable_quote(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            position = SurvivorPosition(
                position_id="survivor:0x1:closed", mint="0x1", symbol="X",
                opened_at=self.now, entry_price_native=Decimal("1"),
                current_price_native=Decimal("0.95"), quantity_token=Decimal("1"),
                remaining_quantity_token=Decimal("0"), invested_bnb=Decimal("0.01"),
                realized_bnb=Decimal("0.002"), status="CLOSED",
            )
            engine.connection.execute(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,current_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (position.position_id, position.mint, position.symbol, self.now.isoformat(), "OPEN", "1", "1", "1", "1", "0.01", "0", self.now.isoformat()),
            )
            engine.connection.commit()
            engine._persist_position(position, self.now, reason="TEST")
            stored = engine.connection.execute("SELECT pnl_pct FROM survivor_positions WHERE position_id=?", (position.position_id,)).fetchone()[0]
            self.assertEqual(Decimal(str(stored)), Decimal("-80"))

    def test_exit_trigger_is_separate_from_final_fill_pnl(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            position = SurvivorPosition(
                position_id="survivor:0x1:trigger", mint="0x1", symbol="X",
                opened_at=self.now, entry_price_native=Decimal("1"),
                current_price_native=Decimal("3"), quantity_token=Decimal("10"),
                remaining_quantity_token=Decimal("10"), invested_bnb=Decimal("1"),
            )
            engine.connection.execute(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,current_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (position.position_id, position.mint, position.symbol, self.now.isoformat(), "OPEN", "1", "3", "10", "10", "1", "0", self.now.isoformat()),
            )
            engine.connection.commit()
            engine._set_exit_intent(position, "TP3_PLUS_200", Decimal("10"), self.now, "REQUEST_TIMEOUT")
            row = engine.connection.execute(
                "SELECT exit_trigger_reason,exit_trigger_pnl_pct,exit_trigger_price_native,exit_triggered_at FROM survivor_positions WHERE position_id=?",
                (position.position_id,),
            ).fetchone()
            self.assertEqual(row[0], "TP3_PLUS_200")
            self.assertEqual(Decimal(str(row[1])), Decimal("200"))
            self.assertEqual(Decimal(str(row[2])), Decimal("3"))
            intent = engine.connection.execute(
                "SELECT value_json FROM runtime_state WHERE mode='paper' AND state_key=?",
                (f"exit_intent:{position.position_id}",),
            ).fetchone()[0]
            self.assertEqual(json.loads(intent)["trigger_reason"], "TP3_PLUS_200")

    def test_open_position_mark_tracks_latest_wss_price(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            candidate = SurvivorCandidate("0x1", "X", self.now, self.now, current_price_native=Decimal("1"), current_price_usd=Decimal("1"))
            position = SurvivorPosition(
                position_id="survivor:0x1:open", mint="0x1", symbol="X",
                opened_at=self.now, entry_price_native=Decimal("1"),
                current_price_native=Decimal("1"), quantity_token=Decimal("1"),
                remaining_quantity_token=Decimal("1"), invested_bnb=Decimal("1"), last_trade_at=self.now,
            )
            engine._candidates[candidate.mint] = candidate
            engine._positions[position.position_id] = position
            engine.connection.execute(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,current_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (position.position_id, position.mint, position.symbol, self.now.isoformat(), "OPEN", "1", "1", "1", "1", "1", "0", self.now.isoformat()),
            )
            engine.connection.commit()
            engine._update_price(candidate, Decimal("0.95"), self.now, "BSC_WSS", native_price=Decimal("0.95"), persist_snapshot=False)
            engine._evaluate_positions(self.now)
            stored = engine.connection.execute("SELECT current_price_native,pnl_pct FROM survivor_positions WHERE position_id=?", (position.position_id,)).fetchone()
            self.assertEqual(Decimal(str(stored[0])), Decimal("0.95"))
            self.assertEqual(Decimal(str(stored[1])), Decimal("-5.00"))

    def _engine(self, path: Path) -> SurvivorReversalEngine:
        connection = initialize_database(path / "runtime.db")
        return SurvivorReversalEngine(
            connection=connection,
            store=RuntimeStore(connection, "paper"),
            health=HealthRegistry(),
            audit=JsonlAuditWriter(path / "events.jsonl"),
            quote_provider=None,
            resolver=None,
            controls=RuntimeControl(path / "control.json"),
        )

    def test_migrated_transition_and_startup_restore_queue_one_recovery(self) -> None:
        """A migrated current token is queued both on transition and restart."""
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            mint = "0x" + "a" * 40
            candidate = SurvivorCandidate(
                mint, "trustmebro", self.now - timedelta(minutes=2), self.now,
                latest_migrate_status=1,
            )
            engine._candidates[mint] = candidate
            scheduled: list[str] = []
            engine._schedule_migrated_pool_recovery = lambda item, _now: scheduled.append(item.mint)  # type: ignore[method-assign]

            self.assertTrue(engine.ensure_migrated_market_data(candidate, self.now))
            self.assertEqual(scheduled, [mint])
            self.assertEqual(candidate.source_status["migrated_pool_recovery_state"], "PENDING")

            candidate.source_status.clear()
            engine._migrated_pool_recovery_jobs.clear()
            engine._hydrate_active_candidate_pools()
            self.assertEqual(scheduled, [mint, mint])

    def test_migrated_no_pool_is_pending_and_next_evaluation_requeues(self) -> None:
        """A point-in-time no-pool result cannot become a terminal state."""
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            mint = "0x" + "b" * 40
            candidate = SurvivorCandidate(
                mint, "焦绿猫", self.now - timedelta(minutes=2), self.now,
                latest_migrate_status=1,
            )
            engine._candidates[mint] = candidate
            event = BscPairEvent(mint, "factory_pair_created", None, None, None, self.now, V2_POOL_TYPE, source="MIGRATED_RUNTIME_DISCOVERY")
            job = FactoryPoolResolutionJob(event, V2_POOL_TYPE, mint, mint, mint, self.now)
            engine._migrated_pool_recovery_jobs.add(mint)
            engine._factory_resolution_results.put(FactoryPoolResolutionResult(
                job, None, ((mint, BscPoolResolution(None, "NO_PANCAKE_PAIR", "MIGRATED_RUNTIME_DISCOVERY")),), self.now, 1,
            ))
            engine._drain_factory_pool_resolution_results(self.now)
            self.assertEqual(candidate.source_status["pool_status"], "MIGRATED_POOL_PENDING")
            self.assertEqual(candidate.source_status["migrated_pool_recovery_state"], "PENDING")

            scheduled: list[str] = []
            engine._schedule_migrated_pool_recovery = lambda item, _now: scheduled.append(item.mint)  # type: ignore[method-assign]
            self.assertTrue(engine.ensure_migrated_market_data(candidate, self.now + timedelta(seconds=3)))
            self.assertEqual(scheduled, [mint])

    def test_stale_pre_migration_result_cannot_overwrite_migrated_state(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            mint = "0x" + "c" * 40
            candidate = SurvivorCandidate(mint, "STALE", self.now, self.now, latest_migrate_status=1)
            candidate.source_status["venue_state"] = "MIGRATED_POOL_PENDING"
            engine._candidates[mint] = candidate
            job = SimpleNamespace(mint=mint)
            engine._venue_resolution_jobs[mint] = job
            engine._venue_resolution_results.put(SimpleNamespace(job=job))
            engine._drain_pre_migration_venue_results(self.now, max_items=1, budget_ms=10)
            self.assertEqual(candidate.source_status["venue_state"], "MIGRATED_POOL_PENDING")
            self.assertEqual(candidate.source_status["venue_resolution_job_state"], "STALE_RESULT_DISCARDED")

    def test_migrated_pool_result_restores_binding_and_candidate_reevaluation(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            mint = "0x" + "d" * 40
            descriptor = BscPoolDescriptor("0x" + "e" * 40, V2_POOL_TYPE, mint=mint, token0=mint, token1=BSC_WBNB_ADDRESS)
            candidate = SurvivorCandidate(mint, "POOL", self.now - timedelta(minutes=2), self.now, latest_migrate_status=1)
            engine._candidates[mint] = candidate
            engine._candidate_eligible = lambda *_: True  # type: ignore[method-assign]
            engine._mark_candidate_entry = lambda item, at: setattr(item, "candidate_at", at)  # type: ignore[method-assign]
            engine._discovery_state = lambda *_: "ACTIVE_CANDIDATE"  # type: ignore[method-assign]
            engine._recompute_pool_canonical_price = lambda *_: True  # type: ignore[method-assign]
            event = BscPairEvent(mint, "factory_pair_created", None, None, None, self.now, V2_POOL_TYPE, source="MIGRATED_RUNTIME_DISCOVERY")
            job = FactoryPoolResolutionJob(event, V2_POOL_TYPE, mint, mint, mint, self.now)
            engine._factory_resolution_results.put(FactoryPoolResolutionResult(
                job, None, ((mint, BscPoolResolution(descriptor, "VALID", "PANCAKE_V2_FACTORY", reserves=(1, 1), quote_asset=BSC_WBNB_ADDRESS)),), self.now, 1,
            ))
            engine._drain_factory_pool_resolution_results(self.now)
            self.assertEqual(candidate.descriptor, descriptor)
            self.assertEqual(candidate.source_status["wss_subscription_status"], "READY")
            self.assertTrue(candidate.candidate_eligible)
            self.assertEqual(candidate.state, "ACTIVE_CANDIDATE")


if __name__ == "__main__":
    unittest.main()
