from __future__ import annotations

import os
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from meme_system.adapters.binance_web3.models import BinanceNormalizedSignal, ObservedField
from meme_system.adapters.protocols import ExecutableQuote
from meme_system.adapters.solana_price import SolanaObservedPrice
from meme_system.domain.models import Signal
from meme_system.runtime_ops import HealthRegistry, JsonlAuditWriter, RuntimeControl
from meme_system.storage.database import initialize_database
from meme_system.storage.runtime_store import RuntimeStore
from meme_system.strategies.sol_survivor_reversal import (
    SolSurvivorQuoteRouter,
    SolSurvivorReversalConfig,
    SolSurvivorReversalEngine,
)
from meme_system.strategies.survivor_reversal import FlowSample, SurvivorCandidate


NOW = datetime(2026, 8, 9, tzinfo=timezone.utc)
MINT = "Er4q21XgvtaSRq3vpJYRzn2Vy64ezcVZ7XFfeNhZpump"


def _record(now: datetime, rank: int, price: str = "0.00001", *, mc: str = "300000", liquidity: str = "50000", holders: int = 350, created_at: datetime = NOW) -> BinanceNormalizedSignal:
    lifecycle = {10: "MEME_NEW", 20: "MEME_FINALIZING", 30: "MEME_MIGRATED"}[rank]
    values = {
        "symbol": "SOLTEST", "rank_type": rank, "lifecycle": lifecycle,
        "price_usd": Decimal(price), "native_token_price": Decimal("100"),
        "market_cap_usd": Decimal(mc), "liquidity_usd": Decimal(liquidity),
        "holders": holders, "progress_pct": Decimal("50"),
        "migrate_status": 1 if rank == 30 else 0,
        "token_created_at": created_at,
    }
    fields = {
        name: ObservedField(value, "fixture", name, now, None, 0, True)
        for name, value in values.items()
    }
    return BinanceNormalizedSignal(
        signal=Signal(f"signal-{rank}-{now.timestamp()}", MINT, now, "fixture", "solana"),
        source_signal_id=None, source_timestamp=None, fetched_at=now,
        historical_bootstrap=False, raw_response_hash="fixture", fields=fields,
    )


class _PriceMonitor:
    def __init__(self) -> None:
        self.registered: set[str] = set()

    def register_position(self, mint: str):
        self.registered.add(mint)
        return SimpleNamespace(mint=mint, stage="pump_bonding_curve", primary_account="Curve111", account_addresses=("Curve111",))

    def unregister_missing(self, mints: set[str]) -> None:
        self.registered.intersection_update(mints)


class _QuoteProvider:
    def quote_candidate(self, mint, amount, context):
        now = NOW
        buy = ExecutableQuote("buy", mint, "buy", amount, Decimal("100"), None, Decimal("1"), now, 0, route=("pump",), provider="pump", quote_source="pump")
        sell = ExecutableQuote("sell", mint, "sell", Decimal("100"), Decimal("0.00099"), None, Decimal("1"), now, 0, route=("pump",), provider="pump", quote_source="pump")
        return buy, sell, None

    def quote(self, mint, side, amount):
        return ExecutableQuote("exit", mint, side, amount, Decimal("0.001"), None, Decimal("1"), NOW, 0, route=("pump",))

    def status(self):
        return {"state": "HEALTHY"}


class SolSurvivorTests(unittest.TestCase):
    def _engine(self, root: Path, config: SolSurvivorReversalConfig | None = None) -> SolSurvivorReversalEngine:
        connection = initialize_database(root / "runtime.db")
        return SolSurvivorReversalEngine(
            connection=connection,
            store=RuntimeStore(connection, "paper"),
            health=HealthRegistry(root / "health.json"),
            audit=JsonlAuditWriter(root / "events.jsonl"),
            quote_provider=_QuoteProvider(),
            price_monitor=_PriceMonitor(),
            controls=RuntimeControl(root / "control.json"),
            config=config or SolSurvivorReversalConfig(data_quality_start_at=NOW),
        )

    def test_frozen_sol_config_and_safety_self_check(self) -> None:
        with patch.dict(os.environ, {"ALLOW_LIVE_TRADING": "false", "EXECUTION_PROVIDER": "paper", "SOL_SIGNING_ENABLED": "false", "SOL_BROADCAST_ENABLED": "false"}, clear=False):
            config = SolSurvivorReversalConfig.from_env()
            self.assertEqual(config.strategy_name, "MEME_SURVIVOR_REVERSAL_SOL_V1")
            self.assertEqual(config.max_candidate_age_sec, 3600)
            self.assertEqual(config.min_active_mc_usd, Decimal("15000"))
            self.assertEqual(config.min_active_liquidity_usd, Decimal("5000"))
            self.assertEqual(config.universe_min_mc_usd, Decimal("15000"))
            self.assertEqual(config.universe_min_liquidity_usd, Decimal("5000"))
            self.assertEqual(config.min_active_holders, 30)
            self.assertEqual(config.universe_min_holders, 30)
            self.assertEqual(config.drawdown_min_pct, Decimal("20"))
            self.assertEqual(config.drawdown_max_pct, Decimal("50"))
            self.assertEqual(config.universe_max_mc_usd, Decimal("3000000"))
            self.assertEqual(config.max_candidate_price_usd, Decimal("0.0001"))
            self.assertEqual(config.self_check()["status"], "OK")

    def test_live_switch_is_rejected(self) -> None:
        with patch.dict(os.environ, {"ALLOW_LIVE_TRADING": "true"}, clear=False):
            with self.assertRaises(ValueError):
                SolSurvivorReversalConfig().validate()

    def test_lifecycle_merges_and_preserves_case_first_seen_and_ath(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            engine.on_records([_record(NOW, 10, "0.00001")], NOW)
            engine.on_records([_record(NOW + timedelta(seconds=10), 20, "0.00002")], NOW + timedelta(seconds=10))
            engine.on_records([_record(NOW + timedelta(seconds=20), 30, "0.000015")], NOW + timedelta(seconds=20))
            candidate = engine._candidates[MINT]
            self.assertEqual(candidate.mint, MINT)
            self.assertEqual(candidate.first_seen_at, NOW)
            self.assertEqual(candidate.latest_lifecycle, "MEME_MIGRATED")
            self.assertEqual(candidate.ath_price_usd, Decimal("0.00002"))

    def test_candidate_history_is_usable_only_with_complete_pre_candidate_coverage(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            for seconds in range(0, 180, 9):
                engine.on_records([_record(NOW + timedelta(seconds=seconds), 10, "0.00001")], NOW + timedelta(seconds=seconds))
            engine.on_records([_record(NOW + timedelta(seconds=201), 20, "0.000018")], NOW + timedelta(seconds=201))
            candidate = engine._candidates[MINT]
            self.assertEqual(candidate.data_quality_cohort, "NATIVE_FRESH")
            self.assertEqual(candidate.price_history_status, "LIVE_USABLE")
            self.assertGreaterEqual(candidate.price_samples_before_candidate, 20)
            frozen_ath = candidate.ath_before_candidate_price_usd
            engine.on_records([_record(NOW + timedelta(seconds=220), 20, "0.00009")], NOW + timedelta(seconds=220))
            self.assertEqual(candidate.ath_before_candidate_price_usd, frozen_ath)

    def test_first_discovery_history_allows_real_samples_without_gap_gate(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            candidate = SurvivorCandidate(MINT, "X", NOW, NOW, data_quality_cohort="NATIVE_FRESH")
            candidate.first_price_at = NOW
            candidate_at = NOW + timedelta(seconds=600)
            samples = [
                FlowSample(NOW + timedelta(seconds=offset), price_usd=Decimal("0.00001"))
                for offset in tuple(range(0, 131, 7)) + (130, 577, 583)
            ]
            engine._freeze_candidate_history(candidate, samples, candidate_at)
            self.assertEqual(candidate.price_coverage_before_candidate_seconds, Decimal("583"))
            self.assertEqual(candidate.max_history_gap_seconds, Decimal("447"))
            self.assertEqual(candidate.price_history_status, "LIVE_USABLE")

    def test_discovery_price_at_candidate_time_is_a_valid_pullback_baseline(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            candidate = SurvivorCandidate(
                MINT, "X", NOW, NOW,
                first_seen_price_usd=Decimal("0.00001"),
                first_seen_price_source="MEME_RUSH",
            )
            engine._freeze_candidate_history(candidate, [], NOW)
            self.assertEqual(candidate.first_price_at, NOW)
            self.assertEqual(candidate.price_history_status, "LIVE_USABLE")
            self.assertIsNone(engine._history_block_reason(candidate))

    def test_active_candidate_enters_paper_without_pullback_audit_or_flow_gates(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            candidate = SurvivorCandidate(
                MINT, "X", NOW - timedelta(minutes=4), NOW,
                state="ACTIVE_CANDIDATE",
                current_price_usd=Decimal("0.00001"),
                current_price_native=Decimal("0.0000001"),
                candidate_eligible=True,
                active_candidate=True,
                price_status="VALID",
            )
            engine._candidates[MINT] = candidate
            engine._evaluate_candidate(candidate, NOW)
            self.assertTrue(candidate.paper_buy)
            self.assertEqual(engine._paper_buy_total, 1)

    def test_preexisting_token_can_use_locally_observed_history(self) -> None:
        engine = object.__new__(SolSurvivorReversalEngine)
        engine.config = SolSurvivorReversalConfig()
        candidate = SurvivorCandidate(MINT, "X", NOW, NOW, data_quality_cohort="BOOTSTRAP_EXISTING")
        candidate.price_history_status = "LIVE_USABLE"
        candidate.price_history_quality = "GOOD"
        self.assertIsNone(engine._history_block_reason(candidate))

    def test_native_fresh_requires_create_time_new_rank_and_first_price(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            engine.on_records([_record(NOW + timedelta(seconds=5), 10, created_at=NOW)], NOW + timedelta(seconds=5))
            self.assertEqual(engine._candidates[MINT].data_quality_cohort, "NATIVE_FRESH")
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            engine.on_records([_record(NOW + timedelta(minutes=5), 10, created_at=NOW - timedelta(hours=1))], NOW + timedelta(minutes=5))
            self.assertEqual(engine._candidates[MINT].data_quality_cohort, "BOOTSTRAP_EXISTING")
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            engine.on_records([_record(NOW + timedelta(minutes=5), 20, created_at=NOW + timedelta(seconds=1))], NOW + timedelta(minutes=5))
            self.assertEqual(engine._candidates[MINT].data_quality_cohort, "BOOTSTRAP_EXISTING")

    def test_sol_entry_gates_do_not_use_lp_mc_ratio(self) -> None:
        engine = object.__new__(SolSurvivorReversalEngine)
        engine.config = SolSurvivorReversalConfig()
        engine._wss_stale = False
        candidate = SurvivorCandidate(MINT, "X", NOW - timedelta(minutes=4), NOW, market_cap_usd=Decimal("1000000"), liquidity_usd=Decimal("40000"), holders=300)
        self.assertIsNone(engine._universe_reason(candidate, NOW))

    def test_candidate_age_has_a_one_hour_upper_limit(self) -> None:
        engine = object.__new__(SolSurvivorReversalEngine)
        engine.config = SolSurvivorReversalConfig()
        candidate = SurvivorCandidate(
            MINT, "X", NOW - timedelta(seconds=3601), NOW,
            market_cap_usd=Decimal("20000"), liquidity_usd=Decimal("6000"), holders=30,
            current_price_usd=Decimal("0.00001"), price_status="VALID",
        )
        self.assertFalse(engine._candidate_eligible(candidate, NOW))
        self.assertEqual(engine._universe_reason(candidate, NOW), "AGE_ABOVE_3600S")

    def test_configured_usd_gate_labels_do_not_report_stale_thresholds(self) -> None:
        self.assertEqual(SolSurvivorReversalEngine._usd_gate_label(Decimal("15000")), "15K")
        self.assertEqual(SolSurvivorReversalEngine._usd_gate_label(Decimal("3000000")), "3M")

    def test_holder_rejection_uses_the_configured_threshold(self) -> None:
        engine = object.__new__(SolSurvivorReversalEngine)
        engine.config = SolSurvivorReversalConfig(min_active_holders=100, universe_min_holders=100)
        engine._wss_stale = False
        candidate = SurvivorCandidate(
            MINT, "X", NOW - timedelta(minutes=4), NOW,
            market_cap_usd=Decimal("20000"), liquidity_usd=Decimal("20000"), holders=99,
        )
        candidate.last_rejection = "FILTERED:SOL_HOLDERS_BELOW_200"
        self.assertEqual(engine._discovery_state(candidate, NOW), "LIGHT_TRACKING")
        self.assertEqual(candidate.last_rejection, "FILTERED:SOL_HOLDERS_BELOW_100")
        self.assertEqual(engine._universe_reason(candidate, NOW), "SOL_HOLDERS_BELOW_100")

    def test_first_price_at_cap_is_permanently_excluded(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            engine.on_records([_record(NOW - timedelta(minutes=4), 10, "0.0001")], NOW)
            self.assertNotIn(MINT, engine._candidates)
            self.assertIn(MINT, engine._excluded_mints)
            self.assertEqual(engine._paper_buy_total, 0)

    def test_first_discovery_price_at_or_above_cap_is_permanently_excluded(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            engine.on_records([_record(NOW, 10, "0.0006")], NOW)
            self.assertNotIn(MINT, engine._candidates)
            self.assertIn(MINT, engine._excluded_mints)
            exclusion = engine.connection.execute(
                "SELECT reason FROM survivor_exclusions WHERE mint=?", (MINT,)
            ).fetchone()
            self.assertEqual(exclusion[0], "SOL_FIRST_DISCOVERY_PRICE_AT_OR_ABOVE_0_0001")
            self.assertIsNone(engine.connection.execute(
                "SELECT 1 FROM survivor_candidates WHERE mint=?", (MINT,)
            ).fetchone())

            # A subsequent cheaper quote cannot revive the token.
            engine.on_records([_record(NOW + timedelta(seconds=30), 10, "0.00001")], NOW + timedelta(seconds=30))
            self.assertNotIn(MINT, engine._candidates)
            self.assertIsNone(engine.connection.execute(
                "SELECT 1 FROM survivor_candidates WHERE mint=?", (MINT,)
            ).fetchone())

    def test_source_switch_gap_blocks_fake_pullback(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            engine.on_records([_record(NOW, 10, "0.00001")], NOW)
            candidate = engine._candidates[MINT]
            candidate.native_token_price_usd = Decimal("100")
            engine.on_solana_price(SolanaObservedPrice(MINT, NOW + timedelta(seconds=1), Decimal("0.00000005"), "pool_wss_indicative", "pump_bonding_curve", "Curve111", 1))
            self.assertEqual(candidate.last_rejection, "PRICE_SOURCE_SWITCH_UNSAFE")
            self.assertFalse(getattr(candidate, "source_switch_safe"))

    def test_verified_swap_direction_and_unknown(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            engine.on_records([_record(NOW, 10)], NOW)
            self.assertEqual(engine.ingest_verified_swap_deltas(MINT, NOW, token_delta=Decimal("10"), quote_delta_sol=Decimal("-1")), "BUY")
            self.assertEqual(engine.ingest_verified_swap_deltas(MINT, NOW, token_delta=Decimal("-10"), quote_delta_sol=Decimal("1")), "SELL")
            self.assertEqual(engine.ingest_verified_swap_deltas(MINT, NOW, token_delta=Decimal("1"), quote_delta_sol=Decimal("1")), "SWAP_DIRECTION_UNKNOWN")

    def test_flow_ratio_uses_rolling_minute(self) -> None:
        engine = object.__new__(SolSurvivorReversalEngine)
        candidate = SurvivorCandidate(MINT, "X", NOW, NOW)
        candidate.flows.extend((FlowSample(NOW - timedelta(seconds=61), Decimal("9"), Decimal("1"), 9, 1), FlowSample(NOW, Decimal("3"), Decimal("2"), 3, 2)))
        self.assertEqual(SolSurvivorReversalEngine._flow_1m(engine, candidate, NOW), (Decimal("3"), Decimal("2"), 3, 2))

    def test_wss_stale_blocks_entry_but_idle_does_not(self) -> None:
        engine = object.__new__(SolSurvivorReversalEngine)
        engine.config = SolSurvivorReversalConfig()
        candidate = SurvivorCandidate(MINT, "X", NOW - timedelta(minutes=4), NOW, market_cap_usd=Decimal("300000"), liquidity_usd=Decimal("50000"), holders=350)
        engine._wss_stale = True
        self.assertEqual(engine._universe_reason(candidate, NOW), "SOLANA_WSS_STALE")
        engine._wss_stale = False
        self.assertIsNone(engine._universe_reason(candidate, NOW))

    def test_subscription_reasons_are_bounded(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            engine.on_records([_record(NOW - timedelta(minutes=4), 10)], NOW)
            candidate = engine._candidates[MINT]
            candidate.state = "ACTIVE_CANDIDATE"
            candidate.candidate_eligible = True
            engine._sync_bindings()
            details = engine.subscription_details()
            self.assertEqual(details[0]["reason"], "ACTIVE_CANDIDATE")

    def test_quote_router_rejects_expired_and_missing_impact(self) -> None:
        config = SolSurvivorReversalConfig()
        quote = ExecutableQuote("q", MINT, "buy", Decimal("1"), Decimal("1"), None, None, NOW - timedelta(seconds=4), 4000, route=("x",))
        provider = SimpleNamespace(quote=lambda *args: quote)
        router = SolSurvivorQuoteRouter(None, provider, config)
        _, _, error = router.quote_candidate(MINT, Decimal("0.001"), {})
        self.assertIn(error, {"BUY_QUOTE_EXPIRED", "BUY_PRICE_IMPACT_UNAVAILABLE"})

    def test_quote_router_keeps_monitoring_alive_when_token_amount_is_not_encodable(self) -> None:
        router = SolSurvivorQuoteRouter(
            None,
            SimpleNamespace(quote=lambda *args: (_ for _ in ()).throw(ValueError("input quantity cannot be represented in token base units"))),
            SolSurvivorReversalConfig(),
        )
        self.assertIsNone(router.quote(MINT, "sell", Decimal("1.0000000000000000001")))
        self.assertEqual(router.last_error, "SOL_QUOTE_INVALID_TOKEN_QUANTITY")

    def test_mint_24h_cooldown_is_per_mint(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            candidate = SurvivorCandidate(MINT, "X", NOW, NOW, current_price_native=Decimal("0.00001"), data_quality_cohort="NATIVE_FRESH")
            candidate.price_history_status = "LIVE_USABLE"
            candidate.price_history_quality = "GOOD"
            engine._try_buy(candidate, NOW)
            for position in engine._positions.values():
                position.status = "CLOSED"
            second = SurvivorCandidate(MINT, "X", NOW, NOW, current_price_native=Decimal("0.00001"), data_quality_cohort="NATIVE_FRESH")
            second.price_history_status = "LIVE_USABLE"
            second.price_history_quality = "GOOD"
            engine._try_buy(second, NOW + timedelta(hours=1))
            self.assertEqual(second.last_rejection, "MINT_24H_COOLDOWN")

    def test_sol_allows_twenty_open_positions_then_blocks_the_next(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            for index in range(20):
                mint = f"mint-{index}"
                candidate = SurvivorCandidate(mint, "X", NOW, NOW, current_price_native=Decimal("0.00001"))
                engine._try_buy(candidate, NOW)
            extra = SurvivorCandidate("mint-extra", "X", NOW, NOW, current_price_native=Decimal("0.00001"))
            engine._try_buy(extra, NOW)
            self.assertEqual(sum(position.status == "OPEN" for position in engine._positions.values()), 20)
            self.assertEqual(extra.last_rejection, "MAX_OPEN_POSITIONS_REACHED")

    def test_sol_staged_exits_use_initial_position_quantities(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            candidate = SurvivorCandidate(MINT, "X", NOW, NOW, current_price_native=Decimal("0.00001"), holders=200)
            engine._candidates[MINT] = candidate
            engine._try_buy(candidate, NOW)
            position = next(iter(engine._positions.values()))

            candidate.current_price_native = Decimal("0.000015")
            engine._evaluate_positions(NOW + timedelta(seconds=1))
            self.assertTrue(position.tp1)
            self.assertEqual(position.remaining_quantity_token, Decimal("70"))

            candidate.current_price_native = Decimal("0.00002")
            engine._evaluate_positions(NOW + timedelta(seconds=2))
            self.assertTrue(position.tp2)
            self.assertEqual(position.remaining_quantity_token, Decimal("40"))

            candidate.current_price_native = Decimal("0.00003")
            engine._evaluate_positions(NOW + timedelta(seconds=3))
            self.assertEqual(position.status, "CLOSED")
            self.assertEqual(position.remaining_quantity_token, Decimal("0"))

    def test_sol_price_gap_through_all_targets_closes_in_one_evaluation(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            candidate = SurvivorCandidate(MINT, "X", NOW, NOW, current_price_native=Decimal("0.00001"), holders=200)
            engine._candidates[MINT] = candidate
            engine._try_buy(candidate, NOW)
            position = next(iter(engine._positions.values()))
            candidate.current_price_native = Decimal("0.00003")
            engine._evaluate_positions(NOW + timedelta(seconds=1))
            self.assertTrue(position.tp1)
            self.assertTrue(position.tp2)
            self.assertEqual(position.status, "CLOSED")
            self.assertEqual(position.remaining_quantity_token, Decimal("0"))

    def test_sol_holder_drop_within_two_minutes_closes_position(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            candidate = SurvivorCandidate(MINT, "X", NOW, NOW, current_price_native=Decimal("0.00001"), holders=200)
            engine._candidates[MINT] = candidate
            engine._try_buy(candidate, NOW)
            position = next(iter(engine._positions.values()))
            engine._record_holder_observation(candidate, NOW)
            candidate.holders = 160
            engine._record_holder_observation(candidate, NOW + timedelta(seconds=119))
            engine._evaluate_positions(NOW + timedelta(seconds=119))
            self.assertEqual(position.status, "CLOSED")
            row = engine.connection.execute("SELECT exit_reason FROM survivor_positions WHERE position_id=?", (position.position_id,)).fetchone()
            self.assertEqual(row[0], "HOLDER_DROP_20PCT_2M")

    def test_sol_normal_stop_loss_is_eighty_percent(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            candidate = SurvivorCandidate(MINT, "X", NOW, NOW, current_price_native=Decimal("0.00001"))
            engine._candidates[MINT] = candidate
            engine._try_buy(candidate, NOW)
            position = next(iter(engine._positions.values()))
            candidate.current_price_native = Decimal("0.000002")
            engine._evaluate_positions(NOW + timedelta(seconds=1))
            self.assertEqual(position.status, "CLOSED")
            row = engine.connection.execute("SELECT exit_reason FROM survivor_positions WHERE position_id=?", (position.position_id,)).fetchone()
            self.assertEqual(row[0], "HARD_STOP_80PCT")

    def test_stale_price_closes_only_from_a_read_only_sell_quote(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            candidate = SurvivorCandidate(MINT, "X", NOW, NOW, current_price_native=Decimal("0.00001"))
            candidate.price_updated_at = NOW
            engine._candidates[MINT] = candidate
            engine._try_buy(candidate, NOW)
            position = next(iter(engine._positions.values()))

            engine._evaluate_positions(NOW + timedelta(seconds=engine.config.idle_ttl_sec + 1))

            self.assertEqual(position.status, "CLOSED")
            row = engine.connection.execute(
                "SELECT exit_reason FROM survivor_positions WHERE position_id=?", (position.position_id,)
            ).fetchone()
            self.assertEqual(row[0], "PRICE_DATA_EXPIRED_EXIT")

    def test_sol_trade_entry_and_exit_context_is_frozen(self) -> None:
        with TemporaryDirectory() as td:
            engine = self._engine(Path(td))
            candidate = SurvivorCandidate(
                MINT, "X", NOW, NOW, current_price_native=Decimal("0.00001"),
                current_price_usd=Decimal("0.00002"), holders=30,
                market_cap_usd=Decimal("15000"), liquidity_usd=Decimal("5000"),
            )
            engine._candidates[MINT] = candidate
            engine._try_buy(candidate, NOW)
            position = next(iter(engine._positions.values()))
            candidate.current_price_usd = Decimal("0.00004")
            candidate.holders = 80
            candidate.market_cap_usd = Decimal("22000")
            candidate.liquidity_usd = Decimal("8000")
            engine._close_position(position, "TEST_EXIT", NOW + timedelta(minutes=1))
            row = engine.connection.execute(
                "SELECT entry_price_usd,exit_price_native,exit_price_usd,entry_holders,exit_holders,entry_market_cap_usd,exit_market_cap_usd,entry_liquidity_usd,exit_liquidity_usd FROM survivor_positions WHERE position_id=?",
                (position.position_id,),
            ).fetchone()
            self.assertEqual(tuple(row), ("0.00002", "0.00001", "0.00004", 30, 80, "15000", "22000", "5000", "8000"))

    def test_database_isolated_path_and_state_key(self) -> None:
        with TemporaryDirectory() as td:
            root = Path(td)
            engine = self._engine(root)
            row = engine.connection.execute("SELECT value_json FROM runtime_state WHERE mode='paper' AND state_key='survivor_reversal_sol_v1'").fetchone()
            self.assertIsNotNone(row)
            self.assertNotEqual(root / "runtime.db", Path("data/bsc/paper/runtime.db"))


if __name__ == "__main__":
    unittest.main()
