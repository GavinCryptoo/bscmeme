from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

from meme_system.adapters.protocols import ExecutableQuote
from meme_system.adapters.replay import ReplayExit, ReplayRunner, ReplayStep
from meme_system.domain.models import EntryFeatures, ShadowExitFeatures, Signal
from meme_system.engines.ledger import SimulationLedger
from meme_system.engines.simulation import DeterministicSimulation
from meme_system.strategies.baseline import (
    BaselineConfig,
    BaselineStrategy,
    SOLANA_SHADOW_MIN_LIQUIDITY_USD,
    bsc_baseline_config,
)
from meme_system.storage.database import initialize_database
from meme_system.storage.queries import LedgerQueries


NOW = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)


def make_signal(signal_id: str = "sig-1", mint: str = "MintA") -> Signal:
    return Signal(signal_id=signal_id, mint=mint, observed_at=NOW, source="fixture")


def make_buy_quote(mint: str = "MintA", quote_id: str = "buy-1") -> ExecutableQuote:
    return ExecutableQuote(
        quote_id=quote_id,
        mint=mint,
        side="buy",
        input_quantity=Decimal("0.001"),
        output_quantity=Decimal("1000"),
        route_fee=Decimal("0.000001"),
        price_impact_pct=Decimal("1"),
        quoted_at=NOW,
        age_ms=20,
    )


def make_sell_quote(
    output_sol: str,
    mint: str = "MintA",
    quote_id: str = "sell-1",
) -> ExecutableQuote:
    return ExecutableQuote(
        quote_id=quote_id,
        mint=mint,
        side="sell",
        input_quantity=Decimal("1000"),
        output_quantity=Decimal(output_sol),
        route_fee=Decimal("0.000001"),
        price_impact_pct=Decimal("2"),
        quoted_at=NOW,
        age_ms=25,
    )


def make_entry_features(
    buy_quote: ExecutableQuote | None = None,
    sell_quote: ExecutableQuote | None = None,
    token_name: str = "Alpha",
) -> EntryFeatures:
    return EntryFeatures(
        token_age_sec=30,
        unique_buyers_15s=8,
        buy_sell_count_ratio_15s=Decimal("2.0"),
        net_buy_15s=Decimal("5"),
        flow_windows_non_negative=(True, True),
        creator_confirmed_sold=False,
        buy_quote=buy_quote or make_buy_quote(),
        sell_quote=sell_quote or make_sell_quote("0.001"),
        evaluated_at=NOW,
        token_name=token_name,
        soft_features={"holders": 21, "market_cap": None},
        holders=21,
        market_cap_usd=Decimal("1000"),
        liquidity_usd=Decimal("100"),
    )


class SimulationTests(unittest.TestCase):
    def test_bsc_capital_profile_is_isolated_from_solana(self) -> None:
        bsc_config = bsc_baseline_config()
        solana_config = BaselineConfig()
        self.assertEqual(bsc_config.initial_virtual_balance_sol, Decimal("0.1"))
        self.assertEqual(bsc_config.position_size_sol, Decimal("0.01"))
        self.assertEqual(solana_config.initial_virtual_balance_sol, Decimal("1"))
        self.assertEqual(solana_config.position_size_sol, Decimal("0.001"))

    def test_updated_baseline_risk_and_quote_limits_are_frozen(self) -> None:
        config = DeterministicSimulation("paper").strategy.config
        self.assertEqual(config.max_buy_price_impact_pct, Decimal("10"))
        self.assertEqual(config.max_immediate_exit_impact_pct, Decimal("15"))
        self.assertEqual(config.initial_virtual_balance_sol, Decimal("1"))
        self.assertEqual(config.position_size_sol, Decimal("0.001"))
        self.assertEqual(config.max_open_positions, 50)
        self.assertEqual(config.pause_new_entries_after_large_losses, 50)
        self.assertEqual(config.observation_delay_sec, 15)
        self.assertEqual(config.shadow_holders_drop_pct, Decimal("0.10"))
        self.assertEqual(config.shadow_liquidity_drop_pct, Decimal("0.15"))
        self.assertEqual(config.min_market_cap_usd, Decimal("1000"))
        self.assertEqual(config.min_liquidity_usd, Decimal("100"))
        self.assertEqual(config.large_loss_threshold_pct, Decimal("-0.40"))
        self.assertEqual(config.daily_full_loss_sol_limit, Decimal("0.01"))
        self.assertTrue(config.require_holders_non_decreasing_after_observation)

    def test_daily_full_loss_limit_is_measured_in_sol_and_resets_by_utc_date(self) -> None:
        ledger = SimulationLedger("paper")
        ledger.record_full_loss(NOW, Decimal("0.01"))
        engine = DeterministicSimulation("paper", ledger=ledger)
        blocked = engine.process_entry(
            make_signal("same-day"),
            make_entry_features(),
            candidate_id="same-day-candidate",
            position_id="same-day-position",
        )
        self.assertIn("daily_full_loss_limit", blocked.decision.failed_reason_codes)

        next_day = NOW + timedelta(days=1)
        next_day_result = engine.process_entry(
            make_signal("next-day", "MintB"),
            replace(make_entry_features(), evaluated_at=next_day),
            candidate_id="next-day-candidate",
            position_id="next-day-position",
        )
        self.assertNotIn("daily_full_loss_limit", next_day_result.decision.failed_reason_codes)

    def test_soft_features_are_recorded_without_rejecting_paper_entry(self) -> None:
        engine = DeterministicSimulation("paper")
        result = engine.process_entry(
            make_signal(),
            make_entry_features(),
            candidate_id="candidate-1",
            position_id="position-1",
        )
        self.assertTrue(result.decision.accepted)
        self.assertIsNotNone(result.position)
        self.assertEqual(result.candidate.soft_features, {"holders": 21, "market_cap": None})
        self.assertIn(
            "same_name_cooldown",
            {check.name for check in result.candidate.checks},
        )

    def test_unavailable_binance_fields_are_recorded_but_do_not_reject_entry(self) -> None:
        engine = DeterministicSimulation("paper")
        features = replace(
            make_entry_features(),
            token_age_sec=None,
            unique_buyers_15s=None,
            buy_sell_count_ratio_15s=None,
            net_buy_15s=None,
            flow_windows_non_negative=(None, None),
            creator_confirmed_sold=None,
        )
        result = engine.process_entry(
            make_signal(),
            features,
            candidate_id="candidate-unavailable-fields",
            position_id="position-unavailable-fields",
        )
        self.assertTrue(result.decision.accepted)
        self.assertIsNotNone(result.position)
        self.assertEqual(
            result.decision.unavailable_reason_codes,
            (
                "token_age_unavailable",
                "unique_buyers_unavailable",
                "buy_sell_ratio_unavailable",
                "net_buy_unavailable",
                "flow_window_unavailable",
                "creator_sell_unavailable",
            ),
        )
        self.assertIn("token_age_unavailable", result.candidate.filter_reason)

    def test_market_cap_is_a_hard_entry_filter(self) -> None:
        engine = DeterministicSimulation("paper")
        missing = engine.process_entry(
            make_signal("market-cap-missing"),
            replace(make_entry_features(), market_cap_usd=None),
            candidate_id="candidate-market-cap-missing",
            position_id="position-market-cap-missing",
        )
        self.assertFalse(missing.decision.accepted)
        self.assertIn("market_cap_unavailable", missing.decision.failed_reason_codes)

        below_min = engine.process_entry(
            make_signal("market-cap-low", "MintB"),
            replace(make_entry_features(), market_cap_usd=Decimal("999.99")),
            candidate_id="candidate-market-cap-low",
            position_id="position-market-cap-low",
        )
        self.assertFalse(below_min.decision.accepted)
        self.assertIn("market_cap_below_min", below_min.decision.failed_reason_codes)

        missing = engine.process_entry(
            make_signal("liquidity-missing", "MintC"),
            replace(make_entry_features(), liquidity_usd=None),
            candidate_id="candidate-liquidity-missing",
            position_id="position-liquidity-missing",
        )
        self.assertFalse(missing.decision.accepted)
        self.assertIn("liquidity_unavailable", missing.decision.failed_reason_codes)

        below_min_liquidity = engine.process_entry(
            make_signal("liquidity-low", "MintD"),
            replace(make_entry_features(), liquidity_usd=Decimal("99.99")),
            candidate_id="candidate-liquidity-low",
            position_id="position-liquidity-low",
        )
        self.assertFalse(below_min_liquidity.decision.accepted)
        self.assertIn("liquidity_below_min", below_min_liquidity.decision.failed_reason_codes)

    def test_shadow_liquidity_threshold_is_5000_without_changing_paper_default(self) -> None:
        paper = DeterministicSimulation("paper")
        shadow = DeterministicSimulation(
            "shadow",
            strategy=BaselineStrategy(
                replace(
                    paper.strategy.config,
                    min_liquidity_usd=SOLANA_SHADOW_MIN_LIQUIDITY_USD,
                )
            ),
        )
        self.assertEqual(paper.strategy.config.min_liquidity_usd, Decimal("100"))
        self.assertEqual(shadow.strategy.config.min_liquidity_usd, Decimal("5000"))

        rejected = shadow.process_entry(
            make_signal("shadow-liquidity-4999", "MintShadow4999"),
            replace(
                make_entry_features(
                    buy_quote=make_buy_quote("MintShadow4999"),
                    sell_quote=make_sell_quote("0.001", mint="MintShadow4999"),
                ),
                liquidity_usd=Decimal("4999.99"),
            ),
            candidate_id="candidate-shadow-liquidity-4999",
            position_id="position-shadow-liquidity-4999",
        )
        self.assertFalse(rejected.decision.accepted)
        self.assertIn("liquidity_below_min", rejected.decision.failed_reason_codes)

        accepted = shadow.process_entry(
            make_signal("shadow-liquidity-5000", "MintShadow5000"),
            replace(
                make_entry_features(
                    buy_quote=make_buy_quote("MintShadow5000"),
                    sell_quote=make_sell_quote("0.001", mint="MintShadow5000"),
                ),
                liquidity_usd=Decimal("5000"),
            ),
            candidate_id="candidate-shadow-liquidity-5000",
            position_id="position-shadow-liquidity-5000",
        )
        self.assertTrue(accepted.decision.accepted)

    def test_holders_is_a_hard_entry_filter(self) -> None:
        engine = DeterministicSimulation("paper")
        missing = engine.process_entry(
            make_signal("holders-missing"),
            replace(make_entry_features(), holders=None),
            candidate_id="candidate-holders-missing",
            position_id="position-holders-missing",
        )
        self.assertFalse(missing.decision.accepted)
        self.assertIn("holders_unavailable", missing.decision.failed_reason_codes)

        below_min = engine.process_entry(
            make_signal("holders-low", "MintB"),
            replace(make_entry_features(), holders=20),
            candidate_id="candidate-holders-low",
            position_id="position-holders-low",
        )
        self.assertFalse(below_min.decision.accepted)
        self.assertIn("holders_below_min", below_min.decision.failed_reason_codes)

        accepted = engine.process_entry(
            make_signal("holders-min", "MintA"),
            replace(make_entry_features(), holders=21),
            candidate_id="candidate-holders-min",
            position_id="position-holders-min",
        )
        self.assertTrue(accepted.decision.accepted)

    def test_solana_holder_threshold_can_be_inclusive(self) -> None:
        strategy = BaselineStrategy(BaselineConfig(min_holders=5, min_holders_inclusive=True))
        engine = DeterministicSimulation("paper", strategy=strategy)
        accepted = engine.process_entry(
            make_signal("holders-inclusive", "MintInclusive"),
            replace(
                make_entry_features(
                    buy_quote=make_buy_quote("MintInclusive"),
                    sell_quote=make_sell_quote("0.001", mint="MintInclusive"),
                ),
                holders=5,
            ),
            candidate_id="candidate-holders-inclusive",
            position_id="position-holders-inclusive",
        )
        self.assertTrue(accepted.decision.accepted)

    def test_missing_price_impact_is_recorded_but_does_not_reject_valid_quotes(self) -> None:
        engine = DeterministicSimulation("paper")
        features = replace(
            make_entry_features(
                buy_quote=replace(make_buy_quote(), price_impact_pct=None),
                sell_quote=replace(make_sell_quote("0.001"), price_impact_pct=None),
            ),
        )
        result = engine.process_entry(
            make_signal(),
            features,
            candidate_id="candidate-missing-impact",
            position_id="position-missing-impact",
        )
        self.assertTrue(result.decision.accepted)
        self.assertIsNotNone(result.position)
        self.assertIn("buy_price_impact_unavailable", result.candidate.filter_reason)
        self.assertIn("sell_price_impact_unavailable", result.candidate.filter_reason)

    def test_zero_quote_output_remains_a_hard_failure(self) -> None:
        engine = DeterministicSimulation("paper")
        result = engine.process_entry(
            make_signal(),
            make_entry_features(buy_quote=replace(make_buy_quote(), output_quantity=Decimal("0"))),
            candidate_id="candidate-zero-output",
            position_id="position-zero-output",
        )
        self.assertFalse(result.decision.accepted)
        self.assertIn("buy_quote_output_unavailable", result.decision.failed_reason_codes)

    def test_missing_sell_route_rejects_entry(self) -> None:
        engine = DeterministicSimulation("paper")
        features = replace(make_entry_features(), sell_quote=None)
        result = engine.process_entry(
            make_signal(),
            features,
            candidate_id="candidate-1",
            position_id="position-1",
        )
        self.assertFalse(result.decision.accepted)
        self.assertIn("sell_quote_unavailable", result.decision.failed_reason_codes)
        self.assertIsNone(result.position)

    def test_expired_buy_quote_is_not_used(self) -> None:
        engine = DeterministicSimulation("paper")
        expired_buy = replace(
            make_buy_quote(),
            expires_at=NOW - timedelta(seconds=1),
        )
        result = engine.process_entry(
            make_signal(),
            make_entry_features(buy_quote=expired_buy),
            candidate_id="candidate-1",
            position_id="position-1",
        )
        self.assertFalse(result.decision.accepted)
        self.assertIn("quote_expired", result.decision.failed_reason_codes)
        self.assertIsNone(result.position)

    def test_paper_take_profit_uses_executable_quote_and_records_estimated_cost(self) -> None:
        engine = DeterministicSimulation("paper")
        entry = engine.process_entry(
            make_signal(),
            make_entry_features(),
            candidate_id="candidate-1",
            position_id="position-1",
        )
        result = engine.process_paper_exit(
            "position-1",
            NOW + timedelta(seconds=20),
            make_sell_quote("0.0012"),
            estimated_network_fee_sol=None,
            estimated_priority_fee_sol=None,
        )
        self.assertIsNotNone(entry.position)
        self.assertTrue(result.decision.triggered)
        self.assertEqual(result.decision.reason, "take_profit")
        self.assertEqual(result.decision.cost.gross_pnl_pct, Decimal("0.2"))
        self.assertTrue(result.decision.cost.net_pnl_is_estimated)
        self.assertEqual(result.closed_position.status, "CLOSED")
        self.assertEqual(engine.ledger.executions[-1].action, "exit")

    def test_solana_timeout_enters_exit_triggered_before_quote_is_available(self) -> None:
        engine = DeterministicSimulation("paper")
        engine.process_entry(
            make_signal("timeout-trigger"),
            make_entry_features(),
            candidate_id="timeout-trigger-candidate",
            position_id="timeout-trigger-position",
        )
        no_route = replace(
            make_sell_quote("0", quote_id="timeout-no-route"),
            route_available=False,
        )

        pending = engine.process_timeout_exit(
            "timeout-trigger-position",
            NOW + timedelta(seconds=600),
            no_route,
        )

        self.assertIsNone(pending.closed_position)
        self.assertEqual(
            engine.ledger.positions["timeout-trigger-position"].status,
            "EXIT_TRIGGERED",
        )
        self.assertEqual(pending.decision.reason, "max_hold_timeout")
        self.assertEqual(len(engine.ledger.lifecycle_events), 3)
        self.assertEqual(engine.ledger.lifecycle_events[-1].event_type, "EXIT_TRIGGERED")

    def test_solana_timeout_prefers_jupiter_quote_and_closes_at_deadline(self) -> None:
        engine = DeterministicSimulation("paper")
        engine.process_entry(
            make_signal("timeout-jupiter"),
            make_entry_features(),
            candidate_id="timeout-jupiter-candidate",
            position_id="timeout-jupiter-position",
        )
        result = engine.process_timeout_exit(
            "timeout-jupiter-position",
            NOW + timedelta(seconds=600),
            make_sell_quote("0.0009", quote_id="timeout-jupiter-quote"),
        )

        self.assertIsNotNone(result.closed_position)
        self.assertEqual(result.closed_position.closed_reason, "max_hold_timeout")
        self.assertEqual(
            (result.closed_position.closed_at - result.closed_position.opened_at).total_seconds(),
            600,
        )
        execution = engine.ledger.executions[-1]
        self.assertEqual(execution.pricing_mode, "executable_quote")
        self.assertTrue(execution.executable_quote)
        self.assertEqual(execution.exit_status, "closed")
        self.assertEqual(execution.pnl_status, "estimated")

    def test_solana_timeout_uses_indicative_fallback_by_610_seconds(self) -> None:
        engine = DeterministicSimulation("shadow")
        engine.process_entry(
            make_signal("timeout-fallback"),
            make_entry_features(),
            candidate_id="timeout-fallback-candidate",
            position_id="timeout-fallback-position",
        )
        no_route = replace(
            make_sell_quote("0", quote_id="timeout-fallback-no-route"),
            route_available=False,
        )
        engine.process_timeout_exit(
            "timeout-fallback-position",
            NOW + timedelta(seconds=600),
            no_route,
        )
        fallback = replace(
            make_sell_quote("0.0008", quote_id="timeout-fallback-indicative"),
            route_fee=None,
            price_impact_pct=None,
            provider="binance_web3",
            route=("binance_indicative",),
            executable_style=False,
            confidence="indicative",
        )
        result = engine.process_timeout_exit(
            "timeout-fallback-position",
            NOW + timedelta(seconds=610),
            None,
            fallback,
        )

        self.assertIsNotNone(result.closed_position)
        self.assertEqual(result.closed_position.closed_reason, "max_hold_timeout")
        execution = engine.ledger.executions[-1]
        self.assertFalse(execution.executable_quote)
        self.assertEqual(execution.pricing_mode, "indicative_timeout_fallback")
        self.assertEqual(execution.exit_status, "closed")
        self.assertEqual(execution.pnl_status, "estimated")
        self.assertTrue(execution.cost.net_pnl_is_estimated)
        self.assertLessEqual(
            (result.closed_position.closed_at - result.closed_position.opened_at).total_seconds(),
            610,
        )

    def test_solana_timeout_closes_with_unknown_valuation_when_no_price_exists(self) -> None:
        engine = DeterministicSimulation("paper")
        engine.process_entry(
            make_signal("timeout-unknown"),
            make_entry_features(),
            candidate_id="timeout-unknown-candidate",
            position_id="timeout-unknown-position",
        )
        engine.process_timeout_exit(
            "timeout-unknown-position",
            NOW + timedelta(seconds=600),
            None,
        )
        result = engine.process_timeout_exit(
            "timeout-unknown-position",
            NOW + timedelta(seconds=610),
            None,
        )

        self.assertIsNotNone(result.closed_position)
        self.assertEqual(result.closed_position.closed_reason, "max_hold_timeout")
        self.assertIsNone(result.decision.cost)
        execution = engine.ledger.executions[-1]
        self.assertEqual(execution.exit_status, "valuation_unavailable")
        self.assertEqual(execution.pnl_status, "unknown")
        self.assertFalse(execution.executable_quote)
        self.assertIsNone(execution.cost)

    def test_exit_holders_loader_runs_only_when_paper_exit_is_triggered(self) -> None:
        engine = DeterministicSimulation("paper")
        engine.process_entry(
            make_signal(),
            make_entry_features(),
            candidate_id="candidate-exit-holders",
            position_id="position-exit-holders",
        )
        loaded: list[int] = []

        not_triggered = engine.process_paper_exit(
            "position-exit-holders",
            NOW + timedelta(seconds=20),
            make_sell_quote("0.001"),
            exit_holders_loader=lambda: loaded.append(44) or 44,
        )
        self.assertFalse(not_triggered.decision.triggered)
        self.assertEqual(loaded, [])

        triggered = engine.process_paper_exit(
            "position-exit-holders",
            NOW + timedelta(seconds=21),
            make_sell_quote("0.0012"),
            exit_holders_loader=lambda: loaded.append(44) or 44,
        )
        self.assertTrue(triggered.decision.triggered)
        self.assertEqual(loaded, [44])
        self.assertEqual(triggered.closed_position.exit_holders, 44)

    def test_holder_drop_does_not_trigger_paper_or_shadow_exit(self) -> None:
        paper = DeterministicSimulation("paper")
        paper.process_entry(
            make_signal("sig-holder-drop"),
            make_entry_features(),
            candidate_id="candidate-holder-drop",
            position_id="position-holder-drop",
        )
        paper_result = paper.process_paper_exit(
            "position-holder-drop",
            NOW + timedelta(seconds=59),
            make_sell_quote("0.001"),
            exit_holders=20,
        )
        self.assertFalse(paper_result.decision.triggered)
        self.assertIsNone(paper_result.decision.reason)

        shadow = DeterministicSimulation("shadow")
        shadow.process_entry(
            make_signal("sig-shadow-holder-drop"),
            make_entry_features(),
            candidate_id="candidate-shadow-holder-drop",
            position_id="position-shadow-holder-drop",
        )
        shadow_result = shadow.process_shadow_exit(
            "position-shadow-holder-drop",
            ShadowExitFeatures(
                position_age_sec=30,
                return_pct=Decimal("0.01"),
                mfe_pct=Decimal("0.01"),
                recent_net_flow_negative=False,
                independent_buyer_growth_stopped=False,
                creator_sell_confident=False,
                buyer_growth_and_flow_slowed=False,
            ),
            NOW + timedelta(seconds=30),
            make_sell_quote("0.001"),
        )
        self.assertFalse(shadow_result.decision.triggered)
        self.assertIsNone(shadow_result.decision.reason)

    def test_shadow_market_structure_drop_triggers_only_shadow_exit(self) -> None:
        shadow = DeterministicSimulation("shadow")
        entry = shadow.process_entry(
            make_signal("sig-shadow-structure-drop"),
            make_entry_features(),
            candidate_id="candidate-shadow-structure-drop",
            position_id="position-shadow-structure-drop",
        )
        self.assertEqual(entry.position.entry_holders, 21)
        self.assertEqual(entry.position.entry_liquidity_usd, Decimal("100"))

        holders_drop = shadow.process_shadow_exit(
            "position-shadow-structure-drop",
            ShadowExitFeatures(
                position_age_sec=30,
                return_pct=Decimal("-0.02"),
                mfe_pct=Decimal("0"),
                recent_net_flow_negative=False,
                independent_buyer_growth_stopped=False,
                creator_sell_confident=False,
                buyer_growth_and_flow_slowed=False,
                holders=18,
                liquidity_usd=Decimal("100"),
            ),
            NOW + timedelta(seconds=30),
            make_sell_quote("0.00098"),
        )
        self.assertTrue(holders_drop.decision.triggered)
        self.assertEqual(holders_drop.decision.reason, "shadow_holders_drop_over_10pct")

        liquidity_shadow = DeterministicSimulation("shadow")
        liquidity_shadow.process_entry(
            make_signal("sig-shadow-liquidity-drop"),
            make_entry_features(),
            candidate_id="candidate-shadow-liquidity-drop",
            position_id="position-shadow-liquidity-drop",
        )
        liquidity_drop = liquidity_shadow.process_shadow_exit(
            "position-shadow-liquidity-drop",
            ShadowExitFeatures(
                position_age_sec=30,
                return_pct=Decimal("-0.02"),
                mfe_pct=Decimal("0"),
                recent_net_flow_negative=False,
                independent_buyer_growth_stopped=False,
                creator_sell_confident=False,
                buyer_growth_and_flow_slowed=False,
                holders=21,
                liquidity_usd=Decimal("84"),
            ),
            NOW + timedelta(seconds=30),
            make_sell_quote("0.00098"),
        )
        self.assertTrue(liquidity_drop.decision.triggered)
        self.assertEqual(liquidity_drop.decision.reason, "shadow_liquidity_drop_over_15pct")

    def test_bsc_shadow_metric_drop_exits_without_changing_paper_rules(self) -> None:
        bsc_features = replace(
            make_entry_features(),
            holders=100,
            liquidity_usd=Decimal("100"),
            pricing_mode="binance_indicative",
            executable_quote=False,
        )
        shadow = DeterministicSimulation(
            "shadow",
            strategy=BaselineStrategy(bsc_baseline_config()),
            pricing_mode="binance_indicative",
            executable_quote=False,
        )
        entry = shadow.process_entry(
            make_signal("bsc-shadow-metric-drop"),
            bsc_features,
            candidate_id="bsc-shadow-metric-candidate",
            position_id="bsc-shadow-metric-position",
        )
        self.assertTrue(entry.decision.accepted)
        self.assertEqual(entry.position.entry_liquidity_usd, Decimal("100"))

        holder_drop = shadow.process_shadow_exit(
            "bsc-shadow-metric-position",
            ShadowExitFeatures(
                position_age_sec=30,
                return_pct=Decimal("-0.01"),
                mfe_pct=Decimal("0"),
                recent_net_flow_negative=False,
                independent_buyer_growth_stopped=False,
                creator_sell_confident=False,
                buyer_growth_and_flow_slowed=False,
                holders=89,
                liquidity_usd=Decimal("100"),
            ),
            NOW + timedelta(seconds=30),
            make_sell_quote("0.00099"),
        )
        self.assertTrue(holder_drop.decision.triggered)
        self.assertEqual(holder_drop.decision.reason, "shadow_holders_drop_over_10pct")

        paper = DeterministicSimulation(
            "paper",
            strategy=BaselineStrategy(bsc_baseline_config()),
            pricing_mode="binance_indicative",
            executable_quote=False,
        )
        paper_entry = paper.process_entry(
            make_signal("bsc-paper-metric-drop"),
            bsc_features,
            candidate_id="bsc-paper-metric-candidate",
            position_id="bsc-paper-metric-position",
        )
        self.assertTrue(paper_entry.decision.accepted)
        paper_result = paper.process_paper_exit(
            "bsc-paper-metric-position",
            NOW + timedelta(seconds=30),
            make_sell_quote("0.01"),
            exit_holders=89,
        )
        self.assertFalse(paper_result.decision.triggered)

    def test_bsc_quote_lifecycle_binds_open_and_close_to_their_quotes(self) -> None:
        entry_quote_at = NOW + timedelta(seconds=7)
        evaluated_at = NOW + timedelta(seconds=20)
        exit_quote_at = NOW + timedelta(seconds=37)
        bsc_features = replace(
            make_entry_features(
                buy_quote=replace(make_buy_quote(), quoted_at=entry_quote_at),
                sell_quote=make_sell_quote("0.001"),
            ),
            evaluated_at=evaluated_at,
            holders=100,
            pricing_mode="binance_indicative",
            executable_quote=False,
        )
        engine = DeterministicSimulation(
            "paper",
            strategy=BaselineStrategy(bsc_baseline_config()),
            pricing_mode="binance_indicative",
            executable_quote=False,
        )
        entry = engine.process_entry(
            make_signal("bsc-quote-lifecycle"),
            bsc_features,
            candidate_id="bsc-quote-lifecycle-candidate",
            position_id="bsc-quote-lifecycle-position",
        )
        self.assertTrue(entry.decision.accepted)
        self.assertEqual(entry.position.opened_at, entry_quote_at)
        self.assertEqual(entry.position.entry_quote_at, entry_quote_at)
        self.assertEqual(entry.position.signal_observed_at, NOW)
        self.assertEqual(entry.position.evaluated_at, evaluated_at)

        result = engine.process_paper_exit(
            "bsc-quote-lifecycle-position",
            NOW + timedelta(seconds=40),
            replace(make_sell_quote("0.012"), quoted_at=exit_quote_at),
        )
        self.assertTrue(result.decision.triggered)
        self.assertEqual(result.closed_position.closed_at, exit_quote_at)
        self.assertEqual(result.closed_position.exit_quote_at, exit_quote_at)

    def test_bsc_shadow_inherits_paper_profit_loss_and_timeout_exits(self) -> None:
        bsc_features = replace(
            make_entry_features(),
            holders=100,
            liquidity_usd=Decimal("100"),
            pricing_mode="binance_indicative",
            executable_quote=False,
        )

        def make_shadow(position_id: str) -> DeterministicSimulation:
            engine = DeterministicSimulation(
                "shadow",
                strategy=BaselineStrategy(bsc_baseline_config()),
                pricing_mode="binance_indicative",
                executable_quote=False,
            )
            entry = engine.process_entry(
                make_signal(position_id, "MintA"),
                bsc_features,
                candidate_id=f"{position_id}-candidate",
                position_id=position_id,
            )
            self.assertTrue(entry.decision.accepted)
            return engine

        def features(age_sec: int, return_pct: str) -> ShadowExitFeatures:
            return ShadowExitFeatures(
                position_age_sec=age_sec,
                return_pct=Decimal(return_pct),
                mfe_pct=Decimal("0"),
                recent_net_flow_negative=False,
                independent_buyer_growth_stopped=False,
                creator_sell_confident=False,
                buyer_growth_and_flow_slowed=False,
                holders=100,
                liquidity_usd=Decimal("100"),
            )

        take_profit = make_shadow("bsc-shadow-paper-take-profit")
        result = take_profit.process_shadow_exit(
            "bsc-shadow-paper-take-profit",
            features(30, "0.20"),
            NOW + timedelta(seconds=30),
            make_sell_quote("0.012"),
        )
        self.assertTrue(result.decision.triggered)
        self.assertEqual(result.decision.reason, "take_profit")

        stop_loss = make_shadow("bsc-shadow-paper-stop-loss")
        result = stop_loss.process_shadow_exit(
            "bsc-shadow-paper-stop-loss",
            features(30, "-0.20"),
            NOW + timedelta(seconds=30),
            make_sell_quote("0.008"),
        )
        self.assertTrue(result.decision.triggered)
        self.assertEqual(result.decision.reason, "stop_loss")

        timeout = make_shadow("bsc-shadow-paper-timeout")
        result = timeout.process_shadow_exit(
            "bsc-shadow-paper-timeout",
            features(600, "0"),
            NOW + timedelta(seconds=600),
            make_sell_quote("0.01"),
        )
        self.assertTrue(result.decision.triggered)
        self.assertEqual(result.decision.reason, "max_hold_timeout")

    def test_solana_shadow_inherits_paper_profit_loss_and_timeout_exits(self) -> None:
        def make_shadow(position_id: str) -> DeterministicSimulation:
            engine = DeterministicSimulation("shadow")
            entry = engine.process_entry(
                make_signal(position_id),
                make_entry_features(),
                candidate_id=f"{position_id}-candidate",
                position_id=position_id,
            )
            self.assertTrue(entry.decision.accepted)
            return engine

        def features(age_sec: int, return_pct: str) -> ShadowExitFeatures:
            return ShadowExitFeatures(
                position_age_sec=age_sec,
                return_pct=Decimal(return_pct),
                mfe_pct=Decimal("0"),
                recent_net_flow_negative=False,
                independent_buyer_growth_stopped=False,
                creator_sell_confident=False,
                buyer_growth_and_flow_slowed=False,
            )

        take_profit = make_shadow("solana-shadow-paper-take-profit")
        result = take_profit.process_shadow_exit(
            "solana-shadow-paper-take-profit",
            features(30, "0.20"),
            NOW + timedelta(seconds=30),
            make_sell_quote("0.0012"),
        )
        self.assertTrue(result.decision.triggered)
        self.assertEqual(result.decision.reason, "take_profit")

        stop_loss = make_shadow("solana-shadow-paper-stop-loss")
        result = stop_loss.process_shadow_exit(
            "solana-shadow-paper-stop-loss",
            features(30, "-0.20"),
            NOW + timedelta(seconds=30),
            make_sell_quote("0.0008"),
        )
        self.assertTrue(result.decision.triggered)
        self.assertEqual(result.decision.reason, "stop_loss")

        timeout = make_shadow("solana-shadow-paper-timeout")
        result = timeout.process_shadow_exit(
            "solana-shadow-paper-timeout",
            features(600, "0"),
            NOW + timedelta(seconds=600),
            make_sell_quote("0.001"),
        )
        self.assertTrue(result.decision.triggered)
        self.assertEqual(result.decision.reason, "max_hold_timeout")

    def test_bsc_shadow_liquidity_drop_requires_more_than_fifteen_percent(self) -> None:
        bsc_features = replace(
            make_entry_features(),
            holders=100,
            liquidity_usd=Decimal("100"),
            pricing_mode="binance_indicative",
            executable_quote=False,
        )
        strategy = BaselineStrategy(bsc_baseline_config())
        exact = DeterministicSimulation("shadow", strategy=strategy, pricing_mode="binance_indicative", executable_quote=False)
        exact_entry = exact.process_entry(
            make_signal("bsc-shadow-liquidity-exact"),
            bsc_features,
            candidate_id="bsc-shadow-liquidity-exact-candidate",
            position_id="bsc-shadow-liquidity-exact-position",
        )
        exact_result = exact.process_shadow_exit(
            "bsc-shadow-liquidity-exact-position",
            ShadowExitFeatures(
                position_age_sec=30,
                return_pct=Decimal("0"),
                mfe_pct=Decimal("0"),
                recent_net_flow_negative=False,
                independent_buyer_growth_stopped=False,
                creator_sell_confident=False,
                buyer_growth_and_flow_slowed=False,
                holders=100,
                liquidity_usd=Decimal("85"),
            ),
            NOW + timedelta(seconds=30),
            make_sell_quote("0.01"),
        )
        self.assertIsNotNone(exact_entry.position)
        self.assertFalse(exact_result.decision.triggered)

        below = DeterministicSimulation("shadow", strategy=strategy, pricing_mode="binance_indicative", executable_quote=False)
        below.process_entry(
            make_signal("bsc-shadow-liquidity-below"),
            bsc_features,
            candidate_id="bsc-shadow-liquidity-below-candidate",
            position_id="bsc-shadow-liquidity-below-position",
        )
        below_result = below.process_shadow_exit(
            "bsc-shadow-liquidity-below-position",
            ShadowExitFeatures(
                position_age_sec=30,
                return_pct=Decimal("0"),
                mfe_pct=Decimal("0"),
                recent_net_flow_negative=False,
                independent_buyer_growth_stopped=False,
                creator_sell_confident=False,
                buyer_growth_and_flow_slowed=False,
                holders=100,
                liquidity_usd=Decimal("84.99"),
            ),
            NOW + timedelta(seconds=30),
            make_sell_quote("0.01"),
        )
        self.assertTrue(below_result.decision.triggered)
        self.assertEqual(below_result.decision.reason, "shadow_liquidity_drop_over_15pct")

    def test_paper_stop_loss_is_prioritized_over_max_hold(self) -> None:
        engine = DeterministicSimulation("paper")
        engine.process_entry(
            make_signal(),
            make_entry_features(),
            candidate_id="candidate-1",
            position_id="position-1",
        )
        result = engine.process_paper_exit(
            "position-1",
            NOW + timedelta(seconds=600),
            make_sell_quote("0.0007"),
        )
        self.assertEqual(result.decision.reason, "stop_loss")

    def test_position_limit_and_one_trade_per_mint_are_per_mode(self) -> None:
        paper = DeterministicSimulation("paper")
        shadow = DeterministicSimulation("shadow")
        paper.process_entry(
            make_signal("sig-paper", "MintA"),
            make_entry_features(),
            candidate_id="paper-candidate-1",
            position_id="paper-position-1",
        )
        shadow_result = shadow.process_entry(
            make_signal("sig-shadow", "MintA"),
            make_entry_features(),
            candidate_id="shadow-candidate-1",
            position_id="shadow-position-1",
        )
        self.assertTrue(shadow_result.decision.accepted)
        duplicate = paper.process_entry(
            make_signal("sig-paper-duplicate", "MintA"),
            make_entry_features(),
            candidate_id="paper-candidate-2",
            position_id="paper-position-2",
        )
        self.assertIn("mint_lifecycle_exists", duplicate.decision.failed_reason_codes)

    def test_shadow_defense_exit_records_all_followup_windows(self) -> None:
        engine = DeterministicSimulation("shadow")
        engine.process_entry(
            make_signal(),
            make_entry_features(),
            candidate_id="candidate-1",
            position_id="position-1",
        )
        result = engine.process_shadow_exit(
            "position-1",
            ShadowExitFeatures(
                position_age_sec=20,
                return_pct=Decimal("-0.10"),
                mfe_pct=Decimal("0"),
                recent_net_flow_negative=True,
                independent_buyer_growth_stopped=True,
                creator_sell_confident=False,
                buyer_growth_and_flow_slowed=False,
            ),
            NOW + timedelta(seconds=20),
            make_sell_quote("0.0009"),
            returns_after_exit_pct={
                5: Decimal("-0.09"),
                15: Decimal("-0.05"),
                30: Decimal("0.02"),
                60: Decimal("0.11"),
                120: Decimal("0.03"),
            },
            avoided_loss_pct=Decimal("0.05"),
            missed_profit_pct=Decimal("0.11"),
        )
        self.assertTrue(result.decision.triggered)
        self.assertEqual(result.decision.reason, "shadow_defense_v1")
        self.assertEqual(set(result.outcome.returns_after_exit_pct), {5, 15, 30, 60, 120})
        self.assertTrue(result.outcome.paper_tp_reached)
        self.assertEqual(result.closed_position.status, "CLOSED")

    def test_shadow_time_exit_requires_all_conditions(self) -> None:
        engine = DeterministicSimulation("shadow")
        engine.process_entry(
            make_signal(),
            make_entry_features(),
            candidate_id="candidate-1",
            position_id="position-1",
        )
        result = engine.process_shadow_exit(
            "position-1",
            ShadowExitFeatures(
                position_age_sec=120,
                return_pct=Decimal("0.01"),
                mfe_pct=Decimal("0.04"),
                recent_net_flow_negative=False,
                independent_buyer_growth_stopped=False,
                creator_sell_confident=False,
                buyer_growth_and_flow_slowed=True,
            ),
            NOW + timedelta(seconds=120),
            make_sell_quote("0.00101"),
        )
        self.assertTrue(result.decision.triggered)
        self.assertEqual(result.decision.reason, "shadow_time_exit")

    def test_sqlite_records_are_separate_for_paper_and_shadow(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            paper_db = initialize_database(Path(directory) / "paper.db")
            shadow_db = initialize_database(Path(directory) / "shadow.db")
            paper = DeterministicSimulation(
                "paper",
                ledger=SimulationLedger("paper", paper_db),
            )
            shadow = DeterministicSimulation(
                "shadow",
                ledger=SimulationLedger("shadow", shadow_db),
            )
            paper.process_entry(
                make_signal("paper-signal"),
                make_entry_features(),
                candidate_id="paper-candidate",
                position_id="paper-position",
            )
            shadow.process_entry(
                make_signal("shadow-signal"),
                make_entry_features(),
                candidate_id="shadow-candidate",
                position_id="shadow-position",
            )
            paper_count = paper_db.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
            shadow_count = shadow_db.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
            self.assertEqual(paper_count, 1)
            self.assertEqual(shadow_count, 1)
            paper_db.close()
            shadow_db.close()

    def test_sqlite_execution_records_quote_and_pnl_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "paper.db")
            engine = DeterministicSimulation(
                "paper",
                ledger=SimulationLedger("paper", connection),
            )
            engine.process_entry(
                make_signal(),
                make_entry_features(),
                candidate_id="candidate-1",
                position_id="position-1",
            )
            engine.process_paper_exit(
                "position-1",
                NOW + timedelta(seconds=20),
                make_sell_quote("0.0012"),
            )
            row = connection.execute(
                "SELECT quote_output_quantity, price_impact_pct, gross_pnl_pct, "
                "net_pnl_estimated_sol FROM executions WHERE action = 'exit'"
            ).fetchone()
            self.assertEqual(row[0], "0.0012")
            self.assertEqual(row[1], "2")
            self.assertEqual(row[2], "0.2")
            self.assertEqual(row[3], "0.000199")
            connection.close()

    def test_mfe_mae_update_and_restart_recovery_preserve_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "paper.db")
            engine = DeterministicSimulation(
                "paper",
                ledger=SimulationLedger("paper", connection),
            )
            engine.process_entry(
                make_signal(),
                make_entry_features(),
                candidate_id="candidate-1",
                position_id="position-1",
            )
            high = make_sell_quote("0.0012", quote_id="mark-high")
            low = make_sell_quote("0.0008", quote_id="mark-low")
            high_observation = engine.ledger.record_observation(
                "position-1",
                NOW + timedelta(seconds=10),
                high,
            )
            low_observation = engine.ledger.record_observation(
                "position-1",
                NOW + timedelta(seconds=20),
                low,
            )
            self.assertEqual(high_observation.mfe_pct, Decimal("0.2"))
            self.assertEqual(low_observation.mae_pct, Decimal("-0.2"))
            recovered = SimulationLedger.recover("paper", connection)
            recovered_position = recovered.positions["position-1"]
            self.assertEqual(recovered_position.mfe_pct, Decimal("0.2"))
            self.assertEqual(recovered_position.mae_pct, Decimal("-0.2"))
            self.assertEqual(recovered_position.last_quote_id, "mark-low")
            duplicate = DeterministicSimulation("paper", ledger=recovered).process_entry(
                make_signal("sig-after-restart"),
                make_entry_features(),
                candidate_id="candidate-after-restart",
                position_id="position-after-restart",
            )
            self.assertIn("mint_lifecycle_exists", duplicate.decision.failed_reason_codes)
            connection.close()

    def test_lifecycle_events_and_queries_are_newest_first(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "paper.db")
            engine = DeterministicSimulation(
                "paper",
                ledger=SimulationLedger("paper", connection),
            )
            first_signal = make_signal("sig-first", "MintA")
            second_signal = make_signal("sig-second", "MintB")
            first_features = make_entry_features(
                buy_quote=make_buy_quote("MintA", "buy-a"),
                sell_quote=make_sell_quote("0.001", "MintA", "sell-a"),
            )
            second_features = make_entry_features(
                buy_quote=make_buy_quote("MintB", "buy-b"),
                sell_quote=make_sell_quote("0.001", "MintB", "sell-b"),
                token_name="Beta",
            )
            engine.process_entry(first_signal, first_features, "candidate-a", "position-a")
            engine.process_entry(second_signal, second_features, "candidate-b", "position-b")
            queries = LedgerQueries(connection, "paper")
            candidates = queries.candidates()
            self.assertEqual(candidates[0]["candidate_id"], "candidate-b")
            self.assertIsInstance(candidates[0]["rule_checks"], list)
            events = queries.lifecycle_events(position_id="position-a")
            event_types = [event["event_type"] for event in events]
            self.assertIn("POSITION_CREATED", event_types)
            self.assertIn("OPEN", event_types)
            positions = queries.positions(status="OPEN")
            self.assertEqual(len(positions), 2)
            connection.close()

    def test_recovered_ledger_uses_unique_lifecycle_event_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "paper.db"
            connection = initialize_database(database)
            engine = DeterministicSimulation("paper", ledger=SimulationLedger("paper", connection))
            engine.process_entry(
                make_signal("sig-restart", "MintA"),
                make_entry_features(),
                "candidate-restart",
                "position-restart",
            )
            recovered = SimulationLedger.recover("paper", connection)
            recovered.record_event(
                "position-restart",
                "MARK_OBSERVED",
                NOW + timedelta(seconds=10),
                {"restart": True},
            )
            event_ids = [row["event_id"] for row in connection.execute("SELECT event_id FROM lifecycle_events")]
            self.assertEqual(len(event_ids), len(set(event_ids)))
            connection.close()

    def test_replay_runner_completes_paper_lifecycle_deterministically(self) -> None:
        engine = DeterministicSimulation("paper")
        step = ReplayStep(
            step_id="replay-1",
            signal=make_signal(),
            entry_features=make_entry_features(),
            exits=(
                ReplayExit(
                    at=NOW + timedelta(seconds=20),
                    sell_quote=make_sell_quote("0.0012"),
                ),
            ),
        )
        result = ReplayRunner(engine).run((step,))
        self.assertEqual(len(result.entries), 1)
        self.assertEqual(len(result.exits), 1)
        self.assertEqual(result.exits[0].decision.reason, "take_profit")
        self.assertEqual(result.exits[0].closed_position.status, "CLOSED")
