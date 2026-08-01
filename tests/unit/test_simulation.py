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
        soft_features={"holders": 0, "market_cap": None},
    )


class SimulationTests(unittest.TestCase):
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
        self.assertEqual(result.candidate.soft_features, {"holders": 0, "market_cap": None})
        self.assertIn(
            "same_name_cooldown",
            {check.name for check in result.candidate.checks},
        )

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
