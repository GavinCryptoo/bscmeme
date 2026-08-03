from __future__ import annotations

import base64
import json
import os
import tempfile
import unittest
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from meme_system.adapters.bsc_wss import (
    SWAP_EVENT_TOPIC,
    SYNC_EVENT_TOPIC,
    BscPairEvent,
    BscPoolDescriptor,
    BscPoolResolver,
    BscPairWssMonitor,
    normalize_bsc_address,
)
from meme_system.adapters.binance_web3.normalizer import normalize_meme_row
from meme_system.adapters.binance_web3.normalizer import normalize_dynamic
from meme_system.adapters.jupiter import JupiterReadOnlyQuoteProvider
from meme_system.adapters.pump_readonly import _b58decode, _b58encode
from meme_system.config.service import ConfigService
from meme_system.dashboard_server import DashboardService
from meme_system.domain.models import BASELINE_IDENTITY, EntryFeatures, VirtualPosition
from meme_system.engines.ledger import SimulationLedger
from meme_system.engines.simulation import DeterministicSimulation
from meme_system.realtime import (
    BinanceRealtimeFeatureProvider,
    ExitHoldersBackfill,
    ExitMarketBackfill,
    PendingSignalObservation,
    RealtimeCoordinator,
)
from meme_system.runtime_ops import HealthRegistry, JsonlAuditWriter, LatencyRecorder, RuntimeControl, SingleInstanceLock
from meme_system.config.runtime import RuntimePaths
from meme_system.storage.database import initialize_database
from meme_system.storage.exit_holders import database_path
from meme_system.storage.runtime_store import RuntimeStore
from meme_system.telegram_control import TelegramConfig, TelegramControl


NOW = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)


class _Source:
    def __init__(self, record):
        self.record = record

    def fetch_once(self):
        return (self.record,)


class Phase4Tests(unittest.TestCase):
    def test_exit_holders_backfill_is_bounded_and_async(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = initialize_database(root / "paper.db")
            position = VirtualPosition(
                position_id="paper:backfill:position",
                mint="BackfillMint",
                mode="paper",
                identity=BASELINE_IDENTITY,
                quantity_sol=Decimal("0.001"),
                opened_at=NOW,
                entry_quantity_token=Decimal("1000"),
                status="CLOSED",
                closed_at=NOW,
                exit_holders_status="pending",
            )
            connection.execute(
                "INSERT INTO virtual_positions(position_id, mint, mode, strategy_name, ruleset_name, "
                "ruleset_version, config_version, quantity_sol, opened_at, status, "
                "entry_quantity_token, remaining_quantity_token, closed_at, closed_reason, "
                "exit_holders_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    position.position_id, position.mint, position.mode,
                    position.identity.strategy_name, position.identity.ruleset_name,
                    position.identity.ruleset_version, position.identity.config_version,
                    "0.001", NOW.isoformat(), "CLOSED", "1000", "0", NOW.isoformat(),
                    "stop_loss", "pending",
                ),
            )
            connection.commit()

            class _Features:
                attempts = 0

                def fetch_exit_holders_snapshot(self, _position):
                    self.attempts += 1
                    if self.attempts < 3:
                        raise RuntimeError("temporary dynamic failure")
                    return normalize_dynamic(
                        {"holders": "44"},
                        mint="BackfillMint",
                        chain_id="CT_501",
                        fetched_at=NOW + timedelta(seconds=1),
                    )

            features = _Features()
            worker = ExitHoldersBackfill(
                features,
                {"paper": database_path(connection)},
                retry_delay_sec=0,
            )
            started = time.monotonic()
            self.assertTrue(worker.submit("paper", position))
            self.assertLess(time.monotonic() - started, 0.5)
            deadline = time.monotonic() + 2
            row = None
            while time.monotonic() < deadline:
                row = connection.execute(
                    "SELECT exit_holders, exit_holders_status, exit_holders_source "
                    "FROM virtual_positions WHERE position_id = ?",
                    (position.position_id,),
                ).fetchone()
                if row[1] == "completed":
                    break
                time.sleep(0.01)
            self.assertEqual(tuple(row), (44, "completed", "binance_dynamic"))
            self.assertEqual(features.attempts, 3)
            worker.shutdown()
            connection.close()

    def test_exit_market_backfill_is_bounded_and_persists_both_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = initialize_database(root / "paper.db")
            position = VirtualPosition(
                position_id="paper:market-backfill:position",
                mint="MarketBackfillMint",
                mode="paper",
                identity=BASELINE_IDENTITY,
                quantity_sol=Decimal("0.001"),
                opened_at=NOW,
                entry_quantity_token=Decimal("1000"),
                status="CLOSED",
                closed_at=NOW,
                exit_market_status="pending",
            )
            connection.execute(
                "INSERT INTO virtual_positions(position_id, mint, mode, strategy_name, ruleset_name, "
                "ruleset_version, config_version, quantity_sol, opened_at, status, "
                "entry_quantity_token, remaining_quantity_token, closed_at, closed_reason, "
                "exit_market_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    position.position_id, position.mint, position.mode,
                    position.identity.strategy_name, position.identity.ruleset_name,
                    position.identity.ruleset_version, position.identity.config_version,
                    "0.001", NOW.isoformat(), "CLOSED", "1000", "0", NOW.isoformat(),
                    "stop_loss", "pending",
                ),
            )
            connection.commit()

            class _Features:
                attempts = 0

                def fetch_exit_market_snapshot(self, _position):
                    self.attempts += 1
                    if self.attempts < 3:
                        raise RuntimeError("temporary dynamic failure")
                    return normalize_dynamic(
                        {"marketCap": "1800", "liquidity": "210"},
                        mint="MarketBackfillMint",
                        chain_id="CT_501",
                        fetched_at=NOW + timedelta(seconds=1),
                    )

            features = _Features()
            worker = ExitMarketBackfill(
                features,
                {"paper": database_path(connection)},
                retry_delay_sec=0,
            )
            self.assertTrue(worker.submit("paper", position))
            deadline = time.monotonic() + 2
            row = None
            while time.monotonic() < deadline:
                row = connection.execute(
                    "SELECT exit_market_cap_usd, exit_liquidity_usd, exit_market_status, "
                    "exit_market_source FROM virtual_positions WHERE position_id = ?",
                    (position.position_id,),
                ).fetchone()
                if row[2] == "completed":
                    break
                time.sleep(0.01)
            self.assertEqual(tuple(row), ("1800", "210", "completed", "binance_dynamic"))
            self.assertEqual(features.attempts, 3)
            worker.shutdown()
            connection.close()

    def test_bsc_pair_wss_filters_valid_pair_and_decodes_only_swap_sync(self) -> None:
        pair = "0x7138b48df7d98d7e3cc221bfe7192d0a178182d8"
        monitor = BscPairWssMonitor(urls=("wss://example.invalid",))
        monitor.set_pool_addresses([pair, "0x" + "e" * 40, "not-an-address"])
        self.assertEqual(monitor.pool_addresses(), (pair,))
        self.assertEqual(normalize_bsc_address("0x" + "0" * 40), None)
        event = monitor._decode_event(json.dumps({
            "jsonrpc": "2.0",
            "method": "eth_subscription",
            "params": {"result": {
                "address": pair.upper(),
                "topics": [SWAP_EVENT_TOPIC],
                "blockNumber": "0x10",
                "transactionHash": "0xabc",
                "logIndex": "0x2",
            }},
        }))
        self.assertIsNotNone(event)
        self.assertEqual(event.event_type, "swap")
        self.assertEqual(event.block_number, 16)
        ignored = monitor._decode_event(json.dumps({
            "method": "eth_subscription",
            "params": {"result": {"address": pair, "topics": ["0xunknown"]}},
        }))
        self.assertIsNone(ignored)

    def test_bsc_wss_missing_endpoint_is_bounded_and_reports_poll_fallback(self) -> None:
        monitor = BscPairWssMonitor()
        monitor.start()
        status = monitor.safe_status()
        self.assertEqual(status["state"], "UNAVAILABLE")
        self.assertEqual(status["last_error_class"], "bsc_wss_missing")
        self.assertEqual(status["fallback_poll_sec"], 2.0)
        monitor.stop()

    def test_bsc_uses_binance_indicative_price_without_quote_rejection(self) -> None:
        class _BscMarketData:
            chain_id = "56"

            def __init__(self, price: str) -> None:
                self.price = price

            def snapshot(self, mint: str):
                return normalize_dynamic(
                    {
                        "price": self.price,
                        "nativeTokenPrice": "100",
                        "liquidity": "100",
                        "holders": "21",
                    },
                    mint=mint,
                    chain_id="56",
                    fetched_at=NOW,
                )

        record = normalize_meme_row(
            {
                "contractAddress": "BscMint",
                "pairAnchorAddress": "0x7138b48df7d98d7e3cc221bfe7192d0a178182d8",
                "symbol": "躺平",
                "name": "BSC Alpha",
                "price": "2",
                "marketCap": "1000",
                "liquidity": "100",
                "holders": "21",
            },
            fetched_at=NOW,
            historical_bootstrap=False,
            chain_id="56",
        )
        provider = BinanceRealtimeFeatureProvider(
            market_data=_BscMarketData("2"),
            quote_provider=None,
            chain_id="56",
        )
        features = provider.entry_features(record, evaluated_at=NOW)
        self.assertEqual(features.pricing_mode, "binance_indicative")
        self.assertEqual(features.token_name, "躺平")
        self.assertFalse(features.executable_quote)
        self.assertTrue(features.soft_features["net_pnl_is_estimated"])
        self.assertEqual(features.buy_quote.output_quantity, Decimal("0.05"))
        engine = DeterministicSimulation(
            "paper",
            pricing_mode="binance_indicative",
            executable_quote=False,
        )
        result = engine.process_entry(record.signal, features, "bsc-candidate", "bsc-position")
        self.assertTrue(result.decision.accepted)
        self.assertNotIn("buy_quote_unavailable", result.decision.failed_reason_codes)
        self.assertNotIn("sell_quote_unavailable", result.decision.failed_reason_codes)
        self.assertEqual(engine.ledger.executions[-1].pricing_mode, "binance_indicative")
        self.assertFalse(engine.ledger.executions[-1].executable_quote)

        invalid_features = BinanceRealtimeFeatureProvider(
            market_data=_BscMarketData("0"),
            quote_provider=None,
            chain_id="56",
        ).entry_features(record, evaluated_at=NOW)
        invalid = engine.strategy.evaluate_entry(invalid_features)
        self.assertFalse(invalid.accepted)
        self.assertIn("bsc_price_unavailable", invalid.failed_reason_codes)
        self.assertNotIn("buy_quote_unavailable", invalid.failed_reason_codes)
        self.assertNotIn("sell_quote_unavailable", invalid.failed_reason_codes)

    def test_bsc_position_event_falls_back_without_resolved_pool_price(self) -> None:
        class _BscMarketData:
            def snapshot(self, mint: str):
                return normalize_dynamic(
                    {"price": "2", "nativeTokenPrice": "100", "liquidity": "100", "holders": "21"},
                    mint=mint,
                    chain_id="56",
                    fetched_at=NOW,
                )

        record = normalize_meme_row(
            {
                "contractAddress": "BscMint",
                "pairAnchorAddress": "0x7138b48df7d98d7e3cc221bfe7192d0a178182d8",
                "symbol": "躺平",
                "price": "2",
                "marketCap": "1000",
                "liquidity": "100",
                "holders": "21",
            },
            fetched_at=NOW,
            historical_bootstrap=False,
            chain_id="56",
        )
        features = BinanceRealtimeFeatureProvider(market_data=_BscMarketData(), chain_id="56")
        engine = DeterministicSimulation("paper", pricing_mode="binance_indicative", executable_quote=False)
        entry = engine.process_entry(record.signal, features.entry_features(record, evaluated_at=NOW), "bsc-candidate", "bsc-position")
        self.assertIsNotNone(entry.position)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = initialize_database(root / "paper.db")
            coordinator = RealtimeCoordinator(
                source=_Source(record),
                features=features,
                engines={"paper": engine},
                controls=RuntimeControl(root / "control.json"),
                health={"paper": HealthRegistry(root / "health.json")},
                latency=LatencyRecorder(),
                stores={"paper": RuntimeStore(connection, "paper")},
                audits={"paper": JsonlAuditWriter(root / "events.jsonl")},
                clock=lambda: NOW,
            )
            coordinator.remember_bsc_pool(record)
            coordinator.notify_bsc_pair_event(BscPairEvent(
                pair_address="0x7138b48df7d98d7e3cc221bfe7192d0a178182d8",
                event_type="sync",
                block_number=16,
                transaction_hash=None,
                log_index=None,
                observed_at=NOW,
            ))
            result = coordinator.run_position_cycle()
            self.assertEqual(result.trigger, "wss")
            self.assertEqual(result.event_count, 1)
            self.assertIsNotNone(engine.ledger.positions["bsc-position"].last_return_pct)
            connection.close()

    def test_bsc_pool_event_uses_wss_price_without_binance_refresh(self) -> None:
        mint = "0x" + "1" * 40
        pair = "0x7138b48df7d98d7e3cc221bfe7192d0a178182d8"
        wbnb = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"

        class _Rpc:
            configured = True

            def call_address(self, _to, selector):
                return wbnb if selector == "0x0dfe1681" else mint

            def call_hex(self, _to, data):
                if data == "0x0902f1ac":
                    return "0x" + "0" * 64 + "0" * 64 + "0" * 64
                return "0x" + "0" * 63 + "12"

            def call_uint(self, _to, _selector):
                return 18

        resolver = BscPoolResolver(_Rpc())

        class _MarketData:
            def __init__(self):
                self.calls = 0

            def snapshot(self, token):
                self.calls += 1
                if self.calls > 1:
                    raise AssertionError("Binance fallback must not run after a valid WSS price")
                return normalize_dynamic(
                    {"price": "2", "nativeTokenPrice": "100", "liquidity": "100", "holders": "21"},
                    mint=token,
                    chain_id="56",
                    fetched_at=NOW,
                )

        record = normalize_meme_row(
            {
                "contractAddress": mint,
                "pairAnchorAddress": pair,
                "symbol": "WSS",
                "price": "2",
                "marketCap": "1000",
                "liquidity": "100",
                "holders": "21",
            },
            fetched_at=NOW,
            historical_bootstrap=False,
            chain_id="56",
        )
        market_data = _MarketData()
        features = BinanceRealtimeFeatureProvider(
            market_data=market_data,
            chain_id="56",
            bsc_pool_resolver=resolver,
        )
        engine = DeterministicSimulation("paper", pricing_mode="binance_indicative", executable_quote=False)
        entry = engine.process_entry(
            record.signal,
            features.entry_features(record, evaluated_at=NOW),
            "bsc-wss-candidate",
            "bsc-wss-position",
        )
        self.assertIsNotNone(entry.position)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = initialize_database(root / "paper.db")
            coordinator = RealtimeCoordinator(
                source=_Source(record),
                features=features,
                engines={"paper": engine},
                controls=RuntimeControl(root / "control.json"),
                health={"paper": HealthRegistry(root / "health.json")},
                latency=LatencyRecorder(),
                stores={"paper": RuntimeStore(connection, "paper")},
                audits={"paper": JsonlAuditWriter(root / "events.jsonl")},
                clock=lambda: NOW + timedelta(seconds=1),
            )
            coordinator.remember_bsc_pool(record)
            descriptors = coordinator.bsc_position_pool_descriptors()
            self.assertEqual(len(descriptors), 1)
            self.assertEqual(descriptors[0].pool_type, "v2")
            coordinator.update_bsc_wss_status({"state": "HEALTHY"})
            coordinator.notify_bsc_pair_event(BscPairEvent(
                pair_address=pair,
                event_type="sync",
                block_number=16,
                transaction_hash="0xabc",
                log_index=2,
                observed_at=NOW + timedelta(seconds=1),
                pool_type="v2",
                data="0x" + f"{10 ** 19:064x}" + f"{10 ** 21:064x}",
                topics=(SYNC_EVENT_TOPIC,),
            ))
            result = coordinator.run_position_cycle()
            self.assertEqual(result.trigger, "wss")
            self.assertEqual(market_data.calls, 1)
            executions = [item for item in engine.ledger.executions if item.action == "exit"]
            self.assertTrue(executions)
            self.assertTrue(executions[-1].quote_id.startswith("bsc-pool-wss:"))
            connection.close()

    def test_bsc_v3_swap_price_requires_sqrt_price_and_wbnb_pair(self) -> None:
        mint = "0x" + "2" * 40
        wbnb = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
        descriptor = BscPoolDescriptor(
            address="0x" + "3" * 40,
            pool_type="v3",
            mint=mint,
            token0=mint,
            token1=wbnb,
            token0_decimals=18,
            token1_decimals=18,
        )
        resolver = BscPoolResolver()
        sqrt_price_x96 = 2 ** 96
        event = BscPairEvent(
            pair_address=descriptor.address,
            event_type="swap",
            block_number=1,
            transaction_hash=None,
            log_index=0,
            observed_at=NOW,
            pool_type="v3",
            data="0x" + "0" * 64 + "0" * 64 + f"{sqrt_price_x96:064x}" + "0" * 128,
            topics=(SWAP_EVENT_TOPIC,),
        )
        price = resolver.price_from_event(descriptor, event)
        self.assertIsNotNone(price)
        self.assertEqual(price.native_token_price, Decimal("1"))

    def test_bsc_exit_holders_are_persisted_separately_from_entry_holders(self) -> None:
        class _BscMarketData:
            def snapshot(self, mint: str):
                return normalize_dynamic(
                    {"price": "2.3", "nativeTokenPrice": "100", "liquidity": "100", "holders": "44"},
                    mint=mint,
                    chain_id="56",
                    fetched_at=NOW,
                )

        record = normalize_meme_row(
            {
                "contractAddress": "BscHoldersMint",
                "symbol": "HOLDERS",
                "price": "2",
                "marketCap": "1000",
                "liquidity": "100",
                "holders": "21",
            },
            fetched_at=NOW,
            historical_bootstrap=False,
            chain_id="56",
        )
        features = BinanceRealtimeFeatureProvider(
            market_data=_BscMarketData(),
            chain_id="56",
        )
        engine = DeterministicSimulation(
            "paper",
            pricing_mode="binance_indicative",
            executable_quote=False,
        )
        entry = engine.process_entry(
            record.signal,
            features.entry_features(record, evaluated_at=NOW),
            "bsc-holders-candidate",
            "bsc-holders-position",
        )
        self.assertIsNotNone(entry.position)
        result = engine.process_paper_exit(
            "bsc-holders-position",
            NOW + timedelta(seconds=600),
            features.quote_for_position(entry.position),
            exit_holders=44,
        )
        self.assertIsNotNone(result.closed_position)
        self.assertEqual(result.closed_position.exit_holders, 44)

    def test_observation_gate_requires_price_rise_and_liquidity(self) -> None:
        record = normalize_meme_row(
            {"contractAddress": "MintA", "price": "1"},
            fetched_at=NOW,
            historical_bootstrap=False,
        )
        pending = PendingSignalObservation(
            record=record,
            first_price_usd=Decimal("1"),
            discovered_at=NOW,
        )
        base = EntryFeatures(
            token_age_sec=None,
            unique_buyers_15s=None,
            buy_sell_count_ratio_15s=None,
            net_buy_15s=None,
            flow_windows_non_negative=(None, None),
            creator_confirmed_sold=None,
            buy_quote=None,
            sell_quote=None,
            evaluated_at=NOW,
            soft_features={"price_usd": "1.01", "observation_snapshot_available": True},
            liquidity_usd=Decimal("100"),
        )
        self.assertIsNone(RealtimeCoordinator._observation_gate(pending, base, Decimal("100")))
        self.assertEqual(
            RealtimeCoordinator._observation_gate(
                pending,
                replace(base, soft_features={"price_usd": "0.99", "observation_snapshot_available": True}),
                Decimal("100"),
            ),
            "price_not_up_after_observation",
        )
        self.assertIsNone(
            RealtimeCoordinator._observation_gate(
                pending,
                replace(base, soft_features={"price_usd": "1.00", "observation_snapshot_available": True}),
                Decimal("100"),
                price_must_rise=False,
            )
        )
        self.assertEqual(
            RealtimeCoordinator._observation_gate(
                pending,
                replace(base, soft_features={"price_usd": "0.99", "observation_snapshot_available": True}),
                Decimal("100"),
                price_must_rise=False,
            ),
            "price_below_after_observation",
        )
        self.assertEqual(
            RealtimeCoordinator._observation_gate(
                pending,
                replace(base, liquidity_usd=Decimal("99.99")),
                Decimal("100"),
            ),
            "observation_liquidity_below_min",
        )

    def test_bsc_observation_gate_requires_holders_not_below_first_discovery(self) -> None:
        record = normalize_meme_row(
            {"contractAddress": "BscMint", "price": "1", "holders": "100"},
            fetched_at=NOW,
            historical_bootstrap=False,
            chain_id="56",
        )
        pending = PendingSignalObservation(
            record=record,
            first_price_usd=Decimal("1"),
            discovered_at=NOW,
            first_holders=100,
        )
        base = EntryFeatures(
            token_age_sec=None,
            unique_buyers_15s=None,
            buy_sell_count_ratio_15s=None,
            net_buy_15s=None,
            flow_windows_non_negative=(None, None),
            creator_confirmed_sold=None,
            buy_quote=None,
            sell_quote=None,
            evaluated_at=NOW,
            soft_features={"price_usd": "1.01", "observation_snapshot_available": True},
            holders=100,
            liquidity_usd=Decimal("100"),
        )
        self.assertIsNone(
            RealtimeCoordinator._observation_gate(
                pending,
                base,
                Decimal("100"),
                price_must_rise=True,
                holders_must_not_decrease=True,
            )
        )
        self.assertEqual(
            RealtimeCoordinator._observation_gate(
                pending,
                replace(base, holders=99),
                Decimal("100"),
                price_must_rise=True,
                holders_must_not_decrease=True,
            ),
            "holders_below_first_discovery_after_observation",
        )
        self.assertEqual(
            RealtimeCoordinator._observation_gate(
                pending,
                replace(base, holders=None),
                Decimal("100"),
                price_must_rise=True,
                holders_must_not_decrease=True,
            ),
            "holders_observation_unavailable",
        )

    def test_bsc_shadow_observation_requires_liquidity_not_below_first_discovery(self) -> None:
        record = normalize_meme_row(
            {"contractAddress": "BscLiquidityMint", "price": "1", "holders": "100", "liquidity": "100"},
            fetched_at=NOW,
            historical_bootstrap=False,
            chain_id="56",
        )
        pending = PendingSignalObservation(
            record=record,
            first_price_usd=Decimal("1"),
            discovered_at=NOW,
            first_holders=100,
            first_liquidity_usd=Decimal("100"),
        )
        base = EntryFeatures(
            token_age_sec=None,
            unique_buyers_15s=None,
            buy_sell_count_ratio_15s=None,
            net_buy_15s=None,
            flow_windows_non_negative=(None, None),
            creator_confirmed_sold=None,
            buy_quote=None,
            sell_quote=None,
            evaluated_at=NOW,
            soft_features={"price_usd": "1.01", "observation_snapshot_available": True},
            holders=100,
            liquidity_usd=Decimal("100"),
        )
        # Paper keeps the existing observation behavior: this is not a
        # global liquidity comparison gate.
        self.assertIsNone(
            RealtimeCoordinator._observation_gate(
                pending,
                replace(base, liquidity_usd=Decimal("99")),
                Decimal("90"),
                price_must_rise=True,
                holders_must_not_decrease=True,
            )
        )
        self.assertIsNone(
            RealtimeCoordinator._observation_gate(
                pending,
                base,
                Decimal("90"),
                price_must_rise=True,
                holders_must_not_decrease=True,
                liquidity_must_not_decrease=True,
            )
        )
        self.assertEqual(
            RealtimeCoordinator._observation_gate(
                pending,
                replace(base, liquidity_usd=Decimal("99")),
                Decimal("90"),
                price_must_rise=True,
                holders_must_not_decrease=True,
                liquidity_must_not_decrease=True,
            ),
            "liquidity_below_first_discovery_after_observation",
        )
        self.assertEqual(
            RealtimeCoordinator._observation_gate(
                replace(pending, first_liquidity_usd=None),
                base,
                Decimal("90"),
                price_must_rise=True,
                holders_must_not_decrease=True,
                liquidity_must_not_decrease=True,
            ),
            "observation_liquidity_unavailable",
        )

    def test_solana_observation_gate_requires_holders_not_below_first_discovery(self) -> None:
        record = normalize_meme_row(
            {"contractAddress": "SolanaMint", "price": "1", "holders": "10"},
            fetched_at=NOW,
            historical_bootstrap=False,
        )
        pending = PendingSignalObservation(
            record=record,
            first_price_usd=Decimal("1"),
            discovered_at=NOW,
            first_holders=10,
        )
        base = EntryFeatures(
            token_age_sec=None,
            unique_buyers_15s=None,
            buy_sell_count_ratio_15s=None,
            net_buy_15s=None,
            flow_windows_non_negative=(None, None),
            creator_confirmed_sold=None,
            buy_quote=None,
            sell_quote=None,
            evaluated_at=NOW,
            soft_features={"price_usd": "1.00", "observation_snapshot_available": True},
            holders=10,
            liquidity_usd=Decimal("100"),
        )
        self.assertIsNone(
            RealtimeCoordinator._observation_gate(
                pending,
                base,
                Decimal("100"),
                price_must_rise=False,
                holders_must_not_decrease=True,
            )
        )
        self.assertEqual(
            RealtimeCoordinator._observation_gate(
                pending,
                replace(base, holders=9),
                Decimal("100"),
                price_must_rise=False,
                holders_must_not_decrease=True,
            ),
            "holders_below_first_discovery_after_observation",
        )
        self.assertEqual(
            RealtimeCoordinator._observation_gate(
                pending,
                replace(base, holders=None),
                Decimal("100"),
                price_must_rise=False,
                holders_must_not_decrease=True,
            ),
            "holders_observation_unavailable",
        )

    def test_jupiter_quote_is_read_only_and_keeps_unknown_impact_unavailable(self) -> None:
        calls = []

        def transport(url, headers, timeout):
            calls.append((url, headers))
            payload = {
                "inputMint": "So11111111111111111111111111111111111111112",
                "inAmount": "1000000",
                "outputMint": "MintA",
                "outAmount": "5000000",
                "otherAmountThreshold": "4900000",
                "swapMode": "ExactIn",
                "slippageBps": 50,
                "priceImpactPct": "0.001",
                "routePlan": [{"swapInfo": {"label": "Test AMM"}}],
                "contextSlot": 123,
            }
            return 200, json.dumps(payload).encode(), {}

        provider = JupiterReadOnlyQuoteProvider(
            api_key="redacted-test-key",
            token_decimals={"MintA": 6},
            transport=transport,
        )
        quote = provider.quote_buy("MintA", __import__("decimal").Decimal("0.001"))
        self.assertEqual(quote.provider, "jupiter")
        self.assertEqual(quote.quote_source, "jupiter_quote")
        self.assertIsNone(quote.price_impact_pct)
        self.assertEqual(quote.route, ("Test AMM",))
        self.assertEqual(calls[0][1]["User-Agent"], "meme0801-readonly/1.0")
        self.assertTrue(provider.safe_status()["credentials_configured"])
        self.assertNotIn("redacted-test-key", json.dumps(provider.safe_status()))

    def test_jupiter_quote_cache_and_finite_429_retry(self) -> None:
        calls = []
        responses = [429, 200]

        def transport(url, headers, timeout):
            calls.append(url)
            status = responses.pop(0)
            payload = {
                "inputMint": "So11111111111111111111111111111111111111112",
                "inAmount": "1000000",
                "outputMint": "MintA",
                "outAmount": "5000000",
                "routePlan": [{"swapInfo": {"label": "Test AMM"}}],
            }
            return status, (json.dumps(payload).encode() if status == 200 else b"{}"), {}

        provider = JupiterReadOnlyQuoteProvider(
            api_key="redacted-test-key",
            token_decimals={"MintA": 6},
            max_retries=1,
            error_cache_ttl_ms=100,
            transport=transport,
        )
        first = provider.quote_buy("MintA", __import__("decimal").Decimal("0.001"))
        second = provider.quote_buy("MintA", __import__("decimal").Decimal("0.001"))
        self.assertEqual(len(calls), 2)
        self.assertEqual(first.quote_id, second.quote_id)
        self.assertEqual(provider.safe_status()["requests"], 2)
        self.assertEqual(provider.safe_status()["max_concurrency"], 2)

    def test_realtime_bootstrap_is_skipped_and_missing_hard_fields_are_explained(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = type("Paths", (), {
                "control_file": root / "control.json",
                "paper_health_file": root / "paper-health.json",
                "shadow_health_file": root / "shadow-health.json",
                "paper_db": root / "paper.db",
                "shadow_db": root / "shadow.db",
                "paper_audit_log": root / "paper.jsonl",
                "shadow_audit_log": root / "shadow.jsonl",
            })()
            record = normalize_meme_row(
                {"contractAddress": "MintA", "name": "Alpha", "holders": "8"},
                fetched_at=NOW,
                historical_bootstrap=False,
            )
            connections = {mode: initialize_database(getattr(paths, f"{mode}_db")) for mode in ("paper", "shadow")}
            engines = {mode: DeterministicSimulation(mode, ledger=SimulationLedger.recover(mode, connections[mode])) for mode in ("paper", "shadow")}
            control = RuntimeControl(paths.control_file)
            coordinator = RealtimeCoordinator(
                source=_Source(record),
                features=BinanceRealtimeFeatureProvider(),
                engines=engines,
                controls=control,
                health={mode: HealthRegistry(getattr(paths, f"{mode}_health_file")) for mode in engines},
                latency=LatencyRecorder(),
                stores={mode: RuntimeStore(connections[mode], mode) for mode in engines},
                audits={mode: JsonlAuditWriter(getattr(paths, f"{mode}_audit_log")) for mode in engines},
                clock=lambda: NOW,
            )
            result = coordinator.run_cycle()
            self.assertEqual(result.candidates, 0)
            coordinator.clock = lambda: NOW + timedelta(seconds=15)
            result = coordinator.run_cycle()
            self.assertEqual(result.candidates, 2)
            self.assertEqual(result.accepted, {"paper": 0, "shadow": 0})
            for mode in engines:
                self.assertEqual(len(engines[mode].ledger.candidates), 1)
                self.assertIn("token_age_unavailable", engines[mode].ledger.candidates[0].filter_reason)
                self.assertTrue(getattr(paths, f"{mode}_health_file").exists())
            for connection in connections.values():
                connection.close()

    def test_live_never_reenters_a_token_seen_in_startup_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = initialize_database(root / "live.db")
            engine = DeterministicSimulation(
                "live",
                ledger=SimulationLedger.recover("live", connection),
                live_executor=object(),
            )
            record = normalize_meme_row(
                {"contractAddress": "OldBscMint", "name": "Old", "holders": "8"},
                fetched_at=NOW,
                historical_bootstrap=False,
                chain_id="56",
            )
            coordinator = RealtimeCoordinator(
                source=_Source(record),
                features=BinanceRealtimeFeatureProvider(chain_id="56"),
                engines={"live": engine},
                controls=RuntimeControl(root / "control.json"),
                health={"live": HealthRegistry(root / "health.json")},
                latency=LatencyRecorder(),
                stores={"live": RuntimeStore(connection, "live")},
                audits={"live": JsonlAuditWriter(root / "events.jsonl")},
                clock=lambda: NOW,
            )
            first = coordinator.run_cycle()
            second = coordinator.run_cycle()
            self.assertEqual(first.candidates, 0)
            self.assertEqual(second.candidates, 0)
            self.assertEqual(first.bootstrap_skipped, 1)
            self.assertEqual(second.bootstrap_skipped, 1)
            self.assertEqual(engine.ledger.candidates, [])
            connection.close()

    def test_dashboard_control_and_config_service_are_whitelisted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            from meme_system.config.runtime import RuntimePaths

            paths = RuntimePaths(
                paper_db=root / "paper.db",
                shadow_db=root / "shadow.db",
                paper_audit_log=root / "paper.jsonl",
                shadow_audit_log=root / "shadow.jsonl",
                control_file=root / "control.json",
                paper_health_file=root / "paper-health.json",
                shadow_health_file=root / "shadow-health.json",
            )
            service = DashboardService(paths=paths)
            status, payload = service.set_control({"mode": "paper", "paused": True})
            self.assertEqual(status, 200)
            self.assertTrue(payload["paper_new_entries_paused"])
            config = ConfigService(root / "config.json", mode="paper")
            version = config.update({"poll_interval_sec": 10}, reason="test")
            self.assertTrue(version.version.startswith("cfg-"))
            with self.assertRaises(ValueError):
                config.update({"position_size_sol": "100"}, reason="unsafe")
            bsc_display = service.config_payload("bsc")
            self.assertEqual(bsc_display["display_name"], "BSC 链上只读报价 Paper/Shadow 策略")
            self.assertEqual(bsc_display["pricing"]["pricing_mode"], "bsc_executable_quote")
            self.assertTrue(bsc_display["pricing"]["executable_quote"])
            self.assertIn("require_readonly_buy_quote", bsc_display["strategy_config"]["entry"])
            self.assertIn("require_readonly_sell_quote", bsc_display["strategy_config"]["entry"])
            self.assertNotIn("require_executable_buy_route", bsc_display["strategy_config"]["entry"])
            self.assertEqual(bsc_display["ruleset_name"], "ultra_early_selective_bsc")
            self.assertEqual(bsc_display["ruleset_version"], "0.1.5")
            self.assertEqual(bsc_display["strategy_config"]["entry"]["observation_delay_sec"], 60)
            self.assertTrue(bsc_display["strategy_config"]["entry"]["require_holders_non_decreasing_after_observation"])
            self.assertEqual(bsc_display["strategy_config"]["entry"]["min_holders"], 100)
            self.assertTrue(bsc_display["strategy_config"]["entry"]["min_holders_inclusive"])
            self.assertEqual(bsc_display["strategy_config"]["paper_exit"]["stop_loss_trigger_pct"], "-10.00")
            self.assertEqual(bsc_display["strategy_config"]["shadow_exit"]["shadow_holders_drop_pct"], "10.00")
            self.assertEqual(bsc_display["strategy_config"]["shadow_exit"]["shadow_liquidity_drop_pct"], "15.00")
            self.assertIn(
                "shadow_holders_drop_over_10pct",
                bsc_display["strategy_config"]["shadow_exit"]["rules"],
            )
            self.assertIn(
                "shadow_liquidity_drop_over_15pct",
                bsc_display["strategy_config"]["shadow_exit"]["rules"],
            )
            self.assertIn(
                "paper_take_profit",
                bsc_display["strategy_config"]["shadow_exit"]["rules"],
            )
            self.assertIn(
                "paper_stop_loss",
                bsc_display["strategy_config"]["shadow_exit"]["rules"],
            )
            self.assertIn(
                "paper_max_hold_timeout",
                bsc_display["strategy_config"]["shadow_exit"]["rules"],
            )
            sol_display = service.config_payload("solana")
            self.assertEqual(sol_display["display_name"], "Solana 超早期基线策略")
            self.assertEqual(sol_display["pricing"]["pricing_mode"], "jupiter_quote")
            self.assertEqual(sol_display["ruleset_version"], "0.1.1")
            self.assertIn(
                "paper_take_profit",
                sol_display["strategy_config"]["shadow_exit"]["rules"],
            )
            self.assertIn(
                "paper_stop_loss",
                sol_display["strategy_config"]["shadow_exit"]["rules"],
            )
            self.assertIn(
                "paper_max_hold_timeout",
                sol_display["strategy_config"]["shadow_exit"]["rules"],
            )

    def test_bsc_runtime_paths_and_dashboard_are_isolated(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            solana_paths = type("Paths", (), {
                "control_file": root / "solana-control.json",
                "paper_health_file": root / "solana-paper-health.json",
                "shadow_health_file": root / "solana-shadow-health.json",
                "paper_db": root / "solana-paper.db",
                "shadow_db": root / "solana-shadow.db",
                "paper_audit_log": root / "solana-paper.jsonl",
                "shadow_audit_log": root / "solana-shadow.jsonl",
            })()
            with patch.dict(
                os.environ,
                {
                    "BSC_PAPER_DB_PATH": str(root / "bsc-paper.db"),
                    "BSC_SHADOW_DB_PATH": str(root / "bsc-shadow.db"),
                    "BSC_RUNTIME_CONTROL_PATH": str(root / "bsc-control.json"),
                    "BSC_PAPER_HEALTH_PATH": str(root / "bsc-paper-health.json"),
                    "BSC_SHADOW_HEALTH_PATH": str(root / "bsc-shadow-health.json"),
                },
                clear=False,
            ):
                bsc_paths = RuntimePaths.from_env("bsc")
                self.assertEqual(bsc_paths.chain, "bsc")
                self.assertNotEqual(bsc_paths.paper_db, bsc_paths.shadow_db)
                service = DashboardService(paths=solana_paths)
                status_code, status = service.payload("/api/status", {"chain": ["bsc"]})
                self.assertEqual(status_code, 200)
                self.assertEqual(status["chain_id"], "56")
                self.assertEqual(status["chain_key"], "bsc")

    def test_lock_and_telegram_disabled_are_safe(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lock = SingleInstanceLock(Path(directory) / "runner.lock", name="test")
            with lock:
                with self.assertRaises(Exception):
                    SingleInstanceLock(lock.path, name="other").acquire()
            telegram = TelegramControl(TelegramConfig(enabled=False), RuntimeControl(Path(directory) / "control.json"))
            self.assertEqual(telegram.poll_once(), ())

    def test_base58_round_trip_and_nested_account_encoding_fixture(self) -> None:
        raw = bytes(range(32))
        encoded = _b58encode(raw)
        self.assertEqual(_b58decode(encoded), raw)
        account = {"value": {"data": [base64.b64encode(b"abc").decode(), "base64"]}}
        from meme_system.adapters.pump_readonly import _account_bytes

        self.assertEqual(_account_bytes(account), b"abc")
