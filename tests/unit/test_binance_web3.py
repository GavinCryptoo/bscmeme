from __future__ import annotations

import json
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from decimal import Decimal

from meme_system.adapters.binance_web3.auth import BinanceWeb3Auth
from meme_system.adapters.binance_web3.client import BinanceWeb3Client, HttpResponse
from meme_system.adapters.binance_web3.errors import BinanceWeb3Error, UnsupportedAuthMethod
from meme_system.adapters.binance_web3.kline import BinanceWeb3KlineAdapter
from meme_system.adapters.binance_web3.market_data import BinanceWeb3MarketDataAdapter
from meme_system.adapters.binance_web3.redaction import contains_sensitive_patterns, redact_payload
from meme_system.adapters.binance_web3.signal_source import BinanceWeb3SignalSource
from meme_system.adapters.binance_web3.smart_money import BinanceWeb3SmartMoneyAdapter
from meme_system.adapters.binance_web3.normalizer import normalize_meme_row
from meme_system.adapters.binance_web3.normalizer import normalize_dynamic, normalize_smart_money_row
from meme_system.config.data_source import DataSourceConfig


FIXTURES = Path(__file__).parents[1] / "fixtures" / "binance_web3"


def load_fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class QueueTransport:
    def __init__(self, *responses: HttpResponse) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, str, dict[str, str], bytes | None]] = []

    def __call__(self, method, url, headers, body, timeout_sec, max_response_bytes):
        self.calls.append((method, url, dict(headers), body))
        if not self.responses:
            raise AssertionError("unexpected transport call")
        return self.responses.pop(0)


def json_response(payload: object, status: int = 200) -> HttpResponse:
    return HttpResponse(
        status=status,
        body=json.dumps(payload).encode("utf-8"),
        headers={"content-type": "application/json"},
    )


class BinanceWeb3ClientTests(unittest.TestCase):
    def test_public_auth_does_not_emit_credentials(self) -> None:
        auth = BinanceWeb3Auth.from_env()
        self.assertEqual(auth.safe_status(), {"credentials_configured": False, "auth_mode": "none"})
        self.assertNotIn("Authorization", auth.headers())
        self.assertNotIn("X-MBX-APIKEY", auth.headers())

    def test_unsupported_auth_mode_fails_closed(self) -> None:
        with patch.dict(os.environ, {"BINANCE_WEB3_AUTH_MODE": "browser_cookie"}, clear=False):
            with self.assertRaises(UnsupportedAuthMethod):
                BinanceWeb3Auth.from_env()

    def test_retry_is_finite_and_429_is_not_silent(self) -> None:
        payload = load_fixture("meme_rush_normal.json")
        transport = QueueTransport(json_response({"code": "100004", "data": []}, status=429), json_response(payload))
        sleeps: list[float] = []
        client = BinanceWeb3Client(transport=transport, sleep=sleeps.append)
        result = client.request_json("meme_rush", body={"chainId": "CT_501", "rankType": 10, "limit": 1})
        self.assertEqual(result.status, 200)
        self.assertEqual(result.retry_count, 1)
        self.assertEqual(len(transport.calls), 2)
        self.assertEqual(len(sleeps), 1)
        self.assertEqual(client.rate_limit_state.rate_limited, 1)

    def test_4xx_does_not_retry(self) -> None:
        transport = QueueTransport(json_response({"error": "bad request"}, status=400))
        client = BinanceWeb3Client(transport=transport)
        with self.assertRaises(BinanceWeb3Error) as raised:
            client.request_json("meme_rush", body={"chainId": "CT_501", "rankType": 10})
        self.assertEqual(raised.exception.context.error_class, "binance_http_4xx")
        self.assertEqual(len(transport.calls), 1)

    def test_invalid_json_is_classified(self) -> None:
        transport = QueueTransport(HttpResponse(200, b"not-json", {}))
        client = BinanceWeb3Client(transport=transport)
        with self.assertRaises(BinanceWeb3Error) as raised:
            client.request_json("meme_rush", body={"chainId": "CT_501", "rankType": 10})
        self.assertEqual(raised.exception.context.error_class, "binance_invalid_json")

    def test_business_rate_limit_is_finite_and_business_error_is_fail_closed(self) -> None:
        transport = QueueTransport(
            json_response({"code": "100004"}),
            json_response({"code": "100002"}),
        )
        sleeps: list[float] = []
        client = BinanceWeb3Client(transport=transport, sleep=sleeps.append)
        with self.assertRaises(BinanceWeb3Error) as raised:
            client.request_json("meme_rush", body={"chainId": "CT_501", "rankType": 10})
        self.assertEqual(raised.exception.context.error_class, "binance_business_error")
        self.assertEqual(len(transport.calls), 2)
        self.assertEqual(len(sleeps), 1)

    def test_non_secret_environment_configuration_is_applied(self) -> None:
        transport = QueueTransport(json_response({"code": "000000", "data": []}))
        with patch.dict(
            os.environ,
            {
                "BINANCE_WEB3_BASE_URL": "https://example.invalid",
                "BINANCE_WEB3_TIMEOUT_SEC": "3",
                "BINANCE_WEB3_MAX_RETRIES": "0",
                "BINANCE_WEB3_MAX_RESPONSE_BYTES": "2048",
            },
            clear=False,
        ):
            client = BinanceWeb3Client.from_env(transport=transport, sleep=lambda _: None)
            client.request_json("meme_rush", body={"chainId": "CT_501", "rankType": 10})
        self.assertTrue(transport.calls[0][1].startswith("https://example.invalid/"))
        self.assertEqual(transport.calls[0][2]["User-Agent"], "binance-web3/2.0 (Skill)")

    def test_data_source_selection_is_explicit(self) -> None:
        self.assertEqual(DataSourceConfig.from_mapping({}).data_source, "fixture")
        self.assertEqual(DataSourceConfig.from_mapping({"DATA_SOURCE": "binance_web3"}).data_source, "binance_web3")
        with self.assertRaises(ValueError):
            DataSourceConfig.from_mapping({"DATA_SOURCE": "live"})


class BinanceWeb3NormalizerTests(unittest.TestCase):
    def test_meme_row_preserves_missing_timestamp_as_unavailable(self) -> None:
        row = load_fixture("meme_rush_normal.json")["data"][0]
        now = datetime.now(timezone.utc)
        record = normalize_meme_row(row, fetched_at=now, historical_bootstrap=True)
        self.assertEqual(record.signal.mint, row["contractAddress"])
        self.assertTrue(record.historical_bootstrap)
        self.assertFalse(record.fields["token_created_at"].available)
        self.assertEqual(record.fields["token_created_at"].parse_error, "timestamp_unit_unknown")
        self.assertEqual(record.fields["count_buy_24h"].value, 12)

    def test_dynamic_maps_windows_without_filling_missing_fields(self) -> None:
        payload = load_fixture("token_dynamic_normal.json")
        transport = QueueTransport(json_response(payload))
        snapshot = BinanceWeb3MarketDataAdapter(BinanceWeb3Client(transport=transport)).snapshot("MintA")
        self.assertEqual(snapshot.value("price_usd"), Decimal("1.25"))
        self.assertEqual(snapshot.value("net_buy_usd_5m"), Decimal("11.5"))
        self.assertIsNone(snapshot.value("net_buy_usd_1h"))
        self.assertFalse(snapshot.fields["net_buy_usd_1h"].available)

    def test_live_response_subsets_are_normalizable_without_promoting_unknown_time(self) -> None:
        now = datetime.now(timezone.utc)
        meme = load_fixture("meme_rush_live.json")
        meme_record = normalize_meme_row(meme["data"][0], fetched_at=now, historical_bootstrap=True)
        self.assertTrue(meme_record.fields["holders"].available)
        self.assertFalse(meme_record.fields["token_created_at"].available)

        smart = load_fixture("smart_money_live.json")
        smart_record = normalize_smart_money_row(smart["data"][0], fetched_at=now, historical_bootstrap=True)
        self.assertIsNotNone(smart_record.source_timestamp)

        dynamic = load_fixture("token_dynamic_live.json")
        snapshot = normalize_dynamic(dynamic["data"], mint="live", chain_id="CT_501", fetched_at=now)
        self.assertEqual(snapshot.value("market_cap_usd"), Decimal("4129.854997735061109808567714390000000000"))
        self.assertTrue(snapshot.fields["volume_usd_5m"].available)
        self.assertFalse(snapshot.fields["net_buy_usd_5m"].available)


class BinanceWeb3AdapterTests(unittest.TestCase):
    def test_bsc_meme_rush_uses_chain_56_and_preserves_chain_identity(self) -> None:
        payload = load_fixture("meme_rush_normal.json")
        transport = QueueTransport(json_response(payload))
        source = BinanceWeb3SignalSource(
            BinanceWeb3Client(transport=transport),
            chain_id="56",
            limit=2,
        )
        records = source.fetch_once()
        self.assertEqual(len(records), 2)
        self.assertTrue(all(record.signal.chain == "bsc" for record in records))
        self.assertTrue(all(record.chain_id == "56" for record in records))
        request_body = json.loads(transport.calls[0][3].decode("utf-8"))
        self.assertEqual(request_body["chainId"], "56")

    def test_bootstrap_and_duplicate_suppression(self) -> None:
        payload = load_fixture("meme_rush_normal.json")
        new_row = dict(payload["data"][0])
        new_row["contractAddress"] = "MintNew"
        new_row["symbol"] = "NEW"
        transport = QueueTransport(json_response(payload), json_response({"code": "000000", "data": [new_row]}))
        source = BinanceWeb3SignalSource(BinanceWeb3Client(transport=transport), limit=3)
        first = source.fetch_once()
        second = source.fetch_once()
        self.assertEqual(len(first), 2)
        self.assertTrue(all(row.historical_bootstrap for row in first))
        self.assertEqual(len(second), 1)
        self.assertFalse(second[0].historical_bootstrap)

    def test_smart_money_is_shadow_only(self) -> None:
        transport = QueueTransport(json_response(load_fixture("smart_money_normal.json")))
        adapter = BinanceWeb3SmartMoneyAdapter(BinanceWeb3Client(transport=transport))
        records = adapter.fetch(page_size=3)
        self.assertTrue(adapter.shadow_feature_only)
        self.assertFalse(adapter.trigger_entry)
        self.assertEqual(len(records), 1)
        self.assertIsNotNone(records[0].normalized.source_timestamp)

    def test_kline_normalizes_and_rejects_duplicate_timestamps(self) -> None:
        payload = load_fixture("kline_normal.json")
        transport = QueueTransport(json_response(payload))
        result = BinanceWeb3KlineAdapter(BinanceWeb3Client(transport=transport)).candles("MintA", limit=3)
        self.assertEqual(len(result.candles), 2)
        self.assertLess(result.candles[0].open_time_ms, result.candles[1].open_time_ms)
        self.assertIn("platform=solana", transport.calls[0][1])
        self.assertIn("address=MintA", transport.calls[0][1])
        self.assertNotIn("contractAddress=", transport.calls[0][1])

        duplicate = {"data": [payload["data"][0], payload["data"][0]], "status": {"error_code": 0}}
        duplicate_transport = QueueTransport(json_response(duplicate))
        with self.assertRaises(BinanceWeb3Error) as raised:
            BinanceWeb3KlineAdapter(BinanceWeb3Client(transport=duplicate_transport)).candles("MintA", limit=3)
        self.assertEqual(raised.exception.context.error_class, "binance_kline_duplicate")

    def test_live_kline_subset_is_normalizable(self) -> None:
        transport = QueueTransport(json_response(load_fixture("kline_live.json")))
        result = BinanceWeb3KlineAdapter(BinanceWeb3Client(transport=transport)).candles(
            "Er4q21XgvtaSRq3vpJYRzn2Vy64ezcVZ7XFfeNhZpump", limit=3
        )
        self.assertEqual(len(result.candles), 1)
        self.assertGreater(result.candles[0].open_time_ms, 100_000_000_000)

    def test_redaction_is_recursive_and_fixture_safe(self) -> None:
        value = {"Authorization": "Bearer abc", "data": [{"apiKey": "secret", "price": "1"}]}
        redacted = redact_payload(value)
        self.assertEqual(redacted["Authorization"], "[REDACTED]")
        self.assertEqual(redacted["data"][0]["apiKey"], "[REDACTED]")
        self.assertTrue(contains_sensitive_patterns(value))
        self.assertFalse(contains_sensitive_patterns({"price": "1", "mint": "MintA"}))


if __name__ == "__main__":
    unittest.main()
