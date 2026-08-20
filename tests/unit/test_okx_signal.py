from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone

from meme_system.adapters.okx_signal import BscDiscoverySource, OkxBscSignalSource, _HttpResponse


NOW = datetime(2026, 8, 16, 4, 0, tzinfo=timezone.utc)
TOKEN = "0x1111111111111111111111111111111111111111"


class OkxSignalSourceTests(unittest.TestCase):
    def _source(self, payload: object, *, now: datetime = NOW) -> OkxBscSignalSource:
        def transport(_request, _timeout):
            return _HttpResponse(200, json.dumps(payload).encode())

        return OkxBscSignalSource(
            api_key="key",
            secret="secret",
            passphrase="passphrase",
            clock=lambda: now,
            transport=transport,
        )

    @staticmethod
    def _payload(*, timestamp: datetime = NOW) -> dict[str, object]:
        return {
            "code": "0",
            "data": [{
                "timestamp": str(int(timestamp.timestamp() * 1000)),
                "walletType": "SMART_MONEY",
                "triggerWalletCount": "2",
                "triggerWalletAddress": "0xaaa,0xbbb",
                "amountUsd": "123.45",
                "soldRatioPercent": "10",
                "token": {
                    "tokenAddress": TOKEN,
                    "symbol": "OKX",
                    "name": "OKX test",
                    "marketCapUsd": "5000",
                    "holders": "31",
                    "top10HolderPercent": "12.5",
                    "price": "0.000009",
                },
            }],
        }

    def test_normalizes_signal_without_promoting_price_to_price_usd(self) -> None:
        source = self._source(self._payload())
        records = source.fetch_once()
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record.signal.source, "okx_signal")
        self.assertEqual(record.signal.mint, TOKEN)
        self.assertEqual(record.fields["okx_wallet_type"].value, "SMART_MONEY")
        self.assertEqual(str(record.fields["okx_amount_usd"].value), "123.45")
        self.assertNotIn("price_usd", record.fields)
        self.assertEqual(str(record.fields["okx_signal_price_reference"].value), "0.000009")
        self.assertEqual(source.stats()["signals_received"], 1)

    def test_startup_only_admits_signals_from_last_five_minutes(self) -> None:
        source = self._source(self._payload(timestamp=NOW - timedelta(minutes=6)))
        self.assertEqual(source.fetch_once(), ())
        self.assertEqual(source.stats()["state"], "HEALTHY")

    def test_duplicate_event_is_counted_but_not_re_emitted(self) -> None:
        source = self._source(self._payload())
        self.assertEqual(len(source.fetch_once()), 1)
        source._last_poll_monotonic = float("-inf")  # exercise a second network poll deterministically
        self.assertEqual(source.fetch_once(), ())
        self.assertEqual(source.stats()["duplicate_signals"], 1)

    def test_secondary_failure_does_not_drop_primary_discovery(self) -> None:
        class Primary:
            def fetch_once(self):
                return ("binance-record",)

            def stats(self):
                return {"last_fetched": 1}

        class BrokenOkx:
            def fetch_once(self):
                raise AssertionError("unexpected")

            def stats(self):
                return {"state": "DEGRADED", "api_errors": 1}

        # The production adapter itself catches HTTP/network failures. This
        # verifies the merger also remains additive for an unexpected optional
        # feed exception.
        merged = BscDiscoverySource(Primary(), BrokenOkx())
        self.assertEqual(merged.fetch_once(), ("binance-record",))
        self.assertEqual(merged.stats()["okx_signal"]["state"], "DEGRADED")
