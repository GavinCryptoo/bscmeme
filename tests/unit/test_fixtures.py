from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal

from meme_system.adapters.fixtures import FixtureSignalSource, ReplayQuoteProvider
from meme_system.adapters.protocols import ExecutableQuote
from meme_system.domain.models import Signal


class FixtureAdapterTests(unittest.TestCase):
    def test_fixture_signal_source_is_deterministic(self) -> None:
        signal = Signal(
            signal_id="sig-1",
            mint="MintCaseSensitive",
            observed_at=datetime.now(timezone.utc),
            source="fixture",
        )
        self.assertEqual(FixtureSignalSource([signal]).signals(), (signal,))

    def test_replay_quote_provider_matches_exact_request(self) -> None:
        quote = ExecutableQuote(
            quote_id="quote-1",
            mint="MintCaseSensitive",
            side="buy",
            input_quantity=Decimal("0.001"),
            output_quantity=Decimal("100"),
            route_fee=Decimal("0.00001"),
            price_impact_pct=Decimal("1.2"),
            quoted_at=datetime.now(timezone.utc),
            age_ms=10,
        )
        provider = ReplayQuoteProvider([quote])
        self.assertEqual(provider.quote(quote.mint, quote.side, quote.input_quantity), quote)
        self.assertIsNone(
            provider.quote(quote.mint.lower(), quote.side, quote.input_quantity)
        )

