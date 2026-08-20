from __future__ import annotations

import unittest
from decimal import Decimal

from meme_system.domain.models import BASELINE_IDENTITY, BSC_BASELINE_IDENTITY
from meme_system.domain.naming import clean_token_name
from meme_system.strategies.baseline import bsc_baseline_config


class DomainIdentityTests(unittest.TestCase):
    def test_frozen_baseline_identity(self) -> None:
        self.assertEqual(BASELINE_IDENTITY.strategy_name, "sol_ultra_early_baseline")
        self.assertEqual(BASELINE_IDENTITY.ruleset_name, "ultra_early_minimal")
        self.assertEqual(BASELINE_IDENTITY.ruleset_version, "0.1.2")
        self.assertEqual(BASELINE_IDENTITY.config_version, "0.1.2")

    def test_lifecycle_key_excludes_mode_and_includes_strategy_version(self) -> None:
        self.assertEqual(
            BASELINE_IDENTITY.lifecycle_key("MintCaseSensitive"),
            ("MintCaseSensitive", "sol_ultra_early_baseline", "0.1.2"),
        )

    def test_bsc_strategy_overlay_is_versioned_and_does_not_change_solana_defaults(self) -> None:
        config = bsc_baseline_config()
        self.assertEqual(BSC_BASELINE_IDENTITY.ruleset_name, "ultra_early_selective_bsc")
        self.assertEqual(BSC_BASELINE_IDENTITY.ruleset_version, "0.1.5")
        self.assertEqual(config.min_holders, 100)
        self.assertTrue(config.min_holders_inclusive)
        self.assertEqual(config.observation_delay_sec, 60)
        self.assertTrue(config.require_holders_non_decreasing_after_observation)
        self.assertEqual(config.shadow_holders_drop_pct, Decimal("0.10"))
        self.assertEqual(config.shadow_liquidity_drop_pct, Decimal("0.15"))
        self.assertEqual(config.stop_loss_trigger_pct, Decimal("-0.10"))
        self.assertEqual(BASELINE_IDENTITY.ruleset_version, "0.1.2")

    def test_display_name_strips_unicode_direction_controls_without_changing_raw_value(self) -> None:
        raw_name = "Alpha\u202eSOL\u2066Token\u2069"
        self.assertEqual(clean_token_name(raw_name), "AlphaSOLToken")
        self.assertEqual(raw_name, "Alpha\u202eSOL\u2066Token\u2069")
