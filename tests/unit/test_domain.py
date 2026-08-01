from __future__ import annotations

import unittest

from meme_system.domain.models import BASELINE_IDENTITY


class DomainIdentityTests(unittest.TestCase):
    def test_frozen_baseline_identity(self) -> None:
        self.assertEqual(BASELINE_IDENTITY.strategy_name, "sol_ultra_early_baseline")
        self.assertEqual(BASELINE_IDENTITY.ruleset_name, "ultra_early_minimal")
        self.assertEqual(BASELINE_IDENTITY.ruleset_version, "0.1.0")
        self.assertEqual(BASELINE_IDENTITY.config_version, "0.1.0")

    def test_lifecycle_key_excludes_mode_and_includes_strategy_version(self) -> None:
        self.assertEqual(
            BASELINE_IDENTITY.lifecycle_key("MintCaseSensitive"),
            ("MintCaseSensitive", "sol_ultra_early_baseline", "0.1.0"),
        )

