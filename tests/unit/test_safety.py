from __future__ import annotations

import unittest

from meme_system.config.safety import SafetyConfig, SafetyViolation


class SafetyConfigTests(unittest.TestCase):
    def test_safe_defaults_are_valid(self) -> None:
        config = SafetyConfig.from_mapping({})
        self.assertTrue(config.paper_only)
        self.assertFalse(config.live_trading)
        self.assertFalse(config.wallet_enabled)
        self.assertFalse(config.signing_enabled)
        self.assertFalse(config.broadcast_enabled)
        self.assertFalse(config.telegram_enabled)

    def test_any_dangerous_capability_fails_closed(self) -> None:
        for name in (
            "LIVE_TRADING",
            "WALLET_ENABLED",
            "SIGNING_ENABLED",
            "BROADCAST_ENABLED",
        ):
            with self.subTest(name=name):
                with self.assertRaises(SafetyViolation):
                    SafetyConfig.from_mapping({name: "true"})

    def test_paper_only_false_fails_closed(self) -> None:
        with self.assertRaises(SafetyViolation):
            SafetyConfig.from_mapping({"PAPER_ONLY": "false"})

    def test_telegram_can_be_enabled_for_gate_a_controls(self) -> None:
        config = SafetyConfig.from_mapping({"TELEGRAM_ENABLED": "true"})
        self.assertTrue(config.telegram_enabled)

    def test_invalid_boolean_fails_closed(self) -> None:
        with self.assertRaises(SafetyViolation):
            SafetyConfig.from_mapping({"LIVE_TRADING": "yes"})
