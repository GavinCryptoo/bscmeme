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
        self.assertFalse(config.bsc_live_enabled)

    def test_dangerous_capabilities_fail_closed_without_bsc_live_switch(self) -> None:
        for name in (
            "LIVE_TRADING",
            "BSC_LIVE_ENABLED",
            "WALLET_ENABLED",
            "SIGNING_ENABLED",
            "BROADCAST_ENABLED",
        ):
            with self.subTest(name=name):
                with self.assertRaises(SafetyViolation):
                    SafetyConfig.from_mapping({name: "true"})

    def test_bsc_live_switches_must_be_enabled_together(self) -> None:
        config = SafetyConfig.from_mapping({
            "PAPER_ONLY": "false",
            "LIVE_TRADING": "true",
            "BSC_LIVE_ENABLED": "true",
        })
        config.validate_for_mode(chain="bsc", mode="live")
        with self.assertRaises(SafetyViolation):
            config.validate_for_mode(chain="solana", mode="live")

    def test_bsc_live_telegram_is_allowed_for_safe_controls(self) -> None:
        config = SafetyConfig.from_mapping({
            "PAPER_ONLY": "false",
            "LIVE_TRADING": "true",
            "BSC_LIVE_ENABLED": "true",
            "TELEGRAM_ENABLED": "true",
        })
        config.validate_for_mode(chain="bsc", mode="live")

    def test_bsc_live_switches_cannot_start_paper(self) -> None:
        config = SafetyConfig.from_mapping({
            "PAPER_ONLY": "false",
            "LIVE_TRADING": "true",
            "BSC_LIVE_ENABLED": "true",
        })
        with self.assertRaises(SafetyViolation):
            config.validate_for_mode(chain="bsc", mode="paper")

    def test_paper_only_and_live_trading_conflict_is_explicit(self) -> None:
        with self.assertRaisesRegex(SafetyViolation, "PAPER_ONLY=true conflicts with LIVE_TRADING=true"):
            SafetyConfig.from_mapping({
                "PAPER_ONLY": "true",
                "LIVE_TRADING": "true",
                "BSC_LIVE_ENABLED": "true",
            })

    def test_bsc_live_requires_paper_only_false(self) -> None:
        config = SafetyConfig.from_mapping({
            "PAPER_ONLY": "false",
            "LIVE_TRADING": "true",
            "BSC_LIVE_ENABLED": "true",
        })
        config.validate_for_mode(chain="bsc", mode="live")

    def test_paper_only_false_fails_closed(self) -> None:
        with self.assertRaises(SafetyViolation):
            SafetyConfig.from_mapping({"PAPER_ONLY": "false"})

    def test_telegram_can_be_enabled_for_gate_a_controls(self) -> None:
        config = SafetyConfig.from_mapping({"TELEGRAM_ENABLED": "true"})
        self.assertTrue(config.telegram_enabled)

    def test_invalid_boolean_fails_closed(self) -> None:
        with self.assertRaises(SafetyViolation):
            SafetyConfig.from_mapping({"LIVE_TRADING": "yes"})
