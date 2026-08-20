from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from meme_system.runtime_ops import RuntimeControl
from meme_system.telegram_control import TelegramConfig, TelegramControl


class TelegramLiveControlTests(unittest.TestCase):
    def _bot(self, calls: list[tuple[str, dict[str, object]]], control: RuntimeControl) -> TelegramControl:
        def transport(url: str, params: dict[str, object], timeout: float) -> tuple[int, bytes]:
            method = url.rsplit("/", 1)[-1]
            calls.append((method, dict(params)))
            if method == "getUpdates":
                return 200, json.dumps({"ok": True, "result": []}).encode()
            return 200, json.dumps({"ok": True, "result": {"message_id": 1}}).encode()

        return TelegramControl(
            TelegramConfig(enabled=True, bot_token="test-token", chat_id="42"),
            control,
            transport=transport,
            allowed_modes=("live",),
        )

    def test_live_commands_only_pause_new_entries(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            calls: list[tuple[str, dict[str, object]]] = []
            control = RuntimeControl(Path(directory) / "live-control.json")
            bot = self._bot(calls, control)

            self.assertEqual(bot.handle_command("/live_pause"), "live_pause")
            self.assertTrue(control.paused("live"))
            self.assertIsNone(bot.handle_command("/live_sell"))
            self.assertEqual(bot.handle_command("/live_resume"), "live_resume")
            self.assertFalse(control.paused("live"))

            sent = [params for method, params in calls if method == "sendMessage"]
            self.assertEqual(len(sent), 2)
            self.assertIn("已有持仓继续", str(sent[0]["text"]))

    def test_live_event_contains_copy_button_but_not_secret(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            calls: list[tuple[str, dict[str, object]]] = []
            bot = self._bot(calls, RuntimeControl(Path(directory) / "live-control.json"))
            self.assertTrue(bot.notify_event(
                "LIVE_ENTRY_CONFIRMED",
                {
                    "mint": "0xToken",
                    "token_name": "TEST",
                    "strategy_name": "bsc_binance_indicative",
                    "input_quantity": "0.001",
                    "actual_received": "1000",
                    "tx_hash": "0xTx",
                    "settlement_verified": True,
                    "private_key": "must-not-appear",
                },
            ))
            method, params = calls[-1]
            self.assertEqual(method, "sendMessage")
            self.assertNotIn("must-not-appear", json.dumps(params, ensure_ascii=False))
            markup = params["reply_markup"]
            self.assertEqual(markup["inline_keyboard"][0][0]["copy_text"]["text"], "0xToken")
            self.assertEqual(markup["inline_keyboard"][0][1]["callback_data"], "live_pause")

    def test_live_trade_notifications_include_persisted_trade_fields_and_stateful_entry_button(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            calls: list[tuple[str, dict[str, object]]] = []
            control = RuntimeControl(Path(directory) / "live-control.json")
            bot = self._bot(calls, control)
            control.set_paused("live", True)
            self.assertTrue(bot.notify_event(
                "LIVE_ENTRY_CONFIRMED",
                {
                    "mint": "0xToken", "token_name": "TEST", "strategy_name": "Balanced",
                    "occurred_at": "2026-08-15T00:00:00+00:00", "entry_price_native": "0.00001",
                    "entry_holders": 123, "input_quantity": "0.001", "entry_liquidity_usd": "5000",
                    "actual_received": "100", "settlement_verified": True,
                },
            ))
            sent = [params for method, params in calls if method == "sendMessage"][-1]
            text = str(sent["text"])
            for expected in ("买入价格：0.00001", "买入持币地址：123", "买入金额：0.001", "买入流动性：5000"):
                self.assertIn(expected, text)
            self.assertEqual(sent["reply_markup"]["inline_keyboard"][0][1]["callback_data"], "live_resume")

            self.assertTrue(bot.notify_event(
                "LIVE_EXIT_CONFIRMED",
                {
                    "mint": "0xToken", "token_name": "TEST", "strategy_name": "Balanced",
                    "occurred_at": "2026-08-15T00:01:00+00:00", "exit_price_native": "0.00002",
                    "exit_holders": 150, "exit_liquidity_usd": "6000", "actual_received": "0.002",
                    "return_pct": "100", "reason": "TP", "settlement_verified": True,
                },
            ))
            text = str([params for method, params in calls if method == "sendMessage"][-1]["text"])
            for expected in ("卖出价格：0.00002", "卖出持币地址：150", "卖出流动性：6000"):
                self.assertIn(expected, text)

    def test_callback_query_is_allowlisted_chat_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            calls: list[tuple[str, dict[str, object]]] = []
            control = RuntimeControl(Path(directory) / "live-control.json")

            def transport(url: str, params: dict[str, object], timeout: float) -> tuple[int, bytes]:
                method = url.rsplit("/", 1)[-1]
                calls.append((method, dict(params)))
                if method == "getUpdates":
                    return 200, json.dumps({"ok": True, "result": [{
                        "update_id": 7,
                        "callback_query": {
                            "id": "callback-1",
                            "data": "live_pause",
                            "message": {"chat": {"id": 42}},
                        },
                    }]}).encode()
                return 200, json.dumps({"ok": True, "result": True}).encode()

            bot = TelegramControl(
                TelegramConfig(enabled=True, bot_token="test-token", chat_id="42"),
                control,
                transport=transport,
                allowed_modes=("live",),
            )
            self.assertEqual(bot.poll_once(), ("live_pause",))
            self.assertTrue(control.paused("live"))
            self.assertIn("answerCallbackQuery", [method for method, _ in calls])


if __name__ == "__main__":
    unittest.main()
