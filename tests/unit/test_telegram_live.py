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
