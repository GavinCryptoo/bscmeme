"""Optional Telegram notifications and Paper/Shadow pause controls.

Only allowlisted control commands are accepted. The bot token and chat id are
read from the environment and are never included in status or error payloads.
There is deliberately no Live command and no wallet/execution integration.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from meme_system.runtime_ops import RuntimeControl


@dataclass(frozen=True)
class TelegramConfig:
    enabled: bool = False
    bot_token: str = ""
    chat_id: str = ""
    api_base_url: str = "https://api.telegram.org"

    @classmethod
    def from_env(cls) -> "TelegramConfig":
        return cls(
            enabled=os.environ.get("TELEGRAM_ENABLED", "false").strip().lower() == "true",
            bot_token=os.environ.get("TELEGRAM_BOT_TOKEN", "").strip(),
            chat_id=os.environ.get("TELEGRAM_CHAT_ID", "").strip(),
            api_base_url=os.environ.get("TELEGRAM_API_BASE_URL", "https://api.telegram.org").strip().rstrip("/"),
        )

    def validate(self) -> None:
        if self.enabled and (not self.bot_token or not self.chat_id):
            raise ValueError("Telegram control requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")

    def safe_status(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "bot_token_configured": bool(self.bot_token),
            "chat_id_configured": bool(self.chat_id),
            "read_only_controls": True,
        }


class TelegramControl:
    def __init__(
        self,
        config: TelegramConfig,
        control: RuntimeControl,
        *,
        status_provider: Callable[[], Mapping[str, object]] | None = None,
        transport: Callable[[str, Mapping[str, object], float], tuple[int, bytes]] | None = None,
        timeout_sec: float = 10.0,
    ) -> None:
        config.validate()
        self.config = config
        self.control = control
        self.status_provider = status_provider or control.snapshot
        self.transport = transport or self._transport
        self.timeout_sec = max(0.1, min(60.0, timeout_sec))
        self.offset: int | None = None
        self.last_error_class: str | None = None

    def send_message(self, text: str) -> bool:
        if not self.config.enabled:
            return False
        payload = {"chat_id": self.config.chat_id, "text": text[:4000], "disable_web_page_preview": "true"}
        result = self._call("sendMessage", payload)
        return bool(result)

    def poll_once(self) -> tuple[str, ...]:
        if not self.config.enabled:
            return ()
        params: dict[str, str] = {"limit": "50", "timeout": "0", "allowed_updates": json.dumps(["message"])}
        if self.offset is not None:
            params["offset"] = str(self.offset)
        result = self._call("getUpdates", params)
        if not isinstance(result, list):
            return ()
        actions: list[str] = []
        for update in result:
            if not isinstance(update, Mapping):
                continue
            update_id = update.get("update_id")
            if isinstance(update_id, int):
                self.offset = update_id + 1
            message = update.get("message")
            if not isinstance(message, Mapping) or not self._allowed_chat(message):
                continue
            text = message.get("text")
            if not isinstance(text, str):
                continue
            action = self.handle_command(text.strip())
            if action is not None:
                actions.append(action)
        return tuple(actions)

    def handle_command(self, command: str) -> str | None:
        parts = command.split()
        if not parts:
            return None
        commands = {
            "/paper_pause": ("paper", True),
            "/paper_resume": ("paper", False),
            "/shadow_pause": ("shadow", True),
            "/shadow_resume": ("shadow", False),
        }
        if command == "/status":
            self.send_message(json.dumps(self.status_provider(), ensure_ascii=False, default=str)[:3500])
            return "status"
        selected = commands.get(parts[0])
        if selected is None:
            return None
        mode, paused = selected
        self.control.set_paused(mode, paused)
        self.send_message(f"{mode} new entries {'paused' if paused else 'resumed'}")
        return f"{mode}_{'pause' if paused else 'resume'}"

    def _allowed_chat(self, message: Mapping[str, object]) -> bool:
        chat = message.get("chat")
        if not isinstance(chat, Mapping):
            return False
        return str(chat.get("id", "")) == self.config.chat_id

    def _call(self, method: str, params: Mapping[str, object]) -> object | None:
        try:
            url = f"{self.config.api_base_url}/bot{self.config.bot_token}/{method}"
            status, body = self.transport(url, params, self.timeout_sec)
            if status >= 400:
                self.last_error_class = "telegram_http_error"
                return None
            parsed = json.loads(body.decode("utf-8"))
            if not isinstance(parsed, Mapping) or parsed.get("ok") is not True:
                self.last_error_class = "telegram_schema_changed"
                return None
            return parsed.get("result")
        except (TimeoutError, URLError, OSError):
            self.last_error_class = "telegram_connection_error"
            return None
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
            self.last_error_class = "telegram_schema_changed"
            return None

    @staticmethod
    def _transport(url: str, params: Mapping[str, object], timeout_sec: float) -> tuple[int, bytes]:
        request = Request(
            url=url,
            data=json.dumps(dict(params), ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=timeout_sec) as response:
                return response.status, response.read(1_000_001)
        except HTTPError as exc:
            return exc.code, exc.read(1_000_001)
