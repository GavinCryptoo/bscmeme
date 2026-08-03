"""Optional Telegram notifications and safe new-entry controls.

Only allowlisted control commands are accepted. The bot token and chat id are
read from the environment and are never included in status or error payloads.
There are no wallet, order, amount, slippage, or process controls.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Callable, Mapping, Sequence
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
            "new_entry_controls_only": True,
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
        allowed_modes: Sequence[str] = ("paper", "shadow"),
    ) -> None:
        config.validate()
        invalid_modes = set(allowed_modes) - {"paper", "shadow", "live"}
        if invalid_modes:
            raise ValueError("unsupported Telegram control mode")
        self.config = config
        self.control = control
        self.status_provider = status_provider or control.snapshot
        self.transport = transport or self._transport
        self.timeout_sec = max(0.1, min(60.0, timeout_sec))
        self.allowed_modes = tuple(dict.fromkeys(allowed_modes))
        self.offset: int | None = None
        self.last_error_class: str | None = None

    def send_message(self, text: str, *, reply_markup: Mapping[str, object] | None = None) -> bool:
        if not self.config.enabled:
            return False
        payload = {"chat_id": self.config.chat_id, "text": text[:4000], "disable_web_page_preview": "true"}
        if reply_markup is not None:
            payload["reply_markup"] = dict(reply_markup)
        result = self._call("sendMessage", payload)
        return bool(result)

    def notify_event(
        self,
        event_type: str,
        payload: Mapping[str, object],
        *,
        chain: str = "BSC",
        mode: str = "live",
    ) -> bool:
        """Send only whitelisted operational events; never send secrets."""

        if not self.config.enabled or mode not in self.allowed_modes:
            return False
        text = _format_event(event_type, payload, chain=chain)
        if text is None:
            return False
        return self.send_message(text, reply_markup=_event_markup(payload, live=mode == "live"))

    def poll_once(self) -> tuple[str, ...]:
        if not self.config.enabled:
            return ()
        params: dict[str, str] = {
            "limit": "50",
            "timeout": "0",
            "allowed_updates": json.dumps(["message", "callback_query"]),
        }
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
            callback = update.get("callback_query")
            if isinstance(callback, Mapping):
                action = self._handle_callback(callback)
                if action is not None:
                    actions.append(action)
                continue
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
        commands: dict[str, tuple[str, bool]] = {}
        if "paper" in self.allowed_modes:
            commands.update({"/paper_pause": ("paper", True), "/paper_resume": ("paper", False)})
        if "shadow" in self.allowed_modes:
            commands.update({"/shadow_pause": ("shadow", True), "/shadow_resume": ("shadow", False)})
        if "live" in self.allowed_modes:
            commands.update({
                "/live_pause": ("live", True),
                "/live_resume": ("live", False),
                "/bsc_live_pause": ("live", True),
                "/bsc_live_resume": ("live", False),
            })
        if parts[0] in {"/start", "/help"}:
            self.send_message(_help_text(self.allowed_modes), reply_markup=_control_markup(self.allowed_modes))
            return "help"
        if parts[0] == "/status":
            self.send_message(json.dumps(self.status_provider(), ensure_ascii=False, default=str)[:3500])
            return "status"
        selected = commands.get(parts[0])
        if selected is None:
            return None
        mode, paused = selected
        self.control.set_paused(mode, paused)
        self.send_message(
            f"{_mode_label(mode)}新开仓已{'暂停' if paused else '恢复'}。\n已有持仓继续按原策略监控和退出。",
            reply_markup=_control_markup(self.allowed_modes),
        )
        return f"{mode}_{'pause' if paused else 'resume'}"

    def _handle_callback(self, callback: Mapping[str, object]) -> str | None:
        callback_id = callback.get("id")
        message = callback.get("message")
        if not isinstance(callback_id, str) or not isinstance(message, Mapping) or not self._allowed_chat(message):
            return None
        data = callback.get("data")
        if not isinstance(data, str):
            self._call("answerCallbackQuery", {"callback_query_id": callback_id})
            return None
        self._call("answerCallbackQuery", {"callback_query_id": callback_id})
        return self.handle_command(data if data.startswith("/") else "/" + data)

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


def _mode_label(mode: str) -> str:
    return {"paper": "Paper", "shadow": "Shadow", "live": "BSC Live"}.get(mode, mode)


def _help_text(modes: Sequence[str]) -> str:
    lines = ["Telegram 控制已接入。", "可用操作：", "/status 查看当前状态"]
    if "live" in modes:
        lines.extend([
            "/live_pause 暂停 BSC Live 新开仓",
            "/live_resume 恢复 BSC Live 新开仓",
            "已有持仓不会因暂停而停止监控或退出。",
        ])
    if "paper" in modes:
        lines.extend(["/paper_pause 暂停 Paper 新开仓", "/paper_resume 恢复 Paper 新开仓"])
    if "shadow" in modes:
        lines.extend(["/shadow_pause 暂停 Shadow 新开仓", "/shadow_resume 恢复 Shadow 新开仓"])
    return "\n".join(lines)


def _control_markup(modes: Sequence[str]) -> dict[str, object]:
    buttons: list[dict[str, str]] = [{"text": "状态", "callback_data": "status"}]
    if "live" in modes:
        buttons.extend([
            {"text": "暂停新开仓", "callback_data": "live_pause"},
            {"text": "恢复新开仓", "callback_data": "live_resume"},
        ])
    return {"inline_keyboard": [buttons]}


def _event_markup(payload: Mapping[str, object], *, live: bool) -> dict[str, object] | None:
    rows: list[list[dict[str, str]]] = []
    mint = str(payload.get("mint") or "").strip()
    if mint:
        rows.append([{"text": "复制合约", "copy_text": {"text": mint}}])
    if live:
        rows.append([
            {"text": "状态", "callback_data": "status"},
            {"text": "暂停新开仓", "callback_data": "live_pause"},
            {"text": "恢复新开仓", "callback_data": "live_resume"},
        ])
    return {"inline_keyboard": rows} if rows else None


def _format_event(event_type: str, payload: Mapping[str, object], *, chain: str) -> str | None:
    mint = str(payload.get("mint") or "—")
    symbol = str(payload.get("token_name") or payload.get("symbol") or "—")
    strategy = str(payload.get("strategy_name") or "—")
    occurred_at = str(payload.get("occurred_at") or payload.get("recorded_at") or "—")
    tx_hash = str(payload.get("tx_hash") or "—")
    if event_type == "LIVE_ENTRY_CONFIRMED":
        return (
            f"🟢 {chain} Live 买入成功\n"
            f"策略：{strategy}\n代币：{symbol}\n合约：{mint}\n时间：{occurred_at}\n"
            f"投入：{payload.get('input_quantity', '—')} BNB\n"
            f"实际到账：{payload.get('actual_received', '—')}\n交易哈希：{tx_hash}\n"
            f"到账核对：{'✅' if payload.get('settlement_verified') else '❌'}"
        )
    if event_type == "LIVE_EXIT_CONFIRMED":
        return (
            f"🔴 {chain} Live 卖出成功\n"
            f"策略：{strategy}\n代币：{symbol}\n合约：{mint}\n时间：{occurred_at}\n"
            f"原因：{payload.get('reason', '—')}\n实际到账：{payload.get('actual_received', '—')} BNB\n"
            f"收益率：{payload.get('return_pct', '—')}\n交易哈希：{tx_hash}\n"
            f"到账核对：{'✅' if payload.get('settlement_verified') else '❌'}"
        )
    if event_type in {"LIVE_ENTRY_FAILED", "LIVE_EXIT_FAILED"}:
        action = "买入" if event_type == "LIVE_ENTRY_FAILED" else "卖出"
        return (
            f"⚠️ {chain} Live {action}失败\n"
            f"策略：{strategy}\n代币：{symbol}\n合约：{mint}\n时间：{occurred_at}\n"
            f"原因分类：{payload.get('error_class', '—')}\n自动重试：否"
        )
    if event_type == "LIVE_ENTRY_LIMIT_REACHED":
        return (
            f"🟡 {chain} Live 新开仓已拦截\n"
            f"策略：{strategy}\n代币：{symbol}\n合约：{mint}\n"
            f"原因：已达到最大入场笔数 {payload.get('max_entries', '—')}"
        )
    if event_type == "BSC_LIVE_STARTED":
        return "✅ BSC Live 已启动\nTelegram 仅提供状态查看及暂停/恢复新开仓。"
    if event_type == "BSC_LIVE_STOPPED":
        return "⏹ BSC Live 已停止\n未新增 Telegram 控制权限。"
    return None
