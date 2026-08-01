"""Local read-only Dashboard HTTP service with safe Paper/Shadow controls."""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import Any, Mapping
from urllib.parse import parse_qs, urlparse

from meme_system.config.dashboard import DashboardConfig
from meme_system.config.runtime import RuntimePaths
from meme_system.config.safety import SafetyConfig
from meme_system.runtime_ops import RuntimeControl
from meme_system.storage.database import initialize_database
from meme_system.storage.queries import LedgerQueries


class DashboardService:
    def __init__(
        self,
        *,
        paths: RuntimePaths,
        config: DashboardConfig | None = None,
        safety: SafetyConfig | None = None,
        control: RuntimeControl | None = None,
    ) -> None:
        self.paths = paths
        self.config = config or DashboardConfig()
        self.config.validate()
        self.safety = safety or SafetyConfig()
        self.safety.validate()
        self.control = control or RuntimeControl(paths.control_file)

    def payload(self, path: str, query: Mapping[str, list[str]]) -> tuple[int, object]:
        if path == "/api/status":
            return 200, self.status()
        if path == "/api/health":
            return 200, self.health()
        if path == "/api/control":
            return 200, self.control.snapshot()
        if path == "/api/config":
            return 200, self.config_payload()
        if path in {
            "/api/signals",
            "/api/candidates",
            "/api/positions",
            "/api/executions",
            "/api/events",
            "/api/shadow-outcomes",
        }:
            mode = _one(query, "mode", "paper")
            if mode not in {"paper", "shadow"}:
                return 400, {"error": "mode must be paper or shadow"}
            limit = _limit(query)
            with self._connection(mode) as connection:
                queries = LedgerQueries(connection, mode)
                if path == "/api/signals":
                    rows = queries.signals(limit)
                elif path == "/api/candidates":
                    rows = queries.candidates(limit)
                elif path == "/api/positions":
                    rows = queries.positions(limit, _one(query, "status", None))
                elif path == "/api/executions":
                    rows = queries.executions(limit)
                elif path == "/api/events":
                    rows = queries.lifecycle_events(limit, _one(query, "position_id", None))
                else:
                    if mode != "shadow":
                        return 400, {"error": "shadow-outcomes requires mode=shadow"}
                    rows = queries.shadow_outcomes(limit)
                return 200, {"mode": mode, "items": rows}
        if path == "/":
            return 200, _HTML
        return 404, {"error": "not_found"}

    def set_control(self, payload: Mapping[str, object]) -> tuple[int, object]:
        mode = payload.get("mode")
        paused = payload.get("paused")
        if mode not in {"paper", "shadow"} or not isinstance(paused, bool):
            return 400, {"error": "expected mode=paper|shadow and boolean paused"}
        return 200, self.control.set_paused(str(mode), paused)

    def status(self) -> dict[str, object]:
        modes: dict[str, object] = {}
        for mode in ("paper", "shadow"):
            with self._connection(mode) as connection:
                counts = {
                    "signals": connection.execute(
                        "SELECT COUNT(*) FROM signals s JOIN candidates c ON c.signal_id = s.signal_id AND c.mode = ?",
                        (mode,),
                    ).fetchone()[0],
                    "candidates": connection.execute("SELECT COUNT(*) FROM candidates WHERE mode = ?", (mode,)).fetchone()[0],
                    "open_positions": connection.execute(
                        "SELECT COUNT(*) FROM virtual_positions WHERE mode = ? AND status != 'CLOSED'", (mode,)
                    ).fetchone()[0],
                    "closed_positions": connection.execute(
                        "SELECT COUNT(*) FROM virtual_positions WHERE mode = ? AND status = 'CLOSED'", (mode,)
                    ).fetchone()[0],
                    "executions": connection.execute("SELECT COUNT(*) FROM executions WHERE mode = ?", (mode,)).fetchone()[0],
                }
            modes[mode] = {"counts": counts, "new_entries_paused": self.control.paused(mode)}
        return {
            "status": "ok",
            "chain": "solana-mainnet",
            "read_only": True,
            "safety": {
                "paper_only": self.safety.paper_only,
                "live_trading": self.safety.live_trading,
                "wallet_enabled": self.safety.wallet_enabled,
                "signing_enabled": self.safety.signing_enabled,
                "broadcast_enabled": self.safety.broadcast_enabled,
                "telegram_enabled": self.safety.telegram_enabled,
            },
            "modes": modes,
        }

    def health(self) -> dict[str, object]:
        result: dict[str, object] = {"status": "ok", "modes": {}}
        for mode, path in (("paper", self.paths.paper_health_file), ("shadow", self.paths.shadow_health_file)):
            snapshot: object = {"state": "UNKNOWN"}
            try:
                snapshot = json.loads(path.read_text(encoding="utf-8"))
            except (FileNotFoundError, OSError, json.JSONDecodeError):
                pass
            with self._connection(mode) as connection:
                events = LedgerQueries(connection, mode).health_events(20)
            result["modes"][mode] = {"snapshot": snapshot, "recent_events": events}
        return result

    def config_payload(self) -> dict[str, object]:
        return {
            "chain": "solana-mainnet",
            "strategy": "sol_ultra_early_baseline",
            "ruleset_version": "0.1.0",
            "live_engine": False,
            "wallet_path": None,
            "signing_path": None,
            "broadcast_path": None,
            "editable_controls": ["paper_new_entries_paused", "shadow_new_entries_paused"],
        }

    def _connection(self, mode: str) -> sqlite3.Connection:
        path = self.paths.paper_db if mode == "paper" else self.paths.shadow_db
        return initialize_database(path)


class _Handler(BaseHTTPRequestHandler):
    service: DashboardService

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        status, payload = self.service.payload(parsed.path, parse_qs(parsed.query))
        self._send(status, payload, html=parsed.path == "/")

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != "/api/control":
            self._send(404, {"error": "not_found"})
            return
        try:
            length = max(0, min(10_000, int(self.headers.get("Content-Length", "0"))))
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            if not isinstance(payload, Mapping):
                raise ValueError
            status, result = self.service.set_control(payload)
        except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError):
            status, result = 400, {"error": "invalid_json"}
        self._send(status, result)

    def log_message(self, format: str, *args: object) -> None:
        # Do not mirror query strings or request data into logs.
        return

    def _send(self, status: int, payload: object, *, html: bool = False) -> None:
        body = payload.encode("utf-8") if html and isinstance(payload, str) else json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        content_type = "text/html; charset=utf-8" if html else "application/json; charset=utf-8"
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def create_server(service: DashboardService) -> ThreadingHTTPServer:
    handler = type("DashboardHandler", (_Handler,), {"service": service})
    return ThreadingHTTPServer((service.config.host, service.config.port), handler)


def serve(service: DashboardService) -> None:
    server = create_server(service)
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()


def _one(query: Mapping[str, list[str]], key: str, default: str | None) -> str | None:
    values = query.get(key)
    return values[0] if values else default


def _limit(query: Mapping[str, list[str]]) -> int:
    try:
        return max(1, min(1000, int(_one(query, "limit", "100") or "100")))
    except ValueError:
        return 100


_HTML = """<!doctype html>
<html lang=\"zh-CN\"><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">
<title>Solana Paper / Shadow</title>
<style>body{font:14px system-ui;background:#10131a;color:#e8edf5;margin:24px}pre{background:#171c25;border:1px solid #2b3442;border-radius:8px;padding:16px;white-space:pre-wrap}button{margin:4px;padding:8px 12px}</style>
<h1>Solana Paper / Shadow</h1><p>只读监控；控制仅允许暂停/恢复 Paper 与 Shadow 新入场。</p>
<button onclick=\"pause('paper',true)\">暂停 Paper</button><button onclick=\"pause('paper',false)\">恢复 Paper</button>
<button onclick=\"pause('shadow',true)\">暂停 Shadow</button><button onclick=\"pause('shadow',false)\">恢复 Shadow</button>
<pre id=\"out\">loading…</pre>
<script>async function load(){let s=await fetch('/api/status').then(r=>r.json());let h=await fetch('/api/health').then(r=>r.json());document.querySelector('#out').textContent=JSON.stringify({status:s,health:h},null,2)}async function pause(mode,paused){await fetch('/api/control',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({mode,paused})});load()}load();setInterval(load,5000)</script>
</html>"""

