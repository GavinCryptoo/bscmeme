#!/usr/bin/env python3
"""Independent read-only dashboard for SOL Survivor V2 Shadow."""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Mapping

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from meme_system.config.dashboard import DashboardConfig
from meme_system.config.runtime import RuntimePaths
from meme_system.config.safety import SafetyConfig
from meme_system.dashboard_server import DashboardService, serve


class V2DashboardService(DashboardService):
    def __init__(self, root: Path, port: int) -> None:
        self.v2_root = root
        paths = RuntimePaths(
            paper_db=root / "runtime.db",
            paper_audit_log=root / "audit.jsonl",
            paper_health_file=root / "health.json",
            control_file=root / "runtime_control.json",
        )
        super().__init__(paths=paths, config=DashboardConfig(port=port), safety=SafetyConfig())

    def payload(self, path: str, query: Mapping[str, list[str]]) -> tuple[int, object]:
        if path == "/api/v2-runtime":
            return 200, self._v2_runtime()
        status, payload = super().payload(path, query)
        if path == "/" and isinstance(payload, str):
            payload = self._v2_html(payload)
        return status, payload

    def _v2_runtime(self) -> dict[str, object]:
        try:
            health = json.loads((self.v2_root / "health.json").read_text(encoding="utf-8"))
        except Exception:
            health = {"runtime": "MEME_SURVIVOR_REVERSAL_SOL_V2_SHADOW", "state": "HEALTH_UNAVAILABLE"}
        database = self.v2_root / "runtime.db"
        freshness: list[dict[str, object]] = []
        intents: list[dict[str, object]] = []
        if database.exists():
            connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
            connection.row_factory = sqlite3.Row
            try:
                freshness = [dict(row) for row in connection.execute(
                    "SELECT f.*,p.mint FROM sol_v2_position_freshness f LEFT JOIN survivor_positions p USING(position_id) ORDER BY f.updated_at DESC"
                )]
                intents = [dict(row) for row in connection.execute(
                    "SELECT i.*,p.mint FROM sol_v2_exit_intents i LEFT JOIN survivor_positions p USING(position_id) ORDER BY i.exit_triggered_at DESC"
                )]
            except sqlite3.Error:
                pass
            finally:
                connection.close()
        return {"health": health, "position_freshness": freshness, "exit_intents": intents}

    @staticmethod
    def _v2_html(html: str) -> str:
        html = html.replace("MEME Survivor Reversal V1 · 只读控制台", "MEME Survivor Runtime V2 Shadow · 独立只读控制台")
        html = html.replace("Survivor V1 状态", "Survivor V2 Shadow 状态")
        panel = '''<section class="panel" style="margin-top:16px"><div class="section-title" style="margin:0 0 12px"><h2>V2 Runtime 独立健康</h2><span>独立数据库 / PID / Lock / Health</span></div><div class="state-grid" id="v2-runtime-health"><div class="state"><span>加载状态</span><strong>读取中…</strong></div></div></section>'''
        html = html.replace('<nav class="module-nav"', panel + '<nav class="module-nav"', 1)
        script = '''<script>
        async function loadV2Runtime(){try{const r=await fetch('/api/v2-runtime',{cache:'no-store'});const v=await r.json();const h=v.health||{},w=h.wss||{},q=h.quote||{},x=h.exit_intents||{},p=h.positions||{};const cells=[['主循环',h.main_loop_last_tick_at?'运行中':'无心跳'],['循环 P95',String(h.main_loop_p95_ms??'—')+' ms'],['WSS',w.state||'—'],['最新 Slot',w.latest_slot??'—'],['订阅确认',`${h.subscriptions?.ack??0}/${h.subscriptions?.desired??0}`],['HTTP RPC',h.http_rpc?.state||'—'],['Jupiter',q.jupiter?.state||'—'],['Direct',q.direct_pump?.state||'—'],['退出等待',x.pending??0],['模拟退出',x.paper_exited??0],['持仓',p.open??0],['单写入线程',h.sqlite_single_writer?'是':'否']];document.getElementById('v2-runtime-health').innerHTML=cells.map(([a,b])=>`<div class="state"><span>${a}</span><strong>${b}</strong></div>`).join('')}catch(e){document.getElementById('v2-runtime-health').innerHTML='<div class="state"><span>V2 接口</span><strong class="bad">不可用</strong></div>'}}
        loadV2Runtime();setInterval(loadV2Runtime,5000);
        </script>'''
        return html.replace("</body>", script + "</body>")


def main() -> int:
    root = Path(os.environ.get("SOL_V2_DATA_DIR", "data/solana/survivor_v2_shadow"))
    port = int(os.environ.get("SOL_V2_DASHBOARD_PORT", "8790"))
    serve(V2DashboardService(root, port))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
