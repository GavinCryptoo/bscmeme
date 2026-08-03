from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError

from meme_system.adapters.jupiter import JupiterQuoteError, TokenDecimalsCache
from meme_system.adapters.solana_readonly import SolanaRpcClient, SolanaWssMonitor


class SolanaReadOnlyFailoverTests(unittest.TestCase):
    def test_get_token_supply_returns_decimals(self) -> None:
        def transport(_url, _body, _headers, _timeout):
            payload = {"jsonrpc": "2.0", "id": "1", "result": {"context": {"slot": 1}, "value": {"decimals": 6}}}
            return 200, json.dumps(payload).encode(), {}

        client = SolanaRpcClient(url="https://rpc.invalid", max_retries=0, transport=transport)
        self.assertEqual(client.get_token_supply_decimals("MintA"), 6)

    def test_token_decimals_cache_prefers_override_then_cache_then_rpc(self) -> None:
        calls: list[str] = []

        class FakeRpc:
            def get_token_supply_decimals(self, mint):
                calls.append(mint)
                return 9

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "decimals.json"
            cache = TokenDecimalsCache(rpc=FakeRpc(), path=path, overrides={"OverrideMint": 6})
            self.assertEqual(cache.resolve("OverrideMint"), 6)
            self.assertEqual(cache.resolve("RpcMint"), 9)
            self.assertEqual(cache.resolve("RpcMint"), 9)
            self.assertEqual(calls, ["RpcMint"])
            reloaded = TokenDecimalsCache(rpc=None, path=path)
            self.assertEqual(reloaded.resolve("RpcMint"), 9)

    def test_token_decimals_without_rpc_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = TokenDecimalsCache(rpc=None, path=Path(directory) / "decimals.json")
            with self.assertRaises(JupiterQuoteError) as raised:
                cache.resolve("MintA")
            self.assertEqual(raised.exception.error_class, "solana_rpc_missing")

    def test_rpc_tries_ordered_backup_and_does_not_expose_urls(self) -> None:
        calls: list[str] = []

        def transport(url, _body, _headers, _timeout):
            calls.append(url)
            if url == "https://primary.invalid":
                raise URLError("primary unavailable")
            return 200, json.dumps({"jsonrpc": "2.0", "id": "1", "result": 321}).encode(), {}

        client = SolanaRpcClient(
            urls=("https://primary.invalid", "https://backup.invalid", "https://backup.invalid"),
            max_retries=0,
            transport=transport,
        )

        self.assertEqual(client.get_slot(), 321)
        self.assertEqual(calls, ["https://primary.invalid", "https://backup.invalid"])
        self.assertEqual(client.safe_status()["configured_endpoints"], 2)
        self.assertEqual(client.safe_status()["active_endpoint_index"], 1)
        self.assertEqual(client.safe_status()["failover_count"], 1)
        self.assertNotIn("primary.invalid", json.dumps(client.safe_status()))
        self.assertNotIn("backup.invalid", json.dumps(client.safe_status()))

    def test_env_supports_primary_backup_and_extra_backup_list(self) -> None:
        values = {
            "SOLANA_RPC_URL": "https://primary.invalid",
            "SOLANA_BACKUP_RPC_URL": "https://backup.invalid",
            "SOLANA_RPC_BACKUP_URLS": "https://third.invalid, https://backup.invalid",
            "SOLANA_WS_URL": "wss://primary.invalid",
            "SOLANA_BACKUP_WS_URL": "wss://backup.invalid",
            "SOLANA_WS_BACKUP_URLS": "wss://third.invalid",
        }
        with patch.dict("os.environ", values, clear=False):
            rpc = SolanaRpcClient.from_env()
            wss = SolanaWssMonitor()
        self.assertEqual(rpc.urls, ("https://primary.invalid", "https://backup.invalid", "https://third.invalid"))
        self.assertEqual(wss.urls, ("wss://primary.invalid", "wss://backup.invalid", "wss://third.invalid"))

    def test_wss_switches_to_backup_after_connection_failure(self) -> None:
        attempts: list[str] = []

        class FailingContext:
            async def __aenter__(self):
                raise RuntimeError("primary unavailable")

            async def __aexit__(self, *_args):
                return False

        class WorkingContext:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def send(self, _payload):
                return None

            async def recv(self):
                stop_event.set()
                return json.dumps({"method": "slotNotification", "params": {"result": {"slot": 123}}})

        class FakeWebsockets:
            @staticmethod
            def connect(url, **_kwargs):
                attempts.append(url)
                return FailingContext() if url == "wss://primary.invalid" else WorkingContext()

        events: list[dict[str, object]] = []
        monitor = SolanaWssMonitor(urls=("wss://primary.invalid", "wss://backup.invalid"), stale_after_sec=1)
        monitor.add_subscription("slotSubscribe", [])
        stop_event = asyncio.Event()

        async def run() -> None:
            with patch.dict(sys.modules, {"websockets": FakeWebsockets}):
                await monitor.run(events.append, stop_event)

        asyncio.run(run())
        self.assertEqual(attempts, ["wss://primary.invalid", "wss://backup.invalid"])
        self.assertEqual(len(events), 1)
        self.assertEqual(monitor._active_endpoint_index, 1)
        self.assertEqual(monitor.safe_status()["failover_count"], 1)

    def test_wss_replaces_and_deduplicates_position_subscriptions(self) -> None:
        monitor = SolanaWssMonitor(urls=("wss://primary.invalid",))
        subscription = ("logsSubscribe", [{"mentions": ["MintA"]}, {"commitment": "processed"}])

        monitor.replace_subscriptions((subscription, subscription))
        self.assertEqual(monitor.subscription_snapshot(), (subscription,))

        monitor.replace_subscriptions(())
        self.assertEqual(monitor.subscription_snapshot(), ())
