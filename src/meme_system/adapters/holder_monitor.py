"""Read-only, event-driven holder counters for active positions only."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock
from typing import Mapping

from meme_system.adapters.bsc_wss import BscPairEvent, BscRpcClient, TRANSFER_EVENT_TOPIC, normalize_bsc_address
from meme_system.adapters.solana_readonly import SolanaRpcClient

TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
_BALANCE_OF_SELECTOR = "0x70a08231"
_ZERO = "0x" + "0" * 40


@dataclass(frozen=True)
class HolderObservation:
    mint: str
    holders: int
    observed_at: datetime
    source: str


class SolanaHolderMonitor:
    """Owner-deduplicated SPL balances; no wallet access or transaction APIs."""

    def __init__(self, rpc: SolanaRpcClient) -> None:
        self.rpc = rpc
        self._lock = RLock()
        self._accounts: dict[str, tuple[str, str, int]] = {}
        self._owner_totals: dict[str, dict[str, int]] = {}

    def register(self, mint: str) -> HolderObservation | None:
        try:
            rows = self.rpc.get_program_accounts(
                TOKEN_PROGRAM,
                filters=[{"memcmp": {"offset": 0, "bytes": mint}}],
            )
        except Exception:
            return None
        with self._lock:
            self._clear_mint(mint)
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                address = row.get("pubkey")
                account = row.get("account")
                decoded = _solana_token_account(account)
                if isinstance(address, str) and decoded is not None:
                    owner, amount = decoded
                    self._set_account(str(address), mint, owner, amount)
            return self._observation(mint, "solana_token_program_snapshot")

    def registered(self, mint: str) -> bool:
        with self._lock:
            return mint in self._owner_totals

    def subscriptions(self) -> tuple[tuple[str, list[object]], ...]:
        with self._lock:
            return tuple((
                "programSubscribe",
                [TOKEN_PROGRAM, [{"memcmp": {"offset": 0, "bytes": mint}}], {"encoding": "base64", "commitment": "processed"}],
            ) for mint in sorted(self._owner_totals))

    def unregister_missing(self, mints: set[str]) -> None:
        with self._lock:
            for mint in {value[0] for value in self._accounts.values()} - mints:
                self._clear_mint(mint)

    def process_wss_event(self, event: Mapping[str, object]) -> HolderObservation | None:
        if event.get("method") != "programNotification":
            return None
        params = event.get("params")
        binding = event.get("_subscription_params")
        if not isinstance(params, Mapping) or not isinstance(binding, list) or len(binding) < 2:
            return None
        filters = binding[1]
        try:
            mint = filters[0]["memcmp"]["bytes"]
        except (TypeError, KeyError, IndexError):
            return None
        result = params.get("result")
        value = result.get("value") if isinstance(result, Mapping) else None
        address = value.get("pubkey") if isinstance(value, Mapping) else None
        account = value.get("account") if isinstance(value, Mapping) else None
        decoded = _solana_token_account(account)
        if not isinstance(mint, str) or not isinstance(address, str) or decoded is None:
            return None
        owner, amount = decoded
        with self._lock:
            self._set_account(address, mint, owner, amount)
            return self._observation(mint, "solana_token_program_wss")

    def _set_account(self, address: str, mint: str, owner: str, amount: int) -> None:
        old = self._accounts.get(address)
        if old is not None:
            old_mint, old_owner, old_amount = old
            totals = self._owner_totals.get(old_mint, {})
            totals[old_owner] = max(0, totals.get(old_owner, 0) - old_amount)
            if totals.get(old_owner) == 0:
                totals.pop(old_owner, None)
        self._accounts[address] = (mint, owner, amount)
        totals = self._owner_totals.setdefault(mint, {})
        totals[owner] = totals.get(owner, 0) + amount
        if totals[owner] == 0:
            totals.pop(owner, None)

    def _clear_mint(self, mint: str) -> None:
        for address, value in tuple(self._accounts.items()):
            if value[0] == mint:
                self._accounts.pop(address, None)
        self._owner_totals.pop(mint, None)

    def _observation(self, mint: str, source: str) -> HolderObservation:
        return HolderObservation(mint, len(self._owner_totals.get(mint, {})), datetime.now(timezone.utc), source)


class BscHolderMonitor:
    """Maintains ERC-20 holders from Transfer events after a bounded seed."""

    def __init__(self, rpc: BscRpcClient) -> None:
        self.rpc = rpc
        self._lock = RLock()
        self._balances: dict[str, dict[str, int]] = {}

    def bootstrap(self, mint: str, *, blocks: int = 3600) -> HolderObservation | None:
        token = normalize_bsc_address(mint)
        latest = self.rpc.call("eth_blockNumber", [])
        try:
            last_block = int(str(latest), 16)
        except (TypeError, ValueError):
            return None
        start = max(0, last_block - max(100, min(20000, int(blocks))))
        logs = self.rpc.call("eth_getLogs", [{"address": token, "fromBlock": hex(start), "toBlock": hex(last_block), "topics": [TRANSFER_EVENT_TOPIC]}])
        if not isinstance(logs, list) or not any(_transfer_from(log) == _ZERO for log in logs if isinstance(log, Mapping)):
            return None
        owners = {_transfer_to(log) for log in logs if isinstance(log, Mapping)}
        owners.discard(None)
        if len(owners) > 2000:
            return None
        balances: dict[str, int] = {}
        for owner in owners:
            if not isinstance(owner, str):
                continue
            raw = self.rpc.call_hex(token, _BALANCE_OF_SELECTOR + owner[2:].rjust(64, "0"))
            try:
                amount = int(str(raw), 16)
            except (TypeError, ValueError):
                return None
            if amount > 0:
                balances[owner] = amount
        with self._lock:
            self._balances[token] = balances
            return HolderObservation(token, len(balances), datetime.now(timezone.utc), "bsc_transfer_snapshot")

    def ingest(self, event: BscPairEvent) -> HolderObservation | None:
        if event.event_type != "transfer" or event.transfer_from is None or event.transfer_to is None or event.transfer_value is None:
            return None
        token = normalize_bsc_address(event.pair_address)
        if token is None:
            return None
        with self._lock:
            balances = self._balances.get(token)
            if balances is None:
                return None
            if event.transfer_from != _ZERO:
                balances[event.transfer_from] = max(0, balances.get(event.transfer_from, 0) - event.transfer_value)
                if balances[event.transfer_from] == 0:
                    balances.pop(event.transfer_from, None)
            if event.transfer_to != _ZERO and event.transfer_value > 0:
                balances[event.transfer_to] = balances.get(event.transfer_to, 0) + event.transfer_value
            return HolderObservation(token, len(balances), event.observed_at, "bsc_transfer_wss")

    def registered(self, mint: str) -> bool:
        token = normalize_bsc_address(mint)
        with self._lock:
            return token in self._balances if token is not None else False


def _solana_token_account(account: object) -> tuple[str, int] | None:
    if not isinstance(account, Mapping):
        return None
    data = account.get("data")
    encoded = data[0] if isinstance(data, list) and data else data
    if not isinstance(encoded, str):
        return None
    try:
        raw = base64.b64decode(encoded, validate=True)
    except Exception:
        return None
    if len(raw) < 72:
        return None
    return raw[32:64].hex(), int.from_bytes(raw[64:72], "little")


def _transfer_from(log: Mapping[str, object]) -> str | None:
    topics = log.get("topics")
    return normalize_bsc_address("0x" + str(topics[1])[-40:]) if isinstance(topics, list) and len(topics) >= 3 else None


def _transfer_to(log: Mapping[str, object]) -> str | None:
    topics = log.get("topics")
    return normalize_bsc_address("0x" + str(topics[2])[-40:]) if isinstance(topics, list) and len(topics) >= 3 else None
