"""Small, read-only OKX Wallet BSC Signal discovery adapter.

OKX signals are discovery metadata only.  They deliberately do not provide a
strategy mark, executable quote, or execution capability.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from meme_system.adapters.binance_web3.models import BinanceNormalizedSignal, ObservedField
from meme_system.domain.models import Signal


OKX_SIGNAL_ENDPOINT = "https://web3.okx.com/api/v6/dex/market/signal/list"
OKX_SIGNAL_PATH = "/api/v6/dex/market/signal/list"
OKX_SIGNAL_POLL_SEC = 30.0
OKX_BSC_CHAIN_INDEX = "56"


class OkxSignalError(RuntimeError):
    def __init__(self, error_class: str, message: str = "") -> None:
        super().__init__(message or error_class)
        self.error_class = error_class


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool) or str(value).strip() == "":
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _as_int(value: object) -> int | None:
    if value is None or isinstance(value, bool) or str(value).strip() == "":
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _signal_time(value: object, fallback: datetime) -> datetime:
    raw = _as_int(value)
    if raw is None:
        return fallback
    # OKX documents a timestamp but does not guarantee the unit in the field
    # description.  Both common epoch units are normalized explicitly; an
    # invalid value remains the receipt time rather than a fabricated date.
    try:
        if raw >= 100_000_000_000:
            return datetime.fromtimestamp(raw / 1000, tz=timezone.utc)
        if raw >= 1_000_000_000:
            return datetime.fromtimestamp(raw, tz=timezone.utc)
    except (OSError, OverflowError, ValueError):
        pass
    return fallback


def _pick(row: Mapping[str, object], *names: str) -> object | None:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return value
    return None


def _field(
    value: object | None,
    *,
    name: str,
    observed_at: datetime,
    source_timestamp: datetime | None,
) -> ObservedField:
    return ObservedField(
        value=value,
        source="okx_signal",
        source_field=name,
        observed_at=observed_at,
        source_timestamp=source_timestamp,
        age_ms=max(0, int((observed_at - source_timestamp).total_seconds() * 1000)) if source_timestamp else None,
        available=value is not None,
        parse_error=None if value is not None else "missing_field",
        adapter_version="okx_signal_v1",
    )


@dataclass(frozen=True)
class _HttpResponse:
    status: int
    body: bytes


class OkxBscSignalSource:
    """Poll the official OKX BSC signal endpoint at a fixed, low cadence."""

    def __init__(
        self,
        *,
        api_key: str,
        secret: str,
        passphrase: str,
        endpoint: str = OKX_SIGNAL_ENDPOINT,
        poll_sec: float = OKX_SIGNAL_POLL_SEC,
        clock: Callable[[], datetime] = _utc_now,
        transport: Callable[[Request, float], _HttpResponse] | None = None,
    ) -> None:
        if not api_key or not secret or not passphrase:
            raise OkxSignalError("OKX_SIGNAL_CREDENTIALS_MISSING")
        self.api_key = api_key
        self.secret = secret
        self.passphrase = passphrase
        self.endpoint = endpoint
        self.poll_sec = max(1.0, float(poll_sec))
        self.clock = clock
        self.transport = transport or self._urlopen
        self._last_poll_monotonic = float("-inf")
        self._startup_complete = False
        self._seen_event_ids: set[str] = set()
        self._stats: dict[str, object] = {
            "state": "PENDING",
            "last_poll_at": None,
            "last_success_at": None,
            "last_signal_at": None,
            "signals_received": 0,
            "new_tokens_discovered": 0,
            "duplicate_signals": 0,
            "api_errors": 0,
            "smart_money_signals": 0,
            "kol_signals": 0,
            "whale_signals": 0,
            "unique_tokens": 0,
        }
        self._known_tokens: set[str] = set()

    @classmethod
    def from_env(cls) -> "OkxBscSignalSource":
        return cls(
            api_key=os.environ.get("OKX_ONCHAIN_API_KEY", "").strip(),
            secret=os.environ.get("OKX_ONCHAIN_SECRET", "").strip(),
            passphrase=os.environ.get("OKX_ONCHAIN_PASSPHRASE", "").strip(),
            poll_sec=float(os.environ.get("OKX_SIGNAL_POLL_SEC", str(OKX_SIGNAL_POLL_SEC))),
        )

    @staticmethod
    def _urlopen(request: Request, timeout: float) -> _HttpResponse:
        with urlopen(request, timeout=timeout) as response:  # nosec B310 - fixed official HTTPS endpoint
            return _HttpResponse(status=int(response.status), body=response.read())

    def _headers(self, body: str, now: datetime) -> Mapping[str, str]:
        timestamp = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        prehash = f"{timestamp}POST{OKX_SIGNAL_PATH}{body}"
        signature = base64.b64encode(
            hmac.new(self.secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
        ).decode("ascii")
        return {
            "Content-Type": "application/json",
            # The official endpoint is protected by its CDN.  A stable,
            # descriptive agent string avoids its anonymous-client 1010
            # denial without impersonating a browser.
            "User-Agent": "meme0801-okx-signal/1.0",
            "Accept": "application/json",
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "OK-ACCESS-TIMESTAMP": timestamp,
        }

    def fetch_once(self) -> tuple[BinanceNormalizedSignal, ...]:
        if time.monotonic() - self._last_poll_monotonic < self.poll_sec:
            return ()
        self._last_poll_monotonic = time.monotonic()
        received_at = self.clock()
        self._stats["last_poll_at"] = received_at.isoformat()
        body = json.dumps([{"chainIndex": OKX_BSC_CHAIN_INDEX, "walletType": "1,2,3", "limit": "100"}], separators=(",", ":"))
        request = Request(self.endpoint, data=body.encode("utf-8"), headers=dict(self._headers(body, received_at)), method="POST")
        try:
            response = self.transport(request, 8.0)
            payload = json.loads(response.body.decode("utf-8"))
        except HTTPError as exc:
            self._fail(f"HTTP_{exc.code}")
            return ()
        except (URLError, TimeoutError):
            self._fail("REQUEST_TIMEOUT")
            return ()
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._fail("INVALID_RESPONSE")
            return ()
        except Exception as exc:  # transport faults must never interrupt Binance discovery
            self._fail(type(exc).__name__.upper())
            return ()
        if not isinstance(payload, Mapping) or str(payload.get("code")) != "0":
            self._fail(str(payload.get("code")) if isinstance(payload, Mapping) else "INVALID_RESPONSE")
            return ()
        data = payload.get("data")
        if isinstance(data, Mapping):
            rows = data.get("list") or data.get("data") or []
        else:
            rows = data or []
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
            self._fail("SCHEMA_CHANGED")
            return ()
        records: list[BinanceNormalizedSignal] = []
        startup_cutoff = received_at - timedelta(minutes=5)
        for raw in rows:
            if not isinstance(raw, Mapping):
                continue
            normalized = self._normalize(raw, received_at)
            if normalized is None:
                continue
            if not self._startup_complete and normalized.signal.observed_at < startup_cutoff:
                continue
            event_id = normalized.signal.signal_id
            if event_id in self._seen_event_ids:
                self._stats["duplicate_signals"] = int(self._stats["duplicate_signals"]) + 1
                continue
            self._seen_event_ids.add(event_id)
            records.append(normalized)
            mint = normalized.signal.mint.lower()
            if mint not in self._known_tokens:
                self._known_tokens.add(mint)
                self._stats["new_tokens_discovered"] = int(self._stats["new_tokens_discovered"]) + 1
            wallet_type = str(_field_value(normalized, "okx_wallet_type") or "")
            if wallet_type == "SMART_MONEY":
                self._stats["smart_money_signals"] = int(self._stats["smart_money_signals"]) + 1
            elif wallet_type == "INFLUENCER":
                self._stats["kol_signals"] = int(self._stats["kol_signals"]) + 1
            elif wallet_type == "WHALE":
                self._stats["whale_signals"] = int(self._stats["whale_signals"]) + 1
        self._startup_complete = True
        self._stats.update({
            "state": "HEALTHY",
            "last_success_at": received_at.isoformat(),
            "signals_received": int(self._stats["signals_received"]) + len(records),
            "unique_tokens": len(self._known_tokens),
        })
        if records:
            self._stats["last_signal_at"] = max(record.signal.observed_at for record in records).isoformat()
        return tuple(records)

    def _fail(self, error_class: str) -> None:
        self._stats["state"] = "DEGRADED"
        self._stats["last_error_class"] = error_class
        self._stats["api_errors"] = int(self._stats["api_errors"]) + 1

    def _normalize(self, row: Mapping[str, object], received_at: datetime) -> BinanceNormalizedSignal | None:
        token = row.get("token")
        token_data: Mapping[str, object] = token if isinstance(token, Mapping) else row
        address = _pick(token_data, "tokenAddress", "tokenContractAddress", "address")
        if not isinstance(address, str) or not address.strip():
            return None
        signal_at = _signal_time(_pick(row, "timestamp", "signalAt", "signalTimestamp"), received_at)
        wallet_type_raw = str(_pick(row, "walletType") or "UNKNOWN").upper()
        wallet_type = {
            "1": "SMART_MONEY",
            "2": "INFLUENCER",
            "3": "WHALE",
        }.get(wallet_type_raw, wallet_type_raw)
        wallets = str(_pick(row, "triggerWalletAddress", "triggerWalletAddresses") or "")
        event_material = "|".join((OKX_BSC_CHAIN_INDEX, address.lower(), signal_at.isoformat(), wallet_type, wallets.lower()))
        signal_id = "okx-signal:" + hashlib.sha256(event_material.encode("utf-8")).hexdigest()[:32]
        fields = {
            "mint": _field(address.lower(), name="token.tokenAddress", observed_at=received_at, source_timestamp=signal_at),
            "symbol": _field(_pick(token_data, "symbol"), name="token.symbol", observed_at=received_at, source_timestamp=signal_at),
            "name": _field(_pick(token_data, "name"), name="token.name", observed_at=received_at, source_timestamp=signal_at),
            "market_cap_usd": _field(_as_decimal(_pick(token_data, "marketCapUsd")), name="token.marketCapUsd", observed_at=received_at, source_timestamp=signal_at),
            "holders": _field(_as_int(_pick(token_data, "holders")), name="token.holders", observed_at=received_at, source_timestamp=signal_at),
            "top10_percent": _field(_as_decimal(_pick(token_data, "top10HolderPercent")), name="token.top10HolderPercent", observed_at=received_at, source_timestamp=signal_at),
            "okx_signal_at": _field(signal_at, name="timestamp", observed_at=received_at, source_timestamp=signal_at),
            "okx_wallet_type": _field(wallet_type, name="walletType", observed_at=received_at, source_timestamp=signal_at),
            "okx_trigger_wallet_count": _field(_as_int(_pick(row, "triggerWalletCount")), name="triggerWalletCount", observed_at=received_at, source_timestamp=signal_at),
            "okx_trigger_wallet_addresses": _field(wallets or None, name="triggerWalletAddress", observed_at=received_at, source_timestamp=signal_at),
            "okx_amount_usd": _field(_as_decimal(_pick(row, "amountUsd")), name="amountUsd", observed_at=received_at, source_timestamp=signal_at),
            "okx_sold_ratio_percent": _field(_as_decimal(_pick(row, "soldRatioPercent")), name="soldRatioPercent", observed_at=received_at, source_timestamp=signal_at),
            # Explicitly not named price_usd: it is diagnostic-only and must
            # never enter Balanced's canonical strategy price state.
            "okx_signal_price_reference": _field(_as_decimal(_pick(token_data, "price")), name="token.price", observed_at=received_at, source_timestamp=signal_at),
        }
        return BinanceNormalizedSignal(
            signal=Signal(signal_id=signal_id, mint=address.lower(), observed_at=signal_at, source="okx_signal", chain="bsc"),
            source_signal_id=signal_id,
            source_timestamp=signal_at,
            fetched_at=received_at,
            historical_bootstrap=False,
            raw_response_hash=hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode("utf-8")).hexdigest(),
            fields=fields,
            endpoint_type="okx_signal",
            chain_id=OKX_BSC_CHAIN_INDEX,
        )

    def stats(self) -> dict[str, object]:
        return dict(self._stats)


def _field_value(record: BinanceNormalizedSignal, name: str) -> object | None:
    value = record.fields.get(name)
    return value.value if value is not None and value.available else None


class BscDiscoverySource:
    """Merge Binance Meme Rush and the optional, read-only OKX signal feed."""

    def __init__(self, binance_source: object, okx_source: OkxBscSignalSource | None) -> None:
        self.binance_source = binance_source
        self.okx_source = okx_source
        self._last_stats: dict[str, object] = {}

    def fetch_once(self) -> tuple[BinanceNormalizedSignal, ...]:
        primary = tuple(self.binance_source.fetch_once())  # type: ignore[attr-defined]
        secondary: tuple[BinanceNormalizedSignal, ...] = ()
        if self.okx_source is not None:
            try:
                secondary = self.okx_source.fetch_once()
            except Exception as exc:
                # The optional discovery feed must be fail-open relative to
                # Binance and all existing live position management.
                fail = getattr(self.okx_source, "_fail", None)
                if callable(fail):
                    fail(type(exc).__name__.upper())
        base_stats = getattr(self.binance_source, "stats", None)
        self._last_stats = {
            "source": "binance_web3:meme_rush+okx_signal",
            "last_fetched": len(primary) + len(secondary),
            "binance": dict(base_stats()) if callable(base_stats) else {},
            "okx_signal": self.okx_source.stats() if self.okx_source is not None else {"state": "DISABLED"},
        }
        return primary + secondary

    def stats(self) -> dict[str, object]:
        return dict(self._last_stats)
