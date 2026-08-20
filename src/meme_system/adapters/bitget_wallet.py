"""Bitget Wallet Trading API adapter for isolated BSC Live execution.

The adapter keeps Bitget API authentication, Order Mode construction, local
EOA signing and chain receipt reconciliation behind one execution boundary.
Paper/Shadow never import or construct this class.  Quote calls are read-only;
order creation/signing/submission is additionally gated by ``swaps_enabled``.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Mapping

import requests

from meme_system.adapters.binance_agentic_wallet import LiveSwapResult, QuoteFailure
from meme_system.adapters.bsc_wss import normalize_bsc_address
from meme_system.adapters.protocols import ExecutableQuote


BITGET_WALLET_PROVIDER = "BITGET_WALLET_ORDER_MODE"
BITGET_API_HOST = "https://bopenapi.bgwapi.io"
BITGET_CHAIN = "bnb"
BITGET_CHAIN_ID = 56
NATIVE_BNB_CONTRACT = ""

_RESPONSE_PUBLIC_KEY = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAk18NCL9CoiE8OQ588ehJ
hVoCenARvVymahlH3Sw8URZATuZw4k8ZKC8Sf7Zu9i9l3L3K5X4m2I20UENkOBzP
YGCRHk3Dy8SQk/e7ucj/hXJH07yNDJuv1t1nWXRhvwpG8rdW03KpDhJy4pgcAMXl
JYnJYqhfj7HW/urMD0KXw7dLNKyWKBoaGzKkoRvvxTSDHk35cjETcYg6H+bEm+Px
a+GnIJkuN5U2/LfZ4WxgNiIdE2zacHLcFoFsM14jTQdcvPid+6ilY8SQCA3GWc72
n1RudWoTj1ThEUVNWXgcwxLFIdiLCNH1YF7qINdRrjOOCCBBBpr6jdANdI2e4Dcy
DQIDAQAB
-----END PUBLIC KEY-----"""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return parsed if parsed.is_finite() else None


def _json_body(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value), ensure_ascii=False, separators=(",", ":"), sort_keys=True)


class BitgetWalletError(RuntimeError):
    """Fail-closed error whose message never contains credentials or raw txs."""

    def __init__(self, code: str, message: str | None = None, *, ambiguous: bool = False) -> None:
        super().__init__(message or code)
        self.code = code
        self.ambiguous = ambiguous


@dataclass(frozen=True)
class BitgetApiResponse:
    payload: dict[str, Any]
    headers: Mapping[str, str]
    latency_ms: int


ResponseVerifier = Callable[[Mapping[str, str]], bool]


class BitgetWalletApiClient:
    """Authenticated client implementing Bitget's documented HMAC envelope."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        api_host: str = BITGET_API_HOST,
        timeout_sec: float = 15.0,
        session: requests.Session | None = None,
        response_verifier: ResponseVerifier | None = None,
    ) -> None:
        if not api_key or not api_secret:
            raise BitgetWalletError("BITGET_CREDENTIALS_MISSING")
        if api_host.rstrip("/") != BITGET_API_HOST:
            raise BitgetWalletError("BITGET_PRODUCTION_HOST_REQUIRED")
        if timeout_sec <= 0 or timeout_sec > 120:
            raise BitgetWalletError("BITGET_TIMEOUT_INVALID")
        self._api_key = api_key
        self._api_secret = api_secret
        self.api_host = api_host.rstrip("/")
        self.timeout_sec = float(timeout_sec)
        self.session = session or requests.Session()
        self._response_verifier = response_verifier or self._verify_security_headers

    @classmethod
    def from_env(cls) -> "BitgetWalletApiClient":
        return cls(
            os.environ.get("BITGET_WALLET_API_KEY", "").strip(),
            os.environ.get("BITGET_WALLET_API_SECRET", "").strip(),
            api_host=os.environ.get("BITGET_WALLET_API_HOST", BITGET_API_HOST).strip(),
            timeout_sec=float(os.environ.get("BITGET_WALLET_TIMEOUT_SEC", "15")),
        )

    def _signature(self, path: str, timestamp: str, body: str) -> str:
        content = _json_body(
            {
                "apiPath": path,
                "body": body,
                "x-api-key": self._api_key,
                "x-api-timestamp": timestamp,
            }
        )
        digest = hmac.new(self._api_secret.encode(), content.encode(), hashlib.sha256).digest()
        return base64.b64encode(digest).decode()

    @staticmethod
    def _verify_security_headers(headers: Mapping[str, str]) -> bool:
        try:
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import padding
        except ImportError as exc:
            raise BitgetWalletError("BITGET_RESPONSE_VERIFIER_UNAVAILABLE") from exc
        values = {str(key).lower(): str(value) for key, value in headers.items()}
        security_check = values.get("security-check")
        request_check = values.get("security-request-check")
        signature = values.get("security-double-check")
        if not security_check or not request_check or not signature:
            raise BitgetWalletError("BITGET_RESPONSE_VERIFICATION_HEADERS_MISSING")
        raw = signature[2:] if signature.startswith("0x") else signature
        try:
            signature_bytes = bytes.fromhex(raw)
            public_key = serialization.load_pem_public_key(_RESPONSE_PUBLIC_KEY.encode())
            public_key.verify(
                signature_bytes,
                (security_check + request_check).encode(),
                padding.PKCS1v15(),
                hashes.SHA256(),
            )
        except BitgetWalletError:
            raise
        except Exception as exc:
            raise BitgetWalletError("BITGET_RESPONSE_VERIFICATION_FAILED") from exc
        return True

    def post(self, path: str, body: Mapping[str, Any], *, verify_response: bool = False) -> BitgetApiResponse:
        raw_body = _json_body(body)
        timestamp = str(int(time.time() * 1000))
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self._api_key,
            "x-api-timestamp": timestamp,
            "x-api-signature": self._signature(path, timestamp, raw_body),
        }
        started = time.monotonic()
        try:
            response = self.session.post(
                self.api_host + path,
                data=raw_body.encode(),
                headers=headers,
                timeout=self.timeout_sec,
            )
        except requests.Timeout as exc:
            raise BitgetWalletError("REQUEST_TIMEOUT", ambiguous=True) from exc
        except requests.RequestException as exc:
            raise BitgetWalletError("PROVIDER_UNAVAILABLE", ambiguous=True) from exc
        latency_ms = max(0, round((time.monotonic() - started) * 1000))
        if response.status_code == 429:
            raise BitgetWalletError("RATE_LIMIT")
        if response.status_code == 403:
            raise BitgetWalletError("AUTH_OR_IP_FORBIDDEN")
        if response.status_code != 200:
            raise BitgetWalletError(f"HTTP_{response.status_code}")
        try:
            payload = response.json()
        except ValueError as exc:
            raise BitgetWalletError("INVALID_RESPONSE") from exc
        if not isinstance(payload, dict):
            raise BitgetWalletError("INVALID_RESPONSE")
        status = payload.get("status")
        error_code = payload.get("error_code")
        if status not in (0, "0") or error_code not in (None, 0, "0"):
            code = str(error_code or status or "BITGET_API_ERROR")
            message = str(payload.get("msg") or payload.get("message") or "")[:160]
            raise BitgetWalletError(f"BITGET_API_{code}", message)
        if verify_response and not self._response_verifier(response.headers):
            raise BitgetWalletError("BITGET_RESPONSE_VERIFICATION_FAILED")
        return BitgetApiResponse(payload=payload, headers=response.headers, latency_ms=latency_ms)

    def chains(self) -> BitgetApiResponse:
        return self.post("/bgw-pro/swapx/order/chains", {})

    def quote(self, body: Mapping[str, Any]) -> BitgetApiResponse:
        return self.post("/bgw-pro/swapx/order/getSwapPrice", body)

    def make_order(self, body: Mapping[str, Any]) -> BitgetApiResponse:
        return self.post("/bgw-pro/swapx/order/makeSwapOrder", body, verify_response=True)

    def submit_order(self, order_id: str, signed_txs: list[str]) -> BitgetApiResponse:
        return self.post(
            "/bgw-pro/swapx/order/submitSwapOrder",
            {"orderId": order_id, "signedTxs": signed_txs},
        )

    def get_order(self, order_id: str) -> BitgetApiResponse:
        return self.post("/bgw-pro/swapx/order/getSwapOrder", {"orderId": order_id})


def classify_bitget_failure(exc: BaseException) -> str:
    if isinstance(exc, BitgetWalletError):
        mapping = {
            "REQUEST_TIMEOUT": "REQUEST_TIMEOUT",
            "RATE_LIMIT": "RATE_LIMIT",
            "AUTH_OR_IP_FORBIDDEN": "AUTH_ERROR",
            "PROVIDER_UNAVAILABLE": "PROVIDER_UNAVAILABLE",
            "INVALID_RESPONSE": "INVALID_RESPONSE",
        }
        if exc.code.startswith("BITGET_API_"):
            api_code = exc.code.removeprefix("BITGET_API_")
            fixed = {
                "80001": "INSUFFICIENT_BALANCE",
                "80002": "AMOUNT_BELOW_MINIMUM",
                "80003": "AMOUNT_ABOVE_MAXIMUM",
                "80004": "ORDER_EXPIRED",
                "80005": "INSUFFICIENT_LIQUIDITY",
                "80009": "TOKEN_UNSUPPORTED",
                "80012": "NO_ROUTE",
                "80013": "CHAIN_UNSUPPORTED",
                "80015": "ORDER_ALREADY_SUBMITTED",
                "80016": "GAS_RESERVE_INSUFFICIENT",
                "80019": "MARKET_NOT_ALLOWED",
                "80020": "QUOTE_VALUE_DEVIATION",
                "80022": "SECURITY_BLOCK",
                "80023": "MARKET_CLOSED",
            }
            if api_code in fixed:
                return fixed[api_code]
            text = str(exc).lower()
            if "liquidity" in text:
                return "INSUFFICIENT_LIQUIDITY"
            if "route" in text or "market" in text:
                return "NO_ROUTE"
            if "token" in text or "contract" in text:
                return "TOKEN_UNSUPPORTED"
        return mapping.get(exc.code, exc.code)
    return "OTHER_ERROR"


class BitgetWalletRouteProvider:
    """Read-only BSC roundtrip quote provider."""

    provider = BITGET_WALLET_PROVIDER

    def __init__(self, client: BitgetWalletApiClient, wallet_address: str) -> None:
        normalized = normalize_bsc_address(wallet_address)
        if normalized is None:
            raise BitgetWalletError("BITGET_WALLET_ADDRESS_INVALID")
        self.client = client
        self.wallet_address = normalized
        self._quote_context: dict[str, dict[str, str]] = {}

    def quote_result(
        self,
        token: str,
        side: str,
        input_quantity: Decimal,
    ) -> tuple[ExecutableQuote | None, QuoteFailure | None]:
        normalized = normalize_bsc_address(token)
        if normalized is None or side not in {"buy", "sell"} or input_quantity <= 0:
            return None, QuoteFailure("INVALID_REQUEST")
        from_contract, to_contract = (
            (NATIVE_BNB_CONTRACT, normalized) if side == "buy" else (normalized, NATIVE_BNB_CONTRACT)
        )
        requested_at = _utc_now()
        started = time.monotonic()
        try:
            response = self.client.quote(
                {
                    "fromAddress": self.wallet_address,
                    "fromAmount": format(input_quantity, "f"),
                    "fromChain": BITGET_CHAIN,
                    "fromContract": from_contract,
                    "toAddress": self.wallet_address,
                    "toChain": BITGET_CHAIN,
                    "toContract": to_contract,
                }
            )
            data = response.payload.get("data")
            if not isinstance(data, dict):
                raise BitgetWalletError("INVALID_RESPONSE")
            output = _decimal(data.get("toAmount"))
            if output is None or output <= 0 or not data.get("market"):
                raise BitgetWalletError("NO_ROUTE")
            impact = _decimal(data.get("priceImpact"))
            slippage = _decimal(data.get("slippage"))
            fee = data.get("fee") if isinstance(data.get("fee"), dict) else {}
            route_fee = _decimal(fee.get("totalAmountInUsd"))
            received_at = _utc_now()
            raw_hash = hashlib.sha256(_json_body(response.payload).encode()).hexdigest()
            quote = ExecutableQuote(
                quote_id=f"bitget:{side}:{normalized}:{int(received_at.timestamp()*1000)}",
                mint=normalized,
                side=side,
                input_quantity=input_quantity,
                output_quantity=output,
                route_fee=route_fee,
                price_impact_pct=impact,
                quoted_at=received_at,
                age_ms=0,
                expires_at=received_at + timedelta(seconds=15),
                provider=self.provider,
                route=(str(data.get("market")),),
                requested_at=requested_at,
                received_at=received_at,
                latency_ms=response.latency_ms,
                executable_style=True,
                confidence="verified",
                raw_response_hash=raw_hash,
                quote_source=self.provider,
            )
            object.__setattr__(quote, "request_statuses", (200,))
            # Store fields needed to create the order without exposing secrets.
            self._quote_context[quote.quote_id] = {
                "market": str(data.get("market")),
                "slippage": str(slippage if slippage is not None else Decimal("0.03")),
                "to_min_amount": str(data.get("toMinAmount") or ""),
                "gas_fee_usd": str((fee.get("gasFee") or {}).get("amountInUsd") or "") if isinstance(fee.get("gasFee"), dict) else "",
            }
            return quote, None
        except Exception as exc:
            return None, QuoteFailure(classify_bitget_failure(exc), latency_ms=max(0, round((time.monotonic() - started) * 1000)))

    def quote_candidate(self, mint: str, amount_bnb: Decimal, _fields: object | None = None):
        buy, buy_failure = self.quote_result(mint, "buy", amount_bnb)
        if buy is None:
            return None, None, buy_failure.reason if buy_failure else "BUY_QUOTE_FAILED"
        sell, sell_failure = self.quote_result(mint, "sell", buy.output_quantity)
        if sell is None:
            return buy, None, sell_failure.reason if sell_failure else "SELL_QUOTE_FAILED"
        return buy, sell, None

    def quote_sell(self, mint: str, quantity: Decimal):
        quote, failure = self.quote_result(mint, "sell", quantity)
        return quote, failure.reason if failure else None


class BitgetNonceManager:
    """Process-local nonce ownership, always anchored to the chain pending nonce."""

    def __init__(self, web3: Any, address: str) -> None:
        self.web3 = web3
        self.address = address
        self._lock = threading.RLock()
        self._reserved: dict[int, str] = {}

    def pending_nonce(self) -> int:
        return int(self.web3.eth.get_transaction_count(self.address, "pending"))

    def validate_and_reserve(self, nonces: list[int], order_id: str) -> None:
        if not nonces or len(set(nonces)) != len(nonces):
            raise BitgetWalletError("BITGET_NONCE_SEQUENCE_INVALID")
        with self._lock:
            baseline = self.pending_nonce()
            if nonces != list(range(nonces[0], nonces[0] + len(nonces))):
                raise BitgetWalletError("BITGET_NONCE_SEQUENCE_INVALID")
            if nonces[0] < baseline or nonces[0] > baseline + 2:
                raise BitgetWalletError("BITGET_NONCE_BASELINE_MISMATCH")
            for nonce in nonces:
                owner = self._reserved.get(nonce)
                if owner is not None and owner != order_id:
                    raise BitgetWalletError("BITGET_NONCE_ALREADY_RESERVED")
            for nonce in nonces:
                self._reserved[nonce] = order_id

    def release(self, order_id: str) -> None:
        with self._lock:
            self._reserved = {nonce: owner for nonce, owner in self._reserved.items() if owner != order_id}


class BitgetExecutionJournal:
    """Small executor-owned journal; it never shares the runtime connection."""

    ACTIVE = frozenset({"CREATING", "SIGNED", "SUBMIT_UNKNOWN", "SUBMITTED", "PENDING"})

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.connection = sqlite3.connect(self.path, timeout=5, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS execution_orders(
              logical_key TEXT PRIMARY KEY, token TEXT NOT NULL, side TEXT NOT NULL,
              quantity TEXT NOT NULL, order_id TEXT, state TEXT NOT NULL,
              error_code TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS execution_order_id_unique
              ON execution_orders(order_id) WHERE order_id IS NOT NULL;
            CREATE TABLE IF NOT EXISTS execution_transactions(
              order_id TEXT NOT NULL, nonce INTEGER NOT NULL, tx_hash TEXT NOT NULL,
              purpose TEXT NOT NULL, state TEXT NOT NULL, created_at TEXT NOT NULL,
              PRIMARY KEY(order_id, nonce), UNIQUE(nonce)
            );
            """
        )
        self.connection.commit()
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass

    def claim(self, logical_key: str, token: str, side: str, quantity: Decimal) -> sqlite3.Row | None:
        now = _utc_now().isoformat()
        with self._lock:
            row = self.connection.execute("SELECT * FROM execution_orders WHERE logical_key=?", (logical_key,)).fetchone()
            if row is not None and str(row["state"]) in self.ACTIVE:
                return row
            self.connection.execute(
                "INSERT INTO execution_orders(logical_key,token,side,quantity,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(logical_key) DO UPDATE SET quantity=excluded.quantity,order_id=NULL,state='CREATING',error_code=NULL,created_at=excluded.created_at,updated_at=excluded.updated_at",
                (logical_key, token, side, str(quantity), "CREATING", now, now),
            )
            self.connection.commit()
            return None

    def bind_order(self, logical_key: str, order_id: str, state: str = "SIGNED") -> None:
        with self._lock:
            self.connection.execute("UPDATE execution_orders SET order_id=?,state=?,updated_at=? WHERE logical_key=?", (order_id, state, _utc_now().isoformat(), logical_key))
            self.connection.commit()

    def bind_transactions(self, order_id: str, txs: list[tuple[int, str, str]]) -> None:
        now = _utc_now().isoformat()
        with self._lock:
            self.connection.executemany(
                "INSERT INTO execution_transactions(order_id,nonce,tx_hash,purpose,state,created_at) VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(order_id,nonce) DO UPDATE SET tx_hash=excluded.tx_hash,purpose=excluded.purpose",
                [(order_id, nonce, tx_hash, purpose, "SIGNED", now) for nonce, tx_hash, purpose in txs],
            )
            self.connection.commit()

    def update(self, *, logical_key: str | None = None, order_id: str | None = None, state: str, error_code: str | None = None) -> None:
        if not logical_key and not order_id:
            return
        field, value = ("logical_key", logical_key) if logical_key else ("order_id", order_id)
        with self._lock:
            self.connection.execute(f"UPDATE execution_orders SET state=?,error_code=?,updated_at=? WHERE {field}=?", (state, error_code, _utc_now().isoformat(), value))
            self.connection.commit()

    def pending(self) -> tuple[dict[str, Any], ...]:
        placeholders = ",".join("?" for _ in self.ACTIVE)
        with self._lock:
            rows = self.connection.execute(
                f"SELECT * FROM execution_orders WHERE state IN ({placeholders}) ORDER BY created_at",
                tuple(sorted(self.ACTIVE)),
            ).fetchall()
        return tuple(dict(row) for row in rows)

    def close(self) -> None:
        with self._lock:
            self.connection.close()


class BitgetWalletLiveExecutor(BitgetWalletRouteProvider):
    """Bitget Order Mode executor with local BSC EOA signing."""

    def __init__(
        self,
        client: BitgetWalletApiClient,
        private_key: str,
        rpc_url: str,
        *,
        swaps_enabled: bool = False,
        gas_reserve_bnb: Decimal = Decimal("0.003"),
        journal_path: Path = Path("data/bsc-balanced/live/bitget_execution.db"),
    ) -> None:
        try:
            from eth_account import Account
            from web3 import Web3
        except ImportError as exc:
            raise BitgetWalletError("BITGET_EVM_DEPENDENCY_MISSING") from exc
        try:
            account = Account.from_key(private_key)
        except Exception as exc:
            raise BitgetWalletError("BITGET_PRIVATE_KEY_INVALID") from exc
        self._Account = Account
        self.web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 15}))
        self.account = account
        self.swaps_enabled = bool(swaps_enabled)
        self.gas_reserve_bnb = Decimal(gas_reserve_bnb)
        self.nonces = BitgetNonceManager(self.web3, account.address)
        self.journal = BitgetExecutionJournal(journal_path)
        self._orders: dict[str, dict[str, Any]] = {}
        super().__init__(client, account.address)

    @classmethod
    def from_env(cls, *, swaps_enabled: bool = False, env_file: Path = Path(".env")) -> "BitgetWalletLiveExecutor":
        values = dict(os.environ)
        try:
            for line in env_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                values.setdefault(key.strip(), value.strip().strip("\"'"))
        except OSError:
            pass
        client = BitgetWalletApiClient(
            values.get("BITGET_WALLET_API_KEY", ""),
            values.get("BITGET_WALLET_API_SECRET", ""),
            api_host=values.get("BITGET_WALLET_API_HOST", BITGET_API_HOST),
            timeout_sec=float(values.get("BITGET_WALLET_TIMEOUT_SEC", "15")),
        )
        return cls(
            client,
            values.get("BSC_PRIVATE_KEY", ""),
            values.get("BSC_RPC_URL", ""),
            swaps_enabled=swaps_enabled,
            gas_reserve_bnb=Decimal(values.get("BITGET_GAS_RESERVE_BNB", "0.003")),
            journal_path=Path(values.get("BITGET_EXECUTION_DB_PATH", "data/bsc-balanced/live/bitget_execution.db")),
        )

    def preflight(self) -> dict[str, Any]:
        try:
            chain_id = int(self.web3.eth.chain_id)
            chains = self.client.chains().payload
            items = chains.get("data", {}).get("chains", [])
            bnb = next((item for item in items if item.get("chainId") == BITGET_CHAIN), None)
            balance = Decimal(self.web3.from_wei(self.web3.eth.get_balance(self.account.address), "ether"))
            return {
                "ok": chain_id == BITGET_CHAIN_ID and bool(bnb and bnb.get("swap", {}).get("enabled")),
                "auth": "HEALTHY",
                "chain_id": chain_id,
                "bsc_supported": bool(bnb and bnb.get("swap", {}).get("enabled")),
                "wallet_address": self.account.address,
                "bnb_balance": str(balance),
                "swaps_enabled": self.swaps_enabled,
            }
        except Exception as exc:
            return {"ok": False, "auth": "DEGRADED", "error_class": classify_bitget_failure(exc)}

    def get_balance(self, token: str | None = None) -> dict[str, Any]:
        try:
            if token is None:
                amount = Decimal(self.web3.from_wei(self.web3.eth.get_balance(self.account.address), "ether"))
                return {"ok": True, "data": [{"symbol": "BNB", "address": "", "balance": str(amount)}]}
            normalized = normalize_bsc_address(token)
            if normalized is None:
                return {"ok": False, "error_class": "INVALID_TOKEN"}
            abi = [
                {"inputs": [{"name": "owner", "type": "address"}], "name": "balanceOf", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
                {"inputs": [], "name": "decimals", "outputs": [{"type": "uint8"}], "stateMutability": "view", "type": "function"},
            ]
            contract = self.web3.eth.contract(address=self.web3.to_checksum_address(normalized), abi=abi)
            raw = int(contract.functions.balanceOf(self.account.address).call())
            decimals = int(contract.functions.decimals().call())
            amount = Decimal(raw) / (Decimal(10) ** decimals)
            return {"ok": True, "data": [{"address": normalized, "balance": str(amount), "decimals": decimals}]}
        except Exception as exc:
            return {"ok": False, "error_class": f"BALANCE_{type(exc).__name__.upper()}"}

    def _order_body(self, token: str, side: str, quantity: Decimal, quote: ExecutableQuote) -> dict[str, Any]:
        normalized = normalize_bsc_address(token)
        if normalized is None:
            raise BitgetWalletError("INVALID_TOKEN")
        context = self._quote_context.get(quote.quote_id) or {}
        market = context.get("market") or (quote.route[0] if quote.route else "")
        if not market:
            raise BitgetWalletError("NO_ROUTE")
        from_contract, to_contract = (("", normalized) if side == "buy" else (normalized, ""))
        body = {
            "fromAddress": self.account.address,
            "fromAmount": format(quantity, "f"),
            "fromChain": BITGET_CHAIN,
            "fromContract": from_contract,
            "market": market,
            "slippage": context.get("slippage") or "0.03",
            "toAddress": self.account.address,
            "toChain": BITGET_CHAIN,
            "toContract": to_contract,
        }
        if context.get("to_min_amount"):
            body["toMinAmount"] = context["to_min_amount"]
        return body

    def _safe_transactions(self, data: Mapping[str, Any], *, side: str, quantity: Decimal) -> list[dict[str, Any]]:
        deadline = int(data.get("deadline") or 0)
        if deadline and deadline <= int(time.time()):
            raise BitgetWalletError("BITGET_ORDER_DEADLINE_EXPIRED")
        txs = data.get("txs")
        if not isinstance(txs, list) or not txs:
            raise BitgetWalletError("BITGET_ORDER_TXS_MISSING")
        safe: list[dict[str, Any]] = []
        native_limit = int(quantity * (Decimal(10) ** 18)) if side == "buy" else 0
        for item in txs:
            if not isinstance(item, dict) or item.get("kind") != "transaction":
                raise BitgetWalletError("BITGET_UNSUPPORTED_SIGNING_KIND")
            if str(item.get("chainId")) != str(BITGET_CHAIN_ID):
                raise BitgetWalletError("BITGET_TRANSACTION_CHAIN_MISMATCH")
            details = dict(item.get("data") or {})
            if details.get("from") and str(details["from"]).lower() != self.account.address.lower():
                raise BitgetWalletError("BITGET_TRANSACTION_FROM_MISMATCH")
            to = normalize_bsc_address(str(details.get("to") or ""))
            calldata = str(details.get("calldata") or "")
            if to is None or not calldata.startswith("0x") or len(calldata) < 10:
                raise BitgetWalletError("BITGET_TRANSACTION_INVALID")
            raw_value = str(details.get("value") or "0")
            # Current Order Mode may return native ``value`` in human BNB
            # units (for example ``0.001``), while documented examples also
            # show integer wei. Handle both explicitly and fail closed.
            value = int(Decimal(raw_value) * (Decimal(10) ** 18)) if "." in raw_value else int(raw_value, 0)
            if value < 0 or value > native_limit:
                raise BitgetWalletError("BITGET_TRANSACTION_VALUE_EXCEEDED")
            code = self.web3.eth.get_code(self.web3.to_checksum_address(to))
            if not code:
                raise BitgetWalletError("BITGET_TRANSACTION_TARGET_NOT_CONTRACT")
            details["to"] = self.web3.to_checksum_address(to)
            details["data"] = calldata
            details.pop("calldata", None)
            details["chainId"] = BITGET_CHAIN_ID
            details["nonce"] = int(details["nonce"])
            details["gas"] = int(details.pop("gasLimit"))
            details["value"] = value
            if details.pop("supportEIP1559", False):
                details["maxFeePerGas"] = int(details.pop("maxFeePerGas"))
                details["maxPriorityFeePerGas"] = int(details.pop("maxPriorityFeePerGas"))
                details.pop("gasPrice", None)
                details["type"] = 2
            else:
                details["gasPrice"] = int(details.pop("gasPrice"))
                details.pop("maxFeePerGas", None)
                details.pop("maxPriorityFeePerGas", None)
            details.pop("baseFee", None)
            # Never sign an unlimited approval returned by an aggregator.
            if calldata[:10].lower() == "0x095ea7b3":
                if side != "sell":
                    raise BitgetWalletError("BITGET_UNEXPECTED_APPROVAL")
                token_balance = self.get_balance(to)
                decimals = int((token_balance.get("data") or [{}])[0].get("decimals") or 18)
                exact_raw = int(quantity * (Decimal(10) ** decimals))
                details["data"] = calldata[:74] + exact_raw.to_bytes(32, "big").hex()
            safe.append(details)
        return safe

    def _sign_order(self, order_id: str, data: Mapping[str, Any], *, side: str, quantity: Decimal) -> list[str]:
        txs = self._safe_transactions(data, side=side, quantity=quantity)
        self.nonces.validate_and_reserve([int(tx["nonce"]) for tx in txs], order_id)
        signed: list[str] = []
        journal_txs: list[tuple[int, str, str]] = []
        for tx in txs:
            estimate = int(self.web3.eth.estimate_gas({**tx, "from": self.account.address}))
            if int(tx["gas"]) < estimate or int(tx["gas"]) > max(estimate * 3, estimate + 100_000):
                self.nonces.release(order_id)
                raise BitgetWalletError("BITGET_GAS_CROSSCHECK_FAILED")
            raw = self.account.sign_transaction(tx).raw_transaction.hex()
            encoded = raw if raw.startswith("0x") else "0x" + raw
            signed.append(encoded)
            purpose = "APPROVE" if str(tx["data"])[:10].lower() == "0x095ea7b3" else side.upper()
            journal_txs.append((int(tx["nonce"]), self.web3.keccak(hexstr=encoded).hex(), purpose))
        self.journal.bind_transactions(order_id, journal_txs)
        return signed

    def _swap(self, token: str, quantity: Decimal, *, side: str) -> LiveSwapResult:
        requested_at = _utc_now()
        if not self.swaps_enabled:
            return LiveSwapResult(stage="SWAP_BLOCKED", error_code="REAL_SWAP_DISABLED", requested_at=requested_at, received_at=requested_at)
        normalized = normalize_bsc_address(token)
        if normalized is None or quantity <= 0:
            return LiveSwapResult(stage="SWAP_FAILED", error_code="INVALID_REQUEST", requested_at=requested_at, received_at=_utc_now())
        logical_key = f"{side}:{normalized}"
        existing = self.journal.claim(logical_key, normalized, side, quantity)
        if existing is not None:
            return LiveSwapResult(stage="SWAP_UNKNOWN", order_id=existing["order_id"], provider_status=existing["state"], error_code="DUPLICATE_ORDER_BLOCKED", needs_reconciliation=True, requested_at=requested_at, received_at=_utc_now())
        quote, failure = self.quote_result(normalized, side, quantity)
        if quote is None:
            self.journal.update(logical_key=logical_key, state="FAILED", error_code=failure.reason if failure else "QUOTE_FAILED")
            return LiveSwapResult(stage="SWAP_FAILED", error_code=failure.reason if failure else "QUOTE_FAILED", requested_at=requested_at, received_at=_utc_now())
        if side == "buy":
            balance = Decimal(self.web3.from_wei(self.web3.eth.get_balance(self.account.address), "ether"))
            if balance < quantity + self.gas_reserve_bnb:
                self.journal.update(logical_key=logical_key, state="FAILED", error_code="GAS_RESERVE_INSUFFICIENT")
                return LiveSwapResult(stage="SWAP_FAILED", error_code="GAS_RESERVE_INSUFFICIENT", requested_at=requested_at, received_at=_utc_now())
        try:
            made = self.client.make_order(self._order_body(token, side, quantity, quote))
            data = made.payload.get("data")
            if not isinstance(data, dict) or not data.get("orderId"):
                raise BitgetWalletError("INVALID_RESPONSE")
            order_id = str(data["orderId"])
            self.journal.bind_order(logical_key, order_id)
            signed = self._sign_order(order_id, data, side=side, quantity=quantity)
            self._orders[order_id] = {"token": token, "side": side, "quantity": str(quantity)}
            try:
                self.client.submit_order(order_id, signed)
            except BitgetWalletError as exc:
                self.journal.update(order_id=order_id, state="SUBMIT_UNKNOWN" if exc.ambiguous else "FAILED", error_code=exc.code)
                return LiveSwapResult(stage="SWAP_UNKNOWN" if exc.ambiguous else "SWAP_FAILED", order_id=order_id, error_code=exc.code, needs_reconciliation=exc.ambiguous, requested_at=requested_at, received_at=_utc_now())
            self.journal.update(order_id=order_id, state="SUBMITTED")
            return LiveSwapResult(stage="SWAP_SUBMITTED", order_id=order_id, needs_reconciliation=True, requested_at=requested_at, received_at=_utc_now(), input_quantity=quantity)
        except BitgetWalletError as exc:
            self.journal.update(logical_key=logical_key, state="SUBMIT_UNKNOWN" if exc.ambiguous else "FAILED", error_code=exc.code)
            return LiveSwapResult(stage="SWAP_UNKNOWN" if exc.ambiguous else "SWAP_FAILED", error_code=exc.code, needs_reconciliation=exc.ambiguous, requested_at=requested_at, received_at=_utc_now())

    def buy(self, token: str, bnb_amount: Decimal) -> LiveSwapResult:
        return self._swap(token, bnb_amount, side="buy")

    def sell(self, token: str, token_amount: Decimal) -> LiveSwapResult:
        return self._swap(token, token_amount, side="sell")

    def get_order_status(self, order_id: str) -> LiveSwapResult:
        requested_at = _utc_now()
        try:
            data = self.client.get_order(order_id).payload.get("data")
            if not isinstance(data, dict):
                raise BitgetWalletError("INVALID_RESPONSE")
            status = str(data.get("status") or "").lower()
            txs = data.get("txs") if isinstance(data.get("txs"), list) else []
            tx_hash = next((str(item.get("txId")) for item in reversed(txs) if isinstance(item, dict) and item.get("txId") and item.get("stage") != "approve"), None)
            if status == "success":
                if not tx_hash:
                    return LiveSwapResult(stage="SWAP_UNKNOWN", order_id=order_id, error_code="TX_HASH_MISSING", needs_reconciliation=True, requested_at=requested_at, received_at=_utc_now())
                receipt = self.web3.eth.get_transaction_receipt(tx_hash)
                if not receipt or int(receipt.get("status", 0)) != 1:
                    return LiveSwapResult(stage="SWAP_UNKNOWN", order_id=order_id, tx_hash=tx_hash, error_code="RECEIPT_NOT_SUCCESS", needs_reconciliation=True, requested_at=requested_at, received_at=_utc_now())
                self.nonces.release(order_id)
                self.journal.update(order_id=order_id, state="CONFIRMED")
                return LiveSwapResult(stage="SWAP_CONFIRMED", order_id=order_id, tx_hash=tx_hash, provider_status=status, requested_at=requested_at, received_at=_utc_now(), input_quantity=_decimal(data.get("fromAmount")), output_quantity=_decimal(data.get("receiveAmount") or data.get("toAmount")))
            if status in {"failed", "refunded"}:
                self.nonces.release(order_id)
                self.journal.update(order_id=order_id, state="FAILED", error_code=status.upper())
                return LiveSwapResult(stage="SWAP_FAILED", order_id=order_id, tx_hash=tx_hash, provider_status=status, requested_at=requested_at, received_at=_utc_now())
            self.journal.update(order_id=order_id, state="PENDING")
            return LiveSwapResult(stage="SWAP_PENDING", order_id=order_id, tx_hash=tx_hash, provider_status=status or None, needs_reconciliation=True, requested_at=requested_at, received_at=_utc_now())
        except BitgetWalletError as exc:
            return LiveSwapResult(stage="SWAP_UNKNOWN", order_id=order_id, error_code=exc.code, needs_reconciliation=True, requested_at=requested_at, received_at=_utc_now())
        except Exception as exc:
            return LiveSwapResult(stage="SWAP_UNKNOWN", order_id=order_id, error_code=f"RECEIPT_{type(exc).__name__.upper()}", needs_reconciliation=True, requested_at=requested_at, received_at=_utc_now())

    def pending_orders(self) -> tuple[dict[str, Any], ...]:
        return self.journal.pending()

    def close(self) -> None:
        self.client.session.close()
        self.journal.close()
