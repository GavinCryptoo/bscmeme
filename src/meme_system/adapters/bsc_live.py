"""Small, isolated BSC Live executor.

The Smart Router quote and calldata are produced by the official PancakeSwap
SDK helper in ``scripts/pancakeswap_smart_router.cjs``. This Python adapter
owns only the explicit BSC safety checks, signing, sending, and receipt
settlement verification. It is never constructed by Solana or Paper/Shadow
runtime paths.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Mapping

from eth_abi import encode as abi_encode
from eth_utils import keccak

from meme_system.adapters.bsc_quote import (
    BscQuoteUnavailable,
    BscReadOnlyQuoteProvider,
    DEFAULT_BSC_RPC_URLS,
    FlapContext,
    ZERO_ADDRESS,
)
from meme_system.adapters.protocols import ExecutableQuote


class BscLiveError(RuntimeError):
    """Fail-closed BSC Live error with no secret-bearing message."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _required(values: Mapping[str, str], name: str) -> str:
    value = values.get(name, "").strip()
    if not value:
        raise BscLiveError(f"missing_required_config:{name}")
    return value


def _positive_decimal(values: Mapping[str, str], name: str) -> Decimal:
    raw = _required(values, name)
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise BscLiveError(f"invalid_decimal_config:{name}") from exc
    if not value.is_finite() or value <= 0:
        raise BscLiveError(f"invalid_positive_config:{name}")
    return value


def _positive_int(values: Mapping[str, str], name: str) -> int:
    raw = _required(values, name)
    try:
        value = int(raw)
    except ValueError as exc:
        raise BscLiveError(f"invalid_integer_config:{name}") from exc
    if value <= 0:
        raise BscLiveError(f"invalid_positive_config:{name}")
    return value


def _bounded_int(values: Mapping[str, str], name: str, default: str, *, minimum: int, maximum: int) -> int:
    raw = values.get(name, default).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise BscLiveError(f"invalid_integer_config:{name}") from exc
    if value < minimum or value > maximum:
        raise BscLiveError(f"out_of_range_config:{name}")
    return value


@dataclass(frozen=True)
class BscLiveConfig:
    rpc_url: str
    broadcast_rpc_urls: tuple[str, ...]
    private_key: str
    trade_amount_bnb: Decimal
    max_positions: int
    slippage_bps: int
    max_entries: int = 1
    deadline_sec: int = 60
    helper_path: Path = Path("scripts/pancakeswap_smart_router.cjs")
    node_binary: str = "node"

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> "BscLiveConfig":
        if values.get("LIVE_TRADING", "false").strip().lower() != "true":
            raise BscLiveError("LIVE_TRADING=true_required")
        if values.get("BSC_LIVE_ENABLED", "false").strip().lower() != "true":
            raise BscLiveError("BSC_LIVE_ENABLED=true_required")
        private_key = _required(values, "BSC_PRIVATE_KEY")
        if not private_key.startswith("0x") or len(private_key) != 66:
            raise BscLiveError("invalid_bsc_private_key_format")
        helper = Path(values.get("PANCAKESWAP_SMART_ROUTER_HELPER", "scripts/pancakeswap_smart_router.cjs").strip())
        if not helper.is_file():
            raise BscLiveError("smart_router_helper_missing")
        max_entries_raw = values.get("BSC_LIVE_MAX_ENTRIES", "1").strip()
        try:
            max_entries = int(max_entries_raw)
        except ValueError as exc:
            raise BscLiveError("invalid_integer_config:BSC_LIVE_MAX_ENTRIES") from exc
        if max_entries < 0:
            raise BscLiveError("invalid_non_negative_config:BSC_LIVE_MAX_ENTRIES")
        rpc_url = _required(values, "BSC_RPC_URL")
        broadcast_rpc_urls = tuple(dict.fromkeys((rpc_url, *DEFAULT_BSC_RPC_URLS)))[:2]
        return cls(
            rpc_url=rpc_url,
            # A transaction is signed only once.  These are bounded official
            # BSC broadcast endpoints for delivering that exact same hash if
            # the primary transport fails before returning a response.
            broadcast_rpc_urls=broadcast_rpc_urls,
            private_key=private_key,
            trade_amount_bnb=_positive_decimal(values, "BSC_TRADE_AMOUNT_BNB"),
            max_positions=_positive_int(values, "BSC_MAX_POSITIONS"),
            slippage_bps=_bounded_int(values, "BSC_SLIPPAGE_BPS", "0", minimum=1, maximum=5000),
            max_entries=max_entries,
            deadline_sec=_bounded_int(values, "BSC_TRADE_DEADLINE_SEC", "60", minimum=15, maximum=300),
            helper_path=helper,
            node_binary=values.get("NODE_BINARY", "node").strip() or "node",
        )

    @classmethod
    def from_env(cls, env_file: Path = Path(".env")) -> "BscLiveConfig":
        values = dict(os.environ)
        # The private key is deliberately sourced only from the selected local
        # env file, never from a shell-exported environment variable.
        values.pop("BSC_PRIVATE_KEY", None)
        try:
            for line in env_file.read_text(encoding="utf-8").splitlines():
                stripped = line.strip()
                if not stripped or stripped.startswith("#") or "=" not in stripped:
                    continue
                name, value = stripped.split("=", 1)
                if name.strip() == "BSC_PRIVATE_KEY":
                    values["BSC_PRIVATE_KEY"] = value.strip().strip("\"'")
                    break
        except (FileNotFoundError, OSError):
            pass
        return cls.from_mapping(values)


@dataclass(frozen=True)
class LiveTradeResult:
    quote: ExecutableQuote
    actual_received: Decimal
    tx_hash: str
    gas_fee_native: Decimal
    settlement_verified: bool


ERC20_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [{"name": "owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "constant": False,
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "amount", "type": "uint256"},
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]


_FLAP_SWAP_EXACT_INPUT_SELECTOR = "0x" + keccak(
    text="swapExactInput((address,address,uint256,uint256,bytes))"
)[:4].hex()


class BscLiveExecutor:
    """Execute one exact-input swap at a time, with no automatic retry."""

    def __init__(
        self,
        config: BscLiveConfig,
        *,
        quote_provider: BscReadOnlyQuoteProvider | None = None,
    ) -> None:
        try:
            from web3 import Web3
            from eth_account import Account
        except ImportError as exc:
            raise BscLiveError("bsc_live_web3_dependency_missing") from exc
        self.config = config
        self._Web3 = Web3
        self._account_type = Account
        self._web3 = Web3(Web3.HTTPProvider(config.rpc_url, request_kwargs={"timeout": 15}))
        self._broadcast_web3s = tuple(
            Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 15}))
            for url in config.broadcast_rpc_urls
        )
        try:
            self._account = Account.from_key(config.private_key)
        except Exception as exc:
            raise BscLiveError("invalid_bsc_private_key") from exc
        self._private_key = config.private_key
        self._quote_provider = quote_provider or BscReadOnlyQuoteProvider(
            rpc_url=config.rpc_url,
            helper_path=config.helper_path,
            node_binary=config.node_binary,
            slippage_bps=config.slippage_bps,
            deadline_sec=config.deadline_sec,
        )
        self._owns_quote_provider = quote_provider is None

    @property
    def account_address(self) -> str:
        return self._account.address

    def verify_chain(self) -> None:
        try:
            chain_id = int(self._web3.eth.chain_id)
        except Exception as exc:
            raise BscLiveError("bsc_chain_id_unavailable") from exc
        if chain_id != 56:
            raise BscLiveError("bsc_chain_id_mismatch")

    def token_decimals(self, token: str) -> int:
        contract = self._token_contract(token)
        try:
            decimals = int(contract.functions.decimals().call())
        except Exception as exc:
            raise BscLiveError("token_decimals_unavailable") from exc
        if decimals < 0 or decimals > 36:
            raise BscLiveError("token_decimals_invalid")
        return decimals

    def quote(self, token: str, side: str, input_quantity: Decimal) -> ExecutableQuote:
        self.verify_chain()
        quote, _venue, _context = self._venue_quote(token, side, input_quantity)
        return quote

    def buy(self, token: str, amount_bnb: Decimal, *, expected_quote: ExecutableQuote | None = None) -> LiveTradeResult:
        self.verify_chain()
        decimals = self.token_decimals(token)
        amount_raw = self._quantity_to_raw(amount_bnb, 18)
        venue_quote, venue, context = self._venue_quote(token, "buy", amount_bnb)
        self._validate_expected_quote(expected_quote, venue_quote, venue)
        if venue == "flap_portal":
            assert isinstance(context, FlapContext)
            plan = self._flap_plan(context, "buy", amount_raw, venue_quote)
            quote = venue_quote
        else:
            plan = self._router_plan(token, "buy", amount_raw, decimals, build=True)
            quote = self._quote_from_plan(token, "buy", amount_bnb, decimals, plan)
        tx, gas_price = self._transaction_from_plan(plan)
        if int(tx["value"]) != amount_raw:
            raise BscLiveError("native_input_amount_mismatch")
        balance = int(self._web3.eth.get_balance(self.account_address))
        gas_estimate = self._estimate_gas(tx)
        required = int(tx["value"]) + gas_estimate * gas_price
        if balance < required:
            raise BscLiveError("bnb_balance_below_amount_and_gas")
        token_contract = self._token_contract(token)
        before = int(token_contract.functions.balanceOf(self.account_address).call())
        receipt, tx_hash = self._send_once(tx, gas_estimate, gas_price)
        after = int(token_contract.functions.balanceOf(self.account_address).call())
        actual_raw = after - before
        minimum_raw = int(plan["minimumOutputRaw"])
        if actual_raw < minimum_raw:
            raise BscLiveError("buy_settlement_below_minimum")
        gas_fee = self._gas_fee(receipt, gas_price)
        return LiveTradeResult(
            quote=quote,
            actual_received=self._raw_to_quantity(actual_raw, decimals),
            tx_hash=tx_hash,
            gas_fee_native=self._raw_to_quantity(gas_fee, 18),
            settlement_verified=True,
        )

    def sell(self, token: str, amount_token: Decimal, *, expected_quote: ExecutableQuote | None = None) -> LiveTradeResult:
        self.verify_chain()
        decimals = self.token_decimals(token)
        amount_raw = self._quantity_to_raw(amount_token, decimals)
        if amount_raw <= 0:
            raise BscLiveError("sell_amount_must_be_positive")
        venue_quote, venue, context = self._venue_quote(token, "sell", amount_token)
        self._validate_expected_quote(expected_quote, venue_quote, venue)
        if venue == "flap_portal":
            assert isinstance(context, FlapContext)
            plan = self._flap_plan(context, "sell", amount_raw, venue_quote)
            quote = venue_quote
        else:
            plan = self._router_plan(token, "sell", amount_raw, decimals, build=True)
            quote = self._quote_from_plan(token, "sell", amount_token, decimals, plan)
        token_contract = self._token_contract(token)
        token_balance = int(token_contract.functions.balanceOf(self.account_address).call())
        if token_balance < amount_raw:
            raise BscLiveError("token_balance_below_sell_amount")
        spender = self._checksum(plan["to"])
        allowance = int(token_contract.functions.allowance(self.account_address, spender).call())
        nonce = int(self._web3.eth.get_transaction_count(self.account_address, "pending"))
        if allowance < amount_raw:
            nonce = self._approve_exact(token_contract, spender, amount_raw, nonce)
            # Approval is a separate transaction; use a fresh quote before the swap.
            venue_quote, refreshed_venue, refreshed_context = self._venue_quote(token, "sell", amount_token)
            if refreshed_venue != venue:
                raise BscLiveError("execution_venue_changed_after_approval")
            if refreshed_venue == "flap_portal":
                assert isinstance(refreshed_context, FlapContext)
                plan = self._flap_plan(refreshed_context, "sell", amount_raw, venue_quote)
                quote = venue_quote
            else:
                plan = self._router_plan(token, "sell", amount_raw, decimals, build=True)
                quote = self._quote_from_plan(token, "sell", amount_token, decimals, plan)
        tx, gas_price = self._transaction_from_plan(plan, nonce=nonce)
        gas_estimate = self._estimate_gas(tx)
        bnb_balance = int(self._web3.eth.get_balance(self.account_address))
        if bnb_balance < gas_estimate * gas_price:
            raise BscLiveError("bnb_balance_below_gas")
        before = bnb_balance
        receipt, tx_hash = self._send_once(tx, gas_estimate, gas_price)
        after = int(self._web3.eth.get_balance(self.account_address))
        gas_fee = self._gas_fee(receipt, gas_price)
        actual_raw = after - before + gas_fee
        minimum_raw = int(plan["minimumOutputRaw"])
        if actual_raw < minimum_raw:
            raise BscLiveError("sell_settlement_below_minimum")
        return LiveTradeResult(
            quote=quote,
            actual_received=self._raw_to_quantity(actual_raw, 18),
            tx_hash=tx_hash,
            gas_fee_native=self._raw_to_quantity(gas_fee, 18),
            settlement_verified=True,
        )

    def close(self) -> None:
        """Release only an internally-created read-only quote provider."""

        if self._owns_quote_provider:
            self._quote_provider.close()

    def _venue_quote(
        self,
        token: str,
        side: str,
        input_quantity: Decimal,
    ) -> tuple[ExecutableQuote, str, object]:
        """Fetch a fresh quote and fail closed unless its venue is executable."""

        try:
            quote, context = self._quote_provider.quote_with_venue(token, side, input_quantity)
        except BscQuoteUnavailable as exc:
            raise BscLiveError("bsc_executable_quote_unavailable") from exc
        if (
            quote.mint.lower() != token.lower()
            or quote.side != side
            or quote.input_quantity != input_quantity
            or quote.output_quantity <= 0
            or quote.unusable_reason(_utc_now()) is not None
        ):
            raise BscLiveError("bsc_executable_quote_invalid")
        source = quote.quote_source or quote.provider
        if isinstance(context, FlapContext) and not context.migrated:
            if source != "flap_bonding_curve_quote":
                raise BscLiveError("quote_execution_venue_mismatch")
            if not (context.fundraising_is_native or context.native_to_quote_swap_enabled):
                raise BscLiveError("flap_native_input_unavailable")
            return quote, "flap_portal", context
        if getattr(context, "migrated", False):
            if source != "pancakeswap_quote":
                raise BscLiveError("quote_execution_venue_mismatch")
            return quote, "pancakeswap_smart_router", context
        raise BscLiveError("unsupported_execution_venue")

    @staticmethod
    def _execution_venue_from_quote(quote: ExecutableQuote) -> str:
        source = quote.quote_source or quote.provider
        if source == "flap_bonding_curve_quote":
            return "flap_portal"
        if source == "pancakeswap_quote":
            return "pancakeswap_smart_router"
        return "unsupported"

    def _validate_expected_quote(
        self,
        expected_quote: ExecutableQuote | None,
        fresh_quote: ExecutableQuote,
        venue: str,
    ) -> None:
        """Permit a fresh re-quote, but never a venue or trade-shape switch."""

        if expected_quote is None:
            return
        if (
            expected_quote.mint.lower() != fresh_quote.mint.lower()
            or expected_quote.side != fresh_quote.side
            or expected_quote.input_quantity != fresh_quote.input_quantity
        ):
            raise BscLiveError("expected_quote_invalid_or_expired")
        if self._execution_venue_from_quote(expected_quote) != venue:
            raise BscLiveError("quote_execution_venue_changed")

    def _flap_plan(
        self,
        context: FlapContext,
        side: str,
        input_raw: int,
        quote: ExecutableQuote,
    ) -> dict[str, object]:
        """Build the current official Flap Portal exact-input transaction."""

        if side not in {"buy", "sell"}:
            raise BscLiveError("unsupported_trade_side")
        if context.migrated or context.status != 1:
            raise BscLiveError("flap_curve_no_longer_tradable")
        if not (context.fundraising_is_native or context.native_to_quote_swap_enabled):
            raise BscLiveError("flap_native_input_unavailable")
        output_decimals = context.token_decimals if side == "buy" else 18
        quoted_raw = self._quantity_to_raw(quote.output_quantity, output_decimals)
        minimum_output = (quoted_raw * (10_000 - self.config.slippage_bps)) // 10_000
        if minimum_output <= 0:
            raise BscLiveError("minimum_output_must_be_positive")
        input_token = ZERO_ADDRESS if side == "buy" else context.mint
        output_token = context.mint if side == "buy" else ZERO_ADDRESS
        data = _FLAP_SWAP_EXACT_INPUT_SELECTOR + abi_encode(
            ["(address,address,uint256,uint256,bytes)"],
            [(input_token, output_token, input_raw, minimum_output, b"")],
        ).hex()
        return {
            "venue": "flap_portal",
            "to": self._checksum(context.launchpad),
            "data": data,
            "value": str(input_raw if side == "buy" else 0),
            "minimumOutputRaw": str(minimum_output),
            # Portal exact-input has no deadline argument.  This local expiry
            # caps quote-to-broadcast time and is checked before estimate/send.
            "deadline": int(time.time()) + self.config.deadline_sec,
        }

    def _router_plan(self, token: str, side: str, amount_raw: int, decimals: int, *, build: bool) -> dict[str, object]:
        if side not in {"buy", "sell"}:
            raise BscLiveError("unsupported_trade_side")
        deadline = int(time.time()) + self.config.deadline_sec
        result = self._helper_request(
            {
                "operation": "build" if build else "quote",
                "rpcUrl": self.config.rpc_url,
                "side": side,
                "token": self._checksum(token),
                "tokenDecimals": decimals,
                "amountRaw": str(amount_raw),
                "recipient": self.account_address,
                "slippageBps": self.config.slippage_bps,
                "deadline": deadline,
            }
        )
        try:
            if int(result["minimumOutputRaw"]) <= 0:
                raise BscLiveError("minimum_output_must_be_positive")
            if int(result["deadline"]) <= int(time.time()):
                raise BscLiveError("trade_deadline_expired")
            router_address = self._checksum(str(result["routerAddress"]))
            if build and self._checksum(str(result.get("to", ""))) != router_address:
                raise BscLiveError("router_target_mismatch")
            if not router_address:
                raise BscLiveError("router_address_unavailable")
        except (KeyError, TypeError, ValueError) as exc:
            raise BscLiveError("smart_router_plan_invalid") from exc
        return result

    def _quote_from_plan(self, token: str, side: str, input_quantity: Decimal, decimals: int, plan: Mapping[str, object]) -> ExecutableQuote:
        output_decimals = decimals if side == "buy" else 18
        output_quantity = self._raw_to_quantity(int(plan["outputRaw"]), output_decimals)
        quoted_at = _utc_now()
        fingerprint = hashlib.sha256(
            json.dumps(
                {"token": token, "side": side, "input": str(input_quantity), "output": str(output_quantity), "router": plan.get("routerAddress")},
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()[:16]
        return ExecutableQuote(
            quote_id=f"pancakeswap-smart-router:{side}:{fingerprint}",
            mint=token,
            side=side,
            input_quantity=input_quantity,
            output_quantity=output_quantity,
            route_fee=None,
            price_impact_pct=None,
            quoted_at=quoted_at,
            age_ms=0,
            expires_at=quoted_at + timedelta(seconds=self.config.deadline_sec),
            provider="pancakeswap_quote",
            route=(str(plan["routerAddress"]),),
            executable_style=True,
            confidence="verified",
            requested_at=quoted_at,
            received_at=quoted_at,
            quote_source="pancakeswap_quote",
        )

    def _transaction_from_plan(self, plan: Mapping[str, object], *, nonce: int | None = None) -> tuple[dict[str, object], int]:
        try:
            value = int(str(plan["value"]), 0)
            to = self._checksum(str(plan["to"]))
            data = str(plan["data"])
            if int(plan["minimumOutputRaw"]) <= 0:
                raise BscLiveError("minimum_output_must_be_positive")
            if int(plan["deadline"]) <= int(time.time()):
                raise BscLiveError("trade_deadline_expired")
        except (KeyError, TypeError, ValueError) as exc:
            raise BscLiveError("smart_router_transaction_invalid") from exc
        gas_price = int(self._web3.eth.gas_price)
        transaction = {
            "from": self.account_address,
            "to": to,
            "data": data,
            "value": value,
            "chainId": 56,
            "nonce": nonce if nonce is not None else int(self._web3.eth.get_transaction_count(self.account_address, "pending")),
            "gasPrice": gas_price,
        }
        if value < 0:
            raise BscLiveError("transaction_value_invalid")
        return transaction, gas_price

    def _approve_exact(self, token_contract, spender: str, amount_raw: int, nonce: int) -> int:
        gas_price = int(self._web3.eth.gas_price)
        tx = token_contract.functions.approve(spender, amount_raw).build_transaction(
            {
                "from": self.account_address,
                "chainId": 56,
                "nonce": nonce,
                "gasPrice": gas_price,
            }
        )
        gas_estimate = self._estimate_gas(tx)
        balance = int(self._web3.eth.get_balance(self.account_address))
        if balance < gas_estimate * gas_price:
            raise BscLiveError("bnb_balance_below_approval_gas")
        self._send_once(tx, gas_estimate, gas_price)
        return nonce + 1

    def _send_once(self, tx: Mapping[str, object], gas_estimate: int, gas_price: int):
        """Sign once and use bounded delivery for that exact transaction hash."""

        transaction = dict(tx)
        transaction["gas"] = gas_estimate
        transaction["gasPrice"] = gas_price
        try:
            # LocalAccount already owns the key supplied at construction.
            # Passing it again is treated as an unsupported second positional
            # argument by current eth-account versions and aborts before any
            # RPC broadcast.
            signed = self._account.sign_transaction(transaction)
            raw = getattr(signed, "raw_transaction", getattr(signed, "rawTransaction", None))
            if raw is None:
                raise BscLiveError("signed_transaction_unavailable")
        except BscLiveError:
            raise
        except Exception as exc:
            raise BscLiveError("transaction_sign_failed") from exc
        tx_hash = self._broadcast_once(raw)
        receipt = self._wait_for_transaction_receipt(tx_hash)
        if int(receipt.get("status", 0)) != 1:
            raise BscLiveError("transaction_receipt_failed")
        return receipt, "0x" + bytes(tx_hash).hex()

    def _broadcast_once(self, raw: object):
        """Broadcast one raw transaction across at most two official RPCs.

        Re-broadcasting the identical signed byte sequence cannot create a
        second order: it has the same sender, nonce and transaction hash.
        There is no fee bump, rebuilt transaction, or unbounded retry.
        """

        expected_hash = self._web3.keccak(raw)
        rejected = False
        for web3 in self._broadcast_web3s:
            try:
                received_hash = web3.eth.send_raw_transaction(raw)
            except ValueError as exc:
                if self._already_known_transaction(exc):
                    return expected_hash
                rejected = True
                continue
            except Exception:
                continue
            if bytes(received_hash) != bytes(expected_hash):
                raise BscLiveError("transaction_hash_mismatch")
            return received_hash
        if rejected:
            raise BscLiveError("transaction_broadcast_rejected")
        raise BscLiveError("transaction_broadcast_unavailable")

    @staticmethod
    def _already_known_transaction(exc: ValueError) -> bool:
        message = str(exc).lower()
        return "already known" in message or "known transaction" in message

    def _wait_for_transaction_receipt(self, tx_hash: object):
        deadline = time.monotonic() + 120.0
        while time.monotonic() < deadline:
            for web3 in self._broadcast_web3s:
                try:
                    receipt = web3.eth.get_transaction_receipt(tx_hash)
                except Exception:
                    continue
                if receipt is not None:
                    return receipt
            time.sleep(2.0)
        raise BscLiveError("transaction_receipt_timeout")

    def _estimate_gas(self, tx: Mapping[str, object]) -> int:
        try:
            return int(self._web3.eth.estimate_gas(dict(tx)))
        except Exception as exc:
            raise BscLiveError("estimate_gas_failed") from exc

    def _gas_fee(self, receipt, gas_price: int) -> int:
        used = int(receipt.get("gasUsed", 0))
        effective = int(receipt.get("effectiveGasPrice", gas_price))
        return used * effective

    def _helper_request(self, payload: Mapping[str, object]) -> dict[str, object]:
        clean_env = {
            "PATH": os.environ.get("PATH", ""),
            "HOME": os.environ.get("HOME", ""),
            "NODE_PATH": os.environ.get("NODE_PATH", ""),
        }
        try:
            completed = subprocess.run(
                [self.config.node_binary, str(self.config.helper_path)],
                input=json.dumps(payload, separators=(",", ":")),
                text=True,
                capture_output=True,
                timeout=30,
                check=False,
                env=clean_env,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise BscLiveError("smart_router_helper_failed") from exc
        if completed.returncode != 0:
            raise BscLiveError("smart_router_quote_failed")
        try:
            result = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise BscLiveError("smart_router_response_invalid") from exc
        if not isinstance(result, dict) or result.get("status") != "ok":
            raise BscLiveError("smart_router_quote_unavailable")
        return result

    def _token_contract(self, token: str):
        return self._web3.eth.contract(address=self._checksum(token), abi=ERC20_ABI)

    def _checksum(self, address: str) -> str:
        try:
            return self._Web3.to_checksum_address(address)
        except Exception as exc:
            raise BscLiveError("invalid_bsc_token_or_router_address") from exc

    @staticmethod
    def _quantity_to_raw(quantity: Decimal, decimals: int) -> int:
        if quantity <= 0:
            raise BscLiveError("trade_amount_must_be_positive")
        return int((quantity * (Decimal(10) ** decimals)).to_integral_value(rounding=ROUND_DOWN))

    @staticmethod
    def _raw_to_quantity(raw: int, decimals: int) -> Decimal:
        return Decimal(raw) / (Decimal(10) ** decimals)
