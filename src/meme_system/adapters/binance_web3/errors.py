"""Safe, bounded error taxonomy for Binance Web3 read-only calls."""

from __future__ import annotations

from dataclasses import dataclass


ERROR_CLASSES = frozenset(
    {
        "binance_auth_missing",
        "binance_auth_rejected",
        "binance_rate_limited",
        "binance_timeout",
        "binance_connection_error",
        "binance_http_4xx",
        "binance_http_5xx",
        "binance_invalid_json",
        "binance_schema_changed",
        "binance_missing_required_field",
        "binance_invalid_timestamp",
        "binance_invalid_decimal",
        "binance_empty_response",
        "binance_pagination_error",
        "binance_duplicate_signal",
        "binance_stale_signal",
        "binance_unsupported_chain",
        "unsupported_auth_method",
        "binance_response_too_large",
        "binance_business_error",
        "binance_kline_duplicate",
    }
)


@dataclass(frozen=True)
class ErrorContext:
    error_class: str
    endpoint_type: str
    request_id: str | None = None
    retryable: bool = False
    retry_count: int = 0
    http_status: int | None = None


class BinanceWeb3Error(RuntimeError):
    """An error whose message is safe to persist in an audit record."""

    def __init__(
        self,
        message: str,
        *,
        context: ErrorContext,
    ) -> None:
        self.context = context
        self.redacted_message = message[:500]
        super().__init__(self.redacted_message)


class UnsupportedAuthMethod(BinanceWeb3Error):
    def __init__(self, method: str) -> None:
        super().__init__(
            f"authentication method is not documented for Binance Web3 read-only endpoints: {method}",
            context=ErrorContext(
                error_class="unsupported_auth_method",
                endpoint_type="auth",
                retryable=False,
            ),
        )


def ensure_known_error_class(error_class: str) -> str:
    if error_class not in ERROR_CLASSES:
        raise ValueError(f"unknown Binance error class: {error_class}")
    return error_class
