"""Finite retry and rate-limit policies; never an infinite retry loop."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int = 1
    base_backoff_sec: float = 0.25
    max_backoff_sec: float = 2.0
    request_budget_sec: float = 10.0

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.base_backoff_sec < 0 or self.max_backoff_sec < 0:
            raise ValueError("backoff values must be non-negative")
        if self.request_budget_sec <= 0:
            raise ValueError("request_budget_sec must be positive")

    def backoff(self, retry_count: int) -> float:
        if retry_count <= 0:
            return 0.0
        return min(self.max_backoff_sec, self.base_backoff_sec * (2 ** (retry_count - 1)))


@dataclass
class RateLimitState:
    requests: int = 0
    retries: int = 0
    rate_limited: int = 0

    def record_request(self) -> None:
        self.requests += 1

    def record_retry(self) -> None:
        self.retries += 1

    def record_rate_limit(self) -> None:
        self.rate_limited += 1
