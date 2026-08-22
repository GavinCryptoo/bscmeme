"""Read-only Dashboard configuration contract for the future Stage 2 server."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DashboardConfig:
    """Read-only local Dashboard bind and authentication settings."""

    host: str = "127.0.0.1"
    port: int = 8788
    read_only: bool = True
    authentication_enabled: bool = False

    def validate(self) -> None:
        """Validate local binding, port range, and read-only guarantees."""
        if self.host != "127.0.0.1" and not self.authentication_enabled:
            raise ValueError("Non-local Dashboard binding requires authentication")
        if self.port < 1 or self.port > 65535:
            raise ValueError("Dashboard port must be between 1 and 65535")
        if not self.read_only:
            raise ValueError("Stage 0 Dashboard contract must remain read-only")
