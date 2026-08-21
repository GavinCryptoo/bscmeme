"""Small, structured operational logger for realtime runtimes.

The runtime keeps its existing stdout status records for supervisors while
writing the same events to a rotating, local log with a stable context.
Strategy decisions and trading state are intentionally outside this module.
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
from pathlib import Path
from typing import Any, Mapping


_LOGGER_NAME = "meme_system.runtime"
_runtime_logger: "RuntimeLogger | None" = None


class _UtcFormatter(logging.Formatter):
    converter = __import__("time").gmtime

    def format(self, record: logging.LogRecord) -> str:
        timestamp = self.formatTime(record, "%Y-%m-%dT%H:%M:%S")
        milliseconds = f"{record.msecs:03.0f}"
        context = " ".join(
            f"{key}={getattr(record, key, '-') or '-'}"
            for key in ("strategy_mode", "chain", "datasource")
        )
        return f"{timestamp}.{milliseconds}Z {record.levelname} {context} {record.getMessage()}"


class RuntimeLogger:
    """Structured runtime logger with a fixed strategy/chain/data context."""

    def __init__(self, logger: logging.Logger, *, strategy_mode: str, chain: str, datasource: str) -> None:
        self._logger = logger
        self.strategy_mode = strategy_mode
        self.chain = chain
        self.datasource = datasource

    def event(self, event: str, *, level: int = logging.INFO, **fields: Any) -> None:
        payload = {"event": event, **fields}
        self._logger.log(
            level,
            json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str),
            extra={
                "strategy_mode": self.strategy_mode,
                "chain": self.chain,
                "datasource": self.datasource,
            },
        )

    def record(self, payload: Mapping[str, Any], *, level: int = logging.INFO) -> None:
        """Record an existing stdout status payload without changing it."""
        event = str(payload.get("event") or payload.get("status") or "runtime_status")
        fields = {str(key): value for key, value in payload.items() if key != "event"}
        self.event(event, level=level, **fields)


def configure_runtime_logging(
    path: str | Path | None = None,
    *,
    strategy_mode: str,
    chain: str,
    datasource: str,
) -> RuntimeLogger:
    """Configure the process logger and return its contextual facade."""
    global _runtime_logger
    log_path = Path(path or os.environ.get("RUNTIME_LOG_PATH", "logs/runtime.log"))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    handler = logging.handlers.RotatingFileHandler(
        log_path,
        maxBytes=10_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    handler.setFormatter(_UtcFormatter())
    logger.addHandler(handler)
    _runtime_logger = RuntimeLogger(
        logger,
        strategy_mode=str(strategy_mode),
        chain=str(chain),
        datasource=str(datasource),
    )
    return _runtime_logger


def runtime_log_event(event: str, *, level: int = logging.INFO, **fields: Any) -> None:
    """Write a diagnostic event when a runner configured the logger.

    Direct strategy tests may run without a runner; in that case diagnostics
    are intentionally dropped instead of introducing stdout side effects.
    """
    if _runtime_logger is not None:
        _runtime_logger.event(event, level=level, **fields)


def shutdown_runtime_logging() -> None:
    """Flush and detach handlers, primarily for clean shutdown and tests."""
    global _runtime_logger
    logger = logging.getLogger(_LOGGER_NAME)
    for handler in list(logger.handlers):
        handler.flush()
        logger.removeHandler(handler)
        handler.close()
    _runtime_logger = None
