"""Redaction and schema hashing for bounded, safe fixtures."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from typing import Any


SENSITIVE_KEY_PARTS = (
    "authorization",
    "api_key",
    "apikey",
    "secret",
    "cookie",
    "session",
    "device_id",
    "deviceid",
    "signature",
    "private_key",
    "seed_phrase",
    "mnemonic",
)
SENSITIVE_VALUE_PATTERNS = (
    re.compile(r"\b(?:sk|pk)_[A-Za-z0-9_-]{16,}\b"),
    re.compile(r"\bBearer\s+[A-Za-z0-9._-]{16,}\b", re.IGNORECASE),
)


def _sensitive_key(key: str) -> bool:
    lowered = key.lower().replace("-", "_")
    return any(part in lowered for part in SENSITIVE_KEY_PARTS)


def _sensitive_value(value: str) -> bool:
    return any(pattern.search(value) for pattern in SENSITIVE_VALUE_PATTERNS)


def redact_payload(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): "[REDACTED]" if _sensitive_key(str(key)) else redact_payload(item)
            for key, item in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [redact_payload(item) for item in value]
    if isinstance(value, str) and _sensitive_value(value):
        return "[REDACTED]"
    return value


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        redact_payload(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def schema_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def contains_sensitive_patterns(value: Any) -> bool:
    redacted = redact_payload(value)
    return "[REDACTED]" in json.dumps(redacted, ensure_ascii=False, sort_keys=True)
