"""Token-name normalization shared by adapters, storage and the Dashboard."""

from __future__ import annotations


# Unicode bidirectional embedding/isolate controls can make a displayed name
# differ from the stored text.  Keep raw_name untouched for audit, and remove
# only the explicitly unsafe directional controls from display_name.
_BIDI_CONTROL_RANGES = (
    (0x202A, 0x202E),
    (0x2066, 0x2069),
)


def clean_token_name(value: object | None) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = "".join(
        char
        for char in value
        if not any(start <= ord(char) <= end for start, end in _BIDI_CONTROL_RANGES)
    ).strip()
    return cleaned or None

