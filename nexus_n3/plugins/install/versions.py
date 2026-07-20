"""Small version comparison helpers without extra runtime dependencies."""

from __future__ import annotations


def normalize_version(raw: str) -> tuple[int | str, ...]:
    parts: list[int | str] = []
    for token in raw.replace("-", ".").split("."):
        if not token:
            continue
        if token.isdigit():
            parts.append(int(token))
        else:
            parts.append(token)
    return tuple(parts)


def version_gte(left: str, right: str) -> bool:
    return normalize_version(left) >= normalize_version(right)
