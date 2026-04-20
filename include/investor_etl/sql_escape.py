"""Minimal SQL string literal escaping for dynamic statements."""

from __future__ import annotations


def lit(s: str | None) -> str:
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"
