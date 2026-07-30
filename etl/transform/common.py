"""Shared transform helpers. The flat-file writer itself lives in
etl.utils.psv (so the load layer can reuse it without importing transform);
it's re-exported here for the transform modules' convenience."""
from __future__ import annotations

from typing import Any, Sequence

from etl.utils.psv import write_psv  # re-export

__all__ = ["write_psv", "to_float", "dedupe"]


def to_float(value: Any) -> float | None:
    """Cast to float, tolerating strings (Open-Meteo returns some numerics as
    strings) and None. Returns None for missing/uncastable values."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def dedupe(rows: list[dict], key: Sequence[str]) -> list[dict]:
    """Keep the last row seen per natural key (later pulls overwrite earlier)."""
    out: dict[tuple, dict] = {}
    for row in rows:
        out[tuple(row.get(k) for k in key)] = row
    return list(out.values())
