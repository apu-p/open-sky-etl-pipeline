"""Filename <-> timestamp helpers for the raw/flat artifacts (Section 6).

Raw JSON is saved UNTOUCHED, so the pieces of context the response doesn't carry
(which location, when it was fetched) live in the filename instead. Both the
extract layer (which builds the names) and the transform layer (which parses
them back) depend only on this shared util — never on each other."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


def utc_stamp() -> str:
    """UTC timestamp for filenames: YYYYMMDDTHHMMSSZ."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def stamp_to_iso(stamp: str) -> str:
    """'20260730T140512Z' -> '2026-07-30T14:05:12+00:00' (Postgres-parseable)."""
    dt = datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    return dt.isoformat()


def _tokens(path: str) -> list[str]:
    return Path(path).stem.split("_")


def fetched_at_iso(path: str) -> str:
    """The fetched-at timestamp is always the last '_'-separated token."""
    return stamp_to_iso(_tokens(path)[-1])


def first_token(path: str) -> str:
    """The leading token (e.g. location_id for weather, event_type for DONKI)."""
    return _tokens(path)[0]
