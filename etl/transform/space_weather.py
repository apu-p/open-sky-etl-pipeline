"""Space-weather convert steps (Section 5): raw NASA JSON -> pipe-delimited flat
files matching staging.donki_raw / staging.neows_raw.

Pure functions, no I/O beyond reading input JSON and writing output .psv.

Run standalone:
  python -m etl.transform.space_weather donki <raw_json_path> [CME|GST|FLR]
  python -m etl.transform.space_weather neows <raw_json_path>
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from etl import config
from etl.transform.common import dedupe, to_float, write_psv
from etl.utils.artifacts import fetched_at_iso, first_token
from etl.utils.logging import get_logger

log = get_logger("etl.transform.space_weather")

# Column orders MUST match the staging tables (Section 6).
DONKI_COLUMNS = [
    "event_id", "event_type", "event_time", "source_location", "speed_kms",
    "kp_index", "flare_class", "flare_severity_score", "linked_event_id", "fetched_at",
]
NEOWS_COLUMNS = [
    "asteroid_id", "close_approach_date", "name", "miss_distance_km",
    "relative_velocity_kph", "estimated_diameter_m_max", "is_potentially_hazardous",
    "fetched_at",
]

_FLARE_RANK = {"A": 1, "B": 2, "C": 3, "M": 4, "X": 5}


def _pg_bool(value) -> str:
    """Postgres-friendly boolean literal for COPY."""
    return "true" if value else "false"


def flare_severity_score(flare_class: str | None) -> float | None:
    """Parse an ordinal flare-class string (A<B<C<M<X, then magnitude) into a
    sortable numeric score. X-class always outranks M, etc.:
      score = letter_rank + magnitude/10   e.g. M9.9 -> 4.99, X1.5 -> 5.15."""
    if not flare_class:
        return None
    rank = _FLARE_RANK.get(flare_class[0].upper())
    if rank is None:
        return None
    try:
        magnitude = float(flare_class[1:]) if len(flare_class) > 1 else 0.0
    except ValueError:
        magnitude = 0.0
    return round(rank + magnitude / 10.0, 2)


def _linked_event_id(event: dict) -> str | None:
    linked = event.get("linkedEvents") or []
    if linked and isinstance(linked, list):
        return linked[0].get("activityID")
    return None


def _cme_row(event: dict, fetched_at: str) -> dict:
    analyses = event.get("cmeAnalyses") or []
    chosen = next((a for a in analyses if a.get("isMostAccurate")), analyses[0] if analyses else {})
    return {
        "event_id": event.get("activityID"),
        "event_type": "CME",
        "event_time": event.get("startTime"),
        "source_location": event.get("sourceLocation") or None,
        "speed_kms": to_float(chosen.get("speed")),
        "kp_index": None,
        "flare_class": None,
        "flare_severity_score": None,
        "linked_event_id": _linked_event_id(event),
        "fetched_at": fetched_at,
    }


def _gst_row(event: dict, fetched_at: str) -> dict:
    kp_values = [to_float(k.get("kpIndex")) for k in (event.get("allKpIndex") or [])]
    kp_values = [k for k in kp_values if k is not None]
    return {
        "event_id": event.get("gstID"),
        "event_type": "GST",
        "event_time": event.get("startTime"),
        "source_location": None,
        "speed_kms": None,
        "kp_index": max(kp_values) if kp_values else None,  # peak storm severity
        "flare_class": None,
        "flare_severity_score": None,
        "linked_event_id": _linked_event_id(event),
        "fetched_at": fetched_at,
    }


def _flr_row(event: dict, fetched_at: str) -> dict:
    flare_class = event.get("classType")
    return {
        "event_id": event.get("flrID"),
        "event_type": "FLR",
        "event_time": event.get("beginTime") or event.get("peakTime"),
        "source_location": event.get("sourceLocation") or None,
        "speed_kms": None,
        "kp_index": None,
        "flare_class": flare_class,
        "flare_severity_score": flare_severity_score(flare_class),
        "linked_event_id": _linked_event_id(event),
        "fetched_at": fetched_at,
    }


_ROW_BUILDERS = {"CME": _cme_row, "GST": _gst_row, "FLR": _flr_row}


def convert_donki(json_path: str, event_type: str | None = None, *, historical: bool = False) -> str:
    """Reshape one raw DONKI JSON file (one event type) into staging shape."""
    event_type = (event_type or first_token(json_path)).upper()
    builder = _ROW_BUILDERS.get(event_type)
    if builder is None:
        raise ValueError(f"unknown DONKI event type: {event_type}")

    fetched_at = fetched_at_iso(json_path)
    events = json.loads(Path(json_path).read_text())
    if not isinstance(events, list):
        events = []

    rows = [builder(ev, fetched_at) for ev in events]
    rows = [r for r in rows if r["event_id"]]           # drop events with no id
    rows = dedupe(rows, ("event_id",))

    flat_path = config.flat_dir("donki", historical=historical) / (Path(json_path).stem + ".psv")
    write_psv(flat_path, DONKI_COLUMNS, rows)
    log.info("converted donki",
             extra={"context": {"event_type": event_type, "rows": len(rows),
                                "flat_path": str(flat_path)}})
    return str(flat_path)


def convert_neows(json_path: str, *, historical: bool = False) -> str:
    """Explode the nested NeoWs feed (date -> asteroid -> approach) into one row
    per (asteroid_id, close_approach_date), staging shape."""
    fetched_at = fetched_at_iso(json_path)
    raw = json.loads(Path(json_path).read_text())
    by_date = raw.get("near_earth_objects", {}) if isinstance(raw, dict) else {}

    rows: list[dict] = []
    for approach_date, neos in by_date.items():
        for neo in neos:
            diameter = (neo.get("estimated_diameter", {})
                           .get("meters", {})
                           .get("estimated_diameter_max"))
            for cad in neo.get("close_approach_data", []):
                if cad.get("close_approach_date") != approach_date:
                    continue  # only the approach that put it under this date
                rows.append({
                    "asteroid_id": neo.get("id"),
                    "close_approach_date": approach_date,
                    "name": neo.get("name"),
                    "miss_distance_km": to_float(cad.get("miss_distance", {}).get("kilometers")),
                    "relative_velocity_kph": to_float(
                        cad.get("relative_velocity", {}).get("kilometers_per_hour")),
                    "estimated_diameter_m_max": to_float(diameter),
                    "is_potentially_hazardous": _pg_bool(neo.get("is_potentially_hazardous_asteroid")),
                    "fetched_at": fetched_at,
                })
    rows = dedupe(rows, ("asteroid_id", "close_approach_date"))

    flat_path = config.flat_dir("neows", historical=historical) / (Path(json_path).stem + ".psv")
    write_psv(flat_path, NEOWS_COLUMNS, rows)
    log.info("converted neows",
             extra={"context": {"rows": len(rows), "flat_path": str(flat_path)}})
    return str(flat_path)


if __name__ == "__main__":
    kind = sys.argv[1]
    if kind == "donki":
        print(convert_donki(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else None))
    elif kind == "neows":
        print(convert_neows(sys.argv[2]))
    else:
        raise SystemExit("usage: space_weather.py [donki|neows] <path> [event_type]")
