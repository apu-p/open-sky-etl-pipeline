"""NASA extract: DONKI (space-weather events) + NeoWs (asteroids). Section 2/5.

Thin clients — fetch raw JSON, write it untouched to a timestamped file, return
(path, metrics). No reshaping here. The registered NASA_API_KEY (not DEMO_KEY)
is sent as a query param and never lands in the saved response.

Run standalone:  python -m etl.extract.nasa
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from etl import config
from etl.utils.artifacts import utc_stamp
from etl.utils.logging import get_logger
from etl.utils.metrics import fetch_json

log = get_logger("etl.extract.nasa")

DONKI_SOURCE = "nasa_donki"
NEOWS_SOURCE = "nasa_neows"


def _daily_donki_window(end: date | None = None) -> tuple[date, date]:
    """Rolling window for the daily DONKI pull (Section 7). One DONKI window
    (<= 30 days) covers the recent past comfortably."""
    end = end or date.today()
    return end - timedelta(days=config.DONKI_WINDOW_DAYS), end


def _daily_neows_window(end: date | None = None) -> tuple[date, date]:
    """Rolling 7-day window for the daily NeoWs pull (feed endpoint hard cap)."""
    end = end or date.today()
    return end - timedelta(days=config.NEOWS_WINDOW_DAYS), end


def extract_donki(
    event_type: str,
    start_date: date | None = None,
    end_date: date | None = None,
    *,
    historical: bool = False,
    pipeline_run_id: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Fetch one DONKI event type (CME/GST/FLR) over a date window."""
    event_type = event_type.upper()
    if event_type not in config.DONKI_EVENT_TYPES:
        raise ValueError(f"unknown DONKI event type: {event_type}")
    if start_date is None or end_date is None:
        start_date, end_date = _daily_donki_window()

    url = f"{config.NASA_DONKI_BASE}/{event_type}"
    params = {
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "api_key": config.NASA_API_KEY,
    }
    data, metrics = fetch_json(DONKI_SOURCE, url, params, pipeline_run_id=pipeline_run_id)
    metrics.records_returned = len(data) if isinstance(data, list) else 0

    stamp = utc_stamp()
    out_dir = config.raw_dir("donki", historical=historical)
    if historical:
        fname = f"{event_type}_{start_date.isoformat()}_{stamp}.json"
    else:
        fname = f"{event_type}_{stamp}.json"
    json_path = out_dir / fname
    Path(json_path).write_text(json.dumps(data))
    log.info("wrote raw json",
             extra={"context": {"source": DONKI_SOURCE, "event_type": event_type,
                                "path": str(json_path), "records": metrics.records_returned}})
    return str(json_path), metrics.as_row()


def extract_neows(
    start_date: date | None = None,
    end_date: date | None = None,
    *,
    historical: bool = False,
    pipeline_run_id: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Fetch the NeoWs feed for a window (<= 7 days, feed endpoint hard cap)."""
    if start_date is None or end_date is None:
        start_date, end_date = _daily_neows_window()
    span = (end_date - start_date).days
    if span > config.NEOWS_WINDOW_DAYS:
        raise ValueError(f"NeoWs feed window must be <= {config.NEOWS_WINDOW_DAYS} days, got {span}")

    params = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "api_key": config.NASA_API_KEY,
    }
    data, metrics = fetch_json(NEOWS_SOURCE, config.NASA_NEOWS_FEED, params,
                               pipeline_run_id=pipeline_run_id)
    # element_count is the total asteroids across the window's dates.
    metrics.records_returned = int(data.get("element_count", 0)) if isinstance(data, dict) else 0

    stamp = utc_stamp()
    out_dir = config.raw_dir("neows", historical=historical)
    if historical:
        fname = f"{start_date.isoformat()}_{stamp}.json"
    else:
        fname = f"{start_date.isoformat()}_{stamp}.json"
    json_path = out_dir / fname
    Path(json_path).write_text(json.dumps(data))
    log.info("wrote raw json",
             extra={"context": {"source": NEOWS_SOURCE, "start_date": start_date.isoformat(),
                                "path": str(json_path), "records": metrics.records_returned}})
    return str(json_path), metrics.as_row()


if __name__ == "__main__":
    for et in config.DONKI_EVENT_TYPES:
        p, m = extract_donki(et)
        print(et, p, m["records_returned"])
    p, m = extract_neows()
    print("neows", p, m["records_returned"])
