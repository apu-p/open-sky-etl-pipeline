"""Open-Meteo extract (Section 2/5).

Thin client: fetches raw JSON and writes it UNTOUCHED to a timestamped file on
the shared volume. That raw artifact is what the convert step reshapes and what
the schema-drift audit reads. No transform logic here.

Daily pull uses the forecast API; the historical backfill uses the *archive*
(ERA5 reanalysis) API — a different host (Section 2). Both return the same daily
variable set, so one convert step handles either.

Run standalone:  python -m etl.extract.open_meteo
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from etl import config
from etl.utils.artifacts import utc_stamp
from etl.utils.logging import get_logger
from etl.utils.metrics import fetch_json

log = get_logger("etl.extract.open_meteo")

SOURCE_NAME = "open_meteo"

DAILY_VARS = "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,weather_code"


def extract_open_meteo(
    location: dict[str, Any],
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    historical: bool = False,
    pipeline_run_id: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Fetch one location's daily weather and write raw JSON to disk.

    Returns (json_file_path, metrics_row). ``metrics_row`` matches
    open_sky_logs.api_call_log's columns."""
    loc_id = location["location_id"]

    if historical:
        if start_date is None or end_date is None:
            start_date, end_date = config.backfill_range()
        url = config.OPEN_METEO_ARCHIVE_URL
        params = {
            "latitude": location["lat"],
            "longitude": location["lon"],
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "daily": DAILY_VARS,
            "timezone": location["timezone"],
        }
    else:
        url = config.OPEN_METEO_FORECAST_URL
        params = {
            "latitude": location["lat"],
            "longitude": location["lon"],
            "daily": DAILY_VARS,
            "timezone": location["timezone"],
            "past_days": 1,
            "forecast_days": 16,
        }

    data, metrics = fetch_json(SOURCE_NAME, url, params, pipeline_run_id=pipeline_run_id)

    daily = data.get("daily", {}) or {}
    metrics.records_returned = len(daily.get("time", []))

    stamp = utc_stamp()
    out_dir = config.raw_dir(SOURCE_NAME, historical=historical)
    if historical:
        fname = f"{loc_id}_{start_date.isoformat()}_{stamp}.json"
    else:
        fname = f"{loc_id}_{stamp}.json"
    json_path = out_dir / fname
    # Save the response UNTOUCHED (raw artifact for replay + schema-drift check).
    # Open-Meteo doesn't echo a location id or fetch time back, so both live in
    # the filename ({location_id}_{fetched_at}.json) — the convert step reads
    # them from there via etl.utils.artifacts, keeping the JSON untouched.
    Path(json_path).write_text(json.dumps(data))
    log.info("wrote raw json",
             extra={"context": {"source": SOURCE_NAME, "location_id": loc_id,
                                "path": str(json_path), "records": metrics.records_returned}})
    return str(json_path), metrics.as_row()


if __name__ == "__main__":
    for loc in config.LOCATIONS[:1]:
        path, m = extract_open_meteo(loc)
        print(path)
        print(m)
