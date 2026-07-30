"""Weather convert step (Section 5): raw Open-Meteo JSON -> pipe-delimited flat
file matching staging.weather_raw's column order.

Pure function, no I/O beyond reading its input JSON and writing its output .psv.
location_id and fetched_at aren't in the response body — they're parsed from the
raw filename (etl.utils.artifacts). Derived open_sky columns (temp_range,
is_rain_day, weather_condition) are NOT produced here — staging doesn't carry
them; they're derived DB-side in sql/upserts/weather_daily.sql.

Run standalone:  python -m etl.transform.weather <raw_json_path>
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from etl import config
from etl.transform.common import dedupe, to_float, write_psv
from etl.utils.artifacts import fetched_at_iso, first_token
from etl.utils.logging import get_logger

log = get_logger("etl.transform.weather")

# Column order MUST match staging.weather_raw (Section 6).
STAGING_COLUMNS = [
    "location_id", "date", "temp_min", "temp_max",
    "precipitation_mm", "wind_speed_max", "weather_code", "fetched_at",
]


def _rows_from_raw(raw: dict, location_id: int, fetched_at: str) -> list[dict]:
    daily = raw.get("daily") or {}
    times = daily.get("time", [])
    tmax = daily.get("temperature_2m_max", [])
    tmin = daily.get("temperature_2m_min", [])
    precip = daily.get("precipitation_sum", [])
    wind = daily.get("wind_speed_10m_max", [])
    codes = daily.get("weather_code", [])

    rows: list[dict] = []
    for i, day in enumerate(times):
        rows.append({
            "location_id": location_id,
            "date": day,  # ISO date string, e.g. 2026-07-30
            "temp_min": to_float(tmin[i] if i < len(tmin) else None),
            "temp_max": to_float(tmax[i] if i < len(tmax) else None),
            "precipitation_mm": to_float(precip[i] if i < len(precip) else None),
            "wind_speed_max": to_float(wind[i] if i < len(wind) else None),
            "weather_code": codes[i] if i < len(codes) else None,
            "fetched_at": fetched_at,
        })
    # Dedupe on the natural key, keeping the most recently fetched value.
    return dedupe(rows, ("location_id", "date"))


def convert_weather(json_path: str, *, historical: bool = False) -> str:
    """Reshape one raw weather JSON file into a staging-shaped .psv. Returns the
    flat file path."""
    raw = json.loads(Path(json_path).read_text())
    location_id = int(first_token(json_path))
    fetched_at = fetched_at_iso(json_path)

    rows = _rows_from_raw(raw, location_id, fetched_at)

    flat_path = config.flat_dir("weather", historical=historical) / (Path(json_path).stem + ".psv")
    write_psv(flat_path, STAGING_COLUMNS, rows)
    log.info("converted weather",
             extra={"context": {"location_id": location_id, "rows": len(rows),
                                "flat_path": str(flat_path)}})
    return str(flat_path)


if __name__ == "__main__":
    print(convert_weather(sys.argv[1]))
