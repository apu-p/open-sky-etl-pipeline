"""Central configuration, sourced from the environment (.env via python-dotenv).

Nothing here is hardcoded that belongs in .env — API keys, DSNs, and the data
directory all come from the environment. The LOCATIONS list is the one piece of
static reference data (it mirrors sql/seed/locations_seed.sql — keep them in
sync)."""
from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

# Load .env if present (local runs). In Docker the vars are already in the
# environment, so a missing .env is fine.
load_dotenv()

# --- Repo layout -------------------------------------------------------------
# In the Airflow container the repo is mounted at /opt/airflow; locally it's the
# parent of this file's package. SQL_DIR resolves relative to the repo root.
REPO_ROOT = Path(os.environ.get("REPO_ROOT", Path(__file__).resolve().parent.parent))
SQL_DIR = REPO_ROOT / "sql"

# --- Secrets / connections ---------------------------------------------------
NASA_API_KEY = os.environ.get("NASA_API_KEY", "DEMO_KEY")
ETL_DSN = os.environ.get("ETL_DSN", "")

# --- Shared data volume ------------------------------------------------------
DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))

# --- API endpoints -----------------------------------------------------------
# Open-Meteo uses two DIFFERENT hosts: the forecast API for the daily pull and
# the archive (ERA5 reanalysis) API for the historical backfill (Section 2).
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

NASA_DONKI_BASE = "https://api.nasa.gov/DONKI"          # + /CME, /GST, /FLR
NASA_NEOWS_FEED = "https://api.nasa.gov/neo/rest/v1/feed"

DONKI_EVENT_TYPES = ("CME", "GST", "FLR")

# --- Reference data: the seeded locations (mirror of locations_seed.sql) ------
LOCATIONS = [
    {"location_id": 1, "name": "London",   "lat": 51.5074,  "lon": -0.1278,   "timezone": "Europe/London"},
    {"location_id": 2, "name": "New York", "lat": 40.7128,  "lon": -74.0060,  "timezone": "America/New_York"},
    {"location_id": 3, "name": "Tokyo",    "lat": 35.6762,  "lon": 139.6503,  "timezone": "Asia/Tokyo"},
    {"location_id": 4, "name": "Sydney",   "lat": -33.8688, "lon": 151.2093,  "timezone": "Australia/Sydney"},
    {"location_id": 5, "name": "Nairobi",  "lat": -1.2864,  "lon": 36.8172,   "timezone": "Africa/Nairobi"},
]

# --- HTTP / retry ------------------------------------------------------------
HTTP_TIMEOUT_S = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_BASE_S = 2.0  # exponential: base ** attempt

# --- Backfill window (Section 2/7): rolling 2 years, ending today - 5 days ----
BACKFILL_END_OFFSET_DAYS = 5
BACKFILL_YEARS = 2
# Per-request window caps enforced by the sources (Section 2).
DONKI_WINDOW_DAYS = 30
NEOWS_WINDOW_DAYS = 7


def backfill_range(today: date | None = None) -> tuple[date, date]:
    """(start, end) for the shared 2-year backfill window."""
    today = today or date.today()
    end = today - timedelta(days=BACKFILL_END_OFFSET_DAYS)
    start = end.replace(year=end.year - BACKFILL_YEARS)
    return start, end


def iter_windows(start: date, end: date, window_days: int):
    """Yield (window_start, window_end) covering [start, end] in per-request
    windows no wider than a source's cap (DONKI 30d, NeoWs 7d — Section 2)."""
    ws = start
    while ws <= end:
        we = min(ws + timedelta(days=window_days), end)
        yield ws, we
        ws = we + timedelta(days=1)


# --- Data-dir helpers --------------------------------------------------------
def raw_dir(source: str, historical: bool = False) -> Path:
    p = DATA_DIR / "raw" / source
    if historical:
        p = p / "historical"
    p.mkdir(parents=True, exist_ok=True)
    return p


def flat_dir(source: str, historical: bool = False) -> Path:
    p = DATA_DIR / "flat" / source
    if historical:
        p = p / "historical"
    p.mkdir(parents=True, exist_ok=True)
    return p


def sql_path(*parts: str) -> str:
    """Absolute path to a checked-in .sql file, as a string for subprocess."""
    return str(SQL_DIR.joinpath(*parts))
