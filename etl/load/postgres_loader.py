"""Load layer (Sections 5, 6, 11): all DB access is `psql` + COPY shelled out
via subprocess — no Python DB driver, no ORM.

Every psql call:
  * runs with ON_ERROR_STOP=1 so any constraint violation / cast failure aborts
    the statement and psql exits non-zero -> the Airflow task fails -> alert
    fires (fail-fast, Section 6). Because the SCD upserts run inside a
    transaction, nothing partial lands.
  * passes pipeline data (flat-file paths, run ids) as psql VARIABLES (-v), never
    string-built into the command — the .sql files reference them via :'var'
    with proper quoting (Section 4 principle).

No I/O contract with the transform layer: this module only ever receives file
paths (from XCom) and plain dicts (metrics/findings)."""
from __future__ import annotations

import os
import subprocess
from typing import Any, Sequence

from etl import config
from etl.utils.artifacts import utc_stamp
from etl.utils.logging import get_logger
from etl.utils.metrics import API_CALL_LOG_COLUMNS
from etl.utils.psv import write_psv

log = get_logger("etl.load")

AUDIT_FINDING_COLUMNS = [
    "check_name", "severity", "related_table", "description", "detected_at", "pipeline_run_id",
]

# Maps a source key to its staging load .sql (SCD1) and open_sky upsert (SCD2).
STAGING_SQL = {
    "weather": "stage_weather_raw.sql",
    "donki": "stage_donki_raw.sql",
    "neows": "stage_neows_raw.sql",
}


def _dsn() -> str:
    dsn = config.ETL_DSN or os.environ.get("ETL_DSN", "")
    if not dsn:
        raise RuntimeError("ETL_DSN is not set")
    return dsn


def _run_psql(args: Sequence[str], *, dsn: str | None = None) -> None:
    """Run psql with the given args as a list (no shell). Raises
    CalledProcessError on non-zero exit, which fails the Airflow task."""
    cmd = [
        "psql", dsn or _dsn(),
        "-X",                      # ignore ~/.psqlrc
        "-q",                      # quiet
        "-v", "ON_ERROR_STOP=1",   # fail fast: first error aborts, exit non-zero
        *args,
    ]
    log.info("psql", extra={"context": {"args": list(args)}})
    subprocess.run(cmd, check=True)


def copy_to_staging(flat_file_path: str, source: str) -> str:
    """SCD1 staging load: \\copy the flat file into a temp landing table, then
    upsert into staging.<source>_raw on the natural key. ``source`` is one of
    weather/donki/neows."""
    sql_file = STAGING_SQL[source]
    _run_psql(["-v", f"flat_file={flat_file_path}",
               "-f", config.sql_path("upserts", sql_file)])
    return flat_file_path


def upsert_to_open_sky(sql_file: str, *, run_id: str | None = None) -> None:
    """SCD2 historize load into open_sky (weather_daily / space_weather_events /
    neo_close_approaches). ``run_id`` is bound as :'run_id' where the .sql needs
    a pipeline_run_id."""
    args: list[str] = []
    if run_id is not None:
        args += ["-v", f"run_id={run_id}"]
    args += ["-f", config.sql_path("upserts", sql_file)]
    _run_psql(args)


def derive_asteroids() -> None:
    """Derive the open_sky.asteroids SCD2 dimension from staging.neows_raw,
    after neo_close_approaches loads (Section 6)."""
    _run_psql(["-f", config.sql_path("upserts", "derive_asteroids.sql")])


def seed_locations() -> None:
    """One-time: load the seeded locations landing table into open_sky.locations
    (SCD2). Run at init, before facts."""
    _run_psql(["-f", config.sql_path("seed", "locations_seed.sql")])
    _run_psql(["-f", config.sql_path("upserts", "locations.sql")])


def _copy_log(rows: list[dict[str, Any]], columns: list[str], sql_file: str, name: str) -> None:
    if not rows:
        log.info("no rows to log", extra={"context": {"target": name}})
        return
    flat_path = config.flat_dir("logs") / f"{name}_{utc_stamp()}.psv"
    write_psv(flat_path, columns, rows)
    _run_psql(["-v", f"flat_file={flat_path}",
               "-f", config.sql_path("upserts", sql_file)])


def log_call_metrics(metrics_rows: list[dict[str, Any]]) -> None:
    """Append API call metrics to open_sky_logs.api_call_log. Accepts one dict
    or a list; a single failed call still gets recorded."""
    if isinstance(metrics_rows, dict):
        metrics_rows = [metrics_rows]
    _copy_log(metrics_rows, API_CALL_LOG_COLUMNS, "log_api_call.sql", "api_call")


def load_audit_findings(finding_rows: list[dict[str, Any]]) -> None:
    """Append audit findings to open_sky_logs.audit_findings."""
    _copy_log(finding_rows, AUDIT_FINDING_COLUMNS, "write_audit_findings.sql", "audit")
