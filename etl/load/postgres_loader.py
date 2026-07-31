"""Load layer (Sections 5, 6, 11): all DB access is `psql` + COPY shelled out
via subprocess — no Python DB driver, no ORM.

Every psql call:
  * runs with ON_ERROR_STOP=1 so any constraint violation / cast failure aborts
    the statement and psql exits non-zero -> the Airflow task fails -> alert
    fires (fail-fast, Section 6). Because the SCD upserts run inside a
    transaction, nothing partial lands.
  * passes pipeline data as psql VARIABLES (-v) or via stdin, never string-built
    into the command. Scalars (run ids) go in as -v vars, referenced in the
    .sql files via :'var' with proper quoting (Section 4 principle). Flat-file
    paths are piped in as psql's stdin instead (see _run_psql) rather than
    templated into \\copy, which doesn't reliably interpolate :'var' itself.

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


def _run_psql(args: Sequence[str], *, dsn: str | None = None, stdin_path: str | None = None) -> None:
    """Run psql with the given args as a list (no shell). Raises
    CalledProcessError on non-zero exit, which fails the Airflow task.

    ``stdin_path``, if given, is opened and streamed in as psql's stdin — this
    is how flat-file paths reach \\copy (the .sql files say ``FROM PSTDIN``).
    psql's own \\copy argument parser does its own ad-hoc tokenizing of the
    FROM/TO target that does NOT reliably apply psql's :'var' interpolation
    (verified empirically: both :'flat_file' and :flat_file silently break the
    WITH (...) options that follow), so the file path is never templated into
    the .sql text at all — it's piped in as a real OS-level stdin stream
    instead, which \\copy's PSTDIN keyword reads regardless of -f context."""
    cmd = [
        "psql", dsn or _dsn(),
        "-X",                      # ignore ~/.psqlrc
        "-q",                      # quiet
        "-v", "ON_ERROR_STOP=1",   # fail fast: first error aborts, exit non-zero
        *args,
    ]
    log.info("psql", extra={"context": {"args": list(args), "stdin_path": stdin_path}})
    if stdin_path is not None:
        with open(stdin_path, "rb") as stdin_file:
            subprocess.run(cmd, check=True, stdin=stdin_file)
    else:
        subprocess.run(cmd, check=True)


def copy_to_staging(flat_file_path: str, source: str) -> str:
    """SCD1 staging load: \\copy the flat file into a temp landing table, then
    upsert into staging.<source>_raw on the natural key. ``source`` is one of
    weather/donki/neows."""
    sql_file = STAGING_SQL[source]
    _run_psql(["-f", config.sql_path("upserts", sql_file)], stdin_path=flat_file_path)
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
    _run_psql(["-f", config.sql_path("upserts", sql_file)], stdin_path=str(flat_path))


def log_call_metrics(metrics_rows: list[dict[str, Any]]) -> None:
    """Append API call metrics to open_sky_logs.api_call_log. Accepts one dict
    or a list; a single failed call still gets recorded."""
    if isinstance(metrics_rows, dict):
        metrics_rows = [metrics_rows]
    _copy_log(metrics_rows, API_CALL_LOG_COLUMNS, "log_api_call.sql", "api_call")


def load_audit_findings(finding_rows: list[dict[str, Any]]) -> None:
    """Append audit findings to open_sky_logs.audit_findings."""
    _copy_log(finding_rows, AUDIT_FINDING_COLUMNS, "write_audit_findings.sql", "audit")
