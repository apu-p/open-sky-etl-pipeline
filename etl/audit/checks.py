"""Audit checks (Section 12): the gap Postgres constraints can't cover — things
that need comparing across rows/tables/time, or noticing data that's simply
*missing*.

Each check is a plain SQL query (read-only, run via `psql -c` — the query
strings are static, no pipeline data interpolated) or, for schema drift, a
file-based comparison. Findings are returned as dicts matching
open_sky_logs.audit_findings; the DAG (or __main__) hands them to
etl.load.postgres_loader.load_audit_findings.

A finding never raises / never fails the DAG — it's recorded and surfaced on
Dashboard 4. run_audit_checks runs with trigger_rule=all_done, so audits report
even when the load itself failed (which is exactly what absence detection is
meant to catch).

Run standalone:  python -m etl.audit.checks
"""
from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from etl import config
from etl.utils.logging import get_logger

log = get_logger("etl.audit")

REFERENCE_DIR = config.REPO_ROOT / "reference_schema"


# --- SQL checks --------------------------------------------------------------
# Each entry: name -> (severity, related_table, sql, description-template).
# The template is .format()'d with the columns each row returns.
SQL_CHECKS: list[dict[str, Any]] = [
    {
        "name": "absence_weather_daily",
        "severity": "warning",
        "related_table": "open_sky.weather_daily",
        "sql": """
            SELECT l.location_id, l.name
            FROM open_sky.locations l
            LEFT JOIN open_sky.weather_daily w
              ON w.location_key = l.location_key AND w.date = current_date AND w.close_date IS NULL
            WHERE l.close_date IS NULL AND w.location_key IS NULL
        """,
        "describe": lambda r: f"No current weather_daily row for today for location {r[0]} ({r[1]}).",
    },
    {
        "name": "anomaly_temp_max",
        "severity": "warning",
        "related_table": "open_sky.weather_daily",
        "sql": """
            SELECT date, location_key, temp_max
            FROM open_sky.weather_daily w
            WHERE w.close_date IS NULL
              AND ABS(temp_max - (
                    SELECT AVG(temp_max) FROM open_sky.weather_daily
                    WHERE location_key = w.location_key AND close_date IS NULL
                      AND date BETWEEN w.date - 7 AND w.date - 1
                  )) > 3 * (
                    SELECT STDDEV(temp_max) FROM open_sky.weather_daily
                    WHERE location_key = w.location_key AND close_date IS NULL
                      AND date BETWEEN w.date - 7 AND w.date - 1
                  )
        """,
        "describe": lambda r: f"temp_max {r[2]} on {r[0]} (location_key {r[1]}) is >3 std-dev from the trailing 7-day mean.",
    },
    {
        "name": "xtable_weather_run_id",
        "severity": "warning",
        "related_table": "open_sky.weather_daily",
        "sql": """
            SELECT DISTINCT w.pipeline_run_id
            FROM open_sky.weather_daily w
            LEFT JOIN open_sky_logs.api_call_log a ON a.pipeline_run_id = w.pipeline_run_id
            WHERE w.pipeline_run_id IS NOT NULL AND a.pipeline_run_id IS NULL
        """,
        "describe": lambda r: f"weather_daily pipeline_run_id {r[0]} has no matching api_call_log entry.",
    },
    {
        "name": "xtable_asteroid_dimension_coverage",
        "severity": "warning",
        "related_table": "open_sky.neo_close_approaches",
        "sql": """
            SELECT DISTINCT f.asteroid_id
            FROM open_sky.neo_close_approaches f
            LEFT JOIN open_sky.asteroids a
              ON a.asteroid_id = f.asteroid_id AND a.close_date IS NULL
            WHERE f.close_date IS NULL AND a.asteroid_id IS NULL
        """,
        "describe": lambda r: f"asteroid_id {r[0]} has current close approaches but no current asteroids dimension row (dimension step fell behind).",
    },
    {
        "name": "trend_api_success_rate",
        "severity": "critical",
        "related_table": "open_sky_logs.api_call_log",
        "sql": """
            SELECT source_name,
                   round(AVG(CASE WHEN success THEN 1 ELSE 0 END), 3) AS success_rate_7d
            FROM open_sky_logs.api_call_log
            WHERE called_at > now() - interval '7 days'
            GROUP BY source_name
            HAVING AVG(CASE WHEN success THEN 1 ELSE 0 END) < 0.9
        """,
        "describe": lambda r: f"Source {r[0]} 7-day success rate is {r[1]} (< 0.90).",
    },
]


def _dsn() -> str:
    dsn = config.ETL_DSN or os.environ.get("ETL_DSN", "")
    if not dsn:
        raise RuntimeError("ETL_DSN is not set")
    return dsn


def _psql_query(sql: str) -> list[list[str]]:
    """Run a read-only query, return rows as lists of string cells. Tuple-only,
    unaligned, pipe-separated output for easy parsing."""
    result = subprocess.run(
        ["psql", _dsn(), "-X", "-t", "-A", "-F", "|", "-c", sql],
        check=True, capture_output=True, text=True,
    )
    return [line.split("|") for line in result.stdout.splitlines() if line.strip()]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _finding(check: dict, description: str, run_id: str | None) -> dict[str, Any]:
    return {
        "check_name": check["name"],
        "severity": check["severity"],
        "related_table": check.get("related_table"),
        "description": description,
        "detected_at": _now_iso(),
        "pipeline_run_id": run_id,
    }


def run_sql_checks(run_id: str | None = None) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for check in SQL_CHECKS:
        try:
            rows = _psql_query(check["sql"])
        except subprocess.CalledProcessError as exc:
            log.error("audit query failed",
                      extra={"context": {"check": check["name"], "error": str(exc)}})
            continue
        for row in rows:
            findings.append(_finding(check, check["describe"](row), run_id))
    return findings


# --- Schema-drift check (file-based, Section 12 #2) --------------------------
def _top_level_keys(raw: Any) -> list[str]:
    """Top-level keys of the response. DONKI returns a JSON array, so use the
    keys of its first element."""
    if isinstance(raw, list):
        return sorted(raw[0].keys()) if raw and isinstance(raw[0], dict) else []
    if isinstance(raw, dict):
        return sorted(raw.keys())
    return []


def check_schema_drift(
    raw_json_paths: dict[str, str],
    run_id: str | None = None,
) -> list[dict[str, Any]]:
    """Compare each raw JSON file's top-level keys against a checked-in
    reference. ``raw_json_paths`` maps a reference name (e.g. 'open_meteo',
    'donki_CME', 'neows') to the raw file to check. First time a source is seen,
    the reference is bootstrapped from it and an info finding is emitted."""
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    findings: list[dict[str, Any]] = []

    for name, path in raw_json_paths.items():
        try:
            raw = json.loads(Path(path).read_text())
        except (OSError, json.JSONDecodeError) as exc:
            findings.append({
                "check_name": f"schema_drift_{name}", "severity": "warning",
                "related_table": None,
                "description": f"Could not read raw JSON for {name}: {exc}",
                "detected_at": _now_iso(), "pipeline_run_id": run_id,
            })
            continue

        current = set(_top_level_keys(raw))
        ref_file = REFERENCE_DIR / f"{name}.json"

        if not ref_file.exists():
            ref_file.write_text(json.dumps({"top_level_keys": sorted(current)}, indent=2))
            findings.append({
                "check_name": f"schema_drift_{name}", "severity": "info",
                "related_table": None,
                "description": f"Bootstrapped schema reference for {name}: {sorted(current)}.",
                "detected_at": _now_iso(), "pipeline_run_id": run_id,
            })
            continue

        reference = set(json.loads(ref_file.read_text()).get("top_level_keys", []))
        missing = reference - current
        added = current - reference
        if missing or added:
            findings.append({
                "check_name": f"schema_drift_{name}",
                "severity": "warning" if missing else "info",
                "related_table": None,
                "description": f"Schema drift for {name}: missing={sorted(missing)}, added={sorted(added)}.",
                "detected_at": _now_iso(), "pipeline_run_id": run_id,
            })
    return findings


def run_all_checks(
    run_id: str | None = None,
    raw_json_paths: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """All 5 checks. Returns findings (never raises for a finding)."""
    findings = run_sql_checks(run_id)
    if raw_json_paths:
        findings += check_schema_drift(raw_json_paths, run_id)
    log.info("audit complete", extra={"context": {"findings": len(findings)}})
    return findings


if __name__ == "__main__":
    from etl.load.postgres_loader import load_audit_findings

    results = run_all_checks()
    for f in results:
        print(f["severity"], f["check_name"], "-", f["description"])
    load_audit_findings(results)
