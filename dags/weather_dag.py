"""weather_dag (@daily) — Open-Meteo -> staging.weather_raw (SCD1) ->
open_sky.weather_daily (SCD2). Section 7.

Task graph (matches the plan):
    generate_run_id
    extract_open_meteo -> convert_to_flat_file -> copy_to_staging -> upsert_to_open_sky
    extract_open_meteo -> log_call_metrics
    [upsert_to_open_sky, log_call_metrics] -> run_audit_checks (all_done)

Each task processes all seeded locations (config.LOCATIONS) in one go and passes
file paths / metrics between tasks via XCom — never the raw JSON itself."""
from __future__ import annotations

import uuid
from datetime import datetime

from airflow.decorators import dag, task

from etl import config
from etl.audit.checks import run_all_checks
from etl.extract.open_meteo import extract_open_meteo as fetch_open_meteo
from etl.load import postgres_loader as loader
from etl.transform.weather import convert_weather


@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["weather", "open_meteo"],
    default_args={"retries": 3, "retry_exponential_backoff": True},
)
def weather_dag():

    @task
    def generate_run_id() -> str:
        return str(uuid.uuid4())

    @task(retries=3, retry_exponential_backoff=True)
    def extract_open_meteo(run_id: str) -> list[dict]:
        # Writes raw JSON files (untouched) and returns [{json_path, metrics}].
        out = []
        for loc in config.LOCATIONS:
            json_path, metrics = fetch_open_meteo(loc, pipeline_run_id=run_id)
            out.append({"json_path": json_path, "metrics": metrics})
        return out

    @task
    def convert_to_flat_file(extracts: list[dict]) -> dict:
        flats = [convert_weather(e["json_path"]) for e in extracts]
        return {"flats": flats, "raw_paths": [e["json_path"] for e in extracts]}

    @task
    def copy_to_staging(converted: dict) -> dict:
        for flat in converted["flats"]:
            loader.copy_to_staging(flat, "weather")
        return converted

    @task
    def upsert_to_open_sky(_staged: dict, run_id: str) -> None:
        loader.upsert_to_open_sky("weather_daily.sql", run_id=run_id)

    @task
    def log_call_metrics(extracts: list[dict]) -> None:
        loader.log_call_metrics([e["metrics"] for e in extracts])

    @task(trigger_rule="all_done")  # audit even if the load failed
    def run_audit_checks(run_id: str, converted: dict) -> None:
        raw_paths = converted.get("raw_paths", [])
        drift = {"open_meteo": raw_paths[0]} if raw_paths else {}
        findings = run_all_checks(run_id, drift)
        loader.load_audit_findings(findings)

    run_id = generate_run_id()
    extracts = extract_open_meteo(run_id)
    converted = convert_to_flat_file(extracts)
    staged = copy_to_staging(converted)
    upserted = upsert_to_open_sky(staged, run_id)
    metrics_done = log_call_metrics(extracts)
    audit = run_audit_checks(run_id, converted)
    [upserted, metrics_done] >> audit


weather_dag()
