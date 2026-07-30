"""space_weather_dag (@daily) — NASA DONKI + NeoWs (both NASA, both daily, so
one DAG). Section 7.

Two parallel TaskGroups:
  * donki: extract CME/GST/FLR -> convert -> stage (SCD1) -> upsert
           open_sky.space_weather_events (SCD2)
  * neows: extract 7-day feed -> convert -> stage (SCD1) -> upsert
           open_sky.neo_close_approaches (SCD2) -> derive open_sky.asteroids
           (SCD2, from the fact table's current versions)

One run_audit_checks task per DAG (Section 12), trigger_rule=all_done, after
both groups' upserts + metrics complete."""
from __future__ import annotations

import uuid
from datetime import datetime

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

from etl import config
from etl.audit.checks import run_all_checks
from etl.extract.nasa import extract_donki as fetch_donki
from etl.extract.nasa import extract_neows as fetch_neows
from etl.load import postgres_loader as loader
from etl.transform.space_weather import convert_donki, convert_neows


@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["space_weather", "nasa", "donki", "neows"],
    default_args={"retries": 3, "retry_exponential_backoff": True},
)
def space_weather_dag():

    @task
    def generate_run_id() -> str:
        return str(uuid.uuid4())

    # --- DONKI ---------------------------------------------------------------
    @task(retries=3, retry_exponential_backoff=True)
    def extract_donki(run_id: str) -> list[dict]:
        out = []
        for event_type in config.DONKI_EVENT_TYPES:
            json_path, metrics = fetch_donki(event_type, pipeline_run_id=run_id)
            out.append({"json_path": json_path, "metrics": metrics, "event_type": event_type})
        return out

    @task
    def convert_donki_files(extracts: list[dict]) -> dict:
        flats = [convert_donki(e["json_path"], e["event_type"]) for e in extracts]
        raw = {f"donki_{e['event_type']}": e["json_path"] for e in extracts}
        return {"flats": flats, "raw": raw}

    @task
    def copy_donki_to_staging(converted: dict) -> dict:
        for flat in converted["flats"]:
            loader.copy_to_staging(flat, "donki")
        return converted

    @task
    def upsert_donki(_staged: dict, run_id: str) -> None:
        loader.upsert_to_open_sky("space_weather_events.sql", run_id=run_id)

    @task
    def log_metrics_donki(extracts: list[dict]) -> None:
        loader.log_call_metrics([e["metrics"] for e in extracts])

    # --- NeoWs ---------------------------------------------------------------
    @task(retries=3, retry_exponential_backoff=True)
    def extract_neows(run_id: str) -> dict:
        json_path, metrics = fetch_neows(pipeline_run_id=run_id)
        return {"json_path": json_path, "metrics": metrics}

    @task
    def convert_neows_files(extract: dict) -> dict:
        flat = convert_neows(extract["json_path"])
        return {"flats": [flat], "raw": {"neows": extract["json_path"]}}

    @task
    def copy_neows_to_staging(converted: dict) -> dict:
        for flat in converted["flats"]:
            loader.copy_to_staging(flat, "neows")
        return converted

    @task
    def upsert_neows(_staged: dict, run_id: str) -> None:
        loader.upsert_to_open_sky("neo_close_approaches.sql", run_id=run_id)

    @task
    def derive_asteroids(_upserted: None) -> None:
        loader.derive_asteroids()

    @task
    def log_metrics_neows(extract: dict) -> None:
        loader.log_call_metrics([extract["metrics"]])

    # --- Audit (one per DAG) -------------------------------------------------
    @task(trigger_rule="all_done")
    def run_audit_checks(run_id: str, donki_conv: dict, neows_conv: dict) -> None:
        drift = {**donki_conv.get("raw", {}), **neows_conv.get("raw", {})}
        findings = run_all_checks(run_id, drift)
        loader.load_audit_findings(findings)

    run_id = generate_run_id()

    with TaskGroup("donki"):
        d_ex = extract_donki(run_id)
        d_conv = convert_donki_files(d_ex)
        d_stg = copy_donki_to_staging(d_conv)
        d_ups = upsert_donki(d_stg, run_id)
        d_met = log_metrics_donki(d_ex)

    with TaskGroup("neows"):
        n_ex = extract_neows(run_id)
        n_conv = convert_neows_files(n_ex)
        n_stg = copy_neows_to_staging(n_conv)
        n_ups = upsert_neows(n_stg, run_id)
        n_der = derive_asteroids(n_ups)
        n_met = log_metrics_neows(n_ex)

    audit = run_audit_checks(run_id, d_conv, n_conv)
    [d_ups, d_met, n_der, n_met] >> audit


space_weather_dag()
