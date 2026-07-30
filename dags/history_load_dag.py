"""history_load_dag (schedule=None) — one-time backfill of all three sources
over the shared rolling 2-year window (today-2y -> today-5d). Section 7.

Same extract -> JSON -> convert -> flat -> staging -> open_sky pipeline as the
daily DAGs (same functions, same SQL, same idempotent SCD1/SCD2 loads). The
differences: the date range each extract requests, how many windows it loops,
and that raw JSON lands under data/raw/{source}/historical/ (so a backfill can
never collide with the daily file or another backfill attempt).

Triggered by hand once at init (`airflow dags trigger history_load_dag`). Safe
to re-run — the loads are the same idempotent upserts the daily DAGs use.

Per Section 7: each source is ONE @task that loops its windows in Python (rather
than ~180 Airflow tasks); ~177 NASA calls total, well under the 1000 req/hour
cap, so they fire sequentially with no pacer. upsert_to_open_sky runs once at
the end over everything staged."""
from __future__ import annotations

import uuid
from datetime import datetime

from airflow.decorators import dag, task

from etl import config
from etl.extract.nasa import extract_donki as fetch_donki
from etl.extract.nasa import extract_neows as fetch_neows
from etl.extract.open_meteo import extract_open_meteo as fetch_open_meteo
from etl.load import postgres_loader as loader
from etl.transform.space_weather import convert_donki, convert_neows
from etl.transform.weather import convert_weather
from etl.utils.logging import get_logger
from etl.utils.metrics import CallFailed

log = get_logger("dags.history_load")


@dag(
    schedule=None,          # on-demand only; triggered once at init
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["backfill", "one_time"],
    default_args={"retries": 3, "retry_exponential_backoff": True},
)
def history_load_dag():

    @task
    def generate_run_id() -> str:
        return str(uuid.uuid4())

    @task
    def backfill_weather(run_id: str) -> None:
        start, end = config.backfill_range()
        # Archive endpoint: one call per location covers the whole range.
        for loc in config.LOCATIONS:
            try:
                json_path, metrics = fetch_open_meteo(
                    loc, start_date=start, end_date=end, historical=True, pipeline_run_id=run_id)
            except CallFailed as exc:
                loader.log_call_metrics([exc.metrics.as_row()])  # record the failed call
                raise
            flat = convert_weather(json_path, historical=True)
            loader.copy_to_staging(flat, "weather")
            loader.log_call_metrics([metrics])

    @task
    def backfill_donki(run_id: str) -> None:
        start, end = config.backfill_range()
        for event_type in config.DONKI_EVENT_TYPES:
            for ws, we in config.iter_windows(start, end, config.DONKI_WINDOW_DAYS):
                try:
                    json_path, metrics = fetch_donki(
                        event_type, ws, we, historical=True, pipeline_run_id=run_id)
                except CallFailed as exc:
                    loader.log_call_metrics([exc.metrics.as_row()])  # record the failed call
                    raise
                flat = convert_donki(json_path, event_type, historical=True)
                loader.copy_to_staging(flat, "donki")
                loader.log_call_metrics([metrics])

    @task
    def backfill_neows(run_id: str) -> None:
        start, end = config.backfill_range()
        for ws, we in config.iter_windows(start, end, config.NEOWS_WINDOW_DAYS):
            try:
                json_path, metrics = fetch_neows(ws, we, historical=True, pipeline_run_id=run_id)
            except CallFailed as exc:
                loader.log_call_metrics([exc.metrics.as_row()])  # record the failed call
                raise
            flat = convert_neows(json_path, historical=True)
            loader.copy_to_staging(flat, "neows")
            loader.log_call_metrics([metrics])

    @task
    def upsert_all_to_open_sky(run_id: str) -> None:
        # Runs once over everything staged (SCD2 historize + dimension derive).
        loader.upsert_to_open_sky("weather_daily.sql", run_id=run_id)
        loader.upsert_to_open_sky("space_weather_events.sql", run_id=run_id)
        loader.upsert_to_open_sky("neo_close_approaches.sql", run_id=run_id)
        loader.derive_asteroids()

    run_id = generate_run_id()
    bw = backfill_weather(run_id)
    bd = backfill_donki(run_id)
    bn = backfill_neows(run_id)
    [bw, bd, bn] >> upsert_all_to_open_sky(run_id)


history_load_dag()
