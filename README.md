# Weather & Space ETL Pipeline (`open-sky-etl-pipeline`)

An ETL pipeline that pulls **weather** and **space** data from free public APIs,
loads it into Postgres via `psql` + `COPY` (no Python DB driver), audits it, and
serves it in Superset dashboards — orchestrated by Airflow, all in Docker
Compose.

> Full design rationale is in [`ETL_Project_Plan.md`](ETL_Project_Plan.md).
> Section references throughout the code/comments point back to it.

## Stack

```
Open-Meteo ┐
NASA DONKI ├─▶ Extract (requests) ─▶ raw JSON file ─▶ Convert (pandas/stdlib)
NASA NeoWs ┘        │                                    │ pipe-delimited .psv
                    ▼                                    ▼
              api_call_log            psql \copy ─▶ staging.* (SCD1) ─▶ open_sky.* (SCD2)
                    │                                    │                    │
                    └──────────────▶ Superset ◀──────────┴── audit_findings ──┘
```

- **Extract** — thin API clients; write each response **untouched** as a
  timestamped JSON file (raw artifact for replay + schema-drift).
- **Transform** — pure functions: reshape JSON → pipe-delimited `.psv`. No I/O
  beyond their own files, no validation (that's the DB's job).
- **Load** — `psql \copy` into a temp landing table, then idempotent upsert:
  `staging.*` overwrites in place (SCD Type 1); `open_sky.*` closes-and-inserts
  (SCD Type 2). **Postgres constraints are the only validation layer**, enforced
  fail-fast at insert time.
- **Audit** — 5 cross-row/table/time checks Postgres can't express, written to
  `open_sky_logs.audit_findings`.
- **Orchestrate** — three Airflow DAGs.
- **Serve** — Superset reads `open_sky` + `open_sky_logs` only, never `staging`.

## Data sources

| Source | Data | Auth |
|---|---|---|
| [Open-Meteo](https://open-meteo.com/) | Daily weather + ERA5 archive | none |
| [NASA DONKI](https://api.nasa.gov/) | CME / GST / FLR space-weather events | free key |
| [NASA NeoWs](https://api.nasa.gov/) | Near-Earth asteroid close approaches | free key |

Register a **free** NASA key at <https://api.nasa.gov/> — do **not** use
`DEMO_KEY` (30 req/hr is too low for the daily DONKI + NeoWs pulls).

## Quickstart

```bash
# 1. Configure
cp .env.example .env
#    edit .env: set NASA_API_KEY and change every *_PASSWORD / SECRET_KEY.
#    keep the passwords in the DSNs consistent with the *_PASSWORD values.

# 2. Bring up Postgres + Airflow + Superset
docker compose up -d --build
#    On first boot, Postgres runs the role/DDL/grant bootstrap and seeds
#    the locations dimension (docker/init-db.sh, Sections 8/13).

# 3. Airflow UI: http://localhost:8080   (creds from .env _AIRFLOW_WWW_USER_*)
#    Superset UI: http://localhost:8088   (creds from .env SUPERSET_ADMIN_*)
```

### Backfill first, then run daily

```bash
# One-time historical backfill (rolling 2-year window, ~177 NASA calls,
# well inside the free tier — fires sequentially, no pacing):
docker compose exec airflow-scheduler airflow dags trigger history_load_dag

# Then enable the daily DAGs (unpause in the UI, or):
docker compose exec airflow-scheduler airflow dags unpause weather_dag
docker compose exec airflow-scheduler airflow dags unpause space_weather_dag
```

Then wire up Superset (add the `SUPERSET_DSN` connection, build charts) — see
[`superset/README.md`](superset/README.md).

## Running a layer standalone

Every stage is independently runnable (no cross-layer imports), which is how you
verify behavior during development — there's no parallel test suite:

```bash
# needs NASA_API_KEY + DATA_DIR in the env (or a .env)
python -m etl.extract.open_meteo
python -m etl.extract.nasa
python -m etl.transform.weather        data/raw/weather/1_<stamp>.json
python -m etl.transform.space_weather  neows data/raw/neows/<date>_<stamp>.json
python -m etl.audit.checks             # needs ETL_DSN
```

## Repository layout

```
docker-compose.yml            # Postgres + Airflow + Superset
docker/
  Dockerfile.airflow          # Airflow image + psql client + etl deps
  init-db.sh                  # first-boot roles -> DDL -> grants -> seed
dags/                         # weather / space_weather / history_load DAGs
etl/
  config.py                   # env-sourced settings, LOCATIONS, backfill window
  extract/{open_meteo,nasa}.py
  transform/{weather,space_weather,common}.py
  load/postgres_loader.py     # all DB access = psql + COPY via subprocess
  audit/checks.py             # the 5 audit checks
  utils/{logging,retry,metrics,artifacts,psv}.py
sql/
  roles/   {01_create_roles, 06_grants}.sql
  ddl/     {02_schemas_and_staging, 03_dimensions, 04_facts, 05_logs}.sql
  upserts/ staging (SCD1) + open_sky (SCD2) + derive_asteroids + log/audit
  seed/    locations_seed.sql
superset/                     # config + dashboard query reference
reference_schema/             # bootstrapped schema-drift references
```

## Data model (three schemas)

- **`staging.*`** — flattened, typed rows, one per natural key, **SCD1**
  (overwrite). Raw JSON is kept as a file, not a column.
- **`open_sky.*`** — analytics-ready domain data, **SCD2** (a changed value
  closes the old version, inserts a new one). `locations` is seeded before
  facts; `asteroids` is *derived from* `neo_close_approaches` after it loads.
- **`open_sky_logs.*`** — data about the pipeline: `api_call_log`,
  `audit_findings`.

## Roles (least privilege, Section 13)

Four `NOLOGIN` group roles, each fronted by a `LOGIN` role:
`dba_role` (DDL only), `etl_role` (the only writer), `developer_role` (read-all),
`superset_role` (read only the tables the dashboards query). No app or person
connects as the Postgres superuser after init.

## Notes / deviations from the plan

- `pipeline_run_id` is bound into the SCD2 upserts as a `psql` variable
  (`-v run_id=<uuid>`) passed as a subprocess arg — the staging tables carry
  only `fetched_at` (mapped to `source_fetched_at`), so run ids don't need a
  staging column. This keeps the "no shell string-building with pipeline data"
  rule while still populating `pipeline_run_id`.
- The WMO weather-code → condition lookup lives in
  `sql/upserts/weather_daily.sql` (the only place `weather_condition` is
  populated), since `staging.weather_raw` doesn't carry that column.
- Schema-drift references self-bootstrap on first sight of a source (see
  `reference_schema/README.md`).
