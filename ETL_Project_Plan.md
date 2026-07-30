# Weather & Space ETL Pipeline — Project Plan

## 1. Overview

An ETL pipeline that pulls weather and space data (space weather events, near-Earth asteroids) from free public APIs, transforms and loads it into Postgres, and visualizes it in Superset. Runs locally via Docker Compose.

**Stack:** Python (extract/transform) → Postgres (storage) → Apache Superset (dashboards), orchestrated with Airflow, all containerized.

## 2. Data Sources

| Source | Data | Auth | Rate limit | Notes |
|---|---|---|---|---|
| [Open-Meteo](https://open-meteo.com/) | Current + forecast weather, historical climate | None required | ~10k calls/day free | No API key — easiest to start with |
| [NASA DONKI](https://api.nasa.gov/) | Space weather events: coronal mass ejections (CME), geomagnetic storms (GST), solar flares (FLR) | Free API key (registered — not `DEMO_KEY`, see rate limit note) | 1000 req/hour with key | Pure structured JSON — no images/media, purely numeric/text fields |
| [NASA NeoWs](https://api.nasa.gov/) | Near-Earth object (asteroid) tracking, close-approach data | Same NASA key | Same as above | Time-series-like feed data, richer transform logic |

Note: use a registered NASA API key (free signup at api.nasa.gov), not `DEMO_KEY` — `DEMO_KEY` is capped at 30 requests/hour, 50/day per IP, which is too low for a scheduled Airflow pipeline hitting DONKI and NeoWs daily. A registered key raises this to 1000 requests/hour.

**Historical data availability, per source** (drives the one-time backfill design in Section 7):

| Source | How far back it *could* go | Per-request window limit | Endpoint |
|---|---|---|---|
| Open-Meteo | 1940-01-01 (ERA5/ERA5-Land reanalysis) | No documented per-request cap — one call can span the full range for a location | **Different endpoint** than the daily forecast pull: `archive-api.open-meteo.com/v1/archive`, not the forecast API |
| NASA DONKI | ~2010 onward (when DONKI itself was established) | **30 days per request**, enforced — a request spanning more than that gets truncated to the last 30 days ending at `endDate` | Same `DONKI/{CME,GST,FLR}` endpoints as the daily pull, just looped over date windows |
| NASA NeoWs | JPL's underlying orbital data technically spans centuries | **7 days per request**, hard limit on the `feed` endpoint | Same `feed` endpoint as the daily pull |

**All three sources backfill the same window: today − 2 years → today − 5 days** (a rolling 2-year window). A shared window is what keeps the sources jointly analyzable ("was there a solar storm during this cold spell?") — every source has data over the same span. Two years is a deliberate scope choice, not a source-imposed floor: all three could go much deeper (Open-Meteo to 1940, NeoWs effectively unbounded, DONKI to ~2010), but two years fully populates every dashboard while keeping the one-time backfill trivially inside NASA's free tier (Section 7), so there's no reason to go further for a demo. If deep history is ever wanted the window just widens — at which point DONKI's ~2010 start becomes the real floor, and NASA rate-limit pacing (which a 2-year window avoids entirely) has to come back.

Section 7 works out the request counts for this 2-year window: ~177 NASA calls total, well under the free-tier cap, so the backfill fires sequentially with no pacing at all.

Optional stretch source: [OpenAQ](https://docs.openaq.org/) for air quality, to join against weather by location.

## 3. Architecture

```mermaid
flowchart TB
    subgraph SOURCES["Free APIs"]
        OM["Open-Meteo\n(weather)"]
        DONKI["NASA DONKI\n(space weather events)"]
        NEO["NASA NeoWs\n(asteroids)"]
    end

    subgraph ETL["ETL Pipeline (Python, orchestrated by Airflow)"]
        direction TB
        EX["Extract\nrequests / API clients\n+ retry w/ backoff"]
        METRICS["Call-metrics capture\n(latency, status, retries,\nrate-limit headroom)"]
        TR["Transform\npandas\nclean / reshape only"]
        LD["Load\npsql + COPY (raw SQL)\nidempotent upserts,\nPostgres constraints validate"]
        AUD["Audit checks\n(absence, drift, anomalies,\ncross-table, trend signals)"]

        EX --> METRICS
        EX --> TR --> LD --> AUD
    end

    subgraph PG["Postgres"]
        direction TB
        STG[("staging.*\nflattened, SCD1 (overwrite)")]
        CUR[("open_sky.*\ndomain data, SCD2")]
        LOG[("open_sky_logs.api_call_log")]
        FIND[("open_sky_logs.audit_findings")]
        STG --> CUR
    end

    subgraph BI["Superset Dashboards"]
        D1["Weather Trends"]
        D2["Near-Earth Objects"]
        D3["Space Weather Events"]
        D4["API Health & Reliability\n+ Audit Findings"]
    end

    OM --> EX
    DONKI --> EX
    NEO --> EX

    LD --> STG
    METRICS --> LOG
    AUD --> FIND

    CUR --> D1
    CUR --> D2
    CUR --> D3
    LOG --> D4
    FIND --> D4

    AF["Airflow DAGs\nschedule, retries, alerting,\nbackfills"] -.orchestrates.-> ETL
```

Layers:

- **Extract** — thin API clients, one per source, responsible only for fetching raw JSON and handling pagination/retries.
- **Transform** — pure functions that clean and reshape raw JSON into tabular records (flatten nested arrays, cast types, derive fields). No validation here — reshaping only. No I/O here either — makes this layer unit-testable.
- **Load** — idempotent upserts into Postgres staging tables, then `open_sky` tables; **validation happens here, enforced by Postgres itself** (`NOT NULL`, `CHECK`, `UNIQUE`, foreign keys) at insert time, not by a separate Python validation library.
- **Orchestrate** — Airflow schedules extract → transform → load as a DAG, handles retries/alerting/backfills.
- **Serve** — Superset connects to Postgres and reads from `open_sky` and `open_sky_logs` only, never `staging`.

## 4. Repository Structure (industry-standard layout)

```
open-sky-etl-pipeline/
├── docker-compose.yml
├── .env.example
├── .gitignore                  # must include .env — see note below
├── pyproject.toml              # or requirements.txt
├── README.md
├── dags/
│   ├── weather_dag.py
│   ├── space_weather_dag.py     # DONKI + NeoWs task groups (see Section 7)
│   └── history_load_dag.py      # one-time backfill, schedule=None (see Section 7)
├── etl/
│   ├── __init__.py
│   ├── config.py                # settings via python-dotenv, reads .env
│   ├── extract/
│   │   ├── __init__.py
│   │   ├── open_meteo.py
│   │   └── nasa.py
│   ├── transform/
│   │   ├── __init__.py
│   │   ├── weather.py
│   │   └── space_weather.py     # DONKI (CME/GST/FLR) + NeoWs transforms
│   ├── load/
│   │   ├── __init__.py
│   │   └── postgres_loader.py    # writes reshaped output to pipe-delimited
│   │                              # flat files, then shells out to `psql`
│   │                              # (\copy + upsert .sql files) — no Python DB driver
│   ├── audit/                    # the 5 checks (Section 12), run after every load
│   │   ├── __init__.py
│   │   └── checks.py
│   └── utils/
│       ├── logging.py
│       └── retry.py
├── sql/
│   ├── ddl/                     # CREATE TABLE statements, versioned
│   ├── roles/                    # CREATE ROLE (before DDL) + GRANT/REVOKE (after
│   │                              # DDL) statements — Section 13, run once at init
│   └── upserts/                  # INSERT ... ON CONFLICT .sql files, run via `psql -f`
└── superset/
    ├── superset_config.py         # SECRET_KEY, metadata DB, feature flags
    └── README.md                  # one-time DB-connection setup + the per-chart
                                    # SQL for all four dashboards; `dashboard_export.json`
                                    # is not checked in — it's produced on demand via
                                    # `superset export-dashboards` once dashboards are built
```

No separate `tests/` directory — the extract/transform/load scripts themselves are run directly (e.g. `python -m etl.extract.open_meteo`) to verify behavior during development, rather than maintaining a parallel automated test suite.

**`.gitignore`** — the one file that actually makes `.env.example` (above) meaningful; without it, nothing stops the real `.env` (live `NASA_API_KEY`, DB passwords, `SUPERSET_SECRET_KEY`) from getting committed alongside it:
```
.env

__pycache__/
*.pyc
.venv/
*.egg-info/

logs/
*.log

.DS_Store
.vscode/
.idea/
```
`logs/` covers Airflow's local task-log output (written under the project directory even though task *data* flows through Postgres); `.env` is the line that matters most — everything else is convenience.

Design principles applied:

- **Separation of concerns** — extract/transform/load are independent modules with no cross-layer imports, so each can be run and checked standalone (e.g. `python -m etl.transform.weather`) and swapped out independently.
- **Idempotency** — every load is keyed on a natural key (e.g., `location_id`, `asteroid_id + close_approach_date`). Staging upserts overwrite in place on that key (SCD1); `open_sky` loads use the same key as the *conflict-detection* target but close-and-insert instead of overwrite (SCD2, Section 6) — either way, re-running a DAG never produces duplicate current rows.
- **Config via environment**, not hardcoded — API keys, DB creds, schedule intervals all come from `.env` / Airflow Variables.
- **DB-enforced validation** — Postgres constraints (`NOT NULL`, `CHECK`, `UNIQUE`, foreign keys) are the single source of truth for what a valid record looks like. Python does not re-implement schema rules; it only reshapes data and lets the insert fail or succeed.
- **Fail-fast load** — the load step writes reshaped output to a file, `\copy`s it into a temp landing table, then upserts into the target in a single transaction. Any row that violates a constraint (or a value that won't cast) aborts the statement, so the batch loads whole or not at all — no partial load, no silently-skipped rows. A failed load fails the Airflow task and fires an alert, which is the intended signal: bad data means production support contacts the source team, not that the pipeline quietly drops rows and carries on.
- **No shell string-building with pipeline data** — any `psql` invocation where the command includes a value that came from the pipeline itself (e.g. a file path returned by an upstream task) is run as a `PythonOperator` calling `subprocess.run([...])` with an argument list, not a `BashOperator` with the value Jinja-templated into a shell string. `BashOperator` is fine when the command is fully static (e.g. `psql -f some/checked-in.sql`).
- **Logging & observability** — structured logging (JSON logs) at each stage, row counts in/out, and Airflow task-level alerting on failure.
- **Three schemas, not two** — flattened, typed rows land in `staging.*` tables, one row per natural key, overwritten on each pull (**SCD Type 1** — see Section 6; the raw JSON itself is saved as a file, not a Postgres column); cleaned, joined, deduplicated *domain* data lives in `open_sky.*` (**SCD Type 2** — a changed value doesn't overwrite, it closes the old version and inserts a new one, preserving history); data *about the pipeline itself* (call metrics, audit findings) lives in a separate `open_sky_logs.*` schema, not mixed into `open_sky`. Superset reads from `open_sky` + `open_sky_logs`; nothing ever reads from `staging`.
- **Dimensions can be populated from fact data, not just source APIs** — `open_sky.locations` is manually seeded (nothing in the raw weather data to derive it from), but `open_sky.asteroids` is derived *from* `open_sky.neo_close_approaches` after the fact table loads: the per-asteroid descriptive attributes (`name`, diameter, hazard flag) that show up repeated across many close-approach fact rows get extracted into their own SCD2 dimension by a transform step, rather than being pulled from a separate API response.

## 5. Transformations

**File flow (all three sources, same shape):**
1. `extract` calls the API and writes the response **untouched, as a JSON file** on the shared volume — this is the raw artifact used for replay/debugging and Section 12's schema-drift check.
2. A **conversion step** reads that JSON file and does the actual reshaping described below (flatten, cast, derive, dedupe), writing the result as a **pipe-delimited flat file** — column order matches the target `staging.*` table (Section 6). The file is written with **RFC-4180 CSV quoting** using Python's stdlib `csv.writer(f, delimiter='|')` (no third-party dependency), so any text field that itself contains the delimiter, a quote character, or a newline (asteroid `name`, DONKI `source_location`, an audit `description`) is quoted/escaped instead of splitting the row — which is exactly what the load's `\copy ... WITH (FORMAT csv, DELIMITER '|')` (Section 6) reads back. `csv.writer` also emits a Python `None` as an unquoted empty field, which `COPY` interprets as SQL `NULL`, matching the many nullable DONKI columns.
3. `psql \copy` loads that flat file into a temp landing table, then an upsert (Section 6) writes it into the real `staging.*` table on the natural key.

Only step 2 is "the transform" in the traditional ETL sense (pure function, no I/O beyond reading/writing its own input/output files); steps 1 and 3 are file/DB plumbing, covered in Section 7's DAG tasks.

**Weather (Open-Meteo)**
- Flatten the nested hourly/daily JSON arrays into row-per-day records
- Cast types: strings → floats/dates (Open-Meteo returns some numerics as strings)
- Unit normalization: confirm everything's in metric (°C, mm, km/h)
- Derive fields: daily temp range (max − min), `is_rain_day` boolean (precipitation > 0)
- Map Open-Meteo's numeric weather codes to human-readable conditions via a lookup table
- Deduplicate on `(location_id, date)` before load, keeping the most recently fetched value

**Space Weather — NASA DONKI**
- Query CME, GST, and FLR endpoints separately (each has its own JSON shape) over a rolling date-range window, similar to NeoWs
- Flatten each event type into its own row: CME → source location, active region, speed (km/s), direction/half-angle; GST → start time, Kp-index (storm severity); FLR → begin/peak/end time, flare class (e.g. `X1.5`, `M2.3`)
- Parse flare class into a sortable numeric severity score (flare classes are ordinal strings — X > M > C > B > A — so a derived `flare_severity_score` makes them usable in charts/sorts)
- Link related events where the API provides it (e.g., a CME's `linkedEvents` referencing a GST it triggered) — kept as a nullable reference column, not a hard FK, since not all events have a link
- Deduplicate on `(event_type, event_id)` before load — DONKI issues a unique ID per event that's stable across overlapping window queries

**NeoWs (asteroids)**
- Flatten/explode nested response (date → asteroid list → close-approach events) into one row per `(asteroid_id, close_approach_date)`
- Unit conversion: standardize distance/velocity to km / kph, drop miles/lunar-distance fields
- Derive `estimated_diameter_m_max` from the min/max meter fields provided
- Cast `is_potentially_hazardous_asteroid` to boolean
- Dedupe across overlapping 7-day rolling-window pulls
- Split output into two shapes at this stage: close-approach measures (`asteroid_id`, `close_approach_date`, `miss_distance_km`, `relative_velocity_kph`) go to `open_sky.neo_close_approaches`; per-asteroid descriptive attributes (`name`, `estimated_diameter_m_max`, `is_potentially_hazardous`) are deduped to one row per `asteroid_id` and go to the separate `open_sky.asteroids` dimension-extraction step (Section 6) — they aren't repeated on the fact table

**Cross-cutting (all sources)**
- No standalone validation library in the transform layer — records are reshaped and typed with plain Python/pandas, then handed to the load step where Postgres constraints (`NOT NULL`, `CHECK`, `UNIQUE`, foreign keys) are the actual validation gate
- Add pipeline metadata columns: `source_fetched_at`, `pipeline_run_id` for traceability
- Shape output to always include the natural key columns needed for upsert in the load step, so re-runs overwrite cleanly instead of duplicating

**API call metrics (observability transform)**
- Wrap every extract function in a decorator/context manager that captures, per call: `source_name`, `response_time_ms`, `http_status_code`, `success`, `retry_count`, `records_returned`, `rate_limit_remaining` (parsed from response headers where the source provides them, e.g. NASA's `X-RateLimit-Remaining`), `called_at`
- This runs independently of the domain transform logic (weather/DONKI/NeoWs) — it's metadata *about* the call, not the data itself — so it never blocks or fails the main pipeline if something's off
- **Failed calls are recorded too, not just successes.** When an extract exhausts its retries the HTTP helper (`etl/utils/metrics.py`) raises `CallFailed`, which *carries* the partial `CallMetrics` (`success=False`, status code, latency). Each `extract_*` task catches `CallFailed`, writes that failed metric (plus any calls that already succeeded in the same task) to `api_call_log` via `log_call_metrics`, and only then re-raises so the task still fails fast and Airflow retries/alerts. This is essential: if only successful calls were logged, the per-source success rate (Dashboard 4) would be structurally pinned at ~100% and audit check #5 (`trend_api_success_rate`, Section 12) could never fire. The re-raise means the DAG's separate `log_call_metrics` task is skipped on the failure path, so a call is never double-logged.
- Output feeds a dedicated `open_sky_logs.api_call_log` table (see Data Modeling below), which is what makes it possible to compare sources empirically instead of by published docs alone: actual observed latency, actual success rate, actual data-quality drift over time

## 6. Data Modeling

**Approach:** three schemas, separating raw data, domain data, and pipeline-about-itself data, plus a change-handling strategy that differs by layer —
1. **`staging`** — flattened, typed rows (one per natural key), **SCD Type 1**: each new pull overwrites the previous one in place. Latest snapshot only, no history. The *raw* JSON response is saved as a file on the shared volume, not as a Postgres column — a separate conversion step reshapes it into the pipe-delimited file that actually gets `\copy`'d into these tables (see below).
2. **`open_sky`** — modeled, analytics-ready **domain** data (weather, space weather, asteroids) and nothing else. Facts (`open_sky.weather_daily`, `open_sky.space_weather_events`, `open_sky.neo_close_approaches`) are **SCD Type 2**: a changed value for the same natural key doesn't overwrite the existing row, it closes it out (`close_date = now()`) and inserts a new version (`open_date = now()`, `close_date = NULL`). Dimensions (`open_sky.locations`, `open_sky.asteroids`) are also SCD Type 2, but populated differently: `locations` is manually seeded (nothing in the weather API response to derive it from) and loaded *before* facts, so `weather_daily` can FK straight into it. `asteroids` is the opposite — it's *derived from* `neo_close_approaches` by a transform step that runs after the fact table loads, extracting the per-asteroid attributes (`name`, diameter, hazard flag) that repeat across that asteroid's close-approach rows.
3. **`open_sky_logs`** — data *about the pipeline*, not domain data: `open_sky_logs.api_call_log` and `open_sky_logs.audit_findings`. Kept in its own schema, separate from `open_sky`, so "what dashboards read for weather/asteroid analysis" and "what dashboards read for pipeline health" are never accidentally conflated by a wildcard grant or a `SELECT * FROM open_sky.*`-style query — Section 13's roles grant them independently.

**Entity relationship (text form):**

```
open_sky.locations (1, SCD2, seeded before facts) ──< (many) open_sky.weather_daily (SCD2)
open_sky.neo_close_approaches (SCD2) ──> open_sky.asteroids (1, SCD2, derived from the fact table, effective-dated join — no FK)
open_sky.space_weather_events (SCD2)    (standalone, no dimension — global feed)
open_sky_logs.api_call_log, open_sky_logs.audit_findings    (standalone, reference open_sky rows via pipeline_run_id, not FK-enforced — cross-schema)
```

**File flow feeding staging (Section 5/7 cover the DAG tasks that do this):** `extract` calls the API and saves the response **untouched, as a JSON file** on the shared volume, and every file `extract` produces gets a `_{fetched_at}` timestamp in its name (UTC, `YYYYMMDDTHHMMSSZ` — e.g. `20260730T140512Z`): `data/raw/weather/{location_id}_{fetched_at}.json`. A separate conversion step then reads that JSON file and writes a **pipe-delimited flat file**, one row per natural key, columns matching the staging table below. `psql \copy` loads *that* flat file into staging — so the raw JSON itself never lands in Postgres; only its flattened, typed form does. The JSON file is what Section 12's schema-drift check reads.

The timestamp means daily files **no longer overwrite each other** — each run leaves its own file behind, which incidentally gives back the on-disk replay history Section 6's earlier trade-off note said you'd only get by deliberately changing the retention policy; that's effectively what this is. The trade-off flips accordingly: unbounded disk growth on `data/raw/` and `data/flat/` over time, since nothing here deletes old files — worth a periodic cleanup task (or object-storage lifecycle rule, if this ever moves off local volume) if disk space becomes a real constraint, but out of scope for this plan otherwise. **The Postgres side is unaffected** — `staging.*` is still SCD1, still overwritten in place on the natural key (Section 6's DB-level upsert logic doesn't change); only the source files persist now.

**Historical backfill writes to a separate, distinctly named path**, never the daily file: `data/raw/{source}/historical/{key}_{window_start}_{fetched_at}.json` (e.g. `data/raw/donki/historical/CME_2011-03-01_20260730T140512Z.json`, `data/raw/neows/historical/2014-07-14_20260730T140830Z.json`). Two identifiers, two different jobs: `{window_start}` is the business date range being backfilled (so windows never collide with each other), `{fetched_at}` is when the backfill actually made that request (so a re-run of the backfill — say, after a partial failure — doesn't silently overwrite the first attempt's file either).

**The converted flat file follows the identical pattern, one directory over** (`data/flat/...` instead of `data/raw/...`, `.psv` instead of `.json`):
- Daily: `data/flat/{source}/{key}_{fetched_at}.psv`
- Historical: `data/flat/{source}/historical/{key}_{window_start}_{fetched_at}.psv`

All three staging tables are **SCD Type 1** — loaded via `INSERT ... ON CONFLICT (natural key) DO UPDATE`, so each new pull overwrites the row for that key rather than appending. Trade-off: no history of past pulls for replay/debugging *in Postgres* — only the latest flattened snapshot is ever in `staging.*`. That's no longer true on disk, though: since raw JSON and flat files are timestamped (above) rather than overwritten, every past pull's file is still sitting on the volume even though the DB row for it is long gone — so replay/debugging works by reading an old file directly, not by querying staging.

**`staging.weather_raw`** — natural key: `(location_id, date)` — Open-Meteo returns a multi-day forecast per call, so the flattened form has one row per day, not one row per location
| Column | Type | Notes |
|---|---|---|
| location_id | int | part of PK |
| date | date | part of PK |
| temp_min | numeric(5,2) | |
| temp_max | numeric(5,2) | |
| precipitation_mm | numeric(6,2) | |
| wind_speed_max | numeric(6,2) | |
| weather_code | int | |
| fetched_at | timestamptz | when the API call was made |
| — | — | **PK:** `(location_id, date)` |

**`staging.donki_raw`** — natural key: `event_id` (DONKI's own stable ID, unique across CME/GST/FLR)
| Column | Type | Notes |
|---|---|---|
| event_id | text | PK |
| event_type | text | `CME`, `GST`, or `FLR` |
| event_time | timestamptz | |
| source_location | text | nullable — CME/FLR only |
| speed_kms | numeric(8,2) | nullable — CME only |
| kp_index | numeric(4,2) | nullable — GST only |
| flare_class | text | nullable — FLR only |
| flare_severity_score | numeric(6,2) | nullable — derived in the conversion step, FLR only |
| linked_event_id | text | nullable |
| fetched_at | timestamptz | |

**`staging.neows_raw`** — natural key: `(asteroid_id, close_approach_date)`
| Column | Type | Notes |
|---|---|---|
| asteroid_id | text | part of PK |
| close_approach_date | date | part of PK |
| name | text | |
| miss_distance_km | numeric(12,2) | |
| relative_velocity_kph | numeric(10,2) | |
| estimated_diameter_m_max | numeric(8,2) | derived in the conversion step from the min/max meter fields |
| is_potentially_hazardous | boolean | |
| fetched_at | timestamptz | |
| — | — | **PK:** `(asteroid_id, close_approach_date)` |

**`open_sky.locations`** (dimension, SCD Type 2)
| Column | Type | Constraints |
|---|---|---|
| location_key | bigserial | PK — surrogate key, one row per version |
| location_id | int | `NOT NULL` — stable business key, repeats across versions |
| name | text | `NOT NULL` |
| lat | numeric(8,5) | `NOT NULL`, `CHECK (lat BETWEEN -90 AND 90)` |
| lon | numeric(8,5) | `NOT NULL`, `CHECK (lon BETWEEN -180 AND 180)` |
| timezone | text | |
| open_date | timestamptz | `NOT NULL DEFAULT now()` — when this version was loaded, app-populated, not from API data |
| close_date | timestamptz | nullable — when the *next* version was loaded; `NULL` = this is the current version |
| — | — | **Unique index:** `(location_id) WHERE close_date IS NULL` — enforces exactly one current version per location |

**`open_sky.weather_daily`** (fact, SCD Type 2)
| Column | Type | Constraints |
|---|---|---|
| location_key | int | `NOT NULL`, `FK → open_sky.locations.location_key` |
| date | date | `NOT NULL`, `CHECK (date <= current_date + interval '16 days')` (forecast horizon) |
| temp_min | numeric(5,2) | `CHECK (temp_min > -100)` |
| temp_max | numeric(5,2) | `CHECK (temp_max < 100)`, `CHECK (temp_max >= temp_min)` |
| temp_range | numeric(5,2) | derived: temp_max − temp_min |
| precipitation_mm | numeric(6,2) | `CHECK (precipitation_mm >= 0)` |
| is_rain_day | boolean | derived, `NOT NULL DEFAULT false` |
| wind_speed_max | numeric(6,2) | `CHECK (wind_speed_max >= 0)` |
| weather_code | int | |
| weather_condition | text | derived from weather_code lookup |
| source_fetched_at | timestamptz | `NOT NULL` |
| pipeline_run_id | uuid | |
| open_date | timestamptz | `NOT NULL DEFAULT now()` — when this version of the row was loaded, app-populated |
| close_date | timestamptz | nullable — set when a revised value for the same `(location_key, date)` lands; `NULL` = current version |
| — | — | **PK:** `(location_key, date, open_date)`; **partial unique index:** `(location_key, date) WHERE close_date IS NULL` (current-version lookup + upsert conflict target) |

**`open_sky.space_weather_events`** (fact, SCD Type 2)
| Column | Type | Constraints |
|---|---|---|
| event_id | text | `NOT NULL` — DONKI's own stable event ID, e.g. `2026-07-20-CME-001`; part of PK |
| event_type | text | `NOT NULL`, `CHECK (event_type IN ('CME', 'GST', 'FLR'))` |
| event_time | timestamptz | `NOT NULL` — start/peak time depending on type |
| source_location | text | nullable — solar source region, CME/FLR only |
| speed_kms | numeric(8,2) | `CHECK (speed_kms >= 0)` — CME only, null otherwise |
| kp_index | numeric(4,2) | `CHECK (kp_index BETWEEN 0 AND 9)` — GST only, null otherwise |
| flare_class | text | nullable — raw class string, e.g. `X1.5` — FLR only |
| flare_severity_score | numeric(6,2) | derived numeric score from `flare_class`, nullable |
| linked_event_id | text | nullable — reference to a related event (e.g. CME → GST it triggered), not FK-enforced |
| source_fetched_at | timestamptz | `NOT NULL` |
| pipeline_run_id | uuid | |
| open_date | timestamptz | `NOT NULL DEFAULT now()` |
| close_date | timestamptz | nullable — `NULL` = current version |
| — | — | **PK:** `(event_id, open_date)`; **partial unique index:** `(event_id) WHERE close_date IS NULL` |

**`open_sky.neo_close_approaches`** (fact, SCD Type 2)
| Column | Type | Constraints |
|---|---|---|
| asteroid_id | text | `NOT NULL`, part of PK — natural key; no FK to `asteroids` (dimension is derived *after* this table loads, see below) |
| close_approach_date | date | `NOT NULL`, part of PK |
| miss_distance_km | numeric(12,2) | `CHECK (miss_distance_km >= 0)` |
| relative_velocity_kph | numeric(10,2) | `CHECK (relative_velocity_kph >= 0)` |
| source_fetched_at | timestamptz | `NOT NULL` |
| pipeline_run_id | uuid | |
| open_date | timestamptz | `NOT NULL DEFAULT now()` |
| close_date | timestamptz | nullable — `NULL` = current version |
| — | — | **PK:** `(asteroid_id, close_approach_date, open_date)`; **partial unique index:** `(asteroid_id, close_approach_date) WHERE close_date IS NULL` |

Note: `name`, `estimated_diameter_m_max`, and `is_potentially_hazardous` moved off this table — they're properties of the *asteroid*, not of a specific close approach, so they now live in `open_sky.asteroids` (below) instead of being repeated on every fact row.

**`open_sky.asteroids`** (dimension, SCD Type 2 — derived from `open_sky.neo_close_approaches`, not from a separate API call)
| Column | Type | Constraints |
|---|---|---|
| asteroid_key | bigserial | PK — surrogate key, one row per version |
| asteroid_id | text | `NOT NULL` — stable business key (NASA's asteroid ID), repeats across versions |
| name | text | `NOT NULL` |
| estimated_diameter_m_max | numeric(8,2) | `CHECK (estimated_diameter_m_max > 0)` |
| is_potentially_hazardous | boolean | `NOT NULL DEFAULT false` |
| open_date | timestamptz | `NOT NULL DEFAULT now()` — when this version was derived/loaded, app-populated |
| close_date | timestamptz | nullable — `NULL` = current version |
| — | — | **Unique index:** `(asteroid_id) WHERE close_date IS NULL` — enforces one current version per asteroid |

**`open_sky_logs.api_call_log`** (fact — observability, not domain data)
| Column | Type | Constraints |
|---|---|---|
| id | bigserial | PK |
| source_name | text | not null — e.g. `open_meteo`, `nasa_donki`, `nasa_neows` |
| called_at | timestamptz | not null |
| response_time_ms | int | |
| http_status_code | int | |
| success | boolean | not null |
| retry_count | int | default 0 |
| records_returned | int | |
| rate_limit_remaining | int | nullable — not all sources expose this |
| pipeline_run_id | uuid | |

**Indexing plan:**
- `open_sky.weather_daily`: index on `date` (for time-range dashboard filters); partial unique index `(location_key, date) WHERE close_date IS NULL` covers current-version location filtering and is the upsert conflict target
- `open_sky.locations`: the partial unique index `(location_id) WHERE close_date IS NULL` doubles as the lookup index for resolving a location's current `location_key`
- `open_sky.neo_close_approaches`: index on `close_approach_date`; partial unique index `(asteroid_id, close_approach_date) WHERE close_date IS NULL` is the upsert conflict target
- `open_sky.asteroids`: the partial unique index `(asteroid_id) WHERE close_date IS NULL` doubles as the lookup index for resolving an asteroid's current attributes; index on `is_potentially_hazardous` (Dashboard 2 filters/sorts on it, now via a join to this table instead of directly on the fact)
- `open_sky.space_weather_events`: index on `(event_type, event_time)`; partial unique index `(event_id) WHERE close_date IS NULL` is the upsert conflict target
- `open_sky_logs.api_call_log`: index on `(source_name, called_at)` for the API health dashboard's time-series and per-source filters

**Modeling notes:**
- Natural keys (not surrogate autoincrement IDs) are used as PKs on fact tables where possible (`asteroid_id+close_approach_date`) since they double as the upsert conflict target — this is what makes loads idempotent. `weather_daily` is the one exception: its PK is `(location_key, date)`, a surrogate, because `location_key` is what SCD2 versioning requires (see below).
- `locations` is deliberately small and manually seeded/managed (a handful of cities you care about), not derived from the API, since Open-Meteo takes lat/lon as query params rather than returning a location list.
- **`locations` is SCD Type 2** — if a location's attributes (e.g. `timezone`) are ever edited, the load step doesn't overwrite the row in place; it closes out the current version and inserts a new one, so historical `weather_daily` rows stay pinned to the location attributes that were true when that weather was recorded. Load logic, run as one transaction against incoming seed data (e.g. `staging.locations_incoming`):
  ```sql
  BEGIN;

  -- close out the current version of any location whose attributes changed
  UPDATE open_sky.locations c
  SET close_date = now()
  FROM staging.locations_incoming s
  WHERE c.location_id = s.location_id
    AND c.close_date IS NULL
    AND (c.name, c.lat, c.lon, c.timezone)
        IS DISTINCT FROM (s.name, s.lat, s.lon, s.timezone);

  -- insert a new current version: brand-new locations, and ones just closed above
  INSERT INTO open_sky.locations (location_id, name, lat, lon, timezone, open_date, close_date)
  SELECT s.location_id, s.name, s.lat, s.lon, s.timezone, now(), NULL
  FROM staging.locations_incoming s
  LEFT JOIN open_sky.locations c
         ON c.location_id = s.location_id AND c.close_date IS NULL
  WHERE c.location_id IS NULL;  -- no current row = new location, or one just closed above

  COMMIT;
  ```
  `IS DISTINCT FROM` is NULL-safe (`timezone` can be NULL), and running both statements in one transaction matters — the `INSERT`'s `LEFT JOIN ... c.close_date IS NULL` won't match the row the `UPDATE` just closed, so it's correctly treated as needing a fresh current version. `weather_daily`'s load resolves the current `location_key` at insert time: `(SELECT location_key FROM open_sky.locations WHERE location_id = :loc_id AND close_date IS NULL)`.
- No foreign key is enforced from `neo_close_approaches` or `space_weather_events` to `locations` since those feeds are global, not location-scoped.
- **Staging load logic (SCD Type 1)** — `\copy` alone can't upsert (`COPY` only inserts/appends), so this is a two-step `psql -f`: load the pipe-delimited file into a temp landing table, then upsert from there into the real staging table on the natural key:
  ```sql
  -- 1. \copy the pipe-delimited file into a same-shaped temp landing table.
  --    FORMAT csv (below) makes embedded delimiters, quotes, and newlines in
  --    text fields safe: the convert step (Section 5) wrote this file with
  --    matching RFC-4180 CSV quoting via stdlib csv.writer, so quoted fields
  --    load intact and an unquoted empty field is read back as NULL.
  CREATE TEMP TABLE weather_raw_incoming (LIKE staging.weather_raw INCLUDING DEFAULTS);
  \copy weather_raw_incoming FROM 'data/flat/weather/{location_id}_{fetched_at}.psv' WITH (FORMAT csv, DELIMITER '|');

  -- 2. upsert into the real staging table — this is what actually gives
  -- staging its SCD1 (overwrite) behavior
  INSERT INTO staging.weather_raw
    (location_id, date, temp_min, temp_max, precipitation_mm, wind_speed_max, weather_code, fetched_at)
  SELECT location_id, date, temp_min, temp_max, precipitation_mm, wind_speed_max, weather_code, fetched_at
  FROM weather_raw_incoming
  ON CONFLICT (location_id, date) DO UPDATE
  SET temp_min = EXCLUDED.temp_min,
      temp_max = EXCLUDED.temp_max,
      precipitation_mm = EXCLUDED.precipitation_mm,
      wind_speed_max = EXCLUDED.wind_speed_max,
      weather_code = EXCLUDED.weather_code,
      fetched_at = EXCLUDED.fetched_at;
  ```
  Same shape for `donki_raw` (conflict target `event_id`) and `neows_raw` (conflict target `asteroid_id, close_approach_date`). The temp table is session-scoped and disappears automatically — nothing to clean up between runs.
- **Fact load logic (SCD Type 2)** — same close-then-insert shape as the `locations` example above, applied per fact table. For `neo_close_approaches`:
  ```sql
  BEGIN;

  UPDATE open_sky.neo_close_approaches c
  SET close_date = now()
  FROM staging.neows_raw s
  WHERE c.asteroid_id = s.asteroid_id
    AND c.close_approach_date = s.close_approach_date
    AND c.close_date IS NULL
    AND (c.miss_distance_km, c.relative_velocity_kph)
        IS DISTINCT FROM (s.miss_distance_km, s.relative_velocity_kph);

  INSERT INTO open_sky.neo_close_approaches
    (asteroid_id, close_approach_date, miss_distance_km, relative_velocity_kph,
     source_fetched_at, pipeline_run_id, open_date, close_date)
  SELECT s.asteroid_id, s.close_approach_date, s.miss_distance_km, s.relative_velocity_kph,
         s.source_fetched_at, s.pipeline_run_id, now(), NULL
  FROM staging.neows_raw s
  LEFT JOIN open_sky.neo_close_approaches c
         ON c.asteroid_id = s.asteroid_id
        AND c.close_approach_date = s.close_approach_date
        AND c.close_date IS NULL
  WHERE c.asteroid_id IS NULL;

  COMMIT;
  ```
  `weather_daily` and `space_weather_events` follow the identical pattern, keyed on their own natural keys (`location_key + date`, `event_id`).
- **`asteroids` dimension: derived from `neo_close_approaches` after the fact table loads**, not from a separate API call. This runs as its own transform step, reading only current fact versions:
  ```sql
  BEGIN;

  UPDATE open_sky.asteroids a
  SET close_date = now()
  FROM (
    SELECT DISTINCT asteroid_id, name, estimated_diameter_m_max, is_potentially_hazardous
    FROM staging.neows_raw
    WHERE close_approach_date = (SELECT MAX(close_approach_date) FROM staging.neows_raw)
  ) s
  WHERE a.asteroid_id = s.asteroid_id
    AND a.close_date IS NULL
    AND (a.name, a.estimated_diameter_m_max, a.is_potentially_hazardous)
        IS DISTINCT FROM (s.name, s.estimated_diameter_m_max, s.is_potentially_hazardous);

  INSERT INTO open_sky.asteroids (asteroid_id, name, estimated_diameter_m_max, is_potentially_hazardous, open_date, close_date)
  SELECT s.asteroid_id, s.name, s.estimated_diameter_m_max, s.is_potentially_hazardous, now(), NULL
  FROM (
    SELECT DISTINCT asteroid_id, name, estimated_diameter_m_max, is_potentially_hazardous
    FROM staging.neows_raw
  ) s
  LEFT JOIN open_sky.asteroids a
         ON a.asteroid_id = s.asteroid_id AND a.close_date IS NULL
  WHERE a.asteroid_id IS NULL;

  COMMIT;
  ```
  Because the dimension is populated *after* the fact, `neo_close_approaches` can't carry an `asteroid_key` FK at load time — the dimension row doesn't exist yet. Instead the two tables join at query/dashboard time on an **effective-dated join**, resolving which dimension version was active on the fact's date:
  ```sql
  SELECT f.*, a.name, a.estimated_diameter_m_max, a.is_potentially_hazardous
  FROM open_sky.neo_close_approaches f
  JOIN open_sky.asteroids a
    ON a.asteroid_id = f.asteroid_id
   AND f.open_date >= a.open_date
   AND (f.open_date < a.close_date OR a.close_date IS NULL)
  WHERE f.close_date IS NULL;  -- current fact versions only
  ```
- **Validation is DB-enforced, not library-enforced.** The `NOT NULL`/`CHECK`/`UNIQUE`/FK constraints above are the entire validation layer — there is no pydantic (or similar) schema validation in the Python code, and no Python DB driver either. The load step writes reshaped rows to a file, `\copy`s them via `psql` into a temp landing table, then upserts into the target. Postgres enforces the constraints at insert time; any violation (or a value that won't cast) aborts the statement, and because the upsert runs inside a transaction nothing partial lands. That failure is deliberately **not** swallowed: `psql` exits non-zero, the Airflow load task fails, and the alert fires — the pipeline never loads constraint-violating data, and never silently skips it either. A bad batch is a signal for production support to contact the source team, not something the pipeline works around.
- Trade-off accepted knowingly: validation happens *after* the reshape work in Python rather than before, so a large batch of malformed records still gets fully transformed before the load rejects it. And because a single bad row aborts the whole batch (fail-fast, above), one malformed record blocks the entire load rather than letting the good rows through — that's the intended behavior here (a source-side data problem should stop the pipeline and page support, not be partially absorbed), but it does mean the batch has to be fixed at the source before it can land. Postgres's error output names the failing constraint and offending row, so per-row diagnostic detail is adequate without a row-by-row Python driver. For this project's data volumes (a handful of cities, a handful of space-weather events/day, weekly NEO windows) both costs are negligible.

## 7. Orchestration & Scheduling

- **Airflow** (via `docker-compose`, using the official `apache/airflow` image) runs three DAGs: `weather_dag` and `space_weather_dag` (DONKI + NeoWs share a DAG since both are NASA, both daily) on recurring schedules, plus `history_load_dag` (`schedule=None`, triggered once — see below) for the initial backfill.
- Suggested schedule: both DAGs run `@daily` — `weather_dag` once a day, `space_weather_dag` pulling a rolling multi-day window for DONKI and a 7-day rolling window for NeoWs.
- Data flow, uniform across both DAGs (Section 5 covers this in detail): `extract` fetches from the source API and writes the response **untouched as a JSON file**; a separate `convert` step reshapes that JSON file into a **pipe-delimited flat file**; that flat file is loaded into Postgres via `psql`'s `\copy` (Section 6/13) — never a direct API-to-database write, and the raw JSON never itself lands in Postgres.
- Retries: 3 attempts with exponential backoff on extract tasks (network flakiness); load tasks fail fast (a `psql` non-zero exit surfaces immediately) so data-quality issues aren't masked by a retry silently succeeding on a partial load.

### DAG Task Graph

Each DAG follows the same shape: extract → write raw JSON file → convert to pipe-delimited flat file → `psql \copy` into a temp landing table + upsert into staging (SCD1 overwrite) → `psql -f` historize into `open_sky` facts (SCD2) → log call metrics → run audit checks (Section 12). The `neows` task group has one extra step after its fact load: deriving the `open_sky.asteroids` dimension from the fact table's current versions (Section 6). Task groups keep each source's steps together while allowing them to run in parallel within the DAG.

```mermaid
flowchart LR
    subgraph weather_dag["weather_dag  (@daily)"]
        direction LR
        W1["extract_open_meteo\n(PythonOperator, writes\nraw JSON file)"] --> W2["convert_to_flat_file\n(PythonOperator, JSON ->\npipe-delimited)"]
        W2 --> W3["copy_to_staging\n(PythonOperator: subprocess\ncalls psql \\copy + upsert, SCD1)"]
        W3 --> W4["upsert_to_open_sky\n(BashOperator: psql -f, static path)"]
        W1 --> W5["log_call_metrics\n(PythonOperator)"]
        W4 --> W6["run_audit_checks\n(PythonOperator, trigger_rule=all_done)"]
        W5 --> W6
    end

    subgraph space_weather_dag["space_weather_dag  (@daily)"]
        direction LR
        subgraph tg_donki["TaskGroup: donki"]
            D1["extract_donki\n(CME/GST/FLR,\nwrites raw JSON file)"] --> D2["convert_to_flat_file"]
            D2 --> D3["copy_to_staging\n(SCD1 upsert)"]
            D3 --> D4["upsert_to_open_sky"]
            D1 --> D5["log_call_metrics"]
            D4 --> D6["run_audit_checks"]
            D5 --> D6
        end
        subgraph tg_neows["TaskGroup: neows"]
            N1["extract_neows\n(7-day window,\nwrites raw JSON file)"] --> N2["convert_to_flat_file"]
            N2 --> N3["copy_to_staging\n(SCD1 upsert)"]
            N3 --> N4["upsert_to_open_sky\n(SCD2, neo_close_approaches)"]
            N4 --> N7["derive_asteroids_dimension\n(SCD2, from current\nneo_close_approaches versions)"]
            N1 --> N5["log_call_metrics"]
            N7 --> N6["run_audit_checks"]
            N5 --> N6
        end
    end
```

### DAG Skeleton (illustrative)

```python
# dags/weather_dag.py
import os
import subprocess
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False, tags=["weather"])
def weather_dag():

    @task(retries=3, retry_exponential_backoff=True)
    def extract_open_meteo():
        # calls etl.extract.open_meteo, wrapped by the call-metrics decorator;
        # writes the API response UNTOUCHED to a JSON file on the shared
        # volume (Section 5's raw artifact, used for replay/schema-drift),
        # and returns (json_file_path, metrics) — no in-memory raw JSON
        # passed downstream via XCom, just the path
        ...

    @task
    def convert_to_flat_file(json_file_path):
        # etl.transform.weather reads the JSON file, reshapes it (flatten,
        # cast, derive, dedupe — Section 5), and writes a pipe-delimited
        # (`|`) flat file to a shared volume path, returns the flat file path
        ...

    @task
    def copy_to_staging(flat_file_path):
        # PythonOperator, not BashOperator: flat_file_path comes from XCom
        # (pipeline data). Rather than building a psql -c string with it,
        # this passes it as a psql variable (-v, officially quoted/escaped
        # via :'var' in the .sql file) — no shell string-building either way.
        # The .sql file does the two-step SCD1 load from Section 6: \copy
        # into a temp landing table, then upsert into staging.weather_raw.
        subprocess.run(
            ["psql", os.environ["ETL_DSN"],
             "-v", f"flat_file={flat_file_path}",
             "-f", "/opt/airflow/sql/upserts/stage_weather_raw.sql"],
            check=True,
        )

    # static, checked-in file path — no pipeline data in the command,
    # so a plain BashOperator is safe here
    upsert_to_open_sky = BashOperator(
        task_id="upsert_to_open_sky",
        bash_command="psql $ETL_DSN -f /opt/airflow/sql/upserts/weather_daily.sql",
    )

    @task
    def log_call_metrics(metrics):
        # writes the metrics dict to a small pipe-delimited file, psql
        # \copy's it into open_sky_logs.api_call_log
        ...

    @task(trigger_rule="all_done")  # runs even if upstream tasks failed
    def run_audit_checks():
        # runs the 5 checks from Section 12 against open_sky.*, writes
        # results to a pipe-delimited file, psql \copy's it into
        # open_sky_logs.audit_findings
        ...

    json_file_path, metrics = extract_open_meteo()
    flat_file_path = convert_to_flat_file(json_file_path)
    staged = copy_to_staging(flat_file_path)
    staged >> upsert_to_open_sky
    metrics_done = log_call_metrics(metrics)
    [upsert_to_open_sky, metrics_done] >> run_audit_checks()

weather_dag()
```

`space_weather_dag.py` follows the same pattern, with a `TaskGroup("donki")` and `TaskGroup("neows")` running in parallel, each with its own extract/convert/copy/upsert/metrics/audit chain as shown in the diagram above.

### One-Time Historical Backfill

A third DAG, `history_load_dag`, runs the same extract → JSON file → convert → flat file → staging → `open_sky` pipeline as the two daily DAGs (same tasks, same SQL, same idempotent SCD1/SCD2 loads) — the differences are the date range each extract call requests, how many times it loops to cover it, and where the raw JSON lands: every backfill request writes to `data/raw/{source}/historical/{key}_{window_start}_{fetched_at}.json` (Section 6), never the daily DAGs' path, so a backfill run can never collide with the current day's file or with another backfill attempt's file for the same window. It's `schedule=None` (Airflow-native "on demand only") and gets triggered exactly once by hand at app initialization, e.g. `airflow dags trigger history_load_dag` — not on a recurring schedule, and not gated by any "have I already run" flag, since replaying it is harmless: the loads it makes are the same idempotent upserts the daily DAGs already use, so an accidental second run just re-verifies existing rows rather than duplicating anything.

Per-source backfill window — **the same rolling 2-year window (today − 2 years → today − 5 days) for all three sources** (Section 2):

| Source | Backfill range | Loop needed? | Requests |
|---|---|---|---|
| Open-Meteo | today − 2 years → today − 5 days | No — one call per location covers the whole range (archive endpoint, not the forecast one) | ~1 per location |
| NASA DONKI | today − 2 years → today − 5 days | Yes — 30-day windows, per event type (CME/GST/FLR) | ~2 years × 365 ÷ 30 ≈ 24 windows per event type → **~73 total** |
| NASA NeoWs | today − 2 years → today − 5 days | Yes — 7-day windows | ~2 years × 365 ÷ 7 ≈ **~104** |

`history_load_dag` task shape, per source: a single `@task` loops the date windows in Python and calls the same `extract_*`/`convert_to_flat_file`/`copy_to_staging` functions the daily DAG uses per iteration, rather than generating ~180 individual Airflow tasks — Airflow's scheduler overhead per task isn't worth paying that many times for what's fundamentally one backfill job. `upsert_to_open_sky` still runs once at the end, over everything staged.

**Window sizing is *inclusive* (implementation detail that matters for correctness).** A "30-day" DONKI window and a "7-day" NeoWs window mean 30 and 7 *inclusive* calendar days, so `config.iter_windows` yields `[ws, ws + window_days - 1]` (not `ws + window_days`). Getting this wrong by one day is not cosmetic: a 31-inclusive-day DONKI request exceeds DONKI's 30-day cap, which *truncates* to the last 30 days ending at `endDate` and thereby silently drops the window's first day — leaving a periodic 1-day gap right through a 2-year backfill. The daily windows (`etl/extract/nasa.py`) use the same `window_days - 1` convention so a scheduled pull can't be truncated either. Windows step by `we + 1 day`, so they stay contiguous with no gaps or overlaps; the natural-key upsert absorbs the deliberate day-of-overlap that daily runs produce against the previous day's data.

Edge case handled in `config.backfill_range`: when `today − 5 days` lands on Feb 29 (leap year), `end.replace(year=end.year − 2)` would raise `ValueError` on the one calendar day the target year isn't a leap year, so the start clamps to Feb 28 instead of crashing the backfill trigger.

**Rate-limit headroom:** DONKI's ~73 requests plus NeoWs's ~104 is **~177 combined NASA calls — well under NASA's 1000 req/hour cap** (Section 2), so `history_load_dag` fires them sequentially with no pacer or token-bucket at all; the backfill finishes in minutes, bounded by API latency rather than the rate limit. Keeping the window at 2 years is partly *why* there's no pacer to build — you wouldn't approach the cap until ~11 years of history, which is the point at which pacing would need reintroducing. The daily DAGs are nowhere near the cap either (~3–4 NASA calls/day).

```yaml
services:
  postgres:        # main data warehouse; individual files from sql/roles/
                    # and sql/ddl/ are each mounted to a numbered path
                    # directly under /docker-entrypoint-initdb.d/ (that
                    # hook only processes flat files, not subdirectories),
                    # so role setup + DDL + grants (Section 13) run once,
                    # in filename order, automatically on first boot
  airflow-webserver:
  airflow-scheduler:
  airflow-init:
  superset:
  superset-init:   # runs superset db upgrade / admin creation on first boot
```

**Init file order** (Section 13 covers why): role *creation* has to happen before the schema DDL, not after, because the DBA role now owns the schemas/tables — but the DML *grants* to `etl_role`/`developer_role`/`superset_role` still have to come last, since those `GRANT ... ON open_sky.<table>` statements need the tables to already exist:
```
sql/roles/01_create_roles.sql        -- CREATE ROLE for all four group roles + logins
sql/ddl/02_schemas_and_staging.sql   -- run as dba_role: CREATE SCHEMA staging/open_sky/open_sky_logs
                                      -- (all AUTHORIZATION dba_role), then staging tables
sql/ddl/03_open_sky_dimensions.sql   -- locations, asteroids (as dba_role, before facts — FK dependency)
sql/ddl/04_open_sky_facts.sql        -- weather_daily, space_weather_events, neo_close_approaches
sql/ddl/05_open_sky_logs.sql         -- api_call_log, audit_findings — separate schema, own file,
                                      -- no dependency on the open_sky facts so order vs. 03/04 doesn't matter
sql/roles/06_grants.sql              -- GRANT to etl_role/developer_role/superset_role; REVOKE DML from dba_role
```
This is a bootstrap that runs once against an empty volume, not a migration system — schema changes after go-live are applied by hand as `dba_app` (Section 13), not by editing these files and expecting a restart to pick them up.

All services on one Docker network; Postgres data persisted via a named volume; `.env` supplies `NASA_API_KEY`, `POSTGRES_*` (superuser, used only for the initial role/DDL bootstrap), `ETL_DSN`, `SUPERSET_DSN`, and `DBA_DSN` (the app/person-facing connection strings, each authenticating as its own least-privilege role — Section 13), `AIRFLOW__*`, `SUPERSET_SECRET_KEY`.

## 9. Superset Dashboard Plan

**Dashboard 1 — Weather Trends**
- Line chart: daily min/max temperature over time, filterable by location
- Bar chart: monthly precipitation totals by location
- Big number KPI: current temp / last updated timestamp
- Map (if multiple locations): geo scatter of locations colored by latest temp

**Dashboard 2 — Near-Earth Objects (Asteroids)**
- All charts join current `open_sky.neo_close_approaches` (`close_date IS NULL`) to current `open_sky.asteroids` (`close_date IS NULL`) on `asteroid_id`, since diameter and hazard flag now live on the dimension, not the fact
- Table: upcoming close approaches sorted by miss distance, flagging `is_potentially_hazardous`
- Scatter plot: miss distance vs. relative velocity, sized by estimated diameter
- Time-series: count of close approaches per week
- Big number KPI: closest approach in next 7 days

**Dashboard 3 — Space Weather Events**
- Time-series: solar flare count per week, colored/grouped by flare class
- Bar chart: geomagnetic storm frequency by Kp-index severity band
- Table: recent CMEs with speed, source location, and linked GST (if any)
- Big number KPI: most severe event (highest flare class / Kp-index) in the last 7 days

**Dashboard 4 — API Health & Reliability (source comparison)**
- Line chart: average response time (ms) per source, over time
- Bar chart: success rate (%) per source, trailing 7/30 days
- Table: schema-drift signal per source — with fail-fast loads there's no silent rejected-row count to trend, so a source quietly changing its schema surfaces instead as a schema-drift finding (Section 12's check #2) plus a hard load failure in the DAG
- Big number KPIs: current rate-limit headroom per source (most relevant for NASA's 1000 req/hour cap)
- Time-series: retry count per source — climbing retries flag a source getting flakier before it fully breaks
- Purpose: this is the dashboard that actually answers "which API is best to use" — it replaces reliance on published docs with observed behavior (real latency, real failure rate, real data-quality drift), and generalizes cleanly if a competing source is ever added for the same domain (e.g., US NWS alongside Open-Meteo)

Cross-cutting: a shared date-range filter and location filter (for weather) using Superset's native dashboard filter components. Superset connects to Postgres as the `superset_app` login, which inherits `superset_role` — `SELECT`-only, and only on the specific tables the four dashboards actually query, spanning `open_sky` (Dashboards 1–3) and `open_sky_logs` (Dashboard 4) (never `staging.*`). See Section 13 for the full role/grant design.

## 10. Build Order (suggested milestones)

1. Stand up Postgres + create staging (SCD1) / open_sky (SCD2) DDL
2. Write Open-Meteo extract → transform → load, verify data lands in `open_sky.weather_daily`, including a version bump (open_date/close_date) on a re-run with changed values
3. Add NASA DONKI + NeoWs extract/transform/load; add the `open_sky.asteroids` dimension-extraction step that runs after `neo_close_approaches` loads (Section 6)
4. Wrap both pipelines in Airflow DAGs with schedules + retries
5. Add `history_load_dag` (Section 7) and run it once against a fresh environment to backfill all three sources over the same rolling 2-year window (~177 NASA calls, well inside the free tier — no pacing needed) before relying on the daily DAGs for anything
6. Add the API call-metrics decorator around all extract functions, feeding `open_sky_logs.api_call_log`
7. Add `etl/audit/checks.py` (the 5 checks) and `open_sky_logs.audit_findings`; wire `run_audit_checks` into both daily DAGs
8. Stand up Superset, connect to Postgres, build the four dashboards (including the audit findings panel on Dashboard 4)
9. Add structured logging across all stages
10. (Stretch) Add OpenAQ air-quality source and join to weather by location; add data-quality checks (e.g., Great Expectations) before load

## 11. Tech Choices Summary

| Concern | Choice | Why |
|---|---|---|
| Language | Python 3.11+ | Best library support for ETL + both APIs are REST/JSON |
| HTTP client | `requests` (or `httpx` for async) | Simple, well-understood |
| Validation | Postgres constraints (`NOT NULL`, `CHECK`, `UNIQUE`, FK) | Single source of truth for what's valid; no separate Python validation library to keep in sync with the schema |
| DB access | `psql` + `COPY` (no Python DB driver) | Postgres's own tooling handles the actual database interaction — no `psycopg2`/ORM dependency at all; bulk loads and upserts are plain SQL invoked as subprocess/`BashOperator` calls |
| Orchestration | Airflow | Industry-standard scheduler, good Postgres/Superset ecosystem fit |
| BI | Superset | Free, native Postgres connector, good for this scale |
| Packaging | Docker Compose | Reproducible local environment, one command to bring everything up |

See Section 7 (DAG Skeleton) for the actual `psql \copy` / `psql -f` commands as they run inside Airflow tasks, and Section 6 (Modeling Notes) for how the Postgres constraints enforce validation at load time — aborting the batch and failing the task on any violation, rather than skipping rows.

## 12. Audit Scripts (covers what Postgres constraints can't)

Postgres constraints (Section 6) validate single rows at insert time — they can't catch things that require comparing across rows, tables, or time, or notice when expected data is simply *missing*. Audit checks close that gap, running as part of every DAG run rather than as deferred work.

**The five checks, each a plain SQL query executed by `etl/audit/checks.py`:**

1. **Absence detection** — did every expected row land?
   ```sql
   -- did today's weather row land for every active (current) location?
   SELECT l.location_id, l.name
   FROM open_sky.locations l
   LEFT JOIN open_sky.weather_daily w
     ON w.location_key = l.location_key AND w.date = current_date AND w.close_date IS NULL
   WHERE l.close_date IS NULL
     AND w.location_key IS NULL;
   ```
2. **Source schema drift** — does today's raw JSON have the same top-level keys as a saved reference sample? Since raw JSON is a file now, not a Postgres column (Section 5/6), this check reads the file directly rather than querying staging:
   ```python
   # reads the raw JSON file extract_* just wrote (same path passed to
   # convert_to_flat_file), compares its top-level keys against the
   # reference_schema/{source}.json baseline
   latest_raw = json.load(open(json_file_path))
   missing = reference_keys - set(latest_raw.keys())
   added = set(latest_raw.keys()) - reference_keys
   ```
   The reference baseline is **bootstrapped on first sighting**, not hand-checked-in: the first run for a source writes `reference_schema/{source}.json` from the observed keys and emits an `info` finding; every run after compares against it. These generated files are environment-specific and gitignored (only `reference_schema/README.md` is tracked). Trade-off: the very first run can't detect drift (it's establishing the baseline) — acceptable, since a genuinely breaking shape change also fails the fail-fast load loudly regardless.
3. **Statistical/trend anomalies** — values valid per `CHECK` but out of line with recent history (current versions only — historized-out rows shouldn't skew the trailing average)
   ```sql
   -- flag a temp reading > 3 std-dev from the trailing 7-day mean for that location
   SELECT date, location_key, temp_max
   FROM open_sky.weather_daily w
   WHERE w.close_date IS NULL
     AND ABS(temp_max - (
       SELECT AVG(temp_max) FROM open_sky.weather_daily
       WHERE location_key = w.location_key AND close_date IS NULL AND date BETWEEN w.date - 7 AND w.date - 1
     )) > 3 * (
       SELECT STDDEV(temp_max) FROM open_sky.weather_daily
       WHERE location_key = w.location_key AND close_date IS NULL AND date BETWEEN w.date - 7 AND w.date - 1
     );
   ```
4. **Cross-table consistency** — e.g., every `pipeline_run_id` in an `open_sky` table has a matching `api_call_log` entry
   ```sql
   SELECT w.pipeline_run_id
   FROM open_sky.weather_daily w
   LEFT JOIN open_sky_logs.api_call_log a ON a.pipeline_run_id = w.pipeline_run_id
   WHERE a.pipeline_run_id IS NULL;
   ```
   Also worth a dedicated check once `asteroids` is live: every *current* `neo_close_approaches` row's `asteroid_id` should resolve to a current `asteroids` row via the effective-dated join (Section 6) — a miss means the dimension-extraction step fell behind the fact load.
5. **Trend-based API health signals** — success rate or latency drifting over a rolling window
   ```sql
   SELECT source_name,
          AVG(CASE WHEN success THEN 1 ELSE 0 END) AS success_rate_7d
   FROM open_sky_logs.api_call_log
   WHERE called_at > now() - interval '7 days'
   GROUP BY source_name
   HAVING AVG(CASE WHEN success THEN 1 ELSE 0 END) < 0.9;
   ```

**`open_sky_logs.audit_findings`** (fact — observability, written by audit checks only)
| Column | Type | Constraints |
|---|---|---|
| id | bigserial | PK |
| check_name | text | `NOT NULL` — e.g. `absence_weather_daily`, `schema_drift_donki`, `anomaly_temp_max` |
| severity | text | `NOT NULL`, `CHECK (severity IN ('info', 'warning', 'critical'))` |
| related_table | text | nullable — e.g. `open_sky.weather_daily` |
| description | text | `NOT NULL` — human-readable finding, includes the offending key/value |
| detected_at | timestamptz | `NOT NULL` |
| pipeline_run_id | uuid | |

Index: `(check_name, detected_at)` for the dashboard's trend view.

**Wiring into Airflow (extends the DAGs already defined in Section 7):** each DAG gets one additional task, `run_audit_checks`, appended after `upsert_to_open_sky` and `log_call_metrics`:

```python
    @task(trigger_rule="all_done")  # runs even if an upstream task failed
    def run_audit_checks():
        # runs the 5 SQL checks above against open_sky.*, writes results
        # to a pipe-delimited file, psql \copy's it into open_sky_logs.audit_findings
        ...

    [upsert_to_open_sky, log_call_metrics(raw)] >> run_audit_checks()
```

`trigger_rule="all_done"` is deliberate — audits should run and report even when the load itself partially failed, since "the load failed" is exactly the kind of thing absence detection is meant to catch. A finding never fails the DAG; it's written and surfaced, not raised as an exception.

Because `all_done` fires the task even when an upstream `convert`/`extract` task failed, the XCom it receives (the converted-file paths, used only to locate raw JSON for the schema-drift check) can be `None`. `run_audit_checks` guards for that (`(converted or {}).get(...)`) so a failed upstream degrades gracefully to "skip the file-based drift check, still run all the SQL checks" — rather than the audit task itself throwing `AttributeError` on `None` and never running the absence/consistency checks that matter most precisely when something upstream broke.

**Dashboard integration:** Dashboard 4 (API Health & Reliability) gains a table panel — recent `audit_findings` sorted by severity and `detected_at` — turning the dashboard from "raw call metrics" into "raw metrics + the anomalies actually found in them."

## 13. Database Roles & Access Control

Four roles, each scoped to what actually needs to happen against Postgres — no app or person connects as the superuser. All of this is native Postgres RBAC (`CREATE ROLE`, `GRANT`, `REVOKE`, `ALTER DEFAULT PRIVILEGES`) — no external grant-management tool.

| Role | Used by | Schemas | Privilege |
|---|---|---|---|
| `dba_role` | A human DBA (`dba_app`), for schema changes | `staging`, `open_sky`, `open_sky_logs` | **DDL only** — `CREATE`/`ALTER`/`DROP` on tables. No `SELECT`/`INSERT`/`UPDATE`/`DELETE` — it can reshape structure but can't read or write data. |
| `etl_role` | Airflow tasks (`ETL_DSN`) — the only thing that writes data | `staging`, `open_sky`, `open_sky_logs` | `SELECT, INSERT, UPDATE` on every table (needed for SCD1 upserts in staging, SCD2 close-then-insert in open_sky, and writing call-metrics/audit rows to `open_sky_logs` — see Sections 5, 6, 12). No `DELETE` (the SCD design never deletes a row) and no DDL — schema changes are `dba_role`'s job, not the running pipeline's. |
| `developer_role` | Individual developers, for debugging/exploration via `psql` | `staging`, `open_sky`, `open_sky_logs` | `SELECT` only, on everything — including `staging`, since raw JSON is exactly what you need when an `open_sky` row looks wrong and you're tracing it back to the source payload. |
| `superset_role` | Superset (`SUPERSET_DSN`) | `open_sky`, `open_sky_logs` | `SELECT` only, and only on the specific tables the four dashboards actually query — granted per-table, not schema-wide, spanning both `open_sky` (domain data, Dashboards 1–3) and `open_sky_logs` (Dashboard 4). `staging.*` is never granted, since raw payloads aren't analytics-ready and shouldn't be queryable from a BI tool even read-only. |

Each of these is a `NOLOGIN` group role; actual connections use a separate `LOGIN` role granted membership in the appropriate group role (standard Postgres pattern — separates "who's connecting" from "what they're allowed to do," so adding a second developer or rotating the ETL password never means touching the privilege grants themselves).

**Why `dba_role` needs to own the schemas, not just be granted rights on them:** Postgres has no `GRANT ALTER` / `GRANT DROP` privilege at the table level the way it has `GRANT SELECT` — the ability to `ALTER TABLE` or `DROP TABLE` an *existing* object comes from **owning** it (or being a member of the owning role), not from a grantable privilege. So `dba_role` is made the owner of all three schemas (`CREATE SCHEMA ... AUTHORIZATION dba_role`), and every table is created while running *as* `dba_role`, so `dba_role` owns them from the start and can `ALTER`/`DROP` them later. The catch: Postgres ownership implicitly includes full DML rights too (`SELECT`/`INSERT`/`UPDATE`/`DELETE`), not just DDL — so to actually make `dba_role` DDL-only as intended, those DML rights have to be explicitly `REVOKE`d back off after the tables exist. This is also why role creation has to run *before* schema/table creation now (Section 8's init order), reversing the earlier plan where roles ran last.

```sql
-- sql/roles/01_create_roles.sql — run first, as the Postgres superuser
-- (POSTGRES_USER), before any schema/table DDL exists

CREATE ROLE dba_role NOLOGIN;
CREATE ROLE etl_role NOLOGIN;
CREATE ROLE developer_role NOLOGIN;
CREATE ROLE superset_role NOLOGIN;

CREATE ROLE dba_app LOGIN PASSWORD :'dba_app_password' IN ROLE dba_role;
CREATE ROLE etl_app LOGIN PASSWORD :'etl_app_password' IN ROLE etl_role;
CREATE ROLE superset_app LOGIN PASSWORD :'superset_app_password' IN ROLE superset_role;
CREATE ROLE dev_achu LOGIN PASSWORD :'dev_achu_password' IN ROLE developer_role;
```

```sql
-- sql/ddl/02_schemas_and_staging.sql (and 03_/04_/05_ after it) — run as
-- dba_role, e.g. `psql ... -c "SET ROLE dba_role;" -f 02_schemas_and_staging.sql`,
-- so every schema and table it creates is owned by dba_role from the start.
-- All three schemas are created here, up front, even though open_sky_logs's
-- tables aren't populated until 05_open_sky_logs.sql.

CREATE SCHEMA staging AUTHORIZATION dba_role;
CREATE SCHEMA open_sky AUTHORIZATION dba_role;
CREATE SCHEMA open_sky_logs AUTHORIZATION dba_role;
GRANT CREATE ON SCHEMA staging, open_sky, open_sky_logs TO dba_role;  -- lets dba_role create future tables too

-- ... CREATE TABLE staging.weather_raw (...), etc. — Section 6 DDL, run here ...
```

```sql
-- sql/roles/06_grants.sql — run last, as the Postgres superuser (superuser
-- bypasses ownership checks, so it can GRANT on dba_role-owned tables directly)

-- etl_role: read/write staging + open_sky + open_sky_logs (it's the one thing
-- writing log_call_metrics and audit findings, not just domain data)
GRANT USAGE ON SCHEMA staging, open_sky, open_sky_logs TO etl_role;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA staging TO etl_role;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA open_sky TO etl_role;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA open_sky_logs TO etl_role;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA staging, open_sky, open_sky_logs TO etl_role;  -- bigserial PKs
ALTER DEFAULT PRIVILEGES FOR ROLE dba_role IN SCHEMA staging GRANT SELECT, INSERT, UPDATE ON TABLES TO etl_role;
ALTER DEFAULT PRIVILEGES FOR ROLE dba_role IN SCHEMA open_sky GRANT SELECT, INSERT, UPDATE ON TABLES TO etl_role;
ALTER DEFAULT PRIVILEGES FOR ROLE dba_role IN SCHEMA open_sky_logs GRANT SELECT, INSERT, UPDATE ON TABLES TO etl_role;

-- developer_role: read-only, everything, for debugging — including
-- open_sky_logs, so a developer can trace a dashboard number back to the
-- raw call/audit rows that produced it
GRANT USAGE ON SCHEMA staging, open_sky, open_sky_logs TO developer_role;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO developer_role;
GRANT SELECT ON ALL TABLES IN SCHEMA open_sky TO developer_role;
GRANT SELECT ON ALL TABLES IN SCHEMA open_sky_logs TO developer_role;
ALTER DEFAULT PRIVILEGES FOR ROLE dba_role IN SCHEMA staging GRANT SELECT ON TABLES TO developer_role;
ALTER DEFAULT PRIVILEGES FOR ROLE dba_role IN SCHEMA open_sky GRANT SELECT ON TABLES TO developer_role;
ALTER DEFAULT PRIVILEGES FOR ROLE dba_role IN SCHEMA open_sky_logs GRANT SELECT ON TABLES TO developer_role;

-- superset_role: read-only, explicit table list only, spanning both schemas
-- it needs — deliberately NOT "ALL TABLES" and NOT covered by ALTER DEFAULT
-- PRIVILEGES in either schema, so a new table isn't auto-exposed to the BI
-- tool just for existing; it has to be added here on purpose when a
-- dashboard actually needs it
GRANT USAGE ON SCHEMA open_sky, open_sky_logs TO superset_role;
GRANT SELECT ON
  open_sky.locations,
  open_sky.weather_daily,
  open_sky.asteroids,
  open_sky.neo_close_approaches,
  open_sky.space_weather_events,
  open_sky_logs.api_call_log,
  open_sky_logs.audit_findings
TO superset_role;

-- dba_role: strip the DML rights that came implicitly with table ownership,
-- so it's left with structural (DDL) rights only
REVOKE SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging, open_sky, open_sky_logs FROM dba_role;
```
`ALTER DEFAULT PRIVILEGES FOR ROLE dba_role IN SCHEMA ...` (rather than the plain form used for `etl_role`/`developer_role` before) matters here specifically because `dba_role`, not the connecting superuser, is who actually creates future tables — default-privilege rules only apply automatically to objects created *by the role named in `FOR ROLE`*.

Passwords are passed in as `psql` variables (`-v etl_app_password=...`) sourced from `.env` at container init, not hardcoded in the checked-in `.sql` files. `ETL_DSN`, `SUPERSET_DSN`, and a new `DBA_DSN` (Section 8) are then just `postgresql://etl_app:...@postgres:5432/...`, `postgresql://superset_app:...@postgres:5432/...`, and `postgresql://dba_app:...@postgres:5432/...` respectively — each app or person authenticates as its own least-privilege login, never as `POSTGRES_USER`. Ongoing schema changes after the initial bootstrap (Section 8's caveat) are exactly what `dba_app` is for: `psql $DBA_DSN -f sql/ddl/06_new_change.sql`, without ever needing the raw superuser credential.
