-- =============================================================================
-- 02_schemas_and_staging.sql  (Sections 6, 8, 13)
-- Run as dba_role (init-db.sh does `SET ROLE dba_role;` before -f) so every
-- schema and table created here is OWNED by dba_role from the start.
-- All three schemas are created up front, even though open_sky_logs's tables
-- aren't defined until 05_.
-- =============================================================================

SET ROLE dba_role;

CREATE SCHEMA IF NOT EXISTS staging       AUTHORIZATION dba_role;
CREATE SCHEMA IF NOT EXISTS open_sky       AUTHORIZATION dba_role;
CREATE SCHEMA IF NOT EXISTS open_sky_logs  AUTHORIZATION dba_role;

-- Let dba_role create future tables in these schemas too (post-bootstrap DDL).
GRANT CREATE ON SCHEMA staging, open_sky, open_sky_logs TO dba_role;

-- -----------------------------------------------------------------------------
-- staging.* — flattened, typed rows, one per natural key. SCD Type 1: each pull
-- overwrites in place (the upsert lives in sql/upserts/stage_*.sql). Raw JSON is
-- kept as a file on the shared volume, never as a column here.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS staging.weather_raw (
    location_id       int            NOT NULL,
    date              date           NOT NULL,
    temp_min          numeric(5,2),
    temp_max          numeric(5,2),
    precipitation_mm  numeric(6,2),
    wind_speed_max    numeric(6,2),
    weather_code      int,
    fetched_at        timestamptz    NOT NULL,
    PRIMARY KEY (location_id, date)
);

CREATE TABLE IF NOT EXISTS staging.donki_raw (
    event_id              text           NOT NULL,
    event_type            text           NOT NULL,
    event_time            timestamptz,
    source_location       text,
    speed_kms             numeric(8,2),
    kp_index              numeric(4,2),
    flare_class           text,
    flare_severity_score  numeric(6,2),
    linked_event_id       text,
    fetched_at            timestamptz    NOT NULL,
    PRIMARY KEY (event_id)
);

CREATE TABLE IF NOT EXISTS staging.neows_raw (
    asteroid_id                text          NOT NULL,
    close_approach_date        date          NOT NULL,
    name                       text,
    miss_distance_km           numeric(12,2),
    relative_velocity_kph      numeric(10,2),
    estimated_diameter_m_max   numeric(8,2),
    is_potentially_hazardous   boolean,
    fetched_at                 timestamptz   NOT NULL,
    PRIMARY KEY (asteroid_id, close_approach_date)
);

-- Landing table for the manually-seeded locations dimension (Section 6). The
-- SCD2 load in sql/upserts/locations.sql reads from here.
CREATE TABLE IF NOT EXISTS staging.locations_incoming (
    location_id  int             NOT NULL,
    name         text            NOT NULL,
    lat          numeric(8,5)    NOT NULL,
    lon          numeric(8,5)    NOT NULL,
    timezone     text,
    PRIMARY KEY (location_id)
);

RESET ROLE;
