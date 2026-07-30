-- =============================================================================
-- 04_open_sky_facts.sql  (Section 6)
-- The three domain fact tables, all SCD Type 2 (a changed value closes the old
-- version and inserts a new one; nothing overwrites in place). Run as dba_role.
-- =============================================================================

SET ROLE dba_role;

-- open_sky.weather_daily — fact, SCD Type 2.
CREATE TABLE IF NOT EXISTS open_sky.weather_daily (
    location_key      int            NOT NULL REFERENCES open_sky.locations (location_key),
    date              date           NOT NULL CHECK (date <= current_date + interval '16 days'),
    temp_min          numeric(5,2)   CHECK (temp_min > -100),
    temp_max          numeric(5,2)   CHECK (temp_max < 100),
    temp_range        numeric(5,2),
    precipitation_mm  numeric(6,2)   CHECK (precipitation_mm >= 0),
    is_rain_day       boolean        NOT NULL DEFAULT false,
    wind_speed_max    numeric(6,2)   CHECK (wind_speed_max >= 0),
    weather_code      int,
    weather_condition text,
    source_fetched_at timestamptz    NOT NULL,
    pipeline_run_id   uuid,
    open_date         timestamptz    NOT NULL DEFAULT now(),
    close_date        timestamptz,
    CHECK (temp_max >= temp_min),
    PRIMARY KEY (location_key, date, open_date)
);
-- Upsert conflict target + current-version lookup.
CREATE UNIQUE INDEX IF NOT EXISTS weather_daily_current_uq
    ON open_sky.weather_daily (location_key, date) WHERE close_date IS NULL;
-- Time-range dashboard filters.
CREATE INDEX IF NOT EXISTS weather_daily_date_idx
    ON open_sky.weather_daily (date);

-- open_sky.space_weather_events — fact, SCD Type 2. Global feed, no dimension.
CREATE TABLE IF NOT EXISTS open_sky.space_weather_events (
    event_id             text           NOT NULL,
    event_type           text           NOT NULL CHECK (event_type IN ('CME', 'GST', 'FLR')),
    event_time           timestamptz    NOT NULL,
    source_location      text,
    speed_kms            numeric(8,2)   CHECK (speed_kms >= 0),
    kp_index             numeric(4,2)   CHECK (kp_index BETWEEN 0 AND 9),
    flare_class          text,
    flare_severity_score numeric(6,2),
    linked_event_id      text,
    source_fetched_at    timestamptz    NOT NULL,
    pipeline_run_id      uuid,
    open_date            timestamptz    NOT NULL DEFAULT now(),
    close_date           timestamptz,
    PRIMARY KEY (event_id, open_date)
);
CREATE UNIQUE INDEX IF NOT EXISTS space_weather_events_current_uq
    ON open_sky.space_weather_events (event_id) WHERE close_date IS NULL;
CREATE INDEX IF NOT EXISTS space_weather_events_type_time_idx
    ON open_sky.space_weather_events (event_type, event_time);

-- open_sky.neo_close_approaches — fact, SCD Type 2. asteroids dimension is
-- derived FROM this table after it loads (no FK the other direction).
CREATE TABLE IF NOT EXISTS open_sky.neo_close_approaches (
    asteroid_id            text           NOT NULL,
    close_approach_date    date           NOT NULL,
    miss_distance_km       numeric(12,2)  CHECK (miss_distance_km >= 0),
    relative_velocity_kph  numeric(10,2)  CHECK (relative_velocity_kph >= 0),
    source_fetched_at      timestamptz    NOT NULL,
    pipeline_run_id        uuid,
    open_date              timestamptz    NOT NULL DEFAULT now(),
    close_date             timestamptz,
    PRIMARY KEY (asteroid_id, close_approach_date, open_date)
);
CREATE UNIQUE INDEX IF NOT EXISTS neo_close_approaches_current_uq
    ON open_sky.neo_close_approaches (asteroid_id, close_approach_date) WHERE close_date IS NULL;
CREATE INDEX IF NOT EXISTS neo_close_approaches_date_idx
    ON open_sky.neo_close_approaches (close_approach_date);

RESET ROLE;
