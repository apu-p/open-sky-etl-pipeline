-- =============================================================================
-- 03_open_sky_dimensions.sql  (Section 6)
-- Dimensions created BEFORE facts because weather_daily FKs into locations.
-- Run as dba_role.
-- =============================================================================

SET ROLE dba_role;

-- open_sky.locations — dimension, SCD Type 2. Manually seeded (nothing in the
-- weather API response to derive it from), loaded before facts so weather_daily
-- can FK straight into the current version's surrogate key.
CREATE TABLE IF NOT EXISTS open_sky.locations (
    location_key  bigserial      PRIMARY KEY,
    location_id   int            NOT NULL,
    name          text           NOT NULL,
    lat           numeric(8,5)   NOT NULL CHECK (lat BETWEEN -90 AND 90),
    lon           numeric(8,5)   NOT NULL CHECK (lon BETWEEN -180 AND 180),
    timezone      text,
    open_date     timestamptz    NOT NULL DEFAULT now(),
    close_date    timestamptz
);
-- Exactly one current version per location; doubles as the location_key lookup.
CREATE UNIQUE INDEX IF NOT EXISTS locations_current_uq
    ON open_sky.locations (location_id) WHERE close_date IS NULL;

-- open_sky.asteroids — dimension, SCD Type 2, DERIVED from neo_close_approaches
-- after the fact table loads (not from a separate API call). No FK from the
-- fact table into here — the dimension row doesn't exist at fact-load time.
CREATE TABLE IF NOT EXISTS open_sky.asteroids (
    asteroid_key              bigserial      PRIMARY KEY,
    asteroid_id               text           NOT NULL,
    name                      text           NOT NULL,
    estimated_diameter_m_max  numeric(8,2)   CHECK (estimated_diameter_m_max > 0),
    is_potentially_hazardous  boolean        NOT NULL DEFAULT false,
    open_date                 timestamptz    NOT NULL DEFAULT now(),
    close_date                timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS asteroids_current_uq
    ON open_sky.asteroids (asteroid_id) WHERE close_date IS NULL;
-- Dashboard 2 filters/sorts on hazard flag (via a join to this table now).
CREATE INDEX IF NOT EXISTS asteroids_hazard_idx
    ON open_sky.asteroids (is_potentially_hazardous);

RESET ROLE;
