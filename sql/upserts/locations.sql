-- open_sky.locations SCD Type 2 load (Section 6). Reads the manually-seeded
-- staging.locations_incoming landing table. Invoked as:
--   psql $DBA_DSN? no -> psql $ETL_DSN -f sql/upserts/locations.sql
-- (etl_role has INSERT/UPDATE on open_sky; run once at init, before facts).
-- Both statements in ONE transaction: the INSERT's LEFT JOIN won't match a row
-- the UPDATE just closed, so a changed location correctly gets a fresh version.

BEGIN;

-- Close out the current version of any location whose attributes changed.
UPDATE open_sky.locations c
SET close_date = now()
FROM staging.locations_incoming s
WHERE c.location_id = s.location_id
  AND c.close_date IS NULL
  AND (c.name, c.lat, c.lon, c.timezone)
      IS DISTINCT FROM (s.name, s.lat, s.lon, s.timezone);  -- NULL-safe

-- Insert a new current version: brand-new locations, and ones just closed above.
INSERT INTO open_sky.locations (location_id, name, lat, lon, timezone, open_date, close_date)
SELECT s.location_id, s.name, s.lat, s.lon, s.timezone, now(), NULL
FROM staging.locations_incoming s
LEFT JOIN open_sky.locations c
       ON c.location_id = s.location_id AND c.close_date IS NULL
WHERE c.location_id IS NULL;

COMMIT;
