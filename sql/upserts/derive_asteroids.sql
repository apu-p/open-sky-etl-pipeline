-- open_sky.asteroids SCD Type 2 — DERIVED from the NeoWs staging rows after the
-- fact table loads (Section 6), not from a separate API call. Invoked as:
--   psql $ETL_DSN -f sql/upserts/derive_asteroids.sql
-- Per-asteroid descriptive attributes (name, diameter, hazard flag) are deduped
-- to one current version per asteroid_id. No run_id needed — this is a pure
-- dimension derivation.

BEGIN;

-- Close changed current versions. Latest-close-approach snapshot decides "the
-- current attributes" when an asteroid's values differ across windows.
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

-- Insert new current versions: never-seen asteroids + just-closed ones.
INSERT INTO open_sky.asteroids
    (asteroid_id, name, estimated_diameter_m_max, is_potentially_hazardous, open_date, close_date)
SELECT s.asteroid_id, s.name, s.estimated_diameter_m_max, s.is_potentially_hazardous, now(), NULL
FROM (
    SELECT DISTINCT asteroid_id, name, estimated_diameter_m_max, is_potentially_hazardous
    FROM staging.neows_raw
) s
LEFT JOIN open_sky.asteroids a
       ON a.asteroid_id = s.asteroid_id AND a.close_date IS NULL
WHERE a.asteroid_id IS NULL;

COMMIT;
