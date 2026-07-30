-- open_sky.neo_close_approaches historize (SCD Type 2). Invoked as:
--   psql $ETL_DSN -v run_id=<uuid> -f sql/upserts/neo_close_approaches.sql
-- Keyed on (asteroid_id, close_approach_date). The asteroids dimension is
-- derived FROM this table afterwards (sql/upserts/derive_asteroids.sql).

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
       s.fetched_at, :'run_id'::uuid, now(), NULL
FROM staging.neows_raw s
LEFT JOIN open_sky.neo_close_approaches c
       ON c.asteroid_id = s.asteroid_id
      AND c.close_approach_date = s.close_approach_date
      AND c.close_date IS NULL
WHERE c.asteroid_id IS NULL;

COMMIT;
