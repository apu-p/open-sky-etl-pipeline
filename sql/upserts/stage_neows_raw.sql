-- Staging load (SCD Type 1) for NeoWs asteroid close approaches. Invoked as:
--   psql $ETL_DSN -v flat_file=<path> -f sql/upserts/stage_neows_raw.sql
-- Natural key: (asteroid_id, close_approach_date).

CREATE TEMP TABLE neows_raw_incoming (LIKE staging.neows_raw INCLUDING DEFAULTS);

\copy neows_raw_incoming FROM :'flat_file' WITH (FORMAT csv, DELIMITER '|');

INSERT INTO staging.neows_raw
    (asteroid_id, close_approach_date, name, miss_distance_km,
     relative_velocity_kph, estimated_diameter_m_max, is_potentially_hazardous, fetched_at)
SELECT asteroid_id, close_approach_date, name, miss_distance_km,
       relative_velocity_kph, estimated_diameter_m_max, is_potentially_hazardous, fetched_at
FROM neows_raw_incoming
ON CONFLICT (asteroid_id, close_approach_date) DO UPDATE
SET name                     = EXCLUDED.name,
    miss_distance_km         = EXCLUDED.miss_distance_km,
    relative_velocity_kph    = EXCLUDED.relative_velocity_kph,
    estimated_diameter_m_max = EXCLUDED.estimated_diameter_m_max,
    is_potentially_hazardous = EXCLUDED.is_potentially_hazardous,
    fetched_at               = EXCLUDED.fetched_at;
