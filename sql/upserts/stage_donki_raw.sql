-- Staging load (SCD Type 1) for DONKI space-weather events. Invoked as:
--   psql $ETL_DSN -v flat_file=<path> -f sql/upserts/stage_donki_raw.sql
-- Natural key: event_id (DONKI's own stable ID, unique across CME/GST/FLR).

CREATE TEMP TABLE donki_raw_incoming (LIKE staging.donki_raw INCLUDING DEFAULTS);

\copy donki_raw_incoming FROM :'flat_file' WITH (FORMAT csv, DELIMITER '|');

INSERT INTO staging.donki_raw
    (event_id, event_type, event_time, source_location, speed_kms, kp_index,
     flare_class, flare_severity_score, linked_event_id, fetched_at)
SELECT event_id, event_type, event_time, source_location, speed_kms, kp_index,
       flare_class, flare_severity_score, linked_event_id, fetched_at
FROM donki_raw_incoming
ON CONFLICT (event_id) DO UPDATE
SET event_type           = EXCLUDED.event_type,
    event_time           = EXCLUDED.event_time,
    source_location      = EXCLUDED.source_location,
    speed_kms            = EXCLUDED.speed_kms,
    kp_index             = EXCLUDED.kp_index,
    flare_class          = EXCLUDED.flare_class,
    flare_severity_score = EXCLUDED.flare_severity_score,
    linked_event_id      = EXCLUDED.linked_event_id,
    fetched_at           = EXCLUDED.fetched_at;
