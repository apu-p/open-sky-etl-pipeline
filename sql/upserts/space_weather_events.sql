-- open_sky.space_weather_events historize (SCD Type 2). Invoked as:
--   psql $ETL_DSN -v run_id=<uuid> -f sql/upserts/space_weather_events.sql
-- Keyed on event_id (DONKI's stable ID). Global feed, no dimension.

BEGIN;

UPDATE open_sky.space_weather_events c
SET close_date = now()
FROM staging.donki_raw s
WHERE c.event_id = s.event_id
  AND c.close_date IS NULL
  AND (c.event_type, c.event_time, c.source_location, c.speed_kms, c.kp_index,
       c.flare_class, c.flare_severity_score, c.linked_event_id)
      IS DISTINCT FROM
      (s.event_type, s.event_time, s.source_location, s.speed_kms, s.kp_index,
       s.flare_class, s.flare_severity_score, s.linked_event_id);

INSERT INTO open_sky.space_weather_events
    (event_id, event_type, event_time, source_location, speed_kms, kp_index,
     flare_class, flare_severity_score, linked_event_id, source_fetched_at,
     pipeline_run_id, open_date, close_date)
SELECT s.event_id, s.event_type, s.event_time, s.source_location, s.speed_kms, s.kp_index,
       s.flare_class, s.flare_severity_score, s.linked_event_id, s.fetched_at,
       :'run_id'::uuid, now(), NULL
FROM staging.donki_raw s
LEFT JOIN open_sky.space_weather_events c
       ON c.event_id = s.event_id AND c.close_date IS NULL
WHERE c.event_id IS NULL;

COMMIT;
