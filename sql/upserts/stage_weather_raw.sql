-- Staging load (SCD Type 1) for weather. Invoked as:
--   psql $ETL_DSN -f sql/upserts/stage_weather_raw.sql < <flat_file>
-- Two steps: \copy the pipe-delimited file into a temp landing table, then
-- upsert on the natural key so each pull overwrites the row in place.
--
-- FORMAT csv makes embedded delimiters/quotes/newlines in text fields safe (the
-- convert step wrote this file with matching RFC-4180 quoting via csv.writer),
-- and an unquoted empty field is read back as SQL NULL.
--
-- The flat file is piped in as psql's stdin (PSTDIN) rather than templated in
-- via a :'flat_file' variable — \copy's own FROM/TO argument parser doesn't
-- reliably apply psql's variable interpolation (verified empirically: it
-- silently drops the WITH (...) options that follow), so the path never goes
-- near the .sql text at all.

CREATE TEMP TABLE weather_raw_incoming (LIKE staging.weather_raw INCLUDING DEFAULTS);

\copy weather_raw_incoming FROM PSTDIN WITH (FORMAT csv, DELIMITER '|')

INSERT INTO staging.weather_raw
    (location_id, date, temp_min, temp_max, precipitation_mm, wind_speed_max, weather_code, fetched_at)
SELECT location_id, date, temp_min, temp_max, precipitation_mm, wind_speed_max, weather_code, fetched_at
FROM weather_raw_incoming
ON CONFLICT (location_id, date) DO UPDATE
SET temp_min         = EXCLUDED.temp_min,
    temp_max         = EXCLUDED.temp_max,
    precipitation_mm = EXCLUDED.precipitation_mm,
    wind_speed_max   = EXCLUDED.wind_speed_max,
    weather_code     = EXCLUDED.weather_code,
    fetched_at       = EXCLUDED.fetched_at;
