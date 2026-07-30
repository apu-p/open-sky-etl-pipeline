-- open_sky.weather_daily historize (SCD Type 2). Invoked as:
--   psql $ETL_DSN -v run_id=<uuid> -f sql/upserts/weather_daily.sql
-- Reads staging.weather_raw, resolves the current location_key, and derives the
-- open_sky-only columns (temp_range, is_rain_day, weather_condition) that
-- staging doesn't carry. source_fetched_at comes from staging.fetched_at;
-- pipeline_run_id is bound via :'run_id' (a psql variable, not string-built).

BEGIN;

-- Close changed current versions.
UPDATE open_sky.weather_daily c
SET close_date = now()
FROM staging.weather_raw s
JOIN open_sky.locations l ON l.location_id = s.location_id AND l.close_date IS NULL
WHERE c.location_key = l.location_key
  AND c.date = s.date
  AND c.close_date IS NULL
  AND (c.temp_min, c.temp_max, c.precipitation_mm, c.wind_speed_max, c.weather_code)
      IS DISTINCT FROM (s.temp_min, s.temp_max, s.precipitation_mm, s.wind_speed_max, s.weather_code);

-- Insert new current versions: new keys + just-closed ones.
INSERT INTO open_sky.weather_daily
    (location_key, date, temp_min, temp_max, temp_range, precipitation_mm, is_rain_day,
     wind_speed_max, weather_code, weather_condition, source_fetched_at, pipeline_run_id,
     open_date, close_date)
SELECT l.location_key,
       s.date,
       s.temp_min,
       s.temp_max,
       (s.temp_max - s.temp_min)                                AS temp_range,
       s.precipitation_mm,
       (COALESCE(s.precipitation_mm, 0) > 0)                    AS is_rain_day,
       s.wind_speed_max,
       s.weather_code,
       CASE s.weather_code                                      -- WMO code lookup
           WHEN 0  THEN 'Clear sky'
           WHEN 1  THEN 'Mainly clear'
           WHEN 2  THEN 'Partly cloudy'
           WHEN 3  THEN 'Overcast'
           WHEN 45 THEN 'Fog'
           WHEN 48 THEN 'Depositing rime fog'
           WHEN 51 THEN 'Light drizzle'
           WHEN 53 THEN 'Moderate drizzle'
           WHEN 55 THEN 'Dense drizzle'
           WHEN 56 THEN 'Light freezing drizzle'
           WHEN 57 THEN 'Dense freezing drizzle'
           WHEN 61 THEN 'Slight rain'
           WHEN 63 THEN 'Moderate rain'
           WHEN 65 THEN 'Heavy rain'
           WHEN 66 THEN 'Light freezing rain'
           WHEN 67 THEN 'Heavy freezing rain'
           WHEN 71 THEN 'Slight snowfall'
           WHEN 73 THEN 'Moderate snowfall'
           WHEN 75 THEN 'Heavy snowfall'
           WHEN 77 THEN 'Snow grains'
           WHEN 80 THEN 'Slight rain showers'
           WHEN 81 THEN 'Moderate rain showers'
           WHEN 82 THEN 'Violent rain showers'
           WHEN 85 THEN 'Slight snow showers'
           WHEN 86 THEN 'Heavy snow showers'
           WHEN 95 THEN 'Thunderstorm'
           WHEN 96 THEN 'Thunderstorm with slight hail'
           WHEN 99 THEN 'Thunderstorm with heavy hail'
           ELSE 'Unknown'
       END                                                      AS weather_condition,
       s.fetched_at                                             AS source_fetched_at,
       :'run_id'::uuid                                          AS pipeline_run_id,
       now(),
       NULL
FROM staging.weather_raw s
JOIN open_sky.locations l ON l.location_id = s.location_id AND l.close_date IS NULL
LEFT JOIN open_sky.weather_daily c
       ON c.location_key = l.location_key AND c.date = s.date AND c.close_date IS NULL
WHERE c.location_key IS NULL;

COMMIT;
