# Superset dashboards

Superset connects to Postgres as the least-privilege `superset_app` login
(`SUPERSET_DSN`), which has `SELECT` on `open_sky.*` and `open_sky_logs.*` only —
never `staging.*` (Section 13).

## One-time setup

1. `docker compose up -d` (Superset comes up on http://localhost:8088, login
   from `SUPERSET_ADMIN_USERNAME` / `SUPERSET_ADMIN_PASSWORD`).
2. **Settings → Database Connections → + Database**, paste your `SUPERSET_DSN`
   (`postgresql://superset_app:...@postgres:5432/open_sky`).
3. Build the charts below (SQL Lab → save as dataset/chart), or import a
   previously exported `dashboard_export.json`:
   ```
   docker compose exec superset superset import-dashboards -p /app/pythonpath/dashboard_export.json
   ```
   To capture your own after building:
   ```
   docker compose exec superset superset export-dashboards -f /app/pythonpath/dashboard_export.json
   ```

The queries below are the source of each panel — all facts are filtered to
current SCD2 versions (`close_date IS NULL`).

## Dashboard 1 — Weather Trends

Daily min/max temperature over time (filter by location):
```sql
SELECT w.date, l.name AS location, w.temp_min, w.temp_max
FROM open_sky.weather_daily w
JOIN open_sky.locations l
  ON l.location_key = w.location_key AND l.close_date IS NULL
WHERE w.close_date IS NULL
ORDER BY w.date;
```

Monthly precipitation totals by location:
```sql
SELECT date_trunc('month', w.date) AS month, l.name AS location,
       SUM(w.precipitation_mm) AS precip_mm
FROM open_sky.weather_daily w
JOIN open_sky.locations l
  ON l.location_key = w.location_key AND l.close_date IS NULL
WHERE w.close_date IS NULL
GROUP BY 1, 2 ORDER BY 1;
```

## Dashboard 2 — Near-Earth Objects

All panels join the current fact to the current dimension on `asteroid_id`
(diameter + hazard flag live on the dimension now, not the fact):
```sql
SELECT f.asteroid_id, a.name, f.close_approach_date,
       f.miss_distance_km, f.relative_velocity_kph,
       a.estimated_diameter_m_max, a.is_potentially_hazardous
FROM open_sky.neo_close_approaches f
JOIN open_sky.asteroids a
  ON a.asteroid_id = f.asteroid_id
 AND f.open_date >= a.open_date
 AND (f.open_date < a.close_date OR a.close_date IS NULL)   -- effective-dated
WHERE f.close_date IS NULL
ORDER BY f.miss_distance_km;
```

Close approaches per week:
```sql
SELECT date_trunc('week', close_approach_date) AS week, COUNT(*) AS approaches
FROM open_sky.neo_close_approaches
WHERE close_date IS NULL
GROUP BY 1 ORDER BY 1;
```

## Dashboard 3 — Space Weather Events

Solar-flare count per week, grouped by class:
```sql
SELECT date_trunc('week', event_time) AS week, LEFT(flare_class, 1) AS class,
       COUNT(*) AS flares
FROM open_sky.space_weather_events
WHERE close_date IS NULL AND event_type = 'FLR'
GROUP BY 1, 2 ORDER BY 1;
```

Geomagnetic-storm frequency by Kp band:
```sql
SELECT width_bucket(kp_index, 0, 9, 9) AS kp_band, COUNT(*) AS storms
FROM open_sky.space_weather_events
WHERE close_date IS NULL AND event_type = 'GST' AND kp_index IS NOT NULL
GROUP BY 1 ORDER BY 1;
```

Recent CMEs with linked GST:
```sql
SELECT event_time, source_location, speed_kms, linked_event_id
FROM open_sky.space_weather_events
WHERE close_date IS NULL AND event_type = 'CME'
ORDER BY event_time DESC LIMIT 50;
```

## Dashboard 4 — API Health & Reliability

Average response time per source over time:
```sql
SELECT date_trunc('hour', called_at) AS ts, source_name,
       AVG(response_time_ms) AS avg_ms
FROM open_sky_logs.api_call_log
GROUP BY 1, 2 ORDER BY 1;
```

Success rate (%) per source, trailing 7 days:
```sql
SELECT source_name,
       100.0 * AVG(CASE WHEN success THEN 1 ELSE 0 END) AS success_rate_pct
FROM open_sky_logs.api_call_log
WHERE called_at > now() - interval '7 days'
GROUP BY source_name;
```

Rate-limit headroom (current) per source:
```sql
SELECT DISTINCT ON (source_name) source_name, rate_limit_remaining, called_at
FROM open_sky_logs.api_call_log
WHERE rate_limit_remaining IS NOT NULL
ORDER BY source_name, called_at DESC;
```

Recent audit findings by severity:
```sql
SELECT detected_at, severity, check_name, related_table, description
FROM open_sky_logs.audit_findings
ORDER BY
  CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,
  detected_at DESC
LIMIT 100;
```
