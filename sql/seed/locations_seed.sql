-- Manually-seeded location dimension source (Section 6: locations is not
-- derivable from the weather API, which takes lat/lon as query params). Loads
-- the landing table; sql/upserts/locations.sql then does the SCD2 load into
-- open_sky.locations. Idempotent so a re-seed is harmless.
--
-- These same rows drive etl/config.py's LOCATIONS list — keep the two in sync.

INSERT INTO staging.locations_incoming (location_id, name, lat, lon, timezone) VALUES
    (1, 'London',   51.50740,   -0.12780, 'Europe/London'),
    (2, 'New York', 40.71280,  -74.00600, 'America/New_York'),
    (3, 'Tokyo',    35.67620,  139.65030, 'Asia/Tokyo'),
    (4, 'Sydney',  -33.86880,  151.20930, 'Australia/Sydney'),
    (5, 'Nairobi',  -1.28640,   36.81720, 'Africa/Nairobi')
ON CONFLICT (location_id) DO UPDATE
SET name     = EXCLUDED.name,
    lat      = EXCLUDED.lat,
    lon      = EXCLUDED.lon,
    timezone = EXCLUDED.timezone;
