-- Append audit findings to open_sky_logs.audit_findings (Section 12). Append-
-- only (bigserial id). Invoked as:
--   psql $ETL_DSN -f sql/upserts/write_audit_findings.sql < <flat_file>
--
-- Flat file piped in as PSTDIN, not a :'flat_file' variable — see
-- stage_weather_raw.sql for why \copy can't reliably template its own path.

\copy open_sky_logs.audit_findings (check_name, severity, related_table, description, detected_at, pipeline_run_id) FROM PSTDIN WITH (FORMAT csv, DELIMITER '|')
