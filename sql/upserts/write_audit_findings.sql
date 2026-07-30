-- Append audit findings to open_sky_logs.audit_findings (Section 12). Append-
-- only (bigserial id). Invoked as:
--   psql $ETL_DSN -v flat_file=<path> -f sql/upserts/write_audit_findings.sql

\copy open_sky_logs.audit_findings (check_name, severity, related_table, description, detected_at, pipeline_run_id) FROM :'flat_file' WITH (FORMAT csv, DELIMITER '|');
