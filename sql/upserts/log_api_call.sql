-- Append call metrics to open_sky_logs.api_call_log (Section 5). Append-only
-- (bigserial id), so a plain \copy is enough — no upsert. Invoked as:
--   psql $ETL_DSN -v flat_file=<path> -f sql/upserts/log_api_call.sql
-- Column list excludes the bigserial id.

\copy open_sky_logs.api_call_log (source_name, called_at, response_time_ms, http_status_code, success, retry_count, records_returned, rate_limit_remaining, pipeline_run_id) FROM :'flat_file' WITH (FORMAT csv, DELIMITER '|');
