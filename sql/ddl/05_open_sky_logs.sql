-- =============================================================================
-- 05_open_sky_logs.sql  (Sections 5, 12)
-- Data ABOUT the pipeline (call metrics, audit findings) — its own schema, kept
-- out of open_sky so a wildcard grant can't conflate domain data with pipeline
-- telemetry. No dependency on the open_sky facts, so order vs. 03/04 is free.
-- Run as dba_role.
-- =============================================================================

SET ROLE dba_role;

-- open_sky_logs.api_call_log — one row per extract API call (observability).
CREATE TABLE IF NOT EXISTS open_sky_logs.api_call_log (
    id                    bigserial      PRIMARY KEY,
    source_name           text           NOT NULL,
    called_at             timestamptz    NOT NULL,
    response_time_ms      int,
    http_status_code      int,
    success               boolean        NOT NULL,
    retry_count           int            DEFAULT 0,
    records_returned      int,
    rate_limit_remaining  int,
    pipeline_run_id       uuid
);
CREATE INDEX IF NOT EXISTS api_call_log_source_time_idx
    ON open_sky_logs.api_call_log (source_name, called_at);

-- open_sky_logs.audit_findings — written by the audit checks only (Section 12).
CREATE TABLE IF NOT EXISTS open_sky_logs.audit_findings (
    id             bigserial     PRIMARY KEY,
    check_name     text          NOT NULL,
    severity       text          NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    related_table  text,
    description    text          NOT NULL,
    detected_at    timestamptz   NOT NULL,
    pipeline_run_id uuid
);
CREATE INDEX IF NOT EXISTS audit_findings_check_time_idx
    ON open_sky_logs.audit_findings (check_name, detected_at);

RESET ROLE;
