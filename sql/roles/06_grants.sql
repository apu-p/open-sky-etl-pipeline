-- =============================================================================
-- 06_grants.sql  (Section 13)
-- Run LAST, as the Postgres superuser (superuser bypasses ownership checks, so
-- it can GRANT on dba_role-owned tables directly). Runs after the DDL because
-- GRANT ... ON <table> needs the tables to already exist.
-- =============================================================================

RESET ROLE;  -- ensure we're the superuser, not dba_role

-- -----------------------------------------------------------------------------
-- etl_role — the only thing that writes data (SCD1 staging upserts, SCD2
-- close-then-insert in open_sky, call-metrics + audit rows in open_sky_logs).
-- SELECT/INSERT/UPDATE, no DELETE (the SCD design never deletes), no DDL.
-- -----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA staging, open_sky, open_sky_logs TO etl_role;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA staging       TO etl_role;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA open_sky       TO etl_role;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA open_sky_logs  TO etl_role;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA staging       TO etl_role;  -- bigserial PKs
GRANT USAGE ON ALL SEQUENCES IN SCHEMA open_sky       TO etl_role;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA open_sky_logs  TO etl_role;
-- Future tables dba_role creates get the same grants automatically.
ALTER DEFAULT PRIVILEGES FOR ROLE dba_role IN SCHEMA staging      GRANT SELECT, INSERT, UPDATE ON TABLES TO etl_role;
ALTER DEFAULT PRIVILEGES FOR ROLE dba_role IN SCHEMA open_sky      GRANT SELECT, INSERT, UPDATE ON TABLES TO etl_role;
ALTER DEFAULT PRIVILEGES FOR ROLE dba_role IN SCHEMA open_sky_logs GRANT SELECT, INSERT, UPDATE ON TABLES TO etl_role;
ALTER DEFAULT PRIVILEGES FOR ROLE dba_role IN SCHEMA staging      GRANT USAGE ON SEQUENCES TO etl_role;
ALTER DEFAULT PRIVILEGES FOR ROLE dba_role IN SCHEMA open_sky      GRANT USAGE ON SEQUENCES TO etl_role;
ALTER DEFAULT PRIVILEGES FOR ROLE dba_role IN SCHEMA open_sky_logs GRANT USAGE ON SEQUENCES TO etl_role;

-- -----------------------------------------------------------------------------
-- developer_role — read-only everything (incl. staging, so a dev can trace an
-- open_sky row back to the raw payload).
-- -----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA staging, open_sky, open_sky_logs TO developer_role;
GRANT SELECT ON ALL TABLES IN SCHEMA staging       TO developer_role;
GRANT SELECT ON ALL TABLES IN SCHEMA open_sky       TO developer_role;
GRANT SELECT ON ALL TABLES IN SCHEMA open_sky_logs  TO developer_role;
ALTER DEFAULT PRIVILEGES FOR ROLE dba_role IN SCHEMA staging      GRANT SELECT ON TABLES TO developer_role;
ALTER DEFAULT PRIVILEGES FOR ROLE dba_role IN SCHEMA open_sky      GRANT SELECT ON TABLES TO developer_role;
ALTER DEFAULT PRIVILEGES FOR ROLE dba_role IN SCHEMA open_sky_logs GRANT SELECT ON TABLES TO developer_role;

-- -----------------------------------------------------------------------------
-- superset_role — read-only, EXPLICIT table list only (deliberately NOT "ALL
-- TABLES" and NOT covered by ALTER DEFAULT PRIVILEGES), so a new table isn't
-- auto-exposed to the BI tool just for existing. staging.* is never granted.
-- -----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA open_sky, open_sky_logs TO superset_role;
GRANT SELECT ON
    open_sky.locations,
    open_sky.weather_daily,
    open_sky.asteroids,
    open_sky.neo_close_approaches,
    open_sky.space_weather_events,
    open_sky_logs.api_call_log,
    open_sky_logs.audit_findings
TO superset_role;

-- -----------------------------------------------------------------------------
-- dba_role — strip the DML rights that came implicitly with table ownership,
-- leaving it DDL-only (structural changes but no read/write of data).
-- -----------------------------------------------------------------------------
REVOKE SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging       FROM dba_role;
REVOKE SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA open_sky       FROM dba_role;
REVOKE SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA open_sky_logs  FROM dba_role;

-- Foreign-key enforcement is a trigger on the REFERENCING table, but Postgres
-- checks the referenced table's row-lock ACL (SELECT + UPDATE) against the
-- table OWNER, not the inserting role — verified empirically: with these two
-- revoked from dba_role (owner of open_sky.locations), etl_role's own INSERT
-- into weather_daily fails with "permission denied for table locations" even
-- though etl_role itself holds SELECT/INSERT/UPDATE. locations is the only
-- FK-referenced table in this schema (weather_daily_location_key_fkey), so
-- this is the one narrow carve-out from "DDL-only" dba_role needs to keep FK
-- enforcement working at all.
GRANT SELECT, UPDATE ON open_sky.locations TO dba_role;
