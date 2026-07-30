#!/usr/bin/env bash
# =============================================================================
# One-time Postgres bootstrap, run by the official postgres image's
# docker-entrypoint-initdb.d hook on FIRST boot only (empty data volume), as
# POSTGRES_USER. Runs the numbered role/DDL/grant files IN ORDER (Section 8):
#   roles (create) -> schemas + DDL (as dba_role) -> grants (revoke DML off dba)
# then seeds the locations dimension.
#
# A shell wrapper (rather than dropping each .sql straight into initdb.d) is what
# lets us (a) pass the login-role passwords in as psql -v variables sourced from
# the environment, and (b) guarantee ordering. The .sql files themselves live in
# /sql (mounted read-only), NOT in initdb.d, so they don't also auto-run without
# their variables.
# =============================================================================
set -euo pipefail

SQL=/sql
PSQL=(psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER")

echo "[init-db] creating Airflow metadata database"
"${PSQL[@]}" --dbname postgres -c "CREATE DATABASE airflow;"

# Runs a schema file against the warehouse DB, passing every login password as a
# psql variable. Files that don't reference a given variable simply ignore it.
run_ddl() {
    echo "[init-db] running $1"
    "${PSQL[@]}" --dbname "$POSTGRES_DB" \
        -v dba_app_password="$DBA_APP_PASSWORD" \
        -v etl_app_password="$ETL_APP_PASSWORD" \
        -v superset_app_password="$SUPERSET_APP_PASSWORD" \
        -v dev_achu_password="$DEV_ACHU_PASSWORD" \
        -f "$1"
}

run_ddl "$SQL/roles/01_create_roles.sql"
run_ddl "$SQL/ddl/02_schemas_and_staging.sql"
run_ddl "$SQL/ddl/03_open_sky_dimensions.sql"
run_ddl "$SQL/ddl/04_open_sky_facts.sql"
run_ddl "$SQL/ddl/05_open_sky_logs.sql"
run_ddl "$SQL/roles/06_grants.sql"

echo "[init-db] seeding locations dimension (SCD2)"
"${PSQL[@]}" --dbname "$POSTGRES_DB" -f "$SQL/seed/locations_seed.sql"
"${PSQL[@]}" --dbname "$POSTGRES_DB" -f "$SQL/upserts/locations.sql"

echo "[init-db] bootstrap complete"
