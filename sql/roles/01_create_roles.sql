-- =============================================================================
-- 01_create_roles.sql  (Section 13)
-- Run FIRST, as the Postgres superuser (POSTGRES_USER), before any schema/table
-- DDL exists. Role creation has to precede DDL because dba_role OWNS the schemas
-- and tables (CREATE SCHEMA ... AUTHORIZATION dba_role), and ownership is the
-- only way to get ALTER/DROP rights in Postgres.
--
-- Passwords come in as psql variables (-v foo_password=...) sourced from .env at
-- container init, never hardcoded here. See docker/init-db.sh.
-- =============================================================================

-- Four NOLOGIN group roles: "what you're allowed to do".
CREATE ROLE dba_role NOLOGIN;
CREATE ROLE etl_role NOLOGIN;
CREATE ROLE developer_role NOLOGIN;
CREATE ROLE superset_role NOLOGIN;

-- LOGIN roles: "who's connecting". Each is a member of exactly one group role,
-- so rotating a password or adding a developer never touches privilege grants.
CREATE ROLE dba_app      LOGIN PASSWORD :'dba_app_password'      IN ROLE dba_role;
CREATE ROLE etl_app      LOGIN PASSWORD :'etl_app_password'      IN ROLE etl_role;
CREATE ROLE superset_app LOGIN PASSWORD :'superset_app_password' IN ROLE superset_role;
CREATE ROLE dev_achu     LOGIN PASSWORD :'dev_achu_password'     IN ROLE developer_role;
