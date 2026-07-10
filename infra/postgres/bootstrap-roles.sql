-- Run once as the managed Timescale bootstrap administrator.
-- Passwords are psql variables supplied by the secret manager, never literals.
\set ON_ERROR_STOP on

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'au_kpis_owner') THEN
        CREATE ROLE au_kpis_owner NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'au_kpis_ddl') THEN
        CREATE ROLE au_kpis_ddl LOGIN NOINHERIT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'au_kpis_runtime') THEN
        CREATE ROLE au_kpis_runtime LOGIN;
    END IF;
END
$$;

ALTER ROLE au_kpis_ddl PASSWORD :'ddl_password';
ALTER ROLE au_kpis_runtime PASSWORD :'runtime_password';
GRANT au_kpis_owner TO au_kpis_ddl;

ALTER SCHEMA public OWNER TO au_kpis_owner;
GRANT USAGE ON SCHEMA public TO au_kpis_runtime;
REVOKE CREATE ON SCHEMA public FROM PUBLIC, au_kpis_runtime;
GRANT CONNECT ON DATABASE :"database_name" TO au_kpis_ddl, au_kpis_runtime;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO au_kpis_runtime;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO au_kpis_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE au_kpis_owner IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO au_kpis_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE au_kpis_owner IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO au_kpis_runtime;

ALTER ROLE au_kpis_runtime SET statement_timeout = '120s';
ALTER ROLE au_kpis_runtime SET lock_timeout = '5s';
ALTER ROLE au_kpis_ddl SET statement_timeout = '15min';
ALTER ROLE au_kpis_ddl SET lock_timeout = '30s';
