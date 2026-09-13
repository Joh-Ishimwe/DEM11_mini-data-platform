-- ---------------------------------------------------------------------------
-- Runs ONCE, on a fresh postgres_data volume only.
-- If you edit this file, you must `docker compose down -v` to see the change.
--
-- Creates three logical databases in one container:
--   analytics : your warehouse. Owned by the pipeline.
--   airflow   : Airflow's own bookkeeping. Never touched by your DAG code.
--   metabase  : Metabase's dashboard storage.
--
-- In production these would be three separate instances. They are combined
-- here to keep the local footprint small. See docs/decisions.md.
-- ---------------------------------------------------------------------------

CREATE DATABASE analytics;
CREATE DATABASE airflow;
CREATE DATABASE metabase;

\connect analytics

-- The actual schema (staging + marts, tables and views) lives in
-- migrations/001_schema.sql, mounted into this container at /migrations.
-- CI applies that exact same file against its Postgres service container, so
-- there is one source of truth for the schema instead of two files that can
-- silently drift apart.
\i /migrations/001_schema.sql

-- ---------------------------------------------------------------------------
-- Least privilege: Metabase gets a user that can ONLY read marts.
-- A BI tool has no business being able to write to your warehouse.
-- ---------------------------------------------------------------------------
-- NOTE: env vars are NOT interpolated inside init.sql, so this literal must
-- match METABASE_READONLY_PASSWORD in your .env file. That duplication is a
-- known wart, documented in docs/decisions.md (ADR-013): a real deployment
-- would substitute this from a secrets manager at container start instead of
-- hardcoding it into a checked-in SQL file.
CREATE USER metabase_ro WITH PASSWORD 'change_me_locally';
GRANT CONNECT ON DATABASE analytics TO metabase_ro;
GRANT USAGE ON SCHEMA marts TO metabase_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO metabase_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO metabase_ro;
