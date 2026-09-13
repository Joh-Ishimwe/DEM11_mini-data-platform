-- ---------------------------------------------------------------------------
-- Schema for the analytics database: staging + marts, tables and views.
--
-- This file is the ONE source of truth for that schema. It runs in two
-- places that must never drift apart:
--   - locally, via config/postgres/init.sql (\i on a fresh volume)
--   - in CI, applied directly against the Postgres service container
--
-- Assumes it is running against the `analytics` database already.
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- marts.sales_orders : the target table.
-- order_id is the PRIMARY KEY, which is what makes the upsert in
-- src/storage/postgres.py idempotent. Re-running a file cannot duplicate rows.
CREATE TABLE IF NOT EXISTS marts.sales_orders (
    order_id          TEXT PRIMARY KEY,
    customer_id       TEXT NOT NULL,
    order_date        DATE NOT NULL,
    product_category  TEXT NOT NULL,
    quantity          INTEGER NOT NULL CHECK (quantity > 0),
    unit_price        NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    currency          CHAR(3) NOT NULL,
    country           TEXT,
    revenue           NUMERIC(14, 2) NOT NULL,
    order_size_bucket TEXT,
    source_file       TEXT NOT NULL,
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sales_orders_date ON marts.sales_orders (order_date);
CREATE INDEX IF NOT EXISTS idx_sales_orders_source ON marts.sales_orders (source_file);

-- staging.rejected_rows : the quarantine table.
-- One bad record must never kill a run. Bad rows land here with the reason,
-- so tomorrow morning somebody has something concrete to investigate.
CREATE TABLE IF NOT EXISTS staging.rejected_rows (
    id            BIGSERIAL PRIMARY KEY,
    run_id        TEXT NOT NULL,
    source_file   TEXT NOT NULL,
    rejection_reason TEXT NOT NULL,
    raw_record    JSONB NOT NULL,
    rejected_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rejected_run ON staging.rejected_rows (run_id);

-- staging.pipeline_runs : the pipeline monitoring itself.
CREATE TABLE IF NOT EXISTS staging.pipeline_runs (
    run_id          TEXT PRIMARY KEY,
    dag_id          TEXT NOT NULL,
    source_file     TEXT,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    status          TEXT NOT NULL,      -- running | success | failed
    rows_read       INTEGER,
    rows_loaded     INTEGER,
    rows_rejected   INTEGER,
    error_message   TEXT
);

-- marts.v_daily_rejections / marts.v_pipeline_runs : read-only windows into
-- staging, exposed in marts so Metabase's read-only user never needs direct
-- access to staging. Backs the dashboard's "rejected rows per day" card.
CREATE OR REPLACE VIEW marts.v_daily_rejections AS
SELECT date_trunc('day', rejected_at)::date AS day, COUNT(*) AS rejected_rows
FROM staging.rejected_rows
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE VIEW marts.v_pipeline_runs AS
SELECT run_id, dag_id, source_file, started_at, finished_at, status,
       rows_read, rows_loaded, rows_rejected
FROM staging.pipeline_runs;
