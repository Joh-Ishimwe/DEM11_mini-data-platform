"""Warehouse writes.

The headline requirement: IDEMPOTENCY. Running the same file twice must not
double your revenue. Achieved with an upsert on the order_id primary key,
never a plain INSERT.

IDEMPOTENT = applying an operation repeatedly gives the same result as
applying it once. It is the most valuable single property in data
engineering, because re-running after a failure is routine, not exceptional.
"""

from __future__ import annotations

import json
from contextlib import contextmanager

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError, OperationalError

from src.monitoring.logging import get_logger
from src.utils.constants import PostgresConfig, load_settings
from src.utils.errors import PermanentError, TransientError

log = get_logger(__name__)

_ORDER_COLUMNS = (
    "order_id",
    "customer_id",
    "order_date",
    "product_category",
    "quantity",
    "unit_price",
    "currency",
    "country",
    "revenue",
    "order_size_bucket",
    "source_file",
    "ingested_at",
)

_UPSERT_SQL = text(
    """
    INSERT INTO marts.sales_orders
        (order_id, customer_id, order_date, product_category, quantity,
         unit_price, currency, country, revenue, order_size_bucket,
         source_file, ingested_at)
    VALUES
        (:order_id, :customer_id, :order_date, :product_category, :quantity,
         :unit_price, :currency, :country, :revenue, :order_size_bucket,
         :source_file, :ingested_at)
    ON CONFLICT (order_id) DO UPDATE SET
        customer_id       = EXCLUDED.customer_id,
        order_date        = EXCLUDED.order_date,
        product_category  = EXCLUDED.product_category,
        quantity          = EXCLUDED.quantity,
        unit_price        = EXCLUDED.unit_price,
        currency          = EXCLUDED.currency,
        country           = EXCLUDED.country,
        revenue           = EXCLUDED.revenue,
        order_size_bucket = EXCLUDED.order_size_bucket,
        source_file       = EXCLUDED.source_file,
        ingested_at       = EXCLUDED.ingested_at
    """
)


def _json_safe(value):
    """Make a single scalar value safe to embed in json.dumps.

    NaN/NaT/pd.NA all mean the same thing to a JSON audit record: absent.
    """
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass  # value is not something pd.isna understands (e.g. a str) - keep it
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


class PostgresStorage:
    def __init__(self, config: PostgresConfig) -> None:
        self.config = config
        self._engine = create_engine(
            config.sqlalchemy_url,
            pool_pre_ping=True,  # checks a pooled connection is alive before reuse
            connect_args={
                "connect_timeout": 10,
                # A runaway query must not hang the pipeline all night.
                "options": f"-c statement_timeout={config.statement_timeout_seconds * 1000}",
            },
        )

    @contextmanager
    def transaction(self):
        """Yield a connection inside a transaction.

        TRANSACTION = a group of database operations committed together or
        rolled back together. This is what stops a failure at row 3,000 of
        5,000 leaving you with half a file loaded and no way to tell.
        """
        conn = self._engine.connect()
        trans = conn.begin()
        try:
            yield conn
            trans.commit()
        except (OperationalError, DBAPIError) as exc:
            trans.rollback()
            # Connection-level problems may pass on a retry; SQL errors will not.
            if isinstance(exc, OperationalError):
                raise TransientError(f"database connection problem: {exc}") from exc
            raise PermanentError(f"database rejected the statement: {exc}") from exc
        except Exception:
            trans.rollback()
            raise
        finally:
            conn.close()

    def upsert_orders(self, df: pd.DataFrame, run_id: str) -> int:
        """Insert rows into marts.sales_orders, updating on order_id conflict.

        Loads inside one transaction, batched by settings['pipeline']['batch_size']
        so a very large file doesn't become one giant statement. Values are
        always bound parameters, never string-concatenated - both the usual
        SQL-injection defence and the only way an apostrophe in a country
        name doesn't break the query.

        Idempotent: running this twice with the same data leaves the row
        count unchanged, since ON CONFLICT updates instead of duplicating.
        """
        if df.empty:
            return 0

        settings = load_settings()
        batch_size = settings.get("pipeline", {}).get("batch_size", 5000)
        records = df[list(_ORDER_COLUMNS)].to_dict(orient="records")

        with self.transaction() as conn:
            for start in range(0, len(records), batch_size):
                conn.execute(_UPSERT_SQL, records[start : start + batch_size])

        log.info("orders_upserted", extra={"run_id": run_id, "rows": len(records)})
        return len(records)

    def quarantine(self, rejected: pd.DataFrame, run_id: str, source_file: str) -> int:
        """Write rejected rows to staging.rejected_rows with their reason."""
        if rejected.empty:
            return 0

        stmt = text(
            """
            INSERT INTO staging.rejected_rows (run_id, source_file, rejection_reason, raw_record)
            VALUES (:run_id, :source_file, :rejection_reason, CAST(:raw_record AS JSONB))
            """
        )
        rows = []
        for record in rejected.to_dict(orient="records"):
            reason = record.pop("rejection_reason", "unknown")
            safe_record = {k: _json_safe(v) for k, v in record.items()}
            rows.append(
                {
                    "run_id": run_id,
                    "source_file": source_file,
                    "rejection_reason": reason,
                    "raw_record": json.dumps(safe_record, default=str),
                }
            )

        with self.transaction() as conn:
            conn.execute(stmt, rows)

        log.warning(
            "rows_quarantined",
            extra={"run_id": run_id, "source_file": source_file, "count": len(rows)},
        )
        return len(rows)

    def start_run(self, run_id: str, dag_id: str, source_file: str) -> None:
        """Insert a row into staging.pipeline_runs with status='running'."""
        stmt = text(
            """
            INSERT INTO staging.pipeline_runs (run_id, dag_id, source_file, started_at, status)
            VALUES (:run_id, :dag_id, :source_file, NOW(), 'running')
            ON CONFLICT (run_id) DO NOTHING
            """
        )
        with self.transaction() as conn:
            conn.execute(
                stmt, {"run_id": run_id, "dag_id": dag_id, "source_file": source_file}
            )

    def finish_run(
        self,
        run_id: str,
        status: str,
        rows_read: int,
        rows_loaded: int,
        rows_rejected: int,
        error_message: str | None = None,
    ) -> None:
        """Close out the pipeline_runs row. Call this even on failure.

        A run that fails and leaves status='running' forever is worse than a
        run that records its own failure. Use a try/finally in the DAG.
        """
        stmt = text(
            """
            UPDATE staging.pipeline_runs
            SET finished_at = NOW(), status = :status, rows_read = :rows_read,
                rows_loaded = :rows_loaded, rows_rejected = :rows_rejected,
                error_message = :error_message
            WHERE run_id = :run_id
            """
        )
        with self.transaction() as conn:
            conn.execute(
                stmt,
                {
                    "run_id": run_id,
                    "status": status,
                    "rows_read": rows_read,
                    "rows_loaded": rows_loaded,
                    "rows_rejected": rows_rejected,
                    "error_message": error_message,
                },
            )

    def previous_run_row_count(self) -> int:
        """Total rows currently in marts.sales_orders.

        Deliberately the warehouse's running total, not one specific past
        run's count (see ADR-014) - that's what makes it useful for proving
        idempotency: re-upserting the same file leaves this number unchanged.
        """
        with self._engine.connect() as conn:
            return conn.execute(
                text("SELECT COUNT(*) FROM marts.sales_orders")
            ).scalar()

    def last_run_rows_loaded(self) -> int:
        """rows_loaded from the most recent successful run.

        Unlike previous_run_row_count() (the warehouse's running total), this
        is "how many rows did a normal run load" - what the volume-drop
        guardrail actually needs. Call it before the current run writes
        anything, or it'll just see its own result.
        """
        stmt = text(
            """
            SELECT rows_loaded FROM staging.pipeline_runs
            WHERE status = 'success'
            ORDER BY finished_at DESC
            LIMIT 1
            """
        )
        with self._engine.connect() as conn:
            row = conn.execute(stmt).fetchone()
            return row[0] if row and row[0] is not None else 0

    def health_check(self) -> bool:
        """Return True if `SELECT 1` succeeds. Used by the e2e test."""
        with self._engine.connect() as conn:
            return conn.execute(text("SELECT 1")).scalar() == 1
