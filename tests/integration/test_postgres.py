"""Integration tests: real Postgres, no mocks.

An INTEGRATION test checks that two real components cooperate. Unlike a unit
test it needs infrastructure, so it is slower and runs in its own CI job
against a service container.

The headline test here is idempotency. If you only write one integration
test in this whole project, write that one.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


def test_health_check(pg_config):
    from src.storage.postgres import PostgresStorage

    assert PostgresStorage(pg_config).health_check() is True


def test_loading_the_same_file_twice_does_not_duplicate(pg_config, clean_df, fixed_now):
    """IDEMPOTENCY. The single most important property in this project.

    Re-running after a failure is routine, not exceptional. If a re-run
    doubles revenue, nobody can ever safely re-run anything.
    """
    from src.storage.postgres import PostgresStorage
    from src.transformations import business_rules, cleaning

    storage = PostgresStorage(pg_config)
    df = cleaning.clean(clean_df, source_file="test.csv", ingested_at=fixed_now)
    df = business_rules.apply_business_rules(df)

    storage.upsert_orders(df, run_id="test-run-1")
    first = storage.previous_run_row_count()

    storage.upsert_orders(df, run_id="test-run-2")
    second = storage.previous_run_row_count()

    assert first == second


def test_partial_failure_rolls_back_the_whole_file(pg_config, clean_df, fixed_now):
    """Half a file loaded is worse than none: you cannot tell it happened."""
    from sqlalchemy import text

    from src.storage.postgres import PostgresStorage
    from src.transformations import business_rules, cleaning
    from src.utils.errors import PermanentError

    storage = PostgresStorage(pg_config)
    df = cleaning.clean(
        clean_df, source_file="rollback_test.csv", ingested_at=fixed_now
    )
    df = business_rules.apply_business_rules(df)
    # Sabotage one row so the batch violates the quantity > 0 check constraint.
    df.loc[df.index[0], "quantity"] = -1

    with pytest.raises(PermanentError):
        storage.upsert_orders(df, run_id="rollback-test")

    with storage._engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM marts.sales_orders WHERE order_id = ANY(:ids)"),
            {"ids": df["order_id"].tolist()},
        ).scalar()
    assert count == 0


def test_rejected_rows_land_in_quarantine_with_a_reason(pg_config, messy_df):
    from sqlalchemy import text

    from src.storage.postgres import PostgresStorage
    from src.transformations import validation
    from src.utils.constants import load_settings

    storage = PostgresStorage(pg_config)
    result = validation.validate(messy_df, load_settings())

    run_id = "quarantine-test"
    written = storage.quarantine(
        result.rejected, run_id=run_id, source_file="messy.csv"
    )
    assert written == len(result.rejected)

    with storage._engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT rejection_reason FROM staging.rejected_rows WHERE run_id = :run_id"
            ),
            {"run_id": run_id},
        ).fetchall()
    assert len(rows) == written
    assert all(row[0] for row in rows)
