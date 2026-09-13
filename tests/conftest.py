"""Shared pytest fixtures.

A FIXTURE is a reusable piece of test setup. Declaring it here makes it
available to every test file without importing anything.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLES = PROJECT_ROOT / "data" / "samples"

FIXED_NOW = datetime(2026, 1, 15, 9, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def fixed_now() -> datetime:
    """A frozen timestamp.

    Never use datetime.now() in a test. A test whose result depends on the
    clock will eventually fail at midnight, or on a leap day, for no reason
    you can reproduce.
    """
    return FIXED_NOW


@pytest.fixture
def clean_df() -> pd.DataFrame:
    """A tiny, perfectly valid frame. The happy path."""
    return pd.DataFrame(
        [
            {
                "order_id": "ORD-001",
                "customer_id": "CUST-01",
                "order_date": "2026-01-10",
                "product_category": "electronics",
                "quantity": 2,
                "unit_price": 99.99,
                "currency": "USD",
                "country": "Rwanda",
            },
            {
                "order_id": "ORD-002",
                "customer_id": "CUST-02",
                "order_date": "2026-01-11",
                "product_category": "apparel",
                "quantity": 1,
                "unit_price": 25.00,
                "currency": "EUR",
                "country": "Kenya",
            },
        ]
    )


@pytest.fixture
def messy_df() -> pd.DataFrame:
    """One row per failure mode. Every row here must be handled, not crashed on.

    Add a row whenever you find a new real-world mess. The suite grows with
    your understanding of the data, which is the whole point.
    """
    return pd.DataFrame(
        [
            # valid control row: must SURVIVE
            {
                "order_id": "ORD-001",
                "customer_id": "C1",
                "order_date": "2026-01-10",
                "product_category": "electronics",
                "quantity": 2,
                "unit_price": 99.99,
                "currency": "USD",
                "country": "Rwanda",
            },
            # currency symbol: cleaning must coerce, NOT quarantine
            {
                "order_id": "ORD-002",
                "customer_id": "C2",
                "order_date": "2026-01-10",
                "product_category": "apparel",
                "quantity": 1,
                "unit_price": "$25.50",
                "currency": "usd",
                "country": " rwanda ",
            },
            # null order_id: QUARANTINE
            {
                "order_id": None,
                "customer_id": "C3",
                "order_date": "2026-01-10",
                "product_category": "home",
                "quantity": 1,
                "unit_price": 10.0,
                "currency": "USD",
                "country": "Kenya",
            },
            # duplicate of ORD-001: cleaning must dedupe
            {
                "order_id": "ORD-001",
                "customer_id": "C1",
                "order_date": "2026-01-10",
                "product_category": "electronics",
                "quantity": 2,
                "unit_price": 99.99,
                "currency": "USD",
                "country": "Rwanda",
            },
            # negative quantity: QUARANTINE
            {
                "order_id": "ORD-004",
                "customer_id": "C4",
                "order_date": "2026-01-10",
                "product_category": "toys",
                "quantity": -3,
                "unit_price": 5.0,
                "currency": "USD",
                "country": "Ghana",
            },
            # future date: QUARANTINE
            {
                "order_id": "ORD-005",
                "customer_id": "C5",
                "order_date": "2099-01-01",
                "product_category": "sports",
                "quantity": 1,
                "unit_price": 12.0,
                "currency": "GBP",
                "country": "Germany",
            },
            # unparseable date: coerce to NaT then QUARANTINE
            {
                "order_id": "ORD-006",
                "customer_id": "C6",
                "order_date": "not-a-date",
                "product_category": "grocery",
                "quantity": 1,
                "unit_price": 3.0,
                "currency": "EUR",
                "country": "Kenya",
            },
            # empty country: nullable, must SURVIVE
            {
                "order_id": "ORD-007",
                "customer_id": "C7",
                "order_date": "2026-01-12",
                "product_category": "home",
                "quantity": 4,
                "unit_price": 7.25,
                "currency": "USD",
                "country": "",
            },
        ]
    )


@pytest.fixture
def empty_df() -> pd.DataFrame:
    """Zero rows, right columns. Must not crash anything."""
    return pd.DataFrame(
        columns=[
            "order_id",
            "customer_id",
            "order_date",
            "product_category",
            "quantity",
            "unit_price",
            "currency",
            "country",
        ]
    )


@pytest.fixture
def settings() -> dict:
    from src.utils.constants import load_settings

    return load_settings()


# --- Integration fixtures --------------------------------------------------
# These read connection details from the environment, which CI supplies via
# service containers and you supply locally via docker compose.


@pytest.fixture(scope="session")
def pg_config():
    pytest.importorskip("sqlalchemy")
    if not os.environ.get("POSTGRES_USER"):
        pytest.skip("no Postgres configured; integration tests skipped")
    from src.utils.constants import PostgresConfig

    return PostgresConfig.from_env()


@pytest.fixture(scope="session")
def minio_config():
    if not os.environ.get("MINIO_ROOT_USER"):
        pytest.skip("no MinIO configured; integration tests skipped")
    from src.utils.constants import MinioConfig

    return MinioConfig.from_env()
