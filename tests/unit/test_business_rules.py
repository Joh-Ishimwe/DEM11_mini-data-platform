"""Unit tests for business rules. Boundaries are where the bugs live."""

from __future__ import annotations

import pandas as pd
import pytest

from src.transformations import business_rules

pytestmark = pytest.mark.unit


def test_revenue_is_quantity_times_price():
    df = pd.DataFrame([{"quantity": 3, "unit_price": 10.50}])
    out = business_rules.calculate_revenue(df)
    assert out["revenue"].iloc[0] == pytest.approx(31.50)


@pytest.mark.parametrize(
    "revenue,bucket",
    [
        (0, "small"),
        (49.99, "small"),
        (50, "medium"),
        (249.99, "medium"),
        (250, "large"),
        (999.99, "large"),
        (1000, "enterprise"),
    ],
)
def test_bucket_boundaries_are_lower_inclusive(revenue, bucket):
    """Pin the boundary. Off-by-one at a bucket edge is a classic silent KPI bug."""
    df = pd.DataFrame([{"revenue": revenue}])
    out = business_rules.bucket_order_size(df)
    assert out["order_size_bucket"].iloc[0] == bucket
