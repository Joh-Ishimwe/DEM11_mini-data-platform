"""Business rules: the logic the BUSINESS owns, not engineering.

Rules here come from analysts and product owners, not from you. Your job is
to encode them faithfully and make the encoding visible enough that they can
check you got it right. Every rule gets a comment naming WHO decided it.
"""

from __future__ import annotations

import pandas as pd

# Decided by: Analytics team. Revenue is gross, exclusive of tax and shipping.
# Change this only with a written sign-off, and add a test when you do.
ORDER_SIZE_BUCKETS = {
    "small": (0, 50),
    "medium": (50, 250),
    "large": (250, 1000),
    "enterprise": (1000, float("inf")),
}


def calculate_revenue(df: pd.DataFrame) -> pd.DataFrame:
    """revenue = quantity * unit_price, rounded to 2 decimal places.

    Decided by: Analytics team - round PER ROW, not on the sum. Rounding the
    sum instead would make revenue for one order depend on how many other
    orders happen to be in the same batch, which is not a property a single
    order's revenue should ever have. See ADR-010 in docs/decisions.md.
    """
    out = df.copy()
    out["revenue"] = (
        out["quantity"].astype(float) * out["unit_price"].astype(float)
    ).round(2)
    return out


def bucket_order_size(df: pd.DataFrame) -> pd.DataFrame:
    """Add order_size_bucket from ORDER_SIZE_BUCKETS, based on revenue.

    Boundaries are [lower, upper). An order of exactly 50 is 'medium'.
    """
    out = df.copy()

    def _bucket(revenue) -> str | None:
        if pd.isna(revenue):
            return None
        for name, (lower, upper) in ORDER_SIZE_BUCKETS.items():
            if lower <= revenue < upper:
                return name
        return None

    out["order_size_bucket"] = out["revenue"].apply(_bucket)
    return out


def apply_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    """Compose the rules above, in order."""
    out = calculate_revenue(df)
    out = bucket_order_size(out)
    return out
