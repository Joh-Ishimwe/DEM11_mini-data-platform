"""Validation: is this data acceptable?

Two kinds of check:

    FATAL      -> stop the entire run. e.g. a required column is missing.
    QUARANTINE -> drop that one row into staging.rejected_rows with a
                  reason, and carry on. e.g. one row has a negative quantity.

Too fatal and a single typo kills the nightly load. Too lenient and you ship
half a dataset and nobody notices until the report is wrong. The checks
themselves live in config/settings.yaml so the choice is visible, not buried
in code - see ADR-009 in docs/decisions.md for the reasoning per check.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

import pandas as pd

from src.utils.errors import ValidationError


@dataclass
class ValidationResult:
    """What came out of validation: the good rows, and why the rest were not."""

    valid: pd.DataFrame
    rejected: pd.DataFrame  # includes a rejection_reason column
    reasons: dict[str, int] = field(default_factory=dict)  # reason -> count

    @property
    def rejection_rate(self) -> float:
        total = len(self.valid) + len(self.rejected)
        return len(self.rejected) / total if total else 0.0


def check_required_columns(df: pd.DataFrame, required: tuple[str, ...]) -> None:
    """FATAL. Raise ValidationError listing every missing column.

    List them ALL in one message. Raising on the first one means the engineer
    fixes it, re-runs, waits, and discovers the next one. Respect their time.
    """
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValidationError(f"missing required column(s): {', '.join(missing)}")


def build_schema():
    """Return a pandera DataFrameSchema describing the expected shape.

    SCHEMA = the agreed structure: which columns, what types, nullable or not.
    Schema testing is the cheapest high-value check in data engineering,
    because a column silently changing type is the most common cause of a
    pipeline producing wrong-but-plausible numbers.

    Nullability here mirrors the QUARANTINE checks, not the final table:
    a null order_id is still a schema-valid *string column entry* (None),
    it is validate() that decides it must be rejected.

    pandera is imported here, not at module level: nothing on the real
    pipeline path calls this function (validate() does its own checks
    directly), and pandera drags in a pydantic/typing_extensions combination
    that conflicts with what Airflow itself pins. Keeping the import lazy
    means the Airflow image doesn't need pandera installed at all - see
    docker/airflow/Dockerfile.
    """
    import pandera as pa

    return pa.DataFrameSchema(
        {
            "order_id": pa.Column(str, nullable=True),
            "customer_id": pa.Column(str, nullable=True),
            "order_date": pa.Column("datetime64[ns]", nullable=True),
            "product_category": pa.Column(str, nullable=True),
            "quantity": pa.Column(nullable=True),
            "unit_price": pa.Column(nullable=True),
            "currency": pa.Column(str, nullable=True),
            "country": pa.Column(str, nullable=True),
        },
        coerce=False,
        strict=False,
    )


def validate(
    df: pd.DataFrame, settings: dict, now: pd.Timestamp | None = None
) -> ValidationResult:
    """Run every quarantine check, splitting the frame into valid and rejected.

    Self-contained: coerces types itself via cleaning.coerce_types, so it
    works whether the caller already ran cleaning.clean() or not (harmless
    either way, since coerce_types is idempotent).

    A row failing more than one check is reported once, with every reason
    joined into a single rejection_reason string. Never raises - that's the
    whole point of "quarantine" - and `rejected` carries the original columns
    plus that reason, ready to write to staging.rejected_rows as JSONB.
    """
    # Local import: avoids a hard import-time dependency loop and keeps
    # cleaning.py (pure, no validation concerns) independent of this module.
    from src.transformations.cleaning import coerce_types

    working = coerce_types(df).reset_index(drop=True)
    now = now or pd.Timestamp.now()

    cfg = settings.get("validation", {})
    checks = set(cfg.get("quarantine_checks", []))
    known_categories = set(cfg.get("known_categories", []))

    reasons_by_row: dict[int, list[str]] = {}

    def flag(mask: pd.Series, reason: str) -> None:
        for idx in working.index[mask.fillna(False)]:
            reasons_by_row.setdefault(idx, []).append(reason)

    if "order_id_not_null" in checks and "order_id" in working.columns:
        flag(working["order_id"].isna(), "order_id_not_null")

    if "customer_id_not_null" in checks and "customer_id" in working.columns:
        flag(working["customer_id"].isna(), "customer_id_not_null")

    if "order_id_unique" in checks and "order_id" in working.columns:
        is_dup = working["order_id"].notna() & working.duplicated(
            subset=["order_id"], keep="first"
        )
        flag(is_dup, "order_id_unique")

    if "quantity_positive" in checks and "quantity" in working.columns:
        qty = pd.to_numeric(working["quantity"], errors="coerce")
        # A missing/unparseable quantity is treated the same as a bad one: we
        # cannot confirm it is positive, and REQUIRED_COLUMNS makes it NOT
        # NULL downstream, so it must be quarantined rather than crash the load.
        flag(qty.isna() | (qty <= 0), "quantity_positive")

    if "unit_price_non_negative" in checks and "unit_price" in working.columns:
        price = pd.to_numeric(working["unit_price"], errors="coerce")
        flag(price.isna() | (price < 0), "unit_price_non_negative")

    if "order_date_not_future" in checks and "order_date" in working.columns:
        order_date = working["order_date"]
        # A missing/unparseable date is treated the same as a future one:
        # we cannot prove it is NOT in the future, so it does not pass.
        flag(order_date.isna() | (order_date > now), "order_date_not_future")

    if "product_category_known" in checks and "product_category" in working.columns:
        category = working["product_category"]
        flag(
            category.notna() & ~category.isin(known_categories),
            "product_category_known",
        )

    rejected_idx = list(reasons_by_row.keys())
    valid = working.loc[~working.index.isin(rejected_idx)].reset_index(drop=True)

    rejected = working.loc[working.index.isin(rejected_idx)].copy()
    rejected["rejection_reason"] = [
        ", ".join(reasons_by_row[i]) for i in rejected.index
    ]
    rejected = rejected.reset_index(drop=True)

    reasons_count = Counter(r for reasons in reasons_by_row.values() for r in reasons)

    return ValidationResult(valid=valid, rejected=rejected, reasons=dict(reasons_count))


def check_volume_drop(current_rows: int, previous_rows: int, threshold: float) -> bool:
    """Return True if this run looks suspiciously small versus the last one.

    This is a check CI structurally CANNOT do, because it needs yesterday's
    real numbers. It belongs at runtime.
    """
    if previous_rows <= 0:
        return False
    drop_fraction = 1 - (current_rows / previous_rows)
    return drop_fraction > threshold
