"""Cleaning: PURE functions only. DataFrame in, DataFrame out.

PURE FUNCTION = same input always gives the same output, and it changes
nothing outside itself. No network. No database. No reading files. No clock.

That constraint is not fussiness. It is what lets tests/unit/ run in
milliseconds on a CI runner with no Docker at all. Every line of logic you
put in the DAG instead of here costs you a slow, painful test later.
"""

from __future__ import annotations

import re

import pandas as pd

# Matches anything that is not a digit, a dot, a comma or a minus sign, e.g.
# the '$' in '$12.50' or a stray space. Stripped before we try to parse.
_PRICE_JUNK = re.compile(r"[^0-9.,\-]")


def normalise_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase, strip whitespace, replace spaces/hyphens with underscores.

    Why: sources send 'Order ID', 'order-id', ' order_id '. Downstream code
    should never have to care.

    Returns a NEW DataFrame. Never mutate the input (use df.copy()).
    """
    out = df.copy()
    out.columns = [re.sub(r"[\s\-]+", "_", str(c).strip().lower()) for c in out.columns]
    return out


def trim_whitespace(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    """Strip leading/trailing whitespace from string columns.

    If columns is None, apply to every object-dtype column.
    Watch out: ' ' (a lone space) becomes '' here, and coerce_types is the
    one responsible for turning that '' into a proper null.
    """
    out = df.copy()
    cols = (
        columns
        if columns is not None
        else list(out.select_dtypes(include="object").columns)
    )
    for col in cols:
        if col not in out.columns:
            continue
        out[col] = out[col].apply(lambda v: v.strip() if isinstance(v, str) else v)
    return out


def standardise_text_case(
    df: pd.DataFrame, column_rules: dict[str, str]
) -> pd.DataFrame:
    """Apply case rules, e.g. {'currency': 'upper', 'country': 'title'}.

    Why: 'rwanda', 'RWANDA' and 'Rwanda' must not become three countries in
    your GROUP BY. Inconsistent casing is the quietest way to corrupt a KPI.
    """
    out = df.copy()
    casers = {
        "upper": str.upper,
        "lower": str.lower,
        "title": str.title,
    }
    for col, rule in column_rules.items():
        if col not in out.columns:
            continue
        caser = casers[rule]
        out[col] = out[col].apply(lambda v: caser(v) if isinstance(v, str) else v)
    return out


def _parse_price(value) -> float | None:
    """Turn '$12.50', '12,50' or 25.5 into a float. Unparseable -> None."""
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = _PRICE_JUNK.sub("", str(value).strip())
    if text == "":
        return None
    if "," in text and "." in text:
        # '1,234.50' - comma is a thousands separator, drop it.
        text = text.replace(",", "")
    elif "," in text:
        # '25,50' - comma is a decimal separator here.
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to their intended types, turning failures into NaN/NaT.

    Handle these real-world messes:
      - unit_price arriving as '$12.50' or '12,50' -> 12.50
      - quantity arriving as '3' or '3.0' -> 3
      - order_date in mixed formats -> datetime64, unparseable -> NaT
      - empty strings -> NA, not '' (pd.NA)

    IMPORTANT: does NOT drop bad rows. Coerces to null and lets
    validation.py decide whether that null is fatal or quarantinable.
    Cleaning decides SHAPE. Validation decides ACCEPTABILITY. Keep them apart.
    """
    out = df.copy()

    # Generic: a bare empty string is not meaningfully different from "missing".
    for col in out.select_dtypes(include="object").columns:
        out[col] = out[col].replace("", pd.NA)

    if "unit_price" in out.columns:
        out["unit_price"] = out["unit_price"].apply(_parse_price)

    if "quantity" in out.columns:
        out["quantity"] = pd.to_numeric(out["quantity"], errors="coerce")

    if "order_date" in out.columns:
        out["order_date"] = pd.to_datetime(out["order_date"], errors="coerce")

    return out


def deduplicate(
    df: pd.DataFrame, key: str = "order_id", keep: str = "last"
) -> pd.DataFrame:
    """Drop duplicate rows on `key`.

    Design decision: when the same order_id appears twice with DIFFERENT
    values, `keep='last'` assumes later-in-file means more recently
    corrected (e.g. a re-sent, fixed record further down the same export).
    That is an assumption about the source system's export order, not a
    universal truth - state it here so nobody rediscovers it by accident.

    A null key is not "the same missing order" repeated: each null-key row
    is its own distinct problem record, so nulls are left untouched here and
    handled by validation's order_id_not_null check instead.
    """
    out = df.copy()
    is_null = out[key].isna()
    deduped = out[~is_null].drop_duplicates(subset=[key], keep=keep)
    return pd.concat([deduped, out[is_null]]).sort_index()


def add_lineage_columns(
    df: pd.DataFrame, source_file: str, ingested_at
) -> pd.DataFrame:
    """Attach source_file and ingested_at.

    LINEAGE = being able to answer 'where did this row come from?'. Without
    source_file, debugging a wrong number means re-reading every file in the
    bucket. This one column saves hours. ingested_at is a parameter rather
    than datetime.now() precisely so this function stays pure and testable.
    """
    out = df.copy()
    out["source_file"] = source_file
    out["ingested_at"] = ingested_at
    return out


def clean(df: pd.DataFrame, source_file: str, ingested_at) -> pd.DataFrame:
    """The full cleaning pipeline, composed of the functions above, in order.

    This is the only function the DAG calls.
    """
    out = normalise_column_names(df)
    out = trim_whitespace(out)
    out = standardise_text_case(out, {"currency": "upper", "country": "title"})
    out = coerce_types(out)
    out = deduplicate(out, key="order_id", keep="last")
    out = add_lineage_columns(out, source_file=source_file, ingested_at=ingested_at)
    return out
