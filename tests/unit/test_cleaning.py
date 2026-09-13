"""Unit tests for src/transformations/cleaning.py.

Rule: nothing in this file may touch the network, a database, or the clock.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.transformations import cleaning

pytestmark = pytest.mark.unit


def test_normalise_column_names_handles_spaces_and_case():
    df = pd.DataFrame(columns=["Order ID", " unit-price ", "Country"])
    out = cleaning.normalise_column_names(df)
    assert list(out.columns) == ["order_id", "unit_price", "country"]


def test_cleaning_never_mutates_its_input(clean_df):
    """Purity check. If this fails, some other test will mysteriously fail later."""
    before = clean_df.copy(deep=True)
    cleaning.normalise_column_names(clean_df)
    pd.testing.assert_frame_equal(clean_df, before)


@pytest.mark.parametrize(
    "raw,expected",
    [("$25.50", 25.50), ("25,50", 25.50), ("25.50", 25.50), ("", None), ("abc", None)],
)
def test_coerce_types_handles_dirty_prices(raw, expected):
    """parametrize runs this test once per row. Five cases, one function."""
    df = pd.DataFrame([{"unit_price": raw}])
    out = cleaning.coerce_types(df)
    value = out["unit_price"].iloc[0]
    if expected is None:
        assert pd.isna(value)
    else:
        assert value == pytest.approx(expected)


def test_coerce_types_does_not_drop_bad_rows(messy_df):
    """Cleaning decides SHAPE. Validation decides ACCEPTABILITY. Keep them apart."""
    out = cleaning.coerce_types(messy_df)
    assert len(out) == len(messy_df)


def test_deduplicate_removes_repeated_order_id(messy_df):
    out = cleaning.deduplicate(messy_df, key="order_id")
    non_null = out["order_id"].dropna()
    assert non_null.is_unique


def test_country_casing_is_normalised(messy_df):
    """' rwanda ' and 'Rwanda' must not become two countries in a GROUP BY."""
    out = cleaning.trim_whitespace(messy_df)
    out = cleaning.standardise_text_case(out, {"country": "title"})
    assert set(out["country"].dropna()) >= {"Rwanda"}


def test_clean_on_empty_frame_does_not_crash(empty_df, fixed_now):
    """The empty case is the one everyone forgets, and it happens every holiday."""
    out = cleaning.clean(empty_df, source_file="x.csv", ingested_at=fixed_now)
    assert len(out) == 0
    assert "source_file" in out.columns
