"""Data quality tests: run the real expectations against the messy sample.

These prove your validation actually CATCHES things. A validation suite that
only ever sees clean data is a green tick that means nothing.
"""

from __future__ import annotations

import pytest

from src.transformations import validation

pytestmark = pytest.mark.data_quality


def test_valid_rows_survive(messy_df, settings):
    result = validation.validate(messy_df, settings)
    assert "ORD-001" in set(result.valid["order_id"])


@pytest.mark.parametrize(
    "order_id,reason",
    [
        ("ORD-004", "quantity_positive"),
        ("ORD-005", "order_date_not_future"),
        ("ORD-006", "order_date_not_future"),
    ],
)
def test_each_bad_row_is_rejected_for_the_right_reason(
    messy_df, settings, order_id, reason
):
    """Not just 'it was rejected'. Rejected for the REASON you expect.

    A row rejected by accident, for the wrong check, is a bug that a weaker
    assertion would hide.
    """
    result = validation.validate(messy_df, settings)
    rejected = result.rejected.set_index("order_id")
    assert order_id in rejected.index
    assert reason in rejected.loc[order_id, "rejection_reason"]


def test_one_bad_row_does_not_reject_the_whole_file(messy_df, settings):
    """Standing rule: one bad record must never kill a pipeline run."""
    result = validation.validate(messy_df, settings)
    assert len(result.valid) > 0
    assert result.rejection_rate < 1.0


def test_missing_required_column_is_fatal(messy_df):
    """FATAL, not quarantine: there is no sensible way to continue."""
    from src.utils.constants import REQUIRED_COLUMNS
    from src.utils.errors import ValidationError

    broken = messy_df.drop(columns=["order_id"])
    with pytest.raises(ValidationError):
        validation.check_required_columns(broken, REQUIRED_COLUMNS)


def test_volume_drop_is_flagged():
    """A check CI structurally CANNOT do: it needs yesterday's real numbers."""
    assert validation.check_volume_drop(
        current_rows=100, previous_rows=10_000, threshold=0.2
    )
    assert not validation.check_volume_drop(
        current_rows=9_500, previous_rows=10_000, threshold=0.2
    )
