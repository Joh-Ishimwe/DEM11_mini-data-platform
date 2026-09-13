"""Tests for the retry decorator. This one is NOT skipped: retry.py is written.

It is here so you have at least one genuinely passing test from day one, and
so you can see what a real test of error-handling behaviour looks like.
"""

from __future__ import annotations

import pytest

from src.utils.errors import PermanentError, TransientError
from src.utils.retry import retry_on_transient

pytestmark = pytest.mark.unit


def test_succeeds_without_retrying():
    calls = []

    @retry_on_transient(max_attempts=3, base_delay=0)
    def ok():
        calls.append(1)
        return "done"

    assert ok() == "done"
    assert len(calls) == 1


def test_retries_transient_then_succeeds():
    calls = []

    @retry_on_transient(max_attempts=3, base_delay=0)
    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise TransientError("server having a moment")
        return "recovered"

    assert flaky() == "recovered"
    assert len(calls) == 3


def test_permanent_error_is_not_retried():
    """THE point of the whole error hierarchy. Retrying a 404 wastes ten
    minutes and hides the real cause."""
    calls = []

    @retry_on_transient(max_attempts=5, base_delay=0)
    def missing():
        calls.append(1)
        raise PermanentError("404 no such key")

    with pytest.raises(PermanentError):
        missing()
    assert len(calls) == 1


def test_gives_up_after_max_attempts():
    calls = []

    @retry_on_transient(max_attempts=3, base_delay=0)
    def always_down():
        calls.append(1)
        raise TransientError("connection refused")

    with pytest.raises(TransientError):
        always_down()
    assert len(calls) == 3
