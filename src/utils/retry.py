"""Retry with exponential backoff.

Written by hand rather than importing tenacity straight away, because you
should understand what the library is doing before you delegate it.

Exponential backoff: wait 2s, then 4s, then 8s. Retrying instantly hammers a
service that is already struggling. Jitter (a small random offset) stops many
clients retrying in lockstep and re-creating the same spike.
"""

from __future__ import annotations

import functools
import random
import time
from typing import Callable, TypeVar

from src.utils.errors import TransientError

T = TypeVar("T")


def retry_on_transient(
    max_attempts: int = 3,
    base_delay: float = 2.0,
    max_delay: float = 30.0,
    jitter: bool = True,
) -> Callable:
    """Retry the wrapped function on TransientError only.

    PermanentError is deliberately NOT caught: it propagates immediately.
    That distinction is the whole reason this decorator exists.

    Usage:
        @retry_on_transient(max_attempts=3)
        def download(...): ...
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # Imported here to avoid a circular import at module load time.
            from src.monitoring.logging import get_logger

            log = get_logger(__name__)
            last_error: Exception | None = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except TransientError as exc:
                    last_error = exc
                    if attempt == max_attempts:
                        break
                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    if jitter:
                        delay += random.uniform(0, delay * 0.1)
                    log.warning(
                        "transient_failure_retrying",
                        extra={
                            "function": func.__name__,
                            "attempt": attempt,
                            "max_attempts": max_attempts,
                            "sleep_seconds": round(delay, 2),
                            "error": str(exc),
                        },
                    )
                    time.sleep(delay)

            raise TransientError(
                f"{func.__name__} failed after {max_attempts} attempts: {last_error}"
            ) from last_error

        return wrapper

    return decorator
