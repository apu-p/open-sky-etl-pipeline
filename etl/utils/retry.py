"""Retry with exponential backoff (Section 7: 3 attempts on extract tasks for
network flakiness). Airflow also retries at the task level; this covers the
within-call transient case so a single blip doesn't burn an Airflow attempt."""
from __future__ import annotations

import functools
import time
from typing import Callable, Iterable, Type, TypeVar

from etl.config import RETRY_ATTEMPTS, RETRY_BACKOFF_BASE_S
from etl.utils.logging import get_logger

log = get_logger("etl.retry")

T = TypeVar("T")


def backoff_delay(attempt: int, base: float = RETRY_BACKOFF_BASE_S) -> float:
    """Seconds to wait before the given (0-indexed) retry attempt."""
    return base ** (attempt + 1)


def retry(
    attempts: int = RETRY_ATTEMPTS,
    base: float = RETRY_BACKOFF_BASE_S,
    exceptions: Iterable[Type[BaseException]] = (Exception,),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Generic retry decorator with exponential backoff."""
    exceptions = tuple(exceptions)

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> T:
            last_exc: BaseException | None = None
            for attempt in range(attempts):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:  # noqa: PERF203
                    last_exc = exc
                    if attempt < attempts - 1:
                        delay = backoff_delay(attempt, base)
                        log.warning(
                            "retrying after failure",
                            extra={"context": {"fn": fn.__name__, "attempt": attempt + 1,
                                                "delay_s": delay, "error": str(exc)}},
                        )
                        time.sleep(delay)
            assert last_exc is not None
            raise last_exc

        return wrapper

    return decorator
