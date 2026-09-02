"""Conservative retries for transient W&B API failures."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable

from wandb_archive.config import ApiRetryConfig

logger = logging.getLogger(__name__)

_TRANSIENT_MESSAGES = (
    "busy",
    "connection",
    "gateway",
    "internal server error",
    "rate limit",
    "temporar",
    "timed out",
    "timeout",
    "too many requests",
    "unavailable",
    "429",
    "502",
    "503",
    "504",
)


def is_transient(error: Exception) -> bool:
    current: BaseException | None = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, (ConnectionError, TimeoutError, OSError)):
            return True
        response = getattr(current, "response", None)
        status = getattr(response, "status_code", None)
        if status == 429 or isinstance(status, int) and status >= 500:
            return True
        message = str(current).lower()
        if any(fragment in message for fragment in _TRANSIENT_MESSAGES):
            return True
        current = current.__cause__ or current.__context__
    return False


def call_with_retry[T](
    operation: Callable[[], T],
    config: ApiRetryConfig,
    description: str,
    *,
    sleep: Callable[[float], None] = time.sleep,
    random_value: Callable[[], float] = random.random,
) -> T:
    """Run an operation with capped exponential backoff and full jitter."""

    for attempt in range(1, config.attempts + 1):
        try:
            return operation()
        except Exception as error:
            if attempt == config.attempts or not is_transient(error):
                raise
            ceiling = min(
                config.maximum_delay_seconds,
                config.initial_delay_seconds * 2 ** (attempt - 1),
            )
            delay = random_value() * ceiling
            logger.warning(
                "W&B API %s failed (attempt %d/%d); retrying in %.1fs: %s",
                description,
                attempt,
                config.attempts,
                delay,
                error,
            )
            sleep(delay)
    raise AssertionError("retry loop did not return or raise")
