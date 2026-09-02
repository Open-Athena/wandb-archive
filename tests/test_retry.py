import pytest

from wandb_archive.config import ApiRetryConfig
from wandb_archive.retry import call_with_retry


def test_transient_error_uses_exponential_full_jitter() -> None:
    attempts = 0
    delays = []

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError(
                "Failed to execute API request: the service process is busy "
                "and did not respond in time"
            )
        return "complete"

    result = call_with_retry(
        operation,
        ApiRetryConfig(attempts=4),
        "test operation",
        sleep=delays.append,
        random_value=lambda: 0.5,
    )

    assert result == "complete"
    assert attempts == 3
    assert delays == [0.5, 1.0]


def test_non_transient_error_is_not_retried() -> None:
    attempts = 0

    def operation() -> None:
        nonlocal attempts
        attempts += 1
        raise ValueError("invalid query")

    with pytest.raises(ValueError, match="invalid query"):
        call_with_retry(operation, ApiRetryConfig(), "test operation")
    assert attempts == 1
