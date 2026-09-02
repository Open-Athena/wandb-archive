"""Small deterministic serialization and hashing helpers."""

from __future__ import annotations

import hashlib
import json
import math
import mimetypes
from pathlib import Path
from typing import Any


def json_default(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if hasattr(value, "tolist"):
        return value.tolist()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def json_safe(value: Any) -> Any:
    """Recursively make API values strict-JSON serializable."""

    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, float):
        return value
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    converted = json_default(value)
    if converted is value:
        return value
    return json_safe(converted)


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        json_safe(value),
        default=json_default,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def pretty_json(value: Any) -> str:
    return (
        json.dumps(
            json_safe(value),
            default=json_default,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    )


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def media_type(path: Path, supplied: str | None = None) -> str:
    if supplied:
        return supplied
    if path.suffix == ".parquet":
        return "application/vnd.apache.parquet"
    guessed, _ = mimetypes.guess_type(path.name)
    return guessed or "application/octet-stream"
