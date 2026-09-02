"""Conservative publication policy for public archives."""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from wandb_archive.config import ContentPolicy, SecurityConfig

_SAFE_SUFFIXES = {
    ".csv",
    ".gif",
    ".html",
    ".jpeg",
    ".jpg",
    ".json",
    ".md",
    ".mp4",
    ".npy",
    ".npz",
    ".ogg",
    ".onnx",
    ".parquet",
    ".pdf",
    ".png",
    ".pt",
    ".pth",
    ".safetensors",
    ".svg",
    ".table.json",
    ".txt",
    ".webm",
    ".yaml",
    ".yml",
    ".zarray",
    ".zattrs",
    ".zgroup",
    ".zmetadata",
}
_TEXT_SUFFIXES = {".csv", ".html", ".json", ".md", ".svg", ".txt", ".yaml", ".yml"}
_SECRET_PATTERNS = {
    "AWS access key": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "private key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "credential assignment": re.compile(
        r"(?i)\b(?:api[_-]?key|access[_-]?key|secret|token|password)\b"
        r"['\"]?\s*[:=]\s*['\"]?[A-Za-z0-9/+_.=-]{16,}"
    ),
    "signed URL": re.compile(
        r"(?i)[?&](?:X-Amz-(?:Credential|Signature)|GoogleAccessId|Signature)="
    ),
}
_PRIVATE_METADATA_KEYS = {
    "args",
    "email",
    "executable",
    "host",
    "hostname",
    "program",
    "root",
    "username",
}


def is_code_path(name: str) -> bool:
    normalized = name.replace("\\", "/").lower()
    return normalized.startswith("code/") or normalized.endswith(
        (".py", ".ipynb", ".sh")
    )


def is_console_path(name: str) -> bool:
    normalized = name.replace("\\", "/").lower()
    return normalized in {"output.log", "console.log"} or normalized.endswith(".log")


def is_media_path(name: str) -> bool:
    return name.replace("\\", "/").lower().startswith("media/")


def is_table_path(name: str) -> bool:
    normalized = name.replace("\\", "/").lower()
    return "/table/" in f"/{normalized}" or normalized.endswith(".table.json")


def is_private_metadata_path(name: str) -> bool:
    return name.replace("\\", "/").lower().endswith("wandb-metadata.json")


def is_zarr_path(name: str) -> bool:
    normalized = name.replace("\\", "/").lower()
    if ".zarr/" not in normalized:
        return False
    leaf = normalized.rsplit("/", 1)[-1]
    return (
        leaf.startswith(".")
        or leaf == "zarr.json"
        or bool(re.fullmatch(r"(?:c/)?\d+(?:[./]\d+)*", leaf))
    )


def safe_file(name: str) -> bool:
    lower = name.lower()
    return is_zarr_path(name) or any(
        lower.endswith(suffix) for suffix in _SAFE_SUFFIXES
    )


def include_file(policy: ContentPolicy, name: str) -> tuple[bool, str | None]:
    if policy in {"none", "metadata"}:
        return False, f"content policy is {policy}"
    if policy == "safe" and not safe_file(name):
        return False, "file type is not allowed by the safe policy"
    return True, None


def scan_file(path: Path) -> list[str]:
    if path.suffix.lower() == ".parquet":
        return _scan_parquet(path)
    if path.suffix.lower() not in _TEXT_SUFFIXES and not path.name.lower().endswith(
        ".table.json"
    ):
        return []
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return ["text-like file is not valid UTF-8"]
    return [name for name, pattern in _SECRET_PATTERNS.items() if pattern.search(text)]


def _strings(value: Any) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for key, item in value.items():
            yield str(key)
            yield from _strings(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _strings(item)


def _scan_parquet(path: Path) -> list[str]:
    findings: set[str] = set()
    parquet = pq.ParquetFile(path)
    columns = [
        field.name
        for field in parquet.schema_arrow
        if pa.types.is_string(field.type)
        or pa.types.is_large_string(field.type)
        or pa.types.is_list(field.type)
        or pa.types.is_struct(field.type)
    ]
    if not columns:
        return []
    for batch in parquet.iter_batches(columns=columns, batch_size=4096):
        for row in batch.to_pylist():
            for text in _strings(row):
                findings.update(
                    name
                    for name, pattern in _SECRET_PATTERNS.items()
                    if pattern.search(text)
                )
    return sorted(findings)


def sanitize_metadata(value: Any, security: SecurityConfig) -> Any:
    """Remove identifying machine metadata in the public-safe profile."""

    if security.profile != "public-safe":
        return value
    if isinstance(value, dict):
        return {
            key: sanitize_metadata(item, security)
            for key, item in value.items()
            if key.lower() not in _PRIVATE_METADATA_KEYS
        }
    if isinstance(value, list):
        return [sanitize_metadata(item, security) for item in value]
    return value
