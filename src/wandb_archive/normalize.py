"""Convert W&B exports into stable, query-oriented Parquet tables."""

from __future__ import annotations

import datetime as dt
import json
import math
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from wandb_archive.model import RunSnapshot
from wandb_archive.util import json_default, json_safe

METRIC_SCHEMA = pa.schema(
    [
        ("entity", pa.string()),
        ("project", pa.string()),
        ("run_id", pa.string()),
        ("step", pa.int64()),
        ("timestamp", pa.float64()),
        ("metric", pa.string()),
        ("value", pa.float64()),
    ]
)
HISTOGRAM_SCHEMA = pa.schema(
    [
        ("entity", pa.string()),
        ("project", pa.string()),
        ("run_id", pa.string()),
        ("step", pa.int64()),
        ("timestamp", pa.float64()),
        ("metric", pa.string()),
        ("bins", pa.list_(pa.float64())),
        ("counts", pa.list_(pa.float64())),
    ]
)


def _timestamp(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, dt.datetime):
        return value.timestamp()
    try:
        return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _step(row: dict[str, Any]) -> int | None:
    value = row.get("_step", row.get("step"))
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _histogram(value: Any) -> tuple[list[float], list[float]] | None:
    if not isinstance(value, dict):
        return None
    kind = str(value.get("_type", "")).lower()
    if "histogram" not in kind and not ({"bins", "values"} <= value.keys()):
        return None
    bins = value.get("bins")
    counts = value.get("values", value.get("counts"))
    if not isinstance(bins, list) or not isinstance(counts, list):
        return None
    try:
        return [float(item) for item in bins], [float(item) for item in counts]
    except (TypeError, ValueError):
        return None


def _rows(path: Path) -> Iterator[dict[str, Any]]:
    for batch in pq.ParquetFile(path).iter_batches(batch_size=4096):
        for row in batch.to_pylist():
            yield dict(row)


def _is_system_history(path: Path) -> bool:
    name = path.name.lower()
    return "event" in name or "system" in name


def normalize_histories(
    raw_paths: list[Path], snapshot: RunSnapshot, output_dir: Path
) -> tuple[Path, Path, Path, int, int, int]:
    """Write scalar, system, and histogram tables from native history exports."""

    output_dir.mkdir(parents=True, exist_ok=True)
    metric_path = output_dir / "metrics.parquet"
    system_path = output_dir / "system_metrics.parquet"
    histogram_path = output_dir / "histograms.parquet"
    buffers: dict[str, list[dict[str, Any]]] = {
        "metrics": [],
        "system": [],
        "histograms": [],
    }
    paths = {
        "metrics": metric_path,
        "system": system_path,
        "histograms": histogram_path,
    }
    schemas = {
        "metrics": METRIC_SCHEMA,
        "system": METRIC_SCHEMA,
        "histograms": HISTOGRAM_SCHEMA,
    }
    writers = {
        name: pq.ParquetWriter(path, schemas[name], compression="zstd")
        for name, path in paths.items()
    }
    counts = {name: 0 for name in buffers}

    def flush(name: str) -> None:
        buffer = buffers[name]
        if buffer:
            table = pa.Table.from_pylist(buffer, schema=schemas[name])
            writers[name].write_table(table)
            counts[name] += len(buffer)
            buffer.clear()

    def append(name: str, row: dict[str, Any]) -> None:
        buffers[name].append(row)
        if len(buffers[name]) >= 50_000:
            flush(name)

    identity = {
        "entity": snapshot.entity,
        "project": snapshot.project,
        "run_id": snapshot.run_id,
    }
    try:
        for path in raw_paths:
            destination = "system" if _is_system_history(path) else "metrics"
            for row in _rows(path):
                step = _step(row)
                timestamp = _timestamp(row.get("_timestamp", row.get("timestamp")))
                for key, value in row.items():
                    if key.startswith("_") or key in {"step", "timestamp"}:
                        continue
                    histogram = _histogram(value)
                    if histogram is not None:
                        bins, histogram_counts = histogram
                        append(
                            "histograms",
                            {
                                **identity,
                                "step": step,
                                "timestamp": timestamp,
                                "metric": key,
                                "bins": bins,
                                "counts": histogram_counts,
                            },
                        )
                    elif isinstance(value, (int, float)) and not isinstance(
                        value, bool
                    ):
                        numeric = float(value)
                        if math.isfinite(numeric):
                            append(
                                destination,
                                {
                                    **identity,
                                    "step": step,
                                    "timestamp": timestamp,
                                    "metric": key,
                                    "value": numeric,
                                },
                            )
        for name in buffers:
            flush(name)
            if counts[name] == 0:
                table = pa.Table.from_pylist([], schema=schemas[name])
                writers[name].write_table(table)
    finally:
        for writer in writers.values():
            writer.close()
    return (
        metric_path,
        system_path,
        histogram_path,
        counts["metrics"],
        counts["system"],
        counts["histograms"],
    )


def _column(values: list[Any]) -> pa.Array:
    non_null = [value for value in values if value is not None]
    if not non_null:
        return pa.array(values, type=pa.string())
    if all(isinstance(value, bool) for value in non_null):
        return pa.array(values, type=pa.bool_())
    if all(
        isinstance(value, (int, float)) and not isinstance(value, bool)
        for value in non_null
    ):
        return pa.array(values, type=pa.float64())
    if all(isinstance(value, str) for value in non_null):
        return pa.array(values, type=pa.string())
    encoded = [
        None
        if value is None
        else json.dumps(
            json_safe(value), default=json_default, sort_keys=True, allow_nan=False
        )
        for value in values
    ]
    return pa.array(encoded, type=pa.string())


def table_json_to_parquet(source: Path, destination: Path) -> int:
    """Convert the JSON representation used by W&B Tables to Parquet."""

    payload = json.loads(source.read_text(encoding="utf-8"))
    columns = payload.get("columns")
    data = payload.get("data")
    if not isinstance(columns, list) or not isinstance(data, list):
        raise ValueError(f"Not a W&B Table JSON file: {source}")
    names = [str(name) for name in columns]
    if len(names) != len(set(names)):
        raise ValueError(f"W&B Table contains duplicate column names: {source}")
    arrays = []
    for index in range(len(names)):
        values = [row[index] if index < len(row) else None for row in data]
        arrays.append(_column(values))
    destination.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_arrays(arrays, names=names), destination)
    return len(data)
