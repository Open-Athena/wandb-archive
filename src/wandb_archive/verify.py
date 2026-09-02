"""Read-only archive verification and inspection."""

from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from wandb_archive.model import ARCHIVE_SCHEMA_VERSION, RunManifest
from wandb_archive.storage import Storage


class VerificationError(RuntimeError):
    pass


def _digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def verify_archive(storage: Storage, *, deep: bool = False) -> dict[str, Any]:
    if not storage.exists("archive.json"):
        raise VerificationError("archive.json does not exist")
    index = json.loads(storage.read_bytes("archive.json"))
    if index.get("archive_schema_version") != ARCHIVE_SCHEMA_VERSION:
        raise VerificationError("Unsupported archive schema version")
    errors: list[str] = []
    object_count = 0
    checked_blobs: set[str] = set()
    expected_files: list[tuple[Any, ...]] = []
    with tempfile.TemporaryDirectory(prefix="wandb-archive-verify-") as raw:
        directory = Path(raw)
        catalogs = index.get("catalogs", {})
        catalog_data: dict[str, list[dict[str, Any]]] = {}
        for name in ("runs", "files", "artifacts", "tables", "deletion_candidates"):
            key = catalogs.get(name)
            if not key or not storage.exists(key):
                errors.append(f"missing catalog: {name}")
                continue
            path = directory / f"catalog-{name}.parquet"
            storage.get_file(key, path)
            try:
                pq.read_metadata(path)
                catalog_data[name] = pq.read_table(path).to_pylist()
            except Exception as error:
                errors.append(f"invalid catalog {name}: {error}")

        run_rows = catalog_data.get("runs", [])
        run_paths = [row["run_path"] for row in run_rows]
        if len(run_paths) != len(set(run_paths)):
            errors.append("runs catalog contains duplicate run paths")
        for catalog_name in ("files", "artifacts", "tables"):
            for row in catalog_data.get(catalog_name, []):
                if row["run_path"] not in run_paths:
                    errors.append(f"orphan {catalog_name} row: {row['run_path']}")
        expected_candidates = {
            row["run_path"] for row in run_rows if row["deletion_ready"]
        }
        actual_candidates = {
            row["run_path"] for row in catalog_data.get("deletion_candidates", [])
        }
        if expected_candidates != actual_candidates:
            errors.append("deletion_candidates catalog is inconsistent")

        for row in run_rows:
            manifest_path = row["manifest_path"]
            try:
                manifest = RunManifest.model_validate_json(
                    storage.read_bytes(manifest_path)
                )
            except Exception as error:
                errors.append(f"invalid manifest {manifest_path}: {error}")
                continue
            if manifest.run_path != row["run_path"]:
                errors.append(f"manifest run path mismatch: {manifest_path}")
            latest_path = f"runs/{manifest.run_path}/latest.json"
            if not storage.exists(latest_path):
                errors.append(f"missing latest pointer: {latest_path}")
            else:
                pointer = json.loads(storage.read_bytes(latest_path))
                if pointer.get("manifest_path") != manifest_path:
                    errors.append(f"stale latest pointer: {latest_path}")
                if pointer.get("source_fingerprint") != manifest.source_fingerprint:
                    errors.append(f"latest fingerprint mismatch: {latest_path}")
            for field in (
                "metric_rows",
                "system_metric_rows",
                "histogram_rows",
                "table_rows",
            ):
                if row[field] != getattr(manifest, field):
                    errors.append(
                        f"catalog/manifest {field} mismatch: {manifest.run_path}"
                    )
            for item in manifest.objects:
                object_count += 1
                expected_files.append(
                    (
                        manifest.run_path,
                        item.source_name,
                        item.logical_path,
                        item.kind,
                        item.size,
                        item.sha256,
                        item.media_type,
                        item.blob_path,
                    )
                )
                if item.blob_path in checked_blobs:
                    continue
                checked_blobs.add(item.blob_path)
                if not storage.exists(item.blob_path):
                    errors.append(f"missing blob: {item.blob_path}")
                    continue
                if storage.size(item.blob_path) != item.size:
                    errors.append(f"wrong blob size: {item.blob_path}")
                    continue
                if deep:
                    blob = directory / f"blob-{item.sha256}"
                    storage.get_file(item.blob_path, blob)
                    if _digest(blob) != item.sha256:
                        errors.append(f"wrong blob digest: {item.blob_path}")
                    if item.media_type == "application/vnd.apache.parquet":
                        try:
                            metadata = pq.read_metadata(blob)
                            if (
                                item.row_count is not None
                                and metadata.num_rows != item.row_count
                            ):
                                errors.append(
                                    f"wrong parquet row count: {item.blob_path}"
                                )
                        except Exception as error:
                            errors.append(f"invalid parquet {item.blob_path}: {error}")
        actual_files = [
            (
                row["run_path"],
                row["source_name"],
                row["logical_path"],
                row["kind"],
                row["size"],
                row["sha256"],
                row["media_type"],
                row["blob_path"],
            )
            for row in catalog_data.get("files", [])
        ]
        if sorted(expected_files, key=repr) != sorted(actual_files, key=repr):
            errors.append("files catalog is inconsistent with run manifests")
    report = {
        "ok": not errors,
        "generation": index.get("generation"),
        "run_count": index.get("run_count", 0),
        "object_references": object_count,
        "unique_blobs_checked": len(checked_blobs),
        "deep": deep,
        "errors": errors,
    }
    if errors:
        raise VerificationError(json.dumps(report, indent=2))
    return report


def inspect_run(storage: Storage, run_path: str) -> dict[str, Any]:
    latest_path = f"runs/{run_path.strip('/')}/latest.json"
    if not storage.exists(latest_path):
        raise FileNotFoundError(f"Archived run not found: {run_path}")
    pointer = json.loads(storage.read_bytes(latest_path))
    manifest = RunManifest.model_validate_json(
        storage.read_bytes(pointer["manifest_path"])
    )
    return {
        **pointer,
        "source_state": manifest.source_state,
        "complete_history": manifest.complete_history,
        "contains_live_data": manifest.contains_live_data,
        "policy_complete": manifest.policy_complete,
        "deletion_ready": manifest.deletion_ready,
        "object_count": len(manifest.objects),
        "object_bytes": sum(item.size for item in manifest.objects),
        "metric_rows": manifest.metric_rows,
        "system_metric_rows": manifest.system_metric_rows,
        "histogram_rows": manifest.histogram_rows,
        "table_rows": manifest.table_rows,
        "exclusions": manifest.exclusions,
    }
