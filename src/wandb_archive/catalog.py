"""Merge published runs into versioned Parquet archive catalogs."""

from __future__ import annotations

import datetime as dt
import json
import tempfile
import uuid
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from wandb_archive import __version__
from wandb_archive.config import AppConfig
from wandb_archive.model import ARCHIVE_SCHEMA_VERSION, RunSnapshot
from wandb_archive.publisher import PublishedRun
from wandb_archive.storage import Storage
from wandb_archive.util import canonical_json, pretty_json

RUN_SCHEMA = pa.schema(
    [
        ("run_path", pa.string()),
        ("entity", pa.string()),
        ("project", pa.string()),
        ("run_id", pa.string()),
        ("name", pa.string()),
        ("state", pa.string()),
        ("created_at", pa.string()),
        ("updated_at", pa.string()),
        ("archived_at", pa.string()),
        ("group", pa.string()),
        ("job_type", pa.string()),
        ("sweep_name", pa.string()),
        ("user", pa.string()),
        ("url", pa.string()),
        ("notes", pa.string()),
        ("tags_json", pa.string()),
        ("config_json", pa.string()),
        ("summary_json", pa.string()),
        ("source_fingerprint", pa.string()),
        ("manifest_path", pa.string()),
        ("manifest_url", pa.string()),
        ("complete_history", pa.bool_()),
        ("contains_live_data", pa.bool_()),
        ("policy_complete", pa.bool_()),
        ("deletion_ready", pa.bool_()),
        ("metric_rows", pa.int64()),
        ("system_metric_rows", pa.int64()),
        ("histogram_rows", pa.int64()),
        ("table_rows", pa.int64()),
        ("exclusions_json", pa.string()),
    ]
)
FILE_SCHEMA = pa.schema(
    [
        ("run_path", pa.string()),
        ("source_name", pa.string()),
        ("logical_path", pa.string()),
        ("kind", pa.string()),
        ("size", pa.int64()),
        ("sha256", pa.string()),
        ("media_type", pa.string()),
        ("blob_path", pa.string()),
        ("blob_url", pa.string()),
    ]
)
ARTIFACT_SCHEMA = pa.schema(
    [
        ("run_path", pa.string()),
        ("direction", pa.string()),
        ("artifact_id", pa.string()),
        ("name", pa.string()),
        ("version", pa.string()),
        ("type", pa.string()),
        ("digest", pa.string()),
        ("size", pa.int64()),
        ("aliases_json", pa.string()),
        ("metadata_json", pa.string()),
        ("manifest_json", pa.string()),
    ]
)
TABLE_SCHEMA = pa.schema(
    [
        ("run_path", pa.string()),
        ("source_name", pa.string()),
        ("logical_path", pa.string()),
        ("rows", pa.int64()),
        ("sha256", pa.string()),
        ("blob_path", pa.string()),
        ("blob_url", pa.string()),
    ]
)

SCHEMAS = {
    "runs": RUN_SCHEMA,
    "files": FILE_SCHEMA,
    "artifacts": ARTIFACT_SCHEMA,
    "tables": TABLE_SCHEMA,
    "deletion_candidates": RUN_SCHEMA,
}


def read_catalog(storage: Storage, name: str) -> list[dict[str, Any]]:
    """Read one catalog from the generation referenced by archive.json."""

    if not storage.exists("archive.json"):
        return []
    index = json.loads(storage.read_bytes("archive.json"))
    if index.get("archive_schema_version") != ARCHIVE_SCHEMA_VERSION:
        raise RuntimeError("Cannot read an unsupported archive schema")
    key = index.get("catalogs", {}).get(name)
    if not key:
        return []
    with tempfile.TemporaryDirectory(prefix="wandb-archive-catalog-read-") as raw:
        path = Path(raw) / f"{name}.parquet"
        storage.get_file(key, path)
        return pq.read_table(path).to_pylist()


class CatalogPublisher:
    def __init__(self, config: AppConfig, storage: Storage) -> None:
        self.config = config
        self.storage = storage

    def update(
        self,
        runs: list[tuple[RunSnapshot, PublishedRun]],
        *,
        resolved_config: bytes | None = None,
    ) -> dict[str, Any]:
        existing = self._read_existing()
        changed_paths = {snapshot.path for snapshot, _ in runs}
        rows = {
            name: [
                row
                for row in existing.get(name, [])
                if row.get("run_path") not in changed_paths
            ]
            for name in ("runs", "files", "artifacts", "tables")
        }
        for snapshot, published in runs:
            self._append(rows, snapshot, published)
        rows["runs"].sort(key=lambda row: row["run_path"])
        for name in ("files", "artifacts", "tables"):
            rows[name].sort(
                key=lambda row: (
                    row["run_path"],
                    row.get("logical_path", row.get("name", "")),
                )
            )
        rows["deletion_candidates"] = [
            row for row in rows["runs"] if row["deletion_ready"]
        ]

        generation = (
            dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
            + "-"
            + uuid.uuid4().hex[:8]
        )
        paths: dict[str, str] = {}
        with tempfile.TemporaryDirectory(prefix="wandb-archive-catalog-") as raw:
            directory = Path(raw)
            for name, schema in SCHEMAS.items():
                path = directory / f"{name}.parquet"
                pq.write_table(pa.Table.from_pylist(rows[name], schema=schema), path)
                key = f"catalogs/{generation}/{path.name}"
                self.storage.put_file_if_missing(
                    path, key, "application/vnd.apache.parquet"
                )
                paths[name] = key

        if resolved_config is not None:
            config_path = f"operations/{generation}/config.yaml"
            self.storage.write_bytes(resolved_config, config_path, "application/yaml")
        else:
            config_path = None
        index = {
            "archive_schema_version": ARCHIVE_SCHEMA_VERSION,
            "exporter_version": __version__,
            "updated_at": dt.datetime.now(dt.UTC).isoformat(),
            "generation": generation,
            "catalogs": paths,
            "resolved_config_path": config_path,
            "run_count": len(rows["runs"]),
            "deletion_candidate_count": len(rows["deletion_candidates"]),
        }
        self.storage.write_bytes(
            pretty_json(index).encode(), "archive.json", "application/json"
        )
        return index

    def _read_existing(self) -> dict[str, list[dict[str, Any]]]:
        if not self.storage.exists("archive.json"):
            return {}
        return {
            name: read_catalog(self.storage, name)
            for name in ("runs", "files", "artifacts", "tables")
        }

    def _append(
        self,
        rows: dict[str, list[dict[str, Any]]],
        snapshot: RunSnapshot,
        published: PublishedRun,
    ) -> None:
        manifest = published.manifest
        encode = lambda value: canonical_json(value).decode()  # noqa: E731
        excluded_sources = {item.get("source") for item in manifest.exclusions}
        metadata_excluded = "metadata.json" in excluded_sources
        rows["runs"].append(
            {
                "run_path": snapshot.path,
                "entity": snapshot.entity,
                "project": snapshot.project,
                "run_id": snapshot.run_id,
                "name": None if metadata_excluded else snapshot.name,
                "state": snapshot.state,
                "created_at": snapshot.created_at,
                "updated_at": snapshot.updated_at,
                "archived_at": manifest.archived_at,
                "group": None if metadata_excluded else snapshot.group,
                "job_type": None if metadata_excluded else snapshot.job_type,
                "sweep_name": None if metadata_excluded else snapshot.sweep_name,
                "user": None if metadata_excluded else snapshot.user,
                "url": None if metadata_excluded else snapshot.url,
                "notes": None if metadata_excluded else snapshot.notes,
                "tags_json": encode([] if metadata_excluded else snapshot.tags),
                "config_json": encode(
                    {} if "config.json" in excluded_sources else snapshot.config
                ),
                "summary_json": encode(
                    {} if "summary.json" in excluded_sources else snapshot.summary
                ),
                "source_fingerprint": snapshot.source_fingerprint,
                "manifest_path": published.manifest_path,
                "manifest_url": self.storage.public_url(published.manifest_path),
                "complete_history": manifest.complete_history,
                "contains_live_data": manifest.contains_live_data,
                "policy_complete": manifest.policy_complete,
                "deletion_ready": manifest.deletion_ready,
                "metric_rows": manifest.metric_rows,
                "system_metric_rows": manifest.system_metric_rows,
                "histogram_rows": manifest.histogram_rows,
                "table_rows": manifest.table_rows,
                "exclusions_json": encode(manifest.exclusions),
            }
        )
        for item in manifest.objects:
            rows["files"].append(
                {
                    "run_path": snapshot.path,
                    "source_name": item.source_name,
                    "logical_path": item.logical_path,
                    "kind": item.kind,
                    "size": item.size,
                    "sha256": item.sha256,
                    "media_type": item.media_type,
                    "blob_path": item.blob_path,
                    "blob_url": self.storage.public_url(item.blob_path),
                }
            )
            if item.kind == "table":
                rows["tables"].append(
                    {
                        "run_path": snapshot.path,
                        "source_name": item.source_name,
                        "logical_path": item.logical_path,
                        "rows": item.row_count,
                        "sha256": item.sha256,
                        "blob_path": item.blob_path,
                        "blob_url": self.storage.public_url(item.blob_path),
                    }
                )
        artifacts = [] if "artifacts.json" in excluded_sources else snapshot.artifacts
        for artifact in artifacts:
            rows["artifacts"].append(
                {
                    "run_path": snapshot.path,
                    "direction": artifact.direction,
                    "artifact_id": artifact.id,
                    "name": artifact.name,
                    "version": artifact.version,
                    "type": artifact.type,
                    "digest": artifact.digest,
                    "size": artifact.size,
                    "aliases_json": encode(artifact.aliases),
                    "metadata_json": encode(artifact.metadata),
                    "manifest_json": encode(artifact.manifest),
                }
            )
