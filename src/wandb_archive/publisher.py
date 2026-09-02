"""Commit staged run exports into an immutable object archive."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from wandb_archive import __version__
from wandb_archive.config import AppConfig
from wandb_archive.model import (
    TERMINAL_STATES,
    ExportResult,
    ObjectManifest,
    RunManifest,
)
from wandb_archive.storage import Storage
from wandb_archive.util import canonical_json, pretty_json


@dataclass(frozen=True)
class PublishedRun:
    manifest: RunManifest
    manifest_path: str
    latest_path: str


class RunPublisher:
    def __init__(self, config: AppConfig, storage: Storage) -> None:
        self.config = config
        self.storage = storage

    @staticmethod
    def latest_path(run_path: str) -> str:
        return f"runs/{run_path}/latest.json"

    def current_fingerprint(self, run_path: str) -> str | None:
        key = self.latest_path(run_path)
        if not self.storage.exists(key):
            return None
        pointer = json.loads(self.storage.read_bytes(key))
        return pointer.get("source_fingerprint")

    def current(self, run_path: str) -> PublishedRun | None:
        latest_path = self.latest_path(run_path)
        if not self.storage.exists(latest_path):
            return None
        pointer = json.loads(self.storage.read_bytes(latest_path))
        manifest_path = str(pointer["manifest_path"])
        manifest = RunManifest.model_validate_json(
            self.storage.read_bytes(manifest_path)
        )
        return PublishedRun(manifest, manifest_path, latest_path)

    def publish(self, result: ExportResult) -> PublishedRun:
        objects = sorted(result.objects, key=lambda item: item.logical_path)

        def upload(item):
            blob_path = f"blobs/sha256/{item.sha256[:2]}/{item.sha256}"
            self.storage.put_file_if_missing(
                item.local_path, blob_path, item.media_type
            )
            return ObjectManifest(
                **item.model_dump(mode="json", exclude={"local_path"}),
                blob_path=blob_path,
            )

        with ThreadPoolExecutor(
            max_workers=self.config.archive.transfers.concurrency
        ) as executor:
            manifested = list(executor.map(upload, objects))

        policy_complete = not any(
            item.get("required") == "true" for item in result.exclusions
        )
        include = self.config.archive.include
        deletion_ready = (
            result.snapshot.state in TERMINAL_STATES
            and self.config.archive.strict
            and include.histories
            and result.complete_history
            and not result.contains_live_data
            and policy_complete
        )
        manifest = RunManifest(
            exporter_version=__version__,
            run_path=result.snapshot.path,
            source_fingerprint=result.snapshot.source_fingerprint,
            source_state=result.snapshot.state,
            complete_history=result.complete_history,
            contains_live_data=result.contains_live_data,
            policy_complete=policy_complete,
            deletion_ready=deletion_ready,
            exclusions=result.exclusions,
            objects=manifested,
            metric_rows=result.metric_rows,
            system_metric_rows=result.system_metric_rows,
            histogram_rows=result.histogram_rows,
            table_rows=result.table_rows,
        )
        generation = result.snapshot.source_fingerprint
        manifest_path = (
            f"runs/{result.snapshot.path}/generations/{generation}/manifest.json"
        )
        if self.storage.exists(manifest_path):
            manifest = RunManifest.model_validate_json(
                self.storage.read_bytes(manifest_path)
            )
            if manifest.source_fingerprint != generation:
                raise RuntimeError(f"Invalid existing manifest: {manifest_path}")
        else:
            self.storage.write_bytes(
                pretty_json(manifest.model_dump(mode="json")).encode(),
                manifest_path,
                "application/json",
            )
        latest_path = self.latest_path(result.snapshot.path)
        pointer = {
            "archive_schema_version": manifest.archive_schema_version,
            "run_path": manifest.run_path,
            "source_fingerprint": manifest.source_fingerprint,
            "manifest_path": manifest_path,
            "archived_at": manifest.archived_at,
        }
        self.storage.write_bytes(
            canonical_json(pointer) + b"\n", latest_path, "application/json"
        )
        return PublishedRun(manifest, manifest_path, latest_path)
