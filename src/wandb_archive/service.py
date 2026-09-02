"""Application orchestration for planning and backing up W&B runs."""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import Any

import yaml

from wandb_archive.catalog import CatalogPublisher, read_catalog
from wandb_archive.config import AppConfig
from wandb_archive.discovery import WandbSource
from wandb_archive.export import RunExporter
from wandb_archive.model import RunSnapshot
from wandb_archive.progress import Progress
from wandb_archive.publisher import PublishedRun, RunPublisher
from wandb_archive.storage import Storage

logger = logging.getLogger(__name__)


class BackupFailures(RuntimeError):
    def __init__(self, failures: list[tuple[str, Exception]]) -> None:
        self.failures = failures
        self.result: dict[str, Any] | None = None
        detail = "; ".join(f"{path}: {error}" for path, error in failures)
        super().__init__(f"{len(failures)} run(s) failed: {detail}")


class ArchiveService:
    def __init__(
        self,
        config: AppConfig,
        storage: Storage,
        source: WandbSource | None = None,
        *,
        show_progress: bool = False,
    ) -> None:
        self.config = config
        self.storage = storage
        self.progress = Progress(enabled=show_progress)
        self.source = source or WandbSource(config, progress=self.progress)
        self.publisher = RunPublisher(config, storage)

    def plan(
        self,
        *,
        project: str | None = None,
        run_path: str | None = None,
        since: str | None = None,
        detailed: bool = False,
    ) -> dict[str, Any]:
        exporter = RunExporter(self.config)
        source_runs = self.source.runs(
            project_override=project, run_path=run_path, since=since
        )
        if not detailed:
            logger.info("Reading the current archive catalog")
            archived_paths = {
                row["run_path"] for row in read_catalog(self.storage, "runs")
            }
            items: list[dict[str, Any]] = []
            for run in self.progress.track(
                source_runs,
                description="Planning runs",
                total=len(source_runs),
                unit="run",
                leave=True,
            ):
                preview = self.source.preview(run)
                archived = preview["run_path"] in archived_paths
                items.append(
                    {
                        **preview,
                        "already_archived": archived,
                        "action": "inspect" if archived else "archive",
                    }
                )
            return {
                "mode": "fast",
                "project_count": len({item["project"] for item in items}),
                "run_count": len(items),
                "archive_count": sum(item["action"] == "archive" for item in items),
                "inspect_count": sum(item["action"] == "inspect" for item in items),
                "skip_count": None,
                "estimated_source_bytes": None,
                "runs": items,
            }
        logger.info("Inspecting complete metadata for %d run(s)", len(source_runs))
        snapshots = []
        for run in self.progress.track(
            source_runs,
            description="Inspecting runs",
            total=len(source_runs),
            unit="run",
            leave=True,
        ):
            snapshots.append(self.source.snapshot(run))
        items = []
        for snapshot in snapshots:
            current = self.publisher.current_fingerprint(snapshot.path)
            exclusions = [
                {"source": item.name, "reason": reason}
                for item in snapshot.files
                if (reason := exporter.run_file_exclusion(item.name)) is not None
            ]
            items.append(
                {
                    "run_path": snapshot.path,
                    "state": snapshot.state,
                    "source_file_count": len(snapshot.files),
                    "estimated_source_bytes": sum(item.size for item in snapshot.files),
                    "artifact_count": len(snapshot.artifacts),
                    "policy_exclusions": exclusions,
                    "action": (
                        "skip" if current == snapshot.source_fingerprint else "archive"
                    ),
                }
            )
        return {
            "mode": "detailed",
            "project_count": len({item.project for item in snapshots}),
            "run_count": len(snapshots),
            "archive_count": sum(item["action"] == "archive" for item in items),
            "skip_count": sum(item["action"] == "skip" for item in items),
            "estimated_source_bytes": sum(
                item["estimated_source_bytes"] for item in items
            ),
            "runs": items,
        }

    def backup(
        self,
        *,
        project: str | None = None,
        run_path: str | None = None,
        since: str | None = None,
    ) -> dict[str, Any]:
        source_runs = self.source.runs(
            project_override=project, run_path=run_path, since=since
        )
        logger.info("Selected %d W&B run(s)", len(source_runs))
        reconciled: list[tuple[RunSnapshot, PublishedRun]] = []
        failures: list[tuple[str, Exception]] = []
        archived = skipped = 0
        exporter = RunExporter(self.config)
        for run in self.progress.track(
            source_runs,
            description="Archiving runs",
            total=len(source_runs),
            unit="run",
            leave=True,
        ):
            snapshot: RunSnapshot | None = None
            fallback_path = "/".join(
                str(getattr(run, field, "unknown"))
                for field in ("entity", "project", "id")
            )
            try:
                snapshot = self.source.snapshot(run)
                current = self.publisher.current(snapshot.path)
                if (
                    current is not None
                    and current.manifest.source_fingerprint
                    == snapshot.source_fingerprint
                ):
                    logger.info("Skipping unchanged run %s", snapshot.path)
                    reconciled.append((snapshot, current))
                    skipped += 1
                    continue
                logger.info("Archiving run %s", snapshot.path)
                with tempfile.TemporaryDirectory(
                    prefix=f"wandb-archive-{snapshot.run_id}-"
                ) as raw:
                    export_result = exporter.export(run, snapshot, Path(raw))
                    published = self.publisher.publish(export_result)
                reconciled.append((snapshot, published))
                archived += 1
            except Exception as error:
                path = snapshot.path if snapshot is not None else fallback_path
                logger.exception("Failed to archive run %s", path)
                failures.append((path, error))

        config_bytes = yaml.safe_dump(
            self.config.model_dump(mode="json"), sort_keys=False
        ).encode()
        index = None
        if reconciled or not self.storage.exists("archive.json"):
            logger.info("Publishing merged Parquet catalogs")
            index = CatalogPublisher(self.config, self.storage).update(
                reconciled, resolved_config=config_bytes
            )
        summary = {
            "selected": len(source_runs),
            "archived": archived,
            "skipped": skipped,
            "failed": len(failures),
            "catalog_generation": index["generation"] if index else None,
        }
        if failures:
            failure = BackupFailures(failures)
            failure.result = summary
            raise failure
        return summary
