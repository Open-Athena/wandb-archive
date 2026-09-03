"""Discover W&B projects and create cheap, deterministic run snapshots."""

from __future__ import annotations

import datetime as dt
import fnmatch
import logging
from collections.abc import Iterable
from typing import Any

import wandb

from wandb_archive.config import AppConfig
from wandb_archive.model import (
    ARCHIVE_SCHEMA_VERSION,
    RunSnapshot,
    SourceArtifact,
    SourceFile,
)
from wandb_archive.progress import Progress
from wandb_archive.retry import call_with_retry
from wandb_archive.util import canonical_json, sha256_bytes

logger = logging.getLogger(__name__)


def _json_mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    raw = getattr(value, "_json_dict", value)
    return dict(raw) if isinstance(raw, dict) else {}


def _attr(obj: Any, name: str, default: Any = None) -> Any:
    value = getattr(obj, name, default)
    if value is not None:
        return value
    attrs = getattr(obj, "_attrs", {})
    return attrs.get(name, default) if isinstance(attrs, dict) else default


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _parse_time(value: str | None) -> dt.datetime | None:
    if value is None:
        return None
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed


def _user_name(value: Any) -> str | None:
    if value is None:
        return None
    for field in ("username", "name"):
        candidate = _attr(value, field)
        if candidate:
            return str(candidate)
    return str(value)


class WandbSource:
    """Thin, injectable wrapper around the W&B Public API."""

    def __init__(
        self,
        config: AppConfig,
        api: Any | None = None,
        progress: Progress | None = None,
    ) -> None:
        self.config = config
        self.progress = progress or Progress()
        if api is not None:
            self.api = api
        else:
            overrides = (
                {"base_url": config.source.base_url} if config.source.base_url else None
            )
            self.api = call_with_retry(
                lambda: wandb.Api(
                    overrides=overrides,
                    timeout=config.source.timeout_seconds,
                ),
                config.source.retry,
                "initialization",
            )

    def projects(self) -> list[str]:
        selection = self.config.source.projects
        logger.info("Discovering projects in W&B entity %s", self.config.source.entity)

        def load_projects() -> list[Any]:
            return list(
                self.progress.track(
                    self.api.projects(entity=self.config.source.entity, per_page=200),
                    description="Discovering projects",
                    unit="project",
                )
            )

        projects = call_with_retry(
            load_projects,
            self.config.source.retry,
            "project discovery",
        )
        names = sorted(str(_attr(project, "name")) for project in projects)
        return [
            name
            for name in names
            if any(fnmatch.fnmatchcase(name, pattern) for pattern in selection.include)
            and not any(
                fnmatch.fnmatchcase(name, pattern) for pattern in selection.exclude
            )
        ]

    def runs(
        self,
        *,
        project_override: str | None = None,
        run_path: str | None = None,
        since: str | None = None,
    ) -> list[Any]:
        if run_path is not None:
            return [
                call_with_retry(
                    lambda: self.api.run(run_path),
                    self.config.source.retry,
                    f"loading run {run_path}",
                )
            ]
        projects = [project_override] if project_override else self.projects()
        selected: list[Any] = []
        for project in projects:
            path = f"{self.config.source.entity}/{project}"
            logger.info("Listing W&B runs in %s", path)

            def load_runs(path: str = path, project: str = project) -> list[Any]:
                return list(
                    self.progress.track(
                        self.api.runs(
                            path,
                            order="+created_at",
                            per_page=100,
                            include_sweeps=True,
                        ),
                        description=f"Listing runs: {project}",
                        unit="run",
                    )
                )

            runs = call_with_retry(
                load_runs,
                self.config.source.retry,
                f"listing runs in {path}",
            )
            selected.extend(run for run in runs if self._include_run(run, since=since))
        return selected

    def _include_run(self, run: Any, *, since: str | None) -> bool:
        selection = self.config.source.runs
        if str(_attr(run, "state")) not in selection.states:
            return False
        tags = {str(tag) for tag in (_attr(run, "tags", []) or [])}
        if selection.include_tags and not set(selection.include_tags).issubset(tags):
            return False
        if tags.intersection(selection.exclude_tags):
            return False
        created = _parse_time(_iso(_attr(run, "created_at")))
        lower = _parse_time(since or selection.created_after)
        upper = _parse_time(selection.created_before)
        if created is not None and lower is not None and created < lower:
            return False
        return not (created is not None and upper is not None and created >= upper)

    def preview(self, run: Any) -> dict[str, str | None]:
        """Return fields available from W&B's lightweight run listing."""

        entity = str(_attr(run, "entity", self.config.source.entity))
        project = str(_attr(run, "project"))
        run_id = str(_attr(run, "id"))
        return {
            "run_path": f"{entity}/{project}/{run_id}",
            "entity": entity,
            "project": project,
            "run_id": run_id,
            "name": str(_attr(run, "name")),
            "state": str(_attr(run, "state")),
            "created_at": _iso(_attr(run, "created_at")),
        }

    def snapshot(self, run: Any) -> RunSnapshot:
        path = "/".join(
            str(_attr(run, field, "unknown")) for field in ("entity", "project", "id")
        )
        return call_with_retry(
            lambda: self._snapshot(run),
            self.config.source.retry,
            f"inspecting run {path}",
        )

    def _snapshot(self, run: Any) -> RunSnapshot:
        files = sorted(
            (
                SourceFile(
                    name=str(_attr(file, "name")),
                    size=int(_attr(file, "size", _attr(file, "sizeBytes", 0)) or 0),
                    md5=_attr(file, "md5"),
                    mimetype=_attr(file, "mimetype"),
                    updated_at=_iso(_attr(file, "updated_at")),
                )
                for file in run.files(per_page=100)
            ),
            key=lambda item: item.name,
        )
        artifacts = [
            *self._artifacts(run.logged_artifacts(per_page=100), "logged"),
            *self._artifacts(run.used_artifacts(per_page=100), "used"),
        ]
        attrs = getattr(run, "_attrs", {})
        metadata = _json_mapping(_attr(run, "metadata", {}))
        summary = _json_mapping(_attr(run, "summary", {}))
        config = _json_mapping(_attr(run, "config", {}))
        snapshot_data = {
            "entity": str(_attr(run, "entity", self.config.source.entity)),
            "project": str(_attr(run, "project")),
            "run_id": str(_attr(run, "id")),
            "name": str(_attr(run, "name")),
            "state": str(_attr(run, "state")),
            "created_at": _iso(_attr(run, "created_at")),
            "updated_at": _iso(_attr(run, "updated_at", _attr(run, "heartbeatAt"))),
            "last_history_step": _attr(run, "lastHistoryStep"),
            "url": _attr(run, "url"),
            "group": _attr(
                run,
                "group",
                attrs.get("group") if isinstance(attrs, dict) else None,
            ),
            "job_type": _attr(
                run,
                "job_type",
                attrs.get("jobType") if isinstance(attrs, dict) else None,
            ),
            "notes": _attr(run, "notes"),
            "tags": [str(tag) for tag in (_attr(run, "tags", []) or [])],
            "sweep_name": _attr(run, "sweep_name"),
            "user": _user_name(_attr(run, "user")),
            "config": config,
            "summary": summary,
            "metadata": metadata,
            "files": [item.model_dump(mode="json") for item in files],
            "artifacts": [item.model_dump(mode="json") for item in artifacts],
        }
        fingerprint_data = {
            **snapshot_data,
            "archive_schema_version": ARCHIVE_SCHEMA_VERSION,
            "archive_policy": self.config.archive.model_dump(
                mode="json", exclude={"transfers"}
            ),
        }
        return RunSnapshot(
            **snapshot_data,
            source_fingerprint=sha256_bytes(canonical_json(fingerprint_data)),
        )

    @staticmethod
    def _artifacts(artifacts: Iterable[Any], direction: str) -> list[SourceArtifact]:
        result = []
        for artifact in artifacts:
            aliases = []
            for alias in _attr(artifact, "aliases", []) or []:
                aliases.append(str(_attr(alias, "alias", alias)))
            manifest_object = _attr(artifact, "manifest")
            manifest = (
                manifest_object.to_manifest_json()
                if manifest_object is not None
                else {}
            )
            result.append(
                SourceArtifact(
                    direction=direction,  # type: ignore[arg-type]
                    id=_attr(artifact, "id"),
                    name=str(_attr(artifact, "name")),
                    version=_attr(artifact, "version"),
                    type=_attr(artifact, "type"),
                    digest=_attr(artifact, "digest"),
                    size=_attr(artifact, "size"),
                    aliases=aliases,
                    metadata=_json_mapping(_attr(artifact, "metadata", {})),
                    manifest=_json_mapping(manifest),
                )
            )
        return sorted(result, key=lambda item: (item.direction, item.name))
