"""Lossless and query-oriented export of one W&B run."""

from __future__ import annotations

import json
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path, PurePosixPath
from typing import Any

from wandb_archive.config import AppConfig
from wandb_archive.model import (
    TERMINAL_STATES,
    ExportResult,
    RunSnapshot,
    StagedObject,
)
from wandb_archive.normalize import normalize_histories, table_json_to_parquet
from wandb_archive.retry import call_with_retry
from wandb_archive.security import (
    include_file,
    is_code_path,
    is_console_path,
    is_media_path,
    is_private_metadata_path,
    is_table_path,
    sanitize_metadata,
    scan_file,
)
from wandb_archive.util import media_type, pretty_json, sha256_file


class ExportError(RuntimeError):
    """A run could not be exported with the requested guarantees."""


class SensitiveDataError(ExportError):
    """A public export contained a credential-shaped value."""


def _attr(obj: Any, name: str, default: Any = None) -> Any:
    value = getattr(obj, name, default)
    if value is not None:
        return value
    attrs = getattr(obj, "_attrs", {})
    return attrs.get(name, default) if isinstance(attrs, dict) else default


def snapshot_path(run: Any, config: AppConfig) -> str:
    return "/".join(
        str(_attr(run, field, default))
        for field, default in (
            ("entity", config.source.entity),
            ("project", "unknown"),
            ("id", "unknown"),
        )
    )


def _download(file: Any, root: Path) -> Any:
    return file.download(root=str(root), replace=True)


def _artifact_files(artifact: Any) -> list[Any]:
    return list(artifact.files(per_page=100))


def _is_missing_source_object(error: Exception) -> bool:
    current: BaseException | None = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        response = getattr(current, "response", None)
        if getattr(response, "status_code", None) == 404:
            return True
        message = str(current).lower()
        if "404" in message and ("not found" in message or "nosuchkey" in message):
            return True
        current = current.__cause__ or current.__context__
    return False


def _relative(name: str) -> Path:
    pure = PurePosixPath(name.replace("\\", "/"))
    if pure.is_absolute() or ".." in pure.parts:
        raise ExportError(f"W&B returned an unsafe file path: {name!r}")
    return Path(*pure.parts)


def _slug(value: str) -> str:
    safe = "".join(char if char.isalnum() or char in "-_." else "-" for char in value)
    return safe.strip("-.") or "artifact"


class _Stage:
    def __init__(self) -> None:
        self.objects: list[StagedObject] = []

    def add(
        self,
        path: Path,
        logical_path: str,
        kind: str,
        *,
        source_name: str | None = None,
        supplied_media_type: str | None = None,
        row_count: int | None = None,
    ) -> None:
        self.objects.append(
            StagedObject(
                logical_path=logical_path,
                local_path=path,
                sha256=sha256_file(path),
                size=path.stat().st_size,
                media_type=media_type(path, supplied_media_type),
                kind=kind,
                source_name=source_name,
                row_count=row_count,
            )
        )


class RunExporter:
    def __init__(self, config: AppConfig) -> None:
        self.config = config

    def export(self, run: Any, snapshot: RunSnapshot, directory: Path) -> ExportResult:
        directory.mkdir(parents=True, exist_ok=True)
        stage = _Stage()
        exclusions: list[dict[str, str]] = []

        metadata = snapshot.model_dump(
            mode="json", exclude={"config", "summary", "metadata"}
        )
        metadata["metadata"] = sanitize_metadata(
            snapshot.metadata, self.config.archive.security
        )
        self._write_sensitive_json(
            directory / "metadata.json",
            metadata,
            stage,
            "metadata",
            exclusions,
        )
        self._write_sensitive_json(
            directory / "config.json",
            snapshot.config,
            stage,
            "config",
            exclusions,
        )
        self._write_sensitive_json(
            directory / "summary.json",
            snapshot.summary,
            stage,
            "summary",
            exclusions,
        )
        self._write_sensitive_json(
            directory / "artifacts.json",
            [item.model_dump(mode="json") for item in snapshot.artifacts],
            stage,
            "artifact-metadata",
            exclusions,
        )

        raw_history, complete_history, contains_live_data = self._history(
            run, snapshot, directory, stage, exclusions
        )
        metric_rows = system_rows = histogram_rows = 0
        if raw_history:
            normalized = normalize_histories(raw_history, snapshot, directory / "data")
            metric_path, system_path, histogram_path = normalized[:3]
            metric_rows, system_rows, histogram_rows = normalized[3:]
            if self.config.archive.include.histories:
                stage.add(
                    metric_path,
                    "data/metrics.parquet",
                    "metrics",
                    row_count=metric_rows,
                )
                stage.add(
                    histogram_path,
                    "data/histograms.parquet",
                    "histograms",
                    row_count=histogram_rows,
                )
            if self.config.archive.include.system_metrics:
                stage.add(
                    system_path,
                    "data/system_metrics.parquet",
                    "system-metrics",
                    row_count=system_rows,
                )

        table_rows = self._run_files(run, directory, stage, exclusions)
        table_rows += self._artifacts(run, directory, stage, exclusions)
        if self.config.archive.include.console_logs:
            self._console_logs(run, directory, stage, exclusions)

        return ExportResult(
            snapshot=snapshot,
            complete_history=complete_history,
            contains_live_data=contains_live_data,
            exclusions=exclusions,
            objects=stage.objects,
            metric_rows=metric_rows,
            system_metric_rows=system_rows,
            histogram_rows=histogram_rows,
            table_rows=table_rows,
        )

    def _write_json(self, path: Path, value: Any, stage: _Stage, kind: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(pretty_json(value), encoding="utf-8")
        stage.add(path, path.name, kind)

    def _write_sensitive_json(
        self,
        path: Path,
        value: Any,
        stage: _Stage,
        kind: str,
        exclusions: list[dict[str, str]],
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(pretty_json(value), encoding="utf-8")
        if self._check_sensitive(path, str(path.name), exclusions):
            stage.add(path, path.name, kind)

    def _check_sensitive(
        self,
        path: Path,
        source_name: str,
        exclusions: list[dict[str, str]],
    ) -> bool:
        if self.config.archive.security.profile != "public-safe":
            return True
        findings = scan_file(path)
        if not findings:
            return True
        reason = "sensitive content: " + ", ".join(findings)
        if self.config.archive.security.on_sensitive_value == "fail":
            raise SensitiveDataError(f"{source_name}: {reason}")
        exclusions.append({"source": source_name, "reason": reason, "required": "true"})
        path.unlink(missing_ok=True)
        return False

    def _history(
        self,
        run: Any,
        snapshot: RunSnapshot,
        directory: Path,
        stage: _Stage,
        exclusions: list[dict[str, str]],
    ) -> tuple[list[Path], bool, bool]:
        include = self.config.archive.include
        if not include.histories and not include.system_metrics:
            exclusions.append({"source": "history", "reason": "disabled by policy"})
            return [], False, False
        method = getattr(run, "download_history_exports", None)
        if method is None:
            raise ExportError(
                "This W&B SDK cannot download complete native history exports"
            )
        raw_dir = directory / "raw" / "history"
        raw_dir.mkdir(parents=True, exist_ok=True)
        require_complete = (
            self.config.archive.strict and snapshot.state in TERMINAL_STATES
        )
        try:
            result = call_with_retry(
                lambda: method(
                    raw_dir,
                    require_complete_history=require_complete,
                ),
                self.config.source.retry,
                f"downloading history for {snapshot.path}",
            )
        except Exception as error:
            raise ExportError(
                f"W&B could not provide complete history for the run: {error}"
            ) from error
        errors = getattr(result, "errors", None) or {}
        contains_live_data = bool(getattr(result, "contains_live_data", False))
        if errors and require_complete:
            raise ExportError(f"W&B history download errors: {errors}")
        paths = [Path(path) for path in getattr(result, "paths", [])]
        if not paths and require_complete:
            if (
                snapshot.last_history_step is not None
                and snapshot.last_history_step < 0
            ):
                return [], True, contains_live_data
            raise ExportError("W&B returned no native history files")
        selected: list[Path] = []
        for path in paths:
            is_system = "event" in path.name.lower() or "system" in path.name.lower()
            enabled = include.system_metrics if is_system else include.histories
            if enabled:
                if not self._check_sensitive(path, str(path.name), exclusions):
                    continue
                selected.append(path)
                stage.add(
                    path,
                    f"raw/history/{path.name}",
                    "raw-system-history" if is_system else "raw-history",
                )
        for path, message in errors.items():
            exclusions.append(
                {
                    "source": str(path),
                    "reason": str(message),
                    "required": "true",
                }
            )
        return selected, not errors and not contains_live_data, contains_live_data

    def _run_files(
        self,
        run: Any,
        directory: Path,
        stage: _Stage,
        exclusions: list[dict[str, str]],
    ) -> int:
        table_rows = 0
        files = call_with_retry(
            lambda: list(run.files(per_page=100)),
            self.config.source.retry,
            f"listing files for {snapshot_path(run, self.config)}",
        )
        selected: list[Any] = []
        for file in files:
            name = str(_attr(file, "name"))
            reason = self.run_file_exclusion(name)
            if reason is not None:
                exclusions.append({"source": name, "reason": reason})
                continue
            selected.append(file)
        root = directory / "run-files"
        for file, name, path, error in self._download_files(selected, root, "run file"):
            if error is not None:
                self._handle_download_error(error, name, exclusions)
                continue
            assert path is not None
            if not self._check_sensitive(path, name, exclusions):
                continue
            stage.add(
                path,
                f"run-files/{name}",
                "run-file",
                source_name=name,
                supplied_media_type=_attr(file, "mimetype"),
            )
            if is_table_path(name) and self.config.archive.include.tables:
                table_rows += self._convert_table(path, name, directory, stage)
        return table_rows

    def _download_files(
        self,
        files: list[Any],
        root: Path,
        description: str,
    ) -> Iterator[tuple[Any, str, Path | None, Exception | None]]:
        """Download independent W&B files concurrently, preserving input order."""

        def download(file: Any) -> tuple[Any, str, Path | None, Exception | None]:
            name = str(_attr(file, "name"))
            try:
                downloaded = call_with_retry(
                    partial(_download, file, root),
                    self.config.source.retry,
                    f"downloading {description} {name}",
                )
                downloaded.close()
                path = root / _relative(name)
                expected = int(_attr(file, "size", 0) or 0)
                if expected and path.stat().st_size != expected:
                    raise ExportError(
                        f"Downloaded size mismatch for {name}: "
                        f"expected {expected}, got {path.stat().st_size}"
                    )
                return file, name, path, None
            except Exception as error:
                return file, name, None, error

        with ThreadPoolExecutor(
            max_workers=self.config.archive.transfers.concurrency
        ) as executor:
            yield from executor.map(download, files)

    @staticmethod
    def _handle_download_error(
        error: Exception,
        source_name: str,
        exclusions: list[dict[str, str]],
    ) -> None:
        if not _is_missing_source_object(error):
            raise error
        exclusions.append(
            {
                "source": source_name,
                "reason": "source object is missing from W&B storage",
                "required": "true",
            }
        )

    def run_file_exclusion(self, name: str) -> str | None:
        include = self.config.archive.include
        if (
            self.config.archive.security.profile == "public-safe"
            and is_private_metadata_path(name)
        ):
            return "machine metadata is disabled by the public-safe policy"
        if is_code_path(name) and not include.code:
            return "source snapshots are disabled"
        if is_console_path(name) and not include.console_logs:
            return "console logs are disabled"
        if is_media_path(name) and not include.media and not is_table_path(name):
            return "media is disabled"
        if is_table_path(name) and not include.tables:
            return "tables are disabled"
        allowed, reason = include_file(include.run_files, name)
        return None if allowed else reason

    def _convert_table(
        self, source: Path, source_name: str, directory: Path, stage: _Stage
    ) -> int:
        destination = directory / "tables" / f"{sha256_file(source)}.parquet"
        try:
            rows = table_json_to_parquet(source, destination)
        except (ValueError, json.JSONDecodeError) as error:
            if self.config.archive.strict:
                raise ExportError(
                    f"Could not convert W&B Table {source_name}"
                ) from error
            return 0
        stage.add(
            destination,
            f"tables/{destination.name}",
            "table",
            source_name=source_name,
            row_count=rows,
        )
        return rows

    def _artifacts(
        self,
        run: Any,
        directory: Path,
        stage: _Stage,
        exclusions: list[dict[str, str]],
    ) -> int:
        include = self.config.archive.include
        table_rows = 0
        run_path = snapshot_path(run, self.config)
        logged = (
            call_with_retry(
                lambda: list(run.logged_artifacts(per_page=100)),
                self.config.source.retry,
                f"listing logged artifacts for {run_path}",
            )
            if include.logged_artifacts in {"all", "safe"}
            else []
        )
        used = (
            call_with_retry(
                lambda: list(run.used_artifacts(per_page=100)),
                self.config.source.retry,
                f"listing used artifacts for {run_path}",
            )
            if include.used_artifacts in {"all", "safe"}
            else []
        )
        for direction, policy, artifacts in (
            ("logged", include.logged_artifacts, logged),
            ("used", include.used_artifacts, used),
        ):
            if policy in {"none", "metadata", "references"}:
                continue
            for artifact in artifacts:
                artifact_name = str(_attr(artifact, "name"))
                root = directory / "artifacts" / direction / _slug(artifact_name)
                files = call_with_retry(
                    partial(_artifact_files, artifact),
                    self.config.source.retry,
                    f"listing files in artifact {artifact_name}",
                )
                selected: list[Any] = []
                for file in files:
                    name = str(_attr(file, "name"))
                    allowed, reason = include_file(policy, name)  # type: ignore[arg-type]
                    source_name = f"{artifact_name}/{name}"
                    if not allowed:
                        exclusions.append(
                            {"source": source_name, "reason": reason or "excluded"}
                        )
                        continue
                    selected.append(file)
                for file, name, path, error in self._download_files(
                    selected, root, f"file in artifact {artifact_name}"
                ):
                    source_name = f"{artifact_name}/{name}"
                    if error is not None:
                        self._handle_download_error(error, source_name, exclusions)
                        continue
                    assert path is not None
                    if not self._check_sensitive(path, source_name, exclusions):
                        continue
                    logical = f"artifacts/{direction}/{_slug(artifact_name)}/{name}"
                    stage.add(
                        path,
                        logical,
                        "artifact-file",
                        source_name=source_name,
                        supplied_media_type=_attr(file, "mimetype"),
                    )
                    if is_table_path(name) and include.tables:
                        table_rows += self._convert_table(
                            path, source_name, directory, stage
                        )
        return table_rows

    def _console_logs(
        self,
        run: Any,
        directory: Path,
        stage: _Stage,
        exclusions: list[dict[str, str]],
    ) -> None:
        path = directory / "console.jsonl"

        def download_logs() -> None:
            with path.open("w", encoding="utf-8") as stream:
                for line in run.console_logs(per_page=1000):
                    payload = {
                        key: _attr(line, key)
                        for key in (
                            "number",
                            "timestamp",
                            "level",
                            "label",
                            "content",
                        )
                    }
                    stream.write(
                        json.dumps(payload, default=str, sort_keys=True) + "\n"
                    )

        call_with_retry(
            download_logs,
            self.config.source.retry,
            f"downloading console logs for {snapshot_path(run, self.config)}",
        )
        if self._check_sensitive(path, "console logs", exclusions):
            stage.add(path, "console.jsonl", "console-log")
