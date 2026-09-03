from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from wandb_archive.config import AppConfig
from wandb_archive.discovery import WandbSource
from wandb_archive.service import ArchiveService
from wandb_archive.storage import LocalStorage
from wandb_archive.verify import inspect_run, verify_archive


class FakeFile:
    def __init__(self, name: str, data: bytes) -> None:
        self.name = name
        self.data = data
        self.size = len(data)
        self.md5 = "source-md5"
        self.mimetype = "application/json"
        self.updated_at = "2026-01-02T00:00:00Z"

    def download(self, root: str, replace: bool = True):
        del replace
        path = Path(root) / self.name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.data)
        return path.open()


class ConcurrentFakeFile(FakeFile):
    active = 0
    maximum_active = 0
    lock = threading.Lock()

    def download(self, root: str, replace: bool = True):
        with self.lock:
            type(self).active += 1
            type(self).maximum_active = max(
                type(self).maximum_active, type(self).active
            )
        try:
            time.sleep(0.05)
            return super().download(root, replace)
        finally:
            with self.lock:
                type(self).active -= 1


class MissingFakeFile(FakeFile):
    def download(self, root: str, replace: bool = True):
        del root, replace
        raise RuntimeError("404 Not Found: NoSuchKey")


@dataclass
class HistoryResult:
    paths: list[Path]
    contains_live_data: bool = False
    errors: dict[Path, str] | None = None


class FakeRun:
    entity = "team"
    project = "ocean"
    id = "abc123"
    name = "quarter-degree"
    state = "finished"
    created_at = "2026-01-01T00:00:00Z"
    updated_at = "2026-01-02T00:00:00Z"
    lastHistoryStep = 1
    url = "https://wandb.example/team/ocean/runs/abc123"
    group = "production"
    job_type = "train"
    notes = "test run"
    tags = ["samudra"]
    sweep_name = None
    user = "researcher"
    metadata = {"python": "3.12", "args": ["--secret", "redacted"]}
    config = {"learning_rate": 0.001, "optional": float("nan")}
    summary = {"loss": 0.5}

    def __init__(self) -> None:
        table = json.dumps(
            {"columns": ["step", "loss"], "data": [[0, 1.0], [1, 0.5]]}
        ).encode()
        self._files = [FakeFile("media/table/example.table.json", table)]

    def files(self, per_page: int = 100):
        del per_page
        return list(self._files)

    def logged_artifacts(self, per_page: int = 100):
        del per_page
        return []

    def used_artifacts(self, per_page: int = 100):
        del per_page
        return []

    def download_history_exports(
        self, directory: Path, require_complete_history: bool = True
    ) -> HistoryResult:
        assert require_complete_history == (self.state == "finished")
        history = Path(directory) / "history.parquet"
        system = Path(directory) / "events.parquet"
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {"_step": 0, "_timestamp": 1.0, "loss": 1.0},
                    {"_step": 1, "_timestamp": 2.0, "loss": 0.5},
                ]
            ),
            history,
        )
        pq.write_table(
            pa.Table.from_pylist([{"_step": 1, "_timestamp": 2.0, "system.gpu": 90.0}]),
            system,
        )
        return HistoryResult(
            [history, system], contains_live_data=self.state == "running"
        )


class FakeProject:
    name = "ocean"


class FakeApi:
    def __init__(self, run: FakeRun) -> None:
        self._run = run

    def projects(self, entity: str, per_page: int):
        assert entity == "team"
        del per_page
        return [FakeProject()]

    def runs(self, path: str, **kwargs: Any):
        assert path == "team/ocean"
        del kwargs
        return [self._run]

    def run(self, path: str):
        assert path == "team/ocean/abc123"
        return self._run


def test_backup_is_idempotent_and_deletion_ready(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    config = AppConfig.model_validate(
        {
            "source": {"entity": "team"},
            "destination": {"type": "local", "path": archive},
        }
    )
    fake_run = FakeRun()
    source = WandbSource(config, api=FakeApi(fake_run))
    service = ArchiveService(config, LocalStorage(archive), source)

    first = service.backup()
    second = service.backup()

    assert first["archived"] == 1
    assert second["archived"] == 0
    assert second["skipped"] == 1
    report = verify_archive(LocalStorage(archive), deep=True)
    assert report["ok"]
    details = inspect_run(LocalStorage(archive), "team/ocean/abc123")
    assert details["deletion_ready"] is True
    assert details["metric_rows"] == 2
    assert details["system_metric_rows"] == 1
    assert details["table_rows"] == 2
    assert len(list((archive / "runs/team/ocean/abc123/generations").iterdir())) == 1


def test_sensitive_config_can_be_excluded_without_catalog_leak(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "archive"
    config = AppConfig.model_validate(
        {
            "source": {"entity": "team"},
            "destination": {"type": "local", "path": archive},
            "archive": {
                "security": {
                    "profile": "public-safe",
                    "on_sensitive_value": "exclude",
                }
            },
        }
    )
    fake_run = FakeRun()
    fake_run.config = {"api_key": "abcdefghijklmnop123456"}
    source = WandbSource(config, api=FakeApi(fake_run))

    ArchiveService(config, LocalStorage(archive), source).backup()

    details = inspect_run(LocalStorage(archive), "team/ocean/abc123")
    assert details["deletion_ready"] is False
    assert any(item["source"] == "config.json" for item in details["exclusions"])
    index = json.loads((archive / "archive.json").read_text())
    rows = pq.read_table(archive / index["catalogs"]["runs"]).to_pylist()
    assert json.loads(rows[0]["config_json"]) == {}


def test_plan_does_not_create_local_destination(tmp_path: Path) -> None:
    archive = tmp_path / "not-created"
    config = AppConfig.model_validate(
        {
            "source": {"entity": "team"},
            "destination": {"type": "local", "path": archive},
        }
    )
    run = FakeRun()
    source = WandbSource(config, api=FakeApi(run))

    plan = ArchiveService(config, LocalStorage(archive), source).plan()

    assert plan["archive_count"] == 1
    assert not archive.exists()


def test_running_run_is_archived_but_not_deletion_ready(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    config = AppConfig.model_validate(
        {
            "source": {
                "entity": "team",
                "runs": {"states": ["running"]},
            },
            "destination": {"type": "local", "path": archive},
        }
    )
    run = FakeRun()
    run.state = "running"
    source = WandbSource(config, api=FakeApi(run))

    ArchiveService(config, LocalStorage(archive), source).backup()

    details = inspect_run(LocalStorage(archive), "team/ocean/abc123")
    assert details["contains_live_data"] is True
    assert details["deletion_ready"] is False


def test_terminal_run_with_no_history_is_archived(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    config = AppConfig.model_validate(
        {
            "source": {"entity": "team"},
            "destination": {"type": "local", "path": archive},
        }
    )
    run = FakeRun()
    run.lastHistoryStep = -1
    run.download_history_exports = lambda *args, **kwargs: HistoryResult([])  # type: ignore[method-assign]
    source = WandbSource(config, api=FakeApi(run))

    result = ArchiveService(config, LocalStorage(archive), source).backup()

    assert result["archived"] == 1
    details = inspect_run(LocalStorage(archive), "team/ocean/abc123")
    assert details["complete_history"] is True
    assert details["metric_rows"] == 0
    assert details["deletion_ready"] is True


def test_missing_source_file_is_recorded_as_required_exclusion(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "archive"
    config = AppConfig.model_validate(
        {
            "source": {"entity": "team"},
            "destination": {"type": "local", "path": archive},
        }
    )
    run = FakeRun()
    run._files.append(MissingFakeFile("media/images/missing.png", b"missing"))
    source = WandbSource(config, api=FakeApi(run))

    result = ArchiveService(config, LocalStorage(archive), source).backup()

    assert result["archived"] == 1
    details = inspect_run(LocalStorage(archive), "team/ocean/abc123")
    assert details["deletion_ready"] is False
    assert {
        "source": "media/images/missing.png",
        "reason": "source object is missing from W&B storage",
        "required": "true",
    } in details["exclusions"]


def test_run_files_download_concurrently(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    config = AppConfig.model_validate(
        {
            "source": {"entity": "team"},
            "destination": {"type": "local", "path": archive},
            "archive": {"transfers": {"concurrency": 4}},
        }
    )
    run = FakeRun()
    run._files = [
        ConcurrentFakeFile(f"media/images/image-{index}.png", b"image")
        for index in range(4)
    ]
    ConcurrentFakeFile.active = 0
    ConcurrentFakeFile.maximum_active = 0
    source = WandbSource(config, api=FakeApi(run))

    ArchiveService(config, LocalStorage(archive), source).backup()

    assert ConcurrentFakeFile.maximum_active > 1
    assert verify_archive(LocalStorage(archive), deep=True)["ok"]


def test_transfer_tuning_does_not_change_source_fingerprint(tmp_path: Path) -> None:
    base = {
        "source": {"entity": "team"},
        "destination": {"type": "local", "path": tmp_path / "archive"},
    }
    first_config = AppConfig.model_validate(base)
    tuned_config = AppConfig.model_validate(
        {
            **base,
            "archive": {"transfers": {"concurrency": 8, "retries": 10}},
        }
    )
    changed_policy_config = AppConfig.model_validate(
        {**base, "archive": {"include": {"media": False}}}
    )
    run = FakeRun()

    first = WandbSource(first_config, api=FakeApi(run)).snapshot(run)
    tuned = WandbSource(tuned_config, api=FakeApi(run)).snapshot(run)
    changed_policy = WandbSource(changed_policy_config, api=FakeApi(run)).snapshot(run)

    assert first.source_fingerprint == tuned.source_fingerprint
    assert first.source_fingerprint != changed_policy.source_fingerprint
