"""Stable models shared by discovery, export, and publication."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

ARCHIVE_SCHEMA_VERSION = 1
TERMINAL_STATES = frozenset({"finished", "failed", "crashed", "killed", "preempted"})


class ArchiveModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SourceFile(ArchiveModel):
    name: str
    size: int
    md5: str | None = None
    mimetype: str | None = None
    updated_at: str | None = None


class SourceArtifact(ArchiveModel):
    direction: Literal["logged", "used"]
    id: str | None = None
    name: str
    version: str | None = None
    type: str | None = None
    digest: str | None = None
    size: int | None = None
    aliases: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    manifest: dict[str, Any] = Field(default_factory=dict)


class RunSnapshot(ArchiveModel):
    entity: str
    project: str
    run_id: str
    name: str
    state: str
    created_at: str | None = None
    updated_at: str | None = None
    last_history_step: int | None = None
    url: str | None = None
    group: str | None = None
    job_type: str | None = None
    notes: str | None = None
    tags: list[str] = Field(default_factory=list)
    sweep_name: str | None = None
    user: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    summary: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    files: list[SourceFile] = Field(default_factory=list)
    artifacts: list[SourceArtifact] = Field(default_factory=list)
    source_fingerprint: str

    @property
    def path(self) -> str:
        return f"{self.entity}/{self.project}/{self.run_id}"


class StagedObject(ArchiveModel):
    logical_path: str
    local_path: Path = Field(exclude=True)
    sha256: str
    size: int
    media_type: str
    kind: str
    source_name: str | None = None
    row_count: int | None = None


class ExportResult(ArchiveModel):
    snapshot: RunSnapshot
    complete_history: bool
    contains_live_data: bool
    exclusions: list[dict[str, str]] = Field(default_factory=list)
    objects: list[StagedObject] = Field(default_factory=list)
    metric_rows: int = 0
    system_metric_rows: int = 0
    histogram_rows: int = 0
    table_rows: int = 0


class ObjectManifest(ArchiveModel):
    logical_path: str
    sha256: str
    size: int
    media_type: str
    kind: str
    source_name: str | None = None
    row_count: int | None = None
    blob_path: str


class RunManifest(ArchiveModel):
    archive_schema_version: int = ARCHIVE_SCHEMA_VERSION
    exporter_version: str
    archived_at: str = Field(
        default_factory=lambda: dt.datetime.now(dt.UTC).isoformat()
    )
    run_path: str
    source_fingerprint: str
    source_state: str
    complete_history: bool
    contains_live_data: bool
    policy_complete: bool
    deletion_ready: bool
    exclusions: list[dict[str, str]] = Field(default_factory=list)
    objects: list[ObjectManifest] = Field(default_factory=list)
    metric_rows: int = 0
    system_metric_rows: int = 0
    histogram_rows: int = 0
    table_rows: int = 0
