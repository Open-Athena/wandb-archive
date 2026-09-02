"""Validated YAML configuration for W&B Archive."""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator
from yaml.nodes import ScalarNode


class ConfigModel(BaseModel):
    """Base model that rejects misspelled configuration fields."""

    model_config = ConfigDict(extra="forbid")


class ProjectSelection(ConfigModel):
    include: list[str] = Field(default_factory=lambda: ["*"])
    exclude: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def require_include_pattern(self) -> Self:
        if not self.include:
            raise ValueError("projects.include must contain at least one pattern")
        return self


TerminalRunState = Literal["finished", "failed", "crashed", "killed", "preempted"]
RunState = TerminalRunState | Literal["running", "pending", "preempting"]


def _terminal_states() -> list[RunState]:
    return ["finished", "failed", "crashed", "killed", "preempted"]


class RunSelection(ConfigModel):
    states: list[RunState] = Field(default_factory=_terminal_states)
    created_after: str | None = None
    created_before: str | None = None
    include_tags: list[str] = Field(default_factory=list)
    exclude_tags: list[str] = Field(default_factory=list)


class SourceConfig(ConfigModel):
    entity: str = Field(min_length=1)
    base_url: str | None = None
    projects: ProjectSelection = Field(default_factory=ProjectSelection)
    runs: RunSelection = Field(default_factory=RunSelection)


class LocalDestination(ConfigModel):
    type: Literal["local"] = "local"
    path: Path

    @model_validator(mode="after")
    def normalize_path(self) -> Self:
        self.path = self.path.expanduser().resolve()
        return self


class S3Destination(ConfigModel):
    type: Literal["s3"] = "s3"
    bucket: str = Field(min_length=1)
    prefix: str = ""
    endpoint_url: str | None = None
    public_url: str | None = None

    @model_validator(mode="after")
    def normalize_paths(self) -> Self:
        self.prefix = self.prefix.strip("/")
        if ".." in PurePosixPath(self.prefix).parts:
            raise ValueError("destination.prefix cannot contain '..'")
        if "/" in self.bucket:
            raise ValueError("destination.bucket must be a bucket name")
        if self.public_url is not None:
            self.public_url = self.public_url.rstrip("/")
        return self


DestinationConfig = Annotated[
    LocalDestination | S3Destination, Field(discriminator="type")
]

ContentPolicy = Literal["all", "safe", "metadata", "none"]
ArtifactPolicy = ContentPolicy | Literal["references"]


class IncludeConfig(ConfigModel):
    histories: bool = True
    system_metrics: bool = True
    media: bool = True
    tables: bool = True
    run_files: ContentPolicy = "safe"
    logged_artifacts: ContentPolicy = "safe"
    used_artifacts: ArtifactPolicy = "references"
    code: bool = False
    console_logs: bool = False


class SecurityConfig(ConfigModel):
    profile: Literal["public-safe", "private"] = "public-safe"
    on_sensitive_value: Literal["fail", "exclude"] = "fail"


class TransferConfig(ConfigModel):
    concurrency: int = Field(default=4, ge=1, le=32)
    retries: int = Field(default=5, ge=0, le=20)


class ArchiveOptions(ConfigModel):
    strict: bool = True
    include: IncludeConfig = Field(default_factory=IncludeConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    transfers: TransferConfig = Field(default_factory=TransferConfig)


class AppConfig(ConfigModel):
    source: SourceConfig
    destination: DestinationConfig
    archive: ArchiveOptions = Field(default_factory=ArchiveOptions)

    def save_resolved(self, path: Path) -> None:
        path.write_text(
            yaml.safe_dump(self.model_dump(mode="json"), sort_keys=False),
            encoding="utf-8",
        )


class _IncludeLoader(yaml.SafeLoader):
    """Safe YAML loader that resolves ``!include`` relative to its file."""

    source_path: Path


def _construct_include(loader: _IncludeLoader, node: ScalarNode) -> Any:
    relative = loader.construct_scalar(node)
    return _load_yaml(loader.source_path.parent / relative)


_IncludeLoader.add_constructor("!include", _construct_include)


def _load_yaml(path: Path) -> Any:
    resolved = path.expanduser().resolve()
    with resolved.open(encoding="utf-8") as stream:
        loader = _IncludeLoader(stream)
        loader.source_path = resolved
        try:
            return loader.get_single_data()
        finally:
            loader.dispose()


def load_config(path: str | os.PathLike[str]) -> AppConfig:
    """Load and validate an archive configuration file."""

    config_path = Path(path).expanduser().resolve()
    if not config_path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    data = _load_yaml(config_path)
    if not isinstance(data, dict):
        raise ValueError(f"Configuration root must be a mapping: {config_path}")
    return AppConfig.model_validate(data)
