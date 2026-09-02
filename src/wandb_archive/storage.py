"""Minimal local and S3-compatible object storage interface."""

from __future__ import annotations

import os
import tempfile
import time
from abc import ABC, abstractmethod
from pathlib import Path, PurePosixPath

import s3fs

from wandb_archive.config import AppConfig, LocalDestination, S3Destination


def _key(value: str) -> str:
    if not value:
        return ""
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"Unsafe archive key: {value!r}")
    return str(path)


class Storage(ABC):
    def __init__(self, retries: int) -> None:
        self.retries = retries

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def size(self, key: str) -> int: ...

    @abstractmethod
    def put_file(
        self, source: Path, key: str, content_type: str | None = None
    ) -> None: ...

    @abstractmethod
    def put_bytes(
        self, data: bytes, key: str, content_type: str | None = None
    ) -> None: ...

    @abstractmethod
    def read_bytes(self, key: str) -> bytes: ...

    @abstractmethod
    def get_file(self, key: str, destination: Path) -> None: ...

    @abstractmethod
    def list(self, prefix: str) -> list[str]: ...

    @abstractmethod
    def uri(self, key: str) -> str: ...

    @abstractmethod
    def public_url(self, key: str) -> str | None: ...

    def put_file_if_missing(
        self, source: Path, key: str, content_type: str | None = None
    ) -> bool:
        if self.exists(key):
            if self.size(key) != source.stat().st_size:
                raise RuntimeError(
                    f"Existing object has the wrong size: {self.uri(key)}"
                )
            return False
        self._retry(lambda: self.put_file(source, key, content_type))
        return True

    def write_bytes(
        self, data: bytes, key: str, content_type: str | None = None
    ) -> None:
        """Write a small mutable object with configured retry behavior."""

        self._retry(lambda: self.put_bytes(data, key, content_type))

    def _retry(self, operation) -> None:
        for attempt in range(self.retries + 1):
            try:
                operation()
                return
            except Exception:
                if attempt >= self.retries:
                    raise
                time.sleep(min(2**attempt, 30))


class LocalStorage(Storage):
    def __init__(self, root: Path, retries: int = 0) -> None:
        super().__init__(retries)
        self.root = root.resolve()

    def path(self, key: str) -> Path:
        path = (self.root / _key(key)).resolve()
        if not path.is_relative_to(self.root):
            raise ValueError(f"Archive key escapes destination: {key!r}")
        return path

    def exists(self, key: str) -> bool:
        return self.path(key).is_file()

    def size(self, key: str) -> int:
        return self.path(key).stat().st_size

    def put_file(self, source: Path, key: str, content_type: str | None = None) -> None:
        del content_type
        destination = self.path(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=destination.parent, delete=False
            ) as tmp:
                temporary = Path(tmp.name)
                with source.open("rb") as stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        tmp.write(block)
            os.replace(temporary, destination)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def put_bytes(self, data: bytes, key: str, content_type: str | None = None) -> None:
        del content_type
        destination = self.path(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=destination.parent, delete=False
            ) as tmp:
                temporary = Path(tmp.name)
                tmp.write(data)
            os.replace(temporary, destination)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def read_bytes(self, key: str) -> bytes:
        return self.path(key).read_bytes()

    def get_file(self, key: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        source = self.path(key)
        with source.open("rb") as src, destination.open("wb") as dst:
            for block in iter(lambda: src.read(1024 * 1024), b""):
                dst.write(block)

    def list(self, prefix: str) -> list[str]:
        base = self.path(prefix)
        if base.is_file():
            return [_key(prefix)]
        if not base.exists():
            return []
        return sorted(
            str(path.relative_to(self.root))
            for path in base.rglob("*")
            if path.is_file()
        )

    def uri(self, key: str) -> str:
        return str(self.path(key))

    def public_url(self, key: str) -> str | None:
        return None


class S3Storage(Storage):
    def __init__(
        self,
        destination: S3Destination,
        retries: int,
        *,
        anonymous: bool = False,
    ) -> None:
        super().__init__(retries)
        self.destination = destination
        self.fs = s3fs.S3FileSystem(
            anon=anonymous,
            endpoint_url=destination.endpoint_url,
        )

    def path(self, key: str) -> str:
        parts = [self.destination.bucket]
        if self.destination.prefix:
            parts.append(self.destination.prefix)
        if key:
            parts.append(_key(key))
        return "/".join(parts)

    def exists(self, key: str) -> bool:
        return bool(self.fs.exists(self.path(key)))

    def size(self, key: str) -> int:
        return int(self.fs.size(self.path(key)))

    def put_file(self, source: Path, key: str, content_type: str | None = None) -> None:
        kwargs = {"ContentType": content_type} if content_type else {}
        self.fs.put_file(str(source), self.path(key), **kwargs)

    def put_bytes(self, data: bytes, key: str, content_type: str | None = None) -> None:
        kwargs = {"ContentType": content_type} if content_type else {}
        self.fs.pipe_file(self.path(key), data, **kwargs)

    def read_bytes(self, key: str) -> bytes:
        return bytes(self.fs.cat_file(self.path(key)))

    def get_file(self, key: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.fs.get_file(self.path(key), str(destination))

    def list(self, prefix: str) -> list[str]:
        root = "/".join(
            part for part in (self.destination.bucket, self.destination.prefix) if part
        )
        full_prefix = self.path(prefix)
        found = self.fs.find(full_prefix)
        marker = root.rstrip("/") + "/"
        return sorted(path.removeprefix(marker) for path in found)

    def uri(self, key: str) -> str:
        return f"s3://{self.path(key)}"

    def public_url(self, key: str) -> str | None:
        if self.destination.public_url is None:
            return None
        return f"{self.destination.public_url}/{_key(key)}"


def build_storage(config: AppConfig, *, anonymous: bool = False) -> Storage:
    destination = config.destination
    retries = config.archive.transfers.retries
    if isinstance(destination, LocalDestination):
        return LocalStorage(destination.path, retries=retries)
    return S3Storage(destination, retries, anonymous=anonymous)
