from typing import Any

from wandb_archive import storage as storage_module
from wandb_archive.config import S3Destination
from wandb_archive.storage import S3Storage


class FlakyFileSystem:
    def __init__(self) -> None:
        self.exists_attempts = 0

    def exists(self, path: str) -> bool:
        del path
        self.exists_attempts += 1
        if self.exists_attempts < 3:
            raise OSError("408 Request Timeout")
        return True


def test_s3_reads_use_exponential_full_jitter(monkeypatch: Any) -> None:
    filesystem = FlakyFileSystem()
    delays: list[float] = []
    monkeypatch.setattr(
        storage_module.s3fs,
        "S3FileSystem",
        lambda **kwargs: filesystem,
    )
    monkeypatch.setattr(storage_module.random, "random", lambda: 0.5)
    monkeypatch.setattr(storage_module.time, "sleep", delays.append)

    storage = S3Storage(
        S3Destination(type="s3", bucket="archive"),
        retries=2,
    )

    assert storage.exists("runs/example/latest.json")
    assert filesystem.exists_attempts == 3
    assert delays == [0.5, 1.0]
