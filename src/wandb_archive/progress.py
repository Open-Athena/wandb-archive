"""Small terminal progress abstraction used by discovery and backup."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import TypeVar

from tqdm.auto import tqdm

T = TypeVar("T")


class Progress:
    """Create consistently styled progress bars that can be disabled centrally."""

    def __init__(self, enabled: bool = False) -> None:
        self.enabled = enabled

    def track(
        self,
        iterable: Iterable[T],
        *,
        description: str,
        total: int | None = None,
        unit: str = "item",
        leave: bool = False,
    ) -> Iterator[T]:
        yield from tqdm(
            iterable,
            desc=description,
            total=total,
            unit=unit,
            disable=not self.enabled,
            dynamic_ncols=True,
            leave=leave,
        )
