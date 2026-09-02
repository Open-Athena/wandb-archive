"""Durable, queryable archives of Weights & Biases experiments."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("wandb-archive")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"

__all__ = ["__version__"]
