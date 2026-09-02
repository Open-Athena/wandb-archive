"""Command-line interface for W&B Archive."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Sequence

from wandb_archive import __version__
from wandb_archive.config import load_config
from wandb_archive.service import ArchiveService, BackupFailures
from wandb_archive.storage import build_storage
from wandb_archive.verify import inspect_run, verify_archive


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wandb-archive",
        description="Publish durable, queryable W&B experiment archives.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("plan", "backup"):
        child = subparsers.add_parser(command)
        child.add_argument("config")
        selection = child.add_mutually_exclusive_group()
        selection.add_argument("--project")
        selection.add_argument("--run", dest="run_path")
        child.add_argument("--since")
    verify = subparsers.add_parser("verify")
    verify.add_argument("config")
    verify.add_argument("--deep", action="store_true")
    verify.add_argument("--anonymous", action="store_true")
    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("config")
    inspect.add_argument("run_path")
    inspect.add_argument("--anonymous", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        config = load_config(args.config)
        anonymous = bool(getattr(args, "anonymous", False))
        storage = build_storage(config, anonymous=anonymous)
        if args.command in {"plan", "backup"}:
            service = ArchiveService(config, storage)
            method = service.plan if args.command == "plan" else service.backup
            result = method(
                project=args.project,
                run_path=args.run_path,
                since=args.since,
            )
        elif args.command == "verify":
            result = verify_archive(storage, deep=args.deep)
        else:
            result = inspect_run(storage, args.run_path)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except BackupFailures as error:
        if error.result is not None:
            print(json.dumps(error.result, indent=2, sort_keys=True))
        print(f"error: {error}", file=sys.stderr)
        return 1
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
