from pathlib import Path

import pytest
from pydantic import ValidationError

from wandb_archive.config import load_config


def test_relative_include_and_unknown_field(tmp_path: Path) -> None:
    destination = tmp_path / "destination.yaml"
    destination.write_text(f"type: local\npath: {tmp_path / 'archive'}\n")
    config = tmp_path / "archive.yaml"
    config.write_text(
        "source:\n  entity: example\ndestination: !include destination.yaml\n"
    )

    loaded = load_config(config)

    assert loaded.source.entity == "example"
    assert loaded.destination.type == "local"

    config.write_text(
        "source:\n  entity: example\n  typo: true\n"
        "destination: !include destination.yaml\n"
    )
    with pytest.raises(ValidationError):
        load_config(config)
