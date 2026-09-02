import json
from pathlib import Path

import pyarrow.parquet as pq

from wandb_archive.normalize import table_json_to_parquet


def test_wandb_table_to_parquet_preserves_mixed_values(tmp_path: Path) -> None:
    source = tmp_path / "example.table.json"
    destination = tmp_path / "example.parquet"
    source.write_text(
        json.dumps(
            {
                "columns": ["step", "label", "metadata"],
                "data": [[1, "a", {"nested": True}], [2, "b", None]],
            }
        )
    )

    assert table_json_to_parquet(source, destination) == 2
    result = pq.read_table(destination).to_pylist()
    assert result[0]["step"] == 1.0
    assert json.loads(result[0]["metadata"]) == {"nested": True}
