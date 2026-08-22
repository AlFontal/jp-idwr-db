from __future__ import annotations

import re
from pathlib import Path

import pyarrow.parquet as pq
import tomllib

ROOT = Path(__file__).resolve().parents[1]


def test_dataset_documentation_row_counts_match_parquet_metadata() -> None:
    documentation = (ROOT / "docs" / "DATASETS.md").read_text(encoding="utf-8")

    for parquet_path in sorted((ROOT / "data" / "parquet").glob("*.parquet")):
        section_match = re.search(
            rf"^### `{re.escape(parquet_path.name)}`$(.*?)(?=^### |^## |\Z)",
            documentation,
            flags=re.MULTILINE | re.DOTALL,
        )
        assert section_match is not None, f"Missing DATASETS.md section for {parquet_path.name}"
        expected = pq.ParquetFile(parquet_path).metadata.num_rows
        assert f"- Rows: `{expected:,}`" in section_match.group(1)


def test_release_version_metadata_stays_synchronized() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    version = pyproject["project"]["version"]
    citation = (ROOT / "CITATION.cff").read_text(encoding="utf-8")
    uv_lock = (ROOT / "uv.lock").read_text(encoding="utf-8")

    assert re.search(rf"^version: {re.escape(version)}$", citation, flags=re.MULTILINE)
    assert re.search(
        rf'^\[\[package\]\]\nname = "jp-idwr-db"\nversion = "{re.escape(version)}"$',
        uv_lock,
        flags=re.MULTILINE,
    )
