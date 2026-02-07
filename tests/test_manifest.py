from __future__ import annotations

import hashlib
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from jp_idwr_db.manifest import build_manifest


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def test_build_manifest_includes_file_size_and_sha256(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SOURCE_DATE_EPOCH", "1735689600")  # 2025-01-01T00:00:00Z
    parquet_path = tmp_path / "unified.parquet"
    pq.write_table(pa.table({"year": [2024], "week": [1], "count": [1]}), parquet_path)

    manifest_path = tmp_path / "manifest.json"
    manifest = build_manifest(
        data_dir=tmp_path,
        release_tag="v1.2.3",
        base_url="https://example.invalid/v1.2.3",
        out_path=manifest_path,
    )

    assert manifest_path.exists()
    assert manifest["release_tag"] == "v1.2.3"
    assert manifest["published_at"] == "2025-01-01T00:00:00Z"
    table = manifest["tables"][0]
    assert table["file"] == "unified.parquet"
    assert table["size_bytes"] == parquet_path.stat().st_size
    assert table["sha256"] == _sha256(parquet_path)


def test_build_manifest_portable_schema_mapping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SOURCE_DATE_EPOCH", "1735689600")
    parquet_path = tmp_path / "typed.parquet"
    table = pa.table(
        {
            "date_col": pa.array([date(2024, 1, 1)], type=pa.date32()),
            "datetime_col": pa.array([datetime(2024, 1, 1, 12, 0, 0)], type=pa.timestamp("us")),
            "int_col": pa.array([1], type=pa.int16()),
            "float_col": pa.array([1.5], type=pa.float32()),
            "bool_col": pa.array([True], type=pa.bool_()),
            "str_col": pa.array(["a"], type=pa.string()),
            "cat_col": pa.array(
                ["x"],
                type=pa.dictionary(index_type=pa.int8(), value_type=pa.string()),
            ),
            "fallback_col": pa.array([b"a"], type=pa.binary()),
        }
    )
    pq.write_table(table, parquet_path)

    manifest = build_manifest(
        data_dir=tmp_path,
        release_tag="v1.2.3",
        base_url="https://example.invalid/v1.2.3",
        out_path=tmp_path / "manifest.json",
    )
    schema = {item["name"]: item for item in manifest["tables"][0]["schema"]}
    assert schema["date_col"]["dtype"] == "date"
    assert schema["datetime_col"]["dtype"] == "datetime"
    assert schema["int_col"]["dtype"] == "int64"
    assert schema["float_col"]["dtype"] == "float64"
    assert schema["bool_col"]["dtype"] == "bool"
    assert schema["str_col"]["dtype"] == "string"
    assert schema["cat_col"]["dtype"] == "categorical"
    assert schema["fallback_col"]["dtype"] == "string"
    assert "note" in schema["fallback_col"]


def test_build_manifest_has_stable_table_and_schema_ordering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SOURCE_DATE_EPOCH", "1735689600")
    pq.write_table(pa.table({"z": [1], "a": [2]}), tmp_path / "b.parquet")
    pq.write_table(pa.table({"z": [3], "a": [4]}), tmp_path / "a.parquet")
    (tmp_path / "jp_idwr_db.duckdb").write_bytes(b"duckdb")

    manifest = build_manifest(
        data_dir=tmp_path,
        release_tag="v1.2.3",
        base_url="https://example.invalid/v1.2.3",
        out_path=tmp_path / "manifest.json",
    )

    table_names = [table["name"] for table in manifest["tables"]]
    assert table_names == sorted(table_names)

    a_schema_names = [column["name"] for column in manifest["tables"][0]["schema"]]
    b_schema_names = [column["name"] for column in manifest["tables"][1]["schema"]]
    assert a_schema_names == sorted(a_schema_names)
    assert b_schema_names == sorted(b_schema_names)
