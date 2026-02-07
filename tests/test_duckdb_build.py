from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from jp_idwr_db.duckdb_build import build_duckdb


def test_build_duckdb_creates_views_and_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    duckdb = pytest.importorskip("duckdb")
    monkeypatch.setenv("SOURCE_DATE_EPOCH", "1735689600")
    monkeypatch.setenv("JPINFECT_DATA_VERSION", "v-test")

    pl.DataFrame({"x": [1, 2, 3]}).write_parquet(tmp_path / "unified.parquet")
    pl.DataFrame({"prefecture": ["Tokyo"]}).write_parquet(tmp_path / "prefecture_en.parquet")

    out_path = tmp_path / "jp_idwr_db.duckdb"
    build_duckdb(data_dir=tmp_path, out_path=out_path)
    assert out_path.exists()

    con = duckdb.connect(out_path.as_posix())
    try:
        metadata_rows = con.execute("SELECT key, value FROM metadata ORDER BY key").fetchall()
        metadata = dict(metadata_rows)
        assert metadata["dataset_id"] == "jp_idwr_db"
        assert metadata["data_version"] == "v-test"
        assert metadata["built_at"] == "2025-01-01T00:00:00Z"

        row_count = con.execute("SELECT COUNT(*) FROM unified").fetchone()
        assert row_count is not None
        assert row_count[0] == 3

        view_sql = con.execute(
            "SELECT sql FROM duckdb_views() WHERE view_name = 'unified'"
        ).fetchone()
        assert view_sql is not None
        assert "read_parquet('unified.parquet')" in view_sql[0]
    finally:
        con.close()
