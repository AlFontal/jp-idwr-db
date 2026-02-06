"""Tests for English sentinel parser used by teitenruiXX.csv files."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from jp_idwr_db.io import _read_sentinel_en_pl


def test_read_sentinel_en_teitenrui_csv(tmp_path: Path) -> None:
    """Parse teitenrui CSV into expected long format."""
    content = """No. of Cases, Cases per Sentinel by Disease and Prefecture,,,,,,
4th week, 2025,Created on Jan 29, 2025,,,,,
,Influenza,,RS virus infection,,"Hand, foot and mouth disease",,
,Current week,per sentinel,Current week,per sentinel,Current week,per sentinel
Total No.,54594,11.06,2283,0.73,1038,0.33
Hokkaido,1794,8.08,234,1.72,47,0.35
Aomori,567,9.78,8,0.22,13,0.35
"""
    csv_path = tmp_path / "teitenrui04.csv"
    csv_path.write_text(content, encoding="utf-8")

    df = _read_sentinel_en_pl(csv_path)

    assert isinstance(df, pl.DataFrame)
    assert df.height == 6  # 2 prefectures x 3 diseases
    assert df["prefecture"].n_unique() == 2
    assert df["disease"].n_unique() == 3
    assert "Hand, foot and mouth disease" in df["disease"].unique().to_list()
    assert df["year"].unique().to_list() == [2025]
    assert df["week"].unique().to_list() == [4]
    assert df["source"].unique().to_list() == ["Sentinel surveillance"]
    assert df["per_sentinel"].null_count() == 0
