from __future__ import annotations

import polars as pl

import jp_idwr_db as jp


def test_prefecture_map_shape() -> None:
    mapping = jp.prefecture_map()
    assert isinstance(mapping, dict)
    assert len(mapping) == 47
    assert mapping["Tokyo"] == "JP-13"


def test_attach_prefecture_id_polars() -> None:
    df = pl.DataFrame({"prefecture": ["Tokyo", "Hokkaido"], "count": [1, 2]})
    out = jp.attach_prefecture_id(df)
    assert isinstance(out, pl.DataFrame)
    assert "prefecture_id" in out.columns
    assert set(out["prefecture_id"].to_list()) == {"JP-01", "JP-13"}
