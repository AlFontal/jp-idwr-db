from __future__ import annotations

import pandas as pd
import polars as pl

import jpinfectpy as jp


def test_prefecture_map_shape() -> None:
    df = jp.prefecture_map(return_type="polars")
    assert isinstance(df, pl.DataFrame)
    assert df.height == 47
    assert set(df.columns) == {"prefecture", "prefecture_id"}
    assert "JP-13" in df["prefecture_id"].to_list()


def test_attach_prefecture_id_polars() -> None:
    df = pl.DataFrame({"prefecture": ["Tokyo", "Hokkaido"], "count": [1, 2]})
    out = jp.attach_prefecture_id(df)
    assert isinstance(out, pl.DataFrame)
    assert "prefecture_id" in out.columns
    assert set(out["prefecture_id"].to_list()) == {"JP-01", "JP-13"}


def test_attach_prefecture_id_pandas() -> None:
    df = pd.DataFrame({"prefecture": ["Tokyo"], "count": [1]})
    out = jp.attach_prefecture_id(df)
    assert isinstance(out, pd.DataFrame)
    assert out.loc[0, "prefecture_id"] == "JP-13"
