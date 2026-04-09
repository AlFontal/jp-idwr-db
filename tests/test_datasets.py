from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from jp_idwr_db import datasets
from jp_idwr_db.datasets import load_dataset, scan_dataset


def test_load_dataset() -> None:
    df = load_dataset("sex_prefecture")
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0


def test_sex_dataset_has_three_categories() -> None:
    """Sex dataset should expose total/male/female categories."""
    df = load_dataset("sex_prefecture")
    cats = set(df["category"].drop_nulls().unique().to_list())
    assert {"total", "male", "female"}.issubset(cats)


def test_unified_dataset_total_only_category() -> None:
    """Unified dataset should be category-harmonized to total only."""
    df = load_dataset("unified")
    cats = set(df["category"].drop_nulls().unique().to_list())
    assert cats == {"total"}


def test_scan_dataset_returns_lazyframe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_dir = tmp_path / "release_data"
    data_dir.mkdir()
    pl.DataFrame({"prefecture": ["Tokyo"], "year": [2024]}).write_parquet(
        data_dir / "unified.parquet"
    )

    monkeypatch.setattr(datasets, "ensure_data", lambda **_: data_dir)

    lazy_df = scan_dataset("unified")

    assert isinstance(lazy_df, pl.LazyFrame)
    collected = lazy_df.collect()
    assert collected.to_dict(as_series=False) == {"prefecture": ["Tokyo"], "year": [2024]}


def test_scan_dataset_supports_aliases(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_dir = tmp_path / "release_data"
    data_dir.mkdir()
    pl.DataFrame({"count": [1]}).write_parquet(data_dir / "sex_prefecture.parquet")

    monkeypatch.setattr(datasets, "ensure_data", lambda **_: data_dir)

    collected = scan_dataset("sex").collect()

    assert collected.to_dict(as_series=False) == {"count": [1]}
