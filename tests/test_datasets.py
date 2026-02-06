from __future__ import annotations

import polars as pl

from jp_idwr_db.datasets import load_dataset


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
