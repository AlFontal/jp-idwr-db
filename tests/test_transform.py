from __future__ import annotations

import polars as pl

from jp_idwr_db.transform import merge, pivot


def test_pivot_roundtrip_polars() -> None:
    long_df = pl.DataFrame(
        {
            "prefecture": ["Total", "Tokyo"],
            "year": [2024, 2024],
            "week": [1, 1],
            "date": ["2024-01-07", "2024-01-07"],
            "disease": ["Influenza", "Influenza"],
            "cases": [18, 7],
        }
    )
    wide = pivot(long_df)
    assert "Influenza" in wide.columns
    long_again = pivot(wide)
    assert set(long_again.columns) == set(long_df.columns)


def test_merge_mixed_inputs() -> None:
    df1 = pl.DataFrame(
        {
            "prefecture": ["Total"],
            "year": [2024],
            "week": [1],
            "date": ["2024-01-07"],
            "Influenza Male weekly": [10],
            "Influenza Female weekly": [8],
            "Influenza Total weekly": [18],
        }
    )
    df2 = pl.DataFrame(
        {
            "prefecture": ["Total"],
            "year": [2024],
            "week": [1],
            "date": ["2024-01-07"],
            "Influenza Total weekly": [18],
            "Influenza Unknown weekly": [0],
            "Influenza Others weekly": [0],
            "Influenza Imported weekly": [0],
        }
    )
    merged = merge(df1, df2)
    assert isinstance(merged, pl.DataFrame)
    assert merged.height == 1
