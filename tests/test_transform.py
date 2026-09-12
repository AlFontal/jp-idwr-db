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
    wide = pivot(long_df, values="cases")
    assert "Influenza" in wide.columns
    long_again = pivot(wide, values="cases")
    assert set(long_again.columns) == set(long_df.columns)


def test_pivot_supports_current_count_schema() -> None:
    long_df = pl.DataFrame(
        {
            "prefecture": ["Tokyo", "Tokyo"],
            "year": [2024, 2024],
            "week": [1, 1],
            "date": ["2024-01-01", "2024-01-01"],
            "category": ["total", "total"],
            "source": ["All-case reporting", "All-case reporting"],
            "disease": ["Influenza", "Measles"],
            "count": [18.0, 2.0],
        }
    )

    wide = pivot(long_df)
    assert wide.select(["Influenza", "Measles"]).row(0) == (18.0, 2.0)
    assert set(pivot(wide).columns) == set(long_df.columns)


def test_merge_normalized_inputs_concatenates_without_many_to_many_join() -> None:
    first = pl.DataFrame(
        {
            "prefecture": ["Tokyo"],
            "year": [2024],
            "week": [1],
            "disease": ["Influenza"],
            "count": [18],
            "source": ["Sentinel surveillance"],
        }
    )
    second = first.with_columns(pl.lit("Measles").alias("disease"))

    merged = merge(first, second)

    assert merged.height == 2


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
