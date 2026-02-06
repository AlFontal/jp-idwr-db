"""Validation utilities for data quality checks.

This module provides functions for validating data schemas, detecting duplicates,
and ensuring data quality across different surveillance data sources.
"""

from __future__ import annotations

from typing import cast

import polars as pl


def get_sentinel_only_diseases() -> set[str]:
    """Get sentinel-only diseases (deprecated static helper).

    Returns:
        Empty set. Sentinel-only detection is now computed dynamically in
        smart_merge() based on disease overlap with zensu data.
    """
    return set()


def validate_schema(df: pl.DataFrame, required_columns: list[str] | None = None) -> None:
    """Validate that a DataFrame has the required schema.

    Args:
        df: DataFrame to validate.
        required_columns: List of required column names. If None, uses standard schema.

    Raises:
        ValueError: If required columns are missing.
    """
    if required_columns is None:
        required_columns = ["prefecture", "year", "week", "disease", "count"]

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def validate_no_duplicates(
    df: pl.DataFrame,
    keys: list[str] | None = None,
) -> None:
    """Validate that there are no duplicate records based on key columns.

    Args:
        df: DataFrame to validate.
        keys: List of column names that define uniqueness. If None, uses
              ["prefecture", "year", "week", "disease", "category"].
              Category is included because the same (prefecture, year, week, disease)
              can have multiple categories (e.g., "male", "female", "total").

    Raises:
        ValueError: If duplicate records are found.
    """
    if keys is None:
        # Include category if it exists, since same disease can have multiple categories
        keys = ["prefecture", "year", "week", "disease"]
        if "category" in df.columns:
            keys.append("category")

    # Count occurrences of each unique combination
    dups = df.group_by(keys).agg(pl.len().alias("count")).filter(pl.col("count") > 1)

    if dups.height > 0:
        raise ValueError(
            f"Found {dups.height} duplicate records. First few duplicates:\n{dups.head(5)}"
        )


def validate_date_ranges(df: pl.DataFrame) -> None:
    """Validate that year and week values are reasonable.

    Args:
        df: DataFrame to validate.

    Raises:
        ValueError: If year or week values are out of expected ranges.
    """
    if "year" in df.columns:
        years = df["year"]
        min_year_raw, max_year_raw = years.min(), years.max()
        min_year = cast(int, min_year_raw)
        max_year = cast(int, max_year_raw)
        if min_year < 1999 or max_year > 2030:
            raise ValueError(f"Year values out of expected range: {min_year}-{max_year}")

    if "week" in df.columns:
        weeks = df["week"]
        min_week_raw, max_week_raw = weeks.min(), weeks.max()
        min_week = cast(int, min_week_raw)
        max_week = cast(int, max_week_raw)
        if min_week < 1 or max_week > 53:
            raise ValueError(f"Week values out of valid range: {min_week}-{max_week}")


def smart_merge(
    zensu_df: pl.DataFrame,
    teiten_df: pl.DataFrame,
) -> pl.DataFrame:
    """Merge zensu and teiten data, preferring confirmed (zensu) data.

    This function implements the "prefer confirmed" strategy:
    - Keep ALL zensu (confirmed case) data
    - Add ONLY sentinel diseases that are absent from zensu
    - This avoids duplication while preserving diseases only in sentinel surveillance

    Args:
        zensu_df: Confirmed case data (from zensu/bullet files).
        teiten_df: Sentinel surveillance data (from teiten files).

    Returns:
        Merged DataFrame with no duplicate diseases.

    Example:
        >>> zensu = pl.DataFrame({"disease": ["Influenza", "Tuberculosis"], "count": [100, 10]})
        >>> teiten = pl.DataFrame({"disease": ["Influenza", "RSV"], "count": [120, 50]})
        >>> merged = smart_merge(zensu, teiten)
        >>> # Result: Influenza from zensu + RSV from teiten
    """
    confirmed_diseases = (
        zensu_df.select("disease").drop_nulls().unique().get_column("disease").to_list()
    )

    # Filter teiten to only include diseases not present in confirmed data.
    teiten_filtered = teiten_df.filter(~pl.col("disease").is_in(confirmed_diseases))

    # Combine zensu (all diseases) + teiten (sentinel-only diseases)
    merged = pl.concat([zensu_df, teiten_filtered], how="diagonal_relaxed")

    return merged
