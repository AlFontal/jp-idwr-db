"""Validation utilities for data quality checks.

This module provides functions for validating data schemas, detecting duplicates,
and ensuring data quality across different surveillance data sources.
"""

from __future__ import annotations

from datetime import date
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


def validate_iso_week_start_dates(df: pl.DataFrame) -> None:
    """Require ``date`` to equal the Monday of its ISO year/week."""
    required = {"year", "week", "date"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing ISO date columns: {sorted(missing)}")

    periods = df.select(["year", "week", "date"]).unique()
    mismatches = periods.filter(
        pl.col("date")
        != pl.struct(["year", "week"]).map_elements(
            lambda value: date.fromisocalendar(int(value["year"]), int(value["week"]), 1),
            return_dtype=pl.Date,
        )
    )
    if mismatches.height > 0:
        raise ValueError(
            "Date must be the Monday at the start of its ISO year/week. "
            f"First mismatches:\n{mismatches.head(10)}"
        )


def validate_non_negative_counts(df: pl.DataFrame) -> None:
    """Validate that case-count metrics do not contain negative values.

    Args:
        df: DataFrame to validate.

    Raises:
        ValueError: If negative count or per-sentinel values are found.
    """
    metric_columns = [col for col in ["count", "per_sentinel"] if col in df.columns]
    for column in metric_columns:
        negative_rows = df.filter(pl.col(column) < 0)
        if negative_rows.height > 0:
            raise ValueError(
                f"Found {negative_rows.height} rows with negative {column}. "
                f"First few rows:\n{negative_rows.head(5)}"
            )


def validate_max_null_rate(
    df: pl.DataFrame,
    column: str,
    *,
    max_rate: float,
    group_by: list[str] | None = None,
) -> None:
    """Reject datasets whose null rate exceeds a stable quality threshold.

    Args:
        df: DataFrame to validate.
        column: Column whose null rate is checked.
        max_rate: Maximum allowed null share in the inclusive range 0-1.
        group_by: Optional columns used to apply the threshold per group.

    Raises:
        ValueError: If the column is missing, the threshold is invalid, or any
            checked group exceeds the threshold.
    """
    if column not in df.columns:
        raise ValueError(f"Missing null-rate column: {column}")
    if not 0 <= max_rate <= 1:
        raise ValueError("max_rate must be between 0 and 1")

    grouping = group_by or []
    if grouping:
        missing_groups = [name for name in grouping if name not in df.columns]
        if missing_groups:
            raise ValueError(f"Missing null-rate grouping columns: {missing_groups}")
        rates = df.group_by(grouping).agg(
            (pl.col(column).null_count() / pl.len()).alias("null_rate")
        )
    else:
        rates = df.select((pl.col(column).null_count() / pl.len()).alias("null_rate"))

    failures = rates.filter(pl.col("null_rate") > max_rate)
    if failures.height > 0:
        raise ValueError(
            f"Null rate for {column} exceeds {max_rate:.1%}. "
            f"Failing groups:\n{failures.sort(grouping).head(10) if grouping else failures}"
        )


def validate_prefecture_coverage(
    df: pl.DataFrame,
    *,
    expected: int = 47,
    allowed_counts: dict[tuple[int, int], int] | None = None,
) -> None:
    """Require the expected prefecture count for every observed year/week.

    Args:
        df: Dataset with ``year``, ``week``, and ``prefecture`` columns.
        expected: Normal number of prefectures per period.
        allowed_counts: Explicit source anomalies keyed by ``(year, week)``.

    Raises:
        ValueError: If a period has an unexpected prefecture count.
    """
    required = {"year", "week", "prefecture"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing prefecture-coverage columns: {sorted(missing)}")

    exceptions = allowed_counts or {}
    coverage = df.group_by(["year", "week"]).agg(
        pl.col("prefecture").n_unique().alias("prefecture_count")
    )
    failures = [
        row
        for row in coverage.iter_rows(named=True)
        if int(row["prefecture_count"])
        != exceptions.get((int(row["year"]), int(row["week"])), expected)
    ]
    if failures:
        raise ValueError(f"Unexpected prefecture coverage. First failures: {failures[:10]}")


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
