"""Data transformation functions for reshaping and merging surveillance data.

This module provides utilities for merging multiple datasets and pivoting between
wide and long formats, with automatic handling of column naming conventions.
"""

from __future__ import annotations

import polars as pl


def _infer_dataset_type(df: pl.DataFrame) -> str:
    """Infer the dataset type from column names.

    Args:
        df: Input DataFrame.

    Returns:
        One of: "long", "bullet", "sex", "place", or "unknown".

    Note:
        This heuristic checks for specific column patterns to determine
        the dataset structure. Long format has "disease" and "cases" columns.
        Bullet (weekly reports) has columns with "weekly" or "cumulative".
        Sex/Place datasets have specific column count patterns.
    """
    cols = df.columns
    if "disease" in cols and "cases" in cols:
        return "long"
    lowered = [c.lower() for c in cols]
    if any("weekly" in c or "cumulative" in c or "total" in c for c in lowered):
        return "bullet"
    # Heuristic: sex data has groups of 3 (total/male/female)
    # place data has groups of 4 (total/japan/others/unknown)
    col_count = df.width - 4  # Subtract key columns
    if col_count > 0 and col_count % 3 == 0:
        return "sex"
    if col_count > 0 and col_count % 4 == 0:
        return "place"
    return "unknown"


def _col_join_rename(df: pl.DataFrame) -> pl.DataFrame:
    """Rename columns to ensure consistent naming across datasets.

    This function standardizes column names for merging compatibility:
    - In place datasets: "Unknown" -> "Unknown place", "Others" -> "Other places"
    - In bullet datasets: "weekly" -> "total"

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with standardized column names.
    """
    dataset_type = _infer_dataset_type(df)
    mapping: dict[str, str] = {}
    if dataset_type == "place":
        for name in df.columns:
            mapping[name] = name.replace("Unknown", "Unknown place").replace(
                "Others", "Other places"
            )
    elif dataset_type == "bullet":
        for name in df.columns:
            mapping[name] = name.replace("weekly", "total")
    if mapping:
        return df.rename(mapping)
    return df


def merge(*dfs: pl.DataFrame) -> pl.DataFrame:
    """Merge multiple datasets with automatic column renaming.

    Performs a full outer join on the first two DataFrames using key columns
    (prefecture, year, week, date), then concatenates any additional DataFrames
    vertically with schema relaxation.

    Args:
        *dfs: Two or more Polars DataFrames to merge.

    Returns:
        Merged Polars DataFrame.

    Raises:
        ValueError: If fewer than two DataFrames are provided.

    Example:
        >>> df1 = jp.read("file1.xlsx", type="sex")
        >>> df2 = jp.read("file2.xlsx", type="place")
        >>> merged = jp.merge(df1, df2)
    """
    if len(dfs) < 2:
        raise ValueError("merge requires at least two dataframes")

    polars_frames = [_col_join_rename(df) for df in dfs]

    key_cols = ["prefecture", "year", "week", "date"]
    merged = polars_frames[0].join(polars_frames[1], on=key_cols, how="full")

    if len(polars_frames) > 2:
        merged = pl.concat([merged, *polars_frames[2:]], how="diagonal_relaxed")

    return merged


def pivot(df: pl.DataFrame) -> pl.DataFrame:
    """Pivot between wide and long formats.

    Automatically detects the input format and converts:
    - Long format (disease, cases columns) → Wide format (disease columns)
    - Wide format (disease columns) → Long format (disease, cases columns)

    Args:
        df: Input Polars DataFrame.

    Returns:
        Pivoted Polars DataFrame.

    Raises:
        ValueError: If the DataFrame is missing required columns for pivoting.

    Example:
        >>> long_df = jp.load("sex")  # Has disease, cases columns
        >>> wide_df = jp.pivot(long_df)  # Now has disease names as columns
        >>> long_again = jp.pivot(wide_df)  # Back to long format
    """
    frame = df
    key_cols = ["prefecture", "year", "week", "date"]
    cols = set(frame.columns)

    if "disease" in cols and "cases" in cols:
        # Long -> Wide
        result = frame.pivot(values="cases", index=key_cols, on="disease")
    else:
        # Wide -> Long
        missing = [col for col in key_cols if col not in cols]
        if missing:
            missing_labels = ", ".join(missing)
            raise ValueError(
                "pivot expects either long-form data with 'disease' and 'cases' "
                f"or wide-form data with key columns. Missing: {missing_labels}"
            )
        result = frame.unpivot(index=key_cols, variable_name="disease", value_name="cases")

    return result
