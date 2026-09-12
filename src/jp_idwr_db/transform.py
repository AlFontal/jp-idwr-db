"""Data transformation functions for reshaping and merging surveillance data.

This module provides utilities for merging multiple datasets and pivoting between
wide and long formats, with automatic handling of column naming conventions.
"""

from __future__ import annotations

import polars as pl

_IDENTIFIER_COLUMNS = ("prefecture", "year", "week", "date", "category", "source")


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

    Normalized long-form datasets are concatenated and deduplicated by their
    analytical key. Legacy wide-form inputs are full-joined on their shared
    surveillance period columns.

    Args:
        *dfs: Two or more Polars DataFrames to merge.

    Returns:
        Merged Polars DataFrame.

    Raises:
        ValueError: If fewer than two DataFrames are provided.

    Example:
        >>> df1 = jp.load("bullet")
        >>> df2 = jp.load("sentinel")
        >>> merged = jp.merge(df1, df2)
    """
    if len(dfs) < 2:
        raise ValueError("merge requires at least two dataframes")

    if all({"disease", "count"}.issubset(df.columns) for df in dfs):
        merged = pl.concat(dfs, how="diagonal_relaxed")
        keys = ["prefecture", "year", "week", "disease"]
        if "category" in merged.columns:
            keys.append("category")
        if "source" in merged.columns:
            keys.append("source")
        return merged.unique(subset=keys, keep="first", maintain_order=True)

    polars_frames = [_col_join_rename(df) for df in dfs]

    key_cols = ["prefecture", "year", "week", "date"]
    merged = polars_frames[0].join(polars_frames[1], on=key_cols, how="full")

    if len(polars_frames) > 2:
        merged = pl.concat([merged, *polars_frames[2:]], how="diagonal_relaxed")

    return merged


def pivot(
    df: pl.DataFrame,
    *,
    values: str | None = None,
    index: list[str] | None = None,
) -> pl.DataFrame:
    """Pivot between wide and long formats.

    Automatically detects the input format and converts between normalized
    long form and disease-column wide form. Current datasets use ``count``;
    callers working with the legacy ``cases`` metric can select it explicitly.

    Args:
        df: Input Polars DataFrame.
        values: Metric column for long-to-wide conversion, or output metric
            name for wide-to-long conversion. Defaults to ``count`` when
            present, otherwise ``cases`` for legacy long-form inputs; defaults
            to ``count`` for wide-form inputs.
        index: Identifier columns to preserve. Defaults to the surveillance
            identifiers present in the frame.

    Returns:
        Pivoted Polars DataFrame.

    Raises:
        ValueError: If the DataFrame is missing required columns for pivoting.

    Example:
        >>> long_df = jp.load("sex")  # Has disease and count columns
        >>> wide_df = jp.pivot(long_df)  # Now has disease names as columns
        >>> long_again = jp.pivot(wide_df)  # Back to long format
    """
    frame = df
    cols = set(frame.columns)
    index_cols = index or [name for name in _IDENTIFIER_COLUMNS if name in cols]

    if "disease" in cols:
        # Long -> Wide
        value_col = values or ("count" if "count" in cols else "cases" if "cases" in cols else None)
        if value_col is None or value_col not in cols:
            raise ValueError("long-form pivot requires a 'count' or 'cases' metric column")
        result = frame.pivot(values=value_col, index=index_cols, on="disease")
    else:
        # Wide -> Long
        required_keys = ["prefecture", "year", "week", "date"]
        missing = [col for col in required_keys if col not in cols]
        if missing:
            missing_labels = ", ".join(missing)
            raise ValueError(
                "pivot expects either long-form data with 'disease' and a metric "
                f"or wide-form data with key columns. Missing: {missing_labels}"
            )
        value_name = values or "count"
        result = frame.unpivot(
            index=index_cols,
            on=[name for name in frame.columns if name not in index_cols],
            variable_name="disease",
            value_name=value_name,
        )

    return result
