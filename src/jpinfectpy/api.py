"""New unified API for accessing infectious disease surveillance data.

This module provides the main user-facing API for jpinfectpy, offering
simple data access with flexible filtering capabilities.
"""

from __future__ import annotations

import logging
from typing import Literal

import polars as pl

from .datasets import load_dataset
from .types import AnyFrame, ReturnType
from .utils import resolve_return_type, to_pandas

logger = logging.getLogger(__name__)


def get_data(
    disease: str | list[str] | None = None,
    prefecture: str | list[str] | None = None,
    year: int | tuple[int, int] | None = None,
    week: int | tuple[int, int] | None = None,
    source: Literal["confirmed", "sentinel", "all"] = "all",
    *,
    return_type: ReturnType | None = None,
) -> AnyFrame:
    """Get infectious disease surveillance data with optional filtering.

    This is the main entry point for accessing jpinfectpy data. It loads all
    available data (historical + recent, confirmed + sentinel) and applies
    optional filters.

    Args:
        disease: Filter by disease name(s). Case-insensitive partial matching.
            Examples: "Influenza", ["COVID-19", "Influenza"], "RS virus"
        prefecture: Filter by prefecture name(s).
            Examples: "Tokyo", ["Tokyo", "Osaka"]
        year: Filter by single year or (start, end) range (inclusive).
            Examples: 2024, (2020, 2024)
        week: Filter by single week or (start, end) range (inclusive).
            Examples: 10, (1, 52)
        source: Data source filter.
            - "confirmed": Only zensu (all-case reporting) data
            - "sentinel": Only teiten (sentinel surveillance) data
            - "all": Both sources (default)
        return_type: "polars" or "pandas". Defaults to global config.

    Returns:
        DataFrame with standardized schema containing:
        - prefecture: Prefecture name
        - year: ISO year
        - week: ISO week
        - date: Week start date
        - disease: Disease name (normalized)
        - count: Weekly case count
        - per_sentinel: Per-sentinel rate (sentinel only, null for confirmed)
        - source: "Confirmed cases" or "Sentinel surveillance"
        - category: "total", "male", "female" (when available)

    Examples:
        >>> import jpinfectpy as jp
        >>> # Get all data
        >>> df = jp.get_data()

        >>> # Filter by disease
        >>> flu = jp.get_data(disease="Influenza")

        >>> # Multiple diseases, specific year
        >>> df = jp.get_data(disease=["COVID-19", "Influenza"], year=2024)

        >>> # Prefecture and year range
        >>> tokyo = jp.get_data(prefecture="Tokyo", year=(2020, 2024))

        >>> # Only sentinel data
        >>> sentinel = jp.get_data(source="sentinel", year=2024)

        >>> # Complex filtering
        >>> df = jp.get_data(
        ...     disease=["Influenza", "RS virus"],
        ...     prefecture=["Tokyo", "Osaka"],
        ...     year=(2023, 2025),
        ...     source="all"
        ... )
    """
    # Load the unified bundled dataset by default.
    try:
        df = load_dataset("unified", return_type="polars")
    except Exception:
        logger.warning("Failed to load unified dataset, falling back to bullet dataset")
        try:
            df = load_dataset("bullet", return_type="polars")
        except Exception:
            logger.warning("Failed to load bullet dataset, returning empty DataFrame")
            df = pl.DataFrame()

    if df.height == 0:
        if resolve_return_type(return_type) == "pandas":
            return to_pandas(df)
        return df

    # Apply filters
    if source != "all" and "source" in df.columns:
        source_map = {
            "confirmed": ["Confirmed cases", "All-case reporting"],
            "sentinel": "Sentinel surveillance",
        }
        if source in source_map:
            target = source_map[source]
            if isinstance(target, list):
                df = df.filter(pl.col("source").is_in(target))
            else:
                df = df.filter(pl.col("source") == target)

    if disease is not None:
        diseases = [disease] if isinstance(disease, str) else disease
        # Case-insensitive partial matching
        disease_filter = pl.lit(False)
        for d in diseases:
            disease_filter = disease_filter | pl.col("disease").str.to_lowercase().str.contains(
                d.lower()
            )
        df = df.filter(disease_filter)

    if prefecture is not None:
        prefectures = [prefecture] if isinstance(prefecture, str) else prefecture
        df = df.filter(pl.col("prefecture").is_in(prefectures))

    if year is not None:
        if isinstance(year, tuple):
            start_year, end_year = year
            df = df.filter((pl.col("year") >= start_year) & (pl.col("year") <= end_year))
        else:
            df = df.filter(pl.col("year") == year)

    if week is not None:
        if isinstance(week, tuple):
            start_week, end_week = week
            df = df.filter((pl.col("week") >= start_week) & (pl.col("week") <= end_week))
        else:
            df = df.filter(pl.col("week") == week)

    # Return in requested format
    if resolve_return_type(return_type) == "pandas":
        return to_pandas(df)
    return df


def list_diseases(source: Literal["confirmed", "sentinel", "all"] = "all") -> list[str]:
    """Get list of available disease names.

    Args:
        source: Filter by data source - "confirmed", "sentinel", or "all".

    Returns:
        Sorted list of disease names.

    Example:
        >>> import jpinfectpy as jp
        >>> all_diseases = jp.list_diseases()
        >>> sentinel_only = jp.list_diseases(source="sentinel")
    """
    df = get_data(source=source, return_type="polars")
    if df.height == 0:
        return []
    return sorted(df["disease"].unique().to_list())


def list_prefectures() -> list[str]:
    """Get list of prefecture names.

    Returns:
        Sorted list of prefecture names.

    Example:
        >>> import jpinfectpy as jp
        >>> prefectures = jp.list_prefectures()
        >>> print(prefectures[:3])
        ['Aichi', 'Akita', 'Aomori']
    """
    df = get_data(return_type="polars")
    if df.height == 0:
        return []
    return sorted(df["prefecture"].unique().to_list())


def get_latest_week() -> tuple[int, int] | None:
    """Get the latest (year, week) with data available.

    Returns:
        Tuple of (year, week) for the most recent data, or None if no data.

    Example:
        >>> import jpinfectpy as jp
        >>> latest = jp.get_latest_week()
        >>> if latest:
        ...     year, week = latest
        ...     print(f"Latest data: {year} week {week}")
    """
    df = get_data(return_type="polars")
    if df.height == 0:
        return None

    # Check if year column exists, otherwise we can't determine the latest week
    if "year" not in df.columns or "week" not in df.columns:
        logger.warning("Cannot determine latest week: missing year or week column")
        return None

    # Get row with maximum year, then maximum week within that year
    latest = df.sort(["year", "week"], descending=True).head(1)
    return (int(latest["year"][0]), int(latest["week"][0]))
