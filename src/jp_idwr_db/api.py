"""New unified API for accessing infectious disease surveillance data.

This module provides the main user-facing API for jp_idwr_db, offering
simple data access with flexible filtering capabilities.
"""

from __future__ import annotations

import logging
from typing import Literal

import polars as pl

from .datasets import scan_dataset

logger = logging.getLogger(__name__)


def get_data(
    disease: str | list[str] | None = None,
    prefecture: str | list[str] | None = None,
    year: int | tuple[int, int] | None = None,
    week: int | tuple[int, int] | None = None,
    source: Literal["confirmed", "sentinel", "all"] = "all",
    version: str | None = None,
    force_download: bool = False,
) -> pl.DataFrame:
    """Get infectious disease surveillance data with optional filtering.

    This is the main entry point for accessing jp_idwr_db data. It loads all
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
            - "confirmed": Historical confirmed cases and modern all-case reporting
            - "sentinel": Only teiten (sentinel surveillance) data
            - "all": Both sources (default)
        version: Optional release selector for packaged parquet assets.
            Use ``"latest"`` for the freshest published snapshot.
        force_download: Force a fresh download of the selected packaged dataset snapshot.

    Returns:
        DataFrame with standardized schema containing:
        - prefecture: Prefecture name
        - year: ISO year
        - week: ISO week
        - date: Week start date
        - disease: Disease name (normalized)
        - count: Weekly case count (null when unknown, see ``count_status``)
        - per_sentinel: Per-sentinel rate (sentinel only, null for confirmed)
        - source: "Confirmed cases", "All-case reporting" or "Sentinel surveillance"
        - category: "total"
        - count_status: Sentinel only: "annual" (final table), "derived"
          (preliminary), or why the count is null ("inconsistent",
          "correction", "gap", "series_start", "missing"); null for confirmed rows

    Examples:
        >>> import jp_idwr_db as jp
        >>> # Get all data
        >>> df = jp.get_data(version="latest")

        >>> # Filter by disease
        >>> flu = jp.get_data(disease="Influenza", version="latest")

        >>> # Multiple diseases, specific year
        >>> df = jp.get_data(disease=["COVID-19", "Influenza"], year=2024, version="latest")

        >>> # Prefecture and year range
        >>> tokyo = jp.get_data(prefecture="Tokyo", year=(2020, 2024), version="latest")

        >>> # Only sentinel data
        >>> sentinel = jp.get_data(source="sentinel", year=2024, version="latest")

        >>> # Complex filtering
        >>> df = jp.get_data(
        ...     disease=["Influenza", "RS virus"],
        ...     prefecture=["Tokyo", "Osaka"],
        ...     year=(2023, 2025),
        ...     source="all"
        ... )
    """
    # Loading and checksum failures must remain visible to callers. Filters are
    # applied lazily so only matching rows are read from the parquet file.
    lf = _scan_unified(source, version=version, force_download=force_download)

    if disease is not None:
        diseases = [disease] if isinstance(disease, str) else disease
        # Case-insensitive partial matching
        disease_filter = pl.lit(False)
        for d in diseases:
            disease_filter = disease_filter | pl.col("disease").str.to_lowercase().str.contains(
                d.lower(), literal=True
            )
        lf = lf.filter(disease_filter)

    if prefecture is not None:
        prefectures = [prefecture] if isinstance(prefecture, str) else prefecture
        lf = lf.filter(pl.col("prefecture").is_in(prefectures))

    if year is not None:
        if isinstance(year, tuple):
            start_year, end_year = year
            lf = lf.filter((pl.col("year") >= start_year) & (pl.col("year") <= end_year))
        else:
            lf = lf.filter(pl.col("year") == year)

    if week is not None:
        if isinstance(week, tuple):
            start_week, end_week = week
            lf = lf.filter((pl.col("week") >= start_week) & (pl.col("week") <= end_week))
        else:
            lf = lf.filter(pl.col("week") == week)

    return lf.collect()


def _scan_unified(
    source: Literal["confirmed", "sentinel", "all"],
    *,
    version: str | None,
    force_download: bool,
) -> pl.LazyFrame:
    """Scan the unified dataset, filtered by surveillance source."""
    lf = scan_dataset("unified", version=version, force_download=force_download)
    source_map = {
        "confirmed": ["Confirmed cases", "All-case reporting"],
        "sentinel": ["Sentinel surveillance"],
    }
    if source in source_map and "source" in lf.collect_schema().names():
        lf = lf.filter(pl.col("source").is_in(source_map[source]))
    return lf


def list_diseases(
    source: Literal["confirmed", "sentinel", "all"] = "all",
    *,
    version: str | None = None,
    force_download: bool = False,
) -> list[str]:
    """Get list of available disease names.

    Args:
        source: Filter by data source - "confirmed", "sentinel", or "all".
        version: Optional packaged data release selector, including ``"latest"``.
        force_download: Force a fresh download of the selected packaged dataset snapshot.

    Returns:
        Sorted list of disease names.

    Example:
        >>> import jp_idwr_db as jp
        >>> all_diseases = jp.list_diseases(version="latest")
        >>> sentinel_only = jp.list_diseases(source="sentinel", version="latest")
    """
    lf = _scan_unified(source, version=version, force_download=force_download)
    return sorted(lf.select(pl.col("disease").unique()).collect()["disease"].to_list())


def list_prefectures(*, version: str | None = None, force_download: bool = False) -> list[str]:
    """Get list of prefecture names.

    Args:
        version: Optional packaged data release selector, including ``"latest"``.
        force_download: Force a fresh download of the selected packaged dataset snapshot.

    Returns:
        Sorted list of prefecture names.

    Example:
        >>> import jp_idwr_db as jp
        >>> prefectures = jp.list_prefectures(version="latest")
        >>> print(prefectures[:3])
        ['Aichi', 'Akita', 'Aomori']
    """
    lf = _scan_unified("all", version=version, force_download=force_download)
    return sorted(lf.select(pl.col("prefecture").unique()).collect()["prefecture"].to_list())


def get_latest_week(
    *, version: str | None = None, force_download: bool = False
) -> tuple[int, int] | None:
    """Get the latest (year, week) with data available.

    Args:
        version: Optional packaged data release selector, including ``"latest"``.
        force_download: Force a fresh download of the selected packaged dataset snapshot.

    Returns:
        Tuple of (year, week) for the most recent data, or None if no data.

    Example:
        >>> import jp_idwr_db as jp
        >>> latest = jp.get_latest_week(version="latest")
        >>> if latest:
        ...     year, week = latest
        ...     print(f"Latest data: {year} week {week}")
    """
    lf = _scan_unified("all", version=version, force_download=force_download)
    # Check if year column exists, otherwise we can't determine the latest week
    if not {"year", "week"}.issubset(lf.collect_schema().names()):
        logger.warning("Cannot determine latest week: missing year or week column")
        return None

    latest = lf.select(["year", "week"]).sort(["year", "week"], descending=True).head(1).collect()
    if latest.height == 0:
        return None
    return (int(latest["year"][0]), int(latest["week"][0]))
