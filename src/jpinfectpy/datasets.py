"""Dataset loading utilities for bundled surveillance data.

This module provides functions for loading pre-processed parquet datasets that
are bundled with the package, as well as a convenience function for loading
all available data (historical + recent).
"""

from __future__ import annotations

import logging
from importlib import resources
from pathlib import Path
from typing import Literal

import polars as pl

from .types import AnyFrame, DatasetName, ReturnType
from .utils import resolve_return_type, to_pandas

logger = logging.getLogger(__name__)

_DATASETS = {
    "sex_prefecture": "sex_prefecture.parquet",
    "place_prefecture": "place_prefecture.parquet",
    "bullet": "bullet.parquet",
    "prefecture_en": "prefecture_en.parquet",
}


def _data_path(name: str) -> Path:
    """Get the path to a bundled dataset file.

    Args:
        name: Dataset name (must be in _DATASETS).

    Returns:
        Path to the parquet file.

    Raises:
        ValueError: If the dataset name is unknown.
    """
    try:
        filename = _DATASETS[name]
    except KeyError as exc:
        raise ValueError(f"Unknown dataset: {name}") from exc
    return Path(str(resources.files("jpinfectpy.data").joinpath(filename)))


def load_dataset(
    name: DatasetName | Literal["sex_prefecture", "place_prefecture"],
    *,
    return_type: ReturnType | None = None,
) -> AnyFrame:
    """Load a bundled dataset.

    Bundled datasets are pre-processed historical data in parquet format,
    included with the package for quick access without downloading.

    Args:
        name: Dataset name - "sex", "place", or "bullet".
            ("sex_prefecture" and "place_prefecture" are accepted aliases)
        return_type: Desired return type ("polars" or "pandas").
            If None, uses global config.

    Returns:
        DataFrame containing the requested dataset.

    Example:
        >>> import jpinfectpy as jp
        >>> df_sex = jp.load("sex")  # Load historical sex-disaggregated data
        >>> df_place = jp.load("place", return_type="polars")
    """
    # Normalize aliases
    if name == "sex":
        name = "sex_prefecture"
    elif name == "place":
        name = "place_prefecture"

    path = _data_path(name)
    df = pl.read_parquet(path)
    if resolve_return_type(return_type) == "pandas":
        return to_pandas(df)
    return df


def load_all(
    *,
    return_type: ReturnType | None = None,
) -> AnyFrame:
    """Load a fused dataset combining historical and recent data.

    This function combines:
    1. Historical sex-disaggregated data (1999-2023) from bundled datasets
    2. Recent weekly reports (2024+) downloaded from the NIID website

    The sex-disaggregated data is filtered to the 'total' category to match
    the granularity of the recent weekly reports (which do not have sex breakdown).

    Args:
        return_type: Desired return type ("polars" or "pandas").
            If None, uses global config.

    Returns:
        Combined DataFrame with a 'source' column indicating data origin
        ("historical_sex" or "recent_bullet").

    Note:
        This function may download data from the internet on first use.
        Subsequent calls will use cached data. Use `download_recent()` with
        `overwrite=True` to force re-download.

    Example:
        >>> import jpinfectpy as jp
        >>> df_all = jp.load_all()  # Historical (1999-2023) + Recent (2024+)
        >>> df_all['source'].value_counts()
    """
    # Import here to avoid circular dependency
    from .io import download_recent, read  # noqa: PLC0415

    # Load historical data (1999-2023) filtered to 'total' category
    hist_df = load_dataset("sex", return_type="polars")
    hist_df = hist_df.filter(pl.col("category") == "total")
    hist_df = hist_df.with_columns(pl.lit("historical_sex").alias("source"))

    # Download and load recent weekly reports (2024+)
    bullet_paths = download_recent()
    recent_dfs: list[pl.DataFrame] = []

    # If no recent data available, return historical only
    if not bullet_paths:
        logger.info("No recent bulletin data found. Returning historical data only.")
        if resolve_return_type(return_type) == "pandas":
            return to_pandas(hist_df)
        return hist_df

    # Read and normalize recent data
    for p in bullet_paths:
        try:
            df = read(p, type="bullet", return_type="polars")
            # Bullet data lacks 'category' column, add it as 'total'
            if "category" not in df.columns:
                df = df.with_columns(pl.lit("total").alias("category"))

            df = df.with_columns(pl.lit("recent_bullet").alias("source"))

            # Ensure column alignment for concatenation
            cols = ["prefecture", "year", "week", "date", "disease", "category", "count", "source"]
            df = df.select([c for c in cols if c in df.columns])
            recent_dfs.append(df)
        except Exception as e:
            # Log failures but continue processing other files
            logger.warning(f"Failed to read {p.name}: {e}")
            continue

    # Combine historical and recent data
    if recent_dfs:
        recent_all = pl.concat(recent_dfs, how="vertical_relaxed")
        combined = pl.concat([hist_df, recent_all], how="diagonal_relaxed")
    else:
        logger.warning("All recent bulletins failed to parse. Returning historical data only.")
        combined = hist_df

    # Sort by time and location
    combined = combined.sort(["year", "week", "prefecture"])

    if resolve_return_type(return_type) == "pandas":
        return to_pandas(combined)
    return combined


def load_prefecture_en() -> list[str]:
    """Load the list of English prefecture names.

    Returns:
        List of prefecture names in English.

    Example:
        >>> import jpinfectpy as jp
        >>> prefectures = jp.load_prefecture_en()
        >>> print(prefectures[:3])
        ['Hokkaido', 'Aomori', 'Iwate']
    """
    path = _data_path("prefecture_en")
    df = pl.read_parquet(path)
    return df.get_column("prefecture").to_list()
