"""Dataset loading utilities for bundled surveillance data.

This module provides functions for loading pre-processed parquet datasets that
are bundled with the package.
"""

from __future__ import annotations

from importlib import resources
from pathlib import Path
from typing import Literal

import polars as pl

from .types import AnyFrame, DatasetName, ReturnType
from .utils import resolve_return_type, to_pandas

_DATASETS = {
    "sex_prefecture": "sex_prefecture.parquet",
    "place_prefecture": "place_prefecture.parquet",
    "bullet": "bullet.parquet",
    "sentinel": "sentinel.parquet",
    "unified": "unified.parquet",
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
    name: DatasetName | Literal["sex_prefecture", "place_prefecture", "unified", "sentinel"],
    *,
    return_type: ReturnType | None = None,
) -> AnyFrame:
    """Load a bundled dataset.

    Bundled datasets are pre-processed historical data in parquet format,
    included with the package for quick access without downloading.

    Args:
        name: Dataset name:
            - "sex": Sex-disaggregated data (1999-2023)
            - "place": Place of infection data (2001-2023)
            - "bullet": Confirmed cases (2024+)
            - "sentinel": Sentinel surveillance (2024+) - RSV, HFMD, etc.
            - "unified": Combined dataset (1999-2026) - RECOMMENDED
            Aliases: "sex_prefecture", "place_prefecture"
        return_type: Desired return type ("polars" or "pandas").
            If None, uses global config.

    Returns:
        DataFrame containing the requested dataset.

    Example:
        >>> import jpinfectpy as jp
        >>> df = jp.load("unified")  # Load complete unified dataset (RECOMMENDED)
        >>> df_sex = jp.load("sex")  # Load historical sex-disaggregated data
        >>> df_sentinel = jp.load("sentinel", return_type="polars")  # Sentinel data
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
