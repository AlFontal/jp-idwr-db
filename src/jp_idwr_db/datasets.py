"""Dataset loading utilities for release-hosted parquet datasets."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import polars as pl

from .data_manager import ensure_data
from .types import DatasetName

_DATASETS = {
    "sex_prefecture": "sex_prefecture.parquet",
    "place_prefecture": "place_prefecture.parquet",
    "bullet": "bullet.parquet",
    "sentinel": "sentinel.parquet",
    "unified": "unified.parquet",
    "prefecture_en": "prefecture_en.parquet",
}


def _data_path(name: str, version: str | None = None, force: bool = False) -> Path:
    """Get the path to a cached dataset file.

    Args:
        name: Dataset name (must be in _DATASETS).
        version: Optional data release version (example: ``v0.1.0``).
        force: Force re-download if cache exists.

    Raises:
        ValueError: If the dataset name is unknown.
    """
    try:
        filename = _DATASETS[name]
    except KeyError as exc:
        raise ValueError(f"Unknown dataset: {name}") from exc
    data_dir = ensure_data(version=version, force=force)
    return Path(data_dir / filename)


def load_dataset(
    name: DatasetName | Literal["sex_prefecture", "place_prefecture", "unified", "sentinel"],
    *,
    version: str | None = None,
    force_download: bool = False,
) -> pl.DataFrame:
    """Load a dataset from local cache (downloaded from release assets when needed).

    Args:
        name: Dataset name:
            - "sex": Sex-disaggregated data (1999-2023)
            - "place": Place of infection data (2001-2023)
            - "bullet": Confirmed cases (2024+)
            - "sentinel": Sentinel surveillance (2023+) - RSV, HFMD, etc.
            - "unified": Combined dataset (1999-2026) - RECOMMENDED
            Aliases: "sex_prefecture", "place_prefecture"
        version: Optional data release version.
        force_download: Force re-download of release assets.

    Returns:
        DataFrame containing the requested dataset.

    Example:
        >>> import jp_idwr_db as jp
        >>> df = jp.load("unified")  # Load complete unified dataset (RECOMMENDED)
        >>> df_sex = jp.load("sex")  # Load historical sex-disaggregated data
        >>> df_sentinel = jp.load("sentinel")  # Sentinel data
    """
    # Normalize aliases
    if name == "sex":
        name = "sex_prefecture"
    elif name == "place":
        name = "place_prefecture"

    path = _data_path(name, version=version, force=force_download)
    return pl.read_parquet(path)


def load_prefecture_en(*, version: str | None = None, force_download: bool = False) -> list[str]:
    """Load the list of English prefecture names.

    Returns:
        List of prefecture names in English.

    Example:
        >>> import jp_idwr_db as jp
        >>> prefectures = jp.load_prefecture_en()
        >>> print(prefectures[:3])
        ['Hokkaido', 'Aomori', 'Iwate']
    """
    path = _data_path("prefecture_en", version=version, force=force_download)
    df = pl.read_parquet(path)
    return df.get_column("prefecture").to_list()
