"""Prefecture mapping helpers.

This module provides a compact prefecture -> ISO code map and helper functions
to attach ISO identifiers to DataFrames on demand.
"""

from __future__ import annotations

from typing import Final, cast

import pandas as pd
import polars as pl

from .types import AnyFrame, ReturnType
from .utils import resolve_return_type

_PREFECTURE_NAMES: Final[list[str]] = [
    "Hokkaido",
    "Aomori",
    "Iwate",
    "Miyagi",
    "Akita",
    "Yamagata",
    "Fukushima",
    "Ibaraki",
    "Tochigi",
    "Gunma",
    "Saitama",
    "Chiba",
    "Tokyo",
    "Kanagawa",
    "Niigata",
    "Toyama",
    "Ishikawa",
    "Fukui",
    "Yamanashi",
    "Nagano",
    "Gifu",
    "Shizuoka",
    "Aichi",
    "Mie",
    "Shiga",
    "Kyoto",
    "Osaka",
    "Hyogo",
    "Nara",
    "Wakayama",
    "Tottori",
    "Shimane",
    "Okayama",
    "Hiroshima",
    "Yamaguchi",
    "Tokushima",
    "Kagawa",
    "Ehime",
    "Kochi",
    "Fukuoka",
    "Saga",
    "Nagasaki",
    "Kumamoto",
    "Oita",
    "Miyazaki",
    "Kagoshima",
    "Okinawa",
]

PREFECTURE_ISO_MAP: Final[dict[str, str]] = {
    name: f"JP-{idx:02d}" for idx, name in enumerate(_PREFECTURE_NAMES, start=1)
}
"""Mapping from English prefecture name to ISO 3166-2 code."""


def prefecture_map(*, return_type: ReturnType = "polars") -> AnyFrame:
    """Return the prefecture -> ISO code map as a DataFrame.

    Args:
        return_type: Target frame type, either ``"polars"`` or ``"pandas"``.

    Returns:
        A DataFrame with columns ``prefecture`` and ``prefecture_id``.
    """
    rows = [
        {"prefecture": name, "prefecture_id": PREFECTURE_ISO_MAP[name]}
        for name in _PREFECTURE_NAMES
    ]
    if resolve_return_type(return_type) == "pandas":
        return pd.DataFrame(rows)
    return pl.DataFrame(rows)


def attach_prefecture_id(
    df: AnyFrame,
    *,
    prefecture_col: str = "prefecture",
    id_col: str = "prefecture_id",
) -> AnyFrame:
    """Attach ISO prefecture codes to a Polars or Pandas DataFrame.

    Args:
        df: Input DataFrame with a prefecture column.
        prefecture_col: Name of the prefecture column.
        id_col: Output name for the ISO code column.

    Returns:
        The input DataFrame with an added ISO code column.
    """
    if isinstance(df, pl.DataFrame):
        mapping = cast(pl.DataFrame, prefecture_map(return_type="polars")).rename(
            {"prefecture_id": id_col}
        )
        return df.join(mapping, left_on=prefecture_col, right_on="prefecture", how="left")

    if isinstance(df, pd.DataFrame):
        mapping_pd = cast(pd.DataFrame, prefecture_map(return_type="pandas")).rename(
            columns={"prefecture": prefecture_col, "prefecture_id": id_col}
        )
        return df.merge(
            mapping_pd,
            how="left",
            on=prefecture_col,
        )

    raise TypeError(f"Unsupported DataFrame type: {type(df)!r}")
