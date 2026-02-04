"""Utility functions for DataFrame type conversion and configuration.

This module provides helper functions for converting between Polars and Pandas
DataFrames, as well as utilities for resolving return type preferences.
"""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd
import polars as pl

from .config import get_config
from .types import AnyFrame, ReturnType


def resolve_return_type(return_type: ReturnType | None) -> ReturnType:
    """Resolve the return type, falling back to global config if not specified.

    Args:
        return_type: Explicitly requested return type, or None to use config default.

    Returns:
        The resolved return type ("pandas" or "polars").
    """
    if return_type is not None:
        return return_type
    return get_config().return_type


def to_polars(df: AnyFrame) -> pl.DataFrame:
    """Convert any DataFrame to Polars format.

    Args:
        df: Input DataFrame (can be either Polars or Pandas).

    Returns:
        A Polars DataFrame.

    Raises:
        TypeError: If the input is neither a Polars nor Pandas DataFrame.
    """
    if isinstance(df, pl.DataFrame):
        return df
    if isinstance(df, pd.DataFrame):
        return pl.from_pandas(df)
    raise TypeError(f"Unsupported frame type: {type(df)!r}")


def to_pandas(df: pl.DataFrame) -> pd.DataFrame:
    """Convert a Polars DataFrame to Pandas format.

    Args:
        df: Input Polars DataFrame.

    Returns:
        A Pandas DataFrame.
    """
    return df.to_pandas()


def ensure_polars(frames: Iterable[AnyFrame]) -> list[pl.DataFrame]:
    """Convert an iterable of DataFrames to a list of Polars DataFrames.

    Args:
        frames: Iterable of DataFrames (can be mixed Polars and Pandas).

    Returns:
        List of Polars DataFrames.
    """
    return [to_polars(frame) for frame in frames]
