"""Type definitions for jpinfectpy package.

This module defines the core type aliases used throughout the package for
consistent type hints and maintaining compatibility between Polars and Pandas.
"""

from __future__ import annotations

from typing import Literal, TypeAlias

import pandas as pd
import polars as pl

ReturnType = Literal["pandas", "polars"]
"""Literal type for specifying return type of data loading functions."""

DatasetName = Literal["sex", "place", "bullet"]
"""Literal type for dataset names used in download and read operations."""

AnyFrame: TypeAlias = pd.DataFrame | pl.DataFrame
"""Type alias for either Pandas or Polars DataFrame."""
