from __future__ import annotations

from .api import get_data, get_latest_week, list_diseases, list_prefectures
from .config import Config, configure, get_config
from .datasets import load_all
from .datasets import load_dataset as load
from .transform import merge, pivot
from .types import AnyFrame, DatasetName, ReturnType
from .utils import to_pandas, to_polars

__all__ = [
    "AnyFrame",
    "Config",
    "DatasetName",
    "ReturnType",
    "configure",
    "get_config",
    "get_data",
    "get_latest_week",
    "list_diseases",
    "list_prefectures",
    "load",
    "load_all",
    "merge",
    "pivot",
    "to_pandas",
    "to_polars",
]

__version__ = "0.1.0"
