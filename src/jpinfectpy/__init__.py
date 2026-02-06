from __future__ import annotations

from .api import get_data, get_latest_week, list_diseases, list_prefectures
from .config import Config, configure, get_config
from .datasets import load_dataset as load
from .prefectures import attach_prefecture_id, prefecture_map
from .transform import merge, pivot
from .types import AnyFrame, DatasetName, ReturnType
from .utils import to_pandas, to_polars

__all__ = [
    "AnyFrame",
    "Config",
    "DatasetName",
    "ReturnType",
    "attach_prefecture_id",
    "configure",
    "get_config",
    "get_data",
    "get_latest_week",
    "list_diseases",
    "list_prefectures",
    "load",
    "merge",
    "pivot",
    "prefecture_map",
    "to_pandas",
    "to_polars",
]

__version__ = "0.1.0"
