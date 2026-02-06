from __future__ import annotations

from .api import get_data, get_latest_week, list_diseases, list_prefectures
from .config import Config, configure, get_config
from .datasets import load_dataset as load
from .transform import merge, pivot
from .types import DatasetName
from .utils import attach_prefecture_id, prefecture_map

__all__ = [
    "Config",
    "DatasetName",
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
]

__version__ = "0.1.0"
