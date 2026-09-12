"""Type definitions for jp_idwr_db package."""

from __future__ import annotations

from typing import Literal

DatasetName = Literal["sex", "place", "bullet", "sentinel", "unified", "prefecture_en"]
"""Literal type for dataset names used in download and read operations."""
