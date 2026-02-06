"""Type definitions for jp_idwr_db package."""

from __future__ import annotations

from typing import Literal

SurveillanceType = Literal["zensu", "teitenrui"]
"""Literal type for surveillance system types.

- "zensu": All-case reporting for serious diseases (confirmed cases)
- "teitenrui": Sentinel surveillance for common diseases
"""

SourceType = Literal["zensu", "teitenrui", "both"]
"""Literal type for filtering data by source.

- "zensu": Only confirmed case data
- "teitenrui": Only sentinel surveillance data  
- "both": Both sources merged (default)
"""

DatasetName = Literal["sex", "place", "bullet", "sentinel", "unified", "prefecture_en"]
"""Literal type for dataset names used in download and read operations."""
