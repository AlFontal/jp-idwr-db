"""Internal download utilities.

This module provides low-level download functionality.
For simple data access, use the public jp.get_data() API instead.
"""

from __future__ import annotations

# Re-export download functions from io.py
from ..io import download, download_recent

__all__ = ["download", "download_recent"]
