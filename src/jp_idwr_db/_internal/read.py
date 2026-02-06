"""Internal read utilities.

This module provides low-level file reading functionality.
For simple data access, use the public jp.get_data() API instead.
"""

from __future__ import annotations

# Re-export read function from io.py
from ..io import read

__all__ = ["read"]
