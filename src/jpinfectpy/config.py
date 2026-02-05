"""Configuration management for jpinfectpy.

This module provides a global configuration system for controlling package behavior,
including return types, caching, rate limiting, and HTTP client settings.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path

from platformdirs import user_cache_dir

from .types import ReturnType


@dataclass(frozen=True)
class Config:
    """Global configuration for jpinfectpy package.

    Attributes:
        return_type: Default return type for data loading functions ("pandas" or "polars").
        cache_dir: Directory for caching downloaded files.
        rate_limit_per_minute: Maximum number of HTTP requests per minute.
        user_agent: User-Agent header for HTTP requests.
        timeout_seconds: Timeout for HTTP requests in seconds.
        retries: Number of retry attempts for failed requests.
    """

    return_type: ReturnType = "polars"
    cache_dir: Path = Path(user_cache_dir("jpinfectpy"))
    rate_limit_per_minute: int = 20
    user_agent: str = "jpinfectpy/0.1.0 (+https://github.com/AlFontal/jpinfectpy)"
    timeout_seconds: float = 30.0
    retries: int = 3


_CONFIG = Config()


def get_config() -> Config:
    """Get the current global configuration.

    Returns:
        The current Config instance.
    """
    return _CONFIG


def configure(**kwargs: object) -> Config:
    """Update the global configuration.

    Args:
        **kwargs: Configuration parameters to update (see Config attributes).

    Returns:
        The updated Config instance.

    Example:
        >>> import jpinfectpy as jp
        >>> jp.configure(return_type="polars", rate_limit_per_minute=10)
    """
    global _CONFIG  # noqa: PLW0603
    _CONFIG = replace(_CONFIG, **kwargs)  # type: ignore[arg-type]
    return _CONFIG
