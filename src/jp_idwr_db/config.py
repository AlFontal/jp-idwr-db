"""Configuration management for jp_idwr_db.

This module provides a global configuration system for controlling package behavior.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path

from platformdirs import user_cache_dir


@dataclass(frozen=True)
class Config:
    """Global configuration for jp_idwr_db package.

    Attributes:
        cache_dir: Directory for caching downloaded files.
        rate_limit_per_minute: Maximum number of HTTP requests per minute.
        user_agent: User-Agent header for HTTP requests.
        timeout_seconds: Timeout for HTTP requests in seconds.
        retries: Number of retry attempts for failed requests.
    """

    cache_dir: Path = Path(user_cache_dir("jp_idwr_db"))
    rate_limit_per_minute: int = 20
    user_agent: str = "jp_idwr_db/0.2.2 (+https://github.com/AlFontal/jp-idwr-db)"
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
        >>> import jp_idwr_db as jp
        >>> jp.configure(rate_limit_per_minute=10)
    """
    global _CONFIG  # noqa: PLW0603
    _CONFIG = replace(_CONFIG, **kwargs)  # type: ignore[arg-type]
    return _CONFIG
