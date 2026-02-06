"""HTTP client with caching and rate limiting for data downloads.

This module provides utilities for downloading data from the NIID surveillance system
with built-in disk caching, ETag support, and polite rate limiting.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import httpx

from .config import Config


@dataclass
class CacheEntry:
    """Represents a cache entry with data and metadata paths.

    Attributes:
        path: Path to the cached data file.
        meta_path: Path to the metadata JSON file.
    """

    path: Path
    meta_path: Path


class DiskCache:
    """Disk-based cache for HTTP responses with ETag support.

    This cache stores downloaded files and their associated metadata (ETag,
    Last-Modified headers) to enable conditional requests and avoid re-downloading
    unchanged data.
    """

    def __init__(self, root: Path) -> None:
        """Initialize the disk cache.

        Args:
            root: Root directory for cache storage.
        """
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def _key(self, url: str) -> str:
        """Generate a cache key (SHA-256 hash) from a URL.

        Args:
            url: The URL to hash.

        Returns:
            Hexadecimal SHA-256 hash of the URL.
        """
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    def entry(self, url: str) -> CacheEntry:
        """Get the cache entry paths for a given URL.

        Args:
            url: The URL to get cache paths for.

        Returns:
            CacheEntry with data and metadata paths.
        """
        key = self._key(url)
        path = self.root / key
        meta_path = self.root / f"{key}.json"
        return CacheEntry(path=path, meta_path=meta_path)

    def read_meta(self, url: str) -> dict[str, str] | None:
        """Read metadata for a cached URL.

        Args:
            url: The URL to read metadata for.

        Returns:
            Dictionary containing ETag, Last-Modified, and original URL,
            or None if no metadata exists.
        """
        entry = self.entry(url)
        if not entry.meta_path.exists():
            return None
        return json.loads(entry.meta_path.read_text())  # type: ignore[no-any-return]

    def write_meta(self, url: str, meta: dict[str, str]) -> None:
        """Write metadata for a cached URL.

        Args:
            url: The URL to write metadata for.
            meta: Dictionary containing cacheheaders (ETag, Last-Modified, etc.).
        """
        entry = self.entry(url)
        entry.meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True))


class RateLimiter:
    """Simple rate limiter to ensure polite HTTP requests.

    Enforces a minimum time interval between requests to avoid overwhelming
    the remote server.
    """

    def __init__(self, per_minute: int) -> None:
        """Initialize the rate limiter.

        Args:
            per_minute: Maximum number of requests allowed per minute.
        """
        self.interval = 60.0 / max(per_minute, 1)
        self._last_time: float | None = None

    def wait(self) -> None:
        """Wait if necessary to respect the rate limit.

        This method blocks until enough time has passed since the last request.
        On the first call, it does not block.
        """
        if self._last_time is None:
            self._last_time = time.monotonic()
            return
        elapsed = time.monotonic() - self._last_time
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        self._last_time = time.monotonic()


def _build_client(config: Config) -> httpx.Client:
    """Build an HTTP client with configured timeout and headers.

    Args:
        config: Configuration object containing user agent and timeout settings.

    Returns:
        Configured httpx.Client instance.
    """
    headers = {"User-Agent": config.user_agent}
    return httpx.Client(timeout=config.timeout_seconds, headers=headers, follow_redirects=True)


def cached_get(url: str, config: Config) -> Path:
    """Download a file with caching and conditional request support.

    Uses ETag and Last-Modified headers to avoid re-downloading unchanged files.
    Returns a path to the cached file.

    Args:
        url: URL to download.
        config: Configuration object for cache directory and HTTP settings.

    Returns:
        Path to the cached file.

    Raises:
        httpx.HTTPStatusError: If the server returns an error status.
    """
    cache = DiskCache(config.cache_dir / "http")
    entry = cache.entry(url)
    meta = cache.read_meta(url) or {}

    # Build conditional request headers
    headers = {}
    if "etag" in meta:
        headers["If-None-Match"] = meta["etag"]
    if "last_modified" in meta:
        headers["If-Modified-Since"] = meta["last_modified"]

    with _build_client(config) as client:
        response = client.get(url, headers=headers)
        if response.status_code == 304:
            # Not modified, return cached file
            if entry.path.exists():
                return entry.path
            # Cache missing despite 304, re-download
            response = client.get(url)
        response.raise_for_status()
        entry.path.write_bytes(response.content)
        new_meta = {
            "etag": response.headers.get("etag", ""),
            "last_modified": response.headers.get("last-modified", ""),
            "url": url,
        }
        cache.write_meta(url, new_meta)
        return entry.path


def cached_head(url: str, config: Config) -> httpx.Response:
    """Perform a HEAD request to check if a URL exists.

    Args:
        url: URL to check.
        config: Configuration object for HTTP settings.

    Returns:
        HTTP Response object from the HEAD request.
    """
    with _build_client(config) as client:
        return client.head(url)


def download_urls(urls: Iterable[str], dest_dir: Path, config: Config) -> list[Path]:
    """Download multiple URLs with rate limiting and caching.

    Args:
        urls: Iterable of URLs to download.
        dest_dir: Destination directory for downloaded files.
        config: Configuration object for cache, rate limit, and HTTP settings.

    Returns:
        List of paths to downloaded files.

    Note:
        Files are first downloaded to cache, then copied to dest_dir with
        original filenames. This ensures idempotent downloads across different
        destination directories while sharing a common cache.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    limiter = RateLimiter(config.rate_limit_per_minute)
    downloaded: list[Path] = []
    for url in urls:
        limiter.wait()
        cache_path = cached_get(url, config)
        dest_path = dest_dir / Path(url).name
        dest_path.write_bytes(cache_path.read_bytes())
        downloaded.append(dest_path)
    return downloaded
