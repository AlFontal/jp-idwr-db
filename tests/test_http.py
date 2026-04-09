from __future__ import annotations

import json
from pathlib import Path
from types import TracebackType

import httpx
import pytest

from jp_idwr_db import http
from jp_idwr_db.config import Config


class _FakeClient:
    def __init__(self: _FakeClient, responses: list[httpx.Response]) -> None:
        self._responses = responses
        self.calls: list[tuple[str, str, dict[str, str] | None]] = []

    def __enter__(self: _FakeClient) -> _FakeClient:
        return self

    def __exit__(
        self: _FakeClient,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    def get(self: _FakeClient, url: str, headers: dict[str, str] | None = None) -> httpx.Response:
        self.calls.append(("GET", url, headers))
        return self._responses.pop(0)

    def head(self: _FakeClient, url: str) -> httpx.Response:
        self.calls.append(("HEAD", url, None))
        return self._responses.pop(0)


def _response(
    status_code: int, url: str, *, content: bytes = b"", headers: dict[str, str] | None = None
) -> httpx.Response:
    request = httpx.Request("GET", url)
    return httpx.Response(status_code, request=request, content=content, headers=headers)


def test_disk_cache_entry_and_metadata_roundtrip(tmp_path: Path) -> None:
    cache = http.DiskCache(tmp_path)
    url = "https://example.invalid/file.csv"

    entry = cache.entry(url)
    meta = {"etag": "abc", "last_modified": "yesterday", "url": url}
    cache.write_meta(url, meta)

    assert entry.path.name == entry.meta_path.stem
    assert json.loads(entry.meta_path.read_text(encoding="utf-8")) == meta
    assert cache.read_meta(url) == meta


def test_rate_limiter_sleeps_only_when_needed(monkeypatch: pytest.MonkeyPatch) -> None:
    limiter = http.RateLimiter(per_minute=60)  # 1 second interval
    moments = iter([100.0, 100.2, 101.5])
    slept: list[float] = []

    monkeypatch.setattr(http.time, "monotonic", lambda: next(moments))
    monkeypatch.setattr(http.time, "sleep", lambda seconds: slept.append(seconds))

    limiter.wait()
    limiter.wait()

    assert slept == [pytest.approx(0.8)]


def test_cached_get_returns_cached_file_on_304(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    url = "https://example.invalid/data.csv"
    config = Config(cache_dir=tmp_path)
    cache = http.DiskCache(tmp_path / "http")
    entry = cache.entry(url)
    entry.path.write_bytes(b"cached-bytes")
    cache.write_meta(url, {"etag": "etag-1", "last_modified": "yesterday", "url": url})

    client = _FakeClient([_response(304, url)])
    monkeypatch.setattr(http, "_build_client", lambda config: client)

    result = http.cached_get(url, config)

    assert result.read_bytes() == b"cached-bytes"
    assert client.calls == [
        ("GET", url, {"If-None-Match": "etag-1", "If-Modified-Since": "yesterday"})
    ]


def test_cached_get_redownloads_when_304_but_cache_file_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    url = "https://example.invalid/data.csv"
    config = Config(cache_dir=tmp_path)
    cache = http.DiskCache(tmp_path / "http")
    cache.write_meta(url, {"etag": "etag-1", "last_modified": "yesterday", "url": url})

    client = _FakeClient(
        [
            _response(304, url),
            _response(
                200,
                url,
                content=b"fresh-bytes",
                headers={"etag": "etag-2", "last-modified": "today"},
            ),
        ]
    )
    monkeypatch.setattr(http, "_build_client", lambda config: client)

    result = http.cached_get(url, config)

    assert result.read_bytes() == b"fresh-bytes"
    assert cache.read_meta(url) == {"etag": "etag-2", "last_modified": "today", "url": url}
    assert [call[0] for call in client.calls] == ["GET", "GET"]


def test_cached_head_uses_client_head(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    url = "https://example.invalid/data.csv"
    config = Config(cache_dir=tmp_path)
    response = httpx.Response(200, request=httpx.Request("HEAD", url))
    client = _FakeClient([response])
    monkeypatch.setattr(http, "_build_client", lambda config: client)

    result = http.cached_head(url, config)

    assert result.status_code == 200
    assert client.calls == [("HEAD", url, None)]


def test_download_urls_copies_cached_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = Config(cache_dir=tmp_path / "cache")
    dest_dir = tmp_path / "dest"
    urls = ["https://example.invalid/a.csv", "https://example.invalid/b.csv"]

    waited: list[str] = []

    class _FakeLimiter:
        def wait(self: _FakeLimiter) -> None:
            waited.append("wait")

    cache_paths = {
        urls[0]: tmp_path / "cached-a",
        urls[1]: tmp_path / "cached-b",
    }
    cache_paths[urls[0]].write_bytes(b"a")
    cache_paths[urls[1]].write_bytes(b"b")

    monkeypatch.setattr(http, "RateLimiter", lambda per_minute: _FakeLimiter())
    monkeypatch.setattr(http, "cached_get", lambda url, config: cache_paths[url])

    downloaded = http.download_urls(urls, dest_dir, config)

    assert waited == ["wait", "wait"]
    assert [path.name for path in downloaded] == ["a.csv", "b.csv"]
    assert (dest_dir / "a.csv").read_bytes() == b"a"
    assert (dest_dir / "b.csv").read_bytes() == b"b"
