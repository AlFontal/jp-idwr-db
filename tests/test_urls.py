from __future__ import annotations

import typing
from typing import Any

import pytest

from jp_idwr_db import urls


def test_url_confirmed_sex_2010() -> None:
    url = urls.url_confirmed(2010, "sex")
    assert url.endswith("H22/Syuukei/Syu_01_1.xls")


def test_url_confirmed_place_2006() -> None:
    url = urls.url_confirmed(2006, "place")
    assert url.endswith("H18/Syuukei/Syu_02_1.xls")


def test_url_bullet_en(monkeypatch: Any) -> None:
    def fake_head(url: str, config: Any) -> Any:
        class Resp:
            status_code = 200
            headers: typing.ClassVar = {"content-length": "100"}

        return Resp()

    monkeypatch.setattr(urls, "cached_head", fake_head)
    result = urls.url_bullet(2025, 1)
    assert result == ["https://id-info.jihs.go.jp/en/surveillance/idwr/rapid/2025/01/zensu01.csv"]


def test_url_bullet_ja(monkeypatch: Any) -> None:
    """Test English bullet URL generation (no Japanese option now)."""

    def fake_head(url: str, config: Any) -> Any:
        class Resp:
            status_code = 200
            headers: typing.ClassVar = {"content-length": "100"}

        return Resp()

    monkeypatch.setattr(urls, "cached_head", fake_head)
    result = urls.url_bullet(2025, 11)
    # Now always returns English version
    assert result == ["https://id-info.jihs.go.jp/en/surveillance/idwr/rapid/2025/11/zensu11.csv"]


def test_url_sentinel_single_week(monkeypatch: Any) -> None:
    """Test sentinel URL generation for a single week."""

    def fake_head(url: str, config: Any) -> Any:
        class Resp:
            status_code = 200
            headers: typing.ClassVar = {"content-length": "100"}

        return Resp()

    monkeypatch.setattr(urls, "cached_head", fake_head)
    result = urls.url_sentinel(2025, 4)
    assert result == [
        "https://id-info.jihs.go.jp/en/surveillance/idwr/rapid/2025/04/teitenrui04.csv"
    ]


def test_url_sentinel_2022_archive_format(monkeypatch: Any) -> None:
    """Test sentinel URL generation for 2015-2022 archive pattern."""

    def fake_head(url: str, config: Any) -> Any:
        class Resp:
            status_code = 200
            headers: typing.ClassVar = {"content-length": "100"}

        return Resp()

    monkeypatch.setattr(urls, "cached_head", fake_head)
    result = urls.url_sentinel(2022, 50)
    assert result == [
        "https://id-info.jihs.go.jp/niid/images/idwr/data-e/idwr-e2022/202250/teitenrui50.csv"
    ]


def test_url_sentinel_2014_archive_format(monkeypatch: Any) -> None:
    """Test sentinel URL generation for pre-2015 archive pattern."""

    def fake_head(url: str, config: Any) -> Any:
        class Resp:
            status_code = 200
            headers: typing.ClassVar = {"content-length": "100"}

        return Resp()

    monkeypatch.setattr(urls, "cached_head", fake_head)
    result = urls.url_sentinel(2014, 47)
    assert result == [
        "https://id-info.jihs.go.jp/niid/images/idwr/data-e/idwr-e2014/1447/teitenrui47.csv"
    ]


def test_url_sentinel_multiple_weeks(monkeypatch: Any) -> None:
    """Test sentinel URL generation for multiple weeks."""

    def fake_head(url: str, config: Any) -> Any:
        class Resp:
            status_code = 200
            headers: typing.ClassVar = {"content-length": "100"}

        return Resp()

    monkeypatch.setattr(urls, "cached_head", fake_head)
    result = urls.url_sentinel(2025, [1, 2, 3])
    assert len(result) == 3
    assert all("teitenrui" in url for url in result)
    assert result[0].endswith("2025/01/teitenrui01.csv")
    assert result[2].endswith("2025/03/teitenrui03.csv")


def test_url_sentinel_validation() -> None:
    """Test sentinel URL validation for invalid years."""
    # Year too old (before 1999)
    with pytest.raises(ValueError, match="Year must be >= 1999 for sentinel data"):
        urls.url_sentinel(1998, 1)
