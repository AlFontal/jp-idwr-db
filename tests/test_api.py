"""Tests for the new get_data() API."""

from __future__ import annotations

import polars as pl

import jp_idwr_db as jp


def test_get_data_basic() -> None:
    """Test basic get_data() call."""
    df = jp.get_data()
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0


def test_get_data_disease_filter() -> None:
    """Test filtering by disease name."""
    df = jp.get_data(disease="Tuberculosis")
    assert df.height > 0
    assert all("tuberculosis" in d.lower() for d in df["disease"].unique().to_list())


def test_get_data_multiple_diseases() -> None:
    """Test filtering by multiple diseases."""
    df = jp.get_data(disease=["Tuberculosis", "Measles"])
    assert df.height > 0
    diseases = [d.lower() for d in df["disease"].unique().to_list()]
    assert any("tuberculosis" in d for d in diseases)
    assert any("measles" in d for d in diseases)


def test_get_data_prefecture_filter() -> None:
    """Test filtering by prefecture."""
    df = jp.get_data(prefecture="Total No.")
    assert df.height > 0
    assert df["prefecture"].unique().to_list() == ["Total No."]


def test_get_data_multiple_prefectures() -> None:
    """Test filtering by multiple prefectures."""
    df = jp.get_data(prefecture=["Total No.", "Hokkaido"])
    assert df.height > 0
    prefs = df["prefecture"].unique().to_list()
    assert set(prefs).issubset({"Total No.", "Hokkaido"})


def test_get_data_week_single() -> None:
    """Test filtering by single week."""
    df = jp.get_data(week=1)
    if df.height > 0:  # Only check if data exists
        assert df["week"].unique().to_list() == [1]


def test_get_data_week_range() -> None:
    """Test filtering by week range."""
    df = jp.get_data(week=(1, 5))
    if df.height > 0:
        weeks = df["week"].unique().sort().to_list()
        assert all(1 <= w <= 5 for w in weeks)


def test_get_data_combined_filters() -> None:
    """Test combining multiple filters."""
    df = jp.get_data(
        disease="Tuberculosis",
        prefecture="Total No.",
        week=(1, 10),
    )
    if df.height > 0:
        assert all("tuberculosis" in d.lower() for d in df["disease"].unique().to_list())
        assert df["prefecture"].unique().to_list() == ["Total No."]
        assert all(1 <= w <= 10 for w in df["week"].unique().to_list())


def test_get_data_no_results() -> None:
    """Test that filtering returns empty DataFrame when no matches."""
    df = jp.get_data(disease="NonexistentDisease12345")
    assert isinstance(df, pl.DataFrame)
    # Should be empty
    assert df.height == 0


def test_list_diseases() -> None:
    """Test listing available diseases."""
    diseases = jp.list_diseases()
    assert isinstance(diseases, list)
    assert len(diseases) > 0
    assert all(isinstance(d, str) for d in diseases)
    # Should be sorted
    assert diseases == sorted(diseases)


def test_list_diseases_all_source() -> None:
    """Test listing diseases from all sources."""
    diseases = jp.list_diseases(source="all")
    assert len(diseases) > 0


def test_list_prefectures() -> None:
    """Test listing available prefectures."""
    prefs = jp.list_prefectures()
    assert isinstance(prefs, list)
    assert len(prefs) > 0
    assert all(isinstance(p, str) for p in prefs)
    # Should be sorted
    assert prefs == sorted(prefs)


def test_get_latest_week() -> None:
    """Test getting latest week."""
    latest = jp.get_latest_week()
    # May be None if data doesn't have year column
    if latest is not None:
        assert isinstance(latest, tuple)
        assert len(latest) == 2
        year, week = latest
        assert isinstance(year, int)
        assert isinstance(week, int)
        assert 1 <= week <= 53
