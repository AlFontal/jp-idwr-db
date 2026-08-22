from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from jp_idwr_db._internal import validation


def test_get_sentinel_only_diseases_returns_empty_set() -> None:
    assert validation.get_sentinel_only_diseases() == set()


def test_validate_schema_accepts_expected_columns() -> None:
    df = pl.DataFrame(
        {
            "prefecture": ["Tokyo"],
            "year": [2024],
            "week": [1],
            "disease": ["Tuberculosis"],
            "count": [1],
        }
    )

    validation.validate_schema(df)


def test_validate_schema_rejects_missing_columns() -> None:
    df = pl.DataFrame({"prefecture": ["Tokyo"]})

    with pytest.raises(ValueError, match="Missing required columns"):
        validation.validate_schema(df)


def test_validate_no_duplicates_uses_category_when_present() -> None:
    df = pl.DataFrame(
        {
            "prefecture": ["Tokyo", "Tokyo"],
            "year": [2024, 2024],
            "week": [1, 1],
            "disease": ["Tuberculosis", "Tuberculosis"],
            "category": ["male", "female"],
        }
    )

    validation.validate_no_duplicates(df)


def test_validate_no_duplicates_rejects_duplicate_rows() -> None:
    df = pl.DataFrame(
        {
            "prefecture": ["Tokyo", "Tokyo"],
            "year": [2024, 2024],
            "week": [1, 1],
            "disease": ["Tuberculosis", "Tuberculosis"],
            "category": ["total", "total"],
        }
    )

    with pytest.raises(ValueError, match="duplicate records"):
        validation.validate_no_duplicates(df)


def test_validate_date_ranges_rejects_out_of_range_years() -> None:
    df = pl.DataFrame({"year": [1998], "week": [1]})

    with pytest.raises(ValueError, match="Year values out of expected range"):
        validation.validate_date_ranges(df)


def test_validate_date_ranges_rejects_out_of_range_weeks() -> None:
    df = pl.DataFrame({"year": [2024], "week": [54]})

    with pytest.raises(ValueError, match="Week values out of valid range"):
        validation.validate_date_ranges(df)


def test_validate_iso_week_start_dates_accepts_monday() -> None:
    df = pl.DataFrame({"year": [2024], "week": [1], "date": [date(2024, 1, 1)]})

    validation.validate_iso_week_start_dates(df)


def test_validate_iso_week_start_dates_rejects_sunday() -> None:
    df = pl.DataFrame({"year": [2024], "week": [1], "date": [date(2024, 1, 7)]})

    with pytest.raises(ValueError, match="Date must be the Monday"):
        validation.validate_iso_week_start_dates(df)


def test_validate_non_negative_counts_accepts_null_metrics() -> None:
    df = pl.DataFrame({"count": [0.0, 1.0, None], "per_sentinel": [None, 0.5, 2.0]})

    validation.validate_non_negative_counts(df)


def test_validate_non_negative_counts_rejects_negative_count() -> None:
    df = pl.DataFrame({"count": [0.0, -1.0], "per_sentinel": [0.0, 1.0]})

    with pytest.raises(ValueError, match="negative count"):
        validation.validate_non_negative_counts(df)


def test_validate_max_null_rate_accepts_groups_below_threshold() -> None:
    df = pl.DataFrame(
        {
            "year": [2024, 2024, 2025, 2025],
            "count": [1.0, None, 2.0, 3.0],
        }
    )

    validation.validate_max_null_rate(df, "count", max_rate=0.5, group_by=["year"])


def test_validate_max_null_rate_rejects_bad_group() -> None:
    df = pl.DataFrame(
        {
            "year": [2024, 2024, 2025, 2025],
            "count": [None, None, 2.0, 3.0],
        }
    )

    with pytest.raises(ValueError, match=r"Null rate for count exceeds 25\.0%"):
        validation.validate_max_null_rate(df, "count", max_rate=0.25, group_by=["year"])


def test_validate_prefecture_coverage_allows_registered_source_anomaly() -> None:
    df = pl.DataFrame(
        {
            "year": [2016, 2016, 2016],
            "week": [37, 37, 38],
            "prefecture": ["Tokyo", "Osaka", "Tokyo"],
        }
    )

    validation.validate_prefecture_coverage(
        df,
        expected=1,
        allowed_counts={(2016, 37): 2},
    )


def test_validate_prefecture_coverage_rejects_unregistered_gap() -> None:
    df = pl.DataFrame(
        {
            "year": [2026, 2026],
            "week": [1, 1],
            "prefecture": ["Tokyo", "Osaka"],
        }
    )

    with pytest.raises(ValueError, match="Unexpected prefecture coverage"):
        validation.validate_prefecture_coverage(df, expected=3)


def test_smart_merge_keeps_confirmed_and_adds_sentinel_only_diseases() -> None:
    confirmed = pl.DataFrame(
        {
            "disease": ["Influenza", "Tuberculosis"],
            "count": [100, 10],
            "source": ["Confirmed cases", "Confirmed cases"],
        }
    )
    sentinel = pl.DataFrame(
        {
            "disease": ["Influenza", "RSV"],
            "count": [120, 50],
            "source": ["Sentinel surveillance", "Sentinel surveillance"],
        }
    )

    merged = validation.smart_merge(confirmed, sentinel)

    assert merged.get_column("disease").to_list() == ["Influenza", "Tuberculosis", "RSV"]
    assert merged.filter(pl.col("disease") == "Influenza").height == 1
