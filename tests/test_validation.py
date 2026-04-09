from __future__ import annotations

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
