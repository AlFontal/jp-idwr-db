"""Utility helpers for Polars data workflows."""

from __future__ import annotations

from datetime import date

import polars as pl

PREFECTURE_ISO_MAP: dict[str, str] = {
    "Hokkaido": "JP-01",
    "Aomori": "JP-02",
    "Iwate": "JP-03",
    "Miyagi": "JP-04",
    "Akita": "JP-05",
    "Yamagata": "JP-06",
    "Fukushima": "JP-07",
    "Ibaraki": "JP-08",
    "Tochigi": "JP-09",
    "Gunma": "JP-10",
    "Saitama": "JP-11",
    "Chiba": "JP-12",
    "Tokyo": "JP-13",
    "Kanagawa": "JP-14",
    "Niigata": "JP-15",
    "Toyama": "JP-16",
    "Ishikawa": "JP-17",
    "Fukui": "JP-18",
    "Yamanashi": "JP-19",
    "Nagano": "JP-20",
    "Gifu": "JP-21",
    "Shizuoka": "JP-22",
    "Aichi": "JP-23",
    "Mie": "JP-24",
    "Shiga": "JP-25",
    "Kyoto": "JP-26",
    "Osaka": "JP-27",
    "Hyogo": "JP-28",
    "Nara": "JP-29",
    "Wakayama": "JP-30",
    "Tottori": "JP-31",
    "Shimane": "JP-32",
    "Okayama": "JP-33",
    "Hiroshima": "JP-34",
    "Yamaguchi": "JP-35",
    "Tokushima": "JP-36",
    "Kagawa": "JP-37",
    "Ehime": "JP-38",
    "Kochi": "JP-39",
    "Fukuoka": "JP-40",
    "Saga": "JP-41",
    "Nagasaki": "JP-42",
    "Kumamoto": "JP-43",
    "Oita": "JP-44",
    "Miyazaki": "JP-45",
    "Kagoshima": "JP-46",
    "Okinawa": "JP-47",
}


def iso_weeks_in_year(year: int) -> int:
    """Return the number of ISO weeks in a year (52 or 53)."""
    return date(year, 12, 28).isocalendar().week


def complete_years(df: pl.DataFrame) -> set[int]:
    """Return years whose data reaches the final ISO week of that year."""
    if df.is_empty():
        return set()
    last_weeks = df.group_by("year").agg(pl.col("week").max().alias("last_week"))
    return {
        int(row["year"])
        for row in last_weeks.iter_rows(named=True)
        if int(row["last_week"]) >= iso_weeks_in_year(int(row["year"]))
    }


def prefecture_map() -> dict[str, str]:
    """Return prefecture -> ISO 3166-2 code mapping."""
    return PREFECTURE_ISO_MAP.copy()


def attach_prefecture_id(
    df: pl.DataFrame,
    *,
    prefecture_col: str = "prefecture",
    id_col: str = "prefecture_id",
) -> pl.DataFrame:
    """Attach ISO prefecture IDs to a Polars DataFrame."""
    mapping_df = pl.DataFrame(
        {
            prefecture_col: list(PREFECTURE_ISO_MAP.keys()),
            id_col: list(PREFECTURE_ISO_MAP.values()),
        }
    )
    return df.join(mapping_df, on=prefecture_col, how="left")
