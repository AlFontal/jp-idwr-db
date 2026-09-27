from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "parquet"
DATASETS = {
    "sex": ("sex_prefecture.parquet", "prefecture, year, week, disease, category"),
    "place": ("place_prefecture.parquet", "prefecture, year, week, disease, category"),
    "bullet": ("bullet.parquet", "prefecture, year, week, disease"),
    "sentinel": ("sentinel.parquet", "prefecture, year, week, disease"),
    "unified": ("unified.parquet", "prefecture, year, week, disease, category"),
}


def _parquet(filename: str) -> str:
    return (DATA_DIR / filename).as_posix()


def test_release_tables_satisfy_core_value_invariants() -> None:
    con = duckdb.connect()
    try:
        for filename, keys in DATASETS.values():
            path = _parquet(filename)
            duplicate_groups = con.execute(
                f"SELECT COUNT(*) FROM ("
                f"SELECT 1 FROM read_parquet(?) GROUP BY {keys} HAVING COUNT(*) > 1)",
                [path],
            ).fetchone()
            invalid_values = con.execute(
                "SELECT COUNT(*) FROM read_parquet(?) "
                "WHERE prefecture IS NULL OR TRIM(prefecture) = '' "
                "OR prefecture NOT IN (SELECT prefecture FROM read_parquet(?)) "
                "OR disease IS NULL OR TRIM(disease) = '' "
                "OR year IS NULL OR week IS NULL OR week NOT BETWEEN 1 AND 53 "
                "OR isodow(date) <> 1 OR yearweek(date) <> year * 100 + week "
                "OR count < 0 OR count <> trunc(count) "
                "OR (count IS NOT NULL AND NOT isfinite(count))",
                [path, _parquet("prefecture_en.parquet")],
            ).fetchone()
            assert duplicate_groups == (0,), filename
            assert invalid_values == (0,), filename
    finally:
        con.close()


def test_release_table_domains_match_documentation() -> None:
    con = duckdb.connect()
    try:
        expected_sources = {
            "sex_prefecture.parquet": ["Confirmed cases"],
            "place_prefecture.parquet": ["Confirmed cases"],
            "bullet.parquet": ["All-case reporting"],
            "sentinel.parquet": ["Sentinel surveillance"],
            "unified.parquet": [
                "All-case reporting",
                "Confirmed cases",
                "Sentinel surveillance",
            ],
        }
        for filename, expected in expected_sources.items():
            actual = [
                row[0]
                for row in con.execute(
                    "SELECT DISTINCT source FROM read_parquet(?) ORDER BY source",
                    [_parquet(filename)],
                ).fetchall()
            ]
            assert actual == expected

        assert con.execute(
            "SELECT DISTINCT category FROM read_parquet(?) ORDER BY category",
            [_parquet("sex_prefecture.parquet")],
        ).fetchall() == [("female",), ("male",), ("total",)]
        assert con.execute(
            "SELECT DISTINCT category FROM read_parquet(?) ORDER BY category",
            [_parquet("place_prefecture.parquet")],
        ).fetchall() == [("japan",), ("others",), ("total",), ("unknown",)]
        assert con.execute(
            "SELECT DISTINCT category FROM read_parquet(?)",
            [_parquet("unified.parquet")],
        ).fetchall() == [("total",)]
    finally:
        con.close()


def test_release_tables_have_complete_prefecture_grain() -> None:
    con = duckdb.connect()
    try:
        for name, (filename, _) in DATASETS.items():
            grouping = "year, week, disease, source"
            if name in {"sex", "place"}:
                grouping += ", category"
            if name == "unified":
                grouping += ", category"
            deviations = con.execute(
                f"SELECT DISTINCT year, week, source, "
                f"COUNT(DISTINCT prefecture) AS prefectures "
                f"FROM read_parquet(?) GROUP BY {grouping} "
                "HAVING prefectures <> 47 ORDER BY year, week, prefectures",
                [str(DATA_DIR / filename)],
            ).fetchall()
            if name in {"sentinel", "unified"}:
                assert deviations == [(2016, 37, "Sentinel surveillance", 26)]
            else:
                assert deviations == []
    finally:
        con.close()


def test_unified_is_an_exact_composition_of_source_tables() -> None:
    con = duckdb.connect()
    try:
        bullet_missing = con.execute(
            """
            SELECT COUNT(*) FROM (
              SELECT prefecture, year, week, date, disease, count, source
              FROM read_parquet(?)
              EXCEPT
              SELECT prefecture, year, week, date, disease, count, source
              FROM read_parquet(?) WHERE source = 'All-case reporting'
            )
            """,
            [_parquet("bullet.parquet"), _parquet("unified.parquet")],
        ).fetchone()
        historical_missing = con.execute(
            """
            SELECT COUNT(*) FROM (
              SELECT prefecture, year, week, date, disease, count, source
              FROM read_parquet(?) WHERE category = 'total' AND year < 2024
              EXCEPT
              SELECT prefecture, year, week, date, disease, count, source
              FROM read_parquet(?) WHERE source = 'Confirmed cases'
            )
            """,
            [_parquet("sex_prefecture.parquet"), _parquet("unified.parquet")],
        ).fetchone()
        # Sentinel rows are kept exactly for disease-years without confirmed coverage.
        sentinel_mismatch = con.execute(
            """
            WITH confirmed AS (
              SELECT DISTINCT disease, year FROM read_parquet($unified)
              WHERE source <> 'Sentinel surveillance'
            ),
            expected AS (
              SELECT prefecture, year, week, date, disease, count, per_sentinel, source
              FROM read_parquet($sentinel) s
              WHERE NOT EXISTS (
                SELECT 1 FROM confirmed c WHERE c.disease = s.disease AND c.year = s.year
              )
            ),
            actual AS (
              SELECT prefecture, year, week, date, disease, count, per_sentinel, source
              FROM read_parquet($unified) WHERE source = 'Sentinel surveillance'
            )
            SELECT
              (SELECT COUNT(*) FROM (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)),
              (SELECT COUNT(*) FROM (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected))
            """,
            {"unified": _parquet("unified.parquet"), "sentinel": _parquet("sentinel.parquet")},
        ).fetchone()
        assert bullet_missing == (0,)
        assert historical_missing == (0,)
        assert sentinel_mismatch == (0, 0)
    finally:
        con.close()


def _iso_weeks_between(start: tuple[int, int], end: tuple[int, int]) -> set[tuple[int, int]]:
    weeks = set()
    for year in range(start[0], end[0] + 1):
        for week in range(1, date(year, 12, 28).isocalendar().week + 1):
            if start <= (year, week) <= end:
                weeks.add((year, week))
    return weeks


def test_release_series_have_no_missing_weeks() -> None:
    con = duckdb.connect()
    try:
        series = {
            "sex_prefecture.parquet": "TRUE",
            "place_prefecture.parquet": "TRUE",
            "bullet.parquet": "TRUE",
            "sentinel.parquet": "TRUE",
            "unified.parquet (confirmed)": "source <> 'Sentinel surveillance'",
            "unified.parquet (sentinel)": "source = 'Sentinel surveillance'",
        }
        for label, condition in series.items():
            filename = label.split(" ", maxsplit=1)[0]
            observed = set(
                con.execute(
                    f"SELECT DISTINCT year, week FROM read_parquet(?) WHERE {condition}",
                    [_parquet(filename)],
                ).fetchall()
            )
            expected = _iso_weeks_between(min(observed), max(observed))
            assert sorted(expected - observed) == [], label
    finally:
        con.close()
