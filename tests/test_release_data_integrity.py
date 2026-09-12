from __future__ import annotations

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
        unexpected_sentinel = con.execute(
            """
            SELECT COUNT(*) FROM read_parquet(?)
            WHERE source = 'Sentinel surveillance'
              AND disease IN (SELECT disease FROM read_parquet(?))
            """,
            [_parquet("unified.parquet"), _parquet("bullet.parquet")],
        ).fetchone()
        assert bullet_missing == (0,)
        assert historical_missing == (0,)
        assert unexpected_sentinel == (0,)
    finally:
        con.close()
