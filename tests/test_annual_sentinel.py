"""Tests for reading annual IDWR sentinel tables."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from jp_idwr_db import io
from jp_idwr_db.urls import url_annual


def _sheet(week_label: str, rows: list[list[str | None]]) -> pl.DataFrame:
    """Build a raw annual sheet: title rows, disease/sex headers, then data rows."""
    raw = [
        [None, None, None, None, None, None, None],
        ["8-1 sentinel weekly table -2023-", week_label],
        ["8-1  SENTINEL-REPORTING DISEASES (WEEKLY)", None],
        [None],
        [
            None,
            "インフルエンザ\n(Influenza(excld. avian influenza and pandemic influenza))",
            None,
            None,
            "伝染性紅斑\n(Erythema infectiosum)",
            None,
            None,
        ],
        [
            None,
            "総数(total No.)",
            "男(male)",
            "女(female)",
            "総数(total No.)",
            "男(male)",
            "女(female)",
        ],
        ["総    数(total No.)", "7", "4", "3", "1", "1", "0"],
        *rows,
    ]
    width = 7
    padded = [row + [None] * (width - len(row)) for row in raw]
    return pl.DataFrame(padded, schema=[f"c{i}" for i in range(width)], orient="row")


def _fake_workbook(monkeypatch: pytest.MonkeyPatch, sheets: list[pl.DataFrame]) -> None:
    """Serve the given sheets as sheet 2.. of a workbook (sheet 1 is the annual total)."""
    names = ["総数", *[f"s{i}" for i in range(len(sheets))]]
    monkeypatch.setitem(
        sys.modules,
        "fastexcel",
        SimpleNamespace(read_excel=lambda path: SimpleNamespace(sheet_names=names)),
    )
    monkeypatch.setattr(
        io.pl, "read_excel", lambda path, sheet_id, has_header: sheets[sheet_id - 2]
    )


def test_read_annual_sentinel_parses_totals_and_conventions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        ["北海道(Hokkaido)", "4", "2", "2", "-", "-", "-"],
        ["東京都(Tokyo)", "3", "2", "1", "…", "…", "…"],
    ]
    _fake_workbook(monkeypatch, [_sheet("(week 37)", rows)])

    df = io.read_annual_sentinel(Path("2023_Syu_08_1.xlsx"), 2023).sort(["disease", "prefecture"])

    assert df.columns == ["prefecture", "year", "week", "date", "disease", "count"]
    assert set(df["week"].to_list()) == {37}
    # Translation-only label changes are harmonised; the national total row is dropped.
    assert sorted(set(df["disease"].to_list())) == [
        "Erythema infection",
        "Influenza(excld. avian influenza and pandemic influenza)",
    ]
    erythema = df.filter(pl.col("disease") == "Erythema infection")
    assert erythema["count"].to_list() == [0.0, None]  # "-" is zero, "…" is not reported
    influenza = df.filter(pl.col("disease").str.starts_with("Influenza"))
    assert influenza["count"].to_list() == [4.0, 3.0]


def test_read_annual_sentinel_drops_zero_template_week_53(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    zero_rows = [["北海道(Hokkaido)", "-", "-", "-", "-", "-", "-"]]
    data_rows = [["北海道(Hokkaido)", "1", "1", "0", "-", "-", "-"]]
    _fake_workbook(monkeypatch, [_sheet("(week 52)", data_rows), _sheet("(week 53)", zero_rows)])

    df = io.read_annual_sentinel(Path("2023_Syu_08_1.xlsx"), 2023)  # 2023 has 52 ISO weeks

    assert set(df["week"].to_list()) == {52}


def test_read_annual_sentinel_rejects_data_beyond_iso_weeks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [["北海道(Hokkaido)", "5", "3", "2", "-", "-", "-"]]
    _fake_workbook(monkeypatch, [_sheet("(week 53)", rows)])

    with pytest.raises(ValueError, match="beyond ISO week"):
        io.read_annual_sentinel(Path("2023_Syu_08_1.xlsx"), 2023)


def test_harmonize_sentinel_disease_only_merges_proven_labels() -> None:
    assert io._harmonize_sentinel_disease("Erythema�@infectiosum") == "Erythema infection"
    assert io._harmonize_sentinel_disease("GroupA streptococcal pharyngitis") == (
        "Group A streptococcal pharyngitis"
    )
    assert io._harmonize_sentinel_disease("Measles") == "Measles(excluding measles in adults)"
    # Definitional changes stay separate.
    assert io._harmonize_sentinel_disease("Influenza") == "Influenza"
    assert io._harmonize_sentinel_disease("Measles(excluding adults)") == (
        "Measles(excluding adults)"
    )


def test_surveillance_windows_drop_placeholder_weeks() -> None:
    df = pl.DataFrame(
        {
            "disease": [
                "COVID-19",
                "COVID-19",
                "Mumps",
                "Acute encephalitis (excluding Japanese encephalitis)",
            ],
            "year": [2023, 2023, 2023, 2004],
            "week": [18, 19, 1, 10],
        }
    )
    kept = df.filter(io._in_surveillance_window())
    assert kept.select(["disease", "week"]).rows() == [("COVID-19", 19), ("Mumps", 1)]


def test_annual_sheet_week_reads_labels_from_all_eras() -> None:
    for label, week in [("(14week)", 14), ("(week 37)", 37), ("(３７週)", 37)]:
        assert io._annual_sheet_week(_sheet(label, [])) == week


def test_url_annual_covers_sentinel_eras() -> None:
    assert url_annual(1999, "sentinel").endswith("Kako/H11/Syuukei/Syu_12.xls")
    assert url_annual(2005, "sentinel").endswith("Kako/H17/Syuukei/Syu_08_1.xls")
    assert url_annual(2016, "sentinel").endswith("ydata/2016/Syuukei/Syu_08_1.xlsx")
    assert url_annual(2024, "sentinel_rate").endswith("annual/2024/syulist/Syu_08_2.xlsx")
    assert url_annual(2000, "sentinel_rate") is None


def test_annual_table_available_fails_safe(monkeypatch: pytest.MonkeyPatch) -> None:
    def head(status: int) -> object:
        return lambda url, config: SimpleNamespace(status_code=status)

    monkeypatch.setattr(io, "cached_head", head(200))
    assert io.annual_table_available("sentinel", 2024) is True
    for status in (404, 403, 500):
        monkeypatch.setattr(io, "cached_head", head(status))
        assert io.annual_table_available("sentinel", 2025) is False

    def fail(url: str, config: object) -> object:
        raise io.httpx.ConnectError("offline")

    monkeypatch.setattr(io, "cached_head", fail)
    assert io.annual_table_available("sentinel", 2025) is False
