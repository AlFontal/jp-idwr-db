from __future__ import annotations

from datetime import date
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType

import polars as pl
import pytest

from jp_idwr_db import io
from jp_idwr_db.utils import PREFECTURE_ISO_MAP, iso_weeks_in_year


@pytest.fixture(autouse=True)
def _no_annual_tables(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keep builds offline: no annual table is cached or published unless a test says so."""
    monkeypatch.setattr(
        io, "annual_cache_path", lambda table, year, **_: tmp_path / "no-cache" / f"{table}{year}"
    )
    monkeypatch.setattr(io, "annual_table_available", lambda table, year: False)


def _load_build_module() -> ModuleType:
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "build_datasets.py"
    spec = spec_from_file_location("jp_idwr_db_build_datasets", script_path)
    if spec is None or spec.loader is None:
        raise AssertionError("Could not load build_datasets.py")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_build_bullet_skips_unpublished_future_weeks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    build_datasets = _load_build_module()
    monkeypatch.setattr(build_datasets, "DATA_DIR", tmp_path)
    monkeypatch.setattr(build_datasets, "BULLET_FIRST_YEAR", 2026)
    monkeypatch.setattr(build_datasets, "CURRENT_YEAR", 2026)
    monkeypatch.setattr(build_datasets, "CURRENT_WEEK", 13)
    monkeypatch.setattr(
        build_datasets.validation, "validate_prefecture_coverage", lambda *args, **kwargs: None
    )

    def fake_download(name: str, year: int, week: range) -> list[Path]:
        assert name == "bullet"
        assert year == 2026
        assert list(week) == list(range(1, 14))
        return [tmp_path / "2026" / "zensu11.csv"]

    def fake_read(path: Path, type: str) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "prefecture": ["Hokkaido", "Tokyo"],
                "disease": ["Acquired immunodeficiency syndrome (AIDS)", "Measles"],
                "count": [4, 1],
                "week": [11, 11],
                "year": [2026, 2026],
                "date": [date(2026, 3, 9), date(2026, 3, 9)],
                "source": ["Confirmed cases", "Confirmed cases"],
            }
        )

    monkeypatch.setattr(build_datasets.io, "download", fake_download)
    monkeypatch.setattr(build_datasets.io, "read", fake_read)

    build_datasets.build_bullet()

    df = pl.read_parquet(tmp_path / "bullet.parquet")
    assert df["week"].max() == 11
    assert df["date"].unique().to_list() == [date(2026, 3, 9)]
    assert "AIDS" in df["disease"].unique().to_list()
    assert "Total No." not in df["prefecture"].unique().to_list()


def test_build_bullet_runs_validation_before_writing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    build_datasets = _load_build_module()
    monkeypatch.setattr(build_datasets, "DATA_DIR", tmp_path)
    monkeypatch.setattr(build_datasets, "BULLET_FIRST_YEAR", 2026)
    monkeypatch.setattr(build_datasets, "CURRENT_YEAR", 2026)
    monkeypatch.setattr(build_datasets, "CURRENT_WEEK", 2)
    monkeypatch.setattr(
        build_datasets.validation, "validate_prefecture_coverage", lambda *args, **kwargs: None
    )

    monkeypatch.setattr(
        build_datasets.io,
        "download",
        lambda name, year, week: [tmp_path / "2026" / "zensu01.csv"],
    )
    monkeypatch.setattr(
        build_datasets.io,
        "read",
        lambda path, type: pl.DataFrame(
            {
                "prefecture": ["Tokyo"],
                "disease": ["Tuberculosis"],
                "count": [1],
                "week": [1],
                "year": [2026],
                "date": [date(2025, 12, 29)],
                "source": ["All-case reporting"],
            }
        ),
    )

    called: list[str] = []
    monkeypatch.setattr(
        build_datasets.validation, "validate_schema", lambda df: called.append("schema")
    )
    monkeypatch.setattr(
        build_datasets.validation, "validate_no_duplicates", lambda df: called.append("duplicates")
    )
    monkeypatch.setattr(
        build_datasets.validation, "validate_date_ranges", lambda df: called.append("dates")
    )

    build_datasets.build_bullet()

    assert called == ["schema", "duplicates", "dates"]
    assert (tmp_path / "bullet.parquet").exists()


def test_build_sentinel_does_not_redifference_preserved_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    build_datasets = _load_build_module()
    monkeypatch.setattr(build_datasets, "DATA_DIR", tmp_path)
    monkeypatch.setattr(build_datasets, "SENTINEL_FIRST_YEAR", 2012)  # preliminary only
    monkeypatch.setattr(build_datasets, "CURRENT_YEAR", 2026)
    monkeypatch.setattr(build_datasets, "CURRENT_WEEK", 2)
    monkeypatch.setattr(
        build_datasets.validation, "validate_prefecture_coverage", lambda *args, **kwargs: None
    )

    pl.DataFrame(
        {
            "prefecture": ["Tokyo", "Tokyo"],
            "disease": [
                "Acquired immunodeficiency syndrome (AIDS)",
                "Acquired immunodeficiency syndrome (AIDS)",
            ],
            "year": [2025, 2025],
            "week": [1, 52],
            "date": [date(2024, 12, 30), date(2025, 12, 22)],
            "count": [7.0, 8.0],
            "per_sentinel": [0.7, 0.8],
            "source": ["Sentinel surveillance", "Sentinel surveillance"],
            "count_status": ["derived", "derived"],
        }
    ).write_parquet(tmp_path / "sentinel.parquet")

    paths = [tmp_path / "teitenrui01.csv", tmp_path / "teitenrui02.csv"]
    monkeypatch.setattr(
        build_datasets.io,
        "download",
        lambda name, year, week: paths,
    )

    def fake_read(path: Path) -> pl.DataFrame:
        week = 1 if path.name.endswith("01.csv") else 2
        cumulative_count = 10.0 if week == 1 else 25.0
        return pl.DataFrame(
            {
                "prefecture": ["Tokyo"],
                "disease": ["AIDS"],
                "year": [2026],
                "week": [week],
                "date": [date.fromisocalendar(2026, week, 7)],
                "count": [cumulative_count],
                "per_sentinel": [cumulative_count / 10],
                "source": ["Sentinel surveillance"],
            }
        )

    monkeypatch.setattr(build_datasets.io, "_read_sentinel_en_pl", fake_read)

    build_datasets.build_sentinel()

    result = pl.read_parquet(tmp_path / "sentinel.parquet").sort(["year", "week"])
    assert result.filter(pl.col("year") == 2025)["count"].to_list() == [7.0, 8.0]
    assert result.filter(pl.col("year") == 2026)["count"].to_list() == [10.0, 15.0]
    assert result["disease"].unique().to_list() == ["AIDS"]


def test_build_bullet_fails_closed_when_download_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    build_datasets = _load_build_module()
    monkeypatch.setattr(build_datasets, "DATA_DIR", tmp_path)
    monkeypatch.setattr(build_datasets, "BULLET_FIRST_YEAR", 2026)
    monkeypatch.setattr(build_datasets, "CURRENT_YEAR", 2026)
    monkeypatch.setattr(build_datasets, "CURRENT_WEEK", 2)
    monkeypatch.setattr(build_datasets.io, "download", lambda *args, **kwargs: [])

    with pytest.raises(RuntimeError, match="Failed to build bullet data for 2026"):
        build_datasets.build_bullet()

    assert not (tmp_path / "bullet.parquet").exists()


def test_build_sentinel_fails_closed_when_parser_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    build_datasets = _load_build_module()
    monkeypatch.setattr(build_datasets, "DATA_DIR", tmp_path)
    monkeypatch.setattr(build_datasets, "SENTINEL_FIRST_YEAR", 2012)  # preliminary only
    monkeypatch.setattr(build_datasets, "CURRENT_YEAR", 2026)
    monkeypatch.setattr(build_datasets, "CURRENT_WEEK", 2)
    monkeypatch.setattr(
        build_datasets.io,
        "download",
        lambda *args, **kwargs: [tmp_path / "teitenrui01.csv"],
    )

    def fail_read(path: Path) -> pl.DataFrame:
        raise ValueError("bad CSV")

    monkeypatch.setattr(build_datasets.io, "_read_sentinel_en_pl", fail_read)

    with pytest.raises(RuntimeError, match="Failed to build sentinel data for 2012"):
        build_datasets.build_sentinel()

    assert not (tmp_path / "sentinel.parquet").exists()


def _bullet_frame(year: int, week: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "prefecture": ["Tokyo"],
            "disease": ["Measles"],
            "count": [1],
            "week": [week],
            "year": [year],
            "date": [date.fromisocalendar(year, week, 1)],
        }
    )


def _bullet_path(tmp_path: Path, year: int, week: int) -> Path:
    return tmp_path / str(year) / f"zensu{week:02d}.csv"


def _patch_bullet_io(
    build_datasets: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    published: dict[int, int],
    requested_years: list[int],
) -> None:
    """Serve bullet weeks 1..published[year] for each year, recording requested years."""

    def fake_download(name: str, year: int, week: range) -> list[Path]:
        requested_years.append(year)
        last_week = min(published.get(year, 0), max(week))
        return [_bullet_path(tmp_path, year, w) for w in range(1, last_week + 1)]

    def fake_read(path: Path, type: str) -> pl.DataFrame:
        return _bullet_frame(int(path.parent.name), int(path.stem.removeprefix("zensu")))

    monkeypatch.setattr(build_datasets.io, "download", fake_download)
    monkeypatch.setattr(build_datasets.io, "read", fake_read)


def test_build_bullet_refetches_incomplete_previous_year_at_rollover(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    build_datasets = _load_build_module()
    monkeypatch.setattr(build_datasets, "DATA_DIR", tmp_path)
    monkeypatch.setattr(build_datasets, "BULLET_FIRST_YEAR", 2025)
    monkeypatch.setattr(build_datasets, "CURRENT_YEAR", 2027)
    monkeypatch.setattr(build_datasets, "CURRENT_WEEK", 2)
    monkeypatch.setattr(
        build_datasets.validation, "validate_prefecture_coverage", lambda *args, **kwargs: None
    )
    # Last December refresh: 2025 complete, 2026 only through week 50.
    pl.concat(
        [_bullet_frame(2025, w) for w in range(1, 53)]
        + [_bullet_frame(2026, w) for w in range(1, 51)]
    ).with_columns(pl.lit("All-case reporting").alias("source")).write_parquet(
        tmp_path / "bullet.parquet"
    )
    requested: list[int] = []
    # 2026 now published through its 53rd ISO week; nothing published for 2027 yet.
    _patch_bullet_io(build_datasets, monkeypatch, tmp_path, {2026: 53}, requested)

    build_datasets.build_bullet()

    assert requested == [2026, 2027]
    df = pl.read_parquet(tmp_path / "bullet.parquet")
    weeks_2026 = df.filter(pl.col("year") == 2026)["week"]
    assert weeks_2026.max() == 53
    assert weeks_2026.n_unique() == 53
    assert df.filter(pl.col("year") == 2025).height == 52
    assert 2027 not in df["year"].to_list()


def test_build_bullet_preserves_complete_previous_year(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    build_datasets = _load_build_module()
    monkeypatch.setattr(build_datasets, "DATA_DIR", tmp_path)
    monkeypatch.setattr(build_datasets, "BULLET_FIRST_YEAR", 2026)
    monkeypatch.setattr(build_datasets, "CURRENT_YEAR", 2027)
    monkeypatch.setattr(build_datasets, "CURRENT_WEEK", 6)
    monkeypatch.setattr(
        build_datasets.validation, "validate_prefecture_coverage", lambda *args, **kwargs: None
    )
    pl.concat([_bullet_frame(2026, w) for w in range(1, 54)]).with_columns(
        pl.lit("All-case reporting").alias("source")
    ).write_parquet(tmp_path / "bullet.parquet")
    requested: list[int] = []
    _patch_bullet_io(build_datasets, monkeypatch, tmp_path, {2027: 4}, requested)

    build_datasets.build_bullet()

    assert requested == [2027]
    df = pl.read_parquet(tmp_path / "bullet.parquet")
    assert df.filter(pl.col("year") == 2026).height == 53
    assert df.filter(pl.col("year") == 2027)["week"].max() == 4


def test_build_bullet_fails_when_new_year_stays_unpublished_after_grace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    build_datasets = _load_build_module()
    monkeypatch.setattr(build_datasets, "DATA_DIR", tmp_path)
    monkeypatch.setattr(build_datasets, "BULLET_FIRST_YEAR", 2026)
    monkeypatch.setattr(build_datasets, "CURRENT_YEAR", 2027)
    monkeypatch.setattr(build_datasets, "CURRENT_WEEK", build_datasets.NEW_YEAR_GRACE_WEEKS + 1)
    pl.concat([_bullet_frame(2026, w) for w in range(1, 54)]).with_columns(
        pl.lit("All-case reporting").alias("source")
    ).write_parquet(tmp_path / "bullet.parquet")
    _patch_bullet_io(build_datasets, monkeypatch, tmp_path, {}, [])

    with pytest.raises(RuntimeError, match="Failed to build bullet data for 2027"):
        build_datasets.build_bullet()


def test_build_sentinel_redifferences_incomplete_previous_year_at_rollover(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    build_datasets = _load_build_module()
    monkeypatch.setattr(build_datasets, "DATA_DIR", tmp_path)
    monkeypatch.setattr(build_datasets, "SENTINEL_FIRST_YEAR", 2012)  # preliminary only
    monkeypatch.setattr(build_datasets, "CURRENT_YEAR", 2027)
    monkeypatch.setattr(build_datasets, "CURRENT_WEEK", 2)
    monkeypatch.setattr(
        build_datasets.validation, "validate_prefecture_coverage", lambda *args, **kwargs: None
    )

    def sentinel_row(year: int, week: int, count: float) -> dict[str, object]:
        return {
            "prefecture": "Tokyo",
            "disease": "AIDS",
            "year": year,
            "week": week,
            "date": date.fromisocalendar(year, week, 1),
            "count": count,
            "per_sentinel": count / 10,
            "source": "Sentinel surveillance",
            "count_status": "derived",
        }

    # Older years are final; 2025 is complete; 2026 was last refreshed at week 50.
    pl.DataFrame(
        [sentinel_row(year, 1, 1.0) for year in range(2012, 2025)]
        + [sentinel_row(2025, w, 1.0) for w in range(1, 53)]
        + [sentinel_row(2026, w, 2.0) for w in range(1, 51)]
    ).write_parquet(tmp_path / "sentinel.parquet")

    requested: list[int] = []

    def fake_download(name: str, year: int, week: range) -> list[Path]:
        requested.append(year)
        if year != 2026:
            return []
        return [tmp_path / "2026" / f"teitenrui{w:02d}.csv" for w in range(1, 54)]

    def fake_read(path: Path) -> pl.DataFrame:
        week = int(path.stem.removeprefix("teitenrui"))
        # Cumulative year-to-date counts: 2 per week.
        return pl.DataFrame([sentinel_row(2026, week, 2.0 * week)])

    monkeypatch.setattr(build_datasets.io, "download", fake_download)
    monkeypatch.setattr(build_datasets.io, "_read_sentinel_en_pl", fake_read)

    build_datasets.build_sentinel()

    assert requested == [2026, 2027]
    result = pl.read_parquet(tmp_path / "sentinel.parquet")
    year_2026 = result.filter(pl.col("year") == 2026).sort("week")
    assert year_2026["week"].to_list() == list(range(1, 54))
    assert set(year_2026["count"].to_list()) == {2.0}
    assert set(result.filter(pl.col("year") == 2025)["count"].to_list()) == {1.0}


PREFECTURES = list(PREFECTURE_ISO_MAP)


def _full_year_grid(year: int, last_week: int | None = None) -> pl.DataFrame:
    """Every prefecture x ISO week of a year (optionally only up to last_week)."""
    weeks = range(1, (last_week or iso_weeks_in_year(year)) + 1)
    return (
        pl.DataFrame(
            {
                "prefecture": [p for p in PREFECTURES for _ in weeks],
                "week": [w for _ in PREFECTURES for w in weeks],
            }
        )
        .with_columns(pl.lit(year).alias("year"))
        .with_columns(
            pl.struct(["year", "week"])
            .map_elements(
                lambda v: date.fromisocalendar(v["year"], v["week"], 1), return_dtype=pl.Date
            )
            .alias("date"),
        )
    )


def _annual_sex_frame(year: int, last_week: int | None = None) -> pl.DataFrame:
    """An annual sex table as io.read returns it: total and male (female derived)."""
    grid = _full_year_grid(year, last_week)
    return pl.concat(
        [
            grid.with_columns(pl.lit(7).alias("count"), pl.lit("total").alias("category")),
            grid.with_columns(pl.lit(4).alias("count"), pl.lit("male").alias("category")),
        ]
    ).with_columns(pl.lit("Measles").alias("disease"), pl.lit("Confirmed cases").alias("source"))


def _sex_rows(year: int, categories: dict[str, int]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "prefecture": ["Tokyo"] * len(categories),
            "year": [year] * len(categories),
            "week": [1] * len(categories),
            "date": [date.fromisocalendar(year, 1, 1)] * len(categories),
            "count": list(categories.values()),
            "category": list(categories),
            "disease": ["Measles"] * len(categories),
            "source": ["Confirmed cases"] * len(categories),
        }
    )


def test_build_sex_adds_newly_published_annual_year_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    build_datasets = _load_build_module()
    monkeypatch.setattr(build_datasets, "DATA_DIR", tmp_path)
    monkeypatch.setattr(build_datasets, "SEX_FIRST_YEAR", 2023)
    monkeypatch.setattr(build_datasets, "CURRENT_YEAR", 2026)
    monkeypatch.setattr(
        build_datasets.validation, "validate_prefecture_coverage", lambda *args, **kwargs: None
    )
    _sex_rows(2023, {"total": 5, "male": 3, "female": 2}).write_parquet(
        tmp_path / "sex_prefecture.parquet"
    )
    published = {2024}
    monkeypatch.setattr(io, "annual_table_available", lambda table, year: year in published)
    downloaded: list[int] = []

    def fake_download(table: str, year: int) -> Path:
        downloaded.append(year)
        return tmp_path / f"{year}_Syu_01_1.xlsx"

    monkeypatch.setattr(io, "download", fake_download)
    # The annual reader yields total and male only; female is derived.
    monkeypatch.setattr(io, "read", lambda path, type: _annual_sex_frame(2024))

    build_datasets.build_sex()

    df = pl.read_parquet(tmp_path / "sex_prefecture.parquet")
    assert downloaded == [2024]
    assert df.filter(pl.col("year") == 2023)["count"].sort().to_list() == [2, 3, 5]
    female_2024 = df.filter((pl.col("year") == 2024) & (pl.col("category") == "female"))
    assert set(female_2024["count"].to_list()) == {3}
    assert female_2024.height == 47 * 52

    before = (tmp_path / "sex_prefecture.parquet").read_bytes()
    build_datasets.build_sex()
    assert downloaded == [2024]  # nothing new: no download, file untouched
    assert (tmp_path / "sex_prefecture.parquet").read_bytes() == before


def test_build_sentinel_switches_preliminary_year_to_annual_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    build_datasets = _load_build_module()
    monkeypatch.setattr(build_datasets, "DATA_DIR", tmp_path)
    monkeypatch.setattr(build_datasets, "SENTINEL_FIRST_YEAR", 2023)
    monkeypatch.setattr(build_datasets, "RAPID_SENTINEL_FIRST_YEAR", 2023)
    monkeypatch.setattr(build_datasets, "CURRENT_YEAR", 2025)
    monkeypatch.setattr(build_datasets, "CURRENT_WEEK", 2)
    monkeypatch.setattr(
        build_datasets.validation, "validate_prefecture_coverage", lambda *args, **kwargs: None
    )

    def rows(year: int, weeks: range, count: float, status: str) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "prefecture": ["Tokyo"] * len(weeks),
                "disease": ["Mumps"] * len(weeks),
                "year": [year] * len(weeks),
                "week": list(weeks),
                "date": [date.fromisocalendar(year, w, 1) for w in weeks],
                "count": [count] * len(weeks),
                "per_sentinel": [count / 10] * len(weeks),
                "source": ["Sentinel surveillance"] * len(weeks),
                "count_status": [status] * len(weeks),
            }
        )

    # 2023 already annual; 2024 complete but preliminary.
    pl.concat(
        [rows(2023, range(1, 53), 1.0, "annual"), rows(2024, range(1, 53), 2.0, "derived")]
    ).write_parquet(tmp_path / "sentinel.parquet")
    monkeypatch.setattr(io, "annual_table_available", lambda table, year: year == 2024)
    read_years: list[int] = []

    def fake_annual(year: int) -> pl.DataFrame:
        read_years.append(year)
        return _full_year_grid(year).with_columns(
            pl.lit("Mumps").alias("disease"),
            pl.lit(9.0).alias("count"),
            pl.lit(0.9).alias("per_sentinel"),
            pl.lit("Sentinel surveillance").alias("source"),
            pl.lit("annual").alias("count_status"),
        )

    monkeypatch.setattr(build_datasets, "_read_annual_sentinel_year", fake_annual)
    # Nothing published for 2025 yet (early January).
    monkeypatch.setattr(build_datasets.io, "download", lambda *args, **kwargs: [])

    build_datasets.build_sentinel()

    df = pl.read_parquet(tmp_path / "sentinel.parquet")
    assert read_years == [2024]  # the existing annual year is not re-read
    by_year = df.group_by("year").agg(pl.col("count").max(), pl.col("count_status").unique())
    result = {row[0]: (row[1], row[2]) for row in by_year.sort("year").rows()}
    assert result[2023] == (1.0, ["annual"])
    assert result[2024] == (9.0, ["annual"])


def test_read_annual_sentinel_year_handles_silent_weeks_and_rsv_rates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    build_datasets = _load_build_module()
    base = {"year": [2011] * 4, "week": [10] * 4, "date": [date(2011, 3, 7)] * 4}
    diseases = ["Mumps", "Herpangina", build_datasets.RSV, "Pertussis"]
    counts = pl.DataFrame(
        {
            "prefecture": ["Tokyo"] * 4 + ["Fukushima"] * 4,
            **{k: v * 2 for k, v in base.items()},
            "disease": diseases * 2,
            "count": [10.0, 4.0, 6.0, 1.0, 0.0, 0.0, 0.0, 0.0],
        }
    )
    rates = counts.rename({"count": "per_sentinel"}).with_columns(
        # Tokyo has 5 pediatric sentinels; RSV has no rate column; Fukushima is blank.
        pl.Series("per_sentinel", [2.0, 0.8, None, 0.1, None, None, None, None])
    )
    monkeypatch.setattr(io, "download_annual", lambda table, year: Path(table))
    monkeypatch.setattr(
        io,
        "read_annual_sentinel",
        lambda path, year, value_name="count": counts if value_name == "count" else rates,
    )

    monkeypatch.setattr(build_datasets, "_annual_year_complete", lambda *a, **k: True)
    df = build_datasets._read_annual_sentinel_year(2011).sort(["prefecture", "disease"])

    fukushima = df.filter(pl.col("prefecture") == "Fukushima")
    assert set(fukushima["count_status"].to_list()) == {"missing"}
    assert fukushima["count"].null_count() == 4
    tokyo_rsv = df.filter(
        (pl.col("prefecture") == "Tokyo") & (pl.col("disease") == build_datasets.RSV)
    )
    assert tokyo_rsv["per_sentinel"].to_list() == [pytest.approx(6.0 / 5.0)]
    assert set(df.filter(pl.col("prefecture") == "Tokyo")["count_status"].to_list()) == {"annual"}


def test_build_sex_does_not_add_incomplete_annual_year(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    build_datasets = _load_build_module()
    monkeypatch.setattr(build_datasets, "DATA_DIR", tmp_path)
    monkeypatch.setattr(build_datasets, "SEX_FIRST_YEAR", 2023)
    monkeypatch.setattr(build_datasets, "CURRENT_YEAR", 2026)
    _sex_rows(2023, {"total": 5, "male": 3, "female": 2}).write_parquet(
        tmp_path / "sex_prefecture.parquet"
    )
    before = (tmp_path / "sex_prefecture.parquet").read_bytes()
    monkeypatch.setattr(io, "annual_table_available", lambda table, year: year == 2024)
    monkeypatch.setattr(io, "download", lambda table, year: tmp_path / "x.xlsx")
    # A partially published 2024 table (weeks 1-50 only) must not be frozen in.
    monkeypatch.setattr(io, "read", lambda path, type: _annual_sex_frame(2024, last_week=50))

    build_datasets.build_sex()

    assert (tmp_path / "sex_prefecture.parquet").read_bytes() == before


def test_build_sentinel_keeps_preliminary_year_when_annual_table_is_incomplete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    build_datasets = _load_build_module()
    monkeypatch.setattr(build_datasets, "DATA_DIR", tmp_path)
    monkeypatch.setattr(build_datasets, "SENTINEL_FIRST_YEAR", 2024)
    monkeypatch.setattr(build_datasets, "RAPID_SENTINEL_FIRST_YEAR", 2024)
    monkeypatch.setattr(build_datasets, "CURRENT_YEAR", 2025)
    monkeypatch.setattr(build_datasets, "CURRENT_WEEK", 2)
    monkeypatch.setattr(
        build_datasets.validation, "validate_prefecture_coverage", lambda *args, **kwargs: None
    )
    preliminary = _full_year_grid(2024).with_columns(
        pl.lit("Mumps").alias("disease"),
        pl.lit(2.0).alias("count"),
        pl.lit(0.2).alias("per_sentinel"),
        pl.lit("Sentinel surveillance").alias("source"),
        pl.lit("derived").alias("count_status"),
    )
    preliminary.write_parquet(tmp_path / "sentinel.parquet")
    monkeypatch.setattr(io, "annual_table_available", lambda table, year: year == 2024)
    monkeypatch.setattr(
        build_datasets,
        "_read_annual_sentinel_year",
        lambda year: preliminary.filter(pl.col("week") <= 50).with_columns(
            pl.lit(9.0).alias("count"), pl.lit("annual").alias("count_status")
        ),
    )
    monkeypatch.setattr(build_datasets.io, "download", lambda *args, **kwargs: [])

    build_datasets.build_sentinel()

    df = pl.read_parquet(tmp_path / "sentinel.parquet")
    assert set(df["count_status"].to_list()) == {"derived"}
    assert df.height == preliminary.height


def test_build_sentinel_keeps_preliminary_year_when_rate_table_is_incomplete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    build_datasets = _load_build_module()
    monkeypatch.setattr(build_datasets, "RSV", "none")
    grid = _full_year_grid(2024).with_columns(pl.lit("Mumps").alias("disease"))
    monkeypatch.setattr(io, "download_annual", lambda table, year: Path(table))
    monkeypatch.setattr(
        io,
        "read_annual_sentinel",
        lambda path, year, value_name="count": (
            grid.with_columns(pl.lit(3.0).alias("count"))
            if value_name == "count"
            else grid.filter(pl.col("week") <= 50).with_columns(pl.lit(0.3).alias("per_sentinel"))
        ),
    )

    with pytest.raises(build_datasets.IncompleteAnnualTableError):
        build_datasets._read_annual_sentinel_year(2024)


def test_build_sentinel_fails_when_historical_annual_run_stops_early(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    build_datasets = _load_build_module()
    monkeypatch.setattr(build_datasets, "DATA_DIR", tmp_path)
    monkeypatch.setattr(build_datasets, "CURRENT_YEAR", 2026)
    # 1999 is available, then a transient error: 2000-2011 would be missing.
    monkeypatch.setattr(io, "annual_table_available", lambda table, year: year == 1999)
    monkeypatch.setattr(
        build_datasets,
        "_read_annual_sentinel_year",
        lambda year: _full_year_grid(year)
        .filter(pl.col("week") >= 14)
        .with_columns(
            pl.lit("Mumps").alias("disease"),
            pl.lit(1.0).alias("count"),
            pl.lit("annual").alias("count_status"),
        ),
    )

    with pytest.raises(RuntimeError, match="Annual sentinel tables stop at 1999"):
        build_datasets.build_sentinel()


def test_annual_year_complete_rejects_missing_disease_block() -> None:
    build_datasets = _load_build_module()
    grid = _full_year_grid(2024)
    table = pl.concat(
        [
            grid.with_columns(pl.lit("Mumps").alias("disease")),
            # Herpangina's block is missing for the last week.
            grid.filter(pl.col("week") < 52).with_columns(pl.lit("Herpangina").alias("disease")),
        ]
    )
    assert not build_datasets._annual_year_complete(table, 2024, "sentinel")
    complete = pl.concat(
        [grid.with_columns(pl.lit(d).alias("disease")) for d in ("Mumps", "Herpangina")]
    )
    assert build_datasets._annual_year_complete(complete, 2024, "sentinel")
    # A disease already published from preliminary data must not disappear.
    assert not build_datasets._annual_year_complete(
        complete, 2024, "sentinel", required_diseases={"Mumps", "Herpangina", "Pertussis"}
    )


def test_derive_female_works_per_disease() -> None:
    build_datasets = _load_build_module()
    measles = _sex_rows(2024, {"total": 5, "male": 3, "female": 2})
    mumps = _sex_rows(2024, {"total": 7, "male": 4}).with_columns(pl.lit("Mumps").alias("disease"))
    out = build_datasets._derive_female(pl.concat([measles, mumps]))
    female = out.filter(pl.col("category") == "female").sort("disease")
    assert female.select(["disease", "count"]).rows() == [("Measles", 2), ("Mumps", 3)]


def test_build_bullet_fails_when_a_downloaded_week_parses_to_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    build_datasets = _load_build_module()
    monkeypatch.setattr(build_datasets, "DATA_DIR", tmp_path)
    monkeypatch.setattr(build_datasets, "BULLET_FIRST_YEAR", 2026)
    monkeypatch.setattr(build_datasets, "CURRENT_YEAR", 2026)
    monkeypatch.setattr(build_datasets, "CURRENT_WEEK", 2)
    monkeypatch.setattr(io, "download", lambda *args, **kwargs: [tmp_path / "zensu01.csv"])
    # io.read logs and returns an empty frame for a file it cannot parse.
    monkeypatch.setattr(io, "read", lambda path, type: pl.DataFrame())

    with pytest.raises(RuntimeError, match="Failed to build bullet data for 2026") as exc:
        build_datasets.build_bullet()
    assert "zensu01.csv parsed to no rows" in str(exc.value.__cause__)


def test_build_sentinel_fails_when_a_downloaded_week_parses_to_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    build_datasets = _load_build_module()
    monkeypatch.setattr(build_datasets, "DATA_DIR", tmp_path)
    monkeypatch.setattr(build_datasets, "SENTINEL_FIRST_YEAR", 2026)
    monkeypatch.setattr(build_datasets, "RAPID_SENTINEL_FIRST_YEAR", 2026)
    monkeypatch.setattr(build_datasets, "CURRENT_YEAR", 2026)
    monkeypatch.setattr(build_datasets, "CURRENT_WEEK", 2)
    monkeypatch.setattr(io, "download", lambda *args, **kwargs: [tmp_path / "teitenrui01.csv"])
    monkeypatch.setattr(io, "_read_sentinel_en_pl", lambda path: pl.DataFrame())

    with pytest.raises(RuntimeError, match="Failed to build sentinel data for 2026") as exc:
        build_datasets.build_sentinel()
    assert "teitenrui01.csv parsed to no rows" in str(exc.value.__cause__)
