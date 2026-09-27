from __future__ import annotations

from datetime import date
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType

import polars as pl
import pytest


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
    monkeypatch.setattr(build_datasets, "LAST_HISTORICAL_YEAR", 2025)
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
    monkeypatch.setattr(build_datasets, "LAST_HISTORICAL_YEAR", 2025)
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
    monkeypatch.setattr(build_datasets, "LAST_HISTORICAL_YEAR", 2025)
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
    monkeypatch.setattr(build_datasets, "LAST_HISTORICAL_YEAR", 2024)
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
    monkeypatch.setattr(build_datasets, "LAST_HISTORICAL_YEAR", 2025)
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
    monkeypatch.setattr(build_datasets, "LAST_HISTORICAL_YEAR", 2025)
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
