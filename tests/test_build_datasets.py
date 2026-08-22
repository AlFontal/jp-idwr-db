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

    monkeypatch.setattr(build_datasets.download, "download", fake_download)
    monkeypatch.setattr(build_datasets.read, "read", fake_read)

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
        build_datasets.download,
        "download",
        lambda name, year, week: [tmp_path / "2026" / "zensu01.csv"],
    )
    monkeypatch.setattr(
        build_datasets.read,
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
            "week": [1, 2],
            "date": [date(2024, 12, 30), date(2025, 1, 6)],
            "count": [7.0, 8.0],
            "per_sentinel": [0.7, 0.8],
            "source": ["Sentinel surveillance", "Sentinel surveillance"],
        }
    ).write_parquet(tmp_path / "sentinel.parquet")

    paths = [tmp_path / "teitenrui01.csv", tmp_path / "teitenrui02.csv"]
    monkeypatch.setattr(
        build_datasets.download,
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
