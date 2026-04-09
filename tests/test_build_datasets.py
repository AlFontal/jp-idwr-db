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

    def fake_download(name: str, year: int, week: range) -> list[Path]:
        assert name == "bullet"
        assert year == 2026
        assert list(week) == list(range(1, 14))
        return [tmp_path / "2026" / "zensu11.csv"]

    def fake_read(path: Path, type: str) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "prefecture": ["Hokkaido", "Tokyo"],
                "disease": ["Tuberculosis", "Measles"],
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
                "date": [date(2026, 1, 5)],
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
