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
