from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest


@pytest.fixture
def local_data_dir(tmp_path: Path) -> Path:
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    sex_df = pl.DataFrame(
        {
            "prefecture": ["Total No.", "Total No.", "Total No."],
            "year": [2023, 2023, 2023],
            "week": [1, 1, 1],
            "date": [date(2023, 1, 2), date(2023, 1, 2), date(2023, 1, 2)],
            "count": [100.0, 60.0, 40.0],
            "category": ["total", "male", "female"],
            "disease": ["Tuberculosis", "Tuberculosis", "Tuberculosis"],
            "source": ["Confirmed cases", "Confirmed cases", "Confirmed cases"],
        }
    )
    place_df = pl.DataFrame(
        {
            "prefecture": ["Tokyo"],
            "year": [2023],
            "week": [1],
            "date": [date(2023, 1, 2)],
            "count": [2.0],
            "category": ["total"],
            "disease": ["Measles"],
            "source": ["Confirmed cases"],
        }
    )
    bullet_df = pl.DataFrame(
        {
            "prefecture": ["Total No.", "Hokkaido"],
            "disease": ["Tuberculosis", "Measles"],
            "count": [10.0, 1.0],
            "week": [1, 2],
            "source": ["Confirmed cases", "Confirmed cases"],
            "year": [2023, 2023],
            "date": [date(2023, 1, 2), date(2023, 1, 9)],
            "category": ["total", "total"],
        }
    )
    sentinel_df = pl.DataFrame(
        {
            "prefecture": ["Tokyo"],
            "disease": ["Respiratory syncytial virus infection"],
            "year": [2023],
            "week": [2],
            "date": [date(2023, 1, 9)],
            "count": [5.0],
            "per_sentinel": [1.2],
            "source": ["Sentinel surveillance"],
            "category": ["total"],
        }
    )
    unified_df = pl.DataFrame(
        {
            "prefecture": ["Total No.", "Hokkaido", "Tokyo"],
            "year": [2023, 2020, 2023],
            "week": [1, 12, 2],
            "date": [date(2023, 1, 2), date(2020, 3, 16), date(2023, 1, 9)],
            "count": [10.0, 5.0, 5.0],
            "category": ["total", "total", "total"],
            "disease": ["Tuberculosis", "Tuberculosis", "Measles"],
            "source": ["Confirmed cases", "Confirmed cases", "Sentinel surveillance"],
            "per_sentinel": [None, None, 1.2],
        }
    )
    prefecture_en_df = pl.DataFrame({"prefecture": ["Hokkaido", "Tokyo", "Total No."]})

    sex_df.write_parquet(data_dir / "sex_prefecture.parquet")
    place_df.write_parquet(data_dir / "place_prefecture.parquet")
    bullet_df.write_parquet(data_dir / "bullet.parquet")
    sentinel_df.write_parquet(data_dir / "sentinel.parquet")
    unified_df.write_parquet(data_dir / "unified.parquet")
    prefecture_en_df.write_parquet(data_dir / "prefecture_en.parquet")
    return data_dir


@pytest.fixture(autouse=True)
def patch_data_source(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch, local_data_dir: Path
) -> None:
    module_name = request.module.__name__ if request.module else ""
    if module_name.endswith("test_data_manager"):
        return
    monkeypatch.setattr("jp_idwr_db.datasets.ensure_data", lambda **_: local_data_dir)
