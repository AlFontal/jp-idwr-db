"""Tests for download path handling."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from jpinfectpy import io


def test_download_sentinel_separates_files_by_year(tmp_path: Path, monkeypatch: Any) -> None:
    """Store week-based sentinel files under year-specific directories."""

    def fake_url_sentinel(year: int, week: int | list[int] | None = None) -> list[str]:
        week_num = int(week) if isinstance(week, int) else 4
        return [f"https://example.invalid/{year}/{week_num:02d}/teitenrui{week_num:02d}.csv"]

    def fake_download_urls(urls: list[str], dest_dir: Path, config: Any) -> list[Path]:
        out: list[Path] = []
        for url in urls:
            year = url.split("/")[-3]
            filename = url.split("/")[-1]
            path = dest_dir / filename
            path.write_text(f"year={year}", encoding="utf-8")
            out.append(path)
        return out

    monkeypatch.setattr(io, "url_sentinel", fake_url_sentinel)
    monkeypatch.setattr(io, "download_urls", fake_download_urls)

    paths_2024 = io.download("sentinel", 2024, week=4, out_dir=tmp_path)
    paths_2025 = io.download("sentinel", 2025, week=4, out_dir=tmp_path)

    assert isinstance(paths_2024, list)
    assert isinstance(paths_2025, list)
    assert paths_2024[0].parent == tmp_path / "2024"
    assert paths_2025[0].parent == tmp_path / "2025"
    assert paths_2024[0].read_text(encoding="utf-8") == "year=2024"
    assert paths_2025[0].read_text(encoding="utf-8") == "year=2025"
