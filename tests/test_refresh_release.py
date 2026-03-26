from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from jp_idwr_db import refresh_release


def _write_refresh_repo(repo_root: Path) -> None:
    (repo_root / "data" / "parquet").mkdir(parents=True, exist_ok=True)
    (repo_root / "docs").mkdir(parents=True, exist_ok=True)
    (repo_root / "src" / "jp_idwr_db").mkdir(parents=True, exist_ok=True)

    (repo_root / "pyproject.toml").write_text('[project]\nversion = "0.2.5"\n', encoding="utf-8")
    (repo_root / "src" / "jp_idwr_db" / "__init__.py").write_text(
        '__version__ = "0.2.5"\n__data_version__ = __version__\n',
        encoding="utf-8",
    )
    (repo_root / "src" / "jp_idwr_db" / "config.py").write_text(
        'user_agent: str = "jp_idwr_db/0.2.5 (+https://github.com/AlFontal/jp-idwr-db)"\n',
        encoding="utf-8",
    )
    (repo_root / "CHANGELOG.md").write_text(
        "# Changelog\n\n## 0.2.5 - 2026-02-07\n\n- Previous release.\n", encoding="utf-8"
    )
    (repo_root / "docs" / "DISEASES.md").write_text("# Disease Coverage\n", encoding="utf-8")

    pl.DataFrame({"year": [2026], "week": [6]}).write_parquet(
        repo_root / "data/parquet/bullet.parquet"
    )
    pl.DataFrame({"year": [2026], "week": [4]}).write_parquet(
        repo_root / "data/parquet/sentinel.parquet"
    )
    pl.DataFrame({"year": [2026], "week": [6]}).write_parquet(
        repo_root / "data/parquet/unified.parquet"
    )


def test_prepare_refresh_release_detects_noop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    _write_refresh_repo(repo_root)

    monkeypatch.setattr(refresh_release, "rebuild_release_outputs", lambda root: None)

    outputs = refresh_release.prepare_refresh_release(
        repo_root=repo_root, dry_run=True, release_date=date(2026, 3, 26)
    )

    assert outputs.changed is False
    assert outputs.version == "2026.3.26"
    assert outputs.tag == "v2026.3.26"
    assert refresh_release.current_version(repo_root) == "0.2.5"


def test_prepare_refresh_release_dry_run_restores_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    _write_refresh_repo(repo_root)
    original_digest = refresh_release._sha256(repo_root / "data/parquet/bullet.parquet")

    def fake_rebuild(root: Path) -> None:
        pl.DataFrame({"year": [2026], "week": [11]}).write_parquet(
            root / "data/parquet/bullet.parquet"
        )
        pl.DataFrame({"year": [2026], "week": [11]}).write_parquet(
            root / "data/parquet/sentinel.parquet"
        )
        pl.DataFrame({"year": [2026], "week": [11]}).write_parquet(
            root / "data/parquet/unified.parquet"
        )
        (root / "docs" / "DISEASES.md").write_text("# Updated\n", encoding="utf-8")

    monkeypatch.setattr(refresh_release, "rebuild_release_outputs", fake_rebuild)

    outputs = refresh_release.prepare_refresh_release(
        repo_root=repo_root, dry_run=True, release_date=date(2026, 3, 26)
    )

    assert outputs.changed is True
    assert outputs.latest_bullet_week == "2026-W11"
    assert refresh_release._sha256(repo_root / "data/parquet/bullet.parquet") == original_digest
    assert refresh_release.current_version(repo_root) == "0.2.5"


def test_prepare_refresh_release_updates_versions_and_changelog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    _write_refresh_repo(repo_root)

    def fake_rebuild(root: Path) -> None:
        pl.DataFrame({"year": [2026], "week": [11]}).write_parquet(
            root / "data/parquet/bullet.parquet"
        )
        pl.DataFrame({"year": [2026], "week": [11]}).write_parquet(
            root / "data/parquet/sentinel.parquet"
        )
        pl.DataFrame({"year": [2026], "week": [11]}).write_parquet(
            root / "data/parquet/unified.parquet"
        )
        (root / "docs" / "DISEASES.md").write_text("# Updated\n", encoding="utf-8")

    monkeypatch.setattr(refresh_release, "rebuild_release_outputs", fake_rebuild)

    outputs = refresh_release.prepare_refresh_release(
        repo_root=repo_root, release_date=date(2026, 3, 26)
    )

    assert outputs.changed is True
    assert outputs.version == "2026.3.26"
    assert refresh_release.current_version(repo_root) == "2026.3.26"
    assert "jp_idwr_db/2026.3.26" in (repo_root / "src" / "jp_idwr_db" / "config.py").read_text(
        encoding="utf-8"
    )
    changelog = (repo_root / "CHANGELOG.md").read_text(encoding="utf-8")
    assert changelog.startswith("# Changelog\n\n## 2026.3.26 - 2026-03-26\n")
    assert "2026-W11" in changelog


def test_next_calver_version_same_day_gets_post_release() -> None:
    assert refresh_release.next_calver_version("2026.3.26", date(2026, 3, 26)) == "2026.3.26.post1"
    assert (
        refresh_release.next_calver_version("2026.3.26.post1", date(2026, 3, 26))
        == "2026.3.26.post2"
    )


def test_write_outputs(tmp_path: Path) -> None:
    outputs = refresh_release.RefreshOutputs(
        changed=True,
        version="2026.3.26",
        tag="v2026.3.26",
        latest_bullet_week="2026-W11",
        latest_sentinel_week="2026-W11",
    )
    output_path = tmp_path / "github-output.txt"

    refresh_release.write_outputs(outputs, output_path)

    assert output_path.read_text(encoding="utf-8").splitlines() == [
        "changed=true",
        "version=2026.3.26",
        "tag=v2026.3.26",
        "latest_bullet_week=2026-W11",
        "latest_sentinel_week=2026-W11",
    ]
