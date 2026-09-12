from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from jp_idwr_db import refresh_release


@pytest.fixture(autouse=True)
def _allow_small_synthetic_prefecture_sets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        refresh_release.validation,
        "validate_prefecture_coverage",
        lambda *args, **kwargs: None,
    )


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
    (repo_root / "CITATION.cff").write_text("version: 0.2.5\n", encoding="utf-8")
    (repo_root / "uv.lock").write_text(
        '[[package]]\nname = "jp-idwr-db"\nversion = "0.2.5"\n',
        encoding="utf-8",
    )

    base_frame = {
        "prefecture": ["Tokyo"],
        "year": [2026],
        "week": [6],
        "disease": ["Tuberculosis"],
        "count": [1],
        "source": ["All-case reporting"],
    }
    pl.DataFrame(base_frame).write_parquet(repo_root / "data/parquet/bullet.parquet")
    pl.DataFrame(
        {
            **base_frame,
            "week": [4],
            "per_sentinel": [0.1],
            "source": ["Sentinel surveillance"],
        }
    ).write_parquet(repo_root / "data/parquet/sentinel.parquet")
    pl.DataFrame(
        {**base_frame, "category": ["total"], "source": ["All-case reporting"]}
    ).write_parquet(repo_root / "data/parquet/unified.parquet")


def _write_extended_refresh_outputs(repo_root: Path) -> None:
    data_dir = repo_root / "data" / "parquet"
    pl.DataFrame(
        {
            "prefecture": ["Tokyo", "Tokyo"],
            "year": [2026, 2026],
            "week": [6, 11],
            "disease": ["Tuberculosis", "Tuberculosis"],
            "count": [1, 1],
            "source": ["All-case reporting", "All-case reporting"],
        }
    ).write_parquet(data_dir / "bullet.parquet")
    pl.DataFrame(
        {
            "prefecture": ["Tokyo", "Tokyo"],
            "year": [2026, 2026],
            "week": [4, 11],
            "disease": ["Tuberculosis", "RSV"],
            "count": [1.0, 1.0],
            "source": ["Sentinel surveillance", "Sentinel surveillance"],
            "per_sentinel": [0.1, 0.1],
        }
    ).write_parquet(data_dir / "sentinel.parquet")
    pl.DataFrame(
        {
            "prefecture": ["Tokyo", "Tokyo"],
            "year": [2026, 2026],
            "week": [6, 11],
            "disease": ["Tuberculosis", "Tuberculosis"],
            "count": [1, 1],
            "source": ["All-case reporting", "All-case reporting"],
            "category": ["total", "total"],
        }
    ).write_parquet(data_dir / "unified.parquet")
    (repo_root / "docs" / "DISEASES.md").write_text("# Updated\n", encoding="utf-8")


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

    monkeypatch.setattr(refresh_release, "rebuild_release_outputs", _write_extended_refresh_outputs)

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

    monkeypatch.setattr(refresh_release, "rebuild_release_outputs", _write_extended_refresh_outputs)

    outputs = refresh_release.prepare_refresh_release(
        repo_root=repo_root, release_date=date(2026, 3, 26)
    )

    assert outputs.changed is True
    assert outputs.version == "2026.3.26"
    assert refresh_release.current_version(repo_root) == "2026.3.26"
    assert "jp_idwr_db/2026.3.26" in (repo_root / "src" / "jp_idwr_db" / "config.py").read_text(
        encoding="utf-8"
    )
    assert "version: 2026.3.26" in (repo_root / "CITATION.cff").read_text(encoding="utf-8")
    assert 'version = "2026.3.26"' in (repo_root / "uv.lock").read_text(encoding="utf-8")
    changelog = (repo_root / "CHANGELOG.md").read_text(encoding="utf-8")
    assert changelog.startswith("# Changelog\n\n## 2026.3.26 - 2026-03-26\n")
    assert "2026-W11" in changelog


def test_prepare_refresh_release_validates_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    _write_refresh_repo(repo_root)
    called: list[Path] = []

    monkeypatch.setattr(refresh_release, "rebuild_release_outputs", lambda root: None)
    monkeypatch.setattr(
        refresh_release,
        "_validate_release_outputs",
        lambda root: called.append(root),
    )

    refresh_release.prepare_refresh_release(
        repo_root=repo_root, dry_run=True, release_date=date(2026, 3, 26)
    )

    assert called == [repo_root.resolve()]


def test_failed_refresh_restores_data_and_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    _write_refresh_repo(repo_root)
    original_data = refresh_release._sha256(repo_root / "data/parquet/bullet.parquet")
    original_project = (repo_root / "pyproject.toml").read_text(encoding="utf-8")

    def fail_after_mutation(root: Path) -> None:
        pl.DataFrame(
            {
                "prefecture": ["Tokyo"],
                "year": [2025],
                "week": [1],
                "disease": ["Tuberculosis"],
                "count": [1],
                "source": ["All-case reporting"],
            }
        ).write_parquet(root / "data/parquet/bullet.parquet")
        (root / "pyproject.toml").write_text('[project]\nversion = "broken"\n', encoding="utf-8")

    monkeypatch.setattr(refresh_release, "rebuild_release_outputs", fail_after_mutation)

    with pytest.raises(ValueError):
        refresh_release.prepare_refresh_release(repo_root=repo_root)

    assert refresh_release._sha256(repo_root / "data/parquet/bullet.parquet") == original_data
    assert (repo_root / "pyproject.toml").read_text(encoding="utf-8") == original_project


def test_validate_release_outputs_rejects_invalid_week(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    _write_refresh_repo(repo_root)
    pl.DataFrame(
        {
            "prefecture": ["Tokyo"],
            "year": [2026],
            "week": [99],
            "disease": ["X"],
            "count": [1],
            "source": ["All-case reporting"],
        }
    ).write_parquet(repo_root / "data/parquet/bullet.parquet")

    with pytest.raises(ValueError, match="Week values out of valid range"):
        refresh_release._validate_release_outputs(repo_root)


def test_validate_release_outputs_rejects_sentinel_null_rate_spike(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    _write_refresh_repo(repo_root)
    pl.DataFrame(
        {
            "prefecture": ["Tokyo", "Osaka"],
            "year": [2026, 2026],
            "week": [4, 4],
            "disease": ["RSV", "RSV"],
            "count": [None, None],
            "per_sentinel": [None, None],
            "source": ["Sentinel surveillance", "Sentinel surveillance"],
        },
        schema_overrides={"count": pl.Float64, "per_sentinel": pl.Float64},
    ).write_parquet(repo_root / "data/parquet/sentinel.parquet")

    with pytest.raises(ValueError, match=r"Null rate for count exceeds 25\.0%"):
        refresh_release._validate_release_outputs(repo_root)


def test_validate_release_preservation_rejects_latest_period_regression(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    backup_root = tmp_path / "backup"
    _write_refresh_repo(repo_root)
    refresh_release._backup_targets(repo_root, backup_root)
    for filename in ("bullet.parquet", "sentinel.parquet", "unified.parquet"):
        path = repo_root / "data" / "parquet" / filename
        pl.read_parquet(path).with_columns(pl.lit(3).alias("week")).write_parquet(path)

    with pytest.raises(ValueError, match="Latest period regressed"):
        refresh_release._validate_release_preservation(repo_root, backup_root)


def test_validate_release_preservation_rejects_historical_change(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    backup_root = tmp_path / "backup"
    _write_refresh_repo(repo_root)
    data_dir = repo_root / "data" / "parquet"
    for filename in ("bullet.parquet", "sentinel.parquet", "unified.parquet"):
        path = data_dir / filename
        current = pl.read_parquet(path)
        historical = current.with_columns(pl.lit(2025).alias("year"))
        pl.concat([historical, current], how="diagonal_relaxed").write_parquet(path)
    refresh_release._backup_targets(repo_root, backup_root)
    path = data_dir / "bullet.parquet"
    pl.read_parquet(path).with_columns(
        pl.when(pl.col("year") == 2025).then(99).otherwise(pl.col("count")).alias("count")
    ).write_parquet(path)

    with pytest.raises(ValueError, match="Stable historical rows changed"):
        refresh_release._validate_release_preservation(repo_root, backup_root)


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
