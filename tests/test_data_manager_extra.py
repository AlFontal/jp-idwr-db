from __future__ import annotations

import json
from pathlib import Path

import pytest

from jp_idwr_db import data_manager


def test_ensure_data_uses_complete_marker_without_redownloading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache_dir = tmp_path / "cache"
    data_dir = cache_dir / "data" / "v-test"
    data_dir.mkdir(parents=True)
    (data_dir / ".complete").write_text("", encoding="utf-8")

    monkeypatch.setenv("JPINFECT_CACHE_DIR", str(cache_dir))
    monkeypatch.setattr(
        data_manager,
        "_download_manifest",
        lambda *args, **kwargs: pytest.fail("should not download when marker exists"),
    )

    resolved = data_manager.ensure_data(version="v-test")

    assert resolved == data_dir


def test_sync_from_manifest_requires_all_expected_datasets(tmp_path: Path) -> None:
    manifest = {
        "spec_version": "1.0.0",
        "dataset_id": "jp_idwr_db",
        "data_version": "test",
        "release_tag": "v-test",
        "published_at": "2025-01-01T00:00:00Z",
        "license": "GPL-3.0-or-later",
        "homepage": "https://example.invalid",
        "assets_base_url": "https://example.invalid/download/v-test",
        "tables": [
            {
                "name": "bullet",
                "file": "bullet.parquet",
                "format": "parquet",
                "size_bytes": 1,
                "sha256": "0" * 64,
            }
        ],
    }

    with pytest.raises(ValueError, match="Missing required parquet datasets"):
        data_manager._sync_from_manifest("https://example.invalid", tmp_path, manifest)


def test_resolve_latest_release_tag_rejects_legacy_latest_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    legacy_manifest = tmp_path / data_manager.LEGACY_MANIFEST_NAME
    legacy_manifest.write_text(
        json.dumps({"archive": "x.zip", "archive_sha256": "0" * 64, "files": {"a.parquet": {}}}),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        data_manager,
        "_download_manifest",
        lambda version, dest_dir: (legacy_manifest, True),
    )

    with pytest.raises(
        ValueError, match="latest' alias requires a manifest with a release_tag field"
    ):
        data_manager._resolve_latest_release_tag()


@pytest.mark.parametrize("tag", ["v/../../etc", "v..", "../v1", "v1/2", "2026.9.30/.."])
def test_ensure_data_rejects_unsafe_release_tags(
    tag: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("JP_IDWR_DB_CACHE_DIR", str(tmp_path))
    with pytest.raises(ValueError, match="Invalid data release tag"):
        data_manager.ensure_data(version=tag, force=True)


def test_ensure_data_latest_falls_back_to_cache_when_offline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("JP_IDWR_DB_CACHE_DIR", str(tmp_path))
    for tag in ("v2026.9.2", "v2026.9.16", "v2026.10.1"):
        (tmp_path / "data" / tag).mkdir(parents=True)
    (tmp_path / "data" / "v2026.9.2" / ".complete").write_text("ok\n")
    (tmp_path / "data" / "v2026.9.16" / ".complete").write_text("ok\n")  # newest complete

    def offline() -> str:
        raise data_manager.httpx.ConnectError("offline")

    monkeypatch.setattr(data_manager, "_resolve_latest_release_tag", offline)

    assert data_manager.ensure_data(version="latest") == tmp_path / "data" / "v2026.9.16"


def test_ensure_data_latest_offline_without_cache_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("JP_IDWR_DB_CACHE_DIR", str(tmp_path))

    def offline() -> str:
        raise data_manager.httpx.ConnectError("offline")

    monkeypatch.setattr(data_manager, "_resolve_latest_release_tag", offline)

    with pytest.raises(data_manager.httpx.ConnectError):
        data_manager.ensure_data(version="latest")
