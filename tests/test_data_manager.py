from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import zipfile
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import pytest

from jp_idwr_db import data_manager


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _make_manifest_entry(path: Path, *, bad_checksum: bool = False) -> dict[str, Any]:
    checksum = _sha256(path)
    if bad_checksum:
        checksum = "0" * 64
    return {
        "name": path.stem,
        "file": path.name,
        "format": "parquet",
        "size_bytes": path.stat().st_size,
        "sha256": checksum,
        "schema": [],
        "stats": {"rows": 1},
    }


def _make_release_assets(tmp_path: Path, bad_checksum: bool = False) -> tuple[Path, Path]:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    parquet_paths: list[Path] = []
    for filename in sorted(data_manager.EXPECTED_DATASETS):
        parquet_path = source_dir / filename
        pl.DataFrame({"x": [1]}).write_parquet(parquet_path)
        parquet_paths.append(parquet_path)

    manifest = {
        "spec_version": "1.0.0",
        "dataset_id": "jp_idwr_db",
        "data_version": "test",
        "release_tag": "v-test",
        "published_at": "2025-01-01T00:00:00Z",
        "license": "GPL-3.0-or-later",
        "homepage": "https://example.invalid",
        "assets_base_url": "https://example.invalid/download/v-test",
        "tables": [_make_manifest_entry(path, bad_checksum=bad_checksum) for path in parquet_paths],
    }

    manifest_path = tmp_path / data_manager.MANIFEST_NAME
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return source_dir, manifest_path


def _make_legacy_release_assets(tmp_path: Path) -> tuple[Path, Path]:
    source_dir = tmp_path / "legacy-source"
    source_dir.mkdir()
    for filename in sorted(data_manager.EXPECTED_DATASETS):
        pl.DataFrame({"x": [1]}).write_parquet(source_dir / filename)

    archive_path = tmp_path / data_manager.ARCHIVE_NAME
    with zipfile.ZipFile(
        archive_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as archive:
        for parquet in sorted(source_dir.glob("*.parquet")):
            archive.write(parquet, arcname=parquet.name)

    manifest = {
        "archive": data_manager.ARCHIVE_NAME,
        "archive_sha256": _sha256(archive_path),
        "files": {
            parquet.name: {
                "sha256": _sha256(parquet),
                "size_bytes": parquet.stat().st_size,
            }
            for parquet in sorted(source_dir.glob("*.parquet"))
        },
    }
    manifest_path = tmp_path / data_manager.LEGACY_MANIFEST_NAME
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return archive_path, manifest_path


def test_ensure_data_downloads_and_extracts_assets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source_dir, manifest_path = _make_release_assets(tmp_path)
    cache_dir = tmp_path / "cache"

    def fake_download(url: str, dest: Path) -> None:
        if url.endswith(data_manager.MANIFEST_NAME):
            shutil.copyfile(manifest_path, dest)
            return
        filename = url.rsplit("/", maxsplit=1)[-1]
        source_path = source_dir / filename
        if source_path.exists():
            shutil.copyfile(source_path, dest)
            return
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(data_manager, "_download_file", fake_download)
    monkeypatch.setenv("JPINFECT_CACHE_DIR", str(cache_dir))

    data_dir = data_manager.ensure_data(version="v-test", force=True)
    assert data_dir.exists()
    assert (data_dir / ".complete").exists()
    assert all((data_dir / name).exists() for name in data_manager.EXPECTED_DATASETS)
    captured = capsys.readouterr()
    assert "local data cache" in captured.err
    assert str(data_dir) in captured.err


def test_ensure_data_checksum_mismatch_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_dir, manifest_path = _make_release_assets(tmp_path, bad_checksum=True)
    cache_dir = tmp_path / "cache"

    def fake_download(url: str, dest: Path) -> None:
        if url.endswith(data_manager.MANIFEST_NAME):
            shutil.copyfile(manifest_path, dest)
            return
        filename = url.rsplit("/", maxsplit=1)[-1]
        source_path = source_dir / filename
        if source_path.exists():
            shutil.copyfile(source_path, dest)
            return
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(data_manager, "_download_file", fake_download)
    monkeypatch.setenv("JPINFECT_CACHE_DIR", str(cache_dir))

    with pytest.raises(ValueError, match="Checksum mismatch"):
        data_manager.ensure_data(version="v-test", force=True)


def test_ensure_data_supports_legacy_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive_path, legacy_manifest_path = _make_legacy_release_assets(tmp_path)
    cache_dir = tmp_path / "cache"

    def fake_download(url: str, dest: Path) -> None:
        if url.endswith(data_manager.LEGACY_MANIFEST_NAME):
            shutil.copyfile(legacy_manifest_path, dest)
            return
        if url.endswith(data_manager.MANIFEST_NAME):
            request = httpx.Request("GET", url)
            response = httpx.Response(404, request=request)
            raise httpx.HTTPStatusError("Not Found", request=request, response=response)
        if url.endswith(data_manager.ARCHIVE_NAME):
            shutil.copyfile(archive_path, dest)
            return
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(data_manager, "_download_file", fake_download)
    monkeypatch.setenv("JPINFECT_CACHE_DIR", str(cache_dir))

    data_dir = data_manager.ensure_data(version="v-test", force=True)
    assert all((data_dir / name).exists() for name in data_manager.EXPECTED_DATASETS)


def test_wheel_does_not_include_parquet(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    out_dir = tmp_path / "dist"
    subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(out_dir)],
        check=True,
        cwd=repo_root,
    )

    wheels = sorted(out_dir.glob("*.whl"))
    assert wheels, "Expected a built wheel"

    with zipfile.ZipFile(wheels[0]) as wheel:
        parquet_entries = [name for name in wheel.namelist() if name.endswith(".parquet")]
    assert parquet_entries == []
