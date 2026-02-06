from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import zipfile
from pathlib import Path

import polars as pl
import pytest

from jp_idwr_db import data_manager


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _make_release_assets(tmp_path: Path, bad_checksum: bool = False) -> tuple[Path, Path]:
    source_dir = tmp_path / "source"
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
    if bad_checksum:
        manifest["archive_sha256"] = "0" * 64

    manifest_path = tmp_path / data_manager.MANIFEST_NAME
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return archive_path, manifest_path


def test_ensure_data_downloads_and_extracts_assets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    archive_path, manifest_path = _make_release_assets(tmp_path)
    cache_dir = tmp_path / "cache"

    def fake_download(url: str, dest: Path) -> None:
        if url.endswith(data_manager.MANIFEST_NAME):
            shutil.copyfile(manifest_path, dest)
            return
        if url.endswith(data_manager.ARCHIVE_NAME):
            shutil.copyfile(archive_path, dest)
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
    archive_path, manifest_path = _make_release_assets(tmp_path, bad_checksum=True)
    cache_dir = tmp_path / "cache"

    def fake_download(url: str, dest: Path) -> None:
        if url.endswith(data_manager.MANIFEST_NAME):
            shutil.copyfile(manifest_path, dest)
            return
        if url.endswith(data_manager.ARCHIVE_NAME):
            shutil.copyfile(archive_path, dest)
            return
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(data_manager, "_download_file", fake_download)
    monkeypatch.setenv("JPINFECT_CACHE_DIR", str(cache_dir))

    with pytest.raises(ValueError, match="Archive checksum mismatch"):
        data_manager.ensure_data(version="v-test", force=True)


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
