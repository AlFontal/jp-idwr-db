"""Runtime dataset manager for release-hosted parquet assets."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import zipfile
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from pathlib import Path
from typing import Any

import httpx
from platformdirs import user_cache_dir

PACKAGE_NAME = "jp_idwr_db"
DEFAULT_REPO = "AlFontal/jp-idwr-db"
DEFAULT_BASE_URL = f"https://github.com/{DEFAULT_REPO}/releases/download"
ARCHIVE_NAME = "jp_idwr_db-parquet.zip"
MANIFEST_NAME = "jp_idwr_db-manifest.json"
EXPECTED_DATASETS = {
    "sex_prefecture.parquet",
    "place_prefecture.parquet",
    "bullet.parquet",
    "sentinel.parquet",
    "unified.parquet",
    "prefecture_en.parquet",
}


def get_cache_dir() -> Path:
    """Return the base cache directory for package data."""
    override = os.getenv("JPINFECT_CACHE_DIR")
    if override:
        return Path(override).expanduser()
    return Path(user_cache_dir(PACKAGE_NAME))


def _resolve_data_version(version: str | None) -> str:
    """Resolve data version from explicit arg, env var, or package version."""
    if version:
        return version
    env_version = os.getenv("JPINFECT_DATA_VERSION")
    if env_version:
        return env_version
    try:
        pkg_version = package_version("jp-idwr-db")
    except PackageNotFoundError:
        pkg_version = "0.0.0"
    return pkg_version if pkg_version.startswith("v") else f"v{pkg_version}"


def _resolve_base_url(version: str) -> str:
    """Resolve base URL for release assets."""
    base_url = os.getenv("JPINFECT_DATA_BASE_URL")
    if base_url:
        return base_url.rstrip("/")
    return f"{DEFAULT_BASE_URL}/{version}"


def _sha256(path: Path) -> str:
    """Compute SHA256 hash for a file path."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _download_file(url: str, dest: Path) -> None:
    """Download URL content to a local path."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    with httpx.stream("GET", url, timeout=60.0, follow_redirects=True) as response:
        response.raise_for_status()
        with dest.open("wb") as handle:
            for chunk in response.iter_bytes():
                handle.write(chunk)


def _verify_manifest(manifest: dict[str, Any]) -> None:
    """Ensure manifest has expected structure."""
    required = {"archive", "archive_sha256", "files"}
    missing = required - set(manifest)
    if missing:
        raise ValueError(f"Invalid manifest; missing keys: {sorted(missing)}")
    files = manifest["files"]
    if not isinstance(files, dict) or not files:
        raise ValueError("Invalid manifest; 'files' must be a non-empty object")


def download_release_assets(version: str, dest_dir: Path) -> tuple[Path, Path]:
    """Download release archive + manifest for a data version."""
    base_url = _resolve_base_url(version)
    manifest_path = dest_dir / MANIFEST_NAME
    archive_path = dest_dir / ARCHIVE_NAME

    _download_file(f"{base_url}/{MANIFEST_NAME}", manifest_path)
    _download_file(f"{base_url}/{ARCHIVE_NAME}", archive_path)
    return archive_path, manifest_path


def _extract_archive(archive_path: Path, dest_dir: Path) -> None:
    """Extract archive into destination directory."""
    with zipfile.ZipFile(archive_path) as archive:
        archive.extractall(dest_dir)


def ensure_data(version: str | None = None, force: bool = False) -> Path:
    """Ensure parquet assets are available in the local cache.

    Args:
        version: Data release version (for example, ``v0.1.0``).
        force: Re-download and replace cached files.

    Returns:
        Directory path containing parquet files for the resolved version.
    """
    resolved = _resolve_data_version(version)
    cache_dir = get_cache_dir()
    data_dir = cache_dir / "data" / resolved
    marker = data_dir / ".complete"

    if marker.exists() and not force:
        return data_dir

    if force and data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    archive_path, manifest_path = download_release_assets(resolved, data_dir)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    _verify_manifest(manifest)

    expected_archive = str(manifest["archive"])
    if archive_path.name != expected_archive:
        expected_path = data_dir / expected_archive
        archive_path.rename(expected_path)
        archive_path = expected_path

    archive_hash = _sha256(archive_path)
    if archive_hash != manifest["archive_sha256"]:
        raise ValueError("Archive checksum mismatch")

    _extract_archive(archive_path, data_dir)

    file_entries: dict[str, dict[str, Any]] = manifest["files"]
    for rel_name, file_info in file_entries.items():
        file_path = data_dir / rel_name
        if not file_path.exists():
            raise ValueError(f"Missing extracted data file: {rel_name}")
        expected_hash = str(file_info["sha256"])
        if _sha256(file_path) != expected_hash:
            raise ValueError(f"Checksum mismatch for {rel_name}")

    missing_expected = [name for name in EXPECTED_DATASETS if not (data_dir / name).exists()]
    if missing_expected:
        raise ValueError(f"Missing required datasets in cache: {sorted(missing_expected)}")

    marker.write_text("ok\n", encoding="utf-8")
    return data_dir
