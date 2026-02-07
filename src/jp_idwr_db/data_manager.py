"""Runtime dataset manager for release-hosted parquet assets."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
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
MANIFEST_NAME = "manifest.json"
LEGACY_MANIFEST_NAME = "jp_idwr_db-manifest.json"
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


def _verify_legacy_manifest(manifest: dict[str, Any]) -> None:
    """Ensure legacy zip manifest has expected structure."""
    required = {"archive", "archive_sha256", "files"}
    missing = required - set(manifest)
    if missing:
        raise ValueError(f"Invalid legacy manifest; missing keys: {sorted(missing)}")
    files = manifest["files"]
    if not isinstance(files, dict) or not files:
        raise ValueError("Invalid legacy manifest; 'files' must be a non-empty object")


def _verify_manifest(manifest: dict[str, Any]) -> None:
    """Ensure new release manifest has expected structure."""
    required = {
        "spec_version",
        "dataset_id",
        "data_version",
        "release_tag",
        "published_at",
        "license",
        "homepage",
        "assets_base_url",
        "tables",
    }
    missing = required - set(manifest)
    if missing:
        raise ValueError(f"Invalid manifest; missing keys: {sorted(missing)}")
    tables = manifest["tables"]
    if not isinstance(tables, list) or not tables:
        raise ValueError("Invalid manifest; 'tables' must be a non-empty list")
    for table in tables:
        if not isinstance(table, dict):
            raise ValueError("Invalid manifest; each table entry must be an object")
        required_table = {"name", "file", "format", "size_bytes", "sha256"}
        missing_table = required_table - set(table)
        if missing_table:
            raise ValueError(f"Invalid manifest table entry; missing keys: {sorted(missing_table)}")


def _download_manifest(version: str, dest_dir: Path) -> tuple[Path, bool]:
    """Download release manifest, preferring the new schema with legacy fallback."""
    base_url = _resolve_base_url(version)
    manifest_path = dest_dir / MANIFEST_NAME
    try:
        _download_file(f"{base_url}/{MANIFEST_NAME}", manifest_path)
        return manifest_path, False
    except httpx.HTTPStatusError as exc:
        response = exc.response
        if response is None or response.status_code != 404:
            raise

    legacy_manifest = dest_dir / LEGACY_MANIFEST_NAME
    _download_file(f"{base_url}/{LEGACY_MANIFEST_NAME}", legacy_manifest)
    return legacy_manifest, True


def _extract_archive(archive_path: Path, dest_dir: Path) -> None:
    """Extract archive into destination directory."""
    with zipfile.ZipFile(archive_path) as archive:
        archive.extractall(dest_dir)


def _download_and_verify_file(
    base_url: str, dest_dir: Path, filename: str, expected: dict[str, Any]
) -> None:
    """Download one asset file and verify size/checksum."""
    file_path = dest_dir / filename
    _download_file(f"{base_url}/{filename}", file_path)

    expected_hash = str(expected["sha256"])
    if _sha256(file_path) != expected_hash:
        raise ValueError(f"Checksum mismatch for {filename}")

    expected_size = int(expected["size_bytes"])
    if file_path.stat().st_size != expected_size:
        raise ValueError(f"Size mismatch for {filename}")


def _sync_from_legacy_manifest(base_url: str, data_dir: Path, manifest: dict[str, Any]) -> None:
    """Download and extract legacy archive assets, then verify per-file checksums."""
    _verify_legacy_manifest(manifest)
    archive_name = str(manifest["archive"])
    archive_path = data_dir / archive_name
    _download_file(f"{base_url}/{archive_name}", archive_path)

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


def _sync_from_manifest(base_url: str, data_dir: Path, manifest: dict[str, Any]) -> None:
    """Download required parquet assets listed in the new release manifest."""
    _verify_manifest(manifest)
    table_entries: list[dict[str, Any]] = manifest["tables"]
    parquet_entries = {
        str(entry["file"]): entry for entry in table_entries if str(entry["format"]) == "parquet"
    }

    missing_listed = [name for name in EXPECTED_DATASETS if name not in parquet_entries]
    if missing_listed:
        raise ValueError(f"Missing required parquet datasets in manifest: {sorted(missing_listed)}")

    for filename in sorted(EXPECTED_DATASETS):
        _download_and_verify_file(base_url, data_dir, filename, parquet_entries[filename])


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
    action = "Refreshing" if force else "Building"
    print(
        f"[jp_idwr_db] {action} local data cache for {resolved} at {data_dir}.",
        file=sys.stderr,
    )
    print(
        "[jp_idwr_db] This happens on first use and may take a moment.",
        file=sys.stderr,
    )

    base_url = _resolve_base_url(resolved)
    manifest_path, is_legacy_manifest = _download_manifest(resolved, data_dir)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if is_legacy_manifest:
        _sync_from_legacy_manifest(base_url, data_dir, manifest)
    else:
        _sync_from_manifest(base_url, data_dir, manifest)

    missing_expected = [name for name in EXPECTED_DATASETS if not (data_dir / name).exists()]
    if missing_expected:
        raise ValueError(f"Missing required datasets in cache: {sorted(missing_expected)}")

    marker.write_text("ok\n", encoding="utf-8")
    print("[jp_idwr_db] Data cache ready.", file=sys.stderr)
    return data_dir
