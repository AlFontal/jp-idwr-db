"""Shared helpers for release metadata and artifact handling."""

from __future__ import annotations

import hashlib
import os
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from pathlib import Path


def configured_value(name: str) -> str | None:
    """Read a current configuration variable with legacy-name fallback."""
    current = os.getenv(f"JP_IDWR_DB_{name}")
    if current is not None:
        return current
    return os.getenv(f"JPINFECT_{name}")


def normalize_release_tag(version: str) -> str:
    """Normalize a version string into a GitHub release tag selector."""
    normalized = version.strip()
    if normalized == "latest":
        return normalized
    return normalized if normalized.startswith("v") else f"v{normalized}"


def installed_release_tag(package: str = "jp-idwr-db") -> str:
    """Return the installed package version as a normalized release tag."""
    try:
        resolved = package_version(package)
    except PackageNotFoundError:
        resolved = "0.0.0"
    return normalize_release_tag(resolved)


def sha256(path: Path) -> str:
    """Return the SHA-256 digest for a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def quote_identifier(identifier: str) -> str:
    """Quote an SQL identifier."""
    return '"' + identifier.replace('"', '""') + '"'
