#!/usr/bin/env python3
"""Seed data/parquet from a published GitHub release.

Datasets are not committed to the repository; the published release is the
source of truth. Builds and the automated refresh start from a copy of the
release that matches the repository's current version, and data tests run
against it.

Usage::

    uv run python scripts/fetch_release_data.py              # release for pyproject version
    uv run python scripts/fetch_release_data.py --version latest
    uv run python scripts/fetch_release_data.py --version v2026.9.30 --dest /tmp/data
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

from jp_idwr_db.data_manager import EXPECTED_DATASETS, MANIFEST_NAME, ensure_data
from jp_idwr_db.refresh_release import current_version

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DEST = ROOT / "data" / "parquet"
TAG_FILE = ".release_tag"


def fetch_release_data(version: str, dest: Path) -> str:
    """Copy a release's checksum-verified datasets into ``dest``; return its tag.

    For an explicit version, the downloaded manifest must name that release, so
    a refresh never builds on data from a different (e.g. unpublished) version.
    """
    cache_dir = ensure_data(version=version)
    manifest_path = cache_dir / MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    tag = str(manifest["release_tag"])
    if version != "latest" and tag != version:
        raise SystemExit(f"Manifest names release {tag}, expected {version}")

    dest.mkdir(parents=True, exist_ok=True)
    for name in sorted(EXPECTED_DATASETS):
        shutil.copy2(cache_dir / name, dest / name)
    (dest / TAG_FILE).write_text(tag + "\n", encoding="utf-8")
    return tag


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--version",
        default=None,
        help="Release tag or 'latest' (default: the tag of the pyproject version).",
    )
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST)
    args = parser.parse_args(argv)

    version = args.version or f"v{current_version(ROOT)}"
    tag = fetch_release_data(version, args.dest)
    print(f"Seeded {args.dest} from release {tag}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
