#!/usr/bin/env python3
"""Build versioned data release assets for GitHub Releases."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import zipfile


ARCHIVE_NAME = "jp_idwr_db-parquet.zip"
MANIFEST_NAME = "jp_idwr_db-manifest.json"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_deterministic_zip(input_dir: Path, output_zip: Path) -> list[Path]:
    parquet_files = sorted(input_dir.glob("*.parquet"))
    if not parquet_files:
        raise ValueError(f"No parquet files found in {input_dir}")

    output_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output_zip, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for file_path in parquet_files:
            info = zipfile.ZipInfo(filename=file_path.name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            zf.writestr(info, file_path.read_bytes())
    return parquet_files


def main() -> None:
    parser = argparse.ArgumentParser(description="Create release data assets (zip + manifest).")
    parser.add_argument("--input", type=Path, required=True, help="Directory containing parquet files.")
    parser.add_argument("--out", type=Path, required=True, help="Output directory for release assets.")
    args = parser.parse_args()

    input_dir = args.input.resolve()
    out_dir = args.out.resolve()
    archive_path = out_dir / ARCHIVE_NAME
    manifest_path = out_dir / MANIFEST_NAME

    parquet_files = _write_deterministic_zip(input_dir, archive_path)

    manifest: dict[str, object] = {
        "archive": ARCHIVE_NAME,
        "archive_sha256": _sha256(archive_path),
        "files": {},
    }
    files = {}
    for file_path in parquet_files:
        files[file_path.name] = {
            "sha256": _sha256(file_path),
            "size_bytes": file_path.stat().st_size,
        }
    manifest["files"] = files

    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Wrote {archive_path}")
    print(f"Wrote {manifest_path}")


if __name__ == "__main__":
    main()
