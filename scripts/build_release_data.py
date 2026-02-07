#!/usr/bin/env python3
"""Build release data assets into an output directory.

This helper keeps a simple script-based workflow while using the same core
logic as ``jp-idwr-db-build-assets``.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from jp_idwr_db.duckdb_build import build_duckdb
from jp_idwr_db.manifest import MANIFEST_NAME, build_manifest

DUCKDB_NAME = "jp_idwr_db.duckdb"


def main() -> None:
    parser = argparse.ArgumentParser(description="Create release data assets (parquet + duckdb + manifest).")
    parser.add_argument("--input", type=Path, required=True, help="Directory containing parquet files.")
    parser.add_argument("--out", type=Path, required=True, help="Output directory for release assets.")
    parser.add_argument("--release-tag", type=str, required=True, help="Release tag (e.g. v0.2.4).")
    parser.add_argument("--base-url", type=str, required=True, help="Release assets base URL.")
    parser.add_argument("--no-duckdb", action="store_true", help="Skip building DuckDB artifact.")
    args = parser.parse_args()

    input_dir = args.input.resolve()
    out_dir = args.out.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    parquet_files = sorted(input_dir.glob("*.parquet"))
    if not parquet_files:
        raise ValueError(f"No parquet files found in {input_dir}")
    for parquet_file in parquet_files:
        shutil.copy2(parquet_file, out_dir / parquet_file.name)

    if not args.no_duckdb:
        duckdb_path = out_dir / DUCKDB_NAME
        build_duckdb(data_dir=out_dir, out_path=duckdb_path)
        print(f"Wrote {duckdb_path}")

    manifest_path = out_dir / MANIFEST_NAME
    build_manifest(
        data_dir=out_dir,
        release_tag=args.release_tag,
        base_url=args.base_url,
        out_path=manifest_path,
    )
    print(f"Wrote {manifest_path}")


if __name__ == "__main__":
    main()
