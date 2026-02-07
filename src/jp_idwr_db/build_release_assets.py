"""CLI for building release data assets (DuckDB + manifest)."""

from __future__ import annotations

import argparse
import importlib
import json
from pathlib import Path
from typing import Any

from .duckdb_build import build_duckdb
from .manifest import MANIFEST_NAME, build_manifest, validate_manifest

DUCKDB_NAME = "jp_idwr_db.duckdb"


def _validate_with_json_schema(manifest: dict[str, Any], schema_path: Path) -> None:
    """Validate manifest against a JSON schema file when requested."""
    try:
        jsonschema = importlib.import_module("jsonschema")
    except ImportError as exc:
        raise RuntimeError(
            "jsonschema is required for --schema-path validation; install jsonschema first"
        ) from exc

    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    jsonschema.validate(instance=manifest, schema=schema)


def build_parser() -> argparse.ArgumentParser:
    """Create the command-line parser."""
    parser = argparse.ArgumentParser(prog="jp-idwr-db-build-assets")
    parser.add_argument(
        "--data-dir", type=Path, required=True, help="Directory containing parquet files."
    )
    parser.add_argument("--release-tag", type=str, required=True, help="Release tag (e.g. v0.2.4).")
    parser.add_argument(
        "--base-url",
        type=str,
        required=True,
        help="Base URL where release assets will be published.",
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        default=None,
        help=f"Output path for manifest JSON (default: <data-dir>/{MANIFEST_NAME}).",
    )
    parser.add_argument(
        "--duckdb-path",
        type=Path,
        default=None,
        help=f"Output path for DuckDB file (default: <data-dir>/{DUCKDB_NAME}).",
    )
    parser.add_argument("--no-duckdb", action="store_true", help="Skip building DuckDB artifact.")
    parser.add_argument(
        "--schema-path",
        type=Path,
        default=None,
        help="Optional JSON schema path for validating the generated manifest.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the release asset build CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    data_dir: Path = args.data_dir.resolve()
    manifest_path: Path = (args.manifest_path or (data_dir / MANIFEST_NAME)).resolve()

    if not args.no_duckdb:
        duckdb_path: Path = (args.duckdb_path or (data_dir / DUCKDB_NAME)).resolve()
        build_duckdb(data_dir=data_dir, out_path=duckdb_path)
        print(f"Wrote {duckdb_path}")

    manifest = build_manifest(
        data_dir=data_dir,
        release_tag=args.release_tag,
        base_url=args.base_url,
        out_path=manifest_path,
    )
    validate_manifest(manifest)
    if args.schema_path is not None:
        _validate_with_json_schema(manifest, args.schema_path.resolve())
    print(f"Wrote {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
