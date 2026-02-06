"""Command-line interface for jp_idwr_db."""

from __future__ import annotations

import argparse

from .data_manager import ensure_data


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level CLI argument parser."""
    parser = argparse.ArgumentParser(prog="jp-idwr-db")
    subparsers = parser.add_subparsers(dest="command")

    data_parser = subparsers.add_parser("data", help="Manage local data cache")
    data_subparsers = data_parser.add_subparsers(dest="data_command")

    download_parser = data_subparsers.add_parser("download", help="Download release parquet assets")
    download_parser.add_argument(
        "--version", type=str, default=None, help="Data version (e.g. v0.1.0)"
    )
    download_parser.add_argument("--force", action="store_true", help="Force re-download")

    return parser


def main(argv: list[str] | None = None) -> int:
    """Run CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "data" and args.data_command == "download":
        data_dir = ensure_data(version=args.version, force=args.force)
        print(data_dir)
        return 0

    parser.print_help()
    return 0
