"""Build a DuckDB artifact with views over release parquet assets."""

from __future__ import annotations

import importlib
import os
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from pathlib import Path

from .manifest import DATASET_ID


def _resolve_data_version() -> str:
    """Resolve data version for DuckDB metadata."""
    env_version = os.getenv("JPINFECT_DATA_VERSION")
    if env_version:
        return env_version
    try:
        resolved = package_version("jp-idwr-db")
    except PackageNotFoundError:
        return "unknown"
    return resolved if resolved.startswith("v") else f"v{resolved}"


def _built_at_utc() -> str:
    """Return UTC ISO-8601 build timestamp, honoring SOURCE_DATE_EPOCH."""
    epoch = os.getenv("SOURCE_DATE_EPOCH")
    if epoch is not None:
        dt = datetime.fromtimestamp(int(epoch), tz=timezone.utc)
    else:
        dt = datetime.now(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _quote_ident(identifier: str) -> str:
    """Quote SQL identifiers."""
    return '"' + identifier.replace('"', '""') + '"'


def _quote_literal(text: str) -> str:
    """Quote SQL string literals."""
    return "'" + text.replace("'", "''") + "'"


def build_duckdb(data_dir: Path, out_path: Path) -> None:
    """Build a DuckDB database file with one view per parquet file.

    Views use relative paths, so the DuckDB artifact can be moved together with the parquet files.
    """
    parquet_files = sorted(data_dir.glob("*.parquet"))
    if not parquet_files:
        raise ValueError(f"No parquet files found in {data_dir}")

    try:
        duckdb = importlib.import_module("duckdb")
    except ImportError as exc:
        raise RuntimeError(
            "duckdb is required to build jp_idwr_db.duckdb; install duckdb or use --no-duckdb"
        ) from exc

    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        out_path.unlink()

    con = duckdb.connect(out_path.as_posix())
    try:
        con.execute("PRAGMA threads=1")
        con.execute("CREATE TABLE metadata (key VARCHAR, value VARCHAR)")
        con.executemany(
            "INSERT INTO metadata VALUES (?, ?)",
            [
                ("dataset_id", DATASET_ID),
                ("data_version", _resolve_data_version()),
                ("built_at", _built_at_utc()),
            ],
        )

        for parquet_path in parquet_files:
            view_name = _quote_ident(parquet_path.stem)
            relative_path = Path(
                os.path.relpath(parquet_path.resolve(), start=out_path.parent.resolve())
            ).as_posix()
            literal_path = _quote_literal(relative_path)
            con.execute(f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet({literal_path})")
    finally:
        con.close()
