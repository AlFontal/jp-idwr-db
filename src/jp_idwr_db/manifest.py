"""Build and validate release manifests for language-agnostic data assets."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

SPEC_VERSION = "1.0.0"
DATASET_ID = "jp_idwr_db"
MANIFEST_NAME = "manifest.json"
DEFAULT_LICENSE = "GPL-3.0-or-later"
DEFAULT_HOMEPAGE = "https://github.com/AlFontal/jp-idwr-db"


@dataclass(frozen=True)
class _TableEntry:
    """Intermediate typed representation for a manifest table entry."""

    name: str
    payload: dict[str, Any]


def _sha256(path: Path) -> str:
    """Return the SHA-256 checksum for a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _published_at_utc() -> str:
    """Return UTC ISO-8601 timestamp, honoring SOURCE_DATE_EPOCH when provided."""
    epoch = os.getenv("SOURCE_DATE_EPOCH")
    if epoch is not None:
        dt = datetime.fromtimestamp(int(epoch), tz=timezone.utc)
    else:
        dt = datetime.now(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_scalar(value: Any) -> Any:
    """Normalize scalar values from Arrow/NumPy to plain Python types."""
    if hasattr(value, "as_py"):
        return value.as_py()
    if hasattr(value, "item"):
        return value.item()
    return value


def _map_portable_dtype(dtype: pa.DataType) -> tuple[str, str | None]:
    """Map Arrow dtypes to portable manifest dtypes."""
    if pa.types.is_dictionary(dtype):
        return "categorical", None
    if pa.types.is_date32(dtype) or pa.types.is_date64(dtype):
        return "date", None
    if pa.types.is_timestamp(dtype):
        return "datetime", None
    if pa.types.is_integer(dtype):
        return "int64", None
    if pa.types.is_floating(dtype):
        return "float64", None
    if pa.types.is_boolean(dtype):
        return "bool", None
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "string", None
    return "string", f"Unsupported Arrow dtype '{dtype}' mapped to portable dtype 'string'."


def _quote_ident(identifier: str) -> str:
    """Quote SQL identifiers for DuckDB queries."""
    return '"' + identifier.replace('"', '""') + '"'


def _scalar_to_iso(value: Any) -> str | None:
    """Convert date-like scalar values to ISO-8601 strings."""
    normalized = _normalize_scalar(value)
    if normalized is None:
        return None
    if isinstance(normalized, datetime):
        return normalized.replace(tzinfo=None).isoformat()
    if isinstance(normalized, date):
        return normalized.isoformat()
    if isinstance(normalized, str):
        return normalized
    return str(normalized)


def _parquet_column_min_max(
    parquet_file: pq.ParquetFile, column: str
) -> tuple[Any | None, Any | None]:
    """Read min/max for a parquet column from row-group statistics when available."""
    col_idx = parquet_file.schema_arrow.get_field_index(column)
    if col_idx < 0:
        return None, None

    min_value: Any | None = None
    max_value: Any | None = None
    metadata = parquet_file.metadata
    for row_group in range(metadata.num_row_groups):
        column_meta = metadata.row_group(row_group).column(col_idx)
        stats = column_meta.statistics
        if stats is None or not stats.has_min_max:
            continue
        local_min = _normalize_scalar(stats.min)
        local_max = _normalize_scalar(stats.max)
        if local_min is not None and (min_value is None or local_min < min_value):
            min_value = local_min
        if local_max is not None and (max_value is None or local_max > max_value):
            max_value = local_max
    return min_value, max_value


def _duckdb_date_min_max(path: Path, column: str) -> tuple[str | None, str | None]:
    """Fetch MIN/MAX values for a date-like column using DuckDB (best effort)."""
    try:
        duckdb = importlib.import_module("duckdb")
    except ImportError:
        return None, None

    quoted = _quote_ident(column)
    con = duckdb.connect()
    try:
        row = con.execute(
            f"SELECT MIN({quoted}) AS min_v, MAX({quoted}) AS max_v FROM read_parquet(?)",
            [path.as_posix()],
        ).fetchone()
    except Exception:
        return None, None
    finally:
        con.close()

    if row is None:
        return None, None
    return _scalar_to_iso(row[0]), _scalar_to_iso(row[1])


def _duckdb_year_week_min_max(path: Path) -> tuple[str | None, str | None]:
    """Fetch MIN/MAX week dates from year/week columns using DuckDB (best effort)."""
    try:
        duckdb = importlib.import_module("duckdb")
    except ImportError:
        return None, None

    con = duckdb.connect()
    try:
        row = con.execute(
            """
            SELECT
              MIN(CAST(year AS BIGINT) * 100 + CAST(week AS BIGINT)) AS min_yw,
              MAX(CAST(year AS BIGINT) * 100 + CAST(week AS BIGINT)) AS max_yw
            FROM read_parquet(?)
            WHERE year IS NOT NULL AND week IS NOT NULL
            """,
            [path.as_posix()],
        ).fetchone()
    except Exception:
        return None, None
    finally:
        con.close()

    if row is None or row[0] is None or row[1] is None:
        return None, None

    def _to_iso(yw: Any) -> str | None:
        try:
            yw_num = int(_normalize_scalar(yw))
            year_val = yw_num // 100
            week_val = yw_num % 100
            return date.fromisocalendar(year_val, week_val, 1).isoformat()
        except Exception:
            return None

    return _to_iso(row[0]), _to_iso(row[1])


def _best_effort_date_range(
    path: Path, parquet_file: pq.ParquetFile
) -> tuple[str | None, str | None]:
    """Compute best-effort date range based on date/week/year columns."""
    schema = parquet_file.schema_arrow
    col_names = set(schema.names)

    if "date" in col_names:
        field = schema.field("date")
        if pa.types.is_date(field.type) or pa.types.is_timestamp(field.type):
            min_value, max_value = _parquet_column_min_max(parquet_file, "date")
            if min_value is not None and max_value is not None:
                return _scalar_to_iso(min_value), _scalar_to_iso(max_value)
            return _duckdb_date_min_max(path, "date")

    if {"year", "week"}.issubset(col_names):
        duckdb_min, duckdb_max = _duckdb_year_week_min_max(path)
        if duckdb_min is not None and duckdb_max is not None:
            return duckdb_min, duckdb_max

        min_year, max_year = _parquet_column_min_max(parquet_file, "year")
        min_week, max_week = _parquet_column_min_max(parquet_file, "week")
        if min_year is not None and min_week is not None:
            try:
                date_min = date.fromisocalendar(int(min_year), int(min_week), 1).isoformat()
            except Exception:
                date_min = None
        else:
            date_min = None

        if max_year is not None and max_week is not None:
            try:
                date_max = date.fromisocalendar(int(max_year), int(max_week), 1).isoformat()
            except Exception:
                date_max = None
        else:
            date_max = None

        return date_min, date_max

    if "year" in col_names:
        min_year, max_year = _parquet_column_min_max(parquet_file, "year")
        if min_year is not None and max_year is not None:
            return f"{int(min_year):04d}-01-01", f"{int(max_year):04d}-12-31"

    return None, None


def _build_parquet_entry(path: Path) -> _TableEntry:
    """Build a manifest table entry for a parquet file."""
    parquet_file = pq.ParquetFile(path)
    schema_fields: list[dict[str, Any]] = []
    for field in sorted(parquet_file.schema_arrow, key=lambda item: item.name):
        portable_dtype, note = _map_portable_dtype(field.type)
        item: dict[str, Any] = {"name": field.name, "dtype": portable_dtype}
        if note is not None:
            item["note"] = note
        schema_fields.append(item)

    stats: dict[str, Any] = {"rows": parquet_file.metadata.num_rows}
    date_min, date_max = _best_effort_date_range(path, parquet_file)
    if date_min is not None:
        stats["date_min"] = date_min
    if date_max is not None:
        stats["date_max"] = date_max

    payload: dict[str, Any] = {
        "name": path.stem,
        "file": path.name,
        "format": "parquet",
        "size_bytes": path.stat().st_size,
        "sha256": _sha256(path),
        "schema": schema_fields,
        "stats": stats,
    }
    return _TableEntry(name=path.stem, payload=payload)


def _build_duckdb_entry(path: Path) -> _TableEntry:
    """Build a manifest table entry for a DuckDB file."""
    payload: dict[str, Any] = {
        "name": path.stem,
        "file": path.name,
        "format": "duckdb",
        "size_bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }
    return _TableEntry(name=path.stem, payload=payload)


def validate_manifest(manifest: dict[str, Any]) -> None:
    """Validate core manifest structure.

    This is a lightweight structural validator used by the asset build CLI.
    """
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
        raise ValueError(f"Invalid manifest: missing keys {sorted(missing)}")
    tables = manifest["tables"]
    if not isinstance(tables, list) or not tables:
        raise ValueError("Invalid manifest: 'tables' must be a non-empty list")

    for table in tables:
        if not isinstance(table, dict):
            raise ValueError("Invalid manifest: each table entry must be an object")
        for key in ("name", "file", "format", "size_bytes", "sha256"):
            if key not in table:
                raise ValueError(f"Invalid manifest table entry: missing '{key}'")


def build_manifest(
    data_dir: Path, release_tag: str, base_url: str, out_path: Path
) -> dict[str, Any]:
    """Build and write a deterministic manifest for release data assets.

    Args:
        data_dir: Directory with data artifacts (`*.parquet` and optional `*.duckdb`).
        release_tag: Release tag (for example: ``v0.2.4``).
        base_url: Base URL where assets are published.
        out_path: Path for the output ``manifest.json``.

    Returns:
        The generated manifest dictionary.
    """
    parquet_files = sorted(data_dir.glob("*.parquet"))
    duckdb_files = sorted(data_dir.glob("*.duckdb"))
    if not parquet_files:
        raise ValueError(f"No parquet files found in {data_dir}")

    entries: list[_TableEntry] = []
    for parquet_path in parquet_files:
        entries.append(_build_parquet_entry(parquet_path))
    for duckdb_path in duckdb_files:
        entries.append(_build_duckdb_entry(duckdb_path))

    ordered_tables = [entry.payload for entry in sorted(entries, key=lambda item: item.name)]
    data_version = release_tag[1:] if release_tag.startswith("v") else release_tag
    manifest: dict[str, Any] = {
        "spec_version": SPEC_VERSION,
        "dataset_id": DATASET_ID,
        "data_version": data_version,
        "release_tag": release_tag,
        "published_at": _published_at_utc(),
        "license": DEFAULT_LICENSE,
        "homepage": DEFAULT_HOMEPAGE,
        "assets_base_url": base_url.rstrip("/"),
        "tables": ordered_tables,
    }
    validate_manifest(manifest)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest
