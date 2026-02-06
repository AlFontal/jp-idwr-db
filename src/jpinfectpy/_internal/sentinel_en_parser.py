"""English sentinel data parser for /rapid/ endpoint CSV format."""

from __future__ import annotations

import csv
import logging
import re
from collections.abc import Iterable
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

OUTPUT_SCHEMA = {
    "prefecture": pl.Utf8,
    "disease": pl.Utf8,
    "year": pl.Int32,
    "week": pl.Int32,
    "date": pl.Date,
    "count": pl.Float64,
    "per_sentinel": pl.Float64,
    "source": pl.Utf8,
}


def _empty_output() -> pl.DataFrame:
    """Build an empty output DataFrame with the expected schema."""
    return pl.DataFrame(schema=OUTPUT_SCHEMA)


def _extract_year_week(rows: list[list[str]], path: Path) -> tuple[int | None, int | None]:
    """Extract year/week from sentinel CSV header with filename fallback."""
    year_match = re.search(r"(19|20)\d{2}", path.name)
    year_value = int(year_match.group(0)) if year_match else None
    week_value: int | None = None

    if len(rows) > 1 and rows[1]:
        header_text = ", ".join(cell.strip() for cell in rows[1] if cell and cell.strip())
        match = re.search(r"(\d+)(?:st|nd|rd|th)\s+week,\s*(\d{4})", header_text, re.IGNORECASE)
        if match:
            week_value = int(match.group(1))
            year_value = int(match.group(2))

    if week_value is None:
        fallback = re.search(r"teiten(?:rui)?(\d{2})", path.stem, re.IGNORECASE)
        if fallback:
            week_value = int(fallback.group(1))

    return year_value, week_value


def _to_float(value: str | None) -> float | None:
    """Convert CSV numeric cell to float and handle blanks/dashes."""
    if value is None:
        return None
    text = value.strip().replace(",", "")
    if text in {"", "-"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _iso_week_date(year: int, week: int) -> date:
    """Calculate ISO week start date (Monday)."""
    jan4 = datetime(year, 1, 4)
    week1_monday = jan4 - timedelta(days=jan4.weekday())
    target_date = week1_monday + timedelta(weeks=week - 1)
    return target_date.date()


def _read_sentinel_en_pl(
    path: Path,
    *,
    year: int | None = None,
    week: Iterable[int] | None = None,
) -> pl.DataFrame:
    """Read English sentinel surveillance CSV files from /rapid/ endpoint.

    CSV structure:
    - Row 0-2: Headers/metadata
    - Row 3: Disease names (properly quoted, may contain commas)
    - Row 4: "Current week", "per sentinel" repeating
    - Row 5: "Total No." row (skip)
    - Row 6+: Prefecture data

    Args:
        path: Path to CSV file or directory.
        year: Year to assign.
        week: Filter to specific week(s).

    Returns:
        DataFrame with: prefecture, disease, year, week, date, count, per_sentinel, source.
    """
    files = list(path.glob("*.csv")) if path.is_dir() else [path]
    week_set = {int(val) for val in week} if week is not None else None
    frames: list[pl.DataFrame] = []

    for p in sorted(files):
        try:
            with p.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.reader(handle))

            if len(rows) < 6:
                logger.warning("Skipping sentinel file with too few rows: %s", p.name)
                continue

            file_year, file_week = _extract_year_week(rows, p)
            y = year if year is not None else file_year
            w = file_week
            if y is None or w is None:
                logger.warning("Skipping sentinel file with unknown year/week: %s", p.name)
                continue
            if week_set is not None and w not in week_set:
                continue

            metric_row_index = next(
                (
                    idx
                    for idx, row in enumerate(rows)
                    if any("current week" in cell.strip().lower() for cell in row if cell)
                ),
                None,
            )
            if metric_row_index is None or metric_row_index == 0:
                logger.warning("Skipping sentinel file with unknown header layout: %s", p.name)
                continue

            disease_row = rows[metric_row_index - 1]
            # Each disease uses two columns: current-week count and per-sentinel rate.
            disease_cols: list[tuple[str, int, int | None]] = []
            for idx in range(1, len(disease_row), 2):
                disease = disease_row[idx].strip() if idx < len(disease_row) else ""
                if not disease:
                    continue
                per_idx = idx + 1 if idx + 1 < len(disease_row) else None
                disease_cols.append((disease, idx, per_idx))

            if not disease_cols:
                logger.warning("Skipping sentinel file with no disease columns: %s", p.name)
                continue

            report_date = _iso_week_date(y, w)
            records: list[dict[str, object]] = []

            # Rows after metric header contain totals + prefecture rows.
            for row in rows[metric_row_index + 1 :]:
                prefecture = row[0].strip() if row else ""
                if not prefecture or prefecture.lower().startswith("total"):
                    continue
                for disease, count_idx, per_idx in disease_cols:
                    count_val = row[count_idx] if count_idx < len(row) else None
                    per_val = row[per_idx] if per_idx is not None and per_idx < len(row) else None
                    records.append(
                        {
                            "prefecture": prefecture,
                            "disease": disease,
                            "year": y,
                            "week": w,
                            "date": report_date,
                            "count": _to_float(count_val),
                            "per_sentinel": _to_float(per_val),
                            "source": "Sentinel surveillance",
                        }
                    )

            if not records:
                logger.warning("Skipping sentinel file with no prefecture records: %s", p.name)
                continue

            frame = (
                pl.DataFrame(records)
                .with_columns(
                    [
                        pl.col("prefecture").cast(pl.Utf8),
                        pl.col("disease").cast(pl.Utf8),
                        pl.col("year").cast(pl.Int32),
                        pl.col("week").cast(pl.Int32),
                        pl.col("date").cast(pl.Date),
                        pl.col("count").cast(pl.Float64),
                        pl.col("per_sentinel").cast(pl.Float64),
                        pl.col("source").cast(pl.Utf8),
                    ]
                )
                .select(list(OUTPUT_SCHEMA.keys()))
            )
            frames.append(frame)

        except Exception:
            logger.exception("Failed to parse sentinel file: %s", p.name)
            continue

    if not frames:
        return _empty_output()
    return pl.concat(frames, how="vertical")
