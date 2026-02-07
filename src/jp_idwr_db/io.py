"""Data download and reading utilities for Japanese infectious disease data.

This module handles downloading Excel and CSV files from the NIID surveillance
system and parsing them into standardized DataFrame format. It includes complex
Excel parsing logic to handle merged headers, varying sheet structures across
years, and data cleaning.

Key functions:
    - download(): Download raw data for a specific year
    - download_recent(): Download all available weekly reports from 2024+
    - read(): Read local Excel or CSV files into DataFrames
"""

from __future__ import annotations

import csv
import datetime as dt
import logging
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Literal, cast

import polars as pl
from platformdirs import user_cache_dir

from .config import get_config
from .http import download_urls
from .types import DatasetName
from .urls import url_bullet, url_confirmed, url_sentinel

logger = logging.getLogger(__name__)

# Disease name mappings to standardize variants and duplicates
_DISEASE_NAME_MAPPINGS = {
    # AIDS variants
    "Acquired immunodeficiency syndrome (AIDS)": "AIDS",
    "Acquired immunodeficiency syndrome (AIDS": "AIDS",
    "HIV/AIDS": "AIDS",
    # Carbapenem-resistant infections
    "Carbapenem-resistant enterobacteriaceae infection": "Carbapenem-resistant Enterobacterales infection",
    # E. coli variants
    "Enterohemorrhagic E. coli infection": "Enterohemorrhagic Escherichia coli infection",
    # Typhus variants
    "Epidemic louse-borne typhus": "Epidemic typhus",
    # B virus
    "Herpes B virus infection": "B virus disease",
    # Scrub typhus / Tsutsugamushi
    "Scrub typhus (Tsutsugamushi disease)": "Scrub typhus",
    "Scrub typhus(Tsutsugamushi disease)": "Scrub typhus",
    "Scrub typhus(Tsutsugamushi disease": "Scrub typhus",
    "Tsutsugamushi disease": "Scrub typhus",
    # Severe invasive streptococcal
    "Severe invasive streptococcal infections (TSLS)": "Severe invasive streptococcal infections",
    "Severe invasive streptococcal infections(TSLS)": "Severe invasive streptococcal infections",
    "Severe invasive streptococcal infections(TSLS": "Severe invasive streptococcal infections",
    # VRE
    "VRE infection": "Vancomycin-resistant Enterococcus infection",
    # West Nile fever
    "West Nile fever (including West Nile encephalitis)": "West Nile fever",
    "West Nile fever(including West Nile encephalitis)": "West Nile fever",
    "West Nile fever(including West Nile encephalitis": "West Nile fever",
    # Parenthesis variants from historical headers
    "Acute Flaccid Paralysis (excluding Acute poliomyelitis)": "Acute Flaccid Paralysis (excluding Acute poliomyelitis)",
    "Acute encephalitis(excluding JE and WNE)": "Acute encephalitis(excluding JE and WNE)",
    "Avian influenza (exclud. Avian influenza H5N1)": "Avian influenza (exclud. Avian influenza H5N1)",
    "Lyssavirus infection(excluding rabies)": "Lyssavirus infection(excluding rabies)",
    "Middle East Respiratory Syndrome (MERS)": "Middle East Respiratory Syndrome (MERS)",
    "Severe Acute Respiratory Syndrome(SARS)": "Severe Acute Respiratory Syndrome(SARS)",
    "Varicella (limited to hospiltalized case)": "Varicella (limited to hospiltalized case)",
    "Viral hepatitis(excluding hepatitis A and E)": "Viral hepatitis(excluding hepatitis A and E)",
    # Avian influenza duplicates (malformed will be handled by normalization)
    "Avian influenza H5N1": "Avian influenza H5N1",
    "Avian influenza H7N9": "Avian influenza H7N9",
}

# Track original -> cleaned disease names (populated during data reading)
_disease_name_tracker: dict[str, str] = {}


def _col_rename_bullet(names: list[str]) -> list[str]:
    """Clean and normalize column names from bullet CSV files.

    Bullet CSV files have messy headers with newlines, full-width characters,
    and unnecessary prefixes. This function standardizes them.

    Args:
        names: Raw column names from CSV header.

    Returns:
        List of cleaned column names.
    """
    cleaned: list[str] = []
    for raw_name in names:
        # Remove newlines that appear in the middle of names
        clean = re.sub(r"^.*[\r\n]+", "", str(raw_name))
        # Remove Excel-generated column names like "...1", "...2"
        clean = re.sub(r"^\.\.\.[0-9]+$", "", clean)
        # Replace full-width characters with ASCII equivalents
        clean = clean.replace("\uff29", "I")
        clean = clean.replace("\uff08", "(").replace("\uff09", ")")
        # Collapse multiple spaces
        clean = re.sub(r"\s+", " ", clean).strip()
        # Remove wrapping parentheses only (not parentheses that are part of the name)
        # Only strip if the entire string is wrapped: "(Something)" -> "Something"
        # Don't strip if parentheses are part of content: "Word (detail)" stays as is
        if clean.startswith("(") and clean.endswith(")") and clean.count("(") == 1:
            clean = clean[1:-1].strip()
        if clean:
            cleaned.append(clean)
    return cleaned


def _clean_cell_text(text: str | None) -> str | None:
    """Clean text from Excel cells (handles null bytes, extracts English).

    Excel files from 1999-2000 contain null bytes. Bilingual cells have
    Japanese text followed by English in parentheses - we extract the English.
    Handles both half-width and full-width parentheses.

    Args:
        text: Raw cell text.

    Returns:
        Cleaned text or None if empty.
    """
    if not text:
        return None
    # Remove null bytes (issue in older data)
    clean = text.replace("\x00", "")
    # Normalize whitespace
    clean = clean.replace("\r", " ").replace("\n", " ").replace("\t", " ")

    # Extract English text from bilingual cells like "日本語 (English)".
    # Support both half-width and full-width parentheses.
    # Use findall to get all matches, then take the LAST one (which is usually the English)
    matches = re.findall(r"[\uFF08(]([^\)\uFF09]+)[)\uFF09]", clean)
    if matches:
        # Take the last match (English is typically at the end)
        english = matches[-1].strip()
        # Normalize full-width ASCII characters to half-width
        english = _normalize_fullwidth(english)
        return english

    # Normalize any full-width characters in the result
    clean = _normalize_fullwidth(clean)
    return clean.strip()


def _normalize_fullwidth(text: str) -> str:
    """Normalize full-width ASCII characters to half-width.

    Args:
        text: Text potentially containing full-width characters.

    Returns:
        Text with full-width ASCII normalized to half-width.
    """
    # Common full-width letters and characters seen in the data
    replacements = {
        "\uff29": "I",
        "\uff4e": "n",
        "\uff21": "A",
        "\uff25": "E",
        "\uff2f": "O",
        "\u3000": " ",  # Full-width space
    }
    for fw, hw in replacements.items():
        text = text.replace(fw, hw)
    return text


def _normalize_disease_name(name: str) -> str:
    """Normalize disease names for consistency.

    Fixes common issues:
    - Malformed parentheses (e.g., "H5N1) (Avian influenza H5N1")
    - Redundant text in parentheses
    - Standardizes to preferred naming

    Args:
        name: Raw disease name.

    Returns:
        Normalized disease name.
    """
    # Normalize spacing first to keep mapping keys stable.
    name = re.sub(r"\s+", " ", name).strip()

    # Fix malformed parentheses like "H5N1) (Avian influenza H5N1" -> "Avian influenza H5N1"
    malformed_match = re.match(r"^[^\(]*\)\s*\((.+)$", name)
    if malformed_match:
        name = malformed_match.group(1).strip()

    # Repair historical headers where trailing ')' is dropped.
    if name.count("(") > name.count(")"):
        name = name + (")" * (name.count("(") - name.count(")")))

    # Apply known disease name mappings for duplicates/variants
    name = _DISEASE_NAME_MAPPINGS.get(name, name)

    return name


def _resolve_headers(
    cols: list[str | None], row2: list[str | None], row3: list[str | None]
) -> list[str]:
    """Resolve column headers from multi-row Excel headers.

    Excel files have a complex header structure:
    - Row 2: Disease names (merged across multiple columns)
    - Row 3: Category names (Total, Male, Female, etc.) under each disease

    This function constructs unique column names in the format "Disease||Category".

    Args:
        cols: Original column names (mostly unused).
        row2: Disease names (sparse - only appears in first column of each disease).
        row3: Category names for each column.

    Returns:
        List of standardized column names.
    """
    headers = ["prefecture"]  # First column is always prefecture
    current_disease = "Unknown"

    for i in range(1, len(cols)):
        r2 = _clean_cell_text(row2[i])
        r3 = _clean_cell_text(row3[i])

        # Update current disease if row2 has a value (merged cells span multiple columns)
        if r2:
            current_disease = r2

        # Filter out Japanese-only category text
        # If r3 contains Japanese characters, it's likely a note/modifier, not a category
        if r3 and any(
            "\u3040" <= c <= "\u309f" or "\u30a0" <= c <= "\u30ff" or "\u4e00" <= c <= "\u9fff"
            for c in r3
        ):
            r3 = None  # Treat as empty, will default to "total"

        # Normalize category name
        cat = r3 if r3 else "total"
        cat_lower = cat.lower()
        if "total" in cat_lower:
            cat = "total"
        elif "male" in cat_lower:
            cat = "male"
        elif "female" in cat_lower:
            cat = "female"
        elif "japan" in cat_lower:
            cat = "japan"
        elif "others" in cat_lower:
            cat = "others"
        elif "unknown" in cat_lower:
            cat = "unknown"

        # Create header and handle duplicates
        base_header = f"{current_disease}||{cat}"
        new_header = base_header
        count = 1
        while new_header in headers:
            new_header = f"{base_header}_{count}"
            count += 1
        headers.append(new_header)

    return headers


def _read_excel_sheets(
    file_path: Path, sheet_range: Iterable[int]
) -> list[tuple[int, pl.DataFrame]]:
    """Read multiple sheets from an Excel file and parse structured data.

    Each sheet represents one week of data with a multi-row header structure.
    This function extracts data rows and applies header resolution.

    Args:
        file_path: Path to the Excel file.
        sheet_range: Sheet indices to read (1-based).

    Returns:
        List of tuples (sheet_id, DataFrame) for successfully parsed sheets.

    Note:
        Sheet structure:
        - Row 0-1: Title/metadata (skipped)
        - Row 2: Disease names (merged cells)
        - Row 3: Category names
        - Row 4+: Data rows
    """
    frames: list[tuple[int, pl.DataFrame]] = []
    path_str = str(file_path)

    for sheet in sheet_range:
        try:
            # Read without header to manually parse the structure
            df_raw = pl.read_excel(path_str, sheet_id=sheet, has_header=False)

            # Handle edge case where read_excel returns dict instead of DataFrame
            if isinstance(df_raw, dict):  # type: ignore[unreachable]
                if len(df_raw) >= 1:  # type: ignore[unreachable]
                    df_raw = next(iter(df_raw.values()))
                else:
                    continue

            # Skip sheets with insufficient rows
            if df_raw.height < 5:
                continue

            # Extract header rows
            row2 = [str(x) if x is not None else None for x in df_raw.row(2)]
            row3 = [str(x) if x is not None else None for x in df_raw.row(3)]

            # Validate that this looks like a data sheet (row3 should contain "total")
            row3_clean = [_clean_cell_text(x) for x in row3]
            if not any("total" in str(x).lower() for x in row3_clean if x):
                logger.debug(f"Skipping sheet {sheet}: No 'total' category in header row")
                continue

            # Resolve header names
            headers = _resolve_headers(list(df_raw.columns), row2, row3)

            # Extract data rows (skip first 4 rows of headers/metadata)
            data_df = df_raw.slice(4)
            data_df.columns = headers

            # Clean prefecture column and remove aggregate rows
            if "prefecture" in data_df.columns:
                data_df = data_df.with_columns(
                    pl.col("prefecture").map_elements(
                        lambda x: _clean_cell_text(str(x)), return_dtype=pl.Utf8
                    )
                )
                # Remove "Total" aggregate rows
                data_df = data_df.filter(
                    ~pl.col("prefecture").str.to_lowercase().str.contains("total")
                )

            if not data_df.is_empty():
                frames.append((sheet, data_df))

        except Exception:
            logger.exception(f"Error reading sheet {sheet} from {file_path.name}")
            continue

    return frames


def _infer_year_from_path(path: Path) -> int | None:
    """Extract year from filename.

    Args:
        path: File path containing a year (e.g., "Syu_01_1_2024.xlsx").

    Returns:
        Four-digit year or None if not found.
    """
    match = re.search(r"(19|20)\d{2}", path.name)
    if not match:
        return None
    return int(match.group(0))


def _extract_year_week(path: Path) -> tuple[int | None, int | None]:
    """Extract year and week from filename.

    Args:
        path: File path containing year and week (e.g., "2024-01-zensu.csv").

    Returns:
        Tuple of (year, week) or (None, None) if not found.
    """
    year_match = re.search(r"(19|20)\d{2}", path.name)
    week_match = re.search(r"(?:-)?(\d{2})|zensu(\d{2})", path.name)
    year = int(year_match.group(0)) if year_match else None
    week = None
    if week_match:
        week = int(week_match.group(1) or week_match.group(2))
    return year, week


def _sheet_range_for_year(year: int) -> range:
    """Determine sheet range for a given year.

    Different years have different numbers of sheets due to leap years and
    starting week variations.

    Args:
        year: Year of the data.

    Returns:
        Range of sheet indices to read (1-based).
    """
    if year == 1999:
        return range(2, 41)  # Started mid-year
    if year in {2004, 2009, 2015}:  # Years with 53 weeks
        return range(2, 55)
    return range(2, 54)


def _combine_confirmed_frames(
    frames: list[tuple[int, pl.DataFrame]],
    year: int,
    *,
    week_offset: int,
) -> pl.DataFrame:
    """Combine multiple sheet DataFrames and add year/week columns.

    Args:
        frames: List of (sheet_id, DataFrame) tuples.
        year: Year to assign to all rows.
        week_offset: Offset to apply to sheet_id to get week number.
            (e.g., 1999 started at week 14, so offset=12 means sheet 2 = week 14)

    Returns:
        Combined DataFrame with year and week columns.
    """
    combined: list[pl.DataFrame] = []
    for sheet, frame in frames:
        week = sheet + week_offset
        enhanced = frame.with_columns([pl.lit(year).alias("year"), pl.lit(week).alias("week")])
        combined.append(enhanced)

    if not combined:
        return pl.DataFrame()

    return pl.concat(combined, how="diagonal_relaxed")


def _iso_week_date(year: int, week: int) -> dt.date | None:
    """Convert ISO year and week to a date (last day of week = Sunday).

    Args:
        year: ISO year.
        week: ISO week number (1-53).

    Returns:
        Date representing the Sunday of that week, or None if invalid.
    """
    try:
        return dt.date.fromisocalendar(int(year), int(week), 7)
    except Exception:
        return None


def _read_confirmed_pl(
    path: Path,
    *,
    type: DatasetName | None = None,
) -> pl.DataFrame:
    """Read confirmed cases data from Excel file(s).

    Args:
        path: Path to file or directory containing Excel files.
        type: Dataset type ("sex" or "place"), used for filename pattern matching.

    Returns:
        DataFrame in long format with columns: prefecture, year, week, date,
        disease, category, count.
    """
    # If path is a directory, find the appropriate file(s)
    if path.is_dir():
        if type == "sex":
            pattern = re.compile(r"(Syu_01_1|01_1)\.(xls|xlsx)$")
        elif type == "place":
            pattern = re.compile(r"(Syu_02_1|02_1)\.(xls|xlsx)$")
        else:
            pattern = re.compile(r"Syu_0[12]_1\.(xls|xlsx)$")
        files = [p for p in path.iterdir() if pattern.search(p.name)]
    else:
        files = [path]

    frames: list[pl.DataFrame] = []
    for file_path in sorted(files):
        year = _infer_year_from_path(file_path) or 0
        if year == 0:
            logger.warning(f"Could not infer year from {file_path.name}, skipping")
            continue

        sheet_range = _sheet_range_for_year(year)
        excel_frames = _read_excel_sheets(file_path, sheet_range)

        # 1999 data starts at week 14 (sheet 2), so offset=12 makes sheet 2 -> week 14
        week_offset = 12 if year == 1999 else -1
        combined = _combine_confirmed_frames(excel_frames, year, week_offset=week_offset)
        frames.append(combined)

    if not frames:
        return pl.DataFrame()

    df = pl.concat(frames, how="diagonal_relaxed")

    # Calculate date column from year and week
    if "date" not in df.columns and "year" in df.columns and "week" in df.columns:
        df = df.with_columns(
            pl.struct(["year", "week"])
            .map_elements(
                lambda x: _iso_week_date(x["year"], x["week"]),
                return_dtype=pl.Date,
            )
            .alias("date")
        )

    # Remove duplicate columns (artifacts from duplicate headers like "Disease||total_1")
    cols_to_drop = [c for c in df.columns if re.search(r"_[0-9]+$", c) and "||" in c]
    if cols_to_drop:
        df = df.drop(cols_to_drop)

    # Melt from wide to long format
    id_vars = [c for c in df.columns if c in {"prefecture", "year", "week", "date"}]
    value_vars = [c for c in df.columns if "||" in c]

    if not value_vars:
        return df

    long_df = df.unpivot(index=id_vars, on=value_vars, variable_name="variable", value_name="count")

    # Split "Disease||Category" into separate columns
    long_df = long_df.with_columns(
        [
            pl.col("variable").str.split("||").list.get(0).alias("disease_raw"),
            pl.col("variable").str.split("||").list.get(1).alias("category"),
        ]
    ).drop("variable")

    # Normalize disease names and track mappings
    disease_mappings = {}
    for raw_name in long_df["disease_raw"].unique():
        if raw_name:
            normalized = _normalize_disease_name(raw_name)
            disease_mappings[raw_name] = normalized
            # Update global tracker
            if raw_name not in _disease_name_tracker:
                _disease_name_tracker[raw_name] = normalized

    long_df = long_df.with_columns(
        pl.col("disease_raw").replace(disease_mappings).alias("disease")
    ).drop("disease_raw")

    # Clean count column (convert to int, treating errors as 0)
    long_df = long_df.with_columns(
        pl.col("count").cast(pl.Float64, strict=False).fill_null(0).cast(pl.Int64)
    )

    # Add source column
    long_df = long_df.with_columns(pl.lit("Confirmed cases").alias("source"))

    return long_df


def _read_bullet_pl(
    path: Path,
    *,
    year: int | None = None,
    week: Iterable[int] | None = None,
) -> pl.DataFrame:
    """Read bullet (weekly report) CSV files.

    Args:
        path: Path to CSV file or directory containing CSV files.
        year: Year to assign (if None, inferred from filename).
        week: Filter to specific week(s) if provided.

    Returns:
        DataFrame in long format with columns: prefecture, year, week, date,
        disease, count.
    """
    # Find CSV files
    files = list(path.glob("*.csv")) if path.is_dir() else [path]

    # Filter by week if specified
    if week is not None:
        week_set = {int(w) for w in week}
        files = [p for p in files if (_extract_year_week(p)[1] in week_set)]

    frames: list[pl.DataFrame] = []
    for p in sorted(files):
        try:
            # Skip metadata rows (0-2), header is row 3, subheader is row 4
            df_raw = pl.read_csv(p, skip_rows=3, infer_schema_length=0)

            # Drop the subheader row (first data row contains "Current week", etc.)
            if df_raw.height > 0:
                df_raw = df_raw.slice(1)

            # Keep only valid columns (exclude cumulative columns with auto-generated names)
            to_select = [
                c
                for c in df_raw.columns
                if c in {"Prefecture", "prefecture"}
                or not (c.startswith("_duplicated_") or c.startswith("field_"))
            ]
            df_raw = df_raw.select(to_select)

            # Clean column names
            new_names = {}
            for c in df_raw.columns:
                clean_name = _col_rename_bullet([c])
                new_names[c] = clean_name[0] if clean_name else c

            df_raw = df_raw.rename(new_names)

            # Standardize prefecture column name
            if "Prefecture" in df_raw.columns:
                df_raw = df_raw.rename({"Prefecture": "prefecture"})

            # Unpivot to long format
            value_vars = [c for c in df_raw.columns if c != "prefecture"]
            if not value_vars:
                continue

            long_df = df_raw.unpivot(
                index=["prefecture"], on=value_vars, variable_name="disease", value_name="count"
            )

            # Add year and week columns
            file_year, file_week = _extract_year_week(p)
            y = year or file_year
            w = file_week

            if y is not None:
                long_df = long_df.with_columns(pl.lit(y).alias("year"))
            if w is not None:
                long_df = long_df.with_columns(pl.lit(w).alias("week"))

            # Calculate date
            if "year" in long_df.columns and "week" in long_df.columns:
                long_df = long_df.with_columns(
                    pl.struct(["year", "week"])
                    .map_elements(
                        lambda x: _iso_week_date(int(x["year"]), int(x["week"])),
                        return_dtype=pl.Date,
                    )
                    .alias("date")
                )

            # Clean count column
            long_df = long_df.with_columns(
                pl.col("count").cast(pl.Float64, strict=False).fill_null(0).cast(pl.Int64)
            )

            # Add source column
            long_df = long_df.with_columns(pl.lit("Confirmed cases").alias("source"))

            frames.append(long_df)

        except Exception:
            logger.exception(f"Failed to parse bullet file: {p.name}")
            continue

    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="vertical")


def _read_sentinel_pl(  # noqa: PLR0915
    path: Path,
    *,
    year: int | None = None,
    week: Iterable[int] | None = None,
) -> pl.DataFrame:
    """Read sentinel surveillance (teitenrui) CSV files.

    Args:
        path: Path to CSV file or directory containing CSV files.
        year: Year to assign (if None, inferred from filename).
        week: Filter to specific week(s) if provided.

    Returns:
        DataFrame in long format with columns: prefecture, year, week, date,
        disease, count, per_sentinel, source.
    """
    # Find CSV files
    files = list(path.glob("*.csv")) if path.is_dir() else [path]

    # Filter by week if specified
    if week is not None:
        week_set = {int(w) for w in week}
        files = [p for p in files if (_extract_year_week(p)[1] in week_set)]

    frames: list[pl.DataFrame] = []
    for p in sorted(files):
        try:
            # Read raw CSV: Row 0-1=metadata, Row 2=diseases, Row 3=count/per-sentinel, Row 4+=data
            df_raw = pl.read_csv(
                p, skip_rows=2, has_header=False, infer_schema_length=0, encoding="shift-jis"
            )

            if df_raw.height < 3:
                continue

            # Extract disease names from row 0 and column types from row 1
            disease_row = df_raw.row(0)
            type_row = df_raw.row(1)
            data_df = df_raw.slice(2)  # Data starts from row 2

            # First column is prefecture
            first_col = df_raw.columns[0]
            data_df = data_df.rename({first_col: "prefecture"})

            # Build (disease, count_col, per_sentinel_col) tuples
            disease_cols: list[tuple[str, str, str | None]] = []
            current_disease: str | None = None
            count_col: str | None = None

            for i, (disease_name, col_type) in enumerate(
                zip(disease_row[1:], type_row[1:], strict=False)
            ):
                original_col = df_raw.columns[i + 1]
                if disease_name and str(disease_name).strip():
                    # New disease
                    current_disease = _clean_cell_text(str(disease_name))
                    count_col = original_col
                elif current_disease and col_type:
                    # Per-sentinel column for current disease
                    if count_col is not None:
                        disease_cols.append((current_disease, count_col, original_col))
                    current_disease = None
                    count_col = None

            # Handle last disease if no per-sentinel column
            if current_disease and count_col:
                disease_cols.append((current_disease, count_col, None))

            # Clean prefecture names and filter totals
            data_df = data_df.with_columns(
                pl.col("prefecture").map_elements(
                    lambda x: _clean_cell_text(str(x)) if x else None,
                    return_dtype=pl.Utf8,
                )
            ).filter(
                pl.col("prefecture").is_not_null() & ~pl.col("prefecture").str.contains("総数|合計")
            )

            # Process each disease
            disease_frames: list[pl.DataFrame] = []
            for disease, count_col, per_sentinel_col in disease_cols:
                disease_df = data_df.select(["prefecture"])
                disease_df = disease_df.with_columns(
                    [
                        pl.lit(disease).alias("disease"),
                        data_df[count_col].alias("count_raw")
                        if count_col in data_df.columns
                        else pl.lit(None).alias("count_raw"),
                    ]
                )

                if per_sentinel_col and per_sentinel_col in data_df.columns:
                    disease_df = disease_df.with_columns(
                        data_df[per_sentinel_col].alias("per_sentinel_raw")
                    )
                else:
                    disease_df = disease_df.with_columns(pl.lit(None).alias("per_sentinel_raw"))

                disease_frames.append(disease_df)

            if not disease_frames:
                continue

            # Concatenate all diseases for this file
            long_df = pl.concat(disease_frames, how="vertical")

            # Add year and week columns
            file_year, file_week = _extract_year_week(p)
            y = year or file_year
            w = file_week

            if y is not None:
                long_df = long_df.with_columns(pl.lit(y).alias("year"))
            if w is not None:
                long_df = long_df.with_columns(pl.lit(w).alias("week"))

            # Calculate date
            if "year" in long_df.columns and "week" in long_df.columns:
                long_df = long_df.with_columns(
                    pl.struct(["year", "week"])
                    .map_elements(
                        lambda x: _iso_week_date(int(x["year"]), int(x["week"])),
                        return_dtype=pl.Date,
                    )
                    .alias("date")
                )

            # Clean count and per_sentinel (replace "-" with null)
            long_df = long_df.with_columns(
                [
                    pl.col("count_raw")
                    .str.replace("-", "")
                    .cast(pl.Float64, strict=False)
                    .fill_null(0)
                    .cast(pl.Int64)
                    .alias("count"),
                    pl.col("per_sentinel_raw")
                    .str.replace("-", "")
                    .cast(pl.Float64, strict=False)
                    .alias("per_sentinel"),
                ]
            ).drop(["count_raw", "per_sentinel_raw"])

            # Add source column
            long_df = long_df.with_columns(pl.lit("Sentinel surveillance").alias("source"))

            # Normalize disease names
            disease_mappings = {}
            for raw_name in long_df["disease"].unique().to_list():
                normalized = _normalize_disease_name(raw_name)
                if normalized != raw_name:
                    disease_mappings[raw_name] = normalized
                    _disease_name_tracker[raw_name] = normalized

            if disease_mappings:
                long_df = long_df.with_columns(pl.col("disease").replace(disease_mappings))

            frames.append(long_df)

        except Exception:
            logger.exception(f"Failed to parse sentinel file: {p.name}")
            continue

    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="vertical")


_SENTINEL_EN_SCHEMA = {
    "prefecture": pl.Utf8,
    "disease": pl.Utf8,
    "year": pl.Int32,
    "week": pl.Int32,
    "date": pl.Date,
    "count": pl.Float64,
    "per_sentinel": pl.Float64,
    "source": pl.Utf8,
}


def _sentinel_cumulative_to_weekly(df: pl.DataFrame) -> pl.DataFrame:
    """Convert cumulative sentinel counts to weekly incidence.

    Sentinel `teitenrui` files report year-to-date cumulative counts per
    prefecture and disease. This helper converts those cumulative counts into
    weekly counts by differencing against the previous week within each
    year/prefecture/disease series. The first observed week in each yearly
    series is kept as-is.
    """
    if df.height == 0:
        return df

    required = {"year", "prefecture", "disease", "week", "count"}
    if not required.issubset(df.columns):
        return df

    group_cols = ["year", "prefecture", "disease"]
    sort_cols = [col for col in ["year", "prefecture", "disease", "week", "date"] if col in df.columns]
    out = df.sort(sort_cols, nulls_last=True).with_columns(pl.col("count").fill_null(0.0).alias("_count_cum"))

    weekly_diff = pl.col("_count_cum") - pl.col("_count_cum").shift(1).over(group_cols)
    out = out.with_columns(
        pl.when(pl.col("week") == 1)
        .then(pl.col("_count_cum"))
        .otherwise(weekly_diff)
        .alias("count")
    ).with_columns(
        pl.when(pl.col("count").is_null()).then(pl.col("_count_cum")).otherwise(pl.col("count")).alias("count")
    )

    return out.drop("_count_cum")


def _extract_year_week_sentinel_en(
    rows: list[list[str]], path: Path
) -> tuple[int | None, int | None]:
    """Extract year/week from English sentinel CSV header with filename fallback."""
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


def _to_float_cell(value: str | None) -> float | None:
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


def _read_sentinel_en_pl(
    path: Path,
    *,
    year: int | None = None,
    week: Iterable[int] | None = None,
) -> pl.DataFrame:
    """Read English sentinel surveillance CSV files from /rapid/ endpoint."""
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

            file_year, file_week = _extract_year_week_sentinel_en(rows, p)
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
                            "count": _to_float_cell(count_val),
                            "per_sentinel": _to_float_cell(per_val),
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
                .select(list(_SENTINEL_EN_SCHEMA.keys()))
            )
            frames.append(frame)

        except Exception:
            logger.exception("Failed to parse sentinel file: %s", p.name)
            continue

    if not frames:
        return pl.DataFrame(schema=_SENTINEL_EN_SCHEMA)
    return pl.concat(frames, how="vertical")


def download(
    name: DatasetName,
    year: int,
    *,
    out_dir: Path | str | None = None,
    overwrite: bool = False,
    week: int | Iterable[int] | None = None,
) -> Path | list[Path]:
    """Download raw data for a specific year.

    Args:
        name: Dataset name ("sex", "place", "bullet", or "sentinel").
        year: Year of the data (e.g., 2023).
        out_dir: Directory to save file. Defaults to system cache.
        overwrite: If True, overwrite existing file(s).
        week: (Bullet/Sentinel only) Specific week(s) to download.

    Returns:
        Path to the downloaded file (for sex/place) or list of Paths (for bullet/sentinel).

    Example:
        >>> path = jp.download("sex", 2024)
        >>> bullet_paths = jp.download("bullet", 2024, week=[1, 2])
    """
    config = get_config()
    if out_dir is None:
        base_cache = Path(user_cache_dir("jp_idwr_db"))
        if name in ("bullet", "sentinel"):
            out_dir = base_cache / "raw" / name
        else:
            out_dir = base_cache / "raw" / "confirmed"

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if name in ("bullet", "sentinel"):
        # Week-based bullet/sentinel files reuse names each year (e.g., zensu01.csv),
        # so isolate storage by year to avoid cross-year collisions.
        year_out_dir = out_dir / str(year)
        year_out_dir.mkdir(parents=True, exist_ok=True)

        # Bullet or sentinel data
        urls = url_bullet(year, week) if name == "bullet" else url_sentinel(year, week)

        if not urls:
            return []

        existing = {p.name: p for p in year_out_dir.glob("*.csv")}
        if overwrite:
            return download_urls(urls, year_out_dir, config)

        needed = [url for url in urls if Path(url).name not in existing]
        if not needed:
            return [existing[Path(url).name] for url in urls]

        downloaded = download_urls(needed, year_out_dir, config)
        downloaded_map = {p.name: p for p in downloaded}

        # Return all requested (existing + newly downloaded)
        return [
            existing[fname] if fname in existing else downloaded_map[fname]
            for url in urls
            if (fname := Path(url).name) in existing or fname in downloaded_map
        ]

    else:
        # Confirmed (sex or place)
        type_ = cast(Literal["sex", "place"], name)
        url = url_confirmed(year, type_)
        filename = f"{year}_{Path(url).name}"
        dest = out_dir / filename

        if dest.exists() and not overwrite:
            return dest

        downloaded = download_urls([url], out_dir, config)
        if not downloaded:
            raise RuntimeError(f"Failed to download {name} data for year {year}")

        actual_file = downloaded[0]
        if actual_file.name != filename:
            final_dest = out_dir / filename
            if actual_file != final_dest:
                actual_file.rename(final_dest)
                return final_dest
        return actual_file


def download_recent(
    *,
    out_dir: Path | str | None = None,
    overwrite: bool = False,
) -> list[Path]:
    """Download all available bullet data (weekly reports) from 2024 onwards.

    Iterates through years and weeks to fetch all available CSVs.
    Stops fetching for a year after multiple consecutive 404s (end of data).

    Args:
        out_dir: Destination directory. Defaults to system cache.
        overwrite: If True, overwrite existing files.

    Returns:
        List of paths to downloaded files.

    Example:
        >>> paths = jp.download_recent()  # Download all 2024+ data
        >>> len(paths)
        52
    """
    current_year = dt.date.today().year
    years = range(2024, current_year + 2)

    all_files: list[Path] = []

    for year in years:
        miss_count = 0
        year_files: list[Path] = []

        for week in range(1, 54):
            try:
                paths = download(
                    "bullet",
                    year,
                    out_dir=out_dir,
                    overwrite=overwrite,
                    week=week,
                )
                if paths:
                    year_files.extend(paths if isinstance(paths, list) else [paths])
                    miss_count = 0
                else:
                    miss_count += 1
            except Exception:
                miss_count += 1

            # Stop if we miss too many weeks (likely future weeks)
            if miss_count > 5:
                break

        if not year_files and year > current_year:
            break

        all_files.extend(year_files)

    return all_files


def read(
    path: Path | str,
    type: DatasetName | None = None,
) -> pl.DataFrame:
    """Read a local raw file into a DataFrame.

    Automatically detects file type (Excel vs CSV) and dataset type
    (sex, place, bullet) from filename if not specified.

    Args:
        path: Path to the Excel or CSV file (or directory).
        type: "sex", "place", or "bullet". Inferred from filename if None.

    Returns:
        Polars DataFrame containing the parsed data.

    Raises:
        ValueError: If dataset type cannot be inferred from filename.

    Example:
        >>> df = jp.read("Syu_01_1_2024.xlsx", type="sex")
        >>> df_bullet = jp.read("2024-01-zensu.csv")  # Auto-detects as bullet
    """
    path = Path(path)

    # Infer type if not specified
    if type is None:
        if path.suffix == ".csv" or (path.is_dir() and list(path.glob("*.csv"))):
            type = "bullet"
        elif "Syu_01" in path.name or "sex" in path.name:
            type = "sex"
        elif "Syu_02" in path.name or "place" in path.name:
            type = "place"
        else:
            raise ValueError("Could not infer dataset type from filename. Please specify 'type'.")

    df = _read_bullet_pl(path) if type == "bullet" else _read_confirmed_pl(path, type=type)

    return df


def get_disease_name_mappings() -> dict[str, str]:
    """Get the tracker of original -> cleaned disease name mappings.

    This function returns a dictionary mapping original disease names (as they
    appear in the raw data) to their cleaned/normalized versions. The tracker
    is populated during data reading operations.

    Returns:
        Dictionary mapping original disease names to normalized names.

    Example:
        >>> import jp_idwr_db as jp
        >>> df = jp.load("sex")  # Populates the tracker
        >>> mappings = jp.get_disease_name_mappings()
        >>> print(mappings.get("H5N1) (Avian influenza H5N1"))
        'Avian influenza H5N1'
    """
    return _disease_name_tracker.copy()
