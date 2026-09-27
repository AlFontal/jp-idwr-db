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

import httpx
import polars as pl
from platformdirs import user_cache_dir

from .config import get_config
from .http import cached_head, download_urls
from .types import DatasetName
from .urls import AnnualTable, url_annual, url_bullet, url_confirmed, url_sentinel
from .utils import PREFECTURE_ISO_MAP, iso_weeks_in_year

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
    # MERS
    "Middle East Respiratory Syndrome Coronavirus": "Middle East Respiratory Syndrome (MERS)",
    # Drug-resistant Acinetobacter
    "Multiple drug-resistant Acinetobacter infection": "Multidrug-resistant Acinetobacter infection",
    # Scrub typhus / Tsutsugamushi
    "Scrub typhus (Tsutsugamushi disease)": "Scrub typhus",
    "Scrub typhus(Tsutsugamushi disease)": "Scrub typhus",
    "Scrub typhus(Tsutsugamushi disease": "Scrub typhus",
    "Tsutsugamushi disease": "Scrub typhus",
    # Severe invasive streptococcal
    "Severe invasive streptococcal infections (TSLS)": "Severe invasive streptococcal infections",
    "Severe invasive streptococcal infections(TSLS)": "Severe invasive streptococcal infections",
    "Severe invasive streptococcal infections(TSLS": "Severe invasive streptococcal infections",
    # SFTS
    "Severe Fever with Thrombocytopenia Syndrome(SFTS)": "Severe Fever with Thrombocytopenia Syndrome",
    # Varicella / chickenpox sentinel label
    "Varicella": "Chickenpox",
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


def _normalize_disease_column(df: pl.DataFrame, column: str) -> pl.DataFrame:
    """Normalize disease names in a DataFrame column and update the tracker."""
    if column not in df.columns or df.height == 0:
        return df

    disease_mappings: dict[str, str] = {}
    for raw_name in df[column].drop_nulls().unique().to_list():
        normalized = _normalize_disease_name(raw_name)
        disease_mappings[raw_name] = normalized
        if raw_name not in _disease_name_tracker:
            _disease_name_tracker[raw_name] = normalized

    return df.with_columns(pl.col(column).replace(disease_mappings))


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


def _is_confirmed_category_row(row: list[str | None]) -> bool:
    """Return whether a raw Excel row looks like a confirmed-data category header."""
    cleaned = [_clean_cell_text(value) for value in row[1:]]
    lowered = {str(value).lower() for value in cleaned if value}
    if not any("total" in value for value in lowered):
        return False
    return any(
        marker in value
        for value in lowered
        for marker in ("male", "female", "japan", "others", "unknown")
    )


def _find_confirmed_header_rows(df_raw: pl.DataFrame) -> list[int]:
    """Find disease-header rows in a raw confirmed-data Excel sheet."""
    header_rows: list[int] = []
    for idx in range(df_raw.height - 1):
        disease_row = [str(value) if value is not None else None for value in df_raw.row(idx)]
        category_row = [str(value) if value is not None else None for value in df_raw.row(idx + 1)]
        has_disease = any(_clean_cell_text(value) for value in disease_row[1:])
        if has_disease and _is_confirmed_category_row(category_row):
            header_rows.append(idx)
    return header_rows


def _parse_excel_sheet_blocks(df_raw: pl.DataFrame) -> list[pl.DataFrame]:
    """Parse one raw confirmed-data Excel sheet into one frame per header block."""
    header_rows = _find_confirmed_header_rows(df_raw)
    frames: list[pl.DataFrame] = []

    for position, header_idx in enumerate(header_rows):
        next_header_idx = (
            header_rows[position + 1] if position + 1 < len(header_rows) else df_raw.height
        )
        data_start = header_idx + 2
        if data_start >= next_header_idx:
            continue

        disease_row = [
            str(value) if value is not None else None for value in df_raw.row(header_idx)
        ]
        category_row = [
            str(value) if value is not None else None for value in df_raw.row(header_idx + 1)
        ]
        headers = _resolve_headers(list(df_raw.columns), disease_row, category_row)

        data_df = df_raw.slice(data_start, next_header_idx - data_start)
        data_df.columns = headers

        if "prefecture" in data_df.columns:
            data_df = data_df.with_columns(
                pl.col("prefecture").map_elements(
                    lambda x: _clean_cell_text(str(x)) if x is not None else None,
                    return_dtype=pl.Utf8,
                )
            )
            data_df = data_df.filter(
                pl.col("prefecture").is_not_null()
                & ~pl.col("prefecture").str.to_lowercase().str.contains("total")
            )

        if not data_df.is_empty():
            frames.append(data_df)

    return frames


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

            sheet_blocks = _parse_excel_sheet_blocks(df_raw)
            if not sheet_blocks:
                logger.debug(f"Skipping sheet {sheet}: No confirmed-data header blocks found")
                continue

            for data_df in sheet_blocks:
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
    year = int(year_match.group(0)) if year_match else None
    if year is None and re.fullmatch(r"(19|20)\d{2}", path.parent.name):
        year = int(path.parent.name)

    week_match = re.search(r"(?:zensu|teiten(?:rui)?)(\d{2})", path.stem, re.IGNORECASE)
    if week_match is None:
        week_match = re.search(r"(?:^|[-_])(\d{2})(?:[-_]|$)", path.stem)
    week = None
    if week_match:
        week = int(week_match.group(1))
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
    # Sheet 1 is the annual total, followed by one sheet per ISO week (52 or 53).
    return range(2, iso_weeks_in_year(year) + 2)


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


def _iso_week_start_date(year: int, week: int) -> dt.date | None:
    """Convert ISO year and week to the week start date (Monday)."""
    try:
        return dt.date.fromisocalendar(int(year), int(week), 1)
    except Exception:
        return None


def _confirmed_wide_to_long(df: pl.DataFrame) -> pl.DataFrame:
    """Convert one confirmed-data wide block to normalized long form."""
    if df.is_empty():
        return df

    # Calculate a consistent ISO week-start date (Monday).
    if "date" not in df.columns and "year" in df.columns and "week" in df.columns:
        df = df.with_columns(
            pl.struct(["year", "week"])
            .map_elements(
                lambda x: _iso_week_start_date(x["year"], x["week"]),
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

    long_df = long_df.rename({"disease_raw": "disease"})
    long_df = _normalize_disease_column(long_df, "disease")

    # Clean count column (convert to int, treating errors as 0)
    long_df = long_df.with_columns(
        pl.col("count").cast(pl.Float64, strict=False).fill_null(0).cast(pl.Int64)
    )

    # Add source column
    return long_df.with_columns(pl.lit("Confirmed cases").alias("source"))


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
        for sheet, frame in excel_frames:
            week = sheet + week_offset
            enhanced = frame.with_columns([pl.lit(year).alias("year"), pl.lit(week).alias("week")])
            frames.append(_confirmed_wide_to_long(enhanced))

    if not frames:
        return pl.DataFrame()

    df = pl.concat(frames, how="diagonal_relaxed")
    return df


# English labels that the annual sentinel tables print differently across eras for
# a disease whose Japanese label is identical (typing, encoding, or translation
# changes only). Definitional changes (e.g. influenza exclusions) are not merged.
SENTINEL_NAME_HARMONIZATION = {
    "Erythema infectiosum": "Erythema infection",  # 伝染性紅斑
    "GroupA streptococcal pharyngitis": "Group A streptococcal pharyngitis",
    "Hand,foot and mouth disease": "Hand, foot and mouth disease",
    "Mycoplasmal pneumonia": "Mycoplasma pneumonia",  # マイコプラズマ肺炎
    "Chlamydial Pneumonia": "Chlamydial pneumonia(excluding psittacosis)",  # クラミジア肺炎(オウム病を除く)
    "Measles": "Measles(excluding measles in adults)",  # 麻疹(成人麻疹を除く)
    "Acute encephalitis": "Acute encephalitis (excluding Japanese encephalitis)",  # 急性脳炎(日本脳炎を除く)
}

# Weeks during which a disease was under sentinel surveillance. The annual tables
# print zeros outside these windows (before a disease was added, or after it moved
# to all-case reporting); those placeholder rows are dropped.
SENTINEL_SURVEILLANCE_WINDOWS: dict[str, tuple[tuple[int, int] | None, tuple[int, int] | None]] = {
    "Acute encephalitis (excluding Japanese encephalitis)": (None, (2003, 45)),
    "Infectious gastroenteritis (only by Rotavirus)": ((2013, 42), None),
    "COVID-19": ((2023, 19), None),
}

_WEEK_LABEL = re.compile(r"\(\s*(?:week\s*)?(\d{1,2})\s*(?:week|週)?\s*\)", re.IGNORECASE)


def _annual_sheet_week(df_raw: pl.DataFrame) -> int | None:
    """Read the week number from an annual table sheet's title rows."""
    for row in df_raw.head(4).iter_rows():
        for value in row:
            if value is None:
                continue
            text = _normalize_fullwidth(str(value).replace("\x00", ""))
            match = _WEEK_LABEL.search(text)
            if match:
                return int(match.group(1))
    return None


def _annual_cell_value(value: object) -> float | None:
    """Parse an annual table cell; IDWR tables write zero as ``-``."""
    if value is None:
        return None
    text = str(value).replace("\x00", "").strip().replace(",", "")
    if text in {"-", "\uff0d"}:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return None


def _harmonize_sentinel_disease(name: str) -> str:
    """Normalise annual sentinel disease labels across table eras."""
    # Old .xls files decode a full-width space as U+FFFD followed by "@".
    clean = " ".join(name.replace("\ufffd@", " ").replace("\ufffd", " ").split())
    clean = _normalize_disease_name(clean)
    return SENTINEL_NAME_HARMONIZATION.get(clean, clean)


def _in_surveillance_window() -> pl.Expr:
    """Expression that is False for rows outside a disease's surveillance window."""
    keep = pl.lit(True)
    year_week = pl.col("year") * 100 + pl.col("week")
    for disease, (start, end) in SENTINEL_SURVEILLANCE_WINDOWS.items():
        outside = pl.lit(False)
        if start is not None:
            outside = outside | (year_week < start[0] * 100 + start[1])
        if end is not None:
            outside = outside | (year_week > end[0] * 100 + end[1])
        keep = keep & ~((pl.col("disease") == disease) & outside)
    return keep


def read_annual_sentinel(path: Path | str, year: int, *, value_name: str = "count") -> pl.DataFrame:
    """Read an annual sentinel table (weekly counts or rates) into long format.

    Each sheet after the first holds one week, labelled in its title rows. Only the
    ``total`` (both sexes) columns are kept. Week sheets beyond the ISO weeks of the
    year are blank templates and are dropped; they must contain only zeros.

    Returns:
        Columns ``prefecture, year, week, date, disease`` and ``value_name``.
    """
    path = Path(path)
    import fastexcel  # noqa: PLC0415 - optional "excel" extra

    sheet_count = len(fastexcel.read_excel(str(path)).sheet_names)
    frames: list[pl.DataFrame] = []
    for sheet in range(2, sheet_count + 1):
        df_raw = pl.read_excel(str(path), sheet_id=sheet, has_header=False)
        week = _annual_sheet_week(df_raw)
        if week is None:
            raise ValueError(f"{path.name} sheet {sheet}: no week label found")
        for block in _parse_excel_sheet_blocks(df_raw):
            totals = [c for c in block.columns if c.endswith("||total")]
            if not totals:
                continue
            long_df = block.select(["prefecture", *totals]).unpivot(
                index="prefecture", on=totals, variable_name="disease", value_name=value_name
            )
            frames.append(
                long_df.with_columns(
                    pl.col("disease").str.replace(r"\|\|total$", ""),
                    pl.col(value_name).map_elements(_annual_cell_value, return_dtype=pl.Float64),
                    pl.lit(week, dtype=pl.Int32).alias("week"),
                )
            )
    if not frames:
        raise ValueError(f"No sentinel data parsed from {path.name}")

    df = pl.concat(frames, how="vertical_relaxed").with_columns(
        pl.lit(year, dtype=pl.Int32).alias("year"),
        pl.col("disease").map_elements(_harmonize_sentinel_disease, return_dtype=pl.String),
    )
    df = df.filter(pl.col("prefecture").is_in(list(PREFECTURE_ISO_MAP)))

    beyond_iso = df.filter(pl.col("week") > iso_weeks_in_year(year))
    if beyond_iso.height and beyond_iso[value_name].fill_null(0).abs().sum() > 0:
        raise ValueError(f"{path.name}: non-zero values in week sheets beyond ISO week count")
    df = df.filter(pl.col("week") <= iso_weeks_in_year(year)).filter(_in_surveillance_window())
    return df.with_columns(
        pl.struct(["year", "week"])
        .map_elements(
            lambda v: dt.date.fromisocalendar(int(v["year"]), int(v["week"]), 1),
            return_dtype=pl.Date,
        )
        .alias("date")
    ).select(["prefecture", "year", "week", "date", "disease", value_name])


def annual_cache_path(table: AnnualTable, year: int, *, out_dir: Path | str | None = None) -> Path:
    """Return where an annual table is cached (``{year}_{file}``, as ``download`` names it)."""
    url = url_annual(year, table)
    if url is None:
        raise ValueError(f"No annual {table} table exists for {year}")
    subdir = "confirmed" if table in {"sex", "place"} else "annual"
    directory = (
        Path(out_dir)
        if out_dir is not None
        else Path(user_cache_dir("jp_idwr_db")) / "raw" / subdir
    )
    return directory / f"{year}_{Path(url).name}"


def download_annual(
    table: AnnualTable,
    year: int,
    *,
    out_dir: Path | str | None = None,
    overwrite: bool = False,
) -> Path:
    """Download an annual IDWR table into the raw cache and return its path."""
    url = url_annual(year, table)
    if url is None:
        raise ValueError(f"No annual {table} table exists for {year}")
    dest = annual_cache_path(table, year, out_dir=out_dir)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and not overwrite:
        return dest
    downloaded = download_urls([url], dest.parent, get_config())[0]
    if downloaded != dest:
        downloaded.replace(dest)
    return dest


def annual_table_available(table: AnnualTable, year: int) -> bool:
    """Return whether an annual table is published, failing safe on errors.

    Only an explicit HTTP 200 counts as available; 404 means not published yet,
    and any other status or network error is logged and treated as unavailable.
    """
    url = url_annual(year, table)
    if url is None:
        return False
    try:
        response = cached_head(url, get_config())
    except httpx.HTTPError as exc:
        logger.warning("Could not check annual %s table for %s: %s", table, year, exc)
        return False
    if response.status_code == 200:
        return True
    if response.status_code != 404:
        logger.warning(
            "Unexpected status %s checking annual %s table for %s",
            response.status_code,
            table,
            year,
        )
    return False


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

            if "prefecture" in df_raw.columns:
                df_raw = df_raw.filter(
                    ~pl.col("prefecture")
                    .cast(pl.Utf8, strict=False)
                    .fill_null("")
                    .str.to_lowercase()
                    .str.starts_with("total")
                )

            # Unpivot to long format
            value_vars = [c for c in df_raw.columns if c != "prefecture"]
            if not value_vars:
                continue

            long_df = df_raw.unpivot(
                index=["prefecture"], on=value_vars, variable_name="disease", value_name="count"
            )
            long_df = _normalize_disease_column(long_df, "disease")

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
                        lambda x: _iso_week_start_date(int(x["year"]), int(x["week"])),
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

            long_df = _normalize_disease_column(long_df, "disease")

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


SENTINEL_COUNT_STATUSES = {
    "derived": "Difference of consecutive year-to-date totals (week 1: its own total).",
    "annual": "Final weekly count from the annual IDWR table.",
    "gap": "Unknown: the previous week's total is missing.",
    "inconsistent": "Unknown: depends on a year-to-date total that is out of line with its neighbours.",
    "correction": "Unknown: the year-to-date total decreased for good (source correction or reset).",
    "series_start": "Unknown: first observed week of the year is not week 1.",
    "missing": "Unknown: the year-to-date total is blank in the source.",
}


# A decrease that recovers within this many weeks marks the low totals as errors.
_MAX_DIP_WEEKS = 4
_KNOWN_STATUSES = ("derived",)


def _series_statuses(weeks: list[int], totals: list[float | None]) -> list[str]:
    """Classify one year/prefecture/disease series of year-to-date totals."""
    n = len(weeks)
    status: list[str] = []
    last_observed: int | None = None
    for i in range(n):
        if totals[i] is None:
            status.append("missing")
            continue
        if weeks[i] == 1:
            status.append("derived")
        elif last_observed is None:
            status.append("series_start")
        elif weeks[last_observed] != weeks[i] - 1:
            status.append("gap")
        else:
            status.append("derived")
        last_observed = i

    def consecutive(start: int, stop: int) -> bool:
        """Rows start..stop are observed consecutive weeks."""
        if start < 0 or stop >= n:
            return False
        return all(
            totals[k] is not None and weeks[k] == weeks[start] + (k - start)
            for k in range(start, stop + 1)
        )

    blank: set[int] = set()
    for i in range(1, n):
        prev = i - 1
        current, previous = totals[i], totals[prev]
        if current is None or previous is None or not consecutive(prev, i):
            continue
        if current >= previous:
            continue
        # C(t) < C(t-1): at least one total around week t is wrong.
        recovery = next(
            (
                k
                for k in range(1, _MAX_DIP_WEEKS + 1)
                if consecutive(i, i + k) and (totals[i + k] or 0.0) >= previous
            ),
            None,
        )
        earlier = totals[prev - 1] if prev >= 1 else None
        previous_too_high = earlier is not None and consecutive(prev - 1, i) and current >= earlier
        # When several explanations fit, blank every week any of them affects.
        if previous_too_high:
            blank.update({prev, i})  # C(t-1) too high
        if recovery is not None:
            blank.update(range(i, i + recovery + 1))  # C(t)..C(t+k-1) too low
        elif status[i] == "derived":
            status[i] = "correction"  # lasting decrease: continue from the new level

    for i in blank:
        if status[i] in {"derived", "correction", "series_start"}:
            status[i] = "inconsistent"
    return status


def _sentinel_cumulative_to_weekly(df: pl.DataFrame) -> pl.DataFrame:
    """Convert cumulative sentinel counts to weekly incidence.

    Sentinel ``teitenrui`` files report year-to-date cumulative counts per
    prefecture and disease. Weekly counts are only published where the
    cumulative data determine them; ``count_status`` records why each value is
    what it is (see ``SENTINEL_COUNT_STATUSES``):

    - ``derived``: consecutive totals ``C(t) - C(t-1)``, or ``C(1)`` for week 1.
    - ``inconsistent``: a decrease ``C(t) < C(t-1)`` means a nearby total is
      wrong. The weeks that depend on a possibly wrong total are unknown:
      ``t`` and ``t+1`` if ``C(t)`` is too low (recovers next week), ``t-1`` and
      ``t`` if ``C(t-1)`` is too high (``C(t)`` back on trend), ``t-1`` to
      ``t+1`` if both fit, or ``t`` to the recovery week for dips of up to four
      weeks.
    - ``correction``: a lasting decrease; counting continues from the new level.
    - ``gap``, ``series_start``, ``missing``: the previous total is unavailable.

    Unknown weeks are null; nothing is imputed.
    """
    if df.height == 0:
        return df

    required = {"year", "prefecture", "disease", "week", "count"}
    if not required.issubset(df.columns):
        return df

    group_cols = ["year", "prefecture", "disease"]
    out = df.sort([*group_cols, "week"], nulls_last=True).with_columns(
        pl.col("count").alias("_cum")
    )

    statuses: list[str] = []
    for _, series in out.select([*group_cols, "week", "_cum"]).group_by(
        group_cols, maintain_order=True
    ):
        statuses.extend(_series_statuses(series["week"].to_list(), series["_cum"].to_list()))
    out = out.with_columns(pl.Series("count_status", statuses, dtype=pl.String))

    previous_total = pl.col("_cum").shift(1).over(group_cols)
    out = out.with_columns(
        pl.when(~pl.col("count_status").is_in(_KNOWN_STATUSES))
        .then(None)
        .when(pl.col("week") == 1)
        .then(pl.col("_cum"))
        .otherwise(pl.col("_cum") - previous_total)
        .alias("count")
    )

    if "per_sentinel" in out.columns:
        sites = (
            pl.when((pl.col("_cum") > 0) & (pl.col("per_sentinel") > 0))
            .then(pl.col("_cum") / pl.col("per_sentinel"))
            .otherwise(None)
        )
        out = out.with_columns(sites.alias("_sentinel_sites"))
        weekly_per_sentinel = (
            pl.when(pl.col("count").is_null())
            .then(None)
            .when(pl.col("count") == 0)
            .then(0.0)
            .when(pl.col("_sentinel_sites").is_null() | (pl.col("_sentinel_sites") <= 0))
            .then(None)
            .otherwise(pl.col("count") / pl.col("_sentinel_sites"))
        )
        out = out.with_columns(weekly_per_sentinel.alias("per_sentinel"))

    helper_cols = [col for col in out.columns if col.startswith("_")]
    return out.drop(helper_cols)


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
    """Convert a sentinel CSV numeric cell to float.

    IDWR tables write zero as ``-`` (the files contain almost no literal zeros),
    so a dash is read as 0. Blank cells are unknown.
    """
    if value is None:
        return None
    text = value.strip().replace(",", "")
    if text == "-":
        return 0.0
    if text == "":
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
            frame = _normalize_disease_column(frame, "disease")
            frames.append(frame)

        except Exception:
            logger.exception("Failed to parse sentinel file: %s", p.name)
            continue

    if not frames:
        return pl.DataFrame(schema=_SENTINEL_EN_SCHEMA)
    return pl.concat(frames, how="vertical")


def _read_sentinel_auto(
    path: Path,
    *,
    year: int | None = None,
    week: Iterable[int] | None = None,
) -> pl.DataFrame:
    """Read sentinel CSV files by trying English parsing, then Japanese fallback."""
    files = list(path.glob("*.csv")) if path.is_dir() else [path]
    frames: list[pl.DataFrame] = []

    for file_path in sorted(files):
        english_df = _read_sentinel_en_pl(file_path, year=year, week=week)
        if english_df.height > 0:
            frames.append(english_df)
            continue

        japanese_df = _read_sentinel_pl(file_path, year=year, week=week)
        if japanese_df.height > 0:
            frames.append(japanese_df)

    if not frames:
        return pl.DataFrame(schema=_SENTINEL_EN_SCHEMA)
    return pl.concat(frames, how="vertical_relaxed")


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
        >>> from jp_idwr_db.io import download
        >>> path = download("sex", 2024)
        >>> bullet_paths = download("bullet", 2024, week=[1, 2])
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
        >>> from jp_idwr_db.io import download_recent
        >>> paths = download_recent()  # Download all 2024+ data
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
        >>> from jp_idwr_db.io import read
        >>> df = read("Syu_01_1_2024.xlsx", type="sex")
        >>> df_bullet = read("2024-01-zensu.csv")  # Auto-detects as bullet
    """
    path = Path(path)

    # Infer type if not specified
    if type is None:
        if path.suffix == ".csv" or (path.is_dir() and list(path.glob("*.csv"))):
            csv_files = [path] if path.suffix == ".csv" else list(path.glob("*.csv"))
            if any(
                re.search(r"teiten(?:rui)?", csv_path.stem, re.IGNORECASE) for csv_path in csv_files
            ):
                type = "sentinel"
            else:
                type = "bullet"
        elif "Syu_01" in path.name or "sex" in path.name:
            type = "sex"
        elif "Syu_02" in path.name or "place" in path.name:
            type = "place"
        else:
            raise ValueError("Could not infer dataset type from filename. Please specify 'type'.")

    if type == "bullet":
        df = _read_bullet_pl(path)
    elif type == "sentinel":
        df = _read_sentinel_auto(path)
    else:
        df = _read_confirmed_pl(path, type=type)

    return df


def get_disease_name_mappings() -> dict[str, str]:
    """Get the tracker of original -> cleaned disease name mappings.

    This function returns a dictionary mapping original disease names (as they
    appear in the raw data) to their cleaned/normalized versions. The tracker
    is populated during data reading operations.

    Returns:
        Dictionary mapping original disease names to normalized names.

    Example:
        >>> from jp_idwr_db.io import get_disease_name_mappings, read
        >>> df = read("Syu_01_1_2024.xlsx", type="sex")  # Populates the tracker
        >>> mappings = get_disease_name_mappings()
        >>> print(mappings.get("H5N1) (Avian influenza H5N1"))
        'Avian influenza H5N1'
    """
    return _disease_name_tracker.copy()
