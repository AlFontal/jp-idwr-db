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

import datetime as dt
import logging
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Literal

import polars as pl
from platformdirs import user_cache_dir

from .config import get_config
from .http import download_urls
from .types import AnyFrame, DatasetName, ReturnType
from .urls import url_bullet, url_confirmed
from .utils import resolve_return_type, to_pandas

logger = logging.getLogger(__name__)


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
        clean = re.sub(r"^.*[\r\n]+", "", clean)
        # Remove Excel-generated column names like "...1", "...2"
        clean = re.sub(r"^\.\.\.[0-9]+$", "", clean)
        # Replace full-width characters with ASCII equivalents
        clean = clean.replace("Ｉ", "I")  # noqa: RUF001
        clean = clean.replace("（", "(").replace("）", ")")  # noqa: RUF001
        # Collapse multiple spaces
        clean = re.sub(r"\s+", " ", clean).strip()
        # Remove leading and trailing parentheses
        clean = re.sub(r"^\(", "", clean)
        clean = re.sub(r"\)$", "", clean)
        if clean:
            cleaned.append(clean)
    return cleaned


def _clean_cell_text(text: str | None) -> str | None:
    """Clean text from Excel cells (handles null bytes, extracts English).

    Excel files from 1999-2000 contain null bytes. Bilingual cells have
    Japanese text followed by English in parentheses - we extract the English.

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
    # Extract English text from bilingual cells like "日本語 (English)"
    match = re.search(r"\(([\x20-\x7E]+)\)", clean)
    if match:
        return match.group(1).strip()
    return clean.strip()


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
    week_match = re.search(r"(?:-)(\\d{2})|zensu(\\d{2})", path.name)
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
            pl.col("variable").str.split("||").list.get(0).alias("disease"),
            pl.col("variable").str.split("||").list.get(1).alias("category"),
        ]
    ).drop("variable")

    # Clean count column (convert to int, treating errors as 0)
    long_df = long_df.with_columns(
        pl.col("count").cast(pl.Float64, strict=False).fill_null(0).cast(pl.Int64)
    )

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

            frames.append(long_df)

        except Exception:
            logger.exception(f"Failed to parse bullet file: {p.name}")
            continue

    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="vertical")


def download(
    name: DatasetName,
    year: int,
    *,
    out_dir: Path | str | None = None,
    overwrite: bool = False,
    week: int | Iterable[int] | None = None,
    lang: Literal["en", "ja"] = "en",
) -> Path | list[Path]:
    """Download raw data for a specific year.

    Args:
        name: Dataset name ("sex", "place", or "bullet").
        year: Year of the data (e.g., 2023).
        out_dir: Directory to save file. Defaults to system cache.
        overwrite: If True, overwrite existing file(s).
        week: (Bullet only) Specific week(s) to download.
        lang: (Bullet only) Language for bullet data ("en" or "ja").

    Returns:
        Path to the downloaded file (for sex/place) or list of Paths (for bullet).

    Example:
        >>> path = jp.download("sex", 2024)
        >>> bullet_paths = jp.download("bullet", 2024, week=[1, 2], lang="en")
    """
    config = get_config()
    if out_dir is None:
        base_cache = Path(user_cache_dir("jpinfectpy"))
        out_dir = base_cache / "raw" / ("bullet" if name == "bullet" else "confirmed")

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if name == "bullet":
        urls = url_bullet(year, week, lang=lang)
        if not urls:
            return []

        existing = {p.name: p for p in out_dir.glob("*.csv")}
        if overwrite:
            return download_urls(urls, out_dir, config)

        needed = [url for url in urls if Path(url).name not in existing]
        if not needed:
            return [existing[Path(url).name] for url in urls]

        downloaded = download_urls(needed, out_dir, config)
        downloaded_map = {p.name: p for p in downloaded}

        # Return all requested (existing + newly downloaded)
        return [
            existing[fname] if fname in existing else downloaded_map[fname]
            for url in urls
            if (fname := Path(url).name) in existing or fname in downloaded_map
        ]

    else:
        # Confirmed (sex or place)
        type_ = name
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
    lang: Literal["en", "ja"] = "en",
) -> list[Path]:
    """Download all available bullet data (weekly reports) from 2024 onwards.

    Iterates through years and weeks to fetch all available CSVs.
    Stops fetching for a year after multiple consecutive 404s (end of data).

    Args:
        out_dir: Destination directory. Defaults to system cache.
        overwrite: If True, overwrite existing files.
        lang: Language for bulletin ("en" or "ja").

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
                    "bullet", year, out_dir=out_dir, overwrite=overwrite, week=week, lang=lang
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
    *,
    return_type: ReturnType | None = None,
) -> AnyFrame:
    """Read a local raw file into a DataFrame.

    Automatically detects file type (Excel vs CSV) and dataset type
    (sex, place, bullet) from filename if not specified.

    Args:
        path: Path to the Excel or CSV file (or directory).
        type: "sex", "place", or "bullet". Inferred from filename if None.
        return_type: "polars" or "pandas". Uses global config if None.

    Returns:
        DataFrame containing the parsed data.

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

    if resolve_return_type(return_type) == "pandas":
        return to_pandas(df)
    return df
