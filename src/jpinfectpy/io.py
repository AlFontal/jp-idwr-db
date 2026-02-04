from __future__ import annotations

import datetime as dt
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


def _col_rename_bullet(names: list[str]) -> list[str]:
    cleaned: list[str] = []
    for name in names:
        name = re.sub(r"^.*[\r\n]+", "", str(name))
        name = re.sub(r"^.*[\r\n]+", "", name)
        name = re.sub(r"^\.\.\.[0-9]+$", "", name)
        name = name.replace("Ｉ", "I")  # noqa: RUF001
        name = name.replace("（", "(").replace("）", ")")  # noqa: RUF001
        name = re.sub(r"\s+", " ", name).strip()
        name = re.sub(r"^\(", "", name)
        name = re.sub(r"\)$", "", name)
        if name:
            cleaned.append(name)
    return cleaned


def _clean_cell_text(text: str | None) -> str | None:
    if not text:
        return None
    # Remove null bytes (1999 issue)
    t = text.replace("\x00", "")
    # Remove newlines/tabs
    t = t.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    # Extract English text in parens if present
    match = re.search(r"\(([\x20-\x7E]+)\)", t)
    if match:
        return match.group(1).strip()
    return t.strip()


def _resolve_headers(
    cols: list[str | None], row2: list[str | None], row3: list[str | None]
) -> list[str]:
    # row2: Disease names (merged, sparse)
    # row3: Categories (Total/Male/Female etc)
    headers = []
    current_disease = "Unknown"

    # First column is always prefecture
    headers.append("prefecture")

    # Iterate from col 1 (since col 0 is prefecture)
    for i in range(1, len(cols)):
        r2 = _clean_cell_text(row2[i])
        r3 = _clean_cell_text(row3[i])

        if r2:
            current_disease = r2

        cat = r3 if r3 else "total"
        # Normalize category
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

        base_header = f"{current_disease}||{cat}"

        # Deduplicate
        count = 1
        new_header = base_header
        while new_header in headers:
            new_header = f"{base_header}_{count}"
            count += 1

        headers.append(new_header)

    return headers


def _read_excel_sheets(
    file_path: Path, sheet_range: Iterable[int]
) -> list[tuple[int, pl.DataFrame]]:
    frames: list[tuple[int, pl.DataFrame]] = []
    path_str = str(file_path)

    # Pre-calculate sheet indices we need
    # Note: sheet_range is 1-based index usually?
    # In io.py originally: `range(2, 41)` for 1999.
    # In my debug: 1999 Sheet 2 was Week 14.
    # If standard is Week 1 starts at logical index X.
    # We should trust existing ranges but adapt how we call read_excel.

    for sheet in sheet_range:
        try:
            # Read headerless
            df = pl.read_excel(path_str, sheet_id=sheet, has_header=False)

            if df.height < 5:
                continue

            # Parse Rows
            # Polars rows() returns values. row(i) returns tuple.
            # Row 0,1: Skip
            # Row 2: Disease
            # Row 3: Category
            # Row 4+: Data

            row2 = [str(x) if x is not None else None for x in df.row(2)]
            row3 = [str(x) if x is not None else None for x in df.row(3)]

            # Check if this looks like a valid header row?
            # heuristic: row3 has "Total" or "total"
            # Cleaning null bytes before check
            row3_clean = [_clean_cell_text(x) for x in row3]
            if not any("total" in str(x).lower() for x in row3_clean if x):
                # Fallback or skip?
                # Maybe offset by 1 if title varies?
                print(f"Skipping Sheet {sheet}: No 'total' in Row 3.")
                continue

            headers = _resolve_headers(df.columns, row2, row3)

            # Slice data
            data_df = df.slice(4)
            data_df.columns = headers

            # Clean prefecture column
            if "prefecture" in data_df.columns:
                data_df = data_df.with_columns(
                    pl.col("prefecture").map_elements(
                        lambda x: _clean_cell_text(str(x)), return_dtype=pl.Utf8
                    )
                )
                # Remove "Total" rows in data
                data_df = data_df.filter(
                    ~pl.col("prefecture").str.to_lowercase().str.contains("total")
                )

            if not data_df.is_empty():
                frames.append((sheet, data_df))

        except Exception as e:
            print(f"Error reading sheet {sheet}: {e}")
            continue

    return frames


def _read_confirmed_pl(
    path: Path,
    *,
    type: DatasetName | None = None,
) -> pl.DataFrame:
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
            continue
        sheet_range = _sheet_range_for_year(year)
        excel_frames = _read_excel_sheets(file_path, sheet_range)
        week_offset = 12 if year == 1999 else -1
        combined = _combine_confirmed_frames(excel_frames, year, week_offset=week_offset)
        # _ensure_prefecture_column is redundant now as we parsed it
        frames.append(combined)

    if not frames:
        return pl.DataFrame()

    df = pl.concat(frames, how="diagonal_relaxed")

    # Calculate Date
    if "date" not in df.columns and "year" in df.columns and "week" in df.columns:
        df = df.with_columns(
            pl.struct(["year", "week"])
            .map_elements(
                lambda x: _iso_week_date(x["year"], x["week"]),
                return_dtype=pl.Date,
            )
            .alias("date")
        )

    # Deduplicate columns (remove _1, _2 suffixes which are artifacts of bad headers)
    # We assume the first occurrence was the correct/primary one.
    cols_to_drop = [c for c in df.columns if re.search(r"_[0-9]+$", c) and "||" in c]
    if cols_to_drop:
        df = df.drop(cols_to_drop)

    # Melton to Long Format
    # ID vars: prefecture, year, week, date
    id_vars = [c for c in df.columns if c in {"prefecture", "year", "week", "date"}]
    # Value vars: contains "||"
    value_vars = [c for c in df.columns if "||" in c]

    if not value_vars:
        return df

    long_df = df.unpivot(  # unpivot is melt in recent polars
        index=id_vars, on=value_vars, variable_name="variable", value_name="count"
    )

    # Split variable into disease and category
    long_df = long_df.with_columns(
        [
            pl.col("variable").str.split("||").list.get(0).alias("disease"),
            pl.col("variable").str.split("||").list.get(1).alias("category"),
        ]
    ).drop("variable")

    # Cast count to int (handle nulls)
    long_df = long_df.with_columns(
        pl.col("count").cast(pl.Float64, strict=False).fill_null(0).cast(pl.Int64)
    )

    return long_df


def _infer_year_from_path(path: Path) -> int | None:
    match = re.search(r"(19|20)\d{2}", path.name)
    if not match:
        return None
    return int(match.group(0))


def _extract_year_week(path: Path) -> tuple[int | None, int | None]:
    year_match = re.search(r"(19|20)\d{2}", path.name)
    week_match = re.search(r"(?:-)(\d{2})|zensu(\d{2})", path.name)
    year = int(year_match.group(0)) if year_match else None
    week = None
    if week_match:
        week = int(week_match.group(1) or week_match.group(2))
    return year, week


def _sheet_range_for_year(year: int) -> range:
    if year == 1999:
        return range(2, 41)
    if year in {2004, 2009, 2015}:
        return range(2, 55)
    return range(2, 54)


def _combine_confirmed_frames(
    frames: list[tuple[int, pl.DataFrame]],
    year: int,
    *,
    week_offset: int,
) -> pl.DataFrame:
    combined: list[pl.DataFrame] = []
    for sheet, df in frames:
        week = sheet + week_offset
        df = df.with_columns([pl.lit(year).alias("year"), pl.lit(week).alias("week")])
        combined.append(df)
    if not combined:
        return pl.DataFrame()

    # Use diagonal_relaxed to handle slight schema mismatches if any
    return pl.concat(combined, how="diagonal_relaxed")


def _iso_week_date(year: int, week: int) -> dt.date | None:
    try:
        return dt.date.fromisocalendar(int(year), int(week), 7)
    except Exception:
        return None


def _read_bullet_pl(
    path: Path,
    *,
    year: int | None = None,
    week: Iterable[int] | None = None,
) -> pl.DataFrame:
    # Handle single file or directory
    if path.is_dir():
        files = list(path.glob("*.csv"))
    else:
        files = [path]

    if week is not None:
        week_set = {int(w) for w in week}
        files = [p for p in files if (_extract_year_week(p)[1] in week_set)]

    frames: list[pl.DataFrame] = []
    for p in sorted(files):
        try:
            df = pl.read_csv(p, infer_schema_length=0)
            cleaned = _col_rename_bullet(df.columns)
            if len(cleaned) == len(df.columns):
                df = df.rename(dict(zip(df.columns, cleaned, strict=True)))
            if df.columns[0] != "prefecture":
                df = df.rename({df.columns[0]: "prefecture"})

            file_year, file_week = _extract_year_week(p)
            y = year or file_year
            w = file_week
            if y is None or w is None:
                continue

            if "year" not in df.columns:
                df = df.with_columns(pl.lit(y).alias("year"))
            if "week" not in df.columns:
                df = df.with_columns(pl.lit(w).alias("week"))
            if "date" not in df.columns:
                df = df.with_columns(
                    pl.struct(["year", "week"])
                    .map_elements(
                        lambda x: _iso_week_date(int(x["year"]), int(x["week"])),
                        return_dtype=pl.Date,
                    )
                    .alias("date")
                )

            df = df.select(
                [
                    "prefecture",
                    "year",
                    "week",
                    "date",
                    *[c for c in df.columns if c not in {"prefecture", "year", "week", "date"}],
                ]
            )
            frames.append(df)
        except Exception:
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
    # Bullet specific options:
    week: int | Iterable[int] | None = None,
    lang: Literal["en", "ja"] = "en",
) -> Path | list[Path]:
    """
    Download raw data for a specific year.

    Args:
        name: Dataset name ("sex", "place", "bullet").
        year: Year of the data (e.g., 2023).
        out_dir: Directory to save file. Defaults to system cache.
        overwrite: If True, overwrite existing file.
        week: (Bullet only) Specific week(s) to download.
        lang: (Bullet only) Language for bullet data ("en", "ja").

    Returns:
        Path to the downloaded file (for sex/place) or list of Paths (for bullet).
    """
    config = get_config()
    if out_dir is None:
        base_cache = Path(user_cache_dir("jpinfectpy"))
        if name == "bullet":
            out_dir = base_cache / "raw" / "bullet"
        else:
            out_dir = base_cache / "raw" / "confirmed"

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
        # Return all requested (existing + downloaded)
        downloaded_map = {p.name: p for p in downloaded}
        results: list[Path] = []
        for url in urls:
            fname = Path(url).name
            if fname in existing:
                results.append(existing[fname])
            elif fname in downloaded_map:
                results.append(downloaded_map[fname])
        return results

    else:
        # Confirmed (sex or place)
        type_ = name  # type: ignore
        url = url_confirmed(year, type_)
        filename = f"{year}_{Path(url).name}"
        dest = out_dir / filename

        if dest.exists() and not overwrite:
            return dest

        downloaded = download_urls([url], out_dir, config)
        if not downloaded:
            raise RuntimeError(f"Failed to download {name} data for year {year}")

        # Rename if needed (download_urls uses basename)
        # But we want {year}_{basename}. download_urls returns list of paths.
        actual_file = downloaded[0]
        if actual_file.name != filename:
            # We want to enforce our naming convention if we downloaded to a temp name?
            # actually download_urls saves as verify_urls.py showed.
            # To respect {year}_ prefix, we might need to rename it
            # But download_urls saves with original basename.
            final_dest = out_dir / filename
            if actual_file != final_dest:
                actual_file.rename(final_dest)
                return final_dest

        return actual_file


def read(
    path: Path | str,
    type: DatasetName | None = None,
    *,
    return_type: ReturnType | None = None,
) -> AnyFrame:
    """
    Read a local raw file into a DataFrame.

    Args:
        path: Path to the Excel or CSV file (or directory).
        type: "sex", "place", or "bullet". Inferred from filename if None.
        return_type: "polars" or "pandas".
    """
    path = Path(path)
    if type is None:
        # Infer type
        if path.suffix == ".csv" or (path.is_dir() and list(path.glob("*.csv"))):
            type = "bullet"
        elif "Syu_01" in path.name or "sex" in path.name:
            type = "sex"
        elif "Syu_02" in path.name or "place" in path.name:
            type = "place"
        else:
            # Default fallback?
            raise ValueError("Could not infer dataset type from filename. Please specify 'type'.")

    if type == "bullet":
        df = _read_bullet_pl(path)
    else:
        df = _read_confirmed_pl(path, type=type)

    if resolve_return_type(return_type) == "pandas":
        return to_pandas(df)
    return df
