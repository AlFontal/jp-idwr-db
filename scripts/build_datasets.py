#!/usr/bin/env python3
"""Build parquet datasets and coverage docs for jp_idwr_db."""

import argparse
from datetime import date
from datetime import datetime
import logging
from pathlib import Path

import httpx
import polars as pl

from jp_idwr_db import io
from jp_idwr_db._internal import validation
from jp_idwr_db.utils import PREFECTURE_ISO_MAP, complete_years, iso_weeks_in_year

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Use the ISO calendar for both values so that early-January days that still belong
# to the previous ISO year (e.g. 2027-01-01 is 2026-W53) resolve consistently.
_TODAY_ISO = datetime.now().isocalendar()
CURRENT_YEAR = _TODAY_ISO.year
CURRENT_WEEK = _TODAY_ISO.week
# First year of the preliminary weekly all-case reports (bullet).
BULLET_FIRST_YEAR = 2024
# Annual tables exist from IDWR's start in 1999 (place of infection from 2001).
SEX_FIRST_YEAR = 1999
PLACE_FIRST_YEAR = 2001
SENTINEL_FIRST_YEAR = 1999
# First year of the English cumulative sentinel files (teitenrui).
RAPID_SENTINEL_FIRST_YEAR = 2012
# Early in a new year the first weekly reports are not yet published (~2 week lag).
# During this window an empty current year is expected rather than an error.
NEW_YEAR_GRACE_WEEKS = 4
DATA_DIR = Path(__file__).parent.parent / "data" / "parquet"
DISEASES_MD = Path(__file__).parent.parent / "docs" / "DISEASES.md"


def _year_week_upper_bound(year: int) -> int:
    """Return the latest week to download for a given year."""
    iso_max = iso_weeks_in_year(year)
    if year == CURRENT_YEAR:
        return min(CURRENT_WEEK, iso_max)
    return iso_max


def _split_preserved_years(
    existing_df: pl.DataFrame, name: str
) -> tuple[pl.DataFrame | None, set[int]]:
    """Split existing output into preserved years and years to re-fetch.

    The previous year is only preserved once it reaches its final ISO week. If it
    was last refreshed before its final weeks were published (at the
    December/January rollover), it is downloaded again so its tail is not lost.
    """
    existing_years = {int(year) for year in existing_df["year"].unique().to_list()}
    finished = complete_years(existing_df)
    # Only the previous year can still be receiving late weeks; older years are
    # final even when the source ended early (matches the release preservation guard).
    incomplete_years = sorted(
        year for year in existing_years if year == CURRENT_YEAR - 1 and year not in finished
    )
    preserved_years = sorted(
        year for year in existing_years if year < CURRENT_YEAR and year not in incomplete_years
    )
    for year in incomplete_years:
        logger.warning(
            f"  Existing {name} data for {year} ends before ISO week "
            f"{iso_weeks_in_year(year)}; re-fetching the full year"
        )

    if not preserved_years:
        return None, set()
    logger.info(
        f"  Preserved existing {name} data for years: {preserved_years[0]}-{preserved_years[-1]}"
    )
    return existing_df.filter(pl.col("year").is_in(preserved_years)), set(preserved_years)


def _annual_years(table: io.AnnualTable, first_year: int, known_years: set[int]) -> list[int]:
    """Return the contiguous run of years with an annual table, starting at first_year.

    A year counts as available if it is already built, its table is cached, or the
    server publishes it. The first unavailable year ends the run, so a transient
    network error can only delay a switch to annual data, never skip a year.
    """
    years: list[int] = []
    year = first_year
    while year < CURRENT_YEAR:
        available = (
            year in known_years
            or io.annual_cache_path(table, year).exists()
            or io.annual_table_available(table, year)
        )
        if not available:
            break
        years.append(year)
        year += 1
    return years


def _annual_year_complete(
    df: pl.DataFrame, year: int, name: str, required_diseases: set[str] | None = None
) -> bool:
    """Check that an annual table covers the full week x prefecture x disease grid.

    A year switched to annual data is never re-read, so a partial table (missing
    weeks, prefectures, or disease blocks) must not be accepted; the year then
    stays on preliminary data until a later run. ``required_diseases`` must all be
    present (e.g. the diseases already published from preliminary data).
    """
    first_week = 14 if year == 1999 else 1  # IDWR started in 1999-W14
    weeks = pl.DataFrame({"week": list(range(first_week, iso_weeks_in_year(year) + 1))})
    diseases = set(df["disease"].unique().to_list())
    problems: list[str] = []
    missing_diseases = sorted((required_diseases or set()) - diseases)
    if missing_diseases:
        problems.append(f"missing diseases {missing_diseases[:3]}")
    expected = (
        weeks.join(pl.DataFrame({"disease": sorted(diseases)}), how="cross")
        .join(pl.DataFrame({"prefecture": list(PREFECTURE_ISO_MAP)}), how="cross")
        .with_columns(pl.lit(year, dtype=pl.Int32).alias("year"))
        .filter(io._in_surveillance_window())
    )
    keys = ["year", "week", "prefecture", "disease"]
    observed = df.select(
        pl.col("year").cast(pl.Int32), pl.col("week").cast(pl.Int64), "prefecture", "disease"
    ).unique()
    absent = expected.with_columns(pl.col("week").cast(pl.Int64)).join(
        observed, on=keys, how="anti"
    )
    if absent.height:
        sample = absent.select(["week", "prefecture", "disease"]).head(3).rows()
        problems.append(f"{absent.height} missing week/prefecture/disease cells, e.g. {sample}")
    if problems:
        logger.warning(
            f"  Annual {name} table for {year} is incomplete ({'; '.join(problems)}); "
            "keeping preliminary data for this and later years"
        )
        return False
    return True


def _derive_female(df: pl.DataFrame) -> pl.DataFrame:
    """Add female = total - male rows wherever a table yields total and male only."""
    key_cols = ["prefecture", "year", "week", "date", "disease"]
    if "source" in df.columns:
        key_cols.append("source")
    has_female = df.filter(pl.col("category") == "female").select(key_cols).unique()
    candidates = df.join(has_female, on=key_cols, how="anti")
    if candidates.is_empty():
        return df
    sex_wide = (
        candidates.select([*key_cols, "category", "count"])
        .group_by([*key_cols, "category"])
        .agg(pl.col("count").sum().alias("count"))
        .pivot(values="count", index=key_cols, on="category")
    )
    if not {"total", "male"}.issubset(sex_wide.columns):
        return df
    female_df = (
        sex_wide.filter(pl.col("total").is_not_null() & pl.col("male").is_not_null())
        .with_columns(
            (pl.col("total") - pl.col("male")).cast(pl.Int64, strict=False).alias("count")
        )
        .with_columns(pl.lit("female").alias("category"))
        .select([*key_cols, "category", "count"])
    )
    if female_df.height:
        logger.info(f"  ✓ Derived female rows: {female_df.height:,}")
    return pl.concat([df, female_df], how="diagonal_relaxed")


def _build_annual_confirmed(name: str, table: io.AnnualTable, first_year: int) -> None:
    """Incrementally build an annual confirmed dataset (sex or place of infection).

    Annual tables are final, so years already built are kept and only newly
    published years are added. The file is left untouched when nothing is new.
    """
    out_path = DATA_DIR / f"{name}.parquet"
    existing = pl.read_parquet(out_path) if out_path.exists() else None
    existing_years = set(existing["year"].unique().to_list()) if existing is not None else set()
    years = _annual_years(table, first_year, existing_years)
    if max(years, default=0) < BULLET_FIRST_YEAR - 1:
        # Preliminary reports only start in 2024: a shorter annual run leaves a hole.
        raise RuntimeError(
            f"Annual {table} tables stop at {max(years, default=None)}; "
            f"expected at least {BULLET_FIRST_YEAR - 1} (network error?)"
        )
    new_years = [year for year in years if year not in existing_years]
    if not new_years:
        logger.info(f"  {name}: no new annual tables (latest {max(years) if years else None})")
        return

    frames: list[pl.DataFrame] = []
    for year in new_years:
        try:
            path = io.download(table, year)
            frame = io.read(path, type=table)
        except Exception as e:
            raise RuntimeError(f"Failed to build {table} data for {year}") from e
        totals = frame.filter(pl.col("category") == "total")
        bullet_path = DATA_DIR / "bullet.parquet"
        if bullet_path.exists():
            # English labels differ between the two publications, so compare counts.
            bullet_diseases = (
                pl.scan_parquet(bullet_path)
                .filter(pl.col("year") == year)
                .select(pl.col("disease").n_unique())
                .collect()
                .item()
            )
            if totals["disease"].n_unique() < bullet_diseases:
                logger.warning(
                    f"  Annual {name} table for {year} has fewer diseases than the "
                    "preliminary reports; keeping preliminary data"
                )
                break
        if not _annual_year_complete(totals, year, name):
            if year < BULLET_FIRST_YEAR:
                raise RuntimeError(f"Historical annual {table} table for {year} is incomplete")
            break
        frames.append(frame)
        logger.info(f"  ✓ Loaded {name} {year}")
    if not frames:
        return

    new_df = pl.concat(frames, how="diagonal_relaxed")
    if table == "sex":
        new_df = _derive_female(new_df)
    columns = ["prefecture", "year", "week", "date", "count", "category", "disease", "source"]
    full_df = pl.concat(
        [*([existing] if existing is not None else []), new_df.select(columns)],
        how="diagonal_relaxed",
    ).select(columns)
    full_df = _sort_for_output(full_df)
    _validate_dataset_output(name, full_df)
    full_df.write_parquet(out_path)
    logger.info(f"Saved {out_path.name} ({full_df.height} rows; added {new_years})")


def _allow_unpublished_current_year(year: int, loaded_frames: list[pl.DataFrame]) -> bool:
    """Return whether an empty current year is expected at the start of a new year."""
    return year == CURRENT_YEAR and CURRENT_WEEK <= NEW_YEAR_GRACE_WEEKS and bool(loaded_frames)


def _format_number(value: int | float | None) -> str:
    """Format case totals for markdown output."""
    if value is None:
        return "0"
    as_float = float(value)
    if as_float.is_integer():
        return f"{int(as_float):,}"
    return f"{as_float:,.2f}"


def _sort_for_output(df: pl.DataFrame) -> pl.DataFrame:
    """Sort output dataframes consistently for stable parquet ordering.

    Primary intent is chronological ordering with prefecture/category grouping:
    date -> prefecture -> category, with additional keys for deterministic ties.
    """
    sort_keys: list[str] = []
    for key in ["date", "year", "week", "prefecture", "category", "disease", "source"]:
        if key in df.columns:
            sort_keys.append(key)

    if not sort_keys:
        return df
    return df.sort(sort_keys, nulls_last=True)


def _validate_dataset_output(name: str, df: pl.DataFrame) -> None:
    """Validate a built dataset before it is written to disk."""
    logger.info(f"Validating {name} dataset...")
    validation.validate_schema(df)
    identifier_columns = ["prefecture", "year", "week", "disease"]
    identifier_columns.extend(column for column in ["category", "source"] if column in df.columns)
    validation.validate_required_values(df, identifier_columns)
    validation.validate_allowed_values(df, "prefecture", set(PREFECTURE_ISO_MAP))
    validation.validate_clean_disease_names(df)
    expected_sources = {
        "sex_prefecture": {"Confirmed cases"},
        "place_prefecture": {"Confirmed cases"},
        "bullet": {"All-case reporting"},
        "sentinel": {"Sentinel surveillance"},
        "unified": {"Confirmed cases", "All-case reporting", "Sentinel surveillance"},
    }
    validation.validate_allowed_values(df, "source", expected_sources[name])
    expected_categories = {
        "sex_prefecture": {"total", "male", "female"},
        "place_prefecture": {"total", "japan", "others", "unknown"},
        "unified": {"total"},
    }
    if name in expected_categories:
        validation.validate_allowed_values(df, "category", expected_categories[name])
    validation.validate_no_duplicates(df)
    validation.validate_date_ranges(df)
    validation.validate_iso_week_start_dates(df)
    validation.validate_non_negative_counts(df)
    validation.validate_prefecture_coverage(df)
    if name in {"sentinel", "unified"}:
        validation.validate_sentinel_count_status(df)
    if name == "sentinel":
        validation.validate_max_null_rate(df, "count", max_rate=0.25, group_by=["year"])
    elif name == "unified":
        sentinel_df = df.filter(pl.col("source") == "Sentinel surveillance")
        validation.validate_max_null_rate(sentinel_df, "count", max_rate=0.25, group_by=["year"])
    logger.info(f"  ✓ {name} validation passed")


def _write_diseases_markdown(unified_df: pl.DataFrame) -> None:
    """Write disease temporal coverage and totals to DISEASES.md."""
    summary = (
        unified_df.group_by("disease")
        .agg(
            [
                pl.col("year").min().alias("first_year"),
                pl.col("year").max().alias("last_year"),
                pl.col("week")
                .filter(pl.col("year") == pl.col("year").min())
                .min()
                .alias("first_week"),
                pl.col("week")
                .filter(pl.col("year") == pl.col("year").max())
                .max()
                .alias("last_week"),
                pl.col("source").drop_nulls().unique().sort().alias("sources"),
                pl.col("count").fill_null(0).sum().alias("total_cases"),
                pl.len().alias("rows"),
            ]
        )
        .sort("disease")
    )

    lines = [
        "# Disease Coverage in Unified Dataset",
        "",
        f"Coverage summary generated from `data/parquet/unified.parquet` (snapshot: {date.today().isoformat()}).",
        "",
        f"- Total diseases: **{summary.height}**",
        f"- Year span: **{int(unified_df['year'].min())}-{int(unified_df['year'].max())}**",
        "",
        "| Disease | First (Year-Week) | Last (Year-Week) | Sources | Total Cases | Rows |",
        "| --- | --- | --- | --- | ---: | ---: |",
    ]

    for row in summary.iter_rows(named=True):
        first = f"{int(row['first_year'])}-W{int(row['first_week']):02d}"
        last = f"{int(row['last_year'])}-W{int(row['last_week']):02d}"
        sources = ", ".join(row["sources"]) if row["sources"] else ""
        total_cases = _format_number(row["total_cases"])
        lines.append(
            f"| {row['disease']} | {first} | {last} | {sources} | {total_cases} | {int(row['rows']):,} |"
        )

    DISEASES_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info(f"Wrote disease coverage report to {DISEASES_MD.name}")


def build_sex() -> None:
    logger.info("Building sex_prefecture dataset...")
    _build_annual_confirmed("sex_prefecture", "sex", SEX_FIRST_YEAR)


def build_place() -> None:
    logger.info("\nBuilding place_prefecture dataset...")
    _build_annual_confirmed("place_prefecture", "place", PLACE_FIRST_YEAR)


def build_bullet() -> None:
    logger.info(f"\nBuilding bullet dataset ({BULLET_FIRST_YEAR}-{CURRENT_YEAR})...")
    out_path = DATA_DIR / "bullet.parquet"
    all_years = list(range(BULLET_FIRST_YEAR, CURRENT_YEAR + 1))
    dfs: list[pl.DataFrame] = []
    preserved_years: set[int] = set()

    if out_path.exists():
        preserved_df, preserved_years = _split_preserved_years(pl.read_parquet(out_path), "bullet")
        if preserved_df is not None:
            dfs.append(preserved_df)

    years = [year for year in all_years if year not in preserved_years]
    total_weeks = 0

    for year in years:
        final_week = _year_week_upper_bound(year)
        try:
            logger.info(f"  Processing year {year}...")
            paths = io.download("bullet", year, week=range(1, final_week + 1))
            if not paths:
                if _allow_unpublished_current_year(year, dfs):
                    logger.info(f"  No bullet reports published yet for {year}; skipping")
                    continue
                raise RuntimeError(f"No bullet data found for {year}")

            path_list = paths if isinstance(paths, list) else [paths]
            year_dfs = []
            for i, p in enumerate(path_list, 1):
                df = io.read(p, type="bullet")

                additions: list[pl.Expr] = [pl.lit("All-case reporting").alias("source")]
                if "year" not in df.columns:
                    additions.append(pl.lit(year).alias("year"))

                # Filter out empty disease names (data quality issue)
                df = df.filter(pl.col("disease") != "")

                if "date" not in df.columns and "year" in df.columns and "week" in df.columns:
                    additions.append(
                        pl.struct(["year", "week"])
                        .map_elements(
                            lambda value: date.fromisocalendar(
                                int(value["year"]), int(value["week"]), 1
                            ),
                            return_dtype=pl.Date,
                        )
                        .alias("date")
                    )

                if additions:
                    df = df.with_columns(additions)

                year_dfs.append(df)
                # Log progress on last week
                if i == len(path_list):
                    logger.info(f"    Loaded weeks 1-{i} for {year}")

            dfs.extend(year_dfs)
            total_weeks += len(path_list)
            logger.info(f"  ✓ Completed year {year}: {len(path_list)} weeks loaded")
        except Exception as e:
            raise RuntimeError(f"Failed to build bullet data for {year}") from e

    if dfs:
        full_df = pl.concat(dfs, how="diagonal_relaxed")
        full_df = io._normalize_disease_column(full_df, "disease")
        full_df = _sort_for_output(full_df)
        _validate_dataset_output("bullet", full_df)
        full_df.write_parquet(out_path)
        logger.info(f"Saved to {out_path.name} ({full_df.height} rows, {total_weeks} weeks total)")
        logger.info(f"  Schema: {full_df.columns}")
    else:
        raise RuntimeError("No bullet data was loaded")


class IncompleteAnnualTableError(Exception):
    """An annual table is published but does not cover the whole year yet."""


# Diseases reported by pediatric sentinel clinics (小児科定点); they share one
# per-sentinel denominator per prefecture and week.
PEDIATRIC_SENTINEL_DISEASES = {
    "Respiratory syncytial virus infection",
    "Pharyngoconjunctival fever",
    "Group A streptococcal pharyngitis",
    "Infectious gastroenteritis",
    "Chickenpox",
    "Hand, foot and mouth disease",
    "Erythema infection",
    "Exanthem subitum",
    "Herpangina",
    "Mumps",
}
RSV = "Respiratory syncytial virus infection"


def _read_annual_sentinel_year(year: int) -> pl.DataFrame:
    """Read one year of final weekly sentinel counts (and rates) from annual tables."""
    keys = ["prefecture", "year", "week"]
    counts = io.read_annual_sentinel(io.download_annual("sentinel", year), year)
    if io.url_annual(year, "sentinel_rate") is None:
        counts = counts.with_columns(pl.lit(None, dtype=pl.Float64).alias("per_sentinel"))
        not_reported = pl.lit(False)
    else:
        rates = io.read_annual_sentinel(
            io.download_annual("sentinel_rate", year), year, value_name="per_sentinel"
        )
        # Rates must cover every counted disease (RSV had no rate column before 2018).
        required = set(counts["disease"].unique().to_list()) - {RSV}
        if not _annual_year_complete(rates, year, "sentinel rate", required_diseases=required):
            raise IncompleteAnnualTableError(f"sentinel rate table for {year}")
        counts = counts.join(
            rates.select([*keys, "disease", "per_sentinel"]),
            on=[*keys, "disease"],
            how="left",
        )
        # A prefecture-week with blank rates everywhere had no reporting sentinel
        # (e.g. Fukushima after the March 2011 earthquake); its zeros are not data.
        silent = (
            counts.filter(pl.col("disease") != RSV)
            .group_by(keys)
            .agg(
                pl.col("per_sentinel").is_null().all().alias("_no_rates"),
                (pl.col("count").fill_null(0) == 0).all().alias("_all_zero"),
            )
            .filter(pl.col("_no_rates") & pl.col("_all_zero"))
            .select([*keys, pl.lit(True).alias("_not_reported")])
        )
        counts = counts.join(silent, on=keys, how="left")
        not_reported = pl.col("_not_reported").fill_null(False)

        # The annual rate tables omit RSV before 2018; the weekly reports published
        # it with the pediatric denominator, which the other pediatric rates give.
        sites = (
            counts.filter(
                pl.col("disease").is_in(list(PEDIATRIC_SENTINEL_DISEASES - {RSV}))
                & (pl.col("count") > 0)
                & (pl.col("per_sentinel") > 0)
            )
            .group_by(keys)
            .agg((pl.col("count") / pl.col("per_sentinel")).median().alias("_pediatric_sites"))
        )
        counts = counts.join(sites, on=keys, how="left").with_columns(
            pl.when((pl.col("disease") == RSV) & pl.col("per_sentinel").is_null())
            .then(
                pl.when(pl.col("count") == 0)
                .then(0.0)
                .otherwise(pl.col("count") / pl.col("_pediatric_sites"))
            )
            .otherwise(pl.col("per_sentinel"))
            .alias("per_sentinel")
        )

    # "…" cells and silent prefecture-weeks are unknown; they keep an explanation.
    unknown = pl.col("count").is_null() | not_reported
    out = counts.with_columns(
        pl.lit("Sentinel surveillance").alias("source"),
        pl.when(unknown).then(pl.lit("missing")).otherwise(pl.lit("annual")).alias("count_status"),
        pl.when(unknown).then(None).otherwise(pl.col("count")).alias("count"),
        pl.when(unknown).then(None).otherwise(pl.col("per_sentinel")).alias("per_sentinel"),
    )
    return out.select([c for c in out.columns if not c.startswith("_")])


def build_sentinel(*, full_rebuild: bool = False, source_dir: Path | None = None) -> None:
    """Build the sentinel dataset.

    Years with a published annual table use its final weekly counts
    (``count_status = annual``). Later years are derived from the preliminary
    cumulative teitenrui files. A preliminary year is replaced as soon as its
    annual table appears.
    """
    logger.info(f"\nBuilding sentinel dataset ({SENTINEL_FIRST_YEAR}-{CURRENT_YEAR})...")
    out_path = DATA_DIR / "sentinel.parquet"
    existing = pl.read_parquet(out_path) if out_path.exists() and not full_rebuild else None
    existing_annual = (
        set(existing.filter(pl.col("count_status") == "annual")["year"].unique().to_list())
        if existing is not None
        else set()
    )
    annual_years = _annual_years("sentinel", SENTINEL_FIRST_YEAR, existing_annual)
    dfs: list[pl.DataFrame] = []
    if existing is not None and existing_annual:
        dfs.append(existing.filter(pl.col("year").is_in(list(existing_annual))))
    accepted: list[int] = []
    for year in annual_years:
        if year in existing_annual:
            accepted.append(year)
            continue
        try:
            annual_df = _read_annual_sentinel_year(year)
        except httpx.HTTPStatusError as e:
            if e.response.status_code != 404:
                raise RuntimeError(f"Failed to build sentinel data for {year}") from e
            # Counts published but the rate table not yet: switch on a later run.
            logger.warning(f"  Annual sentinel rate table for {year} not published yet")
            break
        except IncompleteAnnualTableError:
            break
        except Exception as e:
            raise RuntimeError(f"Failed to build sentinel data for {year}") from e
        preliminary_diseases = (
            set(existing.filter(pl.col("year") == year)["disease"].unique().to_list())
            if existing is not None
            else set()
        )
        if not _annual_year_complete(annual_df, year, "sentinel", preliminary_diseases):
            break
        dfs.append(annual_df)
        accepted.append(year)
        logger.info(f"  ✓ Loaded annual sentinel table for {year}")

    if SENTINEL_FIRST_YEAR < RAPID_SENTINEL_FIRST_YEAR and (
        max(accepted, default=0) < RAPID_SENTINEL_FIRST_YEAR - 1
    ):
        # Preliminary files only start in 2012: a shorter annual run leaves a hole.
        raise RuntimeError(
            f"Annual sentinel tables stop at {max(accepted, default=None)}; "
            f"expected at least {RAPID_SENTINEL_FIRST_YEAR - 1} (network error?)"
        )
    rapid_start = max(RAPID_SENTINEL_FIRST_YEAR, max(accepted, default=0) + 1)
    all_years = list(range(rapid_start, CURRENT_YEAR + 1))
    preserved_years: set[int] = set()
    if existing is not None:
        rapid_existing = existing.filter(pl.col("year") >= rapid_start)
        if rapid_existing.height:
            preserved_df, preserved_years = _split_preserved_years(rapid_existing, "sentinel")
            if preserved_df is not None:
                dfs.append(preserved_df)

    years = [year for year in all_years if year not in preserved_years]
    total_weeks = 0

    for year in years:
        final_week = _year_week_upper_bound(year)
        try:
            logger.info(f"  Processing year {year}...")
            if source_dir is None:
                paths = io.download("sentinel", year, week=range(1, final_week + 1))
            else:
                year_dir = source_dir / str(year)
                paths = [
                    path
                    for path in sorted(year_dir.glob("teitenrui*.csv"))
                    if (year_week := io._extract_year_week(path))[1] is not None
                    and int(year_week[1]) <= final_week
                ]
            if not paths:
                if _allow_unpublished_current_year(year, dfs):
                    logger.info(f"  No sentinel reports published yet for {year}; skipping")
                    continue
                raise RuntimeError(f"No sentinel data found for {year}")

            path_list = paths if isinstance(paths, list) else [paths]
            year_dfs = []
            for i, p in enumerate(path_list, 1):
                # Read English sentinel data from /rapid/ endpoint
                df = io._read_sentinel_en_pl(p)

                # Filter out empty disease names (data quality issue)
                df = df.filter(pl.col("disease") != "")

                # Add year and source columns for consistency with historical data
                df = df.with_columns(
                    [
                        pl.lit(year).alias("year"),
                        pl.lit("Sentinel surveillance").alias("source"),
                    ]
                )

                # Add date column (week start date)
                df = df.with_columns(
                    [
                        pl.struct(["year", "week"])
                        .map_elements(
                            lambda value: date.fromisocalendar(
                                int(value["year"]), int(value["week"]), 1
                            ),
                            return_dtype=pl.Date,
                        )
                        .alias("date")
                    ]
                )

                year_dfs.append(df)
                # Log progress on last week
                if i == len(path_list):
                    logger.info(f"    Loaded weeks 1-{i} for {year}")

            year_df = pl.concat(year_dfs, how="diagonal_relaxed")
            year_df = io._sentinel_cumulative_to_weekly(year_df)
            dfs.append(year_df)
            total_weeks += len(path_list)
            logger.info(f"  ✓ Completed year {year}: {len(path_list)} weeks loaded")
        except Exception as e:
            raise RuntimeError(f"Failed to build sentinel data for {year}") from e

    if dfs:
        full_df = pl.concat(dfs, how="diagonal_relaxed")
        full_df = io._normalize_disease_column(full_df, "disease")
        full_df = _sort_for_output(full_df)
        _validate_dataset_output("sentinel", full_df)
        full_df.write_parquet(out_path)
        logger.info(f"Saved to {out_path.name} ({full_df.height} rows, {total_weeks} weeks total)")
        logger.info(f"  Schema: {full_df.columns}")
    else:
        raise RuntimeError("No sentinel data was loaded")


def build_unified() -> None:
    """Build unified parquet dataset combining all sources with smart merge.

    This creates a single unified.parquet file that combines:
    - Final annual confirmed totals (sex tables) for every year they cover
    - Preliminary bullet reports only for later years
    - Sentinel data (annual where published, preliminary after)

    Uses smart_merge() to prefer confirmed data and only include sentinel rows
    for disease-years without confirmed coverage.
    """
    logger.info("\n" + "=" * 60)
    logger.info("Building unified dataset...")
    logger.info("=" * 60)

    # 1. Load modern bullet (zensu) data first to determine what years we have
    bullet_path = DATA_DIR / "bullet.parquet"
    if bullet_path.exists():
        logger.info(f"Loading modern bullet data from {bullet_path.name}...")
        bullet_df = pl.read_parquet(bullet_path)
        logger.info(f"  ✓ Loaded {bullet_df.height:,} rows")
        zensu_df = bullet_df
    else:
        raise FileNotFoundError(f"Bullet data file not found: {bullet_path}")

    # 2. Load sentinel (teiten) data
    sentinel_path = DATA_DIR / "sentinel.parquet"
    if sentinel_path.exists():
        logger.info(f"Loading modern sentinel data from {sentinel_path.name}...")
        sentinel_df = pl.read_parquet(sentinel_path)
        logger.info(f"  ✓ Loaded {sentinel_df.height:,} rows")
        teiten_df = sentinel_df
    else:
        raise FileNotFoundError(f"Sentinel data file not found: {sentinel_path}")

    # 3. Load final annual confirmed data (sex tables, totals only). Years with an
    # annual table replace the preliminary bullet reports for the same year.
    sex_path = DATA_DIR / "sex_prefecture.parquet"
    if sex_path.exists():
        logger.info(f"\nLoading annual confirmed data from {sex_path.name}...")
        sex_df = pl.read_parquet(sex_path)
        if "category" in sex_df.columns:
            sex_df = sex_df.filter(pl.col("category") == "total")
        annual_years = set(sex_df["year"].unique().to_list())
        zensu_df = zensu_df.filter(~pl.col("year").is_in(list(annual_years)))
        logger.info(
            f"  ✓ Loaded {sex_df.height:,} rows; bullet used for years "
            f"{sorted(set(zensu_df['year'].unique().to_list()))}"
        )
    else:
        raise FileNotFoundError(f"Sex data file not found: {sex_path}")

    # 4. Combine historical and modern confirmed totals, then add sentinel rows only
    # for disease-years that confirmed data does not cover. Merging against all
    # confirmed years (not just bullet) keeps sentinel history for diseases that
    # later became notifiable without duplicating historical confirmed rows.
    logger.info("\nApplying smart merge (prefer confirmed, sentinel-only disease-years)...")
    confirmed_df = pl.concat([sex_df, zensu_df], how="diagonal_relaxed")
    unified_df = validation.smart_merge(confirmed_df, teiten_df)
    logger.info(
        f"  ✓ Merged to {unified_df.height:,} rows "
        f"(confirmed: {confirmed_df.height:,}, "
        f"sentinel kept: {unified_df.height - confirmed_df.height:,})"
    )

    # Fill modern rows with category=total for a consistent schema.
    if "category" in unified_df.columns:
        unified_df = unified_df.with_columns(
            pl.when(pl.col("category").is_null())
            .then(pl.lit("total"))
            .otherwise(pl.col("category"))
            .alias("category")
        )

    # 6. Validate and save unified dataset
    out_path = DATA_DIR / "unified.parquet"
    logger.info(f"\nSaving unified dataset to {out_path.name}...")
    unified_df = _sort_for_output(unified_df)
    _validate_dataset_output("unified", unified_df)
    unified_df.write_parquet(out_path)
    _write_diseases_markdown(unified_df)

    # Summary statistics
    logger.info("\n" + "=" * 60)
    logger.info("UNIFIED DATASET SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total rows: {unified_df.height:,}")
    logger.info(f"Columns: {', '.join(unified_df.columns)}")
    logger.info(f"Date range: {unified_df['year'].min()}-{unified_df['year'].max()}")
    logger.info(f"Unique diseases: {unified_df['disease'].n_unique()}")
    logger.info(f"Unique prefectures: {unified_df['prefecture'].n_unique()}")
    logger.info(f"File size: {out_path.stat().st_size / 1024 / 1024:.2f} MB")
    logger.info(f"Saved to: {out_path}")
    logger.info("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build bundled datasets for jp_idwr_db")
    parser.add_argument(
        "--sex-only", action="store_true", help="Build only the sex_prefecture dataset"
    )
    parser.add_argument(
        "--place-only", action="store_true", help="Build only the place_prefecture dataset"
    )
    parser.add_argument("--bullet-only", action="store_true", help="Build only the bullet dataset")
    parser.add_argument(
        "--sentinel-only", action="store_true", help="Build only the sentinel dataset"
    )
    parser.add_argument(
        "--sentinel-full-rebuild",
        action="store_true",
        help="Rebuild sentinel history from raw cumulative source files",
    )
    parser.add_argument(
        "--sentinel-source-dir",
        type=Path,
        help="Use an existing raw sentinel directory organized by year",
    )
    parser.add_argument(
        "--unified-only",
        action="store_true",
        help="Build only the unified dataset (from existing files)",
    )

    args = parser.parse_args()

    if args.sentinel_source_dir is not None and not args.sentinel_full_rebuild:
        parser.error("--sentinel-source-dir requires --sentinel-full-rebuild")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # If no specific dataset is requested, build all
    build_all = not (
        args.sex_only
        or args.place_only
        or args.bullet_only
        or args.sentinel_only
        or args.sentinel_full_rebuild
        or args.unified_only
    )

    if build_all or args.sex_only:
        build_sex()

    if build_all or args.place_only:
        build_place()

    if build_all or args.bullet_only:
        build_bullet()

    if build_all or args.sentinel_only or args.sentinel_full_rebuild:
        build_sentinel(
            full_rebuild=args.sentinel_full_rebuild,
            source_dir=args.sentinel_source_dir,
        )

    if build_all or args.unified_only:
        build_unified()


if __name__ == "__main__":
    main()
