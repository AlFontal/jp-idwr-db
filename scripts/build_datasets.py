#!/usr/bin/env python3
"""Build parquet datasets and coverage docs for jp_idwr_db."""

import argparse
from datetime import date
from datetime import datetime
import logging
from pathlib import Path

import polars as pl

from jp_idwr_db import io
from jp_idwr_db._internal import download, read, validation

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

CURRENT_YEAR = datetime.now().year
CURRENT_WEEK = datetime.now().isocalendar().week
LAST_HISTORICAL_YEAR = 2023
DATA_DIR = Path(__file__).parent.parent / "data" / "parquet"
DISEASES_MD = Path(__file__).parent.parent / "docs" / "DISEASES.md"


def _max_iso_week(year: int) -> int:
    """Return the number of ISO weeks in a year (52 or 53)."""
    return date(year, 12, 28).isocalendar().week


def _year_week_upper_bound(year: int) -> int:
    """Return the latest week to download for a given year."""
    iso_max = _max_iso_week(year)
    if year == CURRENT_YEAR:
        return min(CURRENT_WEEK, iso_max)
    return iso_max


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


def build_sex():
    logger.info("Building sex_prefecture dataset...")
    years = range(1999, LAST_HISTORICAL_YEAR + 1)
    dfs = []
    success_count = 0
    fail_count = 0

    for year in years:
        try:
            path = download.download("sex", year)
            df = read.read(path, type="sex")
            dfs.append(df)
            success_count += 1
            logger.info(f"  ✓ Loaded year {year} ({df.height} rows)")
        except Exception as e:
            fail_count += 1
            logger.warning(f"  ✗ Failed year {year}: {e}")

    if dfs:
        full_df = pl.concat(dfs, how="diagonal_relaxed")

        # Some source years only provide total + male. Derive female when possible.
        categories = set(full_df["category"].drop_nulls().unique().to_list())
        if "female" not in categories and {"total", "male"}.issubset(categories):
            key_cols = ["prefecture", "year", "week", "date", "disease"]
            if "source" in full_df.columns:
                key_cols.append("source")

            sex_wide = (
                full_df.select(key_cols + ["category", "count"])
                .group_by(key_cols + ["category"])
                .agg(pl.col("count").sum().alias("count"))
                .pivot(values="count", index=key_cols, on="category")
            )

            female_df = (
                sex_wide.filter(pl.col("total").is_not_null() & pl.col("male").is_not_null())
                .with_columns(
                    (pl.col("total") - pl.col("male")).cast(pl.Int64, strict=False).alias("count")
                )
                .with_columns(pl.lit("female").alias("category"))
                .select(key_cols + ["category", "count"])
            )

            full_df = pl.concat([full_df, female_df], how="diagonal_relaxed")
            logger.info(f"  ✓ Derived female rows: {female_df.height:,}")

        out_path = DATA_DIR / "sex_prefecture.parquet"
        full_df = _sort_for_output(full_df)
        full_df.write_parquet(out_path)
        logger.info(f"Saved to {out_path.name} ({full_df.height} rows, {success_count} years)")

    if fail_count > 0:
        logger.warning(f"Failed to load {fail_count} year(s)")


def build_place():
    logger.info("\nBuilding place_prefecture dataset...")
    years = range(2001, LAST_HISTORICAL_YEAR + 1)
    dfs = []
    success_count = 0
    fail_count = 0

    for year in years:
        try:
            path = download.download("place", year)
            df = read.read(path, type="place")
            dfs.append(df)
            success_count += 1
            logger.info(f"  ✓ Loaded year {year} ({df.height} rows)")
        except Exception as e:
            fail_count += 1
            logger.warning(f"  ✗ Failed year {year}: {e}")

    if dfs:
        full_df = pl.concat(dfs, how="diagonal_relaxed")
        full_df = _sort_for_output(full_df)
        out_path = DATA_DIR / "place_prefecture.parquet"
        full_df.write_parquet(out_path)
        logger.info(f"Saved to {out_path.name} ({full_df.height} rows, {success_count} years)")

    if fail_count > 0:
        logger.warning(f"Failed to load {fail_count} year(s)")


def build_bullet():
    logger.info(f"\nBuilding bullet dataset ({LAST_HISTORICAL_YEAR + 1}-{CURRENT_YEAR})...")
    # Fetch recent years
    years = range(LAST_HISTORICAL_YEAR + 1, CURRENT_YEAR + 1)
    dfs = []
    total_weeks = 0

    for year in years:
        final_week = _year_week_upper_bound(year)
        try:
            logger.info(f"  Processing year {year}...")
            paths = download.download("bullet", year, week=range(1, final_week + 1))
            if not paths:
                logger.warning(f"    No data found for year {year}")
                continue

            if isinstance(paths, list):
                year_dfs = []
                for i, p in enumerate(paths, 1):
                    df = read.read(p, type="bullet")

                    # Add year and source columns for consistency with historical data
                    df = df.with_columns(
                        [
                            pl.lit(year).alias("year"),
                            pl.lit("All-case reporting").alias("source"),
                        ]
                    )

                    # Filter out empty disease names (data quality issue)
                    df = df.filter(pl.col("disease") != "")

                    # Add date column (week start date)
                    # Calculate ISO week start date
                    df = df.with_columns(
                        [
                            pl.concat_str(
                                [
                                    pl.col("year").cast(pl.Utf8),
                                    pl.lit("-W"),
                                    pl.col("week").cast(pl.Utf8).str.zfill(2),
                                    pl.lit("-1"),  # Monday
                                ]
                            )
                            .str.strptime(pl.Date, "%Y-W%W-%w")
                            .alias("date")
                        ]
                    )

                    year_dfs.append(df)
                    # Log progress on last week
                    if i == len(paths):
                        logger.info(f"    Loaded weeks 1-{i} for {year}")

                dfs.extend(year_dfs)
                total_weeks += len(paths)
                logger.info(f"  ✓ Completed year {year}: {len(paths)} weeks loaded")
        except Exception as e:
            logger.error(f"  ✗ Failed year {year}: {e}")

    if dfs:
        full_df = pl.concat(dfs, how="diagonal_relaxed")
        full_df = _sort_for_output(full_df)
        out_path = DATA_DIR / "bullet.parquet"
        full_df.write_parquet(out_path)
        logger.info(f"Saved to {out_path.name} ({full_df.height} rows, {total_weeks} weeks total)")
        logger.info(f"  Schema: {full_df.columns}")
    else:
        logger.warning("No bullet data was loaded")


def build_sentinel():
    logger.info(f"\nBuilding sentinel dataset (1999-{CURRENT_YEAR})...")
    # Sentinel URL patterns are available historically via data-e archives.
    start_year = 1999
    years = range(start_year, CURRENT_YEAR + 1)
    dfs = []
    total_weeks = 0

    for year in years:
        final_week = _year_week_upper_bound(year)
        try:
            logger.info(f"  Processing year {year}...")
            paths = download.download("sentinel", year, week=range(1, final_week + 1))
            if not paths:
                logger.warning(f"    No data found for year {year}")
                continue

            if isinstance(paths, list):
                year_dfs = []
                for i, p in enumerate(paths, 1):
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
                            pl.concat_str(
                                [
                                    pl.col("year").cast(pl.Utf8),
                                    pl.lit("-W"),
                                    pl.col("week").cast(pl.Utf8).str.zfill(2),
                                    pl.lit("-1"),  # Monday
                                ]
                            )
                            .str.strptime(pl.Date, "%Y-W%W-%w")
                            .alias("date")
                        ]
                    )

                    year_dfs.append(df)
                    # Log progress on last week
                    if i == len(paths):
                        logger.info(f"    Loaded weeks 1-{i} for {year}")

                dfs.extend(year_dfs)
                total_weeks += len(paths)
                logger.info(f"  ✓ Completed year {year}: {len(paths)} weeks loaded")
        except Exception as e:
            logger.error(f"  ✗ Failed year {year}: {e}")

    if dfs:
        full_df = pl.concat(dfs, how="diagonal_relaxed")
        # teitenrui files provide cumulative year-to-date counts; convert to weekly incidence.
        full_df = io._sentinel_cumulative_to_weekly(full_df)
        full_df = _sort_for_output(full_df)
        out_path = DATA_DIR / "sentinel.parquet"
        full_df.write_parquet(out_path)
        logger.info(f"Saved to {out_path.name} ({full_df.height} rows, {total_weeks} weeks total)")
        logger.info(f"  Schema: {full_df.columns}")
    else:
        logger.warning("No sentinel data was loaded")


def build_unified():
    """Build unified parquet dataset combining all sources with smart merge.

    This creates a single unified.parquet file that combines:
    - Historical sex data (excluding years with modern zensu data)
    - Modern bullet/sentinel data

    Uses smart_merge() to prefer confirmed (zensu) data and only include
    sentinel-exclusive diseases from teiten. Also deduplicates by preferring
    modern data over historical data for overlapping years.
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
        modern_years = set(zensu_df["year"].unique())
    else:
        logger.warning(f"  ! Bullet data file not found: {bullet_path}")
        zensu_df = None
        modern_years = set()

    # 2. Load sentinel (teiten) data
    sentinel_path = DATA_DIR / "sentinel.parquet"
    if sentinel_path.exists():
        logger.info(f"Loading modern sentinel data from {sentinel_path.name}...")
        sentinel_df = pl.read_parquet(sentinel_path)
        logger.info(f"  ✓ Loaded {sentinel_df.height:,} rows")
        teiten_df = sentinel_df
    else:
        logger.warning(f"  ! Sentinel data file not found: {sentinel_path}")
        teiten_df = None

    all_dfs = []

    # 3. Load historical sex data.
    # Exclude only years covered by modern zensu/bullet data.
    # Do NOT exclude years only present in sentinel, otherwise historical confirmed
    # coverage for those years would be dropped.
    sex_path = DATA_DIR / "sex_prefecture.parquet"
    if sex_path.exists():
        logger.info(f"\nLoading historical sex data from {sex_path.name}...")
        sex_df = pl.read_parquet(sex_path)
        if modern_years:
            sex_df = sex_df.filter(~pl.col("year").is_in(list(modern_years)))
        if "category" in sex_df.columns:
            sex_df = sex_df.filter(pl.col("category") == "total")
        logger.info(
            f"  ✓ Loaded {sex_df.height:,} rows "
            f"(total-only; excluded modern years: {sorted(list(modern_years)) if modern_years else 'none'})"
        )
        all_dfs.append(sex_df)
    else:
        logger.warning(f"  ! Sex data file not found: {sex_path}")

    # 4. Smart merge modern data (prefer zensu, only sentinel-exclusive from teiten)
    if zensu_df is not None and teiten_df is not None:
        logger.info("\nApplying smart merge (prefer confirmed, sentinel-only from teiten)...")
        merged_modern = validation.smart_merge(zensu_df, teiten_df)
        logger.info(f"  ✓ Merged to {merged_modern.height:,} rows")
        logger.info(
            f"    (zensu: {zensu_df.height:,}, teiten filtered: {merged_modern.height - zensu_df.height:,})"
        )
        all_dfs.append(merged_modern)
    elif zensu_df is not None:
        logger.info("\nOnly zensu data available (no sentinel data to merge)")
        all_dfs.append(zensu_df)
    elif teiten_df is not None:
        logger.info("\nOnly sentinel data available (no zensu data to merge)")
        all_dfs.append(teiten_df)

    # 5. Combine all dataframes
    if not all_dfs:
        logger.error("No data files found! Cannot build unified dataset.")
        return

    logger.info(f"\nCombining {len(all_dfs)} datasets...")
    unified_df = pl.concat(all_dfs, how="diagonal_relaxed")
    logger.info(f"  ✓ Combined to {unified_df.height:,} total rows")

    # Fill modern rows with category=total for a consistent schema.
    if "category" in unified_df.columns:
        unified_df = unified_df.with_columns(
            pl.when(pl.col("category").is_null())
            .then(pl.lit("total"))
            .otherwise(pl.col("category"))
            .alias("category")
        )

    # 5.5. Deduplicate - the source data itself may have duplicates
    logger.info("\nDeduplicating records...")
    dedup_keys = ["prefecture", "year", "week", "disease", "category"]

    rows_before = unified_df.height
    unified_df = unified_df.unique(subset=dedup_keys, keep="first")
    rows_after = unified_df.height
    rows_removed = rows_before - rows_after

    if rows_removed > 0:
        logger.info(f"  ✓ Removed {rows_removed:,} duplicate rows")
        logger.info(f"  ✓ Deduplicated to {rows_after:,} unique rows")
    else:
        logger.info(f"  ✓ No duplicates found")

    # 6. Validate schema
    logger.info("\nValidating schema...")
    try:
        validation.validate_schema(unified_df)
        logger.info("  ✓ Schema validation passed")
    except ValueError as e:
        logger.error(f"  ✗ Schema validation failed: {e}")
        return

    # 7. Validate no duplicates
    logger.info("Checking for duplicates...")
    try:
        validation.validate_no_duplicates(unified_df)
        logger.info("  ✓ No duplicates found")
    except ValueError as e:
        logger.error(f"  ✗ Duplicate validation failed: {e}")
        return

    # 8. Validate date ranges
    logger.info("Validating date ranges...")
    try:
        validation.validate_date_ranges(unified_df)
        logger.info("  ✓ Date range validation passed")
    except ValueError as e:
        logger.error(f"  ✗ Date range validation failed: {e}")
        return

    # 9. Save unified dataset
    out_path = DATA_DIR / "unified.parquet"
    logger.info(f"\nSaving unified dataset to {out_path.name}...")
    unified_df = _sort_for_output(unified_df)
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


def main():
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
        "--unified-only",
        action="store_true",
        help="Build only the unified dataset (from existing files)",
    )

    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # If no specific dataset is requested, build all
    build_all = not (
        args.sex_only
        or args.place_only
        or args.bullet_only
        or args.sentinel_only
        or args.unified_only
    )

    if build_all or args.sex_only:
        build_sex()

    if build_all or args.place_only:
        build_place()

    if build_all or args.bullet_only:
        build_bullet()

    if build_all or args.sentinel_only:
        build_sentinel()

    if build_all or args.unified_only:
        build_unified()


if __name__ == "__main__":
    main()
