#!/usr/bin/env python3
"""
Script to build bundled datasets for jpinfectpy.
This downloads historical data and saves it as Parquet files in the package.
"""

import argparse
import logging
import polars as pl
from pathlib import Path
from jpinfectpy import io
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

CURRENT_YEAR = datetime.now().year
CURRENT_WEEK = datetime.now().isocalendar().week
LAST_HISTORICAL_YEAR = 2023
DATA_DIR = Path(__file__).parent.parent / "src" / "jpinfectpy" / "data"


def build_sex():
    logger.info("Building sex_prefecture dataset...")
    years = range(1999, LAST_HISTORICAL_YEAR + 1)
    dfs = []
    success_count = 0
    fail_count = 0
    
    for year in years:
        try:
            path = io.download("sex", year)
            df = io.read(path, type="sex", return_type="polars")
            dfs.append(df)
            success_count += 1
            logger.info(f"  ✓ Loaded year {year} ({df.height} rows)")
        except Exception as e:
            fail_count += 1
            logger.warning(f"  ✗ Failed year {year}: {e}")

    if dfs:
        full_df = pl.concat(dfs, how="diagonal_relaxed")
        out_path = DATA_DIR / "sex_prefecture.parquet"
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
            path = io.download("place", year)
            df = io.read(path, type="place", return_type="polars")
            dfs.append(df)
            success_count += 1
            logger.info(f"  ✓ Loaded year {year} ({df.height} rows)")
        except Exception as e:
            fail_count += 1
            logger.warning(f"  ✗ Failed year {year}: {e}")

    if dfs:
        full_df = pl.concat(dfs, how="diagonal_relaxed")
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
        final_week = CURRENT_WEEK if year == CURRENT_YEAR else 53
        try:
            logger.info(f"  Processing year {year}...")
            paths = io.download("bullet", year, week=range(1, final_week + 1))
            if not paths:
                logger.warning(f"    No data found for year {year}")
                continue
            
            if isinstance(paths, list):
                year_dfs = []
                for i, p in enumerate(paths, 1):
                    df = io.read(p, type="bullet", return_type="polars")
                    year_dfs.append(df)
                    # Log progress every 10 weeks or on the last week
                    if i == len(paths):
                        logger.info(f"    Loaded weeks 1-{i} for {year}")
                
                dfs.extend(year_dfs)
                total_weeks += len(paths)
                logger.info(f"  ✓ Completed year {year}: {len(paths)} weeks loaded")
        except Exception as e:
            logger.error(f"  ✗ Failed year {year}: {e}")

    if dfs:
        full_df = pl.concat(dfs, how="diagonal_relaxed")
        out_path = DATA_DIR / "bullet.parquet"
        full_df.write_parquet(out_path)
        logger.info(f"Saved to {out_path.name} ({full_df.height} rows, {total_weeks} weeks total)")
    else:
        logger.warning("No bullet data was loaded")


def main():
    parser = argparse.ArgumentParser(description="Build bundled datasets for jpinfectpy")
    parser.add_argument(
        "--sex-only", action="store_true", help="Build only the sex_prefecture dataset"
    )
    parser.add_argument(
        "--place-only", action="store_true", help="Build only the place_prefecture dataset"
    )
    parser.add_argument("--bullet-only", action="store_true", help="Build only the bullet dataset")

    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # If no specific dataset is requested, build all
    build_all = not (args.sex_only or args.place_only or args.bullet_only)

    if build_all or args.sex_only:
        build_sex()

    if build_all or args.place_only:
        build_place()

    if build_all or args.bullet_only:
        build_bullet()


if __name__ == "__main__":
    main()
