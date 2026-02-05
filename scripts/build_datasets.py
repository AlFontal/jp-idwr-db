#!/usr/bin/env python3
"""
Script to build bundled datasets for jpinfectpy.
This downloads historical data and saves it as Parquet files in the package.
"""

import argparse
import polars as pl
from pathlib import Path
from jpinfectpy import io
from datetime import datetime

CURRENT_YEAR = datetime.now().year
LAST_HISTORICAL_YEAR = 2023
DATA_DIR = Path(__file__).parent.parent / "src" / "jpinfectpy" / "data"


def build_sex():
    print("Building sex_prefecture dataset...")
    years = range(1999, LAST_HISTORICAL_YEAR + 1)
    dfs = []
    for year in years:
        try:
            path = io.download("sex", year)
            df = io.read(path, type="sex", return_type="polars")
            dfs.append(df)
            print(f"  Loaded {year}")
        except Exception as e:
            print(f"  Failed {year}: {e}")

    if dfs:
        full_df = pl.concat(dfs, how="diagonal_relaxed")
        out_path = DATA_DIR / "sex_prefecture.parquet"
        full_df.write_parquet(out_path)
        print(f"Saved to {out_path} ({full_df.height} rows)")


def build_place():
    print("\nBuilding place_prefecture dataset...")
    years = range(2001, LAST_HISTORICAL_YEAR + 1)
    dfs = []
    for year in years:
        try:
            path = io.download("place", year)
            df = io.read(path, type="place", return_type="polars")
            dfs.append(df)
            print(f"  Loaded {year}")
        except Exception as e:
            print(f"  Failed {year}: {e}")

    if dfs:
        full_df = pl.concat(dfs, how="diagonal_relaxed")
        out_path = DATA_DIR / "place_prefecture.parquet"
        full_df.write_parquet(out_path)
        print(f"Saved to {out_path} ({full_df.height} rows)")


def build_bullet():
    print(f"\nBuilding bullet dataset ({LAST_HISTORICAL_YEAR + 1}-{CURRENT_YEAR})...")
    # Fetch recent years
    years = range(LAST_HISTORICAL_YEAR + 1, CURRENT_YEAR + 1)
    dfs = []
    for year in years:
        try:
            paths = io.download("bullet", year, week=range(1, 53))
            if not paths:
                continue
            if isinstance(paths, list):
                for p in paths:
                    df = io.read(p, type="bullet", return_type="polars")
                    dfs.append(df)
                print(f"  Loaded {len(paths)} weeks for {year}")
        except Exception as e:
            print(f"  Failed {year}: {e}")

    if dfs:
        full_df = pl.concat(dfs, how="diagonal_relaxed")
        out_path = DATA_DIR / "bullet.parquet"
        full_df.write_parquet(out_path)
        print(f"Saved to {out_path} ({full_df.height} rows)")


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
