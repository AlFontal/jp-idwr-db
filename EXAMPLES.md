# jpinfectpy Usage Examples

This document provides comprehensive, tested examples for using jpinfectpy to analyze Japanese infectious disease surveillance data.

## Table of Contents

1. [Basic Loading and Exploration](#1-basic-loading-and-exploration)
2. [Working with Different Return Types](#2-working-with-different-return-types)
3. [Downloading and Reading Raw Data](#3-downloading-and-reading-raw-data)
4. [Time Series Analysis](#4-time-series-analysis)
5. [Geographic Analysis](#5-geographic-analysis)
6. [Disease-Specific Analysis](#6-disease-specific-analysis)
7. [Combining Historical and Recent Data](#7-combining-historical-and-recent-data)
8. [Data Transformations](#8-data-transformations)
9. [Custom Workflows](#9-custom-workflows)
10. [Performance Tips](#10-performance-tips)

---

## 1. Basic Loading and Exploration

### Loading Bundled Datasets

```python
import jpinfectpy as jp
import polars as pl

# Load sex-disaggregated data (1999-2023)
df_sex = jp.load("sex")

# View basic statistics
print(df_sex.head())
print(f"Shape: {df_sex.shape}")
print(f"Columns: {df_sex.columns}")
print(f"Date range: {df_sex['date'].min()} to {df_sex['date'].max()}")

# Output:
# shape: (5, 7)
# ┌────────────┬──────┬──────┬────────────┬───────┬──────────┬───────┐
# │ prefecture ┆ year ┆ week ┆ date       ┆ disea ┆ category ┆ count │
# │ ---        ┆ ---  ┆ ---  ┆ ---        ┆ se    ┆ ---      ┆ ---   │
# │ str        ┆ i64  ┆ i64  ┆ date       ┆ ---   ┆ str      ┆ i64   │
# │            ┆      ┆      ┆            ┆ str   ┆          ┆       │
# ╞════════════╪══════╪══════╪════════════╪═══════╪══════════╪═══════╡
# │ Hokkaido   ┆ 1999 ┆ 14   ┆ 1999-04-04 ┆ Infl… ┆ total    ┆ 45    │
# │ Hokkaido   ┆ 1999 ┆ 14   ┆ 1999-04-04 ┆ Infl… ┆ male     ┆ 23    │
# │ Hokkaido   ┆ 1999 ┆ 14   ┆ 1999-04-04 ┆ Infl… ┆ female   ┆ 22    │
# └────────────┴──────┴──────┴────────────┴───────┴──────────┴───────┘
```

### Understanding the Data Structure

```python
# Check available diseases
diseases = df_sex['disease'].unique().sort()
print(f"Available diseases: {len(diseases)}")
print(diseases[:5])  # First 5 diseases

# Check available prefectures  
prefectures = df_sex['prefecture'].unique().sort()
print(f"Prefectures: {len(prefectures)}")

# Check categories (sex disaggregation)
categories = df_sex['category'].unique()
print(f"Categories: {categories}")  # ['total', 'male', 'female']

# Summary by disease
disease_summary = (
    df_sex
    .filter(pl.col('category') == 'total')
    .group_by('disease')
    .agg([
        pl.sum('count').alias('total_cases'),
        pl.count('count').alias('num_reports')
    ])
    .sort('total_cases', descending=True)
)
print(disease_summary.head(10))
```

---

## 2. Working with Different Return Types

### Polars vs Pandas

```python
import jpinfectpy as jp
import polars as pl
import pandas as pd

# Get as Polars DataFrame (default, faster)
df_polars = jp.load("sex", return_type="polars")
print(type(df_polars))  # <class 'polars.dataframe.frame.DataFrame'>

# Get as Pandas DataFrame (more familiar for many users)
df_pandas = jp.load("sex", return_type="pandas")
print(type(df_pandas))  # <class 'pandas.core.frame.DataFrame'>

# Set global default to avoid specifying each time
jp.configure(return_type="polars")
df = jp.load("sex")  # Now returns Polars by default
```

### Converting Between Types

```python
# Convert Polars to Pandas
df_polars = jp.load("sex", return_type="polars")
df_pandas = df_polars.to_pandas()

# Convert Pandas to Polars
df_pandas = jp.load("sex", return_type="pandas")
df_polars = pl.from_pandas(df_pandas)

# Or use jpinfectpy utilities
from jpinfectpy import to_polars, to_pandas

df_p = to_polars(df_pandas)
df_pd = to_pandas(df_polars)
```

---

## 3. Downloading and Reading Raw Data

### Download Specific Year

```python
import jpinfectpy as jp

# Download sex-disaggregated data for 2024
path = jp.download("sex", 2024)
print(f"Downloaded to: {path}")

# Read the downloaded file
df_2024 = jp.read(path)
print(df_2024.head())

# Download to custom directory
from pathlib import Path
custom_dir = Path("./my_data")
path = jp.download("sex", 2024, out_dir=custom_dir)
```

### Download Weekly Bulletins

```python
# Download specific weeks
paths = jp.download("bullet", 2024, week=[1, 2, 3])
print(f"Downloaded {len(paths)} files")

# Read one of the bulletins
df_week1 = jp.read(paths[0])
print(df_week1.head())

# Download all available 2024+ weekly reports
all_paths = jp.download_recent()
print(f"Downloaded {len(all_paths)} weekly bulletins")

# Read multiple bulletins
import polars as pl
dfs = [jp.read(p, return_type="polars") for p in all_paths[:5]]
df_combined = pl.concat(dfs, how="vertical_relaxed")
```

### Force Re-download

```python
# By default, cached files are reused
path1 = jp.download("sex", 2024)  # Downloads if not cached

path2 = jp.download("sex", 2024)  # Reuses cache
assert path1 == path2

# Force re-download (e.g., if data was updated)
path3 = jp.download("sex", 2024, overwrite=True)  # Re-downloads
```

---

## 4. Time Series Analysis

### Influenza Trends Over Time

```python
import jpinfectpy as jp
import polars as pl

# Load data
df = jp.load("sex", return_type="polars")

# Filter to influenza, total category, national level
influenza = (
    df
    .filter(
        (pl.col('disease').str.contains('Influenza')) &
        (pl.col('category') == 'total')
    )
    .group_by(['year', 'week', 'date'])
    .agg(pl.sum('count').alias('total_cases'))
    .sort(['year', 'week'])
)

print(influenza.head(10))

# Calculate yearly totals
yearly = (
    influenza
    .group_by('year')
    .agg(pl.sum('total_cases').alias('annual_cases'))
    .sort('year')
)

print("\nInfluenza cases by year:")
print(yearly)
```

### Epidemic Week Detection

```python
import polars as pl

# Calculate rolling average and identify peaks
df_with_stats = (
    influenza
    .sort('date')
    .with_columns([
        # 4-week moving average
        pl.col('total_cases').rolling_mean(window_size=4).alias('rolling_avg'),
        # Peak detection (value > 1.5x moving average)
        (pl.col('total_cases') > 
         pl.col('total_cases').rolling_mean(window_size=4) * 1.5).alias('is_peak')
    ])
)

# Find peak weeks
peaks = df_with_stats.filter(pl.col('is_peak') == True)
print(f"Found {len(peaks)} peak weeks")
print(peaks.select(['year', 'week', 'date', 'total_cases']).head(10))
```

### Seasonal Pattern Analysis

```python
# Aggregate by week number (across all years)
seasonal_pattern = (
    influenza
    .group_by('week')
    .agg([
        pl.mean('total_cases').alias('avg_cases'),
        pl.std('total_cases').alias('std_cases'),
        pl.count('total_cases').alias('num_years')
    ])
    .sort('week')
)

print("Seasonal pattern (average by week of year):")
print(seasonal_pattern)

# Peak season (weeks with highest average)
peak_weeks = seasonal_pattern.sort('avg_cases', descending=True).head(5)
print(f"\nPeak weeks: {peak_weeks['week'].to_list()}")
```

---

## 5. Geographic Analysis

### Cases by Prefecture

```python
import jpinfectpy as jp
import polars as pl

df = jp.load("sex", return_type="polars")

# Total cases by prefecture for a specific disease
measles_by_pref = (
    df
    .filter(
        (pl.col('disease') == 'Measles') &
        (pl.col('category') == 'total') &
        (pl.col('year') >= 2020)
    )
    .group_by('prefecture')
    .agg(pl.sum('count').alias('total_cases'))
    .sort('total_cases', descending=True)
)

print("Measles cases by prefecture (2020+):")
print(measles_by_pref.head(10))
```

### Regional Aggregation

```python
# Define regions (simplified example)
region_map = {
    'Hokkaido': 'Hokkaido',
    'Aomori': 'Tohoku', 'Iwate': 'Tohoku', 'Miyagi': 'Tohoku',
    'Tokyo': 'Kanto', 'Kanagawa': 'Kanto', 'Chiba': 'Kanto', 'Saitama': 'Kanto',
    'Osaka': 'Kansai', 'Kyoto': 'Kansai', 'Hyogo': 'Kansai',
    # ... (add all prefectures)
}

# Map prefectures to regions
df_with_region = (
    df
    .with_columns(
        pl.col('prefecture').replace(region_map).alias('region')
    )
)

# Aggregate by region
regional_cases = (
    df_with_region
    .filter(
        (pl.col('disease') == 'COVID-19') &
        (pl.col('category') == 'total')
    )
    .group_by(['region', 'year'])
    .agg(pl.sum('count').alias('total_cases'))
    .sort(['region', 'year'])
)

print(regional_cases)
```

### Prefecture Rankings Over Time

```python
# Rank prefectures by cases for each year
ranked = (
    df
    .filter(
        (pl.col('disease') == 'Influenza') &
        (pl.col('category') == 'total')
    )
    .group_by(['prefecture', 'year'])
    .agg(pl.sum('count').alias('annual_cases'))
    .with_columns(
        pl.col('annual_cases').rank(descending=True).over('year').alias('rank')
    )
    .filter(pl.col('rank') <= 5)  # Top 5 each year
    .sort(['year', 'rank'])
)

print("Top 5 prefectures by year:")
print(ranked)
```

---

## 6. Disease-Specific Analysis

### Comparing Multiple Diseases

```python
import jpinfectpy as jp
import polars as pl

df = jp.load("sex", return_type="polars")

# Select diseases of interest
diseases_of_interest = ['Influenza', 'Measles', 'Rubella', 'Mumps']

# Filter and aggregate
disease_comparison = (
    df
    .filter(
        (pl.col('disease').is_in(diseases_of_interest)) &
        (pl.col('category') == 'total') &
        (pl.col('year') >=  2015)
    )
    .group_by(['disease', 'year'])
    .agg(pl.sum('count').alias('annual_cases'))
    .sort(['disease', 'year'])
)

print(disease_comparison)

# Pivot to wide format for easy comparison
disease_wide = disease_comparison.pivot(
    index='year',
    columns='disease',
    values='annual_cases'
)
print(disease_wide)
```

### Sex Disaggregation Analysis

```python
# Compare male vs female cases
sex_comparison = (
    df
    .filter(
        (pl.col('disease') == 'Rubella') &
        (pl.col('category').is_in(['male', 'female'])) &
        (pl.col('year') >= 2010)
    )
    .group_by(['year', 'category'])
    .agg(pl.sum('count').alias('total_cases'))
    .pivot(index='year', columns='category', values='total_cases')
    .with_columns([
        (pl.col('male') / (pl.col('male') + pl.col('female')) * 100).alias('male_pct'),
        (pl.col('female') / (pl.col('male') + pl.col('female')) * 100).alias('female_pct')
    ])
    .sort('year')
)

print("Rubella cases by sex (2010+):")
print(sex_comparison)
```

### Disease Burden Analysis

```python
# Calculate disease burden (total cases per 100k weeks)
disease_burden = (
    df
    .filter(pl.col('category') == 'total')
    .group_by('disease')
    .agg([
        pl.sum('count').alias('total_cases'),
        pl.count('count').alias('num_reports'),
        pl.mean('count').alias('avg_weekly_cases')
    ])
    .with_columns(
        (pl.col('total_cases') / pl.col('num_reports')).alias('cases_per_report')
    )
    .sort('total_cases', descending=True)
)

print("Disease burden ranking:")
print(disease_burden.head(20))
```

---

## 7. Combining Historical and Recent Data

### Using load_all()

```python
import jpinfectpy as jp
import polars as pl

# Get all available data (historical 1999-2023 + recent 2024+)
df_all = jp.load_all(return_type="polars")

print(f"Date range: {df_all['date'].min()} to {df_all['date'].max()}")
print(f"Total records: {len(df_all)}")

# Check data sources
source_counts = df_all.group_by('source').count()
print("\nData by source:")
print(source_counts)
```

### Manual Merging

```python
# Load historical data
df_historical = jp.load("sex", return_type="polars")

# Download and read recent data
recent_paths = jp.download_recent()
df_recent_list = [jp.read(p, return_type="polars") for p in recent_paths]
df_recent = pl.concat(df_recent_list, how="vertical_relaxed")

# Add 'category' column to recent data if missing
if 'category' not in df_recent.columns:
    df_recent = df_recent.with_columns(pl.lit('total').alias('category'))

# Filter historical to 'total' to match recent data granularity
df_historical_total = df_historical.filter(pl.col('category') == 'total')

# Merge using jpinfectpy's merge function
from jpinfectpy import merge
df_combined = merge(df_historical_total, df_recent)

print(f"Combined data: {len(df_combined)} records")
```

---

## 8. Data Transformations

### Pivot to Wide Format

```python
import jpinfectpy as jp
from jpinfectpy import pivot
import polars as pl

# Load data
df = jp.load("sex", return_type="polars")

# Filter to specific diseases and time period
df_filtered = (
    df
    .filter(
        (pl.col('disease').is_in(['Influenza', 'Measles', 'Rubella'])) &
        (pl.col('prefecture') == 'Tokyo') &
        (pl.col('category') == 'total') &
        (pl.col('year') == 2023)
    )
)

# Convert to wide format (diseases as columns)
df_wide = pivot(df_filtered, direction="wide")
print(df_wide)

# Now you have columns: prefecture, year, week, date, Influenza, Measles, Rubella
```

### Pivot to Long Format

```python
# If you have wide data and want to convert back to long
df_long = pivot(df_wide, direction="long")
print(df_long.head())
```

### Merging Multiple Years

```python
from jpinfectpy import merge

# Download multiple years
paths = [jp.download("sex", year) for year in [2021, 2022, 2023, 2024]]

# Read all files
dfs = [jp.read(p, return_type="polars") for p in paths]

# Merge using jpinfectpy's merge function
df_merged = merge(*dfs)

print(f"Merged {len(dfs)} years: {len(df_merged)} total records")
print(f"Year range: {df_merged['year'].min()} to {df_merged['year'].max()}")
```

---

## 9. Custom Workflows

### Complete Analysis Pipeline

```python
import jpinfectpy as jp
import polars as pl
from pathlib import Path

# Configure jpinfectpy
jp.configure(
    return_type="polars",
    cache_dir=Path("./analysis_cache"),
    rate_limit_per_minute=30
)

# 1. Load all available data
print("Loading data...")
df = jp.load_all()

# 2. Filter to disease and time period of interest
disease = "COVID-19"
start_year = 2020

df_filtered = (
    df
    .filter(
        (pl.col('disease') == disease) &
        (pl.col('year') >= start_year)
    )
)

# 3. Calculate national weekly totals
national_weekly = (
    df_filtered
    .filter(pl.col('category') == 'total')
    .group_by(['year', 'week', 'date'])
    .agg(pl.sum('count').alias('national_total'))
    .sort('date')
)

# 4. Add rolling statistics
analysis = (
    national_weekly
    .with_columns([
        pl.col('national_total').rolling_mean(window_size=4).alias('4wk_avg'),
        pl.col('national_total').rolling_max(window_size=4).alias('4wk_max'),
        pl.col('national_total').pct_change().alias('weekly_change')
    ])
)

# 5. Identify significant events
significant_weeks = (
    analysis
    .filter(
        (pl.col('national_total') > pl.col('4wk_avg') * 2) |
        (pl.col('weekly_change') > 0.5)
    )
    .select(['year', 'week', 'date', 'national_total', 'weekly_change'])
)

print(f"\nSignificant weeks for {disease} (since {start_year}):")
print(significant_weeks)

# 6. Export results
analysis.write_csv(f"{disease}_analysis.csv")
print(f"\nAnalysis saved to {disease}_analysis.csv")
```

### Custom Geographic Filtering

```python
import jpinfectpy as jp
import polars as pl

# Define metropolitan areas
metro_prefectures = [
    'Tokyo', 'Kanagawa', 'Chiba', 'Saitama',  # Greater Tokyo
    'Osaka', 'Kyoto', 'Hyogo',                # Kansai
    'Aichi'                                     # Nagoya
]

# Load and filter to metro areas
df = jp.load("sex", return_type="polars")

metro_data = (
    df
    .filter(
        (pl.col('prefecture').is_in(metro_prefectures)) &
        (pl.col('category') == 'total')
    )
    .with_columns(
        pl.when(pl.col('prefecture').is_in(['Tokyo', 'Kanagawa', 'Chiba', 'Saitama']))
          .then(pl.lit('Greater Tokyo'))
          .when(pl.col('prefecture').is_in(['Osaka', 'Kyoto', 'Hyogo']))
          .then(pl.lit('Kansai'))
          .otherwise(pl.lit('Nagoya'))
          .alias('metro_area')
    )
)

# Analyze by metro area
metro_summary = (
    metro_data
    .filter(pl.col('year') >= 2020)
    .group_by(['metro_area', 'disease'])
    .agg(pl.sum('count').alias('total_cases'))
    .sort(['metro_area', 'total_cases'], descending=[False, True])
)

print("Cases by metropolitan area (2020+):")
print(metro_summary)
```

---

## 10. Performance Tips

### Use Polars for Large Datasets

```python
import jpinfectpy as jp
import time

# Set Polars as default
jp.configure(return_type="polars")

# Polars is much faster for large operations
start = time.time()
df = jp.load_all(return_type="polars")
result = (
    df
    .filter(pl.col('year') >= 2015)
    .group_by(['disease', 'year'])
    .agg(pl.sum('count'))
)
polars_time = time.time() - start

print(f"Polars processing time: {polars_time:.2f}s")
```

### Lazy Evaluation with Polars

```python
import polars as pl

# Use lazy evaluation for complex queries
df_lazy = pl.scan_csv("path/to/large_file.csv")

result = (
    df_lazy
    .filter(pl.col('year') >= 2020)
    .group_by('disease')
    .agg(pl.sum('count'))
    .collect()  # Execute the query
)
```

### Minimize Data Loading

```python
import jpinfectpy as jp

# Instead of loading all data
# df_all = jp.load_all()  # Can be slow

# Load only what you need
df_sex_recent = (
    jp.load("sex", return_type="polars")
    .filter(pl.col('year') >= 2020)  # Filter immediately
)

# Or download specific years
path = jp.download("sex", 2024)
df_2024 = jp.read(path)
```

### Efficient Caching

```python
from pathlib import Path
import jpinfectpy as jp

# Set a project-specific cache directory
jp.configure(cache_dir=Path("./project_cache"))

# Downloads are cached automatically
paths = jp.download_recent()  # First call downloads
paths = jp.download_recent()  # Second call uses cache (instant)

# Clear cache when needed
import shutil
cache_dir = jp.get_config().cache_dir
if cache_dir.exists():
    shutil.rmtree(cache_dir)
    print("Cache cleared")
```

---

## Additional Resources

- **API Reference**: See docstrings in the code or use `help(jp.load)`, etc.
- **CONTRIBUTING.md**: Development setup, code standards, and project architecture
- **DATASETS.md**: Detailed dataset descriptions
- **README.md**: Quick start and installation

## Getting Help

If you encounter issues:

1. Check function docstrings with `help(jp.function_name)`
2. Review the troubleshooting tips in [CONTRIBUTING.md](./CONTRIBUTING.md)
3. Search existing issues at [github.com/AlFontal/jpinfectpy/issues](https://github.com/AlFontal/jpinfectpy/issues)
4. File a new issue with a reproducible example
