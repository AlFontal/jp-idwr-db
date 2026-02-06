# Examples

Polars-first examples for working with bundled `jpinfectpy` datasets.

## 1. Load Unified Data

```python
import jpinfectpy as jp

df = jp.load("unified", return_type="polars")
print(df.shape)
print(df.columns)
print(df.select(["year", "week"]).max())
```

## 2. Basic Disease Filter

```python
import jpinfectpy as jp
import polars as pl

df = jp.load("unified", return_type="polars")

measles = df.filter(pl.col("disease") == "Measles")
print(measles.shape)
print(measles["year"].min(), measles["year"].max())
```

## 3. Annual Trend (Polars Group By)

```python
import jpinfectpy as jp
import polars as pl

df = jp.load("unified", return_type="polars")

tb_annual = (
    df.filter(pl.col("disease") == "Tuberculosis")
    .group_by("year")
    .agg(pl.col("count").sum().alias("cases"))
    .sort("year")
)

print(tb_annual.head(5))
print(tb_annual.tail(5))
```

## 4. Recent Sentinel Surveillance Slice

```python
import jpinfectpy as jp
import polars as pl

sentinel = jp.load("sentinel", return_type="polars")

recent = sentinel.filter(
    (pl.col("year") >= 2024) &
    (pl.col("disease") == "Respiratory syncytial virus infection")
)

weekly = (
    recent.group_by(["year", "week"])
    .agg([
        pl.col("count").sum().alias("reported_cases"),
        pl.col("per_sentinel").mean().alias("mean_per_sentinel"),
    ])
    .sort(["year", "week"])
)

print(weekly.head(10))
```

## 5. Source-Aware Comparison in Unified

```python
import jpinfectpy as jp
import polars as pl

df = jp.load("unified", return_type="polars")

source_counts = (
    df.group_by("source")
    .agg(pl.len().alias("rows"))
    .sort("rows", descending=True)
)

print(source_counts)
```

## 6. Prefecture-Level Comparison for One Disease

```python
import jpinfectpy as jp
import polars as pl

df = jp.get_data(
    disease="Hand, foot and mouth disease",
    source="sentinel",
    year=(2024, 2026),
    return_type="polars",
)

by_prefecture = (
    df.group_by("prefecture")
    .agg(pl.col("count").sum().alias("cases"))
    .sort("cases", descending=True)
)

print(by_prefecture.head(10))
```

## 7. Category-Based Historical Analysis

```python
import jpinfectpy as jp
import polars as pl

sex = jp.load("sex", return_type="polars")

male_vs_total = (
    sex.filter(pl.col("disease") == "Tuberculosis")
    .group_by(["year", "category"])
    .agg(pl.col("count").sum().alias("cases"))
    .sort(["year", "category"])
)

print(male_vs_total.head(10))
```

## 8. Convert to Pandas Only at the End

```python
import jpinfectpy as jp
import polars as pl

pl_df = jp.load("unified", return_type="polars")
summary = (
    pl_df.filter(pl.col("year") >= 2020)
    .group_by("year")
    .agg(pl.col("count").sum().alias("cases"))
    .sort("year")
)

pd_df = jp.to_pandas(summary)
print(type(pd_df))
print(pd_df.head())
```

## Notes

- Use `return_type="polars"` for most heavy wrangling workflows.
- `count` is the case-count column in bundled datasets.
- `per_sentinel` is available for sentinel-derived records.
- In `unified`, source values are currently: `Confirmed cases`, `All-case reporting`, and `Sentinel surveillance`.
