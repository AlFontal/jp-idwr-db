# Examples

Polars-first examples for working with bundled `jp_idwr_db` datasets.

## 1. Load Unified Data

```python
import jp_idwr_db as jp

df = jp.load("unified")
print(df.shape)
print(df.columns)
print(df.select(["year", "week"]).max())
```

```text
(5370477, 9)
['prefecture', 'year', 'week', 'date', 'count', 'category', 'disease', 'source', 'per_sentinel']
shape: (1, 2)
┌──────┬──────┐
│ year ┆ week │
╞══════╪══════╡
│ 2026 ┆ 53   │
└──────┴──────┘
```

## 2. Basic Disease Filter

```python
import jp_idwr_db as jp
import polars as pl

df = jp.load("unified")

measles = df.filter(pl.col("disease") == "Measles")
print(measles.shape)
print(measles["year"].min(), measles["year"].max())
```

```text
(44478, 9)
2008 2026
```

## 3. Annual Trend (Polars Group By)

```python
import jp_idwr_db as jp
import polars as pl

df = jp.load("unified")

tb_annual = (
    df.filter(pl.col("disease") == "Tuberculosis")
    .group_by("year")
    .agg(pl.col("count").sum().alias("cases"))
    .sort("year")
)

print(tb_annual.head(5))
print(tb_annual.tail(5))
```

```text
shape: (5, 2)
┌──────┬─────────┐
│ year ┆ cases   │
╞══════╪═════════╡
│ 2007 ┆ 21946.0 │
│ 2008 ┆ 28459.0 │
│ 2009 ┆ 26996.0 │
│ 2010 ┆ 24465.0 │
│ 2011 ┆ 22943.0 │
└──────┴─────────┘
```

## 4. Recent Sentinel Surveillance Slice

```python
import jp_idwr_db as jp
import polars as pl

sentinel = jp.load("sentinel")

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

```text
shape: (10, 4)
┌──────┬──────┬────────────────┬───────────────────┐
│ year ┆ week ┆ reported_cases ┆ mean_per_sentinel │
╞══════╪══════╪════════════════╪═══════════════════╡
│ 2024 ┆ 1    ┆ 131.0          ┆ 0.056897          │
│ 2024 ┆ 2    ┆ 343.0          ┆ 0.106977          │
│ 2024 ┆ 3    ┆ 567.0          ┆ 0.181591          │
│ ...  ┆ ...  ┆ ...            ┆ ...               │
└──────┴──────┴────────────────┴───────────────────┘
```

## 5. Source-Aware Comparison in Unified

```python
import jp_idwr_db as jp
import polars as pl

df = jp.load("unified")

source_counts = (
    df.group_by("source")
    .agg(pl.len().alias("rows"))
    .sort("rows", descending=True)
)

print(source_counts)
```

```text
shape: (3, 2)
┌───────────────────────┬─────────┐
│ source                ┆ rows    │
╞═══════════════════════╪═════════╡
│ Confirmed cases       ┆ 4317843 │
│ Sentinel surveillance ┆ 593274  │
│ All-case reporting    ┆ 459360  │
└───────────────────────┴─────────┘
```

## 6. Prefecture-Level Comparison for One Disease

```python
import jp_idwr_db as jp
import polars as pl

df = jp.get_data(
    disease="Hand, foot and mouth disease",
    source="sentinel",
    year=(2024, 2026),
)

by_prefecture = (
    df.group_by("prefecture")
    .agg(pl.col("count").sum().alias("cases"))
    .sort("cases", descending=True)
)

print(by_prefecture.head(10))
```

```text
shape: (10, 2)
┌────────────┬────────────┐
│ prefecture ┆ cases      │
╞════════════╪════════════╡
│ Tokyo      ┆ 1.150637e6 │
│ Kanagawa   ┆ 929832.0   │
│ Saitama    ┆ 805643.0   │
│ ...        ┆ ...        │
└────────────┴────────────┘
```

## 7. Category-Based Historical Analysis

```python
import jp_idwr_db as jp
import polars as pl

sex = jp.load("sex")

male_vs_total = (
    sex.filter(pl.col("disease") == "Tuberculosis")
    .group_by(["year", "category"])
    .agg(pl.col("count").sum().alias("cases"))
    .sort(["year", "category"])
)

print(male_vs_total.head(10))
```

```text
shape: (10, 3)
┌──────┬──────────┬───────┐
│ year ┆ category ┆ cases │
╞══════╪══════════╪═══════╡
│ 2007 ┆ female   ┆ 8347  │
│ 2007 ┆ male     ┆ 13599 │
│ 2007 ┆ total    ┆ 21946 │
│ ...  ┆ ...      ┆ ...   │
└──────┴──────────┴───────┘
```

## 8. Build a Compact Yearly Summary

```python
import jp_idwr_db as jp
import polars as pl

pl_df = jp.load("unified")
summary = (
    pl_df.filter(pl.col("year") >= 2020)
    .group_by("year")
    .agg(pl.col("count").sum().alias("cases"))
    .sort("year")
)

print(summary.head())
```

```text
shape: (5, 2)
┌──────┬─────────────┐
│ year ┆ cases       │
╞══════╪═════════════╡
│ 2020 ┆ 5.5199732e7 │
│ 2021 ┆ 2.608384e7  │
│ 2022 ┆ 2.8533745e7 │
│ 2023 ┆ 3.2401803e7 │
│ 2024 ┆ 4.3879899e7 │
└──────┴─────────────┘
```

## 9. Add ISO Prefecture IDs Only When Needed

```python
import jp_idwr_db as jp

df = jp.load("unified")
df_with_ids = jp.attach_prefecture_id(df)

print(df_with_ids.select(["prefecture", "prefecture_id"]).head(10))
```

```text
shape: (10, 2)
┌────────────┬───────────────┐
│ prefecture ┆ prefecture_id │
╞════════════╪═══════════════╡
│ Tochigi    ┆ JP-09         │
│ Kochi      ┆ JP-39         │
│ Hokkaido   ┆ JP-01         │
│ ...        ┆ ...           │
└────────────┴───────────────┘
```

## Notes

- The package is Polars-only.
- `count` is the case-count column in bundled datasets.
- `per_sentinel` is available for sentinel-derived records.
- `unified` is normalized to `category = "total"` only.
- `place` data is available as a separate dataset via `jp.load("place")`.
- In `unified`, source values are currently: `Confirmed cases`, `All-case reporting`, and `Sentinel surveillance`.
