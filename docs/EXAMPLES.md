# Examples

Polars-first workflows for realistic surveillance analysis questions using `jp_idwr_db`.

## 1. National Burden by Disease and Year

Question: Which diseases contribute the most total reported cases in each calendar year?

```python
import jp_idwr_db as jp
import polars as pl
from datetime import timedelta

pl.Config.set_tbl_rows(10)

annual_burden = (
    jp.get_data(year=(2020, 2025))
    .group_by(["year", "disease"])
    .agg(pl.col("count").fill_null(0).sum().alias("cases"))
    .with_columns(
        pl.col("cases")
        .rank("dense", descending=True)
        .over("year")
        .alias("rank_within_year")
    )
    .filter(pl.col("rank_within_year") <= 5)
    .sort(["year", "rank_within_year", "disease"])
)

print(annual_burden)
```

## 2. RSV Season Timing and Peak Intensity

Question: For RSV, when does each year peak nationally and what is the peak magnitude?

```python
import jp_idwr_db as jp
import polars as pl

pl.Config.set_tbl_rows(20)

rsv_weekly = (
    jp.get_data(source="sentinel", disease="Respiratory syncytial virus infection", year=(2018, 2025))
    .group_by(["year", "week"])
    .agg(pl.col("count").fill_null(0).sum().alias("cases"))
    .sort(["year", "week"])
)

rsv_peak = (
    rsv_weekly
    .group_by("year")
    .agg(
        pl.col("cases").max().alias("peak_cases"),
        pl.col("week").sort_by("cases", descending=True).first().alias("peak_week"),
    )
    .sort("year")
)

print(rsv_peak)
```

## 3. Early-Warning Signal (Recent 4 Weeks vs Previous 4 Weeks)

Question: Which prefectures show the strongest recent acceleration for RSV?

```python
import jp_idwr_db as jp
import polars as pl

pl.Config.set_tbl_rows(10)

rsv = (
    jp.get_data(source="sentinel", disease="Respiratory syncytial virus infection", year=(2024, 2026))
    .select(["date", "prefecture", "count"])
    .filter(pl.col("date").is_not_null())
    .sort(["prefecture", "date"])
)

latest_date = rsv.select(pl.col("date").max()).item()
recent_start = latest_date - timedelta(days=27)
prior_start = latest_date - timedelta(days=55)
prior_end = latest_date - timedelta(days=28)

trend = (
    rsv.group_by("prefecture")
    .agg(
        pl.col("count")
        .filter(pl.col("date") >= recent_start)
        .fill_null(0)
        .sum()
        .alias("recent_4w"),
        pl.col("count")
        .filter((pl.col("date") >= prior_start) & (pl.col("date") <= prior_end))
        .fill_null(0)
        .sum()
        .alias("previous_4w"),
    )
    .with_columns((pl.col("recent_4w") - pl.col("previous_4w")).alias("absolute_change"))
    .sort(["absolute_change", "recent_4w"], descending=True)
)

print(trend)
```

## 4. Tuberculosis Sex Split Over Time

Question: How has male/female tuberculosis burden changed historically?

```python
import jp_idwr_db as jp
import polars as pl

pl.Config.set_tbl_rows(20)

tb_by_sex = (
    jp.load("sex")
    .filter(pl.col("disease") == "Tuberculosis")
    .group_by(["year", "category"])
    .agg(pl.col("count").fill_null(0).sum().alias("cases"))
    .filter(pl.col("category").is_in(["male", "female", "total"]))
    .sort(["year", "category"])
)

tb_ratio = (
    tb_by_sex
    .pivot(values="cases", index="year", on="category")
    .with_columns((pl.col("male") / pl.col("female")).alias("male_female_ratio"))
    .sort("year")
)

print(tb_ratio)
```

## 5. Disease-Specific Spatial Ranking

Question: Which prefectures carried the largest cumulative burden of hand-foot-mouth disease in the recent period?

```python
import jp_idwr_db as jp
import polars as pl

pl.Config.set_tbl_rows(15)

hfmd = (
    jp.get_data(
        source="sentinel",
        disease="Hand, foot and mouth disease",
        year=(2023, 2026),
    )
    .group_by("prefecture")
    .agg(pl.col("count").fill_null(0).sum().alias("cases"))
    .sort("cases", descending=True)
)

print(hfmd)
```

## 6. Outbreak Window: Weekly Profile for Selected Prefectures

Question: How did influenza trajectories differ across major metropolitan prefectures in 2024?

```python
import jp_idwr_db as jp
import polars as pl

pl.Config.set_tbl_rows(20)

metro = ["Tokyo", "Osaka", "Kanagawa", "Aichi"]

influenza_metro = (
    jp.get_data(
        disease="Influenza",
        source="sentinel",
        prefecture=metro,
        year=2024,
    )
    .group_by(["prefecture", "week"])
    .agg(pl.col("count").fill_null(0).sum().alias("weekly_cases"))
    .sort(["prefecture", "week"])
)

print(influenza_metro)
```

## 7. Add ISO Prefecture IDs Only at Analysis Time

Question: Keep stored data slim, then add merge keys only when needed.

```python
import jp_idwr_db as jp

measles_2024 = (
    jp.get_data(disease="Measles", year=2024)
    .select(["prefecture", "count"])
    .group_by("prefecture")
    .sum()
    .pipe(jp.attach_prefecture_id)
    .sort("prefecture")
)

print(measles_2024)
```

## Notes

- `jp.get_data()` is the recommended default for most analytical tasks.
- Use `jp.load("sex")`, `jp.load("place")`, `jp.load("bullet")`, `jp.load("sentinel")` for source-specific studies.
- `unified` is category-normalized to `total` and source-aware (`Confirmed cases`, `All-case reporting`, `Sentinel surveillance`).
