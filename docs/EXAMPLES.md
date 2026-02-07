# Examples

Polars-first workflows for realistic surveillance analysis questions using `jp_idwr_db`.

## 1. National Burden by Disease and Year

Question: Which diseases contribute the most total reported cases in each calendar year?

```python
import jp_idwr_db as jp
import polars as pl

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

```text
shape: (30, 4)
┌──────┬─────────────────────────────────┬────────────┬──────────────────┐
│ year ┆ disease                         ┆ cases      ┆ rank_within_year │
│ ---  ┆ ---                             ┆ ---        ┆ ---              │
│ i32  ┆ str                             ┆ f64        ┆ u32              │
╞══════╪═════════════════════════════════╪════════════╪══════════════════╡
│ 2020 ┆ Influenza(excld. avian influen… ┆ 563487.0   ┆ 1                │
│ 2020 ┆ Infectious gastroenteritis      ┆ 419973.0   ┆ 2                │
│ 2020 ┆ Group A streptococcal pharyngi… ┆ 200227.0   ┆ 3                │
│ 2020 ┆ Exanthem subitum                ┆ 65531.0    ┆ 4                │
│ 2020 ┆ Pharyngoconjunctival fever      ┆ 35113.0    ┆ 5                │
│ …    ┆ …                               ┆ …          ┆ …                │
│ 2025 ┆ Influenza(excld. avian influen… ┆ 1.853723e6 ┆ 1                │
│ 2025 ┆ Infectious gastroenteritis      ┆ 780085.0   ┆ 2                │
│ 2025 ┆ COVID-19                        ┆ 775814.0   ┆ 3                │
│ 2025 ┆ Group A streptococcal pharyngi… ┆ 284472.0   ┆ 4                │
│ 2025 ┆ Erythema infection              ┆ 157449.0   ┆ 5                │
└──────┴─────────────────────────────────┴────────────┴──────────────────┘
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

```text
shape: (8, 3)
┌──────┬────────────┬───────────┐
│ year ┆ peak_cases ┆ peak_week │
│ ---  ┆ ---        ┆ ---       │
│ i32  ┆ f64        ┆ i32       │
╞══════╪════════════╪═══════════╡
│ 2018 ┆ 7742.0     ┆ 37        │
│ 2019 ┆ 10887.0    ┆ 37        │
│ 2020 ┆ 1121.0     ┆ 5         │
│ 2021 ┆ 18971.0    ┆ 28        │
│ 2022 ┆ 7391.0     ┆ 30        │
│ 2023 ┆ 10616.0    ┆ 27        │
│ 2024 ┆ 5768.0     ┆ 28        │
│ 2025 ┆ 4625.0     ┆ 11        │
└──────┴────────────┴───────────┘
```

## 3. Early-Warning Signal (Recent 4 Weeks vs Previous 4 Weeks)

Question: Which prefectures show the strongest recent acceleration for RSV?

```python
import jp_idwr_db as jp
import polars as pl
from datetime import timedelta

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

```text
shape: (47, 4)
┌────────────┬───────────┬─────────────┬─────────────────┐
│ prefecture ┆ recent_4w ┆ previous_4w ┆ absolute_change │
│ ---        ┆ ---       ┆ ---         ┆ ---             │
│ str        ┆ f64       ┆ f64         ┆ f64             │
╞════════════╪═══════════╪═════════════╪═════════════════╡
│ Kumamoto   ┆ 149.0     ┆ 90.0        ┆ 59.0            │
│ Kagoshima  ┆ 129.0     ┆ 97.0        ┆ 32.0            │
│ Okayama    ┆ 97.0      ┆ 66.0        ┆ 31.0            │
│ Nagasaki   ┆ 38.0      ┆ 9.0         ┆ 29.0            │
│ Miyagi     ┆ 62.0      ┆ 35.0        ┆ 27.0            │
│ …          ┆ …         ┆ …           ┆ …               │
│ Osaka      ┆ 703.0     ┆ 793.0       ┆ -90.0           │
│ Kanagawa   ┆ 138.0     ┆ 243.0       ┆ -105.0          │
│ Hiroshima  ┆ 87.0      ┆ 201.0       ┆ -114.0          │
│ Nagano     ┆ 97.0      ┆ 222.0       ┆ -125.0          │
│ Hokkaido   ┆ 482.0     ┆ 719.0       ┆ -237.0          │
└────────────┴───────────┴─────────────┴─────────────────┘
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

```text
shape: (17, 5)
┌──────┬────────┬───────┬───────┬───────────────────┐
│ year ┆ female ┆ male  ┆ total ┆ male_female_ratio │
│ ---  ┆ ---    ┆ ---   ┆ ---   ┆ ---               │
│ i32  ┆ i64    ┆ i64   ┆ i64   ┆ f64               │
╞══════╪════════╪═══════╪═══════╪═══════════════════╡
│ 2007 ┆ 8347   ┆ 13599 ┆ 21946 ┆ 1.629208          │
│ 2008 ┆ 11279  ┆ 17180 ┆ 28459 ┆ 1.523185          │
│ 2009 ┆ 10997  ┆ 15999 ┆ 26996 ┆ 1.454851          │
│ 2010 ┆ 10915  ┆ 15951 ┆ 26866 ┆ 1.461383          │
│ 2011 ┆ 14278  ┆ 17205 ┆ 31483 ┆ 1.205001          │
│ 2012 ┆ 13334  ┆ 15983 ┆ 29317 ┆ 1.198665          │
│ 2013 ┆ 11902  ┆ 15150 ┆ 27052 ┆ 1.272895          │
│ 2014 ┆ 11518  ┆ 15111 ┆ 26629 ┆ 1.311947          │
│ 2015 ┆ 10677  ┆ 13846 ┆ 24523 ┆ 1.296806          │
│ 2016 ┆ 10765  ┆ 13904 ┆ 24669 ┆ 1.291593          │
│ 2017 ┆ 10262  ┆ 13165 ┆ 23427 ┆ 1.282888          │
│ 2018 ┆ 10022  ┆ 12426 ┆ 22448 ┆ 1.239872          │
│ 2019 ┆ 9711   ┆ 11961 ┆ 21672 ┆ 1.231696          │
│ 2020 ┆ 7779   ┆ 9869  ┆ 17648 ┆ 1.268672          │
│ 2021 ┆ 7189   ┆ 9110  ┆ 16299 ┆ 1.267214          │
│ 2022 ┆ 6524   ┆ 8274  ┆ 14798 ┆ 1.26824           │
│ 2023 ┆ 6791   ┆ 8586  ┆ 15377 ┆ 1.26432           │
└──────┴────────┴───────┴───────┴───────────────────┘
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

```text
shape: (47, 2)
┌────────────┬─────────┐
│ prefecture ┆ cases   │
│ ---        ┆ ---     │
│ str        ┆ f64     │
╞════════════╪═════════╡
│ Tokyo      ┆ 69268.0 │
│ Kanagawa   ┆ 57451.0 │
│ Saitama    ┆ 50964.0 │
│ Aichi      ┆ 39378.0 │
│ Osaka      ┆ 39199.0 │
│ Fukuoka    ┆ 35208.0 │
│ Chiba      ┆ 34801.0 │
│ Hyogo      ┆ 31568.0 │
│ …          ┆ …       │
│ Saga       ┆ 6076.0  │
│ Shimane    ┆ 5610.0  │
│ Kochi      ┆ 5575.0  │
│ Yamanashi  ┆ 4698.0  │
│ Akita      ┆ 4584.0  │
│ Tokushima  ┆ 4281.0  │
│ Tottori    ┆ 3646.0  │
└────────────┴─────────┘
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

```text
shape: (208, 3)
┌────────────┬──────┬──────────────┐
│ prefecture ┆ week ┆ weekly_cases │
│ ---        ┆ ---  ┆ ---          │
│ str        ┆ i32  ┆ f64          │
╞════════════╪══════╪══════════════╡
│ Aichi      ┆ 1    ┆ 3303.0       │
│ Aichi      ┆ 2    ┆ 3428.0       │
│ Aichi      ┆ 3    ┆ 4428.0       │
│ Aichi      ┆ 4    ┆ 4812.0       │
│ Aichi      ┆ 5    ┆ 5368.0       │
│ Aichi      ┆ 6    ┆ 5762.0       │
│ Aichi      ┆ 7    ┆ 4741.0       │
│ Aichi      ┆ 8    ┆ 3549.0       │
│ Aichi      ┆ 9    ┆ 2533.0       │
│ Aichi      ┆ 10   ┆ 2819.0       │
│ …          ┆ …    ┆ …            │
│ Tokyo      ┆ 43   ┆ 307.0        │
│ Tokyo      ┆ 44   ┆ 386.0        │
│ Tokyo      ┆ 45   ┆ 469.0        │
│ Tokyo      ┆ 46   ┆ 873.0        │
│ Tokyo      ┆ 47   ┆ 1000.0       │
│ Tokyo      ┆ 48   ┆ 1807.0       │
│ Tokyo      ┆ 49   ┆ 3425.0       │
│ Tokyo      ┆ 50   ┆ 7256.0       │
│ Tokyo      ┆ 51   ┆ 16727.0      │
│ Tokyo      ┆ 52   ┆ 23625.0      │
└────────────┴──────┴──────────────┘
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

```text
shape: (48, 3)
┌────────────┬───────┬───────────────┐
│ prefecture ┆ count ┆ prefecture_id │
│ ---        ┆ ---   ┆ ---           │
│ str        ┆ f64   ┆ str           │
╞════════════╪═══════╪═══════════════╡
│ Aichi      ┆ 7.0   ┆ JP-23         │
│ Akita      ┆ 0.0   ┆ JP-05         │
│ Aomori     ┆ 0.0   ┆ JP-02         │
│ Chiba      ┆ 12.0  ┆ JP-12         │
│ Ehime      ┆ 0.0   ┆ JP-38         │
│ Fukui      ┆ 0.0   ┆ JP-18         │
│ Fukuoka    ┆ 5.0   ┆ JP-40         │
│ Fukushima  ┆ 0.0   ┆ JP-07         │
│ Gifu       ┆ 2.0   ┆ JP-21         │
│ Gunma      ┆ 1.0   ┆ JP-10         │
│ …          ┆ …     ┆ …             │
│ Tochigi    ┆ 1.0   ┆ JP-09         │
│ Tokushima  ┆ 1.0   ┆ JP-36         │
│ Tokyo      ┆ 16.0  ┆ JP-13         │
│ Total No.  ┆ 135.0 ┆ null          │
│ Tottori    ┆ 0.0   ┆ JP-31         │
│ Toyama     ┆ 0.0   ┆ JP-16         │
│ Wakayama   ┆ 0.0   ┆ JP-30         │
│ Yamagata   ┆ 0.0   ┆ JP-06         │
│ Yamaguchi  ┆ 3.0   ┆ JP-35         │
│ Yamanashi  ┆ 0.0   ┆ JP-19         │
└────────────┴───────┴───────────────┘
```

## Notes

- `jp.get_data()` is the recommended default for most analytical tasks.
- Use `jp.load("sex")`, `jp.load("place")`, `jp.load("bullet")`, `jp.load("sentinel")` for source-specific studies.
- `unified` is category-normalized to `total` and source-aware (`Confirmed cases`, `All-case reporting`, `Sentinel surveillance`).
