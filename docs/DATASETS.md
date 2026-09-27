# Dataset Reference

This document describes the parquet datasets published as GitHub Release assets.
At runtime they are cached under `~/.cache/jp_idwr_db/data/<version>/` (OS-specific via `platformdirs`).

All figures below reflect the repository snapshot on **2026-09-27**.

The `date` column is the Monday at the start of the ISO surveillance week in
every published dataset. Use `year` and `week` as the canonical surveillance
period identifiers.

## Overview

`jp_idwr_db` provides six published datasets:

- `sex_prefecture.parquet`
- `place_prefecture.parquet`
- `bullet.parquet`
- `sentinel.parquet`
- `unified.parquet`
- `prefecture_en.parquet`

Load with:

- `jp.load("sex")` -> `sex_prefecture.parquet`
- `jp.load("place")` -> `place_prefecture.parquet`
- `jp.load("bullet")` -> `bullet.parquet`
- `jp.load("sentinel")` -> `sentinel.parquet`
- `jp.load("unified")` -> `unified.parquet`
- `jp.load_prefecture_en()` -> values from `prefecture_en.parquet`

## Dataset Roles

IDWR publishes each year twice: **preliminary** weekly reports during the year,
and **final** annual tables about 15 months after it ends. Final tables include
cases reported late, so they are higher than the preliminary reports for
diseases with reporting delays (for 2024: syphilis +61%, tuberculosis +31%,
all notifiable diseases +36%). `sex`, `place`, `sentinel` and `unified` use
the final annual tables for every year they cover and the preliminary reports
only after that; `bullet` always holds the preliminary reports. When a new
annual table is published, the automated refresh switches that year over, but
only once the table covers every week and prefecture. Years already switched
are not re-read: IDWR occasionally corrects an annual table in place (see its
[corrections list](https://id-info.jihs.go.jp/surveillance/idwr/annual/correction/index.html));
picking up such a correction needs a deliberate rebuild of that year.

### `sex` (confirmed cases, sex categories)

- Coverage: `1999` onwards, every year with a final annual table (see Snapshot Metrics)
- Categories: `total`, `male`, `female`
- `total` can exceed `male + female`: from 2022 a few cases are recorded with
  neither sex (9 prefecture-weeks in 2022-2024)
- Grain: prefecture x year x week x disease x category
- Source label: `Confirmed cases`

### `place` (confirmed cases, place-of-infection categories)

- Coverage: `2001` onwards, every year with a final annual table (see Snapshot Metrics)
- Categories: `total`, `japan`, `others`, `unknown`
- Grain: prefecture x year x week x disease x category
- Source label: `Confirmed cases`

### `bullet` (preliminary weekly all-case reports / zensu)

- Coverage: `2024+` (kept for every year, including years that now have annual tables)
- Grain: prefecture x year x week x disease
- Source label: `All-case reporting`

### `sentinel` (weekly sentinel surveillance / teitenrui)

- Coverage: `1999-W14` onwards
- Grain: prefecture x year x week x disease
- Metrics: `count`, `per_sentinel`, plus `count_status`
- Source label: `Sentinel surveillance`

#### Sources and `count_status`

Years with a final annual table (table 8-1 for counts and 8-2 for cases per
sentinel; 1-2 in 1999-2000) use it directly. Later years are derived from the
preliminary teitenrui files, which report year-to-date totals `C(t)`. IDWR
tables write zero as `-` and "not reported" as `…`. Nothing is imputed, and
`count_status` says why each value is what it is:

| `count_status` | `count` | Meaning |
| --- | --- | --- |
| `annual` | value | Final weekly count from the annual table |
| `derived` | value | Preliminary: `C(t) - C(t-1)` for consecutive weeks that do not decrease; week 1 is `C(1)` |
| `inconsistent` | null | Preliminary: depends on a total that is out of line. After a decrease `C(t) < C(t-1)`: weeks `t` and `t+1` if `C(t)` is too low (recovers next week), `t-1` and `t` if `C(t-1)` is too high (`C(t) >= C(t-2)`), `t-1` to `t+1` if both fit, or `t` to the recovery week for dips of up to 4 weeks. The combined total over the blank weeks is still known from the totals on either side |
| `correction` | null | Preliminary: lasting decrease (source correction or reset) that fits none of the above; later weeks continue from the new level |
| `gap` | null | Preliminary: the previous week's total is missing |
| `series_start` | null | Preliminary: first observed week of the year is not week 1 |
| `missing` | null | Not reported: `…` in the annual table, a blank preliminary total, or a prefecture-week with no reporting sentinel |

`per_sentinel` is null wherever `count` is. It is also null for 1999-2000,
whose rate table (4-3) is organised by week and not parsed. The annual rate
tables omit RSV before 2018; for those years the RSV rate is RSV cases divided
by the number of pediatric sentinels implied by the other pediatric diseases'
published rates (the denominator the weekly reports used). For analyses that
need a complete series, impute from these statuses rather than treating nulls
as zero.

Rows outside a disease's sentinel surveillance window are not published: the
annual tables print zeros for rotavirus before 2013-W42, COVID-19 before
2023-W19, and acute encephalitis after 2003-W45 (all-case reporting from
November 2003).

### `unified` (recommended analysis table)

- Composition:
  - final annual confirmed totals (`sex` dataset, category `total`) for every year they cover
  - preliminary `bullet` reports only for later years
  - `sentinel` rows for disease-years without confirmed or all-case coverage
    (currently all sentinel rows)
- The `place` dataset is **not fused** into unified.
- Category policy: unified keeps only `category = total`.

### `prefecture_en` (prefecture lookup table)

- Coverage: all 47 prefectures in English
- Grain: one row per prefecture
- Intended use: joins, validation, and helper lookups

## Snapshot Metrics

### `sex_prefecture.parquet`

- Rows: `13,611,153`
- Columns: `prefecture, year, week, date, count, category, disease, source`
- Years: `1999-2024`
- Prefectures: `47`
- Diseases: `94`

### `place_prefecture.parquet`

- Rows: `17,359,168`
- Columns: `prefecture, year, week, date, count, category, disease, source`
- Years: `2001-2024`
- Prefectures: `47`
- Diseases: `96`

### `bullet.parquet`

- Rows: `573,494`
- Columns: `prefecture, disease, count, year, week, date, source`
- Years: `2024-2026`
- Prefectures: `47`
- Diseases: `88`

### `sentinel.parquet`

- Rows: `1,288,176`
- Columns: `prefecture, disease, year, week, date, count, per_sentinel, source, count_status`
- Years: `1999-2026`
- Prefectures: `47`
- Diseases: `27`
- Null `count` rows: `344` (`0.03%`), reasons in `count_status`

### `unified.parquet`

- Rows: `6,186,093`
- Columns: `prefecture, year, week, date, count, category, disease, source, per_sentinel, count_status`
- Years: `1999-2026`
- Prefectures: `47`
- Diseases: `121`
- Categories: `total` only
- Sources: `Confirmed cases`, `All-case reporting`, `Sentinel surveillance`

### `prefecture_en.parquet`

- Rows: `47`
- Columns: `prefecture`

## Known Source Anomalies

- The preliminary sentinel CSV for `2016-W37` is truncated after Kyoto (26 of
  47 prefectures). 2016 now comes from the annual table, which is complete.
- Fukushima reported no sentinel data in `2011-W10` to `2011-W14` (after the
  Great East Japan Earthquake); the annual tables print zeros with blank rates,
  which are published as `missing`.
- In early 2006 a few prefectures have `…` (not reported) for hospital-sentinel
  diseases; these are `missing`.

## Disease Names Across Periods

Disease names are kept as published. They are only merged when the source's
Japanese label is identical and the English label differs in spelling or
translation, for example `Erythema infectiosum` (sentinel 1999-2005) and
`Erythema infection` (2006 onwards), both `伝染性紅斑`. The full list is
`SENTINEL_NAME_HARMONIZATION` in `jp_idwr_db.io`. Series are **not** merged
across definitional changes. A disease whose name first appears in a later
year usually became notifiable that year; see [`DISEASES.md`](DISEASES.md) for
first and last week per name.

Names below look related but are kept separate. Filter on every name that is
relevant to your analysis and check the case definitions before combining them.

| Names in the data (years) | Notes |
| --- | --- |
| `Acute poliomyelitis` (1999-2000, 2006-2026); `Poliomyelitis` (2001-2005) | Labels alternate by period; the data does not show whether definitions match. |
| `Acute viral hepatitis` (1999-2005); `Hepatitis A`, `Hepatitis E`, `Viral hepatitis(excluding hepatitis A and E)` (2006-2026) | One label until 2005, three labels from 2006. |
| `Infant botulism` (1999-2005); `Botulism` (2006-2026) | Consecutive periods with different labels; scope may differ. |
| `Meningococcal meningitis` (1999-2016); `Invasive meningococcal infection` (2013-2026) | Both exist in 2013-2016, so this is not a simple rename; adding them would double count. |
| `Avian influenza virus infection` (2006-2007); `Avian influenza H5N1` (2008-2026); `Avian influenza (exclud. Avian influenza H5N1)` (2008 onwards, annual tables); `Avian influenza H7N9` (2013-2026); `Avian influenza (exclud. Avian influenza both H5N1 and H7N9)` (preliminary reports) | The annual tables and the weekly reports label the residual category differently; the Japanese labels cannot be compared, so they stay separate. |
| `Monkeypox` (2006-2022); `Mpox` (2023-2026) | Consecutive periods with different labels. |
| `A/H1N1` (2009 onwards, annual tables) | Present in the annual tables only; no counterpart in the weekly reports. |
| Sentinel `Influenza` (1999-2005); `Influenza(excld. avian influenza virus infection)` (2006-2007); `Influenza(excld. avian influenza and pandemic influenza)` (2008 onwards) | Same Japanese label, but the legal definition excluded avian (2006) and pandemic (2008) influenza. |
| Sentinel `Measles(excluding measles in adults)` (1999-2005); `Measles(excluding adults)` (2006-2007); `Measles in adults` (1999-2007); confirmed `Measles` (2008 onwards) | Sentinel measles ended when measles became all-case notifiable in 2008. |
| `Pertussis` sentinel (1999-2017); `Pertussis` confirmed / all-case (2018 onwards) | Same name, different surveillance systems (sentinel sites vs all cases); counts are not comparable across 2017/2018. Use `source` to separate them. |
| `Rubella` sentinel (1999-2007); `Rubella` confirmed / all-case (2008 onwards) | Same name, different surveillance systems; not comparable across 2007/2008. Use `source`. |

The confirmed series changes from final annual tables (`Confirmed cases`) to
preliminary weekly reports (`All-case reporting`) after the last year with an
annual table (the last year of `sex_prefecture.parquet`). Preliminary weekly
counts are lower because late reports are not assigned back to their week (the
reports' year-to-date totals do include them), so a drop across that boundary
is expected. See [`DISEASES.md`](DISEASES.md) for exact first and last weeks.

## Prefecture IDs

To avoid increasing parquet storage, ISO prefecture IDs are not materialized in
the datasets by default. Add them when needed:

```python
import jp_idwr_db as jp

df = jp.load("unified")
df = jp.attach_prefecture_id(df)  # adds prefecture_id (JP-01 ... JP-47)
```

Or get the standalone mapping:

```python
pref_map = jp.prefecture_map()
```
