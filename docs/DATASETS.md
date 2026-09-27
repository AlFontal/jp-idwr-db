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

### `sex` (historical confirmed, sex categories)

- Coverage: `1999-2023`
- Categories: `total`, `male`, `female`
- Grain: prefecture x year x week x disease x category
- Source label: `Confirmed cases`

### `place` (historical confirmed, place-of-infection categories)

- Coverage: `2001-2023`
- Categories: `total`, `japan`, `others`, `unknown`
- Grain: prefecture x year x week x disease x category
- Source label: `Confirmed cases`

### `bullet` (modern weekly all-case / zensu)

- Coverage: `2024+`
- Grain: prefecture x year x week x disease
- Source label: `All-case reporting`

### `sentinel` (weekly sentinel / teitenrui)

- Coverage: `2012+` (2012 is partial year)
- Grain: prefecture x year x week x disease
- Metrics: `count`, `per_sentinel`
- `count` is converted to weekly incidence from teitenrui cumulative reports:
  `weekly_count_t = cumulative_t - cumulative_{t-1}` within each year/prefecture/disease
  (first observed week is kept as-is).
- Source label: `Sentinel surveillance`

### `unified` (recommended analysis table)

- Composition:
  - historical **sex dataset only** (category normalized to `total`)
  - modern `bullet`
  - `sentinel` rows for disease-years without confirmed or all-case coverage
    (currently all sentinel rows, including pertussis 2012-2017 before it
    became all-case notifiable in 2018)
- The `place` dataset is **not fused** into unified.
- Category policy: unified keeps only `category = total`.

### `prefecture_en` (prefecture lookup table)

- Coverage: all 47 prefectures in English
- Grain: one row per prefecture
- Intended use: joins, validation, and helper lookups

## Snapshot Metrics

### `sex_prefecture.parquet`

- Rows: `12,965,937`
- Columns: `prefecture, year, week, date, count, category, disease, source`
- Years: `1999-2023`
- Prefectures: `47`
- Diseases: `94`

### `place_prefecture.parquet`

- Rows: `16,498,880`
- Columns: `prefecture, year, week, date, count, category, disease, source`
- Years: `2001-2023`
- Prefectures: `47`
- Diseases: `96`

### `bullet.parquet`

- Rows: `573,494`
- Columns: `prefecture, disease, count, year, week, date, source`
- Years: `2024-2026`
- Prefectures: `47`
- Diseases: `88`

### `sentinel.parquet`

- Rows: `634,801`
- Columns: `prefecture, disease, year, week, date, count, per_sentinel, source`
- Years: `2012-2026`
- Prefectures: `47`
- Diseases: `20`
- Null `count` rows: `65,978` (`10.39%`), primarily missing baselines and source corrections

### `unified.parquet`

- Rows: `5,530,274`
- Columns: `prefecture, year, week, date, count, category, disease, source, per_sentinel`
- Years: `1999-2026`
- Prefectures: `47`
- Diseases: `115`
- Categories: `total` only
- Sources: `Confirmed cases`, `All-case reporting`, `Sentinel surveillance`

### `prefecture_en.parquet`

- Rows: `47`
- Columns: `prefecture`

## Known Source Anomalies

- Sentinel `2016-W37` contains 26 of 47 prefectures because the upstream CSV is
  truncated after Kyoto. Consumers aggregating that period should treat it as
  incomplete.

## Disease Names Across Periods

Disease names are kept exactly as published for each period. Series are **not**
merged across renames or reclassifications, because it has not been verified
that the case definitions are equivalent. A disease whose name first appears in
a later year usually became notifiable that year; see
[`DISEASES.md`](DISEASES.md) for first and last week per name.

Names below look related but are kept separate. Filter on every name that is
relevant to your analysis and check the case definitions before combining them.

| Names in the data (confirmed / all-case years) | Notes |
| --- | --- |
| `Acute poliomyelitis` (1999-2000, 2006-2026); `Poliomyelitis` (2001-2005) | Labels alternate by period; the data does not show whether definitions match. |
| `Acute viral hepatitis` (1999-2005); `Hepatitis A`, `Hepatitis E`, `Viral hepatitis(excluding hepatitis A and E)` (2006-2026) | One label until 2005, three labels from 2006. |
| `Infant botulism` (1999-2005); `Botulism` (2006-2026) | Consecutive periods with different labels; scope may differ. |
| `Meningococcal meningitis` (1999-2016); `Invasive meningococcal infection` (2013-2026) | Both exist in 2013-2016, so this is not a simple rename; adding them would double count. |
| `Avian influenza virus infection` (2006-2007); `Avian influenza H5N1` (2008-2026); `Avian influenza (exclud. Avian influenza H5N1)` (2008-2023); `Avian influenza H7N9` (2013-2026); `Avian influenza (exclud. Avian influenza both H5N1 and H7N9)` (2024-2026) | Subtype-based labels change over time; some overlap in years. |
| `Monkeypox` (2006-2022); `Mpox` (2023-2026) | Consecutive periods with different labels. |
| `A/H1N1` (2009-2023) | Present in the annual tables only; no all-case counterpart from 2024. |
| `Pertussis` sentinel (2012-2017); `Pertussis` confirmed / all-case (2018-2026) | Same name, different surveillance systems (sentinel sites vs all cases); counts are not comparable across 2017/2018. Use `source` to separate them. |

The confirmed series also changes source in 2024: 1999-2023 comes from the
annual tables (`Confirmed cases`) and 2024 onwards from the weekly reports
(`All-case reporting`). The two may not be strictly comparable at that boundary.

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
