# Bundled Dataset Reference

This document describes the bundled parquet datasets in `src/jpinfectpy/data/`.

All figures below reflect the repository snapshot on **2026-02-06**.

## Overview

`jpinfectpy` ships five analytical datasets:

- `sex_prefecture.parquet`
- `place_prefecture.parquet`
- `bullet.parquet`
- `sentinel.parquet`
- `unified.parquet`

Load with:

- `jp.load("sex")` -> `sex_prefecture.parquet`
- `jp.load("place")` -> `place_prefecture.parquet`
- `jp.load("bullet")` -> `bullet.parquet`
- `jp.load("sentinel")` -> `sentinel.parquet`
- `jp.load("unified")` -> `unified.parquet`

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

### `sentinel` (modern weekly sentinel / teitenrui)

- Coverage: `2024+`
- Grain: prefecture x year x week x disease
- Metrics: `count`, `per_sentinel`
- Source label: `Sentinel surveillance`

### `unified` (recommended analysis table)

- Composition:
  - historical **sex dataset only** (category normalized to `total`)
  - modern `bullet`
  - sentinel-exclusive diseases from `sentinel` after smart merge
- The `place` dataset is **not fused** into unified.
- Category policy: unified keeps only `category = total`.

## Snapshot Metrics

### `sex_prefecture.parquet`

- Rows: `12,953,529`
- Columns: `prefecture, year, week, date, count, category, disease, source`
- Years: `1999-2023`
- Prefectures: `47`
- Diseases: `98`

### `place_prefecture.parquet`

- Rows: `22,061,988`
- Columns: `prefecture, year, week, date, count, category, disease, source`
- Years: `2001-2023`
- Prefectures: `47`
- Diseases: `100`

### `bullet.parquet`

- Rows: `459,360`
- Columns: `prefecture, disease, count, week, source, year, date`
- Years: `2024-2026`
- Prefectures: `48` (includes national total row)
- Diseases: `87`

### `sentinel.parquet`

- Rows: `96,444`
- Columns: `prefecture, disease, year, week, date, count, per_sentinel, source`
- Years: `2024-2026`
- Prefectures: `47`
- Diseases: `20`

### `unified.parquet`

- Rows: `4,802,583`
- Columns: `prefecture, year, week, date, count, category, disease, source, per_sentinel`
- Years: `1999-2026`
- Prefectures: `48`
- Diseases: `117`
- Categories: `total` only
- Sources: `Confirmed cases`, `All-case reporting`, `Sentinel surveillance`

## Prefecture IDs

To avoid increasing parquet storage, ISO prefecture IDs are not materialized in
the bundled datasets by default. Add them when needed:

```python
import jpinfectpy as jp

df = jp.load("unified", return_type="polars")
df = jp.attach_prefecture_id(df)  # adds prefecture_id (JP-01 ... JP-47)
```

Or get the standalone mapping:

```python
pref_map = jp.prefecture_map(return_type="polars")
```
