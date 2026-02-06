# Bundled Dataset Reference

This document describes the bundled parquet datasets shipped in `src/jpinfectpy/data/` and how they relate to each other.

All numbers below are from the repository snapshot on **2026-02-06**.

## Overview

`jpinfectpy` bundles five analytical datasets:

- `sex_prefecture.parquet`
- `place_prefecture.parquet`
- `bullet.parquet`
- `sentinel.parquet`
- `unified.parquet`

Use:

- `jp.load("sex")` -> `sex_prefecture.parquet`
- `jp.load("place")` -> `place_prefecture.parquet`
- `jp.load("bullet")` -> `bullet.parquet`
- `jp.load("sentinel")` -> `sentinel.parquet`
- `jp.load("unified")` / `jp.load_all()` -> `unified.parquet`

## Dataset Nature

### `sex` (historical confirmed case reporting)
- Time span: `1999` to `2023`
- Granularity: prefecture x week x disease x category
- Categories: `total`, `male`
- Primary use: long historical trend analysis for confirmed-case records.

### `place` (historical place-category reporting)
- Time span: `2001` to `2023`
- Granularity: prefecture x week x disease x category
- Categories: `total`, `japan`, `others`, `unknown`
- Primary use: domestic/imported/unknown-style place-category analysis.

### `bullet` (modern rapid all-case reports / zensu)
- Time span: `2024+`
- Granularity: prefecture x week x disease
- Includes `source = "All-case reporting"`
- Primary use: most recent weekly all-case monitoring.

### `sentinel` (modern rapid sentinel reports / teitenrui)
- Time span: `2024+`
- Granularity: prefecture x week x disease
- Includes `count` and `per_sentinel`
- Includes `source = "Sentinel surveillance"`
- Primary use: weekly sentinel metrics for common infectious diseases.

### `unified` (recommended)
- Combines historical (`sex`, `place`) plus modern (`bullet`, `sentinel`).
- Applies smart merge logic for modern overlap:
  - keeps all modern all-case (`bullet`) rows
  - includes only sentinel-exclusive diseases from `sentinel`
- Includes source labels and a superset schema.

## Schema Summary

### `sex_prefecture.parquet`
- Rows: `8,635,686`
- Columns: `prefecture, year, week, date, count, category, disease, source`
- Year range: `1999-2023`
- Prefectures: `47`
- Diseases: `98`
- Source values: `Confirmed cases`

### `place_prefecture.parquet`
- Rows: `22,061,988`
- Columns: `prefecture, year, week, date, count, category, disease, source`
- Year range: `2001-2023`
- Prefectures: `47`
- Diseases: `100`
- Source values: `Confirmed cases`

### `bullet.parquet`
- Rows: `459,360`
- Columns: `prefecture, disease, count, week, source, year, date`
- Year range: `2024-2026`
- Prefectures: `48`
- Diseases: `87`
- Source values: `All-case reporting`

### `sentinel.parquet`
- Rows: `96,444`
- Columns: `prefecture, disease, year, week, date, count, per_sentinel, source`
- Year range: `2024-2026`
- Prefectures: `47`
- Diseases: `20`
- Source values: `Sentinel surveillance`
- Non-null `per_sentinel`: `87,136`
- Rows per week are stable in current snapshot (`893` each week across `108` weeks).

### `unified.parquet`
- Rows: `20,047,362`
- Columns: `prefecture, year, week, date, count, category, disease, source, per_sentinel`
- Year range: `1999-2026`
- Prefectures: `48`
- Diseases: `119`
- Source values:
  - `Confirmed cases`: `19,562,622` rows
  - `All-case reporting`: `459,360` rows
  - `Sentinel surveillance`: `25,380` rows

## Sentinel Integration Behavior in Unified

`sentinel.parquet` includes `20` diseases, but only sentinel-exclusive diseases are retained in `unified.parquet` for overlap years. In the current snapshot, unified retains these sentinel diseases:

- `Aseptic meningitis`
- `Hand, foot and mouth disease`
- `Herpangina`
- `Mycoplasma pneumonia`
- `Respiratory syncytial virus infection`

This is intentional dedup behavior to avoid double-counting diseases already covered by modern all-case reporting.

## Practical Guidance

- Use `jp.load("unified")` for most downstream analyses.
- Use `jp.load("sentinel")` when you need full sentinel disease coverage and `per_sentinel` metrics.
- Use `jp.load("bullet")` for modern all-case-only analyses.
- Use `sex`/`place` for historical category-specific studies.

## Reproducibility Snippet

```python
import polars as pl
from pathlib import Path

for fn in [
    "sex_prefecture.parquet",
    "place_prefecture.parquet",
    "bullet.parquet",
    "sentinel.parquet",
    "unified.parquet",
]:
    df = pl.read_parquet(Path("src/jpinfectpy/data") / fn)
    print(fn, df.height, df.columns)
```
