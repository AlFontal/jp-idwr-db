# jp-idwr-db

Python access to Japanese infectious disease surveillance data from NIID/JIHS.

`jp-idwr-db` bundles historical and modern surveillance datasets in Parquet format and provides a Polars-first API for filtering and analysis.
It is inspired by the R package `jpinfect`, but it is not an API-parity port and includes independently curated ingestion and coverage.

## Install

```bash
pip install jp-idwr-db
```

## Quick Start

```python
import jp_idwr_db as jp

# Full bundled dataset (recommended)
df = jp.load("unified")
print(df.select(["prefecture", "disease", "year", "week", "count", "source"]).head(8))
```

```text
shape: (8, 6)
┌────────────┬─────────────────────────────────┬──────┬──────┬───────┬───────────────────────┐
│ prefecture ┆ disease                         ┆ year ┆ week ┆ count ┆ source                │
│ ---        ┆ ---                             ┆ ---  ┆ ---  ┆ ---   ┆ ---                   │
│ str        ┆ str                             ┆ i32  ┆ i32  ┆ f64   ┆ str                   │
╞════════════╪═════════════════════════════════╪══════╪══════╪═══════╪═══════════════════════╡
│ Tochigi    ┆ Lyme disease                    ┆ 2011 ┆ 24   ┆ 0.0   ┆ Confirmed cases       │
│ Kochi      ┆ Avian influenza H5N1            ┆ 2008 ┆ 51   ┆ 0.0   ┆ Confirmed cases       │
│ Hokkaido   ┆ Dengue fever                    ┆ 1999 ┆ 28   ┆ 0.0   ┆ Confirmed cases       │
│ Tokyo      ┆ Congenital rubella syndrome     ┆ 2014 ┆ 41   ┆ 0.0   ┆ Confirmed cases       │
│ Nagasaki   ┆ Severe Acute Respiratory Syndr… ┆ 2018 ┆ 4    ┆ 0.0   ┆ Confirmed cases       │
│ Fukushima  ┆ Infectious gastroenteritis (on… ┆ 2019 ┆ 25   ┆ 145.0 ┆ Sentinel surveillance │
│ Nara       ┆ Severe invasive streptococcal … ┆ 2003 ┆ 10   ┆ 0.0   ┆ Confirmed cases       │
│ Mie        ┆ Plague                          ┆ 2006 ┆ 37   ┆ 0.0   ┆ Confirmed cases       │
└────────────┴─────────────────────────────────┴──────┴──────┴───────┴───────────────────────┘
```

```python
import jp_idwr_db as jp

# Optional: attach ISO prefecture IDs (JP-01 ... JP-47) only when needed
df_with_ids = jp.attach_prefecture_id(df, prefecture_col="prefecture", id_col="prefecture_id")
print(df_with_ids.select(["prefecture", "prefecture_id"]).head())
```

```text
shape: (5, 2)
┌────────────┬───────────────┐
│ prefecture ┆ prefecture_id │
╞════════════╪═══════════════╡
│ Tochigi    ┆ JP-09         │
│ Kochi      ┆ JP-39         │
│ Hokkaido   ┆ JP-01         │
│ Tokyo      ┆ JP-13         │
│ Nagasaki   ┆ JP-42         │
└────────────┴───────────────┘
```

## Main API

Top-level API exported by `jp_idwr_db`:

- `load(name)`
- `get_data(...)`
- `list_diseases(source="all")`
- `list_prefectures()`
- `get_latest_week()`
- `prefecture_map()`
- `attach_prefecture_id(df, prefecture_col="prefecture", id_col="prefecture_id")`
- `merge(...)`, `pivot(...)`
- `configure(...)`, `get_config()`

### Filtered Access with `get_data`

```python
import jp_idwr_db as jp

# Tuberculosis rows for a year range
tb = jp.get_data(disease="Tuberculosis", year=(2018, 2023))
print(tb.select(["prefecture", "disease", "year", "week", "count", "source"]).head(8))
```

```text
shape: (8, 6)
┌────────────┬──────────────┬──────┬──────┬───────┬─────────────────┐
│ prefecture ┆ disease      ┆ year ┆ week ┆ count ┆ source          │
│ ---        ┆ ---          ┆ ---  ┆ ---  ┆ ---   ┆ ---             │
│ str        ┆ str          ┆ i32  ┆ i32  ┆ f64   ┆ str             │
╞════════════╪══════════════╪══════╪══════╪═══════╪═════════════════╡
│ Hokkaido   ┆ Tuberculosis ┆ 2020 ┆ 12   ┆ 5.0   ┆ Confirmed cases │
│ Oita       ┆ Tuberculosis ┆ 2023 ┆ 38   ┆ 6.0   ┆ Confirmed cases │
│ Fukuoka    ┆ Tuberculosis ┆ 2021 ┆ 8    ┆ 12.0  ┆ Confirmed cases │
│ Kagawa     ┆ Tuberculosis ┆ 2020 ┆ 19   ┆ 2.0   ┆ Confirmed cases │
│ Chiba      ┆ Tuberculosis ┆ 2020 ┆ 19   ┆ 9.0   ┆ Confirmed cases │
│ Kanagawa   ┆ Tuberculosis ┆ 2022 ┆ 17   ┆ 25.0  ┆ Confirmed cases │
│ Okinawa    ┆ Tuberculosis ┆ 2021 ┆ 11   ┆ 4.0   ┆ Confirmed cases │
│ Gifu       ┆ Tuberculosis ┆ 2018 ┆ 23   ┆ 7.0   ┆ Confirmed cases │
└────────────┴──────────────┴──────┴──────┴───────┴─────────────────┘
```

```python
import jp_idwr_db as jp

# Sentinel-only diseases from recent years
sentinel = jp.get_data(source="sentinel", year=(2023, 2026))
print(sentinel.select(["prefecture", "disease", "year", "week", "count", "source"]).head(8))
```

```text
shape: (8, 6)
┌────────────┬─────────────────────────────────┬──────┬──────┬───────┬───────────────────────┐
│ prefecture ┆ disease                         ┆ year ┆ week ┆ count ┆ source                │
│ ---        ┆ ---                             ┆ ---  ┆ ---  ┆ ---   ┆ ---                   │
│ str        ┆ str                             ┆ i32  ┆ i32  ┆ f64   ┆ str                   │
╞════════════╪═════════════════════════════════╪══════╪══════╪═══════╪═══════════════════════╡
│ Ishikawa   ┆ Respiratory syncytial virus in… ┆ 2024 ┆ 42   ┆ 813.0 ┆ Sentinel surveillance │
│ Nara       ┆ Erythema infection              ┆ 2025 ┆ 31   ┆ 823.0 ┆ Sentinel surveillance │
│ Saga       ┆ Mumps                           ┆ 2024 ┆ 26   ┆ 14.0  ┆ Sentinel surveillance │
│ Hyogo      ┆ Pharyngoconjunctival fever      ┆ 2023 ┆ 19   ┆ 468.0 ┆ Sentinel surveillance │
│ Miyazaki   ┆ Infectious gastroenteritis      ┆ 2026 ┆ 3    ┆ 339.0 ┆ Sentinel surveillance │
│ Kagoshima  ┆ Infectious gastroenteritis (on… ┆ 2024 ┆ 9    ┆ null  ┆ Sentinel surveillance │
│ Osaka      ┆ Mumps                           ┆ 2024 ┆ 49   ┆ 404.0 ┆ Sentinel surveillance │
│ Aomori     ┆ Erythema infection              ┆ 2024 ┆ 10   ┆ 5.0   ┆ Sentinel surveillance │
└────────────┴─────────────────────────────────┴──────┴──────┴───────┴───────────────────────┘
```

## Bundled Datasets

Use `jp.load(...)` with:

- `"sex"`: historical sex-disaggregated surveillance
- `"place"`: historical place-category surveillance
- `"bullet"`: modern all-case weekly reports (rapid zensu)
- `"sentinel"`: sentinel weekly reports (teitenrui; 2012+ in bundled data)
- `"unified"`: deduplicated combined dataset (sex-total + modern bullet/sentinel, recommended)

Detailed schema and coverage are documented in [DATASETS.md](./docs/DATASETS.md).

## Raw Download and Parsing

Raw file workflows are available in `jp_idwr_db.io`:

- `jp_idwr_db.io.download(...)`
- `jp_idwr_db.io.download_recent(...)`
- `jp_idwr_db.io.read(...)`

These are useful for refreshing local raw weekly files or debugging parser behavior.

## Data Wrangling Examples

See [EXAMPLES.md](./docs/EXAMPLES.md) for Polars-first data wrangling recipes (grouping, trends, regional slices, source-aware filtering).

Disease-by-disease temporal coverage is documented in [DISEASES.md](./docs/DISEASES.md).

## Data Source

NIID/JIHS infectious disease surveillance publications:

- Historical annual archive files (`Syu_01_1`, `Syu_02_1`)
- Rapid weekly CSV reports (`zensuXX.csv`, `teitenruiXX.csv`)

## Development

```bash
uv sync --all-extras --dev
uv run ruff check .
uv run mypy src
uv run pytest
```

## License

GPL-3.0-or-later. See [LICENSE](./LICENSE).
