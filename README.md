# jp-idwr-db
[![PyPI version](https://img.shields.io/pypi/v/jp-idwr-db)](https://pypi.org/project/jp-idwr-db/)
[![Python versions](https://img.shields.io/pypi/pyversions/jp-idwr-db)](https://pypi.org/project/jp-idwr-db/)
[![CI](https://img.shields.io/github/actions/workflow/status/AlFontal/jp-idwr-db/ci.yml?branch=main&label=CI)](https://github.com/AlFontal/jp-idwr-db/actions/workflows/ci.yml)
[![License: GPL-3.0-or-later](https://img.shields.io/badge/License-GPL--3.0--or--later-blue.svg)](https://github.com/AlFontal/jp-idwr-db/blob/main/LICENSE)

`jp-idwr-db` publishes Japan’s infectious disease surveillance data (NIID/JIHS IDWR) as a
versioned, language-agnostic data product: Parquet tables plus a machine-readable
`manifest.json` (and an optional DuckDB file with views).

The Python package adds a convenient API and local caching on top of those release assets.
Internally, data wrangling is Polars-first for speed and consistent transforms.

The goal is to skip the usual work of chasing week-by-week files across changing archives and formats, so you can get straight to building time series and doing epidemiology instead of spending hours on data munging.

## Install

```bash
pip install jp-idwr-db
```

## Quick Start

To fetch the full unified dataset with a single call:

```python
import jp_idwr_db as jp
import polars as pl

df = (
    jp.load("unified")
    .select(["date", "prefecture", "category", "disease", "count", "source"])
)
print(df)
```

```text
shape: (5_370_477, 6)
┌────────────┬────────────┬──────────┬─────────────────────────────┬───────┬────────────────────┐
│ date       ┆ prefecture ┆ category ┆ disease                     ┆ count ┆ source             │
│ ---        ┆ ---        ┆ ---      ┆ ---                         ┆ ---   ┆ ---                │
│ date       ┆ str        ┆ str      ┆ str                         ┆ f64   ┆ str                │
╞════════════╪════════════╪══════════╪═════════════════════════════╪═══════╪════════════════════╡
│ 1999-04-11 ┆ Aichi      ┆ total    ┆ AIDS                        ┆ 0.0   ┆ Confirmed cases    │
│ 1999-04-11 ┆ Aichi      ┆ total    ┆ Acute poliomyelitis         ┆ 0.0   ┆ Confirmed cases    │
│ 1999-04-11 ┆ Aichi      ┆ total    ┆ Acute viral hepatitis       ┆ 4.0   ┆ Confirmed cases    │
│ 1999-04-11 ┆ Aichi      ┆ total    ┆ Amebiasis                   ┆ 0.0   ┆ Confirmed cases    │
│ 1999-04-11 ┆ Aichi      ┆ total    ┆ Anthrax                     ┆ 0.0   ┆ Confirmed cases    │
│ …          ┆ …          ┆ …        ┆ …                           ┆ …     ┆ …                  │
│ 2026-02-09 ┆ Yamanashi  ┆ total    ┆ Viral hepatitis(excluding   ┆ 0.0   ┆ All-case reporting │
│            ┆            ┆          ┆ hepa…                       ┆       ┆                    │
│ 2026-02-09 ┆ Yamanashi  ┆ total    ┆ West Nile fever             ┆ 0.0   ┆ All-case reporting │
│ 2026-02-09 ┆ Yamanashi  ┆ total    ┆ Western equine encephalitis ┆ 0.0   ┆ All-case reporting │
│ 2026-02-09 ┆ Yamanashi  ┆ total    ┆ Yellow fever                ┆ 0.0   ┆ All-case reporting │
│ 2026-02-09 ┆ Yamanashi  ┆ total    ┆ Zika virus infection        ┆ 0.0   ┆ All-case reporting │
└────────────┴────────────┴──────────┴─────────────────────────────┴───────┴────────────────────┘
```

You can also filter at the source with `jp.get_data(...)`:

```python

# Fetch only tuberculosis data for 2024 in Tokyo, Osaka, and Hokkaido
tb = (
    jp.get_data(
        disease="Tuberculosis",
        year=2024,
        prefecture=["Tokyo", "Osaka", "Hokkaido"])
    .select(["date", "prefecture", "disease", "count", "source"])
)
print(tb)
```

```text
shape: (156, 5)
┌────────────┬────────────┬──────────────┬───────┬────────────────────┐
│ date       ┆ prefecture ┆ disease      ┆ count ┆ source             │
│ ---        ┆ ---        ┆ ---          ┆ ---   ┆ ---                │
│ date       ┆ str        ┆ str          ┆ f64   ┆ str                │
╞════════════╪════════════╪══════════════╪═══════╪════════════════════╡
│ 2024-01-01 ┆ Hokkaido   ┆ Tuberculosis ┆ 2.0   ┆ All-case reporting │
│ 2024-01-01 ┆ Osaka      ┆ Tuberculosis ┆ 3.0   ┆ All-case reporting │
│ 2024-01-01 ┆ Tokyo      ┆ Tuberculosis ┆ 15.0  ┆ All-case reporting │
│ 2024-01-08 ┆ Hokkaido   ┆ Tuberculosis ┆ 4.0   ┆ All-case reporting │
│ 2024-01-08 ┆ Osaka      ┆ Tuberculosis ┆ 17.0  ┆ All-case reporting │
│ …          ┆ …          ┆ …            ┆ …     ┆ …                  │
│ 2024-12-16 ┆ Osaka      ┆ Tuberculosis ┆ 17.0  ┆ All-case reporting │
│ 2024-12-16 ┆ Tokyo      ┆ Tuberculosis ┆ 41.0  ┆ All-case reporting │
│ 2024-12-23 ┆ Hokkaido   ┆ Tuberculosis ┆ 5.0   ┆ All-case reporting │
│ 2024-12-23 ┆ Osaka      ┆ Tuberculosis ┆ 16.0  ┆ All-case reporting │
│ 2024-12-23 ┆ Tokyo      ┆ Tuberculosis ┆ 53.0  ┆ All-case reporting │
└────────────┴────────────┴──────────────┴───────┴────────────────────┘
```

```python

# Sentinel-only diseases from recent years in Tokyo prefecture
sentinel_df = (
    jp.get_data(
        source="sentinel",
        prefecture="Tokyo",
        year=(2024, 2026))
    .select(["date", "prefecture", "disease", "count", "per_sentinel"])
)
print(sentinel_df)
```

```text
shape: (2_052, 5)
┌────────────┬────────────┬─────────────────────────────────┬─────────┬──────────────┐
│ date       ┆ prefecture ┆ disease                         ┆ count   ┆ per_sentinel │
│ ---        ┆ ---        ┆ ---                             ┆ ---     ┆ ---          │
│ date       ┆ str        ┆ str                             ┆ f64     ┆ f64          │
╞════════════╪════════════╪═════════════════════════════════╪═════════╪══════════════╡
│ 2024-01-07 ┆ Tokyo      ┆ Acute hemorrhagic conjunctivit… ┆ null    ┆ null         │
│ 2024-01-07 ┆ Tokyo      ┆ Aseptic meningitis              ┆ null    ┆ null         │
│ 2024-01-07 ┆ Tokyo      ┆ Bacterial meningitis            ┆ null    ┆ null         │
│ 2024-01-07 ┆ Tokyo      ┆ COVID-19                        ┆ 1365.0  ┆ 3.38         │
│ 2024-01-07 ┆ Tokyo      ┆ Chickenpox                      ┆ 31.0    ┆ 0.12         │
│ …          ┆ …          ┆ …                               ┆ …       ┆ …            │
│ 2026-01-25 ┆ Tokyo      ┆ Influenza(excld. avian influen… ┆ 13082.0 ┆ 34.07        │
│ 2026-01-25 ┆ Tokyo      ┆ Mumps                           ┆ 30.0    ┆ 0.12         │
│ 2026-01-25 ┆ Tokyo      ┆ Mycoplasma pneumonia            ┆ 32.0    ┆ 1.28         │
│ 2026-01-25 ┆ Tokyo      ┆ Pharyngoconjunctival fever      ┆ 115.0   ┆ 0.47         │
│ 2026-01-25 ┆ Tokyo      ┆ Respiratory syncytial virus in… ┆ 242.0   ┆ 1.0          │
└────────────┴────────────┴─────────────────────────────────┴─────────┴──────────────┘
```

<details>
<summary><strong>Data Download Model</strong></summary>

- Package wheels do not ship the large parquet tables.
- On first call to `jp.load(...)` (or `jp.get_data(...)`), the package downloads versioned parquet assets listed in a release `manifest.json`.
- Cache path defaults to:
  - macOS: `~/Library/Caches/jp_idwr_db/data/<version>/`
  - Linux: `~/.cache/jp_idwr_db/data/<version>/`
  - Windows: `%LOCALAPPDATA%\\jp_idwr_db\\Cache\\data\\<version>\\`

Prefetch explicitly:

```bash
python -m jp_idwr_db data download
python -m jp_idwr_db data download --version v0.2.2 --force
```

Environment overrides:

- `JPINFECT_DATA_VERSION`: choose a specific release tag (example: `v0.2.2`)
- `JPINFECT_DATA_BASE_URL`: override asset host base URL
- `JPINFECT_CACHE_DIR`: override local cache root
</details>

## Language-independent data access

Release data assets are published as:

- `manifest.json`
- one or more `.parquet` tables (including `unified.parquet`)
- optional `jp_idwr_db.duckdb` (views over the parquet files)

Manifest schema reference: [`docs/manifest.schema.json`](./docs/manifest.schema.json).

Compatibility note: releases up to and including `v0.2.4` use legacy assets
(`jp_idwr_db-manifest.json` + `jp_idwr_db-parquet.zip`). The `manifest.json` +
direct parquet/duckdb layout starts at `v0.2.5`.

Fetch the manifest:

```bash
curl -L "https://github.com/AlFontal/jp-idwr-db/releases/download/<tag>/manifest.json"
```

Query with DuckDB CLI (when `jp_idwr_db.duckdb` and parquet files are in the same directory):

```bash
duckdb jp_idwr_db.duckdb -c "SELECT year, week, COUNT(*) AS rows FROM unified GROUP BY 1,2 ORDER BY 1 DESC, 2 DESC LIMIT 5;"
```

### Download assets for any language

```bash
TAG=v0.2.5
BASE="https://github.com/AlFontal/jp-idwr-db/releases/download/${TAG}"

mkdir -p jp-idwr-assets
cd jp-idwr-assets
curl -L -O "${BASE}/manifest.json"
curl -L -O "${BASE}/unified.parquet"
curl -L -O "${BASE}/jp_idwr_db.duckdb"
```

### R example (DuckDB, local)

This example opens the local `jp_idwr_db.duckdb` artifact (downloaded with the parquet files)
and queries the `unified` view:

```r
con <- DBI::dbConnect(duckdb::duckdb(), "jp_idwr_db.duckdb", read_only = TRUE)

tb <- DBI::dbGetQuery(
  con,
  "SELECT date, prefecture, disease, count, source
   FROM unified
   WHERE year = 2024 AND disease = 'Tuberculosis'
   ORDER BY date, prefecture
   LIMIT 20"
)

print(tb)
DBI::dbDisconnect(con, shutdown = TRUE)
```

### R example (Arrow, remote)

You can also query the parquet files directly from the GitHub Release URL without downloading first:

```r

tag <- "v0.2.5"
url <- sprintf(
  "https://github.com/AlFontal/jp-idwr-db/releases/download/%s/unified.parquet",
  tag
)

tb <- arrow::read_parquet(url) %>%
  dplyr::filter(year == 2024, disease == "Tuberculosis") %>%
  dplyr::select(date, prefecture, disease, count, source)

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


## Datasets

Use `jp.load(...)` with:

- `"sex"`: historical sex-disaggregated surveillance
- `"place"`: historical place-category surveillance
- `"bullet"`: modern all-case weekly reports (rapid zensu)
- `"sentinel"`: sentinel reports (teitenrui; 2012+ in release data assets)
- `"unified"`: deduplicated combined dataset (sex-total + modern bullet/sentinel, recommended)

Note: teitenrui CSVs report year-to-date cumulative counts. `jp-idwr-db` converts these to
weekly incidence (`count_t - count_{t-1}` within year/prefecture/disease; first week kept as-is).

Detailed schema and coverage are documented in [DATASETS.md](./docs/DATASETS.md).

## Raw Download and Parsing

Raw file workflows are available in `jp_idwr_db.io`:

- `jp_idwr_db.io.download(...)`
- `jp_idwr_db.io.download_recent(...)`
- `jp_idwr_db.io.read(...)`

These are useful for refreshing local raw weekly files or debugging parser behavior.

## Data Wrangling Examples

See [EXAMPLES.md](./docs/EXAMPLES.md) for data wrangling recipes (grouping, trends, regional slices, source-aware filtering).

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

# Build release data assets (manifest + duckdb + parquet metadata)
uv run --with duckdb jp-idwr-db-build-assets \
  --data-dir data/parquet \
  --release-tag v0.2.5 \
  --base-url https://github.com/AlFontal/jp-idwr-db/releases/download/v0.2.5
```

## Security and Integrity

- Release assets include a `manifest.json` with SHA256 checksums and file sizes.
- `ensure_data()` verifies each downloaded parquet checksum and size before marking cache complete.
- For PyPI publishing, prefer Trusted Publishing (OIDC) over long-lived API tokens.

## License

GPL-3.0-or-later. See [LICENSE](./LICENSE).
