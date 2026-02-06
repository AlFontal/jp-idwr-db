# jp-idwr-db

Python access to Japanese infectious disease surveillance data from NIID/JIHS.

`jp-idwr-db` provides a Polars-first API for filtering and analysis.
Parquet datasets are versioned as GitHub Release assets and downloaded to a local cache on first use.
It is inspired by the R package `jpinfect`, but it is not an API-parity port and includes independently curated ingestion and coverage.

## Install

```bash
pip install jp-idwr-db
```

## Data Download Model

- Package wheels do not ship the large parquet tables.
- On first call to `jp.load(...)` (or `jp.get_data(...)`), the package downloads versioned data assets from GitHub Releases.
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

## Quick Start

```python
import jp_idwr_db as jp
import polars as pl

# Full unified dataset (recommended)
pl.Config.set_tbl_rows(10)

df = (
    jp.load("unified")
    .select(["date", "prefecture", "category", "disease", "count", "source"])
    .sort(["date", "prefecture", "category", "disease"])
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
import polars as pl

# Tuberculosis rows for a year range
pl.Config.set_tbl_rows(10)

tb = (
    jp.get_data(disease="Tuberculosis", year=2024, prefecture=["Tokyo", "Osaka", "Hokkaido"])
    .select(["date", "prefecture", "disease", "count", "source"])
    .sort(["date", "prefecture"])
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
import jp_idwr_db as jp
import polars as pl

# Sentinel-only diseases from recent years
rsv = (
    jp.get_data(source="sentinel", disease="Respiratory syncytial virus infection", year=2024)
    .select(["date", "prefecture", "disease", "count", "per_sentinel"])
    .sort(["date", "prefecture"])
)
print(rsv)
```

```text
shape: (2_444, 5)
┌────────────┬────────────┬─────────────────────────────────┬────────┬──────────────┐
│ date       ┆ prefecture ┆ disease                         ┆ count  ┆ per_sentinel │
│ ---        ┆ ---        ┆ ---                             ┆ ---    ┆ ---          │
│ date       ┆ str        ┆ str                             ┆ f64    ┆ f64          │
╞════════════╪════════════╪═════════════════════════════════╪════════╪══════════════╡
│ 2024-01-07 ┆ Aichi      ┆ Respiratory syncytial virus in… ┆ 1.0    ┆ 0.01         │
│ 2024-01-07 ┆ Akita      ┆ Respiratory syncytial virus in… ┆ null   ┆ null         │
│ 2024-01-07 ┆ Aomori     ┆ Respiratory syncytial virus in… ┆ 1.0    ┆ 0.03         │
│ 2024-01-07 ┆ Chiba      ┆ Respiratory syncytial virus in… ┆ 8.0    ┆ 0.06         │
│ 2024-01-07 ┆ Ehime      ┆ Respiratory syncytial virus in… ┆ null   ┆ null         │
│ …          ┆ …          ┆ …                               ┆ …      ┆ …            │
│ 2024-12-29 ┆ Toyama     ┆ Respiratory syncytial virus in… ┆ 1371.0 ┆ 48.96        │
│ 2024-12-29 ┆ Wakayama   ┆ Respiratory syncytial virus in… ┆ 1702.0 ┆ 58.69        │
│ 2024-12-29 ┆ Yamagata   ┆ Respiratory syncytial virus in… ┆ 1795.0 ┆ 66.48        │
│ 2024-12-29 ┆ Yamaguchi  ┆ Respiratory syncytial virus in… ┆ 3118.0 ┆ 72.51        │
│ 2024-12-29 ┆ Yamanashi  ┆ Respiratory syncytial virus in… ┆ 510.0  ┆ 21.25        │
└────────────┴────────────┴─────────────────────────────────┴────────┴──────────────┘
```

## Datasets

Use `jp.load(...)` with:

- `"sex"`: historical sex-disaggregated surveillance
- `"place"`: historical place-category surveillance
- `"bullet"`: modern all-case weekly reports (rapid zensu)
- `"sentinel"`: sentinel weekly reports (teitenrui; 2012+ in release data assets)
- `"unified"`: deduplicated combined dataset (sex-total + modern bullet/sentinel, recommended)

Detailed schema and coverage are documented in [DATASETS.md](./docs/DATASETS.md).

## Optional Prefecture IDs

Attach ISO prefecture IDs (JP-01 ... JP-47) only when needed:

```python
import jp_idwr_db as jp

df_with_ids = (
    jp.get_data(disease="Measles", year=2024)
    .select(["prefecture", "disease", "count"])
    .sort(["prefecture", "count"])
    .unique(subset=["prefecture"], keep="first")
    .pipe(jp.attach_prefecture_id)
    .sort("prefecture")
)
print(df_with_ids)
```

```text
shape: (48, 4)
┌────────────┬─────────┬───────┬───────────────┐
│ prefecture ┆ disease ┆ count ┆ prefecture_id │
│ ---        ┆ ---     ┆ ---   ┆ ---           │
│ str        ┆ str     ┆ f64   ┆ str           │
╞════════════╪═════════╪═══════╪═══════════════╡
│ Aichi      ┆ Measles ┆ 0.0   ┆ JP-23         │
│ Akita      ┆ Measles ┆ 0.0   ┆ JP-05         │
│ Aomori     ┆ Measles ┆ 0.0   ┆ JP-02         │
│ Chiba      ┆ Measles ┆ 0.0   ┆ JP-12         │
│ Ehime      ┆ Measles ┆ 0.0   ┆ JP-38         │
│ …          ┆ …       ┆ …     ┆ …             │
│ Toyama     ┆ Measles ┆ 0.0   ┆ JP-16         │
│ Wakayama   ┆ Measles ┆ 0.0   ┆ JP-30         │
│ Yamagata   ┆ Measles ┆ 0.0   ┆ JP-06         │
│ Yamaguchi  ┆ Measles ┆ 0.0   ┆ JP-35         │
│ Yamanashi  ┆ Measles ┆ 0.0   ┆ JP-19         │
└────────────┴─────────┴───────┴───────────────┘
```

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

## Security and Integrity

- Release assets include a `jp_idwr_db-manifest.json` with SHA256 checksums.
- `ensure_data()` verifies archive checksum and each extracted parquet checksum before marking cache complete.
- For PyPI publishing, prefer Trusted Publishing (OIDC) over long-lived API tokens.

## License

GPL-3.0-or-later. See [LICENSE](./LICENSE).
