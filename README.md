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
print(df.shape)
print(df.select(["year", "week"]).max())
```

```python
import jp_idwr_db as jp

# Optional: attach ISO prefecture IDs (JP-01 ... JP-47) only when needed
df_with_ids = jp.attach_prefecture_id(df, prefecture_col="prefecture", id_col="prefecture_id")
print(df_with_ids.select(["prefecture", "prefecture_id"]).head())
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
print(tb.shape)
print(tb["year"].min(), tb["year"].max())
```

```python
import jp_idwr_db as jp

# Sentinel-only diseases from recent years
sentinel = jp.get_data(source="sentinel", year=(2023, 2026))
print(sentinel["disease"].n_unique())
print(sentinel.select(["year", "week"]).max())
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
