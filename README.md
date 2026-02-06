# jpinfectpy

Python access to Japanese infectious disease surveillance data from NIID/JIHS.

`jpinfectpy` bundles historical and modern surveillance datasets in Parquet format and provides a Polars/Pandas-friendly API for filtering and analysis.

## Install

```bash
pip install jpinfectpy
```

## Quick Start

```python
import jpinfectpy as jp

# Full bundled dataset (recommended)
df = jp.load("unified", return_type="polars")
print(df.shape)
print(df.select(["year", "week"]).max())
```

```python
import jpinfectpy as jp

# Convenience equivalent to load("unified")
df_all = jp.load_all(return_type="polars")
print(df_all["source"].unique().sort())
```

## Main API

Top-level API exported by `jpinfectpy`:

- `load(name, return_type=None)`
- `load_all(return_type=None)`
- `get_data(...)`
- `list_diseases(source="all")`
- `list_prefectures()`
- `get_latest_week()`
- `merge(...)`, `pivot(...)`
- `configure(...)`, `get_config()`
- `to_polars(...)`, `to_pandas(...)`

### Filtered Access with `get_data`

```python
import jpinfectpy as jp

# Tuberculosis rows for a year range
tb = jp.get_data(disease="Tuberculosis", year=(2018, 2023), return_type="polars")
print(tb.shape)
print(tb["year"].min(), tb["year"].max())
```

```python
import jpinfectpy as jp

# Sentinel-only diseases from recent years
sentinel = jp.get_data(source="sentinel", year=(2024, 2026), return_type="polars")
print(sentinel["disease"].n_unique())
print(sentinel.select(["year", "week"]).max())
```

## Bundled Datasets

Use `jp.load(...)` with:

- `"sex"`: historical sex-disaggregated surveillance
- `"place"`: historical place-category surveillance
- `"bullet"`: modern all-case weekly reports (rapid zensu)
- `"sentinel"`: modern sentinel weekly reports (rapid teitenrui)
- `"unified"`: deduplicated combined dataset (recommended)

Detailed schema and coverage are documented in [DATASETS.md](./DATASETS.md).

## Raw Download and Parsing

Raw file workflows are available in `jpinfectpy.io`:

- `jpinfectpy.io.download(...)`
- `jpinfectpy.io.download_recent(...)`
- `jpinfectpy.io.read(...)`

These are useful for refreshing local raw weekly files or debugging parser behavior.

## Data Wrangling Examples

See [EXAMPLES.md](./EXAMPLES.md) for Polars-first data wrangling recipes (grouping, trends, regional slices, source-aware filtering).

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
