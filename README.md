# jpinfectpy

**Python access to Japanese infectious disease surveillance data from the National Institute of Infectious Diseases (NIID).**

jpinfectpy is a Python port of the R package [`jpinfect`](https://github.com/TomonoriHoshi/jpinfect), providing programmatic access to Japan's weekly infectious disease surveillance reports. It supports both historical data (1999-2023) and recent weekly bulletins (2024+) with a clean, Pythonic API.

## Features

- **Bundled Historical Data**: Pre-processed datasets (1999-2023) for instant analysis
- **Live Data Downloads**: Fetch latest weekly bulletins from NIID automatically
- **Dual DataFrame Support**: Returns Polars or Pandas DataFrames (your choice)
- **Smart Caching**: Disk cache with ETag support to avoid redundant downloads
- **Rate Limiting**: Polite HTTP client with configurable rate limits
- **Data Transformations**: Merge and pivot utilities for common analysis tasks

## Installation

```bash
# With pip
pip install jpinfectpy

# With uv (recommended for development)
uv pip install jpinfectpy
```

## Quick Start

```python
import jpinfectpy as jp

# Load bundled historical data (fastest, recommended for most use cases)
df_sex = jp.load("sex")          # Sex-disaggregated data (1999-2023)
df_place = jp.load("place")      # Place of infection data (2001-2023)

# Get all available data (historical + recent, auto-updates)
df_all = jp.load_all()

# Download and read specific year's raw data
path = jp.download("sex", 2024)
df_2024 = jp.read(path)

# Download recent weekly bulletins (2024+)
paths = jp.download_recent()
df_recent = jp.read(paths[0])
```

## Core API Functions

### Loading Bundled Data

**`load(name, return_type=None)`** - Load pre-processed historical datasets

```python
# Load sex-disaggregated surveillance data (1999-2023)
df_sex = jp.load("sex")

# Load as Polars DataFrame (faster for large data)
df_polars = jp.load("sex", return_type="polars")

# Available datasets: "sex", "place", "bullet"
```

**`load_all(return_type=None)`** - Load combined historical + recent data

```python
# Combines:
# - Historical sex-disaggregated data (1999-2023)
# - Recent weekly bulletins (2024+)
# Useful for time series analysis across the full data range
df = jp.load_all()
```

### Downloading Raw Data

**`download(name, year, out_dir=None, overwrite=False, week=None, lang="en")`** - Download data for a specific year

```python
# Download sex-disaggregated data for 2024
path = jp.download("sex", 2024)

# Download specific weeks of bullet data
paths = jp.download("bullet", 2024, week=[1, 2, 3], lang="en")

# Download to custom directory
path = jp.download("sex", 2023, out_dir="./data")
```

**`download_recent(out_dir=None, overwrite=False, lang="en")`** - Download all available weekly bulletins (2024+)

```python
# Downloads all available 2024+ weekly reports
paths = jp.download_recent()

# Force re-download (ignore cache)
paths = jp.download_recent(overwrite=True)
```

### Reading Data

**`read(path, type=None, return_type=None)`** - Read downloaded Excel or CSV files

```python
# Auto-detect file type from filename
df = jp.read("path/to/Syu_01_1_2024.xlsx")

# Explicitly specify dataset type
df = jp.read("data.xlsx", type="sex")

# Read as Polars (faster)
df = jp.read("data.csv", return_type="polars")
```

### Data Transformations

**`merge(*dfs, type=None)`** - Merge multiple datasets

```python
df1 = jp.load("sex")
df2 = jp.read(jp.download("sex", 2024))
merged = jp.merge(df1, df2)
```

**`pivot(df, direction="long")`** - Convert between wide and long formats

```python
# Convert to long format (prefecture × disease × week)
df_long = jp.pivot(df, direction="long")

# Convert to wide format (diseases as columns)
df_wide = jp.pivot(df, direction="wide")
```

### Configuration

**`configure(**kwargs)`** - Set global configuration

```python
import jpinfectpy as jp

# Set default return type
jp.configure(return_type="polars")

# Increase rate limit (requests per minute)
jp.configure(rate_limit_per_minute=60)

# Change cache directory
from pathlib import Path
jp.configure(cache_dir=Path("./my_cache"))
```

## Return Types: Polars vs Pandas

All data-returning functions accept a `return_type` parameter:

```python
# Returns Pandas DataFrame (default)
df = jp.load("sex", return_type="pandas")

# Returns Polars DataFrame (faster, more memory efficient)
df = jp.load("sex", return_type="polars")

# Set global default to avoid specifying each time
jp.configure(return_type="polars")
df = jp.load("sex")  # Now returns Polars by default
```

**Why Polars?**

- Faster processing (especially for large datasets)
- Lower memory usage
- Better performance for transformations

**Why Pandas?**

- More familiar API for many users
- Broader ecosystem compatibility
- Better for interactive analysis in Jupyter

## Data Sources

Data is sourced from the National Institute of Infectious Diseases (NIID):

- **Historical confirmed cases**: [NIID IDWR Archives](https://www.niid.go.jp/niid/ja/idwr.html)
- **Weekly bulletins**: [JIHS Infectious Disease Surveillance](https://id-info.jihs.go.jp/)

### Available Datasets

- **`sex`**: Sex-disaggregated surveillance data (1999-2023)
  - Columns: prefecture, year, week, date, disease, category (total/male/female), count
- **`place`**: Place of infection surveillance data (2001-2023)
  - Columns: prefecture, year, week, date, disease, category (domestic/imported/unknown), count
- **`bullet`**: Weekly bulletins (2024+)
  - Columns: prefecture, year, week, date, disease, count

For detailed dataset descriptions, see [DATASETS.md](./DATASETS.md).

## Examples

See [EXAMPLES.md](./EXAMPLES.md) for comprehensive usage examples including:

- Loading and exploring data
- Time series analysis
- Geographic analysis
- Combining datasets
- Custom workflows

## R Package Compatibility

This package mirrors the API of the upstream R package [`jpinfect`](https://github.com/TomonoriHoshi/jpinfect):

| R Function                    | Python Function     | Notes                     |
| ----------------------------- | ------------------- | ------------------------- |
| `data()`                    | `load()`          | Load bundled datasets     |
| `jpinfect_get_confirmed()`  | `download()`      | Unified download function |
| `jpinfect_get_bullet()`     | `download()`      | Use `type="bullet"`     |
| `jpinfect_read_confirmed()` | `read()`          | Unified read function     |
| `jpinfect_read_bullet()`    | `read()`          | Auto-detects CSV files    |
| `jpinfect_merge()`          | `merge()`         | Merge multiple datasets   |
| `jpinfect_pivot()`          | `pivot()`         | Wide/long conversion      |
| `jpinfect_url_confirmed()`  | `url_confirmed()` | Generate download URLs    |
| `jpinfect_url_bullet()`     | `url_bullet()`    | Generate bulletin URLs    |

## Polite Usage

Please be respectful of NIID's servers:

- ✅ **Caching is automatic**: Downloaded files are cached in `~/.cache/jpinfectpy/`
- ✅ **Rate limiting is enabled**: Default 20 requests/minute
- ✅ **User agent is set**: Identifies as jpinfectpy with repository link
- ⚠️ **Avoid excessive downloads**: Use `overwrite=False` (default) to reuse cached data
- ⚠️ **Bulk downloads**: Run during off-peak hours and increase rate limit moderately

```python
# Good: Uses cached data
df = jp.load_all()

# Good: Downloads missing weeks only
paths = jp.download_recent()

# Bad: Forces re-download of everything
paths = jp.download_recent(overwrite=True)  # Only use when debugging
```

## Development

```bash
# Clone the repository
git clone https://github.com/AlFontal/jpinfectpy.git
cd jpinfectpy

# Install with development dependencies
uv sync --all-extras --dev

# Run tests
uv run pytest

# Run linting
uv run ruff check .

# Run type checking
uv run mypy src
```

See [CONTRIBUTING.md](./CONTRIBUTING.md) for detailed development guidelines and project architecture.

## Citation

If you use this package in research, please cite both this package and the upstream R package:

```bibtex
@software{jpinfectpy,
  author = {Fontal, Alejandro},
  title = {jpinfectpy: Python access to Japanese infectious disease data},
  year = {2026},
  url = {https://github.com/AlFontal/jpinfectpy}
}

@software{jpinfect,
  author = {Hoshi, Tomonori},
  title = {jpinfect: R interface to Japanese infectious disease surveillance data},
  year = {2024},
  url = {https://github.com/TomonoriHoshi/jpinfect}
}
```

## License

GPL-3.0-or-later. See [LICENSE](./LICENSE) for details.

This project includes attribution to the upstream R package [`jpinfect`](https://github.com/TomonoriHoshi/jpinfect).

## Acknowledgments

- Data provided by the [National Institute of Infectious Diseases (NIID)](https://www.niid.go.jp/)
- Inspired by and compatible with [`jpinfect`](https://github.com/TomonoriHoshi/jpinfect) by Tomonori Hoshi
