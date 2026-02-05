# Contributing to jpinfectpy

Thanks for your interest in contributing to jpinfectpy! This guide will help you get started with development, understand our code standards, and submit high-quality contributions.

## Quick Start

### Prerequisites

- Python 3.10+
- [uv](https://github.com/astral-sh/uv) package manager (recommended)

### Setup

```bash
# Clone the repository
git clone https://github.com/AlFontal/jpinfectpy.git
cd jpinfectpy

# Install with development dependencies
uv sync --all-extras --dev

# Install pre-commit hooks (optional but recommended)
pre-commit install
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=jpinfectpy --cov-report=term-missing

# Run specific test file
uv run pytest tests/test_transform.py

# Skip integration tests (those requiring network)
uv run pytest -m "not integration"
```

### Code Quality

```bash
# Format code
uv run ruff format .

# Check formatting (CI mode)
uv run ruff format --check .

# Lint code
uv run ruff check .

# Auto-fix linting issues
uv run ruff check --fix .

# Type check
uv run mypy src
```

## Repository Structure

```
jpinfectpy/
├── src/jpinfectpy/         # Main package code
│   ├── __init__.py         # Public API exports
│   ├── config.py           # Global configuration
│   ├── datasets.py         # Bundled dataset loading
│   ├── http.py             # HTTP client with caching
│   ├── io.py               # Data download and reading
│   ├── transform.py        # Data manipulation
│   ├── types.py            # Type definitions
│   ├── urls.py             # URL generation
│   ├── utils.py            # Helper functions
│   └── data/               # Bundled parquet datasets
├── tests/                  # Pytest test suite
│   ├── fixtures/           # Test data
│   └── test_*.py          # Test modules
├── scripts/                # Build and utility scripts
│   └── build_datasets.py  # Build bundled datasets
├── docs/                   # MkDocs documentation
└── pyproject.toml          # Project configuration
```

## Code Standards

### Style Guidelines

We use **Ruff** for both linting and formatting with strict rules:

- **Line length**: 100 characters
- **Docstrings**: Google-style, required for all public functions and classes
- **Type hints**: Required for all function signatures
- **Imports**: Organized with isort rules
- **Formatting**: Automatic with Ruff format

### Type Hints

All public functions and methods must have type hints:

```python
from __future__ import annotations

import polars as pl
from pathlib import Path

def read_data(path: Path, return_type: str = "polars") -> pl.DataFrame:
    """Read data from file.
    
    Args:
        path: Path to the data file.
        return_type: Format to return ("polars" or "pandas").
        
    Returns:
        DataFrame containing the data.
    """
    ...
```

### Docstrings

Use **Google-style docstrings** for all public functions and classes:

```python
def example_function(param1: str, param2: int) -> bool:
    """Short one-line summary.

    Longer description if needed. Explain the purpose, behavior,
    and any important caveats.

    Args:
        param1: Description of first parameter.
        param2: Description of second parameter.

    Returns:
        Description of return value.

    Raises:
        ValueError: When and why this exception is raised.

    Example:
        >>> result = example_function("test", 42)
        >>> print(result)
        True
    """
    ...
```

### Testing Guidelines

- **Coverage**: Aim for high test coverage of new code
- **No network calls**: Use fixtures or mocks instead of real HTTP requests
- **Fixtures**: Place test data in `tests/fixtures/`
- **Markers**: Use `@pytest.mark.integration` for tests requiring network access
- **Assertions**: Use clear, descriptive assertions

Example test:

```python
import pytest
import polars as pl
from jpinfectpy import load

def test_load_returns_polars():
    """Test that load returns Polars DataFrame by default."""
    df = load("sex", return_type="polars")
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert "prefecture" in df.columns
```

## Development Workflow

### 1. Create a Feature Branch

```bash
git checkout -b feature/your-feature-name
```

### 2. Make Your Changes

- Write code following our style guidelines
- Add or update tests
- Update documentation if needed
- Keep commits focused and atomic

### 3. Test Your Changes

```bash
# Run all quality checks
uv run ruff format .
uv run ruff check .
uv run mypy src
uv run pytest
```

### 4. Commit Your Changes

Use [Conventional Commits](https://www.conventionalcommits.org/) format:

```bash
git commit -m "feat: add support for new dataset type"
git commit -m "fix: handle missing columns in Excel parsing"
git commit -m "docs: update README with new examples"
git commit -m "refactor: simplify DataFrame conversion logic"
git commit -m "test: add coverage for edge cases"
```

### 5. Push and Create Pull Request

```bash
git push origin feature/your-feature-name
```

Then create a pull request on GitHub with:
- Clear description of changes
- Motivation for the change
- Any breaking changes highlighted
- Screenshots/examples if applicable

## Common Tasks

### Adding a New Dataset Type

1. Update `DatasetName` type in `src/jpinfectpy/types.py`
2. Add URL generation logic in `src/jpinfectpy/urls.py`
3. Add parsing logic in `src/jpinfectpy/io.py` (or reuse existing)
4. Update docstrings in affected functions
5. Add tests in `tests/`

### Adding a New Transformation

1. Add function to `src/jpinfectpy/transform.py`
2. Export in `src/jpinfectpy/__init__.py`
3. Add comprehensive docstring with examples
4. Add tests in `tests/test_transform.py`
5. Update `EXAMPLES.md` if appropriate

### Updating Excel Parsers

Excel format changes are common. If the NIID website changes file structure:

1. Download sample file and inspect manually
2. Update `_read_excel_sheets()` in `src/jpinfectpy/io.py`
3. Update row offsets if headers changed (currently rows 2-3)
4. Update column name cleaning in `_resolve_headers()`
5. Update `_sheet_range_for_year()` if sheet count changed
6. Add tests with new fixture files

## Project-Specific Knowledge

### Excel Parsing Challenges

The NIID Excel files have several quirks:

- **Merged header cells**: Row 2 has disease names spanning multiple columns
- **Year-specific variations**: 1999 has fewer sheets, 2004/2009/2015 have 53 weeks
- **Null bytes**: 1999-2000 files contain `\x00` characters that must be stripped
- **Bilingual text**: Some cells have Japanese followed by English in parentheses

### URL Pattern Evolution

The NIID has changed URL structures multiple times:

- **Pre-2011**: Kako archive with Heisei year (year - 1988)
- **2011-2020**: ydata repository
- **2021+**: annual repository
- **2025+**: Japanese bulletins moved to `idwr/jp/rapid/` path

The `ConfirmedRule` system in `urls.py` handles these transitions.

### Dual Return Type System

All data-returning functions accept `return_type="pandas"|"polars"`:

- Internally, everything uses Polars (faster)
- Pandas conversion happens only at return if requested
- Default return type is configurable globally via `configure()`

### Caching Strategy

- **HTTP cache**: `~/.cache/jpinfectpy/http/` stores raw downloads with ETag metadata
- **Data cache**: `~/.cache/jpinfectpy/raw/` stores renamed files for user access
- Files are copied from HTTP cache to data cache, not moved

## Pull Request Guidelines

### Before Submitting

- [ ] All tests pass (`uv run pytest`)
- [ ] Code is formatted (`uv run ruff format .`)
- [ ] Linting passes (`uv run ruff check .`)
- [ ] Type checking passes (`uv run mypy src`)
- [ ] Documentation is updated
- [ ] Changelog is updated (for significant changes)

### PR Description Should Include

- **What**: Clear description of changes
- **Why**: Motivation and context
- **How**: Brief explanation of approach
- **Breaking Changes**: Highlight any API changes
- **Testing**: How you tested the changes

### Review Process

1. Automated CI checks must pass
2. At least one maintainer approval required
3. Address review comments
4. Squash commits if requested
5. Merge when approved

## Getting Help

If you have questions:

1. Check existing documentation (README, EXAMPLES, docstrings)
2. Search existing issues and discussions
3. Ask in a new GitHub issue or discussion
4. For security issues, see security policy

## Code of Conduct

Please note we have a [Code of Conduct](./CODE_OF_CONDUCT.md). By participating in this project, you agree to abide by its terms.

## License

By contributing, you agree that your contributions will be licensed under the GPL-3.0-or-later license.

## Recognition

Contributors will be recognized in:
- Git commit history
- Release notes
- Potential CONTRIBUTORS file (if project grows)

Thank you for contributing to jpinfectpy! 🎉
