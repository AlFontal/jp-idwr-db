# Changelog

## 0.2.0 - 2026-02-06

- Switched packaged data delivery to GitHub Release assets with runtime caching.
- Added checksum-verified data manager and CLI prefetch (`python -m jp_idwr_db data download`).
- Removed parquet payload from wheels and added release asset build/publish workflow.
- Added CI smoke checks for wheel install/runtime data loading and data-manager tests.

## 0.1.0 - 2026-02-06

- First public release under the new package name `jp-idwr-db`.
- Independent data-engineering scope (inspired by `jpinfect`, not API parity).
- Historical + modern IDWR ingestion, including sentinel (`teitenrui`) archives from 2012+.
- Bundled parquet datasets (`sex`, `place`, `bullet`, `sentinel`, `unified`) and Polars-first API.
