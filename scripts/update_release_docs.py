#!/usr/bin/env python3
"""Refresh documentation snapshots from locally rebuilt release parquet files."""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "parquet"
DATASETS_MD = ROOT / "docs" / "DATASETS.md"
README = ROOT / "README.md"

README_BEGIN = "<!-- BEGIN GENERATED UNIFIED SNAPSHOT -->"
README_END = "<!-- END GENERATED UNIFIED SNAPSHOT -->"
DYNAMIC_DATASETS = ("bullet.parquet", "sentinel.parquet", "unified.parquet")


def _replace_line(section: str, label: str, value: str) -> str:
    """Replace one Markdown metric line inside a dataset section."""
    pattern = rf"^- {re.escape(label)}: .+$"
    replacement = f"- {label}: {value}"
    updated, count = re.subn(pattern, replacement, section, count=1, flags=re.MULTILINE)
    if count != 1:
        raise ValueError(f"Could not update {label!r} in DATASETS.md")
    return updated


def _dataset_metrics(path: Path) -> dict[str, object]:
    """Collect documentation metrics with a lazy parquet scan."""
    scan = pl.scan_parquet(path)
    schema = scan.collect_schema()
    expressions: list[pl.Expr] = [pl.len().alias("rows")]

    if "year" in schema:
        expressions.extend(
            [
                pl.col("year").min().alias("min_year"),
                pl.col("year").max().alias("max_year"),
            ]
        )
    if "prefecture" in schema:
        expressions.append(pl.col("prefecture").n_unique().alias("prefectures"))
    if "disease" in schema:
        expressions.append(pl.col("disease").n_unique().alias("diseases"))
    if path.name == "sentinel.parquet" and "count" in schema:
        expressions.append(pl.col("count").null_count().alias("null_count"))

    row = scan.select(expressions).collect().row(0, named=True)
    return dict(row)


def update_dataset_reference(snapshot_date: date | None = None) -> None:
    """Update dynamic snapshot metrics in ``docs/DATASETS.md``."""
    resolved_date = snapshot_date or date.today()
    documentation = DATASETS_MD.read_text(encoding="utf-8")
    documentation, count = re.subn(
        r"All figures below reflect the repository snapshot on \*\*\d{4}-\d{2}-\d{2}\*\*\.",
        f"All figures below reflect the repository snapshot on **{resolved_date.isoformat()}**.",
        documentation,
        count=1,
    )
    if count != 1:
        raise ValueError("Could not update DATASETS.md snapshot date")

    for filename in DYNAMIC_DATASETS:
        path = DATA_DIR / filename
        metrics = _dataset_metrics(path)
        section_pattern = rf"(^### `{re.escape(filename)}`$.*?)(?=^### |^## |\Z)"
        section_match = re.search(section_pattern, documentation, flags=re.MULTILINE | re.DOTALL)
        if section_match is None:
            raise ValueError(f"Missing DATASETS.md section for {filename}")

        section = section_match.group(1)
        section = _replace_line(section, "Rows", f"`{int(metrics['rows']):,}`")
        section = _replace_line(
            section,
            "Years",
            f"`{int(metrics['min_year'])}-{int(metrics['max_year'])}`",
        )
        section = _replace_line(section, "Prefectures", f"`{int(metrics['prefectures'])}`")
        section = _replace_line(section, "Diseases", f"`{int(metrics['diseases'])}`")

        if filename == "sentinel.parquet":
            null_count = int(metrics["null_count"])
            rows = int(metrics["rows"])
            null_rate = null_count / rows if rows else 0.0
            section = _replace_line(
                section,
                "Null `count` rows",
                f"`{null_count:,}` (`{null_rate:.2%}`), primarily missing baselines and source corrections",
            )

        documentation = (
            documentation[: section_match.start(1)] + section + documentation[section_match.end(1) :]
        )

    DATASETS_MD.write_text(documentation, encoding="utf-8")


def _render_unified_snapshot() -> str:
    """Render the README quick-start output from the rebuilt unified parquet."""
    df = pl.read_parquet(DATA_DIR / "unified.parquet").select(
        ["date", "prefecture", "category", "disease", "count", "source"]
    )
    with pl.Config(tbl_rows=10, tbl_cols=6, fmt_str_lengths=29, tbl_width_chars=120):
        return str(df)


def update_readme_snapshot() -> None:
    """Replace the generated unified quick-start output in ``README.md``."""
    readme = README.read_text(encoding="utf-8")
    generated = f"{README_BEGIN}\n```text\n{_render_unified_snapshot()}\n```\n{README_END}"

    marker_pattern = rf"{re.escape(README_BEGIN)}.*?{re.escape(README_END)}"
    if re.search(marker_pattern, readme, flags=re.DOTALL):
        readme = re.sub(marker_pattern, generated, readme, count=1, flags=re.DOTALL)
    else:
        initial_pattern = r"(?<=print\(df\)\n```\n\n)```text\n.*?\n```"
        readme, count = re.subn(initial_pattern, generated, readme, count=1, flags=re.DOTALL)
        if count != 1:
            raise ValueError("Could not locate README unified quick-start output")

    README.write_text(readme, encoding="utf-8")


def main() -> None:
    """Refresh all documentation derived from release parquet outputs."""
    update_dataset_reference()
    update_readme_snapshot()


if __name__ == "__main__":
    main()
