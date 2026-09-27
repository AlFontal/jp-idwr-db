"""Helpers for automated data refresh releases."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from ._internal import validation
from ._internal.release_utils import sha256 as file_sha256
from .utils import PREFECTURE_ISO_MAP, iso_weeks_in_year

BULLET_PATH = Path("data/parquet/bullet.parquet")
SENTINEL_PATH = Path("data/parquet/sentinel.parquet")
VALIDATED_OUTPUTS = (
    Path("data/parquet/sex_prefecture.parquet"),
    Path("data/parquet/place_prefecture.parquet"),
    BULLET_PATH,
    SENTINEL_PATH,
    Path("data/parquet/unified.parquet"),
)
TARGET_OUTPUTS = (*VALIDATED_OUTPUTS, Path("docs/DISEASES.md"))
# Static lookup table: no builder, carried from release to release.
PREFECTURE_EN_PATH = Path("data/parquet/prefecture_en.parquet")
SENTINEL_SOURCE = "Sentinel surveillance"

CHANGELOG_PATH = Path("CHANGELOG.md")
PYPROJECT_PATH = Path("pyproject.toml")
INIT_PATH = Path("src/jp_idwr_db/__init__.py")
CONFIG_PATH = Path("src/jp_idwr_db/config.py")
CITATION_PATH = Path("CITATION.cff")
UV_LOCK_PATH = Path("uv.lock")
BACKED_UP_OUTPUTS = (
    *TARGET_OUTPUTS,
    CHANGELOG_PATH,
    PYPROJECT_PATH,
    INIT_PATH,
    CONFIG_PATH,
    CITATION_PATH,
    UV_LOCK_PATH,
)


@dataclass(frozen=True)
class RefreshOutputs:
    """Machine-readable outputs for refresh automation."""

    changed: bool
    version: str
    tag: str
    latest_bullet_week: str
    latest_sentinel_week: str

    def to_dict(self: RefreshOutputs) -> dict[str, str]:
        """Return outputs in GitHub Actions-friendly string form."""
        return {
            "changed": str(self.changed).lower(),
            "version": self.version,
            "tag": self.tag,
            "latest_bullet_week": self.latest_bullet_week,
            "latest_sentinel_week": self.latest_sentinel_week,
        }


def _repo_root() -> Path:
    """Resolve the repository root from the installed source tree."""
    return Path(__file__).resolve().parents[2]


def _sha256(path: Path) -> str | None:
    """Compute a SHA256 digest or return ``None`` when the file is absent."""
    if not path.exists():
        return None
    return file_sha256(path)


def _snapshot_paths(repo_root: Path) -> dict[str, str | None]:
    """Capture the digest state of the release datasets.

    Only data files count: generated docs carry the build date, so they would
    make every run on a new day look like a change.
    """
    return {str(rel_path): _sha256(repo_root / rel_path) for rel_path in VALIDATED_OUTPUTS}


# prefecture_en.parquet has no builder; a full rebuild keeps the seeded copy.
FULL_REBUILD_OUTPUTS = VALIDATED_OUTPUTS


def _remove_for_full_rebuild(repo_root: Path) -> None:
    """Delete seeded datasets so every builder rebuilds from the source files."""
    for rel_path in FULL_REBUILD_OUTPUTS:
        (repo_root / rel_path).unlink(missing_ok=True)


def summarize_changes(previous_dir: Path, rebuilt_dir: Path) -> str:
    """Return a markdown summary of rebuilt datasets against the previous release."""
    lines = [
        "| Dataset | Previous rows | Rebuilt rows | Years with changed rows |",
        "| --- | ---: | ---: | --- |",
    ]
    for rel_path in VALIDATED_OUTPUTS:
        previous_path, rebuilt_path = previous_dir / rel_path.name, rebuilt_dir / rel_path.name
        if not rebuilt_path.exists():
            continue
        if previous_path.exists() and _sha256(previous_path) == _sha256(rebuilt_path):
            rows = pq.ParquetFile(rebuilt_path).metadata.num_rows
            lines.append(f"| `{rel_path.name}` | {rows:,} | {rows:,} | none (identical) |")
            continue
        rebuilt = pl.scan_parquet(rebuilt_path)
        previous = pl.scan_parquet(previous_path) if previous_path.exists() else None
        years = _changed_years(previous, rebuilt)
        previous_rows = previous.select(pl.len()).collect().item() if previous is not None else 0
        rebuilt_rows = rebuilt.select(pl.len()).collect().item()
        shown = ", ".join(str(y) for y in years[:12]) + (" …" if len(years) > 12 else "")
        lines.append(
            f"| `{rel_path.name}` | {previous_rows:,} | {rebuilt_rows:,} | {shown or 'none'} |"
        )
    return "\n".join(lines) + "\n"


def _changed_years(previous: pl.LazyFrame | None, rebuilt: pl.LazyFrame) -> list[int]:
    """Return years whose rows differ, via per-year row counts and row hashes."""

    def per_year(frame: pl.LazyFrame, columns: list[str]) -> dict[int, tuple[int, int]]:
        rows = (
            frame.group_by("year")
            .agg(pl.len().alias("n"), pl.struct(columns).hash(seed=0).sum().alias("h"))
            .collect()
        )
        return {int(r["year"]): (int(r["n"]), int(r["h"] or 0)) for r in rows.iter_rows(named=True)}

    rebuilt_schema = rebuilt.collect_schema()
    columns = sorted(rebuilt_schema.names())
    if previous is None:
        return sorted(per_year(rebuilt, columns))
    previous_schema = previous.collect_schema()
    if sorted(previous_schema.names()) != columns or any(
        previous_schema[c] != rebuilt_schema[c] for c in columns
    ):
        # Column or dtype change: every year is affected.
        years = pl.concat([previous.select("year"), rebuilt.select("year")], how="vertical_relaxed")
        return sorted(int(y) for y in years.unique().collect()["year"].to_list())
    old, new = per_year(previous, columns), per_year(rebuilt, columns)
    return sorted(y for y in set(old) | set(new) if old.get(y) != new.get(y))


def _backup_targets(repo_root: Path, backup_root: Path) -> None:
    """Copy existing generated outputs into a temporary backup directory."""
    for rel_path in BACKED_UP_OUTPUTS:
        source = repo_root / rel_path
        if not source.exists():
            continue
        backup_path = backup_root / rel_path
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, backup_path)


def _restore_targets(repo_root: Path, backup_root: Path) -> None:
    """Restore generated outputs from backup, removing newly created files."""
    for rel_path in BACKED_UP_OUTPUTS:
        source = backup_root / rel_path
        dest = repo_root / rel_path
        if source.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, dest)
        elif dest.exists():
            dest.unlink()


def _run_build_step(repo_root: Path, flag: str) -> None:
    """Run one dataset build step."""
    script_path = repo_root / "scripts" / "build_datasets.py"
    subprocess.run([sys.executable, str(script_path), flag], check=True, cwd=repo_root)


def rebuild_release_outputs(repo_root: Path) -> None:
    """Rebuild the release datasets that participate in automated refreshes."""
    # Annual confirmed tables first: they decide which bullet years unified uses.
    for flag in (
        "--sex-only",
        "--place-only",
        "--bullet-only",
        "--sentinel-only",
        "--unified-only",
    ):
        _run_build_step(repo_root, flag)


def current_version(repo_root: Path) -> str:
    """Read the current package version from ``pyproject.toml``."""
    pyproject_text = (repo_root / PYPROJECT_PATH).read_text(encoding="utf-8")
    match = re.search(r'^version = "([^"]+)"$', pyproject_text, re.MULTILINE)
    if match is None:
        raise ValueError("Could not locate version in pyproject.toml")
    return match.group(1)


def next_calver_version(version: str, release_date: date) -> str:
    """Return the next calendar-versioned release string for a refresh run."""
    base_version = f"{release_date.year}.{release_date.month}.{release_date.day}"
    if version == base_version:
        return f"{base_version}.post1"

    same_day_post = re.fullmatch(rf"{re.escape(base_version)}\.post(\d+)", version)
    if same_day_post is not None:
        next_post = int(same_day_post.group(1)) + 1
        return f"{base_version}.post{next_post}"

    return base_version


def _replace_once(pattern: str, replacement: str, text: str, path: Path) -> str:
    """Replace exactly one regex match, failing loudly on drift."""
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.MULTILINE)
    if count != 1:
        raise ValueError(f"Could not update expected pattern in {path}")
    return updated


def update_version_files(repo_root: Path, version: str) -> None:
    """Update version strings across package metadata files."""
    pyproject_path = repo_root / PYPROJECT_PATH
    pyproject_text = pyproject_path.read_text(encoding="utf-8")
    pyproject_path.write_text(
        _replace_once(
            r'^version = "[^"]+"$',
            f'version = "{version}"',
            pyproject_text,
            pyproject_path,
        ),
        encoding="utf-8",
    )

    init_path = repo_root / INIT_PATH
    init_text = init_path.read_text(encoding="utf-8")
    init_path.write_text(
        _replace_once(
            r'^__version__ = "[^"]+"$',
            f'__version__ = "{version}"',
            init_text,
            init_path,
        ),
        encoding="utf-8",
    )

    config_path = repo_root / CONFIG_PATH
    config_text = config_path.read_text(encoding="utf-8")
    config_path.write_text(
        _replace_once(
            r"jp_idwr_db/\d+\.\d+\.\d+(?:\.post\d+)?",
            f"jp_idwr_db/{version}",
            config_text,
            config_path,
        ),
        encoding="utf-8",
    )

    citation_path = repo_root / CITATION_PATH
    citation_text = citation_path.read_text(encoding="utf-8")
    citation_path.write_text(
        _replace_once(
            r"^version: .+$",
            f"version: {version}",
            citation_text,
            citation_path,
        ),
        encoding="utf-8",
    )

    uv_lock_path = repo_root / UV_LOCK_PATH
    uv_lock_text = uv_lock_path.read_text(encoding="utf-8")
    uv_lock_path.write_text(
        _replace_once(
            r'(\[\[package\]\]\nname = "jp-idwr-db"\nversion = ")[^"]+("$)',
            rf"\g<1>{version}\g<2>",
            uv_lock_text,
            uv_lock_path,
        ),
        encoding="utf-8",
    )


def _latest_year_week(path: Path) -> tuple[int, int]:
    """Read the latest ``(year, week)`` tuple from a parquet dataset."""
    latest = (
        pl.scan_parquet(path)
        .select(["year", "week"])
        .unique()
        .sort(["year", "week"], descending=True)
        .head(1)
        .collect()
    )
    if latest.is_empty():
        raise ValueError(f"Dataset contains no surveillance periods: {path}")
    return int(latest["year"][0]), int(latest["week"][0])


def _frozen_year_sources(path: Path, latest_year: int) -> pl.DataFrame:
    """Return ``(year, source)`` pairs whose published rows must not change.

    Every year before the previous calendar year is frozen. The previous year is
    frozen per source only once it reaches its final ISO week, so a refresh after
    the year rollover can still add the weeks published late (including week 53).
    Completeness is tracked per source because ``unified`` mixes sources whose
    publication can finish at different times for the same year.
    """
    scan = pl.scan_parquet(path)
    source_expr = pl.col("source") if "source" in scan.collect_schema().names() else pl.lit("")
    last_weeks = (
        scan.filter(pl.col("year") < latest_year)
        .group_by(pl.col("year"), source_expr.alias("_source"))
        .agg(pl.col("week").max().alias("last_week"))
        .collect()
    )
    is_complete = pl.col("last_week") >= pl.col("year").map_elements(
        iso_weeks_in_year, return_dtype=pl.Int64
    )
    return last_weeks.filter((pl.col("year") < latest_year - 1) | is_complete).select(
        ["year", "_source"]
    )


def _historical_signature(
    path: Path, frozen: pl.DataFrame
) -> tuple[tuple[str, ...], int, int, int]:
    """Return an order-independent signature for immutable historical rows."""
    scan = pl.scan_parquet(path)
    source_expr = pl.col("source") if "source" in scan.collect_schema().names() else pl.lit("")
    scan = scan.join(
        frozen.lazy(),
        left_on=[pl.col("year"), source_expr],
        right_on=[pl.col("year"), pl.col("_source")],
        how="semi",
    )
    columns = tuple(scan.collect_schema().names())
    signature = scan.select(
        pl.len().alias("rows"),
        pl.struct(pl.all()).hash(seed=0).sum().alias("hash_0"),
        pl.struct(pl.all()).hash(seed=1).sum().alias("hash_1"),
    ).collect()
    return (
        columns,
        int(signature["rows"][0]),
        int(signature["hash_0"][0] or 0),
        int(signature["hash_1"][0] or 0),
    )


def _annual_year_families(path: Path) -> set[tuple[int, str]]:
    """Return ``(year, family)`` pairs sourced from final annual tables.

    Families are ``confirmed`` (annual tables are labelled ``Confirmed cases``,
    preliminary reports ``All-case reporting``) and ``sentinel`` (annual rows
    have ``count_status == "annual"``).
    """
    scan = pl.scan_parquet(path)
    names = scan.collect_schema().names()
    if "source" not in names:
        return set()
    is_sentinel = pl.col("source") == SENTINEL_SOURCE
    annual = pl.col("source") == "Confirmed cases"
    if "count_status" in names:
        annual = annual | (pl.col("count_status") == "annual")
    rows = (
        scan.filter(annual)
        .select(
            pl.col("year"),
            pl.when(is_sentinel).then(pl.lit("sentinel")).otherwise(pl.lit("confirmed")).alias("f"),
        )
        .unique()
        .collect()
    )
    return {(int(row["year"]), str(row["f"])) for row in rows.iter_rows(named=True)}


def _period_row_counts(path: Path) -> dict[tuple[int, int], int]:
    """Return row counts for every observed surveillance period."""
    counts = pl.scan_parquet(path).group_by(["year", "week"]).agg(pl.len().alias("rows")).collect()
    return {
        (int(row["year"]), int(row["week"])): int(row["rows"])
        for row in counts.iter_rows(named=True)
    }


def _validate_release_preservation(
    repo_root: Path, backup_root: Path, *, allow_historical_changes: bool = False
) -> None:
    """Reject refreshes that regress recency or alter stable historical data.

    ``allow_historical_changes`` skips the lost-rows and frozen-rows checks for a
    deliberate, reviewed correction; the recency check still applies.
    """
    for rel_path in VALIDATED_OUTPUTS:
        previous = backup_root / rel_path
        rebuilt = repo_root / rel_path
        if not previous.exists():
            continue

        previous_latest = _latest_year_week(previous)
        rebuilt_latest = _latest_year_week(rebuilt)
        if rebuilt_latest < previous_latest:
            raise ValueError(
                f"Latest period regressed for {rel_path}: {previous_latest} -> {rebuilt_latest}"
            )

        # A year whose data switches from preliminary reports to a final annual
        # table may legitimately change; years already annual stay frozen.
        switched = _annual_year_families(rebuilt) - _annual_year_families(previous)
        switched_years = {year for year, _ in switched}
        previous_counts = _period_row_counts(previous)
        rebuilt_counts = _period_row_counts(rebuilt)
        regressed_periods = [
            (period, rows, rebuilt_counts.get(period, 0))
            for period, rows in previous_counts.items()
            if rebuilt_counts.get(period, 0) < rows and period[0] not in switched_years
        ]
        if regressed_periods and not allow_historical_changes:
            raise ValueError(
                f"Previously published periods lost rows in {rel_path}. "
                f"First regressions: {sorted(regressed_periods)[:10]}"
            )

        frozen = _frozen_year_sources(previous, previous_latest[0])
        if switched:
            family = (
                pl.when(pl.col("_source") == SENTINEL_SOURCE)
                .then(pl.lit("sentinel"))
                .otherwise(pl.lit("confirmed"))
            )
            switched_df = pl.DataFrame(
                {"year": [y for y, _ in switched], "_family": [f for _, f in switched]},
                schema={"year": frozen.schema["year"], "_family": pl.String},
            )
            frozen = (
                frozen.with_columns(family.alias("_family"))
                .join(switched_df, on=["year", "_family"], how="anti")
                .drop("_family")
            )
        if (
            _historical_signature(previous, frozen) != _historical_signature(rebuilt, frozen)
            and not allow_historical_changes
        ):
            raise ValueError(f"Stable historical rows changed in {rel_path}")


def _format_year_week(path: Path) -> str:
    """Format the latest dataset week as ``YYYY-Www``."""
    year, week = _latest_year_week(path)
    return f"{year}-W{week:02d}"


def _validate_release_outputs(repo_root: Path) -> None:
    """Validate refreshed parquet outputs before treating them as release-ready."""
    for rel_path in VALIDATED_OUTPUTS:
        dataset_path = repo_root / rel_path
        if not dataset_path.exists():
            raise ValueError(f"Missing validated release dataset: {rel_path}")

        df = pl.read_parquet(dataset_path)
        validation.validate_schema(df)
        identifier_columns = ["prefecture", "year", "week", "disease"]
        identifier_columns.extend(
            column for column in ["category", "source"] if column in df.columns
        )
        validation.validate_required_values(df, identifier_columns)
        validation.validate_allowed_values(df, "prefecture", set(PREFECTURE_ISO_MAP))
        validation.validate_clean_disease_names(df)
        expected_sources = {
            "sex_prefecture.parquet": {"Confirmed cases"},
            "place_prefecture.parquet": {"Confirmed cases"},
            "bullet.parquet": {"All-case reporting"},
            "sentinel.parquet": {"Sentinel surveillance"},
            "unified.parquet": {
                "Confirmed cases",
                "All-case reporting",
                "Sentinel surveillance",
            },
        }
        validation.validate_allowed_values(df, "source", expected_sources[rel_path.name])
        expected_categories = {
            "sex_prefecture.parquet": {"total", "male", "female"},
            "place_prefecture.parquet": {"total", "japan", "others", "unknown"},
            "unified.parquet": {"total"},
        }
        if rel_path.name in expected_categories:
            validation.validate_allowed_values(df, "category", expected_categories[rel_path.name])
        validation.validate_no_duplicates(df)
        validation.validate_date_ranges(df)
        if "date" in df.columns:
            validation.validate_iso_week_start_dates(df)
        validation.validate_non_negative_counts(df)
        validation.validate_prefecture_coverage(df)
        if rel_path.name in {"sentinel.parquet", "unified.parquet"}:
            validation.validate_sentinel_count_status(df)
        if rel_path.name == "sentinel.parquet":
            validation.validate_max_null_rate(df, "count", max_rate=0.25, group_by=["year"])
        elif rel_path.name == "unified.parquet":
            sentinel_df = df.filter(pl.col("source") == "Sentinel surveillance")
            validation.validate_max_null_rate(
                sentinel_df, "count", max_rate=0.25, group_by=["year"]
            )


def prepend_changelog_entry(
    repo_root: Path,
    version: str,
    latest_bullet_week: str,
    latest_sentinel_week: str,
    release_date: date,
    historical_changes: bool = False,
) -> None:
    """Prepend a refresh-release entry to ``CHANGELOG.md``."""
    changelog_path = repo_root / CHANGELOG_PATH
    original = changelog_path.read_text(encoding="utf-8")
    if not original.startswith("# Changelog\n"):
        raise ValueError("CHANGELOG.md must start with '# Changelog'")

    entry = (
        f"## {version} - {release_date.isoformat()}\n\n"
        f"- Refreshed bullet release assets through {latest_bullet_week} and sentinel assets through "
        f"{latest_sentinel_week}.\n"
        + (
            "- Rebuilt previously published historical rows (explicit override).\n"
            if historical_changes
            else ""
        )
        + "- Automated weekly data refresh release.\n\n"
    )
    changelog_path.write_text(
        original.replace("# Changelog\n\n", f"# Changelog\n\n{entry}", 1),
        encoding="utf-8",
    )


def prepare_refresh_release(
    repo_root: Path | None = None,
    *,
    dry_run: bool = False,
    force_release: bool = False,
    full_rebuild: bool = False,
    allow_historical_changes: bool = False,
    keep_outputs: Path | None = None,
    release_date: date | None = None,
) -> RefreshOutputs:
    """Rebuild release outputs and prepare a calendar release when data changed.

    The data in ``data/parquet`` must be seeded from the previous release; it is
    the baseline for change detection and the history guard.

    Args:
        repo_root: Repository root (defaults to the source checkout).
        dry_run: Rebuild and validate, then restore the tree.
        force_release: Prepare a release even when the data is unchanged.
        full_rebuild: Rebuild every dataset from the source files instead of
            incrementally from the seeded release.
        allow_historical_changes: Accept changes to rows the guard treats as
            frozen (for deliberate corrections; review a dry run first).
        keep_outputs: Copy the rebuilt datasets here before a dry run restores
            the tree, together with ``summary.md``.
        release_date: Date used for the calendar version (defaults to today).
    """
    resolved_root = (repo_root or _repo_root()).resolve()
    current = current_version(resolved_root)
    resolved_release_date = release_date or date.today()
    version = next_calver_version(current, resolved_release_date)

    with tempfile.TemporaryDirectory() as tmp_dir:
        backup_root = Path(tmp_dir)
        before = _snapshot_paths(resolved_root)
        _backup_targets(resolved_root, backup_root)
        completed = False

        try:
            if full_rebuild:
                _remove_for_full_rebuild(resolved_root)
            rebuild_release_outputs(resolved_root)
            _validate_release_outputs(resolved_root)
            _validate_release_preservation(
                resolved_root, backup_root, allow_historical_changes=allow_historical_changes
            )
            if keep_outputs is not None:
                keep_outputs.mkdir(parents=True, exist_ok=True)
                for rel_path in (*VALIDATED_OUTPUTS, PREFECTURE_EN_PATH):
                    if (resolved_root / rel_path).exists():
                        shutil.copy2(resolved_root / rel_path, keep_outputs / rel_path.name)
                (keep_outputs / "summary.md").write_text(
                    summarize_changes(backup_root / "data" / "parquet", keep_outputs),
                    encoding="utf-8",
                )
            after = _snapshot_paths(resolved_root)
            changed = before != after or force_release
            latest_bullet_week = _format_year_week(resolved_root / BULLET_PATH)
            latest_sentinel_week = _format_year_week(resolved_root / SENTINEL_PATH)

            if dry_run:
                completed = True
                return RefreshOutputs(
                    changed=changed,
                    version=version,
                    tag=f"v{version}",
                    latest_bullet_week=latest_bullet_week,
                    latest_sentinel_week=latest_sentinel_week,
                )

            if changed:
                update_version_files(resolved_root, version)
                prepend_changelog_entry(
                    resolved_root,
                    version=version,
                    latest_bullet_week=latest_bullet_week,
                    latest_sentinel_week=latest_sentinel_week,
                    release_date=resolved_release_date,
                    historical_changes=allow_historical_changes,
                )

            completed = True
            return RefreshOutputs(
                changed=changed,
                version=version,
                tag=f"v{version}",
                latest_bullet_week=latest_bullet_week,
                latest_sentinel_week=latest_sentinel_week,
            )
        finally:
            if dry_run or not completed:
                _restore_targets(resolved_root, backup_root)


def write_outputs(outputs: RefreshOutputs, output_path: Path) -> None:
    """Write refresh outputs in GitHub Actions ``key=value`` format."""
    lines = [f"{key}={value}" for key, value in outputs.to_dict().items()]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    """Create the refresh helper CLI parser."""
    parser = argparse.ArgumentParser(prog="python -m jp_idwr_db.refresh_release")
    parser.add_argument(
        "--repo-root", type=Path, default=None, help="Repository root to operate on."
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Rebuild outputs, then restore the tree."
    )
    parser.add_argument(
        "--force-release",
        action="store_true",
        help="Prepare a release even when generated outputs are unchanged.",
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="Rebuild every dataset from the source files instead of incrementally.",
    )
    parser.add_argument(
        "--allow-historical-changes",
        action="store_true",
        help="Accept changes to frozen historical rows (review a dry run first).",
    )
    parser.add_argument(
        "--keep-outputs",
        type=Path,
        default=None,
        help="Copy rebuilt datasets and summary.md here (use with --dry-run for review).",
    )
    parser.add_argument(
        "--github-output",
        type=Path,
        default=None,
        help="Optional path for GitHub Actions output lines.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the refresh helper CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)
    outputs = prepare_refresh_release(
        repo_root=args.repo_root,
        dry_run=args.dry_run,
        force_release=args.force_release,
        full_rebuild=args.full_rebuild,
        allow_historical_changes=args.allow_historical_changes,
        keep_outputs=args.keep_outputs,
    )

    output_path = args.github_output
    if output_path is None and "GITHUB_OUTPUT" in os.environ:
        output_path = Path(os.environ["GITHUB_OUTPUT"])
    if output_path is not None:
        write_outputs(outputs, output_path)

    print(json.dumps(outputs.to_dict(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
