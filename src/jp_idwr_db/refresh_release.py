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

from ._internal import validation
from ._internal.release_utils import sha256 as file_sha256
from .utils import PREFECTURE_ISO_MAP

TARGET_OUTPUTS = (
    Path("data/parquet/bullet.parquet"),
    Path("data/parquet/sentinel.parquet"),
    Path("data/parquet/unified.parquet"),
    Path("docs/DISEASES.md"),
)
VALIDATED_OUTPUTS = TARGET_OUTPUTS[:3]

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
    """Capture the digest state for generated release outputs."""
    return {str(rel_path): _sha256(repo_root / rel_path) for rel_path in TARGET_OUTPUTS}


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
    for flag in ("--bullet-only", "--sentinel-only", "--unified-only"):
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


def _historical_signature(path: Path, before_year: int) -> tuple[tuple[str, ...], int, int, int]:
    """Return an order-independent signature for immutable historical rows."""
    scan = pl.scan_parquet(path).filter(pl.col("year") < before_year)
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


def _period_row_counts(path: Path) -> dict[tuple[int, int], int]:
    """Return row counts for every observed surveillance period."""
    counts = pl.scan_parquet(path).group_by(["year", "week"]).agg(pl.len().alias("rows")).collect()
    return {
        (int(row["year"]), int(row["week"])): int(row["rows"])
        for row in counts.iter_rows(named=True)
    }


def _validate_release_preservation(repo_root: Path, backup_root: Path) -> None:
    """Reject refreshes that regress recency or alter stable historical data."""
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

        previous_counts = _period_row_counts(previous)
        rebuilt_counts = _period_row_counts(rebuilt)
        regressed_periods = [
            (period, rows, rebuilt_counts.get(period, 0))
            for period, rows in previous_counts.items()
            if rebuilt_counts.get(period, 0) < rows
        ]
        if regressed_periods:
            raise ValueError(
                f"Previously published periods lost rows in {rel_path}. "
                f"First regressions: {sorted(regressed_periods)[:10]}"
            )

        stable_before_year = previous_latest[0]
        if _historical_signature(previous, stable_before_year) != _historical_signature(
            rebuilt, stable_before_year
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
        expected_sources = {
            "bullet.parquet": {"All-case reporting"},
            "sentinel.parquet": {"Sentinel surveillance"},
            "unified.parquet": {
                "Confirmed cases",
                "All-case reporting",
                "Sentinel surveillance",
            },
        }
        validation.validate_allowed_values(df, "source", expected_sources[rel_path.name])
        if rel_path.name == "unified.parquet":
            validation.validate_allowed_values(df, "category", {"total"})
        validation.validate_no_duplicates(df)
        validation.validate_date_ranges(df)
        if "date" in df.columns:
            validation.validate_iso_week_start_dates(df)
        validation.validate_non_negative_counts(df)
        allowed_prefecture_counts: dict[validation.CoverageKey, int | set[int]] | None = (
            {(2016, 37, "Sentinel surveillance"): 26}
            if rel_path.name in {"sentinel.parquet", "unified.parquet"}
            else None
        )
        validation.validate_prefecture_coverage(df, allowed_counts=allowed_prefecture_counts)
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
        "- Automated bi-weekly data refresh release.\n\n"
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
    release_date: date | None = None,
) -> RefreshOutputs:
    """Rebuild release outputs and prepare a calendar release when data changed."""
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
            rebuild_release_outputs(resolved_root)
            _validate_release_outputs(resolved_root)
            _validate_release_preservation(resolved_root, backup_root)
            after = _snapshot_paths(resolved_root)
            changed = before != after or force_release
            latest_bullet_week = _format_year_week(resolved_root / TARGET_OUTPUTS[0])
            latest_sentinel_week = _format_year_week(resolved_root / TARGET_OUTPUTS[1])

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
