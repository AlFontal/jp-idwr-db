from __future__ import annotations

from datetime import date
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType

import polars as pl


def _load_module() -> ModuleType:
    script = Path(__file__).resolve().parents[1] / "scripts" / "update_release_docs.py"
    spec = spec_from_file_location("jp_idwr_db_update_release_docs", script)
    if spec is None or spec.loader is None:
        raise AssertionError("Could not load update_release_docs.py")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_update_readme_snapshot_refreshes_source_attribution(tmp_path: Path) -> None:
    module = _load_module()
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)
    pl.DataFrame(
        {
            "date": [date(2026, 1, 5)],
            "prefecture": ["Tokyo"],
            "category": ["total"],
            "disease": ["Measles"],
            "count": [1.0],
            "source": ["All-case reporting"],
        }
    ).write_parquet(data_dir / "unified.parquet")
    readme = tmp_path / "README.md"
    readme.write_text(
        "print(df)\n```\n\n"
        "<!-- BEGIN GENERATED UNIFIED SNAPSHOT -->\nold\n"
        "<!-- END GENERATED UNIFIED SNAPSHOT -->\n"
        "accessed 2026-01-01 for release `v2026.1.1`.\n",
        encoding="utf-8",
    )
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[project]\nversion = "2026.9.2"\n', encoding="utf-8")
    module.DATA_DIR = data_dir
    module.README = readme
    module.PYPROJECT = pyproject

    module.update_readme_snapshot(date(2026, 9, 2))

    updated = readme.read_text(encoding="utf-8")
    assert "shape: (1, 6)" in updated
    assert "accessed 2026-09-02 for release `v2026.9.2`." in updated
