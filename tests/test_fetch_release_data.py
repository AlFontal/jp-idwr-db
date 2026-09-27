from __future__ import annotations

import json
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType

import polars as pl
import pytest


def _load_script() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts" / "fetch_release_data.py"
    spec = spec_from_file_location("fetch_release_data", path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _fake_cache(root: Path, tag: str, names: set[str]) -> Path:
    root.mkdir(parents=True)
    for name in names:
        pl.DataFrame({"x": [1]}).write_parquet(root / name)
    (root / "manifest.json").write_text(json.dumps({"release_tag": tag}), encoding="utf-8")
    return root


def test_fetch_release_data_copies_verified_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = _load_script()
    cache = _fake_cache(tmp_path / "cache", "v2026.9.30", script.EXPECTED_DATASETS)
    monkeypatch.setattr(script, "ensure_data", lambda version: cache)

    tag = script.fetch_release_data("v2026.9.30", tmp_path / "dest")

    assert tag == "v2026.9.30"
    copied = {p.name for p in (tmp_path / "dest").iterdir()}
    assert copied == set(script.EXPECTED_DATASETS) | {".release_tag"}


def test_fetch_release_data_rejects_a_different_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = _load_script()
    cache = _fake_cache(tmp_path / "cache", "v2026.9.16", script.EXPECTED_DATASETS)
    monkeypatch.setattr(script, "ensure_data", lambda version: cache)

    with pytest.raises(SystemExit, match=r"expected v2026\.9\.30"):
        script.fetch_release_data("v2026.9.30", tmp_path / "dest")
