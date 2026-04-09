from __future__ import annotations

import json
from pathlib import Path

import pytest

from jp_idwr_db import build_release_assets


def test_validate_with_json_schema_calls_jsonschema_validate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = {"name": "ok"}
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps({"type": "object"}), encoding="utf-8")

    captured: dict[str, object] = {}

    class FakeJsonSchema:
        @staticmethod
        def validate(*, instance: dict[str, object], schema: dict[str, object]) -> None:
            captured["instance"] = instance
            captured["schema"] = schema

    monkeypatch.setattr(
        build_release_assets.importlib,
        "import_module",
        lambda name: FakeJsonSchema if name == "jsonschema" else None,
    )

    build_release_assets._validate_with_json_schema(manifest, schema_path)

    assert captured == {
        "instance": manifest,
        "schema": {"type": "object"},
    }


def test_validate_with_json_schema_requires_dependency(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema_path = tmp_path / "schema.json"
    schema_path.write_text("{}", encoding="utf-8")

    def fake_import_module(name: str) -> None:
        raise ImportError(name)

    monkeypatch.setattr(build_release_assets.importlib, "import_module", fake_import_module)

    with pytest.raises(RuntimeError, match="jsonschema is required"):
        build_release_assets._validate_with_json_schema({}, schema_path)


def test_main_builds_manifest_and_skips_duckdb_when_requested(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    data_dir = tmp_path / "release-data"
    data_dir.mkdir()
    manifest_path = data_dir / "manifest.json"
    schema_path = tmp_path / "schema.json"
    schema_path.write_text("{}", encoding="utf-8")

    called: dict[str, object] = {}
    manifest = {
        "spec_version": "1.0.0",
        "dataset_id": "jp_idwr_db",
        "data_version": "2026.3.26",
        "release_tag": "v2026.3.26",
        "published_at": "2026-03-26T00:00:00Z",
        "license": "GPL-3.0-or-later",
        "homepage": "https://example.invalid",
        "assets_base_url": "https://example.invalid/v2026.3.26",
        "tables": [{"name": "unified", "file": "unified.parquet", "format": "parquet"}],
    }

    def fake_build_manifest(
        *, data_dir: Path, release_tag: str, base_url: str, out_path: Path
    ) -> dict[str, object]:
        called["build_manifest"] = {
            "data_dir": data_dir,
            "release_tag": release_tag,
            "base_url": base_url,
            "out_path": out_path,
        }
        out_path.write_text("{}", encoding="utf-8")
        return manifest

    monkeypatch.setattr(build_release_assets, "build_manifest", fake_build_manifest)
    monkeypatch.setattr(
        build_release_assets,
        "validate_manifest",
        lambda value: called.setdefault("validated", value),
    )
    monkeypatch.setattr(
        build_release_assets,
        "_validate_with_json_schema",
        lambda value, path: called.setdefault("schema_validation", (value, path)),
    )
    monkeypatch.setattr(
        build_release_assets,
        "build_duckdb",
        lambda **_: pytest.fail("build_duckdb should not run when --no-duckdb is set"),
    )

    exit_code = build_release_assets.main(
        [
            "--data-dir",
            str(data_dir),
            "--release-tag",
            "v2026.3.26",
            "--base-url",
            "https://example.invalid/v2026.3.26",
            "--schema-path",
            str(schema_path),
            "--no-duckdb",
        ]
    )

    assert exit_code == 0
    assert called["build_manifest"] == {
        "data_dir": data_dir.resolve(),
        "release_tag": "v2026.3.26",
        "base_url": "https://example.invalid/v2026.3.26",
        "out_path": manifest_path.resolve(),
    }
    assert called["validated"] == manifest
    assert called["schema_validation"] == (manifest, schema_path.resolve())
    assert f"Wrote {manifest_path.resolve()}" in capsys.readouterr().out
