from __future__ import annotations

from pathlib import Path

import pytest

from jp_idwr_db._internal import release_utils


def test_configured_value_prefers_current_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JPINFECT_DATA_VERSION", "v-old")
    monkeypatch.setenv("JP_IDWR_DB_DATA_VERSION", "v-current")

    assert release_utils.configured_value("DATA_VERSION") == "v-current"


def test_configured_value_supports_legacy_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JPINFECT_CACHE_DIR", "/legacy-cache")

    assert release_utils.configured_value("CACHE_DIR") == "/legacy-cache"


def test_release_helpers(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact"
    artifact.write_bytes(b"jp-idwr-db")

    assert release_utils.normalize_release_tag("2026.9.2") == "v2026.9.2"
    assert release_utils.normalize_release_tag("latest") == "latest"
    assert release_utils.quote_identifier('a"b') == '"a""b"'
    assert release_utils.sha256(artifact) == (
        "b4d4cd605b11730e6870b2c99f63aaf595789860a35f6e578478e9cca67db73c"
    )
