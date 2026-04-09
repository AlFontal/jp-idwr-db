from __future__ import annotations

import runpy
from pathlib import Path

import pytest

from jp_idwr_db import cli


def test_build_parser_parses_download_command() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(["data", "download", "--version", "latest", "--force"])

    assert args.command == "data"
    assert args.data_command == "download"
    assert args.version == "latest"
    assert args.force is True


def test_main_download_calls_ensure_data(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    expected = tmp_path / "cache" / "data" / "v-test"
    captured: dict[str, object] = {}

    def fake_ensure_data(*, version: str | None = None, force: bool = False) -> Path:
        captured["version"] = version
        captured["force"] = force
        return expected

    monkeypatch.setattr(cli, "ensure_data", fake_ensure_data)

    exit_code = cli.main(["data", "download", "--version", "v-test", "--force"])

    assert exit_code == 0
    assert captured == {"version": "v-test", "force": True}
    assert capsys.readouterr().out.strip() == str(expected)


def test_main_without_command_prints_help(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = cli.main([])

    assert exit_code == 0
    assert "usage: jp-idwr-db" in capsys.readouterr().out


def test_module_entrypoint_exits_with_cli_status(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "main", lambda: 7)

    with pytest.raises(SystemExit, match="7"):
        runpy.run_module("jp_idwr_db", run_name="__main__")
