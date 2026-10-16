"""Тесты CLI-флагов контроля краула (D1/D2).

Флаги ``hound run``: ``--max-depth``/``--max-documents``/``--exclude-path``/``--exclude-document``
уточняют **run-слой** (поверх ``run.yaml:crawl``); слияние в ``CrawlRunConfig`` проверяется в
``tests/pipeline/test_phases.py`` (локальные хелперы).
"""

from __future__ import annotations

from typing import Any

import pytest

from core.cli.main import build_parser


def _run_parser() -> Any:
    return build_parser()


def test_run_parser_exposes_crawl_control_flags() -> None:
    args = _run_parser().parse_args(
        [
            "run",
            "--run-yaml",
            "r.yaml",
            "--package",
            "pkg",
            "--max-depth",
            "2",
            "--max-documents",
            "10",
            "--exclude-path",
            "/private/*",
            "--exclude-path",
            "/tmp/*",
            "--exclude-document",
            "*.pdf",
        ]
    )
    assert args.max_depth == 2
    assert args.max_documents == 10
    assert args.exclude_paths == ["/private/*", "/tmp/*"]
    assert args.exclude_documents == ["*.pdf"]


def test_run_parser_rejects_negative_max_depth() -> None:
    with pytest.raises(SystemExit):
        _run_parser().parse_args(
            ["run", "--run-yaml", "r.yaml", "--package", "pkg", "--max-depth", "-1"]
        )


def test_run_parser_rejects_zero_max_documents() -> None:
    with pytest.raises(SystemExit):
        _run_parser().parse_args(
            ["run", "--run-yaml", "r.yaml", "--package", "pkg", "--max-documents", "0"]
        )
