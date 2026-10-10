"""Хелперы вывода CLI-тестов: чтение JSON-объекта из stdout (``--json``)."""

from __future__ import annotations

import json
from typing import Any

import pytest

__all__ = ["json_out"]


def json_out(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    """Читает единственный JSON-объект из stdout."""
    captured = capsys.readouterr()
    return json.loads(captured.out.strip().splitlines()[-1])
