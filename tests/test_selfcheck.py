"""Обёртка pytest над автономным ``tests/selfcheck.py`` (W1/T1.5).

Selfcheck остаётся самодостаточным (запускается без pytest); здесь он собирается в один
тест, чтобы ``pytest -q`` покрывал весь фундамент.
"""

from __future__ import annotations

import importlib

import pytest

import selfcheck


@pytest.mark.slow
def test_selfcheck_green() -> None:
    importlib.reload(selfcheck)
    assert selfcheck.main() == 0
