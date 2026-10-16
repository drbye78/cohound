"""Тесты ``core/util/logging.py`` (W12/T12.1, 04 §B.1).

Покрывают: одну строку на вызов, поля записи, валидность JSON, append между вызовами,
создание каталогов по требованию, инжектируемые часы (детерминированный ``ts``),
фильтрацию по ``min_level``, stdlib-fallback без ``structlog`` и нейтральность (§1.3).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

import core.util.logging as phlog
from core.util.logging import PhaseLogger


def _lines(path: Path) -> list[dict]:
    """Разбирает файл JSON-lines в список событий."""
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_one_line_per_call_and_fields(tmp_path: Path) -> None:
    logger = PhaseLogger("run_1", "evaluate", tmp_path)
    returned = logger.info("старт фазы", product="prod_a", param="param_x")

    events = _lines(logger.path)
    assert len(events) == 1
    event = events[0]
    assert returned == event
    assert set(event) == {
        "ts",
        "run_id",
        "phase",
        "component",
        "level",
        "message",
        "ctx",
        "invariant",
    }
    assert event["run_id"] == "run_1"
    assert event["phase"] == "evaluate"
    assert event["component"] == "evaluate"  # по умолчанию — фаза
    assert event["level"] == "INFO"
    assert event["message"] == "старт фазы"
    assert event["ctx"] == {"product": "prod_a", "param": "param_x"}
    assert event["invariant"] is None


def test_append_across_calls(tmp_path: Path) -> None:
    logger = PhaseLogger("run_1", "corpus", tmp_path)
    logger.debug("d")
    logger.info("i")
    logger.warn("w")
    logger.error("e")
    events = _lines(logger.path)
    assert [e["level"] for e in events] == ["DEBUG", "INFO", "WARN", "ERROR"]
    assert [e["message"] for e in events] == ["d", "i", "w", "e"]


def test_dirs_created_on_demand(tmp_path: Path) -> None:
    root = tmp_path / "does" / "not" / "exist"
    logger = PhaseLogger("run_x", "plan", root)
    assert not root.exists()
    assert not logger.path.exists()
    logger.info("создание")
    assert logger.path.is_file()
    assert logger.path == root / "run_x" / "logs" / "plan.jsonl"


def test_injected_clock_is_deterministic(tmp_path: Path) -> None:
    stamp = "2026-10-08T00:00:00+00:00"
    logger = PhaseLogger("run_1", "evaluate", tmp_path, now=lambda: stamp)
    logger.info("a")
    logger.info("b")
    events = _lines(logger.path)
    assert [e["ts"] for e in events] == [stamp, stamp]


def test_component_override(tmp_path: Path) -> None:
    logger = PhaseLogger("run_1", "evaluate", tmp_path, component="llm")
    event = logger.info("x")
    assert event is not None
    assert event["component"] == "llm"


def test_min_level_filters(tmp_path: Path) -> None:
    logger = PhaseLogger("run_1", "evaluate", tmp_path, min_level="WARN")
    assert logger.debug("no") is None
    assert logger.info("no") is None
    assert not logger.path.exists()
    assert logger.warn("yes") is not None
    assert logger.error("yes") is not None
    assert [e["level"] for e in _lines(logger.path)] == ["WARN", "ERROR"]


def test_invalid_level_raises(tmp_path: Path) -> None:
    logger = PhaseLogger("run_1", "evaluate", tmp_path)
    with pytest.raises(ValueError):
        logger.log("TRACE", "nope")


def test_unicode_preserved(tmp_path: Path) -> None:
    logger = PhaseLogger("run_1", "evaluate", tmp_path)
    logger.info("привет", note="значение")
    raw = logger.path.read_text(encoding="utf-8")
    assert "привет" in raw  # ensure_ascii=False
    assert _lines(logger.path)[0]["ctx"]["note"] == "значение"


def test_stdlib_fallback_without_structlog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(phlog, "HAS_STRUCTLOG", False)
    logger = PhaseLogger("run_1", "report", tmp_path, now=lambda: "T")
    logger.warn("fallback", product="prod_a")
    events = _lines(logger.path)
    assert len(events) == 1
    assert events[0]["level"] == "WARN"
    assert events[0]["ctx"] == {"product": "prod_a"}
    # Форма детерминирована: ключи отсортированы.
    raw = logger.path.read_text(encoding="utf-8").strip()
    assert raw == json.dumps(events[0], ensure_ascii=False, sort_keys=True)


def test_structlog_backend_shape_matches_when_available(tmp_path: Path) -> None:
    """Если ``structlog`` доступен — форма записи идентична stdlib-пути."""
    if not phlog.HAS_STRUCTLOG:
        pytest.skip("structlog не установлен")
    logger = PhaseLogger("run_1", "evaluate", tmp_path, now=lambda: "T")
    logger.info("x", k="v")
    event = _lines(logger.path)[0]
    raw = logger.path.read_text(encoding="utf-8").strip()
    assert raw == json.dumps(event, ensure_ascii=False, sort_keys=True)


