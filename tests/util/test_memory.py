"""Операционный рычаг памяти: ``core/util/memory.py`` (``malloc_trim`` через ``ctypes``).

Проверяется ровно то, чем рычаг опасен для прогона:

* возвращает ``bool`` и **никогда** не поднимает (документ дороже RSS-пика);
* результат libc (``0``/``1``) транслируется честно: ``1`` — память отдана, ``0`` — нет;
* платформа без glibc / без символа (``OSError``/``AttributeError`` при загрузке) —
  безопасный no-op ``False``, без исключения;
* ссылка на ``malloc_trim`` разрешается **один раз** (кэш: не ``CDLL`` на документ);
* счётчики процесса (``trim_stats``) отражают вызовы/успехи.

Реального ``malloc_trim`` тест не требует: libc подменяется, поэтому набор детерминирован
на любой платформе (в т.ч. macOS/Windows-CI).
"""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from core.util import memory


class _FakeTrim:
    """Замена ``malloc_trim``: как ``ctypes``-функция, принимает ``argtypes``/``restype``."""

    argtypes: ClassVar[list[Any]] = []
    restype: Any = None

    def __init__(self, rc: int) -> None:
        self.rc = rc
        self.calls = 0

    def __call__(self, pad: int) -> int:
        self.calls += 1
        return self.rc


class _RaisingTrim(_FakeTrim):
    """``malloc_trim``, падающий при вызове (сбой нативной библиотеки)."""

    def __call__(self, pad: int) -> int:  # noqa: ARG002 — сигнатура libc
        raise RuntimeError("нативная библиотека отказала")


class _FakeLibc:
    """Минимальная замена ``libc.so.6`` с одним символом ``malloc_trim``."""

    def __init__(self, trim: _FakeTrim) -> None:
        self.malloc_trim = trim


@pytest.fixture(autouse=True)
def _fresh_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    """Сброс кэша разрешения и счётчиков: тесты не зависят от порядка запуска."""
    monkeypatch.setattr(memory, "_TRIM_FN", None)
    monkeypatch.setattr(memory, "_RESOLVED", False)
    monkeypatch.setattr(memory, "_CALLS", 0)
    monkeypatch.setattr(memory, "_RELEASED", 0)


def _patch_libc(monkeypatch: pytest.MonkeyPatch, trim: _FakeTrim) -> None:
    """Подменяет загрузку libc (``CDLL``) на фейк — без нативной библиотеки."""
    libc = _FakeLibc(trim)
    monkeypatch.setattr(memory.ctypes, "CDLL", lambda *_a, **_k: libc)


def test_release_free_arenas_reports_released(monkeypatch: pytest.MonkeyPatch) -> None:
    """``malloc_trim`` вернул ``1`` ⇒ ``True`` (память ОС отдана)."""
    trim = _FakeTrim(rc=1)
    _patch_libc(monkeypatch, trim)

    assert memory.release_free_arenas() is True
    assert trim.calls == 1
    assert memory.trim_stats() == {"calls": 1, "released": 1}


def test_release_free_arenas_reports_nothing_to_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``malloc_trim`` вернул ``0`` ⇒ ``False``; это не ошибка и не исключение."""
    trim = _FakeTrim(rc=0)
    _patch_libc(monkeypatch, trim)

    assert memory.release_free_arenas() is False
    assert trim.calls == 1
    assert memory.trim_stats() == {"calls": 1, "released": 0}


def test_release_free_arenas_is_glibc_only_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    """Нет ``libc.so.6`` (не-glibc) ⇒ ``False`` без исключения; загрузка — однократная."""

    def _boom(*_args: Any, **_kwargs: Any) -> Any:
        raise OSError("libc.so.6: cannot open shared object file")

    monkeypatch.setattr(memory.ctypes, "CDLL", _boom)

    assert memory.release_free_arenas() is False
    # Разрешение кэшировано: второй вызов не пытается снова грузить библиотеку.
    assert memory.release_free_arenas() is False
    assert memory.trim_stats() == {"calls": 2, "released": 0}


def test_release_free_arenas_missing_symbol_is_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Библиотека есть, но ``malloc_trim`` нет (musl/BSD-подобная libc) ⇒ ``False``."""

    class _NoTrim:
        def __getattr__(self, name: str) -> Any:
            raise AttributeError(name)

    monkeypatch.setattr(memory.ctypes, "CDLL", lambda *_a, **_k: _NoTrim())

    assert memory.release_free_arenas() is False


def test_release_free_arenas_swallows_call_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Сбой самого вызова (не наш код) ⇒ ``False``, вызывающий не видит исключения."""
    _patch_libc(monkeypatch, _RaisingTrim(rc=0))

    assert memory.release_free_arenas() is False
    assert memory.trim_stats()["released"] == 0


def test_malloc_trim_resolved_once_per_process(monkeypatch: pytest.MonkeyPatch) -> None:
    """``CDLL``/``dlsym`` платятся один раз, а не на каждый документ."""
    loads: list[int] = []
    trim = _FakeTrim(rc=1)
    libc = _FakeLibc(trim)

    def _load(*_args: Any, **_kwargs: Any) -> _FakeLibc:
        loads.append(1)
        return libc

    monkeypatch.setattr(memory.ctypes, "CDLL", _load)

    for _ in range(5):
        memory.release_free_arenas()

    assert len(loads) == 1
    assert trim.calls == 5


def test_trim_stats_is_per_process_snapshot() -> None:
    """``trim_stats`` — снимок счётчиков процесса (ключи фиксированы, значения — ``int``)."""
    stats = memory.trim_stats()
    assert set(stats) == {"calls", "released"}
    assert all(isinstance(value, int) for value in stats.values())
    # Снимок, а не живая ссылка: мутация результата не меняет счётчики модуля.
    stats["calls"] = 10_000
    assert memory.trim_stats()["calls"] == 0


def test_release_free_arenas_never_raises_on_real_platform() -> None:
    """На реальной платформе вызов безопасен: ``bool`` и никаких исключений.

    На glibc это настоящий ``malloc_trim`` (объём памяти процесса — не предмет теста);
    на не-glibc — документированный no-op ``False``.
    """
    assert isinstance(memory.release_free_arenas(), bool)
