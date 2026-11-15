"""Доставка rec-моделей OCR (`scripts/fetch_ocr_models.py`).

Проверяется **прибор**, а не сеть: состояние файла (нет / есть / чужой хеш), отказ на
не-https адрес, отказ на несовпавший SHA-256 и то, что при отказе целевой файл не
появляется. Сеть не нужна ни одному тесту — `urlopen` подменяется, а объявленные модели
берутся из `MODELS` (пин адреса и хеша — часть контракта, а не деталь реализации).

Зачем это тестировать вообще: отсутствие файла модели не выглядит поломкой — `full`
просто отказывает `OcrEngineUnavailableError`, а на хосте, где файл когда-то скачали, всё
зелено. Ровно так CI был красным 15 из 15 тестов после `65fc169` при зелёном локальном
прогоне; прибор должен ловить это до прогона.
"""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "fetch_ocr_models.py"


def _load() -> Any:
    """Загружает прибор как модуль: `scripts/` не пакет."""
    spec = importlib.util.spec_from_file_location("fetch_ocr_models", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["fetch_ocr_models"] = module
    spec.loader.exec_module(module)
    return module


FETCH = _load()
PAYLOAD = b"model-bytes"
DIGEST = hashlib.sha256(PAYLOAD).hexdigest()


def _spec(**overrides: str) -> Any:
    base = {
        "name": "model.onnx",
        "url": "https://models.test/model.onnx",
        "sha256": DIGEST,
    }
    base.update(overrides)
    return FETCH.ModelSpec(**base)


class _Response:
    """Минимальный ответ `urlopen`: контекстный менеджер с `read(size)`."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._offset = 0

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        chunk = self._payload[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk


def test_state_of_missing_present_and_foreign_hash(tmp_path: Path) -> None:
    """Три состояния различаются: нет файла, верный хеш, чужой хеш."""
    spec = _spec()
    assert FETCH.state_of(spec, tmp_path)["present"] is False
    assert FETCH.state_of(spec, tmp_path)["ok"] is False

    (tmp_path / spec.name).write_bytes(PAYLOAD)
    state = FETCH.state_of(spec, tmp_path)
    assert state["present"] is True
    assert state["ok"] is True
    assert state["sha256"] == DIGEST

    # Тот же размер, другой байт: размер не доказательство, хеш — доказательство.
    (tmp_path / spec.name).write_bytes(b"model-bytez")
    state = FETCH.state_of(spec, tmp_path)
    assert state["present"] is True
    assert state["ok"] is False
    assert state["sha256"] != DIGEST


def test_download_refuses_non_https_before_any_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Не-https адрес отвергается **до** открытия: `urlopen` принял бы и `file:`."""
    calls: list[str] = []

    def _fail(*args: Any, **kwargs: Any) -> Any:  # pragma: no cover - не должен зваться
        calls.append("urlopen")
        raise AssertionError("urlopen не должен вызываться на не-https адрес")

    monkeypatch.setattr(FETCH.urllib.request, "urlopen", _fail)
    result = FETCH.download(_spec(url="file:///etc/passwd"), tmp_path)
    assert result["ok"] is False
    assert "https" in result["error"]
    assert calls == []
    assert not (tmp_path / "model.onnx").exists()


def test_download_verifies_sha256_and_writes_atomically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Хеш совпал — файл на месте; временный `.part` не остаётся."""
    monkeypatch.setattr(
        FETCH.urllib.request, "urlopen", lambda *a, **k: _Response(PAYLOAD)
    )
    result = FETCH.download(_spec(), tmp_path)
    assert result["ok"] is True
    assert (tmp_path / "model.onnx").read_bytes() == PAYLOAD
    assert [p.name for p in tmp_path.iterdir()] == ["model.onnx"]


def test_download_refuses_mismatched_hash_and_leaves_nothing_behind(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Подменённые веса — отказ: половина модели на месте читалась бы как целая.

    Это не перестраховка: другой файл даёт другой текст под тем же
    `extractor_version`, то есть провенанс назвал бы распознаватель, которого не было.
    """
    monkeypatch.setattr(
        FETCH.urllib.request, "urlopen", lambda *a, **k: _Response(b"other-bytes")
    )
    result = FETCH.download(_spec(), tmp_path)
    assert result["ok"] is False
    assert "sha256" in result["error"]
    assert list(tmp_path.iterdir()) == []


def test_main_check_reports_missing_present_and_mismatched(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """`--check` не качает: код 1 при отсутствии и при чужом хеше, 0 при верном файле.

    Объявленные модели подменяются локальной: тест не должен зависеть от того, скачана ли
    настоящая модель на этом хосте, — иначе он зеленел бы/краснел от состояния машины,
    то есть повторял бы ровно тот дефект, который прибор и ловит.
    """
    monkeypatch.setattr(FETCH, "MODELS", (_spec(),))
    assert FETCH.main(["--check", "--dest", str(tmp_path)]) == 1
    assert "НЕТ" in capsys.readouterr().out

    # Файл есть, но байты чужие: `present` без `ok` — по-прежнему код 1.
    (tmp_path / "model.onnx").write_bytes(b"other-bytes")
    assert FETCH.main(["--check", "--dest", str(tmp_path), "--json"]) == 1
    payload = capsys.readouterr().out
    assert '"ok": false' in payload
    assert "model.onnx" in payload

    # Верный файл — код 0 и явное «ok».
    (tmp_path / "model.onnx").write_bytes(PAYLOAD)
    assert FETCH.main(["--check", "--dest", str(tmp_path)]) == 0
    assert "ok" in capsys.readouterr().out


def test_declared_models_are_pinned_by_sha256() -> None:
    """Пины не пустые и не выдуманы: https, 64 hex-символа, имя — файл модели."""
    assert FETCH.MODELS, "список объявленных моделей пуст"
    for spec in FETCH.MODELS:
        assert spec.url.startswith("https://")
        assert len(spec.sha256) == 64
        assert all(char in "0123456789abcdef" for char in spec.sha256)
        assert spec.name.endswith(".onnx")
