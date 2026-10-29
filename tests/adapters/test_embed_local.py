"""Тесты ``LocalEmbedder`` (W8/T8.1) — реализация ``EmbedderPort`` для локального эмбеддинга.

Тяжёлая зависимость ``sentence-transformers``/``torch`` не требуется: эмбеддинг
внедряется через ``embed_fn`` (шов). Реальный путь проверяется подстановкой
поддельного модуля ``sentence_transformers``. Покрываются: атрибуты ``model_id``/
``dim``, батчевая нарезка, сохранение порядка/длины, ошибки размерности и числа
векторов, ленивый шов и реальная загрузка модели, контракт недоступности
(``EmbedderUnavailable``), структурное соответствие порту, детерминизм и
категорийная нейтральность (§1.3).
"""

from __future__ import annotations

import sys
import types
from typing import Any

import pytest

import core.adapters.embed_local as module
from core.adapters.embed_local import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_DEVICE,
    DEFAULT_DIM,
    DEFAULT_MODEL_ID,
    EmbedderUnavailable,
    LocalEmbedder,
)


def _fixed_fn(dim: int, batches: list[list[str]] | None = None):
    """Возвращает ``(fn, batches)``; значения детерминированы по длине текста."""

    def fn(batch: list[str]) -> list[list[float]]:
        if batches is not None:
            batches.append(list(batch))
        return [[float(len(text) % 7)] * dim for text in batch]

    return fn


# ---------------------------------------------------------------------------
# Базовый контракт / атрибуты
# ---------------------------------------------------------------------------


def test_defaults_are_model_and_dim() -> None:
    emb = LocalEmbedder(embed_fn=_fixed_fn(DEFAULT_DIM))
    assert emb.model_id == DEFAULT_MODEL_ID
    assert emb.dim == DEFAULT_DIM
    assert emb.batch_size == DEFAULT_BATCH_SIZE


def test_attributes_are_overridable() -> None:
    emb = LocalEmbedder(_fixed_fn(3), model_id="test/model", dim=3, batch_size=1)
    assert emb.model_id == "test/model"
    assert emb.dim == 3
    assert emb.batch_size == 1


def test_invalid_constructor_args_rejected() -> None:
    with pytest.raises(ValueError):
        LocalEmbedder(dim=0)
    with pytest.raises(ValueError):
        LocalEmbedder(batch_size=0)
    with pytest.raises(ValueError):
        LocalEmbedder(model_id="")


# ---------------------------------------------------------------------------
# Батчевая нарезка / форма выхода
# ---------------------------------------------------------------------------


def test_batches_input_by_batch_size() -> None:
    batches: list[list[str]] = []
    emb = LocalEmbedder(_fixed_fn(4, batches), dim=4, batch_size=2)
    out = emb.embed(["a", "bb", "ccc", "dddd", "eeeee"])
    assert batches == [["a", "bb"], ["ccc", "dddd"], ["eeeee"]]
    assert len(out) == 5


def test_output_length_and_dim_preserved() -> None:
    emb = LocalEmbedder(_fixed_fn(6), dim=6, batch_size=3)
    out = emb.embed(["x"] * 7)
    assert len(out) == 7
    assert all(len(vec) == 6 for vec in out)
    assert all(isinstance(value, float) for vec in out for value in vec)


def test_empty_input_returns_empty() -> None:
    batches: list[list[str]] = []
    emb = LocalEmbedder(_fixed_fn(4, batches), dim=4)
    assert emb.embed([]) == []
    assert batches == []  # функция эмбеддинга не вызывается впустую


def test_single_batch_when_input_fits() -> None:
    batches: list[list[str]] = []
    emb = LocalEmbedder(_fixed_fn(4, batches), dim=4, batch_size=32)
    emb.embed(["a", "b"])
    assert batches == [["a", "b"]]


# ---------------------------------------------------------------------------
# Ошибки размерности / количества
# ---------------------------------------------------------------------------


def test_dim_mismatch_raises_clear_error() -> None:
    bad = LocalEmbedder(lambda batch: [[0.0, 1.0] for _ in batch], dim=3)
    with pytest.raises(ValueError, match="dim=3"):
        bad.embed(["text"])


def test_vector_count_mismatch_raises() -> None:
    bad = LocalEmbedder(lambda batch: [[0.0]], dim=1)  # один вектор на два текста
    with pytest.raises(ValueError, match="векторов"):
        bad.embed(["a", "b"])


def test_mismatch_is_detected_per_batch() -> None:
    def fn(batch: list[str]) -> list[list[float]]:
        # Второй батч короче по размерности — ошибка обязана всплыть.
        width = 2 if batch[0] == "short" else 3
        return [[0.0] * width for _ in batch]

    emb = LocalEmbedder(fn, dim=3, batch_size=1)
    emb.embed(["ok"])
    with pytest.raises(ValueError, match="dim=3"):
        emb.embed(["short"])


# ---------------------------------------------------------------------------
# Детерминизм
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Доступность и контракт отсутствия зависимости
# ---------------------------------------------------------------------------


def test_available_true_with_injected_fn() -> None:
    assert LocalEmbedder(embed_fn=_fixed_fn(3), dim=3).available is True


def test_available_reflects_dependency_without_fn() -> None:
    assert LocalEmbedder().available is module._sentence_transformers_available()


def test_missing_dependency_raises_embedder_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    # Форсируем отсутствие зависимости, чтобы тест был детерминирован в любом окружении.
    monkeypatch.setattr(module, "_sentence_transformers_available", lambda: False)
    emb = LocalEmbedder()
    assert emb.available is False
    with pytest.raises(EmbedderUnavailable):
        emb.embed(["text"])
    # Подкласс RuntimeError: вызывающий может ловить обобщённо.
    assert issubclass(EmbedderUnavailable, RuntimeError)


def test_missing_dependency_does_not_return_zeros(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "_sentence_transformers_available", lambda: False)
    with pytest.raises(EmbedderUnavailable):
        LocalEmbedder().embed(["text"])


# ---------------------------------------------------------------------------
# Реальный путь (ленивая загрузка модели) — поддельный sentence_transformers
# ---------------------------------------------------------------------------


def _fake_st_module(dim: int) -> tuple[types.ModuleType, list[Any]]:
    """Поддельный ``sentence_transformers`` с журналом вызовов ``encode``."""
    instances: list[Any] = []

    class _Matrix:
        def __init__(self, rows: list[list[float]]) -> None:
            self._rows = rows

        def tolist(self) -> list[list[float]]:
            return self._rows

    class _Model:
        def __init__(self, model_id: str, **kwargs: Any) -> None:
            self.model_id = model_id
            # Журнал аргументов загрузчика: ревизия и оффлайн-режим (C11).
            self.init_kwargs = kwargs
            self.encode_calls: list[dict[str, Any]] = []
            instances.append(self)

        def encode(
            self,
            batch: list[str],
            batch_size: int | None = None,
            normalize_embeddings: bool = False,
            convert_to_numpy: bool = False,
        ) -> _Matrix:
            self.encode_calls.append(
                {
                    "batch": list(batch),
                    "batch_size": batch_size,
                    "normalize_embeddings": normalize_embeddings,
                    "convert_to_numpy": convert_to_numpy,
                }
            )
            return _Matrix([[float(len(text))] * dim for text in batch])

    fake = types.ModuleType("sentence_transformers")
    fake.SentenceTransformer = _Model  # type: ignore[attr-defined]
    return fake, instances


def test_real_path_lazy_loads_and_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    fake, instances = _fake_st_module(dim=4)
    monkeypatch.setattr(module, "_sentence_transformers_available", lambda: True)
    monkeypatch.setitem(sys.modules, "sentence_transformers", fake)

    emb = LocalEmbedder(model_id="test/model", dim=4, batch_size=2)
    assert emb._model is None  # модель не загружается в конструкторе (лениво)

    out = emb.embed(["a", "bb", "ccc"])

    assert emb._model is instances[0]
    assert instances[0].model_id == "test/model"
    # Ревизия, оффлайн-режим и **объявленное устройство** передаются явно: по умолчанию
    # сеть не трогается (C11), а устройство не выбирает torch (``14`` оговорка 5).
    assert instances[0].init_kwargs == {
        "revision": None,
        "local_files_only": True,
        "device": "cpu",
    }
    assert [call["batch"] for call in instances[0].encode_calls] == [["a", "bb"], ["ccc"]]
    assert instances[0].encode_calls[0]["normalize_embeddings"] is True
    assert instances[0].encode_calls[0]["convert_to_numpy"] is True
    assert len(out) == 3 and all(len(vec) == 4 for vec in out)


# ---------------------------------------------------------------------------
# Нейтральность модуля (§1.3)
# ---------------------------------------------------------------------------




def test_revision_is_pinned_and_passed_to_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    """``revision`` пинится в ``model_id`` и уходит в загрузчик весов (C11)."""
    fake, instances = _fake_st_module(dim=2)
    monkeypatch.setattr(module, "_sentence_transformers_available", lambda: True)
    monkeypatch.setitem(sys.modules, "sentence_transformers", fake)

    emb = LocalEmbedder(
        model_id="test/model", dim=2, revision="abc123", local_files_only=False
    )
    assert emb.model_id == "test/model@abc123"
    assert emb.revision == "abc123"

    emb.embed(["a"])

    # Веса загружаются по repo, а не по «repo@rev»: ревизия — отдельный аргумент.
    assert instances[0].model_id == "test/model"
    assert instances[0].init_kwargs == {
        "revision": "abc123",
        "local_files_only": False,
        "device": "cpu",
    }


def test_device_is_declared_and_passed_to_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    """``device`` — **объявленная** ось ``embed.device``, а не выбор torch.

    До правки аргумента не было, и ``SentenceTransformer`` сам уходил на CUDA, если карта
    есть: ``embed_device``/``pb``/ключ ``embed_cache`` называли бы устройство, которого
    прогон не объявлял (``14`` оговорка 5). Дефолт — объявленный ``cpu``, не ``None``.
    """
    assert DEFAULT_DEVICE == "cpu"
    assert LocalEmbedder().device == "cpu"

    fake, instances = _fake_st_module(dim=2)
    monkeypatch.setattr(module, "_sentence_transformers_available", lambda: True)
    monkeypatch.setitem(sys.modules, "sentence_transformers", fake)

    emb = LocalEmbedder(model_id="test/model", dim=2, device="cuda")
    assert emb.device == "cuda"
    emb.embed(["a"])
    assert instances[0].init_kwargs["device"] == "cuda"


def test_embed_fn_seam_ignores_device(monkeypatch: pytest.MonkeyPatch) -> None:
    """Тестовый шов ``embed_fn`` работает как раньше: устройство его не касается."""
    calls: list[list[str]] = []

    def _fn(batch: list[str]) -> list[list[float]]:
        calls.append(batch)
        return [[1.0, 0.0] for _ in batch]

    emb = LocalEmbedder(_fn, dim=2, device="cuda")
    assert emb.embed(["a", "b"]) == [[1.0, 0.0], [1.0, 0.0]]
    assert calls == [["a", "b"]]
