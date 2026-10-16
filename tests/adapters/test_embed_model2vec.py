"""Тесты ``LiteEmbedder`` (P4a) — оффлайн-эмбеддер ``model2vec`` как ``EmbedderPort``.

Тяжёлая зависимость/сеть не требуется: реальный путь проверяется подстановкой поддельного
модуля ``model2vec`` и локальным ``model_path``. Покрываются: атрибуты ``model_id``/``dim``,
пин ревизии в ``model_id``, батчевая нарезка, форма/длина/тип выхода, guard пустого/
пробельного входа (model2vec вернул бы нулевой вектор), ошибки размерности/числа векторов,
контракт недоступности (``EmbedderUnavailable`` — не нулевые векторы), обработка 1-D
одиночного вектора, детерминизм, структурное соответствие порту. Нейтральны (§1.3).
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import Any

import pytest

import core.adapters.embed_model2vec as module
from core.adapters.embed_model2vec import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_DIM,
    DEFAULT_MODEL_ID,
    EmbedderUnavailable,
    LiteEmbedder,
)
from core.ports.embedder import EmbedderPort

try:  # необязательная зависимость
    import numpy as np  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - зависит от окружения
    np = None  # type: ignore[assignment]

requires_numpy = pytest.mark.skipif(np is None, reason="numpy не установлен")


def _fixed_fn(dim: int, batches: list[list[str]] | None = None):
    """Шов ``embed_fn``: детерминированные значения по длине текста."""

    def fn(batch: list[str]) -> list[list[float]]:
        if batches is not None:
            batches.append(list(batch))
        return [[float(len(text) % 7)] * dim for text in batch]

    return fn


# ---------------------------------------------------------------------------
# Базовый контракт / атрибуты
# ---------------------------------------------------------------------------


def test_defaults_are_model_and_dim() -> None:
    emb = LiteEmbedder(embed_fn=_fixed_fn(DEFAULT_DIM))
    assert emb.model_id == DEFAULT_MODEL_ID
    assert emb.dim == DEFAULT_DIM == 256
    assert emb.batch_size == DEFAULT_BATCH_SIZE
    assert emb.max_length is None


def test_revision_is_folded_into_model_id() -> None:
    emb = LiteEmbedder(embed_fn=_fixed_fn(3), dim=3, revision="abc123")
    # Ревизия — версионная граница (§13): течёт в embed_model_id/index_build_id.
    assert emb.model_id == f"{DEFAULT_MODEL_ID}@abc123"
    assert emb.revision == "abc123"


def test_implements_embedder_port() -> None:
    emb = LiteEmbedder(embed_fn=_fixed_fn(DEFAULT_DIM))
    assert isinstance(emb, EmbedderPort)
    assert isinstance(emb.model_id, str)
    assert isinstance(emb.dim, int)


def test_invalid_constructor_args_rejected() -> None:
    with pytest.raises(ValueError):
        LiteEmbedder(dim=0)
    with pytest.raises(ValueError):
        LiteEmbedder(batch_size=0)
    with pytest.raises(ValueError):
        LiteEmbedder(model_id="")
    with pytest.raises(ValueError):
        LiteEmbedder(max_length=0)


# ---------------------------------------------------------------------------
# Батчевая нарезка / форма выхода
# ---------------------------------------------------------------------------


def test_batches_input_by_batch_size() -> None:
    batches: list[list[str]] = []
    emb = LiteEmbedder(_fixed_fn(4, batches), dim=4, batch_size=2)
    out = emb.embed(["a", "bb", "ccc", "dddd", "eeeee"])
    assert batches == [["a", "bb"], ["ccc", "dddd"], ["eeeee"]]
    assert len(out) == 5


def test_output_length_and_dim_preserved() -> None:
    emb = LiteEmbedder(_fixed_fn(6), dim=6, batch_size=3)
    out = emb.embed(["x"] * 7)
    assert len(out) == 7
    assert all(len(vec) == 6 for vec in out)
    assert all(isinstance(value, float) for vec in out for value in vec)


def test_empty_input_returns_empty() -> None:
    batches: list[list[str]] = []
    emb = LiteEmbedder(_fixed_fn(4, batches), dim=4)
    assert emb.embed([]) == []
    assert batches == []  # функция эмбеддинга не вызывается впустую


# ---------------------------------------------------------------------------
# Guard пустого входа (model2vec вернул бы нулевой вектор)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", ["", "   ", "\t\n"])
def test_empty_or_whitespace_text_rejected(bad: str) -> None:
    batches: list[list[str]] = []
    emb = LiteEmbedder(_fixed_fn(4, batches), dim=4)
    with pytest.raises(ValueError, match="нулевой вектор"):
        emb.embed([bad])
    # Вход отвергнут до вызова модели: нулевые векторы в индекс не попадают.
    assert batches == []


def test_one_blank_among_valid_rejected() -> None:
    emb = LiteEmbedder(_fixed_fn(3), dim=3)
    with pytest.raises(ValueError):
        emb.embed(["valid", "  "])


# ---------------------------------------------------------------------------
# Ошибки размерности / количества
# ---------------------------------------------------------------------------


def test_dim_mismatch_raises_clear_error() -> None:
    bad = LiteEmbedder(lambda batch: [[0.0, 1.0] for _ in batch], dim=3)
    with pytest.raises(ValueError, match="dim=3"):
        bad.embed(["text"])


def test_vector_count_mismatch_raises() -> None:
    bad = LiteEmbedder(lambda batch: [[0.0]], dim=1)  # один вектор на два текста
    with pytest.raises(ValueError, match="векторов"):
        bad.embed(["a", "b"])


# ---------------------------------------------------------------------------
# Детерминизм
# ---------------------------------------------------------------------------


def test_deterministic_across_calls() -> None:
    emb = LiteEmbedder(_fixed_fn(5), dim=5, batch_size=2)
    first = emb.embed(["alpha", "beta", "gamma"])
    second = emb.embed(["alpha", "beta", "gamma"])
    assert first == second


# ---------------------------------------------------------------------------
# Доступность и контракт отсутствия зависимости/модели
# ---------------------------------------------------------------------------


def test_available_true_with_injected_fn() -> None:
    assert LiteEmbedder(embed_fn=_fixed_fn(3), dim=3).available is True


def test_available_false_without_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    # Снапшота нет (пустой локальный путь): адаптер не готов, но не бросает.
    monkeypatch.setattr(module, "model2vec_available", lambda: True)
    emb = LiteEmbedder(model_path="/nonexistent/model/dir")
    assert emb.available is False


def test_missing_dependency_raises_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "model2vec_available", lambda: False)
    emb = LiteEmbedder()
    assert emb.available is False
    with pytest.raises(EmbedderUnavailable):
        emb.embed(["text"])
    assert issubclass(EmbedderUnavailable, RuntimeError)


def test_missing_snapshot_raises_unavailable_not_zeros(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(module, "model2vec_available", lambda: True)
    emb = LiteEmbedder(model_path="/nonexistent/model/dir")
    with pytest.raises(EmbedderUnavailable):
        emb.embed(["text"])
    assert emb.available is False


def test_missing_huggingface_hub_degrades_to_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(module, "model2vec_available", lambda: True)
    # ``from huggingface_hub import snapshot_download`` упадёт (None в sys.modules).
    monkeypatch.setitem(sys.modules, "huggingface_hub", None)
    emb = LiteEmbedder(model_id="owner/model")  # без model_path → резолв через HF
    assert emb.available is False
    with pytest.raises(EmbedderUnavailable):
        emb.embed(["text"])


# ---------------------------------------------------------------------------
# Реальный путь — поддельный model2vec + локальный model_path
# ---------------------------------------------------------------------------


def _fake_model2vec(dim: int, *, one_d_for_single: bool = False) -> tuple[types.ModuleType, list[Any]]:
    """Поддельный ``model2vec.StaticModel`` с журналом вызовов ``encode``."""
    instances: list[Any] = []

    class _Model:
        def __init__(self) -> None:
            self.encode_calls: list[dict[str, Any]] = []
            instances.append(self)

        @classmethod
        def from_pretrained(cls, path: str) -> "_Model":
            model = cls()
            model.path = path
            return model

        def encode(
            self,
            batch: list[str] | str,
            batch_size: int | None = None,
            max_length: int | None = None,
            use_multiprocessing: bool = True,
        ) -> Any:
            self.encode_calls.append(
                {
                    "batch": batch,
                    "batch_size": batch_size,
                    "max_length": max_length,
                    "use_multiprocessing": use_multiprocessing,
                }
            )
            texts = [batch] if isinstance(batch, str) else list(batch)
            rows = [[float(len(text))] * dim for text in texts]
            if one_d_for_single and len(rows) == 1:
                return np.asarray(rows[0], dtype=np.float64)  # 1-D
            return np.asarray(rows, dtype=np.float64)  # float64 → адаптер пинит float32

    fake = types.ModuleType("model2vec")
    fake.StaticModel = _Model  # type: ignore[attr-defined]
    return fake, instances


def test_real_path_lazy_loads_and_batches(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake, instances = _fake_model2vec(dim=4)
    monkeypatch.setattr(module, "model2vec_available", lambda: True)
    monkeypatch.setitem(sys.modules, "model2vec", fake)

    emb = LiteEmbedder(model_id="owner/model", dim=4, batch_size=2, model_path=tmp_path)
    assert emb._model is None  # модель не загружается в конструкторе (лениво)
    assert emb.available is True

    out = emb.embed(["a", "bb", "ccc"])

    assert emb._model is instances[0]
    assert instances[0].path == str(tmp_path)  # локальный путь → оффлайн
    assert [call["batch"] for call in instances[0].encode_calls] == [["a", "bb"], ["ccc"]]
    assert instances[0].encode_calls[0]["use_multiprocessing"] is False
    assert instances[0].encode_calls[0]["max_length"] is None
    assert len(out) == 3 and all(len(vec) == 4 for vec in out)


@requires_numpy
def test_real_path_handles_one_d_single_vector(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake, _ = _fake_model2vec(dim=3, one_d_for_single=True)
    monkeypatch.setattr(module, "model2vec_available", lambda: True)
    monkeypatch.setitem(sys.modules, "model2vec", fake)
    emb = LiteEmbedder(dim=3, batch_size=8, model_path=tmp_path)
    out = emb.embed(["abc"])
    assert out == [[3.0, 3.0, 3.0]]
