"""Тесты ``LocalReranker`` (O23 плана 26) — реализация ``RerankerPort``.

Тяжёлые зависимости (``torch``/``transformers``, веса, GPU) не требуются: балл
внедряется через ``rerank_fn`` (шов), а реальный путь проверяется подстановкой поддельных
модулей. Покрываются свойства, поломка которых даёт **тихо неверный** результат:

* ``model_id`` склеен с ревизией, а отсутствие ревизии — отказ: ``repo@None`` не адресует
  байты весов (D3), и провенанс балла перестал бы быть воспроизводимым;
* число баллов обязано совпадать с числом документов: рассинхронизация молча переставила
  бы чужие доказательства;
* нечисловой балл (``nan``/``inf``) отвергается, а не сортируется произвольно;
* объявленное устройство **исполняется**: ``cuda`` без карты — исключение с числами, а не
  тихий откат на CPU; ``fp16`` доезжает до провода как ``torch.autocast``;
* усечение пары объявлено (``truncation=True``, ``max_length``) — оно меняет балл;
* недоступность зависимости — ``RerankerUnavailable``, а не константные баллы.
"""

from __future__ import annotations

import sys
import types
from typing import Any

import pytest

import core.adapters.rerank_local as module
from core.adapters.rerank_local import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_DEVICE,
    DEFAULT_MAX_LENGTH,
    DEFAULT_PRECISION,
    LocalReranker,
    RerankerUnavailable,
)
from core.ports.reranker import RerankerPort


def _fixed_fn(scores: list[float] | None = None, calls: list[tuple[str, list[str]]] | None = None):
    """Шов: балл по длине документа (детерминирован) + журнал вызовов."""

    def fn(query: str, batch: list[str]) -> list[float]:
        if calls is not None:
            calls.append((query, list(batch)))
        if scores is not None:
            return list(scores[: len(batch)])
        return [float(len(text) % 7) for text in batch]

    return fn


# ---------------------------------------------------------------------------
# Конструктор и атрибуты
# ---------------------------------------------------------------------------


def test_defaults_and_pinned_model_id() -> None:
    rr = LocalReranker(_fixed_fn(), model_id="org/model", revision="sha1")
    assert rr.model_id == "org/model@sha1"
    assert rr.revision == "sha1"
    assert rr.device == DEFAULT_DEVICE == "cpu"
    assert rr.precision == DEFAULT_PRECISION == "fp32"
    assert rr.batch_size == DEFAULT_BATCH_SIZE
    assert rr.max_length == DEFAULT_MAX_LENGTH == 512


def test_revision_is_mandatory() -> None:
    """``repo@None`` не адресует байты: ревизия обязана быть объявлена (D3)."""
    with pytest.raises(ValueError, match="revision"):
        LocalReranker(_fixed_fn(), model_id="org/model", revision=None)
    with pytest.raises(ValueError, match="revision"):
        LocalReranker(_fixed_fn(), model_id="org/model", revision="")


@pytest.mark.parametrize(
    ("kwargs", "fragment"),
    [
        ({"model_id": "", "revision": "sha"}, "model_id"),
        ({"model_id": "org/m", "revision": "sha", "device": "auto"}, "device"),
        ({"model_id": "org/m", "revision": "sha", "precision": "bf16"}, "precision"),
        ({"model_id": "org/m", "revision": "sha", "batch_size": 0}, "batch_size"),
        ({"model_id": "org/m", "revision": "sha", "max_length": 0}, "max_length"),
    ],
)
def test_invalid_constructor_args_rejected(kwargs: dict[str, Any], fragment: str) -> None:
    with pytest.raises(ValueError, match=fragment):
        LocalReranker(_fixed_fn(), **kwargs)


def test_implements_port_structurally() -> None:
    assert isinstance(LocalReranker(_fixed_fn(), model_id="org/m", revision="sha"), RerankerPort)


# ---------------------------------------------------------------------------
# Батчи, порядок и контракт значений
# ---------------------------------------------------------------------------


def test_batches_input_and_preserves_order() -> None:
    calls: list[tuple[str, list[str]]] = []
    rr = LocalReranker(
        _fixed_fn(calls=calls), model_id="org/m", revision="sha", batch_size=2
    )
    scores = rr.rerank("q", ["a", "bb", "ccc", "dddd", "eeeee"])
    assert calls == [("q", ["a", "bb"]), ("q", ["ccc", "dddd"]), ("q", ["eeeee"])]
    assert scores == [float(len(text) % 7) for text in ["a", "bb", "ccc", "dddd", "eeeee"]]


def test_empty_documents_returns_empty_without_calling() -> None:
    calls: list[tuple[str, list[str]]] = []
    rr = LocalReranker(_fixed_fn(calls=calls), model_id="org/m", revision="sha")
    assert rr.rerank("q", []) == []
    assert calls == []


def test_score_count_mismatch_is_refused() -> None:
    rr = LocalReranker(lambda q, batch: [1.0], model_id="org/m", revision="sha")
    with pytest.raises(ValueError, match="баллов"):
        rr.rerank("q", ["a", "b"])


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_score_is_refused(value: float) -> None:
    rr = LocalReranker(lambda q, batch: [value], model_id="org/m", revision="sha")
    with pytest.raises(ValueError, match="не конечен"):
        rr.rerank("q", ["a"])


# ---------------------------------------------------------------------------
# Доступность и контракт отсутствия зависимости
# ---------------------------------------------------------------------------


def test_available_true_with_injected_fn() -> None:
    assert LocalReranker(_fixed_fn(), model_id="org/m", revision="sha").available is True


def test_available_reflects_dependency_without_fn() -> None:
    assert LocalReranker(model_id="org/m", revision="sha").available is module._transformers_available()


def test_missing_dependency_raises_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "_transformers_available", lambda: False)
    rr = LocalReranker(model_id="org/m", revision="sha")
    assert rr.available is False
    with pytest.raises(RerankerUnavailable, match="transformers"):
        rr.rerank("q", ["a"])
    # Подкласс RuntimeError: вызывающий может ловить обобщённо.
    assert issubclass(RerankerUnavailable, RuntimeError)


# ---------------------------------------------------------------------------
# Реальный путь — поддельные torch/transformers
# ---------------------------------------------------------------------------


class _Tensor:
    """Поддельный тензор: журнал ``.to(device)``."""

    def __init__(self, payload: Any) -> None:
        self.payload = payload
        self.devices: list[str] = []

    def to(self, device: str) -> _Tensor:
        self.devices.append(device)
        return self


class _Logits:
    def __init__(self, values: list[float]) -> None:
        self.values = values

    def view(self, *_shape: int) -> list[float]:
        return self.values


class _Output:
    def __init__(self, values: list[float]) -> None:
        self.logits = _Logits(values)


class _Tokenizer:
    """Поддельный токенизатор: журнал аргументов вызова и загрузки."""

    def __init__(self, repo: str, init_kwargs: dict[str, Any]) -> None:
        self.repo = repo
        self.init_kwargs = init_kwargs
        self.calls: list[dict[str, Any]] = []

    @classmethod
    def from_pretrained(cls, repo: str, **kwargs: Any) -> _Tokenizer:
        return cls(repo, kwargs)

    def __call__(self, **kwargs: Any) -> dict[str, _Tensor]:
        self.calls.append(kwargs)
        texts = kwargs["text_pair"]
        # ``text_pair`` возвращается наравне с ``input_ids``: поддельная модель читает
        # из него длину документа, как настоящая — из тензоров пары.
        return {
            "input_ids": _Tensor([[1, 2] for _ in texts]),
            "text_pair": _Tensor(list(texts)),
        }


#: Журнал созданных моделей: проверяет ленивую загрузку и аргументы загрузчика.
_MODEL_INSTANCES: list[Any] = []


class _Model:
    """Поддельная модель: балл = длина документа (детерминирован)."""

    def __init__(self, repo: str, init_kwargs: dict[str, Any]) -> None:
        self.repo = repo
        self.init_kwargs = init_kwargs
        self.device: str | None = None
        self.eval_called = False
        self.calls: list[dict[str, Any]] = []
        _MODEL_INSTANCES.append(self)

    @classmethod
    def from_pretrained(cls, repo: str, **kwargs: Any) -> _Model:
        return cls(repo, kwargs)

    def to(self, device: str) -> _Model:
        self.device = device
        return self

    def eval(self) -> _Model:
        self.eval_called = True
        return self

    def __call__(self, **inputs: Any) -> _Output:
        self.calls.append(inputs)
        return _Output([float(len(text)) for text in inputs["text_pair"].payload])


def _fake_transformers() -> types.ModuleType:
    fake = types.ModuleType("transformers")
    fake.AutoTokenizer = _Tokenizer  # type: ignore[attr-defined]
    fake.AutoModelForSequenceClassification = _Model  # type: ignore[attr-defined]
    return fake


def _fake_torch(
    *, cuda_available: bool = True, device_count: int = 1
) -> tuple[types.ModuleType, list[dict[str, Any]]]:
    """Поддельный torch: журнал ``autocast``; cuda управляется аргументами."""
    autocast_calls: list[dict[str, Any]] = []

    class _Ctx:
        def __enter__(self) -> None:
            return None

        def __exit__(self, *exc: object) -> bool:
            return False

    fake = types.ModuleType("torch")
    fake.__version__ = "2.test"  # type: ignore[attr-defined]
    fake.float16 = "float16-sentinel"  # type: ignore[attr-defined]
    fake.no_grad = lambda: _Ctx()  # type: ignore[attr-defined]
    fake.autocast = lambda **kwargs: (autocast_calls.append(kwargs), _Ctx())[1]  # type: ignore[attr-defined]
    fake.cuda = types.SimpleNamespace(  # type: ignore[attr-defined]
        is_available=lambda: cuda_available, device_count=lambda: device_count
    )
    return fake, autocast_calls


def _install(monkeypatch: pytest.MonkeyPatch, *, torch_fake: types.ModuleType) -> None:
    monkeypatch.setattr(module, "_transformers_available", lambda: True)
    monkeypatch.setitem(sys.modules, "transformers", _fake_transformers())
    monkeypatch.setitem(sys.modules, "torch", torch_fake)


def test_real_path_lazy_loads_with_revision_and_offline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _MODEL_INSTANCES.clear()
    torch_fake, _ = _fake_torch()
    _install(monkeypatch, torch_fake=torch_fake)

    rr = LocalReranker(
        model_id="org/m", revision="abc123", batch_size=2, max_length=128
    )
    assert rr._model is None  # модель не грузится в конструкторе (лениво)

    scores = rr.rerank("запрос", ["a", "bb", "ccc"])

    model = _MODEL_INSTANCES[0]
    assert model.repo == "org/m"
    # Ревизия и оффлайн-режим передаются явно: без ревизии загрузились бы «текущие» веса.
    assert model.init_kwargs == {"revision": "abc123", "local_files_only": True}
    assert model.device == "cpu" and model.eval_called is True
    assert scores == [1.0, 2.0, 3.0]
    # Усечение объявлено: truncation=True и max_length — условие замера, а не деталь.
    tokenizer = rr._tokenizer
    assert tokenizer.calls[0]["truncation"] is True
    assert tokenizer.calls[0]["max_length"] == 128
    assert tokenizer.calls[0]["padding"] is True
    assert tokenizer.calls[0]["return_tensors"] == "pt"
    assert tokenizer.calls[0]["text"] == ["запрос", "запрос"]
    assert tokenizer.calls[0]["text_pair"] == ["a", "bb"]


def test_fp16_reaches_the_wire_as_autocast(monkeypatch: pytest.MonkeyPatch) -> None:
    _MODEL_INSTANCES.clear()
    torch_fake, autocast_calls = _fake_torch()
    _install(monkeypatch, torch_fake=torch_fake)

    rr = LocalReranker(
        model_id="org/m", revision="sha", device="cuda", precision="fp16"
    )
    rr.rerank("q", ["a"])

    assert autocast_calls == [{"device_type": "cuda", "dtype": "float16-sentinel"}]
    assert _MODEL_INSTANCES[0].device == "cuda"


def test_fp32_does_not_enter_autocast(monkeypatch: pytest.MonkeyPatch) -> None:
    _MODEL_INSTANCES.clear()
    torch_fake, autocast_calls = _fake_torch()
    _install(monkeypatch, torch_fake=torch_fake)

    LocalReranker(model_id="org/m", revision="sha", precision="fp32").rerank("q", ["a"])
    assert autocast_calls == []


def test_cuda_without_device_is_refused_with_numbers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``cuda`` без карты — отказ с числами, а не тихий откат на CPU."""
    torch_fake, _ = _fake_torch(cuda_available=False, device_count=0)
    _install(monkeypatch, torch_fake=torch_fake)

    rr = LocalReranker(model_id="org/m", revision="sha", device="cuda")
    with pytest.raises(RerankerUnavailable) as excinfo:
        rr.rerank("q", ["a"])
    message = str(excinfo.value)
    assert "device_count=0" in message
    assert "is_available()=False" in message


def test_missing_snapshot_is_unavailable_not_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Отсутствие весов в кэше — ``RerankerUnavailable`` с моделью, а не поход в сеть."""

    class _BoomTokenizer:
        @classmethod
        def from_pretrained(cls, repo: str, **kwargs: Any) -> Any:
            raise OSError("no snapshot in cache")

    torch_fake, _ = _fake_torch()
    _install(monkeypatch, torch_fake=torch_fake)
    fake = _fake_transformers()
    fake.AutoTokenizer = _BoomTokenizer  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "transformers", fake)

    rr = LocalReranker(model_id="org/m", revision="sha")
    with pytest.raises(RerankerUnavailable) as excinfo:
        rr.rerank("q", ["a"])
    assert "org/m@sha" in str(excinfo.value)
    assert "local_files_only=True" in str(excinfo.value)
