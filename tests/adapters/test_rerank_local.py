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
from typing import Any, ClassVar

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


# ---------------------------------------------------------------------------
# Декодер: поддельные токенизатор и модель со «словарём» логитов
# ---------------------------------------------------------------------------

#: Пары «да»/«нет», которые поддельный токенизатор отдаёт одним токеном.
_DECODER_SINGLE = {"yes": [7], "no": [8], "1": [16], "0": [15]}


class _DecoderTokenizer:
    """Поддельный токенизатор декодера: журнал ``encode``/``pad`` и однократные пары."""

    def __init__(self, repo: str, init_kwargs: dict[str, Any]) -> None:
        self.repo = repo
        self.init_kwargs = init_kwargs
        self.padding_side = init_kwargs.get("padding_side", "right")
        self.single: dict[str, list[int]] = dict(_DECODER_SINGLE)
        self.encode_calls: list[str] = []
        self.pad_calls: list[dict[str, Any]] = []

    @classmethod
    def from_pretrained(cls, repo: str, **kwargs: Any) -> _DecoderTokenizer:
        return cls(repo, kwargs)

    def encode(self, text: str, add_special_tokens: bool = False) -> list[int]:
        self.encode_calls.append(text)
        if text in self.single:
            return list(self.single[text])
        # Прочий текст — по токену на символ: длина документа в токенах = длине строки,
        # поэтому бюджет усечения проверяется арифметикой, а не догадкой.
        return [ord(char) % 50 + 10 for char in text]

    def pad(
        self, encoded: dict[str, list[list[int]]], padding: bool = True, return_tensors: str = "pt"
    ) -> dict[str, _Tensor]:
        rows = [list(row) for row in encoded["input_ids"]]
        self.pad_calls.append(
            {"rows": rows, "padding": padding, "return_tensors": return_tensors}
        )
        return {
            "input_ids": _Tensor(rows),
            "attention_mask": _Tensor([[1] * len(row) for row in rows]),
        }


class _CausalLogits:
    """Логиты декодера: ``logits[i, -1, token_id]`` — как у настоящего тензора."""

    def __init__(self, base: dict[int, float], true_ids: frozenset[int]) -> None:
        self.base = base
        self.true_ids = true_ids

    def __getitem__(self, key: tuple[int, int, int]) -> float:
        index, position, token_id = key
        if position != -1:
            raise AssertionError(f"логит читается не с последней позиции: {position}")
        # Надбавка за номер строки — только у «да»: тогда разность логитов различает
        # документы (``2 + i``) и видно, что порядок входа сохранён.
        bonus = float(index) if token_id in self.true_ids else 0.0
        return self.base.get(token_id, 0.0) + bonus


class _CausalOutput:
    def __init__(self, base: dict[int, float], true_ids: frozenset[int]) -> None:
        self.logits = _CausalLogits(base, true_ids)


class _CausalModel:
    """Поддельный декодер: ``logit(«да») = 3 + i``, ``logit(«нет») = 1`` ⇒ балл ``2 + i``."""

    #: Таблица логитов по идентификатору токена (переопределяется тестом при нужде).
    table: ClassVar[dict[int, float]] = {7: 3.0, 8: 1.0, 16: -2.0, 15: -4.0}
    #: Идентификаторы «да» — те, что получают надбавку за номер строки (7 = yes, 16 = 1).
    true_ids: ClassVar[frozenset[int]] = frozenset({7, 16})

    def __init__(self, repo: str, init_kwargs: dict[str, Any]) -> None:
        self.repo = repo
        self.init_kwargs = init_kwargs
        self.device: str | None = None
        self.eval_called = False
        self.calls: list[dict[str, Any]] = []
        _MODEL_INSTANCES.append(self)

    @classmethod
    def from_pretrained(cls, repo: str, **kwargs: Any) -> _CausalModel:
        return cls(repo, kwargs)

    def to(self, device: str) -> _CausalModel:
        self.device = device
        return self

    def eval(self) -> _CausalModel:
        self.eval_called = True
        return self

    def __call__(self, **inputs: Any) -> _CausalOutput:
        self.calls.append(inputs)
        return _CausalOutput(dict(type(self).table), type(self).true_ids)


def _fake_transformers() -> types.ModuleType:
    fake = types.ModuleType("transformers")
    fake.AutoTokenizer = _Tokenizer  # type: ignore[attr-defined]
    fake.AutoModelForSequenceClassification = _Model  # type: ignore[attr-defined]
    fake.AutoModelForCausalLM = _CausalModel  # type: ignore[attr-defined]
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


def _install(
    monkeypatch: pytest.MonkeyPatch,
    *,
    torch_fake: types.ModuleType,
    tokenizer_cls: type[Any] = _Tokenizer,
) -> None:
    monkeypatch.setattr(module, "_transformers_available", lambda: True)
    fake = _fake_transformers()
    fake.AutoTokenizer = tokenizer_cls  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "transformers", fake)
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


# ---------------------------------------------------------------------------
# Объявленный режим скоринга
# ---------------------------------------------------------------------------

#: Минимальный шаблон промпта: ровно по одному плейсхолдеру, суффикс различим, и вся
#: арифметика усечения проверяема вручную (по токену на символ у поддельного токенизатора).
_TEMPLATE = "Q:{query}\nD:{document}\nA:"


def _decoder(**kwargs: Any) -> LocalReranker:
    """Декодер с объявленным шаблоном; ``kwargs`` переопределяют поля."""
    base: dict[str, Any] = {
        "model_id": "org/dec",
        "revision": "sha",
        "scoring": module.SCORING_DECODER_YES_NO,
        "prompt_template": _TEMPLATE,
    }
    base.update(kwargs)
    return LocalReranker(**base)


def test_unknown_scoring_is_refused() -> None:
    """Режим объявляет вызывающий: автоопределение по конфигу — тот же тихий фолбэк."""
    with pytest.raises(ValueError, match="scoring"):
        LocalReranker(_fixed_fn(), model_id="org/m", revision="sha", scoring="auto")


def test_decoder_requires_prompt_template() -> None:
    with pytest.raises(ValueError, match="prompt_template"):
        LocalReranker(
            _fixed_fn(),
            model_id="org/m",
            revision="sha",
            scoring=module.SCORING_DECODER_YES_NO,
        )


@pytest.mark.parametrize(
    "template",
    [
        "без плейсхолдеров",
        "query: {query}\n{query}\ndoc: {document}",
        "query: {query}\ndoc: {document}\nещё: {document}",
    ],
)
def test_prompt_template_placeholders_are_exactly_one(template: str) -> None:
    with pytest.raises(ValueError, match="prompt_template"):
        _decoder(prompt_template=template)


def test_cross_encoder_refuses_a_prompt_template() -> None:
    """У кросс-энкодера промпта нет: объявленный и неисполняемый шаблон — отказ."""
    with pytest.raises(ValueError, match="prompt_template"):
        LocalReranker(
            _fixed_fn(), model_id="org/m", revision="sha", prompt_template=_TEMPLATE
        )


def test_decoder_batch_default_is_smaller_than_cross_encoder() -> None:
    """Батч декодера меньше: логиты — ``batch × seq × vocab`` (см. докстринг константы)."""
    assert module.DEFAULT_BATCH_SIZE_DECODER < module.DEFAULT_BATCH_SIZE
    assert _decoder().batch_size == module.DEFAULT_BATCH_SIZE_DECODER
    assert (
        LocalReranker(_fixed_fn(), model_id="org/m", revision="sha").batch_size
        == module.DEFAULT_BATCH_SIZE
    )
    assert _decoder(batch_size=2).batch_size == 2


@pytest.mark.parametrize(
    "tokens",
    [("yes",), ("yes", "no", "maybe"), ("yes", ""), ("yes", "yes")],
)
def test_decision_tokens_must_be_two_distinct_strings(tokens: tuple[str, ...]) -> None:
    with pytest.raises(ValueError, match="decision_tokens"):
        _decoder(decision_tokens=tokens)


def test_decoder_score_is_the_difference_of_two_logits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Главное свойство режима: балл = ``logit(«да») − logit(«нет»)``, порядок сохранён."""
    _MODEL_INSTANCES.clear()
    torch_fake, _ = _fake_torch()
    _install(monkeypatch, torch_fake=torch_fake, tokenizer_cls=_DecoderTokenizer)

    rr = _decoder(max_length=64)
    scores = rr.rerank("запрос", ["a", "bb", "ccc"])

    # Таблица поддельной модели: logit(yes)=3+i, logit(no)=1 ⇒ 2, 3, 4.
    assert scores == [2.0, 3.0, 4.0]
    model = _MODEL_INSTANCES[0]
    assert model.repo == "org/dec"
    assert model.init_kwargs == {"revision": "sha", "local_files_only": True}
    assert model.device == "cpu" and model.eval_called is True
    assert rr.decision_token_ids == (7, 8)


def test_decoder_prompt_is_built_from_the_declared_template(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Промпт собран из шаблона, и суффикс шаблона стоит **последним** (иначе он срезан)."""
    _MODEL_INSTANCES.clear()
    torch_fake, _ = _fake_torch()
    _install(monkeypatch, torch_fake=torch_fake, tokenizer_cls=_DecoderTokenizer)

    rr = _decoder()
    rr.rerank("мой запрос", ["док"])

    tokenizer = rr._tokenizer
    # Левое поле: логит берётся с позиции -1, а при правом padding там оказался бы pad.
    assert tokenizer.init_kwargs == {
        "revision": "sha",
        "local_files_only": True,
        "padding_side": "left",
    }
    assert tokenizer.padding_side == "left"
    prefix_text, tail_text = _TEMPLATE.split("{document}")
    assert tokenizer.encode_calls[0] == "yes"
    assert tokenizer.encode_calls[1] == "no"
    assert tokenizer.encode_calls[2] == prefix_text.replace("{query}", "мой запрос")
    assert tokenizer.encode_calls[3] == tail_text
    assert tokenizer.encode_calls[4] == "док"
    rows = tokenizer.pad_calls[0]["rows"]
    assert len(rows) == 1
    prefix_len = len(tokenizer.encode(prefix_text.replace("{query}", "мой запрос")))
    assert rows[0][:prefix_len] == tokenizer.encode(
        prefix_text.replace("{query}", "мой запрос")
    )
    assert rows[0][-len(tokenizer.encode(tail_text)) :] == tokenizer.encode(tail_text)
    assert tokenizer.pad_calls[0]["padding"] is True
    assert tokenizer.pad_calls[0]["return_tensors"] == "pt"


def test_decoder_truncates_the_document_and_keeps_the_suffix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Усечение режет **документ**, а не суффикс: иначе промпт оборван и балл лжив."""
    _MODEL_INSTANCES.clear()
    torch_fake, _ = _fake_torch()
    _install(monkeypatch, torch_fake=torch_fake, tokenizer_cls=_DecoderTokenizer)

    rr = _decoder(max_length=10)
    rr.rerank("q", ["x" * 40])

    tokenizer = rr._tokenizer
    prefix_len = len(tokenizer.encode(_TEMPLATE.split("{document}")[0].replace("{query}", "q")))
    suffix_len = len(tokenizer.encode(_TEMPLATE.split("{document}")[1]))
    row = tokenizer.pad_calls[0]["rows"][0]
    # Префикс «Q:q\nD:» = 6 токенов, суффикс «\nA:» = 3, бюджет документа = 10-6-3 = 1.
    assert (prefix_len, suffix_len) == (6, 3)
    assert len(row) == 10
    assert row[-suffix_len:] == tokenizer.encode(_TEMPLATE.split("{document}")[1])
    assert row[:prefix_len] == tokenizer.encode(
        _TEMPLATE.split("{document}")[0].replace("{query}", "q")
    )


def test_decoder_refuses_when_the_prompt_does_not_fit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Шаблон длиннее ``max_length`` — отказ с числами, а не молча обрезанный суффикс."""
    _MODEL_INSTANCES.clear()
    torch_fake, _ = _fake_torch()
    _install(monkeypatch, torch_fake=torch_fake, tokenizer_cls=_DecoderTokenizer)

    rr = _decoder(max_length=4)
    with pytest.raises(ValueError, match="max_length"):
        rr.rerank("q", ["doc"])


def test_decoder_refuses_multi_token_decision_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Пара распалась на несколько токенов — модель/токенизатор не те: отказ с числами."""
    _MODEL_INSTANCES.clear()
    torch_fake, _ = _fake_torch()
    _install(monkeypatch, torch_fake=torch_fake, tokenizer_cls=_DecoderTokenizer)

    class _SplitTokenizer(_DecoderTokenizer):
        def __init__(self, repo: str, init_kwargs: dict[str, Any]) -> None:
            super().__init__(repo, init_kwargs)
            self.single = {"yes": [7, 7], "no": [8]}

    fake = _fake_transformers()
    fake.AutoTokenizer = _SplitTokenizer  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "transformers", fake)

    rr = _decoder()
    with pytest.raises(RerankerUnavailable) as excinfo:
        rr.rerank("q", ["a"])
    message = str(excinfo.value)
    assert "'yes' → 2 токен(ов) [7, 7]" in message
    assert "'no' → 1 токен(ов) [8]" in message


def test_decoder_honours_declared_decision_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Пара объявлена вызывающим (у mxbai-rerank-v2 карточка просит «0/1», не «yes/no»)."""
    _MODEL_INSTANCES.clear()
    torch_fake, _ = _fake_torch()
    _install(monkeypatch, torch_fake=torch_fake, tokenizer_cls=_DecoderTokenizer)

    rr = _decoder(decision_tokens=("1", "0"))
    scores = rr.rerank("q", ["a"])

    assert rr.decision_token_ids == (16, 15)
    # logit(«1») = -2, logit(«0») = -4 ⇒ -2 - (-4) = 2.
    assert scores == [2.0]


def test_cross_encoder_path_is_unchanged_by_the_new_axis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Режим по умолчанию — прежний путь: ``AutoModelForSequenceClassification``."""
    _MODEL_INSTANCES.clear()
    torch_fake, _ = _fake_torch()
    _install(monkeypatch, torch_fake=torch_fake)

    rr = LocalReranker(model_id="org/m", revision="sha", max_length=128)
    assert rr.scoring == module.SCORING_CROSS_ENCODER
    assert rr.prompt_template is None
    assert rr.rerank("q", ["ab"]) == [2.0]
    assert isinstance(_MODEL_INSTANCES[0], _Model)

