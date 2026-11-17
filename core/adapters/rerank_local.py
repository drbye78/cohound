"""Локальный реранкер — реализация ``RerankerPort`` (O23 плана 26).

**Что делает адаптер.** Кросс-энкодер читает пару ``(запрос, документ)`` целиком и отдаёт
один балл на пару; порядок документов после сортировки по баллу — и есть результат
реранка. Механизм — ``transformers.AutoModelForSequenceClassification``: у
``bge-reranker-v2-m3`` это ``XLMRobertaForSequenceClassification``, и карточка модели даёт
именно такой путь (скор = ``logits.view(-1)``), поэтому ``FlagEmbedding``/``sentence-transformers``
для инференса не нужны — лишний экстракт ради одного шага (05 §2.3).

**Сигмоида не применяется — и это не упущение.** Балл берётся сырым логитом. Сигмоида
монотонна, поэтому **порядок** документов от неё не зависит; а абсолютное значение
порогом здесь не служит (порогов проект не объявляет, A3). Применять её значило бы
добавить преобразование, которого никто не мерил, и спрятать шкалу модели за «похожей на
вероятность» величиной.

**Ревизия обязательна.** ``model_id`` склеивается как ``repo@<revision>``, а отсутствие
ревизии — отказ в конструкторе: ``repo@None`` не адресует байты весов, и ``rerank_spec``
описывал бы «какой-то» ``main`` вместо того, что реально считало балл (D3, как у
``embed_spec``). ``local_files_only=True`` по умолчанию: веса берутся из локального кэша
HF, а отсутствие снапшота видно как ``RerankerUnavailable`` с именем модели, а не как
тихий поход в интернет из ядра.

**Объявленное устройство исполняется.** ``device="cuda"`` без видимой карты — исключение
с числами (``torch.cuda.is_available()``, ``device_count``), а **не** тихий откат на CPU:
устройство — условие замера, и подмена сделала бы провенанс ложью. Точность ``fp16``
исполняется ``torch.autocast(device_type=<device>, dtype=torch.float16)`` вокруг вызова
модели — тем же механизмом, что у ``LocalEmbedder``; ``fp32`` идёт прежним путём без
autocast. Третьего значения нет (``auto`` не существует).

**Усечение объявлено.** Токенизация идёт с ``truncation=True, max_length=...`` (дефолт
512): пара длиннее предела режется, и балл считается по префиксу. Это условие замера, а
не деталь реализации, поэтому ``max_length`` — параметр, а не константа внутри вызова.

**Батч.** ``rerank`` нарезает вход на батчи ``batch_size`` (дефолт 16) и конкатенирует
баллы; порядок выхода совпадает с порядком входа.

**Изолированный шов для тестов.** Конструктор принимает ``rerank_fn`` — callable
``(query, list[str]) -> list[float]``. Если он задан, реальный путь (torch/transformers)
не задействуется, и юнит-тесты не требуют весов и GPU.

**Контракт отсутствия зависимости.** Если ``transformers`` не установлен и ``rerank_fn``
не внедрён, ``rerank`` поднимает ``RerankerUnavailable`` (подкласс ``RuntimeError``).
Нулевые/пустые баллы вместо отказа не подставляются: реранк с константным баллом
переупорядочил бы доказательства произвольно и выглядел бы как измерение.

Нейтрален (§1.3): идентификатор модели приходит параметром, категорий продуктов в модуле нет.
"""

from __future__ import annotations

import importlib.util
import math
from collections.abc import Callable, Sequence
from typing import Any

__all__ = [
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_DEVICE",
    "DEFAULT_MAX_LENGTH",
    "DEFAULT_PRECISION",
    "LocalReranker",
    "RerankFn",
    "RerankerUnavailable",
]

#: Батч по умолчанию: компромисс памяти/скорости кросс-энкодера (пара длиннее одного текста).
DEFAULT_BATCH_SIZE = 16
#: Объявленный дефолт оси устройства: ``cpu`` — не догадка библиотеки (``auto`` нет).
DEFAULT_DEVICE = "cpu"
#: Объявленный дефолт точности: ``fp32`` — путь без autocast.
DEFAULT_PRECISION = "fp32"
#: Предел длины пары в токенах: усечение объявлено, потому что оно меняет балл.
DEFAULT_MAX_LENGTH = 512

#: Объявленные значения точности (зеркало ``EmbedPrecision``): адаптер не импортирует
#: домен ради двух строк — значения приходят строками, как и у устройства.
_PRECISION_FP32 = "fp32"
_PRECISION_FP16 = "fp16"

#: Объявленные устройства: ``auto`` отсутствует намеренно (R5).
_DEVICES = frozenset({"cpu", "cuda"})

#: Внедряемая функция реранка: запрос + батч документов → балл на документ.
RerankFn = Callable[[str, list[str]], list[float]]


class RerankerUnavailable(RuntimeError):
    """Реранкер недоступен: нет ``transformers``/``torch``, нет весов в кэше, нет карты."""


def _transformers_available() -> bool:
    """Установлен ли ``transformers`` (без импорта тяжёлого пакета)."""
    return importlib.util.find_spec("transformers") is not None


class LocalReranker:
    """Кросс-энкодер поверх ``AutoModelForSequenceClassification``. Реализация порта.

    :param rerank_fn: шов для тестов/подмены; ``None`` → реальная модель (лениво).
    :param model_id: HF-repo модели; вместе с ``revision`` образует ``repo@<sha>``.
    :param revision: пин ревизии весов (git-sha/тег). **Обязателен**: без него
        ``model_id`` не адресует байты (``repo@None`` — не адрес), и провенанс балла
        перестал бы быть воспроизводимым (D3).
    :param device: объявленное устройство счёта (``cpu``/``cuda``). ``cuda`` без карты —
        исключение с числами, не фолбэк.
    :param precision: объявленная точность (``fp32``/``fp16``); ``fp16`` исполняется
        ``torch.autocast``, ``fp32`` — без него.
    :param batch_size: размер батча при нарезке входа (порядок выхода сохраняется).
    :param max_length: предел длины пары в токенах (усечение объявлено).
    :param local_files_only: не ходить в сеть за весами (дефолт ``True``): отсутствие
        снапшота — видимая ошибка, а не поход в интернет из ядра.
    """

    def __init__(
        self,
        rerank_fn: RerankFn | None = None,
        *,
        model_id: str,
        revision: str | None,
        device: str = DEFAULT_DEVICE,
        precision: str = DEFAULT_PRECISION,
        batch_size: int = DEFAULT_BATCH_SIZE,
        max_length: int = DEFAULT_MAX_LENGTH,
        local_files_only: bool = True,
    ) -> None:
        if not model_id:
            raise ValueError("model_id обязан быть непустым")
        if not revision:
            raise ValueError(
                f"model_id={model_id!r}: revision обязательна — `repo@None` не адресует "
                "байты весов, и rerank_spec описывал бы не ту сборку (D3)"
            )
        if device not in _DEVICES:
            raise ValueError(
                f"device={device!r} вне объявленных значений ({', '.join(sorted(_DEVICES))}): "
                "устройство — ось замера, а не выбор библиотеки"
            )
        if precision not in {_PRECISION_FP32, _PRECISION_FP16}:
            raise ValueError(
                f"precision={precision!r} вне объявленных значений "
                f"({_PRECISION_FP32}/{_PRECISION_FP16}): точность — ось, а не режим библиотеки"
            )
        if batch_size <= 0:
            raise ValueError(f"batch_size обязан быть положительным, получено {batch_size!r}")
        if max_length <= 0:
            raise ValueError(f"max_length обязан быть положительным, получено {max_length!r}")
        self._repo_id = model_id
        #: Идентификатор с пином ревизии: ``repo@<sha>`` (провенанс, §13).
        self.model_id = f"{model_id}@{revision}"
        self.revision = revision
        self.device = device
        self.precision = precision
        self.batch_size = batch_size
        self.max_length = max_length
        self.local_files_only = local_files_only
        self._rerank_fn = rerank_fn
        self._model: Any = None
        self._tokenizer: Any = None

    @property
    def available(self) -> bool:
        """Готов ли адаптер: внедрён ``rerank_fn`` либо установлен ``transformers``."""
        if self._rerank_fn is not None:
            return True
        return _transformers_available()

    def rerank(self, query: str, documents: Sequence[str]) -> list[float]:
        """Балл на каждый документ; порядок выхода совпадает с порядком входа.

        :raises RerankerUnavailable: нет зависимости/весов/карты и не внедрён ``rerank_fn``.
        :raises ValueError: число баллов ≠ числу документов или балл не конечен.
        """
        items = [str(document) for document in documents]
        if not items:
            return []
        score = self._resolve_score()
        scores: list[float] = []
        for start in range(0, len(items), self.batch_size):
            batch = items[start : start + self.batch_size]
            scores.extend(self._validate(score(str(query), batch), expected=len(batch)))
        return scores

    # ------------------------------------------------------------------
    # Внутреннее
    # ------------------------------------------------------------------

    def _resolve_score(self) -> RerankFn:
        """Выбирает шов ``rerank_fn`` либо реальный путь (ленивая загрузка модели)."""
        if self._rerank_fn is not None:
            return self._rerank_fn
        if not _transformers_available():
            raise RerankerUnavailable(
                "transformers не установлен: реранк недоступен. Установите extra "
                "'extract-pdf' (torch/transformers уже нужны PDF-пути) или передайте rerank_fn."
            )
        return self._load_model()

    def _load_model(self) -> RerankFn:
        """Лениво загружает модель и токенизатор из локального кэша; кэширует их.

        Объявленное устройство проверяется **до** загрузки весов: ``cuda`` без карты —
        отказ с числами, а не тихий откат на CPU (иначе ``device``/провенанс лгут).
        """
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        if self.device == "cuda":
            device_count = int(torch.cuda.device_count())
            available = bool(torch.cuda.is_available())
            if not available or device_count == 0:
                raise RerankerUnavailable(
                    "устройство cuda объявлено, но карта не видна: "
                    f"torch {torch.__version__}, torch.cuda.is_available()={available}, "
                    f"device_count={device_count}. Тихий откат на cpu запрещён: устройство — "
                    "объявленная ось замера."
                )
        if self._model is None:
            try:
                tokenizer = AutoTokenizer.from_pretrained(
                    self._repo_id,
                    revision=self.revision,
                    local_files_only=self.local_files_only,
                )
                model = AutoModelForSequenceClassification.from_pretrained(
                    self._repo_id,
                    revision=self.revision,
                    local_files_only=self.local_files_only,
                )
            except Exception as exc:  # noqa: BLE001 — отказ публикуется с моделью и причиной
                raise RerankerUnavailable(
                    f"веса {self.model_id} не загружены (local_files_only="
                    f"{self.local_files_only}): {type(exc).__name__}: {exc}"
                ) from exc
            model.to(self.device)
            model.eval()
            self._tokenizer = tokenizer
            self._model = model

        tokenizer = self._tokenizer
        model = self._model

        def _score(query: str, batch: list[str]) -> list[float]:
            # Усечение объявлено: пара длиннее ``max_length`` режется, и балл считается
            # по префиксу — это условие замера, а не деталь вызова.
            encoded = tokenizer(
                text=[query] * len(batch),
                text_pair=list(batch),
                padding=True,
                truncation=True,
                max_length=self.max_length,
                return_tensors="pt",
            )
            inputs = {key: value.to(self.device) for key, value in dict(encoded).items()}
            with torch.no_grad():
                if self.precision == _PRECISION_FP16:
                    with torch.autocast(device_type=self.device, dtype=torch.float16):
                        logits = model(**inputs).logits
                else:
                    logits = model(**inputs).logits
            # Сигмоида не применяется: она монотонна и на порядок не влияет, а порога
            # проект не объявляет — сырой логит честнее «похожей на вероятность» шкалы.
            return [float(value) for value in logits.view(-1)]

        return _score

    def _validate(self, scores: Any, *, expected: int) -> list[float]:
        """Проверяет длину и конечность баллов; приводит к ``list[float]``."""
        values = [float(value) for value in scores]
        if len(values) != expected:
            raise ValueError(
                f"реранкер вернул {len(values)} баллов на {expected} документов "
                f"(model_id={self.model_id!r}): балл обязан соответствовать документу"
            )
        for index, value in enumerate(values):
            if not math.isfinite(value):
                raise ValueError(
                    f"балл #{index} не конечен (nan/inf) (model_id={self.model_id!r}): "
                    "такой балл нельзя сортировать"
                )
        return values
