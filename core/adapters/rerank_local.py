"""Локальный реранкер — реализация ``RerankerPort`` (O23 плана 26).

**Что делает адаптер.** Реранкер читает пару ``(запрос, документ)`` и отдаёт один балл на
пару; порядок документов после сортировки по баллу — и есть результат реранка. Механизмов
два (см. «Режим скоринга» ниже): кросс-энкодер —
``transformers.AutoModelForSequenceClassification`` (у ``bge-reranker-v2-m3`` это
``XLMRobertaForSequenceClassification``, и карточка модели даёт именно такой путь: скор =
``logits.view(-1)``) и декодер — ``AutoModelForCausalLM`` со скором из пары логитов. Поэтому
``FlagEmbedding``/``sentence-transformers`` для инференса не нужны — лишний экстракт ради
одного шага (05 §2.3).

**Сигмоида не применяется — и это не упущение.** Балл берётся сырым логитом (в
``decoder-yes-no`` — **разностью** двух логитов). Сигмоида и ``softmax`` монотонны, поэтому
**порядок** документов от них не зависит; а абсолютное значение порогом здесь не служит
(порогов проект не объявляет, A3). Применять их значило бы добавить преобразование, которого
никто не мерил, и спрятать шкалу модели за «похожей на вероятность» величиной. Разность
логитов монотонна по отношению «да вероятнее нет» — этого достаточно для порядка.

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

**Усечение объявлено, и у двух режимов оно режет разное.** У кросс-энкодера токенизация
идёт с ``truncation=True, max_length=...`` (дефолт 512): пара длиннее предела режется, и
балл считается по префиксу. У декодера режется **только документ** — по бюджету
``max_length − |префикс| − |суффикс|``: суффикс шаблона провоцирует ответ, и срезать его
значило бы считать балл по оборванному промпту. В обоих случаях ``max_length`` — параметр,
а не константа внутри вызова: это условие замера.

**Батч.** ``rerank`` нарезает вход на батчи ``batch_size`` и конкатенирует баллы; порядок
выхода совпадает с порядком входа. Дефолт батча **разный для двух режимов скоринга**
(см. ниже): 16 у кросс-энкодера, 4 у декодера. Это не удобство, а цена памяти: см. числа в
описании ``DEFAULT_BATCH_SIZE_DECODER``.

**Режим скоринга объявлен, а не выведен из конфига модели.** У моделей две разные головы, и
два объявленных значения ``scoring``:

* ``"cross-encoder"`` — ``AutoModelForSequenceClassification``, балл = ``logits.view(-1)``
  (``bge-reranker-v2-m3``; текущий путь, не изменился);
* ``"decoder-yes-no"`` — ``AutoModelForCausalLM``, балл = ``logit(«да») − logit(«нет»)`` на
  **последней** позиции промпта. Так считают decoder-only реранкеры: Qwen3-Reranker
  (``Qwen3ForCausalLM``) и mxbai-rerank-v2 (``Qwen2ForCausalLM``).

Автоопределение по ``config.json`` **запрещено намеренно**: это тот же класс, что тихий
фолбэк — режим, выбранный библиотекой, попал бы в провенанс балла, не будучи объявленным
(``device``/``precision`` объявляет вызывающий, по той же причине). Неизвестное значение —
``ValueError``, а не «попробуем угадать».

**Промпт — условие замера, поэтому он объявлен вызывающим.** Декодер отвечает не на пару
``(запрос, документ)``, а на **текст промпта**: другой промпт — другой балл. Адаптер не
сочиняет промпт и не читает карточку в рантайме (карточка — не вход вычисления); он требует
``prompt_template`` с ровно одним ``{query}`` и ровно одним ``{document}`` и режет документ
так, чтобы **суффикс шаблона выжил** (усечение срезало бы ``<|im_start|>assistant`` — и балл
считался бы по оборванному промпту молча). Шаблоны, снятые с карточек двух кандидатов
(дословно, 17.11.2026, из ``README.md``/``chat_template.jinja`` снапшота кэша HF):

Qwen3-Reranker-0.6B (``README.md``, раздел «Using Transformers»; ``<Instruct>`` — значение
``prompts["query"]`` из карточки)::

    <|im_start|>system
    Judge whether the Document meets the requirements based on the Query and the Instruct provided. Note that the answer can only be "yes" or "no".<|im_end|>
    <|im_start|>user
    <Instruct>: Given a web search query, retrieve relevant passages that answer the query
    <Query>: {query}
    <Document>: {document}<|im_end|>
    <|im_start|>assistant
    <think>

    </think>

(между ``<Document>:`` и ``<|im_end|>`` перевода строки нет; после ``</think>`` — два
перевода строки, как в карточке: ``suffix = "<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"``.)

mxbai-rerank-base-v2 (``chat_template.jinja`` снапшота — в ``README.md`` сырого промпта нет,
карточка отдаёт его только через ``CrossEncoder``)::

    <|im_start|>system
    You are Qwen, created by Alibaba Cloud. You are a helpful assistant.<|im_end|>
    <|im_start|>user
    query: {query}
    document: {document}
    You are a search relevance expert who evaluates how well documents match search queries. For each query-document pair, carefully analyze the semantic relationship between them, then provide your binary relevance judgment (0 for not relevant, 1 for relevant).
    Relevance:<|im_end|>
    <|im_start|>assistant

**Токены решения — тоже объявленное условие, а не «yes»/«no» по умолчанию.** Слово
«да»/«нет» у моделей разное: у Qwen3-Reranker карточка и ``1_LogitScore/config.json`` дают
``yes``/``no`` (9693/2152), у mxbai-rerank-v2 — ``1``/``0`` (16/15), потому что её промпт
просит бинарную оценку «0/1». Поэтому пара приходит параметром ``decision_tokens``
(дефолт ``("yes", "no")``), а адаптер **проверяет, что каждый токен пары — ровно один
токен** токенизатора: пара, распавшаяся на несколько токенов, означает, что модель или
токенизатор не те, и балл «разность логитов» описывал бы не то решение
(``RerankerUnavailable`` с числами). Хардкода идентификаторов нет: они выводятся из
строки через ``tokenizer.encode(..., add_special_tokens=False)``.

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
    "DEFAULT_BATCH_SIZE_DECODER",
    "DEFAULT_DECISION_TOKENS",
    "DEFAULT_DEVICE",
    "DEFAULT_MAX_LENGTH",
    "DEFAULT_PRECISION",
    "SCORING_CROSS_ENCODER",
    "SCORING_DECODER_YES_NO",
    "LocalReranker",
    "RerankFn",
    "RerankerUnavailable",
]

#: Батч по умолчанию у кросс-энкодера: компромисс памяти/скорости (пара длиннее одного текста).
DEFAULT_BATCH_SIZE = 16
#: Батч по умолчанию у декодера — **меньше**, и это число, а не вкус. Forward декодера
#: материализует логиты по всему словарю: ``batch × seq × vocab``. У Qwen3-Reranker-0.6B
#: ``vocab = 151 669``; при ``max_length = 512`` батч 4 даёт 310 618 112 значений (≈621 МиБ
#: в fp16, ≈1.24 ГиБ в fp32), батч 16 — 2.48/4.97 ГиБ. На карте 4 GiB (RTX 3050 Laptop,
#: замеренная цена этого хоста) батч 16 не влезает и уходит в пейджинг; батч 4 влезает.
#: Значение — параметр: цена батча зависит от карты и длины, поэтому не прибито гвоздями.
DEFAULT_BATCH_SIZE_DECODER = 4
#: Объявленный дефолт оси устройства: ``cpu`` — не догадка библиотеки (``auto`` нет).
DEFAULT_DEVICE = "cpu"
#: Объявленный дефолт точности: ``fp32`` — путь без autocast.
DEFAULT_PRECISION = "fp32"
#: Предел длины пары (у кросс-энкодера) или промпта (у декодера) в токенах: усечение
#: объявлено, потому что оно меняет балл.
DEFAULT_MAX_LENGTH = 512
#: Объявленные значения ``scoring`` — два пути к баллу, и третьего нет.
SCORING_CROSS_ENCODER = "cross-encoder"
SCORING_DECODER_YES_NO = "decoder-yes-no"
_SCORINGS = frozenset({SCORING_CROSS_ENCODER, SCORING_DECODER_YES_NO})
#: Пара «да»/«нет» по умолчанию. Это **дефолт, а не константа модели**: у mxbai-rerank-v2
#: карточка просит «0/1», и вызывающий объявляет ``("1", "0")``.
DEFAULT_DECISION_TOKENS = ("yes", "no")

#: Плейсхолдеры шаблона промпта декодера: ровно один каждого, иначе шаблон неоднозначен.
_QUERY_SLOT = "{query}"
_DOCUMENT_SLOT = "{document}"

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


def _validate_prompt_template(template: str | None, *, model_id: str) -> None:
    """Шаблон промпта декодера: непустой, ровно по одному ``{query}`` и ``{document}``.

    Два ``{document}`` или ноль — неоднозначный шаблон: неизвестно, куда подставлять
    документ, и балл описывал бы промпт, которого никто не объявлял.
    """
    if not template:
        raise ValueError(
            f"scoring='decoder-yes-no' (model_id={model_id!r}): prompt_template обязателен — "
            "декодер отвечает на текст промпта, и промпт здесь условие замера, а не деталь "
            "вызова (другой промпт — другой балл)"
        )
    if template.count(_QUERY_SLOT) != 1:
        raise ValueError(
            f"prompt_template: {_QUERY_SLOT} встречается {template.count(_QUERY_SLOT)} раз(а), "
            "нужен ровно один — иначе неясно, куда подставлять запрос"
        )
    if template.count(_DOCUMENT_SLOT) != 1:
        raise ValueError(
            f"prompt_template: {_DOCUMENT_SLOT} встречается {template.count(_DOCUMENT_SLOT)} "
            "раз(а), нужен ровно один — иначе неясно, где кончается документ и начинается "
            "суффикс шаблона (а суффикс обязан выжить при усечении)"
        )


class LocalReranker:
    """Реранкер поверх ``transformers``: кросс-энкодер **или** декодер. Реализация порта.

    :param rerank_fn: шов для тестов/подмены; ``None`` → реальная модель (лениво).
    :param model_id: HF-repo модели; вместе с ``revision`` образует ``repo@<sha>``.
    :param revision: пин ревизии весов (git-sha/тег). **Обязателен**: без него
        ``model_id`` не адресует байты (``repo@None`` — не адрес), и провенанс балла
        перестал бы быть воспроизводимым (D3).
    :param device: объявленное устройство счёта (``cpu``/``cuda``). ``cuda`` без карты —
        исключение с числами, не фолбэк.
    :param precision: объявленная точность (``fp32``/``fp16``); ``fp16`` исполняется
        ``torch.autocast``, ``fp32`` — без него.
    :param scoring: объявленный режим скоринга — ``cross-encoder`` (по умолчанию) или
        ``decoder-yes-no``. Не выводится из конфига модели: режим — условие замера.
    :param batch_size: размер батча; ``None`` → дефолт режима (16 у кросс-энкодера,
        4 у декодера — цена логитов ``batch × seq × vocab``).
    :param max_length: предел длины пары (кросс-энкодер) или промпта (декодер) в токенах.
    :param prompt_template: шаблон промпта декодера с ``{query}`` и ``{document}``.
        **Обязателен** для ``decoder-yes-no`` и **запрещён** для ``cross-encoder``: промпт —
        условие замера, а не украшение вызова.
    :param decision_tokens: пара «да»/«нет» для ``decoder-yes-no`` (дефолт ``yes``/``no``);
        каждый обязан быть одним токеном токенизатора — иначе отказ с числами.
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
        scoring: str = SCORING_CROSS_ENCODER,
        batch_size: int | None = None,
        max_length: int = DEFAULT_MAX_LENGTH,
        prompt_template: str | None = None,
        decision_tokens: tuple[str, str] = DEFAULT_DECISION_TOKENS,
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
        if scoring not in _SCORINGS:
            raise ValueError(
                f"scoring={scoring!r} вне объявленных значений "
                f"({', '.join(sorted(_SCORINGS))}): режим скоринга объявляет вызывающий — "
                "автоопределение по конфигу модели дало бы провенанс, которого никто не "
                "объявлял (тот же класс, что тихий фолбэк)"
            )
        if scoring == SCORING_DECODER_YES_NO:
            _validate_prompt_template(prompt_template, model_id=model_id)
        elif prompt_template is not None:
            raise ValueError(
                f"prompt_template задан при scoring={scoring!r}: у кросс-энкодера промпта нет "
                "(он читает пару целиком), и молча проигнорировать шаблон значило бы "
                "объявить условие, которое не исполняется"
            )
        if len(decision_tokens) != 2 or not all(
            isinstance(item, str) and item for item in decision_tokens
        ):
            raise ValueError(
                f"decision_tokens={decision_tokens!r}: нужна пара непустых строк «да»/«нет»"
            )
        if decision_tokens[0] == decision_tokens[1]:
            raise ValueError(
                f"decision_tokens={decision_tokens!r}: «да» и «нет» совпадают — "
                "разность логитов была бы тождественным нулём"
            )
        if batch_size is None:
            batch_size = (
                DEFAULT_BATCH_SIZE_DECODER
                if scoring == SCORING_DECODER_YES_NO
                else DEFAULT_BATCH_SIZE
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
        #: Объявленный режим скоринга (условие замера, попадает в отчёт).
        self.scoring = scoring
        self.batch_size = batch_size
        self.max_length = max_length
        #: Шаблон промпта декодера как есть (``None`` у кросс-энкодера).
        self.prompt_template = prompt_template
        #: Объявленная пара «да»/«нет» (строки, не идентификаторы).
        self.decision_tokens = tuple(decision_tokens)
        #: Идентификаторы пары после загрузки токенизатора (``None`` до неё).
        self.decision_token_ids: tuple[int, int] | None = None
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
        return self._load_model() if self.scoring == SCORING_CROSS_ENCODER else self._load_decoder()

    def _require_device(self) -> None:
        """Объявленное устройство проверяется **до** загрузки весов: ``cuda`` без карты —
        отказ с числами, а не тихий откат на CPU (иначе ``device``/провенанс лгут)."""
        import torch

        if self.device != "cuda":
            return
        device_count = int(torch.cuda.device_count())
        available = bool(torch.cuda.is_available())
        if not available or device_count == 0:
            raise RerankerUnavailable(
                "устройство cuda объявлено, но карта не видна: "
                f"torch {torch.__version__}, torch.cuda.is_available()={available}, "
                f"device_count={device_count}. Тихий откат на cpu запрещён: устройство — "
                "объявленная ось замера."
            )

    def _load_model(self) -> RerankFn:
        """Кросс-энкодер: ленивая загрузка модели и токенизатора из локального кэша.

        Объявленное устройство проверяется **до** загрузки весов (см. :meth:`_require_device`).
        """
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        self._require_device()
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

    def _load_decoder(self) -> RerankFn:
        """Декодер: балл = ``logit(«да») − logit(«нет»)`` на последней позиции промпта.

        Три вещи, каждая из которых иначе даёт **тихо неверный** балл:

        * **суффикс шаблона выживает усечение** — документ режется по бюджету
          ``max_length − |префикс| − |суффикс|``, а не весь промпт справа: у Qwen3 и mxbai
          ответ провоцирует ``<|im_start|>assistant`` в конце шаблона, и срезанный суффикс
          превратил бы балл в «продолжение документа»;
        * **padding слева** — логит берётся с позиции ``-1``, а при правом padding там
          оказался бы pad-токен короткой строки батча;
        * **пара «да»/«нет» проверяется на однократность** — распавшаяся пара значит, что
          модель/токенизатор не те, и «разность логитов» описывала бы не то решение.
        """
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer

        self._require_device()
        if self._model is None:
            try:
                tokenizer = AutoTokenizer.from_pretrained(
                    self._repo_id,
                    revision=self.revision,
                    local_files_only=self.local_files_only,
                    padding_side="left",
                )
                # ``Any`` — потому что у transformers 5.x ``from_pretrained`` у CausalLM
                # возвращает ``_BaseModelWithGenerate``, у которого стабы расходятся с
                # реальным объектом (``.to``/``.eval``); тип здесь ничего не доказывает.
                model: Any = AutoModelForCausalLM.from_pretrained(
                    self._repo_id,
                    revision=self.revision,
                    local_files_only=self.local_files_only,
                )
            except Exception as exc:  # noqa: BLE001 — отказ публикуется с моделью и причиной
                raise RerankerUnavailable(
                    f"веса {self.model_id} не загружены (local_files_only="
                    f"{self.local_files_only}): {type(exc).__name__}: {exc}"
                ) from exc
            true_text, false_text = self.decision_tokens
            true_ids = list(tokenizer.encode(true_text, add_special_tokens=False))
            false_ids = list(tokenizer.encode(false_text, add_special_tokens=False))
            if len(true_ids) != 1 or len(false_ids) != 1:
                raise RerankerUnavailable(
                    f"токены решения не однократные (model_id={self.model_id!r}): "
                    f"{true_text!r} → {len(true_ids)} токен(ов) {true_ids}, "
                    f"{false_text!r} → {len(false_ids)} токен(ов) {false_ids}. Разность "
                    "логитов определена только для одного токена на решение: пара, "
                    "распавшаяся на несколько, значит, что модель или токенизатор не те."
                )
            if true_ids[0] == false_ids[0]:
                raise RerankerUnavailable(
                    f"токены решения совпали (model_id={self.model_id!r}): "
                    f"{true_text!r} и {false_text!r} → id {true_ids[0]}"
                )
            model.to(self.device)
            model.eval()
            self._tokenizer = tokenizer
            self._model = model
            self.decision_token_ids = (int(true_ids[0]), int(false_ids[0]))

        tokenizer = self._tokenizer
        model = self._model
        true_id, false_id = self.decision_token_ids or (0, 0)
        template = self.prompt_template or ""
        head, tail = template.split(_DOCUMENT_SLOT)

        def _score(query: str, batch: list[str]) -> list[float]:
            prefix = head.replace(_QUERY_SLOT, query)
            prefix_ids = list(tokenizer.encode(prefix, add_special_tokens=False))
            suffix_ids = list(tokenizer.encode(tail, add_special_tokens=False))
            budget = self.max_length - len(prefix_ids) - len(suffix_ids)
            if budget <= 0:
                raise ValueError(
                    f"шаблон промпта не влезает в max_length={self.max_length}: префикс "
                    f"{len(prefix_ids)} + суффикс {len(suffix_ids)} токенов "
                    f"(model_id={self.model_id!r}) — резать документ некуда, а резать "
                    "суффикс значило бы оборвать промпт"
                )
            rows = [
                prefix_ids
                + list(tokenizer.encode(str(document), add_special_tokens=False))[:budget]
                + suffix_ids
                for document in batch
            ]
            encoded = tokenizer.pad({"input_ids": rows}, padding=True, return_tensors="pt")
            inputs = {key: value.to(self.device) for key, value in dict(encoded).items()}
            with torch.no_grad():
                if self.precision == _PRECISION_FP16:
                    with torch.autocast(device_type=self.device, dtype=torch.float16):
                        logits = model(**inputs).logits
                else:
                    logits = model(**inputs).logits
            # Ни сигмоиды, ни softmax: разность логитов монотонна по «да вероятнее нет»,
            # а порога проект не объявляет (A3) — нормировка была бы неизмеренным шагом.
            return [
                float(logits[index, -1, true_id]) - float(logits[index, -1, false_id])
                for index in range(len(batch))
            ]

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
