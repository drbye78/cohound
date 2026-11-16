"""coHound — эталон **всех отвечающих документов** ячейки (O42 плана 26).

**Зачем второй эталон.** Документный эталон (:mod:`core.pipeline.retrieval_docs`)
отвечает на вопрос «нашёлся ли **названный адрес**»: у ячейки один ожидаемый ``doc_id``,
и ``hit@K`` считает попадание именно его. Замер O15 (16.11.2026) показал, что этот
вопрос — не тот, который читается: у 111 из 132 ячеек вердикт ✅/⚠️ есть, а объявленный
адрес попал в top-10 лишь у 34, цитата взята из объявленного документа в 14, из
**другого** документа продукта — в 100. Значит «0.258» — нижняя оценка качества поиска,
а не его качество: доказательство находится, но в другом документе. Второй вопрос —
«нашёлся ли хоть один документ, который **решает** параметр» — требует набора, а не
адреса, и до этой полосы не мерился.

**Почему метрики здесь НЕ дублируются — это решение, а не экономия.**
:func:`core.pipeline.retrieval_docs.evaluate_documents` уже считает ``hit`` как «любой
член набора в top-K», ``recall`` как ``|набор ∩ top-K| / |набор|``, ``MRR`` по первому
попаданию и ``nDCG@K`` по бинарной релевантности — ровно то, что нужно answer-семейству,
если в ``expected_doc_ids`` положить набор отвечающих. Второй экземпляр тех же формул
означал бы два источника истины для одного числа: правка в одном месте (например,
знаменатель nDCG при ``K < |набор|``) молча разошлась бы с другим, и отчёт публиковал бы
две несравнимые метрики под похожими именами. Поэтому здесь живёт **только файл**:
загрузка, инварианты, каноническая запись и сводки. Числа считает прибор
(``scripts/measure_retrieval_docs.py``), вызывая тот же ``evaluate_documents`` дважды —
на адресном наборе и на наборе отвечающих.

**Формат** ``cohound-retrieval-golden-answers/1``::

    {"format": "cohound-retrieval-golden-answers/1",
     "case_id": "...", "run_id": "...", "cutoff_k": 10,
     "judge": {"model_id": "...", "revision": "...", "prompt_hash": "..."} | null,
     "policy": {"term_min_length": 4, "term_min_distinct": 2, "retrieved_depth": 0,
                "max_candidates_per_cell": 20, "include_partial": false},
     "status": "candidate" | "reviewed",
     "entries": [{"cell": "p/param", "product_id": "p", "param_id": "param",
                  "cited_doc_ids": ["p:..."],
                  "answer_docs": [{"doc_id": "p:...", "how": ["cited", "term"],
                                   "quote": "дословная цитата"}]}]}

**``how`` — происхождение каждого члена, и оно разделимо.** ``cited`` — адрес,
объявленный документным эталоном (по определению отвечает); ``term`` — документ того же
продукта, где встречаются различимые термины параметра (источник **независим** от индекса
под тестом, иначе система составляла бы себе экзамен); ``retrieved`` — документ из top-K
выдачи (единственный источник, порождённый измеряемой системой, поэтому он вычитается в
чувствительности); ``llm`` — кандидат судьи (суждение, а не факт: требует цитаты и
провенанса судьи); ``human`` — решение человека, принятое в обход кандидатов.

**Инварианты — ``ValueError`` с адресом ячейки/поля, тихих дефолтов нет.** Файл несёт
``run_id``: ``doc_id`` — это ``product_id:content_hash``, адреса разных корпусов
несравнимы, поэтому набор не переносится на другой цикл (пере-обход меняет состав).
``cited_doc_ids ⊆ answer_docs[*].doc_id`` — адрес цитаты по определению отвечает; если
разметчик считает иначе, это ошибка разметки O14, а не допустимое состояние. ``quote``
обязателен у ``llm``/``human`` — суждение без цитаты не доказательство (та же дисциплина,
что I22). ``judge`` обязателен, если у любого документа есть ``llm``: иначе провенанс
судьи потерян, и эталон молча становится истиной.

Модуль — чистая логика: единственный I/O — чтение/запись JSON. Порты ретривала сюда не
входят. Категорийно нейтрален (§1.3), без numpy на импорте.

**Чувствительность — по-ячеечно, а не глобально.** ``retrieved`` вычитается
:func:`sensitivity_doc_ids` **внутри ячейки**: один и тот же документ может в одной ячейке
прийти из выдачи системы под тестом, а в другой — по терминам параметра, и глобальное
множество выбросило бы его из обеих, занизив число ровно там, где источник независим.
Пустой остаток после вычитания — законный ноль ячейки, а не повод её пропустить.
"""

from __future__ import annotations

import json
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

__all__ = [
    "ANSWER_HOW",
    "ANSWER_SETS_FORMAT",
    "ANSWER_STATUSES",
    "AnswerDoc",
    "AnswerEntry",
    "AnswerPolicy",
    "AnswerSets",
    "JudgeSpec",
    "answer_doc_ids",
    "answer_doc_ids_without_retrieved",
    "answers_summary",
    "cited_doc_ids",
    "load_answer_sets",
    "sensitivity_by_cell",
    "sensitivity_doc_ids",
    "write_answer_sets",
]

#: Формат файла эталона отвечающих документов (чужой формат — не эталон).
ANSWER_SETS_FORMAT = "cohound-retrieval-golden-answers/1"

#: Канонический порядок токенов ``how``: он же порядок записи в файл, поэтому два
#: прогона с одинаковым набором дают побайтово одинаковый файл.
ANSWER_HOW: tuple[str, ...] = ("cited", "term", "retrieved", "llm", "human")

#: Статусы набора: ``candidate`` — разметка не подтверждена целиком (дефолт компиляции),
#: ``reviewed`` — человек подтвердил набор как эталон (осознанное действие с ``actor``).
ANSWER_STATUSES: tuple[str, ...] = ("candidate", "reviewed")

#: Обязательные ключи ``policy``: правило ``term``, глубина ``retrieved`` и потолок
#: кандидатов на ячейку. ``include_partial`` — дополнительный ключ компиляции.
_POLICY_KEYS = (
    "term_min_length",
    "term_min_distinct",
    "retrieved_depth",
    "max_candidates_per_cell",
)


@dataclass(frozen=True)
class AnswerDoc:
    """Один документ набора отвечающих: адрес, происхождение и цитата.

    :param doc_id: ``"{product_id}:{content_hash}"`` — адрес документа этого корпуса.
    :param how: непустой набор источников из :data:`ANSWER_HOW`, в каноническом порядке.
    :param quote: дословная цитата из документа; пустая допустима только у ``cited``/
        ``term`` (там документ отвечает по определению или по терминам, а не по суждению).
    """

    doc_id: str
    how: tuple[str, ...]
    quote: str = ""


@dataclass(frozen=True)
class AnswerEntry:
    """Одна ячейка эталона отвечающих: параметр продукта и набор отвечающих документов.

    :param cell: ``"{product_id}/{param_id}"``.
    :param product_id: продукт ячейки.
    :param param_id: параметр ячейки.
    :param cited_doc_ids: документы, объявленные адресом цитаты (подмножество набора).
    :param answer_docs: непустой набор отвечающих документов.
    """

    cell: str
    product_id: str
    param_id: str
    cited_doc_ids: tuple[str, ...]
    answer_docs: tuple[AnswerDoc, ...]


@dataclass(frozen=True)
class JudgeSpec:
    """Провенанс судьи-LLM: модель, ревизия весов и отпечаток промпта.

    ``revision`` может быть пустой строкой — ревизия объявляется не всегда, и пустая
    строка означает «не объявлена», а не «совпала с моделью».
    """

    model_id: str
    revision: str
    prompt_hash: str


@dataclass(frozen=True)
class AnswerPolicy:
    """Политика отбора кандидатов — часть определения набора, а не настройка вида.

    :param term_min_length: минимальная длина слова ``title``, считающегося термином.
    :param term_min_distinct: сколько **различных** терминов должно встретиться в документе.
    :param retrieved_depth: глубина выдачи для источника ``retrieved`` (0 — источник выключен).
    :param max_candidates_per_cell: потолок кандидатов на ячейку (``cited`` не выкидывается).
    :param include_partial: включает ``partial`` в набор при компиляции (записано в файле,
        чтобы набор был воспроизводим: без флага ``partial`` в набор не входит).
    """

    term_min_length: int
    term_min_distinct: int
    retrieved_depth: int
    max_candidates_per_cell: int
    include_partial: bool = False

    def as_dict(self) -> dict[str, Any]:
        """Словарь для файла: обязательные ключи плюс ``include_partial``."""
        return {
            "term_min_length": self.term_min_length,
            "term_min_distinct": self.term_min_distinct,
            "retrieved_depth": self.retrieved_depth,
            "max_candidates_per_cell": self.max_candidates_per_cell,
            "include_partial": self.include_partial,
        }


@dataclass(frozen=True)
class AnswerSets:
    """Загруженный эталон отвечающих документов.

    :param case_id: кейс (пакет), на котором собран набор.
    :param run_id: прогон, чей корпус адресуют ``doc_id`` (набор не переносится на другой).
    :param cutoff_k: cut-off, при котором набор объявлен (I20: K эталона = K пайплайна).
    :param judge: провенанс судьи или ``None`` (судья не участвовал).
    :param policy: политика отбора кандидатов.
    :param status: ``candidate`` | ``reviewed``.
    :param entries: записи в порядке файла (каноническая запись сортирует их по ``cell``).
    :param reviewed_by: кто подтвердил набор (только у ``status=reviewed``); пусто — не записано.
    """

    case_id: str
    run_id: str
    cutoff_k: int
    judge: JudgeSpec | None
    policy: AnswerPolicy
    status: str
    entries: tuple[AnswerEntry, ...]
    reviewed_by: str | None = None

    def by_cell(self) -> dict[str, AnswerEntry]:
        """``cell → запись`` (ячейки уникальны по инварианту загрузки)."""
        return {entry.cell: entry for entry in self.entries}

    def entry_for(self, cell: str) -> AnswerEntry:
        """Запись ячейки.

        :raises KeyError: ячейки нет в наборе (молча вернуть ``None`` значило бы
            «ячейка без ответов» там, где её просто не размечали).
        """
        for entry in self.entries:
            if entry.cell == cell:
                return entry
        raise KeyError(f"ячейки {cell!r} нет в эталоне отвечающих документов")


def _required_str(item: Mapping[str, object], field: str, where: str) -> str:
    """Непустая строка ``item[field]`` или ``ValueError`` с адресом поля."""
    value = item.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{where}.{field}: ожидается непустая строка, получено {value!r}")
    return value


def _optional_str(item: Mapping[str, object], field: str, where: str) -> str:
    """Строка ``item[field]``; отсутствие допустимо и даёт пустую строку."""
    value = item.get(field, "")
    if not isinstance(value, str):
        raise ValueError(f"{where}.{field}: ожидается строка, получено {type(value).__name__}")
    return value


def _positive_int(item: Mapping[str, object], field: str, where: str) -> int:
    """Целое ``> 0`` из ``item[field]`` (``bool`` не число для этой цели)."""
    value = item.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{where}.{field}: ожидается целое > 0, получено {value!r}")
    return value


def _non_negative_int(item: Mapping[str, object], field: str, where: str) -> int:
    """Целое ``>= 0`` из ``item[field]``."""
    value = item.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{where}.{field}: ожидается целое >= 0, получено {value!r}")
    return value


def _canonical_how(how: object, where: str) -> tuple[str, ...]:
    """Проверяет ``how`` и возвращает его в каноническом порядке :data:`ANSWER_HOW`.

    :raises ValueError: не список, пустой, неизвестный токен или дубль (со списком
        допустимых — читатель должен видеть, что именно не так).
    """
    if not isinstance(how, list) or not how:
        raise ValueError(
            f"{where}.how: ожидается непустой список источников, получено {how!r}"
        )
    unknown = [item for item in how if item not in ANSWER_HOW]
    if unknown:
        raise ValueError(
            f"{where}.how: неизвестные источники {unknown} "
            f"(допустимо: {', '.join(ANSWER_HOW)})"
        )
    if len(set(how)) != len(how):
        raise ValueError(f"{where}.how: дубли источников {how!r}")
    return tuple(sorted(how, key=ANSWER_HOW.index))


def _load_policy(raw: object) -> AnswerPolicy:
    """Разбирает ``policy``: обязательные ключи, известные типы, никаких тихих дефолтов."""
    if not isinstance(raw, dict):
        raise ValueError(f"эталон ответов: policy должен быть объектом, получено {raw!r}")
    missing = [key for key in _POLICY_KEYS if key not in raw]
    if missing:
        raise ValueError(f"эталон ответов: policy без полей {missing}")
    unknown = sorted(set(raw) - set(_POLICY_KEYS) - {"include_partial"})
    if unknown:
        raise ValueError(f"эталон ответов: policy с неизвестными полями {unknown}")
    include_partial = raw.get("include_partial", False)
    if not isinstance(include_partial, bool):
        raise ValueError(
            "эталон ответов: policy.include_partial ожидается bool, "
            f"получено {include_partial!r}"
        )
    return AnswerPolicy(
        term_min_length=_positive_int(raw, "term_min_length", "policy"),
        term_min_distinct=_positive_int(raw, "term_min_distinct", "policy"),
        retrieved_depth=_non_negative_int(raw, "retrieved_depth", "policy"),
        max_candidates_per_cell=_positive_int(raw, "max_candidates_per_cell", "policy"),
        include_partial=include_partial,
    )


def _load_judge(raw: object) -> JudgeSpec | None:
    """``judge``: ``None`` или объект с непустым ``model_id``."""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError(f"эталон ответов: judge должен быть объектом или null, получено {raw!r}")
    unknown = sorted(set(raw) - {"model_id", "revision", "prompt_hash"})
    if unknown:
        raise ValueError(f"эталон ответов: judge с неизвестными полями {unknown}")
    return JudgeSpec(
        model_id=_required_str(raw, "model_id", "judge"),
        revision=_optional_str(raw, "revision", "judge"),
        prompt_hash=_optional_str(raw, "prompt_hash", "judge"),
    )


def _load_entry(item: object, index: int) -> AnswerEntry:
    """Разбирает одну запись и проверяет её инварианты (все — ``ValueError``)."""
    where = f"entries[{index}]"
    if not isinstance(item, dict):
        raise ValueError(f"{where}: ожидается объект, получено {type(item).__name__}")
    unknown = sorted(
        set(item) - {"cell", "product_id", "param_id", "cited_doc_ids", "answer_docs"}
    )
    if unknown:
        raise ValueError(f"{where}: лишние поля {unknown}")

    cell = _required_str(item, "cell", where)
    product_id = _required_str(item, "product_id", where)
    param_id = _required_str(item, "param_id", where)
    where = f"{where} {cell}"
    expected_cell = f"{product_id}/{param_id}"
    if cell != expected_cell:
        raise ValueError(f"{where}: cell не совпадает с {expected_cell}")

    cited_raw = item.get("cited_doc_ids")
    if not isinstance(cited_raw, list):
        raise ValueError(f"{where}.cited_doc_ids: ожидается список")
    cited: list[str] = []
    for doc_id in cited_raw:
        if not isinstance(doc_id, str) or not doc_id.strip():
            raise ValueError(f"{where}.cited_doc_ids: ожидаются непустые строки")
        cited.append(doc_id)
    if len(set(cited)) != len(cited):
        raise ValueError(f"{where}.cited_doc_ids: дубли {cited!r}")

    docs_raw = item.get("answer_docs")
    if not isinstance(docs_raw, list) or not docs_raw:
        raise ValueError(
            f"{where}.answer_docs: ожидается непустой список документов "
            "(ячейка без отвечающих документов — не запись эталона)"
        )
    docs: list[AnswerDoc] = []
    seen: set[str] = set()
    for doc_index, doc in enumerate(docs_raw):
        doc_where = f"{where}.answer_docs[{doc_index}]"
        if not isinstance(doc, dict):
            raise ValueError(f"{doc_where}: ожидается объект, получено {type(doc).__name__}")
        doc_unknown = sorted(set(doc) - {"doc_id", "how", "quote"})
        if doc_unknown:
            raise ValueError(f"{doc_where}: лишние поля {doc_unknown}")
        doc_id = _required_str(doc, "doc_id", doc_where)
        if doc_id in seen:
            raise ValueError(f"{doc_where}: дубль doc_id {doc_id!r} в ячейке")
        seen.add(doc_id)
        # I17: документ принадлежит продукту ячейки. ``doc_id`` = ``product:hash``,
        # поэтому префикс — единственная проверка, доступная без корпуса.
        prefix = doc_id.split(":", 1)[0]
        if ":" not in doc_id or prefix != product_id:
            raise ValueError(
                f"{doc_where}: doc_id {doc_id!r} не принадлежит продукту {product_id!r} "
                "(префикс doc_id до ':' обязан совпадать с product_id — иначе метрика "
                "смешает продукты, I17)"
            )
        how = _canonical_how(doc.get("how"), doc_where)
        quote = _optional_str(doc, "quote", doc_where)
        if ("llm" in how or "human" in how) and not quote.strip():
            raise ValueError(
                f"{doc_where}: quote пуст, а how содержит {'llm' if 'llm' in how else 'human'} — "
                "суждение без цитаты не доказательство (та же дисциплина, что I22)"
            )
        docs.append(AnswerDoc(doc_id=doc_id, how=how, quote=quote))

    missing_cited = sorted(set(cited) - seen)
    if missing_cited:
        raise ValueError(
            f"{where}.cited_doc_ids: {missing_cited} отсутствуют в answer_docs — адрес "
            "цитаты по определению отвечает; если разметчик считает иначе, это ошибка "
            "разметки O14, а не допустимое состояние"
        )
    return AnswerEntry(
        cell=cell,
        product_id=product_id,
        param_id=param_id,
        cited_doc_ids=tuple(cited),
        answer_docs=tuple(docs),
    )


def load_answer_sets(path: str | Path) -> AnswerSets:
    """Загружает эталон отвечающих документов и проверяет все инварианты формата.

    :raises FileNotFoundError: файла нет (не глотается).
    :raises ValueError: некорректный JSON, чужой ``format``, пустые ``case_id``/``run_id``,
        ``cutoff_k <= 0``, неизвестный ``status``, ``policy`` без обязательных полей,
        дубль ``cell``, ``cell != product_id/param_id``, пустой ``answer_docs``, дубль
        ``doc_id`` в ячейке, чужой продукт в ``doc_id``, пустой/неизвестный/дублирующий
        ``how``, ``cited`` вне набора, ``llm`` без ``quote``, ``llm`` без ``judge``.
    """
    raw = Path(path).read_text(encoding="utf-8")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"эталон ответов: некорректный JSON ({path}): {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(
            f"эталон ответов: ожидается JSON-объект, получено {type(data).__name__}"
        )

    fmt = data.get("format")
    if fmt != ANSWER_SETS_FORMAT:
        raise ValueError(
            f"эталон ответов: поле format={fmt!r}, ожидается {ANSWER_SETS_FORMAT!r}"
        )

    case_id = _required_str(data, "case_id", "эталон ответов")
    run_id = _required_str(data, "run_id", "эталон ответов")
    cutoff_k = _positive_int(data, "cutoff_k", "эталон ответов")
    status = _required_str(data, "status", "эталон ответов")
    if status not in ANSWER_STATUSES:
        raise ValueError(
            f"эталон ответов: status={status!r}, ожидается одно из {list(ANSWER_STATUSES)}"
        )
    judge = _load_judge(data.get("judge"))
    policy = _load_policy(data.get("policy"))
    reviewed_by = data.get("reviewed_by")
    if reviewed_by is not None and not isinstance(reviewed_by, str):
        raise ValueError("эталон ответов: reviewed_by ожидается строкой или отсутствует")

    entries_raw = data.get("entries")
    if not isinstance(entries_raw, list):
        raise ValueError("эталон ответов: поле entries должно быть списком")

    entries: list[AnswerEntry] = []
    seen_cells: dict[str, int] = {}
    for index, item in enumerate(entries_raw):
        entry = _load_entry(item, index)
        if entry.cell in seen_cells:
            raise ValueError(
                f"эталон ответов: дубль cell {entry.cell!r} в entries[{index}] "
                f"и entries[{seen_cells[entry.cell]}]"
            )
        seen_cells[entry.cell] = index
        entries.append(entry)

    if judge is None and any(
        "llm" in doc.how for entry in entries for doc in entry.answer_docs
    ):
        raise ValueError(
            "эталон ответов: judge=null, но у документов в how есть 'llm' — провенанс "
            "судьи потерян (набор нельзя отличить от человеческой разметки)"
        )
    return AnswerSets(
        case_id=case_id,
        run_id=run_id,
        cutoff_k=cutoff_k,
        judge=judge,
        policy=policy,
        status=status,
        entries=tuple(entries),
        reviewed_by=reviewed_by,
    )


def answer_doc_ids(entry: AnswerEntry) -> tuple[str, ...]:
    """``doc_id`` набора отвечающих в порядке записи (для ``expected_doc_ids`` прибора)."""
    return tuple(doc.doc_id for doc in entry.answer_docs)


def cited_doc_ids(entry: AnswerEntry) -> tuple[str, ...]:
    """Документы, объявленные адресом цитаты (подмножество :func:`answer_doc_ids`)."""
    return tuple(entry.cited_doc_ids)


def sensitivity_doc_ids(entry: AnswerEntry) -> tuple[str, ...]:
    """``doc_id`` **этой ячейки**, чей ``how`` содержит ``retrieved``.

    По-ячеечно, а не глобально — и это существенно: документ X в ячейке A может прийти из
    выдачи (``retrieved``), а в ячейке B — по терминам (``term``). Глобальный фильтр
    выбросил бы X и из B, занизив чувствительность ровно в тех ячейках, где источник
    независим, — то есть там, где вопрос «а если убрать подсказку системы» и задан.
    """
    return tuple(
        sorted(doc.doc_id for doc in entry.answer_docs if "retrieved" in doc.how)
    )


def sensitivity_by_cell(entries: Sequence[AnswerEntry]) -> dict[str, tuple[str, ...]]:
    """``cell → doc_id``, пришедшие из выдачи системы под тестом (см. :func:`sensitivity_doc_ids`)."""
    return {entry.cell: sensitivity_doc_ids(entry) for entry in entries}


def answer_doc_ids_without_retrieved(entry: AnswerEntry) -> tuple[str, ...]:
    """Набор ячейки без документов из ``retrieved`` — чувствительность метрики.

    Пустой результат законен и значим: ячейка, чьи ответы целиком пришли из выдачи
    системы под тестом, при вычитании даёт **ноль**, а не «ячейку без вопроса». Прибор
    считает такую ячейку как ``hit = 0`` и держит её в знаменателе: иначе число стало бы
    оптимистичным именно на тех ячейках, ради которых чувствительность и публикуется.
    """
    return tuple(
        doc.doc_id for doc in entry.answer_docs if "retrieved" not in doc.how
    )


def answers_summary(entries: Sequence[AnswerEntry]) -> dict[str, Any]:
    """Сводка набора: ячейки, документы, распределение ``how``, размеры на ячейку.

    Публикуются: ``cells``; ``documents`` — всего членов (сумма по ячейкам);
    ``distinct_documents`` — уникальных ``doc_id``; ``how`` — по каждому токену
    :data:`ANSWER_HOW` (документов с этим происхождением); ``docs_per_cell`` —
    ``min``/``median``/``max``; ``cells_cited_only`` — все члены только ``cited``;
    ``cells_without_term`` — ни один член не пришёл из ``term`` (имя говорит про ``term``,
    и считается именно он: ячейка из одних ``llm``-кандидатов тоже без ``term``);
    ``judged_docs`` — документы с ``llm`` или ``human`` (у них обязательна цитата);
    ``cells_sources`` — по ячейкам, а не по документам: класс = объединение токенов
    ``how`` всех членов ячейки (один токен — он, несколько — ``mixed``, пусто —
    ``unknown``). Все классы присутствуют с нулями и в фиксированном порядке: «0 из 132»
    обязано читаться как ноль, а не как отсутствие поля.
    """
    ordered = tuple(entries)
    how_counts: dict[str, int] = {token: 0 for token in ANSWER_HOW}
    cells_sources: dict[str, int] = {
        token: 0 for token in (*ANSWER_HOW, "mixed", "unknown")
    }
    per_cell: list[int] = []
    distinct: set[str] = set()
    total_docs = 0
    judged_docs = 0
    cited_only = 0
    without_term = 0
    for entry in ordered:
        per_cell.append(len(entry.answer_docs))
        total_docs += len(entry.answer_docs)
        only_cited = True
        has_term = False
        cell_tokens: set[str] = set()
        for doc in entry.answer_docs:
            distinct.add(doc.doc_id)
            cell_tokens.update(doc.how)
            for token in doc.how:
                how_counts[token] += 1
            if doc.how != ("cited",):
                only_cited = False
            if "term" in doc.how:
                has_term = True
            if "llm" in doc.how or "human" in doc.how:
                judged_docs += 1
        if only_cited:
            cited_only += 1
        if not has_term:
            without_term += 1
        if not cell_tokens:
            cells_sources["unknown"] += 1
        elif len(cell_tokens) == 1:
            cells_sources[next(iter(cell_tokens))] += 1
        else:
            cells_sources["mixed"] += 1
    return {
        "cells": len(ordered),
        "documents": total_docs,
        "distinct_documents": len(distinct),
        "how": how_counts,
        "docs_per_cell": {
            "min": min(per_cell) if per_cell else 0,
            "median": float(statistics.median(per_cell)) if per_cell else 0.0,
            "max": max(per_cell) if per_cell else 0,
        },
        "cells_cited_only": cited_only,
        "cells_without_term": without_term,
        "cells_sources": cells_sources,
        "judged_docs": judged_docs,
    }


def _entry_payload(entry: AnswerEntry) -> dict[str, Any]:
    """Каноническая запись ячейки: ``how`` в порядке :data:`ANSWER_HOW`, документы по ``doc_id``."""
    return {
        "cell": entry.cell,
        "product_id": entry.product_id,
        "param_id": entry.param_id,
        "cited_doc_ids": sorted(entry.cited_doc_ids),
        "answer_docs": [
            {
                "doc_id": doc.doc_id,
                "how": list(sorted(doc.how, key=ANSWER_HOW.index)),
                "quote": doc.quote,
            }
            for doc in sorted(entry.answer_docs, key=lambda item: item.doc_id)
        ],
    }


def write_answer_sets(path: str | Path, sets: AnswerSets) -> None:
    """Пишет эталон канонически: файл воспроизводим побайтово при тех же входах.

    Сортировка — часть контракта, а не удобство: ``entries`` по ``cell``, ``answer_docs``
    по ``doc_id``, ``how`` в порядке :data:`ANSWER_HOW`, ``cited_doc_ids`` по возрастанию.
    Иначе один и тот же набор, собранный из XLSX в другом порядке строк, давал бы другой
    дифф, и «файл изменился» нельзя было бы отличить от «файл переписан».
    """
    payload: dict[str, Any] = {
        "format": ANSWER_SETS_FORMAT,
        "case_id": sets.case_id,
        "run_id": sets.run_id,
        "cutoff_k": sets.cutoff_k,
        "judge": (
            None
            if sets.judge is None
            else {
                "model_id": sets.judge.model_id,
                "revision": sets.judge.revision,
                "prompt_hash": sets.judge.prompt_hash,
            }
        ),
        "policy": sets.policy.as_dict(),
        "status": sets.status,
        "entries": [
            _entry_payload(entry) for entry in sorted(sets.entries, key=lambda item: item.cell)
        ],
    }
    if sets.reviewed_by:
        payload["reviewed_by"] = sets.reviewed_by
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
