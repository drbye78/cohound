"""coHound — документный retrieval-эталон (O14 плана 26).

**Зачем документный эталон.** Чанковый эталон (:mod:`core.pipeline.experiments`)
адресуется ``chunk_id`` и потому ломается при любой смене чанкера: новый
``chunker_version`` даёт новые id, и эталон превращается в набор неразрешённых
ожиданий, неотличимый от провала поиска. Документ адресуется URL-ом (за ``doc_id``
стоит документ корпуса), поэтому документный эталон переживает перечанкинг, а
метрика становится простой и устойчивой: «ожидаемый документ ячейки попал в top-K
выдачи».

**Почему ``unresolved`` отдельно.** Если ожидаемого документа нет в корпусе вовсе
(документ не скачан, сменился URL, другой ``corpus_run_id``), это не провал поиска,
а отсутствие предмета измерения. Такие ячейки исключаются из метрик и считаются
отдельно (``DocRetrievalReport.unresolved_cells``) — та же дисциплина, что
``unresolved_expected_ids`` в :mod:`core.pipeline.experiments`.

**Правило знаменателя при наборе больше одного.** Как только ожиданий у ячейки
несколько (answer-семейство, :mod:`core.pipeline.retrieval_answers`), «документа нет в
корпусе» перестаёт быть свойством ячейки целиком. Правило: ячейка ``unresolved`` **только
если ни один член набора не найден** в корпусе; знаменатель ``recall`` — члены,
**присутствующие** в корпусе (``DocQueryResult.resolved_doc_ids``), а число выпавших
публикуется как ``unresolved_docs``. Без этого набор из трёх, где два документа исчезли,
навсегда упирался бы в ``1/3``, и отсутствие документа читалось бы как провал поиска.
Пустой знаменатель — ``0.0`` (ячейка с ответами целиком из выпавших документов даёт
честный ноль), а не исключение из среднего: иначе число становилось бы оптимистичным
ровно там, где вопрос и задан. Формат 1 не меняется: там набор — один документ, ячейка
либо разрешена целиком (``1/1``), либо исключена.

**Ранг — вывод, а не вход.** ``hit``/``MRR`` считаются из пересчитанного ранга
(:func:`first_hit_rank`), а не из ``DocQueryResult.rank``: рассогласованный ранг
(``rank=1`` при пустой выдаче) развёл бы ``hit``/``MRR`` с ``recall``/``nDCG`` в одном
отчёте, и заметить это было бы нечем. Поле ``rank`` остаётся ради прежних конструкторов
и того, что показывает отчёт.

**Почему публикуются и ``hit_at_k``, и ``recall_at_k``.** При одном ожидании на
ячейку они совпадают: «попал ли документ в top-K» = «какая доля ожиданий найдена».
Оба всё равно публикуются: при нескольких ожиданиях они расходятся (документ нашёлся —
``hit`` = 1, а найдена лишь половина ожиданий — ``recall`` = 0.5), и читатель не должен
догадываться, какой из двух смыслов зашит в единственное число. Кроме того, ``recall``
остаётся сравнимой с чанковым отчётом. ``recall`` при этом **растёт с размером набора**
(больше членов — труднее найти все), поэтому он публикуется рядом с ``docs_per_cell``, а
не как одно число.

**Что такое интервал.** ``hit_ci``/``recall_ci`` — бутстрэп по **ячейкам**: ячейки
ресемплируются с возвращением объявленное число раз с объявленным ``seed``,
берутся перцентили 2.5/97.5. Это не «доверительный интервал вообще»: при другом
``seed`` числа сдвинутся, а при ``bootstrap == 1`` интервал вырожден в точку
(точечную оценку). Детерминизм — часть контракта: одинаковые вход и ``seed`` дают
побайтово одинаковый отчёт.

Модуль — чистая логика: единственный I/O — чтение JSON-файла эталона. Порты
ретривала сюда не входят, их собирает прибор
(``scripts/measure_retrieval_docs.py``). Категорийно нейтрален (§1.3), без numpy
на импорте.
"""

from __future__ import annotations

import json
import math
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from core.util.retrieval_eval import ndcg_at_k

__all__ = [
    "DOC_GOLDEN_FORMAT",
    "DocGoldenEntry",
    "DocQueryResult",
    "DocRetrievalReport",
    "document_ids_for",
    "evaluate_documents",
    "first_hit_rank",
    "load_doc_golden",
]

#: Формат файла документного эталона (чужой формат — не эталон).
DOC_GOLDEN_FORMAT = "cohound-retrieval-golden-docs/1"


@dataclass(frozen=True)
class DocGoldenEntry:
    """Одна ячейка документного эталона: параметр продукта и ожидаемые документы.

    :param cell: ``"{product_id}/{param_id}"`` — идентификатор ячейки эталона.
    :param product_id: продукт ячейки.
    :param param_id: параметр ячейки.
    :param source_url: URL, по которому размечен ожидаемый документ (устойчивый адрес).
    :param expected_doc_ids: непустой набор ожидаемых ``doc_id``.
    :param match: способ сопоставления URL с документом
        (``exact|canonical|canonical_query|moved_exact|moved_canonical``).
    """

    cell: str
    product_id: str
    param_id: str
    source_url: str
    expected_doc_ids: tuple[str, ...]
    match: str


def _required_str(item: Mapping[str, object], field: str, where: str) -> str:
    """Непустая строка ``item[field]`` или ``ValueError`` с адресом поля."""
    value = item.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{where}.{field}: ожидается непустая строка")
    return value


def _optional_str(item: Mapping[str, object], field: str, where: str) -> str:
    """Строка ``item[field]``; отсутствие допустимо и даёт пустую строку."""
    value = item.get(field, "")
    if not isinstance(value, str):
        raise ValueError(f"{where}.{field}: ожидается строка")
    return value


def load_doc_golden(path: str | Path) -> tuple[str, str, list[DocGoldenEntry]]:
    """Загружает документный эталон из JSON; возвращает ``(case_id, corpus_run_id, записи)``.

    Ожидаемая форма::

        {"format": "cohound-retrieval-golden-docs/1",
         "case_id": "...", "corpus_run_id": "...",
         "entries": [{"cell": "p/param", "product_id": "p", "param_id": "param",
                      "source_url": "https://...", "expected_doc_ids": ["p:..."],
                      "match": "canonical"}]}

    :raises FileNotFoundError: файла нет (не глотается).
    :raises ValueError: некорректный JSON, чужой ``format``, не список ``entries``,
        запись без непустых ``cell``/``product_id``/``param_id``/``source_url``,
        пустой ``expected_doc_ids`` или дубль ``cell`` (в сообщении — обе позиции).
    """
    raw = Path(path).read_text(encoding="utf-8")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"документный эталон: некорректный JSON ({path}): {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(
            f"документный эталон: ожидается JSON-объект, получено {type(data).__name__}"
        )

    fmt = data.get("format")
    if fmt != DOC_GOLDEN_FORMAT:
        raise ValueError(
            f"документный эталон: поле format={fmt!r}, ожидается {DOC_GOLDEN_FORMAT!r}"
        )

    case_id = _optional_str(data, "case_id", "документный эталон")
    corpus_run_id = _optional_str(data, "corpus_run_id", "документный эталон")

    entries_raw = data.get("entries")
    if not isinstance(entries_raw, list):
        raise ValueError("документный эталон: поле entries должно быть списком")

    entries: list[DocGoldenEntry] = []
    seen: dict[str, int] = {}
    for index, item in enumerate(entries_raw):
        where = f"entries[{index}]"
        if not isinstance(item, dict):
            raise ValueError(f"{where}: ожидается объект, получено {type(item).__name__}")
        cell = _required_str(item, "cell", where)
        product_id = _required_str(item, "product_id", where)
        param_id = _required_str(item, "param_id", where)
        source_url = _required_str(item, "source_url", where)
        match = _optional_str(item, "match", where)

        expected_raw = item.get("expected_doc_ids")
        if not isinstance(expected_raw, list) or not expected_raw:
            raise ValueError(
                f"{where}.expected_doc_ids: ожидается непустой список идентификаторов "
                "документов (ячейка без ожиданий — не ячейка эталона, её место в отчёте "
                "прибора, а не в эталоне)"
            )
        expected: list[str] = []
        for doc_id in expected_raw:
            if not isinstance(doc_id, str) or not doc_id.strip():
                raise ValueError(
                    f"{where}.expected_doc_ids: идентификаторы должны быть непустыми строками"
                )
            expected.append(doc_id)

        if cell in seen:
            raise ValueError(
                f"документный эталон: дубль cell {cell!r} в {where} и entries[{seen[cell]}]"
            )
        seen[cell] = index
        entries.append(
            DocGoldenEntry(
                cell=cell,
                product_id=product_id,
                param_id=param_id,
                source_url=source_url,
                expected_doc_ids=tuple(expected),
                match=match,
            )
        )
    return case_id, corpus_run_id, entries


def document_ids_for(
    chunk_ids: Sequence[str],
    chunk_doc: Mapping[str, str],
    *,
    top_k: int,
) -> tuple[str, ...]:
    """Документы первых ``top_k`` чанков в порядке первого появления, без дублей.

    ``chunk_doc`` — карта ``chunk_id → doc_id``. Чанк без записи в карте пропускается:
    гидртация не дала документа — не выдумываем его. Результат не длиннее ``top_k``.

    :raises ValueError: ``top_k <= 0``.
    """
    if top_k <= 0:
        raise ValueError(f"top_k: ожидается > 0, получено {top_k}")
    documents: list[str] = []
    seen: set[str] = set()
    for chunk_id in chunk_ids[:top_k]:
        doc_id = chunk_doc.get(chunk_id)
        if doc_id is None or doc_id in seen:
            continue
        seen.add(doc_id)
        documents.append(doc_id)
    return tuple(documents)


@dataclass(frozen=True)
class DocQueryResult:
    """Результат одной ячейки: ожидаемые и найденные документы, ранг, разрешимость.

    :param cell: ``"{product_id}/{param_id}"``.
    :param product_id: продукт ячейки.
    :param param_id: параметр ячейки.
    :param expected_doc_ids: ожидаемые ``doc_id`` ячейки (адрес — у адресного семейства,
        весь набор отвечающих — у answer-семейства).
    :param retrieved_doc_ids: документы top-K в порядке выдачи.
    :param rank: 1-based ранг первого найденного ожидаемого документа; ``None`` — не найден.
        **Не вход метрик**: :func:`evaluate_documents` пересчитывает ранг из
        ``(retrieved_doc_ids, expected_doc_ids)``, потому что рассогласованный ранг дал бы
        ``hit``/``MRR`` расходящимися с ``recall``/``nDCG`` в одном отчёте — молча. Поле
        остаётся ради прежних конструкторов и как то, что показывает отчёт.
    :param unresolved: ни один ``expected_doc_id`` не существует в корпусе — ячейка
        исключается из метрик и считается отдельно. Правило знаменателя: при наборе
        больше одного документа частичное выпадение из корпуса — **не** ``unresolved``,
        а меньшее число в знаменателе ``recall`` (см. ``resolved_doc_ids``).
    :param resolved_doc_ids: члены набора, которые **есть** в корпусе (знаменатель
        ``recall``). ``None`` — не объявлено, знаменатель равен ``expected_doc_ids``
        (поведение формата 1: там набор — один документ, и ячейка либо разрешена
        целиком, либо исключена). Пустой кортеж — законное «ни одного не осталось»
        (ячейка всё равно считается, если вызывающий объявил её scorable).
    """

    cell: str
    product_id: str
    param_id: str
    expected_doc_ids: tuple[str, ...]
    retrieved_doc_ids: tuple[str, ...]
    rank: int | None
    unresolved: bool
    resolved_doc_ids: tuple[str, ...] | None = None

    @property
    def recall_denominator_ids(self) -> tuple[str, ...]:
        """Члены набора, по которым считается ``recall`` (разрешённые в корпусе)."""
        if self.resolved_doc_ids is None:
            return self.expected_doc_ids
        return self.resolved_doc_ids


@dataclass(frozen=True)
class DocRetrievalReport:
    """Итог документного ретривала по эталону.

    :param profile: профиль ретривала (``R0``/``R1``/``R2``).
    :param k: cut-off top-K.
    :param cells: сколько записей эталона пришло.
    :param scored: разрешённых ячеек (не ``unresolved``) — по ним считаются метрики.
    :param unresolved_cells: ячеек, чьих ожидаемых документов нет в корпусе.
    :param hit_at_k: доля ``scored``, где ранг первого ожидаемого не ``None``.
    :param recall_at_k: средний по ``scored`` ``|найдено ∩ разрешённые| / |разрешённые|``.
        Знаменатель — члены набора, **присутствующие в корпусе** (``resolved_doc_ids``):
        документ, исчезнувший из корпуса, — отсутствие предмета измерения, а не провал
        поиска, и вечное ``1/3`` на наборе из трёх читалось бы как провал.
    :param mrr: средний по ``scored`` ``1/rank`` (не найден — 0).
    :param ndcg_at_k: средний по ``scored`` nDCG@K по бинарной релевантности: учитывает
        **позиции всех** найденных ожидаемых документов, а не только первого (как MRR)
        и не только их число (как recall). Дефолт ``0.0`` — поле добавлено после первых
        публикаций отчёта, и старые конструкторы обязаны продолжать работать.
    :param hit_ci: бутстрэп-интервал ``hit_at_k`` (перцентили 2.5/97.5).
    :param recall_ci: бутстрэп-интервал ``recall_at_k``.
    :param per_cell: результаты ячеек в порядке входа.
    :param unresolved_docs: сколько членов наборов **не разрешилось** в корпусе (сумма по
        ячейкам). ``0`` у формата 1 (там набор — один документ, и ячейка либо разрешена
        целиком, либо исключена). Дефолт — поле добавлено после первых публикаций.
    """

    profile: str
    k: int
    cells: int
    scored: int
    unresolved_cells: int
    hit_at_k: float
    recall_at_k: float
    mrr: float
    hit_ci: tuple[float, float]
    recall_ci: tuple[float, float]
    per_cell: tuple[DocQueryResult, ...]
    ndcg_at_k: float = 0.0
    unresolved_docs: int = 0


def _percentile(values: Sequence[float], q: float) -> float:
    """Перцентиль без numpy: линейная интерполяция между соседними порядковыми значениями."""
    ordered = sorted(values)
    if not ordered:
        return 0.0
    pos = (len(ordered) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return float(ordered[lo])
    return float(ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo))


def first_hit_rank(
    expected_doc_ids: Sequence[str], retrieved_doc_ids: Sequence[str]
) -> int | None:
    """1-based ранг первого ожидаемого документа в выдаче; ``None`` — не найден.

    Единственное место, где ранг вычисляется: и метрики (:func:`evaluate_documents`), и
    отчёт прибора берут его отсюда. Рассогласованный ``DocQueryResult.rank`` (например,
    ``1`` при пустой выдаче) не может развести ``hit``/``MRR`` с ``recall``/``nDCG`` —
    все четыре считаются из одних и тех же двух списков.
    """
    expected = set(expected_doc_ids)
    for position, doc_id in enumerate(retrieved_doc_ids, start=1):
        if doc_id in expected:
            return position
    return None


def _cell_hit(result: DocQueryResult) -> float:
    """1.0, если ожидаемый документ ячейки попал в top-K, иначе 0.0.

    Ранг пересчитывается из выдачи, а не берётся из ``result.rank``: поле результата —
    то, что показывает отчёт, а не вход метрики.
    """
    return (
        1.0
        if first_hit_rank(result.expected_doc_ids, result.retrieved_doc_ids) is not None
        else 0.0
    )


def _cell_reciprocal_rank(result: DocQueryResult) -> float:
    """``1/rank`` по пересчитанному рангу (не найден — 0.0)."""
    rank = first_hit_rank(result.expected_doc_ids, result.retrieved_doc_ids)
    return 0.0 if rank is None else 1.0 / rank


def _cell_recall(result: DocQueryResult) -> float:
    """Доля найденных **разрешённых** ожиданий; пустой знаменатель даёт 0.0.

    Знаменатель — ``resolved_doc_ids`` (члены набора, присутствующие в корпусе), а не
    весь набор: исчезнувший из корпуса документ не является провалом поиска, и деление
    на него превращало бы «2 из 3 исчезли» в вечный потолок ``1/3``. Пустой знаменатель
    — ``0.0`` (честный ноль ячейки, чьи ответы целиком выпали), а не исключение из
    среднего: иначе число становилось бы оптимистичным ровно там, где вопрос и задан.
    """
    expected = set(result.recall_denominator_ids)
    if not expected:
        return 0.0
    retrieved = set(result.retrieved_doc_ids)
    return len(expected & retrieved) / len(expected)


def evaluate_documents(
    results: Sequence[DocQueryResult],
    *,
    profile: str,
    k: int,
    bootstrap: int = 1000,
    seed: int = 0,
) -> DocRetrievalReport:
    """Считает документные метрики top-K и бутстрэп-интервалы по ячейкам.

    Метрики: ``hit_at_k``, ``recall_at_k``, ``mrr`` и ``ndcg_at_k`` (позиции **всех**
    найденных ожидаемых документов ячейки). Бутстрэп-интервалы считаются только для
    ``hit``/``recall``: для nDCG интервала нет — отдельный вопрос, а не забытое поле.

    ``unresolved`` ячейки исключаются из метрик и считаются отдельно: «документа нет
    в корпусе» — не провал поиска. Если ``scored == 0``, метрики ``0.0``, интервалы
    ``(0.0, 0.0)``, и это не ошибка (пустой корпус — честный ноль). **Читатель обязан
    смотреть ``scored``, а не только ``recall_at_k``**: ноль по одной разрешённой
    ячейке и ноль по сотне — разные утверждения.

    Интервал — бутстрэп по ячейкам с возвращением (``bootstrap`` ресемплов,
    ``random.Random(seed)``), перцентили 2.5/97.5. При ``bootstrap == 1`` ресемплинг
    вырожден: интервал совпадает с точечной оценкой (объявлено, а не запрещено).

    :raises ValueError: ``bootstrap < 1``.
    """
    if bootstrap < 1:
        raise ValueError(f"bootstrap: ожидается >= 1, получено {bootstrap}")

    ordered = tuple(results)
    scored_cells = tuple(result for result in ordered if not result.unresolved)
    unresolved_cells = sum(1 for result in ordered if result.unresolved)
    scored = len(scored_cells)

    if scored == 0:
        return DocRetrievalReport(
            profile=profile,
            k=k,
            cells=len(ordered),
            scored=0,
            unresolved_cells=unresolved_cells,
            hit_at_k=0.0,
            recall_at_k=0.0,
            mrr=0.0,
            hit_ci=(0.0, 0.0),
            recall_ci=(0.0, 0.0),
            per_cell=ordered,
            unresolved_docs=sum(
                len(result.expected_doc_ids) - len(result.resolved_doc_ids)
                for result in ordered
                if result.resolved_doc_ids is not None
            ),
        )

    hits = [_cell_hit(result) for result in scored_cells]
    recalls = [_cell_recall(result) for result in scored_cells]
    # MRR — из **пересчитанного** ранга: рассогласованный ``result.rank`` не должен
    # разводить ``hit``/``MRR`` с ``recall``/``nDCG`` в одном отчёте.
    reciprocal_ranks = [_cell_reciprocal_rank(result) for result in scored_cells]
    # nDCG@K по документам: позиции **всех** найденных ожиданий, а не только первого.
    ndcgs = [
        ndcg_at_k(result.expected_doc_ids, result.retrieved_doc_ids, k)
        for result in scored_cells
    ]

    hit_at_k = sum(hits) / scored
    recall_at_k = sum(recalls) / scored
    mrr = sum(reciprocal_ranks) / scored
    ndcg = sum(ndcgs) / scored
    unresolved_docs = sum(
        len(result.expected_doc_ids) - len(result.resolved_doc_ids)
        for result in ordered
        if result.resolved_doc_ids is not None
    )

    if bootstrap == 1:
        hit_ci = (hit_at_k, hit_at_k)
        recall_ci = (recall_at_k, recall_at_k)
    else:
        rng = random.Random(seed)  # noqa: S311 — bootstrap воспроизводим по seed: криптостойкость вредна
        hit_samples: list[float] = []
        recall_samples: list[float] = []
        for _ in range(bootstrap):
            picks = [rng.randrange(scored) for _ in range(scored)]
            hit_samples.append(sum(hits[i] for i in picks) / scored)
            recall_samples.append(sum(recalls[i] for i in picks) / scored)
        hit_ci = (_percentile(hit_samples, 0.025), _percentile(hit_samples, 0.975))
        recall_ci = (_percentile(recall_samples, 0.025), _percentile(recall_samples, 0.975))

    return DocRetrievalReport(
        profile=profile,
        k=k,
        cells=len(ordered),
        scored=scored,
        unresolved_cells=unresolved_cells,
        hit_at_k=hit_at_k,
        recall_at_k=recall_at_k,
        mrr=mrr,
        hit_ci=hit_ci,
        recall_ci=recall_ci,
        per_cell=ordered,
        ndcg_at_k=ndcg,
        unresolved_docs=unresolved_docs,
    )
