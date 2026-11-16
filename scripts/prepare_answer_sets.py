#!/usr/bin/env python3
"""Подготовка эталона **всех отвечающих документов** (O42 плана 26).

Что утверждает этот эталон: «для ячейки ``{product_id}/{param_id}`` документ X —
один из тех, которыми параметр **решается** для этого продукта». Это не то же, что
документный эталон (``retrieval_golden_docs.json``), который называет **один адрес** —
место, откуда взята цитата comparative table. Замер O15 (16.11.2026) показал разрыв:
вердикт есть у 111 из 132 ячеек, объявленный адрес попал в top-10 у 34, а цитата взята
из другого документа продукта в 100. Значит адресная метрика отвечает на другой вопрос,
и набор отвечающих нужен отдельным файлом (``cohound-retrieval-golden-answers/1``,
:mod:`core.pipeline.retrieval_answers`).

Подкоманды:

``candidates``
    Собирает кандидатов по каждой ячейке эталона и пишет **XLSX** для человека. Источники
    разделимы и записываются в колонку ``how``:

    ``cited`` — ``expected_doc_ids`` ячейки из ``retrieval_golden_docs.json`` (адрес).
        Если документа нет в корпусе, строка остаётся с пустым ``doc_url`` и
        ``note``-подобной пометкой в ``why``: это факт о покрытии, а не повод выбросить.
    ``term`` — документы **того же продукта**, где встречаются различимые термины
        параметра. Правило объявлено целиком: слова из ``title`` параметра длиной
        >= ``--term-min-length`` (4), приведённые к нижнему регистру, минус служебные
        (``_STOPWORDS``); документ-кандидат — тот, у которого в тексте чанков встречаются
        >= ``--term-min-distinct`` (2) **различных** таких слова. Колонка ``why`` несёт
        найденные слова. Источник независим от индекса под тестом — иначе система
        составляла бы себе экзамен.
    ``retrieved`` — только по флагу ``--include-retrieved``: документы из top-K выдачи
        объявленных профилей, с рангом в ``why``. Требует индекс (и для R1/R2 эмбеддер);
        сборка индекса и композиция берутся тем же путём, что у прибора
        ``scripts/measure_retrieval_docs.py`` (импорт его же функций — одна проводка, а не
        две копии, которые разъедутся молча).
    ``llm`` — только по флагу ``--llm``: судья по **объявленной** модели оценщика
        (``--llm-model`` > ``deploy.llm.<slot>.model_id`` > отказ) отвечает строгим JSON
        ``{"answers": "yes|partial|no", "quote": "...", "reason": "..."}``; цитата обязана
        быть подстрокой текста документа, иначе ``judge_verdict=rejected_by_quote_check``
        с видимой причиной. Вердикт судьи — **кандидат**, а не принятый ответ: решение
        остаётся за человеком (колонки ``judge_verdict``/``judge_quote`` — для чтения).

    Обрезка ``--max-candidates-per-cell`` (20) стратифицированная: сначала **все**
    ``cited`` (они не выкидываются никогда), затем ``term`` по убыванию числа совпавших
    терминов (tie-break по ``doc_id``), затем ``retrieved`` по рангу, затем ``llm``.
    Сколько ячеек упёрлось в лимит — называется в отчёте и в листе ``policy``.

``compile``
    Заполненный XLSX → ``retrieval_golden_answers.json``. В набор входят ``yes`` (+
    документы ``cited``, которые отвечают по определению; ``partial`` — только с
    ``--include-partial``, и это записывается в ``policy`` файла). ``no`` у документа с
    ``how=cited`` — **отказ**: это ошибка разметки O14, её надо увидеть. Ячейка без
    единственного ``yes`` не выпадает: её набор — ``cited``-документы, и она считается в
    ``cells_without_answer``.

``validate``
    (а) XLSX: значения ``answers`` из набора или пусто, нет дублей ``(cell, doc_id)``,
    число строк, покрытие ячеек эталона (``--golden``: какие ячейки без кандидатов).
    (б) собранный JSON — через :func:`core.pipeline.retrieval_answers.load_answer_sets`,
    без второй копии правил.

``summary``
    Сводка по листу: распределение ``answers`` и ``how``, размеры наборов, сколько ячеек
    с одним кандидатом, сколько упёрлось в лимит, сколько ``cited``-документов человек
    отверг (число ошибок разметки O14).

Никакого ``random``: порядок строк — сортировка и объявленный приоритет источников,
поэтому два запуска дают одинаковый набор строк.

Формат листа (XLSX): закреплённая шапка с фильтром, выпадающий список в колонке
``answers`` (``yes``/``partial``/``no``), залитые строки с ``how=cited`` (их человек
обязан подтвердить), второй лист «как размечать» — инструкция живёт в файле, а не в
переписке, третий лист ``policy`` — машинные условия, с которыми лист собран (``compile``
и ``summary`` читают их оттуда, а не угадывают).

Нейтрален (§1.3): ни продуктов, ни домена — пакет, эталон и БД приходят аргументами.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Корень репозитория на sys.path: скрипт запускается как `python scripts/…py`, и тогда
# sys.path[0] — каталог scripts, а не корень (плоская раскладка пакета `core`).
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
# Соседний прибор: проводка ретривала (композиция, индексы, эмбеддер) импортируется из
# него, а не копируется. Две копии одной проводки разошлись бы молча — и `retrieved`
# считался бы не тем путём, каким получены вердикты.
_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from core.pipeline.retrieval_answers import (  # noqa: E402 — после правки sys.path
    ANSWER_HOW,
    AnswerDoc,
    AnswerEntry,
    AnswerPolicy,
    AnswerSets,
    JudgeSpec,
    answers_summary,
    write_answer_sets,
)

#: Колонки листа кандидатов. Машинные (``cell``…``judge_quote``) заполняет прибор;
#: ``answers``/``comment`` — человек.
CANDIDATE_COLUMNS = (
    "cell",
    "product_id",
    "param_id",
    "param_title",
    "param_rule",
    "doc_id",
    "doc_url",
    "doc_title",
    "how",
    "why",
    "snippet",
    "judge_verdict",
    "judge_quote",
    "answers",
    "comment",
)

#: Значения колонки ``answers``. Пусто = «ещё не рассмотрено». ``unclear`` намеренно
#: отсутствует: «не хватило фрагмента» — повод открыть документ, а не класс ответа.
ANSWERS = ("yes", "partial", "no")

#: Листы книги: кандидаты, инструкция, машинные условия.
SHEET_CANDIDATES = "кандидаты"
SHEET_GUIDE = "как размечать"
SHEET_POLICY = "policy"

#: Служебные слова, которые не считаются термином параметра (объявленный кортеж).
_STOPWORDS = frozenset(
    {
        "для", "как", "или", "при", "что", "это", "все", "его",
        "the", "and", "for", "with", "that", "from", "this", "are", "not", "can",
        "you", "may",
    }
)

#: Ширина окна ``snippet``: колонка читается глазами, полный текст — в корпусе.
SNIPPET_WIDTH = 300


# ---------------------------------------------------------------------------
# Чтение прогона (read-only, без гейта схемы — прибор читает данные, а не контракт)
# ---------------------------------------------------------------------------
def _open_readonly(path: Path) -> sqlite3.Connection:
    """Соединение ``mode=ro`` со строками-словарями: скрипт ничего не пишет в прогон."""
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


@dataclass(frozen=True)
class RunMeta:
    """Строка ``runs``: прогон, чей корпус адресуют ``doc_id`` набора."""

    run_id: str
    package_id: str
    build_id: str
    cutoff_k: int
    schema_version: str


def read_run_meta(conn: sqlite3.Connection, run_id: str | None) -> RunMeta:
    """Читает условия прогона; неоднозначность — отказ, а не «первая строка».

    :raises SystemExit: прогонов ноль/несколько без ``--run-id`` или не записана сборка
        индекса (без неё ретривал не к чему привязать).
    """
    sql = "SELECT run_id, package_id, index_build_id, retrieval_cutoff_K FROM runs"
    params: list[Any] = []
    if run_id is not None:
        sql += " WHERE run_id = ?"
        params.append(run_id)
    rows = conn.execute(sql, params).fetchall()
    if not rows:
        raise SystemExit(f"answer-sets: прогон не найден: run_id={run_id!r}")
    if len(rows) > 1:
        names = ", ".join(str(row["run_id"]) for row in rows[:5])
        raise SystemExit(
            f"answer-sets: в файле {len(rows)} прогонов ({names}…): назовите --run-id, "
            "иначе неясно, чей корпус адресует набор"
        )
    row = rows[0]
    schema_row = conn.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()
    return RunMeta(
        run_id=str(row["run_id"]),
        package_id=str(row["package_id"] or ""),
        build_id=str(row["index_build_id"] or ""),
        cutoff_k=int(row["retrieval_cutoff_K"] or 0),
        schema_version=str(schema_row["value"]) if schema_row is not None else "",
    )


def load_params(
    db_path: Path, package_id: str, package_dir: Path | None
) -> dict[str, Any]:
    """Каталог прогона: снимок ``plan_inputs.json`` рядом с БД, иначе каталог пакета.

    Снимок предпочтительнее пакета: каталог мог измениться после прогона, а ``title``
    параметра — вход правила ``term``, и подмена заголовка подменила бы набор.
    """
    from core.domain.models import CatalogRow

    snapshot = db_path.parent / "plan_inputs.json"
    if snapshot.is_file():
        payload = json.loads(snapshot.read_text(encoding="utf-8"))
        params = payload.get("params")
        if not isinstance(params, list) or not params:
            raise SystemExit(f"answer-sets: {snapshot}: params пуст или не список")
        return {
            str(item.param_id): item
            for item in (CatalogRow.model_validate(raw) for raw in params)
        }
    if package_dir is None:
        raise SystemExit(
            f"answer-sets: нет снимка входов {snapshot} — назовите --package, чтобы взять "
            "каталог из пакета"
        )
    from core.config import load_catalog

    return {str(row.param_id): row for row in load_catalog(package_dir / "catalog.csv")}


def corpus_documents(conn: sqlite3.Connection) -> dict[str, tuple[str, str]]:
    """``doc_id → (product_id, url)``: адрес и продукт документа корпуса."""
    return {
        str(row["doc_id"]): (str(row["product_id"]), str(row["url"] or ""))
        for row in conn.execute("SELECT doc_id, product_id, url FROM corpus_docs")
    }


# ---------------------------------------------------------------------------
# Кандидаты: правило `term` и индекс терминов
# ---------------------------------------------------------------------------
def param_terms(title: str, min_length: int) -> tuple[str, ...]:
    """Термины параметра: слова ``title`` длиной >= ``min_length`` без служебных.

    Порядок — порядок появления в заголовке (детерминизм: набор терминов не зависит от
    того, как Python упорядочил множество).
    """
    pattern = re.compile(rf"\w{{{min_length},}}")
    terms: list[str] = []
    for token in pattern.findall(title.lower()):
        if token in _STOPWORDS or token in terms:
            continue
        terms.append(token)
    return tuple(terms)


def term_index(
    conn: sqlite3.Connection,
    needed: dict[str, set[str]],
    min_length: int,
) -> dict[tuple[str, str], set[str]]:
    """``(product, term) → doc_id``: один проход по корпусу, потоком по чанкам.

    Индекс строится по **тексту чанков** (а не по заголовкам документов): утверждение
    «документ отвечает» проверяется текстом. Ключ включает продукт, поэтому документ
    другого продукта в кандидаты попасть не может (I17).
    """
    pattern = re.compile(rf"\w{{{min_length},}}")
    index: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in conn.execute("SELECT doc_id, product_id, text FROM corpus_chunks"):
        product_id = str(row["product_id"])
        terms = needed.get(product_id)
        if not terms:
            continue
        tokens = set(pattern.findall(str(row["text"] or "").lower()))
        doc_id = str(row["doc_id"])
        for term in tokens & terms:
            index[(product_id, term)].add(doc_id)
    return dict(index)


@dataclass
class Candidate:
    """Кандидат ячейки: документ, происхождение, пояснение и вердикт судьи (если был)."""

    doc_id: str
    how: list[str] = field(default_factory=list)
    why: str = ""
    rank: int | None = None
    term_count: int = 0
    judge_verdict: str = ""
    judge_quote: str = ""

    def add_how(self, token: str) -> None:
        """Добавляет источник без дублей."""
        if token not in self.how:
            self.how.append(token)


def _candidate_for(
    candidates: dict[str, Candidate], doc_id: str
) -> Candidate:
    """Кандидат ячейки по ``doc_id`` (создаётся при первом упоминании)."""
    if doc_id not in candidates:
        candidates[doc_id] = Candidate(doc_id=doc_id)
    return candidates[doc_id]


def order_candidates(candidates: dict[str, Candidate]) -> list[Candidate]:
    """Порядок внутри ячейки: ``cited`` → ``term`` → ``retrieved`` → ``llm``.

    Внутри ``term`` — по убыванию числа совпавших терминов (tie-break по ``doc_id``):
    документ с тремя терминами параметра вероятнее, чем с двумя, и обрезка обязана
    отбросить менее вероятных, а не случайных.
    """
    cited = sorted(
        (item for item in candidates.values() if "cited" in item.how),
        key=lambda item: item.doc_id,
    )
    term = sorted(
        (
            item
            for item in candidates.values()
            if "cited" not in item.how and "term" in item.how
        ),
        key=lambda item: (-item.term_count, item.doc_id),
    )
    retrieved = sorted(
        (
            item
            for item in candidates.values()
            if "cited" not in item.how
            and "term" not in item.how
            and "retrieved" in item.how
        ),
        key=lambda item: (item.rank if item.rank is not None else 10**9, item.doc_id),
    )
    rest = sorted(
        (
            item
            for item in candidates.values()
            if "cited" not in item.how
            and "term" not in item.how
            and "retrieved" not in item.how
        ),
        key=lambda item: item.doc_id,
    )
    return cited + term + retrieved + rest


def truncate_candidates(
    ordered: list[Candidate], limit: int
) -> tuple[list[Candidate], bool]:
    """Обрезка до ``limit`` без выброса ``cited``; возвращает ``(оставшиеся, обрезано)``.

    ``cited`` не выкидывается никогда: адрес цитаты — часть определения ячейки, и его
    потеря сделала бы набор несравнимым с адресным семейством. Если ``cited`` сам
    превышает лимит, лимит превышается — это видно по флагу ``обрезано`` и в отчёте.
    """
    kept = [item for item in ordered if "cited" in item.how]
    rest = [item for item in ordered if "cited" not in item.how]
    room = max(0, limit - len(kept))
    return kept + rest[:room], len(rest) > room


# ---------------------------------------------------------------------------
# Фрагмент текста документа (окно вокруг термина)
# ---------------------------------------------------------------------------
def _snippet(text: str, terms: tuple[str, ...], width: int = SNIPPET_WIDTH) -> str:
    """Окно ``width`` символов вокруг первого совпадения термина, иначе начало текста.

    Регистр не важен (термины уже в нижнем), переводы строк схлопываются: колонка
    читается глазами, а не парсится.
    """
    collapsed = " ".join(text.split())
    if not collapsed:
        return ""
    lowered = collapsed.lower()
    position = -1
    for term in terms:
        found = lowered.find(term)
        if found != -1 and (position == -1 or found < position):
            position = found
    if position == -1:
        return collapsed[:width] + ("…" if len(collapsed) > width else "")
    start = max(0, position - width // 3)
    end = min(len(collapsed), start + width)
    prefix = "…" if start > 0 else ""
    suffix = "…" if end < len(collapsed) else ""
    return f"{prefix}{collapsed[start:end]}{suffix}"


def doc_snippets(
    conn: sqlite3.Connection,
    wanted: dict[str, tuple[str, ...]],
    width: int = SNIPPET_WIDTH,
) -> dict[str, str]:
    """``doc_id → snippet``: текст документа собирается потоком, по одному документу.

    Документы читаются пачками (SQLite ограничивает число параметров), внутри пачки —
    в порядке ``doc_id, chunk_no``, поэтому склейка воспроизводима.
    """
    out: dict[str, str] = {}
    ids = sorted(wanted)
    for start in range(0, len(ids), 500):
        batch = ids[start : start + 500]
        placeholders = ",".join("?" for _ in batch)
        current: str | None = None
        buffer: list[str] = []
        sql = (
            "SELECT doc_id, text FROM corpus_chunks"  # noqa: S608 — только '?'-плейсхолдеры
            f" WHERE doc_id IN ({placeholders}) ORDER BY doc_id, chunk_no"
        )
        for row in conn.execute(sql, batch):
            doc_id = str(row["doc_id"])
            if doc_id != current:
                if current is not None:
                    out[current] = _snippet(
                        " ".join(buffer), wanted.get(current, ()), width
                    )
                current = doc_id
                buffer = []
            buffer.append(str(row["text"] or ""))
        if current is not None:
            out[current] = _snippet(" ".join(buffer), wanted.get(current, ()), width)
    return out


def doc_text(conn: sqlite3.Connection, doc_id: str) -> str:
    """Полный текст документа (склейка чанков по ``chunk_no``): проверка цитаты судьи."""
    rows = conn.execute(
        "SELECT text FROM corpus_chunks WHERE doc_id = ? ORDER BY chunk_no", (doc_id,)
    )
    return " ".join(str(row["text"] or "") for row in rows)


# ---------------------------------------------------------------------------
# `retrieved`: тот же путь ретривала, что у прибора (импорт его проводки)
# ---------------------------------------------------------------------------
def retrieved_candidates(
    *,
    args: argparse.Namespace,
    db_path: Path,
    meta: RunMeta,
    params: dict[str, Any],
    entries: list[Any],
    depth: int,
) -> dict[str, list[tuple[int, str, str]]]:
    """``cell → [(rank, doc_id, profile)]`` из top-``depth`` выдачи объявленных профилей.

    Ранжирование — ``min`` по профилям: документ, найденный двумя профилями, получает
    лучший ранг. Это источник, **порождённый системой под тестом**, поэтому он вычитается
    в чувствительности прибора; в листе он помечен отдельным ``how=retrieved``.
    """
    import measure_retrieval_docs as harness

    index_root = Path(args.index_root).expanduser()
    if not meta.build_id:
        raise SystemExit(
            f"answer-sets: у прогона {meta.run_id!r} не записан index_build_id — "
            "ретривал не к чему привязать"
        )
    from core.pipeline.composition import read_composition

    package_id = meta.package_id
    view = read_composition(index_root, package_id, meta.build_id)
    if view is None:
        raise SystemExit(
            f"answer-sets: композиция {meta.build_id!r} не найдена под {index_root}: "
            "сборка индекса не разрешается (--include-retrieved требует индекса)"
        )
    from core.config.deploy import load_deploy_config

    deploy = load_deploy_config(args.deploy)
    profiles = [item.strip().upper() for item in args.profiles.split(",") if item.strip()]
    if not profiles:
        raise SystemExit("answer-sets: --profiles пуст")

    conn = _open_readonly(db_path)
    chunks_conn = _open_readonly(db_path)
    try:
        first_product = next(iter(view.members))
        dim = harness._partition_dim(
            index_root, package_id, view.members[first_product], first_product
        )
        embedder = None
        notices: list[str] = []
        if any(profile in ("R1", "R2") for profile in profiles):
            from core.cli.wiring import _resolve_embedder

            embedder = _resolve_embedder(
                deploy.embed.device,
                precision=deploy.embed.precision,
                model_id=deploy.embed.model_id,
                revision=deploy.embed.revision,
                notices=notices,
            )
            if embedder is None:
                raise SystemExit(
                    "answer-sets: dense-профиль запрошен, но embed.model_id не объявлен "
                    f"в {args.deploy} — вектор запроса нечем посчитать"
                )
        from core.adapters.lexical_tantivy import TantivyLexicalIndex
        from core.adapters.vector_npy import NpyVectorIndex
        from core.pipeline.evaluate import precompute_query_vectors, retrieve_for_cell
        from core.pipeline.retrieval_docs import document_ids_for

        lookup = harness._chunk_lookup(chunks_conn)
        lexical_index = None
        if any(profile in ("R0", "R2") for profile in profiles):
            lexical_index = TantivyLexicalIndex(
                profile=deploy.lexical.profile,
                hydrate=lambda ids: {
                    chunk_id: chunk
                    for chunk_id in ids
                    if (chunk := lookup(chunk_id)) is not None
                },
                view=view,
            )
            lexical_index.index(
                harness._iter_lexical_records(conn, meta.build_id), build_id=meta.build_id
            )
        vector_index = None
        if any(profile in ("R1", "R2") for profile in profiles):
            vector_index = NpyVectorIndex(
                index_root, package_id, meta.build_id, dim, lookup, view=view
            )
        chunk_doc = harness._chunk_doc_map(conn)
        query_vectors = None
        if embedder is not None:
            query_vectors = precompute_query_vectors(
                [params[entry.param_id] for entry in entries],
                profile="R1",
                embedder=embedder,
            )
        out: dict[str, list[tuple[int, str, str]]] = {}
        for entry in entries:
            best: dict[str, tuple[int, str]] = {}
            for profile in profiles:
                retrieval = retrieve_for_cell(
                    params[entry.param_id],
                    product_id=entry.product_id,
                    profile=profile,
                    cutoff_k=depth,
                    lexical_index=lexical_index,
                    vector_index=vector_index,
                    query_vectors=query_vectors,
                )
                docs = document_ids_for(
                    [chunk.chunk_id for chunk in retrieval.chunks], chunk_doc, top_k=depth
                )
                for rank, doc_id in enumerate(docs, start=1):
                    if doc_id not in best or rank < best[doc_id][0]:
                        best[doc_id] = (rank, profile)
            out[entry.cell] = sorted(
                ((rank, doc_id, profile) for doc_id, (rank, profile) in best.items()),
                key=lambda item: (item[0], item[1]),
            )
        return out
    finally:
        chunks_conn.close()
        conn.close()


# ---------------------------------------------------------------------------
# `llm`: судья по объявленной модели оценщика (кандидат, а не ответ)
# ---------------------------------------------------------------------------
class JudgeAnswer:
    """Строгий выход судьи: вердикт, дословная цитата, причина."""

    @staticmethod
    def model() -> type:
        """Pydantic-модель ответа (constrained decoding у адаптера разведки)."""
        from pydantic import BaseModel, ConfigDict, Field

        class AnswerJudgeOutput(BaseModel):
            model_config = ConfigDict(extra="forbid")

            answers: str = Field(pattern="^(yes|partial|no)$")
            quote: str = ""
            reason: str = ""

        return AnswerJudgeOutput


def resolve_judge(args: argparse.Namespace) -> tuple[str, str, str]:
    """Модель судьи: ``--llm-model`` > ``deploy.llm.<slot>.model_id`` > отказ.

    Резолв — тот же, что у фазы Ш6 (:func:`core.cli.wiring.resolve_evaluator_model_identity`):
    идентичность модели объявленный вход, а ``COHOUND_LLM_MODEL`` для неё не читается.
    Возвращает ``(model_id, decoder, revision)``.
    """
    from core.cli.wiring import (
        _resolve_llm_slot_and_config,
        resolve_evaluator_model_identity,
    )
    from core.config.deploy import load_deploy_config

    deploy = load_deploy_config(args.deploy)
    slot_name, slot_config = _resolve_llm_slot_and_config(deploy.llm, args.llm_slot)
    model_id, decoder = resolve_evaluator_model_identity(
        slot_config=slot_config,
        slot_name=slot_name,
        model_id=args.llm_model,
        decoder=args.llm_decoder,
    )
    return model_id, decoder, (args.llm_revision or "")


_JUDGE_PROMPT = (
    "Ты — судья эталона поиска. Ниже параметр и документ.\n"
    "Вопрос: содержит ли документ утверждение, которым параметр РЕШАЕТСЯ для продукта?\n"
    "«Упоминает тему» и «похоже» — это no. Утверждение слабее вопроса — partial.\n"
    "Ответ — строгий JSON: {{\"answers\": \"yes|partial|no\", \"quote\": \"...\", "
    "\"reason\": \"...\"}}. quote — дословная подстрока текста документа, иначе ответ "
    "не принимается.\n\nПараметр: {title}\nПравило: {rule}\n\nДокумент:\n{text}"
)


def apply_judge(item: Candidate, *, verdict: str, quote: str, text: str) -> None:
    """Применяет вердикт судьи к кандидату: цитата вне текста — суждение отвергнуто.

    Вердикт судьи — **кандидат**, а не ответ: он попадает в ``judge_verdict``/
    ``judge_quote`` (человек читает), а ``llm`` в ``how`` добавляется только при
    выполненной дисциплине I22 — цитата обязана быть подстрокой текста документа.
    Иначе суждение не доказательство, и это видно машинным токеном
    ``rejected_by_quote_check``, а не тишиной.
    """
    item.judge_verdict = verdict
    item.judge_quote = quote
    if verdict not in {"yes", "partial"}:
        return
    if quote and quote in text:
        item.add_how("llm")
        return
    item.judge_verdict = "rejected_by_quote_check"
    if "llm" in item.how:
        item.how.remove("llm")


def judge_document(
    port: Any, *, model_id: str, title: str, rule: str, text: str
) -> tuple[str, str, str]:
    """Один вызов судьи: ``(verdict, quote, reason)``; невалидный ответ — ``error``."""
    from core.ports.llm import LLMRequest

    schema = JudgeAnswer.model()
    prompt = _JUDGE_PROMPT.format(title=title, rule=rule, text=text[:12000])
    try:
        response = port.complete(
            LLMRequest(prompt=prompt, response_schema=schema, model_id=model_id)
        )
    except Exception as exc:  # noqa: BLE001 — сбой судьи виден как вердикт, а не как тишина
        return "error", "", str(exc)
    output = response.output
    return (
        str(getattr(output, "answers", "")),
        str(getattr(output, "quote", "")),
        str(getattr(output, "reason", "")),
    )


# ---------------------------------------------------------------------------
# XLSX: рабочий лист разметчика
# ---------------------------------------------------------------------------
def _openpyxl() -> Any:
    """``openpyxl`` с внятным отказом (XLSX — формат листа, а не зависимость прибора)."""
    try:
        import openpyxl
    except ImportError as error:  # pragma: no cover - зависит от окружения
        raise SystemExit(
            "XLSX требует openpyxl (extra `extract`: uv sync --extra extract)"
        ) from error
    return openpyxl


def _column_letter(index: int) -> str:
    """1 → ``A``, 27 → ``AA``."""
    letters = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters = chr(ord("A") + remainder) + letters
    return letters


_GUIDE_LINES: tuple[str, ...] = (
    "Размечается ДВЕ колонки: answers и comment. answers — выпадающий список "
    "yes / partial / no; пусто = ещё не рассмотрено. comment — ваши пометки.",
    "",
    "Размечается ДОСТАТОЧНОСТЬ ОТВЕТА, а не релевантность страницы. Вопрос по строке: "
    "содержит ли ЭТОТ документ утверждение, которым параметр РЕШАЕТСЯ для этого продукта?",
    "",
    "yes — документ содержит такое утверждение (даже если формулировка другая).",
    "partial — документ говорит о параметре, но утверждение слабее вопроса: часть "
    "условия, другая редакция, соседняя возможность. В набор по умолчанию НЕ входит "
    "(входит только если компиляция запущена с --include-partial, и это записано в файле).",
    "no — документ о другом либо утверждение слабее вопроса.",
    "",
    "unclear НЕ входит в набор значений намеренно: «не хватило фрагмента» — повод открыть "
    "документ (doc_url), а не класс ответа. Класс ответа, который означает «я не смотрел», "
    "превратил бы эталон в разметку похожести.",
    "",
    "Колонка how — откуда взялся кандидат: cited (адрес цитаты из документного эталона; "
    "такие строки залиты цветом и подтверждаются обязательно), term (в документе "
    "встречаются термины параметра — колонка why называет их), retrieved (документ из "
    "выдачи системы под тестом), llm (судья счёл, что отвечает — см. judge_verdict).",
    "",
    "Почему cited подтверждается отдельно: адрес цитаты по определению отвечает, но замер "
    "O15 показал, что цитата часто взята из ДРУГОГО документа. Если вы поставите no "
    "документу с how=cited, компиляция откажет — это ошибка разметки адресного эталона "
    "(O14), и её надо увидеть, а не замолчать.",
    "",
    "snippet — окно текста вокруг первого совпадения термина (или начало документа). Это "
    "подсказка: решение принимается по документу (doc_url), а не по окну.",
    "",
    "Дальше одна команда: prepare_answer_sets.py compile --xlsx <этот файл> --out "
    "<retrieval_golden_answers.json>. Она сама проверит инварианты формата.",
)


def write_candidates_xlsx(
    path: Path,
    rows: list[dict[str, str]],
    *,
    policy_rows: list[tuple[str, Any]],
) -> None:
    """Пишет лист кандидатов, инструкцию и машинные условия.

    Что делает файл рабочим, а не «CSV в обёртке»: закреплённая шапка с фильтром (сотни
    строк иначе листаются вслепую); выпадающий список в ``answers`` (опечатка не проходит,
    а ``compile`` отказывает именно на неизвестном значении); заливка строк ``how=cited``
    (их человек обязан подтвердить); перенос по словам в текстовых колонках; второй лист —
    инструкция; третий — условия, с которыми лист собран.
    """
    openpyxl = _openpyxl()
    from openpyxl.styles import Alignment, Font, PatternFill  # noqa: PLC0415
    from openpyxl.worksheet.datavalidation import DataValidation  # noqa: PLC0415

    book = openpyxl.Workbook()  # type: ignore[attr-defined]
    sheet = book.active
    sheet.title = SHEET_CANDIDATES
    sheet.append(list(CANDIDATE_COLUMNS))
    for cell in sheet[1]:
        cell.font = Font(bold=True)
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = f"A1:{_column_letter(len(CANDIDATE_COLUMNS))}1"

    widths = {
        "cell": 46,
        "product_id": 18,
        "param_id": 40,
        "param_title": 34,
        "param_rule": 34,
        "doc_id": 34,
        "doc_url": 50,
        "doc_title": 30,
        "how": 14,
        "why": 30,
        "snippet": 70,
        "judge_verdict": 16,
        "judge_quote": 40,
        "answers": 10,
        "comment": 30,
    }
    for index, name in enumerate(CANDIDATE_COLUMNS, start=1):
        sheet.column_dimensions[_column_letter(index)].width = widths.get(name, 16)
    wrap = {"param_title", "param_rule", "doc_id", "doc_url", "why", "snippet",
            "judge_quote", "comment"}

    cited_fill = PatternFill("solid", start_color="FFF2CC")
    for row_index, row in enumerate(rows, start=2):
        sheet.append([row.get(name, "") for name in CANDIDATE_COLUMNS])
        for index, name in enumerate(CANDIDATE_COLUMNS, start=1):
            if name in wrap:
                sheet.cell(row=row_index, column=index).alignment = Alignment(
                    wrap_text=True, vertical="top"
                )
        if "cited" in (row.get("how") or "").split(","):
            for index in range(1, len(CANDIDATE_COLUMNS) + 1):
                sheet.cell(row=row_index, column=index).fill = cited_fill

    answers_index = CANDIDATE_COLUMNS.index("answers") + 1
    validation = DataValidation(
        type="list",
        formula1=f'"{",".join(ANSWERS)}"',
        allow_blank=True,
        showDropDown=False,
    )
    validation.error = "ответ — yes, partial или no; пусто = ещё не рассмотрено"
    validation.errorTitle = "Неизвестный ответ"
    sheet.add_data_validation(validation)
    last = max(2, len(rows) + 1)
    letter = _column_letter(answers_index)
    validation.add(f"{letter}2:{letter}{last}")

    guide = book.create_sheet(SHEET_GUIDE)
    for line in _GUIDE_LINES:
        guide.append([line])
    guide.column_dimensions["A"].width = 110
    for cell in guide["A"]:
        cell.alignment = Alignment(wrap_text=True, vertical="top")

    policy_sheet = book.create_sheet(SHEET_POLICY)
    policy_sheet.append(["key", "value"])
    for key, value in policy_rows:
        policy_sheet.append([key, value])
    policy_sheet.column_dimensions["A"].width = 28
    policy_sheet.column_dimensions["B"].width = 60

    path.parent.mkdir(parents=True, exist_ok=True)
    book.save(path)


def read_sheet(path: Path, name: str) -> list[dict[str, str]]:
    """Лист XLSX → строки-словари (первая строка — шапка, значения приводятся к тексту).

    Числа и пустые ячейки становятся строками: ``compile`` сравнивает ``answers`` со
    множеством значений и не должен зависеть от того, как Excel сохранил ячейку.
    """
    openpyxl = _openpyxl()
    book = openpyxl.load_workbook(path, read_only=True, data_only=True)  # type: ignore[attr-defined]
    try:
        if name not in book.sheetnames:
            raise SystemExit(f"{path}: нет листа {name!r} (есть: {book.sheetnames})")
        sheet = book[name]
        rows = sheet.iter_rows(values_only=True)
        try:
            header = [("" if value is None else str(value)).strip() for value in next(rows)]
        except StopIteration:
            raise SystemExit(f"{path}: лист {name!r} пуст") from None
        out: list[dict[str, str]] = []
        for values in rows:
            if not any(value not in (None, "") for value in values):
                continue
            row = {column: "" for column in header if column}
            for index, column in enumerate(header):
                if not column or index >= len(values):
                    continue
                value = values[index]
                row[column] = "" if value is None else str(value).strip()
            out.append(row)
        return out
    finally:
        book.close()


def read_policy_sheet(path: Path) -> dict[str, str]:
    """Лист ``policy`` → словарь; отсутствие листа — пустой словарь (лист собран вручную)."""
    openpyxl = _openpyxl()
    book = openpyxl.load_workbook(path, read_only=True, data_only=True)  # type: ignore[attr-defined]
    try:
        if SHEET_POLICY not in book.sheetnames:
            return {}
        out: dict[str, str] = {}
        for key, value in book[SHEET_POLICY].iter_rows(values_only=True):
            if key in (None, "", "key"):
                continue
            out[str(key).strip()] = "" if value is None else str(value).strip()
        return out
    finally:
        book.close()


# ---------------------------------------------------------------------------
# candidates
# ---------------------------------------------------------------------------
def cmd_candidates(args: argparse.Namespace) -> int:
    """Собирает кандидатов и пишет лист разметчика."""
    db_path = Path(args.db).expanduser()
    if not db_path.is_file():
        print(f"answer-sets: БД не найдена: {db_path}", file=sys.stderr)
        return 2

    from core.pipeline.retrieval_docs import load_doc_golden

    case_id, golden_run_id, entries = load_doc_golden(args.golden)
    judge_spec: JudgeSpec | None = None
    conn = _open_readonly(db_path)
    try:
        meta = read_run_meta(conn, args.run_id)
        if golden_run_id and golden_run_id != meta.run_id:
            print(
                f"answer-sets: документный эталон собран на прогоне {golden_run_id!r}, а "
                f"читается {meta.run_id!r}: doc_id разных корпусов несравнимы — нужен "
                "эталон, собранный на этом прогоне",
                file=sys.stderr,
            )
            return 2
        package_id = meta.package_id or case_id
        params = load_params(
            db_path,
            package_id,
            Path(args.package).expanduser() if args.package else None,
        )
        missing = [entry.cell for entry in entries if entry.param_id not in params]
        if missing:
            print(
                f"answer-sets: {len(missing)} ячеек эталона ссылаются на param_id, которых "
                f"нет в каталоге прогона (первые: {', '.join(missing[:5])})",
                file=sys.stderr,
            )
            return 2
        if args.limit_cells > 0:
            entries = entries[: args.limit_cells]

        cutoff_k = args.cutoff_k or meta.cutoff_k or 10
        depth = args.retrieved_depth
        if args.include_retrieved and depth <= 0:
            print(
                "answer-sets: --include-retrieved требует --retrieved-depth > 0 "
                "(глубина выдачи — условие источника, а не догадка)",
                file=sys.stderr,
            )
            return 2
        if not args.include_retrieved:
            depth = 0

        docs = corpus_documents(conn)
        terms_by_cell: dict[str, tuple[str, ...]] = {
            entry.cell: param_terms(params[entry.param_id].title, args.term_min_length)
            for entry in entries
        }
        needed: dict[str, set[str]] = defaultdict(set)
        for entry in entries:
            needed[entry.product_id].update(terms_by_cell[entry.cell])
        index = term_index(conn, needed, args.term_min_length)

        retrieved: dict[str, list[tuple[int, str, str]]] = {}
        if args.include_retrieved:
            # Отказы проводки ретривала (нет сборки/композиции/эмбеддера) — код 2 с
            # причиной, а не исключение посреди листа: лист собран не будет, и это
            # видно по коду возврата.
            try:
                retrieved = retrieved_candidates(
                    args=args,
                    db_path=db_path,
                    meta=meta,
                    params=params,
                    entries=entries,
                    depth=depth,
                )
            except SystemExit as exc:
                print(str(exc), file=sys.stderr)
                return 2

        candidates: dict[str, dict[str, Candidate]] = {}
        for entry in entries:
            cell_candidates: dict[str, Candidate] = {}
            # 1. cited — адрес эталона; документ может не разрешаться в корпусе, и это
            #    факт о покрытии, а не повод выбросить строку.
            for doc_id in entry.expected_doc_ids:
                item = _candidate_for(cell_candidates, doc_id)
                item.add_how("cited")
                item.why = (
                    "адрес эталона" if doc_id in docs else "адрес эталона; нет в корпусе"
                )
            # 2. term — документы того же продукта с различимыми терминами параметра.
            doc_terms: dict[str, list[str]] = defaultdict(list)
            for term in terms_by_cell[entry.cell]:
                for doc_id in index.get((entry.product_id, term), ()):
                    doc_terms[doc_id].append(term)
            for doc_id, found in doc_terms.items():
                if len(found) < args.term_min_distinct:
                    continue
                item = _candidate_for(cell_candidates, doc_id)
                item.add_how("term")
                item.term_count = len(found)
                why = "термины: " + ", ".join(found)
                item.why = "; ".join(part for part in (item.why, why) if part)
            # 3. retrieved — источник, порождённый системой под тестом (вычитается).
            for rank, doc_id, profile in retrieved.get(entry.cell, ()):
                item = _candidate_for(cell_candidates, doc_id)
                item.add_how("retrieved")
                item.rank = rank
                why = f"rank={rank}@{profile}"
                item.why = "; ".join(part for part in (item.why, why) if part)
            candidates[entry.cell] = cell_candidates

        # 4. llm — судья по объявленной модели оценщика; вердикт — кандидат, не ответ.
        if args.llm:
            from core.cli.errors import CliError

            try:
                model_id, _decoder, revision = resolve_judge(args)
            except CliError as exc:
                print(f"answer-sets: {exc}", file=sys.stderr)
                return 2
            from core.cli.wiring import build_discovery_llm
            from core.config.deploy import load_deploy_config

            port = build_discovery_llm(
                model_id=model_id,
                deploy_llm=load_deploy_config(args.deploy).llm,
                slot=args.llm_slot,
            )
            if not getattr(port, "available", False):
                print(
                    "answer-sets: судья недоступен (нет endpoint/ключа) — --llm требует "
                    "рабочего LLM-слота",
                    file=sys.stderr,
                )
                return 2
            judge_spec = JudgeSpec(
                model_id=model_id,
                revision=revision,
                prompt_hash="",
            )
            # Судья оценивает уже собранных кандидатов: он не выдумывает новых документов
            # (иначе «отвечает ли документ» стало бы поиском, а не суждением).
            judged = 0
            for entry in entries:
                param = params[entry.param_id]
                for doc_id in sorted(candidates[entry.cell]):
                    text = doc_text(conn, doc_id)
                    verdict, quote, _reason = judge_document(
                        port,
                        model_id=model_id,
                        title=param.title,
                        rule=param.rule,
                        text=text,
                    )
                    judged += 1
                    apply_judge(
                        candidates[entry.cell][doc_id],
                        verdict=verdict,
                        quote=quote,
                        text=text,
                    )
            print(f"судья: оценено пар {judged}, модель {model_id}")

        # Обрезка и сборка строк.
        rows: list[dict[str, str]] = []
        truncated_cells: list[str] = []
        snippet_wanted: dict[str, tuple[str, ...]] = {}
        per_cell: dict[str, list[Candidate]] = {}
        for entry in entries:
            ordered = order_candidates(candidates[entry.cell])
            kept, truncated = truncate_candidates(ordered, args.max_candidates_per_cell)
            per_cell[entry.cell] = kept
            if truncated:
                truncated_cells.append(entry.cell)
            for item in kept:
                terms = terms_by_cell[entry.cell] if "term" in item.how else ()
                if terms:
                    previous = snippet_wanted.get(item.doc_id, ())
                    snippet_wanted[item.doc_id] = tuple(
                        dict.fromkeys((*previous, *terms))
                    )
                else:
                    snippet_wanted.setdefault(item.doc_id, ())

        snippets = doc_snippets(conn, snippet_wanted)
        for entry in entries:
            param = params[entry.param_id]
            for item in per_cell[entry.cell]:
                doc_url = docs.get(item.doc_id, ("", ""))[1]
                rows.append(
                    {
                        "cell": entry.cell,
                        "product_id": entry.product_id,
                        "param_id": entry.param_id,
                        "param_title": param.title,
                        "param_rule": param.rule,
                        "doc_id": item.doc_id,
                        "doc_url": doc_url,
                        "doc_title": "",
                        "how": ",".join(
                            token for token in ANSWER_HOW if token in item.how
                        ),
                        "why": item.why,
                        "snippet": snippets.get(item.doc_id, ""),
                        "judge_verdict": item.judge_verdict,
                        "judge_quote": item.judge_quote,
                        "answers": "",
                        "comment": "",
                    }
                )
    finally:
        conn.close()

    rows.sort(key=lambda row: (row["cell"], row["doc_id"]))
    cells_without = [
        entry.cell for entry in entries if not candidates.get(entry.cell)
    ]
    cited_missing = sum(
        1
        for entry in entries
        for doc_id in entry.expected_doc_ids
        if doc_id not in docs
    )
    policy_rows: list[tuple[str, Any]] = [
        ("case_id", case_id),
        ("run_id", meta.run_id),
        ("cutoff_k", cutoff_k),
        ("term_min_length", args.term_min_length),
        ("term_min_distinct", args.term_min_distinct),
        ("retrieved_depth", depth),
        ("max_candidates_per_cell", args.max_candidates_per_cell),
        ("include_partial", "false"),
        ("cells_truncated", len(truncated_cells)),
        ("cells_without_candidates", len(cells_without)),
        ("rows_total", len(rows)),
    ]
    if judge_spec is not None:
        policy_rows.extend(
            [
                ("judge_model_id", judge_spec.model_id),
                ("judge_revision", judge_spec.revision),
                ("judge_prompt_hash", judge_spec.prompt_hash),
            ]
        )
    out = Path(args.out).expanduser()
    write_candidates_xlsx(out, rows, policy_rows=policy_rows)

    how_counts: Counter[str] = Counter()
    for row in rows:
        for token in row["how"].split(","):
            if token:
                how_counts[token] += 1
    print(
        f"кандидатов: {len(rows)} строк, ячеек эталона {len(entries)}, "
        f"документов корпуса {len(docs)}"
    )
    print(
        "происхождение: "
        + ", ".join(f"{token} {how_counts.get(token, 0)}" for token in ANSWER_HOW)
    )
    print(
        f"cited не разрешилось в корпусе: {cited_missing} "
        f"(строки остаются, doc_url пуст)"
    )
    print(f"ячеек без единого кандидата: {len(cells_without)}")
    print(
        f"ячеек, упёршихся в лимит {args.max_candidates_per_cell}: "
        f"{len(truncated_cells)}"
    )
    print(f"written: {out}")
    return 0


# ---------------------------------------------------------------------------
# compile
# ---------------------------------------------------------------------------
def _row_how(row: dict[str, str]) -> list[str]:
    """Токены ``how`` строки листа в каноническом порядке; неизвестный — отказ."""
    raw = [token.strip() for token in (row.get("how") or "").split(",") if token.strip()]
    unknown = [token for token in raw if token not in ANSWER_HOW]
    if unknown:
        raise ValueError(
            f"неизвестные источники how {unknown} (допустимо: {', '.join(ANSWER_HOW)})"
        )
    return [token for token in ANSWER_HOW if token in raw]


def _policy_from(
    args: argparse.Namespace, sheet: dict[str, str]
) -> AnswerPolicy:
    """Политика компиляции: флаг > лист ``policy`` > дефолт (и это названо в файле)."""
    def pick(name: str, default: int) -> int:
        explicit = getattr(args, name, None)
        if explicit is not None:
            return int(explicit)
        raw = sheet.get(name, "")
        if raw:
            try:
                return int(raw)
            except ValueError as exc:
                raise SystemExit(
                    f"answer-sets: лист policy: {name}={raw!r} — не целое число"
                ) from exc
        return default

    return AnswerPolicy(
        term_min_length=pick("term_min_length", 4),
        term_min_distinct=pick("term_min_distinct", 2),
        retrieved_depth=pick("retrieved_depth", 0),
        max_candidates_per_cell=pick("max_candidates_per_cell", 20),
        include_partial=bool(args.include_partial),
    )


def cmd_compile(args: argparse.Namespace) -> int:
    """Заполненный XLSX → ``retrieval_golden_answers.json``."""
    path = Path(args.xlsx).expanduser()
    if not path.is_file():
        print(f"answer-sets: листа нет: {path}", file=sys.stderr)
        return 2
    rows = read_sheet(path, SHEET_CANDIDATES)
    sheet_policy = read_policy_sheet(path)
    run_id = args.run_id or sheet_policy.get("run_id", "")
    case_id = args.case_id or sheet_policy.get("case_id", "")
    if not run_id or not case_id:
        print(
            "answer-sets: набор не называет run_id/case_id: передайте --run-id/--case-id "
            "(в листе нет листа policy)",
            file=sys.stderr,
        )
        return 2
    cutoff_k = args.cutoff_k or int(sheet_policy.get("cutoff_k") or 0)
    if cutoff_k <= 0:
        print(
            "answer-sets: cutoff_k не объявлен (ни --cutoff-k, ни лист policy): набор "
            "объявляется при конкретном K (I20)",
            file=sys.stderr,
        )
        return 2
    policy = _policy_from(args, sheet_policy)
    status = "reviewed" if args.status == "reviewed" else "candidate"
    if status == "reviewed" and not (args.actor or "").strip():
        print(
            "answer-sets: --status reviewed требует --actor: подтверждение набора — "
            "осознанное действие человека, а не дефолт",
            file=sys.stderr,
        )
        return 2

    problems: list[str] = []
    seen: dict[tuple[str, str], int] = {}
    by_cell: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for lineno, row in enumerate(rows, start=2):
        cell = (row.get("cell") or "").strip()
        doc_id = (row.get("doc_id") or "").strip()
        answers = (row.get("answers") or "").strip()
        if answers and answers not in ANSWERS:
            problems.append(
                f"строка {lineno} ({cell}): answers={answers!r}, допустимо "
                f"{list(ANSWERS)} или пусто"
            )
            continue
        if not cell or not doc_id:
            problems.append(f"строка {lineno}: пустой cell или doc_id")
            continue
        if cell != f"{row.get('product_id', '')}/{row.get('param_id', '')}":
            problems.append(
                f"строка {lineno}: cell {cell!r} не совпадает с "
                f"{row.get('product_id')}/{row.get('param_id')}"
            )
            continue
        if (cell, doc_id) in seen:
            problems.append(
                f"строка {lineno}: дубль (cell, doc_id) = ({cell}, {doc_id}), "
                f"первая — строка {seen[(cell, doc_id)]}"
            )
            continue
        seen[(cell, doc_id)] = lineno
        try:
            how = _row_how(row)
        except ValueError as exc:
            problems.append(f"строка {lineno} ({cell}/{doc_id}): {exc}")
            continue
        if not how:
            problems.append(f"строка {lineno} ({cell}/{doc_id}): пустой how")
            continue
        if answers == "no" and "cited" in how:
            problems.append(
                f"строка {lineno}: ячейка {cell}, документ {doc_id} помечен как cited "
                "(адрес эталона), а человек поставил no — это ошибка разметки O14 "
                "(адрес цитаты по определению отвечает), её надо увидеть"
            )
            continue
        by_cell[cell][doc_id] = {
            "product_id": row.get("product_id", ""),
            "param_id": row.get("param_id", ""),
            "how": how,
            "answers": answers,
            "judge_quote": (row.get("judge_quote") or "").strip(),
        }

    if problems:
        for problem in problems:
            print(f"ОТКАЗ: {problem}", file=sys.stderr)
        return 2

    entries: list[AnswerEntry] = []
    cells_without_answer = 0
    empty_cells: list[str] = []
    needs_judge = False
    for cell in sorted(by_cell):
        docs = by_cell[cell]
        human_accepted = [
            doc_id
            for doc_id, item in docs.items()
            if item["answers"] == "yes"
            or (policy.include_partial and item["answers"] == "partial")
        ]
        # Документы ``cited`` входят в набор по определению: это место, откуда взято
        # утверждение ячейки (человек их подтверждает, но не «допускает»).
        accepted = human_accepted + [
            doc_id for doc_id in docs if doc_id not in human_accepted and "cited" in docs[doc_id]["how"]
        ]
        if not human_accepted:
            # Ячейка без единого yes не выпадает: её набор — cited-документы, и она
            # считается отдельно (``cells_without_answer``), а не исчезает из эталона.
            cells_without_answer += 1
        if not accepted:
            empty_cells.append(cell)
            continue
        answer_docs: list[AnswerDoc] = []
        for doc_id in sorted(accepted):
            item = docs[doc_id]
            how = tuple(token for token in ANSWER_HOW if token in item["how"])
            quote = item["judge_quote"]
            if ("llm" in how or "human" in how) and not quote:
                problems.append(
                    f"ячейка {cell}, документ {doc_id}: how содержит "
                    f"{'llm' if 'llm' in how else 'human'}, а judge_quote пуст — "
                    "суждение без цитаты не доказательство (I22)"
                )
                continue
            if "llm" in how:
                needs_judge = True
            answer_docs.append(AnswerDoc(doc_id=doc_id, how=how, quote=quote))
        cited = sorted(
            doc_id for doc_id, item in docs.items() if "cited" in item["how"]
        )
        if not answer_docs:
            empty_cells.append(cell)
            continue
        entries.append(
            AnswerEntry(
                cell=cell,
                product_id=docs[accepted[0]]["product_id"],
                param_id=docs[accepted[0]]["param_id"],
                cited_doc_ids=tuple(cited),
                answer_docs=tuple(answer_docs),
            )
        )
    if problems:
        for problem in problems:
            print(f"ОТКАЗ: {problem}", file=sys.stderr)
        return 2
    if empty_cells:
        print(
            f"ОТКАЗ: {len(empty_cells)} ячеек не дают ни одного члена набора "
            f"(ни yes, ни cited): {', '.join(empty_cells[:5])}",
            file=sys.stderr,
        )
        return 2
    if not entries:
        print(
            "ОТКАЗ: ни одна ячейка не даёт набора — лист не размечен (пустой лист "
            "не превращается в пустой эталон)",
            file=sys.stderr,
        )
        return 2

    judge: JudgeSpec | None = None
    if needs_judge:
        model_id = args.llm_model or sheet_policy.get("judge_model_id", "")
        if not model_id:
            print(
                "ОТКАЗ: в наборе есть документы с how=llm, но модель судьи неизвестна "
                "(нет ни --llm-model, ни judge_model_id в листе policy) — провенанс "
                "судьи потерян",
                file=sys.stderr,
            )
            return 2
        judge = JudgeSpec(
            model_id=model_id,
            revision=args.llm_revision or sheet_policy.get("judge_revision", ""),
            prompt_hash=sheet_policy.get("judge_prompt_hash", ""),
        )

    sets = AnswerSets(
        case_id=case_id,
        run_id=run_id,
        cutoff_k=cutoff_k,
        judge=judge,
        policy=policy,
        status=status,
        entries=tuple(entries),
        reviewed_by=(args.actor or None) if status == "reviewed" else None,
    )
    out = Path(args.out).expanduser()
    write_answer_sets(out, sets)
    # Компиляция без проверки бессмысленна: сразу читаем записанное тем же загрузчиком,
    # которым пользуется прибор — иначе правила проверялись бы только тестами.
    from core.pipeline.retrieval_answers import load_answer_sets

    loaded = load_answer_sets(out)
    summary = answers_summary(loaded.entries)
    print(
        f"written: {out} ({len(loaded.entries)} ячеек, {summary['documents']} документов, "
        f"status={loaded.status})"
    )
    print(
        "происхождение: "
        + ", ".join(f"{token} {summary['how'][token]}" for token in ANSWER_HOW)
    )
    print(f"ячеек без единого yes (набор = cited): {cells_without_answer}")
    if loaded.status == "reviewed":
        print(f"набор подтверждён: {loaded.reviewed_by}")
    return 0


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------
def cmd_validate(args: argparse.Namespace) -> int:
    """Проверяет лист и/или собранный JSON."""
    problems: list[str] = []
    rows: list[dict[str, str]] = []
    if args.xlsx:
        path = Path(args.xlsx).expanduser()
        if not path.is_file():
            print(f"answer-sets: листа нет: {path}", file=sys.stderr)
            return 2
        rows = read_sheet(path, SHEET_CANDIDATES)
        seen: dict[tuple[str, str], int] = {}
        for lineno, row in enumerate(rows, start=2):
            answers = (row.get("answers") or "").strip()
            if answers and answers not in ANSWERS:
                problems.append(
                    f"строка {lineno} ({row.get('cell')}): answers={answers!r}, "
                    f"допустимо {list(ANSWERS)} или пусто"
                )
            key = (row.get("cell", ""), row.get("doc_id", ""))
            if key in seen:
                problems.append(
                    f"строка {lineno}: дубль (cell, doc_id) = {key}, "
                    f"первая — строка {seen[key]}"
                )
            seen[key] = lineno
            if not key[0] or not key[1]:
                problems.append(f"строка {lineno}: пустой cell или doc_id")
        if args.golden:
            from core.pipeline.retrieval_docs import load_doc_golden

            _case, _run, entries = load_doc_golden(args.golden)
            in_sheet = {row.get("cell", "") for row in rows}
            absent = sorted(entry.cell for entry in entries if entry.cell not in in_sheet)
            extra = sorted(in_sheet - {entry.cell for entry in entries})
            if absent:
                print(
                    f"ВНИМАНИЕ: {len(absent)} ячеек эталона без единого кандидата "
                    f"(первые: {', '.join(absent[:5])})",
                    file=sys.stderr,
                )
            if extra:
                problems.append(
                    f"{len(extra)} ячеек листа отсутствуют в документном эталоне "
                    f"(первые: {', '.join(extra[:5])})"
                )
        print(f"лист: {len(rows)} строк, ячеек {len({row.get('cell') for row in rows})}")
    if args.json:
        path = Path(args.json).expanduser()
        if not path.is_file():
            print(f"answer-sets: набора нет: {path}", file=sys.stderr)
            return 2
        from core.pipeline.retrieval_answers import load_answer_sets

        try:
            sets = load_answer_sets(path)
        except ValueError as exc:
            problems.append(str(exc))
        else:
            summary = answers_summary(sets.entries)
            print(
                f"набор: {summary['cells']} ячеек, {summary['documents']} документов, "
                f"status={sets.status}, run_id={sets.run_id}, K={sets.cutoff_k}, "
                f"judge={'нет' if sets.judge is None else sets.judge.model_id}"
            )
    if problems:
        for problem in problems:
            print(f"ОТКАЗ: {problem}", file=sys.stderr)
        return 2
    if not args.xlsx and not args.json:
        print("answer-sets: validate требует --xlsx и/или --json", file=sys.stderr)
        return 2
    print("OK")
    return 0


# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------
def cmd_summary(args: argparse.Namespace) -> int:
    """Сводка по листу (и по набору, если дан ``--json``)."""
    path = Path(args.xlsx).expanduser()
    if not path.is_file():
        print(f"answer-sets: листа нет: {path}", file=sys.stderr)
        return 2
    rows = read_sheet(path, SHEET_CANDIDATES)
    policy = read_policy_sheet(path)
    answers = Counter((row.get("answers") or "") or "пусто" for row in rows)
    how: Counter[str] = Counter()
    per_cell: Counter[str] = Counter()
    cited_rejected: list[str] = []
    for row in rows:
        per_cell[row.get("cell", "")] += 1
        tokens = [token for token in (row.get("how") or "").split(",") if token]
        for token in tokens:
            how[token] += 1
        if (row.get("answers") or "").strip() == "no" and "cited" in tokens:
            cited_rejected.append(f"{row.get('cell')}/{row.get('doc_id')}")
    sizes = sorted(per_cell.values())
    print(f"строк: {len(rows)}, ячеек: {len(per_cell)}")
    print("ответы: " + ", ".join(f"{key} {answers[key]}" for key in (*ANSWERS, "пусто")))
    print("происхождение: " + ", ".join(f"{token} {how.get(token, 0)}" for token in ANSWER_HOW))
    print(
        "документов на ячейку: "
        f"min {sizes[0] if sizes else 0}, median {_median(sizes)}, max {sizes[-1] if sizes else 0}"
    )
    print(f"ячеек с одним кандидатом: {sum(1 for size in sizes if size == 1)}")
    print(f"ячеек, упёршихся в лимит: {policy.get('cells_truncated', '—')}")
    print(f"ячеек без кандидатов: {policy.get('cells_without_candidates', '—')}")
    print(f"cited-документов отвергнуто человеком (ошибки разметки O14): {len(cited_rejected)}")
    for item in cited_rejected[:10]:
        print(f"  {item}")
    if args.json:
        from core.pipeline.retrieval_answers import load_answer_sets

        sets = load_answer_sets(args.json)
        summary = answers_summary(sets.entries)
        print("")
        print(f"набор: {args.json} (status={sets.status}, K={sets.cutoff_k})")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def _median(values: list[int]) -> float:
    """Медиана без statistics-зависимости от типа (список уже отсортирован)."""
    if not values:
        return 0.0
    middle = len(values) // 2
    if len(values) % 2:
        return float(values[middle])
    return (values[middle - 1] + values[middle]) / 2


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    candidates = sub.add_parser(
        "candidates", help="кандидаты по ячейкам эталона → XLSX для человека"
    )
    candidates.add_argument("--db", required=True, help="cohound.db прогона (только чтение)")
    candidates.add_argument("--golden", required=True, help="retrieval_golden_docs.json")
    candidates.add_argument("--out", required=True, help="XLSX на разметку")
    candidates.add_argument("--package", default=None, help="каталог пакета (фолбэк каталога)")
    candidates.add_argument("--run-id", default=None, help="run_id (если в файле их несколько)")
    candidates.add_argument("--cutoff-k", type=int, default=0, help="0 — из runs.retrieval_cutoff_K")
    candidates.add_argument("--limit-cells", type=int, default=0, help="первые N ячеек (0 — все)")
    candidates.add_argument("--term-min-length", type=int, default=4)
    candidates.add_argument("--term-min-distinct", type=int, default=2)
    candidates.add_argument(
        "--max-candidates-per-cell", type=int, default=20,
        help="потолок кандидатов на ячейку; cited не выкидывается",
    )
    candidates.add_argument(
        "--include-retrieved", action="store_true",
        help="добавить источник retrieved (требует индекса и --retrieved-depth > 0)",
    )
    candidates.add_argument("--retrieved-depth", type=int, default=0)
    candidates.add_argument("--profiles", default="R0", help="профили ретривала для retrieved")
    candidates.add_argument("--index-root", default="index_store")
    candidates.add_argument("--deploy", default="config/deploy.yaml")
    candidates.add_argument("--llm", action="store_true", help="судья-LLM (кандидат, не ответ)")
    candidates.add_argument("--llm-model", default=None, help="модель судьи (объявленный вход)")
    candidates.add_argument("--llm-decoder", default=None)
    candidates.add_argument("--llm-revision", default=None, help="ревизия весов модели судьи")
    candidates.add_argument("--llm-slot", default=None, help="слот deploy.llm")
    candidates.set_defaults(func=cmd_candidates)

    compile_ = sub.add_parser("compile", help="заполненный XLSX → JSON-эталон")
    compile_.add_argument("--xlsx", required=True)
    compile_.add_argument("--out", required=True)
    compile_.add_argument("--run-id", default=None)
    compile_.add_argument("--case-id", default=None)
    compile_.add_argument("--cutoff-k", type=int, default=0)
    compile_.add_argument("--include-partial", action="store_true")
    compile_.add_argument("--status", choices=("candidate", "reviewed"), default="candidate")
    compile_.add_argument("--actor", default=None, help="кто подтвердил набор (для reviewed)")
    compile_.add_argument("--llm-model", default=None)
    compile_.add_argument("--llm-revision", default=None)
    compile_.set_defaults(func=cmd_compile)

    validate = sub.add_parser("validate", help="проверить лист и/или JSON")
    validate.add_argument("--xlsx", default=None)
    validate.add_argument("--json", default=None)
    validate.add_argument("--golden", default=None)
    validate.set_defaults(func=cmd_validate)

    summary = sub.add_parser("summary", help="сводка по листу (и по набору)")
    summary.add_argument("--xlsx", required=True)
    summary.add_argument("--json", default=None)
    summary.set_defaults(func=cmd_summary)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
