#!/usr/bin/env python3
"""Прибор: находит ли **поиск** ZH-документ по русскому запросу — и меняет ли это dense-плечо.

Вопрос замера. Прошлый замер (``measure_cross_lingual_eval.py``) ответил на другую
половину: **оценщик** на ZH не хуже, чем на EN (согласие 26/30, κ 0.632, ни одного
``✅→❓``/``✅→❌``). Но там же обнаружился структурный ноль: русский запрос в лексическом
плече не находил **ни одного** чанка — ни в EN, ни в ZH (0 из 98 ячеек). Значит провал
сидит в **поиске**, а не в чтении, и dense-плечо в том замере не участвовало вовсе.

Этот прибор меряет ровно поиск: те же пары (страница одного мануала PostgreSQL в двух
языках), те же чанки проекта, но три профиля подряд на **одном** материале:

* ``R0`` — лексика (``TantivyLexicalIndex``, анализатор ``stemmed``, как в прогоне);
* ``R1`` — только dense (объявленная модель весов);
* ``R2`` — RRF обоих плеч (``retrieve``, как в прогоне).

Что именно измеряется — **находимость**, а не вердикт. Вердикт уже измерен и его шум
(κ на n=30 не воспроизводится: 0.63/0.79/0.81) забил бы сигнал поиска. Метрика простая и
объявленная: попал ли в top-K **хоть один** чанк страницы (индекс строится только по
чанкам этой страницы, продукт-скоуп I17, — поэтому «попал» значит «страница найдена»),
и попал ли в top-K чанк, содержащий ASCII-якорь пары (имена GUC/команд в переводе
остаются латиницей — это единственный дословный мост между сторонами).

Границы, названные заранее:

* **Векторы считаются на лету.** Персистентного индекса для этих страниц нет (в корпусе
  когорты их не было, у reference-корпуса векторов нет вовсе — ``index_manifest`` 0 строк,
  ``embed_model_id`` NULL). Подставить чужой индекс значило бы мерить не то; поэтому
  чанки стороны эмбеддятся объявленной моделью здесь же. Модель/ревизия — из
  ``deploy.yaml`` (``embed.model_id``/``revision``, ось ``embed_spec``), device/precision —
  флаги с дефолтом из того же файла; всё печатается в отчёте.
* **Один и тот же набор пар для всех профилей.** Парность обязательна: агрегаты двух
  прогонов на разном составе смешивают эффект плеча с составом выборки.
* **Критерий — точный McNemar на дискордантных ячейках** плюс бутстрэп-интервал дельты по
  ячейкам. ``p`` отвечает на «может ли это выйти из подбрасывания монеты», а не на
  «достаточно ли эффект велик» — второе решает человек (A3).
* **Сети нет, если кэш полон.** Страницы берутся через тот же ``PageCache``, что у
  прошлого замера: повторный прогон не качает заново. Пауза — объявленная вежливость.

Запуск::

    .venv/bin/python scripts/measure_cross_lingual_retrieval.py --pairs 30 \
        --profiles R0,R1,R2 --json /tmp/opencode/xling/xling-retrieval.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:  # запуск как скрипт: `core` берётся из дерева репозитория
    sys.path.insert(0, str(ROOT))
SCRIPTS = Path(__file__).resolve().parent
if str(SCRIPTS) not in sys.path:  # переиспользование прибора-соседа и парного сравнения
    sys.path.insert(0, str(SCRIPTS))

from measure_cross_lingual_eval import (  # noqa: E402
    PRODUCT_ID,
    PageCache,
    Pair,
    _load_discovery,
    anchor_pattern,
    build_pairs,
)
from measure_embed_input_variant import paired_comparison  # noqa: E402

from core.adapters.chunker_markdown import MarkdownChunkerPort  # noqa: E402
from core.adapters.extract_html import HtmlExtractor  # noqa: E402
from core.adapters.lexical_tantivy import TantivyLexicalIndex  # noqa: E402
from core.cli.wiring import (  # noqa: E402
    _resolve_embedder,
    build_evaluator_llm,
    resolve_embed_device,
    resolve_embed_precision,
    resolve_embed_spec,
)
from core.config.deploy import load_deploy_config  # noqa: E402
from core.config.run_config import load_catalog  # noqa: E402
from core.pipeline.evaluate import evaluate_cell, run_queries_for_cell  # noqa: E402
from core.pipeline.retrieve import RetrievalProfile, retrieve  # noqa: E402
from core.ports.types import Chunk  # noqa: E402
from core.ports.vector_index import RetrievedChunk, VectorQuery  # noqa: E402

#: Профили, которые прибор умеет исполнять. Список — не «все возможные», а те, у которых
#: есть объявленный смысл в прогоне: R0 лексика, R1 dense, R2 RRF.
PROFILES = ("R0", "R1", "R2")

#: Латиница в запросе: ``postgresql``/``WAL``/``LDAP`` остаются латиницей и в переводе.
_LATIN = re.compile(r"[A-Za-z]")

#: Кириллица: признак «термин русский», а не перевод латинского имени.
_CYRILLIC = re.compile(r"[\u0400-\u04FF]")

#: Языки сторон: EN — редакция postgresql.org, ZH — postgres.cn. Те же адреса, что у
#: прошлого замера, иначе пары были бы другими, а сравнение — непарным.
SIDES = ("en", "zh")


@dataclass(frozen=True)
class SideMaterial:
    """Материал одной стороны пары: чанки страницы и их векторы (если нужно).

    ``vectors`` — ``None``, когда профили не требуют dense: платить эмбеддингом за
    лексический замер значило бы назвать цену R0 неправдой.
    """

    lang: str
    url: str
    chunks: tuple[Chunk, ...]
    vectors: Any = None


class DenseMatrix:
    """Dense-плечо по чанкам одной страницы: косинус, без ANN и без диска.

    Реализует структурный контракт ``VectorIndexPort`` (``build_id``/``dim``/``search``/
    ``search_many``/``shard_ids``), потому что ``retrieve`` зовёт именно его. Это
    **прибор**, а не адаптер продукта: персистентный индекс для страниц замера не
    существует, и делать вид, что он есть, значило бы мерить чужую сборку.

    Нормировка — на входе: косинус тогда равен скалярному произведению, и повторный
    поиск по тому же материалу не зависит от порядка вызовов.
    """

    def __init__(self, chunks: Sequence[Chunk], vectors: Any, *, build_id: str) -> None:
        import numpy as np

        matrix = np.asarray(vectors, dtype=np.float32)
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        self._matrix = matrix / np.maximum(norms, 1e-9)
        self._chunks = list(chunks)
        self.build_id = build_id
        self.dim = int(self._matrix.shape[1])
        self._np = np

    def _search_one(self, query: VectorQuery) -> list[RetrievedChunk]:
        np = self._np
        if query.vector is None:
            raise ValueError("dense-плечо замера требует вектор запроса (текст не считается)")
        vector = np.asarray(query.vector, dtype=np.float32)
        vector = vector / max(float(np.linalg.norm(vector)), 1e-9)
        sims = self._matrix @ vector
        order = np.argsort(-sims)[: query.top_k]
        return [
            RetrievedChunk(
                chunk_id=self._chunks[int(index)].chunk_id,
                product_id=PRODUCT_ID,
                text=self._chunks[int(index)].text,
                content_hash=self._chunks[int(index)].content_hash,
                heading_path=self._chunks[int(index)].heading_path,
                build_id=self.build_id,
                score=float(sims[int(index)]),
            )
            for index in order
        ]

    def search(self, q: VectorQuery) -> list[RetrievedChunk]:
        return self._search_one(q)

    def search_many(self, queries: Sequence[VectorQuery]) -> list[list[RetrievedChunk]]:
        return [self._search_one(query) for query in queries]

    def shard_ids(self, product_id: str) -> list[str]:
        return [self.build_id]


def _lexical_index(chunks: Sequence[Chunk], *, build_id: str) -> TantivyLexicalIndex:
    """Лексическое плечо по чанкам одной стороны (тот же движок и анализатор, что в прогоне)."""
    store = {chunk.chunk_id: chunk for chunk in chunks}
    index = TantivyLexicalIndex(hydrate=lambda ids: {i: store[i] for i in ids if i in store})
    index.index(
        [
            {
                "chunk_id": chunk.chunk_id,
                "product_id": PRODUCT_ID,
                "text": chunk.text,
                "heading_path": chunk.heading_path,
                "content_hash": chunk.content_hash,
            }
            for chunk in chunks
        ],
        build_id=build_id,
    )
    return index


def _material(pair: Pair, *, lang: str, embedder: Any) -> SideMaterial:
    """Материал стороны: чанки страницы и (если нужен dense) их векторы.

    Эмбеддинг — **объявленной моделью** и на объявленном устройстве. Кэша векторов здесь
    нет намеренно: страницы замера не входят ни в ``index_store`` когорты, ни в
    reference-корпус, поэтому любой «кэш» был бы либо чужим, либо одноразовым.
    """
    side = pair.en if lang == "en" else pair.zh
    chunks = tuple(side.chunks)
    vectors = None
    if embedder is not None and chunks:
        vectors = embedder.embed([chunk.text for chunk in chunks])
    return SideMaterial(lang=lang, url=side.url, chunks=chunks, vectors=vectors)


def _queries(param: Any) -> list[str]:
    """Запросы Ш6 — те же, что уходят в фазу оценки (``render_queries``)."""
    return list(run_queries_for_cell(param))


def _ru_only_queries(param: Any) -> list[str]:
    """Запрос **только по-русски**: русские концепт-термины, без латинского моста.

    Зачем отдельный построитель. Всякий замер «переходит ли смысл через язык» упирается
    в выборку: пары строятся по ASCII-якорю, а якорь — это латинский токен, и он же
    попадает в запрос через ``title``/``concept_terms``. Тогда лексика находит обе
    стороны, и вопрос не проверяется (замер 18.11.2026: R0 28/30 «нашёл» на таких парах).

    Здесь берутся только термины кириллицей: если русских терминов нет — ячейка не
    меряется и называется числом, а не подменяется английским. Страница при этом
    **заведомо релевантна** (пара собрана по якорю, который в ней есть) — именно это
    отличает прибор от контроля на произвольной странице, где «не нашли» неотличимо от
    «там этого нет».
    """
    terms: list[str] = []
    for lang, values in (param.concept_terms or {}).items():
        if str(lang).lower() not in ("ru", "rus"):
            continue
        # Термин берётся, только если он **весь** русский: в каталоге первый элемент
        # `concept_terms[lang]` — это `title`, а в заголовке живут «WAL», «LDAP», «MS AD».
        # Оставить их значило бы вернуть латинский мост через чёрный ход и снова измерить
        # не то (замер 18.11.2026: 3/3 «нашёл» на R0 именно из-за этого).
        terms.extend(
            str(value)
            for value in values
            if _CYRILLIC.search(str(value)) and not _LATIN.search(str(value))
        )
    title = str(param.title or "")
    if _CYRILLIC.search(title) and not _LATIN.search(title):
        terms.insert(0, title)
    seen: set[str] = set()
    unique: list[str] = []
    for term in terms:
        key = term.strip().lower()
        if key and key not in seen:
            seen.add(key)
            unique.append(term.strip())
    return unique[:3]


def _precompute_query_vectors(
    pairs: Sequence[Pair], *, embedder: Any, needed: bool
) -> dict[str, list[float]] | None:
    """Векторы всех запросов одним батчем: запросы повторяются между парами.

    Один вызов на уникальный текст вместо вызова на каждый профиль×сторону×запрос:
    у 30 пар это 90 текстов против 540 вызовов, и разница — не оптимизация «по вкусу»,
    а цена замера, которая иначе попала бы в отчёт как свойство плеча.
    """
    if not needed or embedder is None:
        return None
    texts = sorted({query for pair in pairs for query in _queries(pair.param)})
    if not texts:
        return None
    vectors = embedder.embed(texts)
    return {text: list(vector) for text, vector in zip(texts, vectors, strict=True)}


def _found(cells: Sequence[dict[str, Any]], *, side: str, profile: str) -> list[dict[str, Any]]:
    """Ячейки одной стороны одного профиля в форме ``paired_comparison``.

    ``rank`` — не место в выдаче, а факт находимости (``0`` — попал, ``None`` — нет):
    страница в индексе одна, поэтому «место» здесь не имеет смысла, а «нашёл / не нашёл»
    — имеет. Это то же представление, что у приборов retrieval-эталона.
    """
    return [
        {
            "cell": f"{cell['param_id']}:{side}",
            "rank": 0 if cell[f"{side}_found"][profile] else None,
        }
        for cell in cells
    ]


def _anchor_cells(
    cells: Sequence[dict[str, Any]], *, side: str, profile: str, cutoff: int = 10
) -> list[dict[str, Any]]:
    """То же, но «попал» = чанк с ASCII-якорем пары вошёл в срез ``cutoff``.

    Сильнее находимости: находимость отвечает «страница найдена», якорь — «найден тот
    фрагмент, о котором пара». Для ZH это единственная дословная связь с EN-стороной
    (имена GUC/команд не переводятся), поэтому якорь и есть проверка «смысл перешёл».

    ``cutoff`` — не ``top_k`` замера, а более узкий срез: срез на всю глубину насыщается
    (в нём почти всегда есть якорь), и сравнение плеч теряет разрешение. По умолчанию
    сравнивается ``hit@3``.
    """
    return [
        {
            "cell": f"{cell['param_id']}:{side}",
            "rank": 0 if (cell[f"{side}_rank"][profile] or 99) <= cutoff else None,
        }
        for cell in cells
    ]


class PageIndexes:
    """Индексы одной стороны одной страницы: лексика, dense, чанки.

    Кэшируются на страницу, а не строятся на параметр. Это не оптимизация «по вкусу»:
    контроль меряет N параметров на **одной** странице, и без кэша та же страница
    эмбеддилась бы N раз — цена замера выросла бы в N раз, оставаясь ценой того же
    материала. Индекс страницы от параметра не зависит: параметр задаёт только запрос.
    """

    def __init__(self, material: SideMaterial, *, dense_needed: bool) -> None:
        self.material = material
        build_id = f"xling-{material.lang}-{len(material.chunks)}"
        self.lexical = _lexical_index(material.chunks, build_id=build_id)
        self.dense = (
            DenseMatrix(material.chunks, material.vectors, build_id=build_id)
            if dense_needed and material.vectors is not None and material.chunks
            else None
        )

    def close(self) -> None:
        self.lexical.close()


def measure_param(
    pair: Pair,
    param: Any,
    *,
    anchor: str | None,
    profiles: Sequence[str],
    top_k: int,
    embedder: Any,
    query_vectors: dict[str, list[float]] | None,
    indexes: dict[tuple[str, str], PageIndexes],
    judge_profiles: Sequence[str] = (),
    evaluator: Any = None,
    ru_only: bool = False,
) -> dict[str, Any]:
    """Один параметр на одной паре страниц: профили, две стороны, находимость.

    ``anchor=None`` — контрольный параметр: он не имеет ASCII-якоря, поэтому попадание
    якоря не считается (его нет), и остаётся находимость. Именно этот случай и отвечает
    на главный вопрос: лексическое плечо ищет **на языке запроса**, а запрос русский.

    ``indexes`` — кэш индексов по ``(path, lang)``: страница от параметра не зависит,
    поэтому строится один раз, сколько бы параметров на ней ни мерялось.
    """
    needs_dense = any(profile in ("R1", "R2") for profile in profiles)
    pattern = anchor_pattern(anchor) if anchor else None
    queries = _ru_only_queries(param) if ru_only else _queries(param)
    record: dict[str, Any] = {
        "param_id": str(param.param_id),
        "anchor": anchor,
        "path": pair.path,
        "queries": queries,
        # Латиница в запросе — тот самый «мост» между русским вопросом и латиницей
        # в тексте. Без этого признака контроль смешал бы два разных случая:
        # «лексика ищет на языке запроса» и «в запросе случайно оказался латинский токен».
        "latin_in_query": any(_LATIN.search(query) for query in queries),
    }
    for lang in SIDES:
        key = (pair.path, lang)
        if key not in indexes:
            material = _material(
                pair, lang=lang, embedder=embedder if needs_dense else None
            )
            indexes[key] = PageIndexes(material, dense_needed=needs_dense)
        page = indexes[key]
        found: dict[str, bool] = {}
        anchor: dict[str, bool] = {}
        counts: dict[str, int] = {}
        # Ранг первого чанка с якорем (1-based), ``None`` — в срезе его нет. Именно ранг,
        # а не «нашёл/не нашёл»: индекс строится из чанков **одной** страницы, поэтому
        # непустая выдача ничего не доказывает — dense всегда что-то ранжирует, и
        # признак «вернулось ≥1» насыщается на первом же куске. Насыщенный прибор
        # различать плечи не может (замер 18.11.2026: R1 давал 30/30 «нашёл» на всех
        # ячейках именно по этой причине).
        rank: dict[str, int | None] = {}
        judged: dict[str, Any] = {}
        for profile in profiles:
            chunks = _retrieve(
                queries,
                profile=profile,
                top_k=top_k,
                lexical=page.lexical,
                dense=page.dense,
                embedder=embedder if needs_dense else None,
                query_vectors=query_vectors,
            )
            found[profile] = bool(chunks)
            first = (
                next(
                    (
                        position
                        for position, chunk in enumerate(chunks, start=1)
                        if pattern.search(chunk.text)
                    ),
                    None,
                )
                if pattern is not None
                else None
            )
            anchor[profile] = first is not None
            rank[profile] = first
            counts[profile] = len(chunks)
            # Судья вызывается только там, где его спросили: это платный шаг (вызов
            # модели), и на основном наборе он не нужен — там якорь даёт дословную
            # проверку, а модель только подтверждала бы её своими словами.
            if evaluator is not None and profile in judge_profiles and chunks:
                judged[profile] = judge_cell(
                    param, chunks, evaluator=evaluator, run_id=f"xling-judge-{profile.lower()}"
                )
        record[f"{lang}_chunks"] = len(page.material.chunks)
        record[f"{lang}_found"] = found
        record[f"{lang}_anchor"] = anchor
        record[f"{lang}_rank"] = rank
        record[f"{lang}_counts"] = counts
        if judged:
            record[f"{lang}_judged"] = judged
    return record


def _retrieve(
    queries: Sequence[str],
    *,
    profile: str,
    top_k: int,
    lexical: Any,
    dense: Any,
    embedder: Any,
    query_vectors: dict[str, list[float]] | None,
) -> list[Any]:
    """Поиск по **явным** запросам, слияние по ``max`` (то же правило, что в Ш6).

    Почему не ``retrieve_for_cell``. Он собирает запросы сам — ``run_queries_for_cell``
    из параметра. Для замера языка запроса это неверный путь: подменить запрос там
    нельзя, и первая редакция прибора мерила ``render_queries`` в обоих режимах, помечая
    результат как «чисто русский» (поймано сверкой с прямым поиском 18.11.2026: прямой
    запрос дал 0 хитов, а прибор — 5).

    Слияние — ``max`` по ``chunk_id``: это дефолт ``cross_query_fusion`` фазы оценки,
    поэтому срез сравнивается с тем, что делает прогон, а не с самодельной политикой.
    """
    merged: dict[str, Any] = {}
    for query in queries:
        vector = query_vectors.get(query) if query_vectors else None
        if vector is None and profile in ("R1", "R2") and embedder is not None:
            vector = list(embedder.embed([query])[0])
        result = retrieve(
            query_text=query,
            query_vector=vector,
            product_ids=[PRODUCT_ID],
            profile=RetrievalProfile(profile),
            cutoff_k=top_k,
            lexical_index=lexical,
            vector_index=dense,
        )
        for chunk in result.chunks:
            current = merged.get(chunk.chunk_id)
            if current is None or chunk.score > current.score:
                merged[chunk.chunk_id] = chunk
    ordered = sorted(merged.values(), key=lambda chunk: (-chunk.score, chunk.chunk_id))
    return ordered[:top_k]


def judge_cell(
    param: Any,
    chunks: Sequence[Any],
    *,
    evaluator: Any,
    run_id: str,
) -> dict[str, Any]:
    """Судья релевантности: настоящий ``evaluate_cell`` на выданном срезе.

    Зачем отдельный шаг. Находимость отвечает «вернулось ли хоть что-то», и для dense это
    всегда «да»: косинус ранжирует любой текст. Проверяемый вопрос другой — **описал ли
    найденный фрагмент параметр**. Отвечает на него оценщик продукта (``evaluate_cell``):
    ✅ значит «в срезе есть фрагмент, называющий параметр», ❓ — «нашли что-то другое».

    Это не evidence о продукте: ячейка никуда не пишется, а ``run_id`` замера отдельный.
    Оценщик здесь — **инструмент измерения поиска**, и его собственная надёжность на ZH
    уже измерена отдельно (согласие 26/30, κ 0.632).
    """
    outcome = evaluate_cell(
        param=param,
        product_id=PRODUCT_ID,
        run_id=run_id,
        evaluator=evaluator,
        chunks=tuple(chunks),
    )
    return {
        "status": outcome.cell.status.value if outcome.cell.status is not None else None,
        "failure_kind": outcome.failure_kind,
        "quotes": [evidence.quote[:100] for evidence in outcome.cell.evidence if evidence.quote][:2],
    }


def summarize_profile(
    cells: Sequence[dict[str, Any]],
    *,
    side: str,
    profile: str,
    include_anchor: bool = True,
) -> dict[str, Any]:
    """Сводка одного профиля одной стороны: находимость и (если есть якорь) его попадание."""
    found = [cell[f"{side}_found"][profile] for cell in cells]
    counts = [cell[f"{side}_counts"][profile] for cell in cells]
    anchor = (
        [cell[f"{side}_anchor"][profile] for cell in cells] if include_anchor else None
    )
    ranks = [cell[f"{side}_rank"][profile] for cell in cells] if include_anchor else None
    mrr = None
    hit_at = {}
    if ranks:
        # MRR по ячейкам: ячейка без якоря в срезе даёт 0, а не выбрасывается — иначе
        # «плечо ничего не нашло» исчезло бы из среднего, и прибор хвалил бы себя.
        mrr = sum(1.0 / rank if rank else 0.0 for rank in ranks) / len(ranks)
        for k in (1, 3):
            hit_at[str(k)] = sum(1 for rank in ranks if rank and rank <= k)
    return {
        "side": side,
        "profile": profile,
        "cells": len(cells),
        "found": sum(found),
        "found_share": (sum(found) / len(found)) if found else None,
        "anchor": sum(anchor) if anchor is not None else None,
        "anchor_share": (sum(anchor) / len(anchor)) if anchor else None,
        "mrr": mrr,
        "hit_at": hit_at,
        # Пустая выдача — не «ноль попаданий по смыслу», а ноль выдачи: числа разные,
        # и смешивать их в одну долю значило бы назвать пустоту промахом.
        "empty_results": sum(1 for value in counts if value == 0),
        "chunks_mean": (sum(counts) / len(counts)) if counts else None,
    }


def compare_profiles(
    cells: Sequence[dict[str, Any]],
    *,
    side: str,
    baseline: str,
    variant: str,
    bootstrap: int,
    seed: int,
    include_anchor: bool = True,
    cutoff: int = 3,
) -> dict[str, Any]:
    """Парное сравнение двух профилей на **одних и тех же** ячейках.

    Считаются два взгляда: находимость страницы и попадание ASCII-якоря. Первый отвечает
    «стало ли что-то находиться», второй — «находится ли нужное»; разница между ними и
    есть разница между «dense приносит хоть что-то» и «dense приносит тот фрагмент».
    """
    return {
        "side": side,
        "baseline": baseline,
        "variant": variant,
        "found": paired_comparison(
            _found(cells, side=side, profile=baseline),
            _found(cells, side=side, profile=variant),
            bootstrap=bootstrap,
            seed=seed,
        ),
        "anchor": (
            paired_comparison(
                _anchor_cells(cells, side=side, profile=baseline, cutoff=cutoff),
                _anchor_cells(cells, side=side, profile=variant, cutoff=cutoff),
                bootstrap=bootstrap,
                seed=seed,
            )
            if include_anchor
            else None
        ),
    }


def _fmt_p(value: float | None) -> str:
    return "—" if value is None else f"{value:.4f}"


def _fmt_share(value: float | None) -> str:
    return "—" if value is None else f"{value:.3f}"


def render(sections: Sequence[dict[str, Any]], comparisons: Sequence[dict[str, Any]]) -> str:
    """Текстовая сводка: таблица профилей по сторонам и парные сравнения."""
    lines = [
        "находимость по профилям (ячейка = параметр × сторона; индекс только по странице пары)",
        f"{'сторона':8} {'профиль':8} {'нашёл':>7} {'доля':>7} {'якорь':>7} {'доля':>7} "
        f"{'hit@3':>6} {'MRR':>6} {'пусто':>6}",
    ]
    for section in sections:
        anchor = "—" if section["anchor"] is None else f"{section['anchor']}/{section['cells']}"
        hit3 = section["hit_at"].get("3")
        hit3_text = "—" if hit3 is None else f"{hit3}/{section['cells']}"
        mrr = "—" if section["mrr"] is None else f"{section['mrr']:.3f}"
        lines.append(
            f"{section['side']:8} {section['profile']:8} "
            f"{section['found']:>3}/{section['cells']:<3} "
            f"{_fmt_share(section['found_share']):>7} "
            f"{anchor:>7} "
            f"{_fmt_share(section['anchor_share']):>7} "
            f"{hit3_text:>6} {mrr:>6} {section['empty_results']:>6}"
        )
    lines.append("")
    lines.append("парные сравнения (McNemar exact на дискордантных ячейках)")
    for item in comparisons:
        for view in ("found", "anchor"):
            block = item[view]
            if block is None:
                continue
            if not block.get("n_pairs"):
                lines.append(f"  {item['side']} {item['baseline']}→{item['variant']} {view}: сравнивать нечего")
                continue
            ci = block.get("delta_ci") or [None, None]
            lines.append(
                f"  {item['side']} {item['baseline']}→{item['variant']} {view}: "
                f"{_fmt_share(block['hit_baseline'])}→{_fmt_share(block['hit_variant'])} "
                f"Δ{block['delta']:+.4f} CI[{ci[0]:+.4f};{ci[1]:+.4f}] "
                f"{block['better']}/{block['worse']} p={_fmt_p(block['mcnemar_p'])}"
            )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Замер: находит ли поиск ZH-документ по русскому запросу и меняет ли это "
            "dense-плечо (R0/R1/R2 на одних и тех же парах, без LLM)"
        )
    )
    parser.add_argument("--catalog", default="packages/ru-pg-dbms/catalog.csv")
    parser.add_argument("--deploy", default="config/deploy.yaml")
    parser.add_argument(
        "--cache-dir",
        default="/tmp/opencode/xling/cache",  # noqa: S108 — объявленный каталог замера
        help="кэш страниц на диске (тот же, что у замера оценщика)",
    )
    parser.add_argument("--pairs", type=int, default=30, help="сколько пар мерить")
    parser.add_argument("--max-fetches", type=int, default=150)
    parser.add_argument("--pause", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--top-k", type=int, default=10, help="cutoff_K каждого профиля")
    parser.add_argument(
        "--profiles",
        default="R0,R1,R2",
        help="какие профили исполнять (R0 лексика, R1 dense, R2 RRF)",
    )
    parser.add_argument(
        "--dense-device",
        default=None,
        help="устройство dense-плеча; по умолчанию — объявленное в deploy.yaml",
    )
    parser.add_argument(
        "--dense-precision",
        default=None,
        help="точность dense-плеча; по умолчанию — объявленная в deploy.yaml",
    )
    parser.add_argument(
        "--control-params",
        type=int,
        default=10,
        help=(
            "сколько параметров без ASCII-якоря (чисто русский запрос) взять в контроль "
            "языка запроса на тех же страницах; 0 — без контроля"
        ),
    )
    parser.add_argument(
        "--ru-query",
        action="store_true",
        help=(
            "запрос строить только из русских терминов (кириллица), без латинского моста; "
            "страница пары при этом заведомо релевантна — это и есть проверка перехода смысла"
        ),
    )
    parser.add_argument(
        "--judge",
        action="store_true",
        help=(
            "судить релевантность чисто русских контрольных ячеек настоящим evaluate_cell: "
            "находимость для dense насыщается всегда, а ✅/❓ отвечает «нашли нужное»"
        ),
    )
    parser.add_argument("--judge-cells", type=int, default=12, help="сколько контрольных ячеек судить")
    parser.add_argument("--judge-profile", default="R1", help="какой профиль судить (R1 или R2)")
    parser.add_argument("--llm-model", default=None, help="модель судьи (объявленный вход)")
    parser.add_argument("--llm-slot", default=None)
    parser.add_argument("--llm-concurrency", type=int, default=4)
    parser.add_argument("--bootstrap", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=20261118)
    parser.add_argument("--json", default=None)
    return parser


def _profiles(raw: str) -> list[str]:
    """Разбор ``--profiles``: неизвестный профиль — отказ, а не молчаливое понижение."""
    names = [part.strip().upper() for part in raw.split(",") if part.strip()]
    if not names:
        raise SystemExit("--profiles пуст: назовите хотя бы R0")
    unknown = [name for name in names if name not in PROFILES]
    if unknown:
        raise SystemExit(f"неизвестный профиль {', '.join(unknown)} (допустимо: {', '.join(PROFILES)})")
    # Порядок фиксирован: R0 → R1 → R2, иначе отчёт зависел бы от порядка во флаге.
    return [name for name in PROFILES if name in names]


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    profiles = _profiles(args.profiles)
    started = time.monotonic()

    cache = PageCache(
        Path(args.cache_dir),
        pause=args.pause,
        timeout=args.timeout,
        user_agent="coHound-xling-retrieval/0.1 (research; public docs only)",
    )
    catalog = load_catalog(args.catalog)
    sitemap, entries = _load_discovery(cache)
    chunker = MarkdownChunkerPort()
    extractor = HtmlExtractor()
    pairs, drops, dropped = build_pairs(
        catalog,
        entries,
        cache,
        extractor=extractor,
        chunker=chunker,
        limit=args.pairs,
        max_fetches=args.max_fetches,
    )
    if not pairs:
        print("пар не собрано: якорей нет ни у одного параметра", file=sys.stderr)
        return 1

    needs_dense = any(profile in ("R1", "R2") for profile in profiles)
    deploy = load_deploy_config(Path(args.deploy))
    embed_cfg = deploy.embed
    spec = resolve_embed_spec(embed_cfg, None)
    device = args.dense_device or resolve_embed_device(embed_cfg, None)
    precision = args.dense_precision or resolve_embed_precision(embed_cfg, None)
    embedder = None
    notices: list[str] = []
    if needs_dense:
        embedder = _resolve_embedder(
            device,
            precision=precision,
            model_id=spec.model_id,
            revision=spec.revision,
            notices=notices,
        )
    query_vectors = _precompute_query_vectors(pairs, embedder=embedder, needed=needs_dense)

    judge_evaluator = None
    judge_profiles: tuple[str, ...] = ()
    if args.judge:
        judge_evaluator = build_evaluator_llm(
            deploy_llm=deploy.llm, slot=args.llm_slot, model_id=args.llm_model
        )
        if judge_evaluator is None:
            print(
                "отказ: LLM endpoint оценщика не сконфигурирован (base_url слота пуст) — "
                "судить нечем",
                file=sys.stderr,
            )
            return 2
        if args.judge_profile not in profiles:
            print(
                f"отказ: --judge-profile {args.judge_profile} не входит в --profiles "
                f"({', '.join(profiles)})",
                file=sys.stderr,
            )
            return 2
        judge_profiles = (args.judge_profile,)

    indexes: dict[tuple[str, str], PageIndexes] = {}
    cells: list[dict[str, Any]] = []
    for index, pair in enumerate(pairs, start=1):
        cell = measure_param(
            pair,
            pair.param,
            anchor=pair.anchor,
            profiles=profiles,
            top_k=args.top_k,
            embedder=embedder,
            query_vectors=query_vectors,
            indexes=indexes,
            ru_only=args.ru_query,
            judge_profiles=judge_profiles if args.ru_query else (),
            evaluator=judge_evaluator,
        )
        cells.append(cell)
        print(f"[{index}/{len(pairs)}] {cell['param_id']} · {cell['path']}", flush=True)

    # Контроль языка запроса. Пары основного набора отобраны по ASCII-якорю, а якорь —
    # это и есть латинский токен в запросе: на таких парах лексика находит обе стороны,
    # и «переходит ли смысл» по ним не проверить. Здесь берутся параметры **без** якоря
    # (их запрос чисто русский) и те же страницы: лексическое плечо ищет на языке запроса,
    # поэтому ожидание для R0 — структурный ноль, а вопрос замера — снимает ли его dense.
    control_cells: list[dict[str, Any]] = []
    if args.control_params > 0 and not args.ru_query:
        from measure_cross_lingual_eval import choose_anchor

        control = [
            param for param in catalog if choose_anchor(param, entries).anchor is None
        ][: args.control_params]
        pages: list[Pair] = []
        seen_paths: set[str] = set()
        for pair in pairs:
            if pair.path in seen_paths:
                continue
            seen_paths.add(pair.path)
            pages.append(pair)
        total = len(pages) * len(control)
        step = 0
        judged = 0
        for pair in pages:
            for param in control:
                step += 1
                # Судья платный (вызов модели), поэтому его бюджет объявлен числом
                # ячеек: за его пределом замер продолжается без суда, а не молча
                # судит всё подряд.
                active_judge = judge_profiles if judged < args.judge_cells else ()
                cell = measure_param(
                    pair,
                    param,
                    anchor=None,
                    profiles=profiles,
                    top_k=args.top_k,
                    embedder=embedder,
                    query_vectors=query_vectors,
                    indexes=indexes,
                    judge_profiles=active_judge,
                    evaluator=judge_evaluator,
                )
                if active_judge:
                    judged += 1
                control_cells.append(cell)
                if step % 10 == 0 or step == total:
                    print(f"[контроль {step}/{total}] {cell['param_id']} · {cell['path']}", flush=True)

    sections = [
        summarize_profile(cells, side=side, profile=profile)
        for side in SIDES
        for profile in profiles
    ]
    comparisons = [
        compare_profiles(
            cells,
            side=side,
            baseline=profiles[0],
            variant=profile,
            bootstrap=args.bootstrap,
            seed=args.seed,
        )
        for side in SIDES
        for profile in profiles[1:]
    ]
    # Разделение контроля по **языку запроса**, а не по языку документа: ячейка с
    # латинским токеном в запросе имеет мост, и лексика находит по нему. Ответ на вопрос
    # «переходит ли смысл через язык» даёт только чисто русский запрос.
    pure_cells = [cell for cell in control_cells if not cell["latin_in_query"]]
    bridged_cells = [cell for cell in control_cells if cell["latin_in_query"]]

    def _sections(items: Sequence[dict[str, Any]], *, with_anchor: bool) -> list[dict[str, Any]]:
        return [
            summarize_profile(items, side=side, profile=profile, include_anchor=with_anchor)
            for side in SIDES
            for profile in profiles
        ]

    def _comparisons(
        items: Sequence[dict[str, Any]], *, with_anchor: bool
    ) -> list[dict[str, Any]]:
        return [
            compare_profiles(
                items,
                side=side,
                baseline=profiles[0],
                variant=profile,
                bootstrap=args.bootstrap,
                seed=args.seed,
                include_anchor=with_anchor,
            )
            for side in SIDES
            for profile in profiles[1:]
        ]

    judged_cells = [
        cell
        for cell in (*cells, *control_cells)
        if cell.get("en_judged") or cell.get("zh_judged")
    ]
    judge_summary: dict[str, Any] = {}
    if judged_cells and judge_profiles:
        profile = judge_profiles[0]
        for side in SIDES:
            statuses = [
                cell[f"{side}_judged"][profile]["status"]
                for cell in judged_cells
                if cell.get(f"{side}_judged") and profile in cell[f"{side}_judged"]
            ]
            known = [status for status in statuses if status is not None]
            judge_summary[side] = {
                "profile": profile,
                "cells": len(statuses),
                "ok": known.count("✅"),
                "unknown": known.count("❓"),
                "absent": known.count("❌"),
                "warning": known.count("⚠️"),
                "unscored": len(statuses) - len(known),
                "ok_share": (known.count("✅") / len(known)) if known else None,
            }

    control_sections = _sections(control_cells, with_anchor=False)
    control_comparisons = _comparisons(control_cells, with_anchor=False)
    pure_sections = _sections(pure_cells, with_anchor=False) if pure_cells else []
    pure_comparisons = _comparisons(pure_cells, with_anchor=False) if pure_cells else []
    bridged_sections = _sections(bridged_cells, with_anchor=False) if bridged_cells else []

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "inputs": {
            "catalog": args.catalog,
            "deploy": args.deploy,
            "pairs_requested": args.pairs,
            "pairs_built": len(pairs),
            "sitemap_urls_docs18": len(sitemap),
            "index_entries": len(entries),
            "chunk_size": chunker.chunk_size,
            "chunker_version": chunker.version,
            "reader_version": extractor.version,
            "top_k": args.top_k,
            "profiles": profiles,
            "embed_model_id": spec.model_id,
            "embed_revision": spec.revision,
            "embed_device": device,
            "embed_precision": precision,
            "embed_notices": notices,
            "embedder": type(embedder).__name__ if embedder is not None else None,
            "bootstrap": args.bootstrap,
            "seed": args.seed,
            # Число вызовов судьи, а не «сколько хотели»: считается по факту записей.
            "llm_calls": sum(
                1
                for cell in (*cells, *control_cells)
                for side in SIDES
                for _ in (cell.get(f"{side}_judged") or {})
            ),
        },
        "drops": drops,
        "dropped": dropped,
        "cells": cells,
        "sections": sections,
        "comparisons": comparisons,
        "control": {
            "params": sorted({cell["param_id"] for cell in control_cells}),
            "pages": len({cell["path"] for cell in control_cells}),
            "cells": control_cells,
            "sections": control_sections,
            "comparisons": control_comparisons,
            "pure_ru": {
                "cells": len(pure_cells),
                "sections": pure_sections,
                "comparisons": pure_comparisons,
            },
            "latin_in_query": {
                "cells": len(bridged_cells),
                "sections": bridged_sections,
            },
            "judge": judge_summary,
        },
        "cost": {
            **cache.stats.as_dict(),
            "seconds_total": round(time.monotonic() - started, 1),
        },
    }
    out_path = (
        Path(args.json)
        if args.json
        else Path(args.cache_dir).parent / "xling-retrieval.json"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print()
    print(
        f"пар: {len(pairs)}; профили: {', '.join(profiles)}; top_k={args.top_k}; "
        f"чанкер {chunker.version} chunk_size={chunker.chunk_size}; читатель {extractor.version}"
    )
    if needs_dense:
        print(
            f"dense: {spec.model_id}@{spec.revision[:12]}… · {device} · {precision} · "
            f"{type(embedder).__name__}"
        )
        for notice in notices:
            print(f"  заметка: {notice}")
    print("отбраковка: " + ", ".join(f"{k}={v}" for k, v in drops.items() if v))
    print()
    print(render(sections, comparisons))
    if control_cells:
        print()
        print(
            f"КОНТРОЛЬ ЯЗЫКА ЗАПРОСА: {len(payload['control']['params'])} параметров без "
            f"ASCII-якоря × {payload['control']['pages']} страниц = {len(control_cells)} ячеек "
            f"(чисто русский запрос — {len(pure_cells)}, латинский токен — {len(bridged_cells)})"
        )
        print(render(control_sections, control_comparisons))
        if pure_cells:
            print()
            print("ЧИСТО РУССКИЙ ЗАПРОС (моста нет: лексика ищет на языке запроса)")
            print(render(pure_sections, pure_comparisons))
    if judge_summary:
        head = judge_summary.get("en") or judge_summary.get("zh") or {}
        print()
        print(
            f"СУД РЕЛЕВАНТНОСТИ ({head.get('profile')}, "
            f"{head.get('cells')} ячеек на сторону): описал ли найденный фрагмент параметр"
        )
        for side in SIDES:
            block = judge_summary.get(side)
            if block is None:
                continue
            print(
                f"  {side}: ✅ {block['ok']}, ⚠️ {block['warning']}, ❓ {block['unknown']}, "
                f"❌ {block['absent']}"
                + (f", без статуса {block['unscored']}" if block["unscored"] else "")
                + f" → доля ✅ {_fmt_share(block['ok_share'])}"
            )
    cost = payload["cost"]
    print()
    print(
        "цена: страниц из сети {pages_network}, из кэша {pages_from_cache}, "
        "{mb_network} МБ, сеть {seconds} с, всего {seconds_total} с; "
        "LLM-вызовов {llm}".format(**cost, llm=payload["inputs"]["llm_calls"])
    )
    print(f"JSON: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
