#!/usr/bin/env python3
"""Где теряется `present` в разведке: три слоя кросс-языкового сопоставления (план 27 §8.4).

**Вопрос.** Документация предка — английская, параметры каталога — русские. `hound baseline
recon` ищет место параметра в этой документации лексическим индексом. Если он находит мало,
виноват может быть (а) запрос — не те слова и не тот язык, (б) отбор — индекс, (в) правило
зачёта — условие, при котором найденное считается цитатой параметра. Различить их
«на глаз» нельзя: все три дают один и тот же симптом «не нашли».

**Что меряется.** Прибор считает исход по каждому параметру при фиксированных прочих слоях и
печатает три числа: сколько параметров получили цитату, сколько цитат **различны** и сколько
из них **делятся** между двумя и более параметрами. Третье число — структурный признак
ложного `present`: цитата, названная дважды, не называет ни одного из двух параметров, а
наследование по ней — вывод, которого не измеряли (замер 18.11.2026: одна цитата
`'Oracle Compatibility Mode #'` обслуживала 8 параметров).

Три оси, по одной за прогон (или две сразу, если сравнение — именно между ними):

* ``--variants`` — **слой запроса**: чем поставлена задача индексу.
  ``code`` — как было в коде до 18.11.2026 (``title`` + ``concept_terms``, слова);
  ``en`` — мешок слов из авторских ``terms_en`` (``concept_terms["en"]``);
  ``phrase`` — кандидаты те же, что ``en``, но цитатой считается только предложение с
  термином **целиком**.
* ``--rules`` — **слой зачёта** при одном и том же запросе (``parameter_terms``):
  ``any`` — в предложении есть любое слово термина; ``all`` — все слова одного термина;
  ``phrase`` — термин целиком.
* ``--dense`` — **слой отбора**: то же, что ``--rules phrase``, но кандидатов даёт косинус
  по объявленной в deploy модели эмбеддера (``embed.model_id``/``embed.revision``), а не
  BM25. Плечи сравниваются на одном входе, судья судит оба — иначе различие отборщиков
  смешивается с различием запросов (замер 18.11.2026: 37 против 32 при одинаковом входе,
  тогда как пробники с разной токенизацией давали 48 против 37).

**Почему запрос и зачёт разделены.** BM25 всё равно токенизирует, поэтому запрос обязан быть
словами — иначе теряются кандидаты. Точность даёт не запрос, а условие зачёта; сливать их в
один список значило бы либо потерять кандидатов, либо вернуть зачёт по слову.

**Судья — отдельным флагом, потому что он платный.** ``--judge-sample N`` берёт
детерминированную выборку (``seed=0``) найденных цитат по каждому правилу и судит их
настоящим ``evaluate_cell``; печатается доля ✅ с интервалом Уилсона. Модель — объявленный
аргумент (``--llm-model``), а не настройка окружения: вердикт без названной модели — не
наблюдение. Без флага прибор работает без сети и без LLM.

**Границы.** Корпус читается целиком (``--max-chunks`` — только для быстрой проверки, и тогда
число чанков печатается). Один корпус — одна версия: перенос чисел на другую версию или на
другой профиль не следует из этого замера.

Запуск::

    .venv/bin/python scripts/measure_recon_matching.py \
      --db ~/.cohound/stands/ru-pg-dbms/reference/ru-pg-dbms/postgresql/18/builds/{id}/reference.db \
      --catalog packages/ru-pg-dbms/catalog.csv \
      --variants code,en,phrase --rules any,all,phrase [--top-k 20] [--out-json /tmp/x.json]

    # с судьёй (платно, нужен объявленный endpoint и ключ)
    … --judge-sample 25 --llm-model deepseek-v4.1-flash-1017:yellow
"""

from __future__ import annotations

import argparse
import json
import math
import random
import re
import sqlite3
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.adapters.lexical_bm25 import Bm25Index, LexicalDoc
from core.config.run_config import load_catalog
from core.domain.models import CatalogRow
from core.pipeline.reference_corpus import (
    ChunkText,
    parameter_phrases,
    parameter_terms,
)

#: Правила зачёта: чем проверяется, что предложение называет параметр.
RULES = ("any", "all", "phrase")

#: Варианты слоя запроса.
VARIANTS = ("code", "en", "phrase")

#: Минимальная длина слова, участвующего в зачёте (как ``MIN_TERM_LEN`` в pipeline).
MIN_WORD_LEN = 4

#: Разбиение фразы на слова — то же, что в ``reference_corpus._SPLIT_RE``.
_SPLIT_RE = re.compile(r"[^\w\-]+", re.UNICODE)

#: Конец предложения внутри строки: точка с последующим пробелом.
_SENTENCE_RE = re.compile(r"(?<=\.)\s+")

#: Длина публикуемой цитаты (полная цитата факта длиннее и в отчёте не нужна).
QUOTE_PREVIEW = 120


@dataclass(frozen=True)
class Variant:
    """Как поставлена задача индексу и чем засчитывается цитата."""

    name: str
    terms: Callable[[CatalogRow], list[str]]
    phrases: Callable[[CatalogRow], list[str]]
    rule: str


def _words(text: str) -> list[str]:
    """Слова фразы: то, что BM25 всё равно увидит после токенизации."""
    return [word for word in _SPLIT_RE.split(text.lower()) if len(word) >= MIN_WORD_LEN]


def _en_terms(param: CatalogRow) -> list[str]:
    """Авторские термины английской стороны (``concept_terms["en"]``)."""
    return [str(item).strip() for item in (param.concept_terms or {}).get("en") or [] if str(item).strip()]


def _code_terms(param: CatalogRow) -> list[str]:
    """Запрос «как было в коде до 18.11.2026»: ``title`` + все ``concept_terms``, словами."""
    return parameter_terms(param)


def _en_words(param: CatalogRow) -> list[str]:
    """Запрос варианта ``en``: мешок слов из авторских терминов (без ``title``)."""
    return [word for phrase in _en_terms(param) for word in _words(phrase)]


def variants() -> dict[str, Variant]:
    """Три варианта слоя запроса; зачёт у ``code``/``en`` — по любому слову, у ``phrase`` — по фразе."""
    return {
        "code": Variant("code", _code_terms, _code_terms, "any"),
        "en": Variant("en", _en_words, _en_terms, "any"),
        "phrase": Variant("phrase", _en_words, parameter_phrases, "phrase"),
    }


def sentences(text: str) -> list[str]:
    """Предложения чанка: по строкам, внутри строки — по точке с пробелом.

    Цитата обязана быть дословной подстрокой (I22), поэтому границы предложения ищутся
    грубо и предсказуемо: разрезать по смыслу значило бы зависеть от токенизатора, а
    сравнивать надо правила зачёта, а не сегментацию.
    """
    out: list[str] = []
    for line in (text or "").split("\n"):
        for part in _SENTENCE_RE.split(line):
            part = part.strip()
            if part:
                out.append(part)
    return out


def quote_for_rule(text: str, phrases: Sequence[str], rule: str) -> str | None:
    """Цитата по правилу зачёта: ``any`` / ``all`` / ``phrase``; ``None`` — не нашлось.

    Возвращаемое — всегда подстрока ``text``: правило решает, **какое** предложение
    засчитать, но не переписывает его.
    """
    words_by_phrase = [_words(phrase) for phrase in phrases]
    for sentence in sentences(text):
        lowered = sentence.lower()
        for phrase, words in zip(phrases, words_by_phrase, strict=False):
            if rule == "phrase":
                if phrase.lower() in lowered:
                    return sentence[:400]
            elif rule == "all":
                if words and all(word in lowered for word in words):
                    return sentence[:400]
            elif rule == "any":
                if any(word in lowered for word in words):
                    return sentence[:400]
            else:  # pragma: no cover — правило проверено argparse-ом
                raise ValueError(f"неизвестное правило зачёта: {rule!r}")
    return None


def read_chunks(db_path: Path, max_chunks: int = 0) -> list[ChunkText]:
    """Читает reference-корпус одним проходом (порядок ``doc_id, chunk_no``)."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        sql = "SELECT chunk_id, doc_id, url, heading_path, text, content_hash FROM corpus_chunks"
        # Порядок ОБЯЗАН стоять до LIMIT: срез корпуса — первые N чанков в порядке
        # документа, а не «N произвольных строк» (SQLite отвергает LIMIT перед ORDER BY,
        # и до 18.11.2026 срез не исполнялся вовсе — `--max-chunks` падал синтаксисом).
        sql += " ORDER BY doc_id, chunk_no, chunk_id"
        if max_chunks:
            sql += f" LIMIT {int(max_chunks)}"
        return [
            ChunkText(
                chunk_id=str(chunk_id),
                doc_id=str(doc_id),
                url=str(url) if url else None,
                text=str(text or ""),
                content_hash=str(digest) if digest else None,
                heading_path=str(heading) if heading else None,
            )
            for chunk_id, doc_id, url, heading, text, digest in conn.execute(sql)
        ]
    finally:
        conn.close()


def build_index(chunks: Iterable[ChunkText]) -> Bm25Index:
    """Лексический индекс по чанкам: тот же движок, что у ``recon``."""
    index = Bm25Index()
    index.index(
        [LexicalDoc(chunk_id=chunk.chunk_id, product_id="reference", text=chunk.text) for chunk in chunks]
    )
    return index


def _measure_with_candidates(
    *,
    params: Sequence[CatalogRow],
    chunks: Sequence[ChunkText],
    candidates: Callable[[CatalogRow], Sequence[str]],
    phrases: Callable[[CatalogRow], list[str]],
    rule: str,
) -> dict[str, tuple[str, str] | None]:
    """Исход отбора при заданном **отборе** кандидатов и заданном правиле зачёта.

    Отбор вынесен в ``candidates``, потому что он и есть сравниваемая ось: лексика и dense
    подают разные списки ``chunk_id`` на один и тот же параметр, а зачёт у обоих обязан быть
    один — иначе сравнение меряет два различия сразу.
    """
    by_id = {chunk.chunk_id: chunk for chunk in chunks}
    out: dict[str, tuple[str, str] | None] = {}
    for param in params:
        found: tuple[str, str] | None = None
        for chunk_id in candidates(param):
            chunk = by_id.get(chunk_id)
            if chunk is None:
                continue
            quote = quote_for_rule(chunk.text, phrases(param), rule)
            if quote is not None:
                found = (chunk_id, quote)
                break
        out[str(param.param_id)] = found
    return out


def measure(
    *,
    params: Sequence[CatalogRow],
    chunks: Sequence[ChunkText],
    index: Bm25Index,
    terms: Callable[[CatalogRow], list[str]],
    phrases: Callable[[CatalogRow], list[str]],
    rule: str,
    top_k: int,
) -> dict[str, tuple[str, str] | None]:
    """Исход отбора по каждому параметру: ``(chunk_id, цитата)`` или ``None``.

    Кандидатов даёт **запрос** (``terms``), а засчитывает **правило** по ``phrases``: это и
    есть разделение recall/точность, из-за которого прибор существует.
    """
    def candidates(param: CatalogRow) -> list[str]:
        query = terms(param)
        if not query:
            return []
        return [chunk_id for chunk_id, _score in index.search_scores(" ".join(query), top_k=top_k)]

    return _measure_with_candidates(
        params=params, chunks=chunks, candidates=candidates, phrases=phrases, rule=rule
    )


def dense_candidates(
    *,
    params: Sequence[CatalogRow],
    chunks: Sequence[ChunkText],
    embedder: Any,
    terms: Callable[[CatalogRow], list[str]],
    top_k: int,
    batch_size: int = 32,
    progress: Callable[[int, int], None] | None = None,
) -> Callable[[CatalogRow], list[str]]:
    """Отбор по косинусу: векторы чанков считаются **в памяти**, персиста нет.

    Персист здесь не нужен и не проверяется: прибор отвечает на вопрос «лучше ли dense
    отбирает кандидатов», а не «сколько стоит хранить векторы». Модель объявлена аргументом
    (``--dense-model``), потому что вектор без названных весов — не наблюдение.

    Запрос — те же термины, что у лексики (``terms``): сравниваются отборщики, а не запросы.
    """
    import numpy as np

    vectors: list[list[float]] = []
    total = len(chunks)
    for start in range(0, total, batch_size):
        batch = [chunk.text for chunk in chunks[start : start + batch_size]]
        vectors.extend(embedder.embed(batch))
        if progress is not None:
            progress(min(start + batch_size, total), total)
    matrix = np.asarray(vectors, dtype="float32")
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    matrix = matrix / np.where(norms == 0, 1.0, norms)
    ids = [chunk.chunk_id for chunk in chunks]

    def candidates(param: CatalogRow) -> list[str]:
        query = terms(param)
        if not query:
            return []
        vector = np.asarray(embedder.embed([" ".join(query)])[0], dtype="float32")
        norm = float(np.linalg.norm(vector)) or 1.0
        scores = matrix @ (vector / norm)
        order = np.argsort(-scores, kind="stable")[:top_k]
        return [ids[int(index)] for index in order]

    return candidates



def summarise(results: dict[str, tuple[str, str] | None]) -> dict[str, Any]:
    """Найдено / уникальных цитат / делится — три числа, по которым различаются слои.

    ``shared`` — список цитат, обслуживающих ≥2 параметра, с этими параметрами. Именно он
    отличает «цитата называет параметр» от «совпало слово»: делимость цитаты означает, что
    она не адресует ни одного из них.
    """
    found = {param: hit for param, hit in results.items() if hit is not None}
    by_quote: dict[str, list[str]] = {}
    for param, (_chunk_id, quote) in found.items():
        by_quote.setdefault(quote, []).append(param)
    shared = [
        {"quote": quote[:QUOTE_PREVIEW], "params": sorted(ids)}
        for quote, ids in by_quote.items()
        if len(ids) >= 2
    ]
    shared.sort(key=lambda item: (-len(item["params"]), item["quote"]))
    return {
        "params": len(results),
        "found": len(found),
        "unique_quotes": len(by_quote),
        "shared": shared,
        "unique_ratio": (len(by_quote) / len(found)) if found else None,
    }


def wilson(successes: int, total: int) -> tuple[float, float]:
    """Интервал Уилсона для доли: на малых ``n`` нормальное приближение врёт."""
    if total == 0:
        return (0.0, 1.0)
    z = 1.96
    p = successes / total
    denominator = 1 + z * z / total
    centre = (p + z * z / (2 * total)) / denominator
    half = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total)) / denominator
    return (max(0.0, centre - half), min(1.0, centre + half))


def judge(
    *,
    results: dict[str, tuple[str, str] | None],
    chunks: Sequence[ChunkText],
    params: Sequence[CatalogRow],
    model_id: str,
    sample: int,
    deploy_path: Path = Path("config/deploy.yaml"),
) -> dict[str, Any]:
    """Судит детерминированную выборку найденных цитат настоящим ``evaluate_cell``.

    Модель — объявленный аргумент: ``evaluate_cell`` не читает модель из окружения, и это
    правильно (I21), но означает, что вызывающий обязан её назвать.
    """
    from core.cli.wiring import build_evaluator_llm
    from core.config.deploy import load_deploy_config
    from core.pipeline.evaluate import evaluate_cell
    from core.ports.types import Chunk

    by_id = {chunk.chunk_id: chunk for chunk in chunks}
    by_param = {str(param.param_id): param for param in params}
    found = sorted(param for param, hit in results.items() if hit is not None)
    # Детерминированная выборка: S311 не про криптографию — нужен воспроизводимый порядок.
    random.Random(0).shuffle(found)  # noqa: S311
    picked = found[:sample]

    deploy = load_deploy_config(deploy_path)
    evaluator = build_evaluator_llm(deploy_llm=deploy.llm, slot=None, model_id=model_id)
    if evaluator is None:
        raise SystemExit("оценщик не собран: объявите endpoint (config/deploy.yaml) и ключ")

    rows: list[dict[str, Any]] = []
    for param_id in picked:
        chunk_id, quote = results[param_id]  # type: ignore[misc]
        source = by_id[chunk_id]
        # content_hash обязателен: без него валидация I19 отбивает любую ячейку со статусом
        # ✅, и отказ выглядит как свойство модели (замер 18.11.2026 — так и было).
        chunk = Chunk(
            chunk_id=chunk_id,
            chunk_no=0,
            text=quote,
            heading_path=source.heading_path or "",
            context_prefix="",
            content_hash=source.content_hash or "",
        )
        outcome = evaluate_cell(
            param=by_param[param_id],
            product_id="reference",
            run_id="measure_recon_matching",
            evaluator=evaluator,
            chunks=[chunk],
        )
        rows.append(
            {
                "param_id": param_id,
                "status": outcome.cell.status,
                "failure_kind": outcome.failure_kind,
                "quote": quote[:QUOTE_PREVIEW],
            }
        )
    ok = sum(1 for row in rows if row["status"] == "✅")
    low, high = wilson(ok, len(rows))
    return {
        "model_id": model_id,
        "sample": len(rows),
        "present": ok,
        "present_ci": [round(low, 4), round(high, 4)],
        "statuses": {
            key: sum(1 for row in rows if row["status"] == key) for key in ("✅", "⚠️", "❓", None)
        },
        "rows": rows,
    }


def _print_summary(label: str, stats: dict[str, Any]) -> None:
    ratio = "—" if stats["unique_ratio"] is None else f"{stats['unique_ratio']:.2f}"
    print(
        f"{label:28} нашёл {stats['found']:3}/{stats['params']}, "
        f"уникальных {stats['unique_quotes']:3}, делится {len(stats['shared']):3} "
        f"(доля {ratio})",
        flush=True,
    )
    for item in stats["shared"][:3]:
        print(f"    {', '.join(item['params'])}: {item['quote']}", flush=True)
    if len(stats["shared"]) > 3:
        print(f"    … и ещё {len(stats['shared']) - 3}", flush=True)


def _measure_dense_arm(
    *,
    params: Sequence[CatalogRow],
    chunks: Sequence[ChunkText],
    index: Bm25Index,
    top_k: int,
    deploy_path: Path,
    device: str,
    precision: str,
    judge_sample: int = 0,
    llm_model: str = "",
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Лексика против dense при **одном** запросе и **одном** зачёте.

    Сравнивать отборщики можно только на общем входе: запрос — ``parameter_terms``, зачёт —
    ``parameter_phrases`` (правило «фраза»). Иначе различие отборщиков смешивается с
    различием запросов, и вывод «dense хуже» ничего не значит — именно так и получилось
    18.11.2026, когда плечи мерились пробниками с разной токенизацией (48 против 44 на
    одном и том же запросе).

    Модель эмбеддера — **объявленная** в deploy (``embed.model_id`` + ``embed.revision``):
    вектор без названных весов не наблюдение, а флаг «возьми какую-нибудь модель» — ровно
    тот дефект, из-за которого ось ``embed_spec`` вообще появилась (план 26 O6).
    """
    from core.cli.wiring import (
        _resolve_embedder,
        resolve_embed_device,
        resolve_embed_precision,
        resolve_embed_spec,
    )
    from core.config.deploy import load_deploy_config

    deploy = load_deploy_config(deploy_path)
    spec = resolve_embed_spec(deploy.embed, None)
    if spec is None:
        raise SystemExit(
            "dense-плечо не собрано: в deploy не объявлена модель эмбеддера "
            "(embed.model_id/embed.revision) — объявите её, а не выбирайте на ходу"
        )
    chosen_device = device or resolve_embed_device(deploy.embed, None)
    chosen_precision = precision or resolve_embed_precision(deploy.embed, None)
    notices: list[str] = []
    embedder = _resolve_embedder(
        chosen_device,
        precision=chosen_precision,
        model_id=spec.model_id,
        revision=spec.revision,
        notices=notices,
    )
    if embedder is None:
        raise SystemExit("dense-плечо не собрано: " + ("; ".join(notices) or "нет загрузчика"))

    def progress(done: int, total: int) -> None:
        print(f"    векторы: {done}/{total} чанков", flush=True)


    lexical = measure(
        params=params,
        chunks=chunks,
        index=index,
        terms=parameter_terms,
        phrases=parameter_phrases,
        rule="phrase",
        top_k=top_k,
    )
    # Цена эмбеддинга — наблюдение, а не фон: без неё «включить векторы» обсуждается
    # без числа, а персист оценивается на глаз. Секундомер охватывает только векторы
    # чанков (запросы — доли секунды), поэтому в него не попадает лексическое плечо.
    started = time.monotonic()
    dense_cand = dense_candidates(
        params=params,
        chunks=chunks,
        embedder=embedder,
        terms=parameter_terms,
        top_k=top_k,
        progress=progress,
    )
    embed_seconds = time.monotonic() - started
    dense = _measure_with_candidates(
        params=params,
        chunks=chunks,
        candidates=dense_cand,
        phrases=parameter_phrases,
        rule="phrase",
    )
    print(
        f"    эмбеддинг: {embed_seconds:.1f} с на {len(chunks)} чанков "
        f"= {len(chunks) / embed_seconds:.1f} чанк/с",
        flush=True,
    )

    lexical_stats = summarise(lexical)
    dense_stats = summarise(dense)
    _print_summary("лексика (BM25)", lexical_stats)
    _print_summary("dense (косинус)", dense_stats)

    lexical_found = {param for param, hit in lexical.items() if hit is not None}
    dense_found = {param for param, hit in dense.items() if hit is not None}
    only_dense = sorted(dense_found - lexical_found)
    only_lexical = sorted(lexical_found - dense_found)
    print(
        f"    обе стороны: {len(dense_found & lexical_found)}; "
        f"только dense: {len(only_dense)}; только лексика: {len(only_lexical)}",
        flush=True,
    )
    for param in only_dense[:3]:
        print(f"      + dense {param}: {dense[param][1][:120] if dense[param] else ''}", flush=True)
    for param in only_lexical[:3]:
        print(f"      + лексика {param}: {lexical[param][1][:120] if lexical[param] else ''}", flush=True)

    judged: dict[str, Any] = {}
    if judge_sample:
        # Судья судит **оба** плеча на одном входе: иначе сравнение возвращается к счёту
        # найденного, а он, как показал §4, ничего не говорит о качестве цитаты.
        for name, results in (("лексика", lexical), ("dense", dense)):
            judged[name] = judge(
                results=results,
                chunks=chunks,
                params=params,
                model_id=llm_model,
                sample=judge_sample,
                deploy_path=deploy_path,
            )
            arm = judged[name]
            print(
                f"    судья {name}: ✅ {arm['present']}/{arm['sample']} "
                f"CI [{arm['present_ci'][0]:.3f};{arm['present_ci'][1]:.3f}] {arm['statuses']}",
                flush=True,
            )

    payload = {
        "embed_spec": f"{spec.model_id}@{spec.revision}",
        "embed_seconds": round(embed_seconds, 1),
        "embed_chunks": len(chunks),
        "device": str(chosen_device),
        "precision": str(chosen_precision),
        "notices": notices,
        "lexical": lexical_stats,
        "dense": dense_stats,
        "both": len(dense_found & lexical_found),
        "only_dense": only_dense,
        "only_lexical": only_lexical,
        "judge": judged,
    }
    return dense_stats, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, required=True, help="reference.db версии предка")
    parser.add_argument("--catalog", type=Path, required=True, help="catalog.csv профиля")
    parser.add_argument("--variants", default="code,en,phrase", help="слой запроса: code,en,phrase")
    parser.add_argument("--rules", default="", help="слой зачёта: any,all,phrase (пусто — не мерить)")
    parser.add_argument("--top-k", type=int, default=20, help="кандидатов на параметр (RECON_TOP_K)")
    parser.add_argument("--max-chunks", type=int, default=0, help="срез корпуса (0 — весь)")
    parser.add_argument("--judge-sample", type=int, default=0, help="сколько цитат судить (0 — не судить)")
    parser.add_argument("--llm-model", default="", help="модель судьи (обязательна при --judge-sample)")
    parser.add_argument("--dense", action="store_true", help="dense-плечо: объявленная в deploy модель эмбеддера")
    parser.add_argument("--dense-device", default="", help="устройство dense-плеча (по умолчанию — deploy)")
    parser.add_argument("--dense-precision", default="", help="точность dense-плеча (по умолчанию — deploy)")
    parser.add_argument("--deploy", type=Path, default=Path("config/deploy.yaml"), help="deploy-конфиг для осей dense")
    parser.add_argument("--out-json", type=Path, help="куда сложить числа")
    args = parser.parse_args(argv)

    chosen = [name.strip() for name in args.variants.split(",") if name.strip()]
    unknown = [name for name in chosen if name not in VARIANTS]
    if unknown:
        parser.error(f"неизвестный вариант: {unknown}; доступны {list(VARIANTS)}")
    chosen_rules = [name.strip() for name in args.rules.split(",") if name.strip()]
    unknown = [name for name in chosen_rules if name not in RULES]
    if unknown:
        parser.error(f"неизвестное правило: {unknown}; доступны {list(RULES)}")
    if args.judge_sample and not args.llm_model:
        parser.error("--judge-sample требует --llm-model: модель судьи — объявленный вход")

    params = load_catalog(args.catalog)
    chunks = read_chunks(args.db, args.max_chunks)
    if not chunks:
        raise SystemExit(f"корпус пуст: {args.db}")
    print(
        f"корпус: {len(chunks)} чанков, {len({chunk.doc_id for chunk in chunks})} документов, "
        f"параметров {len(params)}, top_k {args.top_k}",
        flush=True,
    )
    index = build_index(chunks)
    payload: dict[str, Any] = {
        "db": str(args.db),
        "catalog": str(args.catalog),
        "chunks": len(chunks),
        "docs": len({chunk.doc_id for chunk in chunks}),
        "params": len(params),
        "top_k": args.top_k,
        "engine": index.engine_version,
        "variants": {},
        "rules": {},
    }

    print("\n=== слой запроса (зачёт — по любому слову, как было в коде)")
    for name in chosen:
        variant = variants()[name]
        results = measure(
            params=params,
            chunks=chunks,
            index=index,
            terms=variant.terms,
            phrases=variant.phrases,
            rule=variant.rule,
            top_k=args.top_k,
        )
        stats = summarise(results)
        _print_summary(name, stats)
        payload["variants"][name] = stats
        if args.judge_sample:
            payload["variants"][name]["judge"] = judge(
                results=results,
                chunks=chunks,
                params=params,
                model_id=args.llm_model,
                sample=args.judge_sample,
                deploy_path=args.deploy,
            )

    if chosen_rules:
        print("\n=== слой зачёта (запрос один и тот же: parameter_terms)")
        for rule in chosen_rules:
            results = measure(
                params=params,
                chunks=chunks,
                index=index,
                terms=parameter_terms,
                phrases=parameter_phrases,
                rule=rule,
                top_k=args.top_k,
            )
            stats = summarise(results)
            _print_summary(rule, stats)
            payload["rules"][rule] = stats
            if args.judge_sample:
                payload["rules"][rule]["judge"] = judge(
                    results=results,
                    chunks=chunks,
                    params=params,
                    model_id=args.llm_model,
                    sample=args.judge_sample,
                )

    if args.dense:
        print(
            "\n=== слой отбора (запрос и зачёт одни: parameter_terms + правило «фраза»)",
            flush=True,
        )
        _dense_stats, dense_payload = _measure_dense_arm(
            params=params,
            chunks=chunks,
            index=index,
            top_k=args.top_k,
            deploy_path=args.deploy,
            device=args.dense_device,
            precision=args.dense_precision,
            judge_sample=args.judge_sample,
            llm_model=args.llm_model,
        )
        payload["dense"] = dense_payload

    if args.out_json:
        args.out_json.parent.mkdir(parents=True, exist_ok=True)
        args.out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"\nчисла: {args.out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
