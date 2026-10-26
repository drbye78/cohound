"""coHound — фаза оценки Ш6: ретривал → промпт → оценщик → эшелон валидации (T9.3, §5/§7.1).

Одна ячейка ``(product × param)`` проходит детерминированный конвейер:

```
queries = render_queries(param)            # ≤3 запроса, S13/I16
chunks  = caller-retrieval(product-scoped, top-K)   # I17/I20 — вне этого модуля
prompt  = compile_prompt(param, product, chunks, satellites)   # T9.1
resp    = evaluator.complete(LLMRequest(prompt, LLMCellOutput, ...))   # I18: без веб-тула
valid   = validate_output(resp.output, chunks_by_id, ...)              # T9.4/§10
strict:      passed → evaluated · не прошёл → failed (retry ≤2)
preliminary: passed → evaluated · не прошёл → preliminary-кандидат
```

**Границы (I16/I17/I18/I20).** Ретривал выполняет **вызывающий**: ``evaluate_cell``
принимает готовые ``chunks`` — продукт-скоупед top-K из :func:`core.pipeline.retrieve.retrieve`
с **зафиксированным** профилем и ``retrieval_cutoff_K`` (I16/I20). Оценщик физически не
вызывает внешний поиск (I18): порт ``EvaluatorPort`` не несёт ``tools``. Тонкий композер
:func:`retrieve_for_cell` собирает конкретный путь I16/I17/I20 (queries → retrieve per query
→ union/dedup → обрезка до K), когда оркестратору удобно делегировать именно это.

**Retry ≤ ``max_retries`` (по умолчанию 2).** Повтор переиздаёт **тот же** запрос: промпт
механический и детерминированный, поэтому для детерминированного оценщика повтор —
**ограниченный no-op** и полезен ровно для транзиентных ошибок адаптера (сеть/сервер).
Сбой вызова → ``evaluation_failed``; сбой цитатной валидации → ``validation_failed``.
Исчерпание в strict → ``failed`` c ``status=None`` (**I4**: failed не несёт статус); в
preliminary → ``preliminary``-кандидат c ``status=None`` (**I13**).

**Провенанс промпта.** Модель ``Cell`` (``core/domain/models.py``) не несёт поля
``prompt_hash`` (колонка есть в БД, но не в доменной модели). Поэтому ``evaluate_cell``
возвращает :class:`EvaluationOutcome` — ``cell`` **плюс** ``prompt_hash``/``validation``/
``failure_kind``. Это осознанное расширение подписей из 01 §4: иначе канонический
``prompt_hash`` (шаблон + rule + absence_rule + sorted chunk_ids, §5) терялся бы.

Модуль категорийно нейтрален (§1.3), без сети, без новых зависимостей.
"""

from __future__ import annotations

import datetime as _dt
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from core.domain import (
    EVAL_SCHEMA_VERSION,
    AbsenceRuleType,
    CatalogRow,
    Cell,
    CellState,
    CorpusCoverage,
    DenialClaim,
    DenialStrength,
    EvidenceScope,
    EvidenceType,
    FailureKind,
    LLMCellOutput,
    Satellite,
    SourceKind,
    Status,
)
from core.domain.anchors import SourceResolver
from core.domain.models import absence_evidence_id
from core.pipeline.eval_cache import (
    EVAL_CACHE_FORMAT,
    EvalVerdictCache,
    build_eval_key,
)
from core.pipeline.exhaustion import ExhaustionFacts, build_protocols
from core.pipeline.precedence import (
    PRECEDENCE_REVIEW_REASON,
    collect_protected_cells,
    resolve_precedence,
)
from core.pipeline.prompt import CompiledPrompt, compile_prompt, render_queries
from core.pipeline.retrieve import (
    DEFAULT_RRF_K,
    RetrievalProfile,
    RetrievalResult,
    retrieve,
)
from core.pipeline.runner import PhaseResult, Ports, run_isolated
from core.pipeline.validate import ChunkLike, ValidationResult, validate_output
from core.ports.embedder import EmbedderPort
from core.ports.lexical_index import LexicalIndexPort
from core.ports.llm import EvaluatorPort, LLMRequest, LLMResponse
from core.ports.store import StorePort
from core.ports.vector_index import (
    RetrievedChunk,
    VectorIndexPort,
    VectorQuery,
    batched_search,
)
from core.util.hashing import content_hash

__all__ = [
    "ABSENCE_UNATTESTED_REVIEW_REASON",
    "CORPUS_INCOMPLETE_REVIEW_REASON",
    "DEFAULT_QUERY_EMBED_BATCH",
    "DEFAULT_RETRIEVAL_CUTOFF_K",
    "DEFAULT_RETRIEVAL_PROFILE",
    "EvaluationOutcome",
    "evaluate_batch",
    "evaluate_cell",
    "evaluate_phase",
    "precompute_dense_hits",
    "precompute_query_vectors",
    "retrieve_for_cell",
    "run_queries_for_cell",
    "unique_queries",
]

#: Ф0 → Вариант 2: машинный токен причины, по которой класс отрицания **понижен** до
#: ``not_stated``/``unobserved``: неполное покрытие корпуса (охват не наблюдался целиком).
CORPUS_INCOMPLETE_REVIEW_REASON = "corpus_incomplete_absence"

#: Токен причины, по которой ``bounded``-отрицание не достигнуто: нет принятия риска
#: (``scope_acceptance``). Отсутствие принятия — процедурное условие, а не сбой прибора
#: (I4), поэтому исход — ``❓`` с этой причиной, а не ``failed``: ``❓`` остаётся в карте,
#: ``failed`` выпадает из неё целиком.
ABSENCE_UNATTESTED_REVIEW_REASON = "absence_unattested"

#: Токен причины, по которой ``❌``-претензия модели не стала ``❌``: заявлен ``explicit``,
#: но цитаты нет. Отказ направлен наружу (ложное «нет» уезжает в карту как факт), поэтому
#: вердикт понижается до ``❓``, а не публикуется.
EXPLICIT_WITHOUT_CITATION_REASON = "explicit_without_citation"

#: Токен причины, по которой ``❌``-претензия модели без цитаты осталась ``not_stated``:
#: «материал релевантен, но дословного утверждения нет» — это незнание, не отсутствие.
ABSENT_CLAIM_WITHOUT_DENIAL_REASON = "absent_claim_without_denial_citation"

#: ``seq`` записи протокола отсутствия в ``evidence_id``. Фиксирован (а не «после цитат»),
#: чтобы повторная оценка ячейки перезаписывала ту же строку ``cell_evidence``.
_PROTOCOL_ABSENCE_SEQ = 0

#: Максимум запросов на ячейку (S13/I16): title + концепт-термины ru → en.
MAX_QUERIES = 3

#: Дефолтный профиль ретривала фазы оценки, если оркестратор его не задал (I16).
#: В проде профиль фиксируется в ``RunConfig``/манифесте; здесь — безопасный baseline.
DEFAULT_RETRIEVAL_PROFILE: RetrievalProfile = RetrievalProfile.R0

#: Дефолтный ``retrieval_cutoff_K`` фазы оценки, если оркестратор его не задал (I20).
#: В проде K фиксируется в ``RunConfig``/манифесте; здесь — нейтральный baseline.
DEFAULT_RETRIEVAL_CUTOFF_K: int = 10

#: Размер батча эмбеддинга запросов (тексты запросов, не чанки корпуса).
#:
#: Число — измеренное, а не «круглое»: на локальном эмбеддере один текст стоит 192 мс,
#: три — 98.7 мс на текст, батч ×32 — 54.1 мс на текст (doc 14 §5). Дальше 32 выигрыш
#: упирается в цену самого текста, а риск частичного ответа растёт (внешний эмбеддер
#: начнёт резать батч сам — оговорка 4 там же). Параметр, а не константа поведения.
DEFAULT_QUERY_EMBED_BATCH: int = 32

#: Дефолтный ``model_id``, если оценщик не объявил свой (данные, не решение LLM, I21).
_UNSPECIFIED_MODEL_ID = "unspecified"

#: Источник ячейки, оценённой по корпусу (§5/Ш6) — не реестр и не наследование.
_CELL_SOURCE_KIND = SourceKind.DOCUMENTATION


@dataclass
class EvaluationOutcome:
    """Результат оценки ячейки: сама ячейка + провенанс, не помещающийся в ``Cell``.

    :param cell: доменная ячейка (``evaluated``/``failed``/``preliminary``, I4/I13).
    :param prompt_hash: канонический хеш промпта (``CompiledPrompt.prompt_hash``, §5).
    :param validation: результат эшелона §10 (заметки/восстановленные evidence).
    :param failure_kind: вид сбоя при ``failed`` (``validation_failed``/``evaluation_failed``);
        ``None`` для успеха и preliminary-кандидата. Дублируется здесь, потому что модель
        ``Cell`` не имеет поля ``failure_kind`` (колонка БД заполняется на W10).
    """

    cell: Cell
    prompt_hash: str
    validation: ValidationResult
    failure_kind: str | None = None
    raw: str | None = field(default=None)


# ---------------------------------------------------------------------------
# Запросы (S13)
# ---------------------------------------------------------------------------


def run_queries_for_cell(param: CatalogRow, max_queries: int = MAX_QUERIES) -> list[str]:
    """Возвращает ≤ ``max_queries`` запросов Ш6 для параметра (S13/I16).

    Делегирует :func:`core.pipeline.prompt.render_queries`: порядок детерминирован
    (``title`` + термины ru → en → прочие), состав не зависит от LLM/реализации.
    """
    return render_queries(param, max_queries=max_queries)


# ---------------------------------------------------------------------------
# Ретривал-композер (I16/I17/I20)
# ---------------------------------------------------------------------------


def _as_profile(profile: RetrievalProfile | str) -> RetrievalProfile:
    """Нормализует профиль к :class:`RetrievalProfile` (принимает ``"R0"``/``"r0"``)."""
    if isinstance(profile, RetrievalProfile):
        return profile
    try:
        return RetrievalProfile(str(profile).upper())
    except ValueError as exc:  # pragma: no cover - защита от опечатки конфигурации
        raise ValueError(f"неизвестный профиль ретривала: {profile!r}") from exc


def _query_vector(
    query: str,
    query_vectors: Mapping[str, Sequence[float]] | None,
    embedder: EmbedderPort | None,
) -> list[float] | None:
    """Вектор запроса для dense-плеча: из карты либо через ``embedder`` (иначе ``None``).

    В фазе оценки карта заполнена пре-компьютом (:func:`precompute_query_vectors`),
    поэтому ``embedder`` здесь — путь **вне** фазы: одиночный вызов на текст.
    """
    if query_vectors is not None and query in query_vectors:
        return list(query_vectors[query])
    if embedder is not None:
        vectors = embedder.embed([query])
        if not vectors:
            raise ValueError("embedder вернул пустой список векторов")
        return list(vectors[0])
    return None


def unique_queries(
    params: Sequence[CatalogRow], *, max_queries: int = MAX_QUERIES
) -> list[str]:
    """Уникальные тексты запросов пакета в детерминированном порядке первого появления.

    ``render_queries`` даёт ≤3 запроса на параметр, и тексты **повторяются**: у когорты
    один и тот же запрос эмбеддится по разу на продукт, а векторы посчитаны бы 900 000
    раз вместо ≤600 на пакет (doc 14 §5, оговорка 2). Порядок первого появления —
    чтобы батч был воспроизводим (A3/R7).
    """
    ordered: dict[str, None] = {}
    for param in params:
        for query in run_queries_for_cell(param, max_queries=max_queries):
            ordered.setdefault(query, None)
    return list(ordered)


def precompute_query_vectors(
    params: Sequence[CatalogRow],
    *,
    profile: RetrievalProfile | str,
    embedder: EmbedderPort | None,
    memo: dict[str, list[float]] | None = None,
    batch_size: int = DEFAULT_QUERY_EMBED_BATCH,
    max_queries: int = MAX_QUERIES,
) -> dict[str, list[float]] | None:
    """Векторы всех уникальных запросов пакета — **до** цикла оценки, батчами.

    Зачем до цикла: ``_query_vector`` без карты зовёт ``embedder.embed([query])`` — по
    одному тексту на запрос и внутри оценки. При внешнем эмбеддере это 900 000 HTTP
    round-trip на прогон (doc 14 §5: 192 мс одиночный против 54 мс батчем ×32), а
    главное — оценщик перестаёт быть офлайн-чистым по построению: он не ходит за
    векторами вовсе, векторы приходят объявленным входом.

    Мемоизация по тексту — ``memo``: карта ``text → vector`` живёт в пределах прогона,
    поэтому второй вызов на том же пакете (или второй продукт когорты) не эмбеддит
    ничего. Порядок первого появления текстов сохраняется: состав батчей детерминирован.

    :param profile: профиль ретривала; для ``R0`` (только лексика) векторы не нужны —
        возвращается ``None``, и фаза не трогает эмбеддер.
    :param embedder: ``None`` — пре-компьют невозможен: возвращается ``None``, дальше
        работает прежний путь (и его ошибки видны в ячейках, как раньше).
    :param batch_size: размер батча ``embed`` (замер: 54.1 мс на 32 текста против 192 мс
        на один — doc 14 §5).
    :param memo: карта-накопитель ``text → vector``; пополняется и переиспользуется.
    :raises ValueError: если эмбеддер вернул не столько векторов, сколько текстов.
    """
    if _as_profile(profile) is RetrievalProfile.R0 or embedder is None:
        return None
    store = memo if memo is not None else {}
    texts = unique_queries(params, max_queries=max_queries)
    missing = [text for text in texts if text not in store]
    if batch_size < 1:
        raise ValueError(f"batch_size обязан быть ≥ 1, получено {batch_size!r}")
    for start in range(0, len(missing), batch_size):
        batch = missing[start : start + batch_size]
        vectors = embedder.embed(batch)
        if len(vectors) != len(batch):
            raise ValueError(
                f"эмбеддер вернул {len(vectors)} векторов на {len(batch)} запросов"
            )
        for text, vector in zip(batch, vectors):
            store[text] = list(vector)
    return {text: store[text] for text in texts}


def precompute_dense_hits(
    params: Sequence[CatalogRow],
    *,
    product_ids: Sequence[str],
    profile: RetrievalProfile | str,
    cutoff_k: int,
    vector_index: VectorIndexPort | None,
    query_vectors: Mapping[str, Sequence[float]] | None,
    max_queries: int = MAX_QUERIES,
) -> dict[str, list[RetrievedChunk]] | None:
    """Dense-плечо для **всех** запросов пакета в продукте — одним батч-вызовом порта.

    Это п.3 doc 14 §6: 600 отдельных ``search`` — это 600 чтений партиции и 600
    BLAS-вызовов, тогда как батч читает матрицу один раз (замер: 88 мс против 2 695 мс).
    ``pipeline`` при этом не знает ни про файлы, ни про формат: он зовёт
    :func:`core.ports.vector_index.batched_search` по порту (I17: ``product_ids`` —
    те же, что у поштучного пути).

    :param product_ids: партиции запроса (продукт и, если вызывающий их добавил,
        сателлиты — I17).
    :param vector_index: ``None`` — батчить нечего, возвращается ``None`` (прежний путь).
    :param query_vectors: векторы запросов; запрос без вектора остаётся поштучному пути
        (``None`` вместо подставленного нуля: выдуманный вектор — не измерение).
    :returns: карта ``query → hits``; ``None`` — батч неприменим.
    """
    if vector_index is None or query_vectors is None:
        return None
    if _as_profile(profile) is RetrievalProfile.R0:
        return None
    queries: list[str] = []
    requests: list[VectorQuery] = []
    for text in unique_queries(params, max_queries=max_queries):
        vector = query_vectors.get(text)
        if vector is None:
            continue
        queries.append(text)
        # Запрос формируется ровно так же, как поштучный путь (``retrieve``):
        # ``VectorQuery`` без ``text``. Иначе батч спрашивал бы порт о другом запросе,
        # и «батч = поштучно» перестало бы быть проверяемым свойством.
        requests.append(
            VectorQuery(product_ids=list(product_ids), top_k=cutoff_k, vector=list(vector))
        )
    if not requests:
        return None
    hits = batched_search(vector_index, requests)
    return {text: list(batch) for text, batch in zip(queries, hits)}


def retrieve_for_cell(
    param: CatalogRow,
    *,
    product_id: str,
    profile: RetrievalProfile | str,
    cutoff_k: int,
    product_ids: Sequence[str] | None = None,
    lexical_index: LexicalIndexPort | None = None,
    vector_index: VectorIndexPort | None = None,
    embedder: EmbedderPort | None = None,
    query_vectors: Mapping[str, Sequence[float]] | None = None,
    dense_by_query: Mapping[str, Sequence[RetrievedChunk]] | None = None,
    rrf_k: int = DEFAULT_RRF_K,
    lang: str | None = None,
    max_queries: int = MAX_QUERIES,
) -> RetrievalResult:
    """Конкретный путь I16/I17/I20: ``render_queries`` → ``retrieve`` по каждому запросу.

    Каждый запрос идёт по **одному** профилю и ``cutoff_k`` (I16), в партиции
    ``product_ids`` (по умолчанию ``[product_id]``; сателлиты добавляет вызывающий, I17).
    Результаты объединяются и дедуплицируются по ``chunk_id``: побеждает лучший скор
    (tie-break по ``chunk_id``, R7), затем список обрезается до ``cutoff_k`` (I20).

    ``build_id`` берётся из первого непустого результата — это сборка, к которой
    привязан срез ретривала (S5/R15). Для R1/R2 нужен dense-вектор: он берётся из
    ``query_vectors`` по тексту запроса либо вычисляется ``embedder``'ом; иначе
    :func:`retrieve` возбуждает ``ValueError``.

    :param dense_by_query: готовые dense-результаты по тексту запроса
        (:func:`precompute_dense_hits`). ``None`` — прежний путь: поиск идёт поштучно.
        Готовый результат используется только для своего запроса, поэтому склейка
        запросов ячейки (union/dedup/срез) остаётся здесь же и не меняется.
    """
    resolved_profile = _as_profile(profile)
    products = list(product_ids) if product_ids is not None else [product_id]
    queries = run_queries_for_cell(param, max_queries=max_queries)

    best: dict[str, RetrievedChunk] = {}
    build_id = ""
    for query in queries:
        vector = None
        dense_hits = None if dense_by_query is None else dense_by_query.get(query)
        if resolved_profile in (RetrievalProfile.R1, RetrievalProfile.R2):
            if dense_hits is None:
                vector = _query_vector(query, query_vectors, embedder)
        result = retrieve(
            query_text=query,
            query_vector=vector,
            product_ids=products,
            profile=resolved_profile,
            cutoff_k=cutoff_k,
            lexical_index=lexical_index,
            vector_index=vector_index,
            dense_hits=dense_hits,
            rrf_k=rrf_k,
            lang=lang,
        )
        if not build_id and result.build_id:
            build_id = result.build_id
        for chunk in result.chunks:
            current = best.get(chunk.chunk_id)
            if current is None or chunk.score > current.score:
                best[chunk.chunk_id] = chunk

    ordered = sorted(best.values(), key=lambda c: (-c.score, c.chunk_id))[:cutoff_k]
    return RetrievalResult(
        chunks=ordered,
        profile=resolved_profile.value,
        cutoff_K=cutoff_k,
        build_id=build_id,
    )


# ---------------------------------------------------------------------------
# Провенанс ячейки из результатов ретривала
# ---------------------------------------------------------------------------


def _provenance(
    retrievals: RetrievalResult | Sequence[RetrievalResult] | None,
) -> tuple[str | None, str | None, str | None]:
    """Извлекает ``(index_build_id, retrieval_profile, retrieval_params_json)`` ячейки.

    Сборка/профиль/K — часть провенанса ячейки (R7/I16/I20): по ним детектируется
    устаревание при смене сборки или профиля. Если результатов нет — провенанс пуст.
    """
    if retrievals is None:
        return None, None, None
    items = [retrievals] if isinstance(retrievals, RetrievalResult) else list(retrievals)
    if not items:
        return None, None, None
    build_id = next((r.build_id for r in items if r.build_id), None)
    first = items[0]
    params = json.dumps(
        {"profile": first.profile, "cutoff_K": first.cutoff_K},
        ensure_ascii=False,
        sort_keys=True,
    )
    return build_id, (first.profile or None), params


# ---------------------------------------------------------------------------
# Отсутствие (Вариант 2: сила отрицания)
# ---------------------------------------------------------------------------


def _absence_dict(ev) -> dict:  # noqa: ANN001 - CellEvidence, без импорта ради читаемости
    """Сериализует ``CellEvidence`` в элемент ``absence_evidence`` (R4/§4.2)."""
    return {
        "evidence_id": ev.evidence_id,
        "source_id": ev.source_id,
        "source_url": ev.source_url,
        "citation_quote": ev.citation_quote,
        "content_hash": ev.content_hash,
        "source_kind": ev.source_kind.value,
        # Модельная запись — цитата, а не протокол отсутствия: в ``cell_evidence`` она уже
        # лежит как ``direct`` (её пишет ``_persist``), и дублировать её как ``absence``
        # значило бы переписать ту же строку другим ``evidence_type``.
        "evidence_type": EvidenceType.DIRECT.value,
        # ``source_scope`` — устаревший скаляр: новый писатель его не выставляет, вопрос
        # решает ``EvidenceScope`` ячейки. Поле остаётся ради чтения исторических строк.
        "source_scope": ev.source_scope.value if ev.source_scope else None,
        "scope_acceptance": ev.scope_acceptance,
        # R7/09 §8.1: та же привязка, что у ``cell_evidence`` — композиция прогона.
        "index_build_id": ev.index_build_id,
    }


def _coverage_vector(coverage: CorpusCoverage | None) -> dict[str, Any]:
    """Вектор охвата из **наблюдённого** покрытия корпуса (факты, не доля).

    Покрытие публикуется как набор наблюдённых величин, а не как «исчерпан ли охват»:
    ``robots_disallow=4`` читается и проверяется, булево ``exhaustive`` — нет. Долю
    «прочитано N из M» даёт только :attr:`EvidenceScope.url_observation_ratio`, и она
    называется мерой обработанных URL, а не вероятностью отсутствия.
    """
    if coverage is None:
        return {}
    return {
        "observed": coverage.observed_sources,
        "expected": coverage.expected_sources,
        "blocked_by_reason": dict(sorted(coverage.blocked.items())),
        "unreadable_sources": list(coverage.unreadable_origins),
    }


def _denial_basis(scope_vector: dict[str, Any], coverage: CorpusCoverage | None) -> str:
    """Класс ``unobserved``/``not_stated`` по наблюдённому охвату — код решает, не модель.

    Охват неполон → ``unobserved`` (часть материала не наблюдалась: это факт о приборе).
    Охват полон (или неизвестен) → ``not_stated`` (вендор не заявил; измерение при этом
    не доказало, что заявлять было нечего).
    """
    if coverage is not None and not coverage.complete:
        return DenialStrength.UNOBSERVED.value
    return DenialStrength.NOT_STATED.value


def _resolve_denial(
    *,
    param: CatalogRow,
    out: LLMCellOutput,
    validation: ValidationResult,
    coverage: CorpusCoverage | None,
    scope_acceptance: str | None,
    composition_id: str | None,
) -> tuple[EvidenceScope | None, list[str]]:
    """Строит :class:`EvidenceScope` для ``❌``/``❓`` — **единственное** место вердикта.

    Правила (Вариант 2):

    * ``explicit`` — только с непустой цитатой подтверждённого фрагмента. Заявленный
      ``explicit`` без цитаты — отказ: класс понижается до ``not_stated`` (``❓``), и
      причина остаётся видимой. Это и лечит живой дефект: модель говорила «отсутствует»
      там, где вендор заявил свойство, но не дословно.
    * ``bounded`` — только для параметров, чей ``absence_rule`` объявляет конечный
      справочник (``parameter_reference``/``distribution_absence``), и только при
      принятии риска человеком (``scope_acceptance``). Иначе — ``not_stated``.
    * ``unobserved``/``not_stated`` — ``❓`` с причиной; класс выбирается по
      наблюдённому охвату, а не по самоотчёту модели.
    * ``authoritative`` здесь **не** выставляется никогда: он приходит от реестра.

    :returns: ``(scope, notes)``: объект силы отрицания и диагностические заметки.
    """
    vector = _coverage_vector(coverage)
    quotes = [
        ev.citation_quote
        for ev in validation.resolved_evidence
        if (ev.citation_quote or "").strip()
    ]
    declared = out.denial_strength
    claim_text = (out.denial_reason or out.justification or "").strip()
    basis_prefix = "evaluator"

    if out.status is Status.ABSENT:
        # Согласие модели здесь — объявленный класс, а не факт наличия цитаты. Цитата
        # «ближайшего» фрагмента (самый частый ответ) доказывает, что фрагмент прочитан,
        # но **не** что вендор отрицает: именно эта подмена дала ложные «нет» в живом
        # контроле (2 из 12 ячеек). Поэтому без объявленного ``explicit`` цитата остаётся
        # наблюдением, а не отрицанием.
        wants_explicit = declared is DenialClaim.EXPLICIT
        if wants_explicit:
            if quotes:
                return (
                    EvidenceScope(
                        denial_strength=DenialStrength.EXPLICIT,
                        citation_quote=quotes[0],
                        reason=claim_text or "прямое отрицание вендора (цитата приложена)",
                        basis=f"{basis_prefix}:explicit",
                        **vector,
                    ),
                    [],
                )
            # Заявлен explicit — цитаты нет. Отказ, а не публикация.
            return (
                EvidenceScope(
                    denial_strength=DenialStrength.NOT_STATED,
                    reason=(
                        f"{EXPLICIT_WITHOUT_CITATION_REASON}: модель заявила explicit без "
                        "цитаты — отрицание считается недоказанным"
                    ),
                    basis=f"{basis_prefix}:explicit_rejected",
                    **vector,
                ),
                [
                    "explicit без цитаты отклонён: вердикт понижен до ❓ "
                    "(недоказанное «нет» не публикуется)"
                ],
            )
        # Нет цитаты: класс зависит от объявленного правила и принятия риска.
        finite_reference = param.absence_rule in {
            AbsenceRuleType.PARAMETER_REFERENCE,
            AbsenceRuleType.DISTRIBUTION_ABSENCE,
        }
        if finite_reference and (scope_acceptance or "").strip():
            return (
                EvidenceScope(
                    denial_strength=DenialStrength.BOUNDED,
                    reason=(
                        claim_text
                        or "конечный справочник объявлен каталогом; процедура полноты "
                        "действует, риск принят"
                    ),
                    enumerated=True,
                    scope_acceptance=scope_acceptance,
                    basis="rule:bounded",
                    **vector,
                ),
                [],
            )
        strength = _denial_basis(vector, coverage)
        reason = claim_text or ABSENT_CLAIM_WITHOUT_DENIAL_REASON
        if finite_reference:
            # Конечный справочник без принятия риска — процедурное незнание.
            reason = f"{ABSENCE_UNATTESTED_REVIEW_REASON}: {reason}"
        else:
            reason = f"{ABSENT_CLAIM_WITHOUT_DENIAL_REASON}: {reason}"
        if strength == DenialStrength.UNOBSERVED.value:
            # Причина, которую читатель видит первой: пространство не наблюдалось.
            reason = f"{CORPUS_INCOMPLETE_REVIEW_REASON}: {reason}"
        return (
            EvidenceScope(
                denial_strength=DenialStrength(strength),
                reason=reason,
                basis=f"{basis_prefix}:absent_without_denial",
                **vector,
            ),
            [
                "❌ без объявленного класса отрицания понижен до ❓: цитата сама по себе "
                f"не является отрицанием (класс {strength})"
            ],
        )

    if out.status is Status.UNKNOWN:
        strength = _denial_basis(vector, coverage)
        reason = claim_text or "модель не установила факт"
        if strength == DenialStrength.UNOBSERVED.value:
            reason = f"{CORPUS_INCOMPLETE_REVIEW_REASON}: {reason}"
        if quotes:
            # Цитата при ❓ — не аргумент отсутствия, но объясняет, что материал найден.
            reason = f"{reason} (найденные фрагменты: {len(quotes)})"
        return (
            EvidenceScope(
                denial_strength=DenialStrength(strength),
                reason=reason,
                basis=f"{basis_prefix}:unknown",
                **vector,
            ),
            [],
        )

    return None, []


def _build_absence_evidence(
    param: CatalogRow,
    validation: ValidationResult,
    *,
    run_id: str,
    product_id: str,
    scope: EvidenceScope,
    composition_id: str | None = None,
) -> list[dict]:
    """Строит ``absence_evidence`` для ``❌`` — протокол отсутствия (R4/§4.2).

    Запись-протокол добавляется **всегда** (даже когда модель дала цитату): именно она
    переживает персистенцию и делает ``❌`` видимым отчёту и ревью. Модельные цитаты
    остаются обычным ``evidence`` (``evidence_type=direct``), поэтому в ``absence_evidence``
    дублируется только протокол.

    Принятие риска (``scope_acceptance``) пишется и на протокол: опубликованный ``❌``
    класса ``bounded`` обязан быть прослеживаем к человеку. Вердикт при этом определяется
    **объектом** (:class:`EvidenceScope`), а не наличием подписи в записи.

    Класс отрицания живёт **в объекте ячейки** (``cells.evidence_scope``) — единственный
    источник истины, который переживает персистенцию. Ключи ``denial_strength``/
    ``denial_basis``/``note`` в записи ниже — аннотация для читателя в памяти процесса
    (они не имеют колонок в ``cell_evidence`` и при записи отбрасываются, как и раньше).

    :param scope: объект силы отрицания — источник причины/класса для протокола.
    """
    entries = [_absence_dict(ev) for ev in validation.resolved_evidence]
    entries.append(
        {
            "evidence_id": absence_evidence_id(
                run_id, product_id, param.param_id, _PROTOCOL_ABSENCE_SEQ
            ),
            "run_id": run_id,
            "product_id": product_id,
            "param_id": param.param_id,
            "source_id": None,
            "source_url": None,
            "citation_quote": None,
            "content_hash": None,
            "source_kind": None,
            "evidence_type": EvidenceType.ABSENCE.value,
            "scope_acceptance": scope.scope_acceptance,
            "index_build_id": composition_id,
            "denial_strength": scope.denial_strength.value,
            "denial_basis": scope.basis,
            "note": (
                "протокол отсутствия: "
                f"класс={scope.denial_strength.value}, "
                f"основание={scope.basis or '—'}; охват observed/expected="
                f"{scope.observed}/{scope.expected}, "
                f"blocked={scope.blocked_by_reason or '{}'}"
            ),
        }
    )
    return entries


def _coverage_for(
    coverage: CorpusCoverage | None,
    store: StorePort | None,
    run_id: str,
    product_id: str,
    *,
    planned: bool = False,
) -> CorpusCoverage | None:
    """Покрытие продукта: явный аргумент либо чтение из store.

    Для продукта **плана** отсутствие строки — аномалия (O3), а не «покрытие
    неизвестно»: возвращается synthetic zero-source строка с
    ``reason=coverage_missing``, которая запрещает ``❌`` и даёт failed-ячейки.
    """
    if coverage is not None:
        return coverage
    reader = getattr(store, "read_corpus_coverage", None)
    if callable(reader):
        try:
            found = reader(run_id, product_id)
        except Exception:  # noqa: BLE001 — чтение покрытия не должно ронять оценку
            found = None
        if found is not None:
            return found
    if planned:
        return CorpusCoverage(
            product_id=product_id,
            complete=False,
            expected_sources=0,
            observed_sources=0,
            computed_at=_dt.datetime.now(_dt.timezone.utc).isoformat(),
            reason=FailureKind.COVERAGE_MISSING.value,
        )
    return None


def _record_corpus_coverage(
    absence_evidence: list[dict],
    coverage: CorpusCoverage,
    *,
    accepted_by: str | None = None,
) -> None:
    """Дописывает в absence-evidence аудит блокировок корпуса (Ф0 → Вариант 2).

    Запись машинно-читаема (``corpus_blocked``/``corpus_complete``/счётчики) и остаётся
    в ячейке, поэтому отчёт видит факт неполного покрытия без чтения БД. Вердикт от неё
    больше **не** зависит: неполнота охвата выражается классом ``unobserved`` (``❓``) в
    :class:`EvidenceScope`, а не запретом публиковать ``❌``. Аудит сохраняется ради отчёта
    и аудита, а не как условие гейта.
    """
    accepted_by = accepted_by or next(
        (
            entry.get("scope_acceptance")
            for entry in absence_evidence
            if entry.get("scope_acceptance")
        ),
        None,
    )
    absence_evidence.append(
        {
            "evidence_id": None,
            "source_id": None,
            "scope_acceptance": accepted_by,
            "note": (
                "Ф0: неполное покрытие корпуса — блокирующие сбои: "
                f"{coverage.blocking_failures or '{}'}; "
                f"нечитаемые origin'ы: {list(coverage.unreadable_origins) or '[]'}; "
                "класс отрицания зависит от этого охвата (unobserved при отсутствии "
                "прямого отрицания/реестра)"
            ),
            "corpus_complete": False,
            "corpus_expected_sources": coverage.expected_sources,
            "corpus_observed_sources": coverage.observed_sources,
            "corpus_blocked": dict(sorted(coverage.blocked.items())),
            "corpus_blocking_failures": dict(coverage.blocking_failures),
            "corpus_unreadable_origins": list(coverage.unreadable_origins),
            "corpus_partial_blocks": {
                origin: dict(sorted(counts.items()))
                for origin, counts in sorted(coverage.partial_blocks.items())
            },
            "corpus_truncated": coverage.truncated,
        }
    )


# ---------------------------------------------------------------------------
# Оценка ячейки
# ---------------------------------------------------------------------------


def _evaluated_cell(
    *,
    out_status: Status,
    evidence,
    absence_evidence: list[dict],
    param: CatalogRow,
    product_id: str,
    run_id: str,
    provenance: tuple[str | None, str | None, str | None],
    evidence_scope: EvidenceScope | None = None,
) -> Cell:
    """Собирает успешную ``evaluated``-ячейку (§2.2; I1/I2/I3/I4/I13-совместимую)."""
    index_build_id, retrieval_profile, retrieval_params_json = provenance
    return Cell(
        run_id=run_id,
        product_id=product_id,
        param_id=param.param_id,
        state=CellState.EVALUATED,
        status=out_status,
        source_kind=_CELL_SOURCE_KIND,
        absence_rule=param.absence_rule,
        evidence=list(evidence),
        absence_evidence=absence_evidence,
        evidence_scope=evidence_scope,
        index_build_id=index_build_id,
        retrieval_profile=retrieval_profile,
        retrieval_params_json=retrieval_params_json,
    )


def _unresolved_cell(
    *,
    state: CellState,
    param: CatalogRow,
    product_id: str,
    run_id: str,
    provenance: tuple[str | None, str | None, str | None],
    evidence_scope: EvidenceScope | None = None,
) -> Cell:
    """Собирает ``failed``/``preliminary``-ячейку без статуса (I4/I13).

    ``failed`` несёт класс ``instrument_failed``: прибор не смог, а не «вендор не заявил»
    (это разные вещи, и различие видно в карте). ``preliminary`` — кандидат без статуса:
    вердикта ещё нет, поэтому объект силы отрицания не заполняется.
    """
    index_build_id, retrieval_profile, retrieval_params_json = provenance
    return Cell(
        run_id=run_id,
        product_id=product_id,
        param_id=param.param_id,
        state=state,
        status=None,
        source_kind=_CELL_SOURCE_KIND,
        absence_rule=param.absence_rule,
        evidence=[],
        absence_evidence=[],
        evidence_scope=evidence_scope,
        index_build_id=index_build_id,
        retrieval_profile=retrieval_profile,
        retrieval_params_json=retrieval_params_json,
    )


def _record_cell_failure(
    store: StorePort | None, cell: Cell, failure_kind: str
) -> None:
    """Фиксирует класс сбоя ``failed``-ячейки (узкий UPDATE, аддитивно).

    Store-двойники без метода пропускаются: доменная модель класса не несёт, и
    отсутствие колонки не должно ломать оценку (контракт duck-typed, как у
    :func:`_record_cell_provenance`).
    """
    if store is None:
        return
    recorder = getattr(store, "record_cell_failure", None)
    if not callable(recorder):
        return
    try:
        recorder(cell.run_id, cell.product_id, cell.param_id, failure_kind)
    except Exception:  # noqa: BLE001 — провенанс сбоя аддитивен
        return


def _record_cell_provenance(
    store: StorePort | None,
    cell: Cell,
    *,
    prompt_hash: str | None,
    model_id: str | None,
) -> None:
    """Дописывает провенанс промпта/модели в ``cells``, если store это умеет.

    ``_CELL_COLUMNS`` намеренно не расширяются: доменная ``Cell`` не несёт
    ``prompt_hash``/``model_id``, и менять набор колонок для всех писателей значило бы
    менять их контракт. Вместо этого — узкий UPDATE **после** успешного upsert.
    Тестовые двойники без метода пропускаются: провенанс аддитивен и не должен ломать
    совместимость duck-typed store.
    """
    if prompt_hash is None and model_id is None:
        return
    writer = getattr(store, "set_cell_provenance", None)
    if not callable(writer):
        return
    writer(
        cell.run_id,
        cell.product_id,
        cell.param_id,
        prompt_hash=prompt_hash,
        model_id=model_id,
    )


def _absence_records(cell: Cell) -> list[dict]:
    """Отбирает записи ``cell.absence_evidence``, которые являются протоколом отсутствия.

    Носитель ``Cell.absence_evidence`` смешивает два вида записей:

    * **цитаты модели** (:func:`_absence_dict`) — они уже лежат в ``cell_evidence`` с
      ``evidence_type=direct`` (их пишет ``add_evidence_batch``); переписывать их как
      ``absence`` значило бы подменить вид доказательства;
    * **протокол отсутствия** (``evidence_type='absence'``) — у него нет чанка и цитаты,
      и именно он иначе не персистился вовсе (пятое звено разрыва: ``❌`` живой ячейки
      терял свою absence-запись на чтении из store).

    Аннотации аудита покрытия (``corpus_*``) сюда не попадают: их машинный источник —
    персистентная ``corpus_coverage``, а не урезанная строка ``cell_evidence`` без колонки
    под JSON. Возвращает копии с достроенными ключами ячейки (``run_id``/``product_id``/
    ``param_id``) — запись, пришедшая из ещё не персистированной ячейки, может их не нести.
    """
    records: list[dict] = []
    for entry in cell.absence_evidence:
        if entry.get("evidence_type") != EvidenceType.ABSENCE.value:
            continue
        if not entry.get("evidence_id"):
            continue
        record = dict(entry)
        record.setdefault("run_id", cell.run_id)
        record.setdefault("product_id", cell.product_id)
        record.setdefault("param_id", cell.param_id)
        records.append(record)
    return records


def _persist(
    store: StorePort | None,
    cell: Cell,
    validation: ValidationResult | None,
    *,
    protected: Cell | None = None,
    prompt_hash: str | None = None,
    model_id: str | None = None,
) -> None:
    """Пишет ячейку и её evidence в store (идемпотентный upsert по PK/evidence_id, S8).

    ``protected`` — ячейка более высокого источника (реестр/наследование), уже лежащая
    в store. Ш6 работает **поверх** неё (§2.2 п.7/п.8): согласие/inconclusive оставляют
    высокую ячейку без записи; расхождение сохраняется как ``pending``-кандидат ревью,
    несущий провенанс высокого источника. Evidence обоих контуров пишется всегда —
    строки ``cell_evidence`` детерминированы и переживают upsert ячейки.

    ``prompt_hash``/``model_id`` — аддитивный провенанс ячейки (колонки ``cells`` существуют,
    но доменная модель их не несёт). Пишутся только для реально сохранённой ячейки, поэтому
    при ``kept_high`` чужой провенанс не затирается.

    **Absence-evidence (R4, §4.2).** Протокол отсутствия пишется тем же батч-механизмом, что
    и прочие evidence (``add_absence_evidence``), с ``evidence_type='absence'`` и
    ``scope_acceptance`` из записи. Без этого ``❌`` существовал только в
    памяти процесса оценки: отчёт и ревью читают ячейки из store, где отсутствие не лежало
    никогда. Store-двойники без метода пропускаются — как у прочих аддитивных писателей.
    """
    if store is None:
        return
    to_save: Cell | None = cell
    if (
        protected is not None
        and protected.state
        in {CellState.EVALUATED, CellState.PENDING, CellState.REVIEWED}
        and protected.status is not None
    ):
        result = resolve_precedence(
            protected, cell, reason=PRECEDENCE_REVIEW_REASON
        )
        to_save = None if result.kept_high else result.cell
    if to_save is not None:
        store.save_cell(to_save)
        _record_cell_provenance(
            store, to_save, prompt_hash=prompt_hash, model_id=model_id
        )
    if validation is not None and validation.resolved_evidence:
        store.add_evidence_batch(validation.resolved_evidence)
    # Absence пишется независимо от прецедента — как и прочие evidence: строки
    # ``cell_evidence`` детерминированы (S8) и переживают upsert ячейки.
    records = _absence_records(cell)
    writer = getattr(store, "add_absence_evidence", None)
    if records and callable(writer):
        writer(records)


def evaluate_cell(
    *,
    param: CatalogRow,
    product_id: str,
    run_id: str,
    evaluator: EvaluatorPort,
    chunks: Sequence[ChunkLike],
    satellites: Sequence[Satellite] = (),
    resolver: SourceResolver | None = None,
    strict: bool = True,
    retrievals: RetrievalResult | Sequence[RetrievalResult] | None = None,
    max_retries: int = 2,
    store: StorePort | None = None,
    model_id: str | None = None,
    min_retrieval_score: float | None = None,
    protected: Cell | None = None,
    coverage: CorpusCoverage | None = None,
    scope_acceptance: str | None = None,
    extra_instructions: str = "",
    eval_cache: EvalVerdictCache | None = None,
    scope_attested_by: str | None = None,
) -> EvaluationOutcome:
    """Оценивает одну ячейку Ш6 и возвращает :class:`EvaluationOutcome` (§5/§7.1).

    :param param: строка каталога (``rule``/``absence_rule``/``role_binding``/…).
    :param product_id / run_id: ключ ячейки (I17: evidence только продукт-скоупа).
    :param evaluator: порт оценщика (self-host, без веб-тула — I18).
    :param chunks: переданный оценщику продукт-скоупед top-K (I17/I20; ретривал — выше).
    :param satellites: сателлиты как контекст промпта (не доказательство).
    :param resolver: резолвер ``source_id`` → URL (I10/I15); опционален.
    :param strict: strict (failed при исчерпании) vs preliminary (кандидат при провале).
    :param retrievals: провенанс сборки/профиля/K для ячейки (R7/I16/I20).
    :param max_retries: число повторов сверх первой попытки (≤2 по регламенту §7.1).
    :param store: если задан — ячейка и её evidence персистятся (идемпотентно, S8).
    :param model_id: переопределение ``model_id`` (иначе берётся у оценщика, I21).
    :param min_retrieval_score: порог аномалии §10.4 (заметка, не сбой).
    :param protected: ячейка более высокого источника для наложения (реестр/наследование,
        §2.2 п.7/п.8); ``None`` — обычная запись Ш6.
    :param coverage: покрытие корпуса продукта (Ф0). ``None`` — прочитать из store
        (``read_corpus_coverage``) либо, если записи нет, сохранить прежнее поведение.
        Покрытие задаёт **класс** отрицания (``unobserved`` при неполном охвате), а не
        запрет публиковать ``❌``.
    :param scope_acceptance: **принятие риска** человеком (прежде ``scope_attested_by``).
        Не подставляется автоматически: это решение человека, переданное вызывающим.
        Нужно только там, где измерение не покрывает пространство (``bounded``);
        ``explicit`` с цитатой в нём не нуждается.
    :param extra_instructions: дополнительный текст промпта (в ``prompt_hash`` §5 не
        входит, но входит в текст и потому в ключ кэша вердиктов).
    :param eval_cache: межпрогонный кэш сырых вердиктов (:class:`EvalVerdictCache`).
        ``None`` ⇒ кэш выключен (поведение байт-в-байт прежнее). Хит отдаёт
        ``LLMCellOutput`` и проходит **весь** существующий эшелон заново; кэшируется
        только успешно провалидированный живой ответ.
    :param scope_attested_by: deprecated-алиас ``scope_acceptance`` (прежнее имя).
        Принят, чтобы вызывающие, объявляющие подпись старым именем, не ломались на
        переименовании; смысл — тот же (принятие риска, не аттестация полноты).

    Повтор переиздаёт тот же запрос — для детерминированного оценщика это ограниченный
    no-op, полезный только для транзиентных ошибок адаптера; см. докстринг модуля.
    """
    if scope_acceptance is None:
        scope_acceptance = scope_attested_by
    if max_retries < 0:
        raise ValueError("max_retries не может быть отрицательным")

    chunks_by_id: dict[str, ChunkLike] = {c.chunk_id: c for c in chunks}
    compiled: CompiledPrompt = compile_prompt(
        param=param,
        product_id=product_id,
        chunks=chunks,
        satellites=satellites,
        extra_instructions=extra_instructions,
    )

    request = LLMRequest(
        prompt=compiled.text,
        response_schema=LLMCellOutput,
        model_id=model_id or getattr(evaluator, "model_id", "") or _UNSPECIFIED_MODEL_ID,
        model_version=getattr(evaluator, "model_version", "") or "",
        seed=getattr(evaluator, "seed", None),
        temperature=getattr(evaluator, "temperature", None),
    )

    # Ключ кэша вердиктов: **отрендеренный** текст промпта + конфигурация оценщика.
    # ``prompt_hash`` (§5) входит ради аудита, но не является ключом: он не покрывает
    # product/title/satellites/extra_instructions/текст чанков. ``run_id``/``build_id``/
    # ``retrieval_profile``/``cutoff_k``/``strict``/coverage/resolver намеренно вне ключа:
    # они либо уже отражены в тексте (какие чанки попали), либо пере-исполняются на хите.
    eval_echo: dict[str, Any] | None = None
    eval_key: str | None = None
    if eval_cache is not None:
        eval_echo = {
            "format": EVAL_CACHE_FORMAT,
            "schema": EVAL_SCHEMA_VERSION,
            "template": compiled.template_version,
            "prompt_sha256": content_hash(compiled.text),
            "prompt_hash": compiled.prompt_hash,
            "model_id": request.model_id,
            "model_version": request.model_version,
            "seed": request.seed,
            "temperature": request.temperature,
            "decoder": getattr(evaluator, "decoder", "none"),
            "endpoint_kind": getattr(evaluator, "endpoint_kind", ""),
        }
        eval_key = build_eval_key(eval_echo)

    provenance = _provenance(retrievals)

    last_response: LLMResponse | None = None
    last_validation: ValidationResult | None = None
    last_kind: str | None = None
    cache_hit = False

    def _validate(response: LLMResponse) -> ValidationResult:
        """Единый эшелон §10 для живого ответа и для кэшированного вердикта."""
        return validate_output(
            output=response.output,
            chunks=chunks_by_id,
            product_id=product_id,
            param_id=param.param_id,
            run_id=run_id,
            resolver=resolver,
            strict=strict,
            min_retrieval_score=min_retrieval_score,
            # 09 §8.1: композиция прогона — из провенанса ретривала
            # (``RetrievalResult.build_id``); нужна, чтобы legacy-чанк с id сборки не
            # выдал композицию за физическую партицию в evidence.
            composition_id=provenance[0],
        )

    # Хит — дополнительная попытка **до** цикла повторов: он не тратит бюджет
    # ``max_retries``. Если кэшированный выход не проходит валидацию (например, сменился
    # resolver), он не публикуется и управление уходит в живой вызов.
    if eval_cache is not None and eval_key is not None and eval_echo is not None:
        cached = eval_cache.get(eval_key, echo=eval_echo)
        if cached is not None:
            cached_response = LLMResponse(
                output=cached,
                raw=cached.model_dump_json(),
                prompt_hash=compiled.prompt_hash,
                decoder=str(eval_echo.get("decoder") or "none"),
            )
            cached_validation = _validate(cached_response)
            if cached_validation.ok:
                last_response = cached_response
                last_validation = cached_validation
                last_kind = None
                cache_hit = True

    if not cache_hit:
        for _ in range(max_retries + 1):
            try:
                response = evaluator.complete(request)
            except Exception as exc:  # noqa: BLE001 — изоляция адаптера, маппинг в failure_kind
                last_response = None
                last_validation = None
                last_kind = FailureKind.EVALUATION_FAILED.value
                last_validation = ValidationResult(
                    ok=False,
                    failure_kind=last_kind,
                    notes=[f"оценщик недоступен: {type(exc).__name__}: {exc}"],
                    resolved_evidence=[],
                )
                continue

            last_response = response
            last_validation = _validate(response)
            if last_validation.ok:
                last_kind = None
                # Только живой, успешно провалидированный ответ становится вердиктом.
                if (
                    eval_cache is not None
                    and eval_key is not None
                    and eval_echo is not None
                ):
                    eval_cache.put(eval_key, response.output, echo=eval_echo)
                break
            last_kind = FailureKind.VALIDATION_FAILED.value

    assert last_validation is not None  # цикл выполняет ≥1 попытку

    # Успешная валидация — оцениваем ячейку.
    if last_validation.ok and last_response is not None:
        out = last_response.output
        effective_coverage = (
            _coverage_for(coverage, store, run_id, product_id)
            if out.status in {Status.ABSENT, Status.UNKNOWN}
            else None
        )
        # Вариант 2: вопрос «можно ли утверждать отсутствие» решает один объект.
        # Никаких ``pending``-кандидатов «механически недоказуемого ❌» больше нет:
        # недоказанное «нет» — это ``❓`` с причиной, и оно остаётся в карте.
        evidence_scope, denial_notes = _resolve_denial(
            param=param,
            out=out,
            validation=last_validation,
            coverage=effective_coverage,
            scope_acceptance=scope_acceptance,
            composition_id=provenance[0],
        )
        out_status = out.status if evidence_scope is None else evidence_scope.verdict()
        absence_evidence: list[dict] = []
        if out_status is Status.ABSENT and evidence_scope is not None:
            absence_evidence = _build_absence_evidence(
                param,
                last_validation,
                run_id=run_id,
                product_id=product_id,
                scope=evidence_scope,
                composition_id=provenance[0],
            )
            # Аудит блокировок корпуса остаётся машинно-читаемым (отчёт/аудит), но
            # вердикт от него не зависит: неполнота уже выражена классом.
            if effective_coverage is not None and not effective_coverage.complete:
                _record_corpus_coverage(
                    absence_evidence,
                    effective_coverage,
                    accepted_by=evidence_scope.scope_acceptance,
                )
        cell = _evaluated_cell(
            out_status=out_status,
            evidence=out.evidence,
            absence_evidence=absence_evidence,
            param=param,
            product_id=product_id,
            run_id=run_id,
            provenance=provenance,
            evidence_scope=evidence_scope,
        )
        _persist(
            store,
            cell,
            last_validation,
            protected=protected,
            prompt_hash=compiled.prompt_hash,
            model_id=request.model_id,
        )
        validation = last_validation
        if denial_notes:
            validation = ValidationResult(
                ok=last_validation.ok,
                failure_kind=last_validation.failure_kind,
                notes=[*last_validation.notes, *denial_notes],
                resolved_evidence=last_validation.resolved_evidence,
            )
        return EvaluationOutcome(
            cell=cell,
            prompt_hash=compiled.prompt_hash,
            validation=validation,
            failure_kind=None,
            raw=last_response.raw,
        )

    # Валидация/вызов не прошли после всех попыток: failed (strict) или кандидат.
    return _unresolved_outcome(
        strict=strict,
        kind=last_kind or FailureKind.VALIDATION_FAILED.value,
        validation=last_validation,
        notes=[],
        param=param,
        product_id=product_id,
        run_id=run_id,
        provenance=provenance,
        prompt_hash=compiled.prompt_hash,
        store=store,
        raw=last_response.raw if last_response is not None else None,
        protected=protected,
        model_id=request.model_id,
    )


def _unresolved_outcome(
    *,
    strict: bool,
    kind: str,
    validation: ValidationResult,
    notes: list[str],
    param: CatalogRow,
    product_id: str,
    run_id: str,
    provenance: tuple[str | None, str | None, str | None],
    prompt_hash: str,
    store: StorePort | None,
    raw: str | None,
    protected: Cell | None = None,
    model_id: str | None = None,
) -> EvaluationOutcome:
    """Собирает исход несостоявшейся валидации: ``failed`` (I4) либо preliminary (I13).

    В strict ячейка получает ``state=failed`` и ``failure_kind``. В preliminary ячейка
    остаётся кандидатом (``state=preliminary``), ``status=None``, ``failure_kind=None``;
    причина — в ``validation.notes``. Дополнительные ``notes`` (например, I2b) вливаются
    в ``validation.notes`` без потери исходных. ``prompt_hash``/``model_id`` пишутся в
    ``cells`` узким UPDATE (провенанс виден и у несостоявшейся ячейки).
    """
    merged = list(validation.notes)
    for note in notes:
        if note not in merged:
            merged.append(note)
    merged_validation = ValidationResult(
        ok=validation.ok,
        failure_kind=validation.failure_kind,
        notes=merged,
        resolved_evidence=validation.resolved_evidence,
    )
    state = CellState.FAILED if strict else CellState.PRELIMINARY
    # ``failed`` несёт класс ``instrument_failed``: прибор не смог (вызов/валидация), и
    # это видно в карте отдельно от «вендор не заявил» (``not_stated``). Кандидат
    # ``preliminary`` объекта не несёт: вердикта ещё нет (I13).
    scope = (
        EvidenceScope.for_instrument_failure(
            kind, detail="; ".join(merged[-2:]) if merged else None
        )
        if strict
        else None
    )
    cell = _unresolved_cell(
        state=state,
        param=param,
        product_id=product_id,
        run_id=run_id,
        provenance=provenance,
        evidence_scope=scope,
    )
    _persist(
        store,
        cell,
        None,
        protected=protected,
        prompt_hash=prompt_hash,
        model_id=model_id,
    )
    return EvaluationOutcome(
        cell=cell,
        prompt_hash=prompt_hash,
        validation=merged_validation,
        failure_kind=kind if strict else None,
        raw=raw,
    )


def evaluate_batch(
    *,
    params: Sequence[CatalogRow],
    product_id: str,
    run_id: str,
    evaluator: EvaluatorPort,
    chunks_by_param: Mapping[str, Sequence[ChunkLike]],
    satellites: Sequence[Satellite] = (),
    resolver: SourceResolver | None = None,
    strict: bool = True,
    max_retries: int = 2,
    store: StorePort | None = None,
    retrievals_by_param: Mapping[
        str, RetrievalResult | Sequence[RetrievalResult]
    ] | None = None,
    protected_by_param: Mapping[str, Cell] | None = None,
    coverage: CorpusCoverage | None = None,
    scope_acceptance: str | None = None,
    extra_instructions: str = "",
    eval_cache: EvalVerdictCache | None = None,
) -> tuple[list[EvaluationOutcome], list[str]]:
    """Прогоняет :func:`evaluate_cell` по параметрам с изоляцией сбоев (§A.1).

    Возвращает ``(успешные_исходы, ошибки)``: исключение на одном параметре не роняет
    батч (``run_isolated``). Порядок исходов соответствует порядку ``params``.
    """

    def _one(param: CatalogRow) -> EvaluationOutcome:
        return evaluate_cell(
            param=param,
            product_id=product_id,
            run_id=run_id,
            evaluator=evaluator,
            chunks=chunks_by_param.get(param.param_id, ()),
            satellites=satellites,
            resolver=resolver,
            strict=strict,
            retrievals=(retrievals_by_param or {}).get(param.param_id),
            max_retries=max_retries,
            store=store,
            protected=(protected_by_param or {}).get(param.param_id),
            coverage=coverage,
            scope_acceptance=scope_acceptance,
            extra_instructions=extra_instructions,
            eval_cache=eval_cache,
        )

    return run_isolated(params, _one)


def evaluate_phase(
    *,
    plan: Any,
    ports: Ports,
    store: StorePort,
    run_id: str,
    satellites: Sequence[Satellite] = (),
    resolver: SourceResolver | None = None,
    profile: RetrievalProfile | str | None = None,
    cutoff_k: int | None = None,
    strict: bool = True,
    scope_acceptance: str | Mapping[str, str] | None = None,
    eval_cache: EvalVerdictCache | None = None,
    exhaustion_facts: ExhaustionFacts | None = None,
    scope_attested_by: str | Mapping[str, str] | None = None,
) -> PhaseResult:
    """Фаза оценки Ш6 на уровне конвейера: продукты × параметры → ретривал → оценщик.

    Композиция ровно того порядка, который фиксируют инварианты:

    0. векторы всех уникальных запросов пакета считаются **до** цикла оценки, батчами
       (:func:`precompute_query_vectors`) и передаются в ячейки через ``query_vectors``:
       ретривал-композер не зовёт эмбеддер, поэтому оценщик не ходит за векторами
       (офлайн-чистота, §5 doc 14) и один и тот же текст не эмбеддится на каждый продукт;
    1. для пары ``(product, param)`` вызывается :func:`retrieve_for_cell` с **единым**
       профилем и ``cutoff_k`` (I16: симметрия; I20: один K на цикл); dense-плечо всех
       запросов продукта считается **одним** батч-вызовом порта
       (:func:`precompute_dense_hits` → ``batched_search``), а не 600 отдельными поисками;
    2. полученный продукт-скоупед срез передаётся в :func:`evaluate_batch`
       (I17: evidence — только чанки продукта/его сателлитов);
    3. оценщик работает только по переданным чанкам (I18): здесь нет ни ``search``,
       ни ``fetch``, ни ``core.adapters``/``core.edge``.

    Ретривал каждого параметра изолирован: сбой ретривала не роняет фазу, а попадает в
    ``PhaseResult.errors``; сбои отдельных ячеек внутри :func:`evaluate_batch` изолируются
    тем же контрактом. Возвращаемый ``PhaseResult`` несёт агрегат ``cells`` по всем
    продуктам.

    :param plan: ``RunPlan`` (``products``, ``params``, ``run_id``).
    :param ports: ``Ports`` с ``evaluator``/``lexical_index``/``vector_index``/``embedder``.
    :param store: ``StorePort`` — персистенция ячеек/evidence.
    :param run_id: идентификатор прогона.
    :param satellites: сателлиты как контекст промпта (не доказательство).
    :param resolver: резолвер ``source_id`` → URL (I10/I15); опционален.
    :param profile: зафиксированный профиль R0/R1/R2; ``None`` → :data:`DEFAULT_RETRIEVAL_PROFILE`.
    :param cutoff_k: зафиксированный ``retrieval_cutoff_K``; ``None`` → :data:`DEFAULT_RETRIEVAL_CUTOFF_K`.
    :param strict: strict (failed при исчерпании) vs preliminary (кандидат).
    :param scope_acceptance: **принятие риска** человеком (прежде ``scope_attested_by``);
        передаётся вызывающим из решения гейта/ревью и никогда не выводится
        автоматически. Строка — прогон-широкое принятие; mapping ``product_id → actor`` —
        per-product (покрытие per product). Оно нужно только классу ``bounded`` (измерение
        не покрывает пространство), поэтому продукт без подписи получает ``❓`` с причиной
        ``absence_unattested``, а не выпадает из карты.
    :param eval_cache: межпрогонный кэш сырых вердиктов оценщика; ``None`` ⇒ выключен
        (поведение байт-в-байт). Хит проходит все пост-шаги (валидация, класс отрицания,
        прецедент) заново — кэшируется только ``LLMCellOutput``, не ``Cell``.
    :param scope_attested_by: deprecated-алиас ``scope_acceptance`` (прежнее имя).
    :param exhaustion_facts: наблюдённые факты других фаз (какие контуры реально
        исполнились, раунды/бюджет SERP). ``None`` ⇒ протоколы не пишутся (прежнее
        поведение модульного вызова). Факты передаёт ``phases`` — Ш6 **не** догадывается,
        что происходило в других фазах (I3/§5). Наблюдённая терминация попадает в
        **вектор охвата** каждой ``❓``-ячейки (``EvidenceScope.termination``).

    При ``ports.evaluator is None`` фаза сразу ``failed``: без оценщика Ш6 не имеет смысла,
    а молчаливый ``ok`` скрыл бы неполноту карты.
    """
    if scope_acceptance is None:
        scope_acceptance = scope_attested_by
    evaluator = getattr(ports, "evaluator", None)
    if evaluator is None:
        return PhaseResult(
            status="failed",
            phase="evaluate",
            errors=["evaluator не сконфигурирован"],
        )

    resolved_profile: RetrievalProfile | str = (
        profile if profile is not None else DEFAULT_RETRIEVAL_PROFILE
    )
    resolved_cutoff: int = (
        int(cutoff_k) if cutoff_k is not None else DEFAULT_RETRIEVAL_CUTOFF_K
    )

    cells: list[Cell] = []
    errors: list[str] = []

    # Запросы пакета одинаковы для всех продуктов когорты, поэтому векторы считаются
    # **один раз до цикла оценки**: батчами и по уникальному тексту. Оценщик за
    # векторами не ходит вовсе (офлайн-чистота §5 doc 14), а повторный текст не
    # эмбеддится на каждый продукт (мемоизация в пределах прогона).
    query_memo: dict[str, list[float]] = {}
    try:
        query_vectors = precompute_query_vectors(
            plan.params,
            profile=resolved_profile,
            embedder=getattr(ports, "embedder", None),
            memo=query_memo,
        )
    except Exception as exc:  # noqa: BLE001 — сбой виден в errors, а не притворяется успехом
        errors.append(
            f"query_vectors: {type(exc).__name__}: {exc} — векторы запросов "
            "считаются поштучно внутри ячеек (прежний путь)"
        )
        query_vectors = None

    # Ш6 работает поверх более высоких источников (§2.2 п.7/п.8, 02 §D.3). Порядок фаз
    # заморожен (registry → inheritance → corpus → evaluate), поэтому карта стабильна.
    # Читаем один раз на главном потоке — не в воркерах ретривала/оценки.
    protected_cells = collect_protected_cells(store, run_id)

    for product_id in plan.products:
        # I2b/I2c: подпись аттестации per product (покрытие per product). Строка —
        # прежняя прогон-широкая семантика; продукт без подписи — как раньше.
        accepted = (
            scope_acceptance.get(product_id)
            if isinstance(scope_acceptance, Mapping)
            else scope_acceptance
        )
        protected_by_param = {
            param_id: cell
            for (owner_id, param_id), cell in protected_cells.items()
            if owner_id == product_id
        }
        chunks_by_param: dict[str, Sequence[ChunkLike]] = {}
        retrievals_by_param: dict[str, RetrievalResult] = {}
        # Ф0: покрытие корпуса продукта читается один раз (на главном потоке) и
        # прокидывается в ячейки. Продукт плана без строки покрытия — аномалия (O3).
        coverage = _coverage_for(None, store, run_id, product_id, planned=True)
        if coverage is not None and coverage.expected_sources == 0:
            # O2/O3: ноль источников — failed-ячейки (I4), не «тихий ok» и не ❌.
            # ``coverage_missing`` (нет строки покрытия вовсе) остаётся своим классом:
            # это аномалия провенанса, а не отсутствие источников. Все zero-source
            # причины (``no_seeds``/``all_seeds_rejected``/``no_urls_entry``) —
            # **наблюдательные** и живут в тексте ошибки; класс сбоя один — ``no_sources``,
            # поэтому словарь классов сбоя (§4.1) не расширяется.
            kind = (
                FailureKind.COVERAGE_MISSING.value
                if coverage.reason == FailureKind.COVERAGE_MISSING.value
                else FailureKind.NO_SOURCES.value
            )
            provenance = (
                None,
                resolved_profile.value
                if isinstance(resolved_profile, RetrievalProfile)
                else resolved_profile,
                None,
            )
            for param in plan.params:
                # P6/O5: ячейка высокого источника (реестр/наследование) не затирается
                # failed-ячейкой даже при нулевом корпусе — она остаётся в отчёте.
                protected = protected_by_param.get(param.param_id)
                if protected is not None:
                    cells.append(protected)
                    continue
                failed = _unresolved_cell(
                    state=CellState.FAILED,
                    param=param,
                    product_id=product_id,
                    run_id=run_id,
                    provenance=provenance,
                    # Класс сбоя прибора виден в карте: это не «вендор не заявил».
                    evidence_scope=EvidenceScope.for_instrument_failure(
                        kind,
                        detail=f"expected_sources=0 ({coverage.reason or 'no coverage'})",
                    ),
                )
                _persist(store, failed, None)
                _record_cell_failure(store, failed, kind)
                cells.append(failed)
            errors.append(
                f"{product_id}: {kind}: expected_sources=0 "
                f"({coverage.reason or 'no coverage'}) — оценка невозможна"
            )
            continue
        # Dense-плечо всех запросов продукта — одним батч-вызовом порта: партиция
        # читается и умножается один раз, а не на каждый из 600 запросов. Сбой батча
        # не выдаётся за успех и не подменяется «пусто»: он виден в ``errors``, а поиск
        # повторяется поштучно в ячейке — то есть отказ не тише, чем до правки.
        try:
            dense_by_query = precompute_dense_hits(
                plan.params,
                product_ids=[product_id],
                profile=resolved_profile,
                cutoff_k=resolved_cutoff,
                vector_index=getattr(ports, "vector_index", None),
                query_vectors=query_vectors,
            )
        except Exception as exc:  # noqa: BLE001 — батч не удался: идём поштучно, но видимо
            errors.append(
                f"{product_id}: dense batch: {type(exc).__name__}: {exc} — "
                "поиск пошёл поштучно"
            )
            dense_by_query = None
        for param in plan.params:
            try:
                result = retrieve_for_cell(
                    param,
                    product_id=product_id,
                    profile=resolved_profile,
                    cutoff_k=resolved_cutoff,
                    lexical_index=getattr(ports, "lexical_index", None),
                    vector_index=getattr(ports, "vector_index", None),
                    embedder=getattr(ports, "embedder", None),
                    query_vectors=query_vectors,
                    dense_by_query=dense_by_query,
                )
            except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                errors.append(
                    f"{product_id}/{param.param_id}: retrieval: "
                    f"{type(exc).__name__}: {exc}"
                )
                continue
            chunks_by_param[param.param_id] = result.chunks
            retrievals_by_param[param.param_id] = result

        outcomes, batch_errors = evaluate_batch(
            params=list(plan.params),
            product_id=product_id,
            run_id=run_id,
            evaluator=evaluator,
            chunks_by_param=chunks_by_param,
            satellites=satellites,
            resolver=resolver,
            strict=strict,
            store=store,
            retrievals_by_param=retrievals_by_param,
            protected_by_param=protected_by_param,
            coverage=coverage,
            scope_acceptance=accepted,
            eval_cache=eval_cache,
        )
        cells.extend(outcome.cell for outcome in outcomes)
        errors.extend(batch_errors)

    # I3/§5: протокол исчерпания для каждого машинного ``❓`` — из **наблюдённых** фактов,
    # переданных вызывающим. Протокол — **провенанс поиска**, а не условие вердикта:
    # ``❓`` обосновывается классом отрицания и причиной. Наблюдённая терминация при этом
    # попадает в вектор охвата ячейки (``EvidenceScope.termination``) — это факт, а не
    # вывод, поэтому он берётся из протокола, а не угадывается в оценщике.
    if exhaustion_facts is not None:
        protocols = build_protocols(
            cells,
            run_id=run_id,
            contours_run=exhaustion_facts.contours_run,
            rounds_by_product=exhaustion_facts.rounds_by_product,
            budget=exhaustion_facts.budget_by_product,
            termination_by_product=exhaustion_facts.termination_by_product,
            queries_by_product=exhaustion_facts.queries_by_product,
            yielded_by_product=exhaustion_facts.yielded_by_product,
            exhausted_at=exhaustion_facts.exhausted_at,
        )
        by_cell = {(p.product_id, p.param_id): p for p in protocols}
        for index, cell in enumerate(cells):
            scope = cell.evidence_scope
            if scope is None or scope.termination is not None:
                continue
            protocol = by_cell.get((cell.product_id, cell.param_id))
            if protocol is None:
                continue
            if (cell.product_id, cell.param_id) in protected_cells:
                # Ячейку не сохранял Ш6: высокий источник оставлен как есть (``❓``
                # inconclusive, §2.2 п.8). Записывать сюда обогащённую копию значило бы
                # перезаписать реестр корпусом — ровно то, что прецедент запрещает.
                continue
            updated = cell.model_copy(
                update={
                    "evidence_scope": scope.model_copy(
                        update={"termination": protocol.termination_reason.value}
                    )
                }
            )
            cells[index] = updated
            store.save_cell(updated)
        for protocol in protocols:
            store.save_exhaustion(protocol)

    return PhaseResult(status="ok", phase="evaluate", cells=cells, errors=errors)
