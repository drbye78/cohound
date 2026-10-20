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
    FailureKind,
    LLMCellOutput,
    Satellite,
    SourceKind,
    SourceScope,
    Status,
)
from core.domain.anchors import SourceResolver
from core.pipeline.eval_cache import (
    EVAL_CACHE_FORMAT,
    EvalVerdictCache,
    build_eval_key,
)
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
from core.ports.vector_index import RetrievedChunk, VectorIndexPort
from core.util.hashing import content_hash

__all__ = [
    "CORPUS_INCOMPLETE_REVIEW_REASON",
    "DEFAULT_RETRIEVAL_CUTOFF_K",
    "DEFAULT_RETRIEVAL_PROFILE",
    "EvaluationOutcome",
    "evaluate_batch",
    "evaluate_cell",
    "evaluate_phase",
    "retrieve_for_cell",
    "run_queries_for_cell",
]

#: Ф0: причина ``pending``-кандидата, когда ``❌`` предложен при неполном корпусе.
#: Свободнотекстовая аннотация (не статус/не перечисление), как ``precedence_conflict``.
CORPUS_INCOMPLETE_REVIEW_REASON = "corpus_incomplete_absence"

#: Максимум запросов на ячейку (S13/I16): title + концепт-термины ru → en.
MAX_QUERIES = 3

#: Дефолтный профиль ретривала фазы оценки, если оркестратор его не задал (I16).
#: В проде профиль фиксируется в ``RunConfig``/манифесте; здесь — безопасный baseline.
DEFAULT_RETRIEVAL_PROFILE: RetrievalProfile = RetrievalProfile.R0

#: Дефолтный ``retrieval_cutoff_K`` фазы оценки, если оркестратор его не задал (I20).
#: В проде K фиксируется в ``RunConfig``/манифесте; здесь — нейтральный baseline.
DEFAULT_RETRIEVAL_CUTOFF_K: int = 10

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
    """Вектор запроса для dense-плеча: из карты либо через ``embedder`` (иначе ``None``)."""
    if query_vectors is not None and query in query_vectors:
        return list(query_vectors[query])
    if embedder is not None:
        vectors = embedder.embed([query])
        if not vectors:
            raise ValueError("embedder вернул пустой список векторов")
        return list(vectors[0])
    return None


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
    """
    resolved_profile = _as_profile(profile)
    products = list(product_ids) if product_ids is not None else [product_id]
    queries = run_queries_for_cell(param, max_queries=max_queries)

    best: dict[str, RetrievedChunk] = {}
    build_id = ""
    for query in queries:
        vector = None
        if resolved_profile in (RetrievalProfile.R1, RetrievalProfile.R2):
            vector = _query_vector(query, query_vectors, embedder)
        result = retrieve(
            query_text=query,
            query_vector=vector,
            product_ids=products,
            profile=resolved_profile,
            cutoff_k=cutoff_k,
            lexical_index=lexical_index,
            vector_index=vector_index,
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
# Отсутствие (I2/I2b)
# ---------------------------------------------------------------------------


def _absence_dict(ev) -> dict:  # noqa: ANN001 - CellEvidence, без импорта ради читаемости
    """Сериализует ``CellEvidence`` в элемент ``absence_evidence`` (I2/I2b)."""
    return {
        "evidence_id": ev.evidence_id,
        "source_id": ev.source_id,
        "source_url": ev.source_url,
        "citation_quote": ev.citation_quote,
        "content_hash": ev.content_hash,
        "source_kind": ev.source_kind.value,
        "source_scope": ev.source_scope.value if ev.source_scope else None,
        "scope_attested_by": ev.scope_attested_by,
        # R7/09 §8.1: та же привязка, что у ``cell_evidence`` — композиция прогона.
        "index_build_id": ev.index_build_id,
    }


def _build_absence_evidence(
    param: CatalogRow,
    validation: ValidationResult,
    *,
    scope_attested_by: str | None = None,
    composition_id: str | None = None,
) -> tuple[list[dict], str | None]:
    """Строит ``absence_evidence`` для ``❌`` и проверяет I2b.

    Для ``explicit`` достаточно непустого списка: отсутствие подтверждается протоколом
    над переданным срезом. Для ``parameter_reference``/``distribution_absence``/
    ``registry_absence`` механически требуется ``source_scope=exhaustive`` (а для первых
    двух — ещё и ``scope_attested_by``, который ставит **человек**). Если подтверждённых
    цитат нет, ставится синтетическая запись протокола над срезом; недоказуемость на
    уровне эшелона (нехватка exhaustive/attestation) возвращается как ошибка — вызывающий
    уводит её в ``failed``/preliminary, а не публикует ``❌`` молча.

    ``scope_attested_by`` — явная человеческая аттестация, переданная вызывающим
    (composition root / решение гейта). Она проставляется на синтетическую запись, чтобы
    Ф0-гейт мог отличить принятый человеком риск от машинного ``❌`` при неполном корпусе.

    ``composition_id`` — композиция прогона (``RetrievalResult.build_id``). Синтетическая
    запись не имеет чанка, но привязана к сборке **прогона**, а не к партиции, поэтому
    несёт ``index_build_id=composition_id`` (R7/09 §8.1).
    """
    absences = [_absence_dict(ev) for ev in validation.resolved_evidence]
    if not absences:
        absences.append(
            {
                "evidence_id": None,
                "source_id": None,
                "source_scope": SourceScope.UNKNOWN.value,
                "scope_attested_by": scope_attested_by,
                "index_build_id": composition_id,
                "note": "явное отсутствие по переданному срезу фрагментов",
            }
        )

    if param.absence_rule in {
        AbsenceRuleType.PARAMETER_REFERENCE,
        AbsenceRuleType.DISTRIBUTION_ABSENCE,
    }:
        attested = any(
            a.get("source_scope") == SourceScope.EXHAUSTIVE.value
            and bool(a.get("scope_attested_by"))
            for a in absences
        )
        if not attested:
            return absences, (
                "I2b: parameter_reference/distribution_absence требуют "
                "source_scope=exhaustive и scope_attested_by (аттестация — только человек)"
            )
    elif param.absence_rule is AbsenceRuleType.REGISTRY_ABSENCE:
        exhaustive = any(
            a.get("source_scope") == SourceScope.EXHAUSTIVE.value for a in absences
        )
        if not exhaustive:
            return absences, "I2b: registry_absence — exhaustive по построению"
    return absences, None


def _absence_attested(absence_evidence: list[dict]) -> bool:
    """Есть ли в absence-evidence человеческая аттестация охвата (``scope_attested_by``)."""
    return any(bool(entry.get("scope_attested_by")) for entry in absence_evidence)


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
    attested_by: str | None = None,
) -> None:
    """Дописывает в absence-evidence аудит блокировок корпуса (Ф0, I2c).

    Запись машинно-читаема (``corpus_blocked``/``corpus_complete``/счётчики) и остаётся
    в ячейке, поэтому отчёт видит факт неполного покрытия без чтения БД. Для аттестованного
    человеком ``❌`` она фиксирует, что риск принят осознанно.
    """
    attested_by = attested_by or next(
        (
            entry.get("scope_attested_by")
            for entry in absence_evidence
            if entry.get("scope_attested_by")
        ),
        None,
    )
    absence_evidence.append(
        {
            "evidence_id": None,
            "source_id": None,
            "source_scope": SourceScope.UNKNOWN.value,
            "scope_attested_by": attested_by,
            "note": (
                "Ф0: неполное покрытие корпуса — блокирующие сбои: "
                f"{coverage.blocking_failures or '{}'}; "
                f"нечитаемые origin'ы: {list(coverage.unreadable_origins) or '[]'}; "
                "❌ допустим только с человеческой аттестацией"
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
) -> Cell:
    """Собирает успешную ``evaluated``-ячейку (§2.2; I1/I2/I4/I13-совместимую)."""
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
) -> Cell:
    """Собирает ``failed``/``preliminary``-ячейку без статуса (I4/I13)."""
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
    scope_attested_by: str | None = None,
    extra_instructions: str = "",
    eval_cache: EvalVerdictCache | None = None,
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
    :param scope_attested_by: человеческая аттестация охвата для ``❌`` (Ф0). Не
        подставляется автоматически: это решение человека, переданное вызывающим.
    :param extra_instructions: дополнительный текст промпта (в ``prompt_hash`` §5 не
        входит, но входит в текст и потому в ключ кэша вердиктов).
    :param eval_cache: межпрогонный кэш сырых вердиктов (:class:`EvalVerdictCache`).
        ``None`` ⇒ кэш выключен (поведение байт-в-байт прежнее). Хит отдаёт
        ``LLMCellOutput`` и проходит **весь** существующий эшелон заново; кэшируется
        только успешно провалидированный живой ответ.

    Повтор переиздаёт тот же запрос — для детерминированного оценщика это ограниченный
    no-op, полезный только для транзиентных ошибок адаптера; см. докстринг модуля.
    """
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
        absence_evidence: list[dict] = []
        if out.status is Status.ABSENT:
            absence_evidence, absence_error = _build_absence_evidence(
                param,
                last_validation,
                scope_attested_by=scope_attested_by,
                composition_id=provenance[0],
            )
            if absence_error is not None:
                # Механически недоказуемое ❌ → не публикуем: failed (strict) / кандидат.
                return _unresolved_outcome(
                    strict=strict,
                    kind=FailureKind.VALIDATION_FAILED.value,
                    validation=last_validation,
                    notes=[absence_error],
                    param=param,
                    product_id=product_id,
                    run_id=run_id,
                    provenance=provenance,
                    prompt_hash=compiled.prompt_hash,
                    store=store,
                    raw=last_response.raw,
                    protected=protected,
                    model_id=request.model_id,
                )

            # Ф0/I2c: неполный корпус не доказывает отсутствие. Машинный ``❌`` при
            # ``absence_rule=explicit`` без человеческой аттестации понижается до
            # ``pending``-кандидата (I6/I7: решает человек). Прочие absence_rule уже
            # требуют exhaustive/attestation (I2b) — их правила не меняются.
            effective_coverage = _coverage_for(coverage, store, run_id, product_id)
            if effective_coverage is not None and not effective_coverage.complete:
                attested = bool(scope_attested_by) or _absence_attested(absence_evidence)
                if param.absence_rule is AbsenceRuleType.EXPLICIT and not attested:
                    _record_corpus_coverage(absence_evidence, effective_coverage)
                    pending = _evaluated_cell(
                        out_status=out.status,
                        evidence=out.evidence,
                        absence_evidence=absence_evidence,
                        param=param,
                        product_id=product_id,
                        run_id=run_id,
                        provenance=provenance,
                    ).model_copy(
                        update={
                            "state": CellState.PENDING,
                            "review_reason": CORPUS_INCOMPLETE_REVIEW_REASON,
                        }
                    )
                    _persist(
                        store,
                        pending,
                        last_validation,
                        protected=protected,
                        prompt_hash=compiled.prompt_hash,
                        model_id=request.model_id,
                    )
                    return EvaluationOutcome(
                        cell=pending,
                        prompt_hash=compiled.prompt_hash,
                        validation=last_validation,
                        failure_kind=None,
                        raw=last_response.raw,
                    )
                # Аттестованный человеком ❌ (или иной absence_rule с уже пройденным I2b)
                # остаётся, но блокировки фиксируются в evidence — принятие риска видно.
                _record_corpus_coverage(
                    absence_evidence,
                    effective_coverage,
                    attested_by=scope_attested_by,
                )
        cell = _evaluated_cell(
            out_status=out.status,
            evidence=out.evidence,
            absence_evidence=absence_evidence,
            param=param,
            product_id=product_id,
            run_id=run_id,
            provenance=provenance,
        )
        _persist(
            store,
            cell,
            last_validation,
            protected=protected,
            prompt_hash=compiled.prompt_hash,
            model_id=request.model_id,
        )
        return EvaluationOutcome(
            cell=cell,
            prompt_hash=compiled.prompt_hash,
            validation=last_validation,
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
    cell = _unresolved_cell(
        state=state,
        param=param,
        product_id=product_id,
        run_id=run_id,
        provenance=provenance,
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
    scope_attested_by: str | None = None,
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
            scope_attested_by=scope_attested_by,
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
    scope_attested_by: str | None = None,
    eval_cache: EvalVerdictCache | None = None,
) -> PhaseResult:
    """Фаза оценки Ш6 на уровне конвейера: продукты × параметры → ретривал → оценщик.

    Композиция ровно того порядка, который фиксируют инварианты:

    1. для каждой пары ``(product, param)`` вызывается :func:`retrieve_for_cell` с
       **единым** профилем и ``cutoff_k`` (I16: симметрия; I20: один K на цикл);
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
    :param scope_attested_by: человеческая аттестация охвата для ``❌`` (Ф0); передаётся
        вызывающим из решения гейта/ревью, никогда не выводится автоматически.
    :param eval_cache: межпрогонный кэш сырых вердиктов оценщика; ``None`` ⇒ выключен
        (поведение байт-в-байт). Хит проходит все пост-шаги (валидация, Ф0, прецедент)
        заново — кэшируется только ``LLMCellOutput``, не ``Cell``.

    При ``ports.evaluator is None`` фаза сразу ``failed``: без оценщика Ш6 не имеет смысла,
    а молчаливый ``ok`` скрыл бы неполноту карты.
    """
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

    # Ш6 работает поверх более высоких источников (§2.2 п.7/п.8, 02 §D.3). Порядок фаз
    # заморожен (registry → inheritance → corpus → evaluate), поэтому карта стабильна.
    # Читаем один раз на главном потоке — не в воркерах ретривала/оценки.
    protected_cells = collect_protected_cells(store, run_id)

    for product_id in plan.products:
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
            kind = (
                FailureKind.NO_SOURCES.value
                if coverage.reason == "no_seeds"
                else (coverage.reason or FailureKind.NO_SOURCES.value)
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
                )
                _persist(store, failed, None)
                _record_cell_failure(store, failed, kind)
                cells.append(failed)
            errors.append(
                f"{product_id}: {kind}: expected_sources=0 "
                f"({coverage.reason or 'no coverage'}) — оценка невозможна"
            )
            continue
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
            scope_attested_by=scope_attested_by,
            eval_cache=eval_cache,
        )
        cells.extend(outcome.cell for outcome in outcomes)
        errors.extend(batch_errors)

    return PhaseResult(status="ok", phase="evaluate", cells=cells, errors=errors)
