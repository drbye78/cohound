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

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from core.domain import (
    AbsenceRuleType,
    CatalogRow,
    Cell,
    CellState,
    FailureKind,
    LLMCellOutput,
    Satellite,
    SourceKind,
    SourceScope,
    Status,
)
from core.domain.anchors import SourceResolver
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

__all__ = [
    "DEFAULT_RETRIEVAL_CUTOFF_K",
    "DEFAULT_RETRIEVAL_PROFILE",
    "EvaluationOutcome",
    "evaluate_batch",
    "evaluate_cell",
    "evaluate_phase",
    "retrieve_for_cell",
    "run_queries_for_cell",
]

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
    }


def _build_absence_evidence(
    param: CatalogRow, validation: ValidationResult
) -> tuple[list[dict], str | None]:
    """Строит ``absence_evidence`` для ``❌`` и проверяет I2b.

    Для ``explicit`` достаточно непустого списка: отсутствие подтверждается протоколом
    над переданным срезом. Для ``parameter_reference``/``distribution_absence``/
    ``registry_absence`` механически требуется ``source_scope=exhaustive`` (а для первых
    двух — ещё и ``scope_attested_by``, который ставит **человек**). Если подтверждённых
    цитат нет, ставится синтетическая запись протокола над срезом; недоказуемость на
    уровне эшелона (нехватка exhaustive/attestation) возвращается как ошибка — вызывающий
    уводит её в ``failed``/preliminary, а не публикует ``❌`` молча.
    """
    absences = [_absence_dict(ev) for ev in validation.resolved_evidence]
    if not absences:
        absences.append(
            {
                "evidence_id": None,
                "source_id": None,
                "source_scope": SourceScope.UNKNOWN.value,
                "scope_attested_by": None,
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


def _persist(
    store: StorePort | None, cell: Cell, validation: ValidationResult | None
) -> None:
    """Пишет ячейку и её evidence в store (идемпотентный upsert по PK/evidence_id, S8)."""
    if store is None:
        return
    store.save_cell(cell)
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
    )

    request = LLMRequest(
        prompt=compiled.text,
        response_schema=LLMCellOutput,
        model_id=model_id or getattr(evaluator, "model_id", "") or _UNSPECIFIED_MODEL_ID,
        model_version=getattr(evaluator, "model_version", "") or "",
        seed=getattr(evaluator, "seed", None),
        temperature=getattr(evaluator, "temperature", None),
    )

    provenance = _provenance(retrievals)

    last_response: LLMResponse | None = None
    last_validation: ValidationResult | None = None
    last_kind: str | None = None

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
        last_validation = validate_output(
            output=response.output,
            chunks=chunks_by_id,
            product_id=product_id,
            param_id=param.param_id,
            run_id=run_id,
            resolver=resolver,
            strict=strict,
            min_retrieval_score=min_retrieval_score,
        )
        if last_validation.ok:
            last_kind = None
            break
        last_kind = FailureKind.VALIDATION_FAILED.value

    assert last_validation is not None  # цикл выполняет ≥1 попытку

    # Успешная валидация — оцениваем ячейку.
    if last_validation.ok and last_response is not None:
        out = last_response.output
        absence_evidence: list[dict] = []
        if out.status is Status.ABSENT:
            absence_evidence, absence_error = _build_absence_evidence(param, last_validation)
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
        _persist(store, cell, last_validation)
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
) -> EvaluationOutcome:
    """Собирает исход несостоявшейся валидации: ``failed`` (I4) либо preliminary (I13).

    В strict ячейка получает ``state=failed`` и ``failure_kind``. В preliminary ячейка
    остаётся кандидатом (``state=preliminary``), ``status=None``, ``failure_kind=None``;
    причина — в ``validation.notes``. Дополнительные ``notes`` (например, I2b) вливаются
    в ``validation.notes`` без потери исходных.
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
    _persist(store, cell, None)
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

    for product_id in plan.products:
        chunks_by_param: dict[str, Sequence[ChunkLike]] = {}
        retrievals_by_param: dict[str, RetrievalResult] = {}
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
        )
        cells.extend(outcome.cell for outcome in outcomes)
        errors.extend(batch_errors)

    return PhaseResult(status="ok", phase="evaluate", cells=cells, errors=errors)
