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
empty:       chunks=() → evaluated ❓, validation_notes=[no_fragments] — вызова модели нет
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

**Выборочная пере-оценка устаревших ячеек (§13/K2).** Фаза пере-оценивает **все** пары
``product × param``; для ячеек, помеченных ``stale_reason`` (новая индексная сборка), это
означало бы платить за незатронутое. Поэтому у фазы есть адресный вход:
:func:`select_stale_cells` (селектор «ровно помеченные ячейки», читается из store, а не из
``stale_cells.csv``) и :func:`reevaluate_stale` (исполнение того же конвейера только по
селекции, со снятием пометки **лишь** при вердикте по текущей сборке). CLI-точка —
``hound revalidate``: она работает на существующем прогоне и ``run_id`` не меняет, потому
что состав объявленных условий измерения не меняется — доводится до конца работа Ш6,
которая была отложена.

Модуль категорийно нейтрален (§1.3), без сети, без новых зависимостей.
"""

from __future__ import annotations

import datetime as _dt
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Literal

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
    MeasurementOrigin,
    ReuseKind,
    Satellite,
    SourceKind,
    Status,
)
from core.domain.anchors import SourceResolver
from core.domain.invariants import check_absence_coverage
from core.domain.models import absence_evidence_id
from core.pipeline.carry_forward import (
    CarriedCell,
    CellReuseConditions,
    carry_forward_cell,
    cell_reuse_conditions,
    evidence_digest,
    measurement_row,
)
from core.pipeline.digests import scoring_policy_version
from core.pipeline.eval_cache import (
    EVAL_CACHE_FORMAT,
    EvalConditions,
    EvalVerdictCache,
    build_eval_key,
    catalog_row_digest,
)
from core.pipeline.exhaustion import ExhaustionFacts, build_protocols
from core.pipeline.precedence import (
    PRECEDENCE_REVIEW_REASON,
    collect_protected_cells,
    resolve_precedence,
)
from core.pipeline.prompt import (
    CompiledPrompt,
    PromptArchive,
    compile_prompt,
    render_queries,
)
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
from core.ports.state import CellMeasurementRow, ProductStatePort
from core.ports.store import StorePort
from core.ports.vector_index import (
    RetrievedChunk,
    VectorIndexPort,
    VectorQuery,
    batched_search,
)
from core.util.hashing import content_hash
from core.util.ids import parse_source_id

__all__ = [
    "ABSENCE_COVERAGE_UNSATISFIED_REASON",
    "ABSENCE_UNATTESTED_REVIEW_REASON",
    "CORPUS_INCOMPLETE_REVIEW_REASON",
    "DEFAULT_QUERY_EMBED_BATCH",
    "DEFAULT_RETRIEVAL_CUTOFF_K",
    "DEFAULT_RETRIEVAL_PROFILE",
    "DENIAL_QUOTE_WITHOUT_MARKERS",
    "NO_FRAGMENTS_REASON",
    "REUSE_UNCONFIRMED_REASON",
    "REVALIDATE_FAILED_PREFIX",
    "REVALIDATE_NOTHING_TO_DO",
    "REVALIDATE_SKIP_BUILD_MISMATCH",
    "REVALIDATE_SKIP_NO_EVALUATOR",
    "REVALIDATE_SKIP_NO_VERDICT",
    "REVALIDATE_STATUS_NOOP",
    "REVALIDATE_STATUS_REEVALUATED",
    "REVALIDATE_STATUS_REFUSED",
    "STALE_SKIP_NO_BUILD",
    "STALE_SKIP_PARAM",
    "STALE_SKIP_SOURCE_KIND",
    "STALE_SKIP_STATE",
    "STALE_SKIP_UNKNOWN_BUILD",
    "SUPPORT_CONCENTRATION_QUOTE_LIMIT",
    "SUPPORT_NOT_ABOUT_SUBJECT",
    "CorpusReuseObservation",
    "EvaluationOutcome",
    "RevalidationReport",
    "StaleSelection",
    "SupportSubjectOverlap",
    "compose_review_reason",
    "evaluate_batch",
    "evaluate_cell",
    "evaluate_phase",
    "observe_corpus_reuse",
    "precompute_dense_hits",
    "precompute_query_vectors",
    "reevaluate_stale",
    "retrieve_for_cell",
    "run_queries_for_cell",
    "select_stale_cells",
    "support_concentration",
    "support_overlap_counts",
    "support_overlap_scan",
    "support_subject_overlap",
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

#: Машинный токен I2c: ``❌`` **не публикуется**, потому что класс отрицания несовместим с
#: наблюдённым охватом корпуса (нет принятия риска, класс не даёт «нет» либо у строки нет
#: объекта силы отрицания). Пишется и в ``evidence_scope.reason`` (почему ``❓``), и в
#: ``cells.validation_notes`` (что именно не так) — предъявим после прогона, а не только
#: в консоли.
ABSENCE_COVERAGE_UNSATISFIED_REASON = "absence_coverage_unsatisfied"

#: Машинный токен правила «``❌`` требует ``live|revalidated``» (16 §8 п.4, 09 §11.2):
#: ``❌`` **не публикуется**, потому что наблюдение, на котором он держится, в этом цикле
#: сервером не подтверждено (``corpus_docs.reuse_kind`` ∈ ``reused``/``stale`` либо не
#: записано вовсе). Пишется и в ``evidence_scope.reason`` (почему ``❓``), и в
#: ``cells.validation_notes`` (что именно не так) — предъявим после прогона.
REUSE_UNCONFIRMED_REASON = "reuse_unconfirmed"

#: Классы отрицания, чьё «нет» держится на **конкретном** документе (цитата вендора либо
#: авторитетная энумерация): у них проверяется каждый документ, на который опирается ``❌``.
#: Остальные классы, дающие ``❌`` (``bounded``), выводят «нет» из **корпуса** — у них
#: проверяется весь корпус продукта: отсутствие не доказано, если часть корпуса в этом
#: цикле не наблюдалась (09 §11.2, два уровня правила).
_EVIDENCE_LEVEL_DENIALS: frozenset[DenialStrength] = frozenset(
    {DenialStrength.EXPLICIT, DenialStrength.AUTHORITATIVE}
)

#: Происхождение наблюдения, **подтверждающее** ``❌``: сервер ответил в этом цикле
#: (живой fetch либо 304 — версия документа не изменилась, но ответ состоялся).
_CONFIRMED_REUSE_KINDS: frozenset[ReuseKind] = frozenset(
    {ReuseKind.LIVE, ReuseKind.REVALIDATED}
)


@dataclass(frozen=True, slots=True)
class CorpusReuseObservation:
    """Снимок происхождения наблюдения корпуса продукта (``corpus_docs.reuse_kind``).

    Носитель правила «``❌`` требует ``live|revalidated``» на уровне **корпуса**: строится
    одним проходом по ``corpus_docs`` продукта, а не на ячейку — 181 параметр × продукты
    читали бы одну и ту же таблицу сотни раз. Срок жизни — вызов фазы: это наблюдение
    **этого** прогона, а не кэш между прогонами (иное означало бы публиковать ``❌`` по
    вчерашнему ответу сервера).

    ``by_doc`` — ``doc_id`` (``{product_id}:{doc_hash}``) → ``reuse_kind``; ``None`` в
    значении значит «наблюдение не записано» (legacy-строка), и это **не** подтверждение.
    ``by_doc_version`` — второй индекс: ``doc_version_id`` (``dv_...``) → тот же
    ``reuse_kind``. Два индекса, потому что у документа две формы адреса: ``doc_id`` —
    content-hash (на нём держится дедуп корпуса), а ``chunk_id``/``source_id`` несут
    версию (16 §6), и ``dv_`` из хеша не выводится. Это **одна** семантика «документ
    наблюдён» в двух формах, а не два правила: второй индекс выводится из первого через
    отображение ``corpus_doc_version`` (версия → документ), поэтому версий ровно столько,
    сколько их у документов корпуса, — а не одна на строку.
    """

    product_id: str
    by_doc: Mapping[str, ReuseKind | None]
    by_doc_version: Mapping[str, ReuseKind | None] = field(default_factory=dict)

    def is_applicable(self) -> bool:
        """Правило применимо только там, где есть наблюдения корпуса.

        Пустая карта — «корпуса нет вовсе»: проверять нечего, вердикт не меняется (та же
        дисциплина, что у I2c при ``coverage is None``).
        """
        return bool(self.by_doc) or bool(self.by_doc_version)

    def unconfirmed_docs(self) -> list[str]:
        """Документы корпуса без серверного подтверждения (сортировано — детерминизм)."""
        return sorted(
            doc_id
            for doc_id, kind in self.by_doc.items()
            if kind not in _CONFIRMED_REUSE_KINDS
        )


def observe_corpus_reuse(
    store: StorePort | None,
    run_id: str,
    product_id: str,
) -> CorpusReuseObservation | None:
    """Один проход по ``corpus_docs`` продукта — снимок для правила ``❌``/reuse.

    :returns: наблюдение продукта либо ``None``, если читать нечем (``store`` без
        ``iter_corpus_docs``): правило тогда неприменимо, вердикт не меняется. Сбой
        чтения здесь **не** глушится — его изолирует вызывающий (``errors`` фазы):
        выдать отказ чтения за «наблюдений нет» значило бы снять правило молча.

    Цена: две выборки таблиц на продукт (не на ячейку) — строки корпуса и отображение
    версий. ``corpus_docs`` не несёт ``run_id``, поэтому выборка — корпус прогона (та же
    семантика, что у ``iter_corpus_docs``).

    Второй индекс строится по отображению ``corpus_doc_version`` (версия → документ),
    которое отдаёт :meth:`SqliteStore.iter_corpus_doc_versions`. Версий у документа может
    быть несколько (``dv`` включает уровень извлечения, ``doc_id`` — нет: один и тот же
    текст на разных уровнях — один документ и две версии), поэтому версия берётся из
    отображения, а не из строки корпуса. Хранилище без этого читателя (или БД прошлой
    версии без таблицы) даёт пустой второй индекс: правило остаётся исполнимым по
    ``by_doc``, а не падает и не выдумывает версии.
    """
    reader = getattr(store, "iter_corpus_docs", None)
    if not callable(reader):
        return None
    by_doc: dict[str, ReuseKind | None] = {}
    for doc in reader(run_id):
        if doc.product_id != product_id:
            continue
        by_doc[str(doc.doc_id)] = doc.reuse_kind
    by_doc_version: dict[str, ReuseKind | None] = {}
    versions_reader = getattr(store, "iter_corpus_doc_versions", None)
    if callable(versions_reader):
        for version, doc_id in versions_reader(run_id, product_id).items():
            # Версия, чей документ не наблюдён строкой корпуса, в индекс не попадает:
            # «не найдено» — честный ответ, а ``None`` здесь означал бы наблюдение,
            # которого нет.
            if doc_id in by_doc:
                by_doc_version[str(version)] = by_doc[doc_id]
    return CorpusReuseObservation(
        product_id=product_id, by_doc=by_doc, by_doc_version=by_doc_version
    )


def _reuse_kind_text(kind: ReuseKind | None) -> str:
    """Текст значения ``reuse_kind`` для причины: ``NULL`` называется прямо."""
    return "не записано (NULL)" if kind is None else kind.value


@dataclass(frozen=True)
class CarryForwardContext:
    """Входы переноса ячеек (09 §10.2) для **одного** продукта одного прогона.

    Условия переноса — свойства прогона (правила, модель, шаблон), но отпечаток
    человеческой подписи **per product** (``scope_acceptance`` подписывается по продукту),
    поэтому контекст собирается на продукт. БД предшественника одна на прогон — её
    открывает composition root, а не фаза: путь ``runs/{run_id}/cohound.db`` знает
    раскладка, а ``pipeline`` раскладку не читает.

    ``state``/``previous_store`` — оба обязательны: без индекса измерений нечего
    сравнивать, без БД предшественника неоткуда взять доказательства. Отсутствие любого
    из двух означает «перенос выключен» — и это видно вызывающему, а не выдаётся за
    «ключи не совпали».
    """

    conditions: CellReuseConditions
    state: ProductStatePort
    previous_store: StorePort
    previous_run_id: str


def _carried_rules_hold(
    cell: Cell,
    *,
    param: CatalogRow,
    product_id: str,
    coverage: CorpusCoverage | None,
    corpus_reuse: CorpusReuseObservation | None,
    evidence: Sequence[Any] = (),
) -> bool:
    """Проходит ли перенесённая ячейка **текущие** правила допуска ``❌`` (I2c, §11.2).

    Перенос обязан проходить тот же эшелон, что и живая оценка, иначе он стал бы обходом
    инварианта: перенесённый ``❌`` опубликовался бы там, где измеренный уже понижен до
    ``❓``. Проверка спрашивает **те же две функции**, а не их копию:

    * :func:`_enforce_absence_coverage` (I2c) — класс отрицания против наблюдённого охвата;
    * :func:`_enforce_reuse_confirmed` (§11.2) — ``❌`` требует ``live|revalidated``.

    Демотирование не применяется к перенесённой ячейке: она **не переносится**, и Ш6 меряет
    её обычным путём — тогда причина понижения рождается в единственном месте (там же, где
    и для живого вердикта), а не дублируется здесь.

    ``I3`` (``❓`` требует протокола исчерпания или ``reviewed_by``) проверять здесь нечего:
    протокол строится фазой **после** ячеек, и перенесённая ``❓`` получает его наравне с
    измеренной — либо остаётся видимой аномалией, как и любая другая (I3 не ослабляется).

    ``evidence`` — типизированный evidence ячейки (``CellEvidence``). Он нужен правилу
    уровня evidence: после круга через store цитаты ``❌`` лежат **не** в
    ``Cell.absence_evidence`` (там остаётся только протокол отсутствия, у которого
    ``source_id`` нет), а в ``cell_evidence``. Проверять только ``absence_evidence``
    значило бы, что перенесённый ``❌`` проходит правило, которое сняло бы тот же ``❌``,
    измеренный в этом цикле, — то есть перенос стал бы обходом инварианта.
    """
    if cell.status is not Status.ABSENT:
        return True
    status_after_coverage, scope_after_coverage, demoted, _ = _enforce_absence_coverage(
        out_status=cell.status,
        scope=cell.evidence_scope,
        absence_evidence=cell.absence_evidence,
        param=param,
        product_id=product_id,
        coverage=coverage,
    )
    if demoted:
        return False
    _, _, demoted_reuse, _ = _enforce_reuse_confirmed(
        out_status=status_after_coverage,
        scope=scope_after_coverage,
        # Цитаты ``❌`` + протокол отсутствия: первое — документы, на которые опирается
        # «нет», второе — контур, в котором искали. Правило уровня evidence смотрит только
        # на записи с ``source_id``, поэтому смешение видов здесь безопасно.
        absence_evidence=[
            *cell.absence_evidence,
            *(_absence_dict(ev) for ev in evidence),
        ],
        param=param,
        product_id=product_id,
        coverage=coverage,
        corpus_reuse=corpus_reuse,
    )
    return not demoted_reuse


def _persist_carried(
    store: StorePort,
    carried: CarriedCell,
    *,
    prompt_hash: str | None,
) -> None:
    """Пишет перенесённую ячейку, копии её evidence и протокол отсутствия (S8).

    ``_persist`` не переиспользуется: он сливает ``validation_notes`` и накладывает
    прецедент источника, а перенос — это **готовая** ячейка прошлого прогона с уже
    принятыми решениями; повторная проверка прецедента запретила бы перенос ячейки,
    которая в прошлом цикле этот прецедент уже прошла (иначе она не была бы записана).
    Evidence пишется батчем: строки детерминированы (``evidence_id_for``), поэтому повтор
    переноса в тот же прогон — upsert, а не дубль.
    """
    store.save_cell(carried.cell)
    _record_cell_provenance(
        store, carried.cell, prompt_hash=prompt_hash, model_id=None
    )
    if carried.evidence:
        store.add_evidence_batch(carried.evidence)
    writer = getattr(store, "add_absence_evidence", None)
    if carried.absence_evidence and callable(writer):
        writer(carried.absence_evidence)


def _measured_at() -> str:
    """Метка времени измерения (ISO-8601, UTC) для строки ``cell_measurement``.

    Не входит в ключ переноса: время — провенанс, а не условие (иначе две одинаковые
    ячейки одного прогона имели бы разные ключи). Нужно читателю ``state.db``, который
    отвечает на вопрос «когда это мерили».
    """
    return _dt.datetime.now(_dt.UTC).isoformat()


def _carry_one(
    *,
    param: CatalogRow,
    product_id: str,
    run_id: str,
    carry: CarryForwardContext,
    coverage: CorpusCoverage | None,
    retrieved: Sequence[ChunkLike],
    retrieval: RetrievalResult | None,
    protected: Cell | None,
    corpus_reuse: CorpusReuseObservation | None,
    scope_acceptance: str | None,
    store: StorePort | None,
) -> EvaluationOutcome | None:
    """Пытается перенести ячейку; ``None`` — мерить (09 §10.2).

    Точка вызова — **между** ретривалом и :func:`evaluate_cell`: ключ переноса содержит
    отпечаток retrieved-набора этой ячейки, а до ретривала он невычислим.
    """
    if store is None:
        # Без хранилища перенос нечем зафиксировать: ячейка осталась бы только в памяти, и
        # отчёт/следующий цикл её не увидели бы. Это не «ключ не совпал», поэтому отказ
        # назван здесь, а не спрятан в условии ключа.
        return None
    carried = carry_forward_cell(
        param,
        product_id=product_id,
        run_id=run_id,
        prev_run_id=carry.previous_run_id,
        state=carry.state,
        prev_store=carry.previous_store,
        conditions=carry.conditions,
        coverage=coverage,
        retrieved=retrieved,
        retrieval=retrieval,
        protected=protected,
    )
    if carried is None:
        return None
    if not _carried_rules_hold(
        carried.cell,
        param=param,
        product_id=product_id,
        coverage=coverage,
        corpus_reuse=corpus_reuse,
        evidence=carried.evidence,
    ):
        return None
    _persist_carried(store, carried, prompt_hash=carried.prompt_hash)
    return EvaluationOutcome(
        cell=carried.cell,
        prompt_hash=carried.prompt_hash or "",
        validation=ValidationResult(
            ok=True,
            failure_kind=None,
            notes=[],
            resolved_evidence=[],
        ),
        # Перенесённая ячейка тоже получает строку индекса: иначе цепочка рвётся на
        # втором цикле — перенесённая ячейка не стала бы источником для третьего прогона
        # (09 §10.1).
        measurement=measurement_row(
            carried.cell,
            param=param,
            conditions=carry.conditions,
            coverage=coverage,
            retrieval=retrieval,
            retrieved=retrieved,
            evidence_digest_value=evidence_digest(carried.evidence),
            measured_at=_measured_at(),
            prompt_hash=carried.prompt_hash,
        ),
    )



#: Токен причины, по которой ``❌``-претензия модели не стала ``❌``: заявлен ``explicit``,
#: но цитаты нет. Отказ направлен наружу (ложное «нет» уезжает в карту как факт), поэтому
#: вердикт понижается до ``❓``, а не публикуется.
EXPLICIT_WITHOUT_CITATION_REASON = "explicit_without_citation"

#: Токен причины, по которой ``❌``-претензия модели без цитаты осталась ``not_stated``:
#: «материал релевантен, но дословного утверждения нет» — это незнание, не отсутствие.
ABSENT_CLAIM_WITHOUT_DENIAL_REASON = "absent_claim_without_denial_citation"

#: Первоклассная заметка **пустого среза**: оценщику не передан ни один фрагмент.
#:
#: Пустой срез — это не вопрос к модели, а факт о ретривале, поэтому вызов не делается
#: вовсе (``evaluate_cell`` → ``no_fragments``), а ячейка получает ``❓`` с этой заметкой.
#: Отличие от «искали, вендор молчит» принципиально: там модель прочитала фрагменты и
#: ничего не нашла в них, здесь материала не было ни одного. Без отдельного токена оба
#: состояния выглядели бы одинаково (`❓` + пустые ``validation_notes``), и «ноль
#: фрагментов» приходилось бы выяснять из БД.
NO_FRAGMENTS_REASON = "no_fragments"

#: Машинный префикс заметки о **неудачном** выборе цитаты отрицания: модель заявила
#: ``explicit``, но ни в одной подтверждённой цитате нет маркеров отрицания. Выбор тогда
#: падает на прежнее правило (первая цитата), и это видно — вердикт не меняется (это
#: предмет ревью, §2.2), но основание выбора опубликовано.
DENIAL_QUOTE_WITHOUT_MARKERS = "denial_quote_without_markers"

#: ``seq`` записи протокола отсутствия в ``evidence_id``. Фиксирован (а не «после цитат»),
#: чтобы повторная оценка ячейки перезаписывала ту же строку ``cell_evidence``.
_PROTOCOL_ABSENCE_SEQ = 0

#: Машинный префикс записи о **ремонте**: модель прислала ``✅``/``⚠️`` вместе с полями
#: отрицания. Вердикт наличия не оспаривается (I1/I22 в силе), поля отбрасываются, но
#: отбрасывание обязано быть видимым: иначе ремонт выглядит как «модель и не заявляла».
PRESENT_DENIAL_REPAIR_PREFIX = "denial_fields_ignored_on_present"

#: Классы отрицания, которые при наличии-вердикте считаются шумом провайдера: они ничего
#: не утверждают о вендоре («не заявлено»/«не наблюдалось»), поэтому отбрасываются без
#: последствий. Всё остальное (``explicit``, а в будущем любой авторитетный класс) —
#: **противоречие**: модель одновременно говорит «есть» и «доказано, что нет». Разрешать
#: его механикой нельзя (это предмет ревью), поэтому вердикт сохраняется, а противоречие
#: пишется в ``review_reason`` тем же текстом, что и запись о ремонте.
_NOISE_DENIAL_STRENGTHS: frozenset[DenialStrength] = frozenset(
    {DenialStrength.NOT_STATED, DenialStrength.UNOBSERVED}
)

#: Предел длины одной записи в ``cells.validation_notes``. Это опубликованное
#: свидетельство о дефекте ячейки, а не журнал: текст исключения валидатора содержит
#: ``input_value`` и легко достигает килобайтов, после чего артефакт перестаёт читаться.
VALIDATION_NOTE_LIMIT = 400

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
    :param support_overlap: замер «о предмете ли доказательства» (:func:`support_subject_overlap`);
        ``None`` — не мерили (не presence-вердикт либо предмет не измерим). Вердикт ячейки
        от него **не зависит**: это публикуемая величина, а не решение.
    :param measurement: строка ``cell_measurement`` — индекс переноса (09 §10.1). Батч её
        только **вычисляет** (это чистая функция условий и evidence), а пишет фаза: у
        ``state.db`` одно соединение и один владелец записи, и раздавать его телу батча
        значило бы связать оценку ячейки с портом состояния.
        ``None`` — механизм выключен (нет ``state.db``/условий оценки) либо ячейку в карте
        этого прогона представляет другой источник (см. ``protected`` в
        :func:`evaluate_batch`).
    """

    cell: Cell
    prompt_hash: str
    validation: ValidationResult
    failure_kind: str | None = None
    raw: str | None = field(default=None)
    support_overlap: SupportSubjectOverlap | None = field(default=None)
    measurement: CellMeasurementRow | None = field(default=None)


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
        for text, vector in zip(batch, vectors, strict=True):
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
    # Контракт ``batched_search``: ``hits[i]`` — выдача ``queries[i]``. ``strict=True`` —
    # чтобы реализация, вернувшая не то число выдач, падала видимо, а не теряла запросы.
    return {text: list(batch) for text, batch in zip(queries, hits, strict=True)}


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
        if (
            resolved_profile in (RetrievalProfile.R1, RetrievalProfile.R2)
            and dense_hits is None
        ):
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


def _primary_retrieval(
    retrievals: RetrievalResult | Sequence[RetrievalResult] | None,
) -> RetrievalResult | None:
    """Первый срез провенанса ячейки (``None`` — провенанса нет).

    Ячейка может нести несколько срезов (композиция плеч), но условия у них одни: профиль
    и ``cutoff_K`` фиксированы на цикл (I16/I20), и :func:`_provenance` берёт именно
    первый. Перенос и индекс переноса обязаны описывать **те же** условия, поэтому берут
    срез той же формулой, а не угадывают.
    """
    if retrievals is None:
        return None
    if isinstance(retrievals, RetrievalResult):
        return retrievals
    items = list(retrievals)
    return items[0] if items else None


def _retrieval_key_conditions(
    retrievals: RetrievalResult | Sequence[RetrievalResult] | None,
) -> tuple[str | None, int | None]:
    """``(retrieval_profile, cutoff_K)`` среза — условия ключа кэша вердиктов (I16/I20).

    Берутся из **провенанса** ретривала, а не из аргументов фазы: ключ обязан описывать
    тот профиль/K, под которым фактически получен переданный срез. ``None`` у профиля —
    «вызывающий провенанс не передал»: это отличимо от любого объявленного профиля и
    даёт промах при сравнении с прогоном, где провенанс есть (безопасное направление —
    лишний промах, а не чужой вердикт).
    """
    if retrievals is None:
        return None, None
    items = [retrievals] if isinstance(retrievals, RetrievalResult) else list(retrievals)
    if not items:
        return None, None
    first = items[0]
    return (first.profile or None), int(first.cutoff_K)


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


def _relevant_evidence_coverage(
    validation: ValidationResult, candidates: int
) -> float | None:
    """Доля **переданных оценщику** фрагментов, использованных в доказательстве.

    Формула: ``|{чанки, на которые ссылается прошедший цитатную сверку evidence}| /
    |{чанки, переданные оценщику}|``. Числитель берётся из ``validation.resolved_evidence``,
    то есть из evidence, чья цитата **дословно** подтверждена в тексте чанка (§10): так
    «использован» означает «процитирован», а не «упомянут в рассуждении», и замер нельзя
    улучшить болтливостью.

    ``None`` — не измерено: знаменателя нет (оценщику не передали ни одного фрагмента).
    Ноль здесь — **реальный** ноль (фрагменты переданы, ни один не процитирован), и он не
    подменяется ``None``: в проекте уже была ошибка, когда «не измерено» выдавалось за
    измеренное значение.
    """
    if candidates <= 0:
        return None
    cited = {
        str(ev.source_id)
        for ev in validation.resolved_evidence
        if ev.source_id is not None
    }
    return min(1.0, len(cited) / candidates)


def _compact_note(text: str) -> str:
    """Схлопывает пробелы и обрезает запись до :data:`VALIDATION_NOTE_LIMIT`."""
    collapsed = " ".join(str(text).split())
    if len(collapsed) <= VALIDATION_NOTE_LIMIT:
        return collapsed
    return collapsed[: VALIDATION_NOTE_LIMIT - 1].rstrip() + "…"


def _compact_notes(notes: Sequence[str]) -> list[str]:
    """Готовит заметки к записи в ``cells.validation_notes`` (без дублей, с пределом длины)."""
    compact: list[str] = []
    for note in notes:
        text = _compact_note(note)
        if text and text not in compact:
            compact.append(text)
    return compact


# ---------------------------------------------------------------------------
# Замер: о предмете ли цитата (fabricated_support без смены вердикта)
# ---------------------------------------------------------------------------

#: Префикс машинной записи «доказательства есть, но ни одно не упоминает предмет».
#: Пишется в ``cells.validation_notes`` (и, для ``✅``, в ``review_reason``) — это
#: **человеческий сигнал**, а не смена вердикта: I22 требует лишь непустую цитату, и
#: строка шапки документа формально её исполняет. Отчёт считает агрегат по этому префиксу.
SUPPORT_NOT_ABOUT_SUBJECT = "support_not_about_subject"

#: Предел длины цитаты в публикуемом замере концентрации поддержки
#: (:func:`support_concentration`). Полный текст цитаты уже лежит в ``cell_evidence``;
#: здесь нужен опознаваемый «якорь», чтобы одна и та же цитата читалась в отчёте как одна,
#: а не как килобайты текста. Это предел читаемости артефакта, а не порог решения.
SUPPORT_CONCENTRATION_QUOTE_LIMIT = 160

#: Длина стема: первые 5 символов слова. Это не подобранный порог, а грубая
#: нормализация словоформы: «Оптимизация»/«оптимизации» → ``оптим``, «SLRU»/``slru`` →
#: ``slru``. Порога «доля ≥ X» здесь нет вовсе — состояние определяется **фактом**
#: пересечения стемов (A3: подобранное число было бы предположением, а не измерением).
SUBJECT_STEM_LEN = 5

#: Минимальная длина слова, участвующего в стемминге: предлоги/союзы короче 4 букв
#: ничего не различают, а их стем — шум («и», «в», «на» → «на»).
SUBJECT_MIN_WORD_LEN = 4

#: Служебные слова названия параметра: не несут предмета, а совпадают со служебными
#: словами любой цитаты и дали бы ложное «про предмет».
SUBJECT_STOPWORDS: frozenset[str] = frozenset(
    {
        "для",
        "при",
        "или",
        "как",
        "что",
        "это",
        "под",
        "над",
        "без",
        "между",
        "через",
        "после",
        "сверх",
    }
)

#: Токен слова: латиница и кириллица обрабатываются одинаково (регистр снят заранее).
_SUBJECT_WORD_RE = re.compile(r"[0-9a-zа-я]+")

#: Состояние замера: хотя бы одна цитата пересекается с предметом.
SUPPORT_STATE_SUPPORTED = "supported"
#: Состояние замера: цитаты есть, ни одна не упоминает предмет.
SUPPORT_STATE_UNRELATED = "unrelated_support"
#: Состояние замера: прямых цитат нет — это не наша аномалия, её ловит I22.
SUPPORT_STATE_NO_QUOTES = "no_quotes"

#: Три состояния замера. Больше состояний нет: это классификация факта пересечения.
SupportSubjectState = Literal["supported", "unrelated_support", "no_quotes"]


def _subject_stems(text: str) -> frozenset[str]:
    """Стемы слов текста: первые :data:`SUBJECT_STEM_LEN` символов слова ≥4 букв.

    Регистр снимается, ``ё`` приводится к ``е`` (иначе «расчёт» и «расчет» — разные
    стемы), служебные слова отбрасываются. Стем — приближение, а не лингвистика:
    задача замера — отличить «цитата про предмет» от «цитата про что угодно другое»,
    поэтому ложное совпадение по одному грубому стему засчитывается как совпадение
    (замер консервативен в сторону «доказательство признано»).
    """
    normalized = str(text or "").lower().replace("ё", "е")
    stems: set[str] = set()
    for word in _SUBJECT_WORD_RE.findall(normalized):
        if len(word) < SUBJECT_MIN_WORD_LEN or word in SUBJECT_STOPWORDS:
            continue
        stems.add(word[:SUBJECT_STEM_LEN])
    return frozenset(stems)


@dataclass(frozen=True)
class SupportSubjectOverlap:
    """Замер «о предмете ли доказательства» для одной ячейки (без смены вердикта).

    :param state: :data:`SUPPORT_STATE_SUPPORTED` / :data:`SUPPORT_STATE_UNRELATED` /
        :data:`SUPPORT_STATE_NO_QUOTES`.
    :param quoted: сколько непустых прямых цитат рассмотрено.
    :param unrelated: сколько из них не пересекается с предметом по стемам.
    :param subject_stems: стемы предмета — чтобы читатель видел, **чем** мерили (иначе
        вердикт замера не проверить).
    """

    state: SupportSubjectState
    quoted: int
    unrelated: int
    subject_stems: frozenset[str] = frozenset()

    @property
    def related(self) -> int:
        """Сколько цитат упоминают предмет."""
        return self.quoted - self.unrelated

    def note(self) -> str | None:
        """Строка ``validation_notes`` для ``unrelated_support``; ``None`` — молчание.

        ``supported`` не пишется вовсе: заметки — свидетельство о дефектах, а норма
        засоряла бы артефакт. ``no_quotes`` — не наша аномалия (I22), молчим и здесь.
        """
        if self.state != SUPPORT_STATE_UNRELATED:
            return None
        return (
            f"{SUPPORT_NOT_ABOUT_SUBJECT}: "
            f"{self.unrelated}/{self.quoted} цитат не упоминают предмет"
        )

    def to_dict(self) -> dict[str, Any]:
        """JSON-совместимый вид замера (счётчики + чем мерили)."""
        return {
            "state": self.state,
            "quoted": self.quoted,
            "related": self.related,
            "unrelated": self.unrelated,
            "subject_stems": sorted(self.subject_stems),
        }


def support_subject_overlap(
    param: CatalogRow, quotes: Sequence[str]
) -> SupportSubjectOverlap | None:
    """Считает, сколько прямых цитат ячейки вообще упоминают предмет параметра.

    Предмет берётся из ``catalog.csv:title`` (при пустом ``title`` — из ``rule``):
    ``concept_terms`` для этого непригодны — в живом пакете они содержат языковые метки
    (``["ru","en"]``), а не понятия. Пересечение считается по стемам
    (:func:`_subject_stems`), без порога «доля ≥ X»: состояние — факт пересечения.

    Замер **не меняет вердикт** и не является гейтом: ``✅``/``⚠️`` остаются как есть,
    публикуемость не затрагивается. Его назначение — сделать видимым
    ``fabricated_support``: I22 требует непустую цитату, и строка шапки документа её
    формально исполняет, хотя предмета не касается.

    :param param: строка каталога — источник предмета (``title``, иначе ``rule``).
    :param quotes: цитаты прямых доказательств, прошедших цитатную сверку §10.
    :returns: замер; ``None`` — предмет не измерим (в ``title``/``rule`` нет ни одного
        слова ≥4 букв вне стоп-слов). Возвращается ``None``, а не ``supported``:
        «не измерено» не должно выглядеть измеренным значением.
    """
    stems = _subject_stems(param.title) or _subject_stems(param.rule)
    if not stems:
        return None
    present = [str(q) for q in quotes if (q or "").strip()]
    if not present:
        return SupportSubjectOverlap(
            state=SUPPORT_STATE_NO_QUOTES, quoted=0, unrelated=0, subject_stems=stems
        )
    unrelated = sum(1 for quote in present if not (_subject_stems(quote) & stems))
    state: SupportSubjectState = (
        SUPPORT_STATE_UNRELATED if unrelated == len(present) else SUPPORT_STATE_SUPPORTED
    )
    return SupportSubjectOverlap(
        state=state,
        quoted=len(present),
        unrelated=unrelated,
        subject_stems=stems,
    )


def _direct_quotes(validation: ValidationResult) -> list[str]:
    """Цитаты прямых доказательств из прошедшего сверку набора (§10, I22).

    Берётся ``resolved_evidence``, а не сырой выход модели: сверка §10 отбросила
    выдуманные цитаты, и мерить надо ровно то, что **сохранится** в ``cell_evidence``
    (и что увидит отчёт). ``absence``/``derived`` не участвуют: они не претендуют на
    доказательство наличия.
    """
    return [
        str(ev.citation_quote)
        for ev in validation.resolved_evidence
        if ev.evidence_type is EvidenceType.DIRECT
        and (ev.citation_quote or "").strip()
    ]


def compose_review_reason(*reasons: str | None) -> str | None:
    """Склеивает причины ревью в одну строку (``None``, если причин нет).

    ``review_reason`` — единственный канал человеческого сигнала на ячейке, и терять
    одну причину из-за появления второй нельзя: противоречие «наличие + отрицание» и
    «доказательство не о предмете» должны читаться оба. Склейка — не решение: порядок
    аргументов задаёт вызывающий, код ничего не отбрасывает и не приоритизирует.

    Функция публичная, потому что склеивают **разные фазы**: Ш6 (противоречие модели +
    постороннее доказательство) и Ш4 (унаследованная ``conditional``-причина +
    постороннее доказательство). Своя копия склейки в каждой фазе разошлась бы.
    """
    kept = [reason for reason in reasons if reason]
    return "; ".join(kept) if kept else None


def support_overlap_counts(cells: Sequence[Cell]) -> dict[str, Any]:
    """Агрегат замера по набору ячеек прогона — для отчёта (читается из заметок).

    Носитель, переживающий прогон, — ``cells.validation_notes``: ячейка считается
    помеченной, если среди заметок есть строка с префиксом
    :data:`SUPPORT_NOT_ABOUT_SUBJECT`. Считается и распределение по статусам: ``✅``
    с посторонним доказательством и ``⚠️`` с ним — разные по весу человеческие сигналы.

    .. note::

       Это счёт **по записям Ш6**: ячейки высоких источников (наследование/реестр)
       заметок Ш6 не несут, хотя их доказательство тоже может быть не о предмете.
       Полный замер по прогону — :func:`support_overlap_scan` (он берёт цитаты из
       ``cell_evidence`` любого вида и потому видит обе ветви).
    """
    flagged = [
        cell
        for cell in cells
        if any(
            str(note).startswith(SUPPORT_NOT_ABOUT_SUBJECT)
            for note in cell.validation_notes
        )
    ]
    by_status: dict[str, int] = {}
    for cell in flagged:
        key = _enum_text(cell.status)
        by_status[key] = by_status.get(key, 0) + 1
    return {
        "measurement": SUPPORT_NOT_ABOUT_SUBJECT,
        "cells_total": len(cells),
        "cells_flagged": len(flagged),
        "by_status": dict(sorted(by_status.items())),
    }


def support_overlap_scan(
    cells: Sequence[Cell],
    params: Mapping[str, CatalogRow],
    quotes_by_cell: Mapping[tuple[str, str], Sequence[str]],
) -> dict[str, Any]:
    """Замер «о предмете ли доказательства» **по прогону**: счётчик + адресный список.

    Единственная точка агрегата для отчёта. Цитаты берутся из ``cell_evidence`` по ключу
    ``(product_id, param_id)`` **любого вида** (``direct`` и ``derived``): у ячеек
    наследования доказательство производное, и именно там живёт наблюдавшийся дефект
    (шапка документа как «поддержка»). Считать только записи Ш6 значило бы показать
    ноль там, где дефект есть.

    Замер не меняет ни вердикт, ни публикуемость: он публикует величину и адреса.

    :param cells: ячейки прогона (``iter_cells`` стора).
    :param params: каталог прогона по ``param_id`` — источник предмета замера.
    :param quotes_by_cell: цитаты доказательств по ключу ``(product_id, param_id)``.
    :returns: ``cells_considered`` (presence-ячейки с измеримым предметом),
        ``cells_flagged`` (``unrelated_support``), ``by_status`` и адресный список
        ``cells`` с числами ``quoted``/``unrelated``.
    """
    considered = 0
    entries: list[dict[str, Any]] = []
    by_status: dict[str, int] = {}
    for cell in cells:
        if cell.status not in {Status.OK, Status.PARTIAL}:
            continue
        param = params.get(str(cell.param_id))
        if param is None:
            # Параметр не объявлен в каталоге — мерить нечего: предмета нет.
            continue
        measured = support_subject_overlap(
            param, quotes_by_cell.get((str(cell.product_id), str(cell.param_id)), ())
        )
        if measured is None:
            continue
        considered += 1
        if measured.state != SUPPORT_STATE_UNRELATED:
            continue
        key = _enum_text(cell.status)
        by_status[key] = by_status.get(key, 0) + 1
        entries.append(
            {
                "product_id": str(cell.product_id),
                "param_id": str(cell.param_id),
                "status": _enum_text(cell.status),
                "source_kind": _enum_text(cell.source_kind),
                "quoted": measured.quoted,
                "unrelated": measured.unrelated,
                "subject_stems": sorted(measured.subject_stems),
            }
        )
    entries.sort(key=lambda item: (item["product_id"], item["param_id"]))
    return {
        "measurement": SUPPORT_NOT_ABOUT_SUBJECT,
        "cells_considered": considered,
        "cells_flagged": len(entries),
        "by_status": dict(sorted(by_status.items())),
        "cells": entries,
    }


def _cell_quotes(
    quotes_by_cell: Mapping[tuple[str, str], Sequence[str]], key: tuple[str, str]
) -> list[str]:
    """Непустые уникальные цитаты ячейки в порядке появления.

    Идентичность цитаты — её **полный текст**: две ячейки «опираются на одну цитату»
    только если строки совпали целиком. Обрезка (:func:`_truncate_quote`) применяется
    позже и только к публикуемому виду, поэтому она не может ни склеить две разные
    цитаты, ни развести одну.
    """
    cleaned: list[str] = []
    for quote in quotes_by_cell.get(key, ()):
        text = str(quote or "").strip()
        if text and text not in cleaned:
            cleaned.append(text)
    return cleaned


def _truncate_quote(quote: str) -> str:
    """Публикуемый вид цитаты: схлопнутые пробелы, обрезка до предела замера."""
    collapsed = " ".join(quote.split())
    if len(collapsed) <= SUPPORT_CONCENTRATION_QUOTE_LIMIT:
        return collapsed
    return collapsed[: SUPPORT_CONCENTRATION_QUOTE_LIMIT - 1].rstrip() + "…"


def support_concentration(
    quotes_by_cell: Mapping[tuple[str, str], Sequence[str]],
    cells: Sequence[Cell],
) -> dict[str, Any]:
    """**Концентрация поддержки**: сколько presence-ячеек держится на одной цитате.

    Это факт о качестве карты, а не о продукте: если 19 разных параметров опираются на
    одну и ту же цитату, «поддержка» у них общая, и карта утверждает больше, чем
    доказала. Замер не меняет вердикты и не вводит порогов (A3): публикуется число
    (:attr:`max_share`) и адресный список, решение — за человеком.

    Что публикуется по продукту:

    * ``cells_present`` — presence-ячейки (``✅``/``⚠️``) — знаменатель доли;
    * ``cells_with_quotes`` / ``cells_without_quotes`` — у presence-ячейки цитата обязана
      быть (I1/I22); отсутствие видно отдельным счётчиком, а не прячется делением;
    * ``distinct_quotes`` — сколько **различных** цитат её поддерживает;
    * ``max_share`` — доля ячеек, опирающихся на самую частую цитату
      (``top_quote_count / cells_present``); ``0.0`` — цитат нет вовсе;
    * ``top_quote`` (обрезанная), ``top_quote_count`` и ``top_quote_source_kinds`` —
      распределение ``source_kind`` по ячейкам именно этой цитаты: одна паспортная
      визитка, размноженная наследованием на N параметров, отличается от N независимых
      корпусных цитат ровно этим распределением.

    Ничьи разрешаются детерминированно (счётчик, затем текст цитаты), поэтому повторный
    расчёт на тех же данных даёт тот же результат (R7).

    :param quotes_by_cell: цитаты доказательств по ключу ``(product_id, param_id)``.
    :param cells: ячейки прогона (``iter_cells`` стора).
    :returns: JSON-совместимый словарь: счётчики по продуктам + адресный список
        ``cells`` — ячейки, опирающиеся на самую частую цитату своего продукта.
        В БД и в файлы функция ничего не пишет.
    """
    presence = [
        cell for cell in cells if cell.status in {Status.OK, Status.PARTIAL}
    ]
    grouped: dict[str, list[Cell]] = {}
    for cell in presence:
        grouped.setdefault(str(cell.product_id), []).append(cell)

    products: dict[str, dict[str, Any]] = {}
    addresses: list[dict[str, Any]] = []
    for product_id in sorted(grouped):
        product_cells = sorted(grouped[product_id], key=lambda cell: str(cell.param_id))
        per_cell: dict[str, list[str]] = {}
        quote_counts: dict[str, int] = {}
        for cell in product_cells:
            param_id = str(cell.param_id)
            quotes = _cell_quotes(quotes_by_cell, (product_id, param_id))
            per_cell[param_id] = quotes
            for quote in quotes:
                quote_counts[quote] = quote_counts.get(quote, 0) + 1
        top_quote: str | None = None
        top_count = 0
        if quote_counts:
            top_quote, top_count = sorted(
                quote_counts.items(), key=lambda item: (-item[1], item[0])
            )[0]
        present = len(product_cells)
        kinds: dict[str, int] = {}
        for cell in product_cells:
            param_id = str(cell.param_id)
            if top_quote is None or top_quote not in per_cell[param_id]:
                continue
            kind = _enum_text(cell.source_kind)
            kinds[kind] = kinds.get(kind, 0) + 1
            addresses.append(
                {
                    "product_id": product_id,
                    "param_id": param_id,
                    "status": _enum_text(cell.status),
                    "source_kind": kind,
                }
            )
        products[product_id] = {
            "cells_present": present,
            "cells_with_quotes": sum(1 for quotes in per_cell.values() if quotes),
            "cells_without_quotes": sum(
                1 for quotes in per_cell.values() if not quotes
            ),
            "distinct_quotes": len(quote_counts),
            "max_share": round(top_count / present, 4) if present else 0.0,
            "top_quote": _truncate_quote(top_quote) if top_quote else None,
            "top_quote_count": top_count,
            "top_quote_source_kinds": dict(sorted(kinds.items())),
        }

    addresses.sort(key=lambda item: (item["product_id"], item["param_id"]))
    return {
        "measurement": "support_concentration",
        "quote_limit": SUPPORT_CONCENTRATION_QUOTE_LIMIT,
        "cells_present": len(presence),
        "cells_without_quotes": sum(
            entry["cells_without_quotes"] for entry in products.values()
        ),
        "products": products,
        "cells": addresses,
    }


def _present_status_denial(out: LLMCellOutput) -> tuple[list[str], str | None]:
    """Ремонт шумовых полей отрицания при наличии-вердикте (§7).

    Модель отвечает ``✅``/``⚠️`` с доказательством, но заполняет ``denial_strength``/
    ``denial_reason`` (шаблон перечисляет их в схеме ответа, а провайдер недетерминирован).
    Вердикт наличия доказывается evidence (I1/I22) и **не оспаривается** шумовым полем:
    ответ принимается, поля в артефакты не попадают.

    Различаются два случая:

    * ``denial_strength ∈ {not_stated, unobserved}`` — шум: запись о ремонте пишется,
      ``review_reason`` не трогается;
    * любой иной класс (``explicit``; в модели сейчас недостижим ``authoritative`` —
      :class:`~core.domain.enums.DenialClaim` разрешает модели только ``explicit`` и
      ``not_stated``) — **противоречие**: «есть» и «вендор прямо отрицает» одновременно.
      Вердикт сохраняется (доказательство наличия на месте), но противоречие видно в
      ``review_reason`` — тем же текстом, что и запись о ремонте.

    :returns: ``(заметки, review_reason)``; пустой список и ``None`` — полей не было.
    """
    if out.status not in {Status.OK, Status.PARTIAL}:
        return [], None
    strength = out.denial_strength
    reason = (out.denial_reason or "").strip()
    if strength is None and not reason:
        return [], None
    note = (
        f"{PRESENT_DENIAL_REPAIR_PREFIX}: "
        f"denial_strength={strength.value if strength is not None else 'none'}, "
        f"denial_reason={'present' if reason else 'none'}"
    )
    contradiction = strength is not None and strength not in _NOISE_DENIAL_STRENGTHS
    return [_compact_note(note)], note if contradiction else None


#: Маркеры отрицания/ограничения в подтверждённой цитате (RU и EN).
#:
#: Это **не** понимание смысла и не второй LLM-вызов: регулярное выражение над текстом
#: цитаты, опубликованное правило выбора. Маркер называет ровно то, чем вендор заявляет
#: отсутствие или ограничение; утвердительные обороты, которые легко спутать с
#: отрицанием, в набор **не** входят осознанно: «без дополнительной настройки» и «не
#: требуется настройка» — это утверждение наличия, а не отрицание.
_DENIAL_MARKER_RE = re.compile(
    r"(?iu)(?<!\w)(?:"
    r"не\s+(?:поддерж|реализ|предусмотр|допуска|может|работа|имеет|входит|включа|совместим)\w*"
    r"|недоступ\w*|невозможн\w*|отсутств\w*|запрещ\w*|исключ\w*|ограничен\w*"
    r"|not\s+supported|unsupported|doesn['’]?t\s+support|do\s+not\s+support"
    r"|no\s+support|not\s+available|cannot|can['’]?t\s+be\s+used|without\s+support"
    r"|disabled|restricted\s+to|only\s+(?:when|if|for)"
    r")"
)

def _denial_marker_count(text: str) -> int:
    """Сколько **различных** маркеров отрицания в тексте (повтор одного не считается)."""
    return len({match.casefold() for match in _DENIAL_MARKER_RE.findall(text)})

def _designated_denial_quote(quotes: Sequence[str]) -> tuple[str, list[str]]:
    """Выбирает цитату отрицания детерминированно, а не «первую попавшуюся».

    Зачем. Цитата отрицания — это доказательство, а ``quotes[0]`` — порядок, в котором
    модель перечислила evidence. Проба на двух цитатах («…добавлена поддержка…» и
    «…не поддерживаются…») давала ``❌`` с ``citation_quote`` = **первая**, то есть
    неотрицающая: отрицание опубликовано без своего доказательства. Живой экспозиции не
    было только потому, что ``❌`` не публиковался ни разу; ``evaluator_v4`` делает ветку
    ``explicit`` нормальной, и дефект перестаёт быть теоретическим.

    Правило (без LLM и без нового поля схемы ответа):

    * каждая цитата оценивается числом **различных** маркеров отрицания/ограничения
      (:data:`_DENIAL_MARKER_RE`) — длинная цитата с повтором одного слова не выигрывает
      у короткой с двумя разными;
    * берётся цитата с максимумом, при равенстве — первая по порядку (порядок evidence —
      часть входа, поэтому выбор детерминирован байт-в-байт);
    * если маркеров нет ни в одной цитате, остаётся прежнее правило (первая цитата), но
      основание публикуется заметкой :data:`DENIAL_QUOTE_WITHOUT_MARKERS`.

    Чего правило **не** делает: не решает, было ли отрицание (это по-прежнему объявление
    модели), не понижает статус (это предмет ревью, §2.2) и не трогает доказательство
    наличия у ``✅``/``⚠️``.

    Альтернатива — поле ``evidence_role`` в схеме ответа — отвергнута: это
    ``SCHEMA_VERSION``, constrained decoding и пере-прогон ради выбора из уже
    подтверждённых цитат, тогда как маркерный выбор ничего не расширяет и ничего не
    отвергает.
    """
    if not quotes:
        raise ValueError("цитата отрицания выбирается из непустого списка цитат")
    scores = [_denial_marker_count(quote) for quote in quotes]
    best = max(scores)
    if best == 0:
        return quotes[0], [
            f"{DENIAL_QUOTE_WITHOUT_MARKERS}: ни одна подтверждённая цитата не содержит "
            "маркеров отрицания — цитатой отрицания оставлена первая в evidence"
        ]
    return quotes[scores.index(best)], []

def _resolve_denial(
    *,
    param: CatalogRow,
    out: LLMCellOutput,
    validation: ValidationResult,
    coverage: CorpusCoverage | None,
    scope_acceptance: str | None,
    composition_id: str | None,
    candidates: int = 0,
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

    :param candidates: сколько фрагментов **передано** оценщику (знаменатель замера
        ``relevant_evidence_coverage``); ``0`` — замера нет, поле остаётся ``None``.
    :returns: ``(scope, notes)``: объект силы отрицания и диагностические заметки.
    """
    vector = _coverage_vector(coverage)
    # Замер релевантности живёт в том же векторе охвата: он отвечает на вопрос «сколько из
    # переданных фрагментов реально вошло в доказательство». Считается по подтверждённому
    # evidence, то есть по факту цитирования, а не по самоотчёту модели.
    vector["relevant_evidence_coverage"] = _relevant_evidence_coverage(
        validation, candidates
    )
    # Цитаты для выбора цитаты отрицания: непустые (но **исходные**, не нормализованные —
    # в ячейку и в scope попадает то, что вернула модель, §10).
    quotes: list[str] = []
    for ev in validation.resolved_evidence:
        quote = ev.citation_quote or ""
        if quote.strip():
            quotes.append(quote)
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
                designated, quote_notes = _designated_denial_quote(quotes)
                return (
                    EvidenceScope(
                        denial_strength=DenialStrength.EXPLICIT,
                        citation_quote=designated,
                        reason=claim_text or "прямое отрицание вендора (цитата приложена)",
                        basis=f"{basis_prefix}:explicit",
                        **vector,
                    ),
                    quote_notes,
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


def _enforce_absence_coverage(
    *,
    out_status: Status,
    scope: EvidenceScope | None,
    absence_evidence: Sequence[dict],
    param: CatalogRow,
    product_id: str,
    coverage: CorpusCoverage | None,
) -> tuple[Status, EvidenceScope | None, bool, list[str]]:
    """I2c: ``❌`` сверяется с **наблюдённым** охватом корпуса — единственное место вызова.

    Инвариант объявлен в домене (:func:`core.domain.invariants.check_absence_coverage`) и
    продублирован в отчёте, но до этой правки в конвейере **не исполнялся**: недоказанное
    «нет» уезжало в ``❓`` только потому, что :func:`_resolve_denial` выбирает класс по
    неполноте охвата. Совпадение по построению — не защита: любое послабление модели
    (или новый писатель ячейки, собирающий ``EvidenceScope`` мимо ``_resolve_denial``)
    публиковало бы ``❌`` на тонком корпусе молча.

    Почему нарушение даёт ``❓``, а не ``failed`` (I2/I2c/I4). Отсутствие принятия риска —
    **процедурное** условие, а не отказ прибора: краул и экстрактор отработали, измерение
    состоялось, но его охват не покрывает пространство. ``failed`` (I4) означает «прибор
    не смог» и выпадает из карты целиком, а здесь факт наблюдался — он просто не доказывает
    отсутствия. Поэтому ячейка **остаётся** в карте как ``❓`` с причиной (I12: ``❓``
    никогда не трактуется как ``❌``), и на ней нет протокола отсутствия: ``absence_evidence``
    — носитель утверждения «искали и не нашли», а утверждение не доказано.

    Кандидатная строка строится через ``model_construct`` намеренно: валидаторы домена
    такие комбинации **не собирают** (двойная защита), поэтому инвариант спрашивается у
    *предполагаемой* строки. Это второй рубеж, и он исполняется, а не подразумевается.

    ``coverage is None`` — записи покрытия нет (нет краула/историческая строка): проверка
    неприменима, вердикт не меняется (объявленная семантика I2c).

    :returns: ``(status, scope, demoted, notes)``. ``demoted`` говорит вызывающему, что
        протокол отсутствия публиковать нельзя; ``notes`` — машинные записи для
        ``cells.validation_notes`` (называют нарушенный компонент и виновника).
    """
    if out_status is not Status.ABSENT:
        # Не ``❌`` — вопрос I2c не стоит (домен проверяет то же условие у себя).
        return out_status, scope, False, []
    prospective = Cell.model_construct(
        run_id="",
        product_id=product_id,
        param_id=param.param_id,
        state=CellState.EVALUATED,
        status=out_status,
        absence_rule=param.absence_rule,
        absence_evidence=list(absence_evidence),
        evidence_scope=scope,
    )
    violations = check_absence_coverage(prospective, coverage)
    if not violations:
        return out_status, scope, False, []

    # Сообщение домена уже называет инвариант (``I2c:``) и недостающий компонент
    # (``scope_acceptance``/``evidence_scope``); приписываем только поле строки.
    detail = "; ".join(
        f"[{violation.field or '—'}] {violation.message}" for violation in violations
    )
    reason = (
        f"{ABSENCE_COVERAGE_UNSATISFIED_REASON}: ❌ не публикуется — {detail}"
    )
    demoted = (
        EvidenceScope(
            denial_strength=DenialStrength.NOT_STATED,
            reason=reason,
            basis="i2c_demoted",
        )
        if scope is None
        else scope.model_copy(
            update={
                "denial_strength": DenialStrength.NOT_STATED,
                "reason": reason,
                "basis": f"{scope.basis or 'pipeline'}->i2c_demoted",
            }
        )
    )
    notes = [
        _compact_note(
            f"{ABSENCE_COVERAGE_UNSATISFIED_REASON}: {product_id}/{param.param_id}: "
            f"I2c [{violation.field or '—'}] {violation.message}"
        )
        for violation in violations
    ]
    return Status.UNKNOWN, demoted, True, notes


def _evidence_level_reuse_violations(
    corpus_reuse: CorpusReuseObservation,
    absence_evidence: Sequence[dict],
) -> list[str]:
    """Уровень evidence: **каждый** документ, на который опирается ``❌``, обязан быть подтверждён.

    Источник документов — записи ``absence_evidence``: у них ``source_id`` формата
    ``product:doc:chunk_no[:build_id]`` (:mod:`core.util.ids`). Второй сегмент — **версия
    документа** (``dv_...``, 16 §6), тогда как ``corpus_docs.doc_id`` = ``f"{product_id}:{content_hash}"``
    (на хеше держится дедуп корпуса). Поэтому документ ищется в **двух формах**, и это
    одна семантика «документ наблюдён», а не два правила: сначала префиксная
    ``f"{product_id}:{doc}"`` в ``by_doc`` (так адресовали документы до границы A —
    ``content_hash``), затем ``doc`` в ``by_doc_version`` (``dv_``). Причина нарушения
    называет ту форму, которую искали: иначе оператор не знает, чего именно не хватило —
    строки корпуса или версии в ней. Записи без ``source_id`` (протокол отсутствия)
    документом не являются и пропускаются.
    """
    violations: list[str] = []
    seen: set[str] = set()
    for entry in absence_evidence:
        raw = entry.get("source_id")
        if not raw:
            continue
        try:
            ref = parse_source_id(str(raw))
        except ValueError:
            # Идентификатор есть, но адресует не документ: подтвердить нечего.
            violation = (
                f"источник '{raw}' не разбирается в документ — "
                "происхождение наблюдения не подтверждаемо"
            )
            if violation not in violations:
                violations.append(violation)
            continue
        doc_id = f"{ref.product_id}:{ref.doc_id}"
        if doc_id in seen:
            continue
        seen.add(doc_id)
        if ref.product_id != corpus_reuse.product_id:
            violations.append(
                f"документ {doc_id} вне корпуса продукта "
                f"{corpus_reuse.product_id} — подтвердить нечем"
            )
            continue
        if doc_id in corpus_reuse.by_doc:
            kind = corpus_reuse.by_doc[doc_id]
        elif ref.doc_id in corpus_reuse.by_doc_version:
            # Вторая форма адреса: ``source_id`` несёт версию документа (16 §6).
            kind = corpus_reuse.by_doc_version[ref.doc_id]
        else:
            violations.append(
                f"документ {doc_id} отсутствует в corpus_docs "
                f"(искали doc_id, затем doc_version_id={ref.doc_id}) — "
                "наблюдение не записано"
            )
            continue
        if kind not in _CONFIRMED_REUSE_KINDS:
            violations.append(
                f"документ {doc_id}: reuse_kind={_reuse_kind_text(kind)}"
            )
    return violations


def _corpus_level_reuse_violations(
    corpus_reuse: CorpusReuseObservation,
) -> list[str]:
    """Уровень корпуса: **ни один** документ продукта не ``reused``/``stale``/``NULL``.

    «Нет» у этих классов выводится из корпуса, поэтому документ, не наблюдённый в этом
    цикле, снимает ``❌`` так же, как неполное покрытие снимает его у ``bounded``: часть
    корпуса не наблюдалась — отсутствие не доказано. Причина называет **счётчики по
    классам** и примеры документов, а не только «нельзя»: иначе оператор не знает, чинить
    ли краул, resume или ревалидацию.
    """
    unconfirmed = corpus_reuse.unconfirmed_docs()
    if not unconfirmed:
        return []
    counts: dict[str, int] = {}
    for doc_id in unconfirmed:
        counts[_reuse_kind_text(corpus_reuse.by_doc[doc_id])] = (
            counts.get(_reuse_kind_text(corpus_reuse.by_doc[doc_id]), 0) + 1
        )
    breakdown = ", ".join(f"{kind}={count}" for kind, count in sorted(counts.items()))
    sample = ", ".join(unconfirmed[:3]) + ("…" if len(unconfirmed) > 3 else "")
    return [
        f"корпус продукта {corpus_reuse.product_id}: {len(unconfirmed)} из "
        f"{len(corpus_reuse.by_doc)} документов без серверного подтверждения "
        f"({breakdown}); например {sample}"
    ]


def _enforce_reuse_confirmed(
    *,
    out_status: Status,
    scope: EvidenceScope | None,
    absence_evidence: Sequence[dict],
    param: CatalogRow,
    product_id: str,
    coverage: CorpusCoverage | None,
    corpus_reuse: CorpusReuseObservation | None,
) -> tuple[Status, EvidenceScope | None, bool, list[str]]:
    """Правило «``❌`` требует ``live|revalidated``» (16 §8 п.4, 09 §11.2) — единственное место вызова.

    Два уровня, потому что у двух групп классов **разный носитель утверждения**:

    * уровень **evidence** (``explicit``/``authoritative``) — «нет» держится на конкретном
      документе, поэтому проверяется **каждый** документ, на который опирается ``❌``;
    * уровень **корпуса** (``bounded`` и прочие классы, дающие ``❌``) — «нет» выводится из
      корпуса, поэтому ``reused``/``stale``/``NULL``-документ в корпусе продукта снимает
      ``❌`` целиком.

    Почему нарушение даёт ``❓``, а не ``failed`` (I4). Неподтверждённое наблюдение —
    **процедурное** условие публикации, а не отказ прибора: краул отработал, но сервер в
    этом цикле не отвечал за документ. ``failed`` выпадает из карты целиком, а здесь факт
    остаётся в карте как ``❓`` (I12: ``❓`` никогда не трактуется как ``❌``), и протокол
    отсутствия снимается: «искали и не нашли» — утверждение, которое не доказано.

    ``coverage is None`` (нет записи покрытия: нет краула/legacy) и отсутствие наблюдений
    корпуса делают проверку **неприменимой** — та же дисциплина, что у I2c
    (:func:`_enforce_absence_coverage`): нет наблюдений — нечего проверять.

    Порядок с I2c. Проверка вызывается **после** неё и только для уцелевшего ``❌``: если
    I2c уже снял ``❌``, ячейка ``❓`` и второй причины не появляется (``out_status`` на
    входе — результат I2c).

    :returns: ``(status, scope, demoted, notes)``; ``demoted`` запрещает публиковать
        протокол отсутствия, ``notes`` — машинные записи для ``cells.validation_notes``.
    """
    if out_status is not Status.ABSENT:
        return out_status, scope, False, []
    if coverage is None or corpus_reuse is None or not corpus_reuse.is_applicable():
        return out_status, scope, False, []

    evidence_level = (
        scope is not None and scope.denial_strength in _EVIDENCE_LEVEL_DENIALS
    )
    violations = (
        _evidence_level_reuse_violations(corpus_reuse, absence_evidence)
        if evidence_level
        else _corpus_level_reuse_violations(corpus_reuse)
    )
    if not violations:
        return out_status, scope, False, []

    level = "evidence" if evidence_level else "corpus"
    detail = "; ".join(violations)
    reason = (
        f"{REUSE_UNCONFIRMED_REASON}: ❌ не публикуется — {product_id}/{param.param_id}: "
        f"уровень {level}: {detail}"
    )
    demoted = (
        EvidenceScope(
            denial_strength=DenialStrength.NOT_STATED,
            reason=reason,
            basis="reuse_unconfirmed",
        )
        if scope is None
        else scope.model_copy(
            update={
                "denial_strength": DenialStrength.NOT_STATED,
                "reason": reason,
                "basis": f"{scope.basis or 'pipeline'}->reuse_unconfirmed",
            }
        )
    )
    notes = [
        _compact_note(
            f"{REUSE_UNCONFIRMED_REASON}: {product_id}/{param.param_id}: "
            f"{level}: {violation}"
        )
        for violation in violations
    ]
    return Status.UNKNOWN, demoted, True, notes


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
            computed_at=_dt.datetime.now(_dt.UTC).isoformat(),
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
    validation_notes: Sequence[str] = (),
    review_reason: str | None = None,
) -> Cell:
    """Собирает успешную ``evaluated``-ячейку (§2.2; I1/I2/I3/I4/I13-совместимую).

    ``validation_notes`` — свидетельство о дефектах оценки (ремонт шумовых полей
    отрицания, аномалии §10.4, доказательство не о предмете): пишется в
    ``cells.validation_notes`` и переживает прогон, поэтому читается отчётом/ревью, а не
    только консолью. ``review_reason`` заполняется противоречием «наличие + прямое
    отрицание» (см. :func:`_present_status_denial`) и посторонним доказательством у ``✅``
    (:func:`support_subject_overlap`): вердикт не оспаривается, но конфликт виден.
    """
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
        review_reason=review_reason,
        validation_notes=list(validation_notes),
    )


def _unresolved_cell(
    *,
    state: CellState,
    param: CatalogRow,
    product_id: str,
    run_id: str,
    provenance: tuple[str | None, str | None, str | None],
    evidence_scope: EvidenceScope | None = None,
    validation_notes: Sequence[str] = (),
) -> Cell:
    """Собирает ``failed``/``preliminary``-ячейку без статуса (I4/I13).

    ``failed`` несёт класс ``instrument_failed``: прибор не смог, а не «вендор не заявил»
    (это разные вещи, и различие видно в карте). ``preliminary`` — кандидат без статуса:
    вердикта ещё нет, поэтому объект силы отрицания не заполняется.

    ``validation_notes`` нужны именно здесь: без них причина отказа (класс исключения,
    провал цитатной сверки) исчезала после прогона, и её приходилось печатать в консоль.
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
        validation_notes=list(validation_notes),
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


def _no_fragments_outcome(
    *,
    param: CatalogRow,
    product_id: str,
    run_id: str,
    provenance: tuple[str | None, str | None, str | None],
    prompt_hash: str,
    store: StorePort | None,
    protected: Cell | None = None,
) -> EvaluationOutcome:
    """Исход **пустого среза**: ``❓`` с заметкой ``no_fragments`` и без вызова модели.

    Почему не вызов. Пустой срез — отсутствие материала, а не вопрос; модель на нём
    отвечает «не установлено», и этот ответ неотличим от честного «прочитал фрагменты,
    утверждения нет». Цена при этом реальная: один оплаченный запрос на ячейку. Заметка
    :data:`NO_FRAGMENTS_REASON` делает различие машинно-читаемым в
    ``cells.validation_notes`` — «материала не было» отдельно от «материал был, вендора
    в нём нет».

    Класс отрицания — ``not_stated`` с причиной ``no_fragments``: незнание, называющее
    свою причину. ``unobserved``/``bounded`` здесь не ставятся осознанно — они говорят о
    **неполноте охвата**, а не об отсутствии фрагментов после ретривала; и ``❌`` из
    пустого входа не следует ни по одному правилу (I2).
    """
    scope = EvidenceScope(
        denial_strength=DenialStrength.NOT_STATED,
        reason=(
            f"{NO_FRAGMENTS_REASON}: ретривал не передал ни одного фрагмента — "
            "вопрос оценщику не задавался, а вызов модели не тратился"
        ),
        # Знаменателя нет: «0 из 0» — не ноль и не единица, а не измерено.
        relevant_evidence_coverage=None,
        basis="pipeline:no_fragments",
    )
    cell = _evaluated_cell(
        out_status=Status.UNKNOWN,
        evidence=[],
        absence_evidence=[],
        param=param,
        product_id=product_id,
        run_id=run_id,
        provenance=provenance,
        evidence_scope=scope,
        validation_notes=_compact_notes([NO_FRAGMENTS_REASON]),
    )
    # ``model_id=None``: модель не вызывалась, и записывать её как источник вердикта
    # значило бы соврать о провенансе. ``prompt_hash`` пишется: промпт ячейки существует.
    _persist(store, cell, None, protected=protected, prompt_hash=prompt_hash)
    return EvaluationOutcome(
        cell=cell,
        prompt_hash=prompt_hash,
        validation=ValidationResult(
            ok=True,
            failure_kind=None,
            notes=_compact_notes([NO_FRAGMENTS_REASON]),
            resolved_evidence=[],
        ),
    )

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
    eval_conditions: EvalConditions | None = None,
    prompt_archive: PromptArchive | None = None,
    corpus_reuse: CorpusReuseObservation | None = None,
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
        Покрытие задаёт **класс** отрицания (``unobserved`` при неполном охвате) и —
        через I2c (:func:`_enforce_absence_coverage`) — проверяет, согласован ли ``❌``
        с наблюдённым охватом. ``coverage=None`` (записи нет) делает I2c неприменимым:
        вердикт не меняется.
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
    :param eval_conditions: объявленные методологические условия вердикта
        (:class:`EvalConditions`: ``methodology_version``, ``status_dict_version``,
        политика расчёта §6). **Обязательны при включённом кэше**: без них ключ описывал
        бы промпт и модель, но не правила ответа, и кэш мог бы вернуть вердикт,
        посчитанный по старым правилам (09 §10.5). ``None`` допустим только при
        ``eval_cache=None``; иначе — :class:`ValueError`, а не тихий промах-каждый-раз.
    :param prompt_archive: каталог аудита исполненных промптов (``prompts/{prompt_hash}
        .json``, V7). ``None`` ⇒ артефакт не пишется (поведение байт-в-байт прежнее).
        Сохраняется **тот же** объект :class:`CompiledPrompt`, из которого построен
        ``LLMRequest``, — не пере-собранный: иначе артефакт описывал бы другой вход.
        Хит ``eval_cache`` не пишется: модель в этом прогоне не вызывалась, и промпт не
        был исполнен (кэш ключуется текстом, см. ``eval_echo`` ниже — ключ не меняется).
    :param corpus_reuse: снимок происхождения наблюдения корпуса продукта
        (:class:`CorpusReuseObservation`): им исполняется правило «``❌`` требует
        ``live|revalidated``» (16 §8 п.4, 09 §11.2). Строится **один раз на продукт**
        вызывающим (:func:`observe_corpus_reuse`), а не на ячейку. ``None`` ⇒ правило
        неприменимо (модульный вызов/нет чтения корпуса), вердикт не меняется.

    **Пустой срез не тратит вызов модели.** При ``chunks == ()`` промпт компилируется
    (провенанс: ``prompt_hash`` ячейки остаётся честной функцией входа), но оценщик не
    вызывается: ``no_fragments`` — факт о ретривале, а не вопрос, на который модель
    отвечает. Ячейка получает ``❓`` с первоклассной заметкой :data:`NO_FRAGMENTS_REASON`,
    отличимой от «искали, вендор молчит» (там фрагменты были). Это не экономия ради
    экономии: вызов на пустом входе давал 1 оплаченный запрос на ячейку и вердикт,
    который нельзя было отличить от честного незнания.

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

    # Пустой срез — факт о ретривале, а не вопрос к модели: вызова нет, ячейка ``❓`` с
    # первоклассной заметкой ``no_fragments``. Промпт уже скомпилирован выше, поэтому
    # провенанс ячейки (``prompt_hash``) остаётся функцией входа.
    if not chunks_by_id:
        return _no_fragments_outcome(
            param=param,
            product_id=product_id,
            run_id=run_id,
            provenance=_provenance(retrievals),
            prompt_hash=compiled.prompt_hash,
            store=store,
            protected=protected,
        )

    request = LLMRequest(
        prompt=compiled.text,
        response_schema=LLMCellOutput,
        model_id=model_id or getattr(evaluator, "model_id", "") or _UNSPECIFIED_MODEL_ID,
        model_version=getattr(evaluator, "model_version", "") or "",
        seed=getattr(evaluator, "seed", None),
        temperature=getattr(evaluator, "temperature", None),
    )

    # Ключ кэша вердиктов: **отрендеренный** текст промпта, конфигурация оценщика и
    # правила, по которым вердикт считается (09 §10.5). ``prompt_hash`` (§5) входит ради
    # аудита, но не является ключом: он не покрывает
    # product/title/satellites/extra_instructions/текст чанков. ``run_id``/``build_id``/
    # ``strict``/coverage/resolver/прецедент намеренно вне ключа: они либо не меняют
    # вердикт, либо пере-исполняются на хите. Методологические условия — **в** ключе:
    # смена правил меняет вердикт, не меняя промпта.
    eval_echo: dict[str, Any] | None = None
    eval_key: str | None = None
    if eval_cache is not None:
        if eval_conditions is None:
            # Кэш без объявленных условий — тот самый дефект §10.5: ключ описывал бы
            # промпт и модель, но не правила ответа. Отказ громкий: тихий промах-каждый-раз
            # выглядел бы как «кэш работает», а кэш с неполным ключом — как «кэш работает
            # правильно».
            raise ValueError(
                "eval_cache требует объявленных условий оценки (eval_conditions): "
                "ключ без methodology_version/status_dict_version/политики расчёта "
                "обслуживал бы вердикт, посчитанный по старым правилам (09 §10.5)"
            )
        retrieval_profile, retrieval_cutoff = _retrieval_key_conditions(retrievals)
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
            # V3: повтор переиздаёт запрос, значит «первый валидный ответ» зависит от
            # политики повтора — она входит в условия, а не остаётся за ключом.
            "max_retries": max_retries,
            **eval_conditions.as_echo(),
            # Строка каталога: rule/absence_rule/param_type/role_binding/… задают, что
            # считается заявленным, а что отсутствием. Дайджест всей строки — чтобы не
            # поддерживать вручную перечень «влияющих» полей.
            "catalog_row_digest": catalog_row_digest(param),
            # I16/I20: профиль и K среза — из провенанса ретривала, не пересчитываются.
            "retrieval_profile": retrieval_profile,
            "cutoff_K": retrieval_cutoff,
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
        # V7/§7.1: артефакт аудита — **ровно то, что уходит в модель**. Сохраняется
        # `compiled`, из которого построен `request`, ниже по коду (не пере-сборка).
        # Пишется один раз на ячейку независимо от числа повторов: вход у повторов тот же.
        if prompt_archive is not None:
            prompt_archive.record(compiled)
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

    if last_validation is None:  # цикл обязан исполнить ≥1 попытку (max_retries ≥ 0)
        raise RuntimeError("цикл валидации не исполнил ни одной попытки: max_retries < 0")

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
            candidates=len(chunks_by_id),
        )
        out_status = out.status if evidence_scope is None else evidence_scope.verdict()
        # Ремонт шумовых полей отрицания при наличии-вердикте (§7). Вердикт не
        # оспаривается; отбрасывание видно в validation_notes, противоречие —
        # дополнительно в review_reason.
        repair_notes, contradiction = _present_status_denial(out)
        # Замер «о предмете ли доказательства» — только для наличия (``✅``/``⚠️``):
        # у ``❓``/``❌`` цитата — наблюдение, а не поддержка. Вердикт замера не меняет
        # (I22 требует лишь непустую цитату), но делает fabricated_support видимым.
        support_overlap = (
            support_subject_overlap(param, _direct_quotes(last_validation))
            if out_status in {Status.OK, Status.PARTIAL}
            else None
        )
        support_note = support_overlap.note() if support_overlap is not None else None
        support_reason = support_note if out_status is Status.OK else None
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
        # I2c: объявленный инвариант исполняется здесь и только здесь. Если класс
        # отрицания не согласован с наблюдённым охватом, ``❌`` не публикуется: ячейка
        # становится ``❓`` с причиной, а протокол отсутствия снимается (см.
        # :func:`_enforce_absence_coverage`).
        out_status, evidence_scope, demoted, coverage_notes = _enforce_absence_coverage(
            out_status=out_status,
            scope=evidence_scope,
            absence_evidence=absence_evidence,
            param=param,
            product_id=product_id,
            coverage=effective_coverage,
        )
        if demoted:
            # Протокол пустоты на ``❓``-ячейке был бы утверждением «искали и не нашли».
            absence_evidence = []
            denial_notes = [*denial_notes, *coverage_notes]
        # 16 §8 п.4 / 09 §11.2: «``❌`` требует ``live|revalidated``». Проверка идёт
        # **после** I2c и только для уцелевшего ``❌``: если I2c уже снял его, ячейка —
        # ``❓`` (``out_status`` на входе — результат I2c) и второй причины не появляется.
        # Нарушение — тоже ``❓`` с причиной и без протокола отсутствия: неподтверждённое
        # наблюдение — процедурное условие, а не отказ прибора (I4).
        out_status, evidence_scope, reuse_demoted, reuse_notes = (
            _enforce_reuse_confirmed(
                out_status=out_status,
                scope=evidence_scope,
                absence_evidence=absence_evidence,
                param=param,
                product_id=product_id,
                coverage=effective_coverage,
                corpus_reuse=corpus_reuse,
            )
        )
        if reuse_demoted:
            absence_evidence = []
            denial_notes = [*denial_notes, *reuse_notes]
        cell = _evaluated_cell(
            out_status=out_status,
            evidence=out.evidence,
            absence_evidence=absence_evidence,
            param=param,
            product_id=product_id,
            run_id=run_id,
            provenance=provenance,
            evidence_scope=evidence_scope,
            # Заметки эшелона §10 (аномалии «✅ на одной цитате», низкий score), ремонт и
            # понижение класса — всё это свидетельства о качестве ячейки, а не отладка.
            validation_notes=_compact_notes(
                [
                    *repair_notes,
                    *last_validation.notes,
                    *denial_notes,
                    *([support_note] if support_note else []),
                ]
            ),
            # ``review_reason`` — единственный человеческий сигнал на ячейке; причины
            # складываются, а не затирают друг друга (противоречие + постороннее
            # доказательство). Решение по ним принимает человек, не код.
            review_reason=compose_review_reason(contradiction, support_reason),
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
            support_overlap=support_overlap,
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
        # Причина отказа обязана пережить прогон: без неё в артефактах остаётся
        # `failed` без объяснения, и тип исключения приходится печатать в консоль.
        validation_notes=_compact_notes(merged),
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
    eval_conditions: EvalConditions | None = None,
    prompt_archive: PromptArchive | None = None,
    corpus_reuse: CorpusReuseObservation | None = None,
    carry: CarryForwardContext | None = None,
    reuse_conditions: CellReuseConditions | None = None,
) -> tuple[list[EvaluationOutcome], list[str]]:
    """Прогоняет :func:`evaluate_cell` по параметрам с изоляцией сбоев (§A.1).

    Возвращает ``(успешные_исходы, ошибки)``: исключение на одном параметре не роняет
    батч (``run_isolated``). Порядок исходов соответствует порядку ``params``.

    **Контракт ``chunks_by_param``.** Ключ обязан быть на каждый параметр: пустой срез —
    это наблюдённый факт («ретривал отработал и ничего не нашёл»), а отсутствие ключа —
    «ретривал не состоялся». Смешивать их нельзя: во втором случае оценщик получал бы
    пустой вход и его ответ выдавался бы за наблюдение, хотя прибора не было (I4).

    **Перенос ячеек (09 §10.2).** ``carry`` не ``None`` — перед :func:`evaluate_cell`
    спрашивается :func:`core.pipeline.carry_forward.carry_forward_cell`. Порядок жёсткий:
    сначала ретривал (ключ переноса содержит отпечаток retrieved-набора **этой** ячейки),
    потом ключ, потом оценщик. Перенесённая ячейка возвращается как обычный исход — с
    ``measurement_origin=carried`` и ``carried_from_run_id``, поэтому счётчик
    ``cells_carried`` не требует отдельного канала.

    **Индекс переноса (09 §10.1).** ``reuse_conditions`` не ``None`` — каждая измеренная
    ячейка получает строку ``cell_measurement`` (``EvaluationOutcome.measurement``). Это
    **не** то же условие, что ``carry``: индекс обязан писаться уже на **первом** прогоне,
    иначе второму нечего сравнивать. ``carry`` отвечает на «можно ли не мерить», индекс —
    на «чем мерили», поэтому при ``carry=None`` и ``reuse_conditions`` заданных переноса
    нет, а индекс есть.
    """

    def _one(param: CatalogRow) -> EvaluationOutcome:
        # Контракт карты: ключ на **каждый** переданный параметр. Отсутствие ключа —
        # это «ретривал не состоялся», а не пустой срез: подстановка ``()`` превращала
        # отказ прибора в измеренное ``❓``/``❌`` (I4). Сбой остаётся видимым
        # (``run_isolated`` → ``errors``), а не маскируется пустым входом.
        if param.param_id not in chunks_by_param:
            raise ValueError(
                f"chunks_by_param не содержит ключ {param.param_id!r}: отсутствие "
                "ключа означает, что ретривал не состоялся, а не что срез пуст"
            )
        chunks = chunks_by_param[param.param_id]
        retrieval = (retrievals_by_param or {}).get(param.param_id)
        # Перенос/индекс переноса описывают **один** срез: условия у ячейки одни
        # (I16/I20), а ``retrieval`` остаётся объединением для ``evaluate_cell``.
        primary_retrieval = _primary_retrieval(retrieval)
        protected = (protected_by_param or {}).get(param.param_id)
        if carry is not None:
            # 09 §10.2: перенос — **между** ретривалом и оценщиком. Ключ содержит
            # отпечаток retrieved-набора этой ячейки, поэтому раньше ретривала он
            # невычислим, а позже оценщика — уже поздно (вызов потрачен).
            carried_outcome = _carry_one(
                param=param,
                product_id=product_id,
                run_id=run_id,
                carry=carry,
                coverage=coverage,
                retrieved=chunks,
                retrieval=primary_retrieval,
                protected=protected,
                corpus_reuse=corpus_reuse,
                scope_acceptance=scope_acceptance,
                store=store,
            )
            if carried_outcome is not None:
                return carried_outcome
        outcome = evaluate_cell(
            param=param,
            product_id=product_id,
            run_id=run_id,
            evaluator=evaluator,
            chunks=chunks,
            satellites=satellites,
            resolver=resolver,
            strict=strict,
            retrievals=retrieval,
            max_retries=max_retries,
            store=store,
            protected=protected,
            coverage=coverage,
            scope_acceptance=scope_acceptance,
            extra_instructions=extra_instructions,
            eval_cache=eval_cache,
            eval_conditions=eval_conditions,
            prompt_archive=prompt_archive,
            corpus_reuse=corpus_reuse,
        )
        if reuse_conditions is None or protected is not None:
            # Строку индекса не пишем, если механизм выключен **или** ячейку в карте этого
            # прогона представляет высокий источник (реестр/наследование): прецедент мог
            # сохранить его вместо вердикта Ш6, и строка «измерено вот так» описывала бы
            # ячейку, которой в карте нет. Высокие ячейки пере-порождаются своей фазой на
            # каждом прогоне — перенос им не нужен и запрещён.
            return outcome
        return replace(
            outcome,
            measurement=measurement_row(
                outcome.cell,
                param=param,
                conditions=reuse_conditions,
                coverage=coverage,
                retrieval=primary_retrieval,
                retrieved=chunks,
                # Evidence берётся то, что реально записано (``resolved_evidence``
                # эшелона §10), а не исходный ответ модели: отпечаток обязан описывать
                # строки ``cell_evidence``, которые переживут прогон.
                evidence_digest_value=evidence_digest(outcome.validation.resolved_evidence),
                measured_at=_measured_at(),
                prompt_hash=outcome.prompt_hash or None,
            ),
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
    eval_conditions: EvalConditions | None = None,
    exhaustion_facts: ExhaustionFacts | None = None,
    prompt_dir: str | Path | None = None,
    previous_run_id: str | None = None,
    state: ProductStatePort | None = None,
    previous_store: StorePort | None = None,
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
    :param eval_conditions: объявленные методологические условия вердикта
        (:class:`~core.pipeline.eval_cache.EvalConditions`); обязательны при включённом
        кэше. Composition root строит их из объявленных входов
        (:func:`~core.pipeline.eval_cache.eval_conditions_from_run`), фаза не догадывается
        о версиях: ``None`` при ``eval_cache is not None`` — отказ, а не тихий неполный ключ.
    :param exhaustion_facts: наблюдённые факты других фаз (какие контуры реально
        исполнились, раунды/бюджет SERP). ``None`` ⇒ протоколы не пишутся (прежнее
        поведение модульного вызова). Факты передаёт ``phases`` — Ш6 **не** догадывается,
        что происходило в других фазах (I3/§5). Наблюдённая терминация попадает в
        **вектор охвата** каждой ``❓``-ячейки (``EvidenceScope.termination``).
    :param prompt_dir: каталог аудита промптов ``runs/{run_id}/prompts`` (V7, §7.1).
        ``None`` ⇒ артефакт не пишется (модульный вызов: раскладки прогона нет, а
        выдумывать путь значило бы писать аудит мимо ``runs/{run_id}``). Каталог передаёт
        composition root через ``phases.build_phases``; фактическое число файлов и байт
        попадает в ``log_entries`` фазы, а сбои записи — в ``errors`` (видимо, но без
        отнятия уже полученных вердиктов: артефакт аудита — не условие измерения).
    :param previous_run_id: прогон, **против которого** идёт оценка (09 §12.3); резолвит
        вызывающий (``phases.build_phases`` → :func:`core.pipeline.run_identity.previous_run_of`),
        фаза лишь называет его в ``log_entries``. ``None`` — предшественника нет/resume
        текущего прогона: сравнение не с чем, и это видно строкой ``previous=—``, а не
        угадывается по ``state.db``. Он же — **источник переноса** ячеек (09 §10.2):
        ``previous_run_id`` без ``state``/``previous_store`` означает, что перенос
        выключен, и это видно строкой ``carry=—`` в логе фазы.
    :param state: durable per-product состояние (``state.db``) — строки ``cell_measurement``
        с ключами переноса. ``None`` ⇒ перенос не исполняется (модульный вызов без
        ``state.db``): ключ сравнивать не с чем, и выдумывать его из БД прогона нельзя —
        ``cells.cell_reuse_key`` текущего прогона пишется этой же фазой.
    :param previous_store: БД **прогона-предшественника** только на чтение. Открывает и
        закрывает её composition root (``phases.build_phases``): путь
        ``runs/{run_id}/cohound.db`` знает раскладка, а ``pipeline`` раскладку не читает.
        ``None`` ⇒ перенос выключен: без доказательств прошлого прогона вердикт не переезжает.

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
    log_entries: list[Any] = []
    # 09 §10.3/§11.4: счётчики происхождения ячеек. Считаются по **исходам** батча, а не
    # перечитыванием БД: ``carried`` — это свойство исхода, и второй источник правды о нём
    # разошёлся бы с первым при первой же правке записи.
    measured_total = 0
    carried_total = 0
    # 09 §12.3: против какого прогона оценивалось — в лог фазы, а не только в
    # ``state.db``: читатель отчёта прогона видит базу сравнения. ``—`` — предшественника
    # нет (первый прогон пакета или resume текущего: carry-forward не выполняется).
    log_entries.append(f"evaluate: previous={previous_run_id or '—'}")
    # 09 §10.2: перенос включается только при **всех** входах. Каждое отсутствие названо в
    # логе: «переноса не было» обязано быть отличимо от «ключи не совпали» — иначе читатель
    # лога решит, что механизм работает, а он выключен.
    carry_ready = (
        previous_run_id is not None
        and previous_run_id != run_id
        and state is not None
        and previous_store is not None
        and eval_conditions is not None
    )
    log_entries.append(f"evaluate: carry={'on' if carry_ready else '—'}")
    # V7: аудит исполненных промптов. Архив живёт в фазе, а не в ячейке: путь артефакта —
    # свойство прогона, и ячейка не должна знать раскладку ``runs/``.
    prompt_archive = PromptArchive(Path(prompt_dir)) if prompt_dir is not None else None

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
        # 09 §10.1: условия переноса/индекса — свойства прогона, но подпись аттестации
        # **per product** (``scope_acceptance`` подписывается по продукту). Собираются один
        # раз на продукт: половина слагаемых ключа не зависит от ячейки, и пересчитывать
        # политику расчёта на каждую пару «продукт × параметр» незачем.
        reuse_conditions: CellReuseConditions | None = None
        if eval_conditions is not None:
            reuse_conditions = cell_reuse_conditions(
                product_id=product_id,
                conditions=eval_conditions,
                scoring_policy_version=scoring_policy_version(),
                evaluator=evaluator,
                scope_acceptance=accepted,
            )
        # Перенос дополнительно требует предшественника и обе БД; индекс — только
        # ``state`` (он обязан писаться и на первом прогоне, иначе второму нечего
        # сравнивать).
        carry_ctx: CarryForwardContext | None = None
        if (
            carry_ready
            and reuse_conditions is not None
            and state is not None
            and previous_store is not None
            and previous_run_id is not None
        ):
            carry_ctx = CarryForwardContext(
                conditions=reuse_conditions,
                state=state,
                previous_store=previous_store,
                previous_run_id=previous_run_id,
            )
        chunks_by_param: dict[str, Sequence[ChunkLike]] = {}
        retrievals_by_param: dict[str, RetrievalResult] = {}
        #: Пары (параметр, готовая причина) для ячеек, чей ретривал упал: они
        #: становятся ``failed`` (I4) и **не** попадают в батч оценки. Хранится текст, а
        #: не объект исключения: держать десятки исключений живыми ради строки не нужно,
        #: а имя ``exc`` вне ``except``-блока Python удаляет — и читать его оттуда
        #: небезопасно.
        retrieval_failures: list[tuple[CatalogRow, str]] = []
        # Ф0: покрытие корпуса продукта читается один раз (на главном потоке) и
        # прокидывается в ячейки. Продукт плана без строки покрытия — аномалия (O3).
        coverage = _coverage_for(None, store, run_id, product_id, planned=True)
        # 16 §8 п.4: происхождение наблюдения корпуса — **один** проход на продукт, не на
        # ячейку (181 параметр × продукты читали бы ту же таблицу сотни раз). Сбой чтения
        # виден в ``errors``, а не выдан за «наблюдений нет»: иначе правило снималось бы
        # молча и ``❌`` публиковался бы на неподтверждённом наблюдении.
        try:
            corpus_reuse: CorpusReuseObservation | None = observe_corpus_reuse(
                store, run_id, product_id
            )
        except Exception as exc:  # noqa: BLE001 — провенанс не роняет фазу, но видим
            errors.append(
                f"{product_id}: corpus_docs.reuse_kind: {type(exc).__name__}: {exc} — "
                "правило «❌ требует live|revalidated» не применено"
            )
            corpus_reuse = None
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
                # I4: сбой прибора — ``failed``, а не ``❓``. Отсутствие ключа в
                # ``chunks_by_param`` означает «ретривал не состоялся», и это **не**
                # пустой срез: ``evaluate_batch`` не имеет права подставить сюда ``()``.
                # До этой правки ячейка с упавшим ретривалом доходила до оценщика с
                # пустым срезом; после того как пустой срез перестал тратить вызов
                # модели, она стала публиковаться как измеренное ``❓`` с заметкой
                # ``no_fragments`` — то есть отказ инструмента выдавался за наблюдение
                # «вендор не заявил». Это ровно тот класс, который запрещает I4.
                retrieval_failures.append((param, f"{type(exc).__name__}: {exc}"))
                continue
            chunks_by_param[param.param_id] = result.chunks
            retrievals_by_param[param.param_id] = result

        failed_ids = {param.param_id for param, _ in retrieval_failures}
        if retrieval_failures:
            # Ячейка высокого источника (реестр/наследование) не затирается сбоем
            # ретривала — так же, как в ветке ``expected_sources=0``.
            failure_provenance = (
                None,
                resolved_profile.value
                if isinstance(resolved_profile, RetrievalProfile)
                else resolved_profile,
                None,
            )
            for param, detail in retrieval_failures:
                protected = protected_by_param.get(param.param_id)
                if protected is not None:
                    cells.append(protected)
                    continue
                failed = _unresolved_cell(
                    state=CellState.FAILED,
                    param=param,
                    product_id=product_id,
                    run_id=run_id,
                    provenance=failure_provenance,
                    # Класс сбоя виден в карте: это не «вендор не заявил».
                    evidence_scope=EvidenceScope.for_instrument_failure(
                        FailureKind.SEARCH_FAILED.value, detail=detail
                    ),
                    # Причина обязана пережить прогон: ``failed`` без объяснения
                    # заставляет оператора разбирать консольный лог.
                    validation_notes=_compact_notes(
                        [f"{FailureKind.SEARCH_FAILED.value}: {detail}"]
                    ),
                )
                _persist(store, failed, None)
                _record_cell_failure(store, failed, FailureKind.SEARCH_FAILED.value)
                cells.append(failed)

        outcomes, batch_errors = evaluate_batch(
            params=[p for p in plan.params if p.param_id not in failed_ids],
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
            eval_conditions=eval_conditions,
            prompt_archive=prompt_archive,
            corpus_reuse=corpus_reuse,
            carry=carry_ctx,
            reuse_conditions=reuse_conditions,
        )
        cells.extend(outcome.cell for outcome in outcomes)
        errors.extend(batch_errors)
        # 09 §10.1: индекс переноса пишется одной пачкой на продукт, здесь, а не в батче:
        # ``state.db`` — одно соединение и один владелец записи. Пишутся и ``observed``,
        # и ``carried``: иначе цепочка рвётся на втором цикле.
        if state is not None:
            rows = [
                outcome.measurement
                for outcome in outcomes
                if outcome.measurement is not None
            ]
            if rows:
                state.record_cell_measurements(rows)
        carried_here = sum(
            1
            for outcome in outcomes
            if outcome.cell.measurement_origin is MeasurementOrigin.CARRIED
        )
        measured_total += len(outcomes) - carried_here
        carried_total += carried_here

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

    if prompt_archive is not None:
        # Наблюдение аудита промптов: число/объём — измеренный факт, аномалии записи —
        # ошибки фазы (прогон продолжается: вердикты уже получены и не зависят от
        # артефакта аудита).
        log_entries.append(f"evaluate: {prompt_archive.summary()}")
        errors.extend(prompt_archive.errors)

    # 09 §10.3/§11.4: происхождение ячеек — измеренный факт фазы. ``measured`` — вызов
    # оценщика состоялся (в том числе ``failed``/``preliminary``: это исходы измерения),
    # ``carried`` — вердикт взят из ``previous_run_id`` без вызова. Ячейки, отсечённые до
    # батча (нулевой корпус, упавший ретривал), в счётчики не входят: они ``failed`` по
    # причине прибора и переносу не подлежат, поэтому строку ``cell_measurement`` не
    # получают — индекс описывает измерения, а не отказы.
    log_entries.append(
        f"evaluate: cells_measured={measured_total} cells_carried={carried_total}"
    )

    return PhaseResult(
        status="ok",
        phase="evaluate",
        cells=cells,
        log_entries=log_entries,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# Пере-оценка устаревших ячеек (§13/K2): селектор + исполнение
# ---------------------------------------------------------------------------

#: Префикс машинной заметки о **неудавшейся** пере-оценке (``cells.validation_notes``).
REVALIDATE_FAILED_PREFIX = "revalidate_failed"

#: Причина, по которой stale-ячейка вне пере-оценки: её вердикт пришёл не из корпуса
#: (реестр/наследование/baselines/ручное) — у неё нет оценки, которую можно пересчитать.
STALE_SKIP_SOURCE_KIND = "source_kind_not_documentation"
#: Причина: состояние ячейки не несёт корпусного вердикта (``pending``/``failed``/
#: ``reviewed``): пересчитывать нечего, а ре-оценка подменила бы чужое решение (I6/I7).
STALE_SKIP_STATE = "state_not_reevaluable"
#: Причина: ``param_id`` ячейки отсутствует в восстановленном каталоге прогона — запросы и
#: промпт по нему не построить (значения не выдумываем).
STALE_SKIP_PARAM = "param_not_in_catalog"
#: Причина: у stale-ячейки нет ``index_build_id`` — снимать пометку «описывает прошлую
#: сборку» не на что (обновлять её на текущую сборку нечего).
STALE_SKIP_NO_BUILD = "no_index_build_id"
#: Причина: текущая сборка прогона не объявлена (``runs.index_build_id`` пуст) — свежесть
#: ячейки после пере-оценки была бы утверждением без предмета.
STALE_SKIP_UNKNOWN_BUILD = "current_build_unknown"
#: Причина: оценщик не сконфигурирован — пере-оценка оценщиком невозможна.
REVALIDATE_SKIP_NO_EVALUATOR = "no_evaluator"
#: Причина остатка: пере-оценка состоялась, но вердикта не получено (I4 ``failed``/I13
#: ``preliminary``) — пометка сохранена, ячейка по-прежнему вне расчёта.
REVALIDATE_SKIP_NO_VERDICT = "no_verdict"
#: Причина остатка: срез ретривала пришёл из **другой** сборки, не из текущей: вердикт
#: описывал бы не тот корпус, свежесть объявлять нельзя.
REVALIDATE_SKIP_BUILD_MISMATCH = "build_mismatch"

#: Статус отчёта: пере-оценка что-то сняла.
REVALIDATE_STATUS_REEVALUATED = "reevaluated"
#: Статус отчёта: работы не было (``stale`` пуст либо ничего не адресуется оценщику).
REVALIDATE_STATUS_NOOP = "noop"
#: Статус отчёта: пере-оценка невозможна по объявленной причине (не «пусто»).
REVALIDATE_STATUS_REFUSED = "refused"

#: Явное сообщение идемпотентного no-op (повторный вызов без stale-ячеек).
REVALIDATE_NOTHING_TO_DO = "пере-оценивать нечего: stale-ячеек нет"

#: Состояния, из которых пере-оценка оценщиком по корпусу вообще возможна: ``evaluated``
#: (вердикт получен) и ``preliminary`` (кандидат того же контура, I13). Прочие состояния —
#: ``pending`` (вердикта ещё нет), ``failed`` (сбой прибора, I4), ``reviewed`` (решение
#: человека, I6/I7) — пере-оценкой не переписываются.
_REEVALUABLE_STATES: frozenset[CellState] = frozenset(
    {CellState.EVALUATED, CellState.PRELIMINARY}
)


def _cell_key(cell: Cell) -> dict[str, str]:
    """Ключ ячейки для payload'ов (``product_id``/``param_id``)."""
    return {"product_id": str(cell.product_id), "param_id": str(cell.param_id)}


def _enum_text(value: Any) -> str:
    """Строковое значение перечисления/``None`` для записей отчёта."""
    return str(getattr(value, "value", value))


@dataclass(frozen=True)
class StaleSelection:
    """Результат селектора: **все** stale-ячейки прогона, разделённые по достижимости.

    :param run_id: прогон, по которому читались ячейки.
    :param reevaluable_cells: ячейки, которые оценщик вправе пере-оценить
        (``source_kind=documentation``, состояние ``evaluated``/``preliminary``), порядок
        ``(product_id, param_id)``.
    :param skipped: остальные stale-ячейки с причиной недостижимости — «сколько осталось и
        почему» видно, а не выпадает молча.

    Инвариант: ``len(reevaluable_cells) + len(skipped)`` = число ячеек прогона со
    ``stale_reason`` (после фильтров ``product_ids``/``param_ids``). Селектор не теряет
    помеченные ячейки: часть, которую нельзя пере-оценить, возвращается с причиной, а не
    отбрасывается.
    """

    run_id: str
    reevaluable_cells: tuple[Cell, ...] = ()
    skipped: tuple[dict[str, Any], ...] = ()

    @property
    def total(self) -> int:
        """Сколько ячеек прогона помечено stale (пере-оцениваемые + недостижимые)."""
        return len(self.reevaluable_cells) + len(self.skipped)

    def to_dict(self) -> dict[str, Any]:
        """JSON-совместимый вид селекции (счётчики + ключи + причины)."""
        return {
            "run_id": self.run_id,
            "stale_total": self.total,
            "reevaluable": len(self.reevaluable_cells),
            "reevaluable_cells": [
                _cell_key(cell) for cell in self.reevaluable_cells
            ],
            "skipped": len(self.skipped),
            "skipped_cells": list(self.skipped),
        }


def _stale_skip_reason(cell: Cell) -> str | None:
    """Почему stale-ячейку **нельзя** пере-оценить оценщиком (``None`` — можно).

    Проверка «нет корпуса» — первая и главная: вердикт реестра/наследования/baselines
    пришёл из другого контура, и пере-оценка «по документации» подменила бы механизм
    (I2b/§2.2 п.7-8), а не обновила измерение. Состояние проверяется вторым: ``reviewed``
    несёт человеческое решение (I6/I7), ``failed``/``pending`` — вердикта нет вовсе.
    """
    if cell.source_kind is not _CELL_SOURCE_KIND:
        return f"{STALE_SKIP_SOURCE_KIND}:{_enum_text(cell.source_kind)}"
    if cell.state not in _REEVALUABLE_STATES:
        return f"{STALE_SKIP_STATE}:{_enum_text(cell.state)}"
    if not cell.index_build_id:
        return STALE_SKIP_NO_BUILD
    return None


def select_stale_cells(
    store: StorePort,
    run_id: str,
    *,
    product_ids: Sequence[str] | None = None,
    param_ids: Sequence[str] | None = None,
) -> StaleSelection:
    """Селектор: stale-ячейки прогона **из хранилища** (не из ``stale_cells.csv``).

    Источник истины — ``iter_cells``: CSV/отчётные артефакты производны и могут быть
    устаревшими или вовсе не сгенерированными (отчёт идёт после ``compute``), поэтому
    пере-оценка читает то же, что читает расчёт.

    Возвращаются **ровно** помеченные ячейки (``stale_reason is not None``), разделённые
    на пере-оцениваемые и недостижимые: см. :func:`_stale_skip_reason`. Ячейки без пометки
    не возвращаются ни в одной части — пере-оценка их не касается (она не «пересчитывает
    фазу на всякий случай»).

    :param product_ids: фильтр продуктов (``None`` — все).
    :param param_ids: фильтр параметров (``None`` — все).
    """
    products = set(product_ids) if product_ids is not None else None
    params = set(param_ids) if param_ids is not None else None
    reevaluable: list[Cell] = []
    skipped: list[dict[str, Any]] = []
    for cell in store.iter_cells(str(run_id)):
        if cell.stale_reason is None:
            continue
        if products is not None and str(cell.product_id) not in products:
            continue
        if params is not None and str(cell.param_id) not in params:
            continue
        reason = _stale_skip_reason(cell)
        if reason is None:
            reevaluable.append(cell)
            continue
        skipped.append(
            {
                **_cell_key(cell),
                "reason": reason,
                "stale_reason": _enum_text(cell.stale_reason),
                "source_kind": _enum_text(cell.source_kind),
                "state": _enum_text(cell.state),
                "index_build_id": cell.index_build_id,
            }
        )
    reevaluable.sort(key=lambda c: (str(c.product_id), str(c.param_id)))
    skipped.sort(key=lambda row: (row["product_id"], row["param_id"]))
    return StaleSelection(
        run_id=str(run_id),
        reevaluable_cells=tuple(reevaluable),
        skipped=tuple(skipped),
    )


@dataclass
class RevalidationReport:
    """Отчёт пере-оценки: что было stale, что пере-оценено, что осталось и почему.

    :param status: :data:`REVALIDATE_STATUS_REEVALUATED` | ``..._NOOP`` | ``..._REFUSED``.
    :param message: человекочитаемая сводка **тех же** чисел, что в полях (оператор видит
        «сколько было / пере-оценено / осталось», не разбирая ``--json``).
    :param stale_total: сколько ячеек было помечено stale на входе.
    :param reevaluated: снятые пометки (успех) — ``product_id``/``param_id``/``status``/
        ``index_build_id``/``state``.
    :param remaining: пометки, которые **сохранились**, с причиной
        (:data:`REVALIDATE_SKIP_NO_VERDICT`/``..._BUILD_MISMATCH``/``retrieval_failed``/
        ``evaluation_error``).
    :param skipped: stale-ячейки, к которым оценщик не имеет права прикасаться
        (реестр/наследование/состояние без вердикта/нет параметра в каталоге).
    :param errors: изолированные сбои ретривала/оценки (видны, а не проглочены).
    """

    run_id: str
    index_build_id: str | None = None
    status: str = REVALIDATE_STATUS_NOOP
    message: str = ""
    stale_total: int = 0
    reevaluated: list[dict[str, Any]] = field(default_factory=list)
    remaining: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Состоялась ли пере-оценка хотя бы одной ячейки."""
        return bool(self.reevaluated)

    def to_dict(self) -> dict[str, Any]:
        """JSON-совместимый вид отчёта (``--json`` payload команды ``revalidate``)."""
        return {
            "run_id": self.run_id,
            "index_build_id": self.index_build_id,
            "status": self.status,
            "message": self.message,
            "stale_total": self.stale_total,
            "reevaluated": len(self.reevaluated),
            "remaining": len(self.remaining),
            "skipped": len(self.skipped),
            "reevaluated_cells": list(self.reevaluated),
            "remaining_cells": list(self.remaining),
            "skipped_cells": list(self.skipped),
            "errors": list(self.errors),
        }


def _reasons_breakdown(rows: Sequence[Mapping[str, Any]]) -> str:
    """``reason=count, ...`` в детерминированном порядке (для сводки)."""
    counts: dict[str, int] = {}
    for row in rows:
        reason = str(row.get("reason", "?"))
        counts[reason] = counts.get(reason, 0) + 1
    return ", ".join(f"{reason}={count}" for reason, count in sorted(counts.items()))


def _revalidation_summary(report: RevalidationReport) -> str:
    """Сводка отчёта: было stale / пере-оценено / осталось (+причины) / вне пере-оценки."""
    parts = [
        f"stale было {report.stale_total}",
        f"пере-оценено {len(report.reevaluated)}",
        f"осталось {len(report.remaining)}",
    ]
    if report.remaining:
        parts.append(f"осталось почему: {_reasons_breakdown(report.remaining)}")
    if report.skipped:
        parts.append(
            f"вне пере-оценки {len(report.skipped)} "
            f"({_reasons_breakdown(report.skipped)})"
        )
    return "; ".join(parts)


def _cell_condition(cell: Cell) -> tuple[str, str, int]:
    """Условия измерения **самой ячейки**: ``(product_id, профиль, cutoff_K)``.

    Пере-оценка не вводит новых условий: она повторяет условия, под которыми ячейка была
    получена (``retrieval_profile``/``retrieval_params_json``, R7/I16/I20). Профиль из
    манифеста — лишь fallback для исторических строк без полей.
    """
    profile = str(cell.retrieval_profile or DEFAULT_RETRIEVAL_PROFILE.value)
    cutoff = DEFAULT_RETRIEVAL_CUTOFF_K
    raw = cell.retrieval_params_json
    if raw:
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            parsed = None
        if isinstance(parsed, Mapping):
            value = parsed.get("cutoff_K")
            if isinstance(value, int) and value > 0:
                cutoff = int(value)
    return str(cell.product_id), profile, cutoff


def _accepted_risk(cell: Cell) -> str | None:
    """Принятие риска, уже данное человеком ранее для этой ячейки (``scope_acceptance``).

    Пере-оценка **не переспрашивает** и не выдумывает подпись: она повторяет ту, что
    лежит в ``cell.evidence_scope``. Иначе ячейка, получившая ``❌`` класса ``bounded``,
    после пере-оценки молча понизилась бы до ``❓`` — то есть пере-оценка корпуса
    отменила бы человеческое решение о принятии риска.
    """
    scope = cell.evidence_scope
    return scope.scope_acceptance if scope is not None else None


def _current_build_id(store: StorePort, run_id: str) -> str | None:
    """Текущая сборка прогона (``runs.index_build_id``), если store это умеет читать."""
    reader = getattr(store, "read_run_index_ref", None)
    if not callable(reader):
        return None
    try:
        ref = reader(str(run_id))
    except Exception:  # noqa: BLE001 — чтение провенанса не должно ронять команду
        return None
    return getattr(ref, "index_build_id", None) or None


def _kept_stale_cell(cell: Cell, *, reason: str, detail: str | None) -> Cell:
    """Возвращает stale-ячейку **с сохранённой пометкой** плюс запись о неудаче.

    Неуспешная пере-оценка не имеет права объявить ячейку свежей (§13/K2): вердикта по
    текущей сборке нет. При этом уже полученный вердикт старой сборки **не стирается** —
    он остаётся привязан к своей сборке и с явной пометкой устаревания, а причина отказа
    пере-оценки ложится в ``cells.validation_notes`` (переживает прогон, читается ревью).
    Терять статус старой сборки ради записи ``failed`` было бы хуже: ``failed`` (I4) —
    факт о текущем приборе, а старая ячейка — факт о прошлом корпусе.
    """
    note = f"{REVALIDATE_FAILED_PREFIX}: {reason}"
    if detail:
        note = f"{note} — {detail}"
    notes = _compact_notes([*cell.validation_notes, note])
    return cell.model_copy(update={"validation_notes": notes})


def reevaluate_stale(
    *,
    store: StorePort,
    run_id: str,
    params: Mapping[str, CatalogRow],
    ports: Ports,
    satellites: Sequence[Satellite] = (),
    resolver: SourceResolver | None = None,
    strict: bool = True,
    eval_cache: EvalVerdictCache | None = None,
    eval_conditions: EvalConditions | None = None,
    max_retries: int = 2,
    product_ids: Sequence[str] | None = None,
    param_ids: Sequence[str] | None = None,
    current_build_id: str | None = None,
    prompt_dir: str | Path | None = None,
) -> RevalidationReport:
    """Пере-оценивает **только** устаревшие ячейки и снимает пометку при вердикте.

    Это исполнение селектора :func:`select_stale_cells`: набор ячеек сужается до
    ``stale_reason is not None`` **и** ``source_kind=documentation`` (см. там же), поэтому
    незатронутое не оплачивается повторно, а вердикты чужих контуров не переписываются.

    Порядок для каждой ячейки — тот же, что в фазе Ш6 (:func:`evaluate_cell`): ретривал
    под условиями ячейки (:func:`retrieve_for_cell`, I16/I17/I20) → промпт → оценщик
    (I18) → эшелон валидации §10. Разница одна: персистенция выполняется **здесь**, после
    классификации исхода, а не внутри оценки.

    **Снятие пометки (§13/K2).** Ячейка объявляется свежей ровно тогда, когда получен
    вердикт (``state=evaluated``) **и** срез пришёл из текущей сборки прогона
    (``index_build_id == current_build_id``). Тогда сохраняется новая ячейка с
    ``stale_reason=None`` и ``index_build_id`` текущей сборки. Иначе пометка
    **сохраняется**: ``no_verdict`` (I4 ``failed``/I13 ``preliminary`` — вердикта нет) или
    ``build_mismatch`` (вердикт описывал бы другую сборку). «Не получилось» не выдаётся за
    «свежо», и старый вердикт при этом не стирается (:func:`_kept_stale_cell`).

    **Идемпотентность.** Повторный вызов без изменений ничего не оценивает: селектор даёт
    пусто, отчёт возвращается со ``status=noop`` и :data:`REVALIDATE_NOTHING_TO_DO`.

    :param params: каталог прогона ``param_id → CatalogRow`` (снимок ``run_inputs`` или
        пакет): из него строятся запросы и промпт. Ячейка с ``param_id`` вне каталога
        остаётся stale с причиной :data:`STALE_SKIP_PARAM` — параметры не выдумываются.
    :param ports: оценщик и (опционально) индексы/эмбеддер ретривала.
    :param current_build_id: текущая сборка прогона; ``None`` — читается из
        ``runs.index_build_id``. Не объявлена — пере-оценка **отказывается** работать:
        снимать пометку «описывает прошлую сборку» не на что.
    :param max_retries: повторы вызова оценщика (как в фазе, §7.1).
    :param eval_cache: межпрогонный кэш вердиктов; ``None`` ⇒ выключен.
    :param eval_conditions: объявленные методологические условия вердикта; обязательны
        при включённом кэше (09 §10.5), как и в фазе Ш6.
    """
    selection = select_stale_cells(
        store, run_id, product_ids=product_ids, param_ids=param_ids
    )
    resolved_build = current_build_id or _current_build_id(store, run_id)
    report = RevalidationReport(
        run_id=str(run_id),
        index_build_id=resolved_build,
        status=REVALIDATE_STATUS_NOOP,
        stale_total=selection.total,
        skipped=[dict(row) for row in selection.skipped],
    )
    if selection.total == 0:
        report.message = REVALIDATE_NOTHING_TO_DO
        return report

    catalog = {str(key): row for key, row in params.items()}
    targets: list[Cell] = []
    for cell in selection.reevaluable_cells:
        if str(cell.param_id) not in catalog:
            report.skipped.append({**_cell_key(cell), "reason": STALE_SKIP_PARAM})
            continue
        targets.append(cell)
    if not targets:
        report.message = (
            f"пере-оценивать нечего: stale-ячеек {selection.total}, но ни одна не "
            f"адресуется оценщику по корпусу ({_reasons_breakdown(report.skipped)})"
        )
        return report

    evaluator = getattr(ports, "evaluator", None)
    if evaluator is None:
        report.status = REVALIDATE_STATUS_REFUSED
        report.remaining = [
            {**_cell_key(cell), "reason": REVALIDATE_SKIP_NO_EVALUATOR} for cell in targets
        ]
        report.message = (
            "пере-оценка невозможна: оценщик не сконфигурирован — stale-ячейки остаются "
            "stale, снимать пометку без вердикта нельзя"
        )
        return report
    if not resolved_build:
        report.status = REVALIDATE_STATUS_REFUSED
        report.remaining = [
            {**_cell_key(cell), "reason": STALE_SKIP_UNKNOWN_BUILD} for cell in targets
        ]
        report.message = (
            "пере-оценка невозможна: текущая сборка прогона не объявлена "
            "(runs.index_build_id пуст) — свежесть ячейки не к чему привязать"
        )
        return report

    model_id = getattr(evaluator, "model_id", "") or _UNSPECIFIED_MODEL_ID

    # V7: пере-оценка исполняет промпты тем же путём, что Ш6, поэтому её артефакт аудита
    # лежит в том же каталоге прогона (``prompt_dir``); ``None`` — пере-оценка без аудита
    # (модульный вызов).
    prompt_archive = PromptArchive(Path(prompt_dir)) if prompt_dir is not None else None

    # Группы = условия измерения ячейки (продукт/профиль/K): векторы запросов и dense-плечо
    # считаются **на группу**, а не на всю фазу — цена пропорциональна затронутым ячейкам.
    groups: dict[tuple[str, str, int], list[Cell]] = {}
    for cell in targets:
        groups.setdefault(_cell_condition(cell), []).append(cell)

    # 16 §8 п.4: происхождение наблюдения корпуса — один снимок на продукт и на вызов
    # (группы одного продукта различаются профилем/K, корпус у них общий).
    reuse_by_product: dict[str, CorpusReuseObservation | None] = {}

    for product_id, profile, cutoff in sorted(groups):
        group = sorted(groups[(product_id, profile, cutoff)], key=lambda c: str(c.param_id))
        group_params = [catalog[str(cell.param_id)] for cell in group]
        try:
            query_vectors = precompute_query_vectors(
                group_params,
                profile=profile,
                embedder=getattr(ports, "embedder", None),
                memo={},
            )
        except Exception as exc:  # noqa: BLE001 — сбой виден в errors, а не притворяется успехом
            report.errors.append(
                f"{product_id}: query_vectors: {type(exc).__name__}: {exc}"
            )
            query_vectors = None
        coverage = _coverage_for(None, store, str(run_id), product_id)
        if product_id not in reuse_by_product:
            try:
                reuse_by_product[product_id] = observe_corpus_reuse(
                    store, str(run_id), product_id
                )
            except Exception as exc:  # noqa: BLE001 — провенанс виден, фазу не роняет
                report.errors.append(
                    f"{product_id}: corpus_docs.reuse_kind: {type(exc).__name__}: {exc} — "
                    "правило «❌ требует live|revalidated» не применено"
                )
                reuse_by_product[product_id] = None
        try:
            dense_by_query = precompute_dense_hits(
                group_params,
                product_ids=[product_id],
                profile=profile,
                cutoff_k=cutoff,
                vector_index=getattr(ports, "vector_index", None),
                query_vectors=query_vectors,
            )
        except Exception as exc:  # noqa: BLE001 — батч не удался: идём поштучно, но видимо
            report.errors.append(
                f"{product_id}: dense batch: {type(exc).__name__}: {exc} — "
                "поиск пошёл поштучно"
            )
            dense_by_query = None

        for cell in group:
            param = catalog[str(cell.param_id)]
            try:
                retrieval = retrieve_for_cell(
                    param,
                    product_id=product_id,
                    profile=profile,
                    cutoff_k=cutoff,
                    lexical_index=getattr(ports, "lexical_index", None),
                    vector_index=getattr(ports, "vector_index", None),
                    embedder=getattr(ports, "embedder", None),
                    query_vectors=query_vectors,
                    dense_by_query=dense_by_query,
                )
            except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                report.errors.append(
                    f"{product_id}/{cell.param_id}: retrieval: {type(exc).__name__}: {exc}"
                )
                report.remaining.append(
                    {
                        **_cell_key(cell),
                        "reason": "retrieval_failed",
                        "stale_reason": _enum_text(cell.stale_reason),
                        "index_build_id": cell.index_build_id,
                    }
                )
                continue
            try:
                # ``store=None``: персистенция — только после классификации исхода.
                # Иначе неуспешная пере-оценка успела бы снять пометку upsert'ом.
                outcome = evaluate_cell(
                    param=param,
                    product_id=product_id,
                    run_id=str(run_id),
                    evaluator=evaluator,
                    chunks=retrieval.chunks,
                    satellites=satellites,
                    resolver=resolver,
                    strict=strict,
                    retrievals=retrieval,
                    max_retries=max_retries,
                    store=None,
                    coverage=coverage,
                    scope_acceptance=_accepted_risk(cell),
                    eval_cache=eval_cache,
                    eval_conditions=eval_conditions,
                    prompt_archive=prompt_archive,
                    corpus_reuse=reuse_by_product.get(product_id),
                )
            except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                report.errors.append(
                    f"{product_id}/{cell.param_id}: evaluate: {type(exc).__name__}: {exc}"
                )
                report.remaining.append(
                    {
                        **_cell_key(cell),
                        "reason": "evaluation_error",
                        "stale_reason": _enum_text(cell.stale_reason),
                        "index_build_id": cell.index_build_id,
                    }
                )
                continue

            keep_reason = _keep_stale_reason(outcome, resolved_build)
            if keep_reason is None:
                fresh = outcome.cell.model_copy(update={"stale_reason": None})
                _persist(
                    store,
                    fresh,
                    outcome.validation,
                    prompt_hash=outcome.prompt_hash,
                    model_id=model_id,
                )
                report.reevaluated.append(
                    {
                        **_cell_key(cell),
                        "state": _enum_text(fresh.state),
                        "status": _enum_text(fresh.status),
                        "index_build_id": fresh.index_build_id,
                        "previous_index_build_id": cell.index_build_id,
                    }
                )
                continue

            detail = outcome.failure_kind or next(
                (note for note in reversed(outcome.validation.notes) if note), None
            )
            kept = _kept_stale_cell(cell, reason=keep_reason, detail=detail)
            store.save_cell(kept)
            report.remaining.append(
                {
                    **_cell_key(cell),
                    "reason": keep_reason,
                    "stale_reason": _enum_text(kept.stale_reason),
                    "index_build_id": kept.index_build_id,
                    "state": _enum_text(outcome.cell.state),
                    "failure_kind": outcome.failure_kind,
                    "note": f"{REVALIDATE_FAILED_PREFIX}: {keep_reason}",
                }
            )

    report.status = (
        REVALIDATE_STATUS_REEVALUATED if report.reevaluated else REVALIDATE_STATUS_NOOP
    )
    report.message = _revalidation_summary(report)
    if prompt_archive is not None:
        # Объём аудита — наблюдённый факт в сообщении, аномалии записи — в ``errors``.
        report.message = f"{report.message}; {prompt_archive.summary()}"
        report.errors.extend(prompt_archive.errors)
    return report


def _keep_stale_reason(outcome: EvaluationOutcome, current_build_id: str) -> str | None:
    """Оставить ли пометку после попытки: причина (``None`` — снимать).

    Снятие пометки — утверждение «вердикт получен по **текущей** сборке». Оба условия
    проверяются явно: ``state=evaluated`` (иначе вердикта нет: I4/I13) и
    ``index_build_id == current_build_id`` (иначе вердикт описывает другую сборку, и
    «свежая» ячейка снова попала бы в расчёт с чужим корпусом).
    """
    cell = outcome.cell
    if cell.state is not CellState.EVALUATED:
        return REVALIDATE_SKIP_NO_VERDICT
    if (cell.index_build_id or "") != current_build_id:
        return REVALIDATE_SKIP_BUILD_MISMATCH
    return None
