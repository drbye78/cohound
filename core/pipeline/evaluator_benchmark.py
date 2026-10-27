"""coHound — benchmark моделей оценщика (Ш6) при **всём прочем фиксированном** (A3, §7.1).

Задача модуля — не «выбрать лучшую модель по вкусу», а **измерить** кандидатов на сценарии
``evaluator`` так, чтобы замена дефолта происходила по измерению (A3), а не по предвидению.
Модуль чистый: без сети, без записи в store, без адаптеров. Модель приходит портом
:class:`~core.ports.llm.EvaluatorPort`; вызов реального endpoint — забота composition root.

**Что фиксировано (и проверяется, а не предполагается).**

* индексная сборка и композиция прогона (I17: ретривал продукт-скоупед, evidence — только
  чанки продукта);
* ``retrieval_profile``/``retrieval_cutoff_K``/``rrf_k`` — читаются из строки прогона
  (``runs``/манифест), не выбираются экспериментом заново (I16/I20);
* набор чанков ячейки: ретривал выполняется **один раз** на ячейку
  (:func:`core.pipeline.evaluate.retrieve_for_cell`), все модели видят один и тот же
  :class:`CellCase`;
* промпт: шаблон :data:`~core.pipeline.prompt.TEMPLATE_VERSION`; одинаковость входа
  доказывается совпадением ``prompt_hash`` по всем моделям (иначе — отказ, а не «ну почти»);
* ``seed=0``, ``temperature=0.0``, ``max_retries=2``, ``strict=True``;
* golden-набор ожиданий (``golden.csv``) — один и тот же для всех моделей.

**Что измеряется.** Валидность выхода (эшелон §10, переиспользуется как есть), согласие с
golden (Cohen's κ через :mod:`core.util.agreement` — вырожденный расчёт честно отдаётся как
``kappa=None`` с причиной), «сдержанность» на ожидаемых ``❌``, **fabricated support**
(модель заявила ✅ с цитатами, ни одна из которых не подтвердилась — худший режим отказа),
дисциплина схемы (прошло с первой попытки), токены/вызовы, латентность и **детерминизм**
(доля ячеек, чей статус изменился между повторами: недетерминированный оценщик ломает
resume и воспроизводимость).

**Честная неопределённость.** Для ``validity_rate`` и κ считается bootstrap-перцентильный
95 % CI (seed фиксирован). Отчёт несёт ``n_cells``/``n_products`` и ``limits`` — включая
прямое предупреждение, что golden-набор мал и вывод не переносится на другие домены.

**Модель — объявленный вход, не env.** ``model_id`` приходит из флага/аргумента
(:class:`ModelUnderTest`), а не из ``COHOUND_LLM_MODEL``: run identity
(:mod:`core.cli.execution_axes`) намеренно игнорирует env, и benchmark, меняющий модель
через среду, воспроизвёл бы ровно тот баг, от которого эта граница защищает.

Модуль категорийно нейтрален (§1.3): идентификаторы моделей — данные.
"""

from __future__ import annotations

import datetime as _dt
import json
import math
import random
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from core.domain import CatalogRow, GoldenRow, LLMCellOutput, Satellite, Status
from core.domain.enums import CellState
from core.pipeline.eval_cache import EvalVerdictCache
from core.pipeline.evaluate import (
    DEFAULT_RETRIEVAL_CUTOFF_K,
    DEFAULT_RETRIEVAL_PROFILE,
    MAX_QUERIES,
    EvaluationOutcome,
    evaluate_cell,
    retrieve_for_cell,
)
from core.pipeline.prompt import TEMPLATE_VERSION
from core.pipeline.retrieve import DEFAULT_RRF_K, RetrievalProfile, RetrievalResult
from core.pipeline.validate import ChunkLike
from core.ports.embedder import EmbedderPort
from core.ports.lexical_index import LexicalIndexPort
from core.ports.llm import EvaluatorPort, LLMRequest, LLMResponse
from core.ports.store import StorePort
from core.ports.vector_index import VectorIndexPort
from core.util.agreement import AnnotationRow, compute_agreement
from core.util.hashing import content_hash

__all__ = [
    "BENCHMARK_CONTRACT",
    "BenchmarkContractError",
    "BenchmarkPreconditionError",
    "BenchmarkReport",
    "CellCase",
    "CellMeasurement",
    "Interval",
    "ModelMetrics",
    "ModelUnderTest",
    "PreparedCases",
    "prepare_cases",
    "run_benchmark",
]

#: Зафиксированные условия измерения. Любое отклонение — отказ, а не тихая подмена.
BENCHMARK_CONTRACT: dict[str, Any] = {
    "seed": 0,
    "temperature": 0.0,
    "max_retries": 2,
    "strict": True,
    "prompt_template": TEMPLATE_VERSION,
    # I18: у порта оценщика структурно нет инструментов/веб-поиска.
    "web_tool": False,
}

#: Причина «нечего измерять» (нет сборки/продуктов) — предпосылка, а не результат.
PRECONDITION_NO_INDEX_BUILD = "no_index_build"
PRECONDITION_NO_PRODUCTS = "no_products"
PRECONDITION_NO_CASES = "no_cases"

class BenchmarkContractError(RuntimeError):
    """Условия измерения нарушены (не тот seed/temperature, включён веб-тул).

    Это ошибка вызывающего/сборки, а не свойство модели: измерение в таких условиях
    сравнивало бы разное, поэтому продолжать нельзя.
    """

class BenchmarkPreconditionError(RuntimeError):
    """Предпосылка измерения отсутствует (нет сборки/продуктов/чанков).

    Несёт машиночитаемую ``reason`` — вызывающий печатает явный ``skipped`` с причиной,
    а не выдумывает результат.
    """

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason

# ---------------------------------------------------------------------------
# Входы измерения
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ModelUnderTest:
    """Кандидат на сценарий ``evaluator``: порт + объявленные оси исполнения.

    :param model_id: идентификатор модели (данные; приходит из флага, не из env).
    :param evaluator: порт оценщика (без веб-тула — I18 структурно).
    :param label: человекочитаемая метка для отчёта (по умолчанию ``model_id``).
    :param model_version: версия весов (манифест §13); пусто — не объявлена.
    :param decoder: механизм constrained decoding (``xgrammar``/``json_schema``/``none``).
    :param endpoint_kind: профиль зоны (``api``/``self_host``).
    :param web_tool: всегда ``False``; ``True`` отвергается (I18).
    """

    model_id: str
    evaluator: EvaluatorPort
    label: str = ""
    model_version: str = ""
    decoder: str = "none"
    endpoint_kind: str = "api"
    web_tool: bool = False

    def __post_init__(self) -> None:
        if not self.model_id:
            raise ValueError("model_id обязан быть непустым")
        if self.web_tool:
            raise BenchmarkContractError(
                "I18: web_tool=True недопустим — оценщик не вызывает внешний поиск"
            )

    @property
    def display_label(self) -> str:
        """Метка для таблицы: ``label`` либо ``model_id``."""
        return self.label or self.model_id

    def declared_axes(self) -> dict[str, Any]:
        """Объявленные оси в форме ``DeclaredExecutionAxes.llm_scenarios`` (execution_axes).

        ``seed``/``temperature`` — **наблюдаемые у порта** значения (``None`` — порт их не
        объявляет; это честное «не подтверждено структурно», а не подставленный дефолт).
        """
        return {
            "model_id": self.model_id,
            "model_version": self.model_version or None,
            "endpoint_kind": self.endpoint_kind,
            "decoder": self.decoder,
            "web_tool": False,
            "seed": getattr(self.evaluator, "seed", None),
            "temperature": getattr(self.evaluator, "temperature", None),
        }

@dataclass(frozen=True)
class CellCase:
    """Ячейка измерения: **замороженный** вход, одинаковый для всех моделей.

    :param param: строка каталога (правило/absence_rule/роль — вход промпта).
    :param product_id: продукт ячейки (I17: evidence только из его чанков).
    :param run_id: прогон (нужен для детерминированного ``evidence_id``; store не пишется).
    :param chunks: продукт-скоупед top-K, полученный **один раз** до моделей (I20).
    :param retrievals: провенанс сборки/профиля/K для ячейки (R7/I16/I20).
    :param golden: эталонная строка (ожидаемый статус) — общая для всех моделей.
    :param satellites: сателлиты как контекст промпта (не доказательство).
    :param index_build_id: сборка прогона, к которой привязан срез (I17/S5).
    :param rrf_k: RRF-константа, с которой выполнен ретривал (провенанс, S1).
    """

    param: CatalogRow
    product_id: str
    run_id: str
    chunks: tuple[ChunkLike, ...]
    retrievals: RetrievalResult
    golden: GoldenRow
    satellites: tuple[Satellite, ...] = ()
    index_build_id: str = ""
    rrf_k: int = DEFAULT_RRF_K

    @property
    def cell_id(self) -> str:
        """Канонический идентификатор ``product_id/param_id``."""
        return f"{self.product_id}/{self.param.param_id}"

@dataclass(frozen=True)
class PreparedCases:
    """Результат подготовки: ячейки + честно пропущенные с причиной.

    ``skipped`` — ``cell_id -> reason``. Пропуск никогда не молчаливый: ячейка либо в
    ``cases``, либо названа здесь.
    """

    cases: tuple[CellCase, ...]
    skipped: dict[str, str]
    run_id: str
    index_build_id: str
    retrieval_profile: str
    retrieval_cutoff_k: int
    rrf_k: int
    notes: tuple[str, ...] = ()

# ---------------------------------------------------------------------------
# Подготовка ячеек (ретривал — один раз, до моделей)
# ---------------------------------------------------------------------------

def _manifest_setting(
    manifest: Any, name: str, fallback: Any
) -> tuple[Any, bool]:
    """Значение настройки из манифеста прогона; ``(value, taken_from_run)``.

    ``None``/пусто в манифесте — настройка не зафиксирована: возвращается безопасный
    дефолт **и** флаг ``False``, чтобы вызывающий записал это в ``notes`` (значение не
    выдаётся за зафиксированное).
    """
    value = getattr(manifest, name, None)
    if value is None or (isinstance(value, str) and not value.strip()):
        return fallback, False
    return value, True

def _manifest_rrf_k(manifest: Any) -> tuple[int, bool]:
    """``rrf_k`` из ``retrieval_params_json`` прогона; иначе дефолт с пометкой."""
    raw = getattr(manifest, "retrieval_params_json", None)
    if not raw:
        return DEFAULT_RRF_K, False
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return DEFAULT_RRF_K, False
    value = data.get("rrf_k") if isinstance(data, Mapping) else None
    if isinstance(value, int) and value > 0:
        return value, True
    return DEFAULT_RRF_K, False

def _product_has_chunks(
    store: StorePort, build_id: str, product_id: str
) -> tuple[bool, str | None]:
    """Есть ли у продукта чанк в сборке; ``(has, error)`` — ошибка чтения названа.

    Ленивая выборка (генератор останавливается на первом элементе) — полный скан не нужен.
    """
    try:
        return next(iter(store.iter_chunks(build_id, product_id=product_id)), None) is not None, None
    except Exception as exc:  # noqa: BLE001 — ошибка чтения ≠ «чанков нет»
        return False, f"{type(exc).__name__}: {exc}"

def prepare_cases(
    *,
    store: StorePort,
    run_id: str,
    golden: Sequence[GoldenRow],
    catalog: Mapping[str, CatalogRow],
    product_ids: Sequence[str] | None = None,
    build_id: str | None = None,
    lexical_index: LexicalIndexPort | None = None,
    vector_index: VectorIndexPort | None = None,
    embedder: EmbedderPort | None = None,
    query_vectors: Mapping[str, Sequence[float]] | None = None,
    satellites: Mapping[str, Sequence[Satellite]] | None = None,
    limit: int | None = None,
    max_queries: int = MAX_QUERIES,
) -> PreparedCases:
    """Готовит :class:`CellCase` из БД прогона, каталога и golden-набора.

    Ретривал вызывается **один раз на ячейку** (:func:`retrieve_for_cell`) с
    зафиксированными профилем/K/``rrf_k``; результат замораживается в :class:`CellCase`,
    поэтому все модели получают байт-в-байт один и тот же срез (I17/I20).

    Профиль/K/``rrf_k`` читаются из строки прогона (``runs``/манифест), а не выбираются
    здесь заново (I16/I20). Если настройка в прогоне не зафиксирована, берётся безопасный
    дефолт **с явной заметкой** в :attr:`PreparedCases.notes`.

    Пропуски всегда названы: ``catalog_missing`` (параметра нет в каталоге),
    ``product_not_in_build`` (продукта нет в сборке), ``no_chunks`` (у продукта нет чанков),
    ``chunk_read_failed`` (ошибка чтения чанков), ``retrieval_failed`` (ретривал бросил),
    ``build_mismatch`` (результат не из сборки прогона — I17). Дубликат golden-строки
    игнорируется с заметкой (не молча).

    :param store: открытый стор прогона (**только чтение**).
    :param catalog: ``param_id -> CatalogRow`` из пакета прогона.
    :param product_ids: продукты сборки; ``None`` — из ``index_manifest``.
    :param build_id: сборка прогона; ``None`` — ``runs.index_build_id``/манифест.
    :param limit: ограничить число ячеек (smoke-подмножество, детерминированно по golden).
    :raises BenchmarkPreconditionError: нет сборки/продуктов/ни одной пригодной ячейки.
    """
    manifest = store.read_manifest(run_id)
    ref = store.read_run_index_ref(run_id) if hasattr(store, "read_run_index_ref") else None
    resolved_build = (
        build_id
        or (getattr(ref, "index_build_id", None) if ref is not None else None)
        or getattr(manifest, "index_build_id", None)
    )
    if not resolved_build:
        raise BenchmarkPreconditionError(
            PRECONDITION_NO_INDEX_BUILD,
            f"в прогоне {run_id!r} не зафиксирована индексная сборка "
            "(runs.index_build_id пуст) — измерять нечего",
        )

    notes: list[str] = []
    profile_raw, profile_fixed = _manifest_setting(
        manifest, "retrieval_profile", DEFAULT_RETRIEVAL_PROFILE.value
    )
    try:
        profile = RetrievalProfile(str(profile_raw).upper())
    except ValueError as exc:
        raise BenchmarkContractError(
            f"профиль ретривала прогона недопустим: {profile_raw!r}"
        ) from exc
    if not profile_fixed:
        notes.append(
            f"retrieval_profile не зафиксирован в прогоне — взят {profile.value} (дефолт)"
        )

    cutoff_raw, cutoff_fixed = _manifest_setting(
        manifest, "retrieval_cutoff_K", DEFAULT_RETRIEVAL_CUTOFF_K
    )
    try:
        cutoff_k = int(cutoff_raw)
    except (TypeError, ValueError) as exc:
        raise BenchmarkContractError(
            f"retrieval_cutoff_K прогона недопустим: {cutoff_raw!r}"
        ) from exc
    if cutoff_k <= 0:
        raise BenchmarkContractError(f"retrieval_cutoff_K обязан быть > 0: {cutoff_k}")
    if not cutoff_fixed:
        notes.append(
            f"retrieval_cutoff_K не зафиксирован в прогоне — взят {cutoff_k} (дефолт)"
        )

    rrf_k, rrf_fixed = _manifest_rrf_k(manifest)
    if not rrf_fixed:
        notes.append(f"rrf_k не зафиксирован в прогоне — взят {rrf_k} (дефолт)")

    if product_ids is None:
        product_ids = sorted(
            {
                entry.product_id
                for entry in store.iter_index_manifest(str(resolved_build))
                if getattr(entry, "product_id", None)
            }
        )
    products = tuple(dict.fromkeys(str(pid) for pid in product_ids))
    if not products:
        raise BenchmarkPreconditionError(
            PRECONDITION_NO_PRODUCTS,
            f"сборка {resolved_build!r} не несёт продукт-партиций — измерять нечего",
        )
    product_set = set(products)
    # Проверяем чанки только у продуктов, которые реально встречаются в golden: для
    # больших когорт это одна ленивая выборка на нужный продукт, а не на всю сборку.
    needed_products = sorted({row.product_id for row in golden} & product_set)
    has_chunks: dict[str, bool] = {}
    chunk_errors: dict[str, str] = {}
    for pid in needed_products:
        found, error = _product_has_chunks(store, str(resolved_build), pid)
        has_chunks[pid] = found
        if error is not None:
            chunk_errors[pid] = error

    cases: list[CellCase] = []
    skipped: dict[str, str] = {}
    seen: set[tuple[str, str]] = set()
    truncated = False
    for row in golden:
        cell_id = f"{row.product_id}/{row.param_id}"
        if limit is not None and len(cases) >= limit:
            truncated = True
            break
        key = (row.product_id, row.param_id)
        if key in seen:
            notes.append(f"{cell_id}: дубликат golden-строки проигнорирован")
            continue
        seen.add(key)
        param = catalog.get(row.param_id)
        if param is None:
            skipped[cell_id] = "catalog_missing"
            continue
        if row.product_id not in product_set:
            skipped[cell_id] = "product_not_in_build"
            continue
        if row.product_id in chunk_errors:
            skipped[cell_id] = f"chunk_read_failed: {chunk_errors[row.product_id]}"
            continue
        if not has_chunks.get(row.product_id, False):
            skipped[cell_id] = "no_chunks"
            continue
        try:
            result = retrieve_for_cell(
                param,
                product_id=row.product_id,
                profile=profile,
                cutoff_k=cutoff_k,
                lexical_index=lexical_index,
                vector_index=vector_index,
                embedder=embedder,
                query_vectors=query_vectors,
                rrf_k=rrf_k,
                max_queries=max_queries,
            )
        except Exception as exc:  # noqa: BLE001 — ячейка изолирована, пропуск назван
            skipped[cell_id] = f"retrieval_failed: {type(exc).__name__}: {exc}"
            continue
        if result.build_id and str(result.build_id) != str(resolved_build):
            skipped[cell_id] = (
                f"build_mismatch: ретривал вернул {result.build_id!r}, "
                f"прогон зафиксировал {resolved_build!r} (I17)"
            )
            continue
        if not result.chunks:
            notes.append(
                f"{cell_id}: ретривал вернул 0 чанков — модель увидит пустой срез "
                "(ячейка сохранена: поведение на пустом входе тоже измеряется)"
            )
        cases.append(
            CellCase(
                param=param,
                product_id=row.product_id,
                run_id=run_id,
                chunks=tuple(result.chunks),
                retrievals=result,
                golden=row,
                satellites=tuple((satellites or {}).get(row.product_id, ())),
                index_build_id=str(resolved_build),
                rrf_k=rrf_k,
            )
        )

    if not cases:
        detail = "; ".join(f"{cid}: {reason}" for cid, reason in sorted(skipped.items()))
        raise BenchmarkPreconditionError(
            PRECONDITION_NO_CASES,
            "ни одной пригодной ячейки: " + (detail or "golden-набор пуст"),
        )
    if truncated:
        notes.append(
            f"limit={limit}: обработаны первые {len(cases)} ячеек — это smoke-подмножество, "
            "не полный golden"
        )

    return PreparedCases(
        cases=tuple(cases),
        skipped=skipped,
        run_id=run_id,
        index_build_id=str(resolved_build),
        retrieval_profile=profile.value,
        retrieval_cutoff_k=cutoff_k,
        rrf_k=rrf_k,
        notes=tuple(notes),
    )

# ---------------------------------------------------------------------------
# Счётчик вызовов (токены/повторы) — обёртка порта, без изменения поведения
# ---------------------------------------------------------------------------

class _CountingEvaluator:
    """Прозрачная обёртка ``EvaluatorPort``: считает вызовы, токены и последний выход.

    Повторы (``max_retries``) увеличивают ``calls``: именно это и надо измерять — цена
    дисциплины схемы. Прокси объявляет те же оси, что несёт внутренний порт, потому что
    :func:`evaluate_cell` читает их через ``getattr`` (``seed``/``temperature``/``decoder``).
    """

    def __init__(self, inner: EvaluatorPort) -> None:
        self._inner = inner
        self.calls = 0
        self.calls_ok = 0
        self.calls_failed = 0
        self.prompt_tokens = 0
        self.completion_tokens = 0
        #: Сколько вызовов реально сообщили usage (нули могут значить «не сообщено»).
        self.usage_reported_calls = 0
        self.last_output: LLMCellOutput | None = None
        self.last_error: str | None = None
        self.model_id = getattr(inner, "model_id", "")
        self.model_version = getattr(inner, "model_version", "")
        self.decoder = getattr(inner, "decoder", "none")
        self.endpoint_kind = getattr(inner, "endpoint_kind", "")
        self.seed = getattr(inner, "seed", None)
        self.temperature = getattr(inner, "temperature", None)

    def complete(self, req: LLMRequest) -> LLMResponse:
        """Делегирует вызов и учитывает расход; исключения пробрасываются как есть."""
        self.calls += 1
        try:
            response = self._inner.complete(req)
        except Exception as exc:
            self.calls_failed += 1
            self.last_error = f"{type(exc).__name__}: {exc}"
            raise
        self.calls_ok += 1
        usage = getattr(response, "usage", None)
        prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
        completion_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
        self.prompt_tokens += prompt_tokens
        self.completion_tokens += completion_tokens
        if prompt_tokens or completion_tokens:
            self.usage_reported_calls += 1
        self.last_output = response.output
        return response

# ---------------------------------------------------------------------------
# Замеры и метрики
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CellMeasurement:
    """Один прогон ячейки одной моделью (один повтор).

    ``status`` — статус ячейки (``None`` у ``failed``, I4); ``state`` — состояние
    (``evaluated``/``failed``/…); ``validation_ok`` — прошёл ли эшелон §10;
    ``fabricated_support`` — модель заявила ✅ с непустыми цитатами, но валидация
    цитат не прошла (худший режим отказа, см. :func:`_is_fabricated_support`).
    """

    product_id: str
    param_id: str
    expected_status: str
    status: str | None
    state: str
    validation_ok: bool
    failure_kind: str | None
    fabricated_support: bool
    calls: int
    prompt_tokens: int
    completion_tokens: int
    latency_ms: float
    repeat: int
    prompt_hash: str | None = None
    harness_error: str | None = None

    @property
    def cell_id(self) -> str:
        """``product_id/param_id``."""
        return f"{self.product_id}/{self.param_id}"

@dataclass(frozen=True)
class Interval:
    """Доверительный интервал (bootstrap-перцентиль) либо честное ``None``.

    ``low``/``high`` пусты, когда расчёт не определён; ``note`` называет причину —
    число не выдумывается.
    """

    low: float | None
    high: float | None
    level: float = 0.95
    method: str = "bootstrap-percentile"
    resamples: int = 0
    note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """JSON-представление интервала."""
        return {
            "low": self.low,
            "high": self.high,
            "level": self.level,
            "method": self.method,
            "resamples": self.resamples,
            "note": self.note,
        }

def _percentile(values: Sequence[float], q: float) -> float:
    """Перцентиль линейной интерполяцией (детерминированно, без numpy)."""
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(ordered[lower])
    weight = position - lower
    return float(ordered[lower] * (1.0 - weight) + ordered[upper] * weight)

def _bootstrap_mean_ci(
    values: Sequence[float], *, resamples: int, seed: int, level: float = 0.95
) -> Interval:
    """Bootstrap-перцентильный CI для среднего (``values`` — 0/1 для доли)."""
    if not values:
        return Interval(None, None, level=level, resamples=resamples, note="нет наблюдений")
    rng = random.Random(seed)  # noqa: S311 — bootstrap воспроизводим по seed: криптостойкость вредна
    n = len(values)
    means = [
        sum(values[rng.randrange(n)] for _ in range(n)) / n for _ in range(resamples)
    ]
    alpha = (1.0 - level) / 2.0
    return Interval(
        _percentile(means, alpha),
        _percentile(means, 1.0 - alpha),
        level=level,
        resamples=resamples,
    )

def _bootstrap_kappa_ci(
    rows: Sequence[AnnotationRow],
    *,
    resamples: int,
    seed: int,
    level: float = 0.95,
    known_labels: Sequence[str] | None = None,
) -> Interval:
    """Bootstrap-перцентильный CI для κ; вырожденные ресэмплы честно отбрасываются.

    κ не определён при нулевой вариации маргиналов; такие ресэмплы не «достраиваются»
    нулём/единицей, а исключаются. Если определённых ресэмплов слишком мало, CI —
    ``None`` с причиной.
    """
    if not rows:
        return Interval(None, None, level=level, resamples=resamples, note="нет парных ячеек")
    rng = random.Random(seed)  # noqa: S311 — bootstrap воспроизводим по seed: криптостойкость вредна
    n = len(rows)
    values: list[float] = []
    undefined = 0
    for _ in range(resamples):
        sample = [rows[rng.randrange(n)] for _ in range(n)]
        kappa = compute_agreement(sample, known_labels=known_labels).overall.kappa
        if kappa is None:
            undefined += 1
        else:
            values.append(float(kappa))
    if not values:
        return Interval(
            None,
            None,
            level=level,
            resamples=resamples,
            note=f"κ не определён ни в одном из {resamples} ресэмплов",
        )
    alpha = (1.0 - level) / 2.0
    note = (
        f"{undefined} из {resamples} ресэмплов вырождены (κ не определён) и исключены"
        if undefined
        else None
    )
    return Interval(
        _percentile(values, alpha),
        _percentile(values, 1.0 - alpha),
        level=level,
        resamples=resamples,
        note=note,
    )

def _is_fabricated_support(
    case: CellCase, outcome: EvaluationOutcome, output: LLMCellOutput | None
) -> bool:
    """«Изобретённая опора»: golden ✅, модель заявила ✅ с цитатами, валидация не прошла.

    Валидация ✅-выхода — это ровно проверка цитат (source_id ∈ чанков, цитата —
    подстрока текста, hash/resolver — §10). Поэтому «``validation.ok=False`` при ✅ с
    непустым evidence» означает: ни одна цитата не подтвердилась. Это не «ошибка схемы»,
    а опаснейший режим — модель сфабриковала доказательство.
    """
    if case.golden.expected_status is not Status.OK:
        return False
    if output is None or output.status is not Status.OK or not output.evidence:
        return False
    return not outcome.validation.ok

def _assert_contract(model: ModelUnderTest) -> None:
    """Проверяет фиксированные условия измерения у конкретной модели (не предполагает).

    ``seed``/``temperature`` читаются у порта: ``None`` — порт не объявляет ось (это
    видно в ``axes``), но **иное** значение недопустимо: сравнение шло бы в других условиях.
    """
    seed = getattr(model.evaluator, "seed", None)
    temperature = getattr(model.evaluator, "temperature", None)
    if seed is not None and int(seed) != 0:
        raise BenchmarkContractError(
            f"{model.display_label}: seed={seed!r}, контракт измерения требует 0"
        )
    if temperature is not None and float(temperature) != 0.0:
        raise BenchmarkContractError(
            f"{model.display_label}: temperature={temperature!r}, "
            "контракт измерения требует 0.0"
        )

def _failure_bucket(kind: str | None) -> str:
    """Ключ гистограммы: ``validation_failed``/``evaluation_failed``/``none``."""
    return kind or "none"

def _confusion_to_dict(confusion: Any) -> dict[str, Any]:
    """Матрица ошибок → JSON-словарь (labels + counts)."""
    return {
        "labels": list(confusion.labels),
        "counts": {row: dict(cols) for row, cols in confusion.counts.items()},
    }

@dataclass(frozen=True)
class ModelMetrics:
    """Метрики одной модели на общем наборе ячеек (первичный повтор)."""

    axes: dict[str, Any]
    n_cells: int
    validity_rate: float
    validity_ci: Interval
    failure_kinds: dict[str, int]
    kappa: float | None
    kappa_ci: Interval | None
    kappa_undefined_reason: str | None
    observed_agreement: float | None
    expected_agreement: float | None
    n_paired: int
    n_unpaired: int
    confusion: dict[str, Any]
    abstention: dict[str, Any]
    first_attempt_pass: float
    tokens: dict[str, Any]
    latency_ms: dict[str, float]
    determinism: dict[str, Any]
    cells: tuple[CellMeasurement, ...]

    def to_dict(self) -> dict[str, Any]:
        """JSON-представление метрик модели."""
        return {
            "label": self.axes.get("label"),
            "axes": dict(self.axes),
            "n_cells": self.n_cells,
            "validity": {
                "rate": self.validity_rate,
                "ok": sum(1 for m in self.cells if m.validation_ok),
                "ci95": self.validity_ci.to_dict(),
            },
            "failure_kinds": dict(self.failure_kinds),
            "agreement": {
                "kappa": self.kappa,
                "kappa_ci95": self.kappa_ci.to_dict() if self.kappa_ci else None,
                "undefined_reason": self.kappa_undefined_reason,
                "observed": self.observed_agreement,
                "expected": self.expected_agreement,
                "n_paired": self.n_paired,
                "n_unpaired": self.n_unpaired,
                "confusion": self.confusion,
            },
            "abstention": dict(self.abstention),
            "first_attempt_pass": self.first_attempt_pass,
            "tokens": dict(self.tokens),
            "latency_ms": dict(self.latency_ms),
            "determinism": dict(self.determinism),
            "cells": [
                {
                    "product_id": m.product_id,
                    "param_id": m.param_id,
                    "expected_status": m.expected_status,
                    "status": m.status,
                    "state": m.state,
                    "validation_ok": m.validation_ok,
                    "failure_kind": m.failure_kind,
                    "fabricated_support": m.fabricated_support,
                    "calls": m.calls,
                    "prompt_tokens": m.prompt_tokens,
                    "completion_tokens": m.completion_tokens,
                    "latency_ms": m.latency_ms,
                    "harness_error": m.harness_error,
                }
                for m in self.cells
            ],
        }

@dataclass(frozen=True)
class BenchmarkReport:
    """Отчёт измерения: контракт, покрытие, честные ограничения и метрики по моделям."""

    run_id: str
    index_build_id: str
    retrieval_profile: str
    retrieval_cutoff_k: int
    rrf_k: int
    repeats: int
    n_cells: int
    n_products: int
    input_digest: str
    skipped: dict[str, str]
    limits: tuple[str, ...]
    models: tuple[ModelMetrics, ...]
    generated_at: str
    eval_cache_enabled: bool = False
    bootstrap_resamples: int = 2000
    notes: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Полный JSON-payload отчёта (единая форма для файла и ``--json``)."""
        return {
            "ok": True,
            "status": "measured",
            "kind": "evaluator_benchmark",
            "generated_at": self.generated_at,
            "run_id": self.run_id,
            "index_build_id": self.index_build_id,
            "retrieval": {
                "profile": self.retrieval_profile,
                "cutoff_k": self.retrieval_cutoff_k,
                "rrf_k": self.rrf_k,
            },
            "contract": {
                "seed": BENCHMARK_CONTRACT["seed"],
                "temperature": BENCHMARK_CONTRACT["temperature"],
                "max_retries": BENCHMARK_CONTRACT["max_retries"],
                "strict": BENCHMARK_CONTRACT["strict"],
                "prompt_template": BENCHMARK_CONTRACT["prompt_template"],
                "web_tool": BENCHMARK_CONTRACT["web_tool"],
                "repeats": self.repeats,
                "eval_cache": self.eval_cache_enabled,
                "bootstrap": {
                    "resamples": self.bootstrap_resamples,
                    "level": 0.95,
                },
            },
            "n_cells": self.n_cells,
            "n_products": self.n_products,
            "input_digest": self.input_digest,
            "skipped": dict(self.skipped),
            "limits": list(self.limits),
            "notes": list(self.notes),
            "models": [model.to_dict() for model in self.models],
        }

def _measure_once(
    case: CellCase,
    model: ModelUnderTest,
    *,
    repeat: int,
    timer: Callable[[], float],
    eval_cache: EvalVerdictCache | None,
) -> CellMeasurement:
    """Один замер ячейки моделью: ``evaluate_cell(store=None)`` + учёт расхода.

    ``store=None`` — измерение, а не прогон: ни ячейка, ни evidence не персистятся.
    """
    proxy = _CountingEvaluator(model.evaluator)
    started = timer()
    try:
        outcome = evaluate_cell(
            param=case.param,
            product_id=case.product_id,
            run_id=case.run_id,
            evaluator=proxy,
            chunks=case.chunks,
            satellites=case.satellites,
            strict=True,
            retrievals=case.retrievals,
            max_retries=int(BENCHMARK_CONTRACT["max_retries"]),
            store=None,
            model_id=model.model_id,
            eval_cache=eval_cache,
        )
    except Exception as exc:  # noqa: BLE001 — сбой харнесса изолирован и назван
        elapsed_ms = (timer() - started) * 1000.0
        return CellMeasurement(
            product_id=case.product_id,
            param_id=case.param.param_id,
            expected_status=case.golden.expected_status.value,
            status=None,
            state=CellState.FAILED.value,
            validation_ok=False,
            failure_kind=None,
            fabricated_support=False,
            calls=proxy.calls,
            prompt_tokens=proxy.prompt_tokens,
            completion_tokens=proxy.completion_tokens,
            latency_ms=elapsed_ms,
            repeat=repeat,
            harness_error=f"{type(exc).__name__}: {exc}",
        )
    elapsed_ms = (timer() - started) * 1000.0
    cell = outcome.cell
    return CellMeasurement(
        product_id=case.product_id,
        param_id=case.param.param_id,
        expected_status=case.golden.expected_status.value,
        status=cell.status.value if cell.status is not None else None,
        state=cell.state.value,
        validation_ok=bool(outcome.validation.ok),
        failure_kind=outcome.failure_kind,
        fabricated_support=_is_fabricated_support(case, outcome, proxy.last_output),
        calls=proxy.calls,
        prompt_tokens=proxy.prompt_tokens,
        completion_tokens=proxy.completion_tokens,
        latency_ms=elapsed_ms,
        repeat=repeat,
        prompt_hash=outcome.prompt_hash,
    )

def _abstention_metrics(measurements: Sequence[CellMeasurement]) -> dict[str, Any]:
    """Сдержанность на ``❌`` и «изобретённая опора» на ``✅`` (раздельно, честно).

    * для golden ``❌``: сколько раз модель ответила ``❌`` против ``❓``/``pending``/
      ``failed``/прочего;
    * для golden ``✅``: сколько раз модель заявила ✅ с невалидируемыми цитатами
      (``fabricated_support``) — это не успех, а худший режим отказа.
    """
    absent = [m for m in measurements if m.expected_status == Status.ABSENT.value]
    ok_rows = [m for m in measurements if m.expected_status == Status.OK.value]
    buckets = Counter[str]()
    for m in absent:
        if m.status == Status.ABSENT.value:
            buckets["answered_absent"] += 1
        elif m.status == Status.UNKNOWN.value:
            buckets["answered_unknown"] += 1
        elif m.state == CellState.PENDING.value:
            buckets["answered_pending"] += 1
        elif m.state == CellState.FAILED.value or m.status is None:
            buckets["answered_failed"] += 1
        else:
            buckets["answered_other"] += 1
    fabricated = sum(1 for m in ok_rows if m.fabricated_support)
    return {
        "golden_absent": len(absent),
        "answered_absent": buckets["answered_absent"],
        "answered_unknown": buckets["answered_unknown"],
        "answered_pending": buckets["answered_pending"],
        "answered_failed": buckets["answered_failed"],
        "answered_other": buckets["answered_other"],
        "absent_rate": (buckets["answered_absent"] / len(absent)) if absent else None,
        "golden_ok": len(ok_rows),
        "fabricated_support": fabricated,
        "fabricated_support_rate": (fabricated / len(ok_rows)) if ok_rows else None,
    }

def _model_metrics(
    model: ModelUnderTest,
    measurements: Sequence[CellMeasurement],
    *,
    n_cells: int,
    repeats: int,
    bootstrap_resamples: int,
    bootstrap_seed: int,
) -> ModelMetrics:
    """Считает метрики модели по первичному повтору + детерминизм по всем повторам."""
    primary = [m for m in measurements if m.repeat == 0]
    ordered = primary[:n_cells]
    validity_flags = [1.0 if m.validation_ok else 0.0 for m in ordered]

    failure_kinds: dict[str, int] = {
        "validation_failed": 0,
        "evaluation_failed": 0,
        "none": 0,
        "harness_error": 0,
    }
    for m in ordered:
        if m.harness_error is not None:
            failure_kinds["harness_error"] += 1
        else:
            failure_kinds[_failure_bucket(m.failure_kind)] = (
                failure_kinds.get(_failure_bucket(m.failure_kind), 0) + 1
            )

    known_labels = [status.value for status in Status]
    annotation_rows = [
        AnnotationRow(
            product_id=m.product_id,
            param_id=m.param_id,
            label_a=m.expected_status,
            label_b=m.status,
        )
        for m in ordered
    ]
    agreement = compute_agreement(annotation_rows, known_labels=known_labels)
    paired_rows = [
        row for row in annotation_rows if row.label_a is not None and row.label_b is not None
    ]
    kappa_ci: Interval | None = None
    if agreement.overall.kappa is not None:
        kappa_ci = _bootstrap_kappa_ci(
            paired_rows,
            resamples=bootstrap_resamples,
            seed=bootstrap_seed,
            known_labels=known_labels,
        )

    prompt_tokens = sum(m.prompt_tokens for m in ordered)
    completion_tokens = sum(m.completion_tokens for m in ordered)
    calls = sum(m.calls for m in ordered)
    usage_reported = sum(1 for m in ordered if m.prompt_tokens or m.completion_tokens)
    latencies = [m.latency_ms for m in ordered]

    determinism: dict[str, Any] = {
        "measured": repeats >= 2,
        "repeats": repeats,
        "changed_cells": 0,
        "fraction_changed": None,
        "stable_fraction": None,
    }
    if repeats >= 2:
        statuses: dict[str, list[str | None]] = {}
        for m in measurements:
            statuses.setdefault(m.cell_id, []).append(m.status)
        changed = sum(1 for values in statuses.values() if len(set(values)) > 1)
        determinism = {
            "measured": True,
            "repeats": repeats,
            "changed_cells": changed,
            "fraction_changed": changed / len(statuses) if statuses else None,
            "stable_fraction": (1.0 - changed / len(statuses)) if statuses else None,
        }

    axes = model.declared_axes()
    axes["label"] = model.display_label
    return ModelMetrics(
        axes=axes,
        n_cells=len(ordered),
        validity_rate=(sum(validity_flags) / len(validity_flags)) if validity_flags else 0.0,
        validity_ci=_bootstrap_mean_ci(
            validity_flags, resamples=bootstrap_resamples, seed=bootstrap_seed
        ),
        failure_kinds=failure_kinds,
        kappa=agreement.overall.kappa,
        kappa_ci=kappa_ci,
        kappa_undefined_reason=agreement.overall.undefined_reason,
        observed_agreement=agreement.overall.observed_agreement,
        expected_agreement=agreement.overall.expected_agreement,
        n_paired=agreement.paired_items,
        n_unpaired=len(agreement.unpaired_items),
        confusion=_confusion_to_dict(agreement.overall_confusion),
        abstention=_abstention_metrics(ordered),
        first_attempt_pass=(
            sum(1 for m in ordered if m.validation_ok and m.calls <= 1) / len(ordered)
            if ordered
            else 0.0
        ),
        tokens={
            "prompt_total": prompt_tokens,
            "completion_total": completion_tokens,
            "total": prompt_tokens + completion_tokens,
            "per_cell_mean": (
                (prompt_tokens + completion_tokens) / len(ordered) if ordered else 0.0
            ),
            "calls": calls,
            "calls_per_cell": (calls / len(ordered)) if ordered else 0.0,
            "usage_reported_cells": usage_reported,
        },
        latency_ms={
            "mean": (sum(latencies) / len(latencies)) if latencies else 0.0,
            "p50": _percentile(latencies, 0.50) if latencies else 0.0,
            "p95": _percentile(latencies, 0.95) if latencies else 0.0,
        },
        determinism=determinism,
        cells=tuple(ordered),
    )

def _limits(
    *,
    n_cells: int,
    n_products: int,
    measurements_by_model: Mapping[str, Sequence[CellMeasurement]],
    skipped: Mapping[str, str],
    repeats: int,
    n_absent: int,
    eval_cache_enabled: bool = False,
) -> tuple[str, ...]:
    """Честные ограничения вывода: покрытие, малые страты, неполнота.

    Числа **вычисляются**, а не хардкодятся: отчёт сам говорит, насколько мала выборка.
    """
    limits: list[str] = []
    limits.append(
        f"golden-набор: {n_cells} ячеек на {n_products} продукт(ах) — вывод не переносится "
        "на другие домены/когорты без собственного измерения"
    )
    if n_absent < 5:
        limits.append(
            f"golden: {n_absent} строк(и) ожидаемого ❌ — abstention/κ по отсутствию "
            "оцениваются на очень малой страте; доверительный интервал широк"
        )
    if skipped:
        limits.append(
            f"{len(skipped)} ячеек(и) пропущено (см. skipped) — сравнение неполное"
        )
    if repeats < 2:
        limits.append("детерминизм не измерен (repeats < 2): стабильность статуса неизвестна")
    if eval_cache_enabled:
        limits.append(
            "eval_cache включён: повторы могут обслуживаться кэшем — first_attempt_pass и "
            "детерминизм отражают кэш, а не только модель"
        )
    all_usage_zero = all(
        all(m.prompt_tokens == 0 and m.completion_tokens == 0 for m in ms)
        for ms in measurements_by_model.values()
    )
    if all_usage_zero:
        limits.append(
            "endpoint не сообщил usage — токены/стоимость не измерены (0 не значит «бесплатно»)"
        )
    limits.append(
        "измеряется только модель на фиксированном срезе: Ф0-покрытие корпуса, прецедент и "
        "персистенция отключены (store=None) — это не прогон, а сравнение оценщиков"
    )
    limits.append(
        "κ считается по парным ячейкам; failed (status=None, I4) в κ не входят — "
        "их видно в failure_kinds/validity"
    )
    return tuple(limits)

def run_benchmark(
    cases: Sequence[CellCase],
    models: Sequence[ModelUnderTest],
    *,
    repeats: int = 1,
    eval_cache: EvalVerdictCache | None = None,
    clock: Callable[[], float] | None = None,
    bootstrap_resamples: int = 2000,
    bootstrap_seed: int = 0,
    generated_at: str | None = None,
    skipped: Mapping[str, str] | None = None,
    notes: Sequence[str] = (),
) -> BenchmarkReport:
    """Прогоняет все модели на одном наборе :class:`CellCase` и возвращает отчёт.

    Первичные метрики считаются по первому повтору (``repeat=0``), детерминизм — по всем
    повторам: смешивать их в одну выборку значило бы завышать ``n``.

    :param cases: замороженные ячейки (ретривал уже выполнен, см. :func:`prepare_cases`).
    :param models: кандидаты; каждый обязан объявлять фиксированные оси (``seed``/
        ``temperature``), иначе :class:`BenchmarkContractError`.
    :param repeats: число повторов для пробы детерминизма (``<2`` — проба не измерена).
    :param eval_cache: кэш вердиктов; ключ включает ``model_id``, поэтому модели не
        смешиваются. ``None`` — выключен (дефолт измерения).
    :param clock: часы (``() -> секунды``); ``None`` — ``time.perf_counter``.
    :param bootstrap_resamples: число bootstrap-ресэмплов для CI.
    :param bootstrap_seed: seed bootstrap (детерминизм отчёта).
    :param generated_at: ISO-время отчёта; ``None`` — текущее UTC.
    :param skipped: ``cell_id -> reason`` из :func:`prepare_cases` (пропуски видны в отчёте).
    :param notes: заметки подготовки (незафиксированные настройки, пустые срезы).
    :raises ValueError: нет ячеек/моделей, ``repeats < 1`` либо дубли меток моделей.
    :raises BenchmarkContractError: у модели нарушены условия измерения либо вход разошёлся.
    """
    if repeats < 1:
        raise ValueError("repeats обязан быть >= 1")
    if not cases:
        raise ValueError("нет ячеек для измерения (пустой набор)")
    if not models:
        raise ValueError("нет моделей для измерения")
    labels = [model.display_label for model in models]
    if len(set(labels)) != len(labels):
        raise ValueError(f"метки моделей обязаны быть уникальны: {labels}")
    for model in models:
        _assert_contract(model)

    timer = clock or time.perf_counter
    measurements_by_model: dict[str, list[CellMeasurement]] = {}
    prompt_hashes: dict[tuple[str, str], set[str]] = {}
    for model in models:
        collected: list[CellMeasurement] = []
        for repeat in range(repeats):
            for case in cases:
                measurement = _measure_once(
                    case,
                    model,
                    repeat=repeat,
                    timer=timer,
                    eval_cache=eval_cache,
                )
                collected.append(measurement)
                if measurement.prompt_hash:
                    prompt_hashes.setdefault(
                        (case.product_id, case.param.param_id), set()
                    ).add(measurement.prompt_hash)
        measurements_by_model[model.display_label] = collected

    # Вход обязан быть одинаковым: один и тот же prompt_hash по всем моделям и повторам.
    divergent = sorted(key for key, hashes in prompt_hashes.items() if len(hashes) > 1)
    if divergent:
        raise BenchmarkContractError(
            "вход измерения разошёлся между моделями/повторами "
            f"({len(divergent)} ячеек): {divergent[:3]} — сравнение недопустимо"
        )

    n_cells = len(cases)
    n_products = len({case.product_id for case in cases})
    n_absent = sum(1 for case in cases if case.golden.expected_status is Status.ABSENT)
    digest_source = "\n".join(
        f"{pid}/{param_id}:{min(hashes)}"
        for (pid, param_id), hashes in sorted(prompt_hashes.items())
        if hashes
    )
    metrics = tuple(
        _model_metrics(
            model,
            measurements_by_model[model.display_label],
            n_cells=n_cells,
            repeats=repeats,
            bootstrap_resamples=bootstrap_resamples,
            bootstrap_seed=bootstrap_seed,
        )
        for model in models
    )
    first = cases[0]
    build_ids = {case.index_build_id or str(case.retrievals.build_id) for case in cases}
    profiles = {case.retrievals.profile for case in cases}
    cutoffs = {int(case.retrievals.cutoff_K) for case in cases}
    rrf_values = {int(case.rrf_k) for case in cases}
    for name, values in (
        ("index_build_id", build_ids),
        ("retrieval_profile", profiles),
        ("retrieval_cutoff_K", cutoffs),
        ("rrf_k", rrf_values),
    ):
        if len(values) > 1:
            raise BenchmarkContractError(
                f"{name} различается между ячейками ({sorted(map(str, values))}) — "
                "условия измерения не зафиксированы"
            )
    resolved_skipped = dict(skipped or {})
    limits = _limits(
        n_cells=n_cells,
        n_products=n_products,
        measurements_by_model=measurements_by_model,
        skipped=resolved_skipped,
        repeats=repeats,
        n_absent=n_absent,
        eval_cache_enabled=eval_cache is not None,
    )
    return BenchmarkReport(
        run_id=first.run_id,
        index_build_id=next(iter(build_ids)) if build_ids else "",
        retrieval_profile=next(iter(profiles)) if profiles else "",
        retrieval_cutoff_k=next(iter(cutoffs)) if cutoffs else 0,
        rrf_k=next(iter(rrf_values)) if rrf_values else DEFAULT_RRF_K,
        repeats=repeats,
        n_cells=n_cells,
        n_products=n_products,
        input_digest=content_hash(digest_source),
        skipped=resolved_skipped,
        limits=limits,
        models=metrics,
        generated_at=generated_at
        or _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds"),
        eval_cache_enabled=eval_cache is not None,
        bootstrap_resamples=bootstrap_resamples,
        notes=tuple(notes or ()),
    )
