"""coHound — отчётные артефакты прогона (W10/T10.2, Ш9, §9, 02 §E.2).

Собирает публикуемые артефакты прогона:

* ``battle_card.xlsx`` — шапка (Capability / Coverage / Comparability), матрица
  (статус + обоснование + primary citation, «+N источников»), сателлиты, модульное
  покрытие, sensitivity-приложение;
* ``preliminary_findings.xlsx`` — кандидаты с маркером «не публикация»;
* ``nda_questions.csv`` — все ``❓`` (продукт, параметр, исчерпанные контуры) плюс
  строки-наблюдения оператора: ``crawl_truncation`` (усечённый краул),
  ``corpus_incomplete`` (Ф0) и ``golden_exempt_product`` (§11.6: для продукта эталон
  объявленно отсутствует — ``sources/golden_status.yaml: exempt_products``);
* ``diff.md`` — изменённые статусы + evidence_id;
* ``run_manifest.yaml`` — агрегат версий/моделей/сборки/профиля+K/бюджета (R7);
* ``outputs/publishability.yaml`` — профиль публикуемости (A1/§6) вместе с объявленным
  исключением стенда RnD (§10): ``rnd_endpoint``, хосты endpoint'ов, замечания;
* ``outputs/measurement_uncertainty.yaml`` — полоса неопределённости вердиктов (P6c) с
  провенансом и машинным ограничением ранжирования;
* ``outputs/corpus_freshness.yaml`` — даты актуальности артефактов корпуса (план 20 §7 F2):
  опорное наблюдение прогона, ``corpus_as_of``/``stale_roots`` и распределение возраста по
  продукту (медиана/p90/max); ``outputs/cell_freshness.csv`` — ``evidence_as_of`` ячеек.

Инварианты:

* **I11 / G4** — ``battle_card`` **не публикуется без Coverage**. Контракт:
  при полностью отсутствующем Coverage (``denominator == 0`` / ``n_used == 0``)
  :func:`render_battle_card` бросает :class:`PublishBlockedError`, а
  :func:`write_reports` не эмитит артефакт (``Artifacts.battle_card is None``);
  при наличии Coverage, но **недостаточном** (Comparability ``INSUFFICIENT`` либо
  ``coverage.value`` ниже порога) артефакт пишется **приглушённым** — с явной пометкой
  «недостаточное покрытие». Молчаливая публикация исключена.
* **K6 / A3** — при демотировании (``computed_ranking == coverage_fallback``)
  продукты упорядочиваются по Coverage с явной пометкой
  :data:`FALLBACK_ORDER_LABEL`; запрошенный режим демотирование не переопределяет.
* **I13** — ``preliminary`` не попадает в battle card/NDA; идёт только в
  ``preliminary_findings`` с маркером «не публикация».
* **§13** — межцикловый ``diff`` запрещён при смене major ``methodology_version``,
  методологического класса (:data:`METHODOLOGY_FIELDS`) либо расхождении **условий
  измерения** (``execution_contract_digest``; для legacy-пары без дайджеста —
  :data:`EXECUTION_FIELDS`); контракт — **отказ** (:class:`CrossCycleDiffError`).
  Причины маркируются классом: ``methodology_change`` / ``execution_change``.
  Новая композиция при неизменном контракте — разрешённый diff класса
  ``content_change`` (баннер + per-product doc-delta), а не граница (09 §12.5).

Модуль категорийно нейтрален (§1.3), детерминирован (сортировка продуктов/параметров).
``openpyxl`` — опциональная зависимость: при её отсутствии XLSX-артефакты
деградируют до CSV (суффикс ``.csv``) с тем же логическим составом; ``csv``/``yaml`` —
stdlib.
"""

from __future__ import annotations

import csv
import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from core.config.paths import resolve_paths
from core.domain.enums import (
    BLOCKING_FAILURE_KINDS,
    CellState,
    ChangeClass,
    DenialStrength,
    MeasurementOrigin,
    RankingPolicy,
    SourceScope,
    Status,
)
from core.domain.invariants import (
    ABSENCE_UNDATED_REASON,
    check_unknown_exhaustion,
)
from core.domain.models import (
    CatalogRow,
    CorpusCoverage,
    CorpusSnapshot,
    CrawlProvenance,
    ProtocolExhaustion,
    RunManifest,
    ScenarioBinding,
    canonical_rate_limit,
)
from core.domain.scoring import evidence_coverage
from core.pipeline.artifacts_gap import write_artifacts_gap
from core.pipeline.compute import (
    ScoreBundle,
    assert_publishable,
    publish_warnings,
)
from core.pipeline.corpus_freshness import (
    collect_corpus_freshness,
    write_cell_freshness_rows,
    write_corpus_freshness_artifact,
)
from core.pipeline.crawl_frontier import resolve_crawl_limits
from core.pipeline.digests import EXECUTION_CONTRACT_FIELDS
from core.pipeline.digests import (
    catalog_digest as _catalog_digest,
)
from core.pipeline.digests import (
    cohort_digest as _cohort_digest,
)
from core.pipeline.digests import (
    scoring_policy_version as _declared_scoring_policy_version,
)
from core.pipeline.digests import (
    seeds_digest as _seeds_digest,
)
from core.pipeline.golden_status import GoldenStatus, read_golden_status
from core.pipeline.measurement_uncertainty import build_measurement_uncertainty
from core.pipeline.onboarding import load_effective_seeds as _load_effective_seeds
from core.pipeline.partial_docs import (
    partial_docs_summary,
    write_partial_docs_artifact,
)
from core.pipeline.refresh_modes import canonical_frozen_source
from core.pipeline.support_quality import (
    support_quality,
    write_support_quality_artifact,
)
from core.util.hashing import short_hash

#: Строковые значения блокирующих классов — для разбора legacy-аудита в ячейке,
#: где нет явного ``corpus_blocking_failures``.
_BLOCKING_FAILURE_VALUES: frozenset[str] = frozenset(
    kind.value for kind in BLOCKING_FAILURE_KINDS
)

if TYPE_CHECKING:  # только для аннотаций: без рантайм-связности слоёв
    from core.pipeline.normalize import RunInputs
    from core.pipeline.plan import RunPlan

__all__ = [
    "COVERAGE_INSUFFICIENT_THRESHOLD",
    "DIFF_BOUNDARY_FIELDS",
    "EXECUTION_FIELDS",
    "FALLBACK_ORDER_LABEL",
    "INSUFFICIENT_COVERAGE_NOTE",
    "METHODOLOGY_FIELDS",
    "NOT_FOR_PUBLICATION",
    "Artifacts",
    "CrossCycleDiffError",
    "PublishBlockedError",
    "build_manifest",
    "collect_absence_undated_anomalies",
    "collect_corpus_coverage_anomalies",
    "collect_crawl_truncation_anomalies",
    "collect_golden_exempt_anomalies",
    "collect_i3_anomalies",
    "collect_run_products",
    "compact_search_effort",
    "crawl_control_digest",
    "crawl_policy_provenance",
    "declared_identity_digests",
    "diff_boundary_violations",
    "manifest_payload",
    "render_battle_card",
    "render_diff",
    "render_nda_questions",
    "render_preliminary_findings",
    "render_run_product",
    "resolve_diff_change_class",
    "write_measurement_uncertainty_artifact",
    "write_reports",
    "write_run_manifest",
]

#: Порог «недостаточного» Coverage для приглушения battle card (§9/02 §E.2).
COVERAGE_INSUFFICIENT_THRESHOLD: float = 0.5

#: Явная пометка fallback-порядка при демотировании (K6/A3, §6). Точная формулировка
#: регламента — не менять.
FALLBACK_ORDER_LABEL: str = (
    "упорядочено по полноте исследования, не по функциональности"
)

#: Пометка приглушения при недостаточном Coverage (§9).
INSUFFICIENT_COVERAGE_NOTE: str = (
    "недостаточное покрытие (insufficient coverage): баллы приглушены, "
    "сравнение неполно"
)

#: Маркер непубликуемости preliminary-кандидатов (§9, I13).
NOT_FOR_PUBLICATION: str = "не публикация"

#: §13/B-7 (v2.1): поля **методологического** класса версий. Их смена означает
#: несовместимость расчёта (major ``methodology_version`` проверяется отдельно) и
#: маркируется причиной ``methodology_change``.
METHODOLOGY_FIELDS: tuple[str, ...] = (
    "scoring_policy_version",
    "status_dict_version",
    "capability_policy",
    "unknown_policy",
    "partial_score",
)

#: §13/B-7 (v2.1): поля **исполнительного** класса (что именно исполнялось). Их смена —
#: версионная граница, но не методологическая несовместимость; маркируется причиной
#: ``execution_change``.
#:
#: 09 §12.5: для пары прогонов, у которых **есть** ``execution_contract_digest``, эти поля
#: построчно **не** сравниваются — контракт уже свёртка тех же значений, а перебор
#: продублировал бы одну причину многими строками (и делал бы ``index_build_id`` границей,
#: запрещая каждый недельный инкремент). Кортеж остаётся границей **legacy-пары** (дайджест
#: ``None`` хотя бы с одной стороны): старые сравнения не легализуются ретроактивно.
EXECUTION_FIELDS: tuple[str, ...] = (
    "case_id",
    "cohort_ref",
    # §13:670: ``catalog_version`` ∈ ``execution_versions``. Раньше смена каталога была
    # видна в diff только косвенно — через ``params_token`` в ``index_build_id``. После
    # разделения id (09 §8.1) ``params_token`` из формулы убран, поэтому каталог обязан
    # быть явной границей ``execution_change``, иначе diff молча сравнил бы несопоставимые
    # условия измерения.
    "catalog_version",
    "index_build_id",
    "embed_model_id",
    "chunker_version",
    "extractor_version",
    "lexical_engine_version",
    "vector_store",
    "retrieval_profile",
    "retrieval_cutoff_K",
    "retrieval_params_json",
    "llm_scenarios",
    # Backlog B (§13): операционные оси краула (commodity-слой, A2). Их смена —
    # версионная граница класса ``execution_change`` (не ``methodology_change``).
    "crawl_strategy",
    "crawl_rate_limit",
    # D1/D2 (§13): оси контроля краула (глубина/потолок документов/блэклисты/воркеры) —
    # тот же класс ``execution_change``.
    "crawl_max_workers",
    "crawl_extract_workers",
    "crawl_extract_max_tasks_per_child",
    "crawl_max_response_bytes",
    "crawl_document_level",
    "crawl_max_depth",
    "crawl_max_documents",
    "crawl_exclude_paths",
    "crawl_exclude_documents",
    # N6 (§13): дайджест разрешённой конфигурации контроля краула (все слои) — смена на
    # любом слое (app/run/per-site) помечается ``execution_change``.
    "crawl_config_digest",
    # E2 (§13): дайджест загруженных package-scoped спецификаций реестров. Персистится в
    # ``runs.registry_spec_digest`` (D1/E2) и читается обратно ``read_manifest``, поэтому смена
    # спеки (или появление/исчезновение реестра) реально помечается ``execution_change``
    # в ``hound diff``. Граница — по **загруженным спецификациям** (провенанс исполнения), а НЕ
    # по выходам: R-ячейки реестровой волны перезаписываются оценщиком Ш6 для того же
    # ``(run_id, product_id, param_id)`` (открытый остаток Ш3↔Ш6, см. progress.md), поэтому
    # «меняет выходные R-ячейки» здесь не утверждается.
    "registry_spec_digest",
)

#: Обратно-совместимый алиас (v2.0): прежде границей считался только исполнительный
#: набор. Сохранён для внешних импортов; новые правки — через :data:`EXECUTION_FIELDS`.
DIFF_BOUNDARY_FIELDS: tuple[str, ...] = EXECUTION_FIELDS


# ---------------------------------------------------------------------------
# Ошибки и результат
# ---------------------------------------------------------------------------


class PublishBlockedError(RuntimeError):
    """G4/I11: battle card не публикуется без Coverage (``denominator == 0``)."""


class CrossCycleDiffError(RuntimeError):
    """§13: межцикловый diff запрещён (major методологии либо версионная граница)."""


@dataclass
class Artifacts:
    """Пути записанных артефактов прогона (``runs/{run_id}/outputs/``, 03 §A.2).

    ``battle_card`` — ``None`` при срабатывании G4/I11 (нет Coverage вообще).
    Остальные поля — пути записанных файлов; ``manifest`` — ``run_manifest.yaml``.

    ``i3_anomalies`` — собранные аномалии I3 (§5): strict-``❓`` без полного протокола
    исчерпания, **вместе** с D1/G-аномалиями усечённого краула (обратная совместимость
    имени). ``crawl_truncation_anomalies`` — только подмножество последних, чтобы CLI/JSON
    мог их отличить (B3). ``corpus_coverage_anomalies`` — Ф0-аномалии неполного покрытия
    корпуса (blocked counts + ``complete``), также входят в ``i3_anomalies``.
    ``golden_exempt_products`` — продукты когорты, для которых эталон **объявленно**
    отсутствует (``sources/golden_status.yaml: exempt_products``), тоже входят в
    ``i3_anomalies``: «эталона для продукта нет» — объявление о проверочном инструменте,
    и оно обязано быть видно в артефактах прогона, а не только в файле пакета.
    """

    battle_card: Path | None
    preliminary_findings: Path | None
    nda_questions: Path | None
    diff: Path | None
    manifest: Path
    i3_anomalies: list[dict[str, Any]] = field(default_factory=list)
    crawl_truncation_anomalies: list[dict[str, Any]] = field(default_factory=list)
    corpus_coverage_anomalies: list[dict[str, Any]] = field(default_factory=list)
    #: Продукты с объявленным waiver'ом эталона (``golden_exempt_product``), по
    #: ``product_id``; пусто — waiver'ов нет либо объявления не читали.
    golden_exempt_products: list[dict[str, Any]] = field(default_factory=list)
    #: Ячейки, у которых ``❌`` **снят** инвариантом I2d — у отрицания не оказалось
    #: датированного носителя (``absence_undated``, план 20 §11.2): продукт/параметр и
    #: названный виновник (документ без даты / корень без ``complete``-снятия). Читается
    #: из записи ячейки, а не пересчитывается — второй носитель правила разошёлся бы с
    #: первым. Входят и в ``i3_anomalies`` (обратная совместимость имени).
    absence_undated_anomalies: list[dict[str, Any]] = field(default_factory=list)
    #: ``outputs/run_product.csv`` — статус продукта **в прогоне** (16 §3.2): mode/status/
    #: scope/счётчики документов и ячеек. ``None`` — строк нет (legacy-прогон без таблицы
    #: ``run_product``): файл не создаётся, «нет строк» не выдаётся за «всё complete».
    run_product: Path | None = None
    #: Те же строки ``run_product``, что в файле, — для ``--json`` payload (провенанс
    #: прогона без чтения БД и без разбора CSV).
    run_products: list[dict[str, Any]] = field(default_factory=list)
    #: ``outputs/artifacts_gap.yaml`` — per-URL список недоступных артефактов и заготовка
    #: входа следующего прогона (13-manual-artifacts §4). ``None`` — файл не записан
    #: (сбой выгрузки); форма отчёта от этого не зависит.
    artifacts_gap: Path | None = None
    #: ``outputs/cell_origin.csv`` — происхождение ячеек по продукту (09 §10.1): сколько
    #: всего и сколько перенесено из прошлого прогона. ``None`` — перенесённых нет (файл
    #: не создаётся: нули уже видны в ``run_product``/снимке, а пустой артефакт читался бы
    #: как «перенос работает и ничего не перенёс»).
    cell_origin: Path | None = None
    #: Те же строки, что в файле, — для ``--json`` payload: счётчик переноса читается без
    #: открытия CSV и без чтения ``state.db``. Пусто — перенесённых ячеек нет.
    cell_origins: list[dict[str, Any]] = field(default_factory=list)
    #: ``outputs/stale_cells.csv`` — ячейки, **исключённые** из расчёта как устаревшие
    #: (§13/K2: ``stale_reason`` = привязка к прошлой сборке). ``None`` — устаревших нет
    #: (файл не создаётся: пустой список — не артефакт).
    stale_cells: Path | None = None
    #: ``outputs/publishability.yaml`` — профиль публикуемости (A1/§6): класс и разложение
    #: неизвестности по причинам и продуктам. ``None`` — бандл без профиля (синтетика).
    publishability: Path | None = None
    #: ``outputs/measurement_uncertainty.yaml`` — полоса неопределённости измерения вердиктов
    #: (P6c): доля ячеек, меняющих вердикт при повторе, с провенансом и машинным
    #: ограничением ранжирования. ``None`` — файл не записан (сбой выгрузки).
    measurement_uncertainty: Path | None = None
    #: ``outputs/cell_diagnostics.csv`` — ячейки, о которых нужно знать человеку:
    #: непустые ``validation_notes``, незаписанное ``absence_rule``, непустой
    #: ``review_reason``. ``None`` — таких ячеек нет (файл не создаётся: пустой список
    #: не артефакт).
    cell_diagnostics: Path | None = None
    #: ``outputs/partial_docs.csv`` — документы корпуса, прочитанные **не целиком**
    #: (I2/I2c): продукт, документ, причина, страницы прочитано/заявлено, символы.
    #: ``None`` — частичных документов нет (пустой список не артефакт).
    partial_docs: Path | None = None
    #: Блок ``partial_docs`` манифеста (те же числа, свёрнутые до продукта) — чтобы CLI
    #: мог назвать факт строкой, даже когда CSV не создан.
    partial_docs_summary: dict[str, Any] | None = None
    #: ``outputs/support_quality.yaml`` — два замера качества поддержки карты: «о предмете
    #: ли доказательство» и «сколько presence-ячеек держится на одной цитате». Наблюдения,
    #: а не гейты. ``None`` — замер не нашёл материала (нет presence-ячеек).
    support_quality: Path | None = None
    #: Те же числа свёрнутыми — чтобы CLI назвал факт строкой, даже когда файла нет.
    support_quality_summary: dict[str, Any] | None = None
    #: ``outputs/corpus_freshness.yaml`` — даты актуальности артефактов корпуса (план 20
    #: §7 F2): опорное наблюдение прогона, ``corpus_as_of``/``stale_roots`` по продукту,
    #: распределение возраста (медиана/p90/max) и счётчики «дат не записано» —
    #: ``cells``/``cells_without_dated_evidence`` парой (ноль без знаменателя не отличим
    #: от «ячеек не было»). Файл пишется всегда: ``corpus_as_of: null`` — факт, а не
    #: отсутствие артефакта.
    corpus_freshness: Path | None = None
    #: ``outputs/cell_freshness.csv`` — по строке на ячейку **с** датой (``evidence_as_of``
    #: из цитируемых артефактов). ``None`` — датированных ячеек нет (файл не создаётся).
    cell_freshness: Path | None = None
    #: Тот же документ ``corpus_freshness.yaml`` — для ``--json`` payload: файл и машинный
    #: вывод собираются одним расчётом и разойтись не могут.
    corpus_freshness_summary: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Внутренние помощники: openpyxl / I/O
# ---------------------------------------------------------------------------


def _load_openpyxl() -> Any | None:
    """Ленивая загрузка ``openpyxl``; ``None``, если пакет недоступен (CSV-fallback)."""
    try:
        import openpyxl  # type: ignore[import-not-found]
    except ImportError:  # pragma: no cover - зависит от окружения
        return None
    return openpyxl


def _write_csv(
    path: Path, fieldnames: list[str], rows: Iterable[Mapping[str, Any]]
) -> Path:
    """Пишет CSV детерминированно (stdlib ``csv``, LF, UTF-8)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return path


def _write_xlsx(path: Path, sheets: list[tuple[str, list[list[Any]]]]) -> Path:
    """Пишет XLSX с заданными листами; требует доступный ``openpyxl``."""
    openpyxl = _load_openpyxl()
    if openpyxl is None:  # pragma: no cover - вызывающий проверяет заранее
        raise RuntimeError("openpyxl недоступен")
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = openpyxl.Workbook()
    default = workbook.active
    workbook.remove(default)
    for title, rows in sheets:
        worksheet = workbook.create_sheet(title=title)
        for row in rows:
            worksheet.append(list(row))
    workbook.save(path)
    return path


def _tabular_path(out_path: Path, *, xlsx: bool) -> Path:
    """Возвращает фактический путь артефакта: ``.xlsx`` либо ``.csv`` (fallback)."""
    return out_path if xlsx else out_path.with_suffix(".csv")


# ---------------------------------------------------------------------------
# Внутренние помощники: ячейки/каталог (duck typing, категорийная нейтральность)
# ---------------------------------------------------------------------------


def _catalog_map(catalog: Mapping[str, CatalogRow] | Iterable[CatalogRow]) -> dict[str, CatalogRow]:
    """Нормализует каталог к ``{param_id: CatalogRow}`` (Mapping или iterable)."""
    if isinstance(catalog, Mapping):
        return {str(k): v for k, v in catalog.items()}
    return {row.param_id: row for row in catalog}


def _cell_list(cells: Iterable[Any]) -> list[Any]:
    """Материализует ячейки; детерминированный порядок (product, param)."""
    return sorted(cells, key=lambda c: (str(c.product_id), str(c.param_id)))


def _product_ids(cells: Iterable[Any]) -> list[str]:
    return sorted({str(c.product_id) for c in cells})


def _param_ids(cells: Iterable[Any]) -> list[str]:
    return sorted({str(c.param_id) for c in cells})


def _evidence_items(cell: Any) -> list[Any]:
    """Evidence-объекты ячейки (dict/объект), включая absence-evidence для ``❌``."""
    evidence = list(getattr(cell, "evidence", None) or [])
    if not evidence and getattr(cell, "status", None) is Status.ABSENT:
        evidence = list(getattr(cell, "absence_evidence", None) or [])
    return evidence


def _evidence_source(item: Any) -> str | None:
    """Идентификатор источника evidence: ``source_id`` либо ``evidence_id``."""
    if isinstance(item, Mapping):
        value = item.get("source_id") or item.get("evidence_id")
    else:
        value = getattr(item, "source_id", None) or getattr(item, "evidence_id", None)
    return str(value) if value else None


def _evidence_quote(item: Any) -> str | None:
    """Цитата evidence: ``citation_quote`` либо ``quote``."""
    if isinstance(item, Mapping):
        value = item.get("citation_quote") or item.get("quote")
    else:
        value = getattr(item, "citation_quote", None) or getattr(item, "quote", None)
    return str(value) if value else None


def _primary_citation(cell: Any) -> tuple[str, str]:
    """Primary-citation ячейки: ``(source_id, quote)`` первого evidence."""
    items = _evidence_items(cell)
    if not items:
        return "", ""
    return _evidence_source(items[0]) or "", _evidence_quote(items[0]) or ""


def _evidence_ids(cell: Any) -> list[str]:
    """Детерминированный набор идентификаторов evidence ячейки (для diff)."""
    ids = {
        sid
        for item in list(getattr(cell, "evidence", None) or [])
        + list(getattr(cell, "absence_evidence", None) or [])
        if (sid := _evidence_source(item))
    }
    return sorted(ids)


def _justification(cell: Any) -> str:
    """Обоснование ячейки, если носитель его предоставляет (иначе пусто)."""
    value = getattr(cell, "justification", None) or getattr(cell, "rule_note", None)
    return str(value) if value else ""


def _status_symbol(cell: Any) -> str:
    """Символ статуса/состояния ячейки для матрицы (failed/preliminary — не статусы, I4/I13)."""
    status = getattr(cell, "status", None)
    if status is not None:
        return status.value if isinstance(status, Status) else str(status)
    state = getattr(cell, "state", None)
    if state is CellState.FAILED:
        return "failed"
    if state is CellState.PRELIMINARY:
        return "preliminary"
    if state is CellState.PENDING:
        return "pending"
    return "—"


def _matrix_cell(cell: Any | None) -> str:
    """Текст ячейки матрицы: символ + обоснование + citation + «+N источников»."""
    if cell is None:
        return "—"
    parts = [_status_symbol(cell)]
    justification = _justification(cell)
    if justification:
        parts.append(justification)
    source_id, quote = _primary_citation(cell)
    if source_id or quote:
        parts.append(f"{source_id}: {quote}".strip(": "))
    count = len(getattr(cell, "evidence", None) or [])
    if count > 1:
        parts.append(f"+{count - 1} источников")
    return "\n".join(parts)


def _per_product_coverage(cells: list[Any], catalog: Mapping[str, CatalogRow]) -> dict[str, float]:
    """Coverage по продукту (для fallback-порядка, K6). Веса — из важности каталога."""
    grouped: dict[str, list[Any]] = {}
    for cell in cells:
        grouped.setdefault(str(cell.product_id), []).append(cell)
    return {
        product_id: (evidence_coverage(group, catalog, {}).value or 0.0)
        for product_id, group in grouped.items()
    }


def _coverage_absent(bundle: ScoreBundle) -> bool:
    """I11: Coverage отсутствует полностью (нечего сопровождать Capability).

    Блокировка завязана на :func:`core.pipeline.compute.publish_warnings` (Лента 1),
    который возвращает причину при ``coverage.defined is False`` (``UNDEFINED``,
    ``universe = 0``) либо при пустом знаменателе legacy-режима. Логика не дублируется.
    """
    return bool(publish_warnings(bundle))


def _coverage_insufficient(bundle: ScoreBundle) -> bool:
    """Покрытие недостаточно для неоглашённой публикации (приглушение, §9).

    Гейт завязан на **Coverage** (I11): Comparability — лишь аннотация в шапке, и её
    ``INSUFFICIENT`` из-за отсутствия когорты не должен «глушить» полноценное покрытие.
    ``UNDEFINED`` (``value is None``) сюда не доходит — блокируется :func:`_coverage_absent`.
    """
    return (bundle.coverage.value or 0.0) < COVERAGE_INSUFFICIENT_THRESHOLD


def _ordered_products(
    cells: list[Any], bundle: ScoreBundle, catalog: Mapping[str, CatalogRow]
) -> tuple[list[str], bool]:
    """Продукты матрицы: fallback-порядок по Coverage при демотировании (K6/A3)."""
    products = _product_ids(cells)
    if bundle.computed_ranking == RankingPolicy.COVERAGE_FALLBACK.value:
        coverage = _per_product_coverage(cells, catalog)
        products = sorted(products, key=lambda p: (-coverage.get(p, 0.0), p))
        return products, True
    return products, False


# ---------------------------------------------------------------------------
# run_manifest.yaml
# ---------------------------------------------------------------------------


def _first_present(*values: Any) -> Any:
    """Первое непустое (не ``None`` и не пустая строка) значение цепочки (провенанс)."""
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _embed_json(value: Any) -> str | None:
    """Детерминированный JSON-фрагмент манифеста (R7/§13).

    Готовая строка возвращается как есть; словарь/список сериализуется с
    ``ensure_ascii=False`` и ``sort_keys=True`` — одинаковый вход даёт байт-в-байт
    одинаковую строку. ``None`` остаётся ``None``.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _as_ranking_policy(value: Any) -> RankingPolicy | None:
    """Приводит ранжирование к :class:`RankingPolicy` (``None`` — если не задано)."""
    if value is None:
        return None
    if isinstance(value, RankingPolicy):
        return value
    return RankingPolicy(str(value))


def _coerce_int(value: Any) -> int | None:
    """Приводит ``cutoff_K`` к ``int`` (устойчиво к строковой сериализации)."""
    if value is None:
        return None
    return int(value)


def _document_level_value(value: Any) -> str | None:
    """P5: строковое значение оси ``document_level`` (``StrEnum``) либо ``None``."""
    if value is None:
        return None
    return str(getattr(value, "value", value))


def _axis_value(value: Any) -> Any:
    """Значение оси в канонической форме: ``StrEnum`` → строка (дайджест — это JSON)."""
    return getattr(value, "value", value)


def _run_embed_device(run: Any) -> str | None:
    """``run.yaml:embed.device`` в канонической форме (``StrEnum`` → строка) либо ``None``.

    Модульные вызовы ``build_manifest`` не несут ``axes``, и без этого запасного пути
    ``embed_device`` в манифесте молчал бы, хотя прогон объявил ось в ``run.yaml``. Слой
    app (``deploy.yaml``) здесь не разрешается: это работа резолвера
    (``core.pipeline.crawl_frontier.resolve_embed_device``), а не манифеста.
    """
    embed = getattr(run, "embed", None)
    return _document_level_value(getattr(embed, "device", None))


#: Литерал версии на случай, когда источника нет вовсе (stub-объект без ``catalog``,
#: модульный вызов без входов). Это **не** значение по умолчанию для живого прогона:
#: там версия выводится из содержимого (см. :func:`_catalog_version_from_inputs`).
_MISSING_VERSION = "0.0.0"


def _catalog_version_from_inputs(inputs: Any) -> str:
    """``catalog_version`` из **содержимого** каталога — тот же источник, что у контракта.

    До 29.10.2026 ``catalog_version`` не имел источника: контракт нёс ``None``, манифест —
    фиктивное ``"0.0.0"``, и смена каталога (новый параметр, изменённое правило, другой
    ``absence_rule``) меняла предмет измерения, не меняя ни ``run_id``, ни ``hound diff``
    (`09` §12.5). Теперь источник объявлен — отпечаток содержимого
    (:func:`core.pipeline.digests.catalog_digest`), и манифест берёт **ту же функцию**,
    что ``build_execution_contract``: второй формулы нет, поэтому манифест и контракт не
    могут разойтись по одному и тому же материалу.

    ``inputs.catalog`` отсутствует/``None`` (stub в тестах, вызов без входов) — источника
    нет, остаётся :data:`_MISSING_VERSION`. Пустой объявленный каталог — **не** то же
    самое: у него есть отпечаток, и он отличается от «версия не записана».
    """
    catalog = getattr(inputs, "catalog", None)
    if catalog is None:
        return _MISSING_VERSION
    return _catalog_digest(catalog)


def crawl_control_digest(
    *,
    run_crawl: Any,
    app: Any,
    profiles: Mapping[str, Any] | None,
    overrides: Mapping[str, Any] | None = None,
) -> str | None:
    """Стабильный дайджест **разрешённой** конфигурации контроля краула (N6/§13).

    Разрешает ``CrawlLimits`` цепочкой app (``deploy.yaml:crawl``) < run (``run.yaml:crawl``) < per-site
    (``domain_profiles``) — по одному «входу на каждый site key» плюс ``__default__`` (app<run без
    per-site; темп ``rate_limit`` входит при объявлении на site/run слое, per-site перекрывает run-level). Канонизируется ``json.dumps(sort_keys=True)``
    и хешируется :func:`short_hash`. Смена осей на **любом** слое меняет дайджест → манифест
    помечает ``execution_change`` (§13), не ``methodology_change``.

    Публичная функция (не только внутренняя деталь манифеста): composition root
    (:mod:`core.cli.execution_axes`) вызывает её для **декларированной** оси контракта.
    Одна формула — два потребителя (контракт и манифест), иначе они разошлись бы.

    :param overrides: операторский слой (``--document-level``/``--pdf-pipeline``): значения
        применяются к **разрешённому** словарю, то есть поверх app < run < per-site.
        Причина — флаг оператора есть объявление этого прогона, а не «уточнение
        исполнения»: ось меняет текст документа, значит корпус. Применение **здесь**, а не
        синтезом секции ``crawl`` в ``inputs.run``, сохраняет честность остальных осей:
        иначе ``run.yaml`` без секции ``crawl`` объявил бы ``strategy=auto``, которого в нём
        нет, и ``--pdf-pipeline structured`` (эффективно no-op) дал бы другой ``run_id``.
        ``None`` — оператор ось не задавал (поведение байт-в-байт).
    """
    from core.config.deploy import CrawlConfig

    app_config = app if isinstance(app, CrawlConfig) else CrawlConfig()
    operator = {key: _axis_value(value) for key, value in (overrides or {}).items()}

    def _resolved(url: str, sites: Mapping[str, Any]) -> dict[str, Any]:
        payload = dict(
            resolve_crawl_limits(url, app=app_config, run=run_crawl, profiles=sites).as_dict()
        )
        payload.update(operator)
        return payload

    payload: dict[str, Any] = {"__default__": _resolved("", {})}
    site_profiles = dict(profiles or {})
    for key in sorted(site_profiles):
        url = str(key) if "://" in str(key) else f"https://{key}"
        payload[str(key)] = _resolved(url, site_profiles)
    material = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return short_hash(material, 24)



def compact_search_effort(facts: Any) -> dict[str, Any] | None:
    """Компактное состояние поискового контура для ``runs.search_effort_budget_json``.

    Манифест обязан называть **объявленный и израсходованный** бюджет (I23), но не список
    запросов: полное наблюдение контура (``queries``/``accepted``/``rejected``) уже лежит
    в провенансе сеанса краула, а колонка ``runs.search_effort_budget_json`` — сводка для
    сравнения прогонов. Избыточность здесь стоила бы размера строки манифеста и ничего не
    добавляла к провенансу.

    :param facts: наблюдение контура (``{"serp": {...}}`` из провенанса сеанса либо уже
        компактный словарь); ``None``/пусто → ``None`` (контур не наблюдался — значение не
        выдумывается, поле остаётся ``null``, как у прогонов без контура).
    """
    if not isinstance(facts, Mapping) or not facts:
        return None
    block = facts.get("serp") if isinstance(facts.get("serp"), Mapping) else facts
    rounds = block.get("rounds") or {}
    budget = block.get("budget") or {}
    accepted = block.get("accepted") or {}
    return {
        "serp": {
            "enabled": bool(block.get("enabled")),
            "rounds": {
                "declared": rounds.get("declared"),
                "executed": rounds.get("executed"),
                "source": rounds.get("source"),
            },
            "budget": {
                "declared_per_round": budget.get("declared_per_round"),
                "effective_per_product": budget.get("effective_per_product"),
                "used": {str(pid): int(n) for pid, n in (budget.get("used") or {}).items()},
                "exhausted": {
                    str(pid): bool(flag)
                    for pid, flag in (budget.get("exhausted") or {}).items()
                },
            },
            "accepted_urls": {
                str(pid): (len(urls) if not isinstance(urls, int) else int(urls))
                for pid, urls in accepted.items()
            },
            "termination": {
                str(pid): reason for pid, reason in (block.get("termination") or {}).items()
            },
            # «Нашли» ≠ «вошло в корпус»: сводка входа находок в корпус (блэклист/потолок).
            "ingest": {
                str(pid): {
                    "enqueued": int(row.get("enqueued", 0) or 0),
                    "dup": int(row.get("dup", 0) or 0),
                    "blacklisted": len(row.get("blacklisted") or ()),
                    "truncated": len(row.get("truncated") or ()),
                }
                for pid, row in (block.get("ingest") or {}).items()
            },
            "unavailable_reason": block.get("unavailable_reason"),
        }
    }


def declared_identity_digests(
    inputs: RunInputs,
    *,
    package_dir: str | Path | None = None,
    urls: Mapping[str, Sequence[str]] | None = None,
    domain_profiles: Mapping[str, Any] | None = None,
) -> tuple[str | None, str | None]:
    """Дайджесты когорты и seed-набора — **те же**, из которых выведен ``run_id``.

    09 §12.5/§13: ``cohort_digest``/``seeds_digest`` — поля ``execution_contract_digest``
    и слагаемые :func:`core.pipeline.plan.derive_run_id`. Прежде они не попадали ни на
    манифест, ни в ``runs``, поэтому при расхождении контракта ``hound diff`` не мог
    назвать ось и печатал «предел диагностики» вместо причины.

    Здесь **нет второй формулы**: вызываются ровно те функции, которыми composition root
    считает те же величины для ``run_id`` (:func:`core.pipeline.digests.cohort_digest` по
    ``inputs.cohort`` и :func:`core.pipeline.digests.seeds_digest` по результату
    :func:`core.pipeline.onboarding.load_effective_seeds`), и на тех же объявленных входах:
    ``inputs.cohort`` + ``run.case_id``/``run.cohort_ref`` и эффективный seed-набор
    (``urls`` — операторский ``--urls``, он **полностью** заменяет пакетный набор).
    Поэтому манифест публикует проекцию объявленного входа, а не результат второго расчёта.

    :param package_dir: каталог пакета — источник пакетного seed-набора. ``None`` без
        ``urls`` означает, что набор не из чего разрешить: ``seeds_digest`` тогда ``None``
        («не записано»), а не дайджест пустого набора (это разные факты).
    :param urls: операторский корпус (``product_id → [url]``). Пустой mapping трактуется
        как «переопределения нет»: пустой ``{}`` приходит и тогда, когда ``--urls`` не
        задавался вовсе (composition root всегда передаёт mapping), и подменять им
        пакетный набор было бы догадкой. Любая последовательность URL приводится к
        ``list`` **на месте вызова**: :func:`core.pipeline.onboarding.load_effective_seeds`
        объявляет ``Mapping[str, list[str]]``, и расширять её сигнатуру ради удобства
        вызывающего — значит менять чужой контракт из-за формы входа.
    :returns: пара ``(cohort_digest, seeds_digest)``.
    """
    run = inputs.run
    cohort = _cohort_digest(
        inputs.cohort,
        case_id=run.case_id,
        cohort_ref=getattr(run, "cohort_ref", None),
    )
    override = (
        {str(key): list(value) for key, value in urls.items()} if urls else None
    )
    if override is None and package_dir is None:
        return cohort, None
    seeds_config, _warnings = _load_effective_seeds(
        package_dir if package_dir is not None else "",
        run.products,
        profiles=domain_profiles,
        urls_override=override,
    )
    return cohort, _seeds_digest(seeds_config)


def crawl_policy_provenance(
    *,
    refresh_policy: Any,
    frozen_source: str | None,
    crawl_policy: Any = None,
    captured_at: str | None = None,
    policy_skips: Iterable[Mapping[str, Any]] = (),
    policy_zero_reasons: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Провенанс эффективной политики краула для манифеста (план 19 §P4).

    Возвращает **провенанс**, а не материал идентичности: в ``run_id`` входят только
    ``refresh_policy``, канонизированный ``frozen_source`` и ``crawl_policy_digest``
    (последний — отдельной колонкой ``runs.crawl_policy_digest``); всё остальное здесь
    (несовпавшие записи, счётчики пропусков, ``captured_at``, оси по корням) описывает
    исполнение и в отпечаток не входит.

    ``crawl_policy`` — тот же объект :class:`~core.pipeline.crawl_policy.CrawlPolicyResolution`,
    из которого выведен ``crawl_policy_digest``: второй формулы нет. ``captured_at``
    берётся из записи снятия **только** при ``refresh_policy=frozen``; для прочих политик
    возвращается ``None`` — время снятия там не наблюдено, и подставлять последнее
    известное значило бы выдать чужое наблюдение за это.

    ``crawl_policy_roots`` — только корни, где ``network_mode``/``corpus_source``
    отличаются от ``fetch``/``network``: перечислять «как обычно» значило бы утопить
    решение в шуме.
    """
    normalized = (
        None if refresh_policy is None else str(refresh_policy)
    )
    if normalized is None:
        frozen_value = None
    else:
        frozen_value = canonical_frozen_source(normalized, frozen_source)
    decisions = tuple(getattr(crawl_policy, "decisions", ()) or ())
    roots: list[dict[str, Any]] = []
    for decision in decisions:
        network_mode = getattr(getattr(decision, "network_mode", None), "value", None)
        corpus_source = getattr(
            getattr(decision, "corpus_source", None), "value", None
        )
        if network_mode == "fetch" and corpus_source == "network":
            continue
        roots.append(
            {
                "product_id": getattr(decision, "product_id", None),
                "scope": getattr(decision, "scope", None),
                "host": getattr(decision, "host", None),
                "path": getattr(decision, "path", None),
                "mode": getattr(getattr(decision, "mode", None), "value", None),
                "decision": getattr(decision, "decision", None),
                "network_mode": network_mode,
                "corpus_source": corpus_source,
                "requested": bool(getattr(decision, "requested", False)),
                "reason": getattr(decision, "reason", None),
            }
        )
    roots.sort(key=lambda item: (str(item["scope"]), str(item["decision"])))
    unmatched = [
        {
            "product_id": getattr(record, "product_id", None),
            "scope_kind": getattr(
                getattr(record, "scope_kind", None), "value", None
            ),
            "host": getattr(record, "host", None),
            "path": getattr(record, "path", None),
            "mode": getattr(getattr(record, "mode", None), "value", None),
            "source": getattr(record, "source", None),
        }
        for record in (getattr(crawl_policy, "unmatched", ()) or ())
    ]
    unmatched.sort(
        key=lambda item: (
            str(item["product_id"]),
            str(item["scope_kind"]),
            str(item["host"]),
            str(item["path"]),
        )
    )
    return {
        # Идентичность (проекция объявленного входа, не второй расчёт).
        "digest": getattr(crawl_policy, "digest", None),
        "refresh_policy": normalized,
        "frozen_source": frozen_value,
        # Провенанс исполнения.
        "unmatched": unmatched,
        "policy_skips": [dict(item) for item in policy_skips],
        "policy_zero_reasons": {
            str(key): str(value) for key, value in (policy_zero_reasons or {}).items()
        },
        "captured_at": captured_at if normalized == "frozen" else None,
        "roots": roots,
    }


def build_manifest(
    *,
    inputs: RunInputs,
    plan: RunPlan,
    bundle: ScoreBundle | None = None,
    index_build_id: str | None = None,
    deploy_profile: str | None = None,
    edge_schema_version: str | None = None,
    edge_capabilities: Mapping[str, Any] | None = None,
    retrieval_profile: str | None = None,
    retrieval_cutoff_K: int | None = None,
    retrieval_params_json: str | None = None,
    created_at: str | None = None,
    settings_provenance: Mapping[str, Any] | None = None,
    ranking_epsilon: float | None = None,
    crawl_provenance: CrawlProvenance | Mapping[str, Any] | None = None,
    domain_profiles: Mapping[str, Any] | None = None,
    crawl_app: Any = None,
    operator_document_level: str | None = None,
    registry_spec_digest: str | None = None,
    execution_contract_digest: str | None = None,
    manual_artifacts: Mapping[str, Any] | None = None,
    manual_artifacts_digest: str | None = None,
    partial_docs: Mapping[str, Any] | None = None,
    axes: Any = None,
    declared_status_dict_version: str | None = None,
    scope_attestations: Mapping[str, str] | None = None,
    observed_versions: Mapping[str, Any] | None = None,
    search_effort: Mapping[str, Any] | None = None,
    run_scope: str | None = None,
    product_plan: Mapping[str, Any] | None = None,
    embed_device: str | None = None,
    previous_run_id: str | None = None,
    change_class: str | None = None,
    cycle_id: str | None = None,
    cohort_digest: str | None = None,
    seeds_digest: str | None = None,
    crawl_policy: Any = None,
    refresh_policy: Any = None,
    frozen_source: str | None = None,
    policy_provenance: Mapping[str, Any] | None = None,
    corpus_snapshot: CorpusSnapshot | Mapping[str, Any] | None = None,
) -> RunManifest:
    """Собирает полностью заполненный :class:`RunManifest` (R7/§13) фазы Ш9.

    Источники по приоритету (провенанс, а не пересчёт):

    * версии — ``inputs.run`` (обогащённый конфиг прогона), затем ``inputs.package`` и
      ``plan``; пустые значения не затирают уже известные (в т.ч. ``methodology_version``
      не откатывается к ``"0.0.0"``, если он есть у входа);
    * ``catalog_version``/``scoring_policy_version`` (`09` §12.5) — те же функции, что у
      контракта: отпечаток содержимого ``inputs.catalog``
      (:func:`core.pipeline.digests.catalog_digest`) и объявленная политика расчёта §6
      (:func:`core.pipeline.digests.scoring_policy_version`). Фиктивного ``"0.0.0"`` в
      живом прогоне больше нет: манифест и ``execution_contract_digest`` описывают один
      материал одним значением, а смена каталога/политики видна и в ``run_id``, и в diff.
      Литерал остаётся только там, где источника нет вовсе (stub без ``catalog``);
    * идентификация — ``plan`` (``run_id``, ``anchor``, scope-модули) и ``inputs.run``
      (``case_id``, ``mode``, ``cohort``, ``snapshot_date``);
    * ранжирование — из ``bundle`` (``requested_ranking``/``computed_ranking``), иначе
      запрошенное берётся из ``run.ranking_policy``;
    * индексный/ретривал-контур — ``index_build_id`` и (если заданы) атрибуты run:
      ``retrieval_profile``/``retrieval_cutoff_K``/``retrieval_params_json`` и версии
      сборки (``embed_model_id``/``chunker_version``/``extractor_version``/
      ``lexical_engine_version``/``vector_store``);
    * §9/A3 (v2.1) — ``settings_provenance`` (провенанс настроек) и ``ranking_epsilon``
      берутся по цепочке ``аргумент → run → inputs → plan`` (``_first_present``); идут в
      манифест **как есть**, без пересчёта дефолтов «по предвидению».
    * §4.4/Phase 2 — ``crawl_provenance`` (провенанс краула: сеансы/движки/виды + дайджест
      источников) передаётся вызывающим как :class:`CrawlProvenance` (или mapping); идёт в
      манифест информационно, вне границ версий (§13).
    * §13/Backlog B — ``crawl_strategy``/``crawl_rate_limit`` берутся из ``run.crawl``
      (секция ``crawl`` ``run.yaml``); при её отсутствии — ``None``. Это **граница**
      ``execution_change`` (операционные оси commodity-слоя, A2), а не ``methodology_change``.
    * §13/E2 — ``registry_spec_digest`` (дайджест загруженных package-scoped спецификаций
      реестров) передаётся вызывающим; при отсутствии реестров — ``None``. Это **граница**
      ``execution_change`` (смена спеки меняет выходные R-ячейки).
    * §13 / 09 §12.5 — ``execution_contract_digest`` передаётся вызывающим **готовым**
      (значение, уже вычисленное composition root для ``run_id``/lineage); здесь оно не
      пересчитывается из другого набора значений и не подменяется пустой строкой.
      ``None`` — дайджест неизвестен (legacy/вызов без идентичности): манифест честно
      молчит, а ``hound diff`` применяет прежнее сравнение ``EXECUTION_FIELDS``.
    * §13 (методология 2.4.0) — ``axes`` (:class:`core.cli.execution_axes.DeclaredExecutionAxes`):
      **декларированные** оси исполнения из того же резолвера, что у контракта
      (``crawl_strategy``/``crawl_rate_limit``/``crawl_config_digest``/
      ``registry_spec_digest``/``deploy_profile``/``edge_schema_version``/
      ``chunker_version``/``llm_scenarios``). Это буквально «источник значений тот же,
      что у манифеста»: второй формулы нет. Наблюдаемые величины (``extractor_version``/
      ``embed_model_id``/``lexical_engine_version``/``vector_store``) по-прежнему
      приходят из ``run``/``inputs`` (их пишет фаза отчёта из корпуса/диска) и в контракт
      не входят. ``axes=None`` — legacy/модульный вызов: прежние цепочки ``_first_present``.
    * P5/P7 — ``operator_document_level`` (``--document-level``): операторская ступень
      сильнее run/app-слоя, поэтому ``crawl_document_level`` манифеста называет именно её.
      Без этого провенанс расходился бы с ``crawl_config_digest`` (манифест: ``full``,
      дайджест: ``text``) — «одно значение в отпечатке, другое в публикации».

    Деплой-провенанс (``deploy_profile``/``edge_schema_version``) заполняется только если
    соответствующие поля есть в текущей схеме :class:`RunManifest` (defensive: иначе
    значения уходят в ``edge_capabilities``), что сохраняет совместимость при разносе
    правок между лейнами.

    * I2b/I2c — ``scope_attestations`` (``product_id → attested_by``): подписи, которыми
      прогон авторизовал ``❌``. Информационный провенанс того же класса, что
      ``reviewed_by``: в ``EXECUTION_CONTRACT_FIELDS`` **не входит** (авторизация, не
      измерение), поэтому смена подписанта не даёт новый ``run_id``.
    * План 20 §5 — ``corpus_snapshot``: снимок корпуса (``corpus_snapshot_id``, снятия
      прогона, ``corpus_as_of``). Провенанс, а не объявленный вход: в ``run_id`` не входит
      (снимок известен только после краула). Приходит готовым из durable-носителя фазы
      ``corpus`` — здесь содержание не пересчитывается.
    * 13-manual-artifacts §3.2/§3.4 — ``manual_artifacts_digest`` (отпечаток объявленных
      файлов оператора; поле контракта) и ``manual_artifacts`` (наблюдённая доля ручного:
      ``count``/``share_of_corpus``/``bytes``/``by_reason``/``by_product``). Первый —
      условие измерения (меняет корпус), второй — факт прогона: он **не** входит в
      контракт, иначе «сколько предъявлено» стало бы входом, влияющим на ``run_id``.
    * ``observed_versions`` — наблюдаемые величины сборки (``embed_model_id``/
      ``extractor_version``/``lexical_engine_version``/``vector_store``/``chunker_version``),
      которые фаза отчёта читает из **durable-манифеста индексной сборки**, когда живой
      ``holder`` пуст (resume: ``corpus`` короткозамкнут персистентным ``ok``). Без этого
      манифест завершённого прогона публиковал ``embed_model_id: null`` при 9398 векторах,
      построенных bge-m3, — то есть молчал о приборе, которым сделано измерение.
      Источник именно манифест сборки, а не текущие порты: он описывает ту сборку, на
      которой реально считался прогон, а порты в новом процессе могут быть другими.
      Приоритет ниже ``run``/``inputs``: живое наблюдение, если оно есть, точнее.
    * 16 §3.1/§4 — ``run_scope`` (``full`` | ``only_failed`` | ``product:<id>``),
      ``product_plan`` (сериализованный ``ProductPlanSet``) и ``embed_device``
      (``cpu``/``cuda``). Первые два — **провенанс среза**: без них полное измерение
      неотличимо от ``--only-product`` без заглядывания в ``state.db`` (09 §13.6).
      Третий — наблюдаемая ось: она уже входит в ``execution_contract_digest``, но «на чём
      считались векторы» обязано читаться запросом. Все три приходят явным аргументом;
      ``embed_device`` дополнительно берётся из ``axes`` (тот же резолвер, что у контракта),
      затем из ``run.yaml:embed.device``. ``None`` — значение **не записано**, и подменять
      его на ``full``/``cpu`` запрещено: догадка выдала бы себя за объявленный вход (A3).
    * 09 §12.3/§13 — ``previous_run_id`` и ``change_class``: против какого прогона считался
      этот и как классифицировано изменение. Источник — **строка lineage прогона**
      (``change_class`` вычисляется при её записи, ``build_run_identity`` →
      ``record_run_lineage``): здесь значение не пересчитывается. Оба ``None`` — не
      записано (догадка запрещена: «предшественника не было» и «его не записали» —
      разные факты, и второй виден по пустой истории ``state.db``).
    * 09 §2.4/§6.2 — ``cycle_id``: эффективный цикл прогона, тот же, что вошёл в материал
      ``run_id``. Источник тот же — строка lineage (писатель у неё один); ``None`` — не
      записано, и подменять его на ``snapshot_date`` запрещено: эффективный цикл мог быть
      задан ``--cycle-id``/``run.yaml`` и отличается от даты снимка.
    * 09 §12.5/§13 — ``cohort_digest``/``seeds_digest``: слагаемые ``run_id`` и поля
      ``execution_contract_digest``. Приходят **готовыми** (:func:`declared_identity_digests`
      считает их теми же функциями и на тех же объявленных входах, что composition root для
      ``run_id``); здесь не пересчитываются — манифест публикует проекцию входа, а не второй
      расчёт. Без них ``hound diff`` не мог назвать ось расхождения контракта.
    * План 19 §P4 — ``crawl_policy`` (тот же :class:`CrawlPolicyResolution`, что дал
      ``crawl_policy_digest`` для ``run_id``), ``refresh_policy``/``frozen_source``
      (исполненная политика и её канонизированное значение) и ``policy_provenance``
      (наблюдённые ``captured_at``/``policy_skips``/``policy_zero_reasons``). Дайджест и
      несовпавшие записи — проекция объекта; провенанс — отдельный блок манифеста и в
      ``run_id`` не входит.

    Функция детерминирована: все встраиваемые JSON-фрагменты сериализуются
    ``sort_keys=True`` (R7/§13), списки нормализуются сортировкой; единственный
    недетерминированный вход — ``created_at`` — передаётся вызывающим (композиция
    использует ``ctx.clock``).
    """
    run: Any = getattr(inputs, "run", inputs)
    package: Any = getattr(inputs, "package", None)

    methodology_version = _first_present(
        getattr(run, "methodology_version", None),
        getattr(inputs, "methodology_version", None),
        getattr(plan, "methodology_version", None),
    ) or "0.0.0"
    package_version = _first_present(
        getattr(run, "package_version", None),
        getattr(package, "package_version", None),
        getattr(inputs, "package_version", None),
    ) or "0.0.0"
    # 09 §12.5: ``catalog_version``/``scoring_policy_version`` были полями контракта без
    # источника — в манифест уходило фиктивное ``"0.0.0"``, а контракт нёс ``None``, то
    # есть смена каталога или политики расчёта не меняла ни ``run_id``, ни diff. Теперь
    # источники честные и **те же**, что у контракта
    # (:func:`core.pipeline.digests.catalog_digest` по ``inputs.catalog`` и
    # :func:`core.pipeline.digests.scoring_policy_version` по политике §6): второй
    # формулы нет, поэтому манифест и контракт не могут разойтись по одному материалу.
    # Литерал ``"0.0.0"`` остаётся только там, где источника нет вовсе (stub-объект без
    # ``catalog``) — «не из чего вывести» отличимо от «каталог пуст».
    catalog_version = _first_present(
        getattr(run, "catalog_version", None),
        getattr(plan, "catalog_version", None),
        getattr(inputs, "catalog_version", None),
    ) or _catalog_version_from_inputs(inputs)
    status_dict_version = _first_present(
        getattr(run, "status_dict_version", None),
        getattr(inputs, "status_dict_version", None),
        declared_status_dict_version,
    ) or "0.0.0"
    scoring_policy_version = (
        _first_present(
            getattr(run, "scoring_policy_version", None),
            getattr(inputs, "scoring_policy_version", None),
        )
        or _declared_scoring_policy_version()
    )

    created = _first_present(
        created_at,
        getattr(run, "created_at", None),
        getattr(inputs, "created_at", None),
    ) or ""

    snapshot_raw = getattr(run, "snapshot_date", None)
    snapshot_date = str(snapshot_raw) if snapshot_raw is not None else None

    anchor_product_id = _first_present(
        getattr(plan, "anchor", None),
        getattr(run, "anchor", None),
    ) or ""

    focus_modules = sorted(str(module) for module in (getattr(plan, "modules", None) or []))

    requested = bundle.requested_ranking if bundle is not None else None
    if requested is None:
        requested = getattr(run, "ranking_policy", None)
    computed = bundle.computed_ranking if bundle is not None else None

    resolved_index_build_id = _first_present(
        index_build_id,
        getattr(run, "index_build_id", None),
        getattr(plan, "index_build_id", None),
    )
    retrieval_profile = _first_present(
        retrieval_profile,
        getattr(run, "retrieval_profile", None),
        getattr(inputs, "retrieval_profile", None),
        getattr(plan, "retrieval_profile", None),
    )
    retrieval_cutoff_K = _coerce_int(
        _first_present(
            retrieval_cutoff_K,
            getattr(run, "retrieval_cutoff_K", None),
            getattr(inputs, "retrieval_cutoff_K", None),
            getattr(plan, "retrieval_cutoff_K", None),
        )
    )
    retrieval_params_json = _embed_json(
        _first_present(
            retrieval_params_json,
            getattr(run, "retrieval_params_json", None),
            getattr(inputs, "retrieval_params_json", None),
            getattr(plan, "retrieval_params_json", None),
        )
    )
    # I23/§5: объявленный и **израсходованный** поисковый бюджет. Приоритет: явный
    # аргумент (наблюдённое состояние SERP-контура из durable-провенанса сеанса) → поле
    # строки прогона → вход. Колонка ``runs.search_effort_budget_json`` читалась отчётом,
    # но не писалась никем и всегда оставалась ``null`` — теперь её заполняет фаза отчёта.
    search_effort_budget_json = _embed_json(
        _first_present(
            search_effort,
            getattr(run, "search_effort_budget_json", None),
            getattr(inputs, "search_effort_budget_json", None),
        )
    )

    llm_scenarios = (
        [
            # ``reasoning_effort`` — слагаемое **контракта** (``llm_scenarios`` →
            # ``execution_contract_digest`` → ``run_id``), а не колонка таблицы
            # ``llm_scenarios``: схема БД не мигрирует (SCHEMA_VERSION не меняется), а
            # ``ScenarioBinding`` объявлен ``extra="forbid"``. Уровень остаётся видимым
            # там, где он действует: ``execution_contract_digest``, ``echo`` конверта
            # ``eval_cache`` (``build_eval_key``) и ``logs/llm_calls.jsonl``
            # (``usage.reasoning``). Манифест его не выдумывает: расхождение по этой оси
            # печатается как «предел диагностики» (``_contract_field_diagnostics``).
            ScenarioBinding(
                run_id=str(getattr(plan, "run_id", "")),
                **{
                    key: value
                    for key, value in dict(row).items()
                    if key != "reasoning_effort"
                },
            )
            for row in (getattr(axes, "llm_scenarios", ()) or ())
        ]
        if axes is not None
        else list(
            _first_present(
                getattr(inputs, "llm_scenarios", None),
                getattr(plan, "llm_scenarios", None),
                getattr(run, "llm_scenarios", None),
            )
            or []
        )
    )

    partial_score_raw = _first_present(
        getattr(run, "partial_score", None),
        getattr(inputs, "partial_score", None),
    )

    # §9/A3 (v2.1): провенанс настроек и порог неразличимости — по цепочке
    # аргумент → run → inputs → plan; без пересчёта «по предвидению» (A3).
    resolved_provenance = _first_present(
        settings_provenance,
        getattr(run, "settings_provenance", None),
        getattr(inputs, "settings_provenance", None),
        getattr(plan, "settings_provenance", None),
    )
    resolved_ranking_epsilon = _first_present(
        ranking_epsilon,
        getattr(run, "ranking_epsilon", None),
        getattr(inputs, "ranking_epsilon", None),
        getattr(plan, "ranking_epsilon", None),
    )
    # §4.4/Phase 2: провенанс краула — типизированный, информационный (вне diff-границ §13).
    # Принимаем готовую модель либо mapping; ``None`` → пустой ``CrawlProvenance()``.
    if crawl_provenance is None:
        resolved_crawl_provenance = CrawlProvenance()
    elif isinstance(crawl_provenance, CrawlProvenance):
        resolved_crawl_provenance = crawl_provenance
    else:
        resolved_crawl_provenance = CrawlProvenance.model_validate(dict(crawl_provenance))

    # §13 (Backlog B): операционные оси краула — стратегия и диапазон задержек (commodity-слой,
    # A2). Заполняются из ``run.crawl`` (если секция задана); при отсутствии секции — ``None``
    # (прежние манифесты не меняются). Их смена — граница класса ``execution_change``.
    crawl_cfg = getattr(run, "crawl", None)
    crawl_strategy: str | None = None
    crawl_rate_limit: str | None = None
    crawl_max_workers: int | None = None
    crawl_extract_workers: int | None = None
    crawl_extract_max_tasks_per_child: int | None = None
    crawl_max_response_bytes: int | None = None
    crawl_max_depth: int | None = None
    crawl_max_documents: int | None = None
    crawl_exclude_paths: list[str] = []
    crawl_exclude_documents: list[str] = []
    crawl_document_level: str | None = None
    if crawl_cfg is not None:
        strategy_value = getattr(crawl_cfg, "strategy", None)
        crawl_strategy = (
            None
            if strategy_value is None
            else str(getattr(strategy_value, "value", strategy_value))
        )
        crawl_rate_limit = canonical_rate_limit(getattr(crawl_cfg, "rate_limit", None))
        crawl_max_workers = getattr(crawl_cfg, "max_workers", None)
        crawl_extract_workers = getattr(crawl_cfg, "extract_workers", None)
        crawl_extract_max_tasks_per_child = getattr(
            crawl_cfg, "extract_max_tasks_per_child", None
        )
        crawl_max_response_bytes = getattr(crawl_cfg, "max_response_bytes", None)
        crawl_max_depth = getattr(crawl_cfg, "max_depth", None)
        crawl_max_documents = getattr(crawl_cfg, "max_documents", None)
        crawl_exclude_paths = list(getattr(crawl_cfg, "exclude_paths", None) or [])
        crawl_exclude_documents = list(
            getattr(crawl_cfg, "exclude_documents", None) or []
        )
        crawl_document_level = _document_level_value(
            getattr(crawl_cfg, "document_level", None)
        )
    # P5: операторский уровень (``--document-level``) — самая сильная ступень цепочки: он
    # применён к разрешённой конфигурации и уже вошёл в ``crawl_config_digest``, поэтому
    # манифест обязан называть именно его (иначе манифест: ``full``, дайджест: ``text``).
    if operator_document_level is not None:
        crawl_document_level = _document_level_value(operator_document_level)
    # P5: если уровень не задан ни оператором, ни run-слоем — фиксируем **эффективный
    # дефолт** app-слоя (``CrawlConfig.document_level``, дефолт ``full``), чтобы манифест не
    # молчал об оси. ``crawl_app`` может быть не передан (модульные теста/исторические
    # вызовы) — тогда ``None`` (значение не выдумываем: A3).
    if crawl_document_level is None:
        from core.config.deploy import CrawlConfig

        if isinstance(crawl_app, CrawlConfig):
            crawl_document_level = crawl_app.document_level.value

    # N6/§13: дайджест **разрешённой** конфигурации контроля краула (app < run < per-site).
    # Декларированные оси (``axes``) — тот же резолвер, что у контракта: composition root
    # разрешает их один раз (:func:`core.cli.execution_axes.resolve_declared_axes`), здесь
    # второй формулы нет. ``axes=None`` — legacy/модульный вызов (значения из run/app).
    if axes is not None:
        crawl_strategy = getattr(axes, "crawl_strategy", crawl_strategy)
        crawl_rate_limit = getattr(axes, "crawl_rate_limit", crawl_rate_limit)
    crawl_config_digest = (
        getattr(axes, "crawl_config_digest", None) if axes is not None else None
    ) or crawl_control_digest(
        run_crawl=crawl_cfg,
        app=crawl_app,
        profiles=domain_profiles,
    )

    extra: dict[str, Any] = {}
    manifest_fields = getattr(RunManifest, "model_fields", {})
    # §13: деплой-провенанс — из декларированных осей (тот же резолвер, что у контракта);
    # ``axes=None`` — прежняя цепочка аргумент → inputs → run (модульные тесты/legacy).
    if "deploy_profile" in manifest_fields:
        extra["deploy_profile"] = _first_present(
            getattr(axes, "deploy_profile", None) if axes is not None else None,
            deploy_profile,
            getattr(inputs, "deploy_profile", None),
            getattr(run, "deploy_profile", None),
        )
    if "edge_schema_version" in manifest_fields:
        extra["edge_schema_version"] = _first_present(
            getattr(axes, "edge_schema_version", None) if axes is not None else None,
            edge_schema_version,
            getattr(inputs, "edge_schema_version", None),
            getattr(run, "edge_schema_version", None),
        )
    edge_caps = edge_capabilities if edge_capabilities is not None else getattr(
        inputs, "edge_capabilities", None
    )
    extra["edge_capabilities"] = dict(edge_caps or {})
    # A1/§6: профиль публикуемости — **блок манифеста**, а не пересчёт читателя. Поле
    # добавляется только если схема ``RunManifest`` его объявляет (``_BASE`` —
    # ``extra="forbid"``): без поля в схеме манифест об этом молчит, а не падает.
    # С появлением поля проводка уже готова — значение то же, что в бандле.
    if "publishability" in manifest_fields and bundle is not None:
        profile = getattr(bundle, "publishability", None)
        if profile is not None:
            extra["publishability"] = asdict(profile)

    # План 19 §P4: провенанс политики краула. ``crawl_policy`` — тот же объект, что дал
    # ``crawl_policy_digest`` для ``run_id`` (второй формулы нет); ``policy_provenance``
    # несёт **наблюдённые** факты исполнения (снятие/пропуски), собранные фазой отчёта из
    # durable-источников. Идентичность (digest/refresh/frozen) и провенанс (unmatched/
    # счётчики/оси) не смешиваются: в ``run_id`` входит только первое.
    policy_block = dict(policy_provenance or {})
    if crawl_policy is not None or refresh_policy is not None:
        resolved_policy = crawl_policy_provenance(
            refresh_policy=refresh_policy,
            frozen_source=frozen_source,
            crawl_policy=crawl_policy,
            captured_at=policy_block.get("captured_at"),
            policy_skips=policy_block.get("policy_skips") or (),
            policy_zero_reasons=policy_block.get("policy_zero_reasons") or {},
        )
    elif policy_block:
        resolved_policy = policy_block
    else:
        resolved_policy = {}

    # План 20 §5: снимок принимается моделью или словарём (durable-носитель —
    # ``runs.corpus_snapshot_json``), но на манифест уходит **моделью**: форма одна.
    resolved_corpus_snapshot = (
        None
        if corpus_snapshot is None
        else corpus_snapshot
        if isinstance(corpus_snapshot, CorpusSnapshot)
        else CorpusSnapshot.model_validate(corpus_snapshot)
    )

    return RunManifest(
        run_id=str(getattr(plan, "run_id", "")),
        case_id=str(getattr(run, "case_id", "")),
        created_at=str(created),
        mode=getattr(run, "mode", None),  # type: ignore[arg-type] — обязателен в RunConfig/плане
        methodology_version=str(methodology_version),
        package_version=str(package_version),
        catalog_version=str(catalog_version),
        status_dict_version=str(status_dict_version),
        scoring_policy_version=str(scoring_policy_version),
        anchor_product_id=str(anchor_product_id),
        cohort_ref=_first_present(
            # 09 §12.2 / 16 §2: колонка/поле несут **авторскую ревизию** состава, потому
            # что именно она входит в ``cohort_digest``. Здесь стояло ``run.cohort`` —
            # имя файла когорты, то есть манифест показывал ревизию, которой в ``run_id``
            # не было, а настоящая ревизия не сохранялась нигде. ``None`` остаётся
            # ``None``: «ревизия не объявлена» — честный факт, а подстановка имени файла
            # была бы догадкой (A3).
            getattr(run, "cohort_ref", None),
            getattr(plan, "cohort_ref", None),
        ),
        # 16 §3.1: провенанс среза прогона. Явный аргумент → ``run.yaml`` → входы; ``None``
        # не подменяется на ``full``: «не записано» — честный факт, догадка — нет.
        run_scope=_first_present(
            run_scope,
            getattr(run, "run_scope", None),
            getattr(inputs, "run_scope", None),
            getattr(plan, "run_scope", None),
        ),
        product_plan=(
            dict(product_plan)
            if product_plan is not None
            else getattr(plan, "product_plan", None)
        ),
        # 16 §4.1: наблюдаемая ось устройства. ``axes`` — тот же резолвер, что у контракта
        # (``app < run``); ниже — прямой run-слой, чтобы модульные вызовы не теряли значение.
        embed_device=_first_present(
            embed_device,
            getattr(axes, "embed_device", None) if axes is not None else None,
            _run_embed_device(run),
            getattr(inputs, "embed_device", None),
        ),
        # 09 §12.3/§13: предшественник и класс изменения — провенанс сравнения. Приходят
        # явным аргументом из строки lineage прогона; ``None`` не подменяется значением
        # «нет истории» — это разные факты (A3).
        previous_run_id=previous_run_id,
        change_class=change_class,
        # 09 §2.4/§6.2: цикл прогона — тот же материал, что вошёл в ``run_id``. Источник
        # тот же (строка lineage); ``None`` не подменяется ``snapshot_date``: эффективный
        # цикл мог быть задан ``--cycle-id`` и отличаться от даты снимка.
        cycle_id=cycle_id,
        # 09 §12.5: дайджесты когорты и seed-набора — **слагаемые** ``run_id`` и поля
        # контракта. Приходят готовыми (тот же дайджест, из которого composition root
        # вывел ``run_id``): здесь они не пересчитываются, иначе манифест описывал бы
        # второй расчёт, а не объявленный вход. ``None`` — не записано (legacy/модульный
        # вызов): ``hound diff`` тогда честно называет ось недоступной, а не выдумывает.
        cohort_digest=cohort_digest,
        seeds_digest=seeds_digest,
        # План 19 §P4: ось политики краула — проекция того же объекта, что дал ``run_id``;
        # остальные поля — провенанс исполнения (в идентичность не входят).
        crawl_policy_digest=resolved_policy.get("digest"),
        refresh_policy=resolved_policy.get("refresh_policy"),
        frozen_source=resolved_policy.get("frozen_source"),
        crawl_policy_unmatched=list(resolved_policy.get("unmatched") or []),
        policy_skips=list(resolved_policy.get("policy_skips") or []),
        policy_zero_reasons=dict(
            resolved_policy.get("policy_zero_reasons") or {}
        ),
        captured_at=resolved_policy.get("captured_at"),
        crawl_policy_roots=list(resolved_policy.get("roots") or []),
        # План 20 §5: снимок корпуса — провенанс прогона, в ``run_id``/контракт не входит.
        # Приходит готовым (из ``runs.corpus_snapshot_json``, куда его записала фаза
        # ``corpus``): здесь содержание **не** пересчитывается, иначе у одной величины было
        # бы два расчёта. ``None`` — снимок не записан (строка прошлой схемы/модульный
        # вызов), а не «снимка нет».
        corpus_snapshot=resolved_corpus_snapshot,
        focus_modules=focus_modules,
        index_build_id=resolved_index_build_id,
        embed_model_id=_first_present(
            getattr(run, "embed_model_id", None),
            getattr(inputs, "embed_model_id", None),
            (observed_versions or {}).get("embed_model_id"),
        ),
        chunker_version=(
            getattr(axes, "chunker_version", None)
            if axes is not None
            else _first_present(
                getattr(run, "chunker_version", None),
                getattr(inputs, "chunker_version", None),
                (observed_versions or {}).get("chunker_version"),
            )
        ),
        extractor_version=_first_present(
            getattr(run, "extractor_version", None),
            getattr(inputs, "extractor_version", None),
            (observed_versions or {}).get("extractor_version"),
        ),
        lexical_engine_version=_first_present(
            getattr(run, "lexical_engine_version", None),
            getattr(inputs, "lexical_engine_version", None),
            (observed_versions or {}).get("lexical_engine_version"),
        ),
        vector_store=_first_present(
            getattr(run, "vector_store", None),
            getattr(inputs, "vector_store", None),
            (observed_versions or {}).get("vector_store"),
        ),
        retrieval_profile=retrieval_profile,
        retrieval_cutoff_K=retrieval_cutoff_K,
        retrieval_params_json=retrieval_params_json,
        search_effort_budget_json=search_effort_budget_json,
        requested_ranking=_as_ranking_policy(requested),
        computed_ranking=_as_ranking_policy(computed),
        llm_scenarios=llm_scenarios,
        capability_policy=_first_present(
            getattr(run, "capability_policy", None),
            getattr(inputs, "capability_policy", None),
        )
        or "resolved-only-v1",
        unknown_policy=_first_present(
            getattr(run, "unknown_policy", None),
            getattr(inputs, "unknown_policy", None),
        )
        or "excluded",
        partial_score=0.5 if partial_score_raw is None else float(partial_score_raw),
        ranking_epsilon=(
            0.0 if resolved_ranking_epsilon is None else float(resolved_ranking_epsilon)
        ),
        settings_provenance=dict(resolved_provenance or {}),
        crawl_provenance=resolved_crawl_provenance,
        crawl_strategy=crawl_strategy,
        crawl_rate_limit=crawl_rate_limit,
        crawl_max_workers=crawl_max_workers,
        crawl_extract_workers=crawl_extract_workers,
        crawl_extract_max_tasks_per_child=crawl_extract_max_tasks_per_child,
        crawl_max_response_bytes=crawl_max_response_bytes,
        crawl_document_level=crawl_document_level,
        crawl_max_depth=crawl_max_depth,
        crawl_max_documents=crawl_max_documents,
        crawl_exclude_paths=crawl_exclude_paths,
        crawl_exclude_documents=crawl_exclude_documents,
        crawl_config_digest=crawl_config_digest,
        # E2/§13: дайджест package-scoped спецификаций реестров (``None`` — нет реестров;
        # поле аддитивно, нетронутые манифесты без реестров не меняются по значению).
        registry_spec_digest=(
            getattr(axes, "registry_spec_digest", None)
            if axes is not None
            else registry_spec_digest
        ),
        # 09 §12.5: готовый отпечаток условий измерения (не пересчитывается здесь).
        execution_contract_digest=execution_contract_digest,
        # 13-manual-artifacts §3.2/§3.4: отпечаток **объявленных** артефактов (поле
        # контракта — его называет ``hound diff``) и **наблюдённая** доля ручного
        # (публикуется числом, потолка нет). Разные классы: первый — условие измерения
        # (входит в ``run_id``), второй — факт прогона и в контракт **не** входит.
        manual_artifacts_digest=(
            getattr(axes, "manual_artifacts_digest", None)
            if manual_artifacts_digest is None and axes is not None
            else manual_artifacts_digest
        ),
        manual_artifacts=dict(manual_artifacts or {}),
        # I2/I2c: частичное извлечение — наблюдение прогона (документы/страницы/символы
        # по продуктам). В ``EXECUTION_CONTRACT_FIELDS`` не входит: это факт о том, что
        # прибор прочитал, а не смена условий измерения.
        partial_docs=dict(partial_docs or {}),
        snapshot_date=snapshot_date,
        # I2b/I2c: подписи, которыми прогон авторизовал ❌ (product → actor). В
        # ``EXECUTION_CONTRACT_FIELDS`` не входят: это авторизация, не измерение.
        scope_attestations=dict(scope_attestations or {}),
        **extra,
    )


def manifest_payload(manifest: RunManifest) -> dict[str, Any]:
    """Публичное представление манифеста — **один** сборщик для файла и ``--json``.

    ``run_manifest.yaml`` (Ш9) и блок ``manifest`` в payload ``hound report --json``
    обязаны быть одним и тем же: второй сборщик разошёлся бы с первым, и потребитель
    читал бы не то, что лежит в артефакте прогона (та же дисциплина, что у ``run_product``
    против ``results[].notes``, 16 §3.2). Поэтому путь один: :func:`write_run_manifest`
    пишет YAML из этого словаря, а команда отчёта отдаёт его в payload как есть.

    ``mode="json"`` — потому что в манифесте есть enum'ы и вложенные модели: потребитель
    payload'а не должен разбирать ``StrEnum``/``Path``.
    """
    return manifest.model_dump(mode="json")


def write_run_manifest(manifest: RunManifest, out_dir: str | Path) -> Path:
    """Пишет ``run_manifest.yaml`` (R7, §9) в ``out_dir``; YAML, ``sort_keys``.

    Использует доменный :class:`RunManifest` (уже агрегирует версии, ``llm_scenarios``,
    индексную сборку, ретривал-профиль+K, бюджет, политику расчёта). Детерминирован:
    ``sort_keys=True`` + ``allow_unicode=True``. Тело берётся у :func:`manifest_payload` —
    тот же словарь уходит в ``hound report --json``, поэтому артефакт и payload не могут
    разойтись.
    """
    directory = Path(out_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "run_manifest.yaml"
    payload = manifest_payload(manifest)
    text = yaml.safe_dump(
        payload, sort_keys=True, allow_unicode=True, default_flow_style=False
    )
    path.write_text(text, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# battle_card
# ---------------------------------------------------------------------------


def _battle_card_header(
    bundle: ScoreBundle,
    fallback: bool,
    insufficient: bool,
    *,
    crawl_provenance: CrawlProvenance | None = None,
) -> list[tuple[str, Any]]:
    """Шапка battle card: метрики + аннотации (приглушение/fallback) + провенанс краула."""
    reasons = "; ".join(bundle.comparability.reasons)
    rows: list[tuple[str, Any]] = [
        ("capability", round(bundle.capability.value, 6)),
        ("capability_n_used", bundle.capability.n_used),
        ("coverage", round(bundle.coverage.value or 0.0, 6)),
        ("coverage_defined", bundle.coverage.defined),
        ("coverage_n_used", bundle.coverage.n_used),
        ("coverage_denominator", bundle.coverage.denominator),
        ("coverage_universe", bundle.coverage.universe),
        ("failure_rate", round(bundle.failure_rate, 6)),
        ("comparability_level", bundle.comparability.level),
        ("comparability_min_coverage", round(bundle.comparability.min_coverage, 6)),
        ("comparability_cross_cycle", bundle.comparability.cross_cycle),
        ("comparability_reasons", reasons),
        ("requested_ranking", bundle.requested_ranking),
        ("computed_ranking", bundle.computed_ranking),
        ("coverage_insufficient", insufficient),
    ]
    # A1/§6: баннер публикуемости — **класс и причина**, а не только «coverage
    # INSUFFICIENT». Строки добавляются, если бандл несёт профиль (синтетические бандлы
    # тестов его не несут — класс тогда не выдумывается).
    profile = getattr(bundle, "publishability", None)
    if profile is not None:
        rows.extend(
            [
                ("publishability_class", profile.klass),
                ("publishability_banner", profile.banner),
                ("publishability_unknown_share", round(profile.unknown_share, 6)),
                ("publishability_unknown_share_threshold", profile.unknown_share_threshold),
                ("publishability_coverage_floor", profile.coverage_floor),
                ("publishability_dominant_cause", profile.dominant_cause or ""),
                ("publishability_measured_products", profile.measured_products),
                ("publishability_weak_denial_share", round(profile.weak_denial_share, 6)),
                ("publishability_caveats", "; ".join(profile.caveats)),
                # §10: объявленное исключение стенда RnD — рядом с классом, а не в
                # примечании. Класс от него не меняется (условие стенда ≠ качество
                # измерения), но «карта со стенда» обязана быть видна в шапке.
                ("publishability_rnd_endpoint", profile.rnd_endpoint),
                ("publishability_llm_hosts", ", ".join(profile.llm_hosts)),
            ]
        )
        for notice in profile.llm_host_notices:
            if notice.get("message"):
                rows.append(("note", str(notice["message"])))
        for cause, bucket in sorted(profile.causes.items()):
            rows.append((f"publishability_unknown_{cause}", round(bucket.share, 6)))
        for skipped, bucket in sorted(profile.excluded.items()):
            rows.append((f"publishability_excluded_{skipped}", bucket.cells))
    # §4.4/Phase 2/2.1: провенанс краула — движки (информационно) и durable-флаг
    # неоднородности. ``crawl_engines`` публикуется безусловно (стабильная схема шапки);
    # ``crawl_engines_heterogeneous`` — только при флаге. Это **наблюдение**, а не
    # демотирование: Comparability/§6 не затрагиваются (I11-only).
    engines = sorted({str(e) for e in (getattr(crawl_provenance, "engines", None) or [])})
    rows.append(("crawl_engines", ", ".join(engines)))
    if getattr(crawl_provenance, "heterogeneous", False):
        rows.append(("crawl_engines_heterogeneous", True))
    if insufficient:
        rows.append(("note", INSUFFICIENT_COVERAGE_NOTE))
    if fallback:
        rows.append(("ranking_note", FALLBACK_ORDER_LABEL))
    return rows


def _battle_card_sheets(
    bundle: ScoreBundle,
    cells: list[Any],
    catalog: Mapping[str, CatalogRow],
    products: list[str],
    params: list[str],
    *,
    fallback: bool,
    insufficient: bool,
    crawl_provenance: CrawlProvenance | None = None,
) -> list[tuple[str, list[list[Any]]]]:
    """Собирает листы battle card (шапка/матрица/сателлиты/модули/sensitivity)."""
    by_key = {(str(c.product_id), str(c.param_id)): c for c in cells}

    header_rows: list[list[Any]] = [["field", "value"]]
    header_rows.extend(
        [name, value] for name, value in _battle_card_header(
            bundle, fallback, insufficient, crawl_provenance=crawl_provenance
        )
    )

    matrix_rows: list[list[Any]] = [["product", *params]]
    for product in products:
        matrix_rows.append(
            [product, *[_matrix_cell(by_key.get((product, param))) for param in params]]
        )

    satellite_rows: list[list[Any]] = [
        ["product", "param", "role", "state", "origin", "relationship", "evidence_state"]
    ]
    for cell in cells:
        for satellite in getattr(cell, "satellites", None) or []:
            satellite_rows.append(
                [
                    str(cell.product_id),
                    str(cell.param_id),
                    _attr(satellite, "role"),
                    _attr(satellite, "state"),
                    _attr(satellite, "origin"),
                    _attr(satellite, "relationship"),
                    _attr(satellite, "evidence_state"),
                ]
            )

    module_rows: list[list[Any]] = [
        ["module", "score", "coverage", "weight", "applicable_weight"]
    ]
    module_rows.extend(
        [m.module, round(m.score, 6), round(m.coverage, 6), m.weight, round(m.applicable_weight, 6)]
        for m in bundle.modules
    )

    sensitivity_rows: list[list[Any]] = [["field", "value"]]
    sensitivity_rows.extend(
        [
            ["stable", bundle.sensitivity.stable],
            ["requested_ranking", bundle.sensitivity.requested_ranking],
            ["computed_ranking", bundle.sensitivity.computed_ranking],
            ["unstable_pairs", "; ".join(f"{a}|{b}" for a, b in bundle.sensitivity.unstable_pairs)],
        ]
    )
    for axis, payload in sorted(bundle.sensitivity.per_axis.items()):
        sensitivity_rows.append([f"axis:{axis}", "; ".join(str(v) for v in payload.get("values", []))])
        sensitivity_rows.append([f"axis:{axis}:stable", payload.get("stable")])

    return [
        ("header", header_rows),
        ("matrix", matrix_rows),
        ("satellites", satellite_rows),
        ("modules", module_rows),
        ("sensitivity", sensitivity_rows),
    ]


def _attr(obj: Any, name: str) -> Any:
    """Значение атрибута/slug для сателлита (строкой), безопасно для dict/объекта."""
    value = obj.get(name) if isinstance(obj, Mapping) else getattr(obj, name, None)
    if value is None:
        return ""
    return value.value if hasattr(value, "value") and not isinstance(value, str) else str(value)


def _battle_card_records(sheets: list[tuple[str, list[list[Any]]]]) -> list[dict[str, Any]]:
    """Разворачивает листы в длинный CSV-формат (fallback без openpyxl)."""
    records: list[dict[str, Any]] = []
    for section, rows in sheets:
        if section == "matrix" and rows:
            header = rows[0]
            for row in rows[1:]:
                product = row[0]
                for param, value in zip(header[1:], row[1:], strict=False):
                    records.append(
                        {"section": section, "product": product, "param": param, "field": "cell", "value": value}
                    )
        elif section in {"header", "sensitivity"} and rows and rows[0] == ["field", "value"]:
            for field, value in rows[1:]:
                records.append({"section": section, "product": "", "param": "", "field": field, "value": value})
        else:
            header = rows[0] if rows else []
            for row in rows[1:]:
                for field_name, value in zip(header, row, strict=False):
                    records.append(
                        {"section": section, "product": "", "param": "", "field": field_name, "value": value}
                    )
    return records


def render_battle_card(
    bundle: ScoreBundle,
    cells: Iterable[Any],
    catalog: Mapping[str, CatalogRow] | Iterable[CatalogRow],
    out_path: str | Path,
    *,
    crawl_provenance: CrawlProvenance | None = None,
) -> Path:
    """Пишет ``battle_card`` (XLSX; CSV-fallback) с G4/I11-гейтом (§9, 02 §E.2).

    :raises PublishBlockedError: если Coverage отсутствует полностью (I11).
    :returns: путь записанного артефакта (``.xlsx`` либо ``.csv`` при отсутствии
        ``openpyxl``).

    При недостаточном Coverage артефакт **приглушается** и помечается
    :data:`INSUFFICIENT_COVERAGE_NOTE`; при демотировании продукты упорядочиваются по
    Coverage и добавляется :data:`FALLBACK_ORDER_LABEL` (K6/A3).

    :param crawl_provenance: опциональный провенанс краула (§4.4/Phase 2); в шапку
        добавляются ``crawl_engines`` и (при >1 движке) ``crawl_engines_heterogeneous`` —
        информационно, без влияния на Comparability/§6.
    """
    if _coverage_absent(bundle):
        raise PublishBlockedError(
            "I11/G4: battle_card не публикуется без Coverage "
            f"(n_used={bundle.coverage.n_used}, denominator={bundle.coverage.denominator})"
        )

    catalog_map = _catalog_map(catalog)
    cell_list = _cell_list(cells)
    products, fallback = _ordered_products(cell_list, bundle, catalog_map)
    params = _param_ids(cell_list)
    insufficient = _coverage_insufficient(bundle)

    sheets = _battle_card_sheets(
        bundle,
        cell_list,
        catalog_map,
        products,
        params,
        fallback=fallback,
        insufficient=insufficient,
        crawl_provenance=crawl_provenance,
    )

    out = Path(out_path)
    if _load_openpyxl() is not None:
        return _write_xlsx(out, sheets)
    return _write_csv(
        _tabular_path(out, xlsx=False),
        ["section", "product", "param", "field", "value"],
        _battle_card_records(sheets),
    )


# ---------------------------------------------------------------------------
# nda_questions
# ---------------------------------------------------------------------------


def render_nda_questions(
    cells: Iterable[Any],
    out_path: str | Path,
    *,
    exhaustions: Mapping[tuple[str, str], Any] | None = None,
    crawl_truncation_anomalies: Iterable[Mapping[str, Any]] | None = None,
    corpus_coverage_anomalies: Iterable[Mapping[str, Any]] | None = None,
    golden_exempt_anomalies: Iterable[Mapping[str, Any]] | None = None,
    absence_undated_anomalies: Iterable[Mapping[str, Any]] | None = None,
) -> Path:
    """Пишет ``nda_questions.csv`` — все ``❓`` (продукт, параметр, исчерпанные контуры).

    ``exhaustions`` — опциональная карта ``(product_id, param_id) → ProtocolExhaustion``
    (``contours`` заполняются, если доступны; §5/S13). Фильтр строго ``status == ❓``.

    §5/I3 (v2.1): для каждого ``❓`` дополнительно фиксируется **аномалия** протокола:
    ``protocol_status`` ∈ {``complete``, ``incomplete``, ``unknown``} и ``i3_anomaly``
    (сообщение :func:`core.domain.invariants.check_unknown_exhaustion`). Это наблюдение
    о полноте протокола — **статус ячейки при этом не меняется**.

    **B3/D1-G.** ``crawl_truncation_anomalies`` — аномалии «``exhaustive``-absence при
    усечённом крауле» (:func:`collect_crawl_truncation_anomalies`); они **дописываются**
    строками в тот же операторский CSV (колонка ``anomaly_kind="crawl_truncation"``,
    ``status="crawl_truncation"``), чтобы человек их увидел, а не только машина. Как и I3,
    это **наблюдение**, не запуск и не жёсткая блокировка.

    **Ф0.** ``corpus_coverage_anomalies`` — неполное покрытие корпуса продукта
    (:func:`collect_corpus_coverage_anomalies`); строки ``anomaly_kind="corpus_incomplete"``
    несут blocked counts и флаг ``complete`` рядом с truncation-аномалией. Статус ячейки
    не меняется (I12): ``pending``-кандидат — видимый исход, CSV — аудит-след.

    **§11.6.** ``golden_exempt_anomalies`` — продукты, для которых эталон **объявленно**
    отсутствует (:func:`collect_golden_exempt_anomalies`); строки
    ``anomaly_kind="golden_exempt_product"`` называют продукт и причину из объявления.
    Это наблюдение об инструменте проверки, а не о ячейке: статусы не меняются.

    **I2d (план 20 §11.2).** ``absence_undated_anomalies`` — ячейки, у которых ``❌`` снят
    инвариантом «у ``❌`` есть датированный носитель» (:func:`collect_absence_undated_anomalies`);
    строки ``anomaly_kind="absence_undated"`` называют продукт, параметр и **чего не
    хватило** (документ без даты наблюдения / корень без ``complete``-снятия). Текст
    берётся из причины ячейки как есть: отчёт не переписывает её в «устарело». Как и
    остальные, это **наблюдение**: ячейка уже ``❓``.
    """
    rows: list[dict[str, Any]] = []
    for cell in _cell_list(cells):
        if getattr(cell, "status", None) is not Status.UNKNOWN:
            continue
        key = (str(cell.product_id), str(cell.param_id))
        exhaustion = exhaustions.get(key) if exhaustions else None
        contours = ",".join(_contours(exhaustion)) if exhaustion is not None else ""
        rows.append(
            {
                "product_id": cell.product_id,
                "param_id": cell.param_id,
                "status": Status.UNKNOWN.value,
                "exhausted_contours": contours,
                "protocol_status": _i3_protocol_status(cell, exhaustion),
                "i3_anomaly": _i3_violations(cell, exhaustion),
                "anomaly_kind": "i3_protocol",
            }
        )
    for anomaly in crawl_truncation_anomalies or ():
        rows.append(
            {
                "product_id": anomaly.get("product_id", ""),
                "param_id": anomaly.get("param_id", ""),
                "status": "crawl_truncation",
                "exhausted_contours": "",
                "protocol_status": "",
                "i3_anomaly": anomaly.get("reason", ""),
                "anomaly_kind": "crawl_truncation",
            }
        )
    for anomaly in corpus_coverage_anomalies or ():
        rows.append(
            {
                "product_id": anomaly.get("product_id", ""),
                "param_id": anomaly.get("param_id", ""),
                "status": "corpus_incomplete",
                "exhausted_contours": "",
                "protocol_status": "",
                "i3_anomaly": anomaly.get("reason", ""),
                "anomaly_kind": "corpus_incomplete",
            }
        )
    for anomaly in golden_exempt_anomalies or ():
        rows.append(
            {
                "product_id": anomaly.get("product_id", ""),
                "param_id": anomaly.get("param_id", ""),
                "status": "golden_exempt_product",
                "exhausted_contours": "",
                "protocol_status": "",
                "i3_anomaly": anomaly.get("reason", ""),
                "anomaly_kind": "golden_exempt_product",
            }
        )
    for anomaly in absence_undated_anomalies or ():
        rows.append(
            {
                "product_id": anomaly.get("product_id", ""),
                "param_id": anomaly.get("param_id", ""),
                "status": "absence_undated",
                "exhausted_contours": "",
                "protocol_status": "",
                "i3_anomaly": anomaly.get("reason", ""),
                "anomaly_kind": "absence_undated",
            }
        )
    return _write_csv(
        Path(out_path),
        [
            "product_id",
            "param_id",
            "status",
            "exhausted_contours",
            "protocol_status",
            "i3_anomaly",
            "anomaly_kind",
        ],
        rows,
    )


def _i3_violations(cell: Any, exhaustion: Any) -> str:
    """Первое нарушение :func:`check_unknown_exhaustion` (или пустая строка).

    Проверка применима к ``None`` (нет протокола) и к ``ProtocolExhaustion``. Иные
    носители (например, «сырой» dict) не классифицируются — во избежание ложной аномалии.
    """
    if exhaustion is not None and not isinstance(exhaustion, ProtocolExhaustion):
        return ""
    violations = check_unknown_exhaustion(cell, exhaustion)
    return violations[0].message if violations else ""


def _i3_protocol_status(cell: Any, exhaustion: Any) -> str:
    """Статус протокола исчерпания ``❓``: ``complete`` / ``incomplete`` / ``unknown``."""
    if exhaustion is not None and not isinstance(exhaustion, ProtocolExhaustion):
        return "unknown"
    return "incomplete" if _i3_violations(cell, exhaustion) else "complete"


def _contours(exhaustion: Any) -> list[str]:
    """Контуры исчерпания из ``ProtocolExhaustion``/dict, строкой."""
    if isinstance(exhaustion, Mapping):
        values = exhaustion.get("contours") or []
    else:
        values = getattr(exhaustion, "contours", None) or []
    return [str(v) for v in values]


def collect_i3_anomalies(
    cells: Iterable[Any],
    exhaustions: Mapping[tuple[str, str], Any] | None = None,
) -> list[dict[str, Any]]:
    """Собирает аномалии I3 (§5): strict-``❓`` без полного протокола исчерпания.

    Возвращает детерминированный (по ``product_id, param_id``) список записей
    ``{"product_id", "param_id", "reason"}``. Это **наблюдения**, не изменение статуса
    ячейки: ``❓`` остаётся ``❓`` (I12 — ❓ никогда не трактуется как ❌).

    I3 — **дизъюнкция**: полный протокол **или** ``reviewed_by ≠ null``. Человеческая
    подпись уже удовлетворяет I3, поэтому её ``❓`` не аномалия (протокол при этом
    остаётся фактом и виден в ``nda_questions.csv``).
    """
    exhaustions = exhaustions or {}
    anomalies: list[dict[str, Any]] = []
    for cell in _cell_list(cells):
        if getattr(cell, "status", None) is not Status.UNKNOWN:
            continue
        if getattr(cell, "reviewed_by", None):
            # I3-OR (§2.3): reviewed_by ≠ null — человеческая ветка исчерпания.
            continue
        key = (str(cell.product_id), str(cell.param_id))
        reason = _i3_violations(cell, exhaustions.get(key))
        if reason:
            anomalies.append(
                {
                    "product_id": str(cell.product_id),
                    "param_id": str(cell.param_id),
                    "reason": reason,
                }
            )
    return anomalies


def collect_absence_undated_anomalies(
    cells: Iterable[Any],
) -> list[dict[str, Any]]:
    """I2d (план 20 §11.2): ``❌`` снят, потому что у отрицания нет датированного носителя.

    Источник — **запись ячейки**, а не пересчёт инварианта: машинный токен
    ``absence_undated`` пишет конвейер в ``evidence_scope.reason`` и в
    ``cells.validation_notes``, и отчёт читает ровно то, что записано. Второе вычисление
    здесь разошлось бы с первым (та же дисциплина, что у ``run_product`` против
    ``results[].notes``): отчёт обязан объяснять опубликованную карту, а не строить свою.

    Запись называет, **чего не хватило**: текст берётся из причины ячейки, где конвейер уже
    указал ``{product_id}/{param_id}`` и виновника — документ без даты наблюдения или
    корень без ``complete``-снятия. Переписывать это в «устарело» запрещено: причина —
    факт о носителе, а не оценка возраста (порогов не вводим, A3). Аномалия — наблюдение
    (I12): статус ячейки не меняется, она и так ``❓``.
    """
    anomalies: list[dict[str, Any]] = []
    for cell in _cell_list(cells):
        scope = getattr(cell, "evidence_scope", None)
        reason = str(getattr(scope, "reason", None) or "")
        notes = [
            str(note) for note in (getattr(cell, "validation_notes", None) or [])
        ]
        # Токен стоит **первым** в причине (``f"{ABSENCE_UNDATED_REASON}: …"``): сравнение
        # по началу строки не ловит чужие причины, где он лишь упомянут.
        starts = reason.startswith(ABSENCE_UNDATED_REASON)
        if not starts and not any(
            note.startswith(ABSENCE_UNDATED_REASON) for note in notes
        ):
            continue
        detail = (
            reason
            if starts
            else next(
                note
                for note in notes
                if note.startswith(ABSENCE_UNDATED_REASON)
            )
        )
        anomalies.append(
            {
                "product_id": str(cell.product_id),
                "param_id": str(cell.param_id),
                "reason": detail,
            }
        )
    return sorted(anomalies, key=lambda row: (row["product_id"], row["param_id"]))


def collect_run_products(
    store: Any | None,
    run_id: str,
) -> list[dict[str, Any]]:
    """Строки ``run_product`` прогона (16 §3.2) — статус продукта **в прогоне**.

    Носитель — таблица ``run_product``; ``results[].notes`` прогона остаётся
    операторским текстом. Два носителя одной величины разошлись бы, если бы отчёт
    собирал статус заново, поэтому здесь **только чтение**: ``iter_run_products``.

    ``store`` без метода (синтетические бандлы тестов, legacy-прогон) — пустой список:
    «строк нет» не выдаётся за «все продукты ``complete``». Сбой чтения тоже пустой
    список, но прогон от этого не падает: таблица — провенанс, а не измерение.
    """
    reader = getattr(store, "iter_run_products", None)
    if not callable(reader):
        return []
    try:
        return list(reader(run_id))
    except Exception:  # noqa: BLE001 — провенанс не роняет отчёт
        return []


#: Колонки ``outputs/run_product.csv`` — порядок совпадает с записью ``run_product``.
_RUN_PRODUCT_COLUMNS: tuple[str, ...] = (
    "run_id",
    "product_id",
    "mode",
    "status",
    "scope",
    "phase",
    "coverage_complete",
    "docs_total",
    "docs_reused",
    "docs_new",
    "cells_total",
    "cells_pending",
    "baseline_complete",
    "error_kind",
    "error",
    "updated_at",
)


def render_run_product(
    rows: Iterable[Mapping[str, Any]],
    out_path: str | Path,
) -> Path | None:
    """Пишет ``outputs/run_product.csv``: строки по продуктам прогона.

    ``None`` при пустом списке: файл с заголовком и без строк читался бы как «продуктов
    нет», а это другой факт. Пустая таблица ``run_product`` — «статус не записан».
    """
    materialized = list(rows)
    if not materialized:
        return None
    return _write_csv(
        Path(out_path),
        list(_RUN_PRODUCT_COLUMNS),
        [{column: row.get(column) for column in _RUN_PRODUCT_COLUMNS} for row in materialized],
    )


def collect_crawl_truncation_anomalies(
    cells: Iterable[Any],
    truncated_products: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    """D1/G: аномалия «заявлен исчерпывающий охват, а краул продукта усечён политикой».

    Если cell заявляет **исчерпывающий охват** (устаревший скаляр ``source_scope:
    exhaustive`` или класс ``bounded``), а краул её продукта был **политико-усечён**
    (``max_documents``/``max_depth``, см.
    :func:`core.pipeline.crawl_session.finalize_session`), заявленная полнота недоказуема —
    corpus неполон по построению. Это **аномалия**, а не смена статуса и не жёсткая
    блокировка: она требует повторной аттестации человеком (паттерн I3 ``reviewed_by``).
    Возвращает детерминированный (``product_id, param_id``) список записей
    ``{"product_id", "param_id", "reason"}``.

    **Классы ``authoritative`` и ``explicit`` под правило не подпадают** (Вариант 2): они
    свидетельствуют о факте, а не о полноте охвата. Реестровая энумерация авторитетна
    независимо от того, что успел скачать краул, а прямое отрицание вендора — цитата, а не
    утверждение «больше нигде ничего нет». Держать их под правилом значило бы выдавать
    ложную аномалию на каждом реестровом ``❌`` усечённого продукта. Аномалия остаётся для
    ``bounded`` (единственный класс, чья сила именно в объявленной полноте) и для legacy-строк
    без объекта силы отрицания (прежнее правило, иначе исторический прогон потерял бы сигнал).
    """
    truncated = {str(p) for p in (truncated_products or []) if p}
    if not truncated:
        return []
    anomalies: list[dict[str, Any]] = []
    for cell in _cell_list(cells):
        pid = str(getattr(cell, "product_id", "") or "")
        if pid not in truncated:
            continue
        scope_obj = getattr(cell, "evidence_scope", None)
        if scope_obj is not None and scope_obj.denial_strength not in (
            DenialStrength.BOUNDED,
        ):
            # Свидетельство о факте (реестр/цитата) от полноты краула не зависит.
            continue
        absences = getattr(cell, "absence_evidence", None) or []
        declared = scope_obj is not None
        if declared:
            anomalies.append(
                {
                    "product_id": pid,
                    "param_id": str(getattr(cell, "param_id", "") or ""),
                    "reason": (
                        "I2b/G: класс bounded при усечённом крауле продукта "
                        "(max_documents/max_depth) — объявленная полнота справочника "
                        "недоказуема, требуется принятие риска человеком (scope_acceptance)"
                    ),
                }
            )
            continue
        for entry in absences:
            scope = (
                entry.get("source_scope")
                if isinstance(entry, Mapping)
                else getattr(entry, "source_scope", None)
            )
            if scope in (SourceScope.EXHAUSTIVE, SourceScope.EXHAUSTIVE.value):
                declared = True
                break
        if declared:
            anomalies.append(
                {
                    "product_id": pid,
                    "param_id": str(getattr(cell, "param_id", "") or ""),
                    "reason": (
                        "I2b/G: source_scope=exhaustive при усечённом крауле продукта "
                        "(max_documents/max_depth) — требуется повторная аттестация "
                        "человеком (reviewed_by)"
                    ),
                }
            )
    return sorted(anomalies, key=lambda row: (row["product_id"], row["param_id"]))


def _coverage_records(
    coverage: Mapping[str, Any] | Iterable[Any] | None,
) -> list[CorpusCoverage]:
    """Нормализует покрытие к списку ``CorpusCoverage`` (mapping/iterable/None)."""
    if coverage is None:
        return []
    values = coverage.values() if isinstance(coverage, Mapping) else coverage
    records = [value for value in values if isinstance(value, CorpusCoverage)]
    return sorted(records, key=lambda record: record.product_id)


def _coverage_anomaly(
    product_id: str, coverage: CorpusCoverage, *, param_id: str = ""
) -> dict[str, Any]:
    """Строка аномалии «корпус продукта неполон» (Ф0).

    Главные действенные факты — **блокирующие сбои** (``blocking_failures``: класс →
    счётчик) и **нечитаемые origin'ы** (``unreadable_origins``): именно они не позволяют
    публиковать ``❌``. Читатель видит ``robots_disallow=50``, а не только
    ``complete=False``. Частичные блокировки перечисляются отдельно (``partial_blocks``).
    """
    blocked = dict(sorted(coverage.blocked.items()))
    blocking = dict(coverage.blocking_failures)
    blocking_text = (
        ", ".join(f"{kind}={count}" for kind, count in blocking.items()) or "{}"
    )
    unreadable = list(coverage.unreadable_origins)
    partial = {
        origin: dict(sorted(counts.items()))
        for origin, counts in sorted(coverage.partial_blocks.items())
    }
    # Причина zero-source/аномалии провенанса (наблюдательное поле, не статус):
    # ``no_seeds``/``all_seeds_rejected``/``no_urls_entry``/``coverage_missing``.
    reason_note = f"причина: {coverage.reason}; " if coverage.reason else ""
    return {
        "product_id": product_id,
        "param_id": param_id,
        "reason": (
            "Ф0: неполное покрытие корпуса — complete=False, "
            f"блокирующие сбои: {blocking_text}, "
            f"нечитаемые origin'ы: {unreadable or '[]'}, "
            f"blocked={blocked or '{}'}, "
            f"observed/expected={coverage.observed_sources}/{coverage.expected_sources}, "
            f"truncated={coverage.truncated}; "
            f"{reason_note}"
            f"частичные блокировки (аудит): {partial or '{}'}; "
            "❌ без человеческой аттестации (scope_attested_by) не публикуется (I2c)"
        ),
        "complete": False,
        "blocked": blocked,
        "blocking_failures": blocking,
        "unreadable_origins": unreadable,
        "partial_blocks": partial,
        "expected_sources": coverage.expected_sources,
        "observed_sources": coverage.observed_sources,
        "truncated": coverage.truncated,
    }


def collect_corpus_coverage_anomalies(
    cells: Iterable[Any],
    coverage: Mapping[str, Any] | Iterable[Any] | None = None,
) -> list[dict[str, Any]]:
    """Ф0: аномалии неполного покрытия корпуса — per-product, детерминированно.

    Источник — явно переданное покрытие (``CorpusCoverage``), если оно есть. Иначе факт
    извлекается из самих ячеек: гейт ``❌`` записывает в absence-evidence машинно-читаемый
    аудит (``corpus_complete=False``/``corpus_blocked``), поэтому неполный корпус виден в
    отчёте без чтения БД. Аномалия — **наблюдение** (I12): статус ячейки не меняется,
    ``pending``-кандидат остаётся видимым исходом.
    """
    records = _coverage_records(coverage)
    known_products = {record.product_id for record in records}
    anomalies = [
        _coverage_anomaly(record.product_id, record)
        for record in records
        if not record.complete
    ]

    derived: dict[str, dict[str, Any]] = {}
    for cell in _cell_list(cells):
        product_id = str(getattr(cell, "product_id", "") or "")
        if not product_id or product_id in known_products:
            continue
        entries = list(getattr(cell, "absence_evidence", None) or [])
        audit = next(
            (
                entry
                for entry in entries
                if isinstance(entry, Mapping) and entry.get("corpus_complete") is False
            ),
            None,
        )
        if audit is None:
            continue
        blocked = dict(sorted((audit.get("corpus_blocked") or {}).items()))
        raw_blocking = audit.get("corpus_blocking_failures") or {
            kind: count
            for kind, count in blocked.items()
            if kind in _BLOCKING_FAILURE_VALUES
        }
        blocking = {
            kind: int(count) for kind, count in sorted(raw_blocking.items())
        }
        unreadable = list(audit.get("corpus_unreadable_origins") or [])
        blocking_text = (
            ", ".join(f"{kind}={count}" for kind, count in blocking.items()) or "{}"
        )
        partial = {
            str(origin): dict(sorted((counts or {}).items()))
            for origin, counts in sorted(
                (audit.get("corpus_partial_blocks") or {}).items()
            )
        }
        derived.setdefault(
            product_id,
            {
                "product_id": product_id,
                "param_id": str(getattr(cell, "param_id", "") or ""),
                "reason": (
                    "Ф0: неполное покрытие корпуса — complete=False, "
                    f"блокирующие сбои: {blocking_text}, "
                    f"нечитаемые origin'ы: {unreadable or '[]'}, "
                    f"blocked={blocked or '{}'}, "
                    f"observed/expected={audit.get('corpus_observed_sources')}/"
                    f"{audit.get('corpus_expected_sources')}, "
                    f"truncated={audit.get('corpus_truncated')}; "
                    f"частичные блокировки (аудит): {partial or '{}'}; "
                    "❌ без человеческой аттестации не публикуется (I2c)"
                ),
                "complete": False,
                "blocked": blocked,
                "blocking_failures": blocking,
                "unreadable_origins": unreadable,
                "partial_blocks": partial,
                "expected_sources": audit.get("corpus_expected_sources"),
                "observed_sources": audit.get("corpus_observed_sources"),
                "truncated": audit.get("corpus_truncated"),
            },
        )
    anomalies.extend(derived[key] for key in sorted(derived))
    return sorted(anomalies, key=lambda row: (row["product_id"], row["param_id"]))


def collect_golden_exempt_anomalies(
    golden_status: GoldenStatus | None,
) -> list[dict[str, Any]]:
    """§11.6: продукты, для которых эталон **объявленно** отсутствует, — по продукту.

    Источник — объявление пакета ``sources/golden_status.yaml: exempt_products``
    (:class:`~core.pipeline.golden_status.GoldenProductExemption`). Зачем аномалия, если
    waiver уже объявлен файлом: «эталона для продукта нет» — это утверждение о том, чем
    продукт **не** проверяли, и читать его должны артефакты прогона, а не только тот, кто
    откроет файл пакета. ``golden.csv`` не входит в ``run_id``, поэтому waiver — не
    измерение, а объявление; отсюда и уровень «наблюдение»: статусы ячеек не меняются
    (I12), а ``Artifacts.golden_exempt_products``/``nda_questions.csv`` называют продукт
    и причину.

    Детерминизм: сортировка по ``product_id``. ``None`` (объявление не читали) — пустой
    список: «не читали» не выдаётся за «waiver'ов нет».

    :return: записи ``{"product_id", "param_id", "reason", "declared_by",
        "declared_at"}``; ``param_id`` пуст — waiver относится к продукту целиком.
    """
    if golden_status is None or not golden_status.exempt_products:
        return []
    anomalies: list[dict[str, Any]] = []
    for entry in sorted(golden_status.exempt_products, key=lambda item: item.product_id):
        declared_at = (
            f", declared_at={entry.declared_at}" if entry.declared_at else ""
        )
        anomalies.append(
            {
                "product_id": entry.product_id,
                "param_id": "",
                "reason": (
                    f"§11.6: для продукта '{entry.product_id}' эталон объявленно "
                    f"отсутствует (sources/golden_status.yaml: exempt_products): "
                    f"{entry.reason} [declared_by={entry.declared_by}{declared_at}]; "
                    "сверка для продукта не выполняется — это решение, а не пропуск"
                ),
                "declared_by": entry.declared_by,
                "declared_at": entry.declared_at,
            }
        )
    return anomalies


def _package_dir_for(case_id: str) -> Path | None:
    """Каталог пакета по ``case_id`` — через объявленную раскладку (03 §A.1).

    Отчёту нужен **авторский** вход (объявление эталона), которого нет в БД прогона:
    waiver не входит в ``run_id`` и не персистится, поэтому источник — файл пакета.
    Каталог находится по объявленной раскладке (``paths.yaml``/``COHOUND_PACKAGES_DIR``/
    ``COHOUND_HOME``), а не догадкой от текущего каталога. ``None`` — пакета нет по этому
    пути (прогон перенесён/пакет удалён): тогда waiver'ов не показываем, отчёт не падает.
    """
    if not case_id:
        return None
    try:
        base = resolve_paths().packages / case_id
    except ValueError:  # некорректный paths.yaml — раскладка не повод ронять отчёт
        return None
    return base if base.is_dir() else None


# ---------------------------------------------------------------------------
# preliminary_findings
# ---------------------------------------------------------------------------


def render_preliminary_findings(cells: Iterable[Any], out_path: str | Path) -> Path:
    """Пишет ``preliminary_findings`` (XLSX; CSV-fallback) — кандидаты с маркером.

    Каждая строка несёт :data:`NOT_FOR_PUBLICATION` («не публикация») — артефакт для
    аналитика, не для публикации (I13).
    """
    prelim = [c for c in _cell_list(cells) if getattr(c, "state", None) is CellState.PRELIMINARY]
    fieldnames = ["product_id", "param_id", "preliminary_status", "validation_notes", "marker"]
    rows = [
        {
            "product_id": str(cell.product_id),
            "param_id": str(cell.param_id),
            "preliminary_status": "" if getattr(cell, "status", None) is None else _status_symbol(cell),
            "validation_notes": "; ".join(
                getattr(cell, "validation_notes", ()) or ()
            )
            or getattr(cell, "notes", None)
            or "",
            "marker": NOT_FOR_PUBLICATION,
        }
        for cell in prelim
    ]

    out = Path(out_path)
    if _load_openpyxl() is not None:
        sheet = [list(fieldnames)]
        sheet.extend([[row[name] for name in fieldnames] for row in rows])
        return _write_xlsx(out, [("preliminary", sheet)])
    return _write_csv(_tabular_path(out, xlsx=False), fieldnames, rows)


# ---------------------------------------------------------------------------
# diff
# ---------------------------------------------------------------------------


def _major(version: str) -> str:
    """Major-компонента версии (``"2.1.0"`` → ``"2"``)."""
    return str(version).split(".", 1)[0]


def _scenario_fingerprint(manifest: RunManifest) -> list[tuple[Any, ...]]:
    """Маршрутизационная подпись ``llm_scenarios`` (§7.4/I21) без ``run_id``.

    ``run_id`` привязан к конкретному циклу и меняется при каждом прогоне, поэтому в
    границу §13 он не входит: значимо **какой** сценарий привязан к **какой** модели
    (и как декодируется), а не идентификатор прогона.
    """
    return [
        (
            getattr(binding, "scenario", None),
            getattr(binding, "model_id", None),
            getattr(binding, "model_version", None),
            getattr(binding, "endpoint_kind", None),
            getattr(binding, "web_tool", None),
            getattr(binding, "decoder", None),
            getattr(binding, "seed", None),
            getattr(binding, "temperature", None),
            getattr(binding, "constrained_decoding", None),
        )
        for binding in (getattr(manifest, "llm_scenarios", None) or [])
    ]


def _boundary_value(manifest: RunManifest, name: str) -> Any:
    """Сравнимое значение поля границы §13 (``llm_scenarios`` — без ``run_id``)."""
    if name == "llm_scenarios":
        return _scenario_fingerprint(manifest)
    return getattr(manifest, name)


#: Поля контракта, которые манифест хранит под **другим** именем атрибута. Это не второй
#: механизм сравнения, а разрешение имени: значение то же (``cutoff_K`` прогона
#: персистится как ``runs.retrieval_cutoff_K`` и читается в ``RunManifest``).
_CONTRACT_FIELD_ATTRS: dict[str, str] = {"cutoff_K": "retrieval_cutoff_K"}


def _serp_rounds_from_manifest(manifest: RunManifest) -> int | None:
    """Объявленные раунды SERP-контура из провенанса манифеста (``None`` — не записано).

    Отдельного столбца у оси нет: значение живёт в
    ``search_effort_budget_json.serp.rounds.declared`` (провенанс контура). Возвращаем
    ровно объявленное число — не «сколько исполнилось»: граница §13 — про **условие**
    измерения, а фактический расход это наблюдение, а не контракт.
    """
    raw = getattr(manifest, "search_effort_budget_json", None)
    if not raw:
        return None
    try:
        payload = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, Mapping):
        return None
    rounds = (payload.get("serp") or {}).get("rounds") or {}
    declared = rounds.get("declared")
    return int(declared) if isinstance(declared, (int, float)) else None


def _serp_mode_from_manifest(manifest: RunManifest) -> str | None:
    """Объявленный режим поиска из провенанса контура (``None`` — не записано).

    Отдельного столбца у оси нет: значение живёт в
    ``search_effort_budget_json.serp.mode.declared``. Читается **объявленное**, а не
    исполненное: контракт описывает условие измерения, а ``used`` — наблюдение (и оно
    остаётся в провенансе, чтобы расхождение «объявлено ≠ исполнено» было видно).
    """
    raw = getattr(manifest, "search_effort_budget_json", None)
    if not raw:
        return None
    try:
        payload = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, Mapping):
        return None
    mode = (payload.get("serp") or {}).get("mode") or {}
    declared = mode.get("declared") if isinstance(mode, Mapping) else None
    return str(declared) if declared else None

#: Поля контракта, значение которых манифест не хранит отдельным атрибутом: геттер
#: читает его из провенанса. ``None`` с обеих сторон — по-прежнему «не восстановимо»
#: (историческая строка без канала), и это называется, а не выдумывается.
_CONTRACT_FIELD_GETTERS: dict[str, Callable[[RunManifest], Any]] = {
    "serp_rounds": _serp_rounds_from_manifest,
    # Режим поиска — тем же путём: иначе расхождение прогонов по этой оси объявлялось бы
    # невосстановимым, хотя значение записано (в провенансе контура).
    "serp_mode": _serp_mode_from_manifest,
}

#: Поля контракта, которые современный прогон записывает **всегда** (слагаемые ``run_id``).
#: Поэтому ``None`` у одной из сторон — не «пустое значение», а «не записано» (строка
#: прошлой схемы, где колонки не было): сравнение с ним дало бы ложное «изменилось».
#: У остальных полей ``None`` — законное состояние (реестров нет, секции ``crawl`` нет), и
#: переход ``None → значение`` там действительно означает смену условий измерения.
_CONTRACT_FIELDS_ALWAYS_RECORDED: frozenset[str] = frozenset(
    {"cohort_digest", "seeds_digest", "crawl_policy_digest", "frozen_source"}
)


def _contract_field_attr(name: str) -> str:
    """Имя атрибута ``RunManifest``, несущего поле контракта (``cutoff_K`` → ``retrieval_cutoff_K``)."""
    return _CONTRACT_FIELD_ATTRS.get(name, name)


def _contract_field_diagnostics(
    prev: RunManifest, curr: RunManifest
) -> tuple[list[str], list[str]]:
    """Детализация расхождения контракта: различающиеся и недоступные поля.

    Возвращает пару ``(различающиеся, отсутствующие)``; порядок обоих списков —
    порядок :data:`~core.pipeline.digests.EXECUTION_CONTRACT_FIELDS` (детерминизм
    вывода). Поле считается **доступным**, только если атрибут есть на манифесте с
    обеих сторон **и** значение записано. ``cohort_digest``/``seeds_digest`` теперь
    несутся манифестом (``runs.cohort_digest``/``runs.seeds_digest``), поэтому
    различаются наравне с прочими; недоступными они остаются только у строки прошлой
    схемы, где колонки нет, — и это называется, а не выдумывается (A3). То же у
    ``crawl_policy_digest`` (план 19 §P4: колонка ``runs.crawl_policy_digest``) и
    канонизированного ``frozen_source``: ось, записанная на манифесте, называется, а не
    попадает в «предел диагностики».

    Сравнение значений — через :func:`_boundary_value` (та же нормализация
    ``llm_scenarios``, что и в legacy-ветке).
    """
    changed: list[str] = []
    missing: list[str] = []
    for name in EXECUTION_CONTRACT_FIELDS:
        getter = _CONTRACT_FIELD_GETTERS.get(name)
        if getter is not None:
            before = getter(prev)
            after = getter(curr)
            if before is None and after is None:
                missing.append(name)
            elif before != after:
                changed.append(
                    f"execution_change: execution_contract_digest: {name}: "
                    f"{before!r} → {after!r}"
                )
            continue
        attr = _contract_field_attr(name)
        if not (hasattr(prev, attr) and hasattr(curr, attr)):
            missing.append(name)
            continue
        before = _boundary_value(prev, attr)
        after = _boundary_value(curr, attr)
        if name in _CONTRACT_FIELDS_ALWAYS_RECORDED and (before is None or after is None):
            # У современного прогона эти слагаемые ``run_id`` записаны всегда, поэтому
            # ``None`` здесь — «не записано» (строка прошлой схемы), а не пустое значение.
            # Назвать это «изменилось» значило бы утверждать смену когорты/seed-набора
            # без данных: причина остаётся невыводимой и называется отсутствием (A3).
            missing.append(name)
            continue
        if before != after:
            changed.append(
                f"execution_change: execution_contract_digest: {name}: "
                f"{before!r} → {after!r}"
            )
    return changed, missing


def _product_set(products: Iterable[Any] | None) -> set[str] | None:
    """Нормализует набор продуктов для сравнения состава когорты.

    ``None`` — «состав неизвестен» (данных нет); пустой вход — тоже ``None``: пустой
    набор не доказывает пустую когорту, и выдумывать его нельзя (A3).
    """
    if products is None:
        return None
    normalized = {str(product) for product in products}
    return normalized or None


def _cohort_change_class(
    prev_products: Iterable[Any] | None, curr_products: Iterable[Any] | None
) -> ChangeClass | None:
    """Класс изменения состава когорты (``cohort_expansion``/``cohort_reduction``).

    ``None`` — состав неизвестен хотя бы с одной стороны либо наборы совпадают.
    """
    previous = _product_set(prev_products)
    current = _product_set(curr_products)
    if previous is None or current is None:
        return None
    if current - previous:
        return ChangeClass.COHORT_EXPANSION
    if previous - current:
        return ChangeClass.COHORT_REDUCTION
    return None


def _cohort_change_reasons(
    prev_products: Iterable[Any] | None, curr_products: Iterable[Any] | None
) -> list[str]:
    """Причины границы для изменения состава когорты (детерминированный порядок)."""
    previous = _product_set(prev_products)
    current = _product_set(curr_products)
    if previous is None or current is None:
        return []
    reasons: list[str] = []
    added = sorted(current - previous)
    removed = sorted(previous - current)
    if added:
        reasons.append("execution_change: cohort_expansion: " + ", ".join(added))
    if removed:
        reasons.append("execution_change: cohort_reduction: " + ", ".join(removed))
    return reasons


def diff_boundary_violations(
    prev: RunManifest,
    curr: RunManifest,
    *,
    prev_products: Iterable[Any] | None = None,
    curr_products: Iterable[Any] | None = None,
) -> list[str]:
    """Версионные границы §13, запрещающие межцикловый diff (список причин).

    §13/B-7 (v2.1): причина помечается классом изменения:

    * ``methodology_change`` — смена ``methodology_version`` (major — отдельной строкой;
      minor/patch — строкой ``methodology_version``), поля :data:`METHODOLOGY_FIELDS`
      (методологическая несовместимость расчёта);
    * ``execution_change`` — расхождение условий измерения.

    **Порядок классификации (09 §12.5).** ``methodology_version`` входит в
    :data:`~core.pipeline.digests.EXECUTION_CONTRACT_FIELDS`, поэтому без явной
    классификации смена правил читалась бы как «изменились условия измерения». При
    расхождении контракта причины проверяются по порядку и на каждую причину — одна
    строка: major ``methodology_version`` → любое иное различие ``methodology_version``
    (minor/patch: §13 требует ``golden-full`` и пере-аннотацию golden, а не только при
    major) → поля :data:`METHODOLOGY_FIELDS` → состав когорты → и лишь затем общий
    ``execution_contract_digest``. Общая строка контракта **не** эмитится, если более
    конкретная причина уже объясняет расхождение.

    **Сравнение условий измерения (09 §12.5).** Если оба манифеста несут
    ``execution_contract_digest`` (не ``None``), сравнивается **контракт**, а
    :data:`EXECUTION_FIELDS` не перебираются: дайджест — свёртка тех же полей, и
    построчный перебор продублировал бы одну причину многими строками. В частности
    ``index_build_id`` перестаёт быть границей: новая композиция при неизменных условиях
    измерения — разрешённый diff (``content_change``, 09 §12.1). Состав когорты при этом
    остаётся различимым: если известны наборы ``product_id`` и они различаются, причина
    называется ``cohort_expansion``/``cohort_reduction`` **вместо** общего
    ``execution_change: execution_contract_digest`` (``cohort_digest`` свёрнут в
    контракт, иначе расширение читалось бы как рядовая смена условий).

    **Диагностика расхождения контракта.** Дайджест — непрозрачная свёртка, поэтому
    вслед за строкой с дайджестами перечисляются **различающиеся поля**
    :data:`~core.pipeline.digests.EXECUTION_CONTRACT_FIELDS`, доступные на
    ``RunManifest`` (в порядке кортежа): ``execution_change: execution_contract_digest:
    {field}: prev → curr``. Тот же префикс — класс и парсинг причин не меняются.
    ``cohort_digest``/``seeds_digest`` — такие же поля манифеста (``runs.cohort_digest``/
    ``runs.seeds_digest``, проекция объявленного входа), поэтому состав когорты и
    seed-набор называются наравне с прочими осями, а не объявляются невосстановимыми.
    Если ни одно доступное поле не различается, а дайджесты разные, эмитится строка о
    **пределе диагностики** с перечнем полей, которых на манифесте нет; если таких нет,
    строка не эмитится вовсе (предела больше нет — печатать его было бы неправдой).
    Причина не выдумывается (A3). Детализация —
    только reporting: :func:`resolve_diff_change_class` и
    :func:`core.pipeline.run_identity.classify_change` не затрагиваются.

    Если **хотя бы один** дайджест ``None`` (legacy-пара) — поведение прежнее:
    перебираются :data:`METHODOLOGY_FIELDS` и :data:`EXECUTION_FIELDS`, включая
    ``index_build_id``; проверка minor/patch ``methodology_version`` не вводится
    ретроактивно (условия измерения legacy-пары недоказуемы).

    :param prev_products/curr_products: наборы ``product_id`` циклов (из ячеек/корпуса
        вызывающего). ``None``/пусто — состав неизвестен: набор не выдумывается, причина
        остаётся общей.
    """
    violations: list[str] = []
    major_changed = _major(prev.methodology_version) != _major(curr.methodology_version)
    if major_changed:
        violations.append(
            "methodology_change: methodology_version major: "
            f"{prev.methodology_version} → {curr.methodology_version}"
        )
    has_contract = (
        prev.execution_contract_digest is not None
        and curr.execution_contract_digest is not None
    )
    # §13: смена methodology_version — версионная граница сама по себе (golden-full +
    # пере-аннотация golden), а не «изменились условия измерения». Major уже назван
    # выше, поэтому здесь — любое иное различие (minor/patch).
    version_changed = has_contract and prev.methodology_version != curr.methodology_version
    if version_changed and not major_changed:
        violations.append(
            "methodology_change: methodology_version: "
            f"{prev.methodology_version} → {curr.methodology_version}"
        )
    methodology_field_changed = False
    for name in METHODOLOGY_FIELDS:
        if getattr(prev, name) != getattr(curr, name):
            methodology_field_changed = True
            violations.append(
                f"methodology_change: {name}: "
                f"{getattr(prev, name)!r} → {getattr(curr, name)!r}"
            )
    if has_contract:
        if prev.execution_contract_digest != curr.execution_contract_digest:
            cohort_reasons = _cohort_change_reasons(prev_products, curr_products)
            if cohort_reasons:
                violations.extend(cohort_reasons)
            elif not (
                major_changed or version_changed or methodology_field_changed
            ):
                violations.append(
                    "execution_change: execution_contract_digest: "
                    f"{prev.execution_contract_digest} → "
                    f"{curr.execution_contract_digest}"
                )
                # Строка дайджеста — граница (её смотрят тесты/операторы); ниже —
                # детализация «что именно изменилось», восстановимая из манифеста.
                changed_fields, missing_fields = _contract_field_diagnostics(prev, curr)
                if changed_fields:
                    violations.extend(changed_fields)
                elif missing_fields:
                    # Ни одно доступное поле не различается, а дайджесты разные: причину
                    # не выдумываем — называем **предел диагностики** и ровно те поля,
                    # которых на манифесте нет. Предела нет — строки нет: пустой список
                    # недоступных означает, что все оси сравнены (и совпали), и печатать
                    # «не восстановимо» было бы неправдой.
                    violations.append(
                        "execution_change: execution_contract_digest: различающееся "
                        "поле не восстановимо из манифеста ("
                        + "/".join(missing_fields)
                        + " либо иная ось)"
                    )
        return violations
    # Legacy-пара: дайджеста нет — прежнее построчное сравнение (байт-в-байт).
    for name in EXECUTION_FIELDS:
        if _boundary_value(prev, name) != _boundary_value(curr, name):
            violations.append(
                f"execution_change: {name}: "
                f"{getattr(prev, name)!r} → {getattr(curr, name)!r}"
            )
    return violations


def resolve_diff_change_class(
    prev: RunManifest,
    curr: RunManifest,
    *,
    prev_products: Iterable[Any] | None = None,
    curr_products: Iterable[Any] | None = None,
) -> ChangeClass:
    """Класс изменения пары прогонов **на момент diff** (09 §12.1/§12.5).

    Разрешение возможно только когда существуют оба прогона:

    * major ``methodology_version`` различается → ``methodology_change``;
    * контракты различаются, а ``methodology_version`` отличается (minor/patch) →
      ``methodology_change``: это смена правил, а не условий измерения (§13; см.
      :func:`diff_boundary_violations`);
    * контракты различаются и наборы продуктов известны и различаются →
      ``cohort_expansion``/``cohort_reduction``;
    * контракты различаются → ``execution_change``;
    * контракты совпадают, а ``index_build_id`` различается (в т.ч. ``None`` с одной
      стороны) → ``content_change``;
    * legacy-пара (дайджест ``None`` хотя бы с одной стороны) → прежняя семантика:
      расхождение :data:`EXECUTION_FIELDS` (включая ``index_build_id``) —
      ``execution_change``, иначе ``none``. ``content_change`` для legacy **не**
      заявляется: недоказуемо, что изменился только контент.

    **Почему не в** :func:`core.pipeline.run_identity.classify_change`.
    ``classify_change`` исполняется **до** корпуса: ``index_build_id`` ещё не существует,
    поэтому ``content_change`` там не выводится и выводиться не должен. Это класс
    **времени diff** — не «исправлять» асимметрию добавлением index id в run identity
    (иначе класс зависел бы от ещё не собранного корпуса и врал бы в lineage).
    """
    if _major(prev.methodology_version) != _major(curr.methodology_version):
        return ChangeClass.METHODOLOGY_CHANGE
    if (
        prev.execution_contract_digest is not None
        and curr.execution_contract_digest is not None
    ):
        # §13: methodology_version входит в контракт, но его смена — смена правил;
        # классифицируется до общего расхождения контракта (как в границе diff).
        if prev.methodology_version != curr.methodology_version:
            return ChangeClass.METHODOLOGY_CHANGE
        if prev.execution_contract_digest != curr.execution_contract_digest:
            return (
                _cohort_change_class(prev_products, curr_products)
                or ChangeClass.EXECUTION_CHANGE
            )
        if prev.index_build_id != curr.index_build_id:
            return ChangeClass.CONTENT_CHANGE
        return ChangeClass.NONE
    for name in EXECUTION_FIELDS:
        if _boundary_value(prev, name) != _boundary_value(curr, name):
            return ChangeClass.EXECUTION_CHANGE
    return ChangeClass.NONE


def _changed_rows(prev_cells: Iterable[Any], curr_cells: Iterable[Any]) -> list[dict[str, Any]]:
    """Изменённые ячейки (статус/evidence_id/добавление/удаление), детерминированно."""
    prev_map = {(str(c.product_id), str(c.param_id)): c for c in prev_cells}
    curr_map = {(str(c.product_id), str(c.param_id)): c for c in curr_cells}
    rows: list[dict[str, Any]] = []
    for key in sorted(set(prev_map) | set(curr_map)):
        prev = prev_map.get(key)
        curr = curr_map.get(key)
        prev_status = _status_symbol(prev) if prev is not None else "—"
        curr_status = _status_symbol(curr) if curr is not None else "—"
        prev_ev = _evidence_ids(prev) if prev is not None else []
        curr_ev = _evidence_ids(curr) if curr is not None else []
        if prev is None or curr is None or prev_status != curr_status or prev_ev != curr_ev:
            rows.append(
                {
                    "product_id": key[0],
                    "param_id": key[1],
                    "prev_status": prev_status,
                    "curr_status": curr_status,
                    "prev_evidence_id": ", ".join(prev_ev),
                    "curr_evidence_id": ", ".join(curr_ev),
                }
            )
    return rows


def _doc_delta_rows(
    doc_delta: Mapping[str, Mapping[str, int]] | None,
) -> list[str]:
    """Строки markdown-таблицы per-product doc-delta (детерминированно, по продукту)."""
    if not doc_delta:
        return []
    lines = [
        "",
        "| product | new | reused | removed |",
        "|---|---|---|---|",
    ]
    for product in sorted(doc_delta):
        counts = doc_delta[product] or {}
        lines.append(
            "| {product} | {new} | {reused} | {removed} |".format(
                product=product,
                new=int(counts.get("new", 0)),
                reused=int(counts.get("reused", 0)),
                removed=int(counts.get("removed", 0)),
            )
        )
    return lines


def render_diff(
    prev_manifest: RunManifest,
    curr_manifest: RunManifest,
    prev_cells: Iterable[Any],
    curr_cells: Iterable[Any],
    out_path: str | Path,
    *,
    doc_delta: Mapping[str, Mapping[str, int]] | None = None,
    prev_products: Iterable[Any] | None = None,
    curr_products: Iterable[Any] | None = None,
) -> Path:
    """Пишет ``diff.md`` — изменённые статусы + evidence_id (§9).

    Контракт §13 (09 §12.5): сравниваются **условия измерения**
    (``execution_contract_digest``), а не ``index_build_id``. При смене major
    ``methodology_version``/поля :data:`METHODOLOGY_FIELDS` (класс
    ``methodology_change``) либо расхождении контракта (класс ``execution_change``;
    для legacy-пары без дайджеста — :data:`EXECUTION_FIELDS`) diff **отказывается** —
    :raises CrossCycleDiffError. Иначе пишется markdown-таблица изменений.

    Если контракты совпадают, а ``index_build_id`` различается, diff **разрешён**
    (``content_change``): ``diff.md`` несёт баннер ``content_change`` и, при переданном
    ``doc_delta``, per-product таблицу ``new``/``reused``/``removed``. При отсутствии
    изменения контента баннер не пишется — diff не заявляет изменение, которого не было.

    :param doc_delta: ``product_id → {"new": n, "reused": n, "removed": n}`` — считается
        вызывающим по ``corpus_docs`` обоих прогонов (здесь нет доступа к БД).
    :param prev_products/curr_products: наборы продуктов циклов — нужны, чтобы отличить
        изменение состава когорты от прочей смены контракта (09 §12.5).
    """
    violations = diff_boundary_violations(
        prev_manifest,
        curr_manifest,
        prev_products=prev_products,
        curr_products=curr_products,
    )
    if violations:
        raise CrossCycleDiffError(
            "§13: межцикловый diff запрещён — " + "; ".join(violations)
        )

    change_class = resolve_diff_change_class(
        prev_manifest,
        curr_manifest,
        prev_products=prev_products,
        curr_products=curr_products,
    )
    rows = _changed_rows(prev_cells, curr_cells)
    lines = [
        f"# diff: {prev_manifest.run_id} → {curr_manifest.run_id}",
        "",
        f"- methodology_version: {prev_manifest.methodology_version} → {curr_manifest.methodology_version}",
        f"- package_version: {prev_manifest.package_version} → {curr_manifest.package_version}",
        f"- catalog_version: {prev_manifest.catalog_version} → {curr_manifest.catalog_version}",
        f"- index_build_id: {prev_manifest.index_build_id} → {curr_manifest.index_build_id}",
        f"- execution_contract_digest: {prev_manifest.execution_contract_digest} → "
        f"{curr_manifest.execution_contract_digest}",
    ]
    if change_class is ChangeClass.CONTENT_CHANGE:
        lines.extend(
            [
                "",
                "> **content_change**: изменился только состав/контент документов "
                "(execution contract совпадает) — diff разрешён; "
                "условия измерения сопоставимы.",
            ]
        )
        lines.extend(_doc_delta_rows(doc_delta))
    lines.extend(
        [
            "",
            "| product | param | prev_status | curr_status | prev_evidence_id | curr_evidence_id |",
            "|---|---|---|---|---|---|",
        ]
    )
    for row in rows:
        lines.append(
            "| {product_id} | {param_id} | {prev_status} | {curr_status} | "
            "{prev_evidence_id} | {curr_evidence_id} |".format(**row)
        )
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _write_placeholder_diff(out_path: Path, manifest: RunManifest) -> Path:
    """Пишет ``diff.md``-заглушку, если предыдущий цикл не передан."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    text = (
        "# diff\n\n"
        f"Предыдущий цикл не передан — diff недоступен (run_id={manifest.run_id}).\n"
    )
    out_path.write_text(text, encoding="utf-8")
    return out_path


# ---------------------------------------------------------------------------
# Оркестрация Ш9
# ---------------------------------------------------------------------------


def write_publishability_artifact(
    profile: Any,
    out_path: str | Path,
    *,
    run_id: str | None = None,
) -> Path | None:
    """Пишет ``outputs/publishability.yaml`` — полное разложение неизвестности (A1, §6).

    Это **измерение**, а не отчёт: класс и числа, из которых он выведен, читаются без
    парсинга battle card. Раньше причина непубликуемости жила только в голове читателя
    отчёта («coverage INSUFFICIENT»), и по ней нельзя было решить, что делать дальше.

    :param profile: :class:`~core.domain.scoring.PublishabilityProfile` либо ``None``
        (синтетический бандл) — тогда файл не пишется и возвращается ``None``.
    :param run_id: для провенанса файла; ``None`` — не записывается.
    """
    if profile is None:
        return None
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Один сериализатор с манифестом и CLI (``PublishabilityProfile.to_dict``): артефакт
    # не может разойтись с манифестом по составу полей.
    payload: dict[str, Any] = (
        profile.to_dict() if hasattr(profile, "to_dict") else asdict(profile)
    )
    payload["artifact"] = "publishability"
    if run_id is not None:
        payload["run_id"] = run_id
    # Происхождение чисел рядом с числами (A3): порог — объявленный дефолт, а не факт.
    payload["thresholds"] = {
        "unknown_share": {
            "value": profile.unknown_share_threshold,
            "source": "assumption",
            "note": (
                "DEFAULT_UNKNOWN_SHARE_THRESHOLD (core.domain.scoring): объявленный "
                "дефолт по ориентиру D-19, а не измеренная константа (A3)"
            ),
        },
        "weak_denial_share": {
            "value": profile.weak_denial_share_threshold,
            "source": "assumption",
            "note": "DEFAULT_WEAK_DENIAL_SHARE_THRESHOLD (A3)",
        },
        "coverage_floor": {
            "value": profile.coverage_floor,
            "source": "assumption",
            "note": (
                "DEFAULT_COVERAGE_FLOOR: согласован с границей Comparability "
                "INSUFFICIENT (<0.50)"
            ),
        },
    }
    text = yaml.safe_dump(payload, sort_keys=True, allow_unicode=True, default_flow_style=False)
    path.write_text(text, encoding="utf-8")
    return path

def write_measurement_uncertainty_artifact(
    payload: Mapping[str, Any],
    out_path: str | Path,
    *,
    run_id: str | None = None,
) -> Path:
    """Пишет ``outputs/measurement_uncertainty.yaml`` — полосу неопределённости вердиктов (P6c).

    Тем же механизмом, что ``publishability.yaml``: ``yaml.safe_dump(sort_keys=True)``,
    ``run_id`` в провенансе файла, никакого пересчёта на выходе. Полоса и её происхождение
    — **одна запись**: «4.2 % из замера» и «4.2 %, измеренные здесь» различимы полем
    ``computed_from_this_run`` и ``provenance.kind``, и это различие нельзя потерять при
    сериализации.

    :param payload: результат :func:`core.pipeline.measurement_uncertainty.build_measurement_uncertainty`.
    :param run_id: для провенанса файла; ``None`` — не записывается.
    """
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    document: dict[str, Any] = dict(payload)
    if run_id is not None:
        document["run_id"] = run_id
    text = yaml.safe_dump(
        document, sort_keys=True, allow_unicode=True, default_flow_style=False
    )
    path.write_text(text, encoding="utf-8")
    return path


def _write_stale_cells(cells: Iterable[Any], path: Path) -> Path | None:
    """Пишет ``stale_cells.csv`` — «требует пере-оценки» (§13/K2); пусто → ``None``.

    Ячейка со ``stale_reason`` привязана к **прошлой** сборке/условиям и исключена из
    расчёта (иначе Capability/Coverage смешали бы две сборки). Статус её при этом не
    меняется (I12) — меняется только участие в цифрах, и этот файл делает исключение
    видимым: пере-оценка — работа, а не пропуск.
    """
    rows = [
        {
            "product_id": str(cell.product_id),
            "param_id": str(cell.param_id),
            "status": getattr(getattr(cell, "status", None), "value", None),
            "stale_reason": str(
                getattr(getattr(cell, "stale_reason", None), "value", cell.stale_reason)
            ),
            "index_build_id": getattr(cell, "index_build_id", None),
            "retrieval_profile": getattr(cell, "retrieval_profile", None),
        }
        for cell in cells
        if getattr(cell, "stale_reason", None) is not None
    ]
    if not rows:
        return None
    rows.sort(key=lambda row: (row["product_id"], row["param_id"]))
    return _write_csv(
        path,
        [
            "product_id",
            "param_id",
            "status",
            "stale_reason",
            "index_build_id",
            "retrieval_profile",
        ],
        rows,
    )


def collect_cell_origins(cells: Iterable[Any]) -> list[dict[str, Any]]:
    """Происхождение ячеек по продуктам (09 §10.1): сколько измерено сейчас, сколько перенесено.

    Единственный носитель — ``cells.measurement_origin``; отчёт его **читает**, а не
    выводит заново: перенос решается в Ш6, и второй расчёт разошёлся бы с записью при
    первой же правке условий. Строки сортируются по ``product_id`` — детерминизм
    артефакта (§S8).

    ``cells_carried`` — подмножество ``cells_measured``, а не дополнение к нему:
    перенесённая ячейка **измерена** (в прошлом цикле), и вычитать её из знаменателя
    значило бы публиковать «эта ячейка не измерена», что неправда. Поэтому
    ``cells_measured - cells_carried`` — «измерено в этом прогоне вызовом оценщика».
    """
    stats: dict[str, list[int]] = {}
    for cell in _cell_list(cells):
        bucket = stats.setdefault(str(cell.product_id), [0, 0])
        bucket[0] += 1
        origin = getattr(cell, "measurement_origin", None)
        if str(getattr(origin, "value", origin) or "") == MeasurementOrigin.CARRIED.value:
            bucket[1] += 1
    return [
        {
            "product_id": product_id,
            "cells_total": total,
            "cells_carried": carried,
        }
        for product_id, (total, carried) in sorted(stats.items())
    ]


def _write_cell_origins(rows: Sequence[Mapping[str, Any]], path: Path) -> Path | None:
    """Пишет ``cell_origin.csv`` — по продукту: всего ячеек и сколько перенесено.

    ``None`` при отсутствии перенесённых ячеек: файл из одних нулей читался бы как
    «перенос работает и ничего не перенёс», тогда как в первом цикле его просто не
    применяли. Пустой перенос виден в ``run_product``/снимке (``cells_carried = 0``),
    а артефакт появляется, когда о переносе есть что сказать.
    """
    if not any(int(row.get("cells_carried") or 0) for row in rows):
        return None
    return _write_csv(
        path,
        ["product_id", "cells_total", "cells_carried"],
        [
            {
                "product_id": row["product_id"],
                "cells_total": row["cells_total"],
                "cells_carried": row["cells_carried"],
            }
            for row in rows
        ],
    )


def _cell_failure_kinds(
    store: Any, run_id: str
) -> dict[tuple[str, str], str | None]:
    """Класс сбоя каждой ячейки прогона (``cells.failure_kind``), если store умеет читать.

    Доменная ``Cell`` колонку не несёт, поэтому отчёт спрашивает store — тот же
    источник, что у метрик и селектора ремонта. Пустая карта — «класс не прочитан»,
    и колонка остаётся пустой: «не записано» ≠ «сбоя не было».
    """
    reader = getattr(store, "read_cell_failure_kinds", None)
    if not callable(reader):
        return {}
    try:
        return dict(reader(str(run_id)))
    except Exception:  # noqa: BLE001 — провенанс не роняет отчёт
        return {}


def _write_cell_diagnostics(
    cells: Iterable[Any],
    path: Path,
    *,
    failure_kinds: Mapping[tuple[str, str], str | None] | None = None,
) -> Path | None:
    """Пишет ``cell_diagnostics.csv`` — ячейки, о которых человек обязан знать.

    Поводом попасть в файл служит **записанный факт**, а не догадка читателя: непустые
    ``validation_notes`` (что было с ячейкой при персисте/валидации), непустой
    ``review_reason`` (причина pending) и незаписанное ``absence_rule`` (правило
    отсутствия не объявлено — тогда ``❌`` опирается на правило, которого никто не
    заявлял). Всё это уже лежало в БД, но было невидимо человеку: три новые записи
    ячейки без артефакта остаются невидимыми.

    Незаписанное ``absence_rule`` — повод **ячейки** только в смешанном прогоне, где
    правило где-то записано. Если не записано ни у одной ячейки (прогон старше
    колонки), это свойство прогона, а не N задач: иначе очередь разбора вырождается в
    полный список ячеек с одним общим поводом (наблюдено: 476 строк, все по этой
    причине; 476 — число ячеек, а не число находок). Такой факт публикуется **одной**
    строкой ``kind=run``, чтобы он не исчез и не притворялся триажем.

    ``denial_strength``/``relevant_evidence_coverage`` берутся из
    ``cell.evidence_scope`` (может отсутствовать) и печатаются пустыми, если не
    измерены — «не измерено» не выдаётся за ноль. Пустой список поводов ⇒ ``None``
    (файл не создаётся: пустой список — не артефакт).

    ``failure_kinds`` — карта ``(product_id, param_id) → cells.failure_kind`` из store:
    доменная ``Cell`` колонку не несёт. Колонка ``failure_kind`` публикуется рядом с
    ``validation_notes`` (машиночитаемо); пустое значение — «класс не записан», что не
    то же самое, что «сбоя не было». Записанный класс сам по себе — повод попасть в
    файл: иначе факт сбоя с пустыми заметками остался бы невидимым.
    """
    material = list(cells)
    kinds = failure_kinds or {}
    # Правило отсутствия записано хотя бы у одной ячейки ⇒ смешанный прогон, и
    # незаписанное правило у остальных — аномалия конкретной ячейки.
    rule_recorded = any(getattr(cell, "absence_rule", None) is not None for cell in material)
    rows: list[dict[str, Any]] = []
    for cell in material:
        notes = list(getattr(cell, "validation_notes", ()) or ())
        absence_rule = getattr(cell, "absence_rule", None)
        review_reason = getattr(cell, "review_reason", None)
        failure_kind = kinds.get(
            (str(cell.product_id), str(cell.param_id))
        )
        if (
            not notes
            and not review_reason
            and not (absence_rule is None and rule_recorded)
            and not failure_kind
        ):
            continue
        scope = getattr(cell, "evidence_scope", None)
        coverage_value = getattr(scope, "relevant_evidence_coverage", None)
        rows.append(
            {
                "kind": "cell",
                "product_id": str(cell.product_id),
                "param_id": str(cell.param_id),
                "state": getattr(getattr(cell, "state", None), "value", None),
                "status": getattr(getattr(cell, "status", None), "value", None),
                "absence_rule": (
                    getattr(absence_rule, "value", None)
                    or ("" if absence_rule is None else str(absence_rule))
                ),
                "review_reason": review_reason or "",
                "failure_kind": failure_kind or "",
                "denial_strength": getattr(
                    getattr(scope, "denial_strength", None), "value", None
                )
                or "",
                "relevant_evidence_coverage": (
                    f"{float(coverage_value):.3f}"
                    if isinstance(coverage_value, (int, float))
                    else ""
                ),
                "validation_notes": " | ".join(str(note) for note in notes),
            }
        )
    if material and not rule_recorded:
        rows.append(
            {
                "kind": "run",
                "product_id": "",
                "param_id": "",
                "state": "",
                "status": "",
                "absence_rule": "",
                "review_reason": "",
                "failure_kind": "",
                "denial_strength": "",
                "relevant_evidence_coverage": "",
                "validation_notes": (
                    f"absence_rule не записан ни у одной из {len(material)} ячеек: прогон "
                    "сделан до появления колонки; правило отсутствия при чтении не "
                    "восстанавливалось (его нельзя выдумать из каталога — объявление и "
                    "факт измерения разные вещи)"
                ),
            }
        )
    if not rows:
        return None
    rows.sort(key=lambda row: (row["kind"] != "run", row["product_id"], row["param_id"]))
    return _write_csv(
        path,
        [
            "kind",
            "product_id",
            "param_id",
            "state",
            "status",
            "absence_rule",
            "review_reason",
            "failure_kind",
            "denial_strength",
            "relevant_evidence_coverage",
            "validation_notes",
        ],
        rows,
    )

def write_reports(
    *,
    bundle: ScoreBundle,
    cells: Iterable[Any],
    catalog: Mapping[str, CatalogRow] | Iterable[CatalogRow],
    manifest: RunManifest,
    out_dir: str | Path,
    run_id: str,
    prev_manifest: RunManifest | None = None,
    prev_cells: Iterable[Any] | None = None,
    exhaustions: Mapping[tuple[str, str], Any] | None = None,
    coverage: Mapping[str, Any] | Iterable[Any] | None = None,
    store: Any | None = None,
    golden_status: GoldenStatus | None = None,
) -> Artifacts:
    """Пишет все артефакты прогона в ``{out_dir}/{run_id}/outputs/`` (Ш9, 03 §A.2).

    :param out_dir: базовый каталог прогонов (например, ``runs/``).
    :param run_id: идентификатор прогона; ``run_manifest.yaml`` — в ``{out_dir}/{run_id}/``.
    :param prev_manifest/prev_cells: предыдущий цикл для ``diff.md`` (опционально; без
        них пишется заглушка).
    :param exhaustions: карта ``(product_id, param_id) → ProtocolExhaustion`` (§5/S13);
        прокидывается в ``nda_questions`` и используется для сбора аномалий I3.
    :param coverage: покрытие корпуса (``CorpusCoverage`` mapping/iterable, Ф0). Если не
        задано и передан ``store`` — читается из БД (``iter_corpus_coverage``). Без обоих
        факт извлекается из absence-evidence ячеек (гейт ``❌`` пишет аудит покрытия).
    :param store: опциональный ``StorePort`` — источник per-product покрытия (Ф0).
    :param golden_status: объявленное состояние эталона пакета
        (:func:`core.pipeline.golden_status.read_golden_status`); из него берутся waiver'ы
        по продуктам (``exempt_products``) для аномалии ``golden_exempt_product``. Если не
        передан, объявление читается из пакета, найденного по ``manifest.case_id`` через
        объявленную раскладку (:func:`_package_dir_for`); нет пакета — waiver'ов не
        показываем. Ошибка разбора объявления **не** глушится: противоречивое объявление —
        видимая ошибка пакета, как в ``plan``/``run``.

    Пишет также ``outputs/measurement_uncertainty.yaml`` (P6c): полоса неопределённости
    вердиктов — объявленная (с путём замера и датой) либо измеренная в этом прогоне;
    ``computed_from_this_run`` различает два случая, чтобы объявленное не выдавалось за
    измеренное. Ограничение ранжирования публикуется машинным токеном
    (``ranking_restriction``), а не только текстом.

    Пишет также ``outputs/artifacts_gap.yaml`` (13-manual-artifacts §4): per-URL список
    недоступных артефактов и заготовку входа следующего прогона. Генератор — тот же,
    что у фазы ``corpus`` (:func:`core.pipeline.artifacts_gap.write_artifacts_gap`), и
    источник тот же (``crawl_session_items``); файл не measurement, его сбой отчёт не
    роняет (``Artifacts.artifacts_gap is None``).

    Пишет также ``outputs/corpus_freshness.yaml`` и (при датированных ячейках)
    ``outputs/cell_freshness.csv`` — даты актуальности артефактов корпуса (план 20 §7 F2):
    опорное наблюдение прогона, ``corpus_as_of``/``stale_roots`` и распределение возраста
    по продукту, ``evidence_as_of`` ячеек. Источник — durable-проекция ``corpus_artifact``
    и ссылки ``cell_evidence``, а не ``state.db``. Вердикты не меняются: дата здесь —
    провенанс (§2), датированное отсутствие — F3.

    G4/I11: вызывается :func:`core.pipeline.compute.assert_publishable` (предупреждение);
    при полностью отсутствующем Coverage (``coverage.defined is False`` либо пустой
    знаменатель) battle card **не эмитится** (``Artifacts.battle_card is None``).

    I3 (§5): strict-``❓`` без полного протокола исчерпания фиксируются как аномалии
    (:attr:`Artifacts.i3_anomalies`) — статус ячейки при этом не меняется.

    16 §3.2/I2d: пишет ``outputs/run_product.csv`` (статус продукта **в прогоне**, из
    таблицы ``run_product``) и собирает аномалии ``absence_undated`` (``❌`` снят
    инвариантом «у отрицания есть датированный носитель»). Оба — чтение уже записанного:
    отчёт не пересчитывает ни статус продукта, ни правило допуска ``❌``.
    """
    cell_list = _cell_list(cells)
    run_dir = Path(out_dir) / run_id
    outputs_dir = run_dir / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = write_run_manifest(manifest, run_dir)

    # G4-предупреждение (мягкий гейт); жёсткий I11 — отказ render_battle_card.
    assert_publishable(bundle)
    try:
        battle_card: Path | None = render_battle_card(
            bundle,
            cell_list,
            catalog,
            outputs_dir / "battle_card.xlsx",
            crawl_provenance=manifest.crawl_provenance,
        )
    except PublishBlockedError:
        battle_card = None

    # A1/§6: полное разложение неизвестности — отдельный артефакт, читаемый без разбора
    # battle card. Пишется всегда, когда бандл несёт профиль: «почему такой класс» —
    # измерение, а не приложение к отчёту.
    publishability_path = write_publishability_artifact(
        getattr(bundle, "publishability", None),
        outputs_dir / "publishability.yaml",
        run_id=run_id,
    )

    # P6c: полоса неопределённости вердиктов — рядом с профилем публикуемости, тем же
    # сериализатором. Без неё точечные вердикты читаются как точные, а замер (5 из 119 при
    # идентичном промпте) остаётся в файле отчёта и в прогон не попадает.
    uncertainty_path = write_measurement_uncertainty_artifact(
        build_measurement_uncertainty(
            cells=cell_list,
            catalog=_catalog_map(catalog),
            run_dir=run_dir,
        ),
        outputs_dir / "measurement_uncertainty.yaml",
        run_id=run_id,
    )

    # D1/G/B3: усечённый краул продукта + заявленный ``exhaustive`` — аномалия для человека
    # (паттерн I3 reviewed_by); статус ячейки не меняется и прогон не блокируется. Вычисляем
    # ДО ``nda_questions``, чтобы аномалии попали в операторский CSV (B3).
    truncated_products = {
        product
        for session in manifest.crawl_provenance.sessions
        for product in (session.truncated_products or [])
    }
    truncation_anomalies = collect_crawl_truncation_anomalies(
        cell_list, truncated_products
    )

    # Ф0: per-product покрытие корпуса. Явный аргумент → чтение из store (если дан) →
    # извлечение из absence-evidence ячеек (гейт ❌ пишет corpus_complete=False).
    coverage_source = coverage
    if coverage_source is None and store is not None:
        reader = getattr(store, "iter_corpus_coverage", None)
        if callable(reader):
            try:
                coverage_source = list(reader(run_id))
            except Exception:  # noqa: BLE001 — покрытие информационно, отчёт не роняем
                coverage_source = None
    coverage_anomalies = collect_corpus_coverage_anomalies(
        cell_list, coverage_source
    )

    # §11.6: waiver'ы эталона по продуктам. Явный аргумент приоритетен; иначе объявление
    # читается из пакета (``manifest.case_id`` + объявленная раскладка): waiver не входит
    # в ``run_id`` и не персистится в БД прогона, поэтому единственный источник —
    # авторский вход пакета. Ошибка разбора не глушится: это ошибка пакета, как в plan/run.
    resolved_golden_status = golden_status
    if resolved_golden_status is None:
        package_dir = _package_dir_for(manifest.case_id)
        if package_dir is not None:
            resolved_golden_status = read_golden_status(package_dir)
    golden_exempt_anomalies = collect_golden_exempt_anomalies(resolved_golden_status)

    # I2d: ячейки, у которых ``❌`` снят инвариантом «у отрицания есть датированный
    # носитель». Читается **из записи ячейки** (причина/заметки), а не пересчитывается:
    # инвариант живёт в конвейере, отчёт обязан объяснять опубликованную карту, а не
    # строить свою.
    absence_undated_anomalies = collect_absence_undated_anomalies(cell_list)

    # 16 §3.2: статус продукта **в прогоне** — строки ``run_product`` (mode/status/scope/
    # счётчики). Носитель — таблица; ``results[].notes`` прогона остаётся операторским
    # текстом, поэтому отчёт ничего не выводит из ячеек: только чтение.
    run_product_rows = collect_run_products(store, run_id)
    run_product_path = render_run_product(
        run_product_rows, outputs_dir / "run_product.csv"
    )

    # §13/K2: ячейки со ``stale_reason`` исключены из Capability/Coverage
    # (:mod:`core.pipeline.compute`), но человек обязан их видеть: это очередь
    # пере-оценки, а не тихое вычитание. Файл пишется только при непустом списке.
    stale_cells = _write_stale_cells(cell_list, outputs_dir / "stale_cells.csv")
    # 09 §10.1/§11.4: происхождение ячеек — по продукту, из ``cells.measurement_origin``.
    # Файл появляется, когда есть что сказать (перенос состоялся); нули живут в снимке
    # ``state.db`` и в ``run_product``.
    cell_origins = collect_cell_origins(cell_list)
    cell_origin_path = _write_cell_origins(
        cell_origins, outputs_dir / "cell_origin.csv"
    )
    # P6/absence_rule/validation_notes: записи ячейки, о которых человек обязан знать.
    # Файл — очередь разбора (как ``stale_cells.csv``), а не измерение; пишется только
    # при непустом списке поводов.
    cell_diagnostics = _write_cell_diagnostics(
        cell_list,
        outputs_dir / "cell_diagnostics.csv",
        failure_kinds=_cell_failure_kinds(store, run_id),
    )
    # I2/I2c: документы, прочитанные не целиком — числом, отдельным артефактом. Без него
    # «7 страниц из 2763» видно только в строке покрытия (``blocked``), а сам документ
    # (URL, причина, страницы, символы) назвать негде. Сбой выгрузки отчёт не роняет
    # (как и ``artifacts_gap``): факт остаётся в покрытии и в манифесте, теряется лишь
    # удобная таблица — ``Artifacts.partial_docs`` тогда ``None``.
    try:
        partial_docs_path = write_partial_docs_artifact(
            store, run_id, outputs_dir / "partial_docs.csv"
        )
    except Exception:  # noqa: BLE001 — артефакт вторичен к измерению
        partial_docs_path = None
    partial_summary = partial_docs_summary(
        store, run_id, products=sorted({str(cell.product_id) for cell in cell_list})
    )
    # A3: два замера качества поддержки получают производителя. До этого числа
    # существовали только в тестах: `support_overlap_scan` и `support_concentration`
    # считались, но ни один артефакт их не публиковал — то есть «поле есть, производителя
    # нет». Это тот же дефект, что пустой `relevant_evidence_coverage`: измерение, которого
    # не видно, не является измерением.
    try:
        support_payload = support_quality(
            cells=cell_list, catalog=catalog, store=store, run_id=run_id
        )
        support_path = write_support_quality_artifact(
            support_payload, outputs_dir / "support_quality.yaml", run_id=run_id
        )
    except Exception:  # noqa: BLE001 — наблюдение вторично к измерению, отчёт не роняем
        support_payload = None
        support_path = None

    # План 20 §7 F2: даты актуальности артефактов корпуса — из durable-проекции прогона
    # (``corpus_artifact``) и ссылок ``cell_evidence``. Отчёт не открывает ``state.db``:
    # у пост-хок ``hound report`` его нет. Один сборщик пишет YAML и отдаёт словарь для
    # ``--json``: файл и машинный вывод не могут разойтись (конвенция манифеста).
    # Вердикты не меняются: это чтение провенанса, а не условие статуса (это F3).
    freshness = collect_corpus_freshness(
        store=store, run_id=run_id, cells=cell_list, manifest=manifest
    )
    corpus_freshness_path = write_corpus_freshness_artifact(
        freshness.document, outputs_dir / "corpus_freshness.yaml"
    )
    cell_freshness_path = write_cell_freshness_rows(
        freshness.cell_rows, outputs_dir / "cell_freshness.csv"
    )

    nda_questions = render_nda_questions(
        cell_list,
        outputs_dir / "nda_questions.csv",
        exhaustions=exhaustions,
        crawl_truncation_anomalies=truncation_anomalies,
        corpus_coverage_anomalies=coverage_anomalies,
        golden_exempt_anomalies=golden_exempt_anomalies,
        absence_undated_anomalies=absence_undated_anomalies,
    )
    preliminary = render_preliminary_findings(
        cell_list, outputs_dir / "preliminary_findings.xlsx"
    )
    if prev_manifest is not None and prev_cells is not None:
        diff: Path = render_diff(
            prev_manifest,
            manifest,
            _cell_list(prev_cells),
            cell_list,
            outputs_dir / "diff.md",
        )
    else:
        diff = _write_placeholder_diff(outputs_dir / "diff.md", manifest)

    anomalies = collect_i3_anomalies(cell_list, exhaustions)
    anomalies.extend(truncation_anomalies)
    anomalies.extend(coverage_anomalies)
    anomalies.extend(golden_exempt_anomalies)
    anomalies.extend(absence_undated_anomalies)

    # 13-manual-artifacts §4: тот же генератор, что у фазы ``corpus`` (один код — две
    # точки вызова), поэтому повторная выгрузка отчёта обновляет файл, а формы двух
    # выходов не расходятся. Best-effort: файл — не measurement, его сбой не роняет Ш9.
    try:
        artifacts_gap = write_artifacts_gap(store, run_id=run_id, out_dir=out_dir).path
    except Exception:  # noqa: BLE001 — отчёт вторичен к фазе
        artifacts_gap = None

    return Artifacts(
        battle_card=battle_card,
        stale_cells=stale_cells,
        cell_origin=cell_origin_path,
        cell_origins=cell_origins,
        cell_diagnostics=cell_diagnostics,
        partial_docs=partial_docs_path,
        partial_docs_summary=partial_summary,
        support_quality=support_path,
        support_quality_summary=support_payload,
        corpus_freshness=corpus_freshness_path,
        cell_freshness=cell_freshness_path,
        corpus_freshness_summary=freshness.document,
        publishability=publishability_path,
        measurement_uncertainty=uncertainty_path,
        preliminary_findings=preliminary,
        nda_questions=nda_questions,
        diff=diff,
        manifest=manifest_path,
        i3_anomalies=anomalies,
        crawl_truncation_anomalies=truncation_anomalies,
        corpus_coverage_anomalies=coverage_anomalies,
        golden_exempt_products=golden_exempt_anomalies,
        absence_undated_anomalies=absence_undated_anomalies,
        run_product=run_product_path,
        run_products=run_product_rows,
        artifacts_gap=artifacts_gap,
    )
