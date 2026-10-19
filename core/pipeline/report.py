"""coHound — отчётные артефакты прогона (W10/T10.2, Ш9, §9, 02 §E.2).

Собирает публикуемые артефакты прогона:

* ``battle_card.xlsx`` — шапка (Capability / Coverage / Comparability), матрица
  (статус + обоснование + primary citation, «+N источников»), сателлиты, модульное
  покрытие, sensitivity-приложение;
* ``preliminary_findings.xlsx`` — кандидаты с маркером «не публикация»;
* ``nda_questions.csv`` — все ``❓`` (продукт, параметр, исчерпанные контуры);
* ``diff.md`` — изменённые статусы + evidence_id;
* ``run_manifest.yaml`` — агрегат версий/моделей/сборки/профиля+K/бюджета (R7).

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
  методологического класса (:data:`METHODOLOGY_FIELDS`) либо исполнительного класса
  (:data:`EXECUTION_FIELDS`); контракт — **отказ** (:class:`CrossCycleDiffError`).
  Причины маркируются классом: ``methodology_change`` / ``execution_change``.

Модуль категорийно нейтрален (§1.3), детерминирован (сортировка продуктов/параметров).
``openpyxl`` — опциональная зависимость: при её отсутствии XLSX-артефакты
деградируют до CSV (суффикс ``.csv``) с тем же логическим составом; ``csv``/``yaml`` —
stdlib.
"""

from __future__ import annotations

import csv
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from core.domain.enums import (
    BLOCKING_FAILURE_KINDS,
    CellState,
    RankingPolicy,
    SourceScope,
    Status,
)
from core.domain.invariants import check_unknown_exhaustion
from core.domain.models import (
    CatalogRow,
    Cell,
    CorpusCoverage,
    CrawlProvenance,
    ProtocolExhaustion,
    RunManifest,
    canonical_rate_limit,
)
from core.domain.scoring import evidence_coverage
from core.pipeline.compute import (
    ScoreBundle,
    assert_publishable,
    publish_warnings,
)
from core.pipeline.crawl_frontier import resolve_crawl_limits
from core.pipeline.evaluate import CORPUS_INCOMPLETE_REVIEW_REASON
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
    "Artifacts",
    "PublishBlockedError",
    "CrossCycleDiffError",
    "COVERAGE_INSUFFICIENT_THRESHOLD",
    "FALLBACK_ORDER_LABEL",
    "INSUFFICIENT_COVERAGE_NOTE",
    "NOT_FOR_PUBLICATION",
    "METHODOLOGY_FIELDS",
    "EXECUTION_FIELDS",
    "DIFF_BOUNDARY_FIELDS",
    "build_manifest",
    "write_run_manifest",
    "render_battle_card",
    "render_nda_questions",
    "render_preliminary_findings",
    "render_diff",
    "write_reports",
    "collect_i3_anomalies",
    "collect_crawl_truncation_anomalies",
    "collect_corpus_coverage_anomalies",
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
EXECUTION_FIELDS: tuple[str, ...] = (
    "case_id",
    "cohort_ref",
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
    """

    battle_card: Path | None
    preliminary_findings: Path | None
    nda_questions: Path | None
    diff: Path | None
    manifest: Path
    i3_anomalies: list[dict[str, Any]] = field(default_factory=list)
    crawl_truncation_anomalies: list[dict[str, Any]] = field(default_factory=list)
    corpus_coverage_anomalies: list[dict[str, Any]] = field(default_factory=list)


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


def _crawl_control_digest(
    *,
    run_crawl: Any,
    app: Any,
    profiles: Mapping[str, Any] | None,
) -> str | None:
    """Стабильный дайджест **разрешённой** конфигурации контроля краула (N6/§13).

    Разрешает ``CrawlLimits`` цепочкой app (``deploy.yaml:crawl``) < run (``run.yaml:crawl``)
    < per-site (``domain_profiles``) — по одному «входу на каждый site key» плюс
    ``__default__`` (app<run без per-site). Канонизируется ``json.dumps(sort_keys=True)``
    и хешируется :func:`short_hash`. Смена осей на **любом** слое меняет дайджест → манифест
    помечает ``execution_change`` (§13), не ``methodology_change``.
    """
    from core.config.deploy import CrawlConfig

    app_config = app if isinstance(app, CrawlConfig) else CrawlConfig()
    payload: dict[str, Any] = {
        "__default__": resolve_crawl_limits(
            "", app=app_config, run=run_crawl, profiles={}
        ).as_dict()
    }
    site_profiles = dict(profiles or {})
    for key in sorted(site_profiles):
        url = str(key) if "://" in str(key) else f"https://{key}"
        payload[str(key)] = resolve_crawl_limits(
            url, app=app_config, run=run_crawl, profiles=site_profiles
        ).as_dict()
    material = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return short_hash(material, 24)


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
    registry_spec_digest: str | None = None,
) -> RunManifest:
    """Собирает полностью заполненный :class:`RunManifest` (R7/§13) фазы Ш9.

    Источники по приоритету (провенанс, а не пересчёт):

    * версии — ``inputs.run`` (обогащённый конфиг прогона), затем ``inputs.package`` и
      ``plan``; пустые значения не затирают уже известные (в т.ч. ``methodology_version``
      не откатывается к ``"0.0.0"``, если он есть у входа);
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

    Деплой-провенанс (``deploy_profile``/``edge_schema_version``) заполняется только если
    соответствующие поля есть в текущей схеме :class:`RunManifest` (defensive: иначе
    значения уходят в ``edge_capabilities``), что сохраняет совместимость при разносе
    правок между лейнами.

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
    catalog_version = _first_present(
        getattr(run, "catalog_version", None),
        getattr(plan, "catalog_version", None),
        getattr(inputs, "catalog_version", None),
    ) or "0.0.0"
    status_dict_version = _first_present(
        getattr(run, "status_dict_version", None),
        getattr(inputs, "status_dict_version", None),
    ) or "0.0.0"
    scoring_policy_version = _first_present(
        getattr(run, "scoring_policy_version", None),
        getattr(inputs, "scoring_policy_version", None),
    ) or "0.0.0"

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
    search_effort_budget_json = _embed_json(
        _first_present(
            getattr(run, "search_effort_budget_json", None),
            getattr(inputs, "search_effort_budget_json", None),
        )
    )

    llm_scenarios = list(
        _first_present(
            getattr(inputs, "llm_scenarios", None),
            getattr(plan, "llm_scenarios", None),
            getattr(run, "llm_scenarios", None),
        )
        or []
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
    crawl_document_level: str | None = None
    crawl_max_depth: int | None = None
    crawl_max_documents: int | None = None
    crawl_exclude_paths: list[str] = []
    crawl_exclude_documents: list[str] = []
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
    # P5: если run-слой не задал уровень явно — фиксируем **эффективный дефолт** app-слоя
    # (``CrawlConfig.document_level``, дефолт ``full``), чтобы манифест не молчал об оси.
    # ``crawl_app`` может быть не передан (модульные теста/исторические вызовы) — тогда
    # ``None`` (значение не выдумываем: A3).
    if crawl_document_level is None:
        from core.config.deploy import CrawlConfig

        if isinstance(crawl_app, CrawlConfig):
            crawl_document_level = crawl_app.document_level.value

    # N6/§13: дайджест **разрешённой** конфигурации контроля краула (app < run < per-site).
    crawl_config_digest = _crawl_control_digest(
        run_crawl=crawl_cfg,
        app=crawl_app,
        profiles=domain_profiles,
    )

    extra: dict[str, Any] = {}
    manifest_fields = getattr(RunManifest, "model_fields", {})
    if "deploy_profile" in manifest_fields:
        extra["deploy_profile"] = _first_present(
            deploy_profile,
            getattr(inputs, "deploy_profile", None),
            getattr(run, "deploy_profile", None),
        )
    if "edge_schema_version" in manifest_fields:
        extra["edge_schema_version"] = _first_present(
            edge_schema_version,
            getattr(inputs, "edge_schema_version", None),
            getattr(run, "edge_schema_version", None),
        )
    edge_caps = edge_capabilities if edge_capabilities is not None else getattr(
        inputs, "edge_capabilities", None
    )
    extra["edge_capabilities"] = dict(edge_caps or {})

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
            getattr(run, "cohort", None),
            getattr(plan, "cohort_ref", None),
        ),
        focus_modules=focus_modules,
        index_build_id=resolved_index_build_id,
        embed_model_id=_first_present(
            getattr(run, "embed_model_id", None),
            getattr(inputs, "embed_model_id", None),
        ),
        chunker_version=_first_present(
            getattr(run, "chunker_version", None),
            getattr(inputs, "chunker_version", None),
        ),
        extractor_version=_first_present(
            getattr(run, "extractor_version", None),
            getattr(inputs, "extractor_version", None),
        ),
        lexical_engine_version=_first_present(
            getattr(run, "lexical_engine_version", None),
            getattr(inputs, "lexical_engine_version", None),
        ),
        vector_store=_first_present(
            getattr(run, "vector_store", None),
            getattr(inputs, "vector_store", None),
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
        registry_spec_digest=registry_spec_digest,
        snapshot_date=snapshot_date,
        **extra,
    )


def write_run_manifest(manifest: RunManifest, out_dir: str | Path) -> Path:
    """Пишет ``run_manifest.yaml`` (R7, §9) в ``out_dir``; YAML, ``sort_keys``.

    Использует доменный :class:`RunManifest` (уже агрегирует версии, ``llm_scenarios``,
    индексную сборку, ретривал-профиль+K, бюджет, политику расчёта). Детерминирован:
    ``sort_keys=True`` + ``allow_unicode=True``.
    """
    directory = Path(out_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "run_manifest.yaml"
    payload = manifest.model_dump(mode="json")
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
    """
    exhaustions = exhaustions or {}
    anomalies: list[dict[str, Any]] = []
    for cell in _cell_list(cells):
        if getattr(cell, "status", None) is not Status.UNKNOWN:
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


def collect_crawl_truncation_anomalies(
    cells: Iterable[Any],
    truncated_products: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    """D1/G: аномалия «``exhaustive`` при усечённом крауле» для человеческой аттестации.

    Если cell несёт absence-evidence с ``source_scope: exhaustive``, а краул её продукта был
    **политико-усечён** (``max_documents``/``max_depth``, см.
    :func:`core.pipeline.crawl_session.finalize_session`), заявленная исчерпывающость охвата
    недоказуема — corpus неполон по построению. Это **аномалия**, а не смена статуса и не
    жёсткая блокировка: она требует повторной аттестации человеком (паттерн I3
    ``reviewed_by``). Возвращает детерминированный (``product_id, param_id``) список записей
    ``{"product_id", "param_id", "reason"}``.
    """
    truncated = {str(p) for p in (truncated_products or []) if p}
    if not truncated:
        return []
    anomalies: list[dict[str, Any]] = []
    for cell in _cell_list(cells):
        pid = str(getattr(cell, "product_id", "") or "")
        if pid not in truncated:
            continue
        absences = getattr(cell, "absence_evidence", None) or []
        declared = False
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
            "validation_notes": str(
                getattr(cell, "validation_notes", None)
                or getattr(cell, "notes", None)
                or ""
            ),
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


def diff_boundary_violations(prev: RunManifest, curr: RunManifest) -> list[str]:
    """Версионные границы §13, запрещающие межцикловый diff (список причин).

    §13/B-7 (v2.1): причина помечается классом изменения:

    * ``methodology_change`` — смена major ``methodology_version`` либо поля
      :data:`METHODOLOGY_FIELDS` (методологическая несовместимость расчёта);
    * ``execution_change`` — расхождение поля :data:`EXECUTION_FIELDS` (что именно
      исполнялось: когорта/индексная сборка/версии моделей/ретривал-профиль и т.п.).
    """
    violations: list[str] = []
    if _major(prev.methodology_version) != _major(curr.methodology_version):
        violations.append(
            "methodology_change: methodology_version major: "
            f"{prev.methodology_version} → {curr.methodology_version}"
        )
    for name in METHODOLOGY_FIELDS:
        if getattr(prev, name) != getattr(curr, name):
            violations.append(
                f"methodology_change: {name}: "
                f"{getattr(prev, name)!r} → {getattr(curr, name)!r}"
            )
    for name in EXECUTION_FIELDS:
        if _boundary_value(prev, name) != _boundary_value(curr, name):
            violations.append(
                f"execution_change: {name}: "
                f"{getattr(prev, name)!r} → {getattr(curr, name)!r}"
            )
    return violations


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


def render_diff(
    prev_manifest: RunManifest,
    curr_manifest: RunManifest,
    prev_cells: Iterable[Any],
    curr_cells: Iterable[Any],
    out_path: str | Path,
) -> Path:
    """Пишет ``diff.md`` — изменённые статусы + evidence_id (§9).

    Контракт §13: при смене major ``methodology_version``/поля :data:`METHODOLOGY_FIELDS`
    (класс ``methodology_change``) либо поля :data:`EXECUTION_FIELDS` (класс
    ``execution_change``) diff **отказывается** — :raises CrossCycleDiffError. Иначе
    пишется markdown-таблица изменений.
    """
    violations = diff_boundary_violations(prev_manifest, curr_manifest)
    if violations:
        raise CrossCycleDiffError(
            "§13: межцикловый diff запрещён — " + "; ".join(violations)
        )

    rows = _changed_rows(prev_cells, curr_cells)
    lines = [
        f"# diff: {prev_manifest.run_id} → {curr_manifest.run_id}",
        "",
        f"- methodology_version: {prev_manifest.methodology_version} → {curr_manifest.methodology_version}",
        f"- package_version: {prev_manifest.package_version} → {curr_manifest.package_version}",
        f"- catalog_version: {prev_manifest.catalog_version} → {curr_manifest.catalog_version}",
        f"- index_build_id: {prev_manifest.index_build_id} → {curr_manifest.index_build_id}",
        "",
        "| product | param | prev_status | curr_status | prev_evidence_id | curr_evidence_id |",
        "|---|---|---|---|---|---|",
    ]
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

    G4/I11: вызывается :func:`core.pipeline.compute.assert_publishable` (предупреждение);
    при полностью отсутствующем Coverage (``coverage.defined is False`` либо пустой
    знаменатель) battle card **не эмитится** (``Artifacts.battle_card is None``).

    I3 (§5): strict-``❓`` без полного протокола исчерпания фиксируются как аномалии
    (:attr:`Artifacts.i3_anomalies`) — статус ячейки при этом не меняется.
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

    nda_questions = render_nda_questions(
        cell_list,
        outputs_dir / "nda_questions.csv",
        exhaustions=exhaustions,
        crawl_truncation_anomalies=truncation_anomalies,
        corpus_coverage_anomalies=coverage_anomalies,
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

    return Artifacts(
        battle_card=battle_card,
        preliminary_findings=preliminary,
        nda_questions=nda_questions,
        diff=diff,
        manifest=manifest_path,
        i3_anomalies=anomalies,
        crawl_truncation_anomalies=truncation_anomalies,
        corpus_coverage_anomalies=coverage_anomalies,
    )
