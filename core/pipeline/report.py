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
* **§13** — межцикловый ``diff`` запрещён при смене major ``methodology_version``
  либо при расхождении cohort/profile+K/retrieval_params/index_build: контракт —
  **отказ** (:class:`CrossCycleDiffError`).

Модуль категорийно нейтрален (§1.3), детерминирован (сортировка продуктов/параметров).
``openpyxl`` — опциональная зависимость: при её отсутствии XLSX-артефакты
деградируют до CSV (суффикс ``.csv``) с тем же логическим составом; ``csv``/``yaml`` —
stdlib.
"""

from __future__ import annotations

import csv
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from core.domain.enums import CellState, RankingPolicy, Status
from core.domain.models import CatalogRow, Cell, RunManifest
from core.domain.scoring import evidence_coverage
from core.pipeline.compute import (
    ScoreBundle,
    assert_publishable,
    publish_warnings,
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
    "DIFF_BOUNDARY_FIELDS",
    "build_manifest",
    "write_run_manifest",
    "render_battle_card",
    "render_nda_questions",
    "render_preliminary_findings",
    "render_diff",
    "write_reports",
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

#: Поля манифеста, смена которых запрещает межцикловый diff (§13 C.2):
#: смена когорты/индексной сборки/ретривал-профиля+K/fusion-параметров. Major
#: ``methodology_version`` проверяется отдельно.
DIFF_BOUNDARY_FIELDS: tuple[str, ...] = (
    "case_id",
    "cohort_ref",
    "index_build_id",
    "retrieval_profile",
    "retrieval_cutoff_K",
    "retrieval_params_json",
)


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
    """

    battle_card: Path | None
    preliminary_findings: Path | None
    nda_questions: Path | None
    diff: Path | None
    manifest: Path


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
        product_id: evidence_coverage(group, catalog, {}).value
        for product_id, group in grouped.items()
    }


def _coverage_absent(bundle: ScoreBundle) -> bool:
    """I11: Coverage отсутствует полностью (нечего сопровождать Capability)."""
    return bool(publish_warnings(bundle))


def _coverage_insufficient(bundle: ScoreBundle) -> bool:
    """Покрытие недостаточно для неоглашённой публикации (приглушение, §9).

    Гейт завязан на **Coverage** (I11): Comparability — лишь аннотация в шапке, и её
    ``INSUFFICIENT`` из-за отсутствия когорты не должен «глушить» полноценное покрытие.
    """
    return bundle.coverage.value < COVERAGE_INSUFFICIENT_THRESHOLD


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


def build_manifest(
    *,
    inputs: RunInputs,
    plan: RunPlan,
    bundle: ScoreBundle | None = None,
    index_build_id: str | None = None,
    deploy_profile: str | None = None,
    edge_schema_version: str | None = None,
    edge_capabilities: Mapping[str, Any] | None = None,
    created_at: str | None = None,
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
      ``lexical_engine_version``/``vector_store``).

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
        getattr(run, "retrieval_profile", None),
        getattr(inputs, "retrieval_profile", None),
        getattr(plan, "retrieval_profile", None),
    )
    retrieval_cutoff_K = _coerce_int(
        _first_present(
            getattr(run, "retrieval_cutoff_K", None),
            getattr(inputs, "retrieval_cutoff_K", None),
            getattr(plan, "retrieval_cutoff_K", None),
        )
    )
    retrieval_params_json = _embed_json(
        _first_present(
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
    bundle: ScoreBundle, fallback: bool, insufficient: bool
) -> list[tuple[str, Any]]:
    """Шапка battle card: метрики + аннотации (приглушение/fallback)."""
    reasons = "; ".join(bundle.comparability.reasons)
    rows: list[tuple[str, Any]] = [
        ("capability", round(bundle.capability.value, 6)),
        ("capability_n_used", bundle.capability.n_used),
        ("coverage", round(bundle.coverage.value, 6)),
        ("coverage_n_used", bundle.coverage.n_used),
        ("coverage_denominator", bundle.coverage.denominator),
        ("failure_rate", round(bundle.failure_rate, 6)),
        ("comparability_level", bundle.comparability.level),
        ("comparability_min_coverage", round(bundle.comparability.min_coverage, 6)),
        ("comparability_cross_cycle", bundle.comparability.cross_cycle),
        ("comparability_reasons", reasons),
        ("requested_ranking", bundle.requested_ranking),
        ("computed_ranking", bundle.computed_ranking),
        ("coverage_insufficient", insufficient),
    ]
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
) -> list[tuple[str, list[list[Any]]]]:
    """Собирает листы battle card (шапка/матрица/сателлиты/модули/sensitivity)."""
    by_key = {(str(c.product_id), str(c.param_id)): c for c in cells}

    header_rows: list[list[Any]] = [["field", "value"]]
    header_rows.extend(
        [name, value] for name, value in _battle_card_header(bundle, fallback, insufficient)
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
) -> Path:
    """Пишет ``battle_card`` (XLSX; CSV-fallback) с G4/I11-гейтом (§9, 02 §E.2).

    :raises PublishBlockedError: если Coverage отсутствует полностью (I11).
    :returns: путь записанного артефакта (``.xlsx`` либо ``.csv`` при отсутствии
        ``openpyxl``).

    При недостаточном Coverage артефакт **приглушается** и помечается
    :data:`INSUFFICIENT_COVERAGE_NOTE`; при демотировании продукты упорядочиваются по
    Coverage и добавляется :data:`FALLBACK_ORDER_LABEL` (K6/A3).
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
) -> Path:
    """Пишет ``nda_questions.csv`` — все ``❓`` (продукт, параметр, исчерпанные контуры).

    ``exhaustions`` — опциональная карта ``(product_id, param_id) → ProtocolExhaustion``
    (``contours`` заполняются, если доступны; §5/S13). Фильтр строго ``status == ❓``.
    """
    rows: list[dict[str, Any]] = []
    for cell in _cell_list(cells):
        if getattr(cell, "status", None) is not Status.UNKNOWN:
            continue
        key = (str(cell.product_id), str(cell.param_id))
        contours = ""
        if exhaustions and key in exhaustions:
            contours = ",".join(_contours(exhaustions[key]))
        rows.append(
            {
                "product_id": cell.product_id,
                "param_id": cell.param_id,
                "status": Status.UNKNOWN.value,
                "exhausted_contours": contours,
            }
        )
    return _write_csv(
        Path(out_path),
        ["product_id", "param_id", "status", "exhausted_contours"],
        rows,
    )


def _contours(exhaustion: Any) -> list[str]:
    """Контуры исчерпания из ``ProtocolExhaustion``/dict, строкой."""
    if isinstance(exhaustion, Mapping):
        values = exhaustion.get("contours") or []
    else:
        values = getattr(exhaustion, "contours", None) or []
    return [str(v) for v in values]


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


def diff_boundary_violations(prev: RunManifest, curr: RunManifest) -> list[str]:
    """Версионные границы §13, запрещающие межцикловый diff (список причин)."""
    violations: list[str] = []
    if _major(prev.methodology_version) != _major(curr.methodology_version):
        violations.append(
            "methodology_version major: "
            f"{prev.methodology_version} → {curr.methodology_version}"
        )
    for name in DIFF_BOUNDARY_FIELDS:
        if getattr(prev, name) != getattr(curr, name):
            violations.append(f"{name}: {getattr(prev, name)!r} → {getattr(curr, name)!r}")
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

    Контракт §13: при смене major ``methodology_version`` либо версионной границы
    (cohort/profile+K/retrieval_params/index_build) diff **отказывается** —
    :raises CrossCycleDiffError. Иначе пишется markdown-таблица изменений.
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
) -> Artifacts:
    """Пишет все артефакты прогона в ``{out_dir}/{run_id}/outputs/`` (Ш9, 03 §A.2).

    :param out_dir: базовый каталог прогонов (например, ``runs/``).
    :param run_id: идентификатор прогона; ``run_manifest.yaml`` — в ``{out_dir}/{run_id}/``.
    :param prev_manifest/prev_cells: предыдущий цикл для ``diff.md`` (опционально; без
        них пишется заглушка).

    G4/I11: вызывается :func:`core.pipeline.compute.assert_publishable` (предупреждение);
    при полностью отсутствующем Coverage battle card **не эмитится**
    (``Artifacts.battle_card is None``).
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
            bundle, cell_list, catalog, outputs_dir / "battle_card.xlsx"
        )
    except PublishBlockedError:
        battle_card = None

    nda_questions = render_nda_questions(cell_list, outputs_dir / "nda_questions.csv")
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

    return Artifacts(
        battle_card=battle_card,
        preliminary_findings=preliminary,
        nda_questions=nda_questions,
        diff=diff,
        manifest=manifest_path,
    )
