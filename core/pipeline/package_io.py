"""coHound — низкоуровневый I/O артефактов пакета категории (итерация I1).

Модуль отвечает за **детерминированное** чтение и запись артефактов раскладки пакета
(``03`` §A.1): ``package.yaml``, ``catalog.csv``, ``baselines.csv``, ``cohort.yaml``,
``golden.csv``, ``domain_profiles.yaml``, ``scope_attestation.yaml``, ``run.yaml``.

Ключевые свойства
-----------------
* **Чтение** делегировано загрузчикам :mod:`core.config` (нет дублирования разбора).
  Отсутствующие артефакты → пустые значения/``None`` (частичный пакет читаем).
* **Запись** атомарна: временный файл в том же каталоге + :func:`os.replace`; при ошибке
  целевой файл не меняется.
* **Детерминизм/идемпотентность**: YAML — ``sort_keys=True, allow_unicode=True``; CSV —
  ``lineterminator="\\n"``, ``QUOTE_MINIMAL``, UTF-8 без BOM, строго фиксированный порядок
  колонок, строки отсортированы по ключу. Повторная запись того же состояния даёт
  байт-идентичные файлы.
* **Проспективный линт** (:func:`prospective_lint`): собирает :class:`RunInputs` в памяти и
  прогоняет блокирующий линтер — носитель главного инварианта любой мутации (см.
  :mod:`core.pipeline.package_admin`).

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
Допустимые зависимости слоя ``pipeline`` — ``domain``/``ports``/``pipeline``/``config``/
``util`` (01 §1); здесь ``ports`` не используются (I1 без сети).
"""

from __future__ import annotations

import csv
import datetime as _dt
import io
import json
import os
import re
import tempfile
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Union

import yaml
from pydantic import BaseModel

from core.config.run_config import (
    _BASELINE_COLUMNS,
    _CATALOG_COLUMNS,
    _GOLDEN_COLUMNS,
    load_baselines,
    load_catalog,
    load_cohort,
    load_domain_profiles,
    load_golden,
    load_package_manifest,
    load_run_config,
)
from core.domain.enums import AnchorRelation, Archetype, RunMode
from core.domain.models import (
    BaselineRow,
    CatalogRow,
    Cohort,
    CohortEntry,
    GoldenRow,
    ModuleDef,
    PackageManifest,
    RunConfig,
)
from core.pipeline.lint import LintReport, lint_run_inputs
from core.pipeline.normalize import RunInputs, cross_check, load_scope_attestation
from core.util.text_utils import normalize_cyrillic

__all__ = [
    "ARTIFACT_FILES",
    "CATALOG_WRITE_COLUMNS",
    "BASELINE_WRITE_COLUMNS",
    "GOLDEN_WRITE_COLUMNS",
    "PackageArtifacts",
    "PackageSummary",
    "atomic_write_text",
    "atomic_write_csv",
    "validate_case_id",
    "load_artifacts",
    "list_artifacts",
    "get_artifact",
    "save_artifact",
    "delete_artifact",
    "build_run_inputs",
    "prospective_lint",
    "assert_no_new_blocking",
    "append_apply_log",
    "APPLY_LOG_RELPATH",
    "matches",
    "scan_packages",
]

#: Канонический порядок CSV-колонок каталога при записи (обязательные + опц. §13).
CATALOG_WRITE_COLUMNS: tuple[str, ...] = _CATALOG_COLUMNS + ("deprecated", "replaced_by")
#: Канонический порядок CSV-колонок baselines при записи.
BASELINE_WRITE_COLUMNS: tuple[str, ...] = ("baseline_id", "param_id", "min_version", "inheritance_mode", "note")
#: Канонический порядок CSV-колонок golden при записи.
GOLDEN_WRITE_COLUMNS: tuple[str, ...] = _GOLDEN_COLUMNS + ("tier", "note")

#: Имя файла артефакта по внутреннему имени (ключи :class:`PackageArtifacts`).
ARTIFACT_FILES: dict[str, str] = {
    "package": "package.yaml",
    "catalog": "catalog.csv",
    "baselines": "baselines.csv",
    "cohort": "cohort.yaml",
    "golden": "golden.csv",
    "domain_profiles": "domain_profiles.yaml",
    "scope_attestation": "scope_attestation.yaml",
    "run": "run.yaml",
}

#: Относительный путь журнала авторских мутаций (append-only, собственный контур I1).
APPLY_LOG_RELPATH = ".authoring/apply_log.jsonl"

#: ``case_id`` — slug строго из ``[a-z0-9_]`` (защита path-конфайнмента).
_CASE_ID_RE = re.compile(r"^[a-z0-9_]+$")

# Тип значения ячейки CSV.
_Cell = Union[str, None]


@dataclass
class PackageArtifacts:
    """Прочитанные артефакты пакета; отсутствующие — пустые/``None``.

    Единый in-memory носитель состояния, над которым работают CRUD-операции
    (:mod:`core.pipeline.package_admin`): сначала мутируется **копия**, затем
    проспективно линтуется, и только при отсутствии новых блокирующих нарушений
    записывается на диск.
    """

    manifest: PackageManifest | None = None
    catalog: list[CatalogRow] = field(default_factory=list)
    baselines: list[BaselineRow] = field(default_factory=list)
    cohort: Cohort | None = None
    golden: list[GoldenRow] = field(default_factory=list)
    domain_profiles: dict[str, Any] = field(default_factory=dict)
    scope_attestation: str | None = None
    run: RunConfig | None = None


# ---------------------------------------------------------------------------
# Скан каталога пакетов и поиск/фильтрация (read-only)
# ---------------------------------------------------------------------------


def _fold(text: str) -> str:
    """Unicode-устойчивая свёртка для поиска: ``casefold`` + ё→е (RU)."""
    return normalize_cyrillic(text.casefold())


def matches(text: str | None, query: str | None) -> bool:
    """Регистронезависимый unicode-устойчивый поиск подстроки ``query`` в ``text``.

    Свёртка через :meth:`str.casefold` и :func:`~core.util.text_utils.normalize_cyrillic`
    (ё→е) делает сравнение устойчивым к регистру/диакритике русских вхождений. Пустой/
    отсутствующий ``query`` → ``True`` (фильтр не активен). ``text is None`` при непустом
    ``query`` → ``False``.
    """
    if not query:
        return True
    if text is None:
        return False
    return _fold(query) in _fold(text)


@dataclass
class PackageSummary:
    """Сводка по одному пакету категории для обзора ``hound package list`` (read-only).

    JSON-совместима (:meth:`as_dict`): счётчики составных элементов, версия/архетип и
    итог проспективного линта (``ok``). ``error`` непуст, если пакет не читается/не
    разбирается — такой подкаталог не валит общий скан, а помечается.
    """

    path: str
    case_id: str | None
    archetype: str | None
    package_version: str | None
    modules: int
    roles: int
    catalog: int
    baselines: int
    golden: int
    cohort: int
    ok: bool
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление сводки."""
        return {
            "path": self.path,
            "case_id": self.case_id,
            "archetype": self.archetype,
            "package_version": self.package_version,
            "modules": self.modules,
            "roles": self.roles,
            "catalog": self.catalog,
            "baselines": self.baselines,
            "golden": self.golden,
            "cohort": self.cohort,
            "ok": self.ok,
            "error": self.error,
        }


def _one_line(exc: BaseException) -> str:
    """Сжимает сообщение исключения в одну строку (для ``PackageSummary.error``)."""
    return " ".join(str(exc).split()) or type(exc).__name__


def _summarize_package(entry: Path) -> PackageSummary:
    """Строит :class:`PackageSummary` по каталогу пакета; ошибки разбора не поднимает."""
    try:
        artifacts = load_artifacts(entry)
        report = prospective_lint(artifacts)
    except (ValueError, OSError) as exc:
        return PackageSummary(
            path=str(entry),
            case_id=entry.name,
            archetype=None,
            package_version=None,
            modules=0,
            roles=0,
            catalog=0,
            baselines=0,
            golden=0,
            cohort=0,
            ok=False,
            error=_one_line(exc),
        )
    manifest = artifacts.manifest
    return PackageSummary(
        path=str(entry),
        case_id=manifest.case_id if manifest is not None else entry.name,
        archetype=manifest.archetype.value if manifest is not None else None,
        package_version=manifest.package_version if manifest is not None else None,
        modules=len(manifest.modules) if manifest is not None else 0,
        roles=len(manifest.roles) if manifest is not None else 0,
        catalog=len(artifacts.catalog),
        baselines=len(artifacts.baselines),
        golden=len(artifacts.golden),
        cohort=len(artifacts.cohort.entries) if artifacts.cohort is not None else 0,
        ok=report.ok,
    )


def scan_packages(
    dir: str | Path = "packages",
    *,
    include_template: bool = False,
    query: str | None = None,
) -> list[PackageSummary]:
    """Обходит непосредственные подкаталоги ``dir`` с ``package.yaml`` и даёт сводку.

    Порядок детерминирован по ``case_id`` (fallback — путь). Каталог ``_template``
    исключается по умолчанию (``include_template=True`` включает его). Подкаталоги без
    ``package.yaml`` пропускаются; нечитаемые/нечётные пакеты не валят скан — попадают в
    результат с ``ok=False`` и непустым ``error``. ``query`` фильтрует по
    ``case_id``/``archetype``/``path``/``package_version`` (:func:`matches`). Ничего не пишет.
    """
    base = Path(dir)
    if not base.is_dir():
        return []
    summaries: list[PackageSummary] = []
    for entry in sorted(base.iterdir(), key=lambda path: path.name):
        if not entry.is_dir():
            continue
        if entry.name == "_template" and not include_template:
            continue
        if not (entry / "package.yaml").is_file():
            continue
        summaries.append(_summarize_package(entry))
    if query:
        fields = ("case_id", "archetype", "path", "package_version")
        summaries = [
            summary
            for summary in summaries
            if any(matches(getattr(summary, field), query) for field in fields)
        ]
    return sorted(summaries, key=lambda summary: (summary.case_id or "", summary.path))


# ---------------------------------------------------------------------------
# Атомарная и детерминированная запись
# ---------------------------------------------------------------------------


def atomic_write_text(path: str | Path, text: str) -> Path:
    """Атомарно пишет ``text`` в ``path`` (temp в том же каталоге + ``os.replace``).

    При любой ошибке временный файл удаляется, а целевой не меняется. Родительский
    каталог создаётся при необходимости.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(target.parent), prefix=f".{target.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        os.replace(tmp_name, target)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    return target


def atomic_write_csv(path: str | Path, header: Sequence[str], rows: Iterable[Sequence[Any]]) -> Path:
    """Атомарно пишет детерминированный CSV (UTF-8 без BOM, ``\\n``).

    ``lineterminator="\\n"`` и ``QUOTE_MINIMAL`` дают стабильные байты на всех системах;
    порядок колонок задаёт вызывающий (строго из ``*_WRITE_COLUMNS``).
    """
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(list(header))
    for row in rows:
        writer.writerow(["" if value is None else value for value in row])
    return atomic_write_text(path, buffer.getvalue())


def _dump_yaml(data: Any) -> str:
    """Сериализует YAML детерминированно (сортировка ключей, unicode без экранирования)."""
    return yaml.safe_dump(data, allow_unicode=True, sort_keys=True, default_flow_style=False)


def _mapping_to_yaml(data: Any) -> str:
    """YAML-строка для доменной модели (JSON-режим ``model_dump`` — сериализуемые типы)."""
    return _dump_yaml(data.model_dump(mode="json", exclude_none=False))


# ---------------------------------------------------------------------------
# Path-конфайнмент
# ---------------------------------------------------------------------------


def validate_case_id(case_id: str) -> str:
    """Проверяет ``case_id`` на соответствие ``[a-z0-9_]+``; возвращает его же.

    :raises ValueError: пустой/недопустимый идентификатор.
    """
    if not case_id or not _CASE_ID_RE.match(case_id):
        raise ValueError(f"case_id должен соответствовать [a-z0-9_]+: {case_id!r}")
    return case_id


def _safe_join(base: str | Path, *parts: str) -> Path:
    """Соединяет пути и гарантирует, что результат лежит внутри ``base``.

    Защищает от ``..`` и абсолютных вставок: после ``resolve()`` результат обязан быть
    самим ``base`` или его потомком. Иначе — :class:`ValueError`.
    """
    base_resolved = Path(base).resolve()
    candidate = base_resolved.joinpath(*parts).resolve()
    if candidate != base_resolved and base_resolved not in candidate.parents:
        raise ValueError(f"путь выходит за пределы {base_resolved}: {candidate}")
    return candidate


# ---------------------------------------------------------------------------
# Хелперы сериализации CSV-строк
# ---------------------------------------------------------------------------


def _catalog_cells(row: CatalogRow, include_deprecated: bool) -> list[_Cell]:
    cells: list[_Cell] = [
        row.param_id,
        row.title,
        row.module,
        row.group,
        row.importance.value,
        row.param_type.value,
        row.rule,
        row.absence_rule.value,
        row.role_binding or "",
        json.dumps(row.concept_terms, ensure_ascii=False, sort_keys=True) if row.concept_terms else "",
        "|".join(row.search_langs),
    ]
    if include_deprecated:
        cells.extend(["true" if row.deprecated else "false", row.replaced_by or ""])
    return cells


def _baseline_cells(row: BaselineRow) -> list[_Cell]:
    return [
        row.baseline_id,
        row.param_id,
        row.min_version or "",
        row.inheritance_mode.value,
        row.note or "",
    ]


def _golden_cells(row: GoldenRow) -> list[_Cell]:
    return [
        row.product_id,
        row.param_id,
        row.expected_status.value,
        "|".join(kind.value for kind in row.expected_evidence_kind),
        row.case_class,
        row.tier.value,
        row.note or "",
    ]


def _sorted_catalog(rows: Sequence[CatalogRow]) -> list[CatalogRow]:
    return sorted(rows, key=lambda row: row.param_id)


def _sorted_baselines(rows: Sequence[BaselineRow]) -> list[BaselineRow]:
    return sorted(rows, key=lambda row: (row.baseline_id, row.param_id))


def _sorted_golden(rows: Sequence[GoldenRow]) -> list[GoldenRow]:
    return sorted(rows, key=lambda row: (row.product_id, row.param_id, row.tier.value, row.case_class))


# ---------------------------------------------------------------------------
# Чтение / запись артефактов
# ---------------------------------------------------------------------------


def load_artifacts(package_dir: str | Path) -> PackageArtifacts:
    """Читает все присутствующие артефакты пакета; отсутствующие → пустые/``None``.

    Обязательные для прогона артефакты могут отсутствовать — это не ошибка чтения:
    частичный пакет (например, только ``package.yaml`` во время авторской правки)
    остаётся читаемым, а полноту проверяет проспективный линт/валидатор.

    :raises ValueError: присутствующий артефакт не разбирается/невалиден.
    """
    base = Path(package_dir)
    artifacts = PackageArtifacts()

    package_file = base / "package.yaml"
    if package_file.is_file():
        artifacts.manifest = load_package_manifest(package_file)

    catalog_file = base / "catalog.csv"
    if catalog_file.is_file():
        artifacts.catalog = load_catalog(catalog_file)

    baselines_file = base / "baselines.csv"
    if baselines_file.is_file():
        artifacts.baselines = load_baselines(baselines_file)

    cohort_file = base / "cohort.yaml"
    if cohort_file.is_file():
        artifacts.cohort = load_cohort(cohort_file)

    golden_file = base / "golden.csv"
    if golden_file.is_file():
        artifacts.golden = load_golden(golden_file)

    profiles_file = base / "domain_profiles.yaml"
    if profiles_file.is_file():
        artifacts.domain_profiles = load_domain_profiles(profiles_file)

    artifacts.scope_attestation = load_scope_attestation(base)

    run_file = base / "run.yaml"
    if run_file.is_file():
        artifacts.run = load_run_config(run_file)

    return artifacts


def _artifact_value(artifacts: PackageArtifacts, name: str) -> Any:
    """Доменное/сырое значение артефакта по имени (для :func:`get_artifact`)."""
    return {
        "package": artifacts.manifest,
        "catalog": artifacts.catalog,
        "baselines": artifacts.baselines,
        "cohort": artifacts.cohort,
        "golden": artifacts.golden,
        "domain_profiles": artifacts.domain_profiles,
        "scope_attestation": artifacts.scope_attestation,
        "run": artifacts.run,
    }[name]


def _jsonable(value: Any) -> Any:
    """Приводит доменное значение к JSON-совместимому виду (``model_dump(mode="json")``).

    Pydantic-модели (в т.ч. внутри списков) сериализуются рекурсивно; ``dict``/``str``/
    скаляры возвращаются как есть. ``None`` сохраняется (артефакт отсутствует).
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    return value


def _artifact_count(artifacts: PackageArtifacts, name: str) -> int | None:
    """Число элементов артефакта (строк/записей) или ``None``, если он отсутствует."""
    if name == "package":
        return len(artifacts.manifest.modules) if artifacts.manifest is not None else None
    if name == "catalog":
        return len(artifacts.catalog)
    if name == "baselines":
        return len(artifacts.baselines)
    if name == "golden":
        return len(artifacts.golden)
    if name == "cohort":
        return len(artifacts.cohort.entries) if artifacts.cohort is not None else None
    if name == "domain_profiles":
        return len(artifacts.domain_profiles)
    if name == "scope_attestation":
        return 1 if artifacts.scope_attestation is not None else 0
    if name == "run":
        return len(artifacts.run.products) if artifacts.run is not None else None
    return None  # pragma: no cover — исчерпано ARTIFACT_FILES


def list_artifacts(package_dir: str | Path) -> list[dict[str, Any]]:
    """Обзор всех 8 артефактов пакета (read-only): присутствие, размер, счётчик, путь.

    Порядок совпадает с :data:`ARTIFACT_FILES` (детерминирован). ``count`` — число
    строк/записей артефакта (для ``package`` — число модулей; ``cohort`` — число записей;
    ``run`` — число продуктов; ``scope_attestation`` — ``1``/``0``), либо ``None``, если
    файл отсутствует или пакет не разбирается. Ничего не пишет; ошибки разбора не
    поднимаются — обзор остаётся доступным по файловой системе.
    """
    base = Path(package_dir)
    artifacts: PackageArtifacts | None
    try:
        artifacts = load_artifacts(base)
    except (ValueError, OSError):
        artifacts = None
    items: list[dict[str, Any]] = []
    for name, filename in ARTIFACT_FILES.items():
        path = base / filename
        present = path.is_file()
        try:
            size = path.stat().st_size if present else 0
        except OSError:  # pragma: no cover — гонка ФС; не валим обзор
            size = 0
        count = _artifact_count(artifacts, name) if (present and artifacts is not None) else None
        items.append(
            {
                "name": name,
                "file": filename,
                "path": str(path),
                "present": present,
                "size": size,
                "count": count,
            }
        )
    return items


def get_artifact(
    package_dir: str | Path, name: str, *, raw: bool = False
) -> tuple[Any, Path]:
    """Читает один артефакт пакета (read-only) → ``(data, path)``.

    :param name: одно из :data:`ARTIFACT_FILES`.
    :param raw: ``True`` — сырой текст файла (``str``); ``False`` — структурированный
        JSON-совместимый вид (``model_dump(mode="json")`` для доменных моделей, ``dict``
        для ``domain_profiles``, ``str`` для ``scope_attestation``).
    :returns: ``(None, path)``, если файл отсутствует (значение не выдумывается).
    :raises ValueError: неизвестное имя артефакта или ошибка чтения файла.
    """
    if name not in ARTIFACT_FILES:
        raise ValueError(f"неизвестный артефакт: {name}")
    path = Path(package_dir) / ARTIFACT_FILES[name]
    if raw:
        if not path.is_file():
            return None, path
        try:
            return path.read_text(encoding="utf-8"), path
        except OSError as exc:
            raise ValueError(f"не удалось прочитать {path}: {exc}") from exc
    artifacts = load_artifacts(package_dir)
    return _jsonable(_artifact_value(artifacts, name)), path


def save_artifact(package_dir: str | Path, name: str, data: Any) -> Path:
    """Детерминированно сериализует артефакт ``name`` в каталог пакета.

    YAML-артефакты (``package``/``cohort``/``run``/``domain_profiles``/
    ``scope_attestation``) пишутся через :func:`_dump_yaml`; табличные
    (``catalog``/``baselines``/``golden``) — CSV строго по каноническим колонкам.

    :param name: одно из :data:`ARTIFACT_FILES`.
    :param data: доменная модель (или ``list`` строк, ``dict``/``str`` для опциональных).
    :raises ValueError: неизвестное имя артефакта или неверный тип данных.
    """
    if name not in ARTIFACT_FILES:
        raise ValueError(f"неизвестный артефакт: {name}")
    path = _safe_join(package_dir, ARTIFACT_FILES[name])

    if name == "package":
        if not isinstance(data, PackageManifest):
            raise ValueError("package: ожидался PackageManifest")
        return atomic_write_text(path, _mapping_to_yaml(data))
    if name == "cohort":
        if not isinstance(data, Cohort):
            raise ValueError("cohort: ожидался Cohort")
        return atomic_write_text(path, _mapping_to_yaml(data))
    if name == "run":
        if not isinstance(data, RunConfig):
            raise ValueError("run: ожидался RunConfig")
        return atomic_write_text(path, _mapping_to_yaml(data))
    if name == "domain_profiles":
        if not isinstance(data, dict):
            raise ValueError("domain_profiles: ожидался dict")
        return atomic_write_text(path, _dump_yaml(data))
    if name == "scope_attestation":
        if not isinstance(data, str):
            raise ValueError("scope_attestation: ожидалась строка процедуры")
        return atomic_write_text(path, _dump_yaml({"procedure": data}))

    rows = list(data)
    if name == "catalog":
        include_deprecated = any(row.deprecated or row.replaced_by for row in rows)
        header = CATALOG_WRITE_COLUMNS if include_deprecated else _CATALOG_COLUMNS
        return atomic_write_csv(
            path, header, [_catalog_cells(row, include_deprecated) for row in _sorted_catalog(rows)]
        )
    if name == "baselines":
        return atomic_write_csv(path, BASELINE_WRITE_COLUMNS, [_baseline_cells(row) for row in _sorted_baselines(rows)])
    if name == "golden":
        return atomic_write_csv(path, GOLDEN_WRITE_COLUMNS, [_golden_cells(row) for row in _sorted_golden(rows)])
    raise ValueError(f"артефакт без сериализатора: {name}")  # pragma: no cover — исчерпано выше


def delete_artifact(package_dir: str | Path, name: str) -> bool:
    """Удаляет файл артефакта ``name`` (path-конфайнмент). ``True``, если он был.

    Нужен для операций «отмены» (например, ``clear_scope_attestation``): отсутствие
    файла — не ошибка (возвращается ``False``).
    """
    if name not in ARTIFACT_FILES:
        raise ValueError(f"неизвестный артефакт: {name}")
    path = _safe_join(package_dir, ARTIFACT_FILES[name])
    if path.is_file():
        path.unlink()
        return True
    return False


# ---------------------------------------------------------------------------
# Проспективный линт (главный инвариант мутаций)
# ---------------------------------------------------------------------------


def _synthesized_cohort(case_id: str) -> Cohort:
    """Синтетическая когорта из одного якоря — для частичных пакетов без ``cohort.yaml``."""
    return Cohort(
        case_id=case_id,
        frozen_at=_dt.date(1970, 1, 1),
        entries=[
            CohortEntry(
                product_id="anchor",
                role_overlap=1.0,
                selection_reason="синтетический якорь (пакет без cohort.yaml)",
                anchor_relation=AnchorRelation.ANCHOR,
            )
        ],
    )


def _synthesized_manifest(case_id: str) -> PackageManifest:
    """Минимальный валидный манифест (Σ весов = 100) для частичных пакетов."""
    return PackageManifest(
        case_id=case_id,
        package_version="0.0.0",
        archetype=Archetype.B,
        modules=[ModuleDef(module_id="core", weight=100.0)],
        roles=[],
        baselines=[],
        baseline_participant=None,
    )


def _synthesized_run(case_id: str, cohort: Cohort) -> RunConfig:
    """Синтетический ``run.yaml`` (products = когорта, anchor ∈ products) для линта."""
    products = [entry.product_id for entry in cohort.entries]
    anchor = next(
        (entry.product_id for entry in cohort.entries if entry.anchor_relation is AnchorRelation.ANCHOR),
        products[0],
    )
    return RunConfig(
        case_id=case_id,
        anchor=anchor,
        cohort=f"cohort_{case_id}",
        products=products,
        package=case_id,
        snapshot_date=cohort.frozen_at,
        mode=RunMode.STRICT,
    )


def build_run_inputs(artifacts: PackageArtifacts) -> RunInputs:
    """Собирает :class:`RunInputs` in-memory из артефактов (без обращения к диску).

    Если ``run``/``manifest``/``cohort`` отсутствуют (частичный пакет), синтезируется
    минимальное валидное состояние — чтобы линт давал осмысленный результат даже для
    неполного каталога.
    """
    case_id = artifacts.manifest.case_id if artifacts.manifest is not None else "package"
    cohort = artifacts.cohort if artifacts.cohort is not None else _synthesized_cohort(case_id)
    manifest = artifacts.manifest if artifacts.manifest is not None else _synthesized_manifest(case_id)
    run = artifacts.run if artifacts.run is not None else _synthesized_run(case_id, cohort)
    return RunInputs(
        run=run,
        package=manifest,
        catalog=list(artifacts.catalog),
        baselines=list(artifacts.baselines),
        cohort=cohort,
        golden=list(artifacts.golden),
        domain_profiles=dict(artifacts.domain_profiles),
        scope_attestation_procedure=artifacts.scope_attestation,
    )


def prospective_lint(artifacts: PackageArtifacts) -> LintReport:
    """Проспективный линт: ``RunInputs`` → :func:`lint_run_inputs` + ``cross_check`` (S11).

    Возвращает :class:`LintReport`, где ``blocking`` — объединение блокирующих правил и
    кросс-артефактных нарушений (без дублей). Warnings допускаются и печатаются вызывающим.
    """
    inputs = build_run_inputs(artifacts)
    report = lint_run_inputs(inputs)
    for violation in cross_check(inputs):
        if violation not in report.blocking:
            report.blocking.append(violation)
    return report


def _blocking_messages(report: Union[LintReport, Sequence[str]]) -> list[str]:
    if isinstance(report, LintReport):
        return list(report.blocking)
    return list(report)


def assert_no_new_blocking(
    before: Union[LintReport, Sequence[str]], after: Union[LintReport, Sequence[str]]
) -> list[str]:
    """Предикат: возвращает **новые** блокирующие нарушения (``after`` минус ``before``).

    Исключение не поднимается — пустой список означает, что инвариант «мутация не
    добавила blocking» выполнен. Решение об отказе принимает
    :mod:`core.pipeline.package_admin`.
    """
    known = set(_blocking_messages(before))
    return [message for message in _blocking_messages(after) if message not in known]


# ---------------------------------------------------------------------------
# Журнал авторских мутаций (append-only)
# ---------------------------------------------------------------------------


def append_apply_log(package_dir: str | Path, entry: dict[str, Any]) -> Path:
    """Добавляет одну JSON-строку в ``.authoring/apply_log.jsonl`` (атомарно).

    Собственный аудит-контур авторских правок пакета (не ``gate_decisions``): каждая
    строка — ``json.dumps(entry, ensure_ascii=False, sort_keys=True)`` + ``\\n``. Запись
    идёт через temp+replace в том же каталоге, поэтому частичная строка не появляется.
    """
    path = _safe_join(package_dir, *APPLY_LOG_RELPATH.split("/"))
    line = json.dumps(entry, ensure_ascii=False, sort_keys=True, default=str) + "\n"
    existing = ""
    if path.is_file():
        existing = path.read_text(encoding="utf-8")
    return atomic_write_text(path, existing + line)
