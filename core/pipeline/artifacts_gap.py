"""``artifacts_gap.yaml`` — список недоступных артефактов как **вход** следующего прогона.

**Зачем файл.** Провалы фетчей видны в отчёте только агрегатами
(``CrawlSessionProvenance.failed``, ``blocked = {error_kind: count}``,
``unreadable_origins``): ни URL, ни per-URL статуса. Оператору нужен не ответ «сколько
отказано», а ответ «какие документы взять руками и куда их положить» — иначе пробел от
robots/таймаутов закрывается подписью риска (``scope_attested_by``, I2c), а не
предъявлением содержимого (``13-manual-artifacts.md`` §4).

**Что это.** Готовый (заполняемый) вход, а не справка: каждый элемент ``gaps`` несёт
заготовку ``provide`` с ``path: null``/``provided_by: null`` — оператор заполняет два
поля, и следующий прогон закрывает пробел, оставляя факт отказа в аудите.

**Источник.** Только персистентные ``crawl_session_items`` (read-only): ни сети, ни
wall-clock. Единственная метка времени — ``last_attempt`` из БД (он там персистентный).

**Состав.** В ``gaps`` идут **терминальные** ``failed`` с ``error_kind`` из
:data:`~core.domain.enums.BLOCKING_FAILURE_KINDS` — ровно те, что делают покрытие
неполным по построению (Ф0/I2c). Прочие ``failed`` (``not_found``,
``source_unavailable``, ``redirect_out_of_scope`` и т.п.) в список не идут, но видны в
``summary.excluded_by_kind``: короткий список не должен читаться как «всё скачалось».

**Форма стабильна.** Пустой список — валидный результат: файл пишется всегда
(``summary.total: 0``). Детерминизм: сортировка по ``(product_id, error_kind, url)``,
все словари ``summary`` — с сортированными ключами; запись атомарна
(:func:`core.util.atomic_write.atomic_write_text`), поэтому убитый процесс оставляет
либо прежний, либо новый файл, но не половину.

**Один генератор, два вызова.** Фазы ``corpus`` (оператор останавливается на
``--until corpus`` — файл обязан быть на месте до дорогой Ш6) и ``report``
(артефакт в :class:`~core.pipeline.report.Artifacts`, обновляется при повторной
выгрузке) вызывают :func:`write_artifacts_gap`; форма не может разойтись между ними.

Нейтрален (§1.3): нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

from core.domain.enums import BLOCKING_FAILURE_KINDS, CrawlItemStatus, FailureKind
from core.domain.models import CrawlSessionItem
from core.util.atomic_write import atomic_write_text

__all__ = [
    "ARTIFACTS_GAP_FILENAME",
    "ARTIFACTS_GAP_SCHEMA",
    "ArtifactGap",
    "ArtifactsGapReport",
    "artifacts_gap_path",
    "collect_artifact_gaps",
    "read_run_items",
    "write_artifacts_gap",
]

#: Версия схемы файла (меняется только при несовместимой смене формы).
ARTIFACTS_GAP_SCHEMA = "artifacts_gap/1"

#: Имя файла в ``runs/{run_id}/outputs/`` (03 §A.2).
ARTIFACTS_GAP_FILENAME = "artifacts_gap.yaml"

#: Блокирующие классы как строки — сравнение с persisted ``error_kind``.
_BLOCKING_VALUES: frozenset[str] = frozenset(kind.value for kind in BLOCKING_FAILURE_KINDS)


def _origin(url: str) -> str:
    """Origin (``netloc``) URL — тот же вид, что в ``CorpusCoverage.unreadable_origins``.

    Отдельная реализация, а не импорт из :mod:`core.pipeline.corpus`: модуль — лист
    зависимостей для фазы корпуса (``corpus`` импортирует **его**, не наоборот).
    """
    return (
        (urlsplit(url).netloc or "").lower() or url.strip().lower() or "unknown"
    )


def _failure_kind(value: str | None) -> FailureKind | None:
    """Канонический класс сбоя; ``None`` — значение вне словаря или пусто.

    ``crawl_session_items.error_kind`` пишется через :func:`core.pipeline.corpus._mark`
    и валидируется :class:`~core.domain.models.CrawlSessionItem`, поэтому значение вне
    ``FailureKind`` — дефект записи. Такую строку файл не классифицирует (и не выдумывает
    класс), но и не роняет генерацию: она не попадёт ни в ``gaps``, ни в
    ``excluded_by_kind``.
    """
    if not value:
        return None
    try:
        return FailureKind(value)
    except ValueError:
        return None


def _counts(values: Iterable[str]) -> dict[str, int]:
    """``value → count`` с сортированными ключами (детерминизм вывода)."""
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return {key: counts[key] for key in sorted(counts)}


@dataclass(frozen=True)
class ArtifactGap:
    """Один недоступный артефакт: факт отказа + заготовка входа следующего прогона.

    ``attempts``/``last_attempt``/``http_status`` — провенанс попытки из
    ``crawl_session_items``; ``provide`` — поля, которые заполняет человек, и ничего
    больше: ``reason`` уже назван машиной (это аудит, а не решение оператора).
    """

    product_id: str
    url: str
    error_kind: FailureKind
    origin: str
    http_status: int | None = None
    attempts: int = 0
    last_attempt: str | None = None

    @property
    def sort_key(self) -> tuple[str, str, str]:
        """Ключ детерминированного порядка ``(product_id, error_kind, url)``."""
        return (self.product_id, self.error_kind.value, self.url)

    def as_record(self) -> dict[str, Any]:
        """YAML-совместимая запись элемента (порядок ключей фиксирован)."""
        return {
            "product_id": self.product_id,
            "url": self.url,
            "error_kind": self.error_kind.value,
            "http_status": self.http_status,
            "attempts": self.attempts,
            "last_attempt": self.last_attempt,
            "origin": self.origin,
            "provide": {
                "product_id": self.product_id,
                "url": self.url,
                "path": None,
                "reason": self.error_kind.value,
                "provided_by": None,
            },
        }


@dataclass(frozen=True)
class ArtifactsGapReport:
    """Отчёт ``artifacts_gap.yaml``: список пробелов + сводка по классам/продуктам/origin'ам.

    ``excluded_by_kind`` — терминальные ``failed`` **не** блокирующих классов: они не
    закрывают ``❌`` и потому не просят ручного артефакта, но их число публикуется,
    чтобы пустой ``gaps`` не означал «всё прочитано». ``path`` заполняет писатель
    (:func:`write_artifacts_gap`); ``None`` — отчёт собран, но файл не писался.
    """

    run_id: str
    gaps: tuple[ArtifactGap, ...] = ()
    excluded_by_kind: Mapping[str, int] = field(default_factory=dict)
    path: Path | None = None

    def summary(self) -> dict[str, Any]:
        """Сводка: всего/по классу/по продукту/по origin'у/исключённые классы."""
        return {
            "total": len(self.gaps),
            "by_error_kind": _counts(gap.error_kind.value for gap in self.gaps),
            "by_product": _counts(gap.product_id for gap in self.gaps),
            "by_origin": _counts(gap.origin for gap in self.gaps),
            "excluded_by_kind": {
                kind: int(self.excluded_by_kind[kind])
                for kind in sorted(self.excluded_by_kind)
            },
        }

    def payload(self) -> dict[str, Any]:
        """Полный YAML-совместимый документ (``schema``/``run_id``/``gaps``/``summary``)."""
        return {
            "schema": ARTIFACTS_GAP_SCHEMA,
            "run_id": self.run_id,
            "gaps": [gap.as_record() for gap in self.gaps],
            "summary": self.summary(),
        }

    def render(self) -> str:
        """YAML-текст документа: ``sort_keys=False`` — порядок задаёт :meth:`payload`."""
        return yaml.safe_dump(
            self.payload(),
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
        )

    def human_line(self) -> str:
        """Строка фазы: «N документов недоступно: <разбивка>…».

        Разбивка — классы отказа (``robots_disallow=17``) и origin'ы в скобках: оператор
        видит и **почему**, и **где**. При нуле пробелов инструкция «заполните ``path``»
        не печатается — заполнять нечего.
        """
        if not self.gaps:
            excluded = sum(int(value) for value in self.excluded_by_kind.values())
            tail = f" (неблокирующих отказов: {excluded})" if excluded else ""
            return f"0 документов недоступно: блокирующих отказов нет{tail}"
        summary = self.summary()
        kinds = ", ".join(
            f"{kind}={count}" for kind, count in summary["by_error_kind"].items()
        )
        origins = ", ".join(summary["by_origin"])
        return (
            f"{len(self.gaps)} документов недоступно: {kinds} ({origins}). "
            "Заполните `path` и перезапустите — пробел закроется, факт отказа "
            "останется в аудите"
        )


def collect_artifact_gaps(
    items: Iterable[CrawlSessionItem], *, run_id: str
) -> ArtifactsGapReport:
    """Делит терминальные ``failed`` на пробелы (блокирующие) и исключённые классы.

    Дубли ``(product_id, url)`` из разных сеансов одного прогона схлопываются по
    **последней** попытке (``last_attempt``, затем ``attempts``, затем ``session_id``) —
    файл описывает URL, а не строку таблицы, и порядок обхода на выбор не влияет.
    Элементы вне ``FAILED`` (``pending``/``running``/``cancelled``/``fetched``) не
    рассматриваются: отчёт про отказы, а не про очередь.
    """
    latest: dict[tuple[str, str], CrawlSessionItem] = {}
    for item in items:
        if item.status is not CrawlItemStatus.FAILED:
            continue
        if _failure_kind(item.error_kind) is None:
            continue
        key = (str(item.product_id), str(item.url))
        previous = latest.get(key)
        if previous is None or _attempt_rank(item) > _attempt_rank(previous):
            latest[key] = item

    gaps: list[ArtifactGap] = []
    excluded: dict[str, int] = {}
    for key in sorted(latest):
        item = latest[key]
        kind = _failure_kind(item.error_kind)
        if kind is None:  # pragma: no cover - отфильтровано выше
            continue
        if kind.value in _BLOCKING_VALUES:
            gaps.append(
                ArtifactGap(
                    product_id=str(item.product_id),
                    url=str(item.url),
                    error_kind=kind,
                    origin=_origin(str(item.url)),
                    http_status=item.http_status,
                    attempts=int(item.attempts),
                    last_attempt=item.last_attempt,
                )
            )
        else:
            excluded[kind.value] = excluded.get(kind.value, 0) + 1
    gaps.sort(key=lambda gap: gap.sort_key)
    return ArtifactsGapReport(
        run_id=str(run_id), gaps=tuple(gaps), excluded_by_kind=dict(excluded)
    )


def _attempt_rank(item: CrawlSessionItem) -> tuple[str, int, str]:
    """Ранг «свежести» попытки для схлопывания дублей (детерминированный tie-break)."""
    return (str(item.last_attempt or ""), int(item.attempts or 0), str(item.session_id))


def read_run_items(
    store: Any | None, *, run_id: str, session_id: str | None = None
) -> list[CrawlSessionItem]:
    """Читает элементы сеансов, привязанных к прогону (read-only, без сети).

    :param session_id: сеанс, который вызывающий уже держит (фаза ``corpus`` знает его
        по имени) — включается всегда, даже если стор не умеет перечислять сеансы.
    :returns: элементы в порядке перечисления стором; детерминированный порядок
        вывода обеспечивает :func:`collect_artifact_gaps`, а не этот сбор.
    """
    if store is None:
        return []
    ids: set[str] = {str(session_id)} if session_id else set()
    reader = getattr(store, "iter_crawl_sessions", None)
    if callable(reader):
        # Отсутствие метода — «стор не умеет перечислять сеансы» (законно: вызывающий уже
        # держит ``session_id``). Остальные сбои **не** глушатся: закрытое или повреждённое
        # соединение обязано быть видимым. Прежний широкий ``except Exception`` превращал
        # ``ProgrammingError: Cannot operate on a closed database`` в пустой список, и
        # ``artifacts_gap.yaml`` писался с ``total: 0`` рядом с 17 отказами в БД — отчёт
        # «недоступного нет» на прогоне, где оно есть.
        try:
            sessions = list(reader())
        except (AttributeError, NotImplementedError):
            sessions = []
        for session in sessions:
            if getattr(session, "run_id", None) == run_id:
                found = str(getattr(session, "session_id", "") or "")
                if found:
                    ids.add(found)
    items: list[CrawlSessionItem] = []
    for found in sorted(ids):
        items.extend(store.iter_crawl_session_items(found))
    return items


def artifacts_gap_path(out_dir: str | Path, run_id: str) -> Path:
    """Путь артефакта: ``{out_dir}/{run_id}/outputs/artifacts_gap.yaml`` (03 §A.2)."""
    return Path(out_dir) / str(run_id) / "outputs" / ARTIFACTS_GAP_FILENAME


def write_artifacts_gap(
    store: Any | None,
    *,
    run_id: str,
    out_dir: str | Path,
    session_id: str | None = None,
) -> ArtifactsGapReport:
    """Собирает отчёт из персистентных элементов и атомарно пишет файл.

    Единственный генератор файла: его зовут фаза ``corpus`` (до дорогой Ш6) и фаза
    ``report`` (в :class:`~core.pipeline.report.Artifacts`). ``store is None`` ⇒
    элементов нет и файл пишется пустым (``total: 0``): форма стабильна, а «нечего
    читать» не превращается в отсутствующий артефакт.

    Сбой записи **не** изолируется здесь: оба вызывающих обязаны поймать исключение и
    оставить его видимым (отчёт вторичен к фазе, но тихая потеря файла — нет).
    """
    items = read_run_items(store, run_id=run_id, session_id=session_id)
    report = collect_artifact_gaps(items, run_id=run_id)
    path = artifacts_gap_path(out_dir, run_id)
    atomic_write_text(path, report.render())
    return replace(report, path=path)
