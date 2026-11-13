"""Отказы краула как рабочий список «что взять руками» — с фильтрами.

**Зачем.** ``artifacts_gap.yaml`` (``core/pipeline/artifacts_gap.py``) уже публикует
недоступные URL, но это **файл** прогона: чтобы выбрать из него подмножество («только
robots-отказы этого продукта», «только PDF под ``/docs``»), оператор открывает YAML и
фильтрует глазами. Модуль отвечает на тот же вопрос **из БД**, не заводя второго
источника правды: отбор и дедуп — те же (:func:`~core.pipeline.artifacts_gap.collect_artifact_gaps`
и общий :func:`~core.pipeline.artifacts_gap.latest_failures`), здесь добавляются только
фильтры и форма записи.

**Чего здесь нет.** Нет ни сети, ни wall-clock, ни записи: чтение — единственный вызов
``store.iter_crawl_session_items`` (порт ``StorePort``), никакого прямого SQL. Нет и
второй классификации отказов: значение вне :class:`~core.domain.enums.FailureKind` не
классифицируется ни здесь, ни в ``artifacts_gap`` — «неизвестное» не выдаётся за класс.

**``kinds`` шире блокирующего набора.** ``collect_artifact_gaps`` делит терминальные
отказы на блокирующие (``gaps``) и прочие (``excluded_by_kind``), поэтому отбор идёт по
**дедуплицированному** набору отказов, а не по ``gaps``: ``kinds=("source_unavailable",)``
обязан находить неблокирующие отказы, которых в ``gaps`` нет по построению. Разбивка
``excluded_by_kind`` при этом публикуется в сводке как есть — она объясняет, почему
отказов в БД больше, чем попало в список.

**``robots_unavailable`` неотличим от прочих сетевых сбоев.** Лента ``robots.txt``,
которую не удалось прочитать, канонизируется в ``source_unavailable``
(``core/pipeline/corpus_persist.canonical_failure_kind``) и отдельным значением в БД не
встречается — поэтому ``kinds=("source_unavailable",)`` смешивает robots-недоступность с
любым другим сетевым сбоем, а не разделяет их.

**``roots`` — удобство листинга, а не решение о scope.** Матч хоста и path-префикса
повторяет семантику scope краула (нормализация ``www.``, граница сегмента), но это
фильтр вывода: право краула ходить по URL решает ``Frontier.host_in_scope``, а не этот
модуль. Терминальный артефакт здесь тоже не освобождается от path-префикса — список
отказов не должен менять состав корпуса.

Нейтрален (§1.3): нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

import fnmatch
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlsplit

from core.domain.enums import FailureKind
from core.domain.models import CrawlSessionItem
from core.pipeline.artifacts_gap import collect_artifact_gaps, latest_failures
from core.pipeline.crawl_frontier import (
    decoded_scope_path,
    normalize_scope_host,
    root_path_prefix,
)
from core.pipeline.crawl_session import default_session_id

__all__ = [
    "BLOCKED_KIND_DEFAULT",
    "BlockedArtifact",
    "attribute_root",
    "blocked_artifacts",
    "blocked_listing",
    "blocked_summary",
]

#: Класс отказа по умолчанию: robots-запрет — самый частый повод ручной подачи.
BLOCKED_KIND_DEFAULT = "robots_disallow"


@dataclass(frozen=True)
class BlockedArtifact:
    """Один терминальный отказ: что не взяли, почему и где он объявлен.

    ``kind`` — канонический ``FailureKind`` (строка словаря, а не свободный текст);
    ``detail`` — текст ошибки из ``crawl_session_items.error`` (``None`` — запись его не
    несла, и выдумывать причину нельзя); ``root`` — самый длинный объявленный корень,
    накрывающий URL (``None`` — корни не передали либо ни один не накрыл).
    """

    product_id: str
    url: str
    kind: str
    detail: str | None
    http_status: int | None
    depth: int | None
    root: str | None


def _counts(values: Iterable[str]) -> dict[str, int]:
    """``value → count`` с сортированными ключами (детерминизм вывода)."""
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return {key: counts[key] for key in sorted(counts)}


def _host(url: str) -> str:
    """Хост URL для сводки (``netloc`` в нижнем регистре); пустой — ``unknown``."""
    return (urlsplit(url).netloc or "").lower() or "unknown"


def _validate_kinds(kinds: Iterable[str]) -> tuple[str, ...]:
    """Канонизирует ``kinds``; неизвестный класс — ``ValueError`` с перечнем допустимых.

    Тихий пустой результат запрещён: опечатка в ``--kind`` иначе читалась бы как
    «отказов нет», то есть как утверждение о материале, которого никто не проверял.
    """
    allowed = sorted(kind.value for kind in FailureKind)
    wanted: list[str] = []
    for raw in kinds:
        text = str(raw).strip()
        try:
            kind = FailureKind(text)
        except ValueError:
            raise ValueError(
                f"kinds: неизвестный класс сбоя {raw!r}; допустимо: {', '.join(allowed)}"
            ) from None
        if kind.value not in wanted:
            wanted.append(kind.value)
    return tuple(wanted)


def _path_within(url_path: str, prefix: str) -> bool:
    """Матч пути по границе сегмента (зеркало ``crawl_frontier._path_within``).

    ``/docs`` накрывает ``/docs`` и ``/docs/x``, но **не** ``/docsx``. Пустой префикс
    (host-корень) накрывает всё.
    """
    if not prefix:
        return True
    return url_path == prefix or url_path.startswith(prefix + "/")


def _root_covers(url: str, root: str) -> bool:
    """Накрывает ли объявленный корень ``root`` адрес ``url`` (хост + path-префикс).

    Хост сравнивается после :func:`normalize_scope_host` (``www.`` эквивалентен базовому
    хосту с обеих сторон). Путь URL percent-декодируется, и dot-segment ``..`` выводит
    адрес из корня (``decoded_scope_path`` → ``None``) — иначе ``/docs/%2e%2e/x``
    выглядел бы лежащим внутри ``/docs``.
    """
    url_host = normalize_scope_host(urlsplit(url).hostname or "")
    root_host = normalize_scope_host(urlsplit(root).hostname or "")
    if not url_host or url_host != root_host:
        return False
    decoded = decoded_scope_path(urlsplit(url).path or "/")
    if decoded is None:
        return False
    return _path_within(decoded, root_path_prefix(root))


def attribute_root(url: str, roots: Sequence[str]) -> str | None:
    """Самый длинный объявленный корень, накрывающий ``url`` (``None`` — ни один).

    Порядок сравнения — ``(длина path-префикса, длина строки)``: вложенный корень
    ``/docs/api`` обязан побеждать ``/docs``, иначе запись назовёт более широкое
    объявление, чем то, что реально накрыло адрес.
    """
    matched = [root for root in roots if _root_covers(url, root)]
    if not matched:
        return None
    return max(matched, key=lambda root: (len(root_path_prefix(root)), len(root)))


def _normalize_prefix(value: str) -> str:
    """Нормализует path-префикс фильтра: ведущий ``/``, без хвостового ``/``."""
    text = unquote(str(value)).strip()
    if not text:
        return ""
    if not text.startswith("/"):
        text = "/" + text
    return text.rstrip("/")


def _keep(
    artifact: BlockedArtifact,
    *,
    products: frozenset[str],
    masks: tuple[str, ...],
    paths: tuple[str, ...],
    urls: frozenset[str],
    roots: bool,
) -> bool:
    """Проходит ли запись все заданные фильтры (незаданный фильтр не сужает)."""
    if products and artifact.product_id not in products:
        return False
    if urls and artifact.url not in urls:
        return False
    if roots and artifact.root is None:
        return False
    if paths:
        decoded = decoded_scope_path(urlsplit(artifact.url).path or "/")
        if decoded is None:
            return False
        if not any(_path_within(decoded, prefix) for prefix in paths if prefix):
            return False
    if masks:
        # Маска — по **сырому** пути URL (как его видит оператор в БД): ``*.pdf``
        # накрывает ``/a/b/x.pdf``, потому что ``fnmatch`` считает ``/`` обычным символом.
        return any(
            fnmatch.fnmatchcase(urlsplit(artifact.url).path, mask) for mask in masks
        )
    return True


def _collect(
    store: Any,
    run_id: str,
    *,
    kinds: Iterable[str],
    products: Iterable[str] | None,
    roots: Sequence[str] | None,
    masks: Sequence[str] | None,
    paths: Sequence[str] | None,
    urls: Iterable[str] | None,
) -> tuple[list[BlockedArtifact], dict[str, int]]:
    """Читает сеанс прогона и отбирает отказы; ``(записи, excluded_by_kind)``.

    Дедуп ``(product_id, url)`` и классификация — общие с ``artifacts_gap``: одна
    формула на файл и на список, иначе два носителя одного факта разошлись бы.
    """
    wanted = _validate_kinds(kinds)
    session_id = default_session_id(str(run_id))
    items: list[CrawlSessionItem] = list(store.iter_crawl_session_items(session_id))
    report = collect_artifact_gaps(items, run_id=str(run_id))
    latest = latest_failures(items)

    product_filter = frozenset(str(value) for value in (products or ()))
    url_filter = frozenset(str(value) for value in (urls or ()))
    mask_filter = tuple(str(mask) for mask in (masks or ()) if str(mask).strip())
    path_filter = tuple(
        prefix
        for prefix in (_normalize_prefix(value) for value in (paths or ()))
        if prefix
    )
    root_list = tuple(str(root) for root in (roots or ()) if str(root).strip())

    selected: list[BlockedArtifact] = []
    for key in sorted(latest):
        item = latest[key]
        kind = FailureKind(str(item.error_kind)).value
        if kind not in wanted:
            continue
        artifact = BlockedArtifact(
            product_id=str(item.product_id),
            url=str(item.url),
            kind=kind,
            detail=item.error,
            http_status=item.http_status,
            depth=item.depth,
            root=attribute_root(str(item.url), root_list) if root_list else None,
        )
        if _keep(
            artifact,
            products=product_filter,
            masks=mask_filter,
            paths=path_filter,
            urls=url_filter,
            roots=bool(root_list),
        ):
            selected.append(artifact)
    # Порядок — ``(product_id, url)``: ``latest`` уже отсортирован по этому ключу.
    return selected, dict(report.excluded_by_kind)


def blocked_artifacts(
    store: Any,
    run_id: str,
    *,
    kinds: Iterable[str] = (BLOCKED_KIND_DEFAULT,),
    products: Iterable[str] | None = None,
    roots: Sequence[str] | None = None,
    masks: Sequence[str] | None = None,
    paths: Sequence[str] | None = None,
    urls: Iterable[str] | None = None,
) -> list[BlockedArtifact]:
    """Терминальные отказы прогона, отфильтрованные по классу/продукту/адресу.

    :param store: ``StorePort`` прогона; читается только ``iter_crawl_session_items``
        (сеанс — ``default_session_id(run_id)``), без прямого SQL и без сети.
    :param run_id: прогон; ``session_id`` выводится из него детерминированно.
    :param kinds: классы ``FailureKind`` по ``error_kind``; дефолт — ``robots_disallow``.
        Неизвестное значение — ``ValueError`` (тихий пустой список был бы утверждением
        «отказов нет»).
    :param products: точные ``product_id``; пусто — все продукты.
    :param roots: объявленные корни для фильтра по хосту и path-префиксу (граница
        сегмента); в записи попадает **самый длинный** накрывший корень.
    :param masks: маски ``fnmatch.fnmatchcase`` по сырому пути URL (``*.pdf``).
    :param paths: префиксы пути по границе сегмента (``/docs/x`` не матчит ``/docsx``).
    :param urls: точные строки URL.
    :returns: записи в порядке ``(product_id, url)``, без дублей (ключ сеанса уникален).
    :raises ValueError: неизвестный класс в ``kinds``.

    Пустой результат — законный: он означает «под фильтр ничего не попало», а не
    «отказов не было» (последнее видно только без фильтров).
    """
    selected, _ = _collect(
        store,
        run_id,
        kinds=kinds,
        products=products,
        roots=roots,
        masks=masks,
        paths=paths,
        urls=urls,
    )
    return selected


def blocked_listing(
    store: Any,
    run_id: str,
    *,
    kinds: Iterable[str] = (BLOCKED_KIND_DEFAULT,),
    products: Iterable[str] | None = None,
    roots: Sequence[str] | None = None,
    masks: Sequence[str] | None = None,
    paths: Sequence[str] | None = None,
    urls: Iterable[str] | None = None,
) -> tuple[list[BlockedArtifact], dict[str, object]]:
    """Список и сводка **одним** чтением стора — вход ``--json`` у CLI.

    ``blocked_summary`` не может восстановить ``excluded_by_kind`` из записей (это факт
    о том, что в список **не** попало), а второе чтение БД ради сводки разошлось бы с
    первым, если между ними завершится краул. Поэтому оба результата считаются из
    одного снимка сеанса.
    """
    selected, excluded = _collect(
        store,
        run_id,
        kinds=kinds,
        products=products,
        roots=roots,
        masks=masks,
        paths=paths,
        urls=urls,
    )
    return selected, blocked_summary(selected, excluded_by_kind=excluded)


def blocked_summary(
    items: Iterable[BlockedArtifact],
    *,
    excluded_by_kind: Mapping[str, int] | None = None,
) -> dict[str, object]:
    """Сводка по списку: ``count``, ``by_product``, ``by_host``, ``by_kind``, исключённые.

    Все словари — с сортированными ключами (детерминизм вывода). ``excluded_by_kind``
    берётся как есть: это счётчики ``artifacts_gap`` по классам, которые в список не
    попадают, и без них короткий список читался бы как «всё скачалось».
    """
    rows = list(items)
    return {
        "count": len(rows),
        "by_product": _counts(artifact.product_id for artifact in rows),
        "by_host": _counts(_host(artifact.url) for artifact in rows),
        "by_kind": _counts(artifact.kind for artifact in rows),
        "excluded_by_kind": {
            str(kind): int(count)
            for kind, count in sorted((excluded_by_kind or {}).items())
        },
    }
