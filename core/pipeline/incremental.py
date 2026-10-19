"""coHound — планировщик инкрементального корпуса и ключи durable-субстрата (09 §6.3/§9).

Срез **MVP-3a**: только планирование work-set и детерминированные идентификаторы.
Сеть, краул, экстракция, чанкование, индексация и интеграция с ``corpus.py`` — следующие
срезы; этот модуль их не вызывает и не импортирует адаптеры/хранилища.

Три части:

* :func:`plan_incremental` — work-set продукта по §6.3: корни всегда ``fetch``, известные
  URL из ``url_state`` — ``revalidate`` (или ``reuse`` при ``offline``), discovery-diff
  вызывающего — ``fetch``. ``next_check_at`` в этом срезе **не читается**: расписания
  ревалидации ещё нет, поэтому поле не влияет на план.
* :func:`classify_reuse` — исход одного URL (304/тот же ``content_hash``/новый контент).
* :func:`doc_version_id`/:func:`chunk_row_id`/:func:`doc_set_digest` — детерминированные
  ключи §4.2/§9/§10.1.

Слой ``pipeline``: зависимости — ``domain``/``ports``/``util``. Нейтрален к предметной
категории (§1.3).
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from urllib.parse import urlsplit

from core.domain.enums import IncrementalKind, RefreshPolicy, ReuseKind
from core.ports.state import DocumentVersionRow, ProductStatePort, UrlStateRow
from core.util.hashing import short_hash

__all__ = [
    "IncrementalItem",
    "chunk_row_id",
    "classify_reuse",
    "doc_set_digest",
    "doc_version_id",
    "plan_incremental",
]

# Разделители полей/записей: unit/record separator не встречаются в версиях, URL и
# идентификаторах, поэтому склейка однозначна (тот же приём, что в ``core.util.hashing``).
_FIELD_SEP = "\x1f"
_RECORD_SEP = "\x1e"

#: Статус версии документа, попадающей в doc-set (§4.2).
_DOC_STATUS_ACTIVE = "active"

#: Default-порты снимаются при нормализации URL (как в §7.2).
_DEFAULT_PORTS = {"http": 80, "https": 443}


@dataclass(frozen=True)
class IncrementalItem:
    """Элемент work-set инкрементального корпуса (09 §6.3).

    :param url: URL в том виде, в каком он пришёл (для отчёта/логов).
    :param normalized_url: канонический URL (§7.2) — ключ дедупликации.
    :param root_url: корень, из которого URL обнаружен; ``None`` — из ``url_state``/
        discovery-diff (происхождение не сохранено в ``url_state``).
    :param depth: глубина BFS. В этом срезе всегда ``0``: ``url_state`` глубину не
        хранит, а планировщик не разворачивает BFS (это делает corpus-срез).
    :param kind: :class:`IncrementalKind` — ``fetch``/``revalidate``/``reuse``.
    :param etag: ``ETag`` для ``If-None-Match`` (``None`` — валидатора нет).
    :param last_modified: ``Last-Modified`` для ``If-Modified-Since``.
    :param doc_version_id: последняя версия документа из ``url_state`` (для
        carry-forward/переиспользования; ``None`` — версии ещё нет).
    """

    url: str
    normalized_url: str
    root_url: str | None
    depth: int
    kind: IncrementalKind
    etag: str | None = None
    last_modified: str | None = None
    doc_version_id: str | None = None


def _normalize_url(url: str) -> str:
    """Нормализует URL по §7.2 (pipeline-локально, без импорта ``config``).

    Схема — только ``http``/``https``; host — нижний регистр; default port снят;
    fragment удалён; path case/trailing slash и query сохраняются (сервер может отдавать
    разное). Правила совпадают с :func:`core.config.seeds.normalize_seed_url`, но модуль
    остаётся в слое ``pipeline`` и не тянет ``config``.

    :raises ValueError: пустой URL, не-http(s) схема, отсутствующий host, учётные данные,
        некорректный порт.
    """
    text = str(url or "").strip()
    if not text:
        raise ValueError("URL пуст")
    if "://" not in text:
        raise ValueError(f"URL без схемы: {text!r} (только http/https)")
    parts = urlsplit(text)
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https"):
        raise ValueError(f"URL: схема только http/https, получено {parts.scheme!r}")
    host = (parts.hostname or "").strip().lower()
    if not host:
        raise ValueError(f"URL без hostname: {text!r}")
    if parts.username or parts.password:
        raise ValueError("URL: учётные данные запрещены")
    try:
        port = parts.port
    except ValueError as exc:
        raise ValueError(f"URL: некорректный порт ({exc})") from exc
    netloc = f"[{host}]" if ":" in host else host
    if port is not None and port != _DEFAULT_PORTS[scheme]:
        netloc = f"{netloc}:{port}"
    path = parts.path or "/"
    query = f"?{parts.query}" if parts.query else ""
    return f"{scheme}://{netloc}{path}{query}"


def plan_incremental(
    *,
    product_id: str,
    roots: Sequence[str],
    state: ProductStatePort,
    policy: RefreshPolicy,
    known_new_urls: Sequence[str] = (),
    exclude_urls: Sequence[str] = (),
) -> list[IncrementalItem]:
    """Строит work-set инкрементального продукта (09 §6.3).

    Правила:

    * **корни** — всегда ``fetch`` (depth 0) в порядке входа: seed — авторское решение,
      он не пропускается ни при какой политике и не выпадает по ``exclude_urls``;
      повторный корень с тем же ``normalized_url`` схлопывается (первый выигрывает);
    * **``url_state``** — известные URL продукта: ``revalidate`` при политике
      ``auto``/``revalidate``/``refresh`` (валидаторы могут отсутствовать — тогда это
      безусловный GET, но элемент всё равно ``revalidate``) и ``reuse`` при ``offline``
      (сеть для известных URL не трогается); порядок — ``normalized_url``;
    * **``known_new_urls``** — discovery-diff (sitemap/feed), который вызывает вызывающий:
      добавляются как ``fetch`` в порядке входа, дедуплицируются по ``normalized_url``
      против корней, ``url_state`` и ``exclude_urls``. При ``offline`` discovery не
      добавляется вовсе (никакого нового фетча); корни остаются в work-set как ``fetch``
      по явному инварианту выше — решает ли фетчер реально ходить в сеть, решает
      вызывающий;
    * **``exclude_urls``** — выкидываются из discovery/``url_state``, но **не** из корней.

    ``next_check_at`` **не читается**: расписание ревалидации (§6.3) — следующий срез;
    в этом срезе поле не влияет на план и не превращает ``revalidate`` в ``reuse``.

    Детерминированный порядок результата: корни (порядок входа) → ``known_new_urls``
    (порядок входа) → ``url_state`` (сортировка по ``normalized_url``).

    :param product_id: продукт, чей ``url_state`` читается.
    :param roots: seed-корни продукта (нормализуются здесь).
    :param state: durable-состояние продукта (нужен только ``iter_url_state``).
    :param policy: политика наблюдения (``auto``|``revalidate``|``refresh``|``offline``).
    :param known_new_urls: URL, которых, по мнению вызывающего, ещё нет в состоянии
        (sitemap/feed diff). Планировщик сам ничего не скачивает.
    :param exclude_urls: URL, исключённые фильтрами пакета (``exclude_paths`` и т.п.).
    :raises ValueError: некорректный URL во входе.
    """
    normalized_policy = RefreshPolicy(policy)
    excluded = {_normalize_url(url) for url in exclude_urls}

    items: list[IncrementalItem] = []
    seen: set[str] = set()

    # 1. Корни — всегда в работе, в порядке входа.
    for raw in roots:
        normalized = _normalize_url(raw)
        if normalized in seen:
            continue
        seen.add(normalized)
        items.append(
            IncrementalItem(
                url=str(raw).strip(),
                normalized_url=normalized,
                root_url=normalized,
                depth=0,
                kind=IncrementalKind.FETCH,
            )
        )

    # Известные URL читаются один раз: они же нужны для дедупликации discovery.
    state_rows = sorted(
        state.iter_url_state(product_id), key=lambda row: row.normalized_url
    )
    known_in_state = {row.normalized_url for row in state_rows}

    # 2. Discovery-diff вызывающего — fetch; известное состояние приоритетнее.
    known_items: list[IncrementalItem] = []
    if normalized_policy is not RefreshPolicy.OFFLINE:
        for raw in known_new_urls:
            normalized = _normalize_url(raw)
            if (
                normalized in seen
                or normalized in known_in_state
                or normalized in excluded
            ):
                continue
            seen.add(normalized)
            known_items.append(
                IncrementalItem(
                    url=str(raw).strip(),
                    normalized_url=normalized,
                    root_url=None,
                    depth=0,
                    kind=IncrementalKind.FETCH,
                )
            )

    # 3. Известные URL — revalidate (offline — reuse), по normalized_url.
    state_items: list[IncrementalItem] = []
    for row in state_rows:
        if row.normalized_url in excluded or row.normalized_url in seen:
            continue
        seen.add(row.normalized_url)
        kind = (
            IncrementalKind.REUSE
            if normalized_policy is RefreshPolicy.OFFLINE
            else IncrementalKind.REVALIDATE
        )
        state_items.append(_item_from_url_state(row, kind))

    return [*items, *known_items, *state_items]


def _item_from_url_state(row: UrlStateRow, kind: IncrementalKind) -> IncrementalItem:
    """Элемент work-set из строки ``url_state`` (валидаторы и версия переносятся)."""
    return IncrementalItem(
        url=row.url,
        normalized_url=row.normalized_url,
        # ``url_state`` не хранит, из какого корня обнаружен URL.
        root_url=None,
        depth=0,
        kind=kind,
        etag=row.etag,
        last_modified=row.last_modified,
        doc_version_id=row.doc_version_id,
    )


def classify_reuse(
    *,
    prior: UrlStateRow | None,
    prior_doc: DocumentVersionRow | None,
    not_modified: bool,
    content_hash: str | None,
) -> ReuseKind:
    """Классифицирует исход одного URL (09 §6.3).

    * ``not_modified`` (HTTP 304) → ``revalidated``;
    * у прежней версии тот же ``content_hash`` (200, контент не изменился) →
      ``revalidated``;
    * прежнего состояния нет → ``live`` (первое наблюдение);
    * иначе контент изменился → ``live``.

    ``content_hash is None`` (страница не дала извлекаемого текста) сравнение не
    проходит: исход трактуется как ``live``, а не как «тот же документ».
    """
    if not_modified:
        return ReuseKind.REVALIDATED
    if (
        prior_doc is not None
        and content_hash is not None
        and prior_doc.content_hash == content_hash
    ):
        return ReuseKind.REVALIDATED
    if prior is None:
        return ReuseKind.LIVE
    return ReuseKind.LIVE


def doc_version_id(
    product_id: str, content_hash: str, extractor_version: str, level: str
) -> str:
    """Детерминированный id версии документа: ``"dv_" + short_hash(tuple)``.

    Хешируется ровно ключ §4.2
    ``UNIQUE (product_id, content_hash, extractor_version, level)`` — смена любого
    элемента даёт новую версию, одинаковый ключ всегда даёт один и тот же id
    (переиспользование §9: «DocumentVersion — точное совпадение»).
    """
    key = _FIELD_SEP.join((product_id, content_hash, extractor_version, level))
    return "dv_" + short_hash(key, 24)


def chunk_row_id(doc_version_id: str, chunker_version: str, chunk_no: int) -> str:
    """Id строки ``chunk_meta``: ``short_hash(doc_version_id, chunker_version, no)``.

    Это **durable**-пространство идентификаторов (state.db). Run-level ``chunk_id``
    (``core.pipeline.corpus._chunk_id``) — другое пространство
    ``f"{product_id}:{doc_hash}:{chunk_no}"``: он адресует чанк внутри конкретного
    прогона/партиции, тогда как ``chunk_meta.chunk_id`` переживает прогоны и различает
    версии чанкера. Смешивать их нельзя — одинаковый текст при разных
    ``chunker_version`` обязан дать разные durable-строки.
    """
    key = _FIELD_SEP.join((doc_version_id, chunker_version, str(int(chunk_no))))
    return short_hash(key, 24)


def doc_set_digest(rows: Iterable[DocumentVersionRow]) -> str:
    """Стабильный дайджест doc-set продукта (09 §10.1).

    Берутся только ``active``-версии и только ключевые поля
    ``(doc_version_id, content_hash, extractor_version, level)``; порядок входа не
    важен (сортировка внутри). Дайджест не зависит от ``last_seen_*``/``status``
    неактивных версий, поэтому два прогона с неизменным набором документов дают одно
    значение — это условие переноса ячеек (§10.1) и сверки аттестаций.

    Пустой набор даёт стабильный дайджест пустой строки, а **не** ``""``: пустая строка
    неотличима от «дайджест не вычислен».
    """
    active = sorted(
        {
            (
                row.doc_version_id,
                row.content_hash,
                row.extractor_version,
                row.level,
            )
            for row in rows
            if row.status == _DOC_STATUS_ACTIVE
        }
    )
    payload = _RECORD_SEP.join(_FIELD_SEP.join(parts) for parts in active)
    return short_hash(payload, 24)
