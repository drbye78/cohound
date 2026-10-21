"""coHound — frontier обхода in-site ссылок и контрольные оси краула (D1/D2).

Модуль собирает **отдельную** цепочку приоритетов для новых осей контроля краула —
``CrawlConfig`` (application default, ``deploy.yaml:crawl``) < ``CrawlRunConfig``
(``run.yaml:crawl``, run profile) < :class:`~core.config.domain_profiles.DomainProfile`
(per-site) — и BFS-frontier одного **seed-root**.

Почему отдельная цепочка (решение заморожено): существующий ``resolve_crawl_target``
(``core.pipeline.crawl_sources``) резолвит оси ``fetch/extract/strategy/wait_for/rate_limit``
по слоям ``crawl_sources.yaml`` и трогать его порядок нельзя (24 layering-теста). Новые оси
(``max_workers``/``max_depth``/``max_documents``/``exclude_paths``/``exclude_documents``) идут
своим путём и **не** добавляются в ``crawl_sources.yaml``.

**Scope — per seed-root, не «SSRF-чек прошёл».** Обнаруженный URL ставится в очередь, только
если (1) :func:`core.util.security.validate_url` его принимает, (2) host совпадает с host
корня через ``_host_matches`` (поддомены включены; общий с ``fetch_plan``), (3) путь укладывается
в path-prefix корня по **границе сегмента** (``/documents`` матчит ``/documents`` и
``/documents/x``, но **не** ``/documents-archive``; правило из ``fetch_plan``), (4) URL не
зачёркнут блэклистом. SSRF-проверка **одна** не является проверкой scope.

**Блэклисты (D1/D2).** ``exclude_paths`` — glob по **пути** URL (``fnmatchcase``,
case-sensitive); ``exclude_documents`` — glob по **basename** (последний сегмент пути, напр.
``*.pdf``). Никаких regex/подстрок: предсказуемо и ReDoS-безопасно. Оба применяются **и при
постановке в очередь, и перед фетчем**.

**Seed-root** — корень, от которого ведётся scope: top-level URL (из ``--urls``/feed-channel)
или запись каталога. Seeds имеют глубину 0; ссылка со страницы глубины ``d`` → глубина ``d+1``.

Нейтрален к предметной категории (§1.3): домены/паттерны — данные.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from fnmatch import fnmatchcase
from urllib.parse import unquote, urlsplit

from core.config.deploy import CrawlConfig
from core.config.domain_profiles import DomainProfile
from core.domain.enums import ArchiveDescent, DocumentLevel
from core.domain.models import (
    DEFAULT_ARCHIVE_MAX_MEMBER_BYTES,
    DEFAULT_ARCHIVE_MAX_UNCOMPRESSED_BYTES,
    DEFAULT_MAX_ARTIFACT_BYTES,
    DEFAULT_MAX_RESPONSE_BYTES,
    CrawlRunConfig,
)
from core.pipeline.fetch_plan import _host_matches, select_domain_profile
from core.util.document_formats import (
    SUPPORTED_ARCHIVE_FORMATS,
    normalize_attachment_formats,
)
from core.util.security import validate_url

__all__ = [
    "CrawlLimits",
    "Frontier",
    "blacklist_match",
    "decoded_scope_path",
    "is_blacklisted",
    "resolve_crawl_limits",
    "root_path_prefix",
]


@dataclass(frozen=True)
class CrawlLimits:
    """Эффективные оси контроля краула после разрешения трёхуровневой цепочки (D1/D2).

    ``max_depth`` уже с дефолтом 0 (текущее поведение: только seeds). ``max_documents`` —
    ``None`` = без лимита. ``max_workers`` (D4, конкурентность) резолвится и попадает в
    провенанс; ``max_workers`` управляет **сетевым** fetch-пулом, ``extract_workers`` (P4) —
    отдельной осью **экстракции** (CPU-параллелизм). Обе оси — ``int | None`` (``None``/``1``
    ⇒ пул не создаётся).
    """

    max_workers: int | None = None
    extract_workers: int | None = None
    #: P4/OOM: рециклинг воркер-процессов локальной экстракции (``None`` — ось не задана).
    #: Значение резолвится той же цепочкой app < run < site, что и ``extract_workers``.
    extract_max_tasks_per_child: int | None = None
    #: P0/OOM: эффективный потолок одного ответа/retained-файла (байты). Дефолт 20 MiB
    #: действует всегда; ``max_response_bytes_explicit`` отмечает, что ось задана на
    #: одном из слоёв (только тогда ключ попадает в ``as_dict``/дайджест).
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES
    max_response_bytes_explicit: bool = False
    #: P6: потоковый потолок допущенного артефакта-контейнера (байты). Страница остаётся
    #: под ``max_response_bytes``; артефакт свыше него уходит в staging-файл, а не в RAM.
    max_artifact_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES
    max_artifact_bytes_explicit: bool = False
    #: P6: заявленные форматы-контейнеры (класс «артефакт»). Дефолт — все поддерживаемые;
    #: сужение набора делает незaявленный контейнер обычным бинарным артефактом.
    attachment_formats: tuple[str, ...] = SUPPORTED_ARCHIVE_FORMATS
    attachment_formats_explicit: bool = False
    #: P6: границы спуска в контейнер (провенанс/дайджест; исполняет следующий слой).
    archive_max_member_bytes: int = DEFAULT_ARCHIVE_MAX_MEMBER_BYTES
    archive_max_member_bytes_explicit: bool = False
    archive_max_uncompressed_bytes: int = DEFAULT_ARCHIVE_MAX_UNCOMPRESSED_BYTES
    archive_max_uncompressed_bytes_explicit: bool = False
    archive_max_members: int | None = None
    archive_max_members_explicit: bool = False
    archive_descent: ArchiveDescent = ArchiveDescent.FULL
    archive_descent_explicit: bool = False
    max_depth: int = 0
    max_documents: int | None = None
    exclude_paths: tuple[str, ...] = ()
    exclude_documents: tuple[str, ...] = ()
    #: P5: уровень извлечения документных форматов (ось ``crawl.document_level``). Дефолт
    #: ``full`` = сегодняшнее эффективное поведение. Резолвится той же цепочкой app < run <
    #: site; per seed-root (один корень = один уровень).
    document_level: DocumentLevel = DocumentLevel.FULL

    def as_dict(self) -> dict[str, object]:
        """Канонический словарь для провенанса/манифеста (сортированно-стабильно).

        Новая ось рециклинга включается **только когда задана**: ``None`` не добавляет
        ключ, поэтому дайджест ``crawl_config_digest`` прогонов без этой оси не меняется
        (обратная совместимость §13).
        """
        payload: dict[str, object] = {
            "max_workers": self.max_workers,
            "extract_workers": self.extract_workers,
            "max_depth": self.max_depth,
            "max_documents": self.max_documents,
            "exclude_paths": list(self.exclude_paths),
            "exclude_documents": list(self.exclude_documents),
            "document_level": self.document_level.value,
        }
        if self.extract_max_tasks_per_child is not None:
            payload["extract_max_tasks_per_child"] = self.extract_max_tasks_per_child
        if self.max_response_bytes_explicit:
            payload["max_response_bytes"] = self.max_response_bytes
        # P6: новые оси — только когда заданы (дефолт не меняет дайджест старых прогонов).
        if self.max_artifact_bytes_explicit:
            payload["max_artifact_bytes"] = self.max_artifact_bytes
        if self.attachment_formats_explicit:
            payload["attachment_formats"] = list(self.attachment_formats)
        if self.archive_max_member_bytes_explicit:
            payload["archive_max_member_bytes"] = self.archive_max_member_bytes
        if self.archive_max_uncompressed_bytes_explicit:
            payload["archive_max_uncompressed_bytes"] = (
                self.archive_max_uncompressed_bytes
            )
        if self.archive_max_members_explicit and self.archive_max_members is not None:
            payload["archive_max_members"] = self.archive_max_members
        if self.archive_descent_explicit:
            payload["archive_descent"] = self.archive_descent.value
        return payload


def _pick(app: object, run: object, site: object) -> object:
    """Высший не-``None`` приоритет: site > run > app (application default)."""
    if site is not None:
        return site
    if run is not None:
        return run
    return app


def _app_explicit(app: object, name: str) -> bool:
    """Задана ли ось **явно** в app-слое (Pydantic ``model_fields_set``).

    Дефолт app не должен делать дайджест другим (байт-идентичность старых прогонов §13).
    """
    return name in getattr(app, "model_fields_set", set())


def _axis_explicit(
    app: object, name: str, run_value: object, site_value: object
) -> bool:
    """«Явно задано» для оси: site/run-слой не ``None`` либо app-поле передано явно."""
    return bool(
        site_value is not None
        or run_value is not None
        or _app_explicit(app, name)
    )


def resolve_crawl_limits(
    url: str,
    *,
    app: CrawlConfig,
    run: CrawlRunConfig | None = None,
    profiles: Mapping[str, DomainProfile] | None = None,
) -> CrawlLimits:
    """Разрешает :class:`CrawlLimits` для ``url`` по цепочке app < run < site.

    Per-site слой выбирается лучшим ``host[+path]``-профилем домена
    (:func:`core.pipeline.fetch_plan.select_domain_profile`) — тем же матчингом, что и
    ``FetchPlan``. Для каждого вида проксимального слоя побеждает высший не-``None``.
    """
    profile = select_domain_profile(url, profiles or {})

    def site_attr(name: str) -> object:
        return getattr(profile, name) if profile is not None else None

    run_max_workers = run.max_workers if run is not None else None
    run_extract_workers = (
        getattr(run, "extract_workers", None) if run is not None else None
    )
    run_extract_max_tasks = (
        getattr(run, "extract_max_tasks_per_child", None) if run is not None else None
    )
    run_max_response_bytes = (
        getattr(run, "max_response_bytes", None) if run is not None else None
    )
    run_max_depth = run.max_depth if run is not None else None
    run_max_documents = run.max_documents if run is not None else None
    run_exclude_paths = run.exclude_paths if run is not None else None
    run_exclude_documents = run.exclude_documents if run is not None else None
    run_document_level = (
        getattr(run, "document_level", None) if run is not None else None
    )
    # P6: оси допуска контейнеров и границ спуска (та же цепочка app < run < site).
    run_max_artifact_bytes = (
        getattr(run, "max_artifact_bytes", None) if run is not None else None
    )
    run_attachment_formats = (
        getattr(run, "attachment_formats", None) if run is not None else None
    )
    run_archive_max_member_bytes = (
        getattr(run, "archive_max_member_bytes", None) if run is not None else None
    )
    run_archive_max_uncompressed_bytes = (
        getattr(run, "archive_max_uncompressed_bytes", None)
        if run is not None
        else None
    )
    run_archive_max_members = (
        getattr(run, "archive_max_members", None) if run is not None else None
    )
    run_archive_descent = (
        getattr(run, "archive_descent", None) if run is not None else None
    )

    max_workers = _pick(app.max_workers, run_max_workers, site_attr("max_workers"))
    extract_workers = _pick(
        getattr(app, "extract_workers", None),
        run_extract_workers,
        site_attr("extract_workers"),
    )
    extract_max_tasks = _pick(
        getattr(app, "extract_max_tasks_per_child", None),
        run_extract_max_tasks,
        site_attr("extract_max_tasks_per_child"),
    )
    app_max_response_bytes = getattr(app, "max_response_bytes", None)
    site_max_response_bytes = site_attr("max_response_bytes")
    max_response_bytes = _pick(
        app_max_response_bytes, run_max_response_bytes, site_max_response_bytes
    )
    # «Явно задано» — поле пришло на site/run-слое либо было передано в app-конструктор.
    # Дефолт app не делает дайджест другим (байт-идентичность старых прогонов §13).
    max_response_bytes_explicit = bool(
        site_max_response_bytes is not None
        or run_max_response_bytes is not None
        or "max_response_bytes" in getattr(app, "model_fields_set", set())
    )
    max_depth = _pick(app.max_depth, run_max_depth, site_attr("max_depth"))
    max_documents = _pick(
        app.max_documents, run_max_documents, site_attr("max_documents")
    )
    exclude_paths = _pick(
        app.exclude_paths, run_exclude_paths, site_attr("exclude_paths")
    )
    exclude_documents = _pick(
        app.exclude_documents, run_exclude_documents, site_attr("exclude_documents")
    )
    document_level = _pick(
        getattr(app, "document_level", None),
        run_document_level,
        site_attr("document_level"),
    )
    # P6: контейнеры. ``None`` на site/run — «наследовать»; app-дефолт всегда есть.
    app_max_artifact_bytes = getattr(app, "max_artifact_bytes", None)
    site_max_artifact_bytes = site_attr("max_artifact_bytes")
    max_artifact_bytes = _pick(
        app_max_artifact_bytes, run_max_artifact_bytes, site_max_artifact_bytes
    )
    app_attachment_formats = getattr(app, "attachment_formats", None)
    site_attachment_formats = site_attr("attachment_formats")
    attachment_formats = _pick(
        app_attachment_formats, run_attachment_formats, site_attachment_formats
    )
    app_archive_max_member_bytes = getattr(app, "archive_max_member_bytes", None)
    site_archive_max_member_bytes = site_attr("archive_max_member_bytes")
    archive_max_member_bytes = _pick(
        app_archive_max_member_bytes,
        run_archive_max_member_bytes,
        site_archive_max_member_bytes,
    )
    app_archive_max_uncompressed_bytes = getattr(
        app, "archive_max_uncompressed_bytes", None
    )
    site_archive_max_uncompressed_bytes = site_attr("archive_max_uncompressed_bytes")
    archive_max_uncompressed_bytes = _pick(
        app_archive_max_uncompressed_bytes,
        run_archive_max_uncompressed_bytes,
        site_archive_max_uncompressed_bytes,
    )
    app_archive_max_members = getattr(app, "archive_max_members", None)
    site_archive_max_members = site_attr("archive_max_members")
    archive_max_members = _pick(
        app_archive_max_members, run_archive_max_members, site_archive_max_members
    )
    app_archive_descent = getattr(app, "archive_descent", None)
    site_archive_descent = site_attr("archive_descent")
    archive_descent = _pick(
        app_archive_descent, run_archive_descent, site_archive_descent
    )

    return CrawlLimits(
        max_workers=None if max_workers is None else int(max_workers),  # type: ignore[arg-type]
        extract_workers=(
            None if extract_workers is None else int(extract_workers)  # type: ignore[arg-type]
        ),
        extract_max_tasks_per_child=(
            None
            if extract_max_tasks is None
            else int(extract_max_tasks)  # type: ignore[arg-type]
        ),
        max_response_bytes=(
            DEFAULT_MAX_RESPONSE_BYTES
            if max_response_bytes is None
            else int(max_response_bytes)  # type: ignore[arg-type]
        ),
        max_response_bytes_explicit=max_response_bytes_explicit,
        max_artifact_bytes=(
            DEFAULT_MAX_ARTIFACT_BYTES
            if max_artifact_bytes is None
            else int(max_artifact_bytes)  # type: ignore[arg-type]
        ),
        max_artifact_bytes_explicit=_axis_explicit(
            app, "max_artifact_bytes", run_max_artifact_bytes, site_max_artifact_bytes
        ),
        attachment_formats=normalize_attachment_formats(
            attachment_formats  # type: ignore[arg-type]
        ),
        attachment_formats_explicit=_axis_explicit(
            app,
            "attachment_formats",
            run_attachment_formats,
            site_attachment_formats,
        ),
        archive_max_member_bytes=(
            DEFAULT_ARCHIVE_MAX_MEMBER_BYTES
            if archive_max_member_bytes is None
            else int(archive_max_member_bytes)  # type: ignore[arg-type]
        ),
        archive_max_member_bytes_explicit=_axis_explicit(
            app,
            "archive_max_member_bytes",
            run_archive_max_member_bytes,
            site_archive_max_member_bytes,
        ),
        archive_max_uncompressed_bytes=(
            DEFAULT_ARCHIVE_MAX_UNCOMPRESSED_BYTES
            if archive_max_uncompressed_bytes is None
            else int(archive_max_uncompressed_bytes)  # type: ignore[arg-type]
        ),
        archive_max_uncompressed_bytes_explicit=_axis_explicit(
            app,
            "archive_max_uncompressed_bytes",
            run_archive_max_uncompressed_bytes,
            site_archive_max_uncompressed_bytes,
        ),
        archive_max_members=(
            None if archive_max_members is None else int(archive_max_members)  # type: ignore[arg-type]
        ),
        archive_max_members_explicit=_axis_explicit(
            app, "archive_max_members", run_archive_max_members, site_archive_max_members
        ),
        archive_descent=ArchiveDescent(
            ArchiveDescent.FULL if archive_descent is None else archive_descent
        ),
        archive_descent_explicit=_axis_explicit(
            app, "archive_descent", run_archive_descent, site_archive_descent
        ),
        max_depth=0 if max_depth is None else int(max_depth),  # type: ignore[arg-type]
        max_documents=(
            None if max_documents is None else int(max_documents)  # type: ignore[arg-type]
        ),
        exclude_paths=tuple(exclude_paths or ()),  # type: ignore[arg-type]
        exclude_documents=tuple(exclude_documents or ()),  # type: ignore[arg-type]
        document_level=DocumentLevel(
            DocumentLevel.FULL if document_level is None else document_level
        ),
    )


def blacklist_match(url: str, limits: CrawlLimits) -> str | None:
    """Возвращает заматчивший паттерн блэклиста или ``None`` (D1/D2, N5).

    ``exclude_paths`` — по пути URL, ``exclude_documents`` — по basename. ``fnmatchcase``
    (case-sensitive) без regex/подстрок.

    **Leading-slash tolerance (N5).** Паттерн пути принимается и с ведущим ``/``, и без
    него: ``docs/*`` нормализуется к ``/docs/*`` (путь URL всегда начинается с ``/``),
    поэтому ``docs/*`` матчит ``/docs/x`` как и ``/docs/*``. Паттерн-``/`` (зачёркивает
    host-корень) сохраняется.

    **Caveat ``exclude_documents`` (N5).** Сравнивается basename **пути** URL (файл —
    сегмент пути, напр. ``/docs/report.pdf``). Имя файла, переданное **только** в
    query-строке (``/download?file=report.pdf``), basename не является и паттерном не
    ловится — это задокументированное ограничение (см. ``03`` §D.2), а не догадка.
    """
    path = urlsplit(url).path or "/"
    for pattern in limits.exclude_paths:
        normalized = "/" + pattern.lstrip("/")
        if fnmatchcase(path, normalized):
            return pattern
    base = path.rsplit("/", 1)[-1]
    for pattern in limits.exclude_documents:
        if fnmatchcase(base, pattern):
            return pattern
    return None


def is_blacklisted(url: str, limits: CrawlLimits) -> bool:
    """Зачёркнут ли ``url`` блэклистом (обёртка над :func:`blacklist_match`)."""
    return blacklist_match(url, limits) is not None


def root_path_prefix(root_url: str) -> str:
    """Path-prefix seed-root: ``""`` — нет префикса (host-корень), иначе путь без хвоста ``/``.

    ``https://h/`` → ``""`` (весь host в scope); ``https://h/documents/`` → ``/documents``.
    """
    path = urlsplit(root_url).path or "/"
    if path in ("", "/"):
        return ""
    return path.rstrip("/")


def _path_within(url_path: str, prefix: str) -> bool:
    """Матч пути по границе сегмента (та же семантика, что ``fetch_plan.py``)."""
    if not prefix:
        return True
    return url_path == prefix or url_path.startswith(prefix + "/")


def decoded_scope_path(path: str) -> str | None:
    """Percent-декодирует путь для scope-сравнения; ``None`` — dot-segment ``..`` (N2).

    Сырой путь сравнивать нельзя: ``%2e%2e`` сервер декодирует в ``..`` и обходит
    path-префикс (``/documents/%2e%2e/private`` обслуживается вне ``/documents``). Любой
    сегмент ``..`` **после** декодирования — вне scope (traversal), в т.ч. буквальный
    ``..`` (нулевая нормализация ``urllib`` его не сворачивает).
    """
    decoded = unquote(path or "/")
    if any(segment == ".." for segment in decoded.split("/")):
        return None
    return decoded


class Frontier:
    """BFS-очередь одного seed-root: scope/блэклист/лимиты при постановке (D1/D2).

    FIFO (:class:`collections.deque`) — детерминированный обход; ключ дедупликации —
    нормализованный URL (см. ``core.util.html_links``). ``fetched`` считает успешно
    добытые (retained) страницы этого корня — для ``max_documents``. ``truncated``/
    ``truncated_reason`` фиксируют усечение (потолок документов или глубины) для
    провенанса (D1/G — «честность усечения»).
    """

    def __init__(self, root_url: str, limits: CrawlLimits) -> None:
        self.root_url = root_url
        parsed = urlsplit(root_url)
        self.root_host = (parsed.hostname or "").lower()
        self.path_prefix = root_path_prefix(root_url)
        self.limits = limits
        self._queue: deque[tuple[str, int]] = deque()
        self._seen: set[str] = set()
        self.fetched = 0
        self.truncated = False
        self.truncated_reason: str | None = None
        #: Seeds/ссылки, отброшенные блэклистом (наблюдаемость: «не молчаливый дроп»).
        self.skipped_blacklisted: list[str] = []
        #: P5: документы, пропущенные уровнем ``off`` (pre-fetch по суффиксу / post-fetch
        #: по content-type). Записи ``{"url", "reason"}`` (`url_suffix`/`content_type`) —
        #: решение обязано быть видимым (никогда не тихо), и питать канал усечения/аномалии.
        self.skipped_documents: list[dict[str, str]] = []

    # -- scope -------------------------------------------------------------- #

    def in_scope(self, url: str) -> bool:
        """Проходит ли ``url`` scope корня (SSRF + host + path-prefix по границе сегмента).

        Путь перед сравнением percent-декодируется, а ``..``-сегменты отвергаются (N2):
        иначе ``%2e%2e`` обошёл бы префикс.
        """
        try:
            validate_url(url)
        except ValueError:
            return False
        parsed = urlsplit(url)
        host = (parsed.hostname or "").lower()
        if not host or not _host_matches(host, self.root_host):
            return False
        decoded = decoded_scope_path(parsed.path or "/")
        if decoded is None:
            return False
        return _path_within(decoded, self.path_prefix)

    def blacklisted(self, url: str) -> bool:
        """Блэклист (путь + basename) — см. :func:`blacklist_match`."""
        return is_blacklisted(url, self.limits)

    # -- capacity ----------------------------------------------------------- #

    @property
    def has_capacity(self) -> bool:
        """Есть ли место под ещё одну добытую страницу по ``max_documents``."""
        return self.limits.max_documents is None or self.fetched < self.limits.max_documents

    def note_fetched(self) -> None:
        """Учесть успешно добытую (retained) страницу этого корня."""
        self.fetched += 1

    def note_document_skip(self, url: str, reason: str) -> None:
        """P5: отметить пропуск документа уровнем ``off`` (видимость решения).

        ``reason`` — ``url_suffix`` (pre-fetch skip) либо ``content_type`` (post-fetch).
        Идемпотентности нет намеренно: дубликат-совпадение не пропускается, но URL в
        потоке дренажа уникален (dedup frontier).
        """
        self.skipped_documents.append({"url": url, "reason": reason})

    def note_consumed(self, count: int) -> None:
        """Учесть уже израсходованный бюджет корня (retained-страницы resume, B2).

        Инициализация из durable-элементов сеанса: resumed root **не может** превысить
        ``max_documents`` при повторном прогоне того же сеанса (иначе бюджет начинался бы
        с нуля и лишние страницы тихо добывались бы заново).
        """
        if count > 0:
            self.fetched += int(count)

    def mark_truncated(self, reason: str) -> None:
        """Отметить политико-усечённый обход (первый повод побеждает; D1/G, D3)."""
        self._mark_truncated(reason)

    # -- enqueue ------------------------------------------------------------ #

    def add_seed(self, url: str) -> str:
        """Ставит seed (глубина 0); seed **задаёт** scope, поэтому scope-чек опущен.

        Блэклист применяется и к seeds (D1/H: отфильтрованы seeds, записи каталогов и
        обнаруженные URL). Возвращает статус: ``"queued"`` | ``"blacklisted"`` |
        ``"truncated"`` | ``"dup"``.
        """
        if url in self._seen:
            return "dup"
        if self.blacklisted(url):
            self.skipped_blacklisted.append(url)
            return "blacklisted"
        if not self.has_capacity:
            self._mark_truncated("max_documents")
            return "truncated"
        self._seen.add(url)
        self._queue.append((url, 0))
        return "queued"

    def discover(self, url: str, depth: int) -> bool:
        """Ставит обнаруженный URL глубины ``depth`` при прохождении scope/блэклиста/ёмкости."""
        if url in self._seen:
            return False
        if depth > self.limits.max_depth:
            return False
        if not self.in_scope(url):
            return False
        if self.blacklisted(url):
            self.skipped_blacklisted.append(url)
            return False
        if not self.has_capacity:
            self._mark_truncated("max_documents")
            return False
        self._seen.add(url)
        self._queue.append((url, depth))
        return True

    def offer(self, links: Sequence[str], parent_depth: int) -> None:
        """Предлагает ссылки страницы глубины ``parent_depth`` к обходу.

        При ``parent_depth < max_depth`` ссылки ставятся на ``parent_depth + 1``. На
        границе (``parent_depth == max_depth``) ссылки уже не ставятся, но наличие
        in-scope/не-блэклистных ссылок фиксируется как усечение по глубине — **тихая**
        штатная остановка BFS, но честная для отчётности (D1/G). Должно вызываться только
        при ``max_depth > 0`` (иначе links не извлекаются вовсе — байт-в-байт поведение).
        """
        if parent_depth >= self.limits.max_depth:
            for url in links:
                if self.in_scope(url) and not self.blacklisted(url):
                    self._mark_truncated("max_depth")
                    break
            return
        for url in links:
            self.discover(url, parent_depth + 1)

    # -- drain -------------------------------------------------------------- #

    def next(self) -> tuple[str, int] | None:
        """Следующий ``(url, depth)`` из FIFO-очереди или ``None`` (очередь пуста)."""
        if not self._queue:
            return None
        return self._queue.popleft()

    def __bool__(self) -> bool:
        return bool(self._queue)

    def __len__(self) -> int:
        return len(self._queue)

    def with_limits(self, limits: CrawlLimits) -> Frontier:
        """Копия frontier с урезанными лимитами (seam для D4/D3 без мутации)."""
        clone = Frontier(self.root_url, limits)
        clone._seen = set(self._seen)
        clone._queue = deque(self._queue)
        clone.fetched = self.fetched
        clone.skipped_blacklisted = list(self.skipped_blacklisted)
        clone.skipped_documents = [dict(rec) for rec in self.skipped_documents]
        return clone

    # -- внутреннее --------------------------------------------------------- #

    def _mark_truncated(self, reason: str) -> None:
        """Фиксирует усечение (первый повод побеждает; не перезаписываем причину)."""
        if not self.truncated:
            self.truncated = True
            self.truncated_reason = reason
