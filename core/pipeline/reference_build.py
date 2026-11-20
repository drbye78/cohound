"""coHound — сборка reference-корпуса (план 27 §4, §8.1 шаги 1–5).

До этого модуля ``reference.db`` умел только **читаться**: ``hound baseline recon``
открывал готовый файл и падал, если его нет. Производителя не было вовсе, то есть
наследование от предка работало лишь там, где кто-то положил БД руками. Здесь
появляется сборка: объявленные корни документации → тот же конвейер, что у корпуса
когорты (``fetch → retention → extract → chunk → content_hash``), но **вне** ``runs/``
и с собственной идентичностью (:func:`~core.pipeline.reference_corpus.reference_build_id`).

Почему это отдельный модуль, а не ветка ``corpus``: у корпуса когорты есть когорта,
сиды, покрытие продуктов и ``run_id``; у reference-корпуса нет ни того, ни другого, ни
третьего. Общая у них только **механика чтения текста**, и она переиспользуется портами
(``CrawlerPort``/``ExtractorPort``/``ChunkerPort``), а не копированием фазы.

**Что здесь переиспользуется, а не пишется заново.**

* scope/дедуп/лимиты/усечение — :class:`~core.pipeline.crawl_frontier.Frontier`: вторая
  формула scope разошлась бы с первой ровно там, где это дороже всего (чужой хост в
  корпусе или потерянный раздел своего);
* темп — :class:`~core.util.rate_limit.RateLimiter` (per-origin, как у краула);
* SSRF — :func:`core.util.security.validate_url` на каждом корне **до** сети;
* retention — тот же приём, что у корпуса (``raw/{sha256}.bin``), но каталог общий для
  сборок версии: имя файла и есть хеш, поэтому общий каталог ничего не путает;
* писатель — ``StorePort`` (:class:`~core.store.sqlite_store.SqliteStore` в composition
  root): схема БД у reference-корпуса та же, и гейт версии схемы действует как обычно.
  Сам модуль живёт в слое ``pipeline`` и ``core.store`` не импортирует (матрица слоёв
  ``01`` §1) — поэтому фабрика хранилища **инъектируется**.

**Идентичность и провенанс — разные вещи.** ``build_id`` — функция **содержимого**
(``reference_content_digest``): тот же текст ⇒ та же сборка, и второй прогон помечается
``reused``, ничего не перезаписывая. ``complete``/``skipped``/``errors``/``roots`` — проход,
а не содержимое: в ``build_id`` они не входят, иначе одна упавшая страница давала бы
«другую сборку» той же документации. Но публикуются они обязательно: ``absent`` в
:func:`~core.pipeline.reference_corpus.derive_facts` допустим **только** при
``complete=True`` (дисциплина I2/I2c), и читатель должен видеть основание отказа.

**Что считается полным проходом (``complete``).** Объявленное пространство прочитано до
исчерпания: ни один корень не отвергнут, ни один URL не пропущен по глубине, ``robots``
ничего не запретил, и ни один фетч/retention/экстракция не провалились. Неполнота
публикуется машинными токенами ``incomplete_reasons`` (``truncated:<причина>`` и
``skipped:<ключ>``), а подробности — в ``errors``.

**Счётчики ``skipped`` — что в них считается (ключи стабильны, см. :class:`SkipReason`).**

============================  ==================================================
ключ                          единица и смысл
============================  ==================================================
``root_rejected``             корень: SSRF-валидация отвергла объявленный URL
``robots``                    URL: ``robots``-политика не разрешила фетч
``out_of_scope``              URL: ссылка вне объявленного пространства (чужой
                              хост, необъявленный поддомен, путь вне префикса);
                              считается **различных** URL на корень, не упоминаний
``depth``                     корень: обход усечён ``max_depth`` (точное число
                              непосещённых ссылок frontier не публикует)
``blacklisted``               URL: исключён объявленным ``exclude_paths`` — это
                              **объявленная** часть пространства, не потеря
``not_a_page``                URL: ответ не страница и не документ (картинка,
                              JSON, контейнер) — читателю нечего извлекать
``fetch_error``               URL: краулер бросил (транзиент/отказ доступа)
``retention_error``           URL: байты не удалось сохранить (I19 не выполнен ⇒
                              документ в корпус не идёт)
``extract_error``             URL: экстрактор вернул ``error`` или бросил
``empty_text``                URL: извлечён пустой текст (материала нет)
``duplicate_text``            URL: текст уже в корпусе (тот же ``content_hash``)
============================  ==================================================

Неполноту дают ``root_rejected``/``robots``/``depth``/``fetch_error``/
``retention_error``/``extract_error``/``empty_text``. ``blacklisted`` (объявленное
исключение), ``out_of_scope`` (вне объявленного пространства), ``not_a_page`` (не
документация) и ``duplicate_text`` (тот же текст) — нет: пространство прочитано
целиком, просто в нём не было другого материала.

Нейтрален к предметной категории (§1.3): домены, версии и продукты — данные.
"""

from __future__ import annotations

import datetime as _dt
import os
import shutil
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

from core.config.domain_profiles import DomainProfile
from core.domain.enums import DocumentLevel, PdfPipeline, ReuseKind
from core.domain.models import CorpusChunk, CorpusDoc, RateLimitSpec, parse_rate_limit
from core.pipeline.crawl_frontier import (
    SCOPE_OUT,
    CrawlLimits,
    Frontier,
    normalize_scope_host,
)
from core.pipeline.fetch_plan import resolve_fetch_plan
from core.pipeline.reference_corpus import (
    CHUNK_ID_SCHEME,
    REFERENCE_SCHEMA,
    ReferenceCorpusLayout,
    ReferenceManifest,
    read_manifest,
    reference_build_id,
    reference_content_digest,
    write_current_build,
    write_manifest,
)
from core.ports.store import StorePort
from core.util.document_formats import (
    document_content_type,
    document_suffix,
    is_epub,
    textual_family,
)
from core.util.hashing import content_hash
from core.util.html_links import extract_links
from core.util.package_lock import PackageLock, PackageLocked
from core.util.rate_limit import RateLimiter
from core.util.security import validate_url

__all__ = [
    "INCOMPLETE_SKIPS",
    "ReferenceBuildBusy",
    "ReferenceBuildConflict",
    "ReferenceBuildError",
    "ReferenceBuildLimits",
    "ReferenceBuildResult",
    "ReferenceBuildUnavailable",
    "ReferenceStoreWriter",
    "SkipReason",
    "build_reference_corpus",
]

#: Каталог staging внутри ``builds/`` — имя начинается с точки, поэтому
#: :func:`~core.pipeline.reference_corpus.list_builds` его не видит.
_STAGING_PREFIX = ".staging-"

#: MIME-типы, которые считаются **страницей** (HTML), а не документом.
_HTML_MIME = frozenset({"text/html", "application/xhtml+xml"})

#: «Глубины нет» в терминах ``CrawlLimits.max_depth`` (``int``, сравнение ``depth > max``).
#: Число не потолок, а отсутствие потолка: URL с большим числом сегментов пути не
#: существует (предел ОС на длину пути — 4096 байт, то есть < 2048 сегментов). Граница
#: пространства — path-prefix объявленного корня, а не это число.
_UNBOUNDED_DEPTH = 10_000


class SkipReason(StrEnum):
    """Машинные ключи счётчиков ``skipped`` (единицы — в докстринге модуля).

    ``StrEnum``, а не литералы: ключ попадает в ``manifest.json`` и читается человеком
    через ``jq``, поэтому он — часть публикуемого формата, а не деталь реализации.
    """

    ROOT_REJECTED = "root_rejected"
    ROBOTS = "robots"
    OUT_OF_SCOPE = "out_of_scope"
    DEPTH = "depth"
    BLACKLISTED = "blacklisted"
    NOT_A_PAGE = "not_a_page"
    FETCH_ERROR = "fetch_error"
    RETENTION_ERROR = "retention_error"
    EXTRACT_ERROR = "extract_error"
    EMPTY_TEXT = "empty_text"
    DUPLICATE_TEXT = "duplicate_text"


#: Ключи, ненулевой счётчик которых означает «объявленное пространство прочитано не
#: полностью» (⇒ ``complete=False``). Порядок — порядок публикации причин.
#:
#: ``depth`` здесь **нет** намеренно: усечение по глубине публикует сам frontier
#: (``truncated:max_depth``), и второй причиной оно было бы одним фактом дважды.
#: Счётчик ``skipped["depth"]`` при этом остаётся — он отвечает на другой вопрос: сколько
#: **корней** упёрлось в объявленную границу.
INCOMPLETE_SKIPS: tuple[SkipReason, ...] = (
    SkipReason.ROOT_REJECTED,
    SkipReason.ROBOTS,
    SkipReason.FETCH_ERROR,
    SkipReason.RETENTION_ERROR,
    SkipReason.EXTRACT_ERROR,
    SkipReason.EMPTY_TEXT,
)


class _PageClass(StrEnum):
    """Класс ответа: страница (ссылки разворачиваются), документ, «не читаемое»."""

    HTML = "html"
    DOCUMENT = "document"
    OTHER = "other"


class ReferenceBuildError(RuntimeError):
    """Сборка reference-корпуса невозможна (базовый класс отказов этого модуля)."""


class ReferenceBuildBusy(ReferenceBuildError):
    """Лок версии занят: другой процесс уже собирает этот (пакет, продукт, версия).

    Ожидания нет намеренно: две одновременные сборки одной версии делили бы
    ``raw/``, бюджет и staging, то есть портили бы друг другу провенанс, а не ускоряли
    работу. Отказ называет держателя (``PackageLock`` пишет host/pid/команду).
    """


class ReferenceBuildConflict(ReferenceBuildError):
    """Каталог сборки существует, но не подтверждает ту же идентичность.

    ``build_id`` — функция содержимого, поэтому «каталог есть, а манифеста нет» или
    «манифест с другим ``content_digest``» недостижимы штатным путём. Это признак
    повреждённого/чужого каталога, и перезаписать его молча значило бы стереть
    доказательство того, что в нём лежало.
    """


class ReferenceBuildUnavailable(ReferenceBuildError):
    """Не передана фабрика хранилища — писать корпус некуда.

    Слой ``pipeline`` не импортирует ``core.store`` (матрица ``01`` §1), поэтому
    ``SqliteStore`` приходит из composition root. Отказ видимый: молчаливая сборка «в
    памяти» вернула бы ``build_id`` без БД, то есть объявила бы измерение, которого нет.
    """


class ReferenceStoreWriter(StorePort, Protocol):
    """Писатель reference-корпуса: порт хранилища + закрытие соединения.

    ``close`` в протоколе, потому что публикация переименовывает каталог: соединение
    обязано быть закрыто **до** ``os.replace``, иначе WAL-файлы уезжают вместе с
    каталогом в неизвестном состоянии (и ``-wal`` остаётся от чужого пути).
    """

    def close(self) -> None: ...


@dataclass(frozen=True)
class ReferenceBuildLimits:
    """Объявленные оси одной сборки (вход измерения, а не настройка «на глаз»).

    Оси те же, что у краула, и это намеренно: reference-корпус читается тем же
    конвейером, поэтому ``max_depth``/``max_documents``/``exclude_paths``/
    ``document_level``/``pdf_pipeline`` означают ровно то же, что в ``CrawlLimits``.
    ``rate_limit`` — **каноническая строка** (``uniform(min=…,max=…)``), как в
    ``CrawlLimits.rate_limit``: она входит в провенанс сборки, и две разные формы записи
    одного темпа сделали бы сравнение сборок строковым лотерейным билетом.

    **``max_depth=None`` — глубины нет, и это дефолт.** Граница пространства здесь не
    глубина, а **объявленный корень**: ``docs_roots`` в ``reference_products.yaml`` задают
    path-prefix, и ``Frontier`` не выпускает обход за него. Документация версии — дерево
    под этим префиксом (план 27 §8.5: 800–1200 страниц), поэтому «глубина 0 = только
    корень» прочитала бы одну страницу руководства и — что хуже — объявила бы это
    исчерпанием. Число документов ограничивает ``max_documents`` (``None`` — без потолка),
    и отказ от потолка здесь осознанный (A3: не ограничивать, а измерять): объём и цена
    публикуются в манифесте, а не прячутся за выдуманным числом.

    ``max_depth=0`` остаётся **явным** объявлением «только корни»: тогда усечение по
    глубине фиксируется в провенансе, ``complete`` становится ``False``, и ``absent`` по
    такому корпусу не публикуется (I2/I2c-дисциплина). Это не запрет, а видимая цена.
    """

    max_documents: int | None = None
    max_depth: int | None = None
    rate_limit: str | None = None
    exclude_paths: tuple[str, ...] = ()
    document_level: DocumentLevel = DocumentLevel.FULL
    pdf_pipeline: PdfPipeline = PdfPipeline.STRUCTURED


@dataclass
class ReferenceBuildResult:
    """Итог сборки: идентичность, объёмы и провенанс прохода.

    ``reused=True`` — сборка с тем же содержимым уже была: ``docs``/``chunks``/
    ``skipped``/``errors``/``complete``/``roots`` взяты из **опубликованного** манифеста,
    а не из этого прохода. Сборка описывается один раз: два манифеста одного ``build_id``
    с разными счётчиками сделали бы вопрос «сколько в ней документов» зависящим от того,
    кто её пересобирал.
    """

    build_id: str
    content_digest: str
    docs: int
    chunks: int
    skipped: dict[str, int]
    errors: list[str]
    truncated: list[str]
    complete: bool
    roots: list[str]
    reused: bool
    db_path: Path
    manifest: ReferenceManifest


# ---------------------------------------------------------------------------
# Мелкие помощники
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    """Текущее время UTC в ISO-8601 (``observed_at`` манифеста — провенанс, не вход)."""
    return _dt.datetime.now(_dt.UTC).isoformat()


def _host(url: str) -> str:
    """Хост URL в нижнем регистре (пустая строка — URL не разбирается)."""
    return (urlsplit(url).hostname or "").lower()


def _mime(content_type: str | None) -> str:
    """MIME без параметров в нижнем регистре (``""`` — заголовка нет)."""
    return (content_type or "").split(";", 1)[0].strip().lower()


def _page_class(url: str, content_type: str | None) -> _PageClass:
    """Класс ответа по суффиксу и ``Content-Type`` — решение «есть ли что читать».

    Порядок проверок повторяет реестр форматов (:mod:`core.util.document_formats`), а не
    изобретает свой: документ (PDF/Office), EPUB и текстовые форматы — документы, их
    читает экстрактор; HTML — страница, у неё разворачиваются ссылки; всё остальное
    (картинки, JSON, контейнеры) — не материал reference-корпуса.

    Заголовка нет и суффикс не документный ⇒ страница: так ведёт себя HTTP-ответ без
    ``Content-Type`` у любого сайта, и обратное правило выбрасывало бы документацию
    целиком ради отсутствующего заголовка.
    """
    if textual_family(url, content_type) is not None:
        return _PageClass.DOCUMENT
    if is_epub(url, content_type):
        return _PageClass.DOCUMENT
    if document_suffix(url) or document_content_type(content_type):
        return _PageClass.DOCUMENT
    mime = _mime(content_type)
    if mime in _HTML_MIME:
        return _PageClass.HTML
    if mime:
        return _PageClass.OTHER
    return _PageClass.HTML


def _retain_bytes(raw_dir: Path, content: bytes) -> tuple[Path, str]:
    """Пишет байты в ``raw/{sha256}.bin`` **атомарно** и возвращает ``(путь, хеш)``.

    Имя файла — хеш содержимого, поэтому запись идемпотентна и иммутабельна по
    построению: второй раз тот же текст не создаёт второй файл, а «перезапись» пишет
    ровно те же байты. Временный файл нужен, потому что читатель (другая сборка,
    ``recon``) может открыть ``raw/`` в любой момент: половина файла под правильным
    именем была бы ложным доказательством (I19).
    """
    digest = content_hash(content)
    path = raw_dir / f"{digest}.bin"
    if path.is_file():
        return path, digest
    raw_dir.mkdir(parents=True, exist_ok=True)
    tmp = raw_dir / f".{digest}.{os.getpid()}.tmp"
    tmp.write_bytes(content)
    os.replace(tmp, path)
    return path, digest


def _declared_subdomains(
    root_host: str,
    profiles: Mapping[str, DomainProfile],
    other_root_hosts: frozenset[str],
) -> tuple[str, ...]:
    """Поддомены корня, объявленные ключами ``domain_profiles.yaml``.

    Правило scope (AGENTS §6): поддомен в scope, только если объявлен. Ключ профиля домена
    — это объявление (оператор назвал хост по имени), поэтому он и передаётся frontier.
    Хост, который сам является корнем другого объявленного корня, **не** передаётся: у
    него свой корень и свой бюджет, и брать его материалы за счёт предка значило бы
    дважды измерить один хост под двумя именами.
    """
    root = normalize_scope_host(root_host)
    if not root:
        return ()
    hosts: set[str] = set()
    for raw_key in profiles:
        host = normalize_scope_host(str(raw_key).split("/", 1)[0])
        if not host or host == root or host in other_root_hosts:
            continue
        if host.endswith("." + root):
            hosts.add(host)
    return tuple(sorted(hosts))


def _robots_allows(robots: Any | None, url: str) -> bool:
    """Разрешает ли ``robots``-политика фетч ``url`` (``None`` — политики нет ⇒ да).

    Проверяется ``allowed``, а не ``verdict``: сборке нужен допуск, а различие
    «запрещено / лента не прочитана» уже публикует сам гейт в своём провенансе
    (``crawl_probe``). Исключение негодного объекта политики — не «разрешение»:
    вызывающий считает такой URL отказом и пишет ошибку (иначе сломанный гейт выглядел
    бы как сайт без ограничений).
    """
    if robots is None:
        return True
    return bool(robots.allowed(url))


def _incomplete_reasons(
    skipped: Mapping[str, int], truncated: Sequence[str]
) -> list[str]:
    """Машинные причины неполноты: ``truncated:<причина>`` и ``skipped:<ключ>``."""
    reasons = [f"truncated:{reason}" for reason in truncated]
    reasons.extend(
        f"skipped:{reason.value}"
        for reason in INCOMPLETE_SKIPS
        if skipped.get(reason.value)
    )
    return sorted(reasons)


def _count(skipped: dict[str, int], reason: SkipReason) -> None:
    """Увеличивает счётчик пропуска (ключ создаётся при первом отказе)."""
    skipped[reason.value] = skipped.get(reason.value, 0) + 1


def _staging_dir(layout: ReferenceCorpusLayout) -> Path:
    """Каталог staging этой сборки: ``builds/.staging-{pid}``.

    Имя начинается с точки, поэтому :func:`~core.pipeline.reference_corpus.list_builds`
    его не видит; pid в имени — чтобы два процесса (разные версии/продукты) не делили
    один staging. Один писатель на версию держит лок, поэтому чужой staging того же pid
    возможен только после SIGKILL/OOM.
    """
    return layout.builds_dir / f"{_STAGING_PREFIX}{os.getpid()}"


def _version_of(port: Any) -> str | None:
    """Версия порта строкой (``None`` — порт её не сообщает).

    Версия читается у **порта**, а не у первого документа: манифест описывает, **чем**
    читался корпус, и выводить это из результата значило бы публиковать «версию того
    документа, который случайно оказался первым».
    """
    version = getattr(port, "version", None)
    return str(version) if version else None


def _truncated_reasons(manifest: ReferenceManifest) -> list[str]:
    """Причины усечения из манифеста (``truncated:<причина>`` → ``<причина>``)."""
    prefix = "truncated:"
    return sorted(
        reason[len(prefix) :]
        for reason in manifest.incomplete_reasons
        if reason.startswith(prefix)
    )


def _offer_links(
    frontier: Frontier,
    links: Sequence[str],
    depth: int,
    skipped: dict[str, int],
    seen_out_of_scope: set[str],
) -> None:
    """Отдаёт ссылки frontier, считая отвергнутые scope — но не решая scope заново.

    Scope решает frontier (:meth:`Frontier.scope_admission`): здесь только **считается**
    его отказ, чтобы «чужой хост / необъявленный поддомен» был числом, а не тишиной.
    Второй формулы scope нет — используется тот же предикат, которым frontier поставит
    ссылку в очередь.

    Считаются **различные** URL (множество на корень): одна и та же внешняя ссылка со ста
    страниц — один отказ, а не сто; иначе счётчик мерил бы разметку сайта, а не объём
    выброшенного материала.
    """
    kept: list[str] = []
    for url in links:
        if frontier.scope_admission(url) == SCOPE_OUT:
            if url not in seen_out_of_scope:
                seen_out_of_scope.add(url)
                _count(skipped, SkipReason.OUT_OF_SCOPE)
            continue
        kept.append(url)
    frontier.offer(kept, depth)


def _drain_root(
    *,
    frontier: Frontier,
    layout: ReferenceCorpusLayout,
    profiles: Mapping[str, DomainProfile],
    robots: Any | None,
    limiter: RateLimiter,
    rate_spec: RateLimitSpec | None,
    crawler: Any,
    extractor: Any,
    chunker: Any,
    store: ReferenceStoreWriter,
    skipped: dict[str, int],
    errors: list[str],
    buffered_chunks: list[CorpusChunk],
    seen_hashes: set[str],
) -> int:
    """Дренирует один корень; возвращает число записанных документов.

    Порядок шагов на URL — тот же, что у корпуса когорты, и это не совпадение: retention
    **до** экстракции (I19: доказательство существует раньше вывода), ``content_hash``
    текста — ключ дедупа, а не хеш байтов (одна и та же статья по двум адресам — один
    документ). ``not_modified`` (304) не обрабатывается: сборка идёт по объявленным
    корням без условных запросов, поэтому «переиспользовать версию» здесь нечего.
    """
    written = 0
    out_of_scope_seen: set[str] = set()
    while True:
        item = frontier.next()
        if item is None:
            break
        url, depth = item

        if robots is not None:
            try:
                allowed = _robots_allows(robots, url)
            except Exception as exc:  # noqa: BLE001 — негодная политика не роняет сборку
                _count(skipped, SkipReason.ROBOTS)
                frontier.note_robots_refusal(url)
                errors.append(
                    f"{layout.product_id}/{url}: robots: {type(exc).__name__}: {exc}"
                )
                continue
            if not allowed:
                _count(skipped, SkipReason.ROBOTS)
                frontier.note_robots_refusal(url)
                continue

        plan = resolve_fetch_plan(url, profiles, default_rate_limit=rate_spec)
        limiter.wait(_host(url), plan.rate_limit)
        try:
            page = crawler.fetch(url, plan)
        except Exception as exc:  # noqa: BLE001 — изоляция по одному URL
            _count(skipped, SkipReason.FETCH_ERROR)
            errors.append(
                f"{layout.product_id}/{url}: fetch: {type(exc).__name__}: {exc}"
            )
            continue

        try:
            raw_path, _raw_digest = _retain_bytes(layout.raw_dir, page.content)
        except Exception as exc:  # noqa: BLE001 — сбой диска не должен ронять обход
            _count(skipped, SkipReason.RETENTION_ERROR)
            errors.append(
                f"{layout.product_id}/{url}: retention: {type(exc).__name__}: {exc}"
            )
            continue
        # Бюджет ``max_documents`` считается по **добытым** (retained) страницам: решение
        # о нём принимает ``frontier.next``, а факт «добыли» — вот здесь.
        frontier.note_fetched()

        final_url = getattr(page, "url", None) or url
        klass = _page_class(final_url, page.content_type)
        if klass is _PageClass.OTHER:
            _count(skipped, SkipReason.NOT_A_PAGE)
            continue

        try:
            extracted = extractor.extract(page, plan.extract)
        except Exception as exc:  # noqa: BLE001 — изоляция по одному документу
            _count(skipped, SkipReason.EXTRACT_ERROR)
            errors.append(
                f"{layout.product_id}/{url}: extract: {type(exc).__name__}: {exc}"
            )
            continue
        if extracted.error:
            _count(skipped, SkipReason.EXTRACT_ERROR)
            errors.append(f"{layout.product_id}/{url}: extract: {extracted.error}")
            continue
        if not (extracted.text or "").strip():
            _count(skipped, SkipReason.EMPTY_TEXT)
            continue
        if extracted.content_hash in seen_hashes:
            _count(skipped, SkipReason.DUPLICATE_TEXT)
            continue
        seen_hashes.add(extracted.content_hash)

        doc_id = f"{layout.product_id}:{extracted.content_hash}"
        doc = _corpus_doc(
            layout=layout,
            doc_id=doc_id,
            url=final_url,
            page=page,
            plan_fetch=plan.fetch.value if plan.fetch is not None else None,
            raw_path=raw_path,
            extracted=extracted,
            extractor_version=_version_of(extractor),
        )
        store.upsert_corpus_doc(doc)
        buffered_chunks.extend(
            _corpus_chunks(
                layout=layout,
                doc_id=doc_id,
                url=final_url,
                page=page,
                doc=doc,
                chunks=chunker.chunk(extracted, doc_id),
            )
        )
        written += 1

        # Ссылки читаются **всегда**, и это не оптимизация, а честность провенанса:
        # границу решает frontier (``offer`` на границе глубины фиксирует усечение
        # ``max_depth``, а не молчит). Пока извлечение было привязано к
        # ``max_depth > 0``, сборка с глубиной 0 читала одну страницу и объявляла
        # ``complete=True`` — то есть давала право публиковать ``absent`` по корпусу из
        # одной страницы (I2/I2c). Цена правки — разбор ссылок одной страницы.
        if klass is _PageClass.HTML:
            _offer_links(
                frontier,
                extract_links(page.content, final_url),
                depth,
                skipped,
                out_of_scope_seen,
            )
    return written


def _corpus_doc(
    *,
    layout: ReferenceCorpusLayout,
    doc_id: str,
    url: str,
    page: Any,
    plan_fetch: str | None,
    raw_path: Path,
    extracted: Any,
    extractor_version: str | None,
) -> CorpusDoc:
    """Строка ``corpus_docs`` из страницы и её извлечённого текста.

    ``render_mode`` берётся у **страницы** (наблюдение: как её действительно достали), а
    при его отсутствии — из плана: ``None`` у страницы означает «получения не было», и
    подставлять туда план значило бы приписать серверу ответ, которого не было.
    """
    observed_mode = getattr(page, "fetch_mode", None)
    return CorpusDoc(
        doc_id=doc_id,
        product_id=layout.product_id,
        url=url,
        fetched_at=getattr(page, "fetched_at", None),
        content_hash=extracted.content_hash,
        raw_path=str(raw_path),
        extractor_version=extracted.extractor_version or extractor_version,
        render_mode=(
            str(getattr(observed_mode, "value", observed_mode))
            if observed_mode is not None
            else plan_fetch
        ),
        lang=getattr(page, "lang", None),
        partial=bool(extracted.partial),
        partial_reason=extracted.partial_reason,
        page_count=extracted.page_count,
        pages_converted=extracted.pages_converted,
        extracted_chars=len(extracted.text or ""),
        http_status=getattr(page, "status_code", None),
        # Страница из локального кэша — не наблюдение этого цикла: ``live`` было бы
        # ложной провенанс-записью (09 §6.3).
        reuse_kind=ReuseKind.REUSED if getattr(page, "from_cache", False) else ReuseKind.LIVE,
        page_map=extracted.page_map,
        ocr_probe=extracted.ocr_probe,
        pages_ocr=extracted.pages_ocr,
        pages_failed=extracted.pages_failed,
        tables=extracted.tables,
        blocks=extracted.blocks,
    )


def _corpus_chunks(
    *,
    layout: ReferenceCorpusLayout,
    doc_id: str,
    url: str,
    page: Any,
    doc: CorpusDoc,
    chunks: Sequence[Any],
) -> list[CorpusChunk]:
    """Строки ``corpus_chunks`` из кусков чанкера (``build_id`` проставит публикатор).

    ``chunk_id`` **приводится к грамматике адреса** (``product:doc:chunk_no``,
    :func:`core.util.ids.parse_source_id`), а не берётся у чанкера как есть: чанкер
    возвращает внутреннюю форму ``{document_id}#{no}``, и у корпуса когорты её переписывает
    ``corpus_index._chunk_id``. Сборка reference-корпуса этого не делала, поэтому адрес
    чанка в её БД не разбирался: карта PG 18 (18.11.2026) получила
    ``evidence_doc_version_unresolved`` и, как следствие, ``absence_undated`` — датировать
    документ было нечем не потому, что даты нет, а потому, что адрес не парсился.
    """
    return [
        CorpusChunk(
            chunk_id=f"{doc_id}:{chunk.chunk_no}",
            doc_id=doc_id,
            product_id=layout.product_id,
            url=url,
            heading_path=chunk.heading_path or None,
            lang=getattr(page, "lang", None),
            chunk_no=chunk.chunk_no,
            text=chunk.text,
            context_prefix=chunk.context_prefix or None,
            content_hash=chunk.content_hash,
            extractor_version=doc.extractor_version,
            char_start=chunk.char_start,
            char_end=chunk.char_end,
            table_ref=chunk.table_ref,
        )
        for chunk in chunks
    ]


# ---------------------------------------------------------------------------
# Сборка
# ---------------------------------------------------------------------------


def build_reference_corpus(
    *,
    layout: ReferenceCorpusLayout,
    roots: Sequence[str],
    crawler: Any,
    extractor: Any,
    chunker: Any,
    limits: ReferenceBuildLimits | None = None,
    domain_profiles: Mapping[str, DomainProfile] | None = None,
    robots: Any | None = None,
    sleep: Callable[[float], None] = time.sleep,
    store_factory: Callable[[Path], ReferenceStoreWriter] | None = None,
) -> ReferenceBuildResult:
    """Собирает reference-корпус версии из объявленных корней документации.

    Порядок шагов — §8.1 плана: SSRF-валидация корня → обход frontier (scope/дедуп/лимиты)
    → ``robots`` → фетч с темпом → retention байтов → экстракция → дедуп по
    ``content_hash`` → нарезка → запись в БД сборки → публикация каталога.

    :param layout: раскладка версии (:class:`~core.pipeline.reference_corpus.ReferenceCorpusLayout`).
    :param roots: объявленные корни; обходятся **в объявленном порядке** (порядок — вход,
        а не следствие каталога файловой системы).
    :param crawler: ``CrawlerPort`` — единственный путь в сеть.
    :param extractor: ``ExtractorPort`` — тот же читатель, что у корпуса когорты.
    :param chunker: ``ChunkerPort`` — тот же чанкер (его версия публикуется в манифесте).
    :param limits: объявленные оси сборки; ``None`` — только корни, глубина 0.
    :param domain_profiles: профили доменов (D.2): оси фетча **и** объявление поддоменов
        в scope.
    :param robots: объект с ``allowed(url) -> bool``; ``None`` — политика не передана.
    :param sleep: шов сна для :class:`~core.util.rate_limit.RateLimiter` (тесты).
    :param store_factory: фабрика писателя ``path -> StorePort`` (composition root даёт
        ``SqliteStore``). Без неё сборка отказывает: ``pipeline`` не импортирует
        ``core.store`` (матрица слоёв ``01`` §1).
    :returns: :class:`ReferenceBuildResult` — идентичность, объёмы и провенанс прохода.
    :raises ReferenceBuildUnavailable: не передана фабрика хранилища.
    :raises ReferenceBuildBusy: версию уже собирает другой процесс.
    :raises ReferenceBuildConflict: каталог ``builds/{build_id}`` не подтверждает ту же
        идентичность (нет манифеста или другой ``content_digest``).

    Публикация атомарна: БД и манифест пишутся в ``builds/.staging-{pid}``, затем каталог
    переименовывается в ``builds/{build_id}``. Существующая сборка с тем же ``build_id``
    ⇒ ``reused=True``, staging удаляется, **ничего не перезаписывается**.
    """
    if store_factory is None:
        raise ReferenceBuildUnavailable(
            "store_factory не передана: слой pipeline не импортирует core.store "
            "(матрица слоёв 01 §1). Composition root передаёт фабрику SqliteStore "
            "(например: store_factory=lambda path: SqliteStore(path))."
        )
    effective = limits or ReferenceBuildLimits()
    profiles: Mapping[str, DomainProfile] = dict(domain_profiles or {})
    declared_roots = [str(root) for root in roots]
    try:
        # Разбор объявленного темпа — до лока и до сети: негодная ось обязана отказать
        # раньше, чем начнётся запись (иначе «сборка не удалась» вместо «ось неверна»).
        rate_spec = parse_rate_limit(effective.rate_limit)
    except ValueError as exc:
        raise ReferenceBuildError(f"rate_limit: {exc}") from exc

    layout.ensure()
    staging = _staging_dir(layout)
    lock = PackageLock(layout.lock_path, timeout_s=0.0, command="baseline recon")
    try:
        lock.acquire()
    except PackageLocked as exc:
        holder = exc.holder
        who = holder.describe() if holder is not None else "неизвестным держателем"
        raise ReferenceBuildBusy(
            f"reference-корпус {layout.product_id}/{layout.version} уже собирается "
            f"{who}; файл лока: {layout.lock_path}"
        ) from exc
    try:
        return _build_locked(
            layout=layout,
            declared_roots=declared_roots,
            crawler=crawler,
            extractor=extractor,
            chunker=chunker,
            limits=effective,
            rate_spec=rate_spec,
            profiles=profiles,
            robots=robots,
            sleep=sleep,
            store_factory=store_factory,
        )
    finally:
        # Оборванный проход не оставляет staging: лок уже снят, значит каталог ничей, а
        # мусор с «правильным» именем читался бы как недописанная сборка. На успешном
        # пути каталога уже нет (он переименован или удалён как reused) — no-op.
        shutil.rmtree(staging, ignore_errors=True)
        lock.release()


def _build_locked(
    *,
    layout: ReferenceCorpusLayout,
    declared_roots: Sequence[str],
    crawler: Any,
    extractor: Any,
    chunker: Any,
    limits: ReferenceBuildLimits,
    rate_spec: RateLimitSpec | None,
    profiles: Mapping[str, DomainProfile],
    robots: Any | None,
    sleep: Callable[[float], None],
    store_factory: Callable[[Path], ReferenceStoreWriter],
) -> ReferenceBuildResult:
    """Сборка под взятым локом: обход, запись в staging, публикация каталога."""
    layout.builds_dir.mkdir(parents=True, exist_ok=True)
    staging = _staging_dir(layout)
    # Остаток прошлого прогона этого же pid (SIGKILL/OOM): лок уже взят, значит
    # staging ничей, и держать его значило бы копить мусор с «правильными» именами.
    if staging.exists():
        shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)

    crawl_limits = CrawlLimits(
        # ``None`` — глубины нет; см. докстринг ``ReferenceBuildLimits``. Frontier
        # сравнивает ``depth > max_depth``, поэтому «нет потолка» выражается числом,
        # недостижимым для URL (глубина не может превысить число сегментов пути), а не
        # выдуманным потолком: реальная граница пространства — path-prefix объявленного
        # корня и ``max_documents``, если он объявлен.
        max_depth=(
            _UNBOUNDED_DEPTH if limits.max_depth is None else max(0, int(limits.max_depth))
        ),
        max_documents=limits.max_documents,
        exclude_paths=tuple(limits.exclude_paths),
        rate_limit=limits.rate_limit,
        rate_limit_explicit=limits.rate_limit is not None,
        document_level=limits.document_level,
        pdf_pipeline=limits.pdf_pipeline,
        # Гейт robots входит в ``CrawlLimits.as_dict`` всегда; объявленным он становится
        # тогда и только тогда, когда политика действительно передана.
        robots=robots is not None,
    )

    skipped: dict[str, int] = {}
    errors: list[str] = []
    truncated: set[str] = set()
    buffered_chunks: list[CorpusChunk] = []
    seen_hashes: set[str] = set()
    docs = 0

    store = store_factory(staging / "reference.db")
    try:
        limiter = RateLimiter(sleep=sleep)
        root_hosts = frozenset(
            normalize_scope_host(_host(root)) for root in declared_roots
        )
        for root in declared_roots:
            try:
                validate_url(root)
            except ValueError as exc:
                _count(skipped, SkipReason.ROOT_REJECTED)
                errors.append(f"{layout.product_id}/{root}: root_rejected: {exc}")
                continue
            frontier = Frontier(
                root,
                crawl_limits,
                scope_hosts=_declared_subdomains(
                    _host(root),
                    profiles,
                    root_hosts - {normalize_scope_host(_host(root))},
                ),
            )
            seed_status = frontier.add_seed(root)
            docs += _drain_root(
                frontier=frontier,
                layout=layout,
                profiles=profiles,
                robots=robots,
                limiter=limiter,
                rate_spec=rate_spec,
                crawler=crawler,
                extractor=extractor,
                chunker=chunker,
                store=store,
                skipped=skipped,
                errors=errors,
                buffered_chunks=buffered_chunks,
                seen_hashes=seen_hashes,
            )
            # Блэклист ведёт frontier (он видит и seeds, и ссылки), поэтому суммируем один
            # раз на корень; seed уже посчитан по коду возврата ``add_seed``.
            already_counted = 1 if seed_status == "blacklisted" else 0
            extra_blacklisted = max(0, len(frontier.skipped_blacklisted) - already_counted)
            if extra_blacklisted:
                key = SkipReason.BLACKLISTED.value
                skipped[key] = skipped.get(key, 0) + extra_blacklisted
            if frontier.truncated_reason == "max_depth":
                _count(skipped, SkipReason.DEPTH)
            if frontier.truncated_reason is not None:
                truncated.add(frontier.truncated_reason)
        # Схема адреса входит в отпечаток: текст может совпасть байт-в-байт, а адрес
        # чанка — нет, и тогда это другая сборка (см. ``reference_content_digest``).
        content_digest = reference_content_digest(
            buffered_chunks, address_scheme=CHUNK_ID_SCHEME
        )
        build_id = reference_build_id(layout.product_id, layout.version, content_digest)
        # Чанки пишутся одним заходом: ``build_id`` чанка — идентичность **сборки**, то
        # есть функция всех её текстов, и до последнего документа он неизвестен.
        store.add_chunks_batch(
            [chunk.model_copy(update={"build_id": build_id}) for chunk in buffered_chunks]
        )
    finally:
        store.close()

    reason_list = _incomplete_reasons(skipped, sorted(truncated))
    complete = not reason_list and not errors
    manifest = ReferenceManifest(
        schema=REFERENCE_SCHEMA,
        build_id=build_id,
        package_id=layout.package_id,
        product_id=layout.product_id,
        version=layout.version,
        content_digest=content_digest,
        docs=docs,
        chunks=len(buffered_chunks),
        extractor_version=_version_of(extractor),
        chunker_version=_version_of(chunker),
        observed_at=_now_iso(),
        roots=list(declared_roots),
        complete=complete,
        incomplete_reasons=reason_list,
        skipped=dict(sorted(skipped.items())),
        errors=list(errors),
    )

    final_dir = layout.build_dir(build_id)
    reused = final_dir.exists()
    if reused:
        existing = read_manifest(layout.manifest_path(build_id))
        if existing is None:
            raise ReferenceBuildConflict(
                f"каталог сборки существует, но манифеста в нём нет: {final_dir}. "
                "Идентичность не подтверждается — удалите каталог вручную, "
                "автоматическая перезапись запрещена."
            )
        if existing.content_digest != content_digest:
            raise ReferenceBuildConflict(
                f"каталог {final_dir} описывает другое содержимое "
                f"({existing.content_digest} против {content_digest}) при совпавшем "
                "build_id — это признак повреждённого каталога, а не повод перезаписать."
            )
        shutil.rmtree(staging, ignore_errors=True)
        manifest = existing
    else:
        write_manifest(staging / "manifest.json", manifest)
        os.replace(staging, final_dir)
    # Указатель пишется в обоих случаях: оператор собрал именно эту версию, и
    # «текущая» — та, которую он только что попросил (переиспользованная сборка
    # становится текущей, а не остаётся в тени более новой).
    write_current_build(layout, build_id)

    return ReferenceBuildResult(
        build_id=build_id,
        content_digest=manifest.content_digest,
        docs=manifest.docs,
        chunks=manifest.chunks,
        skipped=dict(manifest.skipped),
        errors=list(manifest.errors),
        truncated=_truncated_reasons(manifest),
        complete=manifest.complete,
        roots=list(manifest.roots),
        reused=reused,
        db_path=layout.db_path(build_id),
        manifest=manifest,
    )
