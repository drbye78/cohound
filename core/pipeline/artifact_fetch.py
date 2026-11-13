"""Загрузка объявленного артефакта в пакет — единственный путь байтов оператора.

**Зачем.** Механизм ручной подачи (``13-manual-artifacts``) состоял из двух половин:
загрузчик декларации (:mod:`core.pipeline.manual_artifacts`) и фаза ``corpus``
(:func:`~core.pipeline.corpus_ingest.feed_manual_artifact`). Между ними был провал —
**положить файл** в пакет. Оператор делал это руками: сам придумывал путь, сам считал
``sha256``, сам заполнял YAML. Ошибка в любом из трёх шагов видна только на следующем
прогоне (``ValueError`` из загрузчика), а до него ``run_id`` уже посчитан по
несуществующей подаче. Модуль закрывает провал: фетч → файл в пакете → запись
декларации, с провенансом и видимыми отказами.

**Границы те же, что у краула, и они не ослабляются.** SSRF-валидация, scope
объявленных хостов продукта и потолок ``crawl.max_artifact_bytes`` — ровно те же
ворота, через которые проходит любой URL прогона. Подача **не** расширяет scope: иначе
«предоставление» стало бы способом дописать корпус чужим хостом, то есть
``execution_change``, проведённым мимо ``run_id``.

**Контейнеры не спускаются.** Спуск (``zip``/``7z``/``rar``) живёт внутри дренажа корня
(пулы, окна, провенанс) и ручной подаче не подключён. Объявленный контейнер получает
видимый отказ ``container_not_supported_for_manual``, а не «молча скормить архив
экстрактору»: предоставлять нужно членов контейнера.

**Отказ — значение, не исключение.** Каждый шаг допуска возвращает
:class:`FetchOutcome` со ``status="refused"``/``"failed"`` и причиной: один
недоступный файл не должен отменять уже скачанные. Исключение остаётся только для
дефекта (не-``CrawlError`` из краулера) и для ошибок записи на диск.

**Детерминизм.** ``artifact_relpath`` — чистая функция ``(product_id, url) → путь``:
один URL всегда даёт один и тот же файл, поэтому повторный запуск не создаёт копий.
Коллизия слагов (разные URL → одинаковый путь) разрешается суффиксом
``-<8 hex sha1(url)>``, но только когда путь **уже занят другим URL** в декларации —
иначе перезапись чужого файла была бы тихой потерей.

Нейтрален (§1.3): нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

import contextlib
import errno
import hashlib
import os
import re
import shutil
import tempfile
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import unquote, urlsplit

import yaml

from core.config.domain_profiles import DomainProfile
from core.pipeline.crawl_frontier import CrawlLimits
from core.pipeline.manual_artifacts import MANUAL_ARTIFACTS_FILE
from core.ports.crawl_errors import CrawlError
from core.ports.crawler import CrawlerPort
from core.ports.types import FetchMode, FetchPlan
from core.util.document_formats import is_attachment_url
from core.util.hashing import content_hash
from core.util.security import validate_url

__all__ = [
    "MANUAL_DIR",
    "FetchOutcome",
    "artifact_relpath",
    "fetch_artifact",
    "fetch_artifacts",
]

#: Каталог подач внутри пакета (``03`` §A.1, ``sources/``).
MANUAL_DIR = "sources/manual"

#: Предел длины имени файла в пути подачи (байты ФС вторичны: имя обрезается по символам).
MAX_SLUG_CHARS = 120

#: Режим нового файла пакета (как у ``atomic_write_text``): читаемый, не секрет.
_DEFAULT_FILE_MODE = 0o644

#: Размер порции потокового чтения при сверке ``sha256`` (1 MiB: файл не в RAM целиком).
_READ_CHUNK = 1024 * 1024


@dataclass(frozen=True)
class FetchOutcome:
    """Исход одной подачи: что просили, что получили, куда положили.

    ``status`` — ``fetched`` (файл записан), ``unchanged`` (байты те же, файл не
    тронут), ``refused`` (отказ допуска: SSRF, чужой хост, контейнер, потолок байтов),
    ``failed`` (транспорт/сеть), ``planned`` (``dry_run``: путь и хеш посчитаны, записи
    не было). ``declared_path`` — путь **относительно каталога пакета**: декларация
    переезжает вместе с пакетом, абсолютный путь в ней не хранится.
    """

    product_id: str
    requested_url: str
    final_url: str | None
    status: str
    reason: str | None
    declared_path: str | None
    sha256: str | None
    size: int | None
    content_type: str | None
    status_code: int | None


def _safe_segment(value: str) -> str:
    """Безопасный сегмент пути: буквы/цифры (любые Unicode), ``.``/``_``/``-``.

    Прочее заменяется ``_``, повторы схлопываются, крайние ``.``/``_``/``-`` снимаются.
    ``..`` и ``.`` становятся ``_`` — иначе percent-декодированный путь мог бы выйти за
    каталог подач.
    """
    chars = [
        ch if (ch.isalnum() or ch in "._-") else "_" for ch in str(value).strip()
    ]
    text = re.sub(r"_{2,}", "_", "".join(chars))
    return text.strip("._-") or "_"


def _truncate(name: str, limit: int = MAX_SLUG_CHARS) -> str:
    """Обрезает имя до ``limit`` символов, сохраняя расширение (``.pdf`` и т.п.)."""
    if len(name) <= limit:
        return name
    suffix = PurePosixPath(name).suffix
    if not suffix or len(suffix) >= limit:
        return name[:limit]
    head = name[: limit - len(suffix)].rstrip("._-") or "_"
    return head + suffix


def artifact_relpath(product_id: str, url: str) -> str:
    """Относительный путь подачи в пакете: ``sources/manual/<product_id>/<slug>``.

    Slug — percent-декодированный путь URL (каталоги сохраняются, чтобы два файла из
    разных разделов не столкнулись без нужды). Функция **чистая**: файловой системы не
    трогает, поэтому один URL всегда даёт один путь, а результат не зависит от того,
    что уже лежит в пакете.
    """
    path = unquote(urlsplit(url).path or "/")
    segments = [_safe_segment(part) for part in path.split("/") if part.strip()]
    if not segments:
        segments = ["index"]
    segments[-1] = _truncate(segments[-1])
    return f"{MANUAL_DIR}/{_safe_segment(str(product_id))}/{'/'.join(segments)}"


def _sha256_file(path: Path) -> str:
    """SHA-256 файла потоково (файл не поднимается в RAM целиком)."""
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(_READ_CHUNK), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _target_mode(target: Path) -> int:
    """Режим, который получит файл: существующий (авторский выбор) либо дефолт."""
    try:
        return target.stat().st_mode & 0o777
    except OSError:
        return _DEFAULT_FILE_MODE


def _write_bytes_atomic(target: Path, data: bytes) -> None:
    """Атомарная запись байтов: temp в том же каталоге + ``os.replace``.

    При ошибке целевой файл не меняется, временный удаляется. Режим существующего файла
    сохраняется (``mkstemp`` даёт ``0o600``, и без ``chmod`` артефакт пакета стал бы
    нечитаемым для группы).
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    mode = _target_mode(target)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(target.parent), prefix=f".{target.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
        os.chmod(tmp_name, mode)
        os.replace(tmp_name, target)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_name)
        raise


def _publish_staged(staged: Path, target: Path) -> None:
    """Переезд staging-файла в пакет (``os.replace``); ``EXDEV`` — потоковая копия.

    Staging-каталог создаёт вызывающий, но он может оказаться на другом ФС (tmpfs):
    тогда ``os.replace`` не сработает, и файл копируется ``shutil.copyfile`` (sendfile,
    без подъёма тела в RAM) с удалением staging-копии.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    mode = _target_mode(target)
    try:
        os.replace(staged, target)
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
        shutil.copyfile(staged, target)
        staged.unlink(missing_ok=True)
    os.chmod(target, mode)


def _with_collision_suffix(relpath: str, url: str) -> str:
    """Добавляет ``-<8 hex sha1(url)>`` перед расширением (детерминированно для URL)."""
    suffix = hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]  # noqa: S324 - не криптография
    head, dot, ext = relpath.rpartition(".")
    if dot and "/" not in ext:
        return f"{head}-{suffix}.{ext}"
    return f"{relpath}-{suffix}"


def _declared_claims(package_dir: Path) -> dict[str, str]:
    """``path → url`` по **сырой** декларации пакета (без валидации байтов).

    Нужна ровно для одного вопроса: занят ли путь другим URL. Полный
    ``load_manual_artifacts`` здесь неприменим — он требует существующих файлов и
    сверяет хеши, то есть падал бы на подаче, которую как раз и собираются сделать.
    Битый/отсутствующий файл — пустое отображение: коллизию тогда не с чем сверять.
    """
    path = Path(package_dir) / MANUAL_ARTIFACTS_FILE
    if not path.is_file():
        return {}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return {}
    if not isinstance(data, dict):
        return {}
    raw_entries = data.get("provide")
    if not isinstance(raw_entries, list):
        return {}
    claims: dict[str, str] = {}
    for item in raw_entries:
        if not isinstance(item, dict):
            continue
        declared = item.get("path")
        declared_url = item.get("url")
        if isinstance(declared, str) and isinstance(declared_url, str) and declared:
            claims[Path(declared).as_posix()] = declared_url
    return claims


def _refused(
    product_id: str, url: str, reason: str, *, status: str = "refused"
) -> FetchOutcome:
    """Отказ допуска: ни файла, ни пути — только причина."""
    return FetchOutcome(
        product_id=str(product_id),
        requested_url=str(url),
        final_url=None,
        status=status,
        reason=reason,
        declared_path=None,
        sha256=None,
        size=None,
        content_type=None,
        status_code=None,
    )


def _scope_refusal(
    product_id: str, url: str, roots: Sequence[str], domain_profiles: Mapping[str, DomainProfile] | None
) -> str | None:
    """Причина отказа по scope либо ``None`` (хост объявлен).

    Импорт внутри функции: ``corpus_persist`` тянет за собой весь слой корпуса, а
    ``artifact_fetch`` — лист для CLI. Одна семантика с фазой ``corpus`` важнее
    расположения импорта.
    """
    from core.pipeline.corpus_persist import manual_host_scope
    from core.pipeline.serp_loop import serp_host_in_scope

    host = urlsplit(url).hostname or ""
    own_hosts, declared_hosts = manual_host_scope(
        str(product_id),
        url_map={str(product_id): [str(root) for root in roots]},
        domain_profiles=domain_profiles,
    )
    if serp_host_in_scope(host, own_hosts, declared_hosts):
        return None
    return (
        f"host_out_of_scope: хост {host!r} не объявлен ни одним корнем продукта "
        f"{product_id!r} и не назван в domain_profiles — предоставление не расширяет scope"
    )


def fetch_artifact(
    *,
    package_dir: Path,
    product_id: str,
    url: str,
    crawler: CrawlerPort,
    limits_for: Callable[[str], CrawlLimits],
    staging_dir: Path,
    roots: Sequence[str],
    domain_profiles: Mapping[str, DomainProfile] | None = None,
    dry_run: bool = False,
) -> FetchOutcome:
    """Скачивает один объявленный артефакт и кладёт его в пакет.

    Шаги идут строго по порядку — дешёвые отказы раньше сети:

    1. SSRF-валидация (:func:`core.util.security.validate_url`) → ``refused``
       (``ssrf: …``);
    2. scope объявленных хостов продукта → ``refused`` (``host_out_of_scope: …``);
    3. контейнер по суффиксу URL → ``refused`` (``container_not_supported_for_manual``);
    4. пределы ``limits_for(url)`` (те же оси, что у краула);
    5. ``FetchPlan(fetch=HTTP, robots=False, …)``: staging задан **всегда** — терминальный
       артефакт уходит в файл, страница остаётся в RAM под ``max_response_bytes``
       (решает адаптер, ядро лишь объявляет оба потолка);
    6. ``crawler.fetch``: ``CrawlError`` → ``failed`` (класс отказа в ``reason``);
    7. байты — ``page.staged_path`` (файл) либо ``page.content``;
    8. ``size > max_artifact_bytes`` → ``refused`` с обоими числами (тот же потолок, на
       котором краулер отказал бы этому файлу, — не новый лимит);
    9. ``artifact_relpath(product_id, page.url or url)`` + ``sha256`` из байтов;
    10. целевой файл есть и его хеш равен → ``unchanged`` (ничего не пишется); иначе
        атомарная запись → ``fetched``;
    11. ``dry_run`` → ``planned`` с заполненными ``declared_path``/``sha256``/``size``:
        решение «что будет, если» принимается без записи, а сеть при этом трогается
        (иначе хеш и размер были бы выдуманы).

    :returns: исход; ``declared_path`` — путь относительно каталога пакета.
    :raises OSError: сбой записи на диск (место/права) — это дефект окружения, а не
        отказ подачи, и молча его глотать нельзя.
    """
    product_id = str(product_id)
    url = str(url)
    # 1. SSRF — та же единственная проверка URL, что у всего прогона.
    try:
        validate_url(url)
    except ValueError as exc:
        return _refused(product_id, url, f"ssrf: {exc}")
    # 2. Scope — только объявленные хосты продукта (поддомен — если объявлен).
    scope_reason = _scope_refusal(product_id, url, roots, domain_profiles)
    if scope_reason is not None:
        return _refused(product_id, url, scope_reason)
    # 3. Контейнер: спуск исполняет только краул — предоставлять нужно членов.
    if is_attachment_url(url):
        return _refused(
            product_id,
            url,
            "container_not_supported_for_manual: спуск в контейнеры исполняет только "
            "краул (пулы, окна, провенанс) — предоставьте документы-члены",
        )
    # 4. Пределы — объявленные оси краула (app < run < site), не новые потолки.
    limits = limits_for(url)
    staging = Path(staging_dir)
    staging.mkdir(parents=True, exist_ok=True)
    # 5. План: HTTP без robots (robots уже отказал — иначе подачи бы не было), staging
    #    включён всегда: адаптер сам решит, страница это или терминальный артефакт.
    plan = FetchPlan(
        fetch=FetchMode.HTTP,
        robots=False,
        max_response_bytes=limits.max_response_bytes,
        staging_dir=str(staging),
        max_stream_bytes=limits.max_artifact_bytes,
        attachment_formats=limits.attachment_formats,
    )
    # 6. Фетч: сбой транспорта — значение, а не исключение.
    try:
        page = crawler.fetch(url, plan)
    except CrawlError as exc:
        kind = (
            getattr(exc, "failure_kind", None)
            or getattr(exc, "refusal_reason", None)
            or type(exc).__name__
        )
        return replace(
            _refused(product_id, url, f"{kind}: {exc}", status="failed"),
            status_code=getattr(exc, "http_status", None),
        )
    final_url = page.url or url
    staged = Path(page.staged_path) if page.staged_path else None
    # 7. Байты: файл staging либо тело страницы.
    try:
        if staged is not None:
            size = staged.stat().st_size
            digest = _sha256_file(staged)
        else:
            size = len(page.content)
            digest = content_hash(page.content)
    except OSError as exc:
        return _refused(
            product_id, url, f"staging_unreadable: {type(exc).__name__}: {exc}", status="failed"
        )
    # 8. Потолок байтов — оба числа в причине, чтобы решение было проверяемым.
    if size > limits.max_artifact_bytes:
        if staged is not None:
            with contextlib.suppress(OSError):
                staged.unlink(missing_ok=True)
        return _refused(
            product_id,
            url,
            f"payload_too_large: {size} Б > crawl.max_artifact_bytes="
            f"{limits.max_artifact_bytes}",
        )
    # 9. Путь: детерминированный слаг; коллизия с чужим URL — суффикс из sha1(url).
    relpath = artifact_relpath(product_id, final_url)
    claimant = _declared_claims(Path(package_dir)).get(relpath)
    if claimant is not None and claimant != final_url:
        relpath = _with_collision_suffix(relpath, final_url)
    target = Path(package_dir) / relpath
    # 10/11. Сверка и запись (или план).
    if dry_run:
        if staged is not None:
            with contextlib.suppress(OSError):
                staged.unlink(missing_ok=True)
        return FetchOutcome(
            product_id=product_id,
            requested_url=url,
            final_url=final_url,
            status="planned",
            reason=None,
            declared_path=relpath,
            sha256=digest,
            size=size,
            content_type=page.content_type,
            status_code=page.status_code,
        )
    if target.is_file() and _sha256_file(target) == digest:
        if staged is not None:
            with contextlib.suppress(OSError):
                staged.unlink(missing_ok=True)
        return FetchOutcome(
            product_id=product_id,
            requested_url=url,
            final_url=final_url,
            status="unchanged",
            reason=None,
            declared_path=relpath,
            sha256=digest,
            size=size,
            content_type=page.content_type,
            status_code=page.status_code,
        )
    if staged is not None:
        _publish_staged(staged, target)
    else:
        _write_bytes_atomic(target, page.content)
    return FetchOutcome(
        product_id=product_id,
        requested_url=url,
        final_url=final_url,
        status="fetched",
        reason=None,
        declared_path=relpath,
        sha256=digest,
        size=size,
        content_type=page.content_type,
        status_code=page.status_code,
    )


def _as_pair(item: Any) -> tuple[str, str]:
    """``(product_id, url)`` из пары или из записи с одноимёнными полями.

    Вход — либо кортеж (его даёт CLI из ``--url``), либо запись
    :class:`~core.pipeline.blocked_artifacts.BlockedArtifact` (её даёт отбор отказов):
    второй тип импортировать незачем — структурного совпадения полей достаточно.
    """
    if isinstance(item, tuple):
        return str(item[0]), str(item[1])
    return str(item.product_id), str(item.url)


def fetch_artifacts(
    *,
    package_dir: Path,
    items: Iterable[Any],
    crawler: CrawlerPort,
    limits_for: Callable[[str], CrawlLimits],
    staging_dir: Path,
    roots_by_product: Mapping[str, Sequence[str]],
    domain_profiles: Mapping[str, DomainProfile] | None = None,
    dry_run: bool = False,
) -> list[FetchOutcome]:
    """Загружает набор подач по порядку; исход каждой виден отдельно.

    Порядок входа сохраняется, отказы не прерывают набор: один недоступный файл не
    должен отменять остальные. ``roots_by_product`` — объявленные корни **каждого**
    продукта: scope у продуктов разный, и общий список корней разрешил бы подачу
    чужому хосту.
    """
    outcomes: list[FetchOutcome] = []
    for item in items:
        product_id, url = _as_pair(item)
        outcomes.append(
            fetch_artifact(
                package_dir=package_dir,
                product_id=product_id,
                url=url,
                crawler=crawler,
                limits_for=limits_for,
                staging_dir=staging_dir,
                roots=roots_by_product.get(product_id, ()),
                domain_profiles=domain_profiles,
                dry_run=dry_run,
            )
        )
    return outcomes
