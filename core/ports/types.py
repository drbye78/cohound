"""coHound — типы-значения, разделяемые портами (ядро, категорийно-нейтрально).

Здесь живут «сырые» value-объекты, которые пересекают границы портов (краулер/экстрактор/
чанкер/индекс): домен их не импортирует, адаптеры и pipeline — используют. Соответствуют
архитектуре 01 §3.5. Нейтральны (§1.3): никаких предметных терминов.

Классы заморожены (``frozen=True``): provenance и хеши не должны мутироваться на лету.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from core.domain.enums import CrawlStrategy, ExtractionRoute, FetchMode
from core.domain.models import RateLimitSpec

# Переиспользуем доменные оси (D2/D4): краулер видит ``fetch``/``extract``/``strategy``
# как value-объекты, не как строки. Реэкспорт — чтобы адаптеры/edge импортировали их из порт-слоя.
__all__ = [
    "ExtractionRoute",
    "FetchMode",
    "CrawlStrategy",
    "RateLimitSpec",
    "FetchPlan",
    "RawPage",
    "ExtractedDoc",
    "Chunk",
    "ProductRef",
]


@dataclass(frozen=True)
class FetchPlan:
    """План получения и извлечения страницы (D2/D4).

    Оси: ``fetch`` (:class:`~core.domain.enums.FetchMode` — **нужен ли браузер вообще**),
    ``extract`` (:class:`~core.domain.enums.ExtractionRoute` — авто/HTML/документ) и
    ``strategy`` (:class:`~core.domain.enums.CrawlStrategy` — **как** браузер используется
    **при browser-capable адаптере**; при ``fetch=HTTP`` нормализуется в ``AUTO``), плюс
    опциональное JS-условие ожидания для браузерного рендера и опциональный per-host
    диапазон задержек ``rate_limit``. Единый value-объект резолвится в pipeline
    (:func:`core.pipeline.fetch_plan.resolve_fetch_plan`) и передаётся и краулеру, и
    экстрактору — адаптеры сами план не строят.

    ``engine`` — идентификатор движка/транспорта краула, фактически использованного для
    страницы (напр. ``http``/``scrapy``/``browser``): свободная строка-данные, валидируется
    composition root, **не** доменный enum.

    ``robots`` — флаг egress-вежливости (§10): адаптер проверяет ``robots.txt`` только
    если у него **есть** политика (``_robots is not None``) **и** ``robots`` истинно.
    Дефолт ``False`` сохраняет обратную совместимость (никаких лишних fetch ``robots.txt``).

    ``if_none_match``/``if_modified_since`` — **условный запрос** (additive): значения
    заголовков ``If-None-Match``/``If-Modified-Since`` из валидаторов предыдущего
    получения (``RawPage.etag``/``RawPage.last_modified``). ``None`` (дефолт) — обычный
    безусловный GET, байт-идентичный прежнему поведению.
    """

    fetch: FetchMode = FetchMode.HTTP
    extract: ExtractionRoute = ExtractionRoute.AUTO
    wait_for: str | None = None
    strategy: CrawlStrategy = CrawlStrategy.AUTO
    rate_limit: RateLimitSpec | None = None
    engine: str | None = None
    robots: bool = False
    if_none_match: str | None = None
    if_modified_since: str | None = None
    #: P0/OOM: потолок одного ответа (байты) для этого запроса. ``None`` — адаптер
    #: применяет собственный дефолт (20 MiB). Резолвится из ``CrawlLimits`` per seed-root
    #: и переносится на план, чтобы per-site ось дошла до транспорта. Это **in-memory**
    #: потолок: страница свыше него — ``payload_too_large``.
    max_response_bytes: int | None = None
    #: P6: каталог staging для допущенного артефакта-контейнера (``crawl.attachment_*``).
    #: Задан **только** для артефактов: адаптер, умеющий стримить, кладёт тело в файл
    #: этого каталога, а не в RAM. ``None`` (дефолт) — поведение страницы байт-в-байт.
    staging_dir: str | None = None
    #: P6: потоковый потолок артефакта (байты). Действует, только когда задан
    #: ``staging_dir``; превышение — ``payload_too_large`` (видимый отказ, без файла).
    max_stream_bytes: int | None = None
    #: P6: заявленный набор форматов-контейнеров (``crawl.attachment_formats``). Адаптер
    #: решает класс ответа по URL-суффиксу **или** ``Content-Type`` (суффикс-less
    #: download-эндпоинты): только класс «артефакт» вправе уйти в staging-файл.
    #: ``None`` — набор не заявлен, staging не применяется (поведение страницы).
    attachment_formats: tuple[str, ...] | None = None


@dataclass(frozen=True)
class RawPage:
    """Сырая страница — retention-единица (§3.2). ``content`` хранится как байты.

    ``engine`` — идентификатор движка/транспорта, фактически доставившего байты (свободная
    строка-данные, валидируется composition root; **не** доменный enum).

    ``etag``/``last_modified`` — **валидаторы ответа** для условного повторного получения
    (заголовки ``ETag``/``Last-Modified``): их передают обратно в
    :attr:`FetchPlan.if_none_match`/:attr:`FetchPlan.if_modified_since` при следующем
    цикле. ``None`` — сервер валидаторов не дал (или транспорт их не видит).

    ``not_modified`` — сервер подтвердил (``304``), что закэшированное представление
    актуально. Тогда ``content`` — **байты из кэша** (никогда не пустое тело и не
    сфабрикованный ответ); pipeline может пропустить retention/экстракцию. ``False``
    (дефолт) — обычный ответ с новым телом.

    **Файловый staging (P6).** Допущенный артефакт-контейнер (``zip``/``7z``/``rar``)
    может не помещаться в память: тогда адаптер пишет тело в ``staged_path`` (файл
    вызывающего каталога) и оставляет ``content`` **пустым**, а ``head`` — первыми
    8 KiB (классификация/сниф без чтения файла). ``staged_path is None`` — обычная
    страница, ``content`` несёт байты (байт-идентично прежнему поведению).

    **I19.** ``staged_path`` — недоверенный вход: pipeline **сам** пере-хеширует файл
    (потоково) и публикует его в ``corpus_raw``; хеш от адаптера не принимается (его
    здесь и нет). Файл staging после публикации переезжает, а не копируется.

    **Путь к retained-байтам (``source_path``).** Когда задан, экстрактор **может**
    читать файл вместо ``content``, а ``content`` при этом может быть пустым: документ
    не поднимается в RAM воркера только ради передачи его адаптеру (docling всё равно
    читает файл с диска). Это тот же файл, чей хеш — идентичность retention (I19):
    ``corpus_raw/{product_id}/{sha256}.bin``, поэтому чтение с диска сохраняет, а не
    ослабляет верификацию. ``None`` (дефолт) — прежнее поведение: байты в ``content``,
    все существующие вызовы остаются байт-в-байт неизменными.
    """

    url: str
    content: bytes
    fetched_at: str
    fetch_mode: FetchMode = FetchMode.HTTP
    lang: str | None = None
    status_code: int | None = None
    content_type: str | None = None
    engine: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    not_modified: bool = False
    staged_path: str | None = None
    head: bytes | None = None
    #: Путь к retained-байтам этой страницы (``corpus_raw``). Задан — экстрактор вправе
    #: читать файл вместо ``content`` (``content`` может быть пустым); ``None`` — байты в
    #: ``content`` (прежнее поведение).
    source_path: str | None = None


@dataclass(frozen=True)
class ExtractedDoc:
    """Нормализованный документ после экстракции (§3.5).

    ``error`` — грациозная деградация адаптера: сбой извлечения возвращает пустой ``text``
    и заполненный ``error``, а не исключение (см. ``adapters/extract_document.py``).
    """

    text: str
    content_hash: str
    extractor_version: str
    page_count: int | None = None
    error: str | None = None


@dataclass(frozen=True)
class Chunk:
    """Единица индексации (§4.4). ``text`` — чистый текст для валидации цитат."""

    chunk_id: str
    chunk_no: int
    text: str
    heading_path: str
    context_prefix: str
    content_hash: str


@dataclass(frozen=True)
class ProductRef:
    """Минимальная ссылка на продукт для реестровых запросов (нейтрально, §1.3)."""

    product_id: str
    canonical_name: str = ""
    vendor: str = ""
    jurisdiction: str = ""
    extra: dict[str, str] = field(default_factory=dict)
