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
    """

    fetch: FetchMode = FetchMode.HTTP
    extract: ExtractionRoute = ExtractionRoute.AUTO
    wait_for: str | None = None
    strategy: CrawlStrategy = CrawlStrategy.AUTO
    rate_limit: RateLimitSpec | None = None
    engine: str | None = None


@dataclass(frozen=True)
class RawPage:
    """Сырая страница — retention-единица (§3.2). ``content`` хранится как байты.

    ``engine`` — идентификатор движка/транспорта, фактически доставившего байты (свободная
    строка-данные, валидируется composition root; **не** доменный enum).
    """

    url: str
    content: bytes
    fetched_at: str
    fetch_mode: FetchMode = FetchMode.HTTP
    lang: str | None = None
    status_code: int | None = None
    content_type: str | None = None
    engine: str | None = None


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
