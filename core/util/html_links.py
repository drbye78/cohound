"""coHound — извлечение гиперссылок из HTML (чистый stdlib; D1, §10).

Обход in-site ссылок при крауле (``max_depth > 0``) начинается с извлечения `<a href>`
из уже добытой страницы. Утилита намеренно **минимальна** (A1) и не тянет новых
зависимостей: ``html.parser.HTMLParser`` из стандартной библиотеки (``bs4``/``selectolax``
живут в extra ``fallback`` и на пути краула **не** используются).

Нормализация каждой ссылки:

* ``urljoin(base_url, href)`` — относительные ссылки разрешаются против базового URL
  (финального URL страницы после редиректов, если он известен вызывающему);
* ``href`` пустой или начинающийся с ``#`` — отбрасывается (внутристраничная навигация);
* ``validate_url`` (SSRF-политика, ``core.util.security``) отсекает не-``http(s)``
  схемы (``mailto:``/``javascript:``/``data:``/``tel:``/``ftp:``) и приватные/
  metadata-цели: такая ссылка не попадает в frontier;
* фрагмент (``#frag``) отбрасывается; **scheme** и **host** приводятся к нижнему
  регистру, дефолтный порт (80/``http``, 443/``https``) убирается; userinfo
  отбрасывается; **path и query сохраняются дословно** (регистр пути значим);
* дедупликация **в пределах страницы** с сохранением порядка первого появления.

Извлекается **только** ``<a href>``. ``rel``/``nofollow`` игнорируются (решение —
черновик политики, не браузерная семантика), теги ``<area>``/``<link>``/``<iframe>``/
``<img>`` не рассматриваются.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit, urlunsplit

from core.util.security import validate_url

__all__ = ["extract_links"]

#: Дефолтные порты по схеме — убираются из нормализованного URL.
_DEFAULT_PORTS = {"http": "80", "https": "443"}


class _AnchorParser(HTMLParser):
    """Минимальный парсер: собирает значения ``href`` только у тега ``<a>``."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        if tag != "a":
            return
        for name, value in attrs:
            if name == "href" and value:
                self.hrefs.append(value)


def _normalize(base_url: str, href: str) -> str | None:
    """Нормализует одну ссылку; ``None`` — если она непригодна для краула.

    Правила — в докстринге модуля. Возвращает канонический ``http(s)``-URL без фрагмента
    либо ``None`` (пустая/``#``-ссылка, не-``http(s)`` схема, SSRF-отказ, нет хоста,
    некорректный порт).
    """
    if not href or href.startswith("#"):
        return None
    candidate = urljoin(base_url, href)
    parsed = urlsplit(candidate)
    scheme = parsed.scheme.lower()
    if scheme not in ("http", "https"):
        return None
    # SSRF-политика: отклоняем не-http(s) и приватные/metadata-цели (тот же барьер, что у
    # адаптеров). Scope дополнительно проверяет frontier — одного SSRF-чека мало.
    try:
        validate_url(candidate)
    except ValueError:
        return None
    host = (parsed.hostname or "").lower()
    if not host:
        return None
    try:
        port = parsed.port
    except ValueError:
        return None
    netloc = host
    if port is not None and str(port) != _DEFAULT_PORTS.get(scheme):
        netloc = f"{host}:{port}"
    # path/query — дословно; fragment сброшен.
    return urlunsplit((scheme, netloc, parsed.path, parsed.query, ""))


def extract_links(content: bytes, base_url: str) -> list[str]:
    """Извлекает и нормализует in-page ссылки ``<a href>`` из сырых байтов HTML.

    :param content: сырые байты страницы (как в ``RawPage.content``). Декодируются как
        UTF-8 с ``errors="replace"`` — извлечение ссылок устойчиво к битой кодировке.
    :param base_url: базовый URL для разрешения относительных ссылок (финальный URL
        страницы, если известен; иначе — запрошенный).
    :returns: упорядоченный (первое появление) список дедуплицированных нормализованных
        URL. Непригодные ссылки молча отбрасываются (это не ошибка страницы).
    """
    parser = _AnchorParser()
    try:
        parser.feed(content.decode("utf-8", errors="replace"))
        parser.close()
    except Exception:  # noqa: BLE001, S110 — битый HTML не должен ронять краул
        pass

    seen: set[str] = set()
    links: list[str] = []
    for href in parser.hrefs:
        url = _normalize(base_url, href)
        if url is None or url in seen:
            continue
        seen.add(url)
        links.append(url)
    return links
