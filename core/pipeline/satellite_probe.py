"""coHound — драйвер разведки корней сателлита: сеть через инъецируемые порты.

Ядро вывода живёт в :mod:`core.pipeline.satellite_roots` (чистые функции); здесь только
наблюдение: фетч landing-страницы сателлита, извлечение ссылок, чтение карты сайта и
решение robots. Разделение то же, что у ``crawl-probe``: ядро отвечает «что из этого
следует», драйвер — «что мы увидели».

Порядок транспорта: ``http`` → ``browser`` (если браузер инъецирован). Это не «догадка о
продукте»: часть порталов отдаёт 200 только браузеру, и там, где http вернул страницу без
текста, честнее попробовать второй транспорт, чем объявить корень несуществующим. Обе
попытки видны в плане (``notes``), неудача обеих — ``unresolved``, а не пустой корень.

Нейтрален к предметной категории (§1.3): адреса и роли — данные.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from core.domain.enums import ExtractionRoute, FetchMode
from core.pipeline.satellite_roots import SatelliteRootPlan, derive_roots
from core.ports.types import FetchPlan
from core.util.html_links import extract_links

__all__ = ["SatelliteProbePorts", "probe_satellite_roots"]

#: Сколько ссылок landing-страницы разбирать. Порог **наблюдения**, не корпуса: больше
#: ссылок не меняет вывод о префиксе, но платит временем и памятью на каждой странице.
MAX_LINKS = 400


@dataclass
class SatelliteProbePorts:
    """Порты наблюдения (сеть — только через них; тесты инъецируют фейки)."""

    http: Any
    extractor: Any
    browser: Any | None = None
    robots: Any | None = None


#: Маркеры страницы-заглушки антибот-защиты. Это не «сайт пуст»: страница отдала
#: скрипт проверки, и ссылок в ней нет по построению. Прибор обязан назвать это своим
#: именем, иначе «0 ссылок» читается как «дерева нет».
CHALLENGE_MARKERS = (
    "__qrator",
    "jschl",
    "cf-browser-verification",
    "_incapsula_resource",
    "ddos-guard",
    "captcha",
)

#: Сколько символов сообщения об ошибке транспорта сохранять в замечании.
_ERROR_BRIEF = 160


def _brief(exc: BaseException) -> str:
    """``Тип: сообщение`` — коротко, но с диагнозом (тип без текста ничего не говорит)."""
    text = " ".join(str(exc).split())
    if len(text) > _ERROR_BRIEF:
        text = text[:_ERROR_BRIEF] + "…"
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


def _challenge_marker(content: bytes) -> str | None:
    """Маркер антибот-заглушки в теле страницы (``None`` — обычная страница)."""
    if not content:
        return None
    lowered = content[:20000].decode("utf-8", "ignore").lower()
    return next((marker for marker in CHALLENGE_MARKERS if marker in lowered), None)


@dataclass
class _Page:
    final_url: str
    content: bytes
    text_chars: int
    status: int | None
    transport: str
    error: str | None = None
    challenge: str | None = None


def _fetch(url: str, ports: SatelliteProbePorts) -> _Page:
    """Фетч landing-страницы: http, при неудаче — browser (если он есть)."""
    attempts: list[str] = []
    for transport in ("http", "browser"):
        crawler = ports.http if transport == "http" else ports.browser
        if crawler is None:
            attempts.append(f"{transport}: unavailable")
            continue
        # План получения передаётся готовым (контракт ``CrawlerPort``): адаптер лишь
        # исполняет его. Передача голого ``FetchMode`` компилируется, но падает в
        # адаптере на ``plan.fetch`` — ошибка типа, которую ловит только реальный вызов.
        plan = FetchPlan(FetchMode.HTTP if transport == "http" else FetchMode.BROWSER)
        try:
            page = crawler.fetch(url, plan)
        except Exception as exc:  # noqa: BLE001 — сбой транспорта изолируется
            # Причина сохраняется **с текстом**: «AttributeError» без сообщения и
            # «401 Unauthorized» — разные диагнозы, и первый читается как «ничего не
            # нашли», хотя это был отказ сайта или дефект вызова.
            attempts.append(f"{transport}: {_brief(exc)}")
            continue
        content = getattr(page, "content", b"") or b""
        text_chars = 0
        try:
            doc = ports.extractor.extract(page, ExtractionRoute.AUTO)
            text_chars = len(getattr(doc, "text", "") or "")
        except Exception:  # noqa: BLE001 — текст нужен лишь как мера, не как данные
            text_chars = 0
        final_url = str(getattr(page, "url", None) or url)
        if not content:
            attempts.append(f"{transport}: empty")
            continue
        return _Page(
            final_url=final_url,
            content=content,
            text_chars=text_chars,
            status=getattr(page, "status_code", None),
            transport=transport,
            challenge=_challenge_marker(content),
        )
    return _Page(
        final_url=url,
        content=b"",
        text_chars=0,
        status=None,
        transport="none",
        error="; ".join(attempts) or "no_transport",
    )


def _read_sitemap(sitemap_url: str | None, ports: SatelliteProbePorts) -> list[str]:
    """``<loc>`` карты сайта, если оператор её объявил; иначе — пусто (не выдумываем)."""
    if not sitemap_url:
        return []
    from core.util.sitemap import expand_sitemap, parse_sitemap

    try:
        page = ports.http.fetch(sitemap_url, FetchMode.HTTP)
    except Exception:  # noqa: BLE001 — карта сайта опциональна
        return []
    content = getattr(page, "content", b"") or b""
    if not content:
        return []
    def _fetch_child(url: str):
        """Дочерняя карта (``sitemapindex``): тот же транспорт, тот же разбор."""
        child = ports.http.fetch(url, FetchMode.HTTP)
        payload = getattr(child, "content", b"") or b""
        return parse_sitemap(payload, base_url=url)

    try:
        document = parse_sitemap(content, base_url=sitemap_url)
        entries, _truncated = expand_sitemap(document, _fetch_child)
    except Exception:  # noqa: BLE001 — битая карта = нет наблюдения
        return []
    return [entry.url for entry in entries if getattr(entry, "url", None)]


def probe_satellite_roots(
    *,
    product_id: str,
    satellites: Sequence[Any],
    ports: SatelliteProbePorts,
    sitemap_urls: dict[str, str] | None = None,
    seed_urls: dict[str, str] | None = None,
    max_links: int = MAX_LINKS,
) -> list[SatelliteRootPlan]:
    """Разведывает корни каждого сателлита, у которого известно место (evidence URL).

    Сателлит без ``relationship_evidence.url`` пропускается **видимо**: план с
    ``notes=["no_evidence_url"]`` и пустым набором корней. Иначе «корней нет» было бы
    неотличимо от «адреса не знаем», а это разные факты.

    :param sitemap_urls: ``{role: sitemap_url}`` — объявленные оператором карты сайта.
    :param seed_urls: ``{role: url}`` — адрес наблюдения, если свидетельство связи
        указывает не туда (маркетинговый корень, антибот-заглушка). Это **вход
        наблюдения**, объявленный оператором, а не догадка прибора: без него разведка
        корней на таком сайте не начинается вообще.
    :param max_links: предел разбора ссылок одной страницы (порог наблюдения).
    """
    plans: list[SatelliteRootPlan] = []
    sitemap_by_role = sitemap_urls or {}
    seeds_by_role = seed_urls or {}
    for satellite in satellites:
        role = str(getattr(satellite, "role", "") or "")
        component = getattr(satellite, "component", None)
        evidence = getattr(satellite, "relationship_evidence", None)
        seed_url = seeds_by_role.get(role) or (
            str(getattr(evidence, "url", "") or "") if evidence is not None else ""
        )
        if not seed_url:
            plan = SatelliteRootPlan(
                product_id=product_id,
                role=role,
                component=component,
                seed_url="",
            )
            plan.notes.append("no_evidence_url")
            plans.append(plan)
            continue

        page = _fetch(seed_url, ports)
        robots_allows: bool | None = None
        if ports.robots is not None:
            try:
                robots_allows = bool(ports.robots.allowed(seed_url))
            except Exception:  # noqa: BLE001 — отказ чтения robots не роняет разведку
                robots_allows = None
        links: list[str] = []
        if page.content:
            try:
                links = extract_links(page.content, page.final_url)[:max_links]
            except Exception:  # noqa: BLE001 — битая разметка = нет наблюдения
                links = []
        sitemap = _read_sitemap(sitemap_by_role.get(role), ports)
        plan = derive_roots(
            product_id=product_id,
            role=role,
            component=component,
            seed_url=seed_url,
            final_url=page.final_url,
            links=links,
            sitemap_urls=sitemap,
            robots_allows=robots_allows,
            seed_declared=role in seeds_by_role,
        )
        if role in seeds_by_role:
            plan.notes.append(f"seed: {seed_url} (объявлен оператором)")
        plan.notes.append(f"transport: {page.transport}")
        plan.notes.append(f"text_chars: {page.text_chars}")
        plan.notes.append(f"links: {len(links)}")
        # Замечание утверждает «ссылок нет по построению», поэтому оно допустимо
        # только когда ссылок действительно нет. Виджет проверки на обычной странице
        # (форма обратной связи, `recaptcha` в разметке) — не заглушка: измерено
        # 27.11.2026 на `jatoba.ru` (121 ссылка и слово `captcha` в разметке), где
        # прежняя безусловная пометка противоречила собственным числам.
        if page.challenge and not links:
            plan.notes.append(
                f"antibot_challenge: страница отдала проверку ({page.challenge}) — "
                "ссылок в ней нет по построению, это не пустое дерево"
            )
        if page.error:
            plan.notes.append(f"fetch_error: {page.error}")
        elif _host_of(page.final_url) != _host_of(seed_url):
            plan.notes.append("redirect_to_other_host")
        plans.append(plan)
    return plans


def _host_of(url: str) -> str:
    return (urlsplit(url).hostname or "").lower()
