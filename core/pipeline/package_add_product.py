"""coHound — авторское добавление продукта в когорту пакета (``hound package add-product``).

Сценарий: по имени продукта (и опциональному URL) разрешить сайт, провести разведку
паспорта (:func:`core.pipeline.passport_scout.scout_passport`), собрать сайты продукта,
вывести ключи ``domain_profiles.yaml``, зондировать оси краула (переиспользуя
:mod:`core.pipeline.crawl_probe_driver`) и — только при явном ``--apply --actor`` —
записать когорту, профили доменов и (опционально) черновик паспорта.

**Границы (A3).** Планирование ничего не пишет. URL не изобретается: без ``--url`` он
разрешается из SERP детерминированным скорингом; неоднозначность/слабое свидетельство —
ошибка с кандидатами (оператор указывает ``--url``). ``wait_for`` никогда не
синтезируется; ``rate_limit`` берётся только из наблюдённого ``429``. Бюджеты краула
выводятся из ограниченного зонда ``robots.txt``/sitemap (не более
:data:`SITEMAP_MAX_FETCHES` фетчей на ключ, robots-honoured).

Модуль **не выполняет I/O на импорте**: все порты/функции инъектируются. Нейтрален к
предметной категории (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlsplit

import yaml

from core.config.domain_profiles import load_domain_profiles
from core.domain.enums import CrawlStrategy, ExtractionRoute, FetchMode
from core.pipeline.crawl_probe import (
    ProbeSource,
    _dump_profile,
    render_profiles_with_verdicts,
)
from core.pipeline.crawl_probe_driver import ProbePorts, run_probe
from core.pipeline.package_candidates import Candidate, apply_candidate
from core.pipeline.package_io import atomic_write_text, load_artifacts
from core.pipeline.scout_common import dedupe, url_allowed
from core.ports.types import FetchPlan
from core.util.security import sanitize_slug
from core.util.sitemap import (
    ROUTE_INDEX,
    SitemapParseError,
    expand_sitemap,
    parse_sitemap,
)

__all__ = [
    "SITEMAP_MAX_FETCHES",
    "AddProductError",
    "AddProductPlan",
    "DiscoveredSite",
    "plan_add_product",
    "apply_add_product",
    "write_domain_profiles_with_new_keys",
]

#: Потолок фетчей ограниченного sitemap-зонда на один ключ (robots + sitemap + дети).
SITEMAP_MAX_FETCHES = 5

#: Потолки бюджетов обхода (D1): path-ключи с/без sitemap и host-root (I17 trap).
_PATH_BUDGET_SITEMAP = (2, None)  # max_depth; max_documents считается от in-scope
_PATH_BUDGET_NO_SITEMAP = (8, 200)
_ROOT_BUDGET = (0, 10)

#: Семейства путей, несущих продуктовую/документационную специфику.
_SPECIFIC_PATH_SEGMENTS = frozenset(
    {"products", "produkty", "docs", "documentation", "tandocs"}
)

#: Хосты-агрегаторы/соцсети/видео/вики/доски вакансий — не продуктовые сайты.
_NON_PRODUCT_SUFFIXES: tuple[str, ...] = (
    "wikipedia.org",
    "wikimedia.org",
    "wikiwand.com",
    "youtube.com",
    "youtu.be",
    "rutube.ru",
    "vimeo.com",
    "vk.com",
    "facebook.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
    "instagram.com",
    "reddit.com",
    "medium.com",
    "habr.com",
    "github.com",
    "gitlab.com",
    "g2.com",
    "capterra.com",
    "trustradius.com",
    "producthunt.com",
    "otzovik.com",
    "irecommend.ru",
    "hh.ru",
    "indeed.com",
    "glassdoor.com",
    "wildberries.ru",
    "ozon.ru",
    "market.yandex.ru",
)

#: Минимальный скор, при котором URL считается убедительным кандидатом.
_MIN_URL_SCORE = 4.0


class AddProductError(RuntimeError):
    """Ошибка планирования/применения ``add-product`` (``usage`` либо ``error``).

    ``code="usage"`` — оператор должен уточнить вход (например, ``--url``/``--reason``);
    ``code="error"`` — разведка/запись не удались. ``candidates`` — кандидаты разрешения
    URL для человекочитаемой диагностики.
    """

    def __init__(
        self,
        message: str,
        *,
        code: str = "usage",
        candidates: Sequence[Mapping[str, Any]] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.candidates = list(candidates or [])


@dataclass(frozen=True)
class DiscoveredSite:
    """Сайт/корень краула, выведенный из собранных URL.

    :param key: ключ ``domain_profiles.yaml`` (``host`` или ``host/path``).
    :param urls: URL, из которых выведен ключ (детерминированно отсортированы).
    :param reason: короткое обоснование вывода ключа.
    :param is_root: **корпоративный** host-root (хост НЕ выделен продукту и имеет
        product-specific path-ключи): ``max_depth: 0``, ``max_documents: 10`` (I17 trap).
        Выделенный продуктовый хост — обычный crawl root (``is_root=False``).
    """

    key: str
    urls: list[str] = field(default_factory=list)
    reason: str = ""
    is_root: bool = False

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление."""
        return {
            "key": self.key,
            "urls": list(self.urls),
            "reason": self.reason,
            "is_root": self.is_root,
        }


@dataclass
class AddProductPlan:
    """План добавления продукта (ничего не пишет; основа ``--json``/артефакта)."""

    product_name: str
    product_id: str
    resolved_url: str
    url_source: str
    url_candidates: list[dict[str, Any]] = field(default_factory=list)
    sites: list[DiscoveredSite] = field(default_factory=list)
    profiles: dict[str, dict[str, Any]] = field(default_factory=dict)
    verdicts: list[Any] = field(default_factory=list)
    budget_basis: dict[str, dict[str, Any]] = field(default_factory=dict)
    cohort_entry: dict[str, Any] = field(default_factory=dict)
    selection_reason: str | None = None
    product_info: dict[str, Any] = field(default_factory=dict)
    needs_human: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    fetched: list[dict[str, Any]] = field(default_factory=list)
    report: Any = field(default=None, repr=False)
    today: _dt.date = field(default_factory=_dt.date.today)

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление плана (без живого отчёта)."""
        return {
            "product_name": self.product_name,
            "product_id": self.product_id,
            "resolved_url": self.resolved_url,
            "url_source": self.url_source,
            "url_candidates": list(self.url_candidates),
            "sites": [site.as_dict() for site in self.sites],
            "profiles": {key: dict(value) for key, value in self.profiles.items()},
            "verdicts": [
                verdict.as_dict() if hasattr(verdict, "as_dict") else dict(verdict)
                for verdict in self.verdicts
            ],
            "budget_basis": {
                key: dict(value) for key, value in self.budget_basis.items()
            },
            "cohort_entry": dict(self.cohort_entry),
            "selection_reason": self.selection_reason,
            "product_info": dict(self.product_info),
            "needs_human": list(self.needs_human),
            "warnings": list(self.warnings),
            "fetched": list(self.fetched),
            "snapshot_date": self.today.isoformat(),
        }


# ---------------------------------------------------------------------------
# URL: нормализация, скоринг, разрешение из SERP
# ---------------------------------------------------------------------------


def _host(url: str) -> str:
    """Хост URL в нижнем регистре (пустая строка — если не разбирается)."""
    return (urlsplit(url).hostname or "").strip().lower()


def _normalize_url(raw: str) -> str:
    """Достраивает схему ``https://`` и отбрасывает пробелы/фрагмент."""
    text = str(raw or "").strip()
    if not text:
        return ""
    if "://" not in text:
        text = f"https://{text}"
    parts = urlsplit(text)
    if not parts.hostname:
        return ""
    query = f"?{parts.query}" if parts.query else ""
    return f"{parts.scheme}://{parts.netloc}{parts.path or '/'}{query}"


def _host_tokens(host: str) -> list[str]:
    """Токены хоста (по точкам/дефисам/подчёркиваниям)."""
    return [token for token in re.split(r"[.\-_]", host.lower()) if token]


def _name_tokens(name: str) -> list[str]:
    """Значимые токены имени продукта/слага (длина ≥ 2, без служебных слов)."""
    tokens = [
        token
        for token in re.findall(r"\w+", str(name or "").casefold(), flags=re.UNICODE)
        if len(token) >= 2
    ]
    return [token for token in dict.fromkeys(tokens) if token not in {"the", "and", "для"}]


def _is_non_product_host(host: str) -> bool:
    """Соцсеть/видео/вики/агрегатор/доска вакансий — не продуктовый сайт."""
    host = host.lower()
    return any(host == suffix or host.endswith(f".{suffix}") for suffix in _NON_PRODUCT_SUFFIXES)


def _is_docs_path(path: str) -> bool:
    """Путь начинается с документного поддерева."""
    segments = [segment for segment in path.strip("/").split("/") if segment]
    return bool(segments) and segments[0].lower() in _SPECIFIC_PATH_SEGMENTS


def _score_candidate(
    url: str, *, name_tokens: Sequence[str], confidence: float = 0.0
) -> float:
    """Детерминированный скор SERP-кандидата (продуктовый хост/путь важнее ранга)."""
    host = _host(url)
    if not host:
        return -100.0
    score = confidence
    host_tokens = _host_tokens(host)
    path_lower = (urlsplit(url).path or "").lower()
    path_tokens = [token for token in re.split(r"[^a-z0-9]+", path_lower) if token]
    if any(token in host_tokens for token in name_tokens):
        score += 8.0
    if any(token in path_tokens for token in name_tokens):
        score += 4.0
    if _is_docs_path(path_lower):
        score += 3.0
    if _is_non_product_host(host):
        score -= 20.0
    if urlsplit(url).query:
        score -= 1.0
    return score


def _resolve_url_from_serp(
    *,
    product_name: str,
    category: str | None,
    search: Any,
    lang: str,
    limit: int,
    discover: Any,
    explicit_url: str | None = None,
) -> tuple[str, list[dict[str, Any]], list[str]]:
    """Разрешает URL продукта из SERP.

    ``explicit_url`` — подсказка ``--url``: она только бустит одноимённый хост/URL в
    скоринге и снимает требование «единственного сильного» кандидата; набор сайтов она
    не сужает. Без подсказки неоднозначность/слабость — :class:`AddProductError`.
    """
    queries = [
        f'"{product_name}" официальный сайт',
        f'"{product_name}" документация',
        f'"{product_name}" docs',
    ]
    if category:
        queries.append(f'"{product_name}" {category}')

    try:
        report = discover(
            queries=queries,
            adapter=search,
            limit=limit,
            lang=lang,
            phase="discovery",
        )
    except Exception as exc:  # noqa: BLE001 — сбой поиска не должен изобретать URL
        if explicit_url:
            return _normalize_url(explicit_url), [], [
                f"SERP недоступен ({type(exc).__name__}: {exc}); сайты ищутся по --url"
            ]
        raise AddProductError(
            f"разведка URL не удалась: {type(exc).__name__}: {exc}",
            code="error",
        ) from exc

    name_tokens = _name_tokens(product_name) or _name_tokens(sanitize_slug(product_name))
    scored: list[dict[str, Any]] = []
    seen: set[str] = set()
    for candidate in getattr(report, "candidates", []):
        urls = getattr(candidate, "source_urls", None) or []
        url = _normalize_url(urls[0]) if urls else ""
        if not url or url in seen:
            continue
        seen.add(url)
        host = _host(url)
        if not host or _is_non_product_host(host):
            continue
        score = _score_candidate(
            url,
            name_tokens=name_tokens,
            confidence=float(getattr(candidate, "confidence", 0.0) or 0.0),
        )
        if explicit_url:
            preferred = _normalize_url(explicit_url)
            if url == preferred:
                score += 6.0
            elif host == _host(preferred):
                score += 4.0
        scored.append(
            {
                "url": url,
                "host": host,
                "score": round(score, 3),
                "title": getattr(candidate, "title", "") or "",
            }
        )

    scored.sort(key=lambda item: (-float(item["score"]), str(item["url"])))
    alternatives = [dict(item) for item in scored]
    if explicit_url:
        # Подсказка оператора идентифицирует продукт: SERP-кандидаты — дополнение,
        # а не повод отказать/подменить стартовый URL.
        return _normalize_url(explicit_url), alternatives, [
            f"--url задан как подсказка идентификации; SERP-кандидатов: {len(alternatives)}"
        ]
    if not scored:
        raise AddProductError(
            "URL продукта не найден в SERP; укажите --url (URL не выдумывается)",
            candidates=alternatives,
        )

    best = scored[0]
    if float(best["score"]) < _MIN_URL_SCORE:
        raise AddProductError(
            "лучший SERP-кандидат слишком слаб (нет продуктового хоста/пути); "
            "укажите --url; кандидаты: "
            + ", ".join(str(item["url"]) for item in alternatives),
            candidates=alternatives,
        )
    if len(scored) > 1 and float(scored[0]["score"]) == float(scored[1]["score"]):
        raise AddProductError(
            "SERP-кандидаты неразличимы (равный скор); укажите --url; кандидаты: "
            + ", ".join(str(item["url"]) for item in alternatives),
            candidates=alternatives,
        )
    needs_human = [
        f"URL выбран из SERP ({len(alternatives)} кандидат(ов)); проверьте: {best['url']}"
    ]
    return str(best["url"]), alternatives, needs_human


# ---------------------------------------------------------------------------
# Сайты и ключи domain_profiles
# ---------------------------------------------------------------------------


def _site_urls(
    *,
    resolved_url: str,
    report: Any,
    serp_candidates: Sequence[Mapping[str, Any]],
) -> list[str]:
    """Собирает URL из URL-продукта, фетчей, сателлитов и SERP; фильтрует и дедуплицирует."""
    raw: list[str] = [resolved_url]
    for item in getattr(report, "fetched", []) or []:
        if isinstance(item, Mapping) and item.get("url"):
            raw.append(str(item["url"]))
    candidates_json = getattr(report, "candidates_json", {}) or {}
    raw.extend(str(url) for url in candidates_json.get("search_urls", []) or [])
    passport = getattr(report, "passport", None)
    if passport is not None:
        for satellite in getattr(passport, "satellites", []) or []:
            origin = getattr(satellite, "origin", None)
            origin_value = getattr(origin, "value", origin)
            if isinstance(origin_value, str) and ("." in origin_value or "://" in origin_value):
                raw.append(origin_value)
            evidence = getattr(satellite, "relationship_evidence", None)
            if evidence is not None and getattr(evidence, "url", None):
                raw.append(str(evidence.url))
    raw.extend(str(item.get("url")) for item in serp_candidates if item.get("url"))

    normalized: list[str] = []
    for item in raw:
        url = _normalize_url(item)
        if not url:
            continue
        host = _host(url)
        if not host or _is_non_product_host(host):
            continue
        if not url.lower().startswith(("http://", "https://")):
            continue
        normalized.append(url)
    normalized = dedupe(normalized)

    allowed = set(normalized) | {_normalize_url(resolved_url)}
    filtered = [url for url in normalized if url_allowed(url, allowed)]
    resolved = _normalize_url(resolved_url)
    if resolved and resolved not in filtered:
        filtered.insert(0, resolved)
    return dedupe(filtered)


def _path_key(host: str, path: str, *, name_tokens: Sequence[str], dedicated: bool) -> str | None:
    """Возвращает ``host/path`` для продукт-специфичного пути либо ``None`` (root)."""
    segments = [segment for segment in path.strip("/").split("/") if segment]
    if not segments:
        return None
    if dedicated:
        return None
    lowered = [segment.lower() for segment in segments]
    token_index = next(
        (
            index
            for index, segment in enumerate(lowered)
            if any(token in re.split(r"[^a-z0-9]+", segment) for token in name_tokens)
        ),
        None,
    )
    if token_index is not None:
        start = token_index - 1 if token_index > 0 and lowered[token_index - 1] in _SPECIFIC_PATH_SEGMENTS else token_index
        return f"{host}/{'/'.join(segments[start:token_index + 1])}"
    generic_index = next(
        (index for index, segment in enumerate(lowered) if segment in _SPECIFIC_PATH_SEGMENTS),
        None,
    )
    if generic_index is not None:
        return f"{host}/{'/'.join(segments[:generic_index + 1])}"
    return None


def _derive_sites(urls: Sequence[str], *, product_name: str) -> list[DiscoveredSite]:
    """Выводит ключи ``host``/``host/path`` и помечает корпоративные host-root ключи."""
    name_tokens = _name_tokens(product_name) or _name_tokens(sanitize_slug(product_name))
    by_host: dict[str, list[str]] = {}
    for url in urls:
        host = _host(url)
        if host:
            by_host.setdefault(host, []).append(url)

    sites: list[DiscoveredSite] = []
    for host in sorted(by_host):
        dedicated = any(token in _host_tokens(host) for token in name_tokens)
        root_urls: list[str] = []
        path_map: dict[str, list[str]] = {}
        for url in sorted(by_host[host]):
            path = urlsplit(url).path or "/"
            key_path = _path_key(host, path, name_tokens=name_tokens, dedicated=dedicated)
            if key_path is None:
                root_urls.append(url)
            else:
                path_map.setdefault(key_path, []).append(url)
        if root_urls or not path_map:
            sites.append(
                DiscoveredSite(
                    key=host,
                    urls=sorted(set(root_urls or by_host[host])),
                    reason=(
                        "host dedicated to product (normal crawl root)"
                        if dedicated
                        else "host root (corporate site; crawl roots are path keys)"
                    ),
                    # I17 trap — только корпоративный root; выделенный хост краулится
                    # как обычный root (иначе ftdata.ru получил бы max_depth=0).
                    is_root=not dedicated,
                )
            )
        for key in sorted(path_map):
            sites.append(
                DiscoveredSite(
                    key=key,
                    urls=sorted(set(path_map[key])),
                    reason=f"product-specific path '{key.split('/', 1)[1]}'",
                    is_root=False,
                )
            )
    return sites


# ---------------------------------------------------------------------------
# Sitemap-зонд (bounded, robots-honoured)
# ---------------------------------------------------------------------------


def _fetch_page(ports: ProbePorts, url: str) -> Any | None:
    """Фетчит URL обычным HTTP; ошибка/недоступность → ``None``."""
    try:
        return ports.http.fetch(url, FetchPlan(FetchMode.HTTP))
    except Exception:  # noqa: BLE001 — зонд бюджета не роняет план
        return None


def _robots_text(ports: ProbePorts, host: str) -> str | None:
    """Читает ``robots.txt`` хоста (без кэша); ошибка → ``None``."""
    page = _fetch_page(ports, f"https://{host}/robots.txt")
    if page is None:
        return None
    raw = getattr(page, "content", b"") or b""
    return raw.decode("utf-8", errors="replace") if raw else None


def _declared_sitemaps(robots_text: str | None) -> list[str]:
    """Извлекает ``Sitemap:`` из ``robots.txt`` (детерминированный порядок, дедуп)."""
    if not robots_text:
        return []
    found = re.findall(r"(?im)^\s*sitemap\s*:\s*(\S+)\s*$", robots_text)
    return dedupe([_normalize_url(item) for item in found if _normalize_url(item)])


def _in_scope(url: str, *, host: str, path_prefix: str | None) -> bool:
    """URL принадлежит хосту (и поддереву пути для path-ключа)."""
    if _host(url) != host:
        return False
    if path_prefix is None:
        return True
    path = urlsplit(url).path or "/"
    prefix = f"/{path_prefix.strip('/')}"
    return path == prefix or path.startswith(f"{prefix}/")


def _sitemap_budget(
    site: DiscoveredSite,
    *,
    ports: ProbePorts,
    max_fetches: int = SITEMAP_MAX_FETCHES,
) -> dict[str, Any]:
    """Ограниченный зонд sitemap для ключа: бюджет глубины/документов и его базис."""
    host = site.key.split("/", 1)[0]
    path_prefix = site.key.split("/", 1)[1] if "/" in site.key else None
    fetches = 0
    entries: list[str] = []
    sitemap_url: str | None = None

    def allowed(url: str) -> bool:
        if ports.robots is None:
            return True
        try:
            return bool(ports.robots.allowed(url))
        except Exception:  # noqa: BLE001 — сбой политики не роняет зонд
            return True

    robots_text = None
    if fetches < max_fetches:
        ref = f"https://{host}/robots.txt"
        if allowed(ref):
            fetches += 1
            robots_text = _robots_text(ports, host)

    def fetch_document(url: str) -> Any:
        nonlocal fetches
        if fetches >= max_fetches:
            raise SitemapParseError("sitemap: исчерпан бюджет фетчей зонда")
        fetches += 1
        page = _fetch_page(ports, url)
        if page is None:
            raise SitemapParseError(f"sitemap недоступен: {url}")
        raw = getattr(page, "content", b"") or b""
        return parse_sitemap(
            raw,
            content_type=getattr(page, "content_type", None),
            base_url=url,
        )

    for declared in _declared_sitemaps(robots_text):
        if fetches >= max_fetches or not allowed(declared):
            break
        try:
            document = fetch_document(declared)
            if document.route == ROUTE_INDEX:
                document_entries, _ = expand_sitemap(document, fetch_document)
            else:
                document_entries = list(document.entries)
        except (SitemapParseError, OSError, ValueError):
            continue
        entries.extend(entry.url for entry in document_entries if entry.url)
        if sitemap_url is None and document_entries:
            sitemap_url = declared

    in_scope = [
        url
        for url in dedupe(entries)
        if _in_scope(url, host=host, path_prefix=path_prefix)
    ]
    # ``found_url`` — объявленный sitemap, давший записи; ``profile_sitemap_url`` —
    # то, что попадёт в профиль (host-wide sitemap безопасен только для выделенного
    # хоста; для path-ключа host-wide sitemap даёт почти нулевой in-scope и лишний
    # failure-запись — не эмитим).
    found_url = sitemap_url
    has_sitemap_scope = found_url is not None and bool(in_scope)
    profile_sitemap_url: str | None = None
    if site.is_root:
        max_depth, max_documents = _ROOT_BUDGET
        basis = "host-root trap (I17): max_depth=0, max_documents=10"
    elif has_sitemap_scope:
        max_depth = _PATH_BUDGET_SITEMAP[0]
        max_documents = max(50, min(1000, 2 * len(in_scope)))
        if path_prefix is None:
            # Выделенный продуктовый хост: host-wide sitemap — безопасный источник seeds.
            profile_sitemap_url = found_url
            basis = (
                "dedicated host + sitemap in-scope seeds: max_depth=2, "
                "max_documents=2*count (clamped 50..1000)"
            )
        else:
            basis = (
                "path key + sitemap counts: max_depth=2, "
                "max_documents=2*count (clamped 50..1000); sitemap not emitted"
            )
    else:
        max_depth, max_documents = _PATH_BUDGET_NO_SITEMAP
        basis = (
            "no sitemap/in-scope URLs: max_depth=8, max_documents=200"
            if path_prefix is None
            else "path key without in-scope sitemap: max_depth=8, max_documents=200"
        )
    return {
        "sitemap_found": found_url is not None,
        "sitemap_url": profile_sitemap_url,
        "sitemap_source_url": found_url,
        "sitemap": found_url is not None,
        "in_scope_count": len(in_scope),
        "in_scope_urls": in_scope[:20],
        "fetches": fetches,
        "max_fetches": max_fetches,
        "max_depth": max_depth,
        "max_documents": max_documents,
        "basis": basis,
    }


# ---------------------------------------------------------------------------
# План
# ---------------------------------------------------------------------------


def _profile_for_verdict(
    key: str,
    verdict: Any,
    budget: Mapping[str, Any],
) -> dict[str, Any]:
    """Строит профиль по вердикту (неразрешённый — честный ``http/auto`` + warning)."""
    profile: dict[str, Any] = {"fetch": FetchMode.HTTP.value, "extract": ExtractionRoute.AUTO.value}
    if getattr(verdict, "resolved", False):
        fetch = getattr(verdict, "fetch", None)
        profile["fetch"] = (fetch or FetchMode.HTTP).value
        strategy = getattr(verdict, "strategy", None)
        if fetch is FetchMode.BROWSER and strategy is not None:
            profile["strategy"] = strategy.value
        if getattr(verdict, "extract", None) is ExtractionRoute.DOCUMENT:
            profile["extract"] = ExtractionRoute.DOCUMENT.value
        hint = getattr(verdict, "rate_limit_hint", None)
        if hint is not None:
            profile["rate_limit"] = {"min_s": hint.min_s, "max_s": hint.max_s}
    profile["max_depth"] = int(budget["max_depth"])
    profile["max_documents"] = int(budget["max_documents"])
    if budget.get("sitemap_url"):
        profile["sitemap"] = str(budget["sitemap_url"])
    return profile


def _scout_selection_reason(report: Any) -> str | None:
    """Обоснование выбора из разведки (роли/связи сателлитов); иначе ``None`` (A3)."""
    passport = getattr(report, "passport", None)
    if passport is None:
        return None
    roles: list[str] = []
    relations: list[str] = []
    for satellite in getattr(passport, "satellites", []) or []:
        component = getattr(satellite, "component", None)
        if component and component not in roles:
            roles.append(str(component))
        relationship = getattr(satellite, "relationship", None)
        value = getattr(relationship, "value", relationship)
        if value and value not in relations:
            relations.append(str(value))
    if not roles and not relations:
        return None
    details = []
    if roles:
        details.append("роли: " + ", ".join(roles[:5]))
    if relations:
        details.append("связь: " + ", ".join(sorted(relations)))
    return "аналог по разведке паспорта (" + "; ".join(details) + ")"


def plan_add_product(
    package_dir: str | Path,
    product_name: str,
    *,
    url: str | None = None,
    category: str | None = None,
    reason: str | None = None,
    lang: str = "ru",
    limit: int = 10,
    max_pages: int = 5,
    discovery_llm: Any,
    crawler: Any,
    extractor: Any,
    search: Any = None,
    probe_ports: ProbePorts,
    today: _dt.date | str | None = None,
    discover: Any = None,
    scout: Any = None,
    max_sitemap_fetches: int = SITEMAP_MAX_FETCHES,
) -> AddProductPlan:
    """Планирует добавление продукта (ничего не пишет).

    :raises AddProductError: неоднозначный/слабый URL, сбой разведки паспорта.
    """
    from core.pipeline.discovery import discover as _discover
    from core.pipeline.passport_scout import scout_passport

    discover_fn = discover or _discover
    scout_fn = scout or scout_passport
    snapshot = _coerce_date(today)
    pkg = Path(package_dir)
    name = str(product_name).strip()
    if not name:
        raise AddProductError("имя продукта не задано")

    warnings: list[str] = []
    needs_human: list[str] = []

    # 1. URL: подсказка ``--url`` либо разрешение из SERP. SERP выполняется **всегда**
    #    (когда доступен поиск): он расширяет набор сайтов, а не только выбирает стартовый.
    explicit_url = _normalize_url(url) if url else None
    if url and not explicit_url:
        raise AddProductError(f"--url не разбирается как URL: {url!r}")
    url_candidates: list[dict[str, Any]] = []
    if search is None:
        if explicit_url is None:
            raise AddProductError(
                "URL не задан и поиск недоступен; укажите --url (URL не выдумывается)"
            )
        resolved_url = explicit_url
        url_source = "explicit"
        warnings.append("SERP недоступен: поиск связанных сайтов не выполнялся")
    else:
        resolved_url, url_candidates, serp_notes = _resolve_url_from_serp(
            product_name=name,
            category=category,
            search=search,
            lang=lang,
            limit=limit,
            discover=discover_fn,
            explicit_url=explicit_url,
        )
        needs_human.extend(serp_notes)
        url_source = "explicit" if explicit_url else "serp"

    # 2. Разведка паспорта: только её выход — источник product info.
    report = scout_fn(
        product_name=name,
        url=resolved_url,
        crawler=crawler,
        extractor=extractor,
        discovery_llm=discovery_llm,
        search=search,
        max_pages=max_pages,
        today=snapshot,
    )
    if not getattr(report, "ok", False):
        errors = "; ".join(str(item) for item in getattr(report, "errors", []) or [])
        raise AddProductError(
            f"разведка паспорта не удалась: {errors or 'report.ok=False'}", code="error"
        )
    passport = report.passport
    product_info = {
        "canonical_name": passport.canonical_name,
        "vendor": passport.vendor,
        "doc_domains": list(passport.doc_domains),
        "satellites": [
            {
                "role": sat.role,
                "component": sat.component,
                "origin": getattr(sat.origin, "value", sat.origin),
                "relationship": getattr(sat.relationship, "value", sat.relationship),
            }
            for sat in passport.satellites
        ],
    }
    needs_human.extend(str(item) for item in getattr(report, "needs_human", []) or [])

    # 3. Сайты/ключи.
    urls = _site_urls(
        resolved_url=resolved_url,
        report=report,
        serp_candidates=url_candidates,
    )
    sites = _derive_sites(urls, product_name=name)
    if not any(_normalize_url(url) in site.urls for site in sites for url in [resolved_url]):
        sites.insert(
            0,
            DiscoveredSite(
                key=_host(resolved_url),
                urls=[resolved_url],
                reason="resolved URL (fallback)",
                is_root=True,
            ),
        )

    # 4. Зонд осей: переиспользуем crawl-probe драйвер (wait_for — только авторский).
    artifacts = load_artifacts(pkg)
    existing_profiles = artifacts.domain_profiles or {}
    keys = [site.key for site in sites]
    candidates: dict[str, tuple[ProbeSource, list[str]]] = {
        site.key: (ProbeSource.URLS, list(site.urls)) for site in sites
    }
    wait_for_by_key = {
        site.key: (existing_profiles[site.key].wait_for if site.key in existing_profiles else None)
        for site in sites
    }
    verdicts = run_probe(
        keys,
        candidates=candidates,
        ports=probe_ports,
        no_browser=probe_ports.browser is None,
        max_urls_per_key=1,
        wait_for_by_key=wait_for_by_key,
    )
    verdict_by_key = {verdict.key: verdict for verdict in verdicts}

    profiles: dict[str, dict[str, Any]] = {}
    budget_basis: dict[str, dict[str, Any]] = {}
    for site in sites:
        budget = _sitemap_budget(site, ports=probe_ports, max_fetches=max_sitemap_fetches)
        budget_basis[site.key] = budget
        verdict = verdict_by_key.get(site.key)
        profiles[site.key] = _profile_for_verdict(
            site.key,
            verdict,
            budget,
        )
        if verdict is None or not getattr(verdict, "resolved", False):
            reasons = ",".join(getattr(verdict, "reasons", ()) or ("no_verdict",))
            warnings.append(f"key unresolved: {site.key}: {reasons}")
        if getattr(verdict, "wait_for_recommended", False):
            needs_human.append(
                f"{site.key}: рекомендуется wait_for (не синтезирован; задайте вручную)"
            )

    # 5. Когорта.
    product_id = sanitize_slug(name) or "product"
    selection_reason = str(reason).strip() if reason else _scout_selection_reason(report)
    cohort_entry = {
        "product_id": product_id,
        "role_overlap": 0.5,
        "selection_reason": selection_reason,
        "anchor_relation": "analog",
    }

    # 6. Generator drift: предупреждаем, но не блокируем запись.
    if (pkg / "sources" / "build_package.py").is_file():
        warnings.append(
            'generator_drift: "domain_profiles.yaml генерируется; обновите '
            'sources/build_package.py, иначе tests/packages/test_build_package.py упадёт"'
        )

    return AddProductPlan(
        product_name=name,
        product_id=product_id,
        resolved_url=resolved_url,
        url_source=url_source,
        url_candidates=url_candidates,
        sites=sites,
        profiles=profiles,
        verdicts=list(verdicts),
        budget_basis=budget_basis,
        cohort_entry=cohort_entry,
        selection_reason=selection_reason,
        product_info=product_info,
        needs_human=dedupe(needs_human),
        warnings=dedupe(warnings),
        fetched=list(getattr(report, "fetched", []) or []),
        report=report,
        today=snapshot,
    )


def _coerce_date(value: _dt.date | str | None) -> _dt.date:
    """Нормализует дату снимка (``None`` → сегодня)."""
    if value is None:
        return _dt.date.today()
    if isinstance(value, _dt.date):
        return value
    return _dt.date.fromisoformat(str(value))


# ---------------------------------------------------------------------------
# Применение
# ---------------------------------------------------------------------------


def write_domain_profiles_with_new_keys(
    path: str | Path,
    verdicts: Sequence[Any],
    new_profiles: Mapping[str, Mapping[str, Any]],
    *,
    supported_suffixes: Iterable[str] | None = None,
) -> tuple[bool, list[str]]:
    """Дописывает новые ключи и сливает измеренные оси существующих.

    Существующие ключи правятся :func:`render_profiles_with_verdicts` (только
    ``fetch``/``strategy``/``extract``); новые — ``_dump_profile``-блоками в конец файла.
    Шапка/комментарии сохраняются. Возвращает ``(changed, new_keys)``.
    """
    target = Path(path)
    raw = target.read_text(encoding="utf-8") if target.is_file() else ""
    data = yaml.safe_load(raw) if raw.strip() else {}
    existing_keys = set(data) if isinstance(data, dict) else set()

    existing_verdicts = [verdict for verdict in verdicts if getattr(verdict, "key", None) in existing_keys]
    kwargs = {"supported_suffixes": supported_suffixes} if supported_suffixes is not None else {}
    updated = render_profiles_with_verdicts(raw, existing_verdicts, **kwargs)

    new_keys = [key for key in new_profiles if key not in existing_keys]
    if new_keys:
        if updated and not updated.endswith("\n"):
            updated += "\n"
        if updated and not updated.endswith("\n\n"):
            updated += "\n"
        for key in new_keys:
            updated += _dump_profile(key, new_profiles[key])

    if updated == raw:
        return False, new_keys
    atomic_write_text(target, updated)
    return True, new_keys


def apply_add_product(
    package_dir: str | Path,
    plan: AddProductPlan,
    *,
    actor: str,
    write_passport: bool = False,
    passports_dir: str | Path | None = None,
    today: _dt.date | str | None = None,
) -> dict[str, Any]:
    """Применяет план: когорта → ``domain_profiles.yaml`` → паспорт → review-артефакт.

    :raises AddProductError: нет ``actor``/``selection_reason`` или invalid профили.
    """
    if not actor or not str(actor).strip():
        raise AddProductError("--apply требует --actor (человек, изменяющий пакет)")
    if not plan.selection_reason:
        raise AddProductError(
            "укажите --reason: selection_reason не выдумывается (A3)"
        )
    actor = str(actor).strip()
    pkg = Path(package_dir)
    snapshot = _coerce_date(today if today is not None else plan.today)
    written: list[str] = []
    cohort_result: dict[str, Any] = {"changed": False, "noop": False}

    # 1. Когорта (update_cohort также синхронизирует run.yaml products/anchor).
    artifacts = load_artifacts(pkg)
    existing = (
        {entry.product_id for entry in artifacts.cohort.entries}
        if artifacts.cohort is not None
        else set()
    )
    if plan.product_id in existing:
        cohort_result["noop"] = True
    else:
        result = apply_candidate(
            pkg,
            Candidate(
                kind="cohort_member",
                payload=dict(plan.cohort_entry),
                rationale=plan.selection_reason,
                source="add_product",
            ),
            actor=actor,
            dry_run=False,
        )
        cohort_result = {
            "changed": True,
            "noop": False,
            "changed_artifacts": list(result.changed),
            "written": [str(path) for path in result.written],
            "warnings": list(result.warnings),
        }
        written.extend(str(path) for path in result.written)

    # 2. domain_profiles.yaml: существующие — измеренные оси, новые — полные блоки.
    profiles_path = pkg / "domain_profiles.yaml"
    profiles_changed, new_keys = write_domain_profiles_with_new_keys(
        profiles_path, plan.verdicts, plan.profiles
    )
    if profiles_changed:
        written.append(str(profiles_path))
    try:
        load_domain_profiles(profiles_path)
    except ValueError as exc:
        raise AddProductError(
            f"domain_profiles.yaml невалиден после записи: {exc}", code="error"
        ) from exc

    # 3. Черновик паспорта (опционально).
    if write_passport:
        if passports_dir is None:
            raise AddProductError("--passport требует каталог паспортов")
        from core.pipeline.passport_scout import write_passport_draft

        yaml_path, sidecar_path = write_passport_draft(
            plan.report, passports_dir, snapshot
        )
        written.extend([str(yaml_path), str(sidecar_path)])

    # 4. Review-артефакт (всегда при apply).
    artifact_path = pkg / "sources" / "add_product" / f"{snapshot.isoformat()}.json"
    artifact = {
        **plan.as_dict(),
        "apply": {
            "actor": actor,
            "cohort": cohort_result,
            "profiles_changed": profiles_changed,
            "new_profile_keys": new_keys,
            "passport_written": bool(write_passport),
            "written_files": list(written),
        },
    }
    atomic_write_text(
        artifact_path,
        json.dumps(artifact, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
    )
    written.append(str(artifact_path))

    return {
        "ok": True,
        "actor": actor,
        "product_id": plan.product_id,
        "cohort": cohort_result,
        "profiles_changed": profiles_changed,
        "new_profile_keys": new_keys,
        "written_files": written,
        "artifact_path": str(artifact_path),
    }
