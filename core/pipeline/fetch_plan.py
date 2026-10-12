"""coHound — резолвер плана получения страницы (D2, Ш5).

Чистая функция отображения URL → :class:`~core.ports.types.FetchPlan`. Разрешение плана
выполняется **в pipeline** (а не в адаптерах), чтобы краулер и экстрактор получали уже
готовый value-объект и не знали о профилях доменов.

Приоритет (сверху вниз):

1. дефолт прогона (``default_strategy``/``default_rate_limit`` из run.yaml ``crawl``);
2. лучший профиль домена из ``domain_profiles.yaml`` (D.2): его ``strategy``/``rate_limit``
   (если заданы) переопределяют дефолт прогона;
3. явное переопределение ``fetch`` по точному URL (``--render-modes``) — побеждает всегда,
   но **только** ось ``fetch``: ``extract``/``wait_for``/``strategy``/``rate_limit`` остаются
   из профиля (или дефолта).

Инвариант осей: ``fetch`` = «нужен ли браузер вообще», ``strategy`` = «**как** он
используется **при browser-capable адаптере**». Итоговый план нормализуется: если
``fetch == HTTP``, то ``strategy`` приводится к ``AUTO`` (стратегия браузера бессмысленна).
Иначе — ``strategy != AUTO`` ⇒ ``fetch == BROWSER``. ``wait_for`` не нормализуется.

Ключ профиля — ``host`` с опциональным ``/path`` (напр. ``example.org/documents``). Профиль
совпадает, если host URL равен ``host_key`` либо оканчивается на ``"." + host_key`` **и**
(путь не задан **или** ``path_key`` матчит путь URL по границе сегмента: ``url_path`` равен
``path_key`` либо начинается с ``path_key + "/"``). Так ``/documents`` матчит ``/documents`` и
``/documents/x``, но **не** ``/documents-archive``. Побеждает самый длинный ``path_key``, затем
самый длинный ``host_key``, затем лексикографически меньший ключ.

Нейтрален к предметной категории (§1.3): домены — данные.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from urllib.parse import urlsplit

from core.config.domain_profiles import DomainProfile
from core.ports.types import CrawlStrategy, FetchMode, FetchPlan, RateLimitSpec

__all__ = ["resolve_fetch_plan"]


def _split_key(raw_key: str) -> tuple[str, str]:
    """Разбивает ключ профиля на ``(host, path)``; host — в нижнем регистре.

    ``"example.org/documents"`` → ``("example.org", "/documents")``; ``"example.com"`` →
    ``("example.com", "")``. Путь нормализуется к виду с ведущим ``/``.
    """
    text = str(raw_key).strip()
    host, sep, path = text.partition("/")
    host = host.strip().lower()
    path_key = f"/{path.strip('/')}" if sep and path.strip("/") else ""
    return host, path_key


def _host_matches(url_host: str, host_key: str) -> bool:
    """URL-хост равен ключу или является его поддоменом (``a.example.com`` → ``example.com``)."""
    return url_host == host_key or url_host.endswith("." + host_key)


def _profile_rank(host_key: str, path_key: str) -> tuple[int, int]:
    """Ранг профиля: сначала длина пути, затем длина host (больше — приоритетнее)."""
    return (len(path_key), len(host_key))


def resolve_fetch_plan(
    url: str,
    profiles: Mapping[str, DomainProfile],
    explicit: Mapping[str, FetchMode] | None = None,
    *,
    default_strategy: CrawlStrategy = CrawlStrategy.AUTO,
    default_rate_limit: RateLimitSpec | None = None,
) -> FetchPlan:
    """Резолвит :class:`FetchPlan` для ``url``: дефолт → профиль → явное переопределение.

    Итоговый план нормализуется по инварианту осей: при ``fetch=HTTP`` ``strategy``
    приводится к ``AUTO`` (таким образом ``strategy != AUTO`` ⇒ ``fetch == BROWSER``).
    Нормализация выполняется **после** явного переопределения ``--render-modes``.

    :param url: целевой URL (scheme + host обязательны для доменного матчинга).
    :param profiles: профили доменов из ``domain_profiles.yaml`` (D.2).
    :param explicit: явные переопределения ``{url: FetchMode}`` (``--render-modes``);
        переопределяют **только** ``fetch``, ``extract``/``wait_for``/``strategy``/
        ``rate_limit`` берутся из профиля (либо из дефолтов прогона).
    :param default_strategy: дефолт оси стратегии (run.yaml ``crawl.strategy``); профиль,
        задавший ``strategy``, его переопределяет. Действует при ``fetch == BROWSER`` (при HTTP нормализуется в ``AUTO``).
    :param default_rate_limit: дефолт диапазона задержек (run.yaml ``crawl.rate_limit``);
        профиль, задавший ``rate_limit``, его переопределяет.
    """
    parsed = urlsplit(url)
    url_host = (parsed.hostname or "").strip().lower()
    url_path = parsed.path or "/"

    best: DomainProfile | None = None
    best_rank: tuple[int, int] | None = None
    best_key: str | None = None

    if url_host:
        for raw_key, profile in profiles.items():
            host_key, path_key = _split_key(raw_key)
            if not host_key or not _host_matches(url_host, host_key):
                continue
            if path_key and not (
                url_path == path_key or url_path.startswith(path_key + "/")
            ):
                continue
            rank = _profile_rank(host_key, path_key)
            key = str(raw_key)
            tie = best_rank is not None and rank == best_rank and (
                best_key is not None and key < best_key
            )
            if best is None or rank > best_rank or tie:  # type: ignore[operator]
                best, best_rank, best_key = profile, rank, key

    if best is not None:
        plan = FetchPlan(
            best.fetch,
            extract=best.extract,
            wait_for=best.wait_for,
            strategy=best.strategy if best.strategy is not None else default_strategy,
            rate_limit=best.rate_limit if best.rate_limit is not None else default_rate_limit,
        )
    else:
        plan = FetchPlan(strategy=default_strategy, rate_limit=default_rate_limit)

    if explicit:
        override = explicit.get(url)
        if override is not None:
            plan = replace(plan, fetch=override)

    # Нормализация осей (G1): ``strategy != AUTO`` ⇒ ``fetch == BROWSER``. При HTTP-фетче
    # стратегия браузера бессмысленна — приводим к ``AUTO``. ``wait_for`` не трогаем.
    if plan.fetch is FetchMode.HTTP:
        plan = replace(plan, strategy=CrawlStrategy.AUTO)

    return plan
