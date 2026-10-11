"""coHound — резолвер плана получения страницы (D2, Ш5).

Чистая функция отображения URL → :class:`~core.ports.types.FetchPlan`. Разрешение плана
выполняется **в pipeline** (а не в адаптерах), чтобы краулер и экстрактор получали уже
готовый value-объект и не знали о профилях доменов.

Приоритет (сверху вниз):

1. жёсткий дефолт :class:`FetchPlan` (HTTP + авто-извлечение);
2. лучший профиль домена из ``domain_profiles.yaml`` (D.2);
3. явное переопределение ``fetch`` по точному URL (``--render-modes``) — побеждает всегда,
   но **только** ось ``fetch``: ``extract``/``wait_for`` остаются из профиля.

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
from core.ports.types import FetchMode, FetchPlan

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
) -> FetchPlan:
    """Резолвит :class:`FetchPlan` для ``url``: дефолт → профиль → явное переопределение.

    :param url: целевой URL (scheme + host обязательны для доменного матчинга).
    :param profiles: профили доменов из ``domain_profiles.yaml`` (D.2).
    :param explicit: явные переопределения ``{url: FetchMode}`` (``--render-modes``);
        переопределяют **только** ``fetch``, ``extract``/``wait_for`` берутся из профиля.
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

    plan = (
        FetchPlan(best.fetch, extract=best.extract, wait_for=best.wait_for)
        if best is not None
        else FetchPlan()
    )

    if explicit:
        override = explicit.get(url)
        if override is not None:
            plan = replace(plan, fetch=override)
    return plan
