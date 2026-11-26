"""coHound — вывод корней краула сателлита из наблюдений (чистое ядро, без I/O).

Задача. Сателлит продукта (консоль управления, средство резервного копирования, загрузчик
данных) объявляется паспортом, но его **материал** лежит в собственной области: другое
дерево портала, другой хост, свой префикс пути. Объявить корень «на глаз» нельзя — это
выбор границы корпуса, то есть условие измерения (A3). Здесь из наблюдённых фактов
(финальный URL после редиректов, ссылки с landing-страницы, `<loc>` карты сайта, решение
robots) выводится **набор** кандидатов с числами, а не один «угаданный» корень.

Почему набор, а не решение. Две ветки дерева одинаковой глубины с равным покрытием — это
**неоднозначность**, и выбор между ними — решение человека (`--apply --actor`), а не
эвристика прибора. Прибор обязан назвать неоднозначность флагом, а не разрешить её
молча: молчаливый выбор «первого по алфавиту» спрятал бы вторую половину корпуса.

Границы: модуль не ходит в сеть (наблюдения приносит
:mod:`core.pipeline.satellite_probe`) и не пишет паспорт (правку делает
:mod:`core.pipeline.passport_patch` под явную команду оператора).

Нейтрален к предметной категории (§1.3): роли, хосты и префиксы — данные.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

import yaml

__all__ = [
    "MIN_ROOT_LINKS",
    "ROOT_EVIDENCE_LINKS",
    "ROOT_EVIDENCE_SITEMAP",
    "RootCandidate",
    "SatelliteRootPlan",
    "derive_roots",
    "render_plan_yaml",
]

#: Сколько ссылок под префиксом нужно, чтобы считать его деревом, а не навигацией страницы.
#: Это **объявленный порог классификации**, а не потолок корпуса: он отвечает на вопрос
#: «это раздел или пункт меню», а не «сколько документов скачивать». Меньше трёх ссылок —
#: типичная горизонтальная навигация (главная/о продукте/контакты), и корнем она не бывает.
MIN_ROOT_LINKS = 3

ROOT_EVIDENCE_LINKS = "links"
ROOT_EVIDENCE_SITEMAP = "sitemap"


def _host(url: str) -> str:
    return (urlsplit(url).hostname or "").lower()


def _path_segments(url: str) -> list[str]:
    return [segment for segment in urlsplit(url).path.split("/") if segment]


def _prefix_key(host: str, segments: Sequence[str]) -> str:
    return "/".join([host, *segments]) if segments else host


@dataclass(frozen=True)
class RootCandidate:
    """Кандидат-корень: ключ ``host[/path]``, его документы и чем он подтверждён."""

    key: str
    documents: int
    depth: int
    urls: tuple[str, ...]
    evidence: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "documents": self.documents,
            "depth": self.depth,
            "urls": list(self.urls),
            "evidence": list(self.evidence),
        }


@dataclass
class SatelliteRootPlan:
    """Результат разведки корней одного сателлита (наблюдение, не объявление)."""

    product_id: str
    role: str
    component: str | None
    seed_url: str
    final_url: str | None = None
    candidates: list[RootCandidate] = field(default_factory=list)
    #: Кандидаты, отвергнутые прибором, с причиной (``below_min_links``, ``external_host``).
    rejected: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    #: Причина, по которой рекомендации нет **при наличии кандидатов** (``site_root_seed``).
    #: Неоднозначность и вложенность вычисляются из кандидатов, а эта — из адреса
    #: наблюдения, поэтому хранится явно: иначе пришлось бы угадывать по замечаниям.
    blocked: str | None = None

    @property
    def ambiguous(self) -> bool:
        """Неоднозначен ли вывод: два разных корня на минимальной глубине с равным покрытием.

        Именно этот случай нельзя разрешать автоматически: выбор одного из двух спрятал бы
        вторую половину корпуса, и «корень объявлен» было бы правдой о приборе, а не о сайте.
        """
        if not self.candidates:
            return False
        best = min(self.candidates, key=lambda item: (item.depth, -item.documents))
        rivals = [
            item
            for item in self.candidates
            if item.depth == best.depth and item.documents == best.documents
        ]
        return len(rivals) > 1

    @property
    def nested(self) -> bool:
        """Кандидаты вложены друг в друга — масштаб корня наблюдением не определяется.

        Лестница ``/center`` → ``/center/docs`` → ``/center/docs/7`` покрывает всё меньше
        ссылок на каждом шаге, и **все** шаги — законные корни: какой из них объявить,
        решает граница корпуса, а не покрытие ссылок. Прибор, выбравший самый короткий,
        молча вернул бы в корпус весь портал; выбравший самый длинный — потерял бы
        соседние разделы. Поэтому вложенный набор не рекомендуется вовсе.
        """
        keys = [candidate.key for candidate in self.candidates]
        return any(
            other.startswith(f"{key}/") for key in keys for other in keys if other != key
        )

    def recommended(self) -> RootCandidate | None:
        """Первый кандидат по (глубина ↑, документы ↓) — рекомендация, не решение.

        ``None`` при неоднозначности покрытия, при вложенном наборе (:attr:`nested`) и
        при адресе наблюдения без пути (:attr:`blocked`): во всех случаях выбор корня —
        решение человека о границе корпуса.
        """
        if not self.candidates or self.ambiguous or self.nested or self.blocked:
            return None
        return min(self.candidates, key=lambda item: (item.depth, -item.documents, item.key))

    def as_dict(self) -> dict[str, Any]:
        recommended = self.recommended()
        return {
            "product_id": self.product_id,
            "role": self.role,
            "component": self.component,
            "seed_url": self.seed_url,
            "final_url": self.final_url,
            "ambiguous": self.ambiguous,
            "nested": self.nested,
            "blocked": self.blocked,
            "recommended": recommended.key if recommended is not None else None,
            "candidates": [item.as_dict() for item in self.candidates],
            "rejected": list(self.rejected),
            "notes": list(self.notes),
        }


def _count_prefixes(
    urls: Iterable[str], host: str
) -> dict[tuple[str, ...], list[str]]:
    """Группирует URL по всем префиксам пути внутри ``host`` (глубина 1…N)."""
    groups: dict[tuple[str, ...], list[str]] = {}
    for url in urls:
        if _host(url) != host:
            continue
        segments = _path_segments(url)
        for depth in range(1, len(segments) + 1):
            groups.setdefault(tuple(segments[:depth]), []).append(url)
    return groups


def derive_roots(
    *,
    product_id: str,
    role: str,
    component: str | None,
    seed_url: str,
    final_url: str | None,
    links: Sequence[str] = (),
    sitemap_urls: Sequence[str] = (),
    robots_allows: bool | None = None,
    seed_declared: bool = False,
    min_links: int = MIN_ROOT_LINKS,
) -> SatelliteRootPlan:
    """Выводит кандидатов-корней сателлита из наблюдённых фактов.

    Шаги, каждый — наблюдение, а не догадка:

    1. **Хост.** Корень живёт на хосте **финального** URL (после редиректов): сателлит,
       переехавший на другой хост, объявлял бы корень, которого больше нет.
    2. **Ссылки.** Все префиксы путей ссылок landing-страницы группируются по покрытию;
       кандидатом становится префикс, покрывающий ≥ ``min_links`` ссылок. Это отделяет
       раздел (дерево) от пункта меню.
    3. **Карта сайта.** ``<loc>`` под префиксом усиливает кандидата (``sitemap``) и
       добавляет префиксы, которых в ссылках не было: карта сайта — объявление сайта о
       своём составе, и она честнее одной страницы.
    4. **robots.** Отказ robots на seed — замечание в плане, а не причина отбросить корень:
       граница сайта не делает сателлит несуществующим (0.6.0).

    Ссылки вне хоста финального URL попадают в ``rejected`` с причиной ``external_host``:
    это не отказ, а видимый факт — прибор их не считал.

    :param seed_declared: адрес наблюдения объявлен оператором (``--seed``), а не взят из
        свидетельства связи. Различие решает: свидетельство связи обычно указывает на
        **корень сайта** (маркетинговую главную), и его дерево ссылок — это навигация
        сайта, а не документация компонента. Такой набор кандидатов наблюдением получен,
        но рекомендацией не становится (:attr:`SatelliteRootPlan.blocked`).

    :returns: :class:`SatelliteRootPlan` с числами; при пустом наборе ссылок —
        ``notes=["no_links"]`` и пустой список кандидатов (корень не выдумывается).
    """
    host = _host(final_url or seed_url)
    plan = SatelliteRootPlan(
        product_id=product_id,
        role=role,
        component=component,
        seed_url=seed_url,
        final_url=final_url,
    )
    if final_url and _host(final_url) and _host(final_url) != _host(seed_url):
        plan.notes.append(f"redirect_host: {_host(seed_url)} → {_host(final_url)}")
    if robots_allows is False:
        plan.notes.append("robots_disallow_seed")
    if not seed_declared:
        # Дерево ссылок прибор видит всегда, а вот **документация** ли это — объявленный
        # выбор, а не вывод из структуры (A3). Измерено 27.11.2026 на `tantorlabs.ru`:
        # свидетельство связи вело на главную, и самый покрытый префикс был `/products`
        # (11 ссылок из 88) — то есть объявление корнем сателлита маркетингового раздела
        # вернуло бы в корпус ровно тот материал, который сужение границ вывело. То же на
        # странице продукта (`/products/dbms` → `/products`): её ссылки — навигация сайта.
        # Поэтому рекомендация требует **объявленного** адреса наблюдения (`--seed`);
        # без него набор кандидатов остаётся наблюдением с названной причиной.
        if not _path_segments(seed_url):
            plan.blocked = "site_root_seed"
            plan.notes.append(
                "site_root_seed: адрес наблюдения — корень сайта, его дерево ссылок это "
                "навигация, а не документация компонента; задайте --seed ROLE=URL"
            )
        else:
            plan.blocked = "evidence_seed"
            plan.notes.append(
                "evidence_seed: адрес взят из свидетельства связи, а не объявлен "
                "оператором; страница продукта даёт навигацию сайта, а не дерево "
                "документации — задайте --seed ROLE=URL"
            )

    link_groups = _count_prefixes(links, host)
    sitemap_groups = _count_prefixes(sitemap_urls, host)

    external = sorted(
        {url for url in links if _host(url) and _host(url) != host}
    )
    if external:
        plan.rejected.append(
            {
                "reason": "external_host",
                "count": len(external),
                "examples": external[:5],
            }
        )

    keys: dict[str, RootCandidate] = {}
    for prefix, urls in link_groups.items():
        if len(urls) < min_links:
            plan.rejected.append(
                {
                    "reason": "below_min_links",
                    "key": _prefix_key(host, prefix),
                    "documents": len(urls),
                    "min_links": min_links,
                }
            )
            continue
        key = _prefix_key(host, prefix)
        keys[key] = RootCandidate(
            key=key,
            documents=len(urls),
            depth=len(prefix),
            urls=tuple(sorted(set(urls))),
            evidence=(ROOT_EVIDENCE_LINKS,),
        )
    for prefix, urls in sitemap_groups.items():
        if len(urls) < min_links:
            continue
        key = _prefix_key(host, prefix)
        existing = keys.get(key)
        if existing is None:
            keys[key] = RootCandidate(
                key=key,
                documents=len(urls),
                depth=len(prefix),
                urls=tuple(sorted(set(urls))),
                evidence=(ROOT_EVIDENCE_SITEMAP,),
            )
        else:
            merged = sorted(set(existing.urls) | set(urls))
            keys[key] = RootCandidate(
                key=key,
                documents=len(merged),
                depth=existing.depth,
                urls=tuple(merged),
                evidence=tuple(sorted({*existing.evidence, ROOT_EVIDENCE_SITEMAP})),
            )

    # Победители по покрытию: родитель избыточен, если у него есть потомок с **тем же**
    # покрытием — тот же материал под более длинным (и потому более точным) адресом.
    # Обратное правило («оставить самого короткого родителя») расширило бы корень за
    # пределы наблюдаемого дерева: `/center` и `/center/docs/backup` покрывают одно и то
    # же число ссылок, но первый захватил бы соседние разделы, которых в наблюдении нет.
    ordered = sorted(keys.values(), key=lambda item: (item.depth, -item.documents, item.key))
    kept: list[RootCandidate] = []
    for candidate in ordered:
        if any(
            child.key.startswith(f"{candidate.key}/")
            and child.documents == candidate.documents
            for child in ordered
        ):
            continue
        kept.append(candidate)
    plan.candidates = kept

    if not links and not sitemap_urls:
        plan.notes.append("no_links")
    if plan.ambiguous:
        plan.notes.append("ambiguous_roots: выбор корня — решение человека")
    if plan.nested:
        plan.notes.append(
            "nested_roots: кандидаты вложены друг в друга — масштаб корня (глубина) "
            "выбирается человеком; самый короткий вернул бы в корпус весь раздел"
        )
    return plan


def render_plan_yaml(plans: Sequence[SatelliteRootPlan]) -> str:
    """Детерминированный YAML плана разведки (артефакт; в паспорт не пишется).

    Артефакт нужен именно потому, что разведка — наблюдение: он фиксирует, **что** прибор
    увидел в этот день (числа кандидатов, отвергнутые с причиной, замечания), и решение
    человека потом опирается на числа, а не на пересказ вывода.
    """
    return yaml.safe_dump(
        [plan.as_dict() for plan in plans],
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
    )


def roots_by_role(plans: Sequence[SatelliteRootPlan]) -> Mapping[str, Sequence[str]]:
    """Готовит ``{role: [key, …]}`` для :func:`core.pipeline.passport_patch.patch_satellite_roots`.

    Берётся **только** однозначный вывод: при ``ambiguous`` роль пропускается — иначе
    автоматическое применение выбрало бы за человека. Роль без кандидатов тоже
    пропускается (объявлять нечего).
    """
    updates: dict[str, list[str]] = {}
    for plan in plans:
        candidate = plan.recommended()
        if candidate is None:
            continue
        updates[plan.role] = [candidate.key]
    return updates
