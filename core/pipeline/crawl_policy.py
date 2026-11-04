"""coHound — разрешение политики краула по областям (план 19 §4–§6).

Слой P1: политика **разрешается**, но не исполняется. Здесь нет сетевого кода и нет
чтения БД: входы (дефолты пакета, действующие записи ``state.db``, объявленные корни и
наблюдения) приходят аргументами. Поэтому ``plan`` и ``run`` видят одну и ту же
эффективную поверхность решений, а тест на «due» не зависит от системных часов.

Приоритет: ``path`` > ``root`` > ``host`` > ``product``; внутри вида — самый длинный
совпавший литеральный префикс пути. При равной специфичности запись оператора
(``state.db``) переопределяет дефолт пакета. Равная специфичность с разными режимами —
:class:`CrawlPolicyConflict` (CLI переводит в ``EXIT_USAGE``): тай-брейк по
``created_at``/``policy_id`` запрещён — он сделал бы резолв функцией часов и порядка
вставки.

``scheduled`` — это «due», а не демон: режим разрешается в ``refresh``/``reuse``
сравнением ``now - observed_at`` с ``interval_hours``. ``observed_at`` — последнее
наблюдение корня из ``url_state``; отсутствие наблюдения читается как первичный
дискавери (``refresh``).

Несовпавшая ни с одним объявленным корнем строка видима
(``crawl_policy_unmatched``), но в дайджест идентичности не входит. Нейтрален (§1.3).
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from core.config.crawl_policy import CrawlPolicyConfig, parse_site_key
from core.domain.enums import CrawlPolicyMode, CrawlPolicyScopeKind
from core.domain.models import SeedsConfig
from core.pipeline.digests import crawl_policy_digest
from core.ports.state import CrawlPolicyRow
from core.util.rate_limit import normalize_host

__all__ = [
    "CrawlPolicyConflict",
    "CrawlPolicyResolution",
    "DeclaredRoot",
    "PolicyDecision",
    "PolicyRecord",
    "declared_roots",
    "normalize_policy_host",
    "normalize_policy_path",
    "observations_by_root",
    "resolve_crawl_policy",
]

#: Метка дефолта пакета: запись применяется к любому продукту.
WILDCARD_PRODUCT = "*"

#: Источники записи: дефолт пакета и решение оператора (``state.db``).
SOURCE_PACKAGE = "package"
SOURCE_OPERATOR = "operator"

_KIND_RANK = {
    CrawlPolicyScopeKind.PATH: 3,
    CrawlPolicyScopeKind.ROOT: 2,
    CrawlPolicyScopeKind.HOST: 1,
    CrawlPolicyScopeKind.PRODUCT: 0,
}


class CrawlPolicyConflict(ValueError):
    """Равная специфичность записей политики с разными режимами (план 19 §4).

    ``code`` — машинный токен причины; CLI печатает его как код ошибки
    (``crawl_policy_conflict``), чтобы оператор видел класс отказа, а не «какой-то
    ValueError». Ни одна из записей не выбирается: тай-брейк запрещён.
    """

    code = "crawl_policy_conflict"


@dataclass(frozen=True)
class DeclaredRoot:
    """Объявленный корень продукта: ``host`` + префикс ``path`` (пусто — весь host)."""

    product_id: str
    host: str
    path: str = ""


@dataclass(frozen=True)
class PolicyRecord:
    """Запись политики до разрешения (пакет или оператор) с провенансом."""

    product_id: str
    scope_kind: CrawlPolicyScopeKind
    mode: CrawlPolicyMode
    source: str
    host: str | None = None
    path: str | None = None
    interval_hours: int | None = None
    note: str | None = None
    actor: str | None = None
    created_at: str | None = None
    policy_id: int | None = None


@dataclass(frozen=True)
class PolicyDecision:
    """Эффективное решение по одному объявленному корню."""

    product_id: str
    scope_kind: CrawlPolicyScopeKind
    mode: CrawlPolicyMode
    decision: str
    source: str
    host: str | None = None
    path: str | None = None
    interval_hours: int | None = None
    note: str | None = None
    actor: str | None = None
    created_at: str | None = None
    policy_id: int | None = None

    def as_dict(self) -> dict[str, Any]:
        """JSON-представление решения (для ``plan``/``crawl policy list``)."""
        return {
            "product_id": self.product_id,
            "scope_kind": self.scope_kind.value,
            "host": self.host,
            "path": self.path,
            "mode": self.mode.value,
            "decision": self.decision,
            "source": self.source,
            "interval_hours": self.interval_hours,
            "note": self.note,
            "actor": self.actor,
            "created_at": self.created_at,
            "policy_id": self.policy_id,
        }


@dataclass(frozen=True)
class CrawlPolicyResolution:
    """Результат разрешения: решения, несовпавшие строки и отпечаток поверхности."""

    decisions: tuple[PolicyDecision, ...]
    unmatched: tuple[PolicyRecord, ...]
    digest: str

    def as_dict(self) -> dict[str, Any]:
        """JSON-представление (дайджест + решения + несовпавшие записи)."""
        return {
            "digest": self.digest,
            "decisions": [item.as_dict() for item in self.decisions],
            "unmatched": [
                {
                    "product_id": item.product_id,
                    "scope_kind": item.scope_kind.value,
                    "host": item.host,
                    "path": item.path,
                    "mode": item.mode.value,
                    "interval_hours": item.interval_hours,
                    "source": item.source,
                    "policy_id": item.policy_id,
                }
                for item in self.unmatched
            ],
        }


def normalize_policy_host(host: str) -> str:
    """Ключ host'а политики: как у лимитера, плюс ``www.`` эквивалентен bare-host.

    ``www`` снимается с обеих сторон (запись и корень): иначе ``www.example.com`` в
    политике не совпал бы с корнем ``example.com``, и запрет сети молча не действовал бы.
    """
    value = normalize_host(host)
    return value[4:] if value.startswith("www.") else value


def normalize_policy_path(path: str | None) -> str:
    """Префикс пути к виду ``""`` или ``/a/b`` (без хвостового слэша)."""
    raw = (path or "").strip()
    if not raw or raw == "/":
        return ""
    return "/" + raw.strip("/")


def _path_covers(prefix: str, path: str) -> bool:
    """``prefix`` покрывает ``path`` по границе сегмента (``/a`` не покрывает ``/ab``)."""
    if not prefix:
        return True
    return path == prefix or path.startswith(prefix + "/")


def _split_url(url: str) -> tuple[str, str]:
    """URL → ``(host, path)`` в нормализованном виде; неразбираемый — ``("", "")``."""
    parsed = urlsplit(str(url))
    host = normalize_policy_host(parsed.hostname or "")
    return host, normalize_policy_path(parsed.path)


def declared_roots(seeds: SeedsConfig) -> list[DeclaredRoot]:
    """Объявленные корни из эффективного seed-набора (все продукты, только активные).

    Дубликаты схлопываются: один корень — одно решение в поверхности. Порядок —
    ``(product_id, host, path)``, чтобы резолв не зависел от порядка файла.
    """
    roots: set[DeclaredRoot] = set()
    for product_id, product in seeds.products.items():
        for seed in product.enabled_for():
            host, path = _split_url(seed.normalized_url)
            if host:
                roots.add(DeclaredRoot(product_id=str(product_id), host=host, path=path))
    return sorted(roots, key=lambda root: (root.product_id, root.host, root.path))


def package_records(config: CrawlPolicyConfig) -> list[PolicyRecord]:
    """Записи слоёв пакета: ``defaults`` → ``products`` → ``sites``.

    ``defaults`` — product-wildcard (``*``) того же вида ``product``; точная запись
    продукта побеждает wildcard по специфичности. Ключ сайта без пути даёт ``host``,
    с путём — ``root``; явный ``scope_kind`` (``root``/``path``/``host``) переопределяет
    вывод из формы ключа.
    """
    records: list[PolicyRecord] = []
    if config.defaults is not None:
        records.append(
            PolicyRecord(
                product_id=WILDCARD_PRODUCT,
                scope_kind=CrawlPolicyScopeKind.PRODUCT,
                mode=config.defaults.mode,
                interval_hours=config.defaults.interval_hours,
                note=config.defaults.note,
                source=SOURCE_PACKAGE,
            )
        )
    for product_id, entry in sorted(config.products.items()):
        records.append(
            PolicyRecord(
                product_id=str(product_id),
                scope_kind=CrawlPolicyScopeKind.PRODUCT,
                mode=entry.mode,
                interval_hours=entry.interval_hours,
                note=entry.note,
                source=SOURCE_PACKAGE,
            )
        )
    for product_id, sites in sorted(config.sites.items()):
        for key, entry in sorted(sites.items()):
            host, path = parse_site_key(key)
            kind = entry.scope_kind or (
                CrawlPolicyScopeKind.ROOT if path else CrawlPolicyScopeKind.HOST
            )
            if kind is CrawlPolicyScopeKind.HOST and path:
                raise ValueError(
                    f"crawl_policy: sites[{product_id}][{key}] объявлен scope_kind='host', "
                    "но ключ несёт путь"
                )
            records.append(
                PolicyRecord(
                    product_id=str(product_id),
                    scope_kind=kind,
                    mode=entry.mode,
                    interval_hours=entry.interval_hours,
                    note=entry.note,
                    source=SOURCE_PACKAGE,
                    host=normalize_policy_host(host),
                    path=normalize_policy_path(path),
                )
            )
    return records


def operator_records(rows: Iterable[CrawlPolicyRow]) -> list[PolicyRecord]:
    """Действующие записи оператора (``state.db``) в ``PolicyRecord``."""
    records: list[PolicyRecord] = []
    for row in rows:
        if row.superseded_by is not None:
            continue
        records.append(
            PolicyRecord(
                product_id=str(row.product_id),
                scope_kind=CrawlPolicyScopeKind(row.scope_kind),
                mode=CrawlPolicyMode(row.mode),
                source=SOURCE_OPERATOR,
                host=None if row.host is None else normalize_policy_host(row.host),
                path=None if row.path is None else normalize_policy_path(row.path),
                interval_hours=row.interval_hours,
                note=row.note,
                actor=row.actor,
                created_at=row.created_at,
                policy_id=row.policy_id,
            )
        )
    return records


def _matches(record: PolicyRecord, root: DeclaredRoot) -> bool:
    """Запись применима к корню: продукт, host и (для root/path) префикс пути."""
    if record.product_id not in (WILDCARD_PRODUCT, root.product_id):
        return False
    if record.scope_kind is CrawlPolicyScopeKind.PRODUCT:
        return True
    if record.host != root.host:
        return False
    if record.scope_kind is CrawlPolicyScopeKind.HOST:
        return True
    return _path_covers(normalize_policy_path(record.path), root.path)


def _specificity(record: PolicyRecord) -> tuple[int, int, int]:
    """Ключ специфичности: вид области, длина префикса, точность продукта."""
    exact_product = 0 if record.product_id == WILDCARD_PRODUCT else 1
    path_len = (
        len(normalize_policy_path(record.path))
        if record.scope_kind in (CrawlPolicyScopeKind.ROOT, CrawlPolicyScopeKind.PATH)
        else 0
    )
    return (_KIND_RANK[record.scope_kind], path_len, exact_product)


def _describe(record: PolicyRecord) -> str:
    """Короткое описание записи для сообщения о конфликте (обе записи названы)."""
    return (
        f"product={record.product_id} scope={record.scope_kind.value} "
        f"host={record.host or '—'} path={record.path or '—'} "
        f"mode={record.mode.value} interval_hours={record.interval_hours} "
        f"source={record.source} policy_id={record.policy_id or '—'} "
        f"actor={record.actor or '—'}"
    )


def _choose(group: Sequence[PolicyRecord]) -> PolicyRecord:
    """Выбирает запись внутри одной специфичности; конфликт — ошибка, не тай-брейк.

    ``state.db`` переопределяет дефолт пакета **при равной специфичности**. Внутри
    одного источника разные ``(mode, interval_hours)`` неразрешимы: выбор по
    ``created_at``/``policy_id`` сделал бы резолв функцией порядка вставки.
    """
    operators = [item for item in group if item.source == SOURCE_OPERATOR]
    candidates = operators or list(group)
    semantics = {(item.mode, item.interval_hours) for item in candidates}
    if len(semantics) > 1:
        first, second = candidates[0], candidates[1]
        raise CrawlPolicyConflict(
            "crawl_policy_conflict: равная специфичность с разными режимами — "
            f"{_describe(first)} vs {_describe(second)}; "
            "тай-брейк по времени/порядку вставки запрещён (план 19 §4)"
        )
    return candidates[0]


def _as_utc(moment: _dt.datetime) -> _dt.datetime:
    """Наивное время трактуется как UTC — как остальные метки проекта."""
    return moment.replace(tzinfo=_dt.UTC) if moment.tzinfo is None else moment.astimezone(_dt.UTC)


def _parse_observed_at(raw: str | None) -> _dt.datetime | None:
    """ISO-метка наблюдения → ``datetime`` (``None`` — метки нет/не разбирается)."""
    if not raw:
        return None
    try:
        return _as_utc(_dt.datetime.fromisoformat(str(raw).replace("Z", "+00:00")))
    except ValueError:
        return None


def observations_by_root(
    observations: Mapping[tuple[str, str], str | _dt.datetime],
    roots: Sequence[DeclaredRoot],
) -> dict[tuple[str, str, str], _dt.datetime]:
    """Последнее наблюдение каждого корня: максимум по URL под ним.

    ``observations`` — ``{(product_id, normalized_url): last_checked_at}`` из
    ``url_state``. URL принадлежит корню, если совпал host и путь корня покрывает путь
    URL. Отсутствие наблюдения не выдумывается: ключа просто нет.
    """
    parsed: list[tuple[str, str, str, _dt.datetime]] = []
    for (product_id, url), raw in observations.items():
        moment = raw if isinstance(raw, _dt.datetime) else _parse_observed_at(raw)
        if moment is None:
            continue
        host, path = _split_url(url)
        if not host:
            continue
        parsed.append((str(product_id), host, path, _as_utc(moment)))
    result: dict[tuple[str, str, str], _dt.datetime] = {}
    for root in roots:
        best: _dt.datetime | None = None
        for product_id, host, path, moment in parsed:
            if product_id != root.product_id or host != root.host:
                continue
            if not _path_covers(root.path, path):
                continue
            if best is None or moment > best:
                best = moment
        if best is not None:
            result[(root.product_id, root.host, root.path)] = best
    return result


def _decision_for(
    record: PolicyRecord,
    *,
    now: _dt.datetime,
    observed_at: _dt.datetime | None,
) -> str:
    """Эффективное решение записи: режим, а для ``scheduled`` — ``refresh``/``reuse``.

    Нет наблюдения ⇒ ``refresh`` (первичный дискавери). Иначе ``refresh``, когда
    ``now - observed_at >= interval_hours``, и ``reuse`` — до срока.
    """
    if record.mode is not CrawlPolicyMode.SCHEDULED:
        return record.mode.value
    interval = int(record.interval_hours or 0)
    if observed_at is None:
        return "refresh"
    due = now - _as_utc(observed_at)
    return "refresh" if due >= _dt.timedelta(hours=interval) else "reuse"


def resolve_crawl_policy(
    *,
    package_policy: CrawlPolicyConfig,
    operator_rows: Iterable[CrawlPolicyRow],
    roots: Sequence[DeclaredRoot],
    now: _dt.datetime,
    observed_at: Mapping[tuple[str, str, str], _dt.datetime] | None = None,
) -> CrawlPolicyResolution:
    """Разрешает эффективную политику по объявленным корням (план 19 §4/§6.1).

    :param now: момент разрешения — **параметр**, а не ``datetime.now()`` внутри: иначе
        решение ``scheduled`` и его дайджест недетерминированы и тест на «due» невозможен.
    :param observed_at: последнее наблюдение корня по ключу
        ``(product_id, host, path)``; ``None`` — наблюдения нет (первичный дискавери).
    :raises CrawlPolicyConflict: равная специфичность с разными режимами.
    """
    records = [*package_records(package_policy), *operator_records(operator_rows)]
    ordered_roots = sorted(
        {(root.product_id, root.host, root.path): root for root in roots}.values(),
        key=lambda root: (root.product_id, root.host, root.path),
    )
    observed = dict(observed_at or {})
    decisions: list[PolicyDecision] = []
    for root in ordered_roots:
        candidates = [record for record in records if _matches(record, root)]
        if not candidates:
            continue
        groups: dict[tuple[int, int, int], list[PolicyRecord]] = {}
        for record in candidates:
            groups.setdefault(_specificity(record), []).append(record)
        chosen = _choose(groups[max(groups)])
        decision = _decision_for(
            chosen,
            now=_as_utc(now),
            observed_at=observed.get((root.product_id, root.host, root.path)),
        )
        decisions.append(
            PolicyDecision(
                product_id=root.product_id,
                scope_kind=chosen.scope_kind,
                mode=chosen.mode,
                decision=decision,
                source=chosen.source,
                host=chosen.host,
                path=chosen.path,
                interval_hours=chosen.interval_hours,
                note=chosen.note,
                actor=chosen.actor,
                created_at=chosen.created_at,
                policy_id=chosen.policy_id,
            )
        )
    unmatched = tuple(
        record
        for record in records
        if record.product_id != WILDCARD_PRODUCT
        and not any(_matches(record, root) for root in ordered_roots)
    )
    digest = crawl_policy_digest(
        (
            decision.product_id,
            decision.scope_kind.value,
            decision.host,
            decision.path,
            decision.decision,
        )
        for decision in decisions
    )
    return CrawlPolicyResolution(
        decisions=tuple(decisions), unmatched=unmatched, digest=digest
    )