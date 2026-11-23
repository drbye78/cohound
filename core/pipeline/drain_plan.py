"""coHound — план докачки усечённых корней до исчерпания источников.

Зачем отдельный модуль. «Докачать корень» — это три разных вопроса, и до сих пор ни
один не был назван в одном месте: **какие** корни усечены (факт прошлого прогона),
**какое** значение объявить в ``domain_profiles.yaml``, чтобы следующий прогон не
усекал на уже достигнутой отметке, и **как** правку внести, не потеряв авторские
комментарии (шапка файла — это обоснование живых замеров, а не украшение).

Здесь только чистые функции: ни сети, ни БД, ни записи. Провенанс прогона читает
команда (``hound crawl drain``), исполнение докачки — фаза корпуса под политикой
``drain``, а этот модуль отвечает на «что объявить» и «как это записать».

**Почему «измеренное + запас», а не «ровно измеренное».** Значение предела — не
измерение, а объявление: измерено число документов, а предел объявляется с запасом,
чтобы сайт, выросший на десяток страниц, не давал нового усечения через неделю. Запас
при этом обязан быть **объявленным числом** (``margin_percent``), а не «на глаз»:
он входит в артефакт и в печать рядом с измеренным значением, иначе «предел 20 000»
читался бы как замер (A3).

Нейтрален к предметной категории (§1.3): домены и ключи — данные.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import yaml

from core.pipeline.crawl_probe import _split_top_level_blocks, split_header
from core.util.rate_limit import normalize_host

__all__ = [
    "DEFAULT_DEPTH_STEP",
    "DEFAULT_MARGIN_PERCENT",
    "DrainUpdate",
    "DrainedRoot",
    "MeasuredRoot",
    "TruncatedRoot",
    "declared_depth",
    "declared_limit",
    "drainable_reason",
    "is_drainable",
    "matches_profile_key",
    "measured_roots",
    "plan_updates",
    "render_profile_limits",
    "resolve_profile_key",
    "root_key",
    "truncated_roots",
]

#: Объявленный запас по умолчанию, проценты. Значение — **объявление**, а не замер:
#: оно печатается рядом с измеренным числом и входит в артефакт правки.
DEFAULT_MARGIN_PERCENT = 10

#: На сколько поднимать ``max_depth`` за один шаг, когда корень усечён **глубиной**.
#: Глубина — не бюджет, а граница пространства: «+1» — минимальное расширение, при
#: котором появляется следующий уровень, а не оценка размера сайта.
DEFAULT_DEPTH_STEP = 1

#: Причина усечения, которую лечит подъём глубины (остальные лечатся снятием потолка
#: документов). Токен из ``Frontier._mark_truncated``.
_DEPTH_REASON = "max_depth"


@dataclass(frozen=True)
class MeasuredRoot:
    """Корень прогона с измеренным числом документов (``effective_limits.roots[*]``).

    ``documents = pages_fetched + replayed``: под политикой ``drain`` известные страницы
    читаются из сохранённых байтов и в ``pages_fetched`` не попадают (это «сколько
    добавил этот проход»), поэтому размер корня — сумма двух слагаемых. Единица та же,
    что у бюджета ``max_documents`` (добытые страницы), иначе объявленный предел
    сравнивался бы с числом в другой единице.

    :param product_id: продукт-владелец корня (партиция I17).
    :param root_url: авторский seed-корень.
    :param documents: размер корня по наблюдению (``pages_fetched + replayed``).
    :param replayed: сколько страниц прочитано из сохранённых байтов (0 у обычного прогона).
    :param truncated: был ли обход усечён в этом прогоне.
    :param reason: ``truncated_reason`` как записан.
    :param queue_remaining: остаток очереди на момент усечения (нижняя граница).
    :param max_depth: разрешённый ``max_depth``; ``None`` — не записан.
    :param max_documents: разрешённый ``max_documents``; ``None`` — потолка не было.
    """

    product_id: str
    root_url: str
    documents: int
    replayed: int
    truncated: bool
    reason: str | None
    queue_remaining: int | None
    max_depth: int | None
    max_documents: int | None

    @property
    def key(self) -> str:
        """Ключ, выведенный из URL (``host`` или ``host/path``); см. :func:`root_key`."""
        return root_key(self.root_url)

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление (артефакт, печать)."""
        return {
            "product_id": self.product_id,
            "root": self.root_url,
            "key": self.key,
            "documents": self.documents,
            "replayed": self.replayed,
            "truncated": self.truncated,
            "truncated_reason": self.reason,
            "queue_remaining": self.queue_remaining,
            "max_depth": self.max_depth,
            "max_documents": self.max_documents,
        }


@dataclass(frozen=True)
class TruncatedRoot:
    """Усечённый корень из провенанса прогона — вход плана докачки.

    Тонкая обёртка над :class:`MeasuredRoot`: план работает только с усечёнными
    корнями, и «нечего докачивать» не должно выражаться нулём.
    """

    product_id: str
    root_url: str
    reason: str | None
    pages_fetched: int
    queue_remaining: int | None
    max_depth: int | None
    max_documents: int | None

    @property
    def key(self) -> str:
        """Ключ ``domain_profiles.yaml`` этого корня (``host`` или ``host/path``)."""
        return root_key(self.root_url)

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление (артефакт правки, печать)."""
        return {
            "product_id": self.product_id,
            "root": self.root_url,
            "key": self.key,
            "truncated_reason": self.reason,
            "pages_fetched": self.pages_fetched,
            "queue_remaining": self.queue_remaining,
            "max_depth": self.max_depth,
            "max_documents": self.max_documents,
        }


@dataclass(frozen=True)
class DrainUpdate:
    """Одна правка объявленных пределов корня.

    :param key: ключ ``domain_profiles.yaml``.
    :param product_id: продукт (для артефакта: один ключ может принадлежать одному).
    :param root_url: корень как объявлен (артефакт).
    :param measured: измеренное число документов корня.
    :param max_documents: новое объявленное значение (``measured`` + запас).
    :param max_depth: новое значение глубины; ``None`` — глубина не меняется.
    :param previous_max_documents: что было объявлено до правки (``None`` — не было).
    :param previous_max_depth: что было объявлено до правки.
    """

    key: str
    product_id: str
    root_url: str
    measured: int
    max_documents: int
    max_depth: int | None
    previous_max_documents: int | None
    previous_max_depth: int | None

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление правки (артефакт, ``--dry-run``)."""
        return {
            "key": self.key,
            "product_id": self.product_id,
            "root": self.root_url,
            "measured": self.measured,
            "max_documents": self.max_documents,
            "previous_max_documents": self.previous_max_documents,
            "max_depth": self.max_depth,
            "previous_max_depth": self.previous_max_depth,
        }


def root_key(url: str) -> str:
    """Корень URL → ключ профиля домена (``host`` или ``host/path`` без хвостового ``/``).

    Форма совпадает с ключами ``domain_profiles.yaml`` (``03`` §D.2): схема и порт в
    ключ не входят (профиль объявляется по хосту), путь — без хвостового слэша, чтобы
    ``/docs`` и ``/docs/`` были одним ключом. ``www.`` **не** срезается: ключ — это
    объявленный хост, и в пилоте ``www.gaz-is.ru`` объявлен отдельно от ``gaz-is.ru``;
    эквивалентность ``www.`` действует при матчинге (``fetch_plan._host_matches``:
    хост равен ключу или является его поддоменом), а не при выводе ключа.

    :raises ValueError: у URL нет хоста (ключ не выводится из чего-либо).
    """
    parts = urlsplit(url)
    host = normalize_host(parts.netloc)
    if not host:
        raise ValueError(f"drain_plan: у URL нет хоста: {url!r}")
    path = (parts.path or "").rstrip("/")
    return f"{host}{path}" if path else host


def _as_int(value: Any) -> int | None:
    """Целое из провенанса; всё, что не число, — ``None`` (а не выдуманный ноль)."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _split_key(raw_key: str) -> tuple[str, str]:
    """Ключ профиля → ``(host, path)``; путь — с ведущим ``/`` и без хвостового.

    Та же разбивка, что у выбора профиля домена (:func:`core.pipeline.fetch_plan._split_key`):
    вторая формула разошлась бы с исполнением, и правка ушла бы не в тот блок.
    """
    text = str(raw_key).strip()
    host, sep, path = text.partition("/")
    host = host.strip().lower()
    path_key = f"/{path.strip('/')}" if sep and path.strip("/") else ""
    return host, path_key


def matches_profile_key(url: str, key: str) -> bool:
    """Совпадает ли URL с ключом профиля домена (``host[+path]``).

    Семантика — профиля домена, а не scope краула (``fetch_plan._host_matches``): хост
    равен ключу **или является его поддоменом** (``www.example.org`` наследует оси
    ``example.org``), путь матчится по границе сегмента (``/docs`` не матчит
    ``/docs-old``). Именно так исполнение выбирает профиль, поэтому правка предела по
    этому правилу попадёт в блок, который действительно действует на корень.
    """
    parsed = urlsplit(url)
    url_host = (parsed.hostname or "").strip().lower()
    url_path = parsed.path or "/"
    host_key, path_key = _split_key(key)
    if not url_host or not host_key:
        return False
    if not (url_host == host_key or url_host.endswith("." + host_key)):
        return False
    if not path_key:
        return True
    return url_path == path_key or url_path.startswith(path_key + "/")


def resolve_profile_key(url: str, keys: Sequence[str]) -> str | None:
    """Ключ профиля, действующий на URL: самый длинный путь, затем самый длинный хост.

    Тот же порядок, что у :func:`core.pipeline.fetch_plan.select_domain_profile`
    (``path`` > ``host``, затем лексикографически меньший ключ), но над списком ключей:
    предел объявляется **в действующем блоке**, а не в блоке, выведенном из URL. Разница
    реальна: у корня ``https://docs.tantorlabs.ru/tdb/`` блок может быть host-wide
    (``docs.tantorlabs.ru``), и запись нового ключа ``docs.tantorlabs.ru/tdb`` создала бы
    вторую правду о том же корне.

    ``None`` — ни один ключ не подходит (правка невозможна: это не «создадим новый»).
    """
    best: str | None = None
    best_rank: tuple[int, int] | None = None
    for raw_key in keys:
        if not matches_profile_key(url, raw_key):
            continue
        host_key, path_key = _split_key(raw_key)
        rank = (len(path_key), len(host_key))
        if best_rank is None or rank > best_rank or (
            rank == best_rank and best is not None and str(raw_key) < best
        ):
            best, best_rank = str(raw_key), rank
    return best


def measured_roots(
    provenance: Mapping[str, Any] | None, *, errors: list[str]
) -> list[MeasuredRoot]:
    """Корни прогона с измеренным размером (``effective_limits.roots[*]``).

    Читается **записанный** факт, а не пересчитывается: «сколько документов в корне» —
    свойство того прогона и той версии правил. ``errors`` обязателен, потому что запись
    без URL (synthetic-бакет ревалидации) в план не годится — её пропуск обязан быть
    назван вызывающему, а не проглочен (``silent`` — это дефект, а не аккуратность).

    Порядок — ``(product_id, root)``: план не должен зависеть от порядка завершения
    корней в прошлом прогоне (он зависит от планировщика).
    """
    if not provenance:
        return []
    limits = provenance.get("effective_limits")
    records = limits.get("roots") if isinstance(limits, Mapping) else None
    roots: list[MeasuredRoot] = []
    for record in records or ():
        if not isinstance(record, Mapping):
            continue
        root_url = str(record.get("root") or "")
        if not root_url:
            errors.append(
                "drain_plan: запись корня без URL пропущена (synthetic-бакет "
                "ревалидации — не авторский корень, предела в профиле у него нет)"
            )
            continue
        record_limits = record.get("limits")
        limits_map = record_limits if isinstance(record_limits, Mapping) else {}
        fetched = _as_int(record.get("pages_fetched")) or 0
        replayed = _as_int(record.get("replayed")) or 0
        roots.append(
            MeasuredRoot(
                product_id=str(record.get("product_id") or ""),
                root_url=root_url,
                documents=fetched + replayed,
                replayed=replayed,
                truncated=bool(record.get("truncated")),
                reason=(
                    None
                    if record.get("truncated_reason") is None
                    else str(record["truncated_reason"])
                ),
                queue_remaining=_as_int(record.get("queue_remaining")),
                max_depth=_as_int(limits_map.get("max_depth")),
                max_documents=_as_int(limits_map.get("max_documents")),
            )
        )
    roots.sort(key=lambda item: (item.product_id, item.root_url))
    return roots


def truncated_roots(
    provenance: Mapping[str, Any] | None, *, errors: list[str]
) -> list[TruncatedRoot]:
    """Усечённые корни из провенанса: те, у которых ``truncated=True``.

    Обёртка над :func:`measured_roots` (одна формула чтения записи): «нечего докачивать»
    не должно выражаться нулём в общем списке, поэтому отбор — отдельным именем.
    """
    return [
        TruncatedRoot(
            product_id=root.product_id,
            root_url=root.root_url,
            reason=root.reason,
            pages_fetched=root.documents - root.replayed,
            queue_remaining=root.queue_remaining,
            max_depth=root.max_depth,
            max_documents=root.max_documents,
        )
        for root in measured_roots(provenance, errors=errors)
        if root.truncated
    ]


def declared_limit(measured: int, *, margin_percent: int = DEFAULT_MARGIN_PERCENT) -> int:
    """Объявленный предел по измеренному числу: ``ceil(measured × (100+margin)/100)``.

    Запас округляется **вверх** и никогда не даёт нуля: предел ``0`` означал бы пустой
    корпус, а ``max_documents=0`` отвергается конфигом (``gt=0``).

    :raises ValueError: отрицательное измерение или отрицательный запас.
    """
    if measured < 0:
        raise ValueError(f"drain_plan: измеренное число не может быть < 0: {measured}")
    if margin_percent < 0:
        raise ValueError(
            f"drain_plan: запас не может быть отрицательным: {margin_percent}"
        )
    return max(1, math.ceil(measured * (100 + margin_percent) / 100))


def drainable_reason(reason: str | None) -> bool:
    """Лечится ли усечение с такой причиной докачкой (единственная формула решения).

    ``max_documents`` — да (потолок снимается), ``max_depth`` — да (лестница глубины),
    причина не записана — пробуем (неизвестное не объявляем неизлечимым). Остальные
    причины (``round_budget``, ``disk_low_headroom``, ``crawl_policy_skipped`` …)
    докачкой **не** лечатся: ещё один проход потратил бы сеть, ничего не измерив.

    Причина, а не корень: решение зависит **только** от неё, и вызывающий с другой
    формой записи корня (усечённый из провенанса, измеренный из прохода) не должен
    приводить типы ради одного сравнения.
    """
    return reason in (None, "max_documents", _DEPTH_REASON)


def is_drainable(root: MeasuredRoot) -> bool:
    """Лечится ли усечение этого корня докачкой — обёртка над :func:`drainable_reason`."""
    return drainable_reason(root.reason)


def declared_depth(
    root: MeasuredRoot,
    walked_depth: int | None,
    *,
    depth_step: int = DEFAULT_DEPTH_STEP,
) -> int | None:
    """Какую глубину объявить после докачки — по **наблюдённому** результату прохода.

    Глубина — граница пространства, а не бюджет, поэтому объявляется то, что
    действительно обошли:

    * корень усечён не глубиной — глубина не меняется (``root.max_depth`` как было);
    * корень **исчерпан** на глубине ``walked_depth`` — объявляется ``walked_depth``
      (он и есть достаточный), а не «прежний + шаг»: иначе следующий прогон снова
      усечётся, хотя цена уже заплачена;
    * корень **всё ещё** усечён глубиной — объявляется ``walked_depth + depth_step``:
      лестница не дошла, и предел обязан быть выше пройденного, иначе объявление
      повторит известное усечение;
    * глубину не обходили (``walked_depth is None``) — глубина не меняется.

    :raises ValueError: ``walked_depth`` меньше нуля (глубина не бывает отрицательной).
    """
    if walked_depth is not None and walked_depth < 0:
        raise ValueError(
            f"drain_plan: глубина не может быть < 0: {walked_depth}"
        )
    if root.reason != _DEPTH_REASON or walked_depth is None:
        return root.max_depth
    if root.truncated:
        return walked_depth + depth_step
    return walked_depth


@dataclass(frozen=True)
class DrainedRoot:
    """Корень, докачанный до исчерпания, — вход правки предела.

    Отдельный тип, а не :class:`TruncatedRoot`, потому что после докачки это уже **не**
    усечённый корень: у него другое число документов и другая глубина. Смешать их
    значило бы объявлять предел по прежнему (недокачанному) наблюдению.

    :param product_id: продукт-владелец корня.
    :param root_url: авторский seed-корень.
    :param documents: измеренный размер корня (``pages_fetched + replayed``).
    :param max_depth: глубина, которую объявить (``None`` — не менять).
    :param previous_max_documents: что было объявлено до правки (``None`` — не было).
    :param previous_max_depth: что было объявлено до правки.
    :param replayed: сколько страниц прочитано из сохранённых байтов (артефакт).
    """

    product_id: str
    root_url: str
    documents: int
    max_depth: int | None
    previous_max_documents: int | None
    previous_max_depth: int | None
    replayed: int = 0

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление (артефакт, печать)."""
        return {
            "product_id": self.product_id,
            "root": self.root_url,
            "documents": self.documents,
            "replayed": self.replayed,
            "max_depth": self.max_depth,
            "previous_max_documents": self.previous_max_documents,
            "previous_max_depth": self.previous_max_depth,
        }


def plan_updates(
    drained: Sequence[DrainedRoot],
    profile_keys: Sequence[str],
    *,
    margin_percent: int = DEFAULT_MARGIN_PERCENT,
) -> list[DrainUpdate]:
    """Правки объявленных пределов по измеренному числу документов корня.

    ``profile_keys`` — ключи ``domain_profiles.yaml``: предел объявляется в **действующем**
    блоке (:func:`resolve_profile_key`), а не в выведенном из URL. Корень без блока —
    отказ: молча создать новый профиль значило бы завести вторую правду о том же корне.

    Глубину здесь не решают — её уже решил тот, кто вёл лестницу
    (:func:`declared_depth`): правка обязана быть арифметикой над наблюдением, а не
    вторым местом, где глубина угадывается.

    :raises ValueError: для корня нет действующего блока профиля.
    """
    updates: list[DrainUpdate] = []
    for root in drained:
        resolved = resolve_profile_key(root.root_url, profile_keys)
        if resolved is None:
            raise ValueError(
                "drain_plan: у корня нет блока в domain_profiles.yaml — "
                f"{root.product_id}/{root.root_url} (правка не создаёт профиль)"
            )
        updates.append(
            DrainUpdate(
                key=resolved,
                product_id=root.product_id,
                root_url=root.root_url,
                measured=root.documents,
                max_documents=declared_limit(
                    root.documents, margin_percent=margin_percent
                ),
                max_depth=root.max_depth,
                previous_max_documents=root.previous_max_documents,
                previous_max_depth=root.previous_max_depth,
            )
        )
    return updates


#: Строка поля профиля: ``  max_documents: 20000  # комментарий``. Значение — до ``#``;
#: комментарий сохраняется дословно (он объясняет замер, а не украшает файл).
_FIELD_RE = re.compile(
    r"^(?P<indent>[ \t]+)(?P<field>[A-Za-z_][A-Za-z0-9_]*):(?P<sp>[ \t]*)"
    r"(?P<value>[^#\r\n]*?)(?P<tail>[ \t]*#[^\r\n]*)?(?P<nl>\r?\n)?$"
)


def _set_field(block: str, field: str, value: int) -> str:
    """Ставит ``field: value`` в блок профиля, сохраняя отступ и хвостовой комментарий.

    Значение заменяется на месте, если поле есть; иначе вставляется рядом с соседним
    лимитом (``max_depth``/``max_documents``) или сразу после строки ключа — так порядок
    полей остаётся авторским, а diff — одной строкой.
    """
    lines = block.splitlines(keepends=True)
    if not lines:
        return block
    rendered = str(int(value))
    inserted = False
    for index, line in enumerate(lines):
        match = _FIELD_RE.match(line)
        if match is None or match.group("field") != field:
            continue
        tail = match.group("tail") or ""
        newline = match.group("nl") or ""
        lines[index] = (
            f"{match.group('indent')}{field}:{match.group('sp')}{rendered}{tail}{newline}"
        )
        inserted = True
        break
    if inserted:
        return "".join(lines)
    # Поле отсутствует: вставить после последнего из соседних лимитов, иначе после ключа.
    indent = "  "
    anchor = 0
    for index, line in enumerate(lines):
        match = _FIELD_RE.match(line)
        if match is None:
            continue
        if index == 0:
            indent = "  "
            continue
        if match.group("field") in ("max_depth", "max_documents"):
            anchor = index
        if indent == "  " and match.group("indent"):
            indent = match.group("indent")
    newline = "\r\n" if lines[0].endswith("\r\n") else "\n"
    lines.insert(anchor + 1, f"{indent}{field}: {rendered}{newline}")
    return "".join(lines)


def render_profile_limits(
    raw: str,
    updates: Sequence[DrainUpdate],
    *,
    depth_field: bool = True,
) -> str:
    """Правит ``domain_profiles.yaml`` по плану, сохраняя шапку и комментарии.

    Переписываются только блоки, чьи значения изменились; остальные возвращаются
    **дословно** (тот же приём, что у :func:`core.pipeline.crawl_probe.render_profiles_with_verdicts`).
    Если править нечего, исходный текст возвращается байт-в-байт — вызывающий видит это
    сравнением строк и файл не трогает.

    :param depth_field: ``False`` — глубину не писать (``max_depth`` остаётся авторским).
    :raises ValueError: корень файла не отображение, либо ключа плана в файле нет
        (правка не должна создавать новый профиль домена: это другой предмет).
    """
    if not updates:
        return raw
    header, body = split_header(raw)
    data = yaml.safe_load(body) if body.strip() else None
    if data is None:
        data = {}
    if not isinstance(data, dict):
        raise ValueError("domain_profiles.yaml: ожидался YAML-словарь верхнего уровня")

    by_key = {update.key: update for update in updates}
    missing = [key for key in by_key if key not in data]
    if missing:
        raise ValueError(
            "domain_profiles.yaml: ключи плана отсутствуют в файле — "
            f"{sorted(missing)}"
        )

    changed = False
    parts: list[str] = [header]
    for kind, text in _split_top_level_blocks(body):
        if kind != "key":
            parts.append(text)
            continue
        parsed = yaml.safe_load(text)
        if not (isinstance(parsed, dict) and len(parsed) == 1):
            parts.append(text)
            continue
        key = next(iter(parsed))
        update = by_key.get(key)
        if update is None:
            parts.append(text)
            continue
        new_text = _set_field(text, "max_documents", update.max_documents)
        if depth_field and update.max_depth is not None:
            new_text = _set_field(new_text, "max_depth", update.max_depth)
        if new_text != text:
            changed = True
        parts.append(new_text)

    if not changed:
        return raw
    return "".join(parts)
