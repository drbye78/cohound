"""coHound — per-host политика ``robots.txt`` (чистый stdlib; D1, §10).

Единая реализация робот-этики: собственный разбор и матчинг ``robots.txt`` по RFC 9309 с
**инъектируемым** фетчем текста ленты (в core нет сетевого I/O — в реальном пути ленту
приносит egress-слой/DMZ, а не ядро). Класс промотирован сюда из
``core.pipeline.crawl_probe`` (единственная реализация; запрос-зонд импортирует его отсюда).

**Семантика матчинга.** Сравнивается путь **вместе с query** (``/x?y=1``). ``*`` в шаблоне
совпадает с любым отрезком (включая ``/``), завершающий ``$`` якорит конец строки, шаблон
без ``$`` — префиксный. Побеждает **самое длинное** совпавшее правило (длина считается по
шаблону как он написан, звёздочки учитываются); при равенстве ``Allow`` важнее ``Disallow``.
Ничего не совпало — разрешено. Пустой ``Disallow:`` = «разрешено всё».

Матчинг — **линейный glob-матчер** (шаблон делится по ``*``, отрезки ищутся последовательным
``str.find``), без регулярных выражений: ``robots.txt`` — недоверенный вход (RFC 9309 §3),
и вложенные звёздочки не должны вызывать катастрофический бэктрекинг при проверке каждого URL.

**Семантика ленты — наблюдение, а не умолчание.** У каждого origin есть явное состояние
:class:`RobotsFeedState`, и следствие выводится из состояния, а не из «правил нет»:

* ``fetched`` — лента прочитана (в том числе пустая: пустая лента — факт «разрешено всё»);
* ``not_found`` — origin ответил ``404``/``410``: ленты нет, это **наблюдение**, разрешено всё;
* ``unavailable`` — прочитать не удалось (5xx, сеть, таймаут, сбой разбора, а также ``None``
  от фетчера — «наблюдения нет»). По RFC 9309 §2.3.1.4 origin считается **запрещённым**:
  «не смог прочитать запрет» ≠ «запрета нет». Отказ виден вызывающему (:attr:`RobotsVerdict.
  reason` = ``robots_unavailable``), а не превращается в тихий ноль страниц;
* ``not_checked`` — этот origin ещё не наблюдался;
* ``disabled`` — вызывающий не просил проверку (ось ``plan.robots`` выключена).

``allowed`` по-прежнему возвращает ``bool``, но ``False`` теперь означает и выигравший
``Disallow``, и недоступную ленту — различает их :meth:`RobotsGate.verdict` (там же машинный
код причины). Прежний дефолт-разрешение при недоступности был отступлением от RFC и заменён:
он превращал «мы не знаем разрешения» в разрешение.

``crawl_delay`` отдаёт объявленную агенту задержку (сек), если она есть: адаптеры могут
использовать её как **floor** (не как источник пейсинга — им владеет corpus, P2-3);
явный ``rate_limit`` всегда важнее.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from urllib.parse import urlsplit

__all__ = [
    "ROBOTS_DISALLOW_REASON",
    "ROBOTS_UNAVAILABLE_REASON",
    "RobotsFeed",
    "RobotsFeedState",
    "RobotsGate",
    "RobotsVerdict",
    "url_canonical_form",
]

#: Машинный код отказа «явный ``Disallow`` выиграл» (= ``FailureKind.ROBOTS_DISALLOW``).
ROBOTS_DISALLOW_REASON = "robots_disallow"
#: Машинный код отказа «лента недоступна, поэтому origin не обходится» (RFC 9309 §2.3.1.4).
#: Отличается от :data:`ROBOTS_DISALLOW_REASON` намеренно: сайт не запрещал обход — мы не
#: смогли установить разрешение, и запрет выведен из отсутствия наблюдения.
ROBOTS_UNAVAILABLE_REASON = "robots_unavailable"

#: Статусы «ленты определённо нет»: это ответ origin, а не отсутствие наблюдения.
#: Источник истины — ``core.ports.crawl_errors.NOT_FOUND_STATUSES``; равенство проверяется
#: тестом (``tests/test_util_robots.py``), чтобы константы не разъехались: ``core.util``
#: не импортирует контракты портов (направление зависимости ``ports → util``).
_ROBOTS_ABSENT_STATUSES = frozenset({404, 410})


class RobotsFeedState(StrEnum):
    """Наблюдённое состояние ленты ``robots.txt`` одного origin (RFC 9309 §2.3.1.4).

    Состояние — это **наблюдение**, и только из него выводится следствие: ``fetched``/
    ``not_found`` разрешают обход, ``unavailable`` его запрещает (RFC: недоступная лента —
    «полный запрет»), ``not_checked``/``disabled`` означают, что наблюдения не было вовсе.
    Пять значений вместо булева «правил нет» нужны потому, что «правил нет» смешивало
    прочитанную пустую ленту, определённый ``404`` и неудачу чтения — три разных факта с
    тремя разными следствиями.
    """

    FETCHED = "fetched"
    NOT_FOUND = "not_found"
    UNAVAILABLE = "unavailable"
    NOT_CHECKED = "not_checked"
    DISABLED = "disabled"


@dataclass(frozen=True)
class RobotsFeed:
    """Явное наблюдение ленты: состояние + текст, если он прочитан.

    Фетчер вправе вернуть этот объект вместо «текст либо ``None``»: ``None`` означает
    «наблюдения нет» (для вызывающего это ``unavailable``), и отличить его от
    определённого ``404`` можно только состоянием, а не отсутствием строки.
    """

    state: RobotsFeedState
    text: str | None = None


@dataclass(frozen=True)
class RobotsVerdict:
    """Результат проверки одного URL: состояние ленты, решение и машинный код причины.

    ``reason`` — ``None`` при разрешении, :data:`ROBOTS_DISALLOW_REASON` при выигравшем
    ``Disallow`` и :data:`ROBOTS_UNAVAILABLE_REASON` при недоступной ленте. Код нужен
    адаптеру, чтобы отказ по политике доехал до наблюдаемости отличимо от «сайт запретил».
    """

    state: RobotsFeedState
    allowed: bool
    reason: str | None = None


# Незарезервированные символы RFC 3986: percent-escape такого символа и сам символ
# канонически эквивалентны. Остальные escape-последовательности сохраняются как есть.
_UNRESERVED = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
)
_HEX_DIGITS = frozenset("0123456789abcdefABCDEF")


def url_canonical_form(value: str) -> str:
    """Каноническая форма строки для сравнения: не-ASCII → percent-encoded.

    Декодируются только escape-последовательности незарезервированных символов (RFC 3986),
    остальные приводятся к верхнему регистру hex. Благодаря этому правило, записанное
    percent-encoded, и обычный URL сравниваются эквивалентно, а ``%2F`` не превращается
    в разделитель пути.

    Публична не только ради правил ``robots.txt``: это и есть форма сравнения адреса
    (план 20 §F1 — снимок корпуса не должен зависеть от того, записан адрес
    percent-encoded или декодированным). Функция идемпотентна, поэтому применять её к уже
    канонизированной строке безопасно.
    """
    out: list[str] = []
    index = 0
    length = len(value)
    while index < length:
        char = value[index]
        if (
            char == "%"
            and index + 2 < length
            and value[index + 1] in _HEX_DIGITS
            and value[index + 2] in _HEX_DIGITS
        ):
            escaped = value[index + 1 : index + 3]
            decoded = chr(int(escaped, 16))
            out.append(decoded if decoded in _UNRESERVED else f"%{escaped.upper()}")
            index += 3
            continue
        if ord(char) > 0x7F:
            out.append("".join(f"%{byte:02X}" for byte in char.encode("utf-8")))
        else:
            out.append(char)
        index += 1
    return "".join(out)


def _matches(segments: tuple[str, ...], anchored: bool, target: str) -> bool:
    """Линейно сопоставить шаблон (разбитый по ``*``) с нормализованной целью.

    Шаблон ``seg0 * seg1 * … * segN`` читается как «``seg0``, затем любой отрезок, затем
    ``seg1``, …». Первый отрезок — префикс; средние ищутся последовательным ``str.find``
    от текущей позиции (самое левое вхождение оставляет максимум места следующим, поэтому
    полнота не теряется); последний отрезок при ``$`` — суффикс, иначе ищется как обычный
    отрезок (шаблон без ``$`` префиксный, хвост цели свободен).

    Сложность — ``O(len(target) · число отрезков)``, без бэктрекинга: недоверенный
    ``robots.txt`` не может подвесить проверку URL.
    """
    first = segments[0]
    if not target.startswith(first):
        return False
    if len(segments) == 1:
        # Звёздочек нет: без ``$`` — префикс, с ``$`` — точное совпадение.
        return len(target) == len(first) if anchored else True
    position = len(first)
    last = segments[-1]
    for segment in segments[1:-1]:
        found = target.find(segment, position)
        if found < 0:
            return False
        position = found + len(segment)
    if anchored:
        start = len(target) - len(last)
        return start >= position and target.endswith(last)
    return target.find(last, position) >= 0


@dataclass(frozen=True)
class _Rule:
    """Одно правило группы: шаблон как он написан, вид и разбиение для матчинга."""

    pattern: str
    allow: bool
    segments: tuple[str, ...]
    anchored: bool

    def matches(self, target: str) -> bool:
        return _matches(self.segments, self.anchored, target)


@dataclass
class _Group:
    """Группа ``User-agent``: токены агентов, правила и объявленная задержка."""

    agents: list[str] = field(default_factory=list)
    rules: list[_Rule] = field(default_factory=list)
    crawl_delay: float | None = None


def _parse(text: str) -> list[_Group]:
    """Разобрать текст ``robots.txt`` в группы (RFC 9309, неизвестные ключи игнорируются).

    Пустая строка **не** закрывает группу: ABNF RFC 9309 §2.1 допускает ``emptyline`` и
    между ``User-agent``, и между правилами (``group = startgroupline *(startgroupline /
    emptyline) *(rule / emptyline)``). Новую группу открывает только ``User-agent``,
    встреченный после правил. Ведущий UTF-8 BOM отбрасывается: иначе первый ключ не
    распознаётся и вся первая группа молча теряется.
    """
    text = text.removeprefix("\ufeff")
    groups: list[_Group] = []
    group = _Group()
    in_rules = False

    def flush() -> None:
        nonlocal group, in_rules
        if group.agents:
            groups.append(group)
        group = _Group()
        in_rules = False

    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            # Пустая строка внутри группы правил — часть группы, а не её конец.
            continue
        key, separator, value = line.partition(":")
        if not separator:
            continue
        key = key.strip().lower()
        value = value.strip()
        if key == "user-agent":
            if in_rules:
                flush()
            if value:
                group.agents.append(value)
            continue
        if not group.agents:
            # Правило до первого User-agent не принадлежит ни одной группе.
            continue
        if key in ("allow", "disallow"):
            in_rules = True
            if not value:
                # Пустой Disallow/Allow — «нет правила» (пустой Disallow = разрешено всё).
                continue
            normalized = url_canonical_form(value)
            anchored = normalized.endswith("$")
            body = normalized[:-1] if anchored else normalized
            group.rules.append(
                _Rule(
                    pattern=value,
                    allow=key == "allow",
                    segments=tuple(body.split("*")),
                    anchored=anchored,
                )
            )
        elif key == "crawl-delay":
            in_rules = True
            try:
                delay = float(value)
            except ValueError:
                continue
            if group.crawl_delay is None:
                group.crawl_delay = delay
    flush()
    return groups


class _Policy:
    """Выбранные для нашего агента правила одного хоста."""

    def __init__(self, groups: list[_Group]) -> None:
        self._rules: list[_Rule] = [rule for group in groups for rule in group.rules]
        self.crawl_delay: float | None = next(
            (group.crawl_delay for group in groups if group.crawl_delay is not None), None
        )

    def allowed(self, target: str) -> bool:
        """Разрешён ли нормализованный ``target`` (путь + query)."""
        best: tuple[int, bool] | None = None
        for rule in self._rules:
            if not rule.matches(target):
                continue
            # Длиннее — важнее; при равенстве True (Allow) важнее False (Disallow).
            key = (len(rule.pattern), rule.allow)
            if best is None or key > best:
                best = key
        return True if best is None else best[1]


@dataclass(frozen=True)
class _Observation:
    """Что мы **наблюдали** у origin: состояние ленты и, если прочитана, её правила.

    ``policy is None`` при ``not_found`` (правил нет — разрешено всё) и при ``unavailable``
    (правил мы не видели — обход запрещён). Различие держится на ``state``, а не на ``policy``.
    """

    state: RobotsFeedState
    policy: _Policy | None


def _state_of_exception(exc: BaseException) -> RobotsFeedState:
    """Состояние ленты по исключению фетчера: ``not_found`` либо ``unavailable``.

    Признак берётся **структурно** — по атрибутам таксономии сбоев краула
    (``http_status``/``failure_kind`` из ``core.ports.crawl_errors``), а не по тексту:
    ``CrawlNotFound`` несёт ``http_status`` 404/410 либо ``failure_kind="not_found"``, и это
    определённый ответ origin'а («ленты нет»). Любое другое исключение (5xx, сеть, таймаут)
    — отсутствие наблюдения.
    """
    status = getattr(exc, "http_status", None)
    if isinstance(status, int) and status in _ROBOTS_ABSENT_STATUSES:
        return RobotsFeedState.NOT_FOUND
    if getattr(exc, "failure_kind", None) == "not_found":
        return RobotsFeedState.NOT_FOUND
    return RobotsFeedState.UNAVAILABLE


def _normalize_feed(feed: object) -> tuple[RobotsFeedState, str | None]:
    """Приводит ответ фетчера к паре «состояние ленты, текст».

    Принимается и :class:`RobotsFeed` (явное состояние), и ``str`` (прочитанный текст,
    в том числе пустой — это факт «разрешено всё»), и ``None`` — «наблюдения нет».
    """
    if isinstance(feed, RobotsFeed):
        return feed.state, feed.text
    if isinstance(feed, str):
        return RobotsFeedState.FETCHED, feed
    return RobotsFeedState.UNAVAILABLE, None


class RobotsGate:
    """Per-host политика ``robots.txt`` (собственный RFC 9309-матчер, инъектируемый фетч).

    ``fetch(host)`` → текст ``robots.txt``, :class:`RobotsFeed` либо ``None``. ``None``
    означает **отсутствие наблюдения** и трактуется по RFC 9309 §2.3.1.4 как полный запрет
    обхода origin'а; ``404``/``410`` (исключение ``CrawlNotFound`` или
    ``RobotsFeed(NOT_FOUND)``) — наоборот, разрешение: origin ответил, что ленты нет, и это
    наблюдение, а не его отсутствие. Явный выигравший ``Disallow`` запрещает свой путь.

    :meth:`verdict` возвращает решение вместе с состоянием и машинным кодом причины;
    :meth:`allowed` оставлен как булево для мест, которым код не нужен.
    """

    def __init__(
        self, fetch: Callable[[str], object], *, user_agent: str = "*"
    ) -> None:
        self._fetch = fetch
        self._user_agent = user_agent
        self._observations: dict[str, _Observation] = {}

    def _observe(self, host: str) -> _Observation:
        """Наблюдение origin'а (кэшируется: один ``robots.txt`` на origin за прогон)."""
        observation = self._observations.get(host)
        if observation is not None:
            return observation
        observation = self._read(host)
        self._observations[host] = observation
        return observation

    def _read(self, host: str) -> _Observation:
        """Прочитать и разобрать ленту; любой сбой чтения — ``unavailable``."""
        try:
            feed = self._fetch(host)
        except Exception as exc:  # noqa: BLE001 — сбой чтения = отсутствие наблюдения
            return _Observation(_state_of_exception(exc), None)
        state, text = _normalize_feed(feed)
        if state is not RobotsFeedState.FETCHED:
            return _Observation(state, None)
        try:
            groups = _parse(text or "")
        except Exception:  # noqa: BLE001 — недоверенный вход: разбор не роняет краул
            # Лента прочитана, но правил из неё извлечь нельзя: разрешение не установлено,
            # поэтому следствие то же, что у недоступной ленты (не «правил нет»).
            return _Observation(RobotsFeedState.UNAVAILABLE, None)
        return _Observation(state, _Policy(self._select_groups(groups)))

    def state(self, url: str) -> RobotsFeedState:
        """Наблюдённое состояние ленты origin'а ``url`` (при необходимости — прочитать)."""
        host = (urlsplit(url).hostname or "").lower()
        if not host:
            return RobotsFeedState.NOT_CHECKED
        return self._observe(host).state

    def observed_state(self, url: str) -> RobotsFeedState:
        """Состояние **уже сделанного** наблюдения; ``not_checked`` — origin не спрашивали.

        Отличается от :meth:`state` тем, что не делает сетевого шага: это ответ на вопрос
        «что мы знаем», а не «сходи и узнай».
        """
        host = (urlsplit(url).hostname or "").lower()
        if not host:
            return RobotsFeedState.NOT_CHECKED
        observation = self._observations.get(host)
        return observation.state if observation is not None else RobotsFeedState.NOT_CHECKED

    def verdict(self, url: str, *, enabled: bool = True) -> RobotsVerdict:
        """Решение по ``url``: состояние ленты, разрешение и машинный код причины.

        ``enabled=False`` — проверку не просили (ось ``plan.robots`` выключена): состояние
        ``disabled``, обход разрешён, лента не запрашивается. Это состояние оси, а не
        наблюдение: «мы не спрашивали» отличается от «спросили и не смогли прочитать».
        """
        if not enabled:
            return RobotsVerdict(RobotsFeedState.DISABLED, True, None)
        host = (urlsplit(url).hostname or "").lower()
        if not host:
            return RobotsVerdict(RobotsFeedState.NOT_CHECKED, True, None)
        observation = self._observe(host)
        if observation.state is RobotsFeedState.UNAVAILABLE:
            # RFC 9309 §2.3.1.4: недоступная лента = полный запрет origin'а. Отказ видим
            # вызывающему машинным кодом, а не выглядит как «правил нет».
            return RobotsVerdict(
                observation.state, False, ROBOTS_UNAVAILABLE_REASON
            )
        policy = observation.policy
        if policy is None:
            return RobotsVerdict(observation.state, True, None)
        try:
            allowed = policy.allowed(self._target(url))
        except Exception:  # noqa: BLE001 — сбой матчинга = разрешение не установлено
            return RobotsVerdict(RobotsFeedState.UNAVAILABLE, False, ROBOTS_UNAVAILABLE_REASON)
        reason = None if allowed else ROBOTS_DISALLOW_REASON
        return RobotsVerdict(observation.state, allowed, reason)

    def _select_groups(self, groups: list[_Group]) -> list[_Group]:
        """Выбрать группы: самый длинный регистронезависимый префикс агента, иначе ``*``.

        Наш агент ``"*"`` (дефолт ядра) — это «мы безымянный клиент»: ему принадлежит
        **только** группа ``*``, а не именованные группы чужих роботов.
        """
        our_agent = self._user_agent.strip().lower()
        if our_agent == "*":
            return [
                group
                for group in groups
                if any(agent.strip() == "*" for agent in group.agents)
            ]
        best = -1
        matched: list[_Group] = []
        for group in groups:
            specificity = -1
            for agent in group.agents:
                # ``product-token`` агента (RFC 9309 §2.2.1): с ним сравнивается наше
                # собственное имя как с префиксом, поэтому и имя — ``agent_prefix``.
                agent_prefix = agent.strip().lower()
                if agent_prefix == "*":
                    specificity = max(specificity, 0)
                elif agent_prefix and our_agent.startswith(agent_prefix):
                    specificity = max(specificity, len(agent_prefix))
            if specificity < 0:
                continue
            if specificity > best:
                best = specificity
                matched = [group]
            elif specificity == best:
                matched.append(group)
        return matched

    @staticmethod
    def _target(url: str) -> str:
        """Путь + query URL в канонической форме (fragment отбрасывается)."""
        parts = urlsplit(url)
        target = parts.path or "/"
        if parts.query:
            target = f"{target}?{parts.query}"
        return url_canonical_form(target)

    def allowed(self, url: str) -> bool:
        """Разрешён ли ``url`` для нашего агента (``False`` — запрет **или** недоступная лента).

        Булево без причины: кому нужен машинный код (``robots_disallow`` vs
        ``robots_unavailable``), тот вызывает :meth:`verdict`.
        """
        return self.verdict(url).allowed

    def crawl_delay(self, url: str) -> float | None:
        """Объявленная агенту задержка (сек) из ``Crawl-delay``; ``None`` — не задана.

        Возвращается как **floor** для пейсинга: адаптеры применяют её лишь при отсутствии
        явного ``rate_limit`` (явный лимит всегда важнее). Недоступная лента/сбой разбора →
        ``None`` (задержки не наблюдали — не выдумываем её).
        """
        host = (urlsplit(url).hostname or "").lower()
        if not host:
            return None
        try:
            observation = self._observe(host)
        except Exception:  # noqa: BLE001 — сбой чтения robots не роняет краул
            return None
        policy = observation.policy
        return None if policy is None else policy.crawl_delay
