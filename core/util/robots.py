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

**Семантика ленты.** Недоступная лента (``None``/исключение) трактуется как «правил нет»
(как 404 в штатной семантике robots) — это **дефолт-разрешение**, а не молчаливый обход.
``allowed`` возвращает ``False`` только при явном выигравшем ``Disallow`` для нашего агента.
Сбой разбора не роняет краул (``True``).

``crawl_delay`` отдаёт объявленную агенту задержку (сек), если она есть: адаптеры могут
использовать её как **floor** (не как источник пейсинга — им владеет corpus, P2-3);
явный ``rate_limit`` всегда важнее.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from urllib.parse import urlsplit

__all__ = ["RobotsGate"]

# Незарезервированные символы RFC 3986: percent-escape такого символа и сам символ
# канонически эквивалентны. Остальные escape-последовательности сохраняются как есть.
_UNRESERVED = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
)
_HEX_DIGITS = frozenset("0123456789abcdefABCDEF")


def _normalize(value: str) -> str:
    """Каноническая форма строки для сравнения: не-ASCII → percent-encoded.

    Декодируются только escape-последовательности незарезервированных символов (RFC 3986),
    остальные приводятся к верхнему регистру hex. Благодаря этому правило, записанное
    percent-encoded, и обычный URL сравниваются эквивалентно, а ``%2F`` не превращается
    в разделитель пути.
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


def _compile(pattern: str) -> re.Pattern[str]:
    """Скомпилировать шаблон robots-правила (``*`` — любой отрезок, ``$`` — якорь конца)."""
    anchored = pattern.endswith("$")
    body = pattern[:-1] if anchored else pattern
    pieces: list[str] = []
    for char in body:
        pieces.append(".*" if char == "*" else re.escape(char))
    suffix = "$" if anchored else ""
    return re.compile("^" + "".join(pieces) + suffix)


@dataclass(frozen=True)
class _Rule:
    """Одно правило группы: шаблон как он написан, вид и скомпилированное выражение."""

    pattern: str
    allow: bool
    regex: re.Pattern[str]


@dataclass
class _Group:
    """Группа ``User-agent``: токены агентов, правила и объявленная задержка."""

    agents: list[str] = field(default_factory=list)
    rules: list[_Rule] = field(default_factory=list)
    crawl_delay: float | None = None


def _parse(text: str) -> list[_Group]:
    """Разобрать текст ``robots.txt`` в группы (RFC 9309, неизвестные ключи игнорируются)."""
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
            flush()
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
            group.rules.append(
                _Rule(pattern=value, allow=key == "allow", regex=_compile(_normalize(value)))
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
            if rule.regex.match(target) is None:
                continue
            # Длиннее — важнее; при равенстве True (Allow) важнее False (Disallow).
            key = (len(rule.pattern), rule.allow)
            if best is None or key > best:
                best = key
        return True if best is None else best[1]


class RobotsGate:
    """Per-host политика ``robots.txt`` (собственный RFC 9309-матчер, инъектируемый фетч).

    ``fetch(host)`` → текст ``robots.txt`` либо ``None`` (недоступен). Недоступная лента
    трактуется как «правил нет» (как 404 в штатной семантике robots). Проверка — только
    запрет: ``allowed`` возвращает ``False`` лишь при явном выигравшем ``Disallow``.
    """

    def __init__(
        self, fetch: Callable[[str], str | None], *, user_agent: str = "*"
    ) -> None:
        self._fetch = fetch
        self._user_agent = user_agent
        self._policies: dict[str, _Policy] = {}

    def _policy(self, host: str) -> _Policy:
        policy = self._policies.get(host)
        if policy is not None:
            return policy
        text: str | None
        try:
            text = self._fetch(host)
        except Exception:  # noqa: BLE001 — недоступный robots.txt = «правил нет»
            text = None
        groups = _parse(text or "")
        policy = _Policy(self._select_groups(groups))
        self._policies[host] = policy
        return policy

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
                token = agent.strip().lower()
                if token == "*":
                    specificity = max(specificity, 0)
                elif token and our_agent.startswith(token):
                    specificity = max(specificity, len(token))
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
        return _normalize(target)

    def allowed(self, url: str) -> bool:
        """Разрешён ли ``url`` для нашего агента (``True`` при отсутствии запрета)."""
        host = (urlsplit(url).hostname or "").lower()
        if not host:
            return True
        try:
            return bool(self._policy(host).allowed(self._target(url)))
        except Exception:  # noqa: BLE001 — не роняем зонд из-за разбора robots
            return True

    def crawl_delay(self, url: str) -> float | None:
        """Объявленная агенту задержка (сек) из ``Crawl-delay``; ``None`` — не задана.

        Возвращается как **floor** для пейсинга: адаптеры применяют её лишь при отсутствии
        явного ``rate_limit`` (явный лимит всегда важнее). Недоступная лента/сбой разбора →
        ``None``.
        """
        host = (urlsplit(url).hostname or "").lower()
        if not host:
            return None
        try:
            return self._policy(host).crawl_delay
        except Exception:  # noqa: BLE001 — сбой разбора robots не роняет краул
            return None
