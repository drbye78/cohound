"""coHound — ошибки и коды возврата CLI (composition root Z0, A1; ``03`` §E.2).

Инфраструктурный модуль без зависимостей от ``main``/``wiring``: вынесен, чтобы
``package_commands`` мог использовать коды выхода и :class:`CliError` без импорта из
``core.cli.main`` (разрыв циклической связки вывод/ошибки). Ошибки несут **код
инварианта/правила** (``I2b: …``, ``S11: …``) — ссылку на регламент.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

import re
from collections.abc import Sequence

__all__ = [
    "EXIT_OK",
    "EXIT_ERROR",
    "EXIT_USAGE",
    "EXIT_BLOCKED",
    "EXIT_NOT_FOUND",
    "FALLBACK_CODE",
    "CliError",
    "LintFailure",
    "extract_code",
]

# -- Коды возврата ---------------------------------------------------------- #
#: Успех.
EXIT_OK = 0
#: Непредвиденная ошибка исполнения.
EXIT_ERROR = 1
#: Ошибка входа/конфигурации: блокирующий линт, валидация, неразобранные аргументы.
EXIT_USAGE = 2
#: Прогон остановлен на гейте (нужно человеческое решение, ``03`` §E.2).
EXIT_BLOCKED = 3
#: Запрошенный объект не найден (например, ``source_id`` не резолвится).
EXIT_NOT_FOUND = 4

#: Код по умолчанию для ошибок без распознанного инварианта/правила.
FALLBACK_CODE = "E"

#: ``CODE: message`` — сообщения линтера/инвариантов начинаются с кода правила.
_CODE_RE = re.compile(r"^\s*([^\s:]+):")
#: Код инварианта внутри текста (например, ``I2b``).
_INVARIANT_RE = re.compile(r"\bI\d+[a-z]?\b")
#: Ссылка на раздел регламента (например, ``§4.2``/``§4.2/I2b``).
_SECTION_RE = re.compile(r"§[\d.]+(?:/[^\s:,;]+)?")


def extract_code(message: str) -> str:
    """Извлекает код инварианта/правила из сообщения (для ``CODE: message``).

    Порядок попыток: ведущий ``CODE:`` (сообщения линтера), затем инвариант вида
    ``I2b`` в тексте (сообщения домена), затем ссылка ``§4.2``. Если ничего не найдено —
    :data:`FALLBACK_CODE`.
    """
    text = (message or "").strip()
    if not text:
        return FALLBACK_CODE
    match = _CODE_RE.match(text)
    if match and len(match.group(1)) <= 32:
        return match.group(1)
    for pattern in (_INVARIANT_RE, _SECTION_RE):
        found = pattern.search(text)
        if found:
            return found.group(0)
    return FALLBACK_CODE


class CliError(Exception):
    """Ошибка CLI с кодом инварианта/правила и кодом возврата.

    Сообщение печатается как ``CODE: message`` в stderr (или в JSON при ``--json``).
    """

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        exit_code: int = EXIT_ERROR,
    ) -> None:
        super().__init__(message)
        self.code = code or extract_code(message)
        self.exit_code = exit_code


class LintFailure(CliError):
    """Блокирующие нарушения линтера входов (``03`` §D.3/§E.2).

    ``blocking``/``warnings`` сохраняются раздельно: CLI печатает блокирующие в stderr
    (каждое сообщение уже несёт код правила), предупреждения — отдельной строкой.
    """

    def __init__(self, blocking: Sequence[str], warnings: Sequence[str] = ()) -> None:
        super().__init__(
            "блокирующие нарушения линтера", code="lint", exit_code=EXIT_USAGE
        )
        self.blocking = list(blocking)
        self.warnings = list(warnings)
