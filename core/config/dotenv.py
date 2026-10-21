"""coHound — минимальный парсер dotenv-файла (без внешних зависимостей).

CLI historically **не** загружал ``.env``: оператор обязан был экспортировать переменные
в среду процесса (``set -a; . ./.env; set +a``). Забытый экспорт молча менял поведение
(например, терялся ``COHOUND_SCOUT_CRAWLER=scrapy``, и краул деградировал на ``http``).
Модуль даёт маленький предсказуемый парсер и **явный шов инъекции**
(:func:`apply_dotenv`) — никакой глобальной мутации из парсера и никаких side-effect'ов
на импорте (``03`` §D.6).

Поддерживается подмножество dotenv, достаточное для операционных файлов проекта:

* ``KEY=VALUE`` и ``export KEY=VALUE`` (ведущий ``export`` отбрасывается);
* пустые строки и комментарии: whole-line ``# …`` и trailing ``… # …`` — только после
  **непробельного** разделителя у некавыченных значений (``KEY=a#b`` остаётся ``a#b``);
* одинарные кавычки — литерал без раскрытия escape'ов;
* двойные кавычки — раскрываются ``\\n``, ``\\t``, ``\\\\``, ``\\"`` (прочие escape'ы
  сохраняются как есть); после закрывающей кавычки допустим только комментарий;
* некавыченное значение обрезается по trailing-whitespace;
* ``\\r\\n``/``\\r``/``\\n``, BOM (UTF-8 с BOM читается как ``utf-8-sig``), UTF-8.

Некорректная строка **не** прерывает разбор и **не** проглатывается: она попадает в
:attr:`DotenvResult.diagnostics` (видимая диагностика с номером строки), а остальные
строки применяются.

Переменные, уже присутствующие в среде процесса, **не переопределяются** (окружение
оператора/секрет-хранилища приоритетнее файла). Слои резолва файла и opt-out —
:func:`resolve_env_file`/:func:`dotenv_disabled`.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass, field
from pathlib import Path

__all__ = [
    "DEFAULT_DOTENV_FILENAME",
    "ENV_DOTENV_FILE",
    "ENV_NO_DOTENV",
    "DotenvResult",
    "apply_dotenv",
    "dotenv_disabled",
    "parse_dotenv",
    "resolve_env_file",
]

#: Явный путь к dotenv-файлу (средний приоритет; флаг ``--env-file`` выше).
ENV_DOTENV_FILE = "COHOUND_ENV_FILE"

#: Opt-out автозагрузки ``.env`` (``1``/``true``/``yes``/``on``).
ENV_NO_DOTENV = "COHOUND_NO_DOTENV"

#: Имя dotenv-файла по умолчанию (относительно CWD).
DEFAULT_DOTENV_FILENAME = ".env"

#: Значения, считающиеся включённым opt-out (регистронезависимо).
_TRUTHY: frozenset[str] = frozenset({"1", "true", "yes", "on"})

#: Ключ dotenv: классическая POSIX-форма (буква/подчёркивание + буквы/цифры/подчёркивания).
_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

#: Раскрываемые escape-последовательности в двойных кавычках (прочие сохраняются).
_ESCAPES: dict[str, str] = {"n": "\n", "t": "\t", "\\": "\\", '"': '"'}


@dataclass(frozen=True)
class DotenvResult:
    """Результат разбора/загрузки dotenv.

    :param values: все корректно разобранные пары ``KEY → VALUE`` (в порядке файла).
    :param applied: ключи, реально вставленные в среду (не переопределяющие) —
        пусто у :func:`parse_dotenv`.
    :param diagnostics: человекочитаемые предупреждения с номером строки; пусто —
        файл корректен.
    """

    values: Mapping[str, str] = field(default_factory=dict)
    applied: tuple[str, ...] = ()
    diagnostics: tuple[str, ...] = ()


def dotenv_disabled(environ: Mapping[str, str] | None = None) -> bool:
    """Включён ли opt-out автозагрузки (``COHOUND_NO_DOTENV=1``; ``0``/пусто — нет)."""
    env = os.environ if environ is None else environ
    return (env.get(ENV_NO_DOTENV) or "").strip().lower() in _TRUTHY


def resolve_env_file(
    explicit: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> tuple[Path, str]:
    """Резолвит путь dotenv-файла: ``--env-file`` → ``COHOUND_ENV_FILE`` → ``./.env``.

    :param explicit: значение флага ``--env-file`` (``None``/пусто — не задан).
    :param environ: среда для чтения ``COHOUND_ENV_FILE`` (по умолчанию — ``os.environ``).
    :returns: ``(path, origin)``, где ``origin`` ∈ ``{"flag", "env", "default"}``.
        Существование файла здесь **не** проверяется: решение «нет файла — не ошибка»
        принимает composition root (для явных источников диагностика, для дефолта — тишина).
    """
    env = os.environ if environ is None else environ
    if explicit is not None and str(explicit).strip():
        return Path(str(explicit).strip()).expanduser(), "flag"
    raw = (env.get(ENV_DOTENV_FILE) or "").strip()
    if raw:
        return Path(raw).expanduser(), "env"
    return Path(DEFAULT_DOTENV_FILENAME), "default"


def _parse_quoted(
    text: str, *, lineno: int
) -> tuple[str | None, str | None]:
    """Разбирает значение в кавычках (``text[0]`` — кавычка).

    :returns: ``(value, error)``; ровно одно из полей не ``None``.
    """
    quote = text[0]
    if quote == "'":
        end = text.find("'", 1)
        if end < 0:
            return None, f"строка {lineno}: незакрытая одинарная кавычка"
        value = text[1:end]
        rest = text[end + 1 :].lstrip()
        if rest and not rest.startswith("#"):
            return None, f"строка {lineno}: мусор после закрывающей кавычки: {rest!r}"
        return value, None

    out: list[str] = []
    index = 1
    while index < len(text):
        char = text[index]
        if char == '"':
            rest = text[index + 1 :].lstrip()
            if rest and not rest.startswith("#"):
                return None, f"строка {lineno}: мусор после закрывающей кавычки: {rest!r}"
            return "".join(out), None
        if char == "\\" and index + 1 < len(text):
            nxt = text[index + 1]
            if nxt in _ESCAPES:
                out.append(_ESCAPES[nxt])
            else:
                # Неизвестный escape сохраняем буквально (не теряем данные оператора).
                out.append("\\")
                out.append(nxt)
            index += 2
            continue
        out.append(char)
        index += 1
    return None, f"строка {lineno}: незакрытая двойная кавычка"


def _parse_value(
    raw: str, *, lineno: int
) -> tuple[str | None, str | None]:
    """Разбирает правую часть ``KEY=<raw>`` (кавычки/escape'ы/комментарии/обрезка)."""
    text = raw.lstrip()
    if not text:
        return "", None
    if text[0] in {"'", '"'}:
        return _parse_quoted(text, lineno=lineno)
    # Некавыченное значение: trailing-комментарий — только через whitespace.
    comment = re.search(r"\s#", text)
    if comment is not None:
        text = text[: comment.start()]
    return text.rstrip(), None


def parse_dotenv(text: str) -> DotenvResult:
    """Разбирает содержимое dotenv в :class:`DotenvResult` (без записи в среду).

    Некорректные строки попадают в ``diagnostics``; корректные до/после них —
    в ``values``. Дубликат ключа: побеждает последнее значение (как в оболочке).
    """
    values: dict[str, str] = {}
    diagnostics: list[str] = []
    # BOM допускается как в начале файла, так и в уже декодированной строке.
    for lineno, raw_line in enumerate(text.lstrip("\ufeff").splitlines(), start=1):
        body = raw_line.lstrip()
        if not body or body.startswith("#"):
            continue
        # ``export KEY=...``: отбрасываем только самостоятельное слово ``export``.
        prefix_len = len("export")
        if (
            body.startswith("export")
            and len(body) > prefix_len
            and body[prefix_len].isspace()
        ):
            body = body[prefix_len:].lstrip()
        key_part, separator, value_part = body.partition("=")
        if not separator:
            diagnostics.append(f"строка {lineno}: нет разделителя '='")
            continue
        key = key_part.strip()
        if not _KEY_RE.match(key):
            diagnostics.append(f"строка {lineno}: недопустимый ключ {key!r}")
            continue
        value, error = _parse_value(value_part, lineno=lineno)
        if error is not None:
            diagnostics.append(error)
            continue
        values[key] = value if value is not None else ""
    return DotenvResult(values=values, diagnostics=tuple(diagnostics))


def apply_dotenv(
    path: str | Path, environ: MutableMapping[str, str]
) -> DotenvResult:
    """Загружает ``path`` в ``environ``, **не переопределяя** уже заданные ключи.

    Явный шов инъекции: ни парсер, ни этот вызов не трогают ``os.environ`` сами —
    вызывающий передаёт среду (CLI-вход передаёт ``os.environ``, тесты — словарь).
    Отсутствующий/нечитаемый файл не поднимает исключение, а возвращает диагностику
    (решение о фатальности — у composition root).

    :returns: :class:`DotenvResult`; ``values`` — всё разобранное, ``applied`` — реально
        вставленные ключи (пересечение с ``environ`` исключено).
    """
    target = Path(path)
    try:
        text = target.read_text(encoding="utf-8-sig")
    except OSError as exc:
        return DotenvResult(
            diagnostics=(f"{target}: не удалось прочитать файл: {exc}",)
        )
    except UnicodeDecodeError as exc:  # pragma: no cover - utf-8-sig почти всегда декодируется
        return DotenvResult(diagnostics=(f"{target}: файл не в UTF-8: {exc}",))
    parsed = parse_dotenv(text)
    applied: list[str] = []
    for key, value in parsed.values.items():
        if key in environ:
            continue
        environ[key] = value
        applied.append(key)
    return DotenvResult(
        values=parsed.values,
        applied=tuple(applied),
        diagnostics=parsed.diagnostics,
    )
