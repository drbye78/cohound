"""coHound — точечная правка корней сателлитов в тексте паспорта (§4.3).

Зачем отдельный модуль, а не ``yaml.safe_dump``. Паспорт — **авторский** артефакт:
шапка и комментарии внутри него несут обоснование (чем измерен размер корня, почему
выбран этот префикс, какая цитата подтверждает сателлита), а ``passports_digest``
считается по байтам файла. Полная перезапись через PyYAML стёрла бы эти комментарии —
потеря обоснования это не косметика (§1.2: документ без источника — гипотеза).

Поэтому правка строчная и узкая: меняются **только** строки блока ``doc_domains:``
внутри нужного элемента списка ``satellites:``. Всё остальное возвращается дословно, и
повторное применение к уже пропатченному тексту не меняет ни байта (идемпотентность).

Границы модуля: он не решает, **какие** корни объявлять (это
:mod:`core.pipeline.satellite_roots` и человек), и не валидирует доменную модель
(после записи паспорт всё равно читается :func:`core.config.passports.load_passport`).

Нейтрален к предметной категории (§1.3): роли и корни — данные.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from core.pipeline.package_io import _yaml_scalar

__all__ = [
    "SatelliteRootBlock",
    "patch_satellite_roots",
    "satellite_root_blocks",
]

#: Строка верхнеуровневого ключа ``satellites:`` (без отступа, возможно с хвостовым
#: комментарием — он не часть значения).
_SATELLITES_KEY_RE = re.compile(r"^satellites:\s*(?:#.*)?$")

#: Начало элемента списка: ``- role: …`` на любом отступе.
_ITEM_START_RE = re.compile(r"^(?P<indent>[ \t]*)-[ \t]")

#: Строка поля ``role:`` внутри элемента. Роль может стоять как отдельным полем
#: (``    role: x``), так и прямо в строке начала элемента (``  - role: x``) — второй
#: вариант и есть авторский стиль паспортов, поэтому префикс ``- `` разбирается явно.
_ROLE_RE = re.compile(
    r"^(?P<indent>[ \t]*)(?P<dash>-[ \t]+)?role:[ \t]*(?P<value>.*?)[ \t]*(?:#.*)?$"
)

#: Строка поля ``doc_domains:`` внутри элемента.
_DOC_DOMAINS_RE = re.compile(r"^(?P<indent>[ \t]*)doc_domains:[ \t]*(?:#.*)?$")


@dataclass(frozen=True)
class SatelliteRootBlock:
    """Позиция элемента списка ``satellites`` в тексте паспорта (номера строк 0-based).

    ``start`` — строка ``- role: …``; ``end`` — первая строка **после** элемента
    (исключающая граница). ``field_indent`` — отступ полей элемента (``    `` при
    ``  - role:``): по нему вставляется ``doc_domains``.
    """

    role: str
    start: int
    end: int
    field_indent: str
    doc_domains: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {
            "role": self.role,
            "start": self.start,
            "end": self.end,
            "doc_domains": list(self.doc_domains),
        }


def _unquote(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        return text[1:-1]
    return text


def _satellites_span(lines: Sequence[str]) -> tuple[int, int] | None:
    """Возвращает ``(start, end)`` блока ``satellites:`` (номера строк, ``end`` — за блоком)."""
    start: int | None = None
    for index, line in enumerate(lines):
        if _SATELLITES_KEY_RE.match(line.rstrip("\r\n")):
            start = index
            break
    if start is None:
        return None
    end = len(lines)
    for index in range(start + 1, len(lines)):
        stripped = lines[index].strip()
        if stripped == "" or stripped.startswith("#"):
            continue
        if lines[index][:1] not in (" ", "\t"):
            end = index
            break
    return start, end


def satellite_root_blocks(raw: str) -> list[SatelliteRootBlock]:
    """Разбирает элементы ``satellites:`` без разбора всего YAML.

    Возвращает блоки в порядке файла. Элемент без ``role:`` пропускается (это дефект
    паспорта, и его назовёт загрузчик — здесь молча не угадываем). ``doc_domains``
    читается как перечень ``- …`` строк под ключом элемента.
    """
    lines = raw.splitlines(keepends=True)
    span = _satellites_span(lines)
    if span is None:
        return []
    start, end = span

    item_starts: list[int] = []
    # Отступ элементов списка задаёт **первый** элемент. Без этой привязки вложенный
    # перечень (``doc_domains:`` → ``- корень``) читался бы как ещё один сателлит.
    item_indent: str | None = None
    for index in range(start + 1, end):
        match = _ITEM_START_RE.match(lines[index])
        if match is None:
            continue
        indent = match.group("indent")
        if item_indent is None:
            item_indent = indent
        if indent != item_indent:
            continue
        item_starts.append(index)

    blocks: list[SatelliteRootBlock] = []
    for position, item_start in enumerate(item_starts):
        item_end = item_starts[position + 1] if position + 1 < len(item_starts) else end
        role: str | None = None
        field_indent = ""
        for index in range(item_start, item_end):
            role_match = _ROLE_RE.match(lines[index].rstrip("\r\n"))
            if role_match is not None:
                role = _unquote(role_match.group("value"))
                # Поля элемента стоят на отступ больше начала строки, если роль пришла
                # в строке ``- role:`` (``- `` — два символа).
                dash = role_match.group("dash") or ""
                field_indent = role_match.group("indent") + ("  " if dash else "")
                break
        if role is None:
            continue
        domains: list[str] = []
        for index in range(item_start, item_end):
            if _DOC_DOMAINS_RE.match(lines[index].rstrip("\r\n")) is None:
                continue
            for follower in range(index + 1, item_end):
                text = lines[follower].rstrip("\r\n")
                stripped = text.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                if text[:1] not in (" ", "\t"):
                    break
                item_match = re.match(r"^[ \t]*-[ \t]+(?P<value>.+)$", text)
                if item_match is None:
                    break
                domains.append(_unquote(item_match.group("value")))
            break
        blocks.append(
            SatelliteRootBlock(
                role=role,
                start=item_start,
                end=item_end,
                field_indent=field_indent,
                doc_domains=tuple(domains),
            )
        )
    return blocks


def _render_doc_domains(domains: Sequence[str], indent: str, newline: str) -> list[str]:
    """Строки блока ``doc_domains`` в стиле паспорта (одна запись — одна строка)."""
    if not domains:
        return [f"{indent}doc_domains: []{newline}"]
    rendered = [f"{indent}doc_domains:{newline}"]
    rendered.extend(f"{indent}  - {_yaml_scalar(domain)}{newline}" for domain in domains)
    return rendered


def patch_satellite_roots(raw: str, updates: Mapping[str, Sequence[str]]) -> str:
    """Заменяет/вставляет ``doc_domains`` у элементов ``satellites`` по роли.

    ``updates`` — ``{role: [корень, …]}``. Роль, которой нет в паспорте, — **ошибка**
    вызывающего (её называет :func:`satellite_root_blocks`), а не тихий пропуск: иначе
    разведка корней «применилась», ничего не изменив.

    Порядок корней сохраняется как передан (он же приоритет обхода в
    :func:`core.config.passports.passport_urls`). Существующий блок ``doc_domains``
    заменяется целиком — вместе со строками-значениями, но **без** соседних полей и
    комментариев элемента.

    :raises KeyError: роль из ``updates`` не найдена среди элементов ``satellites``.
    :raises ValueError: в паспорте нет блока ``satellites:``.
    """
    lines = raw.splitlines(keepends=True)
    blocks = satellite_root_blocks(raw)
    if not blocks:
        raise ValueError("паспорт: нет блока 'satellites:' — правка корней невозможна")
    known = {block.role for block in blocks}
    missing = [role for role in updates if role not in known]
    if missing:
        raise KeyError(f"паспорт: сателлиты не найдены по ролям {missing}")
    newline = "\r\n" if any(line.endswith("\r\n") for line in lines) else "\n"

    # Правки идут от конца к началу, чтобы номера строк предыдущих блоков не сдвигались.
    for block in sorted(blocks, key=lambda item: item.start, reverse=True):
        if block.role not in updates:
            continue
        domains = [str(domain) for domain in updates[block.role]]
        if tuple(domains) == block.doc_domains:
            continue
        replacement = _render_doc_domains(domains, block.field_indent, newline)

        existing_start: int | None = None
        existing_end: int | None = None
        for index in range(block.start, block.end):
            if _DOC_DOMAINS_RE.match(lines[index].rstrip("\r\n")) is None:
                continue
            existing_start = index
            existing_end = index + 1
            while existing_end < block.end:
                text = lines[existing_end].rstrip("\r\n")
                stripped = text.strip()
                if not stripped or stripped.startswith("#"):
                    existing_end += 1
                    continue
                if text[:1] not in (" ", "\t"):
                    break
                if re.match(r"^[ \t]*-[ \t]+", text) is None:
                    break
                existing_end += 1
            break

        if existing_start is not None:
            lines[existing_start:existing_end] = replacement
            continue
        # Нет блока — вставляем в конец элемента. Отступаем назад через пустые строки и
        # комментарии **уровня списка** (отступ меньше отступа полей): такой комментарий
        # относится к следующему элементу, и вставка после него приписала бы поле чужому
        # сателлиту.
        insert_at = block.end
        while insert_at > block.start:
            text = lines[insert_at - 1]
            stripped = text.strip()
            if stripped == "":
                insert_at -= 1
                continue
            if stripped.startswith("#"):
                indent = text[: len(text) - len(text.lstrip())]
                if len(indent) < len(block.field_indent):
                    insert_at -= 1
                    continue
            break
        lines[insert_at:insert_at] = replacement
    return "".join(lines)
