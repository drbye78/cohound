"""Хеширование контента и промптов (I19, §10).

``content_hash`` — SHA-256 в полном hex (64 символа); им фиксируется внешний контент
на пути ``corpus_raw → extract → chunk → content_hash → index`` (I19). ``prompt_hash``
детерминированно связывает ячейку с версией шаблона и входными чанками (§5): смена
шаблона → новый hash → инвалидация ячеек (§13).

Модуль категорийно-нейтрален (§1.3), зависимостей на ядро не имеет.
"""

from __future__ import annotations

import hashlib

# Разделитель полей — unit separator: не встречается в версиях/правилах/chunk_id
# и делает склейку однозначной.
_FIELD_SEP = "\x1f"

__all__ = ["content_hash", "prompt_hash", "short_hash"]


def content_hash(data: bytes | str) -> str:
    """SHA-256 контента в полном hex (64 символа).

    Строки кодируются UTF-8 — ``content_hash("ж")`` и ``content_hash("ж".encode())``
    совпадают. Детерминирован для одинаковых байтов.
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def short_hash(data: bytes | str, n: int = 24) -> str:
    """Первые ``n`` hex-символов SHA-256 (стиль ``derived_evidence_id``, S8).

    По умолчанию ``n=24``. ``n`` не может быть отрицательным (``ValueError``).
    Если ``n`` больше длины дайджеста, возвращается полный дайджест.
    """
    if n < 0:
        raise ValueError("n не может быть отрицательным")
    return content_hash(data)[:n]


def prompt_hash(
    template_version: str,
    rule: str,
    absence_rule: str,
    chunk_ids: list[str],
    *,
    group: str,
    module: str,
    semantics: str = "",
    param_class: str = "",
    tool_instructions: str = "",
    table_material: str = "",
) -> str:
    """Детерминированный хэш промпта (§5): версия шаблона, правило, группа, модуль, чанки.

    ``chunk_ids`` сортируются, поэтому порядок входа не влияет на результат
    (prompt_hash инвариантен к перестановке чанков). Склейка — фиксированным
    разделителем. SHA-256 в полном hex.

    ``group``/``module`` входят в материал **обязательными** аргументами (с 19.11.2026,
    шаблон ``evaluator_v6``): они печатаются в тексте промпта (слоты ``{{GROUP}}``/
    ``{{MODULE}}``), и hash, не покрывающий их, описывал бы не тот текст — два разных
    промпта несли бы один hash. Дефолта нет намеренно: пропущенный аргумент у нового
    вызывающего — ошибка сборки, а не тихий hash «без группы».

    С 21.11.2026 (шаблон ``evaluator_v7``) материал расширен тем, что тоже печатается в
    промпте: ``semantics``/``param_class`` (авторская семантика параметра и класс),
    ``tool_instructions`` (блок инструментов) и ``table_material`` (канонический текст
    табличной шапки фрагментов, включая пометку «объект не передан»). Правило одно:
    **всё, что меняет текст промпта, обязано менять hash**, иначе два разных промпта
    несут один hash — прямой запрет проекта.

    Эти четыре аргумента имеют пустой дефолт, в отличие от ``group``/``module``: пустая
    строка — это ровно то, что печатается пустым (слот не добавляет текста), поэтому
    вызывающий, который знает только старую часть входа, получает корректный hash
    старого текста. ``compile_prompt`` передаёт их **всегда**; новый вызывающий обязан
    делать так же.
    """
    parts = [
        template_version,
        rule,
        absence_rule,
        group,
        module,
        semantics,
        param_class,
        tool_instructions,
        table_material,
        *sorted(chunk_ids),
    ]
    raw = _FIELD_SEP.join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
