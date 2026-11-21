"""Локальные инструменты оценщика: адресация уже собранного материала (T1/T2/T4/T7).

Что здесь исполняется. Модель просит вызвать инструмент по имени; вызов исполняется
**ядром** (этот модуль), читает корпус прогона через узкое окно
:class:`core.ports.tools.ToolCorpusPort` и возвращает текст. Никакой сети, никакой записи,
никакого второго LLM: инструмент умеет только адресовать то, что уже извлечено и
проиндексировано (I18/I19 остаются структурными).

Четыре объявленных инструмента и класс неоднозначности, который снимает каждый:

* ``expand_to_section`` (T1) — граница раздела, а не окрестность по ``chunk_no``: оговорка
  стоит в конце раздела, а цитата вырвана из середины;
* ``section_role`` (T2) — роль раздела по заголовку (``limitation``/``planned``/
  ``deprecated``/``comparison``/``reference``/``howto``) **с названным маркером**:
  «Planned features» прочитано как «есть» — ошибка не о нехватке текста, а о метке;
* ``table_row`` (T4) — строка таблицы с шапкой и номерами строк: «есть в матрице
  обновлений» читается как «сертифицировано», если не видно, из какого столбца взято;
* ``subject_mention`` (T7) — упоминает ли цитата субъект параметра: термины каталога и
  значимые слова семантики дословно, плюс маркер отрицания/ограничителя.

Общие правила, и они не косметика:

* **Никогда не исключение.** Неизвестное имя, аргументы не по схеме, неизвестный адрес —
  ``ToolOutcome(ok=False, reason=…)``. Модель обязана увидеть причину и продолжить:
  исключение сделало бы поведение ячейки функцией того, что модель попросила.
* **Бюджет ответа объявлен и обрезка видима** (A3). У каждого инструмента —
  константа :data:`TOOL_CHAR_BUDGETS`. При превышении текст режется, ``meta["truncated"]``
  и ``meta["total_chars"]`` публикуются, в конце текста остаётся строка об обрезке, а
  ``reason`` называет бюджет. Тихая обрезка запрещена: она выглядела бы как «материал
  кончился».
* **Только объявленное.** :meth:`_LocalTools.specs` отдаёт инструменты профиля в
  детерминированном порядке объявления; ``invoke`` отказывает на инструменте, которого
  нет в профиле, — иначе ``run_id`` описывал бы набор, который не исполнялся.
* **Инструмент не создаёт доказательств.** Возвращённый текст — материал; цитата
  по-прежнему обязана быть подстрокой текста фрагмента (I22).

Нейтральность (§1.3): ни одного предметного термина. Термины параметра приходят **данными**
(``CatalogRow.concept_terms``/``semantics``), лексикон ролей — данными модуля
:mod:`core.pipeline.section_lexicon`; в коде предметных знаний нет.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import replace
from functools import cache
from typing import Any

from core.domain.models import CatalogRow
from core.domain.tools import ToolInvocation, ToolOutcome, ToolProfile, ToolSpec
from core.pipeline.prompt import _render_chunk
from core.pipeline.section_lexicon import UNKNOWN_ROLE, classify_heading
from core.pipeline.tool_corpus import HEADING_PATH_SEP
from core.ports.tools import LocalToolPort, ToolCorpusPort

__all__ = [
    "TOOL_CHAR_BUDGETS",
    "TOOL_NAMES",
    "build_local_tools",
]

#: Объявленный бюджет ответа каждого инструмента, в символах.
#:
#: Числа — объявленное условие ответа, а не предохранитель (A3): они публикуются вместе с
#: ``meta["truncated"]``, поэтому «модель увидела меньше» — факт, а не догадка. Ориентир:
#: один фрагмент корпуса — около 1500 символов (бюджет куска), поэтому
#:
#: * ``expand_to_section`` — 6000: около четырёх фрагментов, то есть обычный раздел;
#: * ``table_row`` — 4000: шапка + диапазон строк куска таблицы;
#: * ``subject_mention`` — 3000: списки терминов параметра (пилот: до ~20 терминов);
#: * ``section_role`` — 1200: метка и маркер, ответ заведомо короче.
TOOL_CHAR_BUDGETS: Mapping[str, int] = {
    "expand_to_section": 6000,
    "section_role": 1200,
    "table_row": 4000,
    "subject_mention": 3000,
}

#: Имена инструментов в порядке объявления (детерминированный порядок ``specs()``).
TOOL_NAMES: tuple[str, ...] = tuple(TOOL_CHAR_BUDGETS)

#: Объявленные описания и схемы аргументов. Схема — JSON Schema **объекта** аргументов
#: (тот же вид, что ``tools[].function.parameters`` у OpenAI-совместимого API).
_SPECS: Mapping[str, ToolSpec] = {
    "expand_to_section": ToolSpec(
        name="expand_to_section",
        description=(
            "Вернуть весь раздел документа, в котором лежит фрагмент: путь заголовка, "
            "число фрагментов и их текст по порядку с адресами. Это адрес и текст уже "
            "собранного материала, а не доказательство: цитата по-прежнему обязана быть "
            "подстрокой текста фрагмента."
        ),
        parameters={
            "type": "object",
            "properties": {
                "chunk_id": {
                    "type": "string",
                    "description": "адрес фрагмента (source_id из шапки фрагмента)",
                }
            },
            "required": ["chunk_id"],
            "additionalProperties": False,
        },
    ),
    "section_role": ToolSpec(
        name="section_role",
        description=(
            "Назвать роль раздела по его заголовку (limitation, planned, deprecated, "
            "comparison, reference, howto) и маркер, на котором стоит метка. Метка "
            "описывает назначение раздела, а не утверждает наличие свойства."
        ),
        parameters={
            "type": "object",
            "properties": {
                "chunk_id": {
                    "type": "string",
                    "description": "адрес фрагмента (source_id из шапки фрагмента)",
                }
            },
            "required": ["chunk_id"],
            "additionalProperties": False,
        },
    ),
    "table_row": ToolSpec(
        name="table_row",
        description=(
            "Вернуть таблицу, из которой взят фрагмент: подпись, шапку столбцов и строки "
            "адресованного диапазона с номерами строк. Нужен, когда без шапки непонятно, "
            "к какому столбцу относится значение."
        ),
        parameters={
            "type": "object",
            "properties": {
                "chunk_id": {
                    "type": "string",
                    "description": "адрес фрагмента (source_id из шапки фрагмента)",
                }
            },
            "required": ["chunk_id"],
            "additionalProperties": False,
        },
    ),
    "subject_mention": ToolSpec(
        name="subject_mention",
        description=(
            "Проверить дословно: какие термины параметра присутствуют в цитате, каких нет "
            "и есть ли в ней маркер отрицания или ограничителя. Ответ — список терминов, "
            "а не суждение о том, относится ли цитата к параметру."
        ),
        parameters={
            "type": "object",
            "properties": {
                "quote": {
                    "type": "string",
                    "description": "цитата, которую собирается предъявить оценщик",
                }
            },
            "required": ["quote"],
            "additionalProperties": False,
        },
    ),
}

#: Слово семантики параметра: буквы, не короче четырёх. Цифры и знаки препинания в
#: термин не входят — они не различают субъект. Для письменностей без пробелов (CJK)
#: извлечение слов этим шаблоном не работает: там термины приходят из
#: ``CatalogRow.concept_terms`` (данные), а не из прозы семантики.
_SEMANTICS_WORD_RE = re.compile(r"[^\W\d_]{4,}")

#: Служебные слова, которые не называют субъект (ru/en). Это языковые данные, а не
#: предметные: список расширяется по мере появления корпусов, логика не меняется.
_SEMANTICS_STOPWORDS: frozenset[str] = frozenset(
    {
        "который",
        "которая",
        "которые",
        "которых",
        "этого",
        "этому",
        "этот",
        "этом",
        "также",
        "более",
        "менее",
        "может",
        "могут",
        "быть",
        "было",
        "были",
        "если",
        "чтобы",
        "через",
        "между",
        "после",
        "перед",
        "является",
        "являются",
        "своего",
        "своей",
        "всех",
        "того",
        "этих",
        "либо",
        "that",
        "this",
        "these",
        "those",
        "which",
        "their",
        "there",
        "with",
        "from",
        "have",
        "been",
        "were",
        "when",
        "what",
        "into",
        "than",
        "then",
        "also",
        "such",
        "does",
        "only",
    }
)


class _BadArguments(ValueError):
    """Аргументы вызова не по объявленной схеме (превращается в ``ok=False``)."""


@cache
def _denial_marker_re() -> re.Pattern[str]:
    """Набор маркеров отрицания/ограничителя — **переиспользуется**, а не дублируется.

    Источник — :data:`core.pipeline.evaluate._DENIAL_MARKER_RE` (тот же, чем I2 отличает
    цитату отрицания: замер 19.11.2026 показал, зачем в нём «только», «не могут»,
    «does not <глагол>»). Второй набор разошёлся бы с правилом при первой правке, и
    инструмент называл бы ограничителем не то, что считает правило.

    Импорт ленивый: по плану 28 §9.2 цикл ячейки живёт в ``evaluate``, и он импортирует
    этот модуль. Импорт на уровне модуля дал бы циклический граф, а ``getattr``-обход
    спрятал бы ошибку имени. К моменту вызова оба модуля уже загружены целиком.
    """
    from core.pipeline.evaluate import _DENIAL_MARKER_RE

    return _DENIAL_MARKER_RE


def _string_argument(arguments: Mapping[str, Any], key: str, tool: str) -> str:
    """Обязательный строковый аргумент по схеме инструмента (иначе ``_BadArguments``)."""
    if key not in arguments:
        raise _BadArguments(f"{tool}: нет обязательного аргумента {key!r}")
    extra = sorted(set(arguments) - {key})
    if extra:
        raise _BadArguments(
            f"{tool}: неизвестные аргументы {extra} — схема принимает только {key!r}"
        )
    value = arguments[key]
    if not isinstance(value, str) or not value.strip():
        raise _BadArguments(
            f"{tool}: аргумент {key!r} обязан быть непустой строкой, "
            f"получено {type(value).__name__}"
        )
    return value.strip()


def _param_terms(param: CatalogRow) -> tuple[str, ...]:
    """Термины параметра: ``concept_terms`` по всем языкам + значимые слова семантики.

    Порядок детерминирован: языки — по алфавиту, термины — в порядке объявления, затем
    слова семантики в порядке появления. Дубликаты снимаются без учёта регистра, но
    сохраняется первое написание — оно и печатается.
    """
    terms: list[str] = []
    seen: set[str] = set()

    def add(raw: str) -> None:
        term = " ".join(str(raw).split())
        if not term:
            return
        key = term.casefold()
        if key in seen:
            return
        seen.add(key)
        terms.append(term)

    for language in sorted(param.concept_terms):
        for term in param.concept_terms[language]:
            add(term)
    for match in _SEMANTICS_WORD_RE.finditer(param.semantics or ""):
        word = match.group(0)
        if word.casefold() in _SEMANTICS_STOPWORDS:
            continue
        add(word)
    return tuple(terms)


def _limitation_markers(text: str) -> tuple[str, ...]:
    """Сработавшие маркеры отрицания/ограничителя — как записаны в правиле, по алфавиту."""
    found = {match.group(0).casefold() for match in _denial_marker_re().finditer(text)}
    return tuple(sorted(found))


def _quote_list(values: Sequence[str]) -> str:
    """Список терминов для ответа модели: ``«a», «b»`` (пусто — прочерк)."""
    return ", ".join(f"«{value}»" for value in values) if values else "—"


def _fit(tool: str, content: str, meta: Mapping[str, Any]) -> ToolOutcome:
    """Собрать результат с объявленным бюджетом: обрезка **видимая**, а не тихая (A3).

    ``meta["complete"]`` — «ответ содержит весь собранный набор»; он же ``not truncated``,
    но назван отдельно, потому что вопрос «полон ли набор» задаётся аудитом, а не только
    длиной текста.
    """
    budget = TOOL_CHAR_BUDGETS[tool]
    total = len(content)
    if total <= budget:
        return ToolOutcome(
            name=tool,
            content=content,
            meta={
                **meta,
                "total_chars": total,
                "truncated": False,
                "complete": True,
            },
        )
    marker = (
        f"\n[…обрезано: показано {budget} из {total} символов; "
        f"бюджет инструмента {tool} — {budget}]"
    )
    visible = content[: max(0, budget - len(marker))] + marker
    return ToolOutcome(
        name=tool,
        content=visible,
        meta={
            **meta,
            "total_chars": total,
            "truncated": True,
            "complete": False,
        },
        reason=f"ответ обрезан по бюджету {tool}: {budget} из {total} символов",
    )


class _LocalTools:
    """Реализация :class:`core.ports.tools.LocalToolPort` для одной ячейки.

    ``param`` нужен только ``subject_mention``: термины и семантика **текущего** параметра.
    Инструменты не знают о других параметрах и не читают каталог.
    """

    def __init__(
        self, *, profile: ToolProfile, corpus: ToolCorpusPort, param: CatalogRow
    ) -> None:
        self._profile = profile
        self._corpus = corpus
        self._param = param

    def specs(self) -> tuple[ToolSpec, ...]:
        """Инструменты профиля в порядке объявления (пустой кортеж — профиль пуст)."""
        return tuple(spec for name, spec in _SPECS.items() if name in self._profile.tools)

    def invoke(self, invocation: ToolInvocation) -> ToolOutcome:
        """Исполнить вызов; любая ошибка — ``ok=False`` с причиной, не исключение."""
        name = invocation.name
        if name not in _SPECS:
            return ToolOutcome(
                name=name,
                ok=False,
                reason=(
                    f"неизвестный инструмент {name!r}: объявлены {', '.join(TOOL_NAMES)}"
                ),
            )
        if name not in self._profile.tools:
            return ToolOutcome(
                name=name,
                ok=False,
                reason=(
                    f"инструмент {name!r} не включён в профиль {self._profile.name!r}: "
                    f"включены {', '.join(sorted(self._profile.tools)) or '—'}"
                ),
            )
        try:
            if name == "expand_to_section":
                return self._expand_to_section(invocation.arguments)
            if name == "section_role":
                return self._section_role(invocation.arguments)
            if name == "table_row":
                return self._table_row(invocation.arguments)
            if name == "subject_mention":
                return self._subject_mention(invocation.arguments)
            # Объявленный инструмент без исполнения — дефект сборки, а не ответ модели;
            # он обязан быть видимым (ok=False), а не подменяться соседним обработчиком.
            raise RuntimeError(f"инструмент {name!r} объявлен, но не исполняется")
        except _BadArguments as exc:
            return ToolOutcome(name=name, ok=False, reason=str(exc))
        except Exception as exc:  # noqa: BLE001 — контракт: модель видит причину, не падение
            return ToolOutcome(
                name=name,
                ok=False,
                reason=f"внутренняя ошибка инструмента {name}: {type(exc).__name__}: {exc}",
                meta={"error": type(exc).__name__},
            )

    # -- T1 --------------------------------------------------------------- #

    def _expand_to_section(self, arguments: Mapping[str, Any]) -> ToolOutcome:
        """Весь раздел документа: путь заголовка, число фрагментов, тексты по порядку.

        Обрезка — **по границе фрагмента**, а не по символу: модели показывается целое
        число фрагментов, и ``meta["chunks_shown"]`` называет, сколько из скольких. Резать
        посреди фрагмента значило бы отдать половину предложения — ровно тот дефект,
        который инструмент и лечит.
        """
        name = "expand_to_section"
        chunk_id = _string_argument(arguments, "chunk_id", name)
        anchor = self._corpus.chunk(chunk_id)
        if anchor is None:
            return ToolOutcome(
                name=name,
                ok=False,
                reason=f"адрес {chunk_id!r} не найден в корпусе этого продукта",
            )
        section = self._corpus.section_chunks(chunk_id)
        heading = (anchor.heading_path or "").strip()
        head = [
            f"раздел: {heading}" if heading else "раздел: корень документа (заголовка нет)",
            f"фрагментов в разделе: {len(section)}",
        ]
        budget = TOOL_CHAR_BUDGETS[name]
        parts: list[str] = []
        used = len("\n\n".join(head))
        shown = 0
        for number, chunk in enumerate(section, 1):
            rendered = _render_chunk(chunk, number)
            cost = len(rendered) + 2  # разделитель между фрагментами
            if parts and used + cost > budget:
                break
            parts.append(rendered)
            used += cost
            shown += 1
        content = "\n\n".join(head + parts)
        partial = shown < len(section)
        if partial:
            content += (
                f"\n\n[…показано {shown} из {len(section)} фрагментов раздела; "
                f"бюджет инструмента {name} — {budget} символов]"
            )
        outcome = _fit(
            name,
            content,
            {
                "chunks": len(section),
                "chunks_shown": shown,
                "heading": heading,
            },
        )
        if partial and not outcome.meta.get("truncated"):
            # Резали по границе фрагментов, поэтому длина уложилась в бюджет — но набор
            # **неполон**. `_fit` судит по длине и здесь сказал бы «complete», то есть
            # аудит считал бы ответ исчерпывающим. Признак неполноты обязан пережить
            # обрезку по структуре, а не только по символам.
            return replace(
                outcome,
                meta={**outcome.meta, "truncated": True, "complete": False},
                reason=(
                    f"показано {shown} из {len(section)} фрагментов раздела "
                    f"по бюджету {name} ({budget} символов)"
                ),
            )
        return outcome

    # -- T2 --------------------------------------------------------------- #

    def _section_role(self, arguments: Mapping[str, Any]) -> ToolOutcome:
        """Роль раздела по заголовку + сработавший маркер (или ``unknown``)."""
        name = "section_role"
        chunk_id = _string_argument(arguments, "chunk_id", name)
        anchor = self._corpus.chunk(chunk_id)
        if anchor is None:
            return ToolOutcome(
                name=name,
                ok=False,
                reason=f"адрес {chunk_id!r} не найден в корпусе этого продукта",
            )
        heading = (anchor.heading_path or "").strip()
        if not heading:
            return _fit(
                name,
                "роль раздела: unknown\nмаркер: — (у фрагмента нет пути заголовков)",
                {"role": UNKNOWN_ROLE, "marker": "", "heading": "", "matched_in": ""},
            )
        section_title = heading.split(HEADING_PATH_SEP)[-1].strip() or heading
        try:
            role, marker = classify_heading(section_title)
            matched_in = "section"
            if role == UNKNOWN_ROLE and section_title != heading:
                # Родительский заголовок тоже объявляет назначение: «Ограничения > Таблица»
                # остаётся разделом об ограничениях. Проверка второго уровня видима в
                # `matched_in`, поэтому ответ не выглядит догадкой.
                role, marker = classify_heading(heading)
                matched_in = "path"
        except ValueError as exc:
            # Пустой/неполный лексикон — отказ, а не `unknown`: `unknown` обязан значить
            # «маркеров нет в заголовке».
            return ToolOutcome(name=name, ok=False, reason=str(exc), meta={"heading": heading})
        if role == UNKNOWN_ROLE:
            content = (
                "роль раздела: unknown\n"
                f"маркер: — (ни один объявленный маркер не входит в заголовок «{heading}»)"
            )
        else:
            where = "в заголовке раздела" if matched_in == "section" else "в пути заголовков"
            content = f"роль раздела: {role}\nмаркер: «{marker}» ({where}: «{heading}»)"
        return _fit(
            name,
            content,
            {"role": role, "marker": marker, "heading": heading, "matched_in": matched_in},
        )

    # -- T4 --------------------------------------------------------------- #

    def _table_row(self, arguments: Mapping[str, Any]) -> ToolOutcome:
        """Строки таблицы адресованного диапазона: подпись, шапка, номера строк."""
        name = "table_row"
        chunk_id = _string_argument(arguments, "chunk_id", name)
        anchor = self._corpus.chunk(chunk_id)
        if anchor is None:
            return ToolOutcome(
                name=name,
                ok=False,
                reason=f"адрес {chunk_id!r} не найден в корпусе этого продукта",
            )
        ref = anchor.table_ref
        if ref is None:
            return ToolOutcome(
                name=name,
                ok=False,
                reason=(
                    "фрагмент не из таблицы: у чанка нет адреса внутри таблицы "
                    "(читатель таблиц её не даёт либо сопоставление не выполнялось)"
                ),
            )
        tables = self._corpus.document_tables(chunk_id)
        if tables is None:
            return ToolOutcome(
                name=name,
                ok=False,
                reason=(
                    "читатель таблиц этого документа их не даёт "
                    "(corpus_docs.tables пуст как «не измерено», а не как «таблиц нет»)"
                ),
                meta={"table_id": ref.table_id},
            )
        if not tables:
            return ToolOutcome(
                name=name,
                ok=False,
                reason="в документе нет таблиц: читатель искал и не нашёл ни одной",
                meta={"table_id": ref.table_id},
            )
        table = next((item for item in tables if item.table_id == ref.table_id), None)
        if table is None:
            return ToolOutcome(
                name=name,
                ok=False,
                reason=(
                    f"таблица {ref.table_id!r} не найдена в документе "
                    f"(есть: {', '.join(item.table_id for item in tables) or '—'})"
                ),
                meta={"table_id": ref.table_id},
            )
        if ref.row_start is None or ref.row_end is None:
            selected: tuple[tuple[str, ...], ...] = ()
            first_row = 1
        else:
            if ref.row_start < 1 or ref.row_end > len(table.rows):
                return ToolOutcome(
                    name=name,
                    ok=False,
                    reason=(
                        f"адрес строк {ref.row_start}..{ref.row_end} вне таблицы "
                        f"{table.table_id!r}: в ней {len(table.rows)} строк данных"
                    ),
                    meta={"table_id": table.table_id, "rows_total": len(table.rows)},
                )
            selected = table.rows[ref.row_start - 1 : ref.row_end]
            first_row = ref.row_start
        lines = [f"таблица: {table.table_id}"]
        if table.page_no is not None:
            lines.append(f"страница: {table.page_no}")
        if table.caption:
            lines.append(f"подпись: {table.caption}")
        if table.header:
            cells = " | ".join(f"{index}) {cell}" for index, cell in enumerate(table.header, 1))
            lines.append(f"шапка: {cells}")
        else:
            lines.append("шапка: — (читатель шапку не дал)")
        if not selected:
            lines.append(
                "строки: — (фрагмент содержит только шапку таблицы, строк данных в нём нет)"
            )
        else:
            for offset, row in enumerate(selected):
                number = first_row + offset
                lines.append(f"строка {number}: {' | '.join(row)}")
        return _fit(
            name,
            "\n".join(lines),
            {
                "table_id": table.table_id,
                "rows": len(selected),
                "rows_total": len(table.rows),
                "header": len(table.header),
                "page_no": table.page_no,
            },
        )

    # -- T7 --------------------------------------------------------------- #

    def _subject_mention(self, arguments: Mapping[str, Any]) -> ToolOutcome:
        """Термины параметра в цитате дословно: что есть, чего нет, есть ли ограничитель."""
        name = "subject_mention"
        quote = _string_argument(arguments, "quote", name)
        terms = _param_terms(self._param)
        folded = quote.casefold()
        present = [term for term in terms if term.casefold() in folded]
        absent = [term for term in terms if term not in present]
        markers = _limitation_markers(quote)
        if not terms:
            content = (
                "термины параметра: — (у параметра нет объявленных терминов: "
                "concept_terms пуст, семантика не заполнена)"
            )
        else:
            content = "\n".join(
                [
                    f"термины параметра в цитате: {_quote_list(present)} "
                    f"({len(present)} из {len(terms)})",
                    f"термины параметра, которых в цитате нет: {_quote_list(absent)}",
                ]
            )
        if markers:
            content += f"\nмаркеры отрицания/ограничителя в цитате: {_quote_list(list(markers))}"
        else:
            content += "\nмаркеры отрицания/ограничителя в цитате: —"
        return _fit(
            name,
            content,
            {
                "terms": len(terms),
                "present": len(present),
                "absent": len(absent),
                "markers": list(markers),
            },
        )


def build_local_tools(
    *, profile: ToolProfile, corpus: ToolCorpusPort, param: CatalogRow
) -> LocalToolPort:
    """Собрать инструменты профиля ячейки (``param`` — только для ``subject_mention``).

    :raises ValueError: профиль называет инструмент, которого нет в объявленном наборе.
        Отказ, а не пропуск: профиль — условие измерения (он входит в ``run_id``), и
        молча выброшенное имя означало бы идентификатор, описывающий набор, который не
        исполнялся. Пустой профиль ошибкой не является — это объявленный путь без
        инструментов.
    """
    unknown = [name for name in profile.tools if name not in _SPECS]
    if unknown:
        raise ValueError(
            f"ToolProfile {profile.name!r}: неизвестные инструменты {unknown}; "
            f"объявлены {', '.join(TOOL_NAMES)}"
        )
    return _LocalTools(profile=profile, corpus=corpus, param=param)
