"""coHound — механическая компиляция промпта оценщика Ш6 (T9.1, §7.1).

Промпт собирается **механически**: версия шаблона + правило параметра + правило
отсутствия + группа и модуль параметра + сателлиты релевантных ролей + фрагменты со
**структурной шапкой**
(``[фрагмент N]`` / ``source_id`` / ``document`` / ``heading`` / ``context`` /
``текст:`` + дословный текст, см. :func:`_render_chunk`).
Никакой генерации/перефразирования: одна и та же ячейка (при том же входе) даёт
байт-в-байт тот же промпт и тот же ``prompt_hash`` (детерминизм, R7).

**Зачем шапка (C7).** Раньше в промпт уходило ``[source: {chunk_id}]\\n{text}``: модель
видела 1500 символов без пути заголовков и без того, из какого документа они взяты.
``RetrievedChunk.heading_path`` заполнялся ретривалом и до оценщика не доходил. Теперь
шапка отделена от текста явной границей (``текст:``), поэтому:

* текст фрагмента остаётся в промпте **дословным** фрагментом — цитатная сверка
  (``validate.py``, I22) сравнивает цитату именно с чистым ``text`` чанка, и шапка в
  это сравнение не попадает;
* шапка машинно-различима (свои строки), а не слипается с текстом;
* пустые ``heading_path``/документ/контекст **не** печатаются вовсе — ни ``None``, ни
  ``[]``, ни пустых строк.

**``context_prefix``.** Контекст абзаца/заголовка печатается в шапке (``context:``), а не
приклеивается к тексту: он не входит в цитатную сверку и не должен ею считаться. Поле
есть у :class:`~core.ports.types.Chunk`; у ``RetrievedChunk`` его нет, поэтому на
retrieval-пути строка отсутствует (это видимая граница порта, а не тихий пропуск).

**Схема плейсхолдеров шаблона.** ``{{NAME}}``, где ``NAME`` — один из слотов
(``PRODUCT_ID``, ``TITLE``, ``GROUP``, ``MODULE``, ``RULE``, ``ABSENCE_RULE``,
``ABSENCE_GUIDANCE``, ``SATELLITES``, ``EXTRA_INSTRUCTIONS``, ``CHUNKS``). Подстановка —
**однопроходная** (``re.sub`` по токенам исходного шаблона), поэтому значения не
пересканируются: ``{{...}}`` внутри текста чанка остаётся литеральным. Неизвестный слот в
шаблоне — ``ValueError`` (шаблон и код рассинхронизированы — это ошибка сборки, не данных).

**``prompt_hash``.** Ровно контракт :func:`core.util.hashing.prompt_hash`:
``H(template_version, rule, absence_rule, group, module, sorted(chunk_ids))``. Сателлиты,
заголовок параметра и отрендеренный текст в hash **не входят** — такова
фиксированная формула §5/01 §5. Полный текст сохраняется в каталоге аудита
(``prompts/{sha256(text)}.json``), чтобы аудит мог сверить содержимое.

**Адрес артефакта ≠ ``prompt_hash`` (замер 06.11.2026).** Формула §5 не покрывает ни
``product_id``, ни заголовок параметра, ни текст чанков, поэтому две разные ячейки
законно получают **один** ``prompt_hash`` (в прогоне ``ru-pg-dbms-c2e7b860ac17``:
``xlsx_056`` и ``xlsx_057`` у ``jatoba``). Пока имя файла было ``{prompt_hash}.json``,
второй промпт отказывал в записи (``prompt_hash_conflict``) и в архиве оставался текст
**другой** ячейки. Формула hash — методология (§5) и здесь не меняется; исправлена
**адресация артефакта**: имя файла — ``sha256(текста)``, ``prompt_hash`` остаётся полем
внутри. Контентный адрес не может совпасть у разных текстов по построению.

**Инварианты.** I18 — оценщик работает только по переданным чанкам, промпт не
инструктирует его искать. I10/I15 — модель не генерирует URL. I7 — N/A запрещён.
Схема ответа навязывается constrained decoding'ом на адаптере (§7.1); здесь — лишь
напоминание формы.

Модуль категорийно нейтрален (§1.3), без сети, без новых зависимостей.
"""

from __future__ import annotations

import json
import re
import threading
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path
from typing import Any

from core.domain.enums import AbsenceRuleType
from core.domain.models import CatalogRow, Satellite
from core.ports.types import Chunk
from core.ports.vector_index import RetrievedChunk
from core.util.hashing import content_hash, prompt_hash

__all__ = [
    "ABSENCE_GUIDANCE",
    "PROMPT_HASH_CONFLICT",
    "PROMPT_SAVE_FAILED",
    "SATELLITE_OUT_OF_VOCABULARY_TOKEN",
    "TEMPLATES_DIR",
    "TEMPLATE_VERSION",
    "CompiledPrompt",
    "PromptArchive",
    "compile_prompt",
    "render_queries",
    "role_vocabulary_of",
    "satellite_role_note",
    "save_prompt",
]

#: Машинный токен: сателлиты переданы как контекст, потому что их роли **вне** словаря
#: ролей каталога (см. :func:`_relevant_satellites`). Строка с этим префиксом попадает в
#: ``PhaseResult.log_entries`` → ``results[].notes`` прогона: расхождение двух словарей —
#: факт о данных, и он обязан быть видимым, а не выводимым из «сателлиты не пришли».
SATELLITE_OUT_OF_VOCABULARY_TOKEN = "satellite_roles_out_of_vocabulary"  # noqa: S105 - машинный токен, не секрет

#: Машинный токен: исполненный промпт не удалось записать в каталог аудита (I/O).
PROMPT_SAVE_FAILED = "prompt_save_failed"

#: Машинный токен: файл с тем же ``prompt_hash`` уже несёт **другой** текст.
#:
#: Это не рутинный повтор: ``prompt_hash`` обязан адресовать содержимое, поэтому
#: расхождение означает, что формула hash перестала покрывать вход (напр. правка
#: шаблона без bump версии) — аудит доверять такому файлу не может.
PROMPT_HASH_CONFLICT = "prompt_hash_conflict"

#: Версия шаблона промпта оценщика. Имя файла — ``templates/{TEMPLATE_VERSION}.txt``.
#: Смена содержимого шаблона = bump версии → новый ``prompt_hash`` → инвалидация ячеек (§13).
#:
#: История (файлы версий не правятся и не удаляются — они провенанс прошлых прогонов, а
#: ``prompt_hash`` считается по содержимому):
#:
#: * ``evaluator_v1`` — правило для ``❌`` требовало несуществующего поля
#:   («absence-evidence»), инструкция была **неисполнима** при ``extra="forbid"``;
#: * ``evaluator_v2`` — ``❌`` объявлен законным и без цитаты («материала по существу нет»);
#:   это вылечило недостижимость ``❌``, но породило **ложные «нет»**: живой контроль дал
#:   2 из 12 ячеек с «отсутствует» там, где вендор заявил свойство (не дословно);
#: * ``evaluator_v3`` — Вариант 2: ``❌`` только при прямом отрицании/ограничении **с
#:   цитатой** (``denial_strength="explicit"``); всё остальное — ``❓`` с причиной
#:   (``denial_strength="not_stated"``). Прямой запрет в тексте: «если фрагмент релевантен,
#:   но дословного утверждения нет — это unknown, not absent». Замер полной популяции
#:   (2026-10-27): выдуманных ``❌`` ноль, но потеряно различение частичного утверждения —
#:   15 ``⚠️`` → ``❓`` из 24;
#: * ``evaluator_v4`` — тот же запрет на выдуманное отсутствие плюс явные правила выбора
#:   между ⚠️ и ❓ (есть ли в тексте УТВЕРЖДЕНИЕ, пусть частичное) и между ❌ и ❓ (❌ — это
#:   заявление вендора, а не вывод из молчания). Замер на тех же 119 ячейках: вернул 9 из
#:   15 потерянных ``⚠️``, сохранил ноль выдуманных ``❌``; из 9 сохранённых ``✅`` ни один
#:   не содержит цитаты, не упоминающей предмет параметра.
#: * ``evaluator_v5`` — правила v4 без изменений плюс четыре вещи, которых не было ни в
#:   одной версии: (1) язык цитаты («цитата дословно на языке фрагмента, перевод и
#:   пересказ цитатой не являются») — без этого правила RU-вопрос над EN-документом давал
#:   перевод цитаты, а сверка I22 отвергала его как «не подстрока», то есть retry ≤ ``max_retries``
#:   и ``failed``; (2) описание структурной шапки фрагмента и запрет цитировать шапку
#:   (C7: заголовки и документ теперь видны, но не являются текстом); (3) семантика
#:   пустого раздела ДАННЫЕ (``no_fragments`` → ``❓``, ``❌`` запрещён); (4) явное
#:   правило выбора цитаты отрицания, когда отрицающих фрагментов несколько.
#:   Плюс снятие бэкслешей в guidance ``distribution_absence`` (см. :data:`ABSENCE_GUIDANCE`).
#: * ``evaluator_v6`` (19.11.2026) — правила v5 дословно плюс слоты ``{{GROUP}}`` и
#:   ``{{MODULE}}`` (группа параметра и модуль) и абзац о том, что это **ориентир**, а не
#:   доказательство: цитата по-прежнему обязана быть подстрокой ТЕКСТА фрагмента. Причина:
#:   строка каталога несёт ``group`` (8 значений) и ``module``, но до модели они не
#:   доходили — правило параметра читалось без указания, о какой части продукта речь.
#:   Это ``execution_change`` (``prompt_template`` входит в контракт), поэтому ``run_id``
#:   меняется у всех пакетов; ``prompt_hash`` покрывает ``group``/``module`` тем же
#:   аргументом (см. :func:`core.util.hashing.prompt_hash`).
TEMPLATE_VERSION = "evaluator_v6"

#: Каталог версионированных шаблонов (данные модуля, не пакет Python).
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

#: Токен плейсхолдера ``{{NAME}}`` (NAME — верхний регистр/цифры/подчёркивание).
_PLACEHOLDER_RE = re.compile(r"\{\{([A-Z0-9_]+)\}\}")

#: Порядок языков концепт-терминов для запросов S13: сначала ru, затем en.
_QUERY_LANG_ORDER = ("ru", "en")

#: Рекомендация по доказательству отсутствия в зависимости от типа правила (Вариант 2).
#:
#: **Адресат — модель, а не человек.** Guidance описывает только то, что модель **вправе**
#: заявить: ``explicit`` (прямое отрицание с цитатой) или ``not_stated`` (незнание).
#: Авторитет (реестр) и принятие риска (человек) модели недоступны по построению, поэтому
#: упоминаются как чужие роли, а не как условие, которое она исполняет.
#:
#: Прежняя (v2) формулировка «если в предоставленных фрагментах нет ни самого параметра, ни
#: ссылки на него, ставь ❌» — именно тот текст, который дал ложные «нет»: отсутствие
#: дословного упоминания не означает отсутствия свойства.
ABSENCE_GUIDANCE: dict[AbsenceRuleType, str] = {
    AbsenceRuleType.EXPLICIT: (
        "❌ допустим только при прямом отрицании или ограничении вендора, и тогда "
        "denial_strength=\"explicit\" с цитатой этого фрагмента. Если материал релевантен, "
        "но дословного утверждения нет — ❓ с denial_reason."
    ),
    AbsenceRuleType.PARAMETER_REFERENCE: (
        "❌ по полноте справочника ставит не ты: полноту принимает человек, и только тогда "
        "класс становится bounded. Твой выбор — explicit (прямое отрицание с цитатой) или "
        "❓ с denial_reason («в переданных фрагментах дословного утверждения нет»)."
    ),
    AbsenceRuleType.DISTRIBUTION_ABSENCE: (
        "❌ допустим только при прямом отрицании вендора с цитатой и "
        "denial_strength=\"explicit\"; полноту охвата принимает человек, а не ты "
        "(только тогда класс становится bounded). Нет дословного утверждения — ❓ с "
        "denial_reason, даже если вопрос кажется «не относящимся к продукту»."
    ),
    AbsenceRuleType.REGISTRY_ABSENCE: (
        "Реестр — авторитетная энумерация: вердикт по нему ставит реестровый контур, не ты. "
        "Если в переданных фрагментах дословного утверждения нет — ❓ с denial_reason."
    ),
}

#: Любой из двух допустимых типов чанка на входе компиляции (у обоих есть ``chunk_id``/``text``).
ChunkLike = Chunk | RetrievedChunk

#: Хвост ``chunk_id``, отделяющий номер фрагмента от идентификатора документа.
#:
#: Оба объявленных формата ядра несут номер в конце: ``{document_id}#{no}`` (чанкер) и
#: ``{product_id}:{doc_hash}:{no}`` (run-level id, ``corpus_index._chunk_id``). Отрезается
#: **только** числовой хвост, поэтому непредусмотренный формат даёт пустой документ
#: (строка ``document:`` не печатается), а не выдуманный идентификатор.
_CHUNK_NO_SUFFIX_RE = re.compile(r"(?:#|:)\d+$")


@dataclass(frozen=True)
class CompiledPrompt:
    """Скомпилированный промпт ячейки: текст + провенанс для аудита (§3 A.2, §5).

    ``prompt_hash`` — канонический ключ §5 (шаблон + правило + absence_rule + группа +
    модуль + набор ``chunk_id``); он сохраняется **полем** артефакта и в ячейке
    (воспроизводимость и инвалидация при смене шаблона). Адресует файл аудита не он, а
    ``sha256`` текста: формула §5 не покрывает product/title/текст чанков, поэтому один
    ``prompt_hash`` законно описывает несколько разных промптов (см. докстринг модуля).
    ``cell_key`` — ``(product_id, param_id)`` целевой ячейки.
    """

    text: str
    prompt_hash: str
    template_version: str
    chunk_ids: list[str]
    cell_key: tuple[str, str]

    def to_json(self) -> dict:
        """Словарь для файла артефакта (JSON-совместимый, детерминированный)."""
        return {
            "prompt_hash": self.prompt_hash,
            "template_version": self.template_version,
            "cell_key": list(self.cell_key),
            "chunk_ids": list(self.chunk_ids),
            "text": self.text,
        }

    def save(self, out_dir: str | Path) -> Path:
        """Сохраняет промпт в ``out_dir/{sha256(text)}.json`` (см. :func:`save_prompt`)."""
        return save_prompt(self, out_dir)


@cache
def _load_template(version: str) -> str:
    """Читает шаблон ``templates/{version}.txt`` (локальный файл, кэшируется; без сети)."""
    path = TEMPLATES_DIR / f"{version}.txt"
    if not path.is_file():
        raise FileNotFoundError(f"шаблон промпта не найден: {path}")
    return path.read_text(encoding="utf-8")


def _fill_template(template: str, mapping: dict[str, str]) -> str:
    """Однопроходная подстановка ``{{NAME}}`` из ``mapping`` (значения не пересканируются)."""

    def _replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in mapping:
            raise ValueError(f"неизвестный плейсхолдер шаблона: {{{{{name}}}}}")
        return mapping[name]

    return _PLACEHOLDER_RE.sub(_replace, template)


def _enum_value(value: object) -> str:
    """Строковое значение enum/None для рендера (``None`` → ``—``)."""
    if value is None:
        return "—"
    raw = getattr(value, "value", value)
    return str(raw)


def role_vocabulary_of(params: Iterable[CatalogRow]) -> frozenset[str]:
    """Словарь ролей каталога: все непустые ``role_binding`` его строк.

    Один резолв на прогон (как ``query_lang``): значение считает фаза и передаёт вниз, а
    не каждый уровень вычисляет своё — иначе «словарь» у промпта и у замечания о
    расхождении словарей мог бы оказаться разным.
    """
    return frozenset(
        str(param.role_binding)
        for param in params
        if getattr(param, "role_binding", None)
    )


def _relevant_satellites(
    param: CatalogRow,
    satellites: Sequence[Satellite],
    role_vocabulary: Collection[str] | None = None,
) -> list[Satellite]:
    """Отбирает сателлиты релевантных ролей и упорядочивает детерминированно.

    Правило трёхзначное (``role_vocabulary`` — словарь ролей каталога):

    * роль сателлита **входит** в словарь и равна ``param.role_binding`` → включить;
    * роль **входит** в словарь, но это не роль данного параметра → отбросить;
    * роль **вне** словаря каталога → включить: это факт о расхождении двух словарей
      (паспортного и каталожного), а не о нерелевантности сателлита.

    Почему вне-словарная роль не отбрасывается: фильтрация по роли — оптимизация, и её
    посылка («у паспорта и каталога общий словарь ролей») в данных ложна — замер
    19.11.2026 на ``ru-pg-dbms``: ``role_binding`` = {core_engine, backup_recovery,
    management_platform, migration}, роли сателлитов в паспортах = {security_crypto,
    backup, query_optimizer, unknown, management_console, streaming, database_appliance,
    ha_orchestrator, in_memory_cache}, пересечение **пусто**. Сателлит — контекст
    промпта, а не доказательство (I17 не про него), поэтому потерять контекст хуже, чем
    передать его: на пилоте это 0–4 сателлита на продукт. Цена ошибки видна в замечании
    фазы (:func:`satellite_role_note`), а не выводится читателем.

    ``role_vocabulary is None`` — прежнее строгое поведение (только совпадение роли):
    так чистые вызовы и существующие тесты не меняют результат молча. Фаза Ш6 словарь
    передаёт всегда — она знает каталог.

    Порядок — ``(role, component, state)``: стабилен между прогонами независимо от
    порядка входа.
    """
    if param.role_binding is None:
        selected = list(satellites)
    elif role_vocabulary is None:
        selected = [sat for sat in satellites if sat.role == param.role_binding]
    else:
        selected = [
            sat
            for sat in satellites
            if sat.role == param.role_binding or sat.role not in role_vocabulary
        ]
    return sorted(
        selected, key=lambda sat: (sat.role, sat.component or "", str(sat.state))
    )


def satellite_role_note(
    role_vocabulary: Collection[str] | None,
    satellites_by_product: Mapping[str, Sequence[Satellite]],
) -> str | None:
    """Замечание фазы о том, сколько сателлитов доходит до промпта и почему.

    Считается **до** цикла оценки из двух чистых множеств (паспорта × словарь ролей
    каталога), поэтому LLM и ретривал для неё не нужны, а число не зависит от того,
    сколько ячеек успело отработать.

    Возвращает ``None``, когда сателлитов нет вовсе: строка «0 сателлитов» на каждом
    прогоне пакета без паспортов была бы шумом. Иначе — одна строка с тремя фактами:
    сколько сателлитов передаётся как контекст **вне** словаря ролей, сколько
    отбирается по роли, и пересекаются ли словари вообще.
    """
    groups = [tuple(group or ()) for group in (satellites_by_product or {}).values()]
    total = sum(len(group) for group in groups)
    if not total:
        return None
    vocabulary = frozenset(str(role) for role in (role_vocabulary or ()))
    roles = [str(sat.role) for group in groups for sat in group]
    out_of_vocabulary = sum(1 for role in roles if role not in vocabulary)
    by_role = total - out_of_vocabulary
    overlap = "пересекаются" if vocabulary & set(roles) else "не пересекаются"
    return (
        f"{SATELLITE_OUT_OF_VOCABULARY_TOKEN}: {out_of_vocabulary} из {total} "
        f"сателлитов переданы как контекст (роль вне словаря ролей каталога), "
        f"по роли отбираются {by_role}; словари ролей {overlap}"
    )


def _render_satellites(satellites: Sequence[Satellite]) -> str:
    """Рендер сателлитов как контекста (не доказательства, §B.4/02 C.1)."""
    if not satellites:
        return "(нет сателлитов)"
    lines = [
        (
            f"- role={sat.role}; component={sat.component or '—'}; "
            f"origin={_enum_value(sat.origin)}; relationship={_enum_value(sat.relationship)}; "
            f"evidence_state={_enum_value(sat.evidence_state)}; state={_enum_value(sat.state)}"
        )
        for sat in satellites
    ]
    return "\n".join(lines)


def _document_id(chunk_id: str) -> str:
    """Идентификатор документа из ``chunk_id`` (``""`` — формат не опознан).

    Это не разбор чужого формата, а обратное чтение двух форматов, которые ядро само и
    строит (см. :data:`_CHUNK_NO_SUFFIX_RE`). Нужен, потому что ``RetrievedChunk`` не
    несёт ни URL, ни заголовка документа: единственное, что говорит «откуда фрагмент», —
    префикс ``chunk_id`` до номера фрагмента. Выдумывать вместо него пустое значение
    нельзя (модель решила бы, что документ один), поэтому непознанный формат просто
    опускает строку шапки.
    """
    match = _CHUNK_NO_SUFFIX_RE.search(chunk_id)
    return chunk_id[: match.start()] if match else ""


def _text_field(chunk: ChunkLike, name: str) -> str:
    """Непустое строковое поле чанка (``""``, если поля нет или оно пустое).

    ``context_prefix`` есть только у :class:`~core.ports.types.Chunk`: ``RetrievedChunk``
    его не несёт, поэтому ``getattr`` с дефолтом, а не прямое обращение — компиляция не
    должна падать на другом виде чанка.
    """
    value = getattr(chunk, name, "")
    return value.strip() if isinstance(value, str) else ""


def _render_chunk(chunk: ChunkLike, number: int) -> str:
    """Один фрагмент: структурная шапка + строка ``текст:`` + дословный текст (C7).

    Шапка — метаданные, отделённые от текста явной границей: цитатная сверка (I22)
    сравнивает цитату с чистым ``text``, и текст фрагмента остаётся его дословным
    фрагментом. Пустые поля шапки **не печатаются** (никаких ``None``/``[]``): строк
    может быть меньше, и это нормальное состояние, о котором шаблон предупреждает.

    ``context_prefix`` печатается, только если он не повторяет ``heading_path``:
    markdown-чанкер кладёт туда тот же путь заголовков, и дубль стоил бы токенов,
    ничего не добавляя.
    """
    lines = [f"[фрагмент {number}]", f"source_id: {chunk.chunk_id}"]
    document = _document_id(chunk.chunk_id)
    if document:
        lines.append(f"document: {document}")
    heading = _text_field(chunk, "heading_path")
    if heading:
        lines.append(f"heading: {heading}")
    context = _text_field(chunk, "context_prefix")
    if context and context != heading:
        lines.append(f"context: {context}")
    lines.append("текст:")
    lines.append(chunk.text)
    return "\n".join(lines)


def _render_chunks(chunks: Sequence[ChunkLike]) -> str:
    """Рендер фрагментов: шапка + дословный текст каждого (см. :func:`_render_chunk`)."""
    if not chunks:
        return "(нет фрагментов)"
    return "\n\n".join(
        _render_chunk(chunk, number) for number, chunk in enumerate(chunks, 1)
    )


def compile_prompt(
    *,
    param: CatalogRow,
    product_id: str,
    chunks: Sequence[ChunkLike],
    satellites: Sequence[Satellite] = (),
    absence_rule: AbsenceRuleType | None = None,
    role_vocabulary: Collection[str] | None = None,
) -> CompiledPrompt:
    """Механически компилирует промпт оценщика и его ``prompt_hash`` (§7.1/§5).

    :param param: строка каталога (``rule``, ``absence_rule``, ``role_binding``,
        ``group``, ``module``, …).
    :param product_id: продукт-скоуп ячейки (I17 — evidence только из его чанков).
    :param chunks: переданные оценщику фрагменты (``Chunk``|``RetrievedChunk``).
    :param satellites: сателлиты **этого** продукта (смешивать продукты нельзя: сателлит —
        контекст промпта, но привязка к продукту у него та же, что у evidence); фильтруются
        по ``param.role_binding`` и ``role_vocabulary`` (см. :func:`_relevant_satellites`).
    :param absence_rule: переопределяет ``param.absence_rule`` (по умолчанию — из каталога).
    :param role_vocabulary: словарь ролей каталога (:func:`role_vocabulary_of`) — им
        отличается «роль чужого параметра» от «роли, которой в каталоге нет». ``None`` —
        прежнее строгое поведение (только точное совпадение роли).

    Слот ``{{EXTRA_INSTRUCTIONS}}`` шаблона подставляется **пустой строкой** и параметром
    не является: значения у него нет ни в одном объявленном входе, а текст выпущенных
    шаблонов ``templates/evaluator_v*.txt`` менять нельзя — иначе поедет ``prompt_template``
    и ``run_id``.

    Чанки упорядочиваются по ``chunk_id`` (детерминизм R7); ``chunk_ids`` уходят в
    ``prompt_hash`` уже отсортированными, поэтому hash не зависит от порядка входа.
    ``group``/``module`` входят в материал hash (шаблон v6 печатает их): hash обязан
    описывать текст, иначе два разных промпта несли бы один hash.
    """
    resolved_absence = absence_rule if absence_rule is not None else param.absence_rule
    if not isinstance(resolved_absence, AbsenceRuleType):
        resolved_absence = AbsenceRuleType(str(resolved_absence))

    ordered_chunks = sorted(chunks, key=lambda chunk: chunk.chunk_id)
    chunk_ids = sorted(chunk.chunk_id for chunk in ordered_chunks)

    digest = prompt_hash(
        TEMPLATE_VERSION,
        param.rule,
        resolved_absence.value,
        chunk_ids,
        group=param.group,
        module=param.module,
    )

    mapping: dict[str, str] = {
        "PRODUCT_ID": product_id,
        "TITLE": param.title,
        # Группа и модуль — ориентир, а не доказательство (см. evaluator_v6.txt): они
        # уточняют, о какой части продукта правило, и не подменяют цитату.
        "GROUP": param.group,
        "MODULE": param.module,
        "RULE": param.rule,
        "ABSENCE_RULE": resolved_absence.value,
        "ABSENCE_GUIDANCE": ABSENCE_GUIDANCE[resolved_absence],
        "SATELLITES": _render_satellites(
            _relevant_satellites(param, satellites, role_vocabulary)
        ),
        # Слот шаблона существует (``{{EXTRA_INSTRUCTIONS}}`` в evaluator_v*.txt), но
        # значения у него нет: параметра нет ни у одного вызывающего. Подстановка пустой
        # строки сохраняет отрендеренный промпт байт-в-байт и не трогает текст шаблона.
        "EXTRA_INSTRUCTIONS": "",
        "CHUNKS": _render_chunks(ordered_chunks),
    }
    text = _fill_template(_load_template(TEMPLATE_VERSION), mapping)

    return CompiledPrompt(
        text=text,
        prompt_hash=digest,
        template_version=TEMPLATE_VERSION,
        chunk_ids=chunk_ids,
        cell_key=(product_id, param.param_id),
    )


def render_queries(
    param: CatalogRow,
    max_queries: int = 3,
    query_lang: str = "all",
) -> list[str]:
    """Собирает запросы Ш6 (S13): ``[title]`` + концепт-термины, ≤ ``max_queries``.

    Порядок детерминирован и не зависит от реализации/LLM (сохраняет симметрию I16):
    заголовок, затем термины объявленного языка, затем остальные в порядке
    :data:`_QUERY_LANG_ORDER`, затем прочие языки лексикографически; внутри языка —
    порядок пакета.

    :param query_lang: объявленная ось ``ru`` | ``en`` | ``all`` (``retrieval.query_lang``
        / ``--query-lang``). ``all`` — прежнее поведение (все языки), и оно остаётся
        значением по умолчанию, чтобы чистые вызовы и тесты не меняли результат молча.
        ``ru``/``en`` оставляют **только** термины этого языка (плюс ``title``, который
        на языке каталога): на английском корпусе это и есть разница между «нашли» и
        «пусто» — замер 19.11.2026: релевантный чанк в top-10 у 8 из 10 параметров
        (EN) против 1 из 10 (RU).

    **Дубликаты схлопываются (замер).** Автор каталога кладёт в ``concept_terms[lang][0]``
    полный заголовок параметра — иначе говоря, повторяет ``title``. Без схлопывания
    ``[title, title, 'первый-термин']``: из трёх слотов бюджета ретривала один уходил на
    повтор уже выполненного запроса. Замер по каталогу ``ru-pg-dbms``: **119 из 119**
    параметров имели дубликат, ровно один потерянный слот на параметр. Схлопывание
    сохраняет порядок первого появления, поэтому выбор «какие ≤3 запроса пойдут»
    остаётся тем же, что и прежде, — меняется только состав: вместо повтора приходит
    следующий по порядку термин.
    """
    if max_queries < 0:
        raise ValueError("max_queries не может быть отрицательным")
    lang = str(query_lang).strip().lower()
    if lang not in ("ru", "en", "all"):
        raise ValueError(
            f"query_lang: недопустимый язык «{query_lang}»; ожидается ru|en|all"
        )
    terms = param.concept_terms or {}
    if lang == "all":
        ordered_langs = [item for item in _QUERY_LANG_ORDER if item in terms]
        ordered_langs += sorted(item for item in terms if item not in _QUERY_LANG_ORDER)
    else:
        # Объявленный язык идёт первым и остаётся единственным: смешивать его с прочими
        # значило бы вернуть тот же дефект, от которого ось избавляет (бюджет 3 запроса
        # съедали термины не того языка).
        ordered_langs = [lang] if lang in terms else []
    queries: list[str] = []
    seen: set[str] = set()
    for candidate in [param.title, *(t for item in ordered_langs for t in terms[item])]:
        key = candidate.strip().casefold()
        if not key or key in seen:
            continue
        seen.add(key)
        queries.append(candidate)
    return queries[:max_queries]


def prompt_artifact_name(compiled: CompiledPrompt) -> str:
    """Имя файла артефакта: ``sha256(текста)`` + ``.json`` — **контентный** адрес.

    ``prompt_hash`` (§5) адресом быть не может: он не покрывает product/title/текст
    чанков, поэтому разные промпты законно делят его (замер 06.11.2026: ``xlsx_056`` и
    ``xlsx_057`` одного продукта). sha256 текста — адрес, который по построению не
    совпадает у разных текстов и совпадает у одинаковых (дедуп сохраняется).
    """
    return f"{content_hash(compiled.text)}.json"


def _payload(compiled: CompiledPrompt) -> str:
    """Детерминированный JSON-текст артефакта ``prompts/{sha256(text)}.json``.

    Единственный источник формы файла: и :func:`save_prompt`, и сверка содержимого в
    :meth:`PromptArchive.record` берут текст отсюда, иначе сравнение «тот же файл или
    нет» зависело бы от двух разных сериализаций.
    """
    return json.dumps(
        compiled.to_json(), ensure_ascii=False, indent=2, sort_keys=True
    ) + "\n"


def save_prompt(compiled: CompiledPrompt, out_dir: str | Path) -> Path:
    """Пишет ``out_dir/{sha256(text)}.json``; **иммутабельно** — существующий файл не трогает.

    Возвращает путь. Повторный вызов идемпотентен (тот же текст → тот же файл, без
    перезаписи), что защищает аудит-артефакт от мутации. ``prompt_hash`` внутрь файла
    по-прежнему пишется — он остаётся провенансом ячейки, но адресом файла не служит.

    Расхождение содержимого по тому же адресу здесь **не** диагностируется
    (функция остаётся «пиши, если нет»): видимая диагностика — дело
    :class:`PromptArchive`, который пишет в наблюдение прогона, а не в файл.
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / prompt_artifact_name(compiled)
    if not path.exists():
        path.write_text(_payload(compiled), encoding="utf-8")
    return path


@dataclass
class PromptArchive:
    """Каталог аудита **исполненных** промптов: ``{dir}/{sha256(text)}.json`` (V7, §7.1).

    Зачем объект, а не один вызов :func:`save_prompt`: запись артефакта аудита не имеет
    права ни ронять фазу, ни пропадать молча. Фаза отдаёт архив в ячейки, а после цикла
    забирает из него наблюдение — так сбой диска виден в ``PhaseResult.errors``, а
    вердикты остаются полученными (артефакт аудита — не условие измерения).

    **Адрес — контент, не ``prompt_hash``.** Имя файла — ``sha256`` текста промпта.
    ``prompt_hash`` §5 не покрывает product/title/текст чанков, поэтому две разные
    ячейки могут получить один hash (замер 06.11.2026: ``xlsx_056``/``xlsx_057``);
    при адресации по hash второй промпт отказывал (``prompt_hash_conflict``) и в
    архиве оставался текст **чужой** ячейки, то есть аудит свидетельствовал о входе,
    которого не было.

    **Стоимость.** Файл — один на текст промпта, поэтому запись дедуплицируется по
    содержимому входа: две ячейки с одинаковым входом дают один файл. Учёт
    (``saved``/``reused``/``bytes_written``) — наблюдённые факты, их печатает фаза;
    оценок «сколько должно быть» архив не делает.
    """

    directory: Path
    #: Файлов реально создано этим архивом.
    saved: int = 0
    #: Файлов уже было на диске (тот же адрес) — переиспользованы, не перезаписаны.
    reused: int = 0
    #: Байт записано этим архивом (фактический объём каталога, не прогноз).
    bytes_written: int = 0
    #: Видимые аномалии записи: ``prompt_save_failed`` / ``prompt_hash_conflict``.
    errors: list[str] = field(default_factory=list)
    #: Сколько сбоев записи уже сжато в один агрегат (чтобы не печатать 119 строк).
    _collapsed_failures: int = field(default=0, repr=False)
    _last_failure: str = field(default="", repr=False)
    #: Замок записи: архив наполняется из пула потоков оценки (оценщик вызывается
    #: параллельно), а адрес файла — контентный. Два потока с одним текстом промпта
    #: писали бы один и тот же файл одновременно, и ``write_text`` не атомарен.
    _lock: Any = field(default_factory=threading.Lock, repr=False, compare=False)

    def __post_init__(self) -> None:
        self.directory = Path(self.directory)

    def record(self, compiled: CompiledPrompt) -> Path | None:
        """Сохраняет промпт и возвращает путь; ``None`` — запись не удалась (видно в ``errors``).

        Идемпотентность — content-addressed: адрес файла — ``sha256`` текста, существующий
        файл не перезаписывается, а его текст сверяется с тем, что дал бы этот вход.
        Расхождение означает **подмену артефакта на диске** (запись под тем же адресом с
        другим текстом) — это видимая аномалия :data:`PROMPT_HASH_CONFLICT`, а не тихая
        перезапись и не падение прогона. Две разные ячейки с одним ``prompt_hash`` сюда не
        попадают вовсе: у них разные тексты, значит разные файлы.

        Замок охватывает проверку существования и запись вместе: иначе два потока с
        одинаковым текстом промпта увидели бы «файла нет» и писали бы его одновременно.
        """
        path = self.directory / prompt_artifact_name(compiled)
        expected = _payload(compiled)
        with self._lock:
            return self._record_locked(compiled, path, expected)

    def _record_locked(
        self, compiled: CompiledPrompt, path: Path, expected: str
    ) -> Path | None:
        """Тело :meth:`record` под замком (см. его докстринг)."""
        if path.exists():
            self.reused += 1
            try:
                existing = path.read_text(encoding="utf-8")
            except OSError as exc:
                self._fail(path, f"{type(exc).__name__}: {exc}", reading=True)
                return None
            if existing != expected:
                self.errors.append(
                    f"{PROMPT_HASH_CONFLICT}: {path} — файл с тем же контентным адресом "
                    "несёт другой текст (не перезаписан): артефакт подменён на диске"
                )
            return path
        try:
            path = save_prompt(compiled, self.directory)
        except OSError as exc:
            self._fail(path, f"{type(exc).__name__}: {exc}")
            return None
        self.saved += 1
        self.bytes_written += len(expected.encode("utf-8"))
        return path

    def _fail(self, path: Path, detail: str, *, reading: bool = False) -> None:
        """Регистрирует сбой записи, сжимая повторы (диск кончился — 119 одинаковых строк).

        Сбой не пробрасывается: артефакт аудита вторичен к измерению, а ``OSError`` из
        фазы отнял бы уже полученные вердикты. Видимость даёт агрегат в ``errors``.
        """
        what = "чтении" if reading else "записи"
        self._collapsed_failures += 1
        self._last_failure = f"{PROMPT_SAVE_FAILED}: {what} {path}: {detail}"
        if self._collapsed_failures == 1:
            self.errors.append(self._last_failure)

    def summary(self) -> str:
        """Сводка для ``PhaseResult.log_entries``: сколько файлов, байт, где."""
        parts = [
            f"промпты (аудит): {self.saved} записано, {self.reused} переиспользовано",
            f"{self.bytes_written} байт",
            f"каталог {self.directory}",
        ]
        if self._collapsed_failures > 1:
            parts.append(
                f"сбоев записи: {self._collapsed_failures} "
                f"(последний: {self._last_failure})"
            )
        return "; ".join(parts)
