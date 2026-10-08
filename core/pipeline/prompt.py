"""coHound — механическая компиляция промпта оценщика Ш6 (T9.1, §7.1).

Промпт собирается **механически**: версия шаблона + правило параметра + правило
отсутствия + сателлиты релевантных ролей + чанки в формате ``[source: {chunk_id}]``.
Никакой генерации/перефразирования: одна и та же ячейка (при том же входе) даёт
байт-в-байт тот же промпт и тот же ``prompt_hash`` (детерминизм, R7).

**Схема плейсхолдеров шаблона.** ``{{NAME}}``, где ``NAME`` — один из слотов
(``PRODUCT_ID``, ``TITLE``, ``RULE``, ``ABSENCE_RULE``, ``ABSENCE_GUIDANCE``,
``SATELLITES``, ``EXTRA_INSTRUCTIONS``, ``CHUNKS``). Подстановка — **однопроходная**
(``re.sub`` по токенам исходного шаблона), поэтому значения не пересканируются:
``{{...}}`` внутри текста чанка остаётся литеральным. Неизвестный слот в шаблоне —
``ValueError`` (шаблон и код рассинхронизированы — это ошибка сборки, не данных).

**``prompt_hash``.** Ровно контракт :func:`core.util.hashing.prompt_hash`:
``H(template_version, rule, absence_rule, sorted(chunk_ids))``. Сателлиты,
``extra_instructions`` и отрендеренный текст в hash **не входят** — такова
фиксированная формула §5/01 §5. Полный текст сохраняется рядом с hash
(``prompts/{hash}.json``), чтобы аудит мог сверить содержимое.

**Инварианты.** I18 — оценщик работает только по переданным чанкам, промпт не
инструктирует его искать. I10/I15 — модель не генерирует URL. I7 — N/A запрещён.
Схема ответа навязывается constrained decoding'ом на адаптере (§7.1); здесь — лишь
напоминание формы.

Модуль категорийно нейтрален (§1.3), без сети, без новых зависимостей.
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from core.domain.enums import AbsenceRuleType
from core.domain.models import CatalogRow, Satellite
from core.ports.types import Chunk
from core.ports.vector_index import RetrievedChunk
from core.util.hashing import prompt_hash

__all__ = [
    "TEMPLATE_VERSION",
    "TEMPLATES_DIR",
    "ABSENCE_GUIDANCE",
    "CompiledPrompt",
    "compile_prompt",
    "render_queries",
    "save_prompt",
]

#: Версия шаблона промпта оценщика. Имя файла — ``templates/{TEMPLATE_VERSION}.txt``.
#: Смена содержимого шаблона = bump версии → новый ``prompt_hash`` → инвалидация ячеек (§13).
TEMPLATE_VERSION = "evaluator_v1"

#: Каталог версионированных шаблонов (данные модуля, не пакет Python).
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

#: Токен плейсхолдера ``{{NAME}}`` (NAME — верхний регистр/цифры/подчёркивание).
_PLACEHOLDER_RE = re.compile(r"\{\{([A-Z0-9_]+)\}\}")

#: Порядок языков концепт-терминов для запросов S13: сначала ru, затем en.
_QUERY_LANG_ORDER = ("ru", "en")

#: Рекомендация по доказательству отсутствия в зависимости от типа правила (I2/I2b).
ABSENCE_GUIDANCE: dict[AbsenceRuleType, str] = {
    AbsenceRuleType.EXPLICIT: (
        "Отсутствие подтверждается явным отсутствием в предоставленных фрагментах."
    ),
    AbsenceRuleType.PARAMETER_REFERENCE: (
        "❌ требует source_scope=exhaustive и scope_attested_by."
    ),
    AbsenceRuleType.DISTRIBUTION_ABSENCE: (
        "❌ требует source_scope=exhaustive и scope_attested_by."
    ),
    AbsenceRuleType.REGISTRY_ABSENCE: (
        "❌: охват exhaustive по построению источника."
    ),
}

#: Любой из двух допустимых типов чанка на входе компиляции (у обоих есть ``chunk_id``/``text``).
ChunkLike = Chunk | RetrievedChunk


@dataclass(frozen=True)
class CompiledPrompt:
    """Скомпилированный промпт ячейки: текст + провенанс для аудита (§3 A.2, §5).

    ``prompt_hash`` — ключ адресации ``prompts/{prompt_hash}.json`` и значение,
    сохраняемое в ячейке (воспроизводимость + инвалидация при смене шаблона).
    ``cell_key`` — ``(product_id, param_id)`` целевой ячейки.
    """

    text: str
    prompt_hash: str
    template_version: str
    chunk_ids: list[str]
    cell_key: tuple[str, str]

    def to_json(self) -> dict:
        """Словарь для ``prompts/{hash}.json`` (JSON-совместимый, детерминированный)."""
        return {
            "prompt_hash": self.prompt_hash,
            "template_version": self.template_version,
            "cell_key": list(self.cell_key),
            "chunk_ids": list(self.chunk_ids),
            "text": self.text,
        }

    def save(self, out_dir: str | Path) -> Path:
        """Сохраняет промпт в ``out_dir/{prompt_hash}.json`` (см. :func:`save_prompt`)."""
        return save_prompt(self, out_dir)


@lru_cache(maxsize=None)
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


def _relevant_satellites(
    param: CatalogRow, satellites: Sequence[Satellite]
) -> list[Satellite]:
    """Отбирает сателлиты релевантных ролей и упорядочивает детерминированно.

    При заданном ``param.role_binding`` остаются сателлиты с совпадающей ролью; иначе
    берутся все переданные. Порядок — ``(role, component, state)``: стабилен между
    прогонами независимо от порядка входа.
    """
    if param.role_binding is None:
        selected = list(satellites)
    else:
        selected = [sat for sat in satellites if sat.role == param.role_binding]
    return sorted(
        selected, key=lambda sat: (sat.role, sat.component or "", str(sat.state))
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


def _render_chunks(chunks: Sequence[ChunkLike]) -> str:
    """Рендер чанков: ``[source: {chunk_id}]`` + чистый текст (§7.1)."""
    if not chunks:
        return "(нет фрагментов)"
    return "\n\n".join(f"[source: {chunk.chunk_id}]\n{chunk.text}" for chunk in chunks)


def compile_prompt(
    *,
    param: CatalogRow,
    product_id: str,
    chunks: Sequence[ChunkLike],
    satellites: Sequence[Satellite] = (),
    absence_rule: AbsenceRuleType | None = None,
    extra_instructions: str = "",
) -> CompiledPrompt:
    """Механически компилирует промпт оценщика и его ``prompt_hash`` (§7.1/§5).

    :param param: строка каталога (``rule``, ``absence_rule``, ``role_binding``, …).
    :param product_id: продукт-скоуп ячейки (I17 — evidence только из его чанков).
    :param chunks: переданные оценщику фрагменты (``Chunk``|``RetrievedChunk``).
    :param satellites: сателлиты; фильтруются по ``param.role_binding`` (если задан).
    :param absence_rule: переопределяет ``param.absence_rule`` (по умолчанию — из каталога).
    :param extra_instructions: дополнительный текст (в hash не входит; сохраняется в тексте).

    Чанки упорядочиваются по ``chunk_id`` (детерминизм R7); ``chunk_ids`` уходят в
    ``prompt_hash`` уже отсортированными, поэтому hash не зависит от порядка входа.
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
    )

    mapping: dict[str, str] = {
        "PRODUCT_ID": product_id,
        "TITLE": param.title,
        "RULE": param.rule,
        "ABSENCE_RULE": resolved_absence.value,
        "ABSENCE_GUIDANCE": ABSENCE_GUIDANCE[resolved_absence],
        "SATELLITES": _render_satellites(_relevant_satellites(param, satellites)),
        "EXTRA_INSTRUCTIONS": extra_instructions,
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


def render_queries(param: CatalogRow, max_queries: int = 3) -> list[str]:
    """Собирает запросы Ш6 (S13): ``[title]`` + концепт-термины ru → en, ≤ ``max_queries``.

    Порядок детерминирован и не зависит от реализации/LLM (сохраняет симметрию I16):
    заголовок, затем термины языка ``ru``, затем ``en``, затем прочие языки в
    лексикографическом порядке; внутри языка — порядок пакета (порядок списка).
    """
    if max_queries < 0:
        raise ValueError("max_queries не может быть отрицательным")
    terms = param.concept_terms or {}
    ordered_langs = [lang for lang in _QUERY_LANG_ORDER if lang in terms]
    ordered_langs += sorted(lang for lang in terms if lang not in _QUERY_LANG_ORDER)
    queries = [param.title]
    for lang in ordered_langs:
        queries.extend(terms[lang])
    return queries[:max_queries]


def save_prompt(compiled: CompiledPrompt, out_dir: str | Path) -> Path:
    """Пишет ``out_dir/{prompt_hash}.json``; **иммутабельно** — существующий файл не трогает.

    Возвращает путь. Повторный вызов идемпотентен (тот же hash → тот же файл, без
    перезаписи), что защищает аудит-артефакт от мутации.
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{compiled.prompt_hash}.json"
    if not path.exists():
        payload = json.dumps(
            compiled.to_json(), ensure_ascii=False, indent=2, sort_keys=True
        )
        path.write_text(payload + "\n", encoding="utf-8")
    return path
