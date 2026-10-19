#!/usr/bin/env python3
"""Воспроизводимый генератор пакета категории ``ru-pg-dbms`` (coHound).

Источники (read-only, внешние):

* ``psq_params_091_demo.xlsx`` лист ``final`` — технический каталог 119 параметров
  (колонки A Модуль, B Группа, C Параметр, D Важность, E Референс PG,
  F Семантика (механизм), G Границы зачёта, H Ценность, I Рубрика);
* ``golden_091.xlsx`` лист ``data`` — эталонные статусы (F Postgres Pro,
  G Platform V Pangolin DB) для подмножества параметров.

Генератор **детерминирован**: YAML пишется с ``sort_keys=True``/``allow_unicode``,
CSV — фиксированным порядком строк/колонок и ``lineterminator="\\n"``. Повторный
прогон даёт байт-идентичные ``catalog.csv`` и ``catalog.lock.csv``: существующие
``param_id`` переиспользуются из ``catalog.lock.csv`` по ключу
``(source_file, source_row)`` (иммутабельность slug, §13).

Повреждение psq (``?"`` в 82 ячейках, 63 строки) обходится так:

* если ячейка G («Границы зачёта») чистая (нет ``?"``) и непустая **и F тоже чистая**
  — ``rule`` берётся из G (origin ``source``);
* если G повреждена/пуста **или** строка сдвинута из F (голова G «утекла» в F) —
  ``rule = "Засчитывается: " + <F до первого ``?"``>, усечение до ~300 символов``,
  origin ``needs_human``. Дополнительно каждая повреждённая строка фиксируется полем
  ``row_integrity`` (origin ``needs_human``).

Соблюдается A3: выведенные поля не выдаются за факт — см. ``derived_fields.csv``.

Правило A3/§1.3: генератор лежит в ``packages/`` и не влияет на нейтральность ``core/``.
"""

from __future__ import annotations

import csv
import hashlib
import re
import sys
from pathlib import Path

import yaml

# ``openpyxl`` (extra ``extract``) импортируется лениво внутри читателей xlsx: чистые
# эмиттеры артефактов (напр. ``build_domain_profiles``) обязаны быть импортируемы в
# каноническом dev/CI-окружении (``--extra dev``), где openpyxl не выставлен. Иначе
# регрессионный тест генератора молча скипался бы и дрейф схемы вернулся бы незамеченным.

# ---------------------------------------------------------------------------
# Константы кейса
# ---------------------------------------------------------------------------

CASE_ID = "ru-pg-dbms"
PACKAGE_VERSION = "1.0.0"
SNAPSHOT_DATE = "2026-10-01"
GENERATED_AT = "2026-10-01T00:00:00Z"
BASELINE_ID = "upstream-pg"
BASELINE_PARTICIPANT = "postgresql"

SRC_PSQ = Path("/mnt/c/Work/gtm/psq_params_091_demo.xlsx")
SRC_GOLDEN = Path("/mnt/c/Work/gtm/golden_091.xlsx")
PSQ_SHEET = "final"
GOLDEN_SHEET = "data"
PSQ_SOURCE_REF = f"{SRC_PSQ.name}#{PSQ_SHEET}"
GOLDEN_SOURCE_REF = f"{SRC_GOLDEN.name}#{GOLDEN_SHEET}"

PACKAGE_DIR = Path(__file__).resolve().parent.parent

#: Отображение модулей psq → module_id пакета.
MODULE_MAP = {
    "А. Ядро СУБД": "core",
    "Б. Резервное копирование": "backup",
    "В. Управление и эксплуатация": "management",
}

#: Отображение групп psq → ASCII-slug.
GROUP_MAP = {
    "А.1. Архитектура ядра": "a1_architecture",
    "А.2. Планировщик": "a2_planner",
    "А.3. Интеграция с 1С": "a3_1c_integration",
    "А.4. Миграция": "a4_migration",
    "А.5. Мультимодельность": "a5_multimodel",
    "Б.1. Базовые возможности": "b1_backup_basic",
    "Б.2. Эффективность хранения": "b2_storage_eff",
    "Б.3. Защита от угроз": "b3_backup_threats",
    "Б.4. Сценарии восстановления": "b4_recovery",
    "В.1. Ядро платформы": "v1_platform_core",
}

#: Группа → роль каталога.
GROUP_ROLE_MAP = {
    "a1_architecture": "core_engine",
    "a2_planner": "query_planner",
    "a3_1c_integration": "app_integration",
    "a4_migration": "migration",
    "a5_multimodel": "core_engine",
    "b1_backup_basic": "backup_recovery",
    "b2_storage_eff": "backup_recovery",
    "b3_backup_threats": "backup_recovery",
    "b4_recovery": "backup_recovery",
    "v1_platform_core": "management_platform",
}

#: Веса модулей (перенормировка 55/18/22 от суммы 95 → ровно 100).
MODULE_WEIGHTS = [
    ("core", 58.0),
    ("backup", 19.0),
    ("management", 23.0),
]

#: Словарь транслитерации RU→EN (ГОСТ-подобный, практичный для slug).
_TRANSLIT = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
    "ж": "zh", "з": "z", "и": "i", "й": "j", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "h", "ц": "c", "ч": "ch", "ш": "sh", "щ": "shch",
    "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
}

#: Русские стоп-слова для concept_terms (грубо, для отсечения служебных токенов).
_RU_STOP = {
    "и", "в", "во", "на", "с", "со", "по", "для", "из", "к", "ко", "о", "об",
    "от", "до", "за", "при", "над", "под", "без", "же", "ли", "не", "ни",
    "или", "а", "но", "как", "что", "это", "его", "ее", "их", "the", "of",
}

#: Максимальная длина ``param_id`` (slug, §4.2).
_MAX_SLUG = 64

# ---------------------------------------------------------------------------
# concept_terms: курируемый глоссарий RU→EN и латинские технические токены
# ---------------------------------------------------------------------------
#
# EN-часть ``concept_terms`` НЕ строится транслитерацией русских слов (это давало
# бесполезный для англоязычного поиска мусор вида ``masshtabirovanie``). Вместо этого
# собираются:
#   1) латинские технические токены, реально присутствующие в ``title``/``Семантика``
#      (XID, MXID, CSN, LWLock, TOAST, WAL, PITR, JSONB, …);
#   2) курируемый глоссарий RU→EN для типовых технических слов заголовков
#      («счётчик транзакций» → transaction id counter, «резервное копирование» → backup, …).
# Транслитерация (:func:`translit`) остаётся ТОЛЬКО источником ``param_id`` (slug §13).

#: Латинские технические токены «в каноне» — с подчёркиванием/длиной, которые эвристика
#: валидатора (``^[a-z0-9_]+$`` и ``_``/длина>12) иначе сочла бы подозрительными.
#: Все — реальные идентификаторы из источников, а не транслит-артефакты.
_TECH_LATIN: frozenset[str] = frozenset(
    {
        # подчёркивания из psq (title/F)
        "shared_buffers", "autonomous_transaction", "pg_catalog", "dbms_sql",
        "dbms_lob", "dbms_crypto", "dbms_scheduler", "utl_file", "utl_http",
        "utl_mail", "utl_smtp", "dba_tables", "exception_init",
        "raise_application_error", "json_table", "system_time", "pg_checksums",
        "pg_verifybackup",
        # часто встречающиеся идентификаторы ядра/расширений
        "pg_hint_plan", "pg_stat_statements", "pgvector", "hypopg", "autovacuum",
        "jsonb", "lwlock", "slru", "walmethods", "wal_level", "pg_upgrade",
        "shared_buffers", "work_mem", "on_premise", "postgresql", "postgres",
    }
)

#: Префиксы латинских идентификаторов, которые считаются техническими (pg_*, dbms_*, …).
_TECH_LATIN_PREFIXES: tuple[str, ...] = (
    "pg_", "dbms_", "utl_", "dba_", "all_", "user_", "shared_", "system_",
    "json_", "exception_", "raise_", "autonomous_",
)

#: Английские служебные слова — не термины, в EN-список не попадают.
_LATIN_STOP: frozenset[str] = frozenset(
    {
        "for", "the", "and", "with", "from", "into", "of", "in", "on", "at",
        "to", "a", "an", "per", "via", "vs", "etc", "id", "dr", "ii", "iii",
    }
)

#: Глоссарий RU→EN: фрагмент заголовка/семантики (без учёта регистра) → EN-термины.
#: Сопоставление — по подстроке; проверяется в порядке объявления. ~100 частотных
#: технических слов, покрывающих заголовки каталога.
_RU_EN_GLOSSARY: tuple[tuple[str, tuple[str, ...]], ...] = (
    # --- общие/CУБД ---
    ("субд", ("dbms", "database")),
    ("база данных", ("database",)),
    ("базами", ("database",)),
    ("подключени", ("connections",)),
    ("соединени", ("connections",)),
    ("эксплуатац", ("operations",)),
    ("платформ", ("platform",)),
    ("управлени", ("management",)),
    ("ландшафт", ("landscape",)),
    ("гетерогенн", ("heterogeneous",)),
    ("консол", ("console",)),
    ("транзакц", ("transaction",)),
    ("счётчик", ("counter",)),
    ("счетчик", ("counter",)),
    ("масштабир", ("scaling",)),
    ("оптимизац", ("optimization",)),
    ("оптимизатор", ("optimizer",)),
    ("автооптимизац", ("auto-tuning",)),
    # --- a1 архитектура ядра ---
    ("снимок", ("snapshot",)),
    ("снимки", ("snapshots",)),
    ("блокиров", ("locking", "locks")),
    ("субтранзакц", ("subtransactions",)),
    ("автономн", ("autonomous",)),
    ("сжат", ("compression",)),
    ("прозрачн", ("transparent",)),
    ("партиц", ("partitioning", "partitions")),
    ("секциониров", ("partitioning",)),
    ("вставк", ("insert",)),
    ("кэш", ("cache",)),
    ("системн", ("system",)),
    ("каталог", ("catalog",)),
    ("релиз", ("release",)),
    ("повреждени", ("corruption",)),
    ("временн", ("temporary",)),
    ("таблиц", ("tables",)),
    ("буферн", ("buffer",)),
    ("карт", ("maps",)),
    ("конкуренц", ("contention",)),
    ("аллокац", ("allocation",)),
    ("памят", ("memory",)),
    ("инструкц", ("instructions",)),
    ("размещени", ("placement",)),
    ("блоков", ("blocks",)),
    ("энергонезависим", ("persistent", "non-volatile")),
    ("восстановлени", ("recovery",)),
    # --- a2 планировщик ---
    ("параллельн", ("parallel",)),
    ("выполнени", ("execution",)),
    ("запрос", ("query",)),
    ("планов", ("plans",)),
    ("подмена", ("substitution",)),
    ("квот", ("quotas",)),
    ("ресурс", ("resources",)),
    ("изоляц", ("isolation",)),
    ("нагрузок", ("workloads",)),
    ("приоритизац", ("prioritization",)),
    ("гипотетическ", ("hypothetical",)),
    ("индекс", ("indexes",)),
    ("подсказк", ("hints",)),
    ("предикат", ("predicates",)),
    ("продвижени", ("pushdown",)),
    ("отсечени", ("pruning",)),
    ("преобразовани", ("transformation",)),
    # --- a3 интеграция 1С ---
    ("сертификат", ("certificate",)),
    ("совместимост", ("compatibility",)),
    ("совместимо", ("compatible",)),
    ("производительност", ("performance",)),
    ("тест", ("testing", "tests")),
    ("пользовател", ("users",)),
    ("планировщик", ("planner",)),
    ("эвристик", ("heuristics",)),
    ("паттерн", ("patterns",)),
    ("профил", ("profiles",)),
    ("регистр", ("registers",)),
    ("корреляц", ("correlation",)),
    ("журнал", ("log",)),
    ("технологическ", ("technology",)),
    ("поддержк", ("support",)),
    ("кодировк", ("encoding",)),
    ("иерархическ", ("hierarchical",)),
    ("закрыт", ("closed",)),
    ("производственн", ("production",)),
    # --- a4 миграция ---
    ("оценк", ("assessment",)),
    ("сложност", ("complexity",)),
    ("миграц", ("migration",)),
    ("пакет", ("packages",)),
    ("тип", ("types",)),
    ("эмуляц", ("emulation",)),
    ("функц", ("functions",)),
    ("инструмент", ("tool",)),
    ("конвертер", ("converter",)),
    ("исключени", ("exceptions",)),
    ("коллекц", ("collections",)),
    ("оператор", ("operator",)),
    ("синоним", ("synonyms",)),
    ("объект", ("objects",)),
    ("формат", ("format",)),
    ("дамп", ("dumps",)),
    ("распределённ", ("distributed",)),
    ("простой", ("downtime",)),
    ("простоя", ("downtime",)),
    ("пользовательск", ("user-defined",)),
    # --- a5 мультимодельность ---
    ("колоночн", ("columnar",)),
    ("хранилищ", ("storage",)),
    ("расширен", ("extended",)),
    ("обработк", ("processing",)),
    ("вектор", ("vector",)),
    ("поиск", ("search",)),
    ("нативн", ("native",)),
    ("сценар", ("scenarios",)),
    ("механизм", ("mechanisms", "mechanism")),
    ("механик", ("mechanisms",)),
    ("версионируем", ("versioned",)),
    ("внешн", ("external",)),
    ("доступ", ("access",)),
    ("графов", ("graph",)),
    # --- backup ---
    ("резервн", ("backup",)),
    ("копировани", ("backup", "copying")),
    ("копий", ("copies",)),
    ("копии", ("copies",)),
    ("копия", ("copy",)),
    ("копи", ("copies",)),
    ("бэкап", ("backup",)),
    ("инкрементальн", ("incremental",)),
    ("момент времени", ("point-in-time",)),
    ("дедупликац", ("deduplication",)),
    ("выгрузк", ("export",)),
    ("облак", ("cloud",)),
    ("политик", ("policies",)),
    ("хранени", ("retention", "storage")),
    ("тиражировани", ("tiering",)),
    ("компресси", ("compression",)),
    ("шифровани", ("encryption",)),
    ("неизменяем", ("immutable",)),
    ("контур", ("perimeter",)),
    ("полномочий", ("privileges", "duties")),
    ("администратор", ("administrator",)),
    ("криптографическ", ("cryptographic",)),
    ("удалени", ("erasure", "deletion")),
    ("верификац", ("verification",)),
    ("целостност", ("integrity",)),
    ("запуск", ("startup",)),
    ("гранулярн", ("granular",)),
    ("кроссплатформенн", ("cross-platform",)),
    ("автоматическ", ("automated",)),
    ("проверк", ("verification", "checking")),
    ("просмотр", ("browse", "viewing")),
    ("содержимого", ("contents",)),
    # --- management ---
    ("действий", ("actions",)),
    ("аудит", ("audit",)),
    ("аутентификац", ("authentication",)),
    ("двухфакторн", ("two-factor", "2fa")),
    ("ролевой", ("role-based",)),
    ("функциям", ("functions",)),
    ("секрет", ("secrets",)),
    ("менеджер", ("manager",)),
    ("чуж", ("third-party",)),
    ("опасн", ("dangerous",)),
    ("атрибут", ("attribute",)),
    ("плагин", ("plugins",)),
    ("канал", ("channels",)),
    ("оповещен", ("notifications",)),
    ("трафик", ("traffic",)),
    ("родной", ("native",)),
    ("интеграц", ("integration",)),
    ("внешний api", ("api",)),
    ("работа платформы", ("platform operations",)),
    # --- уточняющие частотные термины заголовков ---
    ("подтвержд", ("verified", "confirmed")),
    ("российск", ("russian",)),
    ("глобальн", ("global",)),
    ("компиляц", ("compilation",)),
    ("сквозн", ("end-to-end",)),
    ("сессионн", ("session",)),
    ("ролям", ("roles",)),
    ("создани", ("creation",)),
    ("отложенн", ("deferred",)),
    ("изменени", ("changes",)),
    ("мгновенн", ("instant",)),
    ("мгновенный запуск", ("instant recovery",)),
    ("файлов", ("file",)),
    ("вм", ("vm",)),
    ("тб", ("terabyte",)),
    ("1с", ("1c",)),
    ("работа", ("operations",)),
    ("нагрузочн", ("load-testing",)),
    ("суперпользовател", ("superuser",)),
    ("пдн", ("personal-data",)),
    ("одновременн", ("concurrent",)),
    ("последовательн", ("sequential",)),
    ("64-бит", ("64-bit",)),
    ("битн", ("64-bit",)),
    ("событий", ("events",)),
    ("порог", ("thresholds",)),
    ("сервер", ("server",)),
)

#: Фолбэк EN-термина, гарантирующий непустой ``en`` (warning ``empty-concept-terms``).
_MODULE_EN_FALLBACK: dict[str, tuple[str, ...]] = {
    "core": ("database",),
    "backup": ("backup",),
    "management": ("management",),
}

#: Курированное множество допустимых EN-токенов (глоссарий + технические идентификаторы).
#: Используется в валидации: «подозрительный транслит» = токен, которого здесь нет.
CURATED_EN_TOKENS: frozenset[str] = frozenset(
    {term for _phrase, terms in _RU_EN_GLOSSARY for term in terms}
) | _TECH_LATIN


# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------


def translit(text: str) -> str:
    """Транслитерирует RU→EN; латиница/цифры сохраняются, прочее → разделитель."""
    out: list[str] = []
    for ch in text.lower():
        if ch in _TRANSLIT:
            out.append(_TRANSLIT[ch])
        elif ch.isalnum() and ch.isascii():
            out.append(ch)
        else:
            out.append("_")
    slug = re.sub(r"_+", "_", "".join(out)).strip("_")
    return slug


def normalize_key(text: str) -> str:
    """Нормализация заголовка для сопоставления psq↔golden (alnum-lower)."""
    return re.sub(r"[^0-9a-zа-яё]+", "", str(text).lower())


def is_damaged(value: object) -> bool:
    """Повреждена ли ячейка psq (содержит маркер ``?"``)."""
    return isinstance(value, str) and '?"' in value


def clean_head(value: object) -> str:
    """Голова ячейки до маркера ``?"`` (обходит склейку/сдвиг колонок)."""
    if value is None:
        return ""
    return str(value).split('?"', 1)[0].strip()


def ru_tokens(text: str, *, min_len: int = 3, limit: int = 8) -> list[str]:
    """Значимые русские токены строки (для ``concept_terms.ru``)."""
    tokens = re.findall(r"[А-Яа-яЁёA-Za-z0-9][А-Яа-яЁёA-Za-z0-9.-]*", text)
    out: list[str] = []
    for token in tokens:
        low = token.lower()
        if len(low) < min_len or low in _RU_STOP:
            continue
        if low not in out:
            out.append(low)
        if len(out) >= limit:
            break
    return out


_LATIN_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9]*(?:[-_.][A-Za-z0-9]+)*")


def is_technical_latin(token: str) -> bool:
    """Технический ли латинский токен (``pg_*``/``dbms_*``/… или из канона)."""
    return token in _TECH_LATIN or token.startswith(_TECH_LATIN_PREFIXES)


def latin_tokens(text: str, *, limit: int = 12, keep_underscore: bool = True) -> list[str]:
    """Латинские токены строки (для ``concept_terms.en``), без служебных слов.

    Нормализует хвостовую пунктуацию; сохраняет дефисы/слэши/точки в компаундах
    (``avx-512``, ``pl/sql``), идентификаторы с ``_`` — только технические.
    """
    if not text:
        return []
    out: list[str] = []
    for raw in _LATIN_TOKEN_RE.findall(str(text)):
        token = raw.lower().strip(".-_")
        if len(token) < 2 or token in _LATIN_STOP or token.isdigit():
            continue
        # «_» допускаем только у технических идентификаторов (pg_catalog, dbms_sql, …).
        if "_" in token and not (keep_underscore and is_technical_latin(token)):
            continue
        if token not in out:
            out.append(token)
        if len(out) >= limit:
            break
    return out


def glossary_terms(text: str, *, limit: int = 6) -> list[str]:
    """EN-термины из курируемого глоссария по фрагменту RU-текста (``title``/семантика)."""
    if not text:
        return []
    low = str(text).lower()
    out: list[str] = []
    for phrase, terms in _RU_EN_GLOSSARY:
        if phrase in low:
            for term in terms:
                if term not in out:
                    out.append(term)
        if len(out) >= limit:
            break
    return out[:limit]


def build_concept_terms(
    title: str, semantics: str, *, module: str | None = None
) -> dict[str, list[str]]:
    """Собирает ``concept_terms``: RU — заголовок и ключевые слова; EN — латиница+глоссарий.

    RU-часть всегда непуста (заголовок входит первым) — линтер ``empty-concept-terms``
    не срабатывает. EN-часть непуста у всех параметров: латинские технические токены
    заголовка/семантики + курируемый глоссарий, а при их отсутствии — фолбэк по модулю.
    Транслитерация в EN **не** используется (только для ``param_id``).
    """
    meaningful = title.strip()
    ru: list[str] = [meaningful]
    for token in ru_tokens(title):
        if token not in ru:
            ru.append(token)

    en: list[str] = []

    def _add(term: str) -> None:
        if term and term not in en:
            en.append(term)

    # 1) латинские технические токены заголовка (высшая ценность).
    for token in latin_tokens(title, limit=6):
        _add(token)
    # 2) глоссарий по заголовку.
    for term in glossary_terms(title, limit=8):
        _add(term)
    # 3) добираем латиницей из семантики, если EN-список ещё беден.
    if len(en) < 5:
        for token in latin_tokens(clean_head(semantics), limit=4):
            _add(token)
    # 4) гарантия непустого en (§4.2/empty-concept-terms).
    if not en:
        for term in _MODULE_EN_FALLBACK.get(module or "", ("database",)):
            _add(term)
    return {"ru": ru, "en": en[:10]}


def slug_for(title: str, group_slug: str) -> str:
    """``param_id`` = ``{group_slug}__{translit(title)}`` c усечением до 64 (§4.2)."""
    mnemonic = translit(title)
    prefix = f"{group_slug}__"
    budget = _MAX_SLUG - len(prefix)
    mnemonic = mnemonic[:budget].strip("_")
    if not mnemonic:
        mnemonic = "param"
    return f"{prefix}{mnemonic}"


def ensure_unique(candidate: str, used: set[str]) -> str:
    """Гарантирует уникальность ``candidate`` суффиксом ``_2``, ``_3`` (в порядке следования)."""
    slug = candidate
    suffix = 2
    while slug in used:
        tail = f"_{suffix}"
        slug = candidate[: _MAX_SLUG - len(tail)].rstrip("_") + tail
        suffix += 1
    used.add(slug)
    return slug


# ---------------------------------------------------------------------------
# Чтение источников
# ---------------------------------------------------------------------------


def read_psq() -> list[dict]:
    """Читает лист ``final`` psq: список строк-параметров (119)."""
    import openpyxl  # extra 'extract' — нужен только реальному чтению xlsx

    wb = openpyxl.load_workbook(SRC_PSQ, data_only=True)
    ws = wb[PSQ_SHEET]
    rows: list[dict] = []
    for r in range(2, ws.max_row + 1):
        title = ws.cell(r, 3).value
        if title is None or str(title).strip() == "":
            continue
        rows.append(
            {
                "source_row": r,
                "module": ws.cell(r, 1).value,
                "group_raw": ws.cell(r, 2).value,
                "title": str(title).strip(),
                "importance": ws.cell(r, 4).value,
                "reference": ws.cell(r, 5).value,
                "semantics": ws.cell(r, 6).value,
                "rule_raw": ws.cell(r, 7).value,
                "damaged_cols": [
                    "ABCDEFGHI"[c - 1]
                    for c in range(1, 10)
                    if is_damaged(ws.cell(r, c).value)
                ],
            }
        )
    wb.close()
    return rows


def read_golden() -> dict[str, dict]:
    """Читает golden: ``{normalized_title: {postgrespro, pangolin}}`` (277 параметров)."""
    import openpyxl  # extra 'extract' — нужен только реальному чтению xlsx

    wb = openpyxl.load_workbook(SRC_GOLDEN, data_only=True)
    ws = wb[GOLDEN_SHEET]
    result: dict[str, dict] = {}
    for r in range(2, ws.max_row + 1):
        title = ws.cell(r, 3).value
        if title is None or str(title).strip() == "":
            continue
        result[normalize_key(str(title))] = {
            "source_row": r,
            "title": str(title).strip(),
            "postgrespro": ws.cell(r, 6).value,
            "pangolin": ws.cell(r, 7).value,
        }
    wb.close()
    return result


# ---------------------------------------------------------------------------
# Вывод по baselines
# ---------------------------------------------------------------------------


def derive_baseline(reference: object) -> dict | None:
    """Выводит строку baselines.csv из колонки E «Референс PG» (или ``None``).

    Решение консервативно (A3): точные PG-версии/``базлайн: <механизм>`` → ``direct``;
    расширения/orafce и всё неоднозначное → ``conditional``.
    """
    ref = "" if reference is None else str(reference).strip()
    if ref in {"", "—", "-"}:
        return None

    direct_versions = {"PG 13+", "PG 17+", "PG 12+ (частично)", "базлайн: PG 13+"}
    if ref in direct_versions:
        match = re.search(r"(\d+)", ref)
        return {
            "min_version": match.group(1) if match else None,
            "inheritance_mode": "direct",
            "note": None,
            "class": "version",
        }
    if ref.startswith("расш.:") or "orafce" in ref.lower():
        return {
            "min_version": None,
            "inheritance_mode": "conditional",
            "note": ref,
            "class": "extension",
        }
    if ref.lower().startswith("базлайн:"):
        mechanism = ref.split(":", 1)[1].strip()
        return {
            "min_version": None,
            "inheritance_mode": "direct",
            "note": mechanism,
            "class": "mechanism",
        }
    # Иные (upstream (актуальные/локали), PG 18+/11+/15+, PG 17/18 (частично)) —
    # консервативно conditional.
    return {
        "min_version": None,
        "inheritance_mode": "conditional",
        "note": ref,
        "class": "other",
    }


# ---------------------------------------------------------------------------
# Роли
# ---------------------------------------------------------------------------


def build_roles() -> list[dict]:
    """Реестр ролей пакета: роли каталога + сателлитные роли паспортов."""
    return [
        {
            "role_id": "core_engine",
            "definition": (
                "Ядро СУБД: архитектурные механизмы (транзакции/снимки/блокировки/хранение) "
                "и мультимодельные типы данных."
            ),
            "markers": [
                "ядро СУБД", "MVCC", "XID/MXID", "снимки CSN", "LWLock",
                "субтранзакции", "партиционирование", "мультимодельность",
                "векторный поиск", "JSONB", "JSONB/time-series", "columnar",
            ],
            "query_patterns": {
                "ru": ["архитектура ядра СУБД", "расширение счётчика транзакций", "мультимодельные типы данных"],
                "en": ["database kernel architecture", "64-bit transaction id", "multimodel database"],
            },
            "core_coverage_allowed": True,
            "multi_instance": False,
        },
        {
            "role_id": "query_planner",
            "definition": "Планировщик запросов: оптимизация планов, JIT, параллелизм, подсказки, статистика.",
            "markers": [
                "планировщик", "оптимизатор запросов", "JIT", "parallel SELECT",
                "pg_hint_plan", "HypoPG", "оценка кардинальности",
            ],
            "query_patterns": {
                "ru": ["планировщик запросов", "подсказки плана", "параллельные запросы"],
                "en": ["query planner", "query hints", "parallel query"],
            },
            "core_coverage_allowed": True,
            "multi_instance": False,
        },
        {
            "role_id": "app_integration",
            "definition": "Интеграция прикладных платформ (1С и смежные) с СУБД: профили нагрузки и совместимость.",
            "markers": ["интеграция 1С", "1С:Предприятие", "регламентные операции", "временные таблицы", "OLTP 1С"],
            "query_patterns": {
                "ru": ["интеграция с 1С", "производительность 1С на СУБД", "базы 1С большого объёма"],
                "en": ["1C application integration", "1C database performance", "ERP database profile"],
            },
            "core_coverage_allowed": False,
            "multi_instance": False,
        },
        {
            "role_id": "migration",
            "definition": "Миграция с иных СУБД (Oracle/MS SQL) на PostgreSQL: совместимость синтаксиса и перенос кода.",
            "markers": ["миграция Oracle", "PL/SQL", "T-SQL", "orafce", "конвертер кода", "синонимы объектов"],
            "query_patterns": {
                "ru": ["миграция с Oracle", "конвертация PL/SQL", "совместимость с MS SQL"],
                "en": ["Oracle migration", "PL/SQL conversion", "MS SQL compatibility"],
            },
            "core_coverage_allowed": False,
            "multi_instance": False,
        },
        {
            "role_id": "backup_recovery",
            "definition": "Резервное копирование и восстановление: базовые копии, PITR, дедупликация, защита копий.",
            "markers": [
                "резервное копирование", "PITR", "инкрементальный бэкап", "дедупликация",
                "WORM", "шифрование копий", "air-gap", "восстановление",
            ],
            "query_patterns": {
                "ru": ["резервное копирование СУБД", "восстановление на момент времени", "защита бэкапов от ransomware"],
                "en": ["database backup", "point in time recovery", "ransomware protection backup"],
            },
            "core_coverage_allowed": False,
            "multi_instance": True,
        },
        {
            "role_id": "management_platform",
            "definition": "Платформа управления и эксплуатации: консоль, RBAC, интеграция с IdP, аудит, API.",
            "markers": [
                "платформа управления", "консоль", "RBAC", "LDAP/AD", "OAuth/OIDC",
                "2FA", "аудит платформы", "REST API платформы",
            ],
            "query_patterns": {
                "ru": ["платформа управления СУБД", "ролевой доступ консоли", "интеграция с LDAP"],
                "en": ["database management platform", "console RBAC", "LDAP integration"],
            },
            "core_coverage_allowed": False,
            "multi_instance": True,
        },
        # --- Сателлитные роли (для паспортов продуктов; в каталоге не используются) ---
        {
            "role_id": "backup",
            "definition": "Внешнее решение резервного копирования (сателлит СУБД): сервер/шлюз копий, каталог, политики.",
            "markers": ["бэкап-сервер", "каталог бэкапов", "политики хранения"],
            "query_patterns": {
                "ru": ["сервер резервного копирования", "каталог резервных копий"],
                "en": ["backup server", "backup catalog"],
            },
            "core_coverage_allowed": False,
            "multi_instance": True,
        },
        {
            "role_id": "management_console",
            "definition": "Консоль/панель управления (сателлит): веб-интерфейс, автоматизация, агенты.",
            "markers": ["веб-консоль", "панель управления", "агент управления"],
            "query_patterns": {
                "ru": ["веб-консоль управления СУБД"],
                "en": ["web management console"],
            },
            "core_coverage_allowed": False,
            "multi_instance": True,
        },
        {
            "role_id": "in_memory_cache",
            "definition": "Кэш в оперативной памяти (сателлит): Redis-совместимый кэш для ускорения приложений.",
            "markers": ["Redis", "in-memory cache", "кэш сессий"],
            "query_patterns": {
                "ru": ["кэш в оперативной памяти"],
                "en": ["in-memory cache"],
            },
            "core_coverage_allowed": False,
            "multi_instance": True,
        },
        {
            "role_id": "ha_orchestrator",
            "definition": "Оркестратор высокой доступности и отказоустойчивости (сателлит): failover, репликация.",
            "markers": ["Patroni", "failover", "менеджер репликации"],
            "query_patterns": {
                "ru": ["оркестратор высокой доступности"],
                "en": ["high availability orchestrator", "failover manager"],
            },
            "core_coverage_allowed": False,
            "multi_instance": True,
        },
        {
            "role_id": "identity_provider",
            "definition": "Внешний провайдер идентичности (сателлит): LDAP/AD/OIDC/SSO.",
            "markers": ["LDAP", "Active Directory", "OIDC", "SSO"],
            "query_patterns": {
                "ru": ["провайдер идентичности", "единый вход"],
                "en": ["identity provider", "single sign-on"],
            },
            "core_coverage_allowed": False,
            "multi_instance": False,
        },
        {
            "role_id": "monitoring",
            "definition": "Система мониторинга (сателлит): метрики, экспортёры, алертинг.",
            "markers": ["Prometheus", "экспортёр метрик", "алертинг"],
            "query_patterns": {
                "ru": ["мониторинг СУБД"],
                "en": ["database monitoring", "metrics exporter"],
            },
            "core_coverage_allowed": False,
            "multi_instance": True,
        },
    ]


def build_domain_profiles() -> dict[str, dict[str, object]]:
    """Профили доменов (``domain_profiles.yaml``) в схеме D.2 (``03`` §D.2/D4).

    Оси :class:`~core.config.domain_profiles.DomainProfile` (``extra="forbid"``):
    ``fetch`` (``http``|``browser``), ``extract`` (``auto``|``html``|``document``),
    ``strategy`` (``auto``|``honest``|``hybrid``), ``wait_for`` (JS-условие готовности),
    плюс бюджетные оси контроля краула ``max_depth``/``max_documents`` и per-site
    ``sitemap`` (D3). Ключ — ``host`` с опциональным ``/path`` (приоритет резолвит pipeline).

    Значения согласованы с живыми замерами (2026-10): ``*.sbertech.ru`` отдают 401
    обычному HTTP (антибот) → ``browser``; ``pangolin.sbertech.ru`` дополнительно требует
    ``wait_for`` (JS-challenge завершается и перезагружает страницу после
    ``domcontentloaded``); ``tantorlabs.ru/products/dbms`` — 403 обычному HTTP → ``browser``;
    остальные домены отдают 200 обычному HTTP.

    **Ловушка матчинга sitemap (P7).** ``_sitemap_url_for(seed)`` берёт **самый
    специфичный** профиль (``sbertech.ru/documents`` приоритетнее ``sbertech.ru``), поэтому
    host-wide ``https://sbertech.ru/sitemap.xml`` объявлен только на path-ключе
    ``sbertech.ru/documents``: на host-ключе он при seed-корне ``https://sbertech.ru/``
    затянул бы в партицию все 89 продуктов (нарушение I17). Host-ключ намеренно без sitemap.

    **Host-root seed ≠ продуктовый seed (I17).** Корень сайта компании — не корень
    продукта. Любой discovery на host-корне мульти-продуктового сайта (BFS или host-wide
    sitemap) затянет в партицию одного продукта страницы соседних продуктов. Поэтому
    host-ключи ``sbertech.ru``, ``gaz-is.ru`` и ``wiki.astralinux.ru`` объявлены с
    ``max_depth: 0`` (только сам корень) и малым ``max_documents``; обход живёт на
    path-ключе продукта (``sbertech.ru/documents``,
    ``gaz-is.ru/produkty/inform-sistemy/subd-jatoba``, ``wiki.astralinux.ru/tandocs`` —
    Confluence-пространство TANDOCS «База Знаний Tantor»).

    **Sitemap — только там, где он даёт покрытие.** Path-scoped seed внутри host-wide
    sitemap почти не получает ``<loc>`` в scope (``platformv.sbertech.ru`` — 1 из 801,
    ``tantorlabs.ru/products/dbms`` — 3 из 58) и лишь порождает запись об отказе на каждом
    прогоне, поэтому sitemap объявлен на host-ключах, а не на этих path-ключах.

    **Бюджеты — по измеренному размеру, а не «круглым» числом.** Где sitemap даёт точный
    размер, ``max_documents`` покрывает его с запасом (``tantorlabs.ru`` — 283 URL ⇒ 300;
    ``sbertech.ru/documents`` — в scope seed'а ~22 URL pangolin ⇒ 60). Где сайт крошечный,
    бюджет не раздувается (``jatoba.ru`` — 2 URL ⇒ 50, ``pangolin.sbertech.ru`` — 23 URL
    ⇒ 60). Где размер не измерен (sitemap 404/500), бюджет — явный потолок, а не заявка
    на полноту (``postgrespro.ru`` и ``docs.tantorlabs.ru`` — 800; ``gaz-is.ru`` product —
    200). Host-корни при ``max_depth: 0`` получают 10: обходится только сам корень.

    Набор ключей совпадает с эталонным ``packages/ru-pg-dbms/domain_profiles.yaml``
    (повторный прогон генератора идемпотентен и не затирает домены).

    Функция — чистая (без I/O/сети): её выход проверяется тестом против строгой
    модели, чтобы дрейф схемы (старые ``render``/``index``) не вернулся.
    """
    return {
        "docs.tantorlabs.ru": {
            "fetch": "http",
            "extract": "auto",
            "max_depth": 8,
            "max_documents": 800,
        },
        # Host-root seed (https://www.gaz-is.ru/) — корень компании, а не продукта:
        # любой обход с depth > 0 затянет в партицию jatoba чужие продукты (I17).
        # При depth 0 обходится только сам корень ⇒ бюджет 10 (как у прочих host-корней).
        "gaz-is.ru": {
            "fetch": "http",
            "extract": "auto",
            "max_depth": 0,
            "max_documents": 10,
        },
        # Продуктовый путь jatoba (реальная раскладка сайта — /produkty/...: robots.txt
        # перечисляет /produkty/inform-sistemy/subd-jotoba, он 302 → .../subd-jatoba).
        # Прежний seed /products/jatoba/ отдавал 404 и исправлен в паспорте jatoba
        # (corpus_urls.yaml регенерирован). Внутри поддерева обход уместен.
        # Замер 2026-10: с лендинга продукта в scope ведут 2 URL; 200 — потолок с запасом.
        "gaz-is.ru/produkty/inform-sistemy/subd-jatoba": {
            "fetch": "http",
            "extract": "auto",
            "max_depth": 8,
            "max_documents": 200,
        },
        # 2 URL, из них 1 PDF; sitemap даёт depth-0 seeds, max глубина пути — 1.
        "jatoba.ru": {
            "fetch": "http",
            "extract": "auto",
            "max_depth": 2,
            "max_documents": 50,
            "sitemap": "https://jatoba.ru/sitemap.xml",
        },
        # 23 URL, плоско — depth 1 + один хоп на документ; 60 покрывает 23 с запасом.
        "pangolin.sbertech.ru": {
            "fetch": "browser",
            "extract": "auto",
            "strategy": "honest",
            "wait_for": "() => document.body && document.body.innerText.length > 200",
            "max_depth": 2,
            "max_documents": 60,
            "sitemap": "https://pangolin.sbertech.ru/sitemap.xml",
        },
        "platformv.sbertech.ru": {
            "fetch": "browser",
            "extract": "auto",
            "strategy": "honest",
            "max_depth": 8,
            "max_documents": 300,
            # Sitemap намеренно НЕ задан: seed — глубокий путь внутри host-wide sitemap
            # (801 <loc>, в scope 1) ⇒ почти no-op плюс запись об отказе на каждом прогоне.
        },
        # Sitemap 404 (advertised) ⇒ размер не измерен; 800 — потолок для крупного
        # docs-портала (несколько версий документации), а не заявка на полноту.
        "postgrespro.ru": {
            "fetch": "http",
            "extract": "auto",
            "max_depth": 8,
            "max_documents": 800,
        },
        # Sitemap намеренно НЕ на host-ключе (см. ловушку матчинга в докстринге):
        # host-wide <loc> затянул бы другие продукты в партицию якоря (I17).
        # Host-root seed (https://sbertech.ru/) — корень компании: discovery запрещён
        # (max_depth 0), иначе BFS уходит в /documents → 89 продуктов (I17).
        "sbertech.ru": {
            "fetch": "browser",
            "extract": "auto",
            "strategy": "honest",
            "max_depth": 0,
            "max_documents": 10,
        },
        # Path-scope: из 748 <loc> host-wide sitemap 502 — /documents (501 документ + индекс)
        # на 89 продуктов; фильтр scope (дефект B) оставляет 22 URL pangolin ⇒ 60 с запасом.
        # Глубина 2: sitemap даёт subordinate seeds на depth 0, +1 хоп; depth 4 — no-op.
        "sbertech.ru/documents": {
            "fetch": "browser",
            "extract": "auto",
            "strategy": "honest",
            "max_depth": 2,
            "max_documents": 60,
            "sitemap": "https://sbertech.ru/sitemap.xml",
        },
        # 283 URL (sitemap-индекс: 58 <loc> + дочерние фиды), 3 уровня on-host; 300 — запас.
        "tantorlabs.ru": {
            "fetch": "browser",
            "extract": "auto",
            "strategy": "honest",
            "max_depth": 3,
            "max_documents": 300,
            "sitemap": "https://tantorlabs.ru/sitemap.xml",
        },
        "tantorlabs.ru/products/dbms": {
            "fetch": "browser",
            "extract": "auto",
            "strategy": "honest",
            "max_depth": 8,
            "max_documents": 200,
            # Sitemap намеренно НЕ задан: path-scoped seed внутри host-wide sitemap
            # (58 <loc>, в scope 3) ⇒ почти no-op плюс запись об отказе на каждом прогоне.
            # Host-ключ tantorlabs.ru свой sitemap сохраняет. 200 — потолок продукта
            # (весь хост — 283 URL), 800 размером не подтверждён.
        },
        # Host-root seed (https://wiki.astralinux.ru/) — корень общего справочного центра
        # Astra Linux, а не продукта: discovery с depth > 0 затянет в партицию tantor чужие
        # страницы (I17). Тантор-контент — Confluence-пространство TANDOCS («База Знаний
        # Tantor»), публичные URL — /tandocs/<slug>-<id>.html (замер 2026-10: 68 уникальных
        # /tandocs-ссылок с домашней страницы пространства; /display/TANDOCS 302 →
        # /tandocs/baza-znanij-tantor-238752972.html). Префикс — /tandocs: path-матч
        # литеральный, /display/TANDOCS после 302 вне scope.
        "wiki.astralinux.ru": {
            "fetch": "http",
            "extract": "auto",
            "max_depth": 0,
            "max_documents": 10,
        },
        "wiki.astralinux.ru/tandocs": {
            "fetch": "http",
            "extract": "auto",
            "max_depth": 8,
            "max_documents": 300,
        },
    }


# ---------------------------------------------------------------------------
# Генерация артефактов
# ---------------------------------------------------------------------------


def load_lock() -> dict[tuple[str, str], str]:
    """Читает существующий ``catalog.lock.csv`` → ``{(source_file, source_row): param_id}``."""
    path = PACKAGE_DIR / "catalog.lock.csv"
    lock: dict[tuple[str, str], str] = {}
    if not path.is_file():
        return lock
    with path.open("r", encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            key = (row["source_file"], str(row["source_row"]))
            lock[key] = row["param_id"]
    return lock


def yaml_dump(data: object) -> str:
    """Детерминированный YAML (``sort_keys``, unicode)."""
    return yaml.safe_dump(data, allow_unicode=True, sort_keys=True, default_flow_style=False)


def write_text(name: str, text: str) -> None:
    """Пишет текстовый артефакт рядом с генератором (родитель ``sources/``)."""
    (PACKAGE_DIR / name).write_text(text, encoding="utf-8")


def write_csv(name: str, header: list[str], rows: list[list[object]]) -> None:
    """Пишет CSV с фиксированным заголовком/порядком и ``\\n`` (байт-детерминизм)."""
    with (PACKAGE_DIR / name).open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def main() -> int:
    psq = read_psq()
    golden = read_golden()
    lock = load_lock()
    used_slugs: set[str] = set()

    # --- catalog.csv + derived_fields --------------------------------------
    catalog_rows: list[list[object]] = []
    lock_rows: list[list[object]] = []
    derived_rows: list[list[object]] = []
    baseline_rows: list[list[object]] = []
    param_by_row: dict[int, str] = {}
    needs_human = 0
    fallback_count = 0
    shift_fragments = 0
    corrupted_rows = 0

    for row in psq:
        module = MODULE_MAP[row["module"]]
        group = GROUP_MAP[row["group_raw"]]
        role_binding = GROUP_ROLE_MAP[group]

        title = row["title"]
        lock_key = (PSQ_SOURCE_REF, str(row["source_row"]))
        if lock_key in lock:
            param_id = lock[lock_key]
        else:
            param_id = ensure_unique(slug_for(title, group), used_slugs)
        used_slugs.add(param_id)
        param_by_row[row["source_row"]] = param_id

        # rule: чистая G → source; иначе (G повреждена/пуста ИЛИ строка сдвинута из F)
        # fallback на семантику F (до первого ?"). Сдвиг F означает, что голова G
        # «утекла» в F, поэтому G — фрагмент и доверять ему нельзя.
        g_raw = row["rule_raw"]
        g_damaged = is_damaged(g_raw) or not (g_raw and str(g_raw).strip())
        f_damaged = is_damaged(row["semantics"])
        if not (g_damaged or f_damaged):
            rule = str(g_raw).strip()
            rule_origin = "source"
        else:
            body = clean_head(row["semantics"])[: 300 - len("Засчитывается: ")]
            rule = f"Засчитывается: {body}" if body else "Засчитывается: (семантика отсутствует)"
            rule_origin = "needs_human"
            fallback_count += 1

        # Помечаем строку needs_human, если повреждены F или G (сдвиг колонок).
        row_damaged = g_damaged or f_damaged
        if row_damaged:
            needs_human += 1
        if row["damaged_cols"]:
            corrupted_rows += 1
        if (not g_damaged) and f_damaged:
            shift_fragments += 1

        concept_terms = build_concept_terms(
            title, clean_head(row["semantics"]), module=module
        )
        catalog_rows.append(
            [
                param_id,
                title,
                module,
                group,
                str(row["importance"]).strip(),
                "fact",
                rule,
                "distribution_absence",
                role_binding,
                # JSON в CSV: csv.writer сам закавычит и удвоит внутренние кавычки.
                _json_compact(concept_terms),
                "ru|en",
            ]
        )
        lock_rows.append([param_id, PSQ_SOURCE_REF, row["source_row"], title, GENERATED_AT])

        # --- derived_fields: выведенные/сопоставленные поля (A3) -----------
        note = ""
        if row["damaged_cols"]:
            note = "повреждённые колонки psq: " + ",".join(row["damaged_cols"])
        derived_rows.append([param_id, "title", title, "source", "колонка C psq дословно", note])
        derived_rows.append([param_id, "module", module, "mapped", "A psq → module_id", row["module"]])
        derived_rows.append([param_id, "group", group, "mapped", "B psq → ASCII-slug", row["group_raw"]])
        derived_rows.append([param_id, "importance", str(row["importance"]).strip(), "source", "колонка D psq", ""])
        derived_rows.append([param_id, "param_type", "fact", "derived", "нет гарантированной публичной величины → fact", ""])
        derived_rows.append([param_id, "absence_rule", "distribution_absence", "mapped", "политика пакета для технических fact-параметров", ""])
        derived_rows.append([param_id, "role_binding", role_binding, "mapped", "group → роль", group])
        derived_rows.append([param_id, "concept_terms", _json_compact(concept_terms), "derived", "title (RU) + латинские токены и глоссарий RU→EN (EN)", ""])
        derived_rows.append([param_id, "rule", rule, rule_origin, "чистая G → source; иначе fallback F", note])
        derived_rows.append(
            [
                param_id,
                "row_integrity",
                ",".join(row["damaged_cols"]) if row["damaged_cols"] else "clean",
                "needs_human" if row["damaged_cols"] else "source",
                "проверка повреждаемости ячеек psq (F..I)",
                note,
            ]
        )
        derived_rows.append([param_id, "param_id", param_id, "derived", "translit(title) с префиксом группы", ""])

        # --- baselines ------------------------------------------------------
        baseline = derive_baseline(row["reference"])
        if baseline is not None:
            baseline_rows.append(
                [
                    BASELINE_ID,
                    param_id,
                    baseline["min_version"] or "",
                    baseline["inheritance_mode"],
                    baseline["note"] or "",
                ]
            )
            derived_rows.append(
                [
                    param_id,
                    "baseline",
                    f"{BASELINE_ID}|{baseline['inheritance_mode']}|{baseline['min_version'] or ''}",
                    "derived",
                    f"колонка E psq ({baseline['class']})",
                    str(row["reference"]),
                ]
            )

    # --- golden.csv (118 сопоставленных × 2 продукта = 236) ----------------
    golden_rows: list[list[object]] = []
    matched = 0
    for row in psq:
        key = normalize_key(row["title"])
        entry = golden.get(key)
        if entry is None:
            continue
        matched += 1
        param_id = param_by_row[row["source_row"]]
        for product_id, status in (
            ("postgrespro", entry["postgrespro"]),
            ("pangolin", entry["pangolin"]),
        ):
            status = str(status).strip()
            if status == "✅":
                evidence, case_class = "documentation", "class_ok"
            elif status == "⚠️":
                evidence, case_class = "documentation|web", "class_partial"
            elif status == "❌":
                evidence, case_class = "distribution", "class_absent"
            else:  # pragma: no cover — источники проверены; страховка от выдумывания
                raise ValueError(f"golden: неизвестный статус {status!r} для {row['title']!r}")
            golden_rows.append(
                [product_id, param_id, status, evidence, case_class, "full", "source=golden_091"]
            )

    # --- запись артефактов -------------------------------------------------
    write_csv(
        "catalog.csv",
        ["param_id", "title", "module", "group", "importance", "param_type",
         "rule", "absence_rule", "role_binding", "concept_terms", "search_langs"],
        catalog_rows,
    )
    write_csv(
        "catalog.lock.csv",
        ["param_id", "source_file", "source_row", "title", "generated_at"],
        lock_rows,
    )
    write_csv(
        "baselines.csv",
        ["baseline_id", "param_id", "min_version", "inheritance_mode", "note"],
        baseline_rows,
    )
    write_csv(
        "golden.csv",
        ["product_id", "param_id", "expected_status", "expected_evidence_kind",
         "case_class", "tier", "note"],
        golden_rows,
    )
    write_csv(
        "derived_fields.csv",
        ["param_id", "field", "value", "origin", "rule", "note"],
        derived_rows,
    )

    package_yaml = {
        "case_id": CASE_ID,
        "package_version": PACKAGE_VERSION,
        "archetype": "A",
        "modules": [{"module_id": mid, "weight": weight} for mid, weight in MODULE_WEIGHTS],
        "roles": build_roles(),
        "baselines": [BASELINE_ID],
        "baseline_participant": BASELINE_PARTICIPANT,
        "jurisdictions": ["RU"],
        "registries": [],
        "languages": ["ru", "en"],
    }
    write_text("package.yaml", yaml_dump(package_yaml))

    cohort_yaml = {
        "case_id": CASE_ID,
        "frozen_at": SNAPSHOT_DATE,
        "entries": [
            {
                "product_id": "pangolin",
                "role_overlap": 1.0,
                "selection_reason": "Якорь/объект сравнения: Platform V Pangolin DB.",
                "anchor_relation": "anchor",
            },
            {
                "product_id": "postgrespro",
                "role_overlap": 1.0,
                "selection_reason": "Российский лидер PG-СУБД; максимальное перекрытие ролей с якорем.",
                "anchor_relation": "analog",
            },
            {
                "product_id": "tantor",
                "role_overlap": 0.9,
                "selection_reason": "Российская СУБД на PostgreSQL; высокое перекрытие ролей.",
                "anchor_relation": "analog",
            },
            {
                "product_id": "jatoba",
                "role_overlap": 0.7,
                "selection_reason": "Российская СУБД на PostgreSQL; частичное перекрытие ролей.",
                "anchor_relation": "analog",
            },
        ],
    }
    write_text("cohort.yaml", yaml_dump(cohort_yaml))

    run_yaml = {
        "case_id": CASE_ID,
        "anchor": "pangolin",
        "cohort": f"cohort-{CASE_ID}-{SNAPSHOT_DATE}",
        "products": ["pangolin", "postgrespro", "tantor", "jatoba"],
        "package": CASE_ID,
        "focus": {"modules": ["core", "backup", "management"], "params": []},
        "languages": ["ru", "en"],
        "snapshot_date": SNAPSHOT_DATE,
        "mode": "strict",
        "ranking_policy": "capability_ranked",
        # Операционные (execution) дефолты краула — не методология (§13). Генератор
        # обязан эмитить тот же блок, что лежит в ``run.yaml`` (иначе повторная
        # генерация молча обнулит бюджеты обхода — дрейф, P7).
        "crawl": {
            "strategy": "honest",
            "rate_limit": {"min_s": 1.0, "max_s": 3.0},
            "max_depth": 8,
            "max_documents": 300,
            "max_workers": 2,
            "extract_workers": 1,
            "document_level": "layout_tables",
            "exclude_paths": [
                "/blog/*",
                "/news/*",
                "/events/*",
                "/cases/*",
                "/mediacenter/*",
            ],
            "exclude_documents": [
                "*.zip", "*.rar", "*.7z", "*.gz", "*.tar", "*.exe", "*.msi", "*.dmg",
                "*.iso", "*.mp4", "*.avi", "*.css", "*.js", "*.png", "*.jpg", "*.jpeg",
                "*.gif", "*.svg", "*.webp", "*.ico", "*.woff", "*.woff2", "*.ttf",
            ],
        },
    }
    write_text("run.yaml", yaml_dump(run_yaml))

    scope_yaml = {
        "procedure": (
            "Человеческая процедура аттестации полноты дистрибутива/документации (I2b): "
            "для каждого продукта когорты ответственный эксперт перечисляет все "
            "поставляемые компоненты и разделы документации (release notes, "
            "administration guide, reference) на зафиксированную дату snapshot_date; "
            "проверяет, что среди них нет умолчанного носителя заявленного механизма, и "
            "подтверждает исчерпывающий охват источником (source_scope=exhaustive) "
            "личной подписью scope_attested_by в листе аттестации пакета. При "
            "невозможности подтвердить полноту источник используется с "
            "source_scope=partial и не даёт права на ❌ по distribution_absence."
        )
    }
    write_text("scope_attestation.yaml", yaml_dump(scope_yaml))

    write_text("domain_profiles.yaml", yaml_dump(build_domain_profiles()))

    provenance_yaml = {
        "case_id": CASE_ID,
        "snapshot_date": SNAPSHOT_DATE,
        "generated_at": GENERATED_AT,
        "generator": "packages/ru-pg-dbms/sources/build_package.py",
        "sources": [
            {
                "path": str(SRC_PSQ),
                "sheet": PSQ_SHEET,
                "sha256": _sha256(SRC_PSQ),
                "role": "технический каталог 119 параметров",
            },
            {
                "path": str(SRC_GOLDEN),
                "sheet": GOLDEN_SHEET,
                "sha256": _sha256(SRC_GOLDEN),
                "role": "эталонные статусы (golden)",
            },
        ],
    }
    write_text("sources/provenance.yaml", yaml_dump(provenance_yaml))

    # --- отчёт -------------------------------------------------------------
    print(f"catalog rows:  {len(catalog_rows)}")
    print(f"baselines:     {len(baseline_rows)}")
    print(f"golden rows:   {len(golden_rows)} (matched psq params: {matched})")
    print(f"derived rows:  {len(derived_rows)}")
    print(f"needs_human:   {needs_human} (fallback rule: {fallback_count} = "
          f"повреждена G: {fallback_count - shift_fragments} + сдвиг из F: {shift_fragments})")
    print(f"corrupted rows (любая из F..I): {corrupted_rows}")
    return 0


def _sha256(path: Path) -> str:
    """sha256 файла-источника (провенанс)."""
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_compact(data: object) -> str:
    """Компактный JSON с unicode (для ячейки ``concept_terms``)."""
    import json

    return json.dumps(data, ensure_ascii=False, sort_keys=False, separators=(",", ":"))


if __name__ == "__main__":
    sys.exit(main())
