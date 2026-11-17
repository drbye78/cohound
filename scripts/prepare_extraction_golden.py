#!/usr/bin/env python3
"""Подготовка **набора-кандидата** PDF-эталона извлечения (O38б + D10 плана 26).

Что этот файл делает и чего он **не** делает. Набор PDF-эталона (30–50 документов × пять
направлений: полнота, структурная верность, порядок чтения, идентификаторы, провенанс)
размечает **человек** — это решение D10. Задача инструмента — снять с человека рутину,
которая иначе делается глазами по всему корпусу: найти документы, где каждое из пяти
направлений вообще имеет предмет, и положить их листом XLSX с числами, по которым решение
принимается. **Ни одно поле разметки не заполняется автоматически**: колонки решений пусты,
пока человек их не заполнит, а собранный набор несёт ``status: candidate`` — состояние
«человек ещё не подтвердил».

Подкоманды:

``candidates``
    Выбирает набор-кандидат из **run-DB** (``corpus_docs``, только чтение) и пишет XLSX:
    лист «кандидаты» (строка = документ), лист «как размечать» (инструкция живёт в файле, а
    не в переписке) и лист ``policy`` (машинные условия сборки: их читает ``compile``).

    Отбор **стратифицирован** по направлениям, а не «первые N по алфавиту»: каждая страта
    — это документ, у которого у соответствующего направления есть предмет. Порядок страт
    и квоты объявлены (:data:`STRATA`), остаток добирается детерминированно по
    ``sha256(seed|doc_id)`` с чередованием продуктов. Тот же ``--seed`` и тот же корпус дают
    тот же набор: выбор воспроизводим, а не «как получилось».

    **Двухколоночность определяет прибор**, а не этот скрипт: страница считается
    двухколоночной, если ``scripts/measure_reading_order.py`` нашёл на ней канавку и
    колонки **прозы** (``reason == "two_column"``), а не таблицу. Прибор не публикует
    отдельной функции «вердикт по документу», поэтому вызывается его собственная
    постраничная точка решения — это единственный носитель ответа, и копировать её правила
    сюда значило бы завести вторую реализацию, которая разъедется молча. Если прибор
    недоступен (``--no-reading-order`` или нет ``pypdfium2``), колонка остаётся пустой и
    это **названо** в ``policy``: «двухколоночные не идентифицированы прибором».

    Документ — PDF по **сигнатуре байтов** (``%PDF``), а не по суффиксу URL: retained-файл
    называется ``{sha256}.bin``, и суффикса у него нет вовсе.

``compile``
    Заполненный XLSX → ``extraction_golden.json`` в формате
    :data:`core.pipeline.extraction_eval.EXTRACTION_GOLDEN_FORMAT` (те же поля кейса, что у
    прогона). Отказ (``return 2``, ``ОТКАЗ:`` в stderr) на: строке без решения ``include``;
    неизвестном значении направления; кейсе без единого ожидания (эталон, который проходит
    всё, не проверяет ничего); отсутствующих по ``raw_path`` байтах; наборе без единого
    принятого документа («пустой лист не превращается в пустой эталон»); разметке
    ``reading_order=columns`` на документе, который прибор двухколоночным **не** назвал.

    Отсутствие двухколоночного документа в наборе — **предупреждение**, а не отказ: прибор
    мог не найти ни одного (на пилотном корпусе их ноль — O37), и отказ заблокировал бы
    набор из-за свойства корпуса, а не из-за ошибки разметки.

``validate``
    Перечитывает собранный JSON **тем же загрузчиком**, что и прогон, и печатает сводку;
    отказ, если файла нет или формат чужой.

``summary``
    Числа листа: строки, решения, продукты, страты, двухколоночные; и числа набора, если
    JSON уже собран.

Детерминизм: ни времени, ни ``random`` в файле набора нет — два прогона ``compile`` на одном
листе дают один и тот же JSON (дата решения человека живёт в git, а не в артефакте).

Нейтрален (§1.3): ни продуктов, ни домена — БД прогона и каталог пакета приходят аргументами.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import sqlite3
import sys
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Корень репозитория на sys.path: скрипт запускается как `python scripts/…py`, и тогда
# sys.path[0] — каталог scripts, а не корень (плоская раскладка пакета `core`).
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.pipeline.extraction_eval import (  # noqa: E402 — после правки sys.path
    EXTRACTION_GOLDEN_FORMAT,
    EXTRACTION_GOLDEN_STATUSES,
    load_extraction_golden_document,
)

# ---------------------------------------------------------------------------
# Лист разметчика: имена листов, колонки, словари значений
# ---------------------------------------------------------------------------
SHEET_CANDIDATES = "кандидаты"
SHEET_GUIDE = "как размечать"
SHEET_POLICY = "policy"

#: Пять направлений O38б — ровно те, по которым набор и размечается.
DIRECTIONS: tuple[str, ...] = (
    "completeness",
    "structure",
    "reading_order",
    "identifiers",
    "provenance",
)

#: Решение по документу. Пусто — «ещё не рассмотрено», и ``compile`` на этом откажет.
INCLUDE_VALUES: tuple[str, ...] = ("yes", "no")

#: Словари направлений. Пустое значение = «направление для этого документа не размечается»
#: (это осознанный пропуск, а не забытая ячейка: решение по документу — колонка ``include``).
DIRECTION_VALUES: dict[str, tuple[str, ...]] = {
    # полнота: текст обязан быть полным / частичная потеря допустима (тогда ``min_chars``)
    "completeness": ("full", "partial_ok", "skip"),
    # структурная верность: таблицы обязаны выжить как структура / их в документе нет
    "structure": ("expect_tables", "no_tables", "skip"),
    # порядок чтения: колонки обязаны читаться по колонкам / не проверяется
    "reading_order": ("columns", "skip"),
    # идентификаторы: в тексте обязаны выжить перечисленные в expected_substrings
    "identifiers": ("expect", "skip"),
    # провенанс: страница каждой цитаты обязана выводиться из карты страниц
    "provenance": ("pages_required", "skip"),
}

#: Колонки листа «кандидаты»: сначала **измеренное** (по нему человек решает), затем
#: колонки решений (пустые), затем поля кейса, которые из решений получаются.
CANDIDATE_COLUMNS: tuple[str, ...] = (
    # измеренное (заполнено инструментом, руками не правится)
    "case_id",
    "doc_id",
    "product_id",
    "url",
    "raw_path",
    "pages",
    "pages_converted",
    "chars",
    "chars_per_page",
    "chars_per_page_basis",
    "extractor_version",
    "pdf_pipeline",
    "partial",
    "two_column",
    "two_column_pages",
    "table_like_pages",
    "tables",
    "page_map",
    "ocr_probe",
    "stratum",
    "why",
    # решения человека
    "include",
    "completeness",
    "structure",
    "reading_order",
    "identifiers",
    "provenance",
    "min_chars",
    "expected_substrings",
    "forbidden_substrings",
    "expect_error",
    "notes",
)

#: Порог «подозрительно мало текста на страницу» для страты ``low_text``. Не потолок и не
#: вердикт: это признак, по которому документ попадает в набор — решает человек.
LOW_TEXT_CHARS_PER_PAGE = 200

#: Документ считается многостраничным (страта ``multi_page``) начиная с этого числа страниц.
MULTI_PAGE_MIN = 10

#: Сколько страниц документа смотрит прибор порядка чтения (0 — все).
DEFAULT_READING_ORDER_PAGES = 8

#: Объявленный объём набора по умолчанию (план 26 §O38б: 30–50 документов).
DEFAULT_TARGET = 40

#: Разделитель списков в ячейках листа (``expected_substrings``/``forbidden_substrings``).
LIST_SEPARATOR = "|"


# ---------------------------------------------------------------------------
# Run-DB: чтение только на чтение
# ---------------------------------------------------------------------------
def _ro_connect(path: Path) -> sqlite3.Connection:
    """Соединение только на чтение: запись в БД прогона запрещена контрактом инструмента."""
    uri = f"file:{path.expanduser().resolve()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=5000")
    return connection


def read_run_meta(conn: sqlite3.Connection, run_id: str | None) -> tuple[str, str | None]:
    """``(run_id, package_id)`` прогона; без ``--run-id`` — единственный прогон в БД.

    :raises SystemExit: прогонов несколько (нужен ``--run-id``) или их нет.
    """
    rows = conn.execute("SELECT run_id, package_id FROM runs ORDER BY run_id").fetchall()
    if run_id:
        for row in rows:
            if str(row["run_id"]) == run_id:
                return str(row["run_id"]), row["package_id"]
        raise SystemExit(
            f"extraction-golden: в БД нет прогона {run_id!r} "
            f"(есть: {', '.join(str(r['run_id']) for r in rows[:5])})"
        )
    if len(rows) == 1:
        return str(rows[0]["run_id"]), rows[0]["package_id"]
    if not rows:
        raise SystemExit("extraction-golden: в БД нет ни одного прогона")
    raise SystemExit(
        f"extraction-golden: в БД {len(rows)} прогонов — укажите --run-id"
    )


def corpus_columns(conn: sqlite3.Connection) -> set[str]:
    """Колонки ``corpus_docs`` **этой** схемы.

    Схема БД не мигрирует (AGENTS §9): у прогона прошлой версии нужной колонки может не
    быть. Отсутствие колонки — это «не измерено в этой схеме», и оно обязано быть видно
    как пустая ячейка и строка в ``policy``, а не как ``0``/``no``.
    """
    return {str(row["name"]) for row in conn.execute("PRAGMA table_info(corpus_docs)")}


def read_schema_version(conn: sqlite3.Connection) -> str:
    """Объявленная версия схемы прогона (``schema_meta`` — таблица ``key/value``).

    Набор собирается из **этого** корпуса, поэтому версия схемы — часть его провенанса:
    прогон прошлой версии читается инструментом (он только читает), но корпус в нём мог
    быть извлечён другим кодом, и читатель обязан это видеть, а не догадываться.
    ``""`` — версия не объявлена (таблицы/строки нет).
    """
    try:
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()
    except sqlite3.Error:  # pragma: no cover — таблицы нет в очень старых файлах
        return ""
    return str(row["value"]) if row and row["value"] else ""


# ---------------------------------------------------------------------------
# Байты документа: канон разрешения raw_path
# ---------------------------------------------------------------------------
def _raw_base() -> Path:
    """Каталог, относительно которого резолвится относительный ``raw_path``.

    Канон — ``scripts/measure_html_extraction.py:_raw_base`` (одна трактовка раскладки на
    все приборы): объявленный ``corpus_raw`` из :func:`core.config.paths.resolve_paths`,
    если он абсолютный, иначе ``COHOUND_HOME/data``. Копия, а не импорт подчёркнутого
    имени: у той функции нет контракта, и связать два прибора её именем значило бы
    унаследовать любое её изменение молча.
    """
    try:
        from core.config.paths import resolve_paths

        config = resolve_paths()
    except ValueError as exc:  # невалидный paths.yaml — печатаем, но не падаем
        print(f"extraction-golden: раскладка не прочитана ({exc}); беру ~/.cohound/data",
              file=sys.stderr)
        config = None
    if config is not None and config.corpus_raw.is_absolute():
        return config.corpus_raw
    home = os.environ.get("COHOUND_HOME") or "~/.cohound"
    return Path(home).expanduser() / "data"


def _resolve_raw(raw_path: str, base: Path) -> Path | None:
    """Абсолютный путь к retained-байтам; ``None`` — файла нет ни в одной трактовке."""
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path if path.is_file() else None
    for candidate in (base / path, base.parent / path):
        if candidate.is_file():
            return candidate
    return None


def _is_pdf(path: Path) -> bool:
    """PDF ли это — по сигнатуре байтов: retained-файл называется ``{sha256}.bin``."""
    try:
        with path.open("rb") as handle:
            return handle.read(5).startswith(b"%PDF")
    except OSError:
        return False


#: Сигнатуры контейнеров: у члена контейнера ``raw_path`` указывает на **архив**, а не на
#: документ (спуск читает члены в ядре). Такой документ не может быть кейсом набора: его
#: байтов по ``raw_path`` просто нет, есть байты архива. Это отдельный факт, а не «не PDF».
CONTAINER_MAGIC: tuple[bytes, ...] = (b"PK\x03\x04", b"7z\xbc\xaf", b"Rar!")


def _is_container(path: Path) -> bool:
    """Лежат ли по ``raw_path`` байты архива (член контейнера, а не документ)."""
    try:
        with path.open("rb") as handle:
            head = handle.read(8)
    except OSError:
        return False
    return any(head.startswith(magic) for magic in CONTAINER_MAGIC)


# ---------------------------------------------------------------------------
# Факты о документе (только измеренное; ничего не додумывается)
# ---------------------------------------------------------------------------
@dataclass
class DocFacts:
    """Что о документе **известно** из корпуса прогона и прибора порядка чтения.

    ``None`` у поля означает «не измерено» (нет колонки в схеме прогона, читатель числа не
    сообщает, прибор не запускался) — и остаётся пустой ячейкой в листе. ``False`` —
    измеренный факт «нет»; сводить их нельзя (A3).
    """

    doc_id: str
    product_id: str
    url: str
    raw_path: str
    pages: int | None = None
    pages_converted: int | None = None
    chars: int | None = None
    chars_per_page: float | None = None
    chars_per_page_basis: str = ""
    extractor_version: str = ""
    pdf_pipeline: str = "unknown"
    partial: bool = False
    #: Прибор порядка чтения: ``True`` — есть страница с канавкой и колонками прозы.
    two_column: bool | None = None
    two_column_pages: int | None = None
    table_like_pages: int | None = None
    #: Таблицы документа как объекты: ``True`` — читатель вернул список (пусть пустой),
    #: ``False`` — вернул ``NULL`` (читатель таблиц не даёт), ``None`` — колонки нет.
    tables: bool | None = None
    #: Карта страниц: ``True`` — есть, ``False`` — читатель её не дал, ``None`` — колонки нет.
    page_map: bool | None = None
    ocr_probe: str | None = None
    reading_order_error: str | None = None

    @property
    def case_id(self) -> str:
        """Предлагаемый ``case_id``: продукт + короткий хвост хеша документа.

        Короткий, но не «угаданный»: полный ``doc_id`` остаётся отдельной колонкой, а
        дубликат ``case_id`` ``compile`` отвергает — человек вправе переименовать.
        """
        tail = self.doc_id.split(":", 1)[-1][:12]
        return f"pdf-{self.product_id}-{tail}"


def _mechanism_of(extractor_version: str) -> str:
    """Механизм PDF по **объявленному** штампу экстрактора; иначе ``unknown``.

    Правило объявлено и односторонне: ``pdfium`` в штампе — ``text_layer`` (текстовый слой
    файла), ``docling`` — ``structured`` (модели разметки). Догадка вместо правила дала бы
    колонку, по которой нельзя пересчитать выборку, поэтому всё остальное — ``unknown``,
    а не ``structured`` по умолчанию.
    """
    version = extractor_version.lower()
    if "pdfium" in version:
        return "text_layer"
    if "docling" in version:
        return "structured"
    return "unknown"


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _load_documents(
    conn: sqlite3.Connection, base: Path
) -> tuple[list[DocFacts], dict[str, int]]:
    """PDF-документы корпуса с retained-байтами + счётчики отсева.

    Отсев называется числами: «страниц нет» и «страницы не PDF» — разные факты, и молча
    выброшенный корпус читался бы как «в корпусе столько и было».
    """
    columns = corpus_columns(conn)
    wanted = [
        "doc_id",
        "product_id",
        "url",
        "raw_path",
        "page_count",
        "pages_converted",
        "extracted_chars",
        "extractor_version",
        "partial",
        "tables",
        "page_map",
        "ocr_probe",
    ]
    select = ", ".join(name for name in wanted if name in columns)
    rows = conn.execute(
        f"SELECT {select} FROM corpus_docs WHERE raw_path IS NOT NULL ORDER BY doc_id"  # noqa: S608 — имена колонок из PRAGMA, не ввод
    ).fetchall()

    skipped = Counter({"no_bytes": 0, "not_pdf": 0, "container_member": 0})
    documents: list[DocFacts] = []
    for row in rows:
        record = dict(row)
        resolved = _resolve_raw(str(record["raw_path"]), base)
        if resolved is None:
            skipped["no_bytes"] += 1
            continue
        if not _is_pdf(resolved):
            # Член контейнера называется отдельно: его ``raw_path`` — архив, и «PDF нет»
            # читалось бы как дефект корпуса, хотя это свойство хранения.
            skipped["container_member" if _is_container(resolved) else "not_pdf"] += 1
            continue
        pages = _int_or_none(record.get("page_count"))
        converted = _int_or_none(record.get("pages_converted"))
        chars = _int_or_none(record.get("extracted_chars"))
        # Знаменатель «символов на страницу» называется, а не подразумевается: заявленные
        # документом страницы и реально прочитанные — разные факты, и подменять один другим
        # молча значило бы публиковать число с неизвестным знаменателем.
        basis = ""
        denominator: int | None = None
        if converted:
            basis, denominator = "pages_converted", converted
        elif pages:
            basis, denominator = "page_count", pages
        per_page = round(chars / denominator, 1) if (chars is not None and denominator) else None
        extractor_version = str(record.get("extractor_version") or "")
        documents.append(
            DocFacts(
                doc_id=str(record["doc_id"]),
                product_id=str(record["product_id"]),
                url=str(record["url"]),
                raw_path=str(resolved),
                pages=pages,
                pages_converted=converted,
                chars=chars,
                chars_per_page=per_page,
                chars_per_page_basis=basis,
                extractor_version=extractor_version,
                pdf_pipeline=_mechanism_of(extractor_version),
                partial=bool(record.get("partial") or 0),
                tables=(record["tables"] is not None) if "tables" in columns else None,
                page_map=(record["page_map"] is not None) if "page_map" in columns else None,
                ocr_probe=record.get("ocr_probe") if "ocr_probe" in columns else None,
            )
        )
    return documents, dict(skipped)


# ---------------------------------------------------------------------------
# Прибор порядка чтения (O37): двухколоночность определяет он, а не этот скрипт
# ---------------------------------------------------------------------------
def _instrument() -> Any | None:
    """Модуль ``scripts/measure_reading_order.py`` либо ``None`` (прибор недоступен)."""
    path = Path(__file__).resolve().parent / "measure_reading_order.py"
    if not path.is_file():
        return None
    spec = importlib.util.spec_from_file_location("measure_reading_order", path)
    if spec is None or spec.loader is None:  # pragma: no cover — битый файл прибора
        return None
    module = importlib.util.module_from_spec(spec)
    sys.modules["measure_reading_order"] = module
    spec.loader.exec_module(module)
    return module


def _apply_reading_order(
    documents: list[DocFacts], *, max_pages: int, instrument: Any
) -> dict[str, int]:
    """Проставляет ``two_column``/``table_like`` по вердикту прибора; возвращает счётчики.

    Документ двухколоночный, если **хотя бы одна** его страница названа ``two_column``:
    «порядок чтения» — свойство страницы, а набор размечается по документам, и документ с
    одной такой страницей уже даёт направлению предмет.

    Вызывается собственная постраничная точка решения прибора
    (:func:`measure_reading_order._page_geometry`) — публичной функции «вердикт по
    документу» у него нет, а копировать правила канавки/заполнения/выравнивания сюда
    значило бы завести вторую реализацию, которая разъедется с прибором молча.
    """
    try:
        import pypdfium2 as pdfium  # noqa: PLC0415 — тяжёлый импорт только на этом пути
    except ImportError:  # pragma: no cover — зависит от окружения
        print(
            "extraction-golden: pypdfium2 не установлен — двухколоночные не идентифицированы "
            "прибором (колонка остаётся ручной)",
            file=sys.stderr,
        )
        return {"documents": 0, "two_column": 0, "table_like": 0, "errors": 0}

    stats = Counter({"documents": 0, "two_column": 0, "table_like": 0, "errors": 0})
    for position, document in enumerate(documents, start=1):
        print(f"[{position}/{len(documents)}] {document.doc_id.split(':', 1)[-1][:12]}",
              flush=True)
        try:
            pdf = pdfium.PdfDocument(document.raw_path)
        except Exception as exc:  # noqa: BLE001 — нечитаемый PDF это факт, а не падение
            document.reading_order_error = f"{type(exc).__name__}: {exc}"
            stats["errors"] += 1
            continue
        try:
            page_total = len(pdf)
            limit = page_total if max_pages <= 0 else min(max_pages, page_total)
            two_column = 0
            table_like = 0
            for index in range(limit):
                try:
                    geometry = instrument._page_geometry(pdf, index)  # noqa: SLF001 — единственная точка вердикта
                except Exception as exc:  # noqa: BLE001 — страница, а не документ
                    document.reading_order_error = f"{type(exc).__name__}: {exc}"
                    continue
                reason = geometry.get("reason")
                if reason == "two_column":
                    two_column += 1
                elif reason == "table_like":
                    table_like += 1
        finally:
            pdf.close()
        document.two_column_pages = two_column
        document.table_like_pages = table_like
        document.two_column = two_column > 0
        stats["documents"] += 1
        stats["two_column"] += 1 if two_column else 0
        stats["table_like"] += 1 if table_like else 0
    return dict(stats)


# ---------------------------------------------------------------------------
# Отбор: стратификация по направлениям + детерминированный добор
# ---------------------------------------------------------------------------
#: Страты отбора: ``(имя, описание для человека, предикат)``. Порядок — приоритет: он же
#: порядок заполнения квоты. Имя попадает в колонку ``stratum`` и пересчитывается из
#: колонок листа, поэтому его нельзя менять, не меняя ``compile``-проверок.
STRATA: tuple[tuple[str, str, Callable[[DocFacts], bool]], ...] = (
    (
        "two_column",
        "порядок чтения: прибор нашёл канавку и колонки ПРОЗЫ (не таблицу)",
        lambda doc: doc.two_column is True,
    ),
    (
        "low_text",
        f"полнота: меньше {LOW_TEXT_CHARS_PER_PAGE} символов на страницу",
        lambda doc: doc.chars_per_page is not None
        and doc.chars_per_page < LOW_TEXT_CHARS_PER_PAGE,
    ),
    (
        "partial",
        "полнота: документ прочитан частично (partial=1)",
        lambda doc: doc.partial,
    ),
    (
        "tables",
        "структурная верность: читатель вернул таблицы как объекты",
        lambda doc: doc.tables is True,
    ),
    (
        "no_page_map",
        "провенанс: карты страниц нет (номер страницы цитаты не выводится)",
        lambda doc: doc.page_map is False,
    ),
    (
        "multi_page",
        f"структурная верность: не меньше {MULTI_PAGE_MIN} страниц",
        lambda doc: (doc.pages or 0) >= MULTI_PAGE_MIN,
    ),
)


def _rank(doc: DocFacts, seed: int) -> str:
    """Детерминированный порядок внутри страты: ``sha256(seed|doc_id)``.

    Не ``random``: порядок обязан воспроизводиться из (seed, корпус), иначе «тот же набор»
    не проверяем.
    """
    return hashlib.sha256(f"{seed}|{doc.doc_id}".encode()).hexdigest()


def _interleave(documents: list[DocFacts], seed: int) -> list[DocFacts]:
    """Чередование продуктов: набор не должен оказаться из одного продукта.

    Внутри продукта — порядок :func:`_rank`; между продуктами — по кругу, продукты в
    алфавитном порядке. Детерминирован целиком.
    """
    by_product: dict[str, list[DocFacts]] = defaultdict(list)
    for document in sorted(documents, key=lambda item: (item.product_id, _rank(item, seed))):
        by_product[document.product_id].append(document)
    ordered: list[DocFacts] = []
    products = sorted(by_product)
    while any(by_product[product] for product in products):
        for product in products:
            if by_product[product]:
                ordered.append(by_product[product].pop(0))
    return ordered


@dataclass
class Selected:
    """Отобранный документ + страты, которым он отвечает (для колонок ``stratum``/``why``)."""

    doc: DocFacts
    strata: list[str] = field(default_factory=list)

    @property
    def case_id(self) -> str:
        return self.doc.case_id


def matched_strata(doc: DocFacts) -> list[str]:
    """Страты, которым документ отвечает, — в объявленном порядке."""
    return [name for name, _description, predicate in STRATA if predicate(doc)]


def select_documents(
    documents: list[DocFacts], target: int, seed: int
) -> list[Selected]:
    """Детерминированный стратифицированный отбор ``target`` документов.

    Квота страты — ``max(1, target // len(STRATA))``: каждое направление получает место
    даже в маленьком наборе (иначе редкая страта — например, двухколоночная — вытеснялась
    бы числом). Остаток добирается чередованием продуктов из всего пула: набор не должен
    состоять только из «интересных» документов, иначе он перестаёт быть выборкой корпуса.

    :raises ValueError: ``target <= 0``.
    """
    if target <= 0:
        raise ValueError("target обязан быть положительным")
    matched = {doc.doc_id: matched_strata(doc) for doc in documents}
    quota = max(1, target // len(STRATA))
    picked: dict[str, list[str]] = {}
    for name, _description, _predicate in STRATA:
        pool = [
            doc
            for doc in documents
            if doc.doc_id not in picked and name in matched[doc.doc_id]
        ]
        for doc in _interleave(pool, seed)[:quota]:
            picked[doc.doc_id] = matched[doc.doc_id]
    remainder = [doc for doc in documents if doc.doc_id not in picked]
    need = max(0, target - len(picked))
    for doc in _interleave(remainder, seed)[:need]:
        picked[doc.doc_id] = matched[doc.doc_id]
    selected = [
        Selected(doc=doc, strata=picked[doc.doc_id])
        for doc in documents
        if doc.doc_id in picked
    ]
    return sorted(selected, key=lambda item: item.case_id)


# ---------------------------------------------------------------------------
# XLSX: рабочий лист разметчика
# ---------------------------------------------------------------------------
def _openpyxl() -> Any:
    """``openpyxl`` с внятным отказом (XLSX — формат листа, а не зависимость прибора)."""
    try:
        import openpyxl
    except ImportError as error:  # pragma: no cover — зависит от окружения
        raise SystemExit(
            "XLSX требует openpyxl (extra `extract`: uv sync --extra extract)"
        ) from error
    return openpyxl


def _column_letter(index: int) -> str:
    """1 → ``A``, 27 → ``AA``."""
    letters = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters = chr(ord("A") + remainder) + letters
    return letters


def _fmt(value: Any) -> str:
    """Значение → текст ячейки: ``None`` остаётся пустым (не ``0`` и не ``no``)."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "yes" if value else "no"
    return str(value)


def candidate_rows(selected: list[Selected]) -> list[dict[str, str]]:
    """Строки листа: измеренное заполнено, колонки решений — пустые.

    Это и есть граница D10 в коде: здесь **нет** ни одного присваивания в ``include``/
    ``completeness``/``structure``/``reading_order``/``identifiers``/``provenance``/
    ``min_chars``/``expected_substrings``/``forbidden_substrings``/``expect_error``/``notes``.
    """
    rows: list[dict[str, str]] = []
    for item in selected:
        doc = item.doc
        why = []
        if item.strata:
            why.append("страты: " + ",".join(item.strata))
        else:
            # Пустая колонка stratum — не пропуск, а факт: документ добран для объёма и ни
            # одной страте не отвечает. Сказать это в why дешевле, чем оставить читателя
            # гадать, почему строка в листе.
            why.append("добран для объёма (ни одной страты не отвечает)")
        if doc.two_column is not None:
            why.append(
                f"двухколоночных страниц: {doc.two_column_pages}"
                f" (просмотрено до {DEFAULT_READING_ORDER_PAGES} стр.)"
            )
        if doc.reading_order_error:
            why.append(f"прибор порядка чтения: {doc.reading_order_error}")
        if doc.chars_per_page is not None:
            why.append(f"символов на страницу: {doc.chars_per_page}")
        rows.append(
            {
                "case_id": item.case_id,
                "doc_id": doc.doc_id,
                "product_id": doc.product_id,
                "url": doc.url,
                "raw_path": doc.raw_path,
                "pages": _fmt(doc.pages),
                "pages_converted": _fmt(doc.pages_converted),
                "chars": _fmt(doc.chars),
                "chars_per_page": _fmt(doc.chars_per_page),
                "chars_per_page_basis": doc.chars_per_page_basis,
                "extractor_version": doc.extractor_version,
                "pdf_pipeline": doc.pdf_pipeline,
                "partial": _fmt(doc.partial),
                "two_column": _fmt(doc.two_column),
                "two_column_pages": _fmt(doc.two_column_pages),
                "table_like_pages": _fmt(doc.table_like_pages),
                "tables": _fmt(doc.tables),
                "page_map": _fmt(doc.page_map),
                "ocr_probe": _fmt(doc.ocr_probe),
                "stratum": ",".join(item.strata),
                "why": "; ".join(why),
            }
        )
    return rows


_GUIDE_LINES: tuple[str, ...] = (
    "Этот лист — НАБОР-КАНДИДАТ, а не эталон. Строки отобраны по измерениям, ни одно "
    "решение не заполнено: пока вы не заполнили include, набора нет.",
    "",
    "Размечаются колонки решений (все остальные — измеренное, их не правят): include, "
    "completeness, structure, reading_order, identifiers, provenance, min_chars, "
    "expected_substrings, forbidden_substrings, expect_error, notes.",
    "",
    "include — yes (документ входит в набор) либо no (не входит). Пусто = не рассмотрено, "
    "и compile на этом откажет. no — это тоже решение, и оно не требует остальных колонок.",
    "",
    "Колонка stratum говорит, почему документ попал в лист (страты — по направлениям, "
    "подробности в policy). Пустая stratum = документ добран для объёма и ни одной страте "
    "не отвечает; это не ошибка отбора.",
    "",
    "Пять направлений (O38б). Пустое значение направления = «для этого документа не "
    "размечаю»; это осознанный пропуск, а не забытая ячейка — решение по документу целиком "
    "живёт в include.",
    "",
    "completeness — full (текст обязан быть полным; тогда min_chars обязателен и > 0), "
    "partial_ok (частичная потеря допустима; min_chars — нижняя граница), skip.",
    "structure — expect_tables (таблицы обязаны выжить: их шапку/строки перечислите в "
    "expected_substrings), no_tables (таблиц в документе нет — проверять нечего), skip.",
    "reading_order — columns (документ двухколоночный: колонки обязаны читаться по "
    "колонкам). Ставить columns документу, который прибор двухколоночным НЕ назвал, "
    "compile не даст — это была бы разметка против измерения.",
    "identifiers — expect (в тексте обязаны выжить перечисленные идентификаторы: имена "
    "функций, флаги, версии — в expected_substrings), skip.",
    "provenance — pages_required (номер страницы каждой цитаты обязан выводиться из карты "
    "страниц), skip. Колонка page_map говорит, есть ли карта у этого документа сейчас.",
    "",
    "expected_substrings / forbidden_substrings — списки через «|» (вертикальная черта). "
    "Порядок в expected_substrings — ожидаемый порядок появления в тексте: так проверяется "
    "и порядок чтения (первая строка правой колонки не должна стоять перед последней "
    "строкой левой).",
    "forbidden_substrings — то, чего в тексте быть НЕ должно: колонтитулы, меню, "
    "«Скачать PDF», служебные маркеры формата.",
    "min_chars — целое: нижняя граница объёма текста документа.",
    "expect_error — для скана без текстового слоя: no_text_layer. Кейс успешен, только "
    "если читатель отказал ИМЕННО этой ошибкой; остальные направления тогда skip, "
    "expected/forbidden пусты.",
    "notes — что именно вы проверяете этим кейсом (свободный текст для следующего читателя).",
    "",
    "Двухколоночность определена прибором scripts/measure_reading_order.py (колонка "
    "two_column и число страниц two_column_pages), а не на глаз. На пилотном корпусе "
    "двухколоночной ПРОЗЫ ноль (O37, 16.11.2026: 0 из 19 331 страницы) — если набор "
    "собран на нём, направление «порядок чтения» останется без предмета, и это факт о "
    "корпусе, а не о разметке.",
    "",
    "Дальше одна команда: prepare_extraction_golden.py compile --xlsx <этот файл> "
    "--out <extraction_golden.json>. Она сама проверит инварианты разметки.",
)


def write_candidates_xlsx(
    path: Path,
    rows: list[dict[str, str]],
    *,
    policy_rows: list[tuple[str, Any]],
) -> None:
    """Пишет лист кандидатов, инструкцию и машинные условия.

    Что делает файл рабочим, а не «CSV в обёртке»: закреплённая шапка с фильтром;
    выпадающие списки в колонках решений (опечатка не проходит, а ``compile`` отказывает
    именно на неизвестном значении); заливка измеренных колонок (их не правят) и пустых
    колонок решений (их заполняют); перенос по словам в текстовых колонках; второй лист —
    инструкция; третий — условия, с которыми лист собран.
    """
    openpyxl = _openpyxl()
    from openpyxl.styles import Alignment, Font, PatternFill  # noqa: PLC0415
    from openpyxl.worksheet.datavalidation import DataValidation  # noqa: PLC0415

    book = openpyxl.Workbook()  # type: ignore[attr-defined]
    sheet = book.active
    sheet.title = SHEET_CANDIDATES
    sheet.append(list(CANDIDATE_COLUMNS))
    for cell in sheet[1]:
        cell.font = Font(bold=True)
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = f"A1:{_column_letter(len(CANDIDATE_COLUMNS))}1"

    widths = {
        "case_id": 30,
        "doc_id": 46,
        "product_id": 18,
        "url": 50,
        "raw_path": 50,
        "extractor_version": 40,
        "stratum": 22,
        "why": 40,
        "expected_substrings": 50,
        "forbidden_substrings": 40,
        "notes": 40,
    }
    wrap = {"url", "raw_path", "extractor_version", "why", "expected_substrings",
            "forbidden_substrings", "notes"}
    measured_indexes = [CANDIDATE_COLUMNS.index(name) + 1 for name in CANDIDATE_COLUMNS[: CANDIDATE_COLUMNS.index("include")]]
    for index, name in enumerate(CANDIDATE_COLUMNS, start=1):
        sheet.column_dimensions[_column_letter(index)].width = widths.get(name, 16)

    measured_fill = PatternFill("solid", start_color="EEEEEE")
    for row_index, row in enumerate(rows, start=2):
        sheet.append([row.get(name, "") for name in CANDIDATE_COLUMNS])
        for index, name in enumerate(CANDIDATE_COLUMNS, start=1):
            cell = sheet.cell(row=row_index, column=index)
            if name in wrap:
                cell.alignment = Alignment(wrap_text=True, vertical="top")
        for index in measured_indexes:
            sheet.cell(row=row_index, column=index).fill = measured_fill

    last = max(2, len(rows) + 1)
    for name, values in (("include", INCLUDE_VALUES), *DIRECTION_VALUES.items()):
        index = CANDIDATE_COLUMNS.index(name) + 1
        validation = DataValidation(
            type="list",
            formula1=f'"{",".join(values)}"',
            allow_blank=True,
            showDropDown=False,
        )
        validation.error = f"{name}: допустимо {', '.join(values)} или пусто"
        validation.errorTitle = "Неизвестное значение"
        sheet.add_data_validation(validation)
        letter = _column_letter(index)
        validation.add(f"{letter}2:{letter}{last}")

    guide = book.create_sheet(SHEET_GUIDE)
    for line in _GUIDE_LINES:
        guide.append([line])
    guide.column_dimensions["A"].width = 110
    for cell in guide["A"]:
        cell.alignment = Alignment(wrap_text=True, vertical="top")

    policy_sheet = book.create_sheet(SHEET_POLICY)
    policy_sheet.append(["key", "value"])
    for key, value in policy_rows:
        policy_sheet.append([key, _fmt(value)])
    policy_sheet.column_dimensions["A"].width = 30
    policy_sheet.column_dimensions["B"].width = 70

    path.parent.mkdir(parents=True, exist_ok=True)
    book.save(path)


def read_sheet(path: Path, name: str) -> list[dict[str, str]]:
    """Лист XLSX → строки-словари (первая строка — шапка, значения приводятся к тексту)."""
    openpyxl = _openpyxl()
    book = openpyxl.load_workbook(path, read_only=True, data_only=True)  # type: ignore[attr-defined]
    try:
        if name not in book.sheetnames:
            raise SystemExit(f"{path}: нет листа {name!r} (есть: {book.sheetnames})")
        sheet = book[name]
        rows = sheet.iter_rows(values_only=True)
        try:
            header = [("" if value is None else str(value)).strip() for value in next(rows)]
        except StopIteration:
            raise SystemExit(f"{path}: лист {name!r} пуст") from None
        out: list[dict[str, str]] = []
        for values in rows:
            if not any(value not in (None, "") for value in values):
                continue
            row = {column: "" for column in header if column}
            for index, column in enumerate(header):
                if not column or index >= len(values):
                    continue
                value = values[index]
                row[column] = "" if value is None else str(value).strip()
            out.append(row)
        return out
    finally:
        book.close()


def read_policy_sheet(path: Path) -> dict[str, str]:
    """Лист ``policy`` → словарь; отсутствие листа — пустой словарь."""
    openpyxl = _openpyxl()
    book = openpyxl.load_workbook(path, read_only=True, data_only=True)  # type: ignore[attr-defined]
    try:
        if SHEET_POLICY not in book.sheetnames:
            return {}
        out: dict[str, str] = {}
        for key, value in book[SHEET_POLICY].iter_rows(values_only=True):
            if key in (None, "", "key"):
                continue
            out[str(key).strip()] = "" if value is None else str(value).strip()
        return out
    finally:
        book.close()


# ---------------------------------------------------------------------------
# candidates
# ---------------------------------------------------------------------------
def cmd_candidates(args: argparse.Namespace) -> int:
    """Собирает набор-кандидат из run-DB и пишет лист разметчика."""
    db_path = Path(args.db).expanduser()
    if not db_path.is_file():
        print(f"extraction-golden: БД не найдена: {db_path}", file=sys.stderr)
        return 2
    if args.target <= 0:
        print("extraction-golden: --target обязан быть положительным", file=sys.stderr)
        return 2

    conn = _ro_connect(db_path)
    try:
        run_id, package_id = read_run_meta(conn, args.run_id)
        base = _raw_base()
        documents, skipped = _load_documents(conn, base)
        schema_version = read_schema_version(conn)
        missing_columns = sorted(
            {"tables", "page_map", "ocr_probe", "pages_converted"} - corpus_columns(conn)
        )
    finally:
        conn.close()

    if not documents:
        print(
            "extraction-golden: в корпусе прогона нет PDF с retained-байтами "
            f"(отсев: {skipped})",
            file=sys.stderr,
        )
        return 2

    case_id = args.case_id or package_id or ""
    if not case_id:
        print(
            "extraction-golden: пакет прогона не назван — передайте --case-id",
            file=sys.stderr,
        )
        return 2

    instrument_name = "не запускался"
    reading_stats: dict[str, int] = {}
    if not args.no_reading_order:
        module = _instrument()
        if module is None:
            print(
                "extraction-golden: прибор scripts/measure_reading_order.py не найден — "
                "двухколоночные не идентифицированы прибором (колонка ручная)",
                file=sys.stderr,
            )
        else:
            print(
                f"прибор порядка чтения: до {args.reading_order_pages} стр. на документ "
                f"(0 — все), документов: {len(documents)}",
                flush=True,
            )
            reading_stats = _apply_reading_order(
                documents, max_pages=args.reading_order_pages, instrument=module
            )
            instrument_name = "scripts/measure_reading_order.py"

    selected = select_documents(documents, args.target, args.seed)
    rows = candidate_rows(selected)

    two_column_documents = sum(1 for item in selected if item.doc.two_column is True)
    per_product = Counter(item.doc.product_id for item in selected)
    stratum_counts = Counter(
        name for item in selected for name in item.strata
    )
    policy_rows: list[tuple[str, Any]] = [
        ("instrument", "scripts/prepare_extraction_golden.py"),
        ("format", EXTRACTION_GOLDEN_FORMAT),
        ("status", "candidate"),
        ("run_id", run_id),
        ("case_id", case_id),
        ("db", str(db_path)),
        ("schema_version", schema_version),
        ("seed", args.seed),
        ("target", args.target),
        ("selected", len(selected)),
        ("documents_pool", len(documents)),
        ("skipped_no_bytes", skipped.get("no_bytes", 0)),
        ("skipped_not_pdf", skipped.get("not_pdf", 0)),
        ("skipped_container_member", skipped.get("container_member", 0)),
        ("raw_base", str(base)),
        ("missing_columns", ",".join(missing_columns) or ""),
        ("reading_order_instrument", instrument_name),
        ("reading_order_pages", args.reading_order_pages),
        ("reading_order_documents", reading_stats.get("documents", 0)),
        ("reading_order_errors", reading_stats.get("errors", 0)),
        ("table_like_documents", reading_stats.get("table_like", 0)),
        ("two_column_documents", two_column_documents),
        ("two_column_required", 1),
        ("per_product", ",".join(f"{name}={count}" for name, count in sorted(per_product.items()))),
        ("strata", ",".join(f"{name}={stratum_counts.get(name, 0)}" for name, _d, _p in STRATA)),
    ]

    out = Path(args.out).expanduser()
    write_candidates_xlsx(out, rows, policy_rows=policy_rows)

    print(f"written: {out} (документов: {len(selected)}, из пула {len(documents)})")
    print(f"прогон: {run_id}, пакет: {case_id}")
    print("по продуктам: " + ", ".join(f"{n} {c}" for n, c in sorted(per_product.items())))
    print(
        "по стратам: "
        + ", ".join(f"{name} {stratum_counts.get(name, 0)}" for name, _d, _p in STRATA)
    )
    if instrument_name == "не запускался":
        print("двухколоночные: НЕ идентифицированы прибором (колонка остаётся ручной)")
    else:
        print(
            f"двухколоночные: {two_column_documents} документов "
            f"(прибор {instrument_name}; просмотрено документов: "
            f"{reading_stats.get('documents', 0)}, ошибок: {reading_stats.get('errors', 0)})"
        )
    if missing_columns:
        print(f"колонок нет в схеме прогона (ячейки пусты): {', '.join(missing_columns)}")
    if schema_version:
        from core.store.sqlite import SCHEMA_VERSION  # noqa: PLC0415 — только для сравнения

        if schema_version != SCHEMA_VERSION:
            # Инструмент читает что есть (он не гейт), но молчать об этом нельзя: корпус
            # прогона прошлой схемы мог быть извлечён другим кодом, и набор описывал бы его.
            print(
                f"ВНИМАНИЕ: схема прогона {schema_version}, код ожидает {SCHEMA_VERSION} — "
                "корпус мог быть извлечён другим кодом; версия записана в policy",
                file=sys.stderr,
            )
    if skipped.get("no_bytes") or skipped.get("not_pdf") or skipped.get("container_member"):
        print(
            f"отсев: без байтов {skipped.get('no_bytes', 0)}, "
            f"не PDF {skipped.get('not_pdf', 0)}, "
            f"члены контейнеров (raw_path — архив) {skipped.get('container_member', 0)}"
        )
    return 0


# ---------------------------------------------------------------------------
# compile: лист → JSON набора
# ---------------------------------------------------------------------------
def _split_list(raw: str) -> list[str]:
    """``a|b|c`` → ``['a','b','c']``; пустые элементы отбрасываются."""
    return [part.strip() for part in raw.split(LIST_SEPARATOR) if part.strip()]


def _direction_value(row: dict[str, str], name: str, problems: list[str], where: str) -> str:
    """Значение направления из строки: пусто = skip; вне словаря — проблема."""
    value = (row.get(name) or "").strip().lower()
    if not value:
        return "skip"
    if value not in DIRECTION_VALUES[name]:
        problems.append(
            f"{where}: {name}={value!r}, допустимо "
            f"{', '.join(DIRECTION_VALUES[name])} или пусто"
        )
        return "skip"
    return value


def _case_from_row(
    row: dict[str, str],
    where: str,
    problems: list[str],
    *,
    raw_base: Path,
) -> dict[str, Any] | None:
    """Строка листа с ``include=yes`` → кейс формата extraction-golden.

    Возвращает ``None``, если разметка противоречива (проблемы уже записаны). Ничего не
    достраивает: поля кейса берутся из решений человека, а измеренные колонки служат
    только проверкой (например, ``reading_order=columns`` против вердикта прибора).
    """
    directions = {name: _direction_value(row, name, problems, where) for name in DIRECTIONS}

    expected = _split_list(row.get("expected_substrings", ""))
    forbidden = _split_list(row.get("forbidden_substrings", ""))
    expect_error = (row.get("expect_error") or "").strip()
    min_chars_raw = (row.get("min_chars") or "").strip()
    min_chars = 0
    if min_chars_raw:
        try:
            min_chars = int(min_chars_raw)
        except ValueError:
            problems.append(f"{where}: min_chars={min_chars_raw!r} — не целое число")
            return None
        if min_chars < 0:
            problems.append(f"{where}: min_chars={min_chars} — отрицательное")
            return None

    if directions["completeness"] == "full" and min_chars <= 0:
        problems.append(
            f"{where}: completeness=full требует min_chars > 0 — иначе «полнота» не "
            "проверяется ничем"
        )
        return None
    if directions["identifiers"] == "expect" and not expected:
        problems.append(
            f"{where}: identifiers=expect без expected_substrings — идентификаторов для "
            "проверки нет"
        )
        return None
    if directions["structure"] == "expect_tables" and not expected:
        problems.append(
            f"{where}: structure=expect_tables без expected_substrings — нечего требовать "
            "от таблиц"
        )
        return None
    if expect_error:
        contradictory = [
            name for name, value in directions.items() if value not in ("skip",)
        ]
        if contradictory or expected or forbidden or min_chars:
            problems.append(
                f"{where}: expect_error={expect_error!r} вместе с "
                f"{', '.join(contradictory) or 'ожиданиями текста'} — негативный кейс "
                "проверяет отказ, а не текст"
            )
            return None
    if directions["reading_order"] == "columns" and (row.get("two_column") or "").strip() == "no":
        problems.append(
            f"{where}: reading_order=columns, а прибор порядка чтения назвал документ "
            f"НЕ двухколоночным (two_column_pages={(row.get('two_column_pages') or '0')}) — "
            "разметка против измерения"
        )
        return None

    if not expected and not forbidden and not min_chars and not expect_error:
        problems.append(
            f"{where}: кейс без единого ожидания (expected/forbidden/min_chars/expect_error) "
            "— такой эталон проходит всё и не проверяет ничего"
        )
        return None

    raw_path = (row.get("raw_path") or "").strip()
    if not raw_path:
        problems.append(f"{where}: пустой raw_path — байтов кейса нет")
        return None
    resolved = _resolve_raw(raw_path, raw_base)
    if resolved is None:
        problems.append(
            f"{where}: байтов нет по raw_path {raw_path!r} (искали абсолютно и относительно "
            f"{raw_base})"
        )
        return None

    return {
        "case_id": (row.get("case_id") or "").strip(),
        "format": "pdf",
        "raw_path": str(resolved),
        "expected_substrings": expected,
        "forbidden_substrings": forbidden,
        "min_chars": min_chars,
        "expect_error": expect_error or None,
        "doc_id": (row.get("doc_id") or "").strip() or None,
        "notes": (row.get("notes") or "").strip(),
        "directions": directions,
    }


def cmd_compile(args: argparse.Namespace) -> int:
    """Заполненный XLSX → ``extraction_golden.json`` (конверт набора).

    Кроме полей кейса каждый кейс несёт блок ``directions`` — **пометки направлений**
    разметчика (что именно проверяется: полнота/структура/порядок/идентификаторы/
    провенанс). Прогон его не читает: это аннотация для следующего читателя набора, и
    правила кейса от неё не зависят.
    """
    path = Path(args.xlsx).expanduser()
    if not path.is_file():
        print(f"extraction-golden: листа нет: {path}", file=sys.stderr)
        return 2
    rows = read_sheet(path, SHEET_CANDIDATES)
    policy = read_policy_sheet(path)
    run_id = args.run_id or policy.get("run_id", "")
    case_id = args.case_id or policy.get("case_id", "")
    if not case_id:
        print(
            "extraction-golden: набор не называет case_id: передайте --case-id "
            "(в листе нет листа policy)",
            file=sys.stderr,
        )
        return 2
    status = "reviewed" if args.status == "reviewed" else "candidate"
    if status == "reviewed" and not (args.actor or "").strip():
        print(
            "extraction-golden: --status reviewed требует --actor: подтверждение набора — "
            "осознанное действие человека, а не дефолт",
            file=sys.stderr,
        )
        return 2

    raw_base = Path(policy["raw_base"]).expanduser() if policy.get("raw_base") else _raw_base()

    problems: list[str] = []
    cases: list[dict[str, Any]] = []
    seen: dict[str, int] = {}
    rejected = 0
    for lineno, row in enumerate(rows, start=2):
        doc_id = (row.get("doc_id") or "").strip()
        where = f"строка {lineno} ({doc_id or '?'})"
        include = (row.get("include") or "").strip().lower()
        if not include:
            problems.append(f"{where}: нет решения include (yes|no) — строка не рассмотрена")
            continue
        if include not in INCLUDE_VALUES:
            problems.append(
                f"{where}: include={include!r}, допустимо {', '.join(INCLUDE_VALUES)}"
            )
            continue
        if include == "no":
            rejected += 1
            continue
        case = _case_from_row(row, where, problems, raw_base=raw_base)
        if case is None:
            continue
        if not case["case_id"]:
            problems.append(f"{where}: пустой case_id")
            continue
        if case["case_id"] in seen:
            problems.append(
                f"{where}: дубликат case_id {case['case_id']!r} "
                f"(первая — строка {seen[case['case_id']]})"
            )
            continue
        seen[case["case_id"]] = lineno
        cases.append(case)

    if problems:
        for problem in problems:
            print(f"ОТКАЗ: {problem}", file=sys.stderr)
        return 2
    if not cases:
        print(
            "ОТКАЗ: ни один документ не принят — лист не размечен (пустой лист не "
            "превращается в пустой эталон)",
            file=sys.stderr,
        )
        return 2

    cases.sort(key=lambda case: case["case_id"])
    two_column_cases = [
        case for case in cases if case["directions"]["reading_order"] == "columns"
    ]
    two_column_required = policy.get("two_column_required", "1") not in ("", "0", "no", "false")
    if two_column_required and not two_column_cases:
        found = policy.get("two_column_documents", "")
        print(
            "ПРЕДУПРЕЖДЕНИЕ: в наборе нет ни одного двухколоночного документа "
            f"(reading_order=columns), а политика этого требует. Причина может быть в "
            f"корпусе: прибор нашёл двухколоночных документов {found or '—'}. "
            "«Порядок чтения» в таком наборе измеряется на пустом множестве (O37), и это "
            "видно в policy — отказом это не делается: свойство корпуса не ошибка разметки",
            file=sys.stderr,
        )

    directions_summary = {
        name: Counter(case["directions"][name] for case in cases) for name in DIRECTIONS
    }
    payload = {
        "format": EXTRACTION_GOLDEN_FORMAT,
        "case_id": case_id,
        "run_id": run_id,
        "status": status,
        "reviewed_by": (args.actor or None) if status == "reviewed" else None,
        "instrument": "scripts/prepare_extraction_golden.py",
        "policy": dict(policy),
        "directions": {
            name: {value: directions_summary[name].get(value, 0) for value in values}
            for name, values in DIRECTION_VALUES.items()
        },
        "cases": cases,
    }
    out = Path(args.out).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    # Компиляция без проверки бессмысленна: сразу читаем записанное тем же загрузчиком,
    # которым пользуется прогон, — иначе правила проверялись бы только тестами.
    loaded = load_extraction_golden_document(out)
    print(
        f"written: {out} ({len(loaded.cases)} кейсов, status={loaded.status}, "
        f"формат {loaded.format})"
    )
    print(f"строк в листе: {len(rows)}, принято: {len(cases)}, отклонено человеком: {rejected}")
    print(
        "двухколоночные кейсы: "
        + (", ".join(case["case_id"] for case in two_column_cases) or "нет")
    )
    print(
        "направления: "
        + "; ".join(
            f"{name}: "
            + ", ".join(f"{value} {count}" for value, count in sorted(directions_summary[name].items()))
            for name in DIRECTIONS
        )
    )
    if status == "reviewed":
        print(f"набор подтверждён: {loaded.reviewed_by}")
    else:
        print("набор НЕ подтверждён человеком (status=candidate, D10)")
    return 0


# ---------------------------------------------------------------------------
# validate / summary
# ---------------------------------------------------------------------------
def _missing_raw_paths(cases: list[Any]) -> list[str]:
    """Кейсы, чьих байтов нет на диске (``raw_path`` уже разрешён загрузчиком)."""
    missing: list[str] = []
    for case in cases:
        if case.raw_path is not None and not Path(case.raw_path).is_file():
            missing.append(case.case_id)
    return missing


def cmd_validate(args: argparse.Namespace) -> int:
    """Перечитывает набор тем же загрузчиком и печатает сводку."""
    path = Path(args.golden).expanduser()
    if not path.is_file():
        print(f"extraction-golden: набора нет: {path}", file=sys.stderr)
        return 2
    try:
        document = load_extraction_golden_document(path)
    except ValueError as exc:
        print(f"ОТКАЗ: {exc}", file=sys.stderr)
        return 2
    if not document.cases:
        print("ОТКАЗ: в наборе 0 кейсов", file=sys.stderr)
        return 2
    missing = _missing_raw_paths(document.cases)
    if missing:
        print(
            f"ОТКАЗ: у {len(missing)} кейсов нет байтов по raw_path "
            f"(первые: {', '.join(missing[:5])})",
            file=sys.stderr,
        )
        return 2

    formats = Counter(case.format for case in document.cases)
    with_expected = sum(1 for case in document.cases if case.expected_substrings)
    with_forbidden = sum(1 for case in document.cases if case.forbidden_substrings)
    with_min_chars = sum(1 for case in document.cases if case.min_chars > 0)
    with_error = sum(1 for case in document.cases if case.expect_error)
    with_doc_id = sum(1 for case in document.cases if case.doc_id)
    print(f"файл: {path}")
    print(
        f"формат: {document.format or 'нет конверта (исторический список)'}; "
        f"status: {document.status if document.format else 'не объявлен'}"
        + (f"; подтвердил: {document.reviewed_by}" if document.reviewed_by else "")
    )
    print(f"case_id: {document.case_id or '—'}, run_id: {document.run_id or '—'}")
    print(f"кейсов: {len(document.cases)}; форматы: {dict(sorted(formats.items()))}")
    print(
        f"с expected: {with_expected}, с forbidden: {with_forbidden}, "
        f"с min_chars>0: {with_min_chars}, негативных (expect_error): {with_error}, "
        f"с doc_id: {with_doc_id}"
    )
    if document.format and document.status == "candidate":
        print("набор НЕ подтверждён человеком: разметка ещё не сделана (D10)")
    return 0


def cmd_summary(args: argparse.Namespace) -> int:
    """Числа листа (и набора, если JSON уже собран)."""
    path = Path(args.xlsx).expanduser()
    if not path.is_file():
        print(f"extraction-golden: листа нет: {path}", file=sys.stderr)
        return 2
    rows = read_sheet(path, SHEET_CANDIDATES)
    policy = read_policy_sheet(path)
    include_counts = Counter((row.get("include") or "").strip().lower() or "пусто" for row in rows)
    per_product = Counter((row.get("product_id") or "").strip() for row in rows)
    strata = Counter(
        name for row in rows for name in (row.get("stratum") or "").split(",") if name
    )
    two_column = sum(1 for row in rows if (row.get("two_column") or "") == "yes")
    print(f"лист: {path} (прогон {policy.get('run_id', '—')}, пакет {policy.get('case_id', '—')})")
    print(f"строк: {len(rows)}")
    print("решения include: " + ", ".join(f"{name} {count}" for name, count in sorted(include_counts.items())))
    print("по продуктам: " + ", ".join(f"{name} {count}" for name, count in sorted(per_product.items())))
    print("по стратам: " + ", ".join(f"{name} {count}" for name, count in sorted(strata.items())))
    print(f"двухколоночные (вердикт прибора): {two_column}")
    if args.golden:
        golden = Path(args.golden).expanduser()
        if not golden.is_file():
            print(f"extraction-golden: набора нет: {golden}", file=sys.stderr)
            return 2
        try:
            document = load_extraction_golden_document(golden)
        except ValueError as exc:
            print(f"ОТКАЗ: {exc}", file=sys.stderr)
            return 2
        print(
            f"набор: {len(document.cases)} кейсов, status={document.status}, "
            f"формат {document.format}"
        )
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    """Парсер подкоманд (``candidates|compile|validate|summary``)."""
    parser = argparse.ArgumentParser(
        description="Набор-кандидат PDF-эталона извлечения (O38б/D10 плана 26): "
        "кандидаты из корпуса → XLSX разметчика → JSON набора"
    )
    sub = parser.add_subparsers(dest="command", metavar="КОМАНДА")

    candidates = sub.add_parser(
        "candidates", help="выбрать набор-кандидат из run-DB и записать XLSX"
    )
    candidates.add_argument("--db", required=True, help="cohound.db прогона (только чтение)")
    candidates.add_argument("--run-id", default=None, help="run_id (по умолчанию — единственный)")
    candidates.add_argument(
        "--case-id",
        dest="case_id",
        default=None,
        help="пакет/кейс (по умолчанию — package_id прогона)",
    )
    candidates.add_argument(
        "--target",
        type=int,
        default=DEFAULT_TARGET,
        help=f"сколько документов отобрать (30–50 по плану; дефолт {DEFAULT_TARGET})",
    )
    candidates.add_argument(
        "--seed", type=int, default=0, help="seed детерминированного добора (дефолт 0)"
    )
    candidates.add_argument(
        "--reading-order-pages",
        dest="reading_order_pages",
        type=int,
        default=DEFAULT_READING_ORDER_PAGES,
        help="страниц на документ для прибора порядка чтения (0 — все)",
    )
    candidates.add_argument(
        "--no-reading-order",
        dest="no_reading_order",
        action="store_true",
        help="не запускать прибор (колонка two_column останется пустой и это будет названо)",
    )
    candidates.add_argument("--out", required=True, help="путь XLSX (не в пакет до разметки)")
    candidates.set_defaults(func=cmd_candidates)

    compile_ = sub.add_parser("compile", help="заполненный XLSX → JSON набора")
    compile_.add_argument("--xlsx", required=True, help="заполненный лист разметчика")
    compile_.add_argument("--out", required=True, help="куда писать extraction_golden.json")
    compile_.add_argument("--case-id", dest="case_id", default=None, help="пакет/кейс")
    compile_.add_argument("--run-id", dest="run_id", default=None, help="run_id корпуса")
    compile_.add_argument(
        "--status",
        choices=sorted(EXTRACTION_GOLDEN_STATUSES),
        default="candidate",
        help="состояние набора: candidate (дефолт, человек не подтвердил) | reviewed",
    )
    compile_.add_argument("--actor", default=None, help="кто подтвердил (обязателен для reviewed)")
    compile_.set_defaults(func=cmd_compile)

    validate = sub.add_parser("validate", help="перечитать набор тем же загрузчиком")
    validate.add_argument("--golden", required=True, help="extraction_golden.json")
    validate.set_defaults(func=cmd_validate)

    summary = sub.add_parser("summary", help="числа листа (и набора)")
    summary.add_argument("--xlsx", required=True, help="лист разметчика")
    summary.add_argument("--golden", default=None, help="собранный набор (опц.)")
    summary.set_defaults(func=cmd_summary)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Точка входа: без подкоманды — usage и код 2."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "func", None):
        parser.print_help()
        return 2
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
