#!/usr/bin/env python3
"""Пик RSS **одного** извлечения с разложением на копии (O33, план 26).

**Вопрос, на который прибор отвечает.** ``DOCLING_PEAK_MB[layout_tables] = 3679`` —
пик одного 2763-страничного документа, взятый как цена **каждого** воркера
(``docs/reports/measurements/2026-11-13-extraction-peak-justification.md``). Чтобы
заменить константу функцией документа, нужно знать не одно число, а **из чего оно
состоит**: сколько стоит чтение байтов, сколько — сам экстрактор, сколько — вторая копия
текста при финализации и хешировании, сколько — конверт кэша (``json.dumps``) и сколько —
``pickle`` на границе процесса.

**Почему отдельный процесс.** ``ru_maxrss`` монотонен в пределах процесса, поэтому два
документа в одном процессе дают пик **суммы** и число перестаёт быть интерпретируемым
(эта ошибка уже разобрана в ``2026-10-25-docling-memory-and-streaming.md`` §4). Прибор
запускает каждый замер дочерним процессом (``sys.executable`` + этот же файл в режиме
``--child``) и читает:

* **пик** — ``resource.getrusage(RUSAGE_SELF).ru_maxrss`` **внутри** дочернего процесса
  (на Linux — KiB), то есть максимум RSS именно этого замера;
* **текущий RSS** — ``/proc/self/status:VmRSS`` (Linux) — чтобы разложение показывало
  живую аллокацию шага, а не только поднятый потолок;
* **кросс-проверку** — ``RUSAGE_CHILDREN.ru_maxrss`` в родителе. Это максимум по **всем**
  уже завершённым детям (не по последнему), поэтому как цена одного замера он не годится
  и печатается только как контроль: он не может быть **меньше** пика любого ребёнка.

**Разложение, а не одно число.** Шаги идут в том же порядке, что у боевого пути
(``core/pipeline/extraction_runtime.py::_run_extract``): потоковый ``sha256``
retained-байтов → экстрактор → финализация + ``content_hash`` → конверт кэша →
``pickle``. Где шаг внутри адаптера не отделяется (накопление страниц и склейка у
``text_layer``, постраничное состояние внутри ``docling``), прибор пишет «не разделимо», а
не выдумывает число. Для ``text_layer`` есть отдельная проба ``--page-copies``: она
**воспроизводит** постраничное накопление адаптера (список строк → склейка) в своём
процессе и показывает цену двух копий, которые в самом вызове не разделить.

Запуск::

    # явные файлы
    .venv/bin/python scripts/measure_extract_peak.py --pdf /path/a.pdf --mechanism text_layer

    # из операторского state.db: N документов с наибольшим числом страниц
    .venv/bin/python scripts/measure_extract_peak.py \\
        --from-state-db ~/.cohound/stands/ru-pg-dbms/state/ru-pg-dbms/state.db --top 3 \\
        --mechanism text_layer --json

    # structured: уровень и устройство объявлены, иначе замер не про то
    .venv/bin/python scripts/measure_extract_peak.py --from-state-db … --top 1 \\
        --mechanism structured --level layout_tables --device cpu

**Хостовый предохранитель, а не потолок методологии.** ``structured`` на документе
больше ``--guard-pages`` (200) страниц прибор отказывается запускать: на измеренном хосте
(6.8 GB RAM) это уронит машину, а не снимет число. Отказ называется вслух и снимается
явным ``--allow-large`` — это цена хоста, а не предел метода (A3: не ограничивать, а
измерять; если хост позволяет — измеряй).

Код выхода: 0 — замер снят, 2 — не найдено входных данных (или отказ предохранителя).
Нейтрален к предметной категории (§1.3): читает файлы и строки ``document_version``,
не зная, о чём документы.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pickle
import resource
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

#: Префикс служебной строки дочернего процесса: stdout принадлежит машине, не человеку.
_CHILD_PREFIX = "__PEAK_JSON__"

#: Размер блока потокового чтения retained-байтов (как у боевого пути).
_READ_CHUNK = 1 << 20

#: Порог хостового предохранителя для ``structured`` (страниц). См. докстринг модуля.
_GUARD_PAGES = 200


# --------------------------------------------------------------------------- #
# Измерение памяти
# --------------------------------------------------------------------------- #


def _proc_rss_mb() -> float | None:
    """Текущий RSS процесса, MB (``/proc/self/status:VmRSS``); ``None`` — не Linux."""
    try:
        with open("/proc/self/status", encoding="ascii") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    return round(float(line.split()[1]) / 1024.0, 1)
    except (OSError, ValueError, IndexError):
        return None
    return None


def _ru_maxrss_mb() -> float:
    """Пик RSS процесса, MB (``RUSAGE_SELF.ru_maxrss``; Linux отдаёт KiB)."""
    return round(float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024.0, 1)


def _children_maxrss_mb() -> float:
    """Пик RSS по **всем** завершённым детям, MB (кросс-проверка, не цена одного замера)."""
    return round(float(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss) / 1024.0, 1)


class _Steps:
    """Накопитель разложения: шаг → RSS до/после, поднятый потолок, секунды.

    ``peak_delta_mb`` — насколько шаг **поднял потолок** процесса; ``rss_delta_mb`` —
    сколько он добавил к текущему RSS. Две величины расходятся законно: шаг может
    выделить и сразу освободить память (текущий RSS вернулся, потолок остался).
    """

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self._started = time.perf_counter()
        self._last_mark = self._started
        self._rss = _proc_rss_mb()
        self._peak = _ru_maxrss_mb()

    def mark(self, step: str, note: str | None = None) -> None:
        """Фиксирует шаг: ``step`` уже выполнен, значения снимаются после него."""
        now = time.perf_counter()
        rss = _proc_rss_mb()
        peak = _ru_maxrss_mb()
        self.rows.append(
            {
                "step": step,
                "seconds": round(now - self._last_mark, 3),
                "rss_before_mb": self._rss,
                "rss_after_mb": rss,
                "rss_delta_mb": (
                    None if rss is None or self._rss is None else round(rss - self._rss, 1)
                ),
                "peak_mb": peak,
                "peak_delta_mb": round(peak - self._peak, 1),
                "note": note,
            }
        )
        self._last_mark = now
        self._rss = rss
        self._peak = peak

    @property
    def elapsed(self) -> float:
        """Полное время замера, с."""
        return round(time.perf_counter() - self._started, 3)


# --------------------------------------------------------------------------- #
# Дочерний процесс: один документ, один механизм
# --------------------------------------------------------------------------- #


def _sha256_stream(path: Path) -> str:
    """``sha256`` файла потоково — тело в RAM не поднимается (как в боевом пути)."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(_READ_CHUNK)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _build_extractor(mechanism: str, level: str, device: str) -> Any:
    """Экстрактор объявленного механизма; импорт — здесь, чтобы шаг был виден в разложении."""
    if mechanism == "text_layer":
        from core.adapters.extract_pdf_text import PdfiumDocumentExtractor

        return PdfiumDocumentExtractor()
    from core.adapters.extract_document import DoclingDocumentExtractor
    from core.domain.enums import DocumentLevel, PdfDevice

    return DoclingDocumentExtractor(
        DocumentLevel(level), pdf_device=PdfDevice(device)
    )


def _is_pdf(path: Path) -> bool:
    """PDF ли это — по сигнатуре файла (retained-файл суффикса формата не несёт)."""
    try:
        with path.open("rb") as handle:
            return handle.read(5).startswith(b"%PDF")
    except OSError:
        return False


def _page_copies_probe(path: Path, out: dict[str, Any]) -> dict[str, Any]:
    """Проба цены **двух копий** постраничного чтения ``text_layer`` (свой процесс).

    Воспроизводит ровно то накопление, которое делает
    ``PdfiumDocumentExtractor._convert``: список строк «страница → текст», затем склейка
    через разделитель. Сам вызов адаптера этого разделить не даёт (обе копии живут
    одновременно внутри одного вызова), поэтому цена списка и цена склейки снимаются
    здесь отдельными метками. Это **модель** шага, а не вызов адаптера — и так и названо.
    """
    import pypdfium2 as pdfium  # локально: цена импорта не должна попасть в шаг накопления

    steps = _Steps()
    steps.mark("start")
    pdf = pdfium.PdfDocument(str(path))
    segments: list[str] = []
    for index in range(len(pdf)):
        page = pdf[index]
        try:
            textpage = page.get_textpage()
            try:
                segments.append(textpage.get_text_range())
            finally:
                textpage.close()
        finally:
            page.close()
    steps.mark("pages_list", "список строк «страница → текст» (модель цикла адаптера)")
    joined = "\n\n".join(segments)
    steps.mark("join", "склейка страниц в один текст (вторая копия)")
    pdf.close()
    out["steps"] = steps.rows
    out["page_count"] = len(segments)
    out["chars"] = len(joined)
    out["peak_mb"] = _ru_maxrss_mb()
    out["seconds"] = steps.elapsed
    return out


def _trim(steps: _Steps, name: str) -> None:
    """Возвращает свободные арены ОС — тот же шаг, что делает боевой воркер.

    ``_extract_worker`` зовёт ``_trim_worker_arenas`` → ``malloc_trim(0)`` после **каждого**
    документа, до возврата результата. Без этого шага цена следующего документа была бы
    завышена собственным мусором прибора, а не работой.
    """
    try:
        from core.util.memory import release_free_arenas

        trimmed = release_free_arenas()
        steps.mark(
            name,
            "malloc_trim(0), как в боевом воркере после документа"
            + ("" if trimmed else " (рычаг недоступен)"),
        )
    except Exception as exc:  # noqa: BLE001 — гигиена памяти не ломает замер
        steps.mark(name, f"не выполнено: {type(exc).__name__}: {exc}")


def _run_child(args: argparse.Namespace) -> dict[str, Any]:
    """Один замер в дочернем процессе: шаги в порядке боевого пути ``_run_extract``."""
    # В дочернем режиме приходит один файл; в родительском ``--pdf`` — список.
    raw = args.pdf[-1] if isinstance(args.pdf, list) else args.pdf
    path = Path(raw)
    out: dict[str, Any] = {
        "path": str(path),
        "bytes": path.stat().st_size if path.is_file() else None,
        "mechanism": args.mechanism,
        "level": None if args.mechanism == "text_layer" else args.level,
        "device": None if args.mechanism == "text_layer" else args.device,
    }
    if args.probe == "page-copies":
        return _page_copies_probe(path, out)

    steps = _Steps()
    steps.mark(
        "start",
        "процесс после импортов скрипта (пол питона, без моделей)",
    )

    extractor = _build_extractor(args.mechanism, args.level, args.device)
    steps.mark("extractor_construct", f"version={getattr(extractor, 'version', '?')}")

    if args.mechanism == "structured":
        # Веса и рантайм (torch, layout, TableFormer) — «пол» пика. Строим конвертер
        # отдельным шагом: иначе он сольётся с извлечением и пол станет невидим.
        # Приватный метод: другого входа «построить конвертер, не извлекая» нет.
        try:
            extractor._converter = extractor._build_converter(pdf=True)  # noqa: SLF001
            steps.mark("converter_build", "веса и рантайм docling (пол пика)")
        except Exception as exc:  # noqa: BLE001 — шаг не разделим, это и говорим
            steps.mark("converter_build", f"не разделимо: {type(exc).__name__}: {exc}")

    # Шаг 1: потоковое чтение retained-байтов (ключ кэша). Тело в RAM не поднимается.
    digest = _sha256_stream(path)
    steps.mark("read_bytes", f"sha256 потоково, {_READ_CHUNK >> 10} KiB блоками")

    # Шаг 2: извлечение. Внутри адаптера — накопление страниц и склейка; отдельно не
    # разделимо (для text_layer цена копий снимается пробой --page-copies).
    started = time.perf_counter()
    doc = extractor.extract_path(str(path), filename_hint="document.pdf")
    extract_seconds = round(time.perf_counter() - started, 3)
    steps.mark(
        "extract",
        "вызов адаптера; накопление страниц и склейка внутри — не разделимо",
    )

    out["page_count"] = getattr(doc, "page_count", None)
    out["error"] = getattr(doc, "error", None)
    out["partial"] = getattr(doc, "partial", None)
    out["extractor_version"] = getattr(doc, "extractor_version", None)
    out["chars"] = len(getattr(doc, "text", "") or "")
    out["extract_seconds"] = extract_seconds

    # Шаг 3: финализация + content_hash **отдельным вызовом** — это ровно та вторая
    # копия текста, которую боевой путь платит внутри читателя (и которую здесь видно
    # отдельно, потому что она не первая).
    text = getattr(doc, "text", "") or ""
    finalized = None
    if text:
        from core.util.sanitize import finalize_extracted_text

        finalized = finalize_extracted_text(text)
        hashlib.sha256(finalized.encode("utf-8")).hexdigest()
        steps.mark(
            "finalize_hash",
            "вторая копия текста: очистка + sha256 (в боевом пути — внутри читателя)",
        )
    else:
        steps.mark("finalize_hash", "пропущено: текста нет")

    # Шаг 4: конверт кэша. Форма воспроизведена по ExtractionCache.put (файл не пишется);
    # цена определяется текстом, а не набором полей.
    envelope = {
        "format": "extracted-doc-v5",
        "content_hash": digest,
        "extractor_version": getattr(doc, "extractor_version", "") or "",
        "level": out["level"] or "",
        "text": text,
        "page_count": getattr(doc, "page_count", None),
        "family": getattr(doc, "family", None),
        "doc_content_hash": getattr(doc, "content_hash", ""),
        "ocr_probe": getattr(doc, "ocr_probe", None),
        "page_map": getattr(doc, "page_map", None),
    }
    payload = json.dumps(
        envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    steps.mark("json_envelope", f"json.dumps конверта, {len(payload)} симв.")
    out["envelope_bytes"] = len(payload.encode("utf-8"))

    # Шаг 5: запись в кэш боевым классом (json.dumps + атомарная запись + fsync).
    try:
        from core.pipeline.extract_cache import ExtractionCache

        with tempfile.TemporaryDirectory(prefix="cohound-peak-") as sandbox:
            cache = ExtractionCache(sandbox)
            cache.put(doc, level=out["level"] or "", content_hash=digest)
            written = list(Path(sandbox).rglob("*.json"))
            steps.mark(
                "cache_put",
                f"ExtractionCache.put в песочницу, файлов {len(written)}",
            )
            del written
    except Exception as exc:  # noqa: BLE001 — шаг необязательный, сбой называем
        steps.mark("cache_put", f"не выполнено: {type(exc).__name__}: {exc}")

    # Шаг 6: pickle — граница процесса (воркер отдаёт результат родителю).
    blob = pickle.dumps(doc, protocol=pickle.HIGHEST_PROTOCOL)
    steps.mark("pickle", f"pickle.dumps, {len(blob)} байт")

    del blob
    if finalized is not None:
        del finalized
    # Маргинальная цена следующего документа: результат предыдущего освобождён, а веса
    # конвертера уже загружены. Разделяет «пол» (платится один раз на процесс) и цену
    # документа — вопрос окна воркеров ровно про это различие.
    del doc
    del text
    _trim(steps, "trim_0")
    for index in range(1, max(1, int(args.repeats))):
        started = time.perf_counter()
        repeated = extractor.extract_path(str(path), filename_hint="document.pdf")
        steps.mark(
            f"extract_repeat_{index}",
            f"тот же документ повторно за {round(time.perf_counter() - started, 3)} s; "
            "предыдущий результат освобождён, веса загружены",
        )
        del repeated
        _trim(steps, f"trim_{index}")

    out["steps"] = steps.rows
    out["peak_mb"] = _ru_maxrss_mb()
    out["seconds"] = steps.elapsed
    return out


# --------------------------------------------------------------------------- #
# Родитель: выбор документов и запуск замеров
# --------------------------------------------------------------------------- #


def _documents_from_state_db(
    db: str, top: int, *, min_pages: int, max_pages: int | None
) -> list[dict[str, Any]]:
    """Документы с наибольшим числом страниц из ``state.db:document_version``.

    Читается только объявленный факт: ``extract_page_count`` (заявлено документом),
    ``raw_path`` (retained-байты) и штамп экстрактора. Формат определяется сигнатурой
    файла, а не суффиксом: retained-файл называется ``{sha256}.bin``.
    """
    if not Path(db).is_file():
        return []
    uri = f"file:{Path(db).expanduser().resolve()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        rows = connection.execute(
            "select extract_page_count, content_hash, extractor_version, level, raw_path"
            " from document_version"
            " where raw_path is not null and extract_page_count is not null"
            " and extract_page_count >= ?"
            " order by extract_page_count desc",
            (min_pages,),
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        connection.close()
    picked: list[dict[str, Any]] = []
    seen: set[str] = set()
    for page_count, content_hash, extractor_version, level, raw_path in rows:
        if max_pages is not None and int(page_count) > max_pages:
            continue
        path = Path(raw_path)
        if not path.is_file() or not _is_pdf(path):
            continue
        key = str(path)
        if key in seen:  # одна версия на несколько URL — мерим один раз
            continue
        seen.add(key)
        picked.append(
            {
                "path": key,
                "page_count": int(page_count),
                "content_hash": content_hash,
                "recorded_extractor_version": extractor_version,
                "recorded_level": level,
            }
        )
        if len(picked) >= top:
            break
    return picked


def _run_one(
    path: str,
    *,
    mechanism: str,
    level: str,
    device: str,
    probe: str | None,
    timeout: float,
    verbose: bool,
    repeats: int = 1,
) -> dict[str, Any]:
    """Запускает дочерний процесс на один документ и возвращает его JSON."""
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--child",
        "--pdf",
        path,
        "--mechanism",
        mechanism,
        "--level",
        level,
        "--device",
        device,
        "--repeats",
        str(max(1, repeats)),
    ]
    if probe is not None:
        command.extend(["--probe", probe])
    started = time.perf_counter()
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(REPO),
        )
    except subprocess.TimeoutExpired:
        return {"path": path, "error": f"timeout {timeout} s", "steps": []}
    wall = round(time.perf_counter() - started, 3)
    if verbose and completed.stderr:
        sys.stderr.write(completed.stderr[-4000:])
    payload: dict[str, Any] | None = None
    for line in completed.stdout.splitlines():
        if line.startswith(_CHILD_PREFIX):
            try:
                payload = json.loads(line[len(_CHILD_PREFIX) :])
            except ValueError:
                payload = None
    if payload is None:
        return {
            "path": path,
            "error": f"дочерний процесс не дал замера (exit {completed.returncode})",
            "stderr_tail": completed.stderr[-2000:],
            "steps": [],
        }
    payload["wall_seconds"] = wall
    payload["children_peak_mb"] = _children_maxrss_mb()
    return payload


def _print_table(results: list[dict[str, Any]]) -> None:
    """Человекочитаемое разложение: документ → шаг → RSS до/после/потолок."""
    for result in results:
        print("=" * 78)
        head = (
            f"{Path(result['path']).name} | страниц {result.get('page_count')} | "
            f"механизм {result.get('mechanism')}"
        )
        if result.get("level"):
            head += f"/{result.get('level')}"
        if result.get("device"):
            head += f" | устройство {result.get('device')}"
        print(head)
        print(
            f"  пик процесса: {result.get('peak_mb')} MB | время {result.get('seconds')} s | "
            f"символов {result.get('chars')} | error={result.get('error')} "
            f"partial={result.get('partial')}"
        )
        if result.get("error") and not result.get("steps"):
            print(f"  ОТКАЗ: {result['error']}")
            continue
        print(
            f"  {'шаг':<20}{'RSS до':>10}{'RSS после':>11}{'Δ RSS':>9}"
            f"{'потолок':>10}{'Δ потолка':>11}{'с':>8}"
        )
        for row in result["steps"]:
            print(
                f"  {row['step']:<20}"
                f"{_fmt(row['rss_before_mb']):>10}{_fmt(row['rss_after_mb']):>11}"
                f"{_fmt(row['rss_delta_mb']):>9}{_fmt(row['peak_mb']):>10}"
                f"{_fmt(row['peak_delta_mb']):>11}{row['seconds']:>8.3f}"
                + (f"   {row['note']}" if row.get("note") else "")
            )
    print("=" * 78)


def _fmt(value: float | None) -> str:
    """Значение MB для таблицы; отсутствие — ``—`` (а не ноль: это разные факты)."""
    return "—" if value is None else f"{value:.0f}"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Пик RSS одного извлечения с разложением на копии (O33)",
    )
    parser.add_argument("--pdf", action="append", default=[], help="явный файл (можно несколько)")
    parser.add_argument("--from-state-db", dest="state_db", help="state.db: выбрать из document_version")
    parser.add_argument("--top", type=int, default=1, help="сколько документов взять (по страницам)")
    parser.add_argument("--min-pages", type=int, default=1, help="нижняя граница страниц при выборе")
    parser.add_argument("--max-pages", type=int, default=None, help="верхняя граница страниц при выборе")
    parser.add_argument(
        "--mechanism", choices=("text_layer", "structured"), default="text_layer"
    )
    parser.add_argument(
        "--level",
        default="layout_tables",
        help="уровень для structured (у text_layer уровней нет)",
    )
    parser.add_argument("--device", choices=("cpu", "cuda"), default="cpu")
    parser.add_argument(
        "--page-copies",
        action="store_true",
        help="для text_layer: отдельная проба цены списка страниц и склейки",
    )
    parser.add_argument("--guard-pages", type=int, default=_GUARD_PAGES, help="хостовый предохранитель structured")
    parser.add_argument("--allow-large", action="store_true", help="снять предохранитель явно")
    parser.add_argument("--timeout", type=float, default=1800.0, help="таймаут одного замера, с")
    parser.add_argument(
        "--repeats",
        type=int,
        default=1,
        help="сколько раз повторить извлечение в том же процессе (маргинальная цена)",
    )
    parser.add_argument("--json", action="store_true", help="машинный вывод")
    parser.add_argument("--verbose", action="store_true", help="показать stderr дочерних процессов")
    # Служебное: режим дочернего процесса.
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--probe", default=None, help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Точка входа: дочерний режим печатает JSON, родительский ведёт замеры."""
    args = _parse_args(argv)

    if args.child:
        result = _run_child(args)
        print(_CHILD_PREFIX + json.dumps(result, ensure_ascii=False))
        return 0

    documents: list[dict[str, Any]] = []
    missing: list[str] = []
    for raw in args.pdf:
        candidate = Path(raw).expanduser().resolve()
        if candidate.is_file():
            documents.append({"path": str(candidate)})
        else:
            missing.append(str(candidate))
    for path in missing:
        print(f"файла нет: {path}", file=sys.stderr)
    if args.state_db:
        documents.extend(
            _documents_from_state_db(
                args.state_db, args.top, min_pages=args.min_pages, max_pages=args.max_pages
            )
        )
    if not documents:
        print("нет входных данных: ни --pdf, ни подходящих строк в --from-state-db", file=sys.stderr)
        return 2

    if args.mechanism == "structured":
        oversized = [
            item
            for item in documents
            if (item.get("page_count") or _pdf_pages(item["path"])) > args.guard_pages
        ]
        if oversized and not args.allow_large:
            for item in oversized:
                print(
                    f"отказ хостового предохранителя: {item['path']} "
                    f"({item.get('page_count') or _pdf_pages(item['path'])} страниц) "
                    f"> {args.guard_pages}; structured на таком документе роняет хост. "
                    "Это цена машины, а не предел метода: снять — --allow-large",
                    file=sys.stderr,
                )
            return 2

    results: list[dict[str, Any]] = []
    for item in documents:
        path = item["path"]
        measured = _run_one(
            path,
            mechanism=args.mechanism,
            level=args.level,
            device=args.device,
            probe=None,
            timeout=args.timeout,
            verbose=args.verbose,
            repeats=args.repeats,
        )
        for key in ("page_count", "content_hash", "recorded_extractor_version", "recorded_level"):
            if key in item and measured.get(key) is None:
                measured[key] = item[key]
        results.append(measured)
        if args.page_copies and args.mechanism == "text_layer":
            probe = _run_one(
                path,
                mechanism=args.mechanism,
                level=args.level,
                device=args.device,
                probe="page-copies",
                timeout=args.timeout,
                verbose=args.verbose,
            )
            probe["probe"] = "page-copies"
            results.append(probe)

    if args.json:
        print(json.dumps(results, ensure_ascii=False, indent=2))
    else:
        _print_table(results)
    return 0 if results else 2


def _pdf_pages(path: str) -> int:
    """Число страниц по сигнатуре файла (для предохранителя); 0 — не прочитать."""
    try:
        import pypdfium2 as pdfium

        pdf = pdfium.PdfDocument(path)
        try:
            return len(pdf)
        finally:
            pdf.close()
    except Exception:  # noqa: BLE001 — предохранитель без числа не срабатывает молча
        return 0


if __name__ == "__main__":
    sys.exit(main())
