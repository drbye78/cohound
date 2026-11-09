#!/usr/bin/env python3
"""Микрозамер стоимости ``safe_cut_offsets`` на патологическом абзаце (план 26, O17).

**Что меряется.** ``core.adapters.md_ast.safe_cut_offsets`` — функция, отдающая
допустимые позиции разреза абзаца. Вызывается чанкером markdown на **каждый абзац**
(``core/adapters/chunker_markdown.py:375``), поэтому её цена умножается на число абзацев
документа. Сеть не используется, входы синтетические и детерминированные.

**Классы входа** (все — один абзац, без пустых строк):

``plain_ru``
    Обычная русская проза: пробел каждые ~6 символов, точки/точки с запятой. Базовая линия.
``minified_js``
    Минифицированный JS без пробелов: ``;``, ``{}``, ``<>``, кавычки, template-строки.
    Реалистичный вид «вставленного как абзац» кода.
``minified_js_lt``
    Он же, но худший случай внутри класса: сравнения ``a<b`` и **ни одного** ``>`` в
    тексте. Проверяет путь ``re.match(r"<[A-Za-z/!?][^>\\n]*>", text[position:end])``,
    который на каждый ``<`` копирует срез до конца абзаца.
``sparse_words``
    Редкие пробелы: «слова» по 189 символов, пробел раз в 190. Кандидатов мало.
``markdown_log``
    Лог в markdown: ISO-время, уровни, поля ``key=value``, ``;``, точки, JSON-фрагменты.
``opaque_blob``
    Контроль: base64-подобная строка без пробелов и без знаков конца предложения —
    кандидатов ноль, цикл обязан быть дешёвым при любой длине.

**Прибор.** Время — ``time.perf_counter``: один прогрев, затем N повторов; публикуются
медиана и max. Память — ``tracemalloc``: пик одного вызова, tracemalloc стартует **после**
сборки входа, поэтому в пик не входит сам текст. Диагностика — отдельный замер
``_protected_ranges`` и число найденных диапазонов: он показывает, где уходит время —
в сканере защищённых конструкций или в цикле кандидатов.

**Режим ``--cache``** (необязательный) отвечает на вопрос «а такие абзацы бывают?» на
реальном кэше извлечения: кэш читается **только на чтение**, документы разбираются тем же
``parse_markdown`` (профиль ``gfm``), абзацные блоки прогоняются через ``safe_cut_offsets``
ровно как у чанкера. Публикуются распределение длин абзацев и суммарная стоимость на
документ. Без флага режим не исполняется.

**Границы прибора** (хост 7 GB, общий):
  * у каждого вызова свой жёсткий дедлайн (``SIGALRM``); превышение — результат
    «таймаут», а не зависание процесса;
  * длина 400k исполняется только если медиана 100k < 0.5 с **и** пик 100k < 64 MiB;
    иначе строка помечается «не исполнено» с названной причиной — это и есть граница
    прибора, а не молчаливый пропуск;
  * ``--cache`` ограничен 200 документами и 180 с стены; выборка детерминирована
    (сортировка путей + шаг), поэтому повтор даёт те же файлы.

Запуск::

    .venv/bin/python scripts/measure_safe_cut_offsets.py
    .venv/bin/python scripts/measure_safe_cut_offsets.py --json /tmp/opencode/o17.json
    .venv/bin/python scripts/measure_safe_cut_offsets.py --cache ~/.cohound/data/extract_cache
"""

from __future__ import annotations

import argparse
import json
import platform
import signal
import statistics
import sys
import time
import tracemalloc
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from functools import partial
from pathlib import Path
from types import FrameType
from typing import Final

from core.adapters.md_ast import _protected_ranges, parse_markdown, safe_cut_offsets

#: Длины абзаца в символах. 400k — только за гейтом (см. ``_gate_400k``).
LENGTHS: Final = (1_000, 10_000, 100_000, 400_000)

#: Повторы на длину: мелкие входы дешевле, поэтому повторов больше.
REPEATS: Final = {1_000: 7, 10_000: 5, 100_000: 3, 400_000: 2}

#: Дедлайн одного вызова, секунды.
TIMEOUT_S: Final = {1_000: 5.0, 10_000: 5.0, 100_000: 10.0, 400_000: 15.0}

#: Гейт 400k: медиана и пик на 100k должны быть ниже этих значений.
GATE_MEDIAN_MS: Final = 500.0
GATE_PEAK_MIB: Final = 64.0


class _Timeout(Exception):
    """Превышен жёсткий дедлайн одного вызова."""


def _raise_timeout(signum: int, frame: FrameType | None) -> None:
    raise _Timeout


@contextmanager
def _deadline(seconds: float) -> Iterator[None]:
    """Жёсткий дедлайн на блок: по истечении поднимается :class:`_Timeout`."""
    previous = signal.signal(signal.SIGALRM, _raise_timeout)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, previous)


def _timed(fn: Callable[[], object], timeout: float) -> float | None:
    """Время одного вызова в миллисекундах; ``None`` — таймаут."""
    try:
        with _deadline(timeout):
            started = time.perf_counter()
            fn()
            return (time.perf_counter() - started) * 1000.0
    except _Timeout:
        return None


def _peak_mib(fn: Callable[[], object], timeout: float) -> float | None:
    """Пик ``tracemalloc`` одного вызова в МиБ; ``None`` — таймаут."""
    tracemalloc.start()
    try:
        with _deadline(timeout):
            tracemalloc.reset_peak()
            fn()
            _, peak = tracemalloc.get_traced_memory()
            return peak / (1024.0 * 1024.0)
    except _Timeout:
        return None
    finally:
        tracemalloc.stop()


# ---------------------------------------------------------------------------
# Синтетические входы: детерминированы, без сети и без случайности
# ---------------------------------------------------------------------------


def _cycle(units: Sequence[str], length: int) -> str:
    """Повторяет единицы по кругу и обрезает до точной длины."""
    parts: list[str] = []
    size = 0
    index = 0
    while size < length:
        unit = units[index % len(units)]
        parts.append(unit)
        size += len(unit)
        index += 1
    return "".join(parts)[:length]


def plain_ru(length: int) -> str:
    """Обычная проза: базовая линия стоимости."""
    unit = (
        "Поставляемая конфигурация проверяет журнал транзакций и сохраняет снимок "
        "состояния перед каждой контрольной точкой. "
    )
    return _cycle((unit,), length)


def minified_js(length: int) -> str:
    """Минифицированный JS без пробелов: ``;``, ``{}``, ``<>``, кавычки."""
    units = (
        "var a=1,b=2,c=3;",
        "function f(x,y){return x<y?a:b;}",
        "function g(t){if(t<=0){return null;}return t*t+1;}",
        "for(var i=0;i<10;i++){a+=i;}",
        "var s='k'+a+';';",
        "var q=`v=${a}`;",
        "var o={k:[1,2,3],m:{n:4}};",
        "while(c>0){c--;}",
        "var h=x=>x<3?x:-x;",
        "document.write('<b>'+a+'</b>');",
    )
    return _cycle(units, length)


def minified_js_lt(length: int) -> str:
    """Худший случай: ``<`` без единого ``>`` — срез до конца абзаца на каждый ``<``."""
    units = (
        "var a=1,b=2;",
        "if(a<b){c=1;}",
        "x<y?1:2;",
        "while(a<b){a=a+1;}",
        "f(a,b);",
    )
    text = _cycle(units, length)
    if ">" in text:
        raise AssertionError("класс minified_js_lt обязан не содержать '>'")
    return text


def sparse_words(length: int) -> str:
    """Редкие пробелы: «слово» 189 символов, пробел раз в 190."""
    return _cycle(("ж" * 189 + " ",), length)


def markdown_log(length: int) -> str:
    """Лог в markdown: ISO-время, уровни, ``key=value``, ``;``, JSON-фрагменты."""
    units = (
        "2026-11-09T10:00:00.123Z INFO worker-3 fetched /api/v1/items?page=2 "
        "status=200 in 41ms; retry=0.\n",
        "2026-11-09T10:00:01.456Z WARN worker-1 slow query took 2.5s; pool=8/8.\n",
        '2026-11-09T10:00:02.789Z DEBUG worker-2 payload {"items":[1,2,3],"ok":true}.\n',
        "2026-11-09T10:00:03.012Z ERROR worker-4 timeout after 30s; attempt=1.\n",
    )
    return _cycle(units, length)


def opaque_blob(length: int) -> str:
    """Контроль: base64 без пробелов и без знаков конца предложения."""
    unit = "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0NTY3ODkrLw"
    return _cycle((unit,), length)


#: Класс → генератор. Порядок задаёт порядок строк в отчёте.
CLASSES: Final[dict[str, Callable[[int], str]]] = {
    "plain_ru": plain_ru,
    "minified_js": minified_js,
    "minified_js_lt": minified_js_lt,
    "sparse_words": sparse_words,
    "markdown_log": markdown_log,
    "opaque_blob": opaque_blob,
}


# ---------------------------------------------------------------------------
# Замер
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class Measurement:
    """Строка таблицы: один класс × одна длина."""

    cls: str
    length: int
    median_ms: float | None = None
    max_ms: float | None = None
    peak_mib: float | None = None
    protected_ms: float | None = None
    ranges: int | None = None
    cuts: int | None = None
    repeats: int = 0
    note: str = ""

    @property
    def ok(self) -> bool:
        return self.median_ms is not None


def measure_case(cls: str, length: int, text: str) -> Measurement:
    """Время, пик памяти и диагностика для одного входа."""
    timeout = TIMEOUT_S[length]
    repeats = REPEATS[length]
    call = partial(safe_cut_offsets, text, 0, len(text))
    result = Measurement(cls=cls, length=length)

    warm = _timed(call, timeout)
    if warm is None:
        result.note = f"таймаут >{timeout:.0f} с на прогреве"
        return result

    samples = [warm]
    for index in range(1, repeats):
        value = _timed(call, timeout)
        if value is None:
            result.note = f"таймаут >{timeout:.0f} с на повторе {index + 1}"
            break
        samples.append(value)

    result.median_ms = statistics.median(samples)
    result.max_ms = max(samples)
    result.repeats = len(samples)

    peak = _peak_mib(call, timeout)
    if peak is None:
        result.note = (result.note + "; " if result.note else "") + "tracemalloc-проход: таймаут"
    else:
        result.peak_mib = peak

    result.protected_ms = _timed(
        lambda: _protected_ranges(text, 0, len(text), math=False), timeout
    )
    if result.protected_ms is not None:
        result.ranges = len(_protected_ranges(text, 0, len(text), math=False))
        result.cuts = len(safe_cut_offsets(text, 0, len(text)))
    return result


def _gate_400k(previous: Measurement) -> tuple[bool, str]:
    """Пускать ли 400k: по числу, а не по вкусу (граница прибора — 7 GB хоста)."""
    if not previous.ok:
        return False, "100k не уложился в дедлайн"
    median = previous.median_ms or 0.0
    peak = previous.peak_mib or 0.0
    if median >= GATE_MEDIAN_MS:
        return False, f"медиана 100k {median:.1f} мс ≥ {GATE_MEDIAN_MS:.0f} мс"
    if peak >= GATE_PEAK_MIB:
        return False, f"пик 100k {peak:.1f} МиБ ≥ {GATE_PEAK_MIB:.0f} МиБ"
    return True, ""


def run(lengths: Sequence[int]) -> list[Measurement]:
    """Полная матрица класс × длина с гейтом на 400k."""
    rows: list[Measurement] = []
    for cls, factory in CLASSES.items():
        previous: Measurement | None = None
        for length in lengths:
            if length >= 400_000 and previous is not None:
                allowed, reason = _gate_400k(previous)
                if not allowed:
                    rows.append(
                        Measurement(
                            cls=cls,
                            length=length,
                            note=f"не исполнено: {reason} (граница прибора)",
                        )
                    )
                    break
            text = factory(length)
            row = measure_case(cls, length, text)
            rows.append(row)
            previous = row
            if not row.ok and length >= 100_000:
                break
    return rows


# ---------------------------------------------------------------------------
# Реальный корпус (только чтение): бывают ли такие абзацы
# ---------------------------------------------------------------------------

#: Сколько документов кэша читать в режиме ``--cache`` и сколько секунд стены тратить.
CORPUS_LIMIT: Final = 200
CORPUS_BUDGET_S: Final = 180.0


#: Пороги длин абзацев, по которым публикуется счётчик (сколько абзацев длиннее).
SPAN_THRESHOLDS: Final = (3_000, 10_000, 50_000)


@dataclass(slots=True)
class CorpusReport:
    """Сводка по реальному кэшу извлечения."""

    cache: str
    files_total: int
    docs_measured: int
    chars: int
    paragraphs: int
    spans: list[int]
    total_ms: float
    timeouts: int
    worst: list[tuple[float, int, str]]
    slowest_docs: list[tuple[float, int, int, str]]

    def above(self, threshold: int) -> int:
        """Сколько абзацев длиннее порога."""
        return sum(1 for span in self.spans if span > threshold)


def _percentile(values: Sequence[int], share: float) -> int:
    """Ближайший ранг перцентиля: значения целочисленные, интерполяция не нужна."""
    if not values:
        return 0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(share * (len(ordered) - 1))))
    return ordered[index]


def corpus_measure(cache: Path, limit: int = CORPUS_LIMIT) -> CorpusReport:
    """Абзацы реального корпуса: длины и стоимость ``safe_cut_offsets`` по документам."""
    files = sorted(cache.rglob("*.json"))
    if not files:
        raise SystemExit(f"кэш пуст или не найден: {cache}")
    stride = max(1, len(files) // limit)
    sample = files[::stride][:limit]

    spans: list[int] = []
    worst: list[tuple[float, int, str]] = []
    slowest_docs: list[tuple[float, int, int, str]] = []
    chars = 0
    paragraphs = 0
    total_ms = 0.0
    timeouts = 0
    started = time.perf_counter()

    for path in sample:
        if time.perf_counter() - started > CORPUS_BUDGET_S:
            break
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        text = payload.get("text") or ""
        if not text:
            continue
        doc = parse_markdown(text, profile="gfm")
        source = doc.text
        digest = str(payload.get("content_hash") or path.stem)
        doc_ms = 0.0
        doc_paragraphs = 0
        for block in doc.blocks:
            for item in block.walk():
                if item.kind != "paragraph":
                    continue
                start, end = item.span.start_char, item.span.end_char
                spans.append(end - start)
                doc_paragraphs += 1
                call = partial(safe_cut_offsets, source, start, end)
                elapsed = _timed(call, TIMEOUT_S[100_000])
                if elapsed is None:
                    timeouts += 1
                    continue
                doc_ms += elapsed
                worst.append((elapsed, end - start, digest))
        chars += len(source)
        paragraphs += doc_paragraphs
        total_ms += doc_ms
        slowest_docs.append((doc_ms, len(source), doc_paragraphs, digest))
        del doc, source

    worst.sort(reverse=True)
    slowest_docs.sort(reverse=True)
    return CorpusReport(
        cache=str(cache),
        files_total=len(files),
        docs_measured=len(slowest_docs),
        chars=chars,
        paragraphs=paragraphs,
        spans=spans,
        total_ms=total_ms,
        timeouts=timeouts,
        worst=worst[:5],
        slowest_docs=slowest_docs[:5],
    )


def _print_corpus(report: CorpusReport) -> None:
    spans = report.spans
    print()
    print("## Реальный кэш извлечения: длины абзацев и стоимость")
    print()
    print(f"кэш: `{report.cache}` (файлов {report.files_total}, прочитано {report.docs_measured})")
    print()
    print("| метрика | значение |")
    print("|---|---|")
    print(f"| символов | {report.chars} |")
    print(f"| абзацных блоков | {report.paragraphs} |")
    print(
        "| длина абзаца, символы | "
        f"p50 {_percentile(spans, 0.50)} · p90 {_percentile(spans, 0.90)} · "
        f"p99 {_percentile(spans, 0.99)} · max {max(spans) if spans else 0} |"
    )
    print(
        "| `safe_cut_offsets` всего | "
        f"{report.total_ms:.1f} мс на {report.chars} символов "
        f"({report.total_ms / max(report.chars, 1) * 1000.0:.3f} мс/1k) |"
    )
    for threshold in SPAN_THRESHOLDS:
        count = report.above(threshold)
        share = 100.0 * count / max(report.paragraphs, 1)
        print(f"| абзацев длиннее {threshold} | {count} ({share:.3f} %) |")
    print(f"| таймаутов на абзац | {report.timeouts} |")
    if report.worst:
        print()
        print("Самые дорогие абзацы:")
        print()
        print("| мс | длина, символов | документ (content_hash) |")
        print("|---|---|---|")
        for elapsed, length, digest in report.worst:
            print(f"| {elapsed:.2f} | {length} | `{digest[:16]}` |")
    if report.slowest_docs:
        print()
        print("Самые дорогие документы:")
        print()
        print("| мс | символов | абзацев | документ |")
        print("|---|---|---|---|")
        for elapsed, length, count, digest in report.slowest_docs:
            print(f"| {elapsed:.1f} | {length} | {count} | `{digest[:16]}` |")


# ---------------------------------------------------------------------------
# Вывод
# ---------------------------------------------------------------------------


def _num(value: float | None, digits: int = 2) -> str:
    return "—" if value is None else f"{value:.{digits}f}"


def _table(rows: Sequence[Measurement]) -> str:
    lines = [
        "| класс | n | медиана, мс | max, мс | пик, МиБ | `_protected_ranges`, мс "
        "| диапазонов | разрезов | повторов | примечание |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            f"| `{row.cls}` | {row.length} | {_num(row.median_ms)} | {_num(row.max_ms)} "
            f"| {_num(row.peak_mib, 3)} | {_num(row.protected_ms)} | "
            f"{'—' if row.ranges is None else row.ranges} | "
            f"{'—' if row.cuts is None else row.cuts} | {row.repeats} | {row.note or '—'} |"
        )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Микрозамер safe_cut_offsets на патологическом абзаце (план 26, O17).",
    )
    parser.add_argument(
        "--lengths",
        default=",".join(str(item) for item in LENGTHS),
        help="длины абзаца в символах через запятую (по умолчанию 1000,10000,100000,400000)",
    )
    parser.add_argument("--json", type=Path, default=None, help="куда сохранить матрицу")
    parser.add_argument(
        "--cache",
        type=Path,
        default=None,
        help="кэш извлечения для режима «реальные абзацы» (только чтение)",
    )
    parser.add_argument(
        "--cache-limit",
        type=int,
        default=CORPUS_LIMIT,
        help=f"сколько документов кэша читать (по умолчанию {CORPUS_LIMIT})",
    )
    args = parser.parse_args(argv)

    lengths = tuple(int(item) for item in args.lengths.split(",") if item.strip())
    rows = run(lengths)

    print(f"# safe_cut_offsets: микрозамер ({time.strftime('%Y-%m-%d')})")
    print()
    print(f"Python {platform.python_version()} · {platform.platform()}")
    print()
    print(_table(rows))

    corpus: CorpusReport | None = None
    if args.cache is not None:
        corpus = corpus_measure(args.cache, limit=args.cache_limit)
        _print_corpus(corpus)

    if args.json is not None:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "date": time.strftime("%Y-%m-%d"),
            "python": platform.python_version(),
            "platform": platform.platform(),
            "rows": [asdict(row) | {"ok": row.ok} for row in rows],
        }
        if corpus is not None:
            payload["corpus"] = {
                "cache": corpus.cache,
                "files_total": corpus.files_total,
                "docs_measured": corpus.docs_measured,
                "chars": corpus.chars,
                "paragraphs": corpus.paragraphs,
                "total_ms": corpus.total_ms,
                "timeouts": corpus.timeouts,
                "span_p50": _percentile(corpus.spans, 0.50),
                "span_p90": _percentile(corpus.spans, 0.90),
                "span_p99": _percentile(corpus.spans, 0.99),
                "span_max": max(corpus.spans) if corpus.spans else 0,
                "span_above": {
                    str(threshold): corpus.above(threshold) for threshold in SPAN_THRESHOLDS
                },
                "worst": corpus.worst,
                "slowest_docs": corpus.slowest_docs,
            }
        args.json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
        print()
        print(f"json: {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
