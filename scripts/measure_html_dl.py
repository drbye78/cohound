"""Замер D3: что даёт разворачивание списков определений ``<dl>/<dt>/<dd>`` до ``trafilatura``.

Инструмент отвечает на один вопрос: **сколько документов корпуса затронуто** дефектом
``<item>``-склейки и что меняется в извлечённом тексте, когда ``<dl>``/``<dt>``/``<dd>``
заменяются обычными блоками **до** извлечения.

Причина дефекта. ``trafilatura`` 2.3.1 в ``htmlprocessing.convert_lists()`` переписывает
``dl``/``ol``/``ul`` в собственные ``<list>``, а ``dt``/``dd``/``li`` — в ``<item>``
(``rend`` = ``dt-1``/``dd-1``…). Markdown-сериализатор печатает ``<item>`` **одной
строкой**, поэтому ``<div class="note"><h3>Примечание</h3><p>…</p></div>`` внутри ``<dd>``
приезжает как ``- текст ### Примечание текст``: заголовок перестаёт быть заголовком,
границы секций теряются, ``heading_path`` чанкера несёт абзац.

**Вход** — сохранённая БД прогона (``cohound.db``), читается **только на чтение**, тот же
срез, что у :mod:`measure_md_chunking`: ``--limit`` страниц на хост (дефолт 400), только
HTML-страницы (``content_type`` содержит ``html``), тела — по ``raw_path``. Сеть, запись
в БД и правки ``core/`` — вне контракта инструмента.

**Обе колонки — внутренние функции читателя, а не копия логики**: декодирование
(``_decode_page``), пре-клин (``strip_active_html``), объявление основного блока
(``_declare_main_region``), трансформация (``_neutralize_definition_lists``), извлечение
(``_default_extract``) и финальная очистка (``HtmlExtractor._postprocess``). Колонки
различаются **ровно одной** функцией, поэтому разность принадлежит ей.

**Метрики на документ** (определения зафиксированы):

``chars``
    Длина извлечённого текста до и после (после ``_postprocess``, то есть ровно то, что
    увидит чанкер).
``headings``
    Строки, начинающиеся с ATX-маркера (до трёх пробелов отступа, 1–6 ``#``, пробел).
``longest_heading``
    Длина самой длинной такой строки в символах.
``inline_defect_lines``
    Строки, где ATX-маркер стоит **внутри** строки (после снятия ведущих ``#``): ровно
    след ``<item>``-склейки. Настоящий заголовок сюда не попадает. Считаются маркеры из
    2–6 ``#``, блоки кода и инлайн-код исключены (там ``#`` — комментарий или
    psql-приглашение), поэтому величина — **нижняя граница**: потерянный ``<h1>`` внутри
    абзаца в неё не входит.

**Сводка**: сколько документов затронуто (изменилась любая из величин), сколько
строк-дефектов исчезло, суммарная дельта символов и её распределение (выросло /
уменьшилось / не изменилось), 3 примера до/после.

**Чего замер НЕ показывает**: качество чанков и ретривала на новом тексте (это отдельный
замер по индексной сборке), и эффект в корпусе — ``content_hash`` затронутых документов
изменится, поэтому он виден только после пере-краула, который здесь **не** делается.

Запуск:

    python3 scripts/measure_html_dl.py \\
        --db ~/.cohound/data/runs/ru-pg-dbms-1e0b04d6d83a/cohound.db --limit 400

    python3 scripts/measure_html_dl.py --db … --out-json /tmp/dl.json
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.adapters.extract_html import (  # noqa: E402
    HtmlExtractor,
    _declare_main_region,
    _default_extract,
    _neutralize_definition_lists,
)
from core.util.sanitize import strip_active_html  # noqa: E402


def _load_sibling(name: str):
    """Подгружает соседний скрипт как модуль (та же выборка, что у ``measure_md_chunking``)."""
    path = Path(__file__).with_name(f"{name}.py")
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"не удалось загрузить {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_mhe = _load_sibling("measure_html_extraction")

#: ATX-заголовок: до трёх пробелов отступа, 1–6 ``#``, пробел, текст.
_ATX_LINE = re.compile(r"^\s{0,3}#{1,6}\s+\S")

#: ATX-маркер в любом месте строки (кандидат в «маркер внутри строки»).
#: Считаются маркеры из 2–6 ``#`` (то есть ``<h2>…<h6>``): одиночный ``#`` в корпусе —
#: это psql-приглашение (``test=# CREATE``), YAML- и C-комментарий, а не потерянный
#: заголовок. Цена определения — потерянный ``<h1>`` внутри абзаца в число не входит
#: (величина получается нижней границей, а не завышенной).
_ATX_MARKER = re.compile(r"#{2,6}[ \t]+\S")

#: Граница блока кода: строка-забор ``` или ~~~. Внутри кода ``#`` — комментарий,
#: а не потерянный заголовок, поэтому такие строки в метрику не входят.
_FENCE_LINE = re.compile(r"^\s{0,3}(?:```|~~~)")

#: Инлайн-код: ``#`` внутри обратных кавычек — тоже не заголовок.
_INLINE_CODE = re.compile(r"`[^`\n]*`")

#: Длина фрагмента в примере (обрезка, не настройка метрики).
_EXAMPLE_CHARS = 120


@dataclass(frozen=True)
class DocResult:
    """Замер одного документа. ``error`` непуст — страница не измерена."""

    host: str
    url: str
    chars_before: int
    chars_after: int
    headings_before: int
    headings_after: int
    longest_heading_before: int
    longest_heading_after: int
    inline_before: int
    inline_after: int
    transform_applied: bool
    error: str | None = None
    inline_sample: str = ""
    heading_sample: str = ""

    @property
    def chars_delta(self) -> int:
        return self.chars_after - self.chars_before

    @property
    def affected(self) -> bool:
        return (
            self.error is None
            and (
                self.chars_delta != 0
                or self.headings_before != self.headings_after
                or self.longest_heading_before != self.longest_heading_after
                or self.inline_before != self.inline_after
            )
        )


def _headings(text: str) -> list[str]:
    """Строки-заголовки извлечения (ATX) без обрезки — длина считается по ним."""
    return [line.strip() for line in text.splitlines() if _ATX_LINE.match(line)]


def _inline_defect_lines(text: str) -> list[str]:
    """Строки с ATX-маркером внутри строки (после снятия ведущих ``#``).

    Блоки кода и инлайн-код исключены: ``# the role's password`` в листинге — не
    потерянный заголовок, и считать его дефектом значило бы мерить чужую величину.
    """
    out: list[str] = []
    in_fence = False
    for line in text.splitlines():
        if _FENCE_LINE.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        body = _INLINE_CODE.sub("", line.strip().lstrip("#"))
        if _ATX_MARKER.search(body):
            out.append(line.strip())
    return out


def _collapsed(text: str, limit: int = _EXAMPLE_CHARS) -> str:
    """Одна строка для примера: пробелы схлопнуты, хвост обрезан."""
    collapsed = " ".join(text.split())
    return collapsed if len(collapsed) <= limit else collapsed[: limit - 1] + "…"


def _defect_window(line: str, limit: int = _EXAMPLE_CHARS) -> str:
    """Фрагмент строки-дефекта **вокруг маркера**: обрезка с начала его бы скрывала."""
    collapsed = " ".join(line.split())
    match = _ATX_MARKER.search(collapsed)
    if match is None or len(collapsed) <= limit:
        return _collapsed(collapsed, limit)
    start = max(0, match.start() - limit // 4)
    return "…" + collapsed[start : start + limit - 1] + "…"


def _recovered_heading(inline_before: Sequence[str], headings_after: Sequence[str]) -> str:
    """Заголовок «после», чей текст был внутри строки-дефекта «до».

    Это и есть проверяемое превращение: строка ``- текст ### Примечание текст`` даёт
    отдельную строку ``### Примечание``.
    """
    compact = [" ".join(line.split()) for line in inline_before]
    for heading in headings_after:
        body = " ".join(heading.lstrip("#").split())
        if body and any(body in line for line in compact):
            return heading
    return ""


def _new_heading(headings_before: Sequence[str], headings_after: Sequence[str]) -> str:
    """Первый заголовок «после», которого не было отдельной строкой «до» (запасной пример)."""
    return next((line for line in headings_after if line not in headings_before), "")


def _longest_heading(headings: Sequence[str]) -> int:
    """Длина самой длинной строки-заголовка (0 — заголовков нет)."""
    return max((len(line) for line in headings), default=0)


def _measure(item: _mhe.Item, path: Path) -> DocResult:
    """Извлекает одну страницу дважды: без трансформации и с ней.

    Различие колонок — ровно :func:`core.adapters.extract_html._neutralize_definition_lists`;
    всё остальное (декодирование, пре-клин, ``<main>``, ``trafilatura``, ``_postprocess``)
    общее, иначе «до/после» сравнивало бы разные входы.
    """
    empty = DocResult(
        host=item.host,
        url=item.url,
        chars_before=0,
        chars_after=0,
        headings_before=0,
        headings_after=0,
        longest_heading_before=0,
        longest_heading_after=0,
        inline_before=0,
        inline_after=0,
        transform_applied=False,
    )
    try:
        content = path.read_bytes()
    except OSError as exc:
        return _replace(empty, error=f"чтение raw_path: {exc}")
    try:
        html = strip_active_html(_mhe._decode_page(content, item.content_type))
        prepared = _declare_main_region(html)
    except Exception as exc:  # noqa: BLE001 - битые байты не должны валить замер
        return _replace(empty, error=f"подготовка HTML: {exc}")

    try:
        transformed = _neutralize_definition_lists(prepared)
    except Exception as exc:  # noqa: BLE001 - та же грациозная деградация, что у читателя
        return _replace(empty, error=f"трансформация <dl>: {exc}")

    try:
        before_raw = _default_extract(prepared)
        after_raw = _default_extract(transformed) if transformed != prepared else before_raw
    except Exception as exc:  # noqa: BLE001
        return _replace(empty, error=f"trafilatura: {exc}")
    if not before_raw or not after_raw:
        return _replace(empty, error="trafilatura: извлечение не дало текста")

    before = HtmlExtractor._postprocess(before_raw)
    after = HtmlExtractor._postprocess(after_raw)
    headings_before = _headings(before)
    headings_after = _headings(after)
    inline_before = _inline_defect_lines(before)
    inline_after = _inline_defect_lines(after)
    # Пример «после» — заголовок, который был внутри строки «до» и стал отдельной строкой.
    heading_sample = _recovered_heading(inline_before, headings_after) or _new_heading(
        headings_before, headings_after
    )
    return DocResult(
        host=item.host,
        url=item.url,
        chars_before=len(before),
        chars_after=len(after),
        headings_before=len(headings_before),
        headings_after=len(headings_after),
        longest_heading_before=_longest_heading(headings_before),
        longest_heading_after=_longest_heading(headings_after),
        inline_before=len(inline_before),
        inline_after=len(inline_after),
        transform_applied=transformed != prepared,
        inline_sample=_defect_window(inline_before[0]) if inline_before else "",
        heading_sample=_collapsed(heading_sample) if heading_sample else "",
    )


def _replace(result: DocResult, *, error: str) -> DocResult:
    """Копия результата с заполненным ``error`` (остальные величины не определены)."""
    return DocResult(
        host=result.host,
        url=result.url,
        chars_before=0,
        chars_after=0,
        headings_before=0,
        headings_after=0,
        longest_heading_before=0,
        longest_heading_after=0,
        inline_before=0,
        inline_after=0,
        transform_applied=False,
        error=error,
    )


def _examples(results: Sequence[DocResult], count: int) -> list[DocResult]:
    """Примеры: сначала где исчезло больше всего строк-дефектов, затем где заголовок короче.

    Детерминированно: при равных величинах порядок задаёт URL. Это выборка **показать**,
    а не ранжирование качества.
    """
    ranked = [result for result in results if result.affected]
    ranked.sort(
        key=lambda item: (
            -(item.inline_before - item.inline_after),
            -(item.longest_heading_before - item.longest_heading_after),
            item.url,
        )
    )
    return ranked[:count]


def _summary(results: Sequence[DocResult], args: argparse.Namespace) -> dict[str, Any]:
    """Машинная сводка: параметры, счётчики, распределение дельты, примеры."""
    measured = [result for result in results if result.error is None]
    affected = [result for result in measured if result.affected]
    deltas = [result.chars_delta for result in measured]
    samples = _examples(measured, max(args.examples, 0))
    return {
        "tool": "measure_html_dl",
        "params": {
            "db": args.db,
            "limit_per_host": args.limit,
            "host_filter": sorted(args.host),
            "examples": max(args.examples, 0),
        },
        "documents": {
            "selected": len(results),
            "measured": len(measured),
            "errors": len(results) - len(measured),
            "affected": len(affected),
            "transform_applied": sum(1 for result in measured if result.transform_applied),
        },
        "inline_defect_lines": {
            "before": sum(result.inline_before for result in measured),
            "after": sum(result.inline_after for result in measured),
            "removed": sum(
                result.inline_before - result.inline_after for result in measured
            ),
            "documents_before": sum(1 for result in measured if result.inline_before),
            "documents_after": sum(1 for result in measured if result.inline_after),
        },
        "chars": {
            "before": sum(result.chars_before for result in measured),
            "after": sum(result.chars_after for result in measured),
            "delta": sum(deltas),
            "grew": sum(1 for delta in deltas if delta > 0),
            "shrank": sum(1 for delta in deltas if delta < 0),
            "unchanged": sum(1 for delta in deltas if delta == 0),
            "min": min(deltas, default=0),
            "max": max(deltas, default=0),
        },
        "headings": {
            "before": sum(result.headings_before for result in measured),
            "after": sum(result.headings_after for result in measured),
            "longest_before": max(
                (result.longest_heading_before for result in measured), default=0
            ),
            "longest_after": max(
                (result.longest_heading_after for result in measured), default=0
            ),
        },
        "examples": [
            {
                "url": result.url,
                "inline_before": result.inline_before,
                "inline_after": result.inline_after,
                "headings": [result.headings_before, result.headings_after],
                "longest_heading": [
                    result.longest_heading_before,
                    result.longest_heading_after,
                ],
                "chars": [result.chars_before, result.chars_after],
                "before_sample": result.inline_sample,
                "after_sample": result.heading_sample,
            }
            for result in samples
        ],
        "pages": [
            {**asdict(result), "chars_delta": result.chars_delta, "affected": result.affected}
            for result in sorted(results, key=lambda item: (item.host, item.url))
        ],
    }


def _print_report(summary: dict[str, Any], results: Sequence[DocResult]) -> None:
    """Печать сводки и трёх примеров до/после (одна строка на пример)."""
    documents: dict[str, int] = summary["documents"]
    inline: dict[str, int] = summary["inline_defect_lines"]
    chars: dict[str, int] = summary["chars"]
    headings: dict[str, int] = summary["headings"]

    print(
        f"срез: {documents['selected']} страниц, измерено {documents['measured']}, "
        f"ошибок {documents['errors']}; трансформация применилась к "
        f"{documents['transform_applied']}"
    )
    print(f"затронуто документов: {documents['affected']}")
    print("")
    print("строки с ATX-маркером ВНУТРИ строки (дефект <item>-склейки):")
    print(
        f"  до: {inline['before']} (в {inline['documents_before']} документах); "
        f"после: {inline['after']} (в {inline['documents_after']}); "
        f"исчезло: {inline['removed']}"
    )
    print("")
    print("заголовки (строки, начинающиеся с ATX-маркера):")
    print(f"  всего: {headings['before']} → {headings['after']}")
    print(
        f"  самый длинный: {headings['longest_before']} → {headings['longest_after']} символов"
    )
    print("")
    print("символы текста:")
    print(f"  сумма: {chars['before']} → {chars['after']} (дельта {chars['delta']:+d})")
    print(
        f"  распределение: выросло {chars['grew']}, уменьшилось {chars['shrank']}, "
        f"не изменилось {chars['unchanged']}; минимум {chars['min']:+d}, "
        f"максимум {chars['max']:+d}"
    )
    errors = [result for result in results if result.error is not None]
    if errors:
        print("")
        print(f"ошибки ({len(errors)}), первые 3:")
        for result in errors[:3]:
            print(f"  {result.url}: {result.error}")
    print("")
    print("примеры до/после:")
    for example in summary["examples"]:
        print(
            f"  {example['url']} | строк-дефектов {example['inline_before']}→"
            f"{example['inline_after']} | заголовков {example['headings'][0]}→"
            f"{example['headings'][1]} | самый длинный {example['longest_heading'][0]}→"
            f"{example['longest_heading'][1]} | символов {example['chars'][0]}→"
            f"{example['chars'][1]}"
        )
        print(f"    до:    {example['before_sample']}")
        print(f"    после: {example['after_sample']}")


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Аргументы: БД (обязательна), фильтры хостов, размер выборки, JSON, примеры."""
    parser = argparse.ArgumentParser(
        prog="measure_html_dl.py",
        description=(
            "Замер D3 (read-only): что даёт разворачивание <dl>/<dt>/<dd> до trafilatura — "
            "заголовки, строки-дефекты, дельта символов."
        ),
        epilog=(
            "Примеры:\n"
            "  python3 scripts/measure_html_dl.py --db runs/{run_id}/cohound.db --limit 400\n"
            "  python3 scripts/measure_html_dl.py --db runs/{run_id}/cohound.db \\\n"
            "      --host docs.example.org --out-json /tmp/dl.json\n"
            "\n"
            "Срез тот же, что у measure_md_chunking (--limit страниц на хост).\n"
            "БД не изменяется: соединение открывается в режиме mode=ro."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", required=True, help="cohound.db прогона (только чтение)")
    parser.add_argument(
        "--host",
        action="append",
        default=[],
        metavar="SUBSTR",
        help="фильтр по URL, подстрока (можно повторять); без него — все хосты",
    )
    parser.add_argument(
        "--limit", type=int, default=400, help="страниц на хост (дефолт 400, как у чанкера)"
    )
    parser.add_argument("--examples", type=int, default=3, help="сколько примеров показать")
    parser.add_argument("--out-json", metavar="PATH", help="куда записать машинный отчёт")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Точка входа: выборка страниц → две колонки извлечения → сводка → (опционально) JSON."""
    args = _parse_args(argv)
    if args.limit < 1:
        print("measure: --limit должен быть ≥ 1", file=sys.stderr)
        return 2
    if not Path(args.db).expanduser().is_file():
        print(f"measure: БД не найдена: {args.db}", file=sys.stderr)
        return 2

    base = _mhe._raw_base()
    conn = _mhe._ro_connect(args.db)
    try:
        items, skipped_no_raw, skipped_non_html = _mhe._load_items(conn, args.host, args.limit)
    finally:
        conn.close()

    results: list[DocResult] = []
    for item in items:
        path = _mhe._resolve_raw(item.raw_path, base)
        if path is None:
            results.append(
                _replace(
                    DocResult(
                        host=item.host,
                        url=item.url,
                        chars_before=0,
                        chars_after=0,
                        headings_before=0,
                        headings_after=0,
                        longest_heading_before=0,
                        longest_heading_after=0,
                        inline_before=0,
                        inline_after=0,
                        transform_applied=False,
                    ),
                    error=f"нет файла по raw_path: {item.raw_path}",
                )
            )
            continue
        results.append(_measure(item, path))

    print(
        f"limit/per-host={args.limit} base={base} "
        f"skipped(no_raw_path)={skipped_no_raw} skipped(non_html)={skipped_non_html}"
    )
    summary = _summary(results, args)
    _print_report(summary, results)

    if args.out_json:
        target = Path(args.out_json).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(f"\njson: {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())