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

**Политика трансформации** (``--policy``). ``with-heading`` (дефолт) — боевое правило:
разворачиваются только ``<dl>``, внутри которых есть заголовок. ``all`` — те же замены
тегов и то же снятие ``<a>``, но **все** ``<dl>`` подряд, без фильтра по заголовку.
Второй режим существует ровно для того, чтобы «замена всех ``<dl>`` задевает N документов
и ломает выбор основного блока у M» было **числом этого инструмента**, а не пересказом.
Правило живёт здесь **копией** (:func:`_neutralize_definition_lists` в этом файле), а не
параметром боевой функции: боевой модуль правит другая полоса. Копия проверяется против
боевой функции на каждом документе (``copy_mismatches``), поэтому расхождение копии с
правилом видно, а не подразумевается.

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
``headings_lost``
    Документы, где заголовков стало **меньше** (``headings_after < headings_before``), и
    документы, потерявшие **все** заголовки (``after == 0`` при ``before > 0``). Это и
    есть проверяемая форма слова «ломает»: не «плохо выглядит», а «счётчик упал».
``timing``
    Суммарное время трансформации и извлечения по корпусу и число документов, где ``<dl``
    вообще встретился (быстрый путь ``_DEFINITION_LIST_RE``). Цена лишнего разбора
    lxml публикуется в мс/страница, а не подразумевается.

**Сводка**: сколько документов затронуто (изменилась любая из величин), сколько
строк-дефектов исчезло, суммарная дельта символов и её распределение (выросло /
уменьшилось / не изменилось), 3 примера до/после.

**Чего замер НЕ показывает**: качество чанков и ретривала на новом тексте (это отдельный
замер по индексной сборке), и эффект в корпусе — ``content_hash`` затронутых документов
изменится, поэтому он виден только после пере-краула, который здесь **не** делается.

Запуск:

    python3 scripts/measure_html_dl.py \\
        --db ~/.cohound/data/runs/ru-pg-dbms-1e0b04d6d83a/cohound.db --limit 400

    python3 scripts/measure_html_dl.py --db … --policy all --out-json /tmp/dl-all.json
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.adapters.extract_html import (  # noqa: E402
    _DEFINITION_HEADING_XPATH,
    _DEFINITION_LIST_RE,
    _DEFINITION_TAGS,
    HtmlExtractor,
    _declare_main_region,
    _default_extract,
)
from core.adapters.extract_html import (  # noqa: E402
    _neutralize_definition_lists as _neutralize_definition_lists_production,
)
from core.util.sanitize import strip_active_html  # noqa: E402

#: Политики трансформации: боевая (только ``<dl>`` с заголовком) и «развернуть всё».
POLICIES: tuple[str, ...] = ("with-heading", "all")

#: Пороги «самый длинный заголовок документа» в символах — те же, что публикует отчёт.
LONGEST_HEADING_BUCKETS: tuple[int, ...] = (200, 500)


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


def _neutralize_definition_lists(html: str, *, require_heading: bool = True) -> str:
    """**Копия правила** ``core/adapters/extract_html.py:_neutralize_definition_lists``.

    Это не вызов боевого кода, а копия с одним добавленным параметром
    (``require_heading``): боевая функция правится другой полосой, и параметр политики в
    неё не добавляется. Всё, что можно было не копировать, взято из модуля читателя —
    ``_DEFINITION_TAGS``, ``_DEFINITION_LIST_RE``, ``_DEFINITION_HEADING_XPATH``, — поэтому
    копируется ровно обход дерева.

    ``require_heading=True`` повторяет боевое поведение байт в байт (это проверяется на
    каждом документе в :func:`_measure`, счётчик ``copy_mismatches``);
    ``require_heading=False`` — тот же обход, но без фильтра по заголовку, то есть
    «развернуть все ``<dl>``». Различие двух колонок замера — ровно этот аргумент.
    """
    if not _DEFINITION_LIST_RE.search(html):
        return html
    import lxml.html

    tree = lxml.html.document_fromstring(html)
    targets = [
        definition_list
        for definition_list in tree.xpath("//dl")
        if not require_heading or definition_list.xpath(_DEFINITION_HEADING_XPATH)
    ]
    if not targets:
        return html
    for definition_list in targets:
        for link in definition_list.xpath(".//a"):
            link.drop_tag()
        for node in definition_list.iter("dl", "dt", "dd"):
            node.tag = _DEFINITION_TAGS[node.tag]
    return lxml.html.tostring(tree, encoding="unicode")

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
    #: Политика, с которой считалась колонка «после».
    policy: str = "with-heading"
    #: ``<dl`` встретился в подготовленном HTML (дешёвая проверка до разбора).
    has_dl: bool = False
    #: Время трансформации и обоих извлечений на этом документе, мс.
    transform_ms: float = 0.0
    extract_ms: float = 0.0
    #: Копия правила разошлась с боевой функцией на этом документе (должно быть 0).
    copy_mismatch: bool = False

    @property
    def chars_delta(self) -> int:
        return self.chars_after - self.chars_before

    @property
    def headings_fewer(self) -> bool:
        """Заголовков стало меньше: проверяемая форма слова «ломает»."""
        return self.error is None and self.headings_after < self.headings_before

    @property
    def headings_zeroed(self) -> bool:
        """Были заголовки — не осталось ни одного."""
        return self.error is None and self.headings_after == 0 and self.headings_before > 0

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


def _measure(item: _mhe.Item, path: Path, policy: str = "with-heading") -> DocResult:
    """Извлекает одну страницу дважды: без трансформации и с ней (по ``policy``).

    Различие колонок — ровно аргумент ``require_heading`` у
    :func:`_neutralize_definition_lists` этого файла; всё остальное (декодирование,
    пре-клин, ``<main>``, ``trafilatura``, ``_postprocess``) общее, иначе «до/после»
    сравнивало бы разные входы. Время обеих операций пишется в результат: цена правки
    публикуется числом.
    """
    require_heading = policy != "all"
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
        policy=policy,
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

    started = time.perf_counter()
    try:
        transformed = _neutralize_definition_lists(prepared, require_heading=require_heading)
    except Exception as exc:  # noqa: BLE001 - та же грациозная деградация, что у читателя
        return _replace(empty, error=f"трансформация <dl>: {exc}")
    transform_ms = (time.perf_counter() - started) * 1000.0

    # Копия правила не имеет права тихо разойтись с боевым кодом: при боевой политике
    # результат обязан совпасть байт в байт. Проверка стоит один разбор lxml только на
    # страницах с ``<dl>`` (остальные выходят по дешёвому регексу).
    copy_mismatch = False
    if require_heading:
        try:
            copy_mismatch = transformed != _neutralize_definition_lists_production(prepared)
        except Exception:  # noqa: BLE001 - расхождение в поведении всё равно видно по тексту
            copy_mismatch = True

    started = time.perf_counter()
    try:
        before_raw = _default_extract(prepared)
        after_raw = _default_extract(transformed) if transformed != prepared else before_raw
    except Exception as exc:  # noqa: BLE001
        return _replace(empty, error=f"trafilatura: {exc}")
    extract_ms = (time.perf_counter() - started) * 1000.0
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
        policy=policy,
        has_dl=bool(_DEFINITION_LIST_RE.search(prepared)),
        transform_ms=transform_ms,
        extract_ms=extract_ms,
        copy_mismatch=copy_mismatch,
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
        policy=result.policy,
        has_dl=result.has_dl,
        transform_ms=result.transform_ms,
        extract_ms=result.extract_ms,
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
    """Машинная сводка: параметры, счётчики, распределение дельты, цена, примеры."""
    measured = [result for result in results if result.error is None]
    affected = [result for result in measured if result.affected]
    deltas = [result.chars_delta for result in measured]
    samples = _examples(measured, max(args.examples, 0))
    transform_ms = sum(result.transform_ms for result in measured)
    extract_ms = sum(result.extract_ms for result in measured)
    with_dl = [result for result in measured if result.has_dl]
    pages = len(measured)
    return {
        "tool": "measure_html_dl",
        "params": {
            "db": args.db,
            "limit_per_host": args.limit,
            "host_filter": sorted(args.host),
            "examples": max(args.examples, 0),
            "policy": args.policy,
        },
        "documents": {
            "selected": len(results),
            "measured": len(measured),
            "errors": len(results) - len(measured),
            "affected": len(affected),
            "transform_applied": sum(1 for result in measured if result.transform_applied),
            "with_dl": len(with_dl),
            "copy_mismatches": sum(1 for result in measured if result.copy_mismatch),
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
            # Проверяемая форма «ломает»: не эпитет, а падение счётчика.
            "documents_fewer": sum(1 for result in measured if result.headings_fewer),
            "documents_zeroed": sum(1 for result in measured if result.headings_zeroed),
            "documents_more": sum(
                1 for result in measured if result.headings_after > result.headings_before
            ),
            "lost": sum(
                max(result.headings_before - result.headings_after, 0) for result in measured
            ),
            # «Документов, у которых самый длинный заголовок длиннее N»: раньше это число
            # в отчёте было, а воспроизвести его было нечем.
            "documents_longest_over": {
                str(threshold): {
                    "before": sum(
                        1
                        for result in measured
                        if result.longest_heading_before > threshold
                    ),
                    "after": sum(
                        1 for result in measured if result.longest_heading_after > threshold
                    ),
                }
                for threshold in LONGEST_HEADING_BUCKETS
            },
        },
        "timing": {
            "pages": pages,
            "documents_with_dl": len(with_dl),
            "transform_ms": transform_ms,
            "extract_ms": extract_ms,
            "transform_ms_per_page": transform_ms / (pages or 1),
            "extract_ms_per_page": extract_ms / (pages or 1),
            "transform_ms_per_dl_page": transform_ms / (len(with_dl) or 1),
            "extract_ms_per_dl_page": extract_ms / (len(with_dl) or 1),
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
    timing: dict[str, int] = summary["timing"]

    print(f"политика: {summary['params']['policy']} (require_heading={summary['params']['policy'] != 'all'})")
    print(
        f"срез: {documents['selected']} страниц, измерено {documents['measured']}, "
        f"ошибок {documents['errors']}; трансформация применилась к "
        f"{documents['transform_applied']}"
    )
    print(
        f"страниц с <dl: {documents['with_dl']}; копия правила разошлась с боевой "
        f"функцией: {documents['copy_mismatches']}"
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
    print(
        f"  документов с МЕНЬШИМ числом заголовков: {headings['documents_fewer']} "
        f"(потеряно строк-заголовков: {headings['lost']}); "
        f"потеряли ВСЕ заголовки: {headings['documents_zeroed']}; "
        f"получили больше: {headings['documents_more']}"
    )
    for threshold, bucket in headings["documents_longest_over"].items():
        print(
            f"  документов с заголовком > {threshold} символов: "
            f"{bucket['before']} → {bucket['after']}"
        )
    print("")
    print("символы текста:")
    print(f"  сумма: {chars['before']} → {chars['after']} (дельта {chars['delta']:+d})")
    print(
        f"  распределение: выросло {chars['grew']}, уменьшилось {chars['shrank']}, "
        f"не изменилось {chars['unchanged']}; минимум {chars['min']:+d}, "
        f"максимум {chars['max']:+d}"
    )
    print("")
    print("цена (суммарно по измеренным страницам, Python-таймер на страницу):")
    print(
        f"  трансформация <dl>: {timing['transform_ms']:.1f} мс "
        f"({timing['transform_ms_per_page']:.2f} мс/страница, "
        f"{timing['transform_ms_per_dl_page']:.2f} мс на страницу с <dl>)"
    )
    print(
        f"  извлечение (два прохода): {timing['extract_ms']:.1f} мс "
        f"({timing['extract_ms_per_page']:.2f} мс/страница, "
        f"{timing['extract_ms_per_dl_page']:.2f} мс на страницу с <dl>)"
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
    parser.add_argument(
        "--policy",
        choices=POLICIES,
        default="with-heading",
        help=(
            "with-heading (дефолт) — боевое правило: только <dl> с заголовком внутри; "
            "all — развернуть все <dl> (замер цены узости D3)"
        ),
    )
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
                        policy=args.policy,
                    ),
                    error=f"нет файла по raw_path: {item.raw_path}",
                )
            )
            continue
        results.append(_measure(item, path, args.policy))

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