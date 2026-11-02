"""Приёмка нарезки markdown: инварианты P1–P13 на реальном корпусе, а не на примерах.

Инструмент отвечает на вопрос, который нельзя закрыть юнит-тестами: **что нарезка делает
с настоящими документами**. Он не «смотрит глазами» — он проверяет свойства, объявленные
в ``docs/architecture/18-markdown-chunker-plan.md`` §3, и печатает числа, включая нули.

**Вход** — сохранённая БД прогона (``cohound.db``), читается **только на чтение**:
страницы берутся из ``crawl_session_items``, тела — по ``raw_path``, текст — тем же
экстрактором, что и в прогоне (``HtmlExtractor``), чтобы мерить именно то, что увидит
чанкер. Сеть, запись в БД и правки в ``core/`` — вне контракта инструмента.

**Метрики** (определения зафиксированы):

``p1_violations``
    Документы, где ``"".join(chunk.text)`` ≠ нормализованный вход. Инвариант: **0**.
``p4_violations``
    Документы, где текст чанка не является подстрокой входа. Инвариант: **0**.
``p8_violations``
    Чанки, начинающиеся с закрывающей пунктуации (``) ] } » ” 」 』``). Инвариант: **0**.
``over_budget``
    Чанки длиннее ``chunk_size``. Ноль — цель; ненулевое значение означает, что хвост
    приклеен к предыдущему куску (объявленный компромисс, см. ``_absorb_tail``).
``tiny``
    Чанки короче ``min_chunk_size``: самостоятельно объявленные единицы документа
    (однострочная секция). Не ошибка, но число публикуется — «мелкий мусор» проверяем.
``without_heading``
    Чанки без ``heading_path`` (документ без заголовков или текст до первого).
``with_context``
    Чанки, чей ``context_prefix`` отличается от ``heading_path``: продолжения таблиц,
    fence и цитат (P9/P10).
``mid_sentence``
    Границы, оборвавшие предложение: предыдущий чанк не кончается знаком конца
    предложения, следующий начинается с буквы/иероглифа. Считается только для прозы
    (не для кода и таблиц), поэтому это доля от ``boundaries``.
``unknown_lines``
    Строки, не покрытые ни одним блоком (P13). Инвариант: **0**.

**Чего метрики НЕ измеряют:** качество эмбеддингов и ретривала (это отдельный замер на
индексной сборке), κ оценщика, стоимость эмбеддинга. Здесь — только геометрия нарезки.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

from core.adapters.chunker_markdown import MIN_CHUNK_SIZE, MarkdownChunker
from core.adapters.extract_html import HtmlExtractor
from core.adapters.md_ast import CLOSING_CHARS, MdDocument, parse_markdown
from core.ports.types import Chunk, RawPage
from core.util.document_formats import FAMILY_TEXT


def _load_sibling(name: str):
    """Подгружает соседний скрипт как модуль (переиспользование раскладки и выборки)."""
    path = Path(__file__).with_name(f"{name}.py")
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"не удалось загрузить {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_mhe = _load_sibling("measure_html_extraction")

#: Знаки конца предложения: западные и полноширинные (CJK-пунктуация).
_SENTENCE_END = frozenset(".!?;…。！？；…:：)\"»”」』")


#: Блоки, где начало чанка с закрывающей пунктуации — норма (код, таблица, формула).
_NON_PROSE = frozenset({"fence", "code", "html", "math", "table_row", "reference", "front_matter"})


@dataclass
class _Spans:
    """Все блоки документа: ``(начало, конец, вид)`` — для вопроса «что здесь за текст»."""

    spans: list[tuple[int, int, str]]

    @classmethod
    def of(cls, document: MdDocument) -> _Spans:
        return cls(
            [
                (block.span.start_char, block.span.end_char, block.kind)
                for top in document.blocks
                for block in top.walk()
            ]
        )

    def kind_at(self, offset: int) -> str | None:
        """Вид самого глубокого блока, содержащего смещение."""
        best: tuple[int, str] | None = None
        for start, end, kind in self.spans:
            if start <= offset < end and (best is None or end - start < best[0]):
                best = (end - start, kind)
        return best[1] if best else None

    def paragraph_at(self, offset: int) -> tuple[int, int] | None:
        """Спан абзаца, содержащего смещение (для отличения реза внутри абзаца)."""
        best: tuple[int, int, int] | None = None
        for start, end, kind in self.spans:
            if kind != "paragraph" or not start <= offset < end:
                continue
            if best is None or end - start < best[0]:
                best = (end - start, start, end)
        return (best[1], best[2]) if best else None


@dataclass
class DocResult:
    """Замер одного документа. ``None`` — величина не определена (не ноль)."""

    host: str
    url: str
    chars: int
    chunks: list[Chunk]
    boundaries: int
    mid_sentence: int
    unknown_lines: int
    p1_violation: bool
    p4_violation: bool
    prose_p8: int = 0
    examples: list[str] = field(default_factory=list)
    constructions: dict[str, int] = field(default_factory=dict)


def _host_of(url: str) -> str:
    return url.split("://", 1)[-1].split("/", 1)[0].split(":", 1)[0].lower()


def _mid_sentence_breaks(chunks: Sequence[Chunk], spans: _Spans) -> tuple[int, int]:
    """``(разрывы посреди предложения, резы внутри абзаца)``.

    Считаются только резы **внутри одного абзаца**: граница между абзацами — объявленная
    единица структуры, и «обрывом предложения» она не является. Иначе метрика мерила бы
    долю абзацных границ и ничего не говорила бы о качестве разрезов.
    """
    breaks = 0
    boundaries = 0
    position = 0
    for index in range(len(chunks) - 1):
        current, following = chunks[index], chunks[index + 1]
        position += len(current.text)
        left_paragraph = spans.paragraph_at(position - 1)
        right_paragraph = spans.paragraph_at(position)
        if left_paragraph is None or left_paragraph != right_paragraph:
            continue
        left = current.text.rstrip()
        right = following.text.lstrip()
        if not left or not right:
            continue
        boundaries += 1
        if left[-1] not in _SENTENCE_END and (right[0].isalnum() or _is_cjk(right[0])):
            breaks += 1
    return breaks, boundaries


def _is_cjk(char: str) -> bool:
    """Иероглиф/кандзи/хангыль — письменность без пробелов между словами."""
    code = ord(char)
    return (
        0x3040 <= code <= 0x30FF
        or 0x3400 <= code <= 0x4DBF
        or 0x4E00 <= code <= 0x9FFF
        or 0xAC00 <= code <= 0xD7AF
    )


def _measure(url: str, text: str, chunker: MarkdownChunker) -> DocResult:
    document = parse_markdown(text, chunker.profile)
    chunks = chunker.chunk(url, text)
    spans = _Spans.of(document)
    joined = "".join(chunk.text for chunk in chunks)
    breaks, boundaries = _mid_sentence_breaks(chunks, spans)
    examples: list[str] = []
    prose_p8 = 0
    position = 0
    for chunk in chunks:
        kind = spans.kind_at(position)
        position += len(chunk.text)
        if chunk.text[:1] in CLOSING_CHARS and kind not in _NON_PROSE:
            prose_p8 += 1
            if len(examples) < 3:
                examples.append(f"P8 [{kind}]: {url} :: {chunk.text[:60]!r}")
    return DocResult(
        host=_host_of(url),
        url=url,
        chars=len(document.text),
        chunks=chunks,
        boundaries=boundaries,
        mid_sentence=breaks,
        unknown_lines=int(document.counts.get("unknown_lines") or 0),
        p1_violation=joined != document.text,
        p4_violation=any(chunk.text not in document.text for chunk in chunks),
        prose_p8=prose_p8,
        examples=examples,
        constructions={
            key: int(document.counts.get(key) or 0)
            for key in ("table", "fence", "list", "blockquote", "code", "footnote_def")
        },
    )


@dataclass
class Totals:
    """Сводка по корпусу: счётчики, а не средние — средние по документам обманывают."""

    documents: int = 0
    chars: int = 0
    chunks: int = 0
    p1_violations: int = 0
    p4_violations: int = 0
    p8_violations: int = 0
    over_budget: int = 0
    tiny: int = 0
    without_heading: int = 0
    with_context: int = 0
    unknown_lines: int = 0
    boundaries: int = 0
    mid_sentence: int = 0
    constructions: dict[str, int] = field(default_factory=dict)
    worst: list[tuple[int, str]] = field(default_factory=list)

    def add(self, result: DocResult, chunk_size: int, min_chunk_size: int) -> None:
        self.documents += 1
        self.chars += result.chars
        self.chunks += len(result.chunks)
        self.p1_violations += int(result.p1_violation)
        self.p4_violations += int(result.p4_violation)
        self.unknown_lines += result.unknown_lines
        self.boundaries += result.boundaries
        self.mid_sentence += result.mid_sentence
        for key, value in result.constructions.items():
            self.constructions[key] = self.constructions.get(key, 0) + value
        self.p8_violations += result.prose_p8
        for chunk in result.chunks:
            self.over_budget += int(len(chunk.text) > chunk_size)
            self.tiny += int(len(chunk.text.strip()) < min_chunk_size)
            self.without_heading += int(not chunk.heading_path)
            self.with_context += int(chunk.context_prefix != chunk.heading_path)
        if result.p1_violation:
            self.worst.append((0, f"P1: {result.url}"))
        for message in result.examples:
            self.worst.append((1, message))

    @property
    def share(self) -> dict[str, float]:
        chunks = self.chunks or 1
        return {
            "over_budget": self.over_budget / chunks,
            "tiny": self.tiny / chunks,
            "without_heading": self.without_heading / chunks,
            "with_context": self.with_context / chunks,
            "mid_sentence": self.mid_sentence / (self.boundaries or 1),
        }


def _print_report(
    totals: Totals,
    *,
    chunk_size: int,
    min_chunk_size: int,
    profile: str,
    hosts: dict[str, int],
) -> None:
    share = totals.share
    print(f"markdown-чанкер: профиль {profile}, бюджет {chunk_size}, порог {min_chunk_size}")
    print(f"документов: {totals.documents} ({totals.chars} символов)")
    print(f"чанков: {totals.chunks}")
    print("")
    print("инварианты (цель — 0):")
    print(f"  P1  конкатенация ≠ вход      : {totals.p1_violations}")
    print(f"  P4  чанк не подстрока входа   : {totals.p4_violations}")
    print(f"  P8  чанк начинается с ')'/']' : {totals.p8_violations}")
    print(f"  P13 строки вне блоков         : {totals.unknown_lines}")
    print("")
    print("геометрия (число чанков и доля):")
    print(f"  длиннее бюджета      : {totals.over_budget} ({share['over_budget']:.3%})")
    print(f"  короче порога        : {totals.tiny} ({share['tiny']:.3%})")
    print(f"  без heading_path     : {totals.without_heading} ({share['without_heading']:.3%})")
    print(f"  с контекстом продолж.: {totals.with_context} ({share['with_context']:.3%})")
    print(
        f"  обрыв предложения    : {totals.mid_sentence} из {totals.boundaries} границ "
        f"({share['mid_sentence']:.3%})"
    )
    print("")
    print("конструкции в корпусе:", ", ".join(f"{k}={v}" for k, v in sorted(totals.constructions.items())))
    if hosts:
        top = sorted(hosts.items(), key=lambda item: -item[1])[:10]
        print("документов по хостам:", ", ".join(f"{host}={count}" for host, count in top))
    for _, message in totals.worst[:5]:
        print(f"  ! {message}")


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="measure_md_chunking.py",
        description=(
            "Приёмка markdown-чанкера на сохранённом корпусе (read-only): "
            "инварианты P1/P4/P8/P13 и геометрия нарезки."
        ),
        epilog=(
            "Примеры:\n"
            "  python3 scripts/measure_md_chunking.py --db runs/{run_id}/cohound.db\n"
            "  python3 scripts/measure_md_chunking.py --db runs/{run_id}/cohound.db \\\n"
            "      --host docs.example.org --limit 50 --chunk-size 800 --out-json /tmp/m.json\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", required=True, help="cohound.db прогона (только чтение)")
    parser.add_argument(
        "--host", action="append", default=[], metavar="SUBSTR",
        help="фильтр по URL, подстрока (можно повторять)",
    )
    parser.add_argument("--limit", type=int, default=300, help="страниц на хост (дефолт 300)")
    parser.add_argument("--chunk-size", type=int, default=1500, help="бюджет куска")
    parser.add_argument("--min-chunk-size", type=int, default=MIN_CHUNK_SIZE, help="порог хвоста")
    parser.add_argument(
        "--profile", choices=("commonmark", "gfm", "pandoc"), default="gfm",
        help="профиль грамматики (дефолт gfm — как у страниц и офисных документов)",
    )
    parser.add_argument("--out-json", metavar="PATH", help="куда записать машинный отчёт")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.limit < 1:
        print("measure: --limit должен быть ≥ 1", file=sys.stderr)
        return 2
    if not Path(args.db).expanduser().is_file():
        print(f"measure: БД не найдена: {args.db}", file=sys.stderr)
        return 2

    extractor = HtmlExtractor()
    if not extractor.available:
        print("measure: trafilatura не установлена (extra 'extract')", file=sys.stderr)
        return 2

    base = _mhe._raw_base()
    conn = _mhe._ro_connect(args.db)
    try:
        items, skipped_no_raw, skipped_non_html = _mhe._load_items(conn, args.host, args.limit)
    finally:
        conn.close()

    chunker = MarkdownChunker(chunk_size=args.chunk_size, min_chunk_size=args.min_chunk_size, profile=args.profile)
    totals = Totals()
    hosts: dict[str, int] = {}
    skipped_text, skipped_error = 0, 0

    for item in items:
        path = _mhe._resolve_raw(item.raw_path, base)
        if path is None:
            skipped_no_raw += 1
            continue
        page = RawPage(url=item.url, content=path.read_bytes(), fetched_at="", content_type=item.content_type)
        extracted = extractor.extract(page)
        if extracted.error or not extracted.text:
            skipped_error += 1
            continue
        # HTML/офисные документы идут в markdown-чанкер (``FamilyChunker`` выбирает
        # абзацный только для семейства ``text``), поэтому пропускается ровно оно.
        if extracted.family == FAMILY_TEXT:
            skipped_text += 1
            continue
        result = _measure(item.url, extracted.text, chunker)
        totals.add(result, args.chunk_size, args.min_chunk_size)
        hosts[result.host] = hosts.get(result.host, 0) + 1

    _print_report(
        totals,
        chunk_size=args.chunk_size,
        min_chunk_size=args.min_chunk_size,
        profile=args.profile,
        hosts=hosts,
    )
    print("")
    print(
        f"пропущено: без сырья={skipped_no_raw}, не-HTML={skipped_non_html}, "
        f"ошибка извлечения={skipped_error}, семейство text={skipped_text}"
    )

    if args.out_json:
        payload = {
            "profile": args.profile,
            "chunk_size": args.chunk_size,
            "min_chunk_size": args.min_chunk_size,
            "documents": totals.documents,
            "chars": totals.chars,
            "chunks": totals.chunks,
            "invariants": {
                "p1_violations": totals.p1_violations,
                "p4_violations": totals.p4_violations,
                "p8_violations": totals.p8_violations,
                "unknown_lines": totals.unknown_lines,
            },
            "geometry": {
                "over_budget": totals.over_budget,
                "tiny": totals.tiny,
                "without_heading": totals.without_heading,
                "with_context": totals.with_context,
                "mid_sentence": totals.mid_sentence,
                "boundaries": totals.boundaries,
            },
            "share": totals.share,
            "constructions": totals.constructions,
            "hosts": hosts,
            "skipped": {
                "no_raw": skipped_no_raw,
                "non_html": skipped_non_html,
                "extract_error": skipped_error,
                "text_family": skipped_text,
            },
        }
        Path(args.out_json).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())