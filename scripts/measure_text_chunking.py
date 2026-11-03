"""Приёмка абзацного чанкера plain text на сохранённом корпусе: P1/P4 и геометрия.

Инструмент отвечает на вопрос, который не закрывается юнит-тестами: **что делают точные
срезы ``text-para-4`` с настоящими документами**. Вход — сохранённая БД прогона
(``cohound.db``), читается **только на чтение** (``mode=ro``): документы берутся из
``corpus_docs`` по объявленной версии читателя (``text-plain-2``), тело — по ``raw_path``
через те же ``_raw_base``/``_resolve_raw``, что у markdown-харнесса, а для архивных
членов — из контейнера по пути из URL (``archive.zip!/member.txt``). Текст извлекается
тем же читателем (:class:`core.adapters.extract_text.TextExtractor`), что и в прогоне:
мерить надо то, что увидит чанкер.

**Метрики** (определения зафиксированы):

``p1_violations``
    Документы, где ``"".join(chunk.text)`` ≠ нормализованный вход. Инвариант: **0**.
``p4_violations``
    Чанки, где ``text`` ≠ ``normalized[char_start:char_end]``. Инвариант: **0**.
``coverage_pct``
    Доля символов входа, покрытых чанками ровно один раз. Инвариант: **100 %**;
    ``gap_chars``/``overlap_chars`` показывают причину расхождения, если он есть.
``edge_whitespace_chunks``
    Чанки, у которых ``text != text.strip()``: краевые пробелы **больше не срезаются**
    (P1), и это видно числом. ``chars_lost_to_strip`` — сколько символов потерял бы
    прежний ``.strip()`` на текущей нарезке. Это честная замена строке «до»: прежняя
    политика нарушала P1 по построению, поэтому «до/после» сравнением быть не может.
``over_budget``
    Чанки длиннее ``chunk_size``. Ноль — цель; ненулевое значение означает, что
    разделитель до следующего куска или короткий хвост приклеены (объявленный
    компромисс), а не незамеченный выход за границу.
``tiny``
    Чанки короче ``min_chunk_size`` (по ``text.strip()``). Порог больше не выбрасывает
    текст: такой чанк — объявленная единица документа или последний остаток. Доли
    публикуются по числу чанков, по символам и по токенам (если включён ``--tokens``).
``max_chars`` / ``max_tokens``
    Самый длинный чанк: геометрия в символах и в информационных единицах.

``--tokens`` грузит **тот же** токенизатор, что у эмбеддера, из локального кэша
(``local_files_only=True``); символьный прокси вместо токенов не подставляется — без
кэша инструмент работает, но токены не измеряет и говорит об этом.

**Чего метрики НЕ измеряют:** качество эмбеддингов/ретривала, κ оценщика, стоимость
эмбеддинга. Здесь — только геометрия нарезки и её инварианты.

Нейтрален (§1.3).
"""

from __future__ import annotations

import argparse
import importlib.util
import io
import json
import math
import sys
import zipfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from core.adapters.chunker_text import (
    MIN_CHUNK_SIZE,
    TEXT_CHUNKER_VERSION,
    TextChunker,
    _normalize,
)
from core.adapters.extract_text import TEXT_EXTRACTOR_VERSION, TextExtractor
from core.ports.types import Chunk, RawPage


def _load_sibling(name: str):
    """Подгружает соседний скрипт как модуль (раскладка, выборка, токенизатор)."""
    path = Path(__file__).with_name(f"{name}.py")
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"не удалось загрузить {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


#: Markdown-харнесс — источник раскладки/выборки и токенизатора, а не копия.
_md = _load_sibling("measure_md_chunking")
_mhe = _md._mhe

#: Разделитель пути архивного члена в URL (``archive.zip!/member``).
_MEMBER_SEP = "!/"


@dataclass
class DocResult:
    """Замер одного документа. ``None`` — величина не определена (не ноль)."""

    url: str
    chars: int
    chunks: list[Chunk]
    p1_violation: bool
    p4_violations: int
    gap_chars: int
    overlap_chars: int
    edge_whitespace_chunks: int
    chars_lost_to_strip: int
    over_budget: int
    tiny: int
    tiny_chars: int


def _body(url: str, raw_path: str, base: Path) -> bytes | None:
    """Байты документа: файл по ``raw_path`` или член контейнера по пути из URL."""
    path = _mhe._resolve_raw(raw_path, base)
    if path is None:
        return None
    payload = path.read_bytes()
    remainder = url
    while _MEMBER_SEP in remainder:
        remainder, member = remainder.split(_MEMBER_SEP, 1)
        try:
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                payload = archive.read(member)
        except (zipfile.BadZipFile, KeyError, OSError):
            return None
    return payload


def _coverage(text: str, chunks: Sequence[Chunk]) -> tuple[int, int]:
    """``(непокрытые символы, покрытые дважды)`` — по смещениям, без массива на текст."""
    ranges = sorted(
        (chunk.char_start, chunk.char_end)
        for chunk in chunks
        if chunk.char_start is not None and chunk.char_end is not None
    )
    gaps = 0
    overlaps = 0
    position = 0
    for start, end in ranges:
        if start > position:
            gaps += start - position
        elif start < position:
            overlaps += position - start
        position = max(position, end)
    if position < len(text):
        gaps += len(text) - position
    return gaps, overlaps


def _measure(url: str, text: str, chunker: TextChunker) -> DocResult:
    chunks = chunker.chunk(url, text)
    normalized = _normalize(text)
    joined = "".join(chunk.text for chunk in chunks)
    p4 = 0
    edge = 0
    lost = 0
    over = 0
    tiny = 0
    tiny_chars = 0
    for chunk in chunks:
        start, end = chunk.char_start, chunk.char_end
        if start is None or end is None or normalized[start:end] != chunk.text:
            p4 += 1
        stripped = chunk.text.strip()
        if stripped != chunk.text:
            edge += 1
            lost += len(chunk.text) - len(stripped)
        if len(chunk.text) > chunker.chunk_size:
            over += 1
        if len(stripped) < chunker.min_chunk_size:
            tiny += 1
            tiny_chars += len(chunk.text)
    gaps, overlaps = _coverage(normalized, chunks)
    return DocResult(
        url=url,
        chars=len(normalized),
        chunks=chunks,
        p1_violation=bool(normalized.strip()) and joined != normalized,
        p4_violations=p4,
        gap_chars=gaps,
        overlap_chars=overlaps,
        edge_whitespace_chunks=edge,
        chars_lost_to_strip=lost,
        over_budget=over,
        tiny=tiny,
        tiny_chars=tiny_chars,
    )


@dataclass
class Totals:
    """Сводка по корпусу: счётчики, а не средние — средние по документам обманывают."""

    documents: int = 0
    chars: int = 0
    chunks: int = 0
    p1_violations: int = 0
    p4_violations: int = 0
    gap_chars: int = 0
    overlap_chars: int = 0
    edge_whitespace_chunks: int = 0
    chars_lost_to_strip: int = 0
    over_budget: int = 0
    tiny: int = 0
    tiny_chars: int = 0
    lengths: list[int] = field(default_factory=list)

    def add(self, result: DocResult) -> None:
        self.documents += 1
        self.chars += result.chars
        self.chunks += len(result.chunks)
        self.p1_violations += int(result.p1_violation)
        self.p4_violations += result.p4_violations
        self.gap_chars += result.gap_chars
        self.overlap_chars += result.overlap_chars
        self.edge_whitespace_chunks += result.edge_whitespace_chunks
        self.chars_lost_to_strip += result.chars_lost_to_strip
        self.over_budget += result.over_budget
        self.tiny += result.tiny
        self.tiny_chars += result.tiny_chars
        self.lengths.extend(len(chunk.text) for chunk in result.chunks)


def _share(value: int, total: int) -> float:
    return value / (total or 1)


def _percentile(ordered: Sequence[int], quantile: float) -> float:
    """Перцентиль линейной интерполяцией — определение то же, что у markdown-харнесса."""
    if not ordered:
        raise ValueError("перцентиль пустого ряда не определён")
    if len(ordered) == 1:
        return float(ordered[0])
    position = (len(ordered) - 1) * quantile / 100.0
    low = math.floor(position)
    high = math.ceil(position)
    if low == high:
        return float(ordered[low])
    weight = position - low
    return float(ordered[low]) + (float(ordered[high]) - float(ordered[low])) * weight


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="measure_text_chunking.py",
        description=(
            "Приёмка абзацного чанкера plain text на сохранённом корпусе (read-only): "
            "P1/P4, покрытие и геометрия точных срезов."
        ),
        epilog=(
            "Пример:\n"
            "  python3 scripts/measure_text_chunking.py \\\n"
            "      --db ~/.cohound/data/runs/{run_id}/cohound.db --tokens \\\n"
            "      --out-json /tmp/text-chunking.json\n"
            "\n"
            "--tokens требует локального кэша модели (сеть запрещена): без него инструмент\n"
            "работает, но информационные единицы не измеряет — прокси не подставляется.\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", required=True, help="cohound.db прогона (только чтение)")
    parser.add_argument("--limit", type=int, default=0, help="документов (0 — все)")
    parser.add_argument("--chunk-size", type=int, default=1500, help="бюджет куска")
    parser.add_argument(
        "--min-chunk-size", type=int, default=MIN_CHUNK_SIZE, help="порог хвоста"
    )
    parser.add_argument(
        "--tokens",
        action="store_true",
        help="считать токены тем же токенизатором, что у эмбеддера (дефолт: выкл)",
    )
    parser.add_argument(
        "--tokenizer-model",
        default=None,
        metavar="MODEL_ID",
        help="модель токенизатора (по умолчанию DEFAULT_MODEL_ID из embed_local.py)",
    )
    parser.add_argument("--out-json", metavar="PATH", help="куда записать машинный отчёт")
    return parser.parse_args(argv)


def _print_report(
    totals: Totals,
    *,
    chunk_size: int,
    min_chunk_size: int,
    hosts: dict[str, int],
    token_summary: dict[str, Any] | None,
) -> None:
    print(
        f"абзацный чанкер plain text: {TEXT_CHUNKER_VERSION}, "
        f"бюджет {chunk_size}, порог {min_chunk_size}"
    )
    print(f"читатель: {TEXT_EXTRACTOR_VERSION}")
    print(f"документов: {totals.documents} ({totals.chars} символов)")
    print(f"чанков: {totals.chunks}")
    print("")
    print("инварианты (цель — 0):")
    print(f"  P1  конкатенация ≠ вход         : {totals.p1_violations}")
    print(f"  P4  text ≠ normalized[start:end] : {totals.p4_violations}")
    print(f"  покрытие: дыры {totals.gap_chars} символов, перекрытия {totals.overlap_chars}")
    covered = totals.chars - totals.gap_chars - totals.overlap_chars
    print(f"  покрытие символов               : {_share(covered, totals.chars):.3%}")
    print("")
    print("краевые пробелы (следствие P1 — больше не срезаются):")
    print(
        f"  чанков с краевыми пробелами     : {totals.edge_whitespace_chunks} "
        f"({_share(totals.edge_whitespace_chunks, totals.chunks):.3%})"
    )
    print(f"  символов потерял бы .strip()    : {totals.chars_lost_to_strip}")
    print("")
    print("геометрия:")
    print(
        f"  длиннее бюджета                 : {totals.over_budget} "
        f"({_share(totals.over_budget, totals.chunks):.3%})"
    )
    print(
        f"  короче порога (tiny)            : {totals.tiny} "
        f"({_share(totals.tiny, totals.chunks):.3%}); "
        f"символов в них {_share(totals.tiny_chars, totals.chars):.3%}"
    )
    if totals.lengths:
        ordered = sorted(totals.lengths)
        print(
            f"  длина чанка, симв               : "
            f"p50={_percentile(ordered, 50):.0f} p90={_percentile(ordered, 90):.0f} "
            f"p99={_percentile(ordered, 99):.0f} max={ordered[-1]}"
        )
    if token_summary is not None:
        text = token_summary["text"]
        print("")
        print("информационные единицы (токенизатор эмбеддера, local_files_only=True):")
        print(
            f"  модель: {token_summary['model_id']}; чанков {token_summary['chunks']}; "
            f"max={text['max']} токенов; p50={text['p50']} p90={text['p90']} "
            f"p99={text['p99']}"
        )
        print(
            f"  >{_md.EMBED_CONTEXT_WINDOW} (окно bge-m3): {text['over_window']}; "
            f">512: {text['over_512']}"
        )
        print(
            f"  tiny по токенам                 : {token_summary['tiny']['chunks']} "
            f"({token_summary['tiny']['chunk_share']:.3%}); "
            f"токенов в них {token_summary['tiny']['token_share']:.3%}"
        )
        print(
            "  разрез по письменности          : "
            + ", ".join(
                f"{name}={value['n']}"
                for name, value in sorted(token_summary["script"].items())
            )
        )
    if hosts:
        top = sorted(hosts.items(), key=lambda item: -item[1])[:10]
        print("документов по хостам:", ", ".join(f"{host}={count}" for host, count in top))
    print("")
    print(
        "! строки «до» нет: прежняя политика нарушала P1 по построению, поэтому вместо "
        "«до/после» публикуется точная величина потерь .strip() на текущей нарезке."
    )


def _token_summary(
    geometry: Any,
    tiny_chunks: int,
    tiny_tokens: int,
    total_chunks: int,
    total_tokens: int,
) -> dict[str, Any]:
    """Токены текста, tiny-доля в токенах и разрез по письменности — без прокси."""
    summary = geometry.summary()
    summary.pop("prompt", None)
    summary.pop("context", None)
    summary.pop("script_diagnostics", None)
    summary["tiny"] = {
        "chunks": tiny_chunks,
        "chunk_share": _share(tiny_chunks, total_chunks),
        "tokens": tiny_tokens,
        "token_share": _share(tiny_tokens, total_tokens),
    }
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if not Path(args.db).expanduser().is_file():
        print(f"measure: БД не найдена: {args.db}", file=sys.stderr)
        return 2
    if args.chunk_size <= args.min_chunk_size:
        print("measure: --chunk-size должен быть больше --min-chunk-size", file=sys.stderr)
        return 2

    extractor = TextExtractor()
    base = _mhe._raw_base()
    conn = _mhe._ro_connect(args.db)
    try:
        rows = list(
            conn.execute(
                "SELECT url, raw_path FROM corpus_docs "
                "WHERE extractor_version = ? ORDER BY url",
                (TEXT_EXTRACTOR_VERSION,),
            )
        )
        if args.limit:
            rows = rows[: args.limit]
        declared_model = _md._declared_embed_model(conn) if args.tokens else None
    finally:
        conn.close()

    tokenizer: Any = None
    counter: Callable[[str], int] | None = None
    provenance: dict[str, Any] = {}
    resolution: dict[str, Any] = {}
    if args.tokens:
        requested = args.tokenizer_model or _md._default_tokenizer_model()
        try:
            resolution = _md._resolve_tokenizer_model(declared_model, requested)
            tokenizer = _md._load_tokenizer(requested)
        except (_md.TokenizerModelMismatch, _md.TokenizerUnavailable) as exc:
            print(f"measure: {exc}", file=sys.stderr)
            return 2
        provenance = _md._tokenizer_provenance(requested, tokenizer)
        counter = lambda value: _md._count_tokens(tokenizer, value)  # noqa: E731

    chunker = TextChunker(chunk_size=args.chunk_size, min_chunk_size=args.min_chunk_size)
    totals = Totals()
    hosts: dict[str, int] = {}
    skipped_no_raw = 0
    skipped_error = 0
    geometry = _md.TokenGeometry(model_id=str(provenance.get("model_id") or ""))
    tiny_chunks = 0
    tiny_tokens = 0
    total_chunks = 0
    total_tokens = 0

    for row in rows:
        url = str(row["url"])
        payload = _body(url, str(row["raw_path"]), base)
        if payload is None:
            skipped_no_raw += 1
            continue
        extracted = extractor.extract(
            RawPage(url=url, content=payload, fetched_at="", content_type=None)
        )
        if extracted.error or not extracted.text:
            skipped_error += 1
            continue
        result = _measure(url, extracted.text, chunker)
        totals.add(result)
        hosts[_host_of(url)] = hosts.get(_host_of(url), 0) + 1
        if counter is not None:
            for chunk in result.chunks:
                tokens = counter(chunk.text)
                total_chunks += 1
                total_tokens += tokens
                if len(chunk.text.strip()) < chunker.min_chunk_size:
                    tiny_chunks += 1
                    tiny_tokens += tokens
                geometry.add(chunk, counter, document=url)

    token_summary: dict[str, Any] | None = None
    if counter is not None:
        token_summary = _token_summary(
            geometry, tiny_chunks, tiny_tokens, total_chunks, total_tokens
        )

    _print_report(
        totals,
        chunk_size=args.chunk_size,
        min_chunk_size=args.min_chunk_size,
        hosts=hosts,
        token_summary=token_summary,
    )
    print(f"пропущено: без сырья={skipped_no_raw}, ошибка извлечения={skipped_error}")

    if args.out_json:
        payload_json: dict[str, Any] = {
            "extractor_version": TEXT_EXTRACTOR_VERSION,
            "chunker_version": TEXT_CHUNKER_VERSION,
            "chunk_size": args.chunk_size,
            "min_chunk_size": args.min_chunk_size,
            "documents": totals.documents,
            "chars": totals.chars,
            "chunks": totals.chunks,
            "invariants": {
                "p1_violations": totals.p1_violations,
                "p4_violations": totals.p4_violations,
                "gap_chars": totals.gap_chars,
                "overlap_chars": totals.overlap_chars,
                "coverage_pct": _share(
                    totals.chars - totals.gap_chars - totals.overlap_chars,
                    totals.chars,
                )
                * 100.0,
            },
            "edge_whitespace": {
                "chunks": totals.edge_whitespace_chunks,
                "share": _share(totals.edge_whitespace_chunks, totals.chunks),
                "chars_lost_to_strip": totals.chars_lost_to_strip,
            },
            "geometry": {
                "over_budget": totals.over_budget,
                "tiny": totals.tiny,
                "tiny_share": _share(totals.tiny, totals.chunks),
                "tiny_char_share": _share(totals.tiny_chars, totals.chars),
                "max_chars": max(totals.lengths) if totals.lengths else None,
            },
            "tokens": token_summary,
            "tokenizer": (
                {**provenance, **resolution} if token_summary is not None else None
            ),
            "hosts": hosts,
            "skipped": {"no_raw": skipped_no_raw, "extract_error": skipped_error},
            "boundaries": {
                "note": (
                    "строки «до» нет: text-para-3 нарушал P1 по построению; границы на "
                    "фикстурах с разделителем ровно \\n\\n не изменились (пин "
                    "tests/adapters/test_chunker_text.py::test_cut_boundaries_are_pinned)"
                )
            },
        }
        target = Path(args.out_json).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(payload_json, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"json: {target}")
    return 0


def _host_of(url: str) -> str:
    return url.split("://", 1)[-1].split("/", 1)[0].split(":", 1)[0].lower()


if __name__ == "__main__":
    raise SystemExit(main())