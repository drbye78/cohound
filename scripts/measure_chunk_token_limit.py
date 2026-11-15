"""Прибор: распределение токенов чанка до и после токенного ограничителя (O21 плана 26).

Вход — **сохранённый** корпус (`cohound.db` прогона, ``mode=ro``): чанки берутся из
``corpus_chunks`` ровно в том виде, в каком они попали в индекс, а не пере-нарезаются из
байтов. Причина: ограничитель работает **поверх** структурной нарезки и не знает
документа — его единица это чанк. Поэтому «до» здесь тождественно корпусу, а «после»
отличается от него только резами ограничителя.

**Что публикуется** (числа, не оценки):

``before`` / ``after``
    Распределение токенов ``chunk.text`` (токенизатор эмбеддера, ``local_files_only``):
    число чанков, p50/p90/p95/p99/max, доля чанков длиннее лимита и доля токенов в них,
    доля чанков короче 64 токенов. Разрез — по семейству читателя (``text`` против
    markdown/офис/PDF): у них разная единица смысла и разные бюджеты в символах.
``policy``
    Счётчики ограничителя (:class:`core.adapters.token_limit.LimitReport`): сколько
    чанков разрезано, сколько хвостов приклеено (объявленный выход за лимит), сколько
    чанков не резалось из-за отсутствия точного среза.
``invariants``
    ``p1_violations`` — документы, где склейка текстов чанков после ограничителя не равна
    склейке до него. Инвариант: **0** (P1). ``span_violations`` — куски, где
    ``text`` не совпал со срезом ``[char_start:char_end]``. Инвариант: **0**.
``cost``
    Время и число вызовов токенизатора: цена ограничителя — измеренная величина, а не
    «быстро/медленно».

**Чего прибор НЕ измеряет:** качество ретривала и κ оценщика (для этого нужен
``golden``), стоимость эмбеддинга, влияние на промпт оценщика. Только геометрию в
информационных единицах и её инварианты.

Нейтрален (§1.3): ни продуктов, ни домена — только семейства форматов.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.adapters.chunker_family import TOKEN_LIMIT
from core.adapters.token_limit import (
    TOKEN_LIMIT_POLICY,
    HfTokenCounter,
    TokenizerUnavailable,
    TokenLimiter,
)
from core.ports.types import Chunk

#: Порог «обрывка»: тот же, что у ограничителя по умолчанию (публикуется отдельно).
TINY_TOKENS = 64

#: Порог публикуемого хвоста распределения — лимит замера, а не свойство чанкера.
DEFAULT_TOKEN_LIMIT = 512


@dataclass
class Distribution:
    """Накопитель распределения по одному семейству."""

    chunks: int = 0
    tokens: list[int] | None = None
    chars: int = 0
    over_limit: int = 0
    over_limit_tokens: int = 0
    tiny: int = 0
    tiny_tokens: int = 0

    def __post_init__(self) -> None:
        if self.tokens is None:
            self.tokens = []

    def add(self, tokens: int, chars: int, limit: int) -> None:
        assert self.tokens is not None
        self.tokens.append(tokens)
        self.chunks += 1
        self.chars += chars
        if tokens > limit:
            self.over_limit += 1
            self.over_limit_tokens += tokens
        if tokens < TINY_TOKENS:
            self.tiny += 1
            self.tiny_tokens += tokens

    def summary(self, limit: int) -> dict[str, Any]:
        assert self.tokens is not None
        ordered = sorted(self.tokens)
        total = sum(ordered)
        return {
            "chunks": self.chunks,
            "chars": self.chars,
            "tokens_total": total,
            "tokens_per_char": round(total / self.chars, 4) if self.chars else None,
            "p50": _percentile(ordered, 50),
            "p90": _percentile(ordered, 90),
            "p95": _percentile(ordered, 95),
            "p99": _percentile(ordered, 99),
            "max": ordered[-1] if ordered else 0,
            "over_limit": self.over_limit,
            "over_limit_share": _share(self.over_limit, self.chunks),
            "over_limit_token_share": _share(self.over_limit_tokens, total),
            "tiny": self.tiny,
            "tiny_share": _share(self.tiny, self.chunks),
            "tiny_token_share": _share(self.tiny_tokens, total),
            "limit": limit,
        }


def _share(part: int, whole: int) -> float:
    return part / whole if whole else 0.0


def _percentile(ordered: list[int], quantile: float) -> float:
    """Линейная интерполяция — та же формула, что у приборов чанкеров."""
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return float(ordered[0])
    position = (len(ordered) - 1) * quantile / 100.0
    low, high = int(position), min(int(position) + 1, len(ordered) - 1)
    if low == high:
        return float(ordered[low])
    weight = position - low
    return float(ordered[low]) + (float(ordered[high]) - float(ordered[low])) * weight


def _family(extractor_version: str | None) -> str:
    """Семейство по версии читателя: у плоского текста своя единица смысла."""
    version = extractor_version or ""
    return "text" if version.startswith("text-plain") else "markdown"


def _chunk(row: sqlite3.Row) -> Chunk:
    """Строка ``corpus_chunks`` → ``Chunk`` (провенанс сохраняется как есть)."""
    return Chunk(
        chunk_id=str(row["chunk_id"]),
        chunk_no=int(row["chunk_no"] or 0),
        text=str(row["text"] or ""),
        heading_path=str(row["heading_path"] or ""),
        context_prefix=str(row["context_prefix"] or ""),
        content_hash=str(row["content_hash"] or ""),
        char_start=row["char_start"],
        char_end=row["char_end"],
    )


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="measure_chunk_token_limit.py",
        description=(
            "Приёмка токенного ограничителя чанка на сохранённом корпусе (read-only): "
            "распределение токенов до/после, инварианты P1 и точных срезов, цена."
        ),
        epilog=(
            "Пример:\n"
            "  python3 scripts/measure_chunk_token_limit.py \\\n"
            "      --db ~/.cohound/data/runs/{run_id}/cohound.db --token-limit 512 \\\n"
            "      --out-json /tmp/chunk-token-limit.json\n"
            "\n"
            "Токенизатор берётся из локального кэша (local_files_only=True); сеть запрещена.\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", required=True, help="cohound.db прогона (только чтение)")
    parser.add_argument(
        "--token-limit",
        type=int,
        default=TOKEN_LIMIT[0] if TOKEN_LIMIT is not None else DEFAULT_TOKEN_LIMIT,
        help=(
            "лимит в токенах (по умолчанию объявленный TOKEN_LIMIT, иначе "
            f"{DEFAULT_TOKEN_LIMIT})"
        ),
    )
    parser.add_argument(
        "--tokenizer-model",
        default=TOKEN_LIMIT[1] if TOKEN_LIMIT is not None else "BAAI/bge-m3",
        metavar="MODEL_ID[@REVISION]",
        help="токенизатор того же семейства, что у эмбеддера (по умолчанию — объявленный)",
    )
    parser.add_argument("--limit", type=int, default=0, help="документов (0 — все)")
    parser.add_argument("--out-json", metavar="PATH", help="куда записать машинный отчёт")
    return parser.parse_args(argv)


def _split_model(value: str) -> tuple[str, str | None]:
    model, sep, revision = value.rpartition("@")
    if not sep:
        return value, None
    return model, revision or None


def _measure_document(
    chunks: list[Chunk],
    family: str,
    limiter: TokenLimiter,
    counter: HfTokenCounter,
    limit: int,
    before: dict[str, Distribution],
    after: dict[str, Distribution],
    totals: dict[str, int],
    invariant: dict[str, int],
    calls: int,
) -> tuple[dict[str, int], dict[str, int], int]:
    """Один документ: «до», «после», инварианты и счётчики ограничителя.

    Токены чанка, который ограничитель не тронул (на выходе один кусок того же текста),
    **не пересчитываются**: величина та же по построению, а вызов токенизатора — самая
    дорогая часть замера (измерено: 0.8 мс/чанк на 253 306 чанках).
    """
    limited, report = limiter.apply(chunks)
    calls += report.chunks_in + report.chunks_out
    totals["chunks_in"] += report.chunks_in
    totals["chunks_out"] += report.chunks_out
    totals["split_chunks"] += report.split_chunks
    totals["absorbed_tails"] += report.absorbed_tails
    totals["unsplittable"] += report.unsplittable
    totals["over_limit"] += report.over_limit
    totals["max_tokens_after"] = max(totals["max_tokens_after"], report.max_tokens_after)

    if "".join(chunk.text for chunk in limited) != "".join(chunk.text for chunk in chunks):
        invariant["p1_violations"] += 1
    document_text = "".join(chunk.text for chunk in chunks)
    base = chunks[0].char_start or 0
    for chunk in limited:
        if chunk.char_start is None or chunk.char_end is None:
            continue
        if document_text[chunk.char_start - base : chunk.char_end - base] != chunk.text:
            invariant["span_violations"] += 1

    counts_before: list[int] = []
    for chunk in chunks:
        tokens = len(counter.token_ends(chunk.text))
        counts_before.append(tokens)
        before[family].add(tokens, len(chunk.text), limit)

    # Куски сгруппированы по родителю: ``chunk_no`` выходного списка сквозной, поэтому
    # принадлежность видна по префиксу ``chunk_id``.
    by_parent: dict[str, list[Chunk]] = defaultdict(list)
    for chunk in limited:
        by_parent[chunk.chunk_id.rsplit("#", 1)[0]].append(chunk)
    for chunk, tokens in zip(chunks, counts_before, strict=True):
        prefix = chunk.chunk_id.rsplit("#", 1)[0]
        pieces = by_parent.get(prefix, [])
        if len(pieces) == 1 and pieces[0].text == chunk.text:
            after[family].add(tokens, len(chunk.text), limit)
            continue
        for piece in pieces:
            after[family].add(len(counter.token_ends(piece.text)), len(piece.text), limit)
    return totals, invariant, calls


def _report(
    before: dict[str, Distribution],
    after: dict[str, Distribution],
    *,
    limit: int,
    report: dict[str, Any],
    invariant: dict[str, int],
    cost: dict[str, Any],
    model: str,
    declared: bool,
) -> dict[str, Any]:
    families = sorted(set(before) | set(after))
    payload: dict[str, Any] = {
        "token_limit": limit,
        "token_limit_policy": TOKEN_LIMIT_POLICY,
        "tokenizer": model,
        "declared_in_code": declared,
        "tiny_tokens": TINY_TOKENS,
        "families": {
            family: {
                "before": before[family].summary(limit),
                "after": after[family].summary(limit),
            }
            for family in families
        },
        "total": {
            "before": _merge(before, limit),
            "after": _merge(after, limit),
        },
        "policy": report,
        "invariants": invariant,
        "cost": cost,
    }
    return payload


def _merge(parts: dict[str, Distribution], limit: int) -> dict[str, Any]:
    merged = Distribution()
    for distribution in parts.values():
        assert distribution.tokens is not None
        assert merged.tokens is not None
        merged.tokens.extend(distribution.tokens)
        merged.chunks += distribution.chunks
        merged.chars += distribution.chars
        merged.over_limit += distribution.over_limit
        merged.over_limit_tokens += distribution.over_limit_tokens
        merged.tiny += distribution.tiny
        merged.tiny_tokens += distribution.tiny_tokens
    return merged.summary(limit)


def _print_report(payload: dict[str, Any]) -> None:
    print(
        f"токенный ограничитель {payload['token_limit_policy']}, лимит "
        f"{payload['token_limit']} токенов; токенизатор {payload['tokenizer']}"
        f"{'' if payload['declared_in_code'] else ' (объявленный лимит в коде отсутствует)'}"
    )
    for family in payload["families"]:
        before = payload["families"][family]["before"]
        after = payload["families"][family]["after"]
        print("")
        print(f"семейство {family}: чанков {before['chunks']} → {after['chunks']}")
        print(
            f"  токенов p50/p90/p95/p99/max: "
            f"{before['p50']:.0f}/{before['p90']:.0f}/{before['p95']:.0f}/"
            f"{before['p99']:.0f}/{before['max']} → "
            f"{after['p50']:.0f}/{after['p90']:.0f}/{after['p95']:.0f}/"
            f"{after['p99']:.0f}/{after['max']}"
        )
        print(
            f"  >{payload['token_limit']}: {before['over_limit']} "
            f"({before['over_limit_share']:.3%}) → {after['over_limit']} "
            f"({after['over_limit_share']:.3%}); доля токенов "
            f"{before['over_limit_token_share']:.3%} → {after['over_limit_token_share']:.3%}"
        )
        print(
            f"  <{payload['tiny_tokens']}: {before['tiny']} ({before['tiny_share']:.3%}) → "
            f"{after['tiny']} ({after['tiny_share']:.3%}); доля токенов "
            f"{before['tiny_token_share']:.3%} → {after['tiny_token_share']:.3%}"
        )
    total_before = payload["total"]["before"]
    total_after = payload["total"]["after"]
    print("")
    print(
        f"итого чанков {total_before['chunks']} → {total_after['chunks']} "
        f"({_share(total_after['chunks'] - total_before['chunks'], total_before['chunks']):+.2%}); "
        f"токенов {total_before['tokens_total']} → {total_after['tokens_total']}"
    )
    policy = payload["policy"]
    print(
        f"ограничитель: разрезано {policy['split_chunks']}, приклеено хвостов "
        f"{policy['absorbed_tails']}, без точного среза {policy['unsplittable']}, "
        f">лимита после {policy['over_limit']}, max {policy['max_tokens_after']}"
    )
    invariants = payload["invariants"]
    print(
        f"инварианты (цель 0): P1 {invariants['p1_violations']}, "
        f"срез {invariants['span_violations']}"
    )
    cost = payload["cost"]
    print(
        f"цена: {cost['seconds']:.1f} с, вызовов токенизатора {cost['tokenizer_calls']} "
        f"({cost['seconds_per_chunk_ms']:.2f} мс/чанк)"
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if not Path(args.db).expanduser().is_file():
        print(f"measure: БД не найдена: {args.db}", file=sys.stderr)
        return 2
    if args.token_limit <= 0:
        print("measure: --token-limit должен быть положительным", file=sys.stderr)
        return 2

    model, revision = _split_model(args.tokenizer_model)
    counter = HfTokenCounter(model, revision)
    if not HfTokenCounter.available():
        print(
            "measure: transformers недоступен — токены не измеряются. Символьный прокси "
            "вместо токенов не подставляется.",
            file=sys.stderr,
        )
        return 2
    try:
        counter.token_ends("проверка")
    except TokenizerUnavailable as exc:
        print(f"measure: {exc}", file=sys.stderr)
        return 2

    limiter = TokenLimiter(counter, args.token_limit)
    conn = sqlite3.connect(f"file:{Path(args.db).expanduser()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    before: dict[str, Distribution] = defaultdict(Distribution)
    after: dict[str, Distribution] = defaultdict(Distribution)
    totals: dict[str, int] = {
        "chunks_in": 0,
        "chunks_out": 0,
        "split_chunks": 0,
        "absorbed_tails": 0,
        "unsplittable": 0,
        "over_limit": 0,
        "max_tokens_after": 0,
    }
    invariant = {"p1_violations": 0, "span_violations": 0}
    calls = 0
    documents = 0
    # Один проход по ``corpus_chunks``, а не запрос на документ: у ``corpus_chunks`` нет
    # индекса по ``doc_id`` (пайплайн читает чанки по ``build_id``), поэтому выборка
    # «чанки документа» стоила бы полного скана таблицы на каждый из 8 115 документов.
    # Измерено 15.11.2026: три документа за 5 минут против секунд при одном проходе.
    try:
        rows = conn.execute(
            "SELECT c.doc_id, d.extractor_version, c.chunk_id, c.chunk_no, c.text, "
            "c.heading_path, c.context_prefix, c.content_hash, c.char_start, c.char_end "
            "FROM corpus_chunks c JOIN corpus_docs d ON d.doc_id = c.doc_id "
            "ORDER BY c.doc_id, c.chunk_no"
        )
        started = time.perf_counter()
        current_doc: str | None = None
        current_family = "markdown"
        chunks = []
        exhausted = False
        for row in rows:
            doc_id = str(row["doc_id"])
            if doc_id != current_doc:
                if chunks:
                    totals, invariant, calls = _measure_document(
                        chunks,
                        current_family,
                        limiter,
                        counter,
                        args.token_limit,
                        before,
                        after,
                        totals,
                        invariant,
                        calls,
                    )
                    documents += 1
                if args.limit and documents >= args.limit:
                    exhausted = True
                    break
                current_doc = doc_id
                current_family = _family(row["extractor_version"])
                chunks = []
            chunks.append(_chunk(row))
        if chunks and not exhausted:
            totals, invariant, calls = _measure_document(
                chunks,
                current_family,
                limiter,
                counter,
                args.token_limit,
                before,
                after,
                totals,
                invariant,
                calls,
            )
            documents += 1
        seconds = time.perf_counter() - started
    finally:
        conn.close()

    payload = _report(
        before,
        after,
        limit=args.token_limit,
        report=totals,
        invariant=invariant,
        cost={
            "seconds": round(seconds, 2),
            "seconds_per_chunk_ms": round(seconds / max(totals["chunks_in"], 1) * 1000, 3),
            "tokenizer_calls": calls,
            "documents": documents,
        },
        model=args.tokenizer_model,
        declared=TOKEN_LIMIT is not None,
    )
    _print_report(payload)
    if args.out_json:
        target = Path(args.out_json).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(f"json: {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
