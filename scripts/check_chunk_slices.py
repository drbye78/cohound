"""Инвариант «чанк — точный срез текста документа» на реальном корпусе (read-only).

**Зачем отдельный прибор.** Токенный ограничитель проверял этот инвариант срезом от
**конкатенации текстов чанков** — а она равна документу только тогда, когда между кусками
нет пропусков. На корпусе с пропусками проверка даёт ложные «нарушения»: замер
22.11.2026 на ZH-корпусе — 5 243 «нарушения» из 50 813 чанков при **нуле** настоящих,
потому что пропуск в 1 500 символов сдвигал все последующие куски. Здесь сверка идёт с
**текстом документа**, восстановленным из сохранённых байтов через тот же читатель и тот
же парсер, что у чанкера, поэтому пропуск виден как пропуск, а не как сдвиг.

**Что считается нарушением, а что фактом.**

* ``slice_mismatch`` — ``chunk.text != text[char_start:char_end]``. Нарушение: кусок
  обязан быть срезом (на этом держатся цитата I22 и ``page_map``-адресация).
* ``content_gap`` — между соседними кусками документа есть разрыв, в котором остался
  **непробельный** символ. Нарушение: символ документа не попал ни в один чанк.
* ``overlap`` — следующий кусок начинается раньше конца предыдущего. Нарушение:
  перекрытие удваивает evidence-шум и ломает адрес.
* ``whitespace_gap`` — разрыв из одних пробелов/переводов строк. **Не** нарушение, а
  названный факт: кусок из одних пробелов чанком не становится (``if not body.strip():
  continue``), поэтому P1 в формулировке «конкатенация равна входу» на таком документе
  неверна, а «каждый кусок — точный срез» — верна. Различать эти два утверждения и есть
  смысл прибора.

**Что прибор не может проверить и говорит об этом числом.** Восстановить текст документа
можно только у семейств, чей читатель даёт текст без разметки: ``text`` (плоский текст) и
``markdown``. У HTML/PDF/Office текст документа в БД не хранится (живёт в чанках), поэтому
такие документы считаются **непроверяемыми**, а не «чистыми». Семейство берётся тем же
:func:`core.util.document_formats.textual_family`, что у маршрутизатора, — по адресу
документа, а не по версии читателя: ``.md`` читает ``text-plain-2``, а режет
**markdown**-чанкер, и вывод семейства из версии читателя дал бы неверную метку.

``content_type`` при получении в БД не сохраняется, поэтому байты декодируются как UTF-8;
расхождение декодирования видно как ``slice_mismatch`` (и расследуется), а не прощается.

Нейтрален (§1.3): ни продуктов, ни домена — только семейства форматов.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from core.adapters.chunker_text import _normalize
from core.adapters.extract_text import TextExtractor
from core.adapters.md_ast import parse_markdown
from core.ports.types import RawPage
from core.util.document_formats import FAMILY_MARKDOWN, FAMILY_TEXT, textual_family

__all__ = ["analyze_document", "document_text", "main"]

#: Семейства, у которых текст документа восстановим из сохранённых байтов.
CHECKABLE = (FAMILY_TEXT, FAMILY_MARKDOWN)


@dataclass
class Finding:
    """Один документ: нарушения и факты. Пустые списки — документ чист."""

    doc_id: str
    family: str
    chunks: int = 0
    slice_mismatch: list[int] = field(default_factory=list)
    content_gap: list[tuple[int, int]] = field(default_factory=list)
    overlap: list[tuple[int, int]] = field(default_factory=list)
    whitespace_gap: list[tuple[int, int]] = field(default_factory=list)

    @property
    def defects(self) -> int:
        return len(self.slice_mismatch) + len(self.content_gap) + len(self.overlap)

    def as_dict(self) -> dict[str, Any]:
        return {
            "doc_id": self.doc_id,
            "family": self.family,
            "chunks": self.chunks,
            "slice_mismatch": self.slice_mismatch,
            "content_gap": self.content_gap,
            "overlap": self.overlap,
            "whitespace_gap": self.whitespace_gap,
        }


def document_text(url: str, raw: bytes) -> tuple[str, str] | None:
    """Текст документа в пространстве координат чанкера: ``(family, text)`` или ``None``.

    ``None`` — семейство не восстановимо (HTML/PDF/Office) либо не опознано по адресу:
    это «не проверялось», а не «проверено и чисто».

    У markdown координаты относятся к **разобранному** тексту (``parse_markdown().text``),
    а не к нормализованным байтам: чанкер режет именно его. У плоского текста — к
    нормализованному входу (``chunker_text._normalize``). Обе функции те же, что в
    пайплайне, поэтому пространство координат совпадает по построению.
    """
    family = textual_family(url)
    if family not in CHECKABLE:
        return None
    extracted = TextExtractor().extract(
        RawPage(
            url=url,
            content=raw,
            fetched_at="",
            fetch_mode=None,
        )
    )
    if extracted.error or not extracted.text:
        return None
    if family == FAMILY_MARKDOWN:
        return family, parse_markdown(extracted.text).text
    return family, _normalize(extracted.text)


def analyze_document(
    doc_id: str, family: str, text: str, chunks: Sequence[tuple[int, int, str]]
) -> Finding:
    """Сверка кусков с текстом документа.

    ``chunks`` — ``(chunk_no, char_start, char_end, chunk_text)`` в порядке ``chunk_no``.
    Куски без смещений (``NULL``) не проверяются: у абзацного чанкера до ``text-para-4``
    смещений не было, и выдумывать их нельзя — они считаются отдельным числом.
    """
    finding = Finding(doc_id=doc_id, family=family, chunks=len(chunks))
    ordered = sorted(chunks, key=lambda item: item[0])
    previous: tuple[int, int] | None = None
    for chunk_no, start, end, body in ordered:
        if start is None or end is None:
            continue
        if text[start:end] != body:
            finding.slice_mismatch.append(chunk_no)
        if previous is not None:
            prev_end = previous[1]
            if start > prev_end:
                gap = text[prev_end:start]
                if gap.strip():
                    finding.content_gap.append((prev_end, start))
                else:
                    finding.whitespace_gap.append((prev_end, start))
            elif start < prev_end:
                finding.overlap.append((start, prev_end))
        previous = (start, end)
    return finding


def _load_documents(
    db: Path, limit: int, product: str | None
) -> dict[str, dict[str, Any]]:
    """Один проход по корпусу: документы и их куски, сгруппированные по ``doc_id``."""
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        where = " WHERE c.product_id = ?" if product else ""
        params: tuple[Any, ...] = (product,) if product else ()
        rows = conn.execute(
            "SELECT c.doc_id, c.product_id, c.chunk_no, c.char_start, c.char_end, c.text, "
            "d.url, d.raw_path, d.extractor_version "
            "FROM corpus_chunks c JOIN corpus_docs d ON d.doc_id = c.doc_id"
            + where
            + " ORDER BY c.doc_id, c.chunk_no",
            params,
        )
        documents: dict[str, dict[str, Any]] = {}
        for row in rows:
            doc_id = str(row["doc_id"])
            entry = documents.get(doc_id)
            if entry is None:
                if limit and len(documents) >= limit:
                    break
                entry = {
                    "url": str(row["url"] or ""),
                    "raw_path": row["raw_path"],
                    "extractor_version": str(row["extractor_version"] or ""),
                    "chunks": [],
                }
                documents[doc_id] = entry
            entry["chunks"].append(
                (row["chunk_no"], row["char_start"], row["char_end"], str(row["text"] or ""))
            )
        return documents
    finally:
        conn.close()


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="check_chunk_slices.py",
        description=(
            "Инвариант «чанк — точный срез текста документа» на сохранённом корпусе "
            "(read-only): срез, пропуски (пробельные и содержательные) и перекрытия."
        ),
        epilog=(
            "Пример:\n"
            "  python3 scripts/check_chunk_slices.py --db "
            "~/.cohound/data/runs/{run_id}/cohound.db --out-json /tmp/slices.json\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", required=True, help="cohound.db прогона (только чтение)")
    parser.add_argument("--limit", type=int, default=0, help="документов (0 — все)")
    parser.add_argument("--product", help="только один продукт")
    parser.add_argument("--out-json", metavar="PATH", help="куда записать машинный отчёт")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    db = Path(args.db).expanduser()
    if not db.is_file():
        print(f"check: БД не найдена: {args.db}", file=sys.stderr)
        return 2
    documents = _load_documents(db, args.limit, args.product)
    findings: list[Finding] = []
    skipped: dict[str, int] = defaultdict(int)
    for doc_id, entry in documents.items():
        family = textual_family(entry["url"])
        if family not in CHECKABLE:
            skipped[f"семейство не восстановимо: {family or 'не опознано'}"] += 1
            continue
        raw_path = entry["raw_path"]
        if not raw_path:
            skipped["нет сохранённых байтов"] += 1
            continue
        path = Path(str(raw_path))
        try:
            raw = path.read_bytes()
        except OSError:
            skipped["байты не читаются"] += 1
            continue
        restored = document_text(entry["url"], raw)
        if restored is None:
            skipped["текст документа не восстановлен"] += 1
            continue
        family, text = restored
        findings.append(analyze_document(doc_id, family, text, entry["chunks"]))

    defects = [finding for finding in findings if finding.defects]
    total_chunks = sum(finding.chunks for finding in findings)
    gaps = [gap for finding in findings for gap in finding.whitespace_gap]
    print(
        f"проверено документов {len(findings)} ({total_chunks} чанков), "
        f"пропущено {sum(skipped.values())}"
    )
    for reason, count in sorted(skipped.items()):
        print(f"  пропуск: {reason} — {count}")
    print(
        f"нарушения: документов {len(defects)}, "
        f"срез {sum(len(f.slice_mismatch) for f in findings)}, "
        f"содержательный пропуск {sum(len(f.content_gap) for f in findings)}, "
        f"перекрытие {sum(len(f.overlap) for f in findings)}"
    )
    print(
        f"пробельные пропуски (факт, не нарушение): {len(gaps)} в "
        f"{sum(1 for f in findings if f.whitespace_gap)} документах, "
        f"суммарно {sum(end - start for start, end in gaps)} символов"
    )
    for finding in defects[:10]:
        print(
            f"  {finding.doc_id[:70]}: срез {finding.slice_mismatch[:5]}, "
            f"пропуск {finding.content_gap[:3]}, перекрытие {finding.overlap[:3]}"
        )
    payload = {
        "documents": len(findings),
        "chunks": total_chunks,
        "skipped": dict(skipped),
        "defects": [finding.as_dict() for finding in defects],
        "whitespace_gaps": {
            "count": len(gaps),
            "chars": sum(end - start for start, end in gaps),
            "documents": sum(1 for finding in findings if finding.whitespace_gap),
        },
    }
    if args.out_json:
        target = Path(args.out_json).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(f"json: {target}")
    return 1 if defects else 0


if __name__ == "__main__":
    sys.exit(main())
