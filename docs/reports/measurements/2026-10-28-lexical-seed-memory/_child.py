#!/usr/bin/env python3
"""Один замер в изолированном процессе (вызывается ``probe.py``).

Изоляция обязательна: ``ru_maxrss`` — максимум за жизнь **процесса**, поэтому вторая
точка в том же процессе унаследовала бы пик первой и замер стал бы неубывающим по
построению.
"""

from __future__ import annotations

import json
import os
import resource
import shutil
import sys
import tempfile

TEXT = ("терминология гиперконвергентного кластера отказоустойчивость репликация " * 20)[:1000]


def _text(i: int) -> str:
    """Тело чанка **уникально** на запись: так выглядит реальный корпус.

    Общий константный текст занижал бы стоимость зеркала: одна строка на все чанки
    занимает один объект, а не по килобайту на запись. Уникальность делает замер
    консервативным в нужную сторону — меряется то, что действительно жило бы в памяти.
    """
    return f"{TEXT}{i:08d}"


def main(argv: list[str]) -> int:
    n = int(argv[1])
    mode = argv[2]
    from core.adapters.lexical_tantivy import TantivyLexicalIndex

    def records():
        for i in range(n):
            yield {
                "chunk_id": f"c{i:08d}",
                "product_id": "p1",
                "text": _text(i),
                "heading_path": "h/1",
                "content_hash": f"hash{i:08d}",
                "build_id": "pb_test",
            }

    # Каталог индекса убирается вручную: ``TemporaryDirectory`` падает на гонке с
    # ещё не закрытыми файлами движка (``Directory not empty``), а замер памяти к уборке
    # отношения не имеет.
    tmp = tempfile.mkdtemp(dir=os.environ.get("TMPDIR", None))
    try:
        index = TantivyLexicalIndex(tmp)
        chunks = list(records()) if mode == "list" else records()
        written = index.index(chunks, build_id="cb_test")
        peak_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        index.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print(json.dumps({"n": n, "mode": mode, "written": written, "peak_rss_mb": round(peak_mb, 1)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
