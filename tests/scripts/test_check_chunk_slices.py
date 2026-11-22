"""Тесты прибора «чанк — точный срез текста документа» (`scripts/check_chunk_slices.py`).

Прибор отвечает на вопрос, который до 22.11.2026 задавался неверно: срез сверялся с
**конкатенацией текстов чанков**, а она равна документу только при отсутствии пропусков.
На ZH-корпусе это дало 5 243 «нарушения» из 50 813 чанков при нуле настоящих — пропуск в
1 500 символов сдвигал все последующие куски. Поэтому проверяются ровно те четыре класса,
которые прибор обязан различать:

* **пробельный пропуск** — факт (кусок из одних пробелов чанком не становится), а не
  нарушение; смешать его с нарушением значит вернуть ложную тревогу;
* **содержательный пропуск** — нарушение: символ документа не попал ни в один чанк;
* **перекрытие** — нарушение: адрес куска перестаёт быть адресом;
* **несовпадение среза** — нарушение: ``chunk.text`` больше не ``text[cs:ce]``.

Плюс два свойства, без которых прибор молча отвечал бы не на тот вопрос: пространство
координат берётся у **чанкера** (``.md`` читает ``text-plain-2``, а режет markdown-чанкер),
а невосстановимое семейство (HTML/PDF) публикуется как «не проверялось», а не как «чисто».
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]


def _load_module():
    """Модуль прибора по пути: `scripts/` не пакет."""
    path = _ROOT / "scripts" / "check_chunk_slices.py"
    spec = importlib.util.spec_from_file_location("check_chunk_slices", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_chunk_slices"] = module
    spec.loader.exec_module(module)
    return module


check = _load_module()


def _chunks(text: str, *needles: str) -> list[tuple[int, int, int, str]]:
    """Куски по подстрокам: адрес берётся из текста, поэтому срез точен по построению."""
    out = []
    for number, needle in enumerate(needles):
        start = text.index(needle)
        out.append((number, start, start + len(needle), needle))
    return out


# --------------------------------------------------------------------------- #
# Четыре класса: что нарушение, а что факт
# --------------------------------------------------------------------------- #


def test_whitespace_gap_is_a_fact_not_a_defect() -> None:
    """Пропуск из одних пробелов: ноль нарушений и один названный пропуск.

    Это ровно тот случай, на котором прежняя проверка (срез от конкатенации) давала
    ложное нарушение: ``join`` короче документа, и все последующие куски «съезжали».
    """
    text = "первый\n\n     \n\nвторой"
    finding = check.analyze_document(
        "doc", "markdown", text, _chunks(text, "первый", "второй")
    )

    assert finding.defects == 0
    assert finding.whitespace_gap == [(6, 15)]
    assert finding.content_gap == []


def test_content_gap_is_a_defect() -> None:
    """Непробельный символ между кусками: документ потерял текст — нарушение."""
    text = "первый\n\nX\n\nвторой"
    finding = check.analyze_document(
        "doc", "markdown", text, _chunks(text, "первый", "второй")
    )

    assert finding.content_gap == [(6, 11)]
    assert finding.defects == 1


def test_overlap_is_a_defect() -> None:
    """Кусок начинается раньше конца предыдущего: адрес сломан."""
    text = "первыйвторой"
    finding = check.analyze_document(
        "doc", "text", text, [(0, 0, 8, "первыйвт"), (1, 6, 12, "второй")]
    )

    assert finding.overlap == [(6, 8)]
    assert finding.defects == 1


def test_slice_mismatch_is_a_defect() -> None:
    """``chunk.text`` не равен своему срезу — на этом держатся цитата и адрес."""
    text = "первый второй"
    finding = check.analyze_document("doc", "text", text, [(0, 0, 6, "другой")])

    assert finding.slice_mismatch == [0]
    assert finding.defects == 1


def test_chunks_without_offsets_are_skipped_not_blamed() -> None:
    """Куски без смещений (старый абзацный чанкер) не считаются нарушением."""
    finding = check.analyze_document(
        "doc", "text", "первый", [(0, None, None, "первый")]
    )

    assert finding.chunks == 1
    assert finding.defects == 0


# --------------------------------------------------------------------------- #
# Пространство координат: у чанкера, а не у читателя
# --------------------------------------------------------------------------- #


def test_markdown_url_is_checked_in_the_markdown_chunkers_space() -> None:
    """``.md`` читает ``text-plain-2``, а режет markdown-чанкер — семейство берётся по адресу.

    Метка «text» здесь означала бы чужое пространство координат: срез сверялся бы с
    текстом, которого чанкер не видел, и прибор «нашёл» бы нарушение на целом корпусе.
    """
    restored = check.document_text(
        "https://example.test/docs/guide.md", "# Заголовок\n\nАбзац.\n".encode()
    )

    assert restored is not None
    family, text = restored
    assert family == "markdown"
    assert text.startswith("# Заголовок")


def test_plain_text_url_is_checked_in_the_text_chunkers_space() -> None:
    """``.txt`` — абзацный чанкер: пространство координат то же, что у него."""
    restored = check.document_text("https://example.test/notes.txt", "первый\r\nвторой".encode())

    assert restored is not None
    family, text = restored
    assert family == "text"
    assert text == "первый\nвторой"


def test_unrecoverable_family_is_reported_as_not_checked() -> None:
    """HTML/PDF: текст документа в БД не хранится — «не проверялось», а не «чисто»."""
    assert check.document_text("https://example.test/page.html", b"<html></html>") is None


# --------------------------------------------------------------------------- #
# CLI: код возврата и отчёт
# --------------------------------------------------------------------------- #


def _make_db(tmp_path: Path, raw_text: str, *needles: str) -> Path:
    """Минимальная БД прогона: куски строятся в **пространстве координат чанкера**.

    Смещения считаются по восстановленному тексту, а не по байтам файла: читатель
    схлопывает пустые строки и обрезает края, поэтому «тот же текст» до и после чтения —
    разные строки, и адреса, взятые из байтов, сдвинулись бы на целом документе.
    """
    url = "https://example.test/doc.md"
    raw = tmp_path / "doc.bin"
    raw.write_bytes(raw_text.encode("utf-8"))
    restored = check.document_text(url, raw_text.encode("utf-8"))
    assert restored is not None
    text = restored[1]
    chunks = _chunks(text, *needles)
    db = tmp_path / "cohound.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE corpus_docs (doc_id TEXT PRIMARY KEY, url TEXT, raw_path TEXT, "
        "extractor_version TEXT)"
    )
    conn.execute(
        "CREATE TABLE corpus_chunks (doc_id TEXT, product_id TEXT, chunk_no INTEGER, "
        "char_start INTEGER, char_end INTEGER, text TEXT)"
    )
    conn.execute(
        "INSERT INTO corpus_docs VALUES (?, ?, ?, ?)",
        ("doc", url, str(raw), "text-plain-2"),
    )
    conn.executemany(
        "INSERT INTO corpus_chunks VALUES ('doc', 'p', ?, ?, ?, ?)", chunks
    )
    conn.commit()
    conn.close()
    return db


def test_cli_reports_clean_corpus_with_exit_zero(tmp_path: Path, capsys) -> None:
    """Пробельный пропуск — не нарушение: код возврата 0, пропуск опубликован числом."""
    db = _make_db(tmp_path, "первый\n" + " " * 30 + "\nвторой", "первый", "второй")

    assert check.main(["--db", str(db)]) == 0
    out = capsys.readouterr().out
    assert "нарушения: документов 0" in out
    assert "пробельные пропуски (факт, не нарушение): 1" in out


def test_cli_reports_defect_with_exit_one(tmp_path: Path, capsys) -> None:
    """Содержательный пропуск — код возврата 1: молча «зелёный» прибор хуже отсутствующего."""
    db = _make_db(tmp_path, "первый\nX\nвторой", "первый", "второй")

    assert check.main(["--db", str(db)]) == 1
    assert "содержательный пропуск 1" in capsys.readouterr().out
