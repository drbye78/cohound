"""Тесты прибора эквивалентности набора кусков (``scripts/measure_chunker_equivalence.py``).

Прибор отвечает на вопрос «изменилась ли нарезка при смене версии чанкера», и цена ошибки
здесь — ложный вердикт о ``execution_change``. Поэтому проверяются не только «счастливые»
пути: каждая ветка, которая **пропускает** документ или объявляет расхождение, имеет свой
тест. Иначе прибор, тихо пропустивший документ, читался бы как «всё совпало».
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

import measure_chunker_equivalence as mce  # noqa: E402

from core.adapters.chunker_markdown import MarkdownChunker  # noqa: E402
from core.util.hashing import content_hash  # noqa: E402

#: Текст, который чанкер режет на несколько кусков (две секции с абзацами).
DOC_TEXT = (
    "# Раздел\n\n"
    + "Первый абзац про изоляцию транзакций. " * 12
    + "\n\n## Подраздел\n\n"
    + "Второй абзац про снимки данных. " * 12
    + "\n"
)


def _chunks(text: str = DOC_TEXT) -> list:
    """Куски реальным чанкером: прибор обязан сравнивать то, что делает прод."""
    return MarkdownChunker(chunk_size=1500, profile="gfm").chunk_text("d1", text)


def _make_db(tmp_path: Path, docs: list[tuple[str, str, str, list]]) -> Path:
    """Создаёт run-DB с колонками, которые читает прибор.

    ``docs`` — ``[(doc_id, url, extractor_version, chunks)]``, где ``chunks`` — список
    объектов ``Chunk`` (берутся ``chunk_no``/``text``/смещения/пути/префикс).
    """
    db_path = tmp_path / "cohound.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE corpus_docs (
            doc_id TEXT PRIMARY KEY,
            product_id TEXT,
            url TEXT,
            extractor_version TEXT
        );
        CREATE TABLE corpus_chunks (
            doc_id TEXT,
            chunk_no INTEGER,
            text TEXT,
            content_hash TEXT,
            char_start INTEGER,
            char_end INTEGER,
            heading_path TEXT,
            context_prefix TEXT
        );
        """
    )
    for doc_id, url, extractor_version, chunks in docs:
        conn.execute(
            "INSERT INTO corpus_docs VALUES (?, ?, ?, ?)",
            (doc_id, doc_id.split(":")[0], url, extractor_version),
        )
        for chunk in chunks:
            conn.execute(
                "INSERT INTO corpus_chunks VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    doc_id,
                    chunk.chunk_no,
                    chunk.text,
                    content_hash(chunk.text),
                    chunk.char_start,
                    chunk.char_end,
                    chunk.heading_path,
                    chunk.context_prefix,
                ),
            )
    conn.commit()
    conn.close()
    return db_path


# --- _restore_text ---------------------------------------------------------------


def test_restore_text_concatenates_continuous_slices() -> None:
    chunks = _chunks()
    rows = [
        {
            "chunk_no": c.chunk_no,
            "text": c.text,
            "char_start": c.char_start,
            "char_end": c.char_end,
        }
        for c in chunks
    ]
    text, reason = mce._restore_text(rows)
    assert reason is None
    # Восстановленный текст — нормализованный вход парсера: он обязан начинаться с
    # заголовка и содержать оба абзаца, иначе сравнивать нечего.
    assert text is not None
    assert text.startswith("# Раздел")
    assert "Второй абзац" in text


def test_restore_text_reports_gap() -> None:
    chunks = _chunks()
    rows = [
        {"text": c.text, "char_start": c.char_start, "char_end": c.char_end}
        for c in chunks
    ]
    rows[1]["char_start"] += 5  # дыра между кусками
    text, reason = mce._restore_text(rows)
    assert text is None
    assert reason == "gap_or_overlap"


def test_restore_text_reports_overlap() -> None:
    chunks = _chunks()
    rows = [
        {"text": c.text, "char_start": c.char_start, "char_end": c.char_end}
        for c in chunks
    ]
    rows[1]["char_start"] -= 5  # перекрытие
    text, reason = mce._restore_text(rows)
    assert text is None
    assert reason == "gap_or_overlap"


def test_restore_text_reports_missing_offsets() -> None:
    text, reason = mce._restore_text(
        [{"text": "x", "char_start": None, "char_end": None}]
    )
    assert text is None
    assert reason == "no_offsets"


def test_restore_text_reports_length_mismatch() -> None:
    text, reason = mce._restore_text(
        [{"text": "abc", "char_start": 0, "char_end": 10}]
    )
    assert text is None
    assert reason == "length_mismatch"


def test_restore_text_reports_no_chunks() -> None:
    text, reason = mce._restore_text([])
    assert text is None
    assert reason == "no_chunks"


# --- _family_for -----------------------------------------------------------------


def test_family_text_reader_plain_suffix_is_text() -> None:
    family, suspicion = mce._family_for("text-plain-2", "https://h/a.txt")
    assert family == "text"
    assert suspicion is None


def test_family_text_reader_markdown_suffix_is_flagged() -> None:
    """Markdown-суффикс при текстовом читателе: семейство зависит от ``Content-Type``.

    В run-DB его нет, поэтому сторона — предположение. Прибор обязан назвать документ
    (``markdown_suffix_in_text_reader``), а не выбрать сторону молча: неверный чанкер дал бы
    расхождение, которого нет в продукте.
    """
    family, suspicion = mce._family_for("text-plain-2", "https://h/readme.md")
    assert family == "markdown"
    assert suspicion == "markdown_suffix_in_text_reader"


@pytest.mark.parametrize(
    "extractor_version",
    ["trafilatura-html-6", "docling-doc-3:layout_tables/cuda/x/y/z", "epub-3"],
)
def test_family_document_readers_are_markdown(extractor_version: str) -> None:
    family, suspicion = mce._family_for(extractor_version, "https://h/a.txt")
    assert family == "markdown"
    assert suspicion is None


# --- _compare --------------------------------------------------------------------


def _row(**kwargs: object) -> sqlite3.Row:
    """Строка ``corpus_chunks`` с дефолтами: тесту важны только переопределённые поля."""
    values = {
        "chunk_no": 0,
        "text": "",
        "content_hash": "",
        "char_start": 0,
        "char_end": 0,
        "heading_path": "",
        "context_prefix": "",
    }
    values.update(kwargs)
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE t (chunk_no, text, content_hash, char_start, char_end,"
        " heading_path, context_prefix)"
    )
    conn.execute(
        "INSERT INTO t VALUES (:chunk_no, :text, :content_hash, :char_start, :char_end,"
        " :heading_path, :context_prefix)",
        values,
    )
    return conn.execute("SELECT * FROM t").fetchone()


def _doc(url: str = "https://h/a", version: str = "trafilatura-html-6") -> sqlite3.Row:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE d (doc_id, product_id, url, extractor_version)")
    conn.execute(
        "INSERT INTO d VALUES ('p:d1', 'p', ?, ?)", (url, version)
    )
    return conn.execute("SELECT * FROM d").fetchone()


def test_compare_equal_sets_have_no_mismatch() -> None:
    chunks = _chunks()
    rows = [
        _row(
            chunk_no=c.chunk_no,
            text=c.text,
            content_hash=content_hash(c.text),
            char_start=c.char_start,
            char_end=c.char_end,
            heading_path=c.heading_path,
            context_prefix=c.context_prefix,
        )
        for c in chunks
    ]
    outcome = mce._compare(_doc(), rows, chunks)
    assert outcome.skipped is None
    assert outcome.compared == len(chunks)
    assert not any(outcome.mismatches.values()), outcome.mismatches


def test_compare_detects_changed_text() -> None:
    chunks = _chunks()
    rows = [
        _row(
            chunk_no=c.chunk_no,
            text=c.text,
            content_hash=content_hash(c.text),
            char_start=c.char_start,
            char_end=c.char_end,
            heading_path=c.heading_path,
            context_prefix=c.context_prefix,
        )
        for c in chunks
    ]
    # ``sqlite3.Row`` неизменяем — подменяется элемент списка, а не поле строки.
    first = rows[0]
    rows[0] = _row(
        chunk_no=first["chunk_no"],
        text=first["text"] + " вставка",
        content_hash=first["content_hash"],
        char_start=first["char_start"],
        char_end=first["char_end"],
        heading_path=first["heading_path"],
        context_prefix=first["context_prefix"],
    )
    outcome = mce._compare(_doc(), rows, chunks)
    assert outcome.mismatches["text"] == 1
    # Хеш в БД — от **исходного** текста, и чанкер вернул тот же текст, поэтому хеш
    # сходится: расхождение обязано быть в тексте, а не размазано по обоим полям. Именно
    # это отличает прибор от сравнения двух хешей (которое промолчало бы о подмене текста
    # при сохранённом хеше).
    assert outcome.mismatches["content_hash"] == 0
    assert outcome.examples


def test_compare_detects_hash_mismatch_with_same_text() -> None:
    """Подменённый в БД хеш при верном тексте — тоже расхождение, и оно видно."""
    chunks = _chunks()
    rows = [
        _row(
            chunk_no=c.chunk_no,
            text=c.text,
            content_hash=content_hash(c.text),
            char_start=c.char_start,
            char_end=c.char_end,
            heading_path=c.heading_path,
            context_prefix=c.context_prefix,
        )
        for c in chunks
    ]
    first = rows[0]
    rows[0] = _row(
        chunk_no=first["chunk_no"],
        text=first["text"],
        content_hash="0" * 64,
        char_start=first["char_start"],
        char_end=first["char_end"],
        heading_path=first["heading_path"],
        context_prefix=first["context_prefix"],
    )
    outcome = mce._compare(_doc(), rows, chunks)
    assert outcome.mismatches["content_hash"] == 1
    assert outcome.mismatches["text"] == 0


def test_compare_detects_short_hash_mismatch_class() -> None:
    """Короткий ``Chunk.content_hash`` (16 hex) не сравнивается с авторитетным (64 hex).

    Первый прогон прибора дал «1702 из 1702 расхождений» ровно из-за этого: сравнивались
    два разных пространства идентификаторов. Тест держит границу.
    """
    chunks = _chunks()
    rows = [
        _row(
            chunk_no=c.chunk_no,
            text=c.text,
            content_hash=content_hash(c.text),
            char_start=c.char_start,
            char_end=c.char_end,
            heading_path=c.heading_path,
            context_prefix=c.context_prefix,
        )
        for c in chunks
    ]
    assert len(chunks[0].content_hash) == 16
    assert len(rows[0]["content_hash"]) == 64
    outcome = mce._compare(_doc(), rows, chunks)
    assert outcome.mismatches["content_hash"] == 0


def test_compare_detects_chunk_count_change() -> None:
    chunks = _chunks()
    rows = [
        _row(
            chunk_no=c.chunk_no,
            text=c.text,
            content_hash=content_hash(c.text),
            char_start=c.char_start,
            char_end=c.char_end,
            heading_path=c.heading_path,
            context_prefix=c.context_prefix,
        )
        for c in chunks[:-1]
    ]
    outcome = mce._compare(_doc(), rows, chunks)
    assert outcome.mismatches["chunk_count"] == 1


def test_compare_flags_markdown_suffix_suspicion() -> None:
    chunks = _chunks()
    rows = [
        _row(
            chunk_no=c.chunk_no,
            text=c.text,
            content_hash=content_hash(c.text),
            char_start=c.char_start,
            char_end=c.char_end,
            heading_path=c.heading_path,
            context_prefix=c.context_prefix,
        )
        for c in chunks
    ]
    outcome = mce._compare(
        _doc(url="https://h/readme.md", version="text-plain-2"), rows, chunks
    )
    assert outcome.mismatches["markdown_suffix_in_text_reader"] == 1


# --- run / main ------------------------------------------------------------------


def test_run_reports_equivalence_on_synthetic_db(tmp_path: Path) -> None:
    chunks = _chunks()
    db_path = _make_db(
        tmp_path,
        [("p:d1", "https://h/a", "trafilatura-html-6", chunks)],
    )
    report = mce.run(db_path)
    summary = report["summary"]
    assert summary["documents"] == 1
    assert summary["compared"] == 1
    assert summary["chunks_compared"] == len(chunks)
    assert summary["mismatches"] == {}
    assert summary["families"] == {"markdown": 1}


def test_run_skips_document_with_gap_and_names_reason(tmp_path: Path) -> None:
    """Документ с дырой в смещениях — пропуск с причиной, а не «совпало».

    Молчаливый пропуск читался бы как эквивалентность; именно поэтому причина входит в
    сводку отдельным счётчиком.
    """
    chunks = _chunks()
    db_path = _make_db(
        tmp_path,
        [("p:d1", "https://h/a", "trafilatura-html-6", chunks)],
    )
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE corpus_chunks SET char_start = char_start + 5 WHERE chunk_no = 1"
    )
    conn.commit()
    conn.close()
    report = mce.run(db_path)
    summary = report["summary"]
    assert summary["compared"] == 0
    assert summary["skipped"] == 1
    assert summary["skipped_reasons"] == {"gap_or_overlap": 1}


def test_main_returns_two_for_missing_db(tmp_path: Path, capsys) -> None:
    code = mce.main(["--db", str(tmp_path / "нет.db")])
    assert code == 2
    assert "ОШИБКА" in capsys.readouterr().err


def test_main_returns_zero_and_writes_json_when_equal(tmp_path: Path) -> None:
    chunks = _chunks()
    db_path = _make_db(
        tmp_path, [("p:d1", "https://h/a", "trafilatura-html-6", chunks)]
    )
    out = tmp_path / "out.json"
    code = mce.main(["--db", str(db_path), "--out-json", str(out)])
    assert code == 0
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["summary"]["mismatches"] == {}
    assert payload["conditions"]["chunk_size"] == 1500


def test_main_returns_one_when_mismatch(tmp_path: Path) -> None:
    chunks = _chunks()
    db_path = _make_db(
        tmp_path, [("p:d1", "https://h/a", "trafilatura-html-6", chunks)]
    )
    conn = sqlite3.connect(db_path)
    # Расхождение берётся в поле, которое не участвует в восстановлении текста: иначе
    # документ уйдёт в пропуск (``length_mismatch``), и тест проверял бы пропуск, а не
    # вердикт «есть расхождение».
    conn.execute(
        "UPDATE corpus_chunks SET heading_path = 'Чужой/Путь' WHERE chunk_no = 0"
    )
    conn.commit()
    conn.close()
    code = mce.main(["--db", str(db_path)])
    assert code == 1


def test_limit_restricts_documents(tmp_path: Path) -> None:
    first = _chunks()
    second = _chunks()
    db_path = _make_db(
        tmp_path,
        [
            ("p:a", "https://h/a", "trafilatura-html-6", first),
            ("p:b", "https://h/b", "trafilatura-html-6", second),
        ],
    )
    report = mce.run(db_path, limit=1)
    assert report["summary"]["documents"] == 1
    assert report["conditions"]["documents_in_run"] == 1


def test_document_without_chunks_is_named_not_silently_absent(tmp_path: Path) -> None:
    """Документ без кусков не попадает в поток — и это публикуется числом.

    Молчаливое исчезновение читалось бы как «сравнено и совпало»: ровно та ошибка, из-за
    которой прибор с пропуском выглядит зелёным.
    """
    chunks = _chunks()
    db_path = _make_db(
        tmp_path,
        [
            ("p:d1", "https://h/a", "trafilatura-html-6", chunks),
            ("p:d2", "https://h/empty", "trafilatura-html-6", []),
        ],
    )
    report = mce.run(db_path)
    summary = report["summary"]
    assert summary["documents"] == 1
    assert summary["documents_without_chunks"] == 1
    assert summary["mismatches"] == {}
