"""Тесты абзацного чанкера plain text (``text-para-2``).

Ключевое отличие от markdown-чанкера: у плоского текста нет заголовков, поэтому единица
нарезки — **абзац**, а не «единственный раздел, порезанный по размеру». Проверяется:

* абзац, который помещается, не режется никогда;
* абзацы сливаются до ``chunk_size`` (без лишней фрагментации);
* блок больше ``chunk_size`` режется по границам предложений (``.``/``!``/``?``/``;``),
  и только «бесконечное» предложение — жёстко;
* ``heading_path`` всегда пуст (заголовков в тексте нет — выдумывать нельзя);
* ``context_prefix`` — первое предложение первого абзаца чанка, ограниченное по длине;
* порог фрагмента (``MIN_CHUNK_SIZE``): кусок короче в индекс не попадает (C6);
* детерминизм и стабильность версии (``chunker_version`` входит в контракт/``run_id``).

Нейтрален (§1.3).
"""

from __future__ import annotations

import pytest

from core.adapters.chunker_text import (
    CONTEXT_PREFIX_MAX,
    MIN_CHUNK_SIZE,
    TEXT_CHUNKER_VERSION,
    TextChunker,
    TextChunkerPort,
)
from core.ports.chunker import ChunkerPort
from core.ports.types import ExtractedDoc

_LONG = (
    "Первый абзац про установку. Он состоит из двух предложений.\n\n"
    "Второй абзац про настройку; здесь тоже два предложения.\n\n"
    "Третий абзац короткий."
)


def test_version_and_port_contract() -> None:
    assert TextChunker.version == TEXT_CHUNKER_VERSION == "text-para-2"
    assert isinstance(TextChunkerPort(), ChunkerPort)


def test_paragraphs_are_kept_whole() -> None:
    chunks = TextChunker(chunk_size=1500).chunk("doc", _LONG)

    # Три абзаца умещаются в один чанк и не дробятся.
    assert len(chunks) == 1
    assert chunks[0].text == _LONG


def test_heading_path_is_always_empty() -> None:
    chunks = TextChunker(chunk_size=60).chunk("doc", _LONG)

    assert chunks
    assert all(chunk.heading_path == "" for chunk in chunks)


def test_blocks_merge_up_to_chunk_size() -> None:
    text = "\n\n".join(f"Абзац номер {index}." for index in range(6))

    chunks = TextChunker(chunk_size=45).chunk("doc", text)

    assert len(chunks) > 1
    for chunk in chunks:
        assert len(chunk.text) <= 45
        # Ни один абзац не разрезан: каждый чанк кончается точкой абзаца.
        assert chunk.text.endswith(".")


def test_fitting_block_is_never_split() -> None:
    block = "A" * 40
    chunks = TextChunker(chunk_size=40).chunk("doc", block)

    assert [chunk.text for chunk in chunks] == [block]


def test_oversized_block_falls_back_to_sentences() -> None:
    text = (
        "Первое предложение здесь. Второе предложение здесь! "
        "Третье предложение здесь? Четвёртое предложение здесь; "
        "Пятое предложение здесь."
    )

    chunks = TextChunker(chunk_size=55).chunk("doc", text)

    assert len(chunks) > 1
    for chunk in chunks:
        # Разрез пришёл на границу предложения, а не на середину слова/фразы.
        assert chunk.text.endswith((".", "!", "?", ";"))
        assert len(chunk.text) <= 55


def test_endless_sentence_is_hard_split() -> None:
    text = "слово " * 200  # ни одной точки — предложение «бесконечное»

    chunks = TextChunker(chunk_size=100).chunk("doc", text)

    assert len(chunks) > 1
    assert all(len(chunk.text) <= 100 for chunk in chunks)
    # Жёсткий разрез идёт по пробелам: ни одного разрубленного слова.
    words = set(text.split())
    assert all(set(chunk.text.split()) <= words for chunk in chunks)


def test_context_prefix_is_first_sentence_of_leading_block() -> None:
    text = "Заголовок раздела без точки. Второе предложение.\n\nДругой абзац."

    chunks = TextChunker(chunk_size=1500).chunk("doc", text)

    assert chunks[0].context_prefix == "Заголовок раздела без точки."


def test_context_prefix_is_bounded() -> None:
    text = "С" * (CONTEXT_PREFIX_MAX * 2)

    chunks = TextChunker(chunk_size=1500).chunk("doc", text)

    assert len(chunks[0].context_prefix) <= CONTEXT_PREFIX_MAX


def test_chunk_ids_and_hashes_are_deterministic() -> None:
    first = TextChunker(chunk_size=60).chunk("doc", _LONG)
    second = TextChunker(chunk_size=60).chunk("doc", _LONG)

    assert first == second
    assert [chunk.chunk_id for chunk in first] == [
        f"doc#{chunk.chunk_no}" for chunk in first
    ]
    assert [chunk.chunk_no for chunk in first] == list(range(len(first)))


def test_port_path_chunks_extracted_doc() -> None:
    doc = ExtractedDoc(text=_LONG, content_hash="h", extractor_version="text-plain-1")

    chunks = TextChunkerPort().chunk(doc, "doc")

    assert len(chunks) == 1
    assert chunks[0].text == _LONG


def test_empty_text_produces_no_chunks() -> None:
    assert TextChunker().chunk("doc", "   \n\n  \n") == []


# ---------------------------------------------------------------------------
# порог фрагмента (C6)
# ---------------------------------------------------------------------------


def test_min_chunk_size_default_is_declared() -> None:
    """Порог объявлен константой и действует по умолчанию (C6)."""
    assert MIN_CHUNK_SIZE == 20
    assert TextChunker().min_chunk_size == MIN_CHUNK_SIZE


def test_fragment_below_min_chunk_size_is_not_indexed() -> None:
    """Обломок разметки не попадает в индекс: как ``source_id`` он ложен (C6/I22)."""
    assert TextChunker().chunk("doc", "|\n\nЛистов 81") == []
    assert [c.text for c in TextChunker().chunk("doc", _LONG)] == [_LONG]


def test_min_chunk_size_must_fit_chunk_size() -> None:
    with pytest.raises(ValueError):
        TextChunker(chunk_size=20, min_chunk_size=20)
    with pytest.raises(ValueError):
        TextChunker(min_chunk_size=-1)
