"""Тесты абзацного чанкера plain text (``text-para-4``).

Ключевое отличие от markdown-чанкера: у плоского текста нет заголовков, поэтому единица
нарезки — **абзац**, а не «единственный раздел, порезанный по размеру». Проверяется:

* куски — **точные срезы** нормализованного входа: ``text == normalized[start:end]``
  (P4), склейка всех ``text`` равна входу (P1), покрытие без дыр и перекрытий;
* абзац, который помещается, не режется никогда;
* абзацы сливаются до ``chunk_size`` (без лишней фрагментации);
* блок больше ``chunk_size`` режется по границам предложений (``.``/``!``/``?``/``;``),
  и только «бесконечное» предложение — жёстко;
* разделитель между кусками остаётся в **предыдущем** куске, а не переписывается
  пробелом: иначе ``join != вход``;
* ``heading_path`` всегда пуст (заголовков в тексте нет — выдумывать нельзя);
* ``context_prefix`` — первое предложение первого абзаца чанка, ограниченное по длине;
* порог фрагмента (``MIN_CHUNK_SIZE``): текст не выбрасывается, короткий хвост
  приклеивается к соседу (P1);
* набор **границ** не изменился там, где точный срез его не вынуждал (пин на фикстурах);
* детерминизм и стабильность версии (``chunker_version`` входит в контракт/``run_id``).

Нейтрален (§1.3).
"""

from __future__ import annotations

import itertools

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
from core.util.text_utils import sentence_terminated

_LONG = (
    "Первый абзац про установку. Он состоит из двух предложений.\n\n"
    "Второй абзац про настройку; здесь тоже два предложения.\n\n"
    "Третий абзац короткий."
)

_LONG_EN = (
    "Installation is described first. It takes two steps.\n\n"
    "Configuration follows; it has two sentences as well.\n\n"
    "The third paragraph is short."
)


def _normalized(text: str) -> str:
    """Ожидаемая нормализация входа (та же, что объявляет чанкер)."""
    text = text.removeprefix("\ufeff")
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _assert_exact(text: str, chunks: list) -> None:
    """P1/P4/покрытие: куски — непрерывные точные срезы нормализованного входа."""
    expected = _normalized(text)
    if not expected.strip():
        assert chunks == []
        return
    assert chunks, "непустой документ не дал ни одного чанка"
    assert "".join(chunk.text for chunk in chunks) == expected, "P1: склейка ≠ вход"
    for chunk in chunks:
        assert chunk.char_start is not None and chunk.char_end is not None
        assert chunk.text == expected[chunk.char_start : chunk.char_end], "P4: text ≠ срез"
        assert chunk.char_end - chunk.char_start == len(chunk.text)
        assert chunk.text.strip(), "кусок не должен быть пустым/пробельным"
    assert chunks[0].char_start == 0
    assert chunks[-1].char_end == len(expected)
    for previous, following in itertools.pairwise(chunks):
        assert previous.char_end == following.char_start, "покрытие с дырой/перекрытием"


def test_version_and_port_contract() -> None:
    assert TextChunker.version == TEXT_CHUNKER_VERSION == "text-para-4"
    assert isinstance(TextChunkerPort(), ChunkerPort)


def test_paragraphs_are_kept_whole() -> None:
    chunks = TextChunker(chunk_size=1500).chunk("doc", _LONG)

    # Три абзаца умещаются в один чанк и не дробятся.
    assert len(chunks) == 1
    assert chunks[0].text == _LONG
    _assert_exact(_LONG, chunks)


def test_heading_path_is_always_empty() -> None:
    chunks = TextChunker(chunk_size=60).chunk("doc", _LONG)

    assert chunks
    assert all(chunk.heading_path == "" for chunk in chunks)


def test_blocks_merge_up_to_chunk_size() -> None:
    text = "\n\n".join(f"Абзац номер {index}." for index in range(6))

    chunks = TextChunker(chunk_size=45).chunk("doc", text)

    assert len(chunks) > 1
    _assert_exact(text, chunks)
    for chunk in chunks:
        # Содержимое куска укладывается в бюджет; разделитель до следующего куска
        # принадлежит предыдущему, поэтому он виден в хвосте.
        assert len(chunk.text.strip()) <= 45
        assert chunk.text.rstrip().endswith(".")


def test_fitting_block_is_never_split() -> None:
    block = "A" * 40
    chunks = TextChunker(chunk_size=40).chunk("doc", block)

    assert [chunk.text for chunk in chunks] == [block]
    _assert_exact(block, chunks)


def test_oversized_block_falls_back_to_sentences() -> None:
    text = (
        "Первое предложение здесь. Второе предложение здесь! "
        "Третье предложение здесь? Четвёртое предложение здесь; "
        "Пятое предложение здесь."
    )

    chunks = TextChunker(chunk_size=55).chunk("doc", text)

    assert len(chunks) > 1
    _assert_exact(text, chunks)
    for chunk in chunks:
        # Разрез пришёл на границу предложения, а не на середину слова/фразы.
        assert len(chunk.text.strip()) <= 55
        assert chunk.text.rstrip().endswith((".", "!", "?", ";"))


def test_endless_sentence_is_hard_split() -> None:
    text = "слово " * 200  # ни одной точки — предложение «бесконечное»

    chunks = TextChunker(chunk_size=100).chunk("doc", text)

    assert len(chunks) > 1
    _assert_exact(text, chunks)
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
    # Два прогона дают одинаковые границы — детерминизм, а не «похожесть».
    assert [
        (chunk.char_start, chunk.char_end) for chunk in first
    ] == [(chunk.char_start, chunk.char_end) for chunk in second]


def test_port_path_chunks_extracted_doc() -> None:
    doc = ExtractedDoc(text=_LONG, content_hash="h", extractor_version="text-plain-2")

    chunks = TextChunkerPort().chunk(doc, "doc")

    assert len(chunks) == 1
    assert chunks[0].text == _LONG
    _assert_exact(_LONG, chunks)


def test_empty_text_produces_no_chunks() -> None:
    assert TextChunker().chunk("doc", "   \n\n  \n") == []


def test_single_character_document_is_one_exact_chunk() -> None:
    """Документ из одного символа не выбрасывается: P1 запрещает терять вход."""
    chunks = TextChunker().chunk("doc", "Я")

    assert len(chunks) == 1
    assert chunks[0].text == "Я"
    assert (chunks[0].char_start, chunks[0].char_end) == (0, 1)
    _assert_exact("Я", chunks)


# ---------------------------------------------------------------------------
# точные срезы: языки, края, нормализация
# ---------------------------------------------------------------------------


def test_ru_paragraphs_are_exact_slices() -> None:
    chunks = TextChunker(chunk_size=70).chunk("doc", _LONG)

    assert len(chunks) > 1
    _assert_exact(_LONG, chunks)
    assert all("\r" not in chunk.text for chunk in chunks)


def test_en_paragraphs_are_exact_slices() -> None:
    chunks = TextChunker(chunk_size=70).chunk("doc", _LONG_EN)

    assert len(chunks) > 1
    _assert_exact(_LONG_EN, chunks)


def test_mixed_ru_en_cjk_is_exact_slices() -> None:
    text = (
        "Первый абзац тут. Второй абзац здесь.\n\n"
        "中文段落没有空格。English paragraph here.\n\n"
        "Хвост."
    )

    chunks = TextChunker(chunk_size=45).chunk("doc", text)

    assert len(chunks) > 1
    _assert_exact(text, chunks)


def test_long_word_without_spaces_is_hard_split_without_loss() -> None:
    text = "x" * 5000

    chunks = TextChunker(chunk_size=400).chunk("doc", text)

    assert len(chunks) > 1
    _assert_exact(text, chunks)
    assert all(len(chunk.text) <= 400 for chunk in chunks)


def test_leading_and_trailing_newlines_stay_in_the_document() -> None:
    """Ведущие/хвостовые переводы строк входят в крайние куски, а не отбрасываются."""
    text = "\n\n  Абзац один. Абзац два.\n\n\nАбзац три.   \n"

    chunks = TextChunker(chunk_size=40).chunk("doc", text)

    _assert_exact(text, chunks)
    assert chunks[0].char_start == 0
    assert chunks[0].text.startswith("\n\n")
    assert chunks[-1].char_end == len(text)
    assert chunks[-1].text.endswith("   \n")


def test_crlf_is_normalized_and_offsets_point_at_normalized_text() -> None:
    text = "Первый абзац.\r\n\r\nВторой абзац про настройку.\r\n"

    chunks = TextChunker(chunk_size=1500).chunk("doc", text)

    expected = _normalized(text)
    assert expected == "Первый абзац.\n\nВторой абзац про настройку.\n"
    assert len(chunks) == 1
    assert chunks[0].text == expected
    assert chunks[0].char_end == len(expected)
    _assert_exact(text, chunks)


def test_empty_document_between_blank_lines_gives_no_chunks() -> None:
    assert TextChunker(chunk_size=40).chunk("doc", "\n\n   \n\n\t\n") == []


# ---------------------------------------------------------------------------
# порог фрагмента (C6): не выбрасывает, а приклеивает
# ---------------------------------------------------------------------------


def test_min_chunk_size_default_is_declared() -> None:
    """Порог объявлен константой и действует по умолчанию (C6)."""
    assert MIN_CHUNK_SIZE == 20
    assert TextChunker().min_chunk_size == MIN_CHUNK_SIZE


def test_short_document_is_kept_as_one_chunk_not_dropped() -> None:
    """Короткий документ — объявленная единица документа, а не «мусор»: P1.

    Раньше кусок короче порога просто выбрасывался, и склейка не могла вернуть вход.
    Теперь он остаётся чанком: число таких («tiny») публикует харнесс замера, а не
    молчаливая потеря.
    """
    text = "|\n\nЛистов 81"

    chunks = TextChunker().chunk("doc", text)

    assert [chunk.text for chunk in chunks] == [text]
    _assert_exact(text, chunks)


def test_short_tail_is_absorbed_into_previous_chunk() -> None:
    """Хвост короче порога приклеивается к предыдущему куску — без потери символов."""
    text = "А" * 45 + "\n\nХвост."

    chunks = TextChunker(chunk_size=50, min_chunk_size=20).chunk("doc", text)

    assert len(chunks) == 1
    assert chunks[0].text == text
    _assert_exact(text, chunks)


def test_min_chunk_size_must_fit_chunk_size() -> None:
    with pytest.raises(ValueError):
        TextChunker(chunk_size=20, min_chunk_size=20)
    with pytest.raises(ValueError):
        TextChunker(min_chunk_size=-1)


# ---------------------------------------------------------------------------
# CJK: границы без пробелов и сохранение разделителей
# ---------------------------------------------------------------------------


def test_cjk_sentences_are_cut_without_whitespace() -> None:
    """Полноширинный терминатор — граница сам по себе: в CJK после него пробела нет.

    Правило «терминатор + пробел» не находило здесь ни одной границы, и абзац резался
    по номеру символа: чанк кончался посреди фразы, а цитата — посреди слова.
    """
    text = "".join(f"第{i}句话说明这个功能的行为与限制。" for i in range(40))
    chunks = TextChunker(chunk_size=200).chunk("doc", text)

    assert len(chunks) > 1
    _assert_exact(text, chunks)
    assert all(sentence_terminated(chunk.text) for chunk in chunks)
    assert max(len(chunk.text) for chunk in chunks) <= 200


def test_separators_are_preserved_exactly() -> None:
    """Склейка чанков возвращает вход: разделитель принадлежит тексту, а не склейке.

    Иначе чанкер дописывал бы в китайский текст пробелы, которых в нём не было (и
    ``content_hash`` переставал бы соответствовать источнику).
    """
    zh = "他说「你好。」然后走了。第二句在这里。" * 12
    ru = "Первое предложение тут. Второе предложение тоже. Третье." * 8

    zh_chunks = TextChunker(chunk_size=120).chunk("d", zh)
    ru_chunks = TextChunker(chunk_size=100).chunk("d", ru)

    assert "".join(c.text for c in zh_chunks) == zh
    assert "".join(c.text for c in ru_chunks) == ru
    _assert_exact(zh, zh_chunks)
    _assert_exact(ru, ru_chunks)
    assert all(sentence_terminated(c.text) for c in zh_chunks)
    assert all(sentence_terminated(c.text) for c in ru_chunks)
    assert not any(c.text[0] in "」』，、：。！？；" for c in zh_chunks)


def test_clause_boundaries_are_the_second_level() -> None:
    """Предложение больше бюджета режется по клаузам, а не по номеру символа."""
    text = "这个功能提供了一个非常好的接口，" * 20 + "并且没有任何问题。"
    chunks = TextChunker(chunk_size=80).chunk("doc", text)

    _assert_exact(text, chunks)
    # Кусок может превысить бюджет только на объявленную склейку короткого хвоста.
    assert max(len(chunk.text) for chunk in chunks) <= 80 + MIN_CHUNK_SIZE
    assert all(
        chunk.text.rstrip().endswith(("，", "。")) for chunk in chunks
    )


def test_hard_cut_keeps_grapheme_clusters_whole() -> None:
    """Нет пунктуации вовсе — жёсткий разрез, но не внутри графемного кластера."""
    blob = "汉" * 500
    chunks = TextChunker(chunk_size=100).chunk("doc", blob)

    assert "".join(chunk.text for chunk in chunks) == blob
    assert all(len(chunk.text) <= 100 for chunk in chunks)
    _assert_exact(blob, chunks)


# ---------------------------------------------------------------------------
# границы разреза: пины (не порождение)
# ---------------------------------------------------------------------------

#: Фикстуры и **ожидаемые** границы ``(char_start, char_end)``. ``min_chunk_size=0``
#: намеренно: порог — отдельное объявленное правило (см. тесты выше), здесь
#: фиксируется только геометрия разреза. Значения — свойство прежних правил
#: (``text-para-3``), а не вывод этого кода: на фикстурах с разделителем ровно
#: ``\n\n`` точный срез обязан был сохранить те же позиции начал кусков.
_BOUNDARY_PINS = [
    (
        "Абзац один. Абзац два. Абзац три. Абзац четыре. Абзац пять. Абзац шесть.",
        45,
        [(0, 34), (34, 72)],
    ),
    (
        "Первый абзац тут.\n\nВторой абзац тут.\n\nТретий абзац тут.\n\n"
        "Четвёртый абзац тут.\n\nПятый абзац тут.",
        40,
        [(0, 38), (38, 79), (79, 95)],
    ),
    (
        "".join(f"第{i}句话说明这个功能。" for i in range(12)),
        50,
        [(0, 44), (44, 88), (88, 134)],
    ),
]


@pytest.mark.parametrize(("text", "chunk_size", "expected"), _BOUNDARY_PINS)
def test_cut_boundaries_are_pinned(
    text: str, chunk_size: int, expected: list[tuple[int, int]]
) -> None:
    """Набор границ не поехал там, где точный срез его не вынуждал.

    Пин, а не порождение: ожидаемые смещения выписаны литералами. Если правка правил
    сдвинет границу на фикстуре, где разделитель и так ровно ``\\n\\n``, тест обязан
    упасть — иначе «набор границ не изменился» осталось бы заявлением.
    """
    chunks = TextChunker(chunk_size=chunk_size, min_chunk_size=0).chunk("doc", text)

    assert [(chunk.char_start, chunk.char_end) for chunk in chunks] == expected
    _assert_exact(text, chunks)


def test_pinned_boundaries_match_old_stripped_chunk_texts() -> None:
    """Тот же пин со стороны содержания: прежние куски = ``strip()`` новых срезов.

    Это и есть смысл «границы не изменились»: старый чанкер резал в тех же точках, но
    терял краевые пробелы и разделители. Здесь проверяется совпадение **содержимого**,
    а смещения — в пине выше, поэтому тест не может пройти на другой нарезке.
    """
    expected_texts = [
        [
            "Абзац один. Абзац два. Абзац три.",
            "Абзац четыре. Абзац пять. Абзац шесть.",
        ],
        [
            "Первый абзац тут.\n\nВторой абзац тут.",
            "Третий абзац тут.\n\nЧетвёртый абзац тут.",
            "Пятый абзац тут.",
        ],
        [
            "第0句话说明这个功能。第1句话说明这个功能。第2句话说明这个功能。"
            "第3句话说明这个功能。",
            "第4句话说明这个功能。第5句话说明这个功能。第6句话说明这个功能。"
            "第7句话说明这个功能。",
            "第8句话说明这个功能。第9句话说明这个功能。第10句话说明这个功能。"
            "第11句话说明这个功能。",
        ],
    ]

    for (text, chunk_size, _expected), texts in zip(
        _BOUNDARY_PINS, expected_texts, strict=True
    ):
        chunks = TextChunker(chunk_size=chunk_size, min_chunk_size=0).chunk("doc", text)
        assert [chunk.text.strip() for chunk in chunks] == texts