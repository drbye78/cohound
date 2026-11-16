"""Маршрутизация композита чанкеров по семейству формата.

``FamilyChunker`` — **один** ``ChunkerPort`` для всего корпуса: семейство ``text`` уходит
в абзацный :class:`~core.adapters.chunker_text.TextChunker`, markdown/HTML/EPUB — в
:class:`~core.adapters.chunker_markdown.MarkdownChunker`. Тест фиксирует именно выбор:
если маршрут поедет, точные срезы ``text-para-4`` перестанут попадать в корпус незаметно
(тип чанка один и тот же, отличается только форма). Поэтому различие проверяется по
свойствам результата: у текстового семейства ``heading_path == ""`` и смещения — точные
срезы, у markdown — путь заголовка и своя версия.

Нейтрален (§1.3).
"""

from __future__ import annotations

import logging

import pytest

from core.adapters import chunker_family
from core.adapters.chunker_family import (
    FamilyChunker,
    composite_chunker_version,
    composite_chunking_spec,
    declared_token_limiter,
    embed_input_text,
)
from core.adapters.chunker_markdown import MarkdownChunker, MarkdownChunkerPort
from core.adapters.chunker_text import TextChunker
from core.adapters.token_limit import TokenLimiter
from core.ports.types import Chunk, ExtractedDoc
from core.util import embed_input

_TEXT = "# not a heading\n\nПервый абзац достаточно длинный.\n\nВторой абзац тоже длинный."
_MARKDOWN = "# Раздел\n\n## Подраздел\n\nТекст раздела достаточно длинный."


class _CharCounter:
    """Фейковый счётчик: токен — один символ (без ``transformers`` и без сети)."""

    spec = "fake-char@1"

    def token_ends(self, text: str) -> tuple[int, ...]:
        return tuple(range(1, len(text) + 1))


def _doc(text: str, family: str | None) -> ExtractedDoc:
    return ExtractedDoc(text=text, content_hash="h", extractor_version="v", family=family)


def test_text_family_goes_to_text_chunker() -> None:
    chunks = FamilyChunker().chunk(_doc(_TEXT, "text"), "doc")

    assert chunks
    assert all(chunk.heading_path == "" for chunk in chunks)
    assert all(chunk.char_start is not None and chunk.char_end is not None for chunk in chunks)
    # Семейство ``text`` не разбирает markdown: решётки остаются текстом первого чанка.
    assert chunks[0].text.startswith("# not a heading")


def test_markdown_family_goes_to_markdown_chunker() -> None:
    chunks = FamilyChunker().chunk(_doc(_MARKDOWN, "markdown"), "doc")

    assert chunks
    assert any(chunk.heading_path for chunk in chunks)
    assert any("# Раздел" in chunk.text for chunk in chunks)


def test_epub_and_none_families_go_to_markdown_chunker() -> None:
    for family in ("epub", None):
        chunks = FamilyChunker().chunk(_doc(_MARKDOWN, family), "doc")

        assert any(chunk.heading_path for chunk in chunks), family


def test_markdown_family_does_not_use_text_chunker() -> None:
    """Точные смещения есть у обоих, поэтому маршрут различается структурой, не полем."""
    text_doc = FamilyChunker().chunk(_doc(_TEXT, "text"), "doc")
    md_doc = FamilyChunker().chunk(_doc(_TEXT, "markdown"), "doc")

    assert all(chunk.heading_path == "" for chunk in text_doc)
    assert any("# not a heading" in chunk.text for chunk in text_doc)
    assert any(chunk.heading_path for chunk in md_doc)


def test_version_is_composite_of_both_chunkers() -> None:
    assert composite_chunker_version() == f"{MarkdownChunker.version}+{TextChunker.version}"
    assert FamilyChunker().version == composite_chunker_version()
    assert TextChunker.version in FamilyChunker().version


def test_chunking_spec_declares_text_geometry() -> None:
    spec = composite_chunking_spec()

    assert spec["text"] == {
        "chunk_size": TextChunker().chunk_size,
        "min_chunk_size": TextChunker().min_chunk_size,
    }
    assert spec["embed_input"] == ["text"]


# --------------------------------------------------------------------------- #
# Вход эмбеддера (O19): объявленный состав исполняется, дефолт — байт-в-байт text
# --------------------------------------------------------------------------- #


def test_embed_input_is_exactly_the_text_by_default() -> None:
    """При составе ``("text",)`` вход эмбеддера **байт-в-байт** равен ``text``.

    На этом свойстве держится неизменность ``embed_cache`` и дедупа: ключ кэша —
    SHA-256 входа, а ``content_hash`` чанка — SHA-256 его текста. Разойдись они на один
    байт — объявленная ось молча инвалидировала бы весь кэш при выключенном значении.
    """
    chunk = Chunk(
        chunk_id="c1",
        chunk_no=0,
        text="текст куска",
        heading_path="Раздел > Подраздел",
        context_prefix="шапка",
        content_hash="h",
    )
    assert embed_input_text(chunk) == chunk.text


def test_embed_input_follows_the_declared_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    """Состав входа — функция :data:`EMBED_INPUT_FIELDS`, а не ``chunk.text``."""
    # Состав живёт в ``core.util.embed_input`` (его исполняет слой ``pipeline``), а
    # ``chunker_family`` его реэкспортирует только для объявления в ``chunking_spec``:
    # подменять надо источник, иначе тест проверял бы копию.
    monkeypatch.setattr(
        embed_input, "EMBED_INPUT_FIELDS", ("heading_path", "context_prefix", "text")
    )
    chunk = Chunk(
        chunk_id="c1",
        chunk_no=0,
        text="текст",
        heading_path="Раздел",
        context_prefix="шапка",
        content_hash="h",
    )
    assert embed_input_text(chunk) == "Раздел\n\nшапка\n\nтекст"


def test_embed_input_skips_empty_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    """Пустое поле не оставляет разделителя: вектор зависит от содержания, не от формы."""
    monkeypatch.setattr(embed_input, "EMBED_INPUT_FIELDS", ("heading_path", "text"))
    chunk = Chunk(
        chunk_id="c1",
        chunk_no=0,
        text="текст",
        heading_path="   ",
        context_prefix="",
        content_hash="h",
    )
    assert embed_input_text(chunk) == "текст"


# --------------------------------------------------------------------------- #
# Токенный ограничитель (O21): выключенное условие ничего не меняет
# --------------------------------------------------------------------------- #


def test_token_limit_is_off_by_default() -> None:
    """Пока ограничитель не объявлен, его нет ни в версии, ни в спеке."""
    assert chunker_family.TOKEN_LIMIT is None
    assert declared_token_limiter() is None
    assert "token_limit" not in composite_chunking_spec()
    assert "+token-limit" not in composite_chunker_version()


def test_disabled_limiter_keeps_chunks_and_spec_identical() -> None:
    """Выключенный ограничитель — байт-в-байт прежний корпус и прежний контракт.

    Иначе включение/выключение условия меняло бы ``run_id``, не меняя корпуса: ложный
    ``execution_change``. Проверяется сравнением с чанкером семейства напрямую — тот же
    вход, тот же выход.
    """
    doc = _doc(_MARKDOWN * 3, "markdown")

    composite = FamilyChunker().chunk(doc, "doc")
    direct = MarkdownChunkerPort().chunk(doc, "doc")

    assert composite == direct
    assert "token_limit" not in composite_chunking_spec()
    assert "token_limit_model" not in composite_chunking_spec()


# --------------------------------------------------------------------------- #
# Токенный ограничитель: объявленное условие исполняется
# --------------------------------------------------------------------------- #


def test_declared_limiter_limits_chunks_and_versions_the_chunker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(chunker_family, "TOKEN_LIMIT", (200, "fake/char@1"))
    limiter = TokenLimiter(_CharCounter(), 200, min_tail_tokens=0)

    chunker = FamilyChunker(token_limiter=limiter)
    doc = _doc(_MARKDOWN * 4, "markdown")
    chunks = chunker.chunk(doc, "doc")

    assert len(chunks) > 1
    assert all(len(chunk.text) <= 200 for chunk in chunks)
    # P1 и точные срезы держатся и после ограничителя.
    assert "".join(chunk.text for chunk in chunks) == _MARKDOWN * 4
    for chunk in chunks:
        assert chunk.char_start is not None and chunk.char_end is not None
        assert (_MARKDOWN * 4)[chunk.char_start : chunk.char_end] == chunk.text
    # Версия и спека называют ограничитель: это условие измерения, а не деталь.
    assert chunker.version == f"{composite_chunker_version()}"
    assert chunker.version.endswith("+token-limit-1:200")
    spec = composite_chunking_spec()
    assert spec["token_limit"] == 200
    assert spec["token_limit_model"] == "fake/char@1"
    assert spec["token_limit_policy"] == "token-limit-1"


def test_declared_limiter_without_counter_is_an_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Объявленное условие без исполнителя — отказ, а не тихая настройка по умолчанию."""
    monkeypatch.setattr(chunker_family, "TOKEN_LIMIT", (200, "fake/char@1"))

    with pytest.raises(ValueError):
        FamilyChunker()


def test_injected_limiter_without_declaration_warns(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Обратный случай видим: корпус меняется, а версия/спека этого не называют."""
    monkeypatch.setattr(chunker_family, "TOKEN_LIMIT", None)

    with caplog.at_level(logging.WARNING, logger="core.adapters.chunker_family"):
        FamilyChunker(token_limiter=TokenLimiter(_CharCounter(), 200))

    assert any("token_limit_undeclared" in record.message for record in caplog.records)


def test_limiter_application_is_logged(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Молчаливого применения быть не должно: отчёт ограничителя уходит в лог."""
    monkeypatch.setattr(chunker_family, "TOKEN_LIMIT", (200, "fake/char@1"))
    chunker = FamilyChunker(token_limiter=TokenLimiter(_CharCounter(), 200, min_tail_tokens=0))

    with caplog.at_level(logging.INFO, logger="core.adapters.chunker_family"):
        chunker.chunk(_doc(_MARKDOWN * 4, "markdown"), "doc")

    message = next(
        record.message for record in caplog.records if record.message.startswith("token_limit:")
    )
    for key in (
        "chunks_in=",
        "chunks_out=",
        "split=",
        "absorbed_tails=",
        "unsplittable=",
        "over_limit=",
        "max_tokens_after=",
    ):
        assert key in message


def test_declared_token_limiter_builds_counter_from_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Разбор ``model_id@revision`` — по последнему ``@``: провенанс условия не теряется."""
    monkeypatch.setattr(chunker_family, "TOKEN_LIMIT", (512, "org/model@abc123"))

    limiter = declared_token_limiter()

    assert limiter is not None
    assert limiter.limit == 512
    counter = limiter._counter  # noqa: SLF001 - проверяется провенанс счётчика
    assert counter.spec == "org/model@abc123"


def test_declared_token_limiter_without_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(chunker_family, "TOKEN_LIMIT", (512, "org/model"))

    limiter = declared_token_limiter()

    assert limiter is not None
    assert limiter._counter.spec == "org/model"  # noqa: SLF001 - пин не выдумывается


def test_token_limiter_does_not_touch_short_documents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Короткий документ проходит ограничитель без изменения состава чанков."""
    monkeypatch.setattr(chunker_family, "TOKEN_LIMIT", (10_000, "fake/char@1"))
    chunker = FamilyChunker(token_limiter=TokenLimiter(_CharCounter(), 10_000))

    limited = chunker.chunk(_doc(_MARKDOWN, "markdown"), "doc")
    plain = MarkdownChunkerPort().chunk(_doc(_MARKDOWN, "markdown"), "doc")

    assert [chunk.text for chunk in limited] == [chunk.text for chunk in plain]
    assert all(isinstance(chunk, Chunk) for chunk in limited)


def test_text_family_also_goes_through_limiter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ограничитель стоит после выбора семейства — он общий, а не markdown-only."""
    monkeypatch.setattr(chunker_family, "TOKEN_LIMIT", (100, "fake/char@1"))
    chunker = FamilyChunker(token_limiter=TokenLimiter(_CharCounter(), 100, min_tail_tokens=0))
    text = _TEXT * 6

    chunks = chunker.chunk(_doc(text, "text"), "doc")

    assert len(chunks) > 1
    assert all(len(chunk.text) <= 100 for chunk in chunks)
    assert "".join(chunk.text for chunk in chunks) == text
    assert all(chunk.heading_path == "" for chunk in chunks)


def test_versions_of_direct_chunkers_unchanged_by_limiter() -> None:
    """Ограничитель — надстройка: версии самих чанкеров он не переписывает."""
    assert MarkdownChunker.version.startswith("md-ast-")
    assert TextChunker.version == "text-para-4"
