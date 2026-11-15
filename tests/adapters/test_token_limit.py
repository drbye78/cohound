"""Тесты токенного ограничителя чанка (O21): ограничитель поверх структурной нарезки.

Ограничитель не режет «по N токенов» — он **делит** уже нарезанные по структуре чанки и
обязан сохранить то, ради чего чанкер существует: кусок остаётся **точным срезом**
документа, склейка кусков равна склейке входа (P1), ``heading_path``/``context_prefix``
переходят к продолжению от родителя.

Токенизатор в тестах — фейковый счётчик (без ``transformers`` и без сети): тест
ограничителя проверяет **правила** ограничителя, а не загрузку весов. Один фейк считает
токеном один символ (тогда «токены» и «символы» совпадают, и ожидания читаются глазами),
второй воспроизводит граничный эффект настоящего токенизатора (у префикса токенов больше,
чем у того же текста внутри целого) — им проверяется гарантия лимита.

Тесты, которые проверяют **строгий** лимит, передают ``min_tail_tokens=0``: иначе
приклеенный хвост (объявленный компромисс, см. ``MIN_TAIL_TOKENS``) законно выводит
последний кусок за бюджет, и проверка «все куски в пределах лимита» стала бы неправдой.

Нейтрален (§1.3): ``BAAI/bge-m3`` встречается только как идентификатор модели в проверке
конвенции ``model_id@revision``.
"""

from __future__ import annotations

import hashlib
import itertools
import sys
import types

import pytest

from core.adapters.token_limit import (
    MIN_TAIL_TOKENS,
    TOKEN_LIMIT_FLOOR_SHARE,
    TOKEN_LIMIT_POLICY,
    HfTokenCounter,
    LimitReport,
    TokenizerUnavailable,
    TokenLimiter,
)
from core.ports.tokenizer import TokenCounterPort
from core.ports.types import Chunk

_LONG_TEXT = (
    "Первый абзац про установку. Он состоит из двух предложений.\n\n"
    "Второй абзац про настройку; здесь тоже два предложения.\n\n"
    "Третий абзац короткий."
)


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _chunk(
    text: str,
    *,
    no: int = 0,
    start: int = 0,
    document: str = "doc",
    heading: str = "Раздел",
    context: str = "контекст",
) -> Chunk:
    """Чанк как его отдаёт семейный чанкер: точный срез ``text`` начиная с ``start``."""
    return Chunk(
        chunk_id=f"{document}#{no}",
        chunk_no=no,
        text=text,
        heading_path=heading,
        context_prefix=context,
        content_hash=_hash(text),
        char_start=start,
        char_end=start + len(text),
    )


class _StepCounter:
    """Фейковый счётчик: токен — ``step`` символов (пустые отрезки не порождаются).

    При ``step=1`` число токенов равно длине текста — так ожидания в тестах видны без
    арифметики. Реализует порт структурно (:class:`TokenCounterPort`).
    """

    def __init__(self, step: int = 1, spec: str = "fake-step@1") -> None:
        if step <= 0:
            raise ValueError("step обязан быть положительным")
        self._step = step
        self._spec = spec

    @property
    def spec(self) -> str:
        return self._spec

    def token_ends(self, text: str) -> tuple[int, ...]:
        if not text:
            return ()
        ends = list(range(self._step, len(text) + 1, self._step))
        if ends[-1] != len(text):
            ends.append(len(text))
        return tuple(ends)


class _BoundaryCounter:
    """Счётчик с граничным эффектом: текст, кончающийся пробелом, получает лишний токен.

    Так ведёт себя настоящий BPE-токенизатор: разбиение префикса не равно разбиению того
    же текста внутри целого — на стыке появляется дополнительное подслово. Без шага
    «отодвинуть рез» ограничитель выдал бы кусок сверх лимита.
    """

    spec = "fake-boundary@1"

    def __init__(self, step: int = 1) -> None:
        self._step = step

    def token_ends(self, text: str) -> tuple[int, ...]:
        if not text:
            return ()
        ends = list(range(self._step, len(text) + 1, self._step))
        if ends[-1] != len(text):
            ends.append(len(text))
        if text[-1].isspace() and len(text) >= 2 and ends[-1] != len(text) - 1:
            ends.insert(len(ends) - 1, len(text) - 1)
        return tuple(ends)


def _limiter(limit: int, counter: TokenCounterPort | None = None, **kwargs: int) -> TokenLimiter:
    return TokenLimiter(counter if counter is not None else _StepCounter(), limit, **kwargs)


# --------------------------------------------------------------------------- #
# Базовое: короткий чанк не трогается
# --------------------------------------------------------------------------- #


def test_short_chunk_survives_unchanged() -> None:
    chunk = _chunk("короткий текст", no=3)

    out, report = _limiter(100).apply([chunk])

    assert [piece.text for piece in out] == ["короткий текст"]
    assert out[0].char_start == chunk.char_start
    assert out[0].char_end == chunk.char_end
    assert out[0].chunk_id == "doc#0"  # нумерация сквозная по выходу, начиная с 0
    assert report == LimitReport(
        chunks_in=1,
        chunks_out=1,
        split_chunks=0,
        absorbed_tails=0,
        unsplittable=0,
        over_limit=0,
        max_tokens_after=len("короткий текст"),
    )


def test_empty_input_gives_empty_output() -> None:
    out, report = _limiter(10).apply([])

    assert out == []
    assert report == LimitReport()


# --------------------------------------------------------------------------- #
# Лимит, P1 и точные срезы
# --------------------------------------------------------------------------- #


def test_long_chunk_is_split_within_limit() -> None:
    text = _LONG_TEXT * 6

    out, report = _limiter(80, min_tail_tokens=0).apply([_chunk(text)])

    assert report.chunks_in == 1
    assert report.chunks_out == len(out) > 1
    assert report.split_chunks == 1
    assert report.over_limit == 0
    assert report.max_tokens_after <= 80
    for piece in out:
        assert len(piece.text) <= 80, piece.text[:40]


def test_p1_and_exact_slices_hold() -> None:
    text = _LONG_TEXT * 6
    chunk = _chunk(text, start=0)

    out, _ = _limiter(80).apply([chunk])

    # P1: склейка кусков равна входу — ограничитель режет, а не переписывает.
    assert "".join(piece.text for piece in out) == text
    # Точный срез: кусок стоит в документе там, где ему место (I22).
    for piece in out:
        assert piece.char_start is not None and piece.char_end is not None
        assert text[piece.char_start : piece.char_end] == piece.text


def test_slices_are_relative_to_parent_offset() -> None:
    """Родитель может стоять в середине документа: смещения считаются от его ``char_start``."""
    document = "ПРЕФИКС " + _LONG_TEXT * 4
    offset = len("ПРЕФИКС ")
    chunk = _chunk(document[offset:], start=offset)

    out, _ = _limiter(70).apply([chunk])

    assert len(out) > 1
    for piece in out:
        assert piece.char_start is not None and piece.char_end is not None
        assert document[piece.char_start : piece.char_end] == piece.text
    assert out[0].char_start == offset


# --------------------------------------------------------------------------- #
# Предпочтение границ
# --------------------------------------------------------------------------- #


def test_paragraph_boundary_preferred_over_sentence() -> None:
    """В окне есть и конец абзаца, и (позже) конец предложения — режем по абзацу."""
    text = "aaaa bbbb cccc dddd eeee\n\nffff. gggg hhhh iiii jjjj"
    # limit=40, floor_share=0.5 ⇒ окно = text[20:40] — в нём "\n\n" и позже него ".".
    out, _ = _limiter(40, min_tail_tokens=0).apply([_chunk(text)])

    assert out[0].text == "aaaa bbbb cccc dddd eeee\n\n"
    assert out[0].text.endswith("\n\n")
    assert "ffff." not in out[0].text


def test_sentence_boundary_preferred_over_whitespace() -> None:
    """В окне есть конец предложения и более поздний пробел — режем по предложению."""
    text = "aaaa bbbb cccc dddd. eeee ffff gggg"
    # limit=30 ⇒ окно = text[15:30] = "dddd. eeee ffff": последний пробел позже точки.
    out, _ = _limiter(30, min_tail_tokens=0).apply([_chunk(text)])

    assert out[0].text == "aaaa bbbb cccc dddd. "


def test_whitespace_boundary_used_when_no_sentence_end() -> None:
    text = "aaaa bbbb cccc dddd eeee ffff gggg hhhh"

    out, _ = _limiter(25, min_tail_tokens=0).apply([_chunk(text)])

    assert out[0].text.endswith(" ")
    assert not out[0].text.endswith(". ")


def test_hard_cut_when_no_boundaries_at_all() -> None:
    """Сплошной текст без пробелов режется жёстко, но лимит всё равно держится."""
    text = "a" * 5030

    out, report = _limiter(100).apply([_chunk(text)])

    assert "".join(piece.text for piece in out) == text
    assert all(len(piece.text) <= 100 for piece in out[:-1])
    assert report.split_chunks == 1
    # 5030 = 50 кусков по 100 и хвост 30 токенов < MIN_TAIL_TOKENS ⇒ хвост приклеен.
    assert report.absorbed_tails == 1
    assert len(out) == 50
    assert len(out[-1].text) == 130
    assert report.over_limit == 1  # цена приклейки названа, а не спрятана


# --------------------------------------------------------------------------- #
# Хвост
# --------------------------------------------------------------------------- #


def test_short_tail_is_absorbed() -> None:
    text = "a" * (100 * 3 + 10)

    out, report = _limiter(100).apply([_chunk(text)])

    assert report.absorbed_tails == 1
    assert len(out) == 3
    assert len(out[-1].text) == 110
    assert report.max_tokens_after == 110


def test_tail_above_threshold_is_not_absorbed() -> None:
    text = "a" * (100 * 3 + MIN_TAIL_TOKENS)

    out, report = _limiter(100).apply([_chunk(text)])

    assert report.absorbed_tails == 0
    assert len(out) == 4
    assert report.over_limit == 0


def test_min_tail_tokens_zero_never_absorbs() -> None:
    text = "a" * (100 * 2 + 3)

    out, report = _limiter(100, min_tail_tokens=0).apply([_chunk(text)])

    assert report.absorbed_tails == 0
    assert len(out) == 3
    assert len(out[-1].text) == 3


# --------------------------------------------------------------------------- #
# Гарантия лимита при граничном эффекте токенизатора
# --------------------------------------------------------------------------- #


def test_limit_holds_despite_tokenizer_boundary_effect() -> None:
    """Префикс токенизируется не так, как тот же текст внутри целого: рез отодвигается.

    Конструкция точная: ``limit=24``, окно ``text[12:24]``, последний пробел окна — на
    позиции 23, поэтому естественный рез даёт кусок в 24 символа, а счётчик считает у
    него **25** токенов (лишний — на стыке). Гарантия обязана отодвинуть рез к концу
    предыдущего токена.
    """
    counter = _BoundaryCounter()
    text = "x" * 23 + " " + "y" * 20

    out, report = _limiter(24, counter=counter, min_tail_tokens=0).apply([_chunk(text)])

    assert "".join(piece.text for piece in out) == text
    assert out[0].text == "x" * 23  # рез отодвинут: наивный дал бы 24 символа
    for piece in out:
        assert len(counter.token_ends(piece.text)) <= 24
    assert report.over_limit == 0


def test_limit_holds_on_mixed_text() -> None:
    counter = _BoundaryCounter()
    text = ("слово " * 60).strip()

    out, report = _limiter(24, counter=counter, min_tail_tokens=0).apply([_chunk(text)])

    assert "".join(piece.text for piece in out) == text
    assert all(len(counter.token_ends(piece.text)) <= 24 for piece in out)
    assert report.over_limit == 0


# --------------------------------------------------------------------------- #
# Чанк без точного среза
# --------------------------------------------------------------------------- #


def test_chunk_without_offsets_is_kept_whole() -> None:
    chunk = Chunk(
        chunk_id="doc#0",
        chunk_no=0,
        text="a" * 400,
        heading_path="",
        context_prefix="",
        content_hash=_hash("a" * 400),
        char_start=None,
        char_end=None,
    )

    out, report = _limiter(50).apply([chunk])

    assert len(out) == 1
    assert out[0].text == "a" * 400
    assert out[0].char_start is None and out[0].char_end is None
    assert report.unsplittable == 1
    assert report.split_chunks == 0
    # Сверх лимита — да, но резать нечем: это видно в отчёте, а не спрятано.
    assert report.over_limit == 1


def test_partial_offsets_are_also_unsplittable() -> None:
    chunk = Chunk(
        chunk_id="doc#0",
        chunk_no=0,
        text="a" * 300,
        heading_path="",
        context_prefix="",
        content_hash=_hash("a" * 300),
        char_start=None,
        char_end=300,
    )

    out, report = _limiter(50).apply([chunk])

    assert len(out) == 1
    assert report.unsplittable == 1


# --------------------------------------------------------------------------- #
# Пересборка чанков
# --------------------------------------------------------------------------- #


def test_chunk_no_and_id_are_sequential_and_unique() -> None:
    first = _chunk(_LONG_TEXT * 4, no=0)
    second = _chunk("короткий", no=1, start=len(_LONG_TEXT * 4))
    third = _chunk(_LONG_TEXT * 3, no=2, start=len(_LONG_TEXT * 4) + len("короткий"))

    out, report = _limiter(80).apply([first, second, third])

    assert [piece.chunk_no for piece in out] == list(range(len(out)))
    assert [piece.chunk_id for piece in out] == [f"doc#{no}" for no in range(len(out))]
    assert len({piece.chunk_id for piece in out}) == len(out) == report.chunks_out
    assert report.chunks_in == 3


def test_content_hash_matches_piece_text() -> None:
    out, _ = _limiter(80).apply([_chunk(_LONG_TEXT * 4)])

    for piece in out:
        assert piece.content_hash == hashlib.sha256(piece.text.encode("utf-8")).hexdigest()[:16]


def test_heading_and_context_inherited_from_parent() -> None:
    """Шапка таблицы и путь заголовка действуют и для продолжения."""
    out, _ = _limiter(80).apply(
        [_chunk(_LONG_TEXT * 4, heading="Раздел > Подраздел", context="шапка таблицы")]
    )

    assert len(out) > 1
    assert all(piece.heading_path == "Раздел > Подраздел" for piece in out)
    assert all(piece.context_prefix == "шапка таблицы" for piece in out)


# --------------------------------------------------------------------------- #
# Детерминизм и идемпотентность
# --------------------------------------------------------------------------- #


def test_determinism() -> None:
    text = _LONG_TEXT * 5

    first, _ = _limiter(80).apply([_chunk(text)])
    second, _ = _limiter(80).apply([_chunk(text)])

    assert first == second


def test_idempotent_when_every_piece_fits() -> None:
    """Куски в пределах лимита второй проход не меняет (состав и адреса — те же)."""
    once, report = _limiter(80, min_tail_tokens=0).apply([_chunk(_LONG_TEXT * 4)])
    twice, second_report = _limiter(80, min_tail_tokens=0).apply(once)

    assert report.split_chunks == 1
    assert second_report.split_chunks == 0
    assert twice == once


def test_second_pass_keeps_p1_and_slices_when_tail_was_absorbed() -> None:
    """Приклеенный хвост — объявленное исключение из идемпотентности: он сверх лимита.

    Второй проход может поделить такой кусок (он длиннее бюджета по построению), но
    обязан сохранить P1 и точные срезы — то есть корпус остаётся проверяемым.
    """
    text = _LONG_TEXT * 6
    once, first_report = _limiter(80).apply([_chunk(text)])
    twice, _ = _limiter(80).apply(once)

    assert "".join(piece.text for piece in twice) == text
    for piece in twice:
        assert piece.char_start is not None and piece.char_end is not None
        assert text[piece.char_start : piece.char_end] == piece.text
    if first_report.absorbed_tails:
        assert len(twice) >= len(once)


def test_multiple_chunks_are_processed_independently() -> None:
    text_a = _LONG_TEXT * 4
    text_b = _LONG_TEXT * 3
    chunks = [
        _chunk(text_a, no=0),
        _chunk(text_b, no=1, start=len(text_a)),
    ]

    out, report = _limiter(80).apply(chunks)

    assert report.split_chunks == 2
    assert "".join(piece.text for piece in out) == text_a + text_b


def test_output_is_ordered_as_input() -> None:
    chunks = [_chunk("a" * 300, no=0), _chunk("b" * 300, no=1, start=300)]

    out, _ = _limiter(100).apply(chunks)

    letters = [piece.text[0] for piece in out]
    assert letters == sorted(letters)


# --------------------------------------------------------------------------- #
# Конструктор
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("limit", [0, -1])
def test_non_positive_limit_is_rejected(limit: int) -> None:
    with pytest.raises(ValueError):
        TokenLimiter(_StepCounter(), limit)


@pytest.mark.parametrize("share", [0.0, 1.0, -0.5, 1.5])
def test_floor_share_outside_open_interval_is_rejected(share: float) -> None:
    with pytest.raises(ValueError):
        TokenLimiter(_StepCounter(), 10, floor_share=share)


def test_negative_tail_threshold_is_rejected() -> None:
    with pytest.raises(ValueError):
        TokenLimiter(_StepCounter(), 10, min_tail_tokens=-1)


def test_declared_defaults() -> None:
    assert TOKEN_LIMIT_POLICY == "token-limit-1"
    assert 0 < TOKEN_LIMIT_FLOOR_SHARE < 1
    assert MIN_TAIL_TOKENS > 0
    assert TokenLimiter(_StepCounter(), 42).limit == 42


# --------------------------------------------------------------------------- #
# Счётчик на transformers: конвенция провенанса и видимый отказ
# --------------------------------------------------------------------------- #


def test_hf_counter_spec_is_model_at_revision() -> None:
    counter = HfTokenCounter("BAAI/bge-m3", "5617a9f6")

    assert counter.spec == "BAAI/bge-m3@5617a9f6"
    assert HfTokenCounter("BAAI/bge-m3").spec == "BAAI/bge-m3"
    assert isinstance(counter, TokenCounterPort)


def test_hf_counter_rejects_empty_model_id() -> None:
    with pytest.raises(ValueError):
        HfTokenCounter("")


def test_hf_counter_empty_text_needs_no_model() -> None:
    """Пустой текст не требует ни токенизатора, ни сети."""
    assert HfTokenCounter("BAAI/bge-m3").token_ends("") == ()


def _fake_transformers(monkeypatch: pytest.MonkeyPatch, tokenizer: object) -> None:
    """Подменяет ``transformers`` модулем-заглушкой: реальная зависимость не нужна."""
    module = types.ModuleType("transformers")
    module.AutoTokenizer = types.SimpleNamespace(  # type: ignore[attr-defined]
        from_pretrained=lambda *args, **kwargs: tokenizer
    )
    monkeypatch.setitem(sys.modules, "transformers", module)


def test_hf_counter_drops_empty_segments_and_asks_for_content_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Служебные/пустые отрезки отбрасываются; считается текст, а не вызов модели."""
    calls: list[dict[str, object]] = []

    class _Tokenizer:
        def __call__(self, text: str, **kwargs: object) -> dict[str, object]:
            calls.append(dict(kwargs))
            return {"offset_mapping": [(0, 0), (0, 3), (3, 3), (3, 7)]}

    _fake_transformers(monkeypatch, _Tokenizer())

    ends = HfTokenCounter("BAAI/bge-m3", "5617a9f6").token_ends("abcdefg")

    assert ends == (3, 7)
    assert calls == [
        {
            "add_special_tokens": False,  # токены содержимого, а не вызов модели
            "truncation": False,
            "return_offsets_mapping": True,
        }
    ]


def test_hf_counter_reports_unavailable_when_model_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Недоступный токенизатор — видимый отказ, а не символьная оценка."""
    module = types.ModuleType("transformers")

    def _from_pretrained(*args: object, **kwargs: object) -> object:
        raise OSError("снапшот отсутствует в локальном кэше")

    module.AutoTokenizer = types.SimpleNamespace(from_pretrained=_from_pretrained)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "transformers", module)

    with pytest.raises(TokenizerUnavailable) as excinfo:
        HfTokenCounter("BAAI/bge-m3", "5617a9f6").token_ends("текст")

    message = str(excinfo.value)
    assert "BAAI/bge-m3@5617a9f6" in message
    assert "local_files_only" in message
    assert "прокси" in message  # подмена символьной оценкой названа явно


def test_hf_counter_reports_unavailable_without_transformers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "transformers", None)

    with pytest.raises(TokenizerUnavailable):
        HfTokenCounter("BAAI/bge-m3").token_ends("текст")


def test_token_counter_port_is_runtime_checkable() -> None:
    assert isinstance(_StepCounter(), TokenCounterPort)
    assert isinstance(_BoundaryCounter(), TokenCounterPort)


def test_step_counter_covers_text_left_to_right() -> None:
    """Контракт порта: отрезки строго возрастают и не выходят за текст."""
    counter = _StepCounter(step=3)
    text = "abcdefghij"

    ends = counter.token_ends(text)

    assert list(ends) == sorted(ends)
    assert len(set(ends)) == len(ends)
    assert ends[-1] == len(text)
    assert list(itertools.islice(ends, 0, 3)) == [3, 6, 9]
