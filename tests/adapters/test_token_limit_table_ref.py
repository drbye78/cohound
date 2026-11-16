"""Адрес куска в таблице под токенным ограничителем (O21 + O39 фаза 2).

``table_ref`` описывает **весь** кусок: ``row_start``/``row_end`` — те строки данных, которые
в нём лежат. Ограничитель делит кусок по бюджету токенов, и перенести адрес на половину
значило бы назвать строки, которых в ней нет. Поэтому правило узкое и проверяется здесь
целиком:

* **неразрезанный кусок адрес сохраняет** (в том числе кусок без точного среза: резать его
  нечем, состав не изменился);
* **разрезанный теряет адрес** — у всех своих частей;
* **потеря видима числом**: ``LimitReport.table_ref_dropped``, иначе включение ограничителя
  стирало бы провенанс молча;
* **кусок без адреса счётчик не двигает** — иначе число перестало бы значить «сколько
  таблиц разрезано».

Счётчик токенов — фейковый (один символ = один токен), как в ``test_token_limit.py``: тест
проверяет правило ограничителя, а не загрузку весов. Нейтрален (§1.3).
"""

from __future__ import annotations

from core.adapters.token_limit import TokenLimiter
from core.domain.doc_tables import TableRef
from core.ports.types import Chunk
from tests.adapters.test_token_limit import _hash, _StepCounter

_LONG_TEXT = "Первый абзац про установку. Он состоит из двух предложений.\n\n" * 4


def _chunk(
    text: str,
    *,
    no: int = 0,
    start: int = 0,
    table_ref: TableRef | None = None,
) -> Chunk:
    """Кусок с адресом в таблице — как его отдаёт markdown-чанкер после ``policy-6``."""
    return Chunk(
        chunk_id=f"doc#{no}",
        chunk_no=no,
        text=text,
        heading_path="Раздел",
        context_prefix="| a | b |",
        content_hash=_hash(text),
        char_start=start,
        char_end=start + len(text),
        table_ref=table_ref,
    )


def _limiter(limit: int, **kwargs: int) -> TokenLimiter:
    return TokenLimiter(_StepCounter(), limit, **kwargs)


# --------------------------------------------------------------------------- #
# Неразрезанный кусок
# --------------------------------------------------------------------------- #


def test_unsplit_chunk_keeps_address() -> None:
    """Кусок уложился в бюджет — состав не изменился, значит и адрес остаётся прежним."""
    ref = TableRef("t0", 1, 3)

    out, report = _limiter(100).apply([_chunk("| a | 1 |\n", table_ref=ref)])

    assert len(out) == 1
    assert out[0].table_ref == ref
    assert report.table_ref_dropped == 0


def test_unsplittable_chunk_keeps_address() -> None:
    """Кусок без точного среза не режется вовсе: адрес не теряется, хотя лимит превышен.

    ``table_ref`` переносится на «неразрезанный кусок», а неразрезанность здесь вынужденная:
    резать нечем (нет ``char_start``/``char_end``). Состав не изменился — адрес верен.
    """
    ref = TableRef("t0", 1, 2)
    chunk = Chunk(
        chunk_id="doc#0",
        chunk_no=0,
        text="a" * 400,
        heading_path="",
        context_prefix="",
        content_hash=_hash("a" * 400),
        char_start=None,
        char_end=None,
        table_ref=ref,
    )

    out, report = _limiter(50).apply([chunk])

    assert len(out) == 1
    assert out[0].table_ref == ref
    assert report.unsplittable == 1
    assert report.table_ref_dropped == 0


# --------------------------------------------------------------------------- #
# Разрезанный кусок
# --------------------------------------------------------------------------- #


def test_split_chunk_loses_address_and_is_counted() -> None:
    """Диапазон строк описывал целый кусок: у его половины он был бы ложью.

    Потеря видна числом ``table_ref_dropped``, а не молчанием: иначе включение
    ограничителя стирало бы провенанс незаметно.
    """
    ref = TableRef("t0", 1, 9)

    out, report = _limiter(80, min_tail_tokens=0).apply([_chunk(_LONG_TEXT, table_ref=ref)])

    assert len(out) > 1
    assert report.split_chunks == 1
    assert all(piece.table_ref is None for piece in out)
    assert report.table_ref_dropped == 1


def test_split_keeps_text_and_slices_intact() -> None:
    """Потеря адреса не трогает текст: P1 и точные срезы держатся и у разрезанного куска."""
    chunk = _chunk(_LONG_TEXT, table_ref=TableRef("t0", 1, 9))

    out, _ = _limiter(80).apply([chunk])

    assert "".join(piece.text for piece in out) == _LONG_TEXT
    for piece in out:
        assert piece.char_start is not None and piece.char_end is not None
        assert _LONG_TEXT[piece.char_start : piece.char_end] == piece.text


# --------------------------------------------------------------------------- #
# Счётчик
# --------------------------------------------------------------------------- #


def test_chunk_without_address_does_not_touch_counter() -> None:
    """Кусок без адреса режется как обычно: счётчик считает таблицы, а не разрезы."""
    out, report = _limiter(80, min_tail_tokens=0).apply([_chunk(_LONG_TEXT)])

    assert len(out) > 1
    assert report.split_chunks == 1
    assert report.table_ref_dropped == 0


def test_counter_counts_only_addressed_chunks() -> None:
    """Смешанная пачка: разрезанный кусок с адресом даёт 1, остальные — ничего."""
    addressed = _chunk(_LONG_TEXT, no=0, table_ref=TableRef("t0", 1, 9))
    plain = _chunk(_LONG_TEXT, no=1, start=len(_LONG_TEXT))
    short_with_ref = _chunk(
        "| a | 1 |\n",
        no=2,
        start=2 * len(_LONG_TEXT),
        table_ref=TableRef("t1", 1, 1),
    )

    out, report = _limiter(80, min_tail_tokens=0).apply([addressed, plain, short_with_ref])

    assert (
        report.chunks_in,
        report.chunks_out,
        report.split_chunks,
        report.absorbed_tails,
        report.unsplittable,
        report.over_limit,
        report.table_ref_dropped,
    ) == (3, len(out), 2, 0, 0, 0, 1)
    # Короткий кусок с адресом адрес сохранил — он не резался.
    assert out[-1].table_ref == TableRef("t1", 1, 1)
