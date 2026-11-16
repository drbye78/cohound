"""Тесты эмбеддинга продукт-партиции: дедуп по тексту и провенанс ``embedded_at``.

Закрываемый дефект: ``_embed_batched`` спрашивал кэш и вызывал эмбеддер **на каждый
чанк**, поэтому одинаковый текст, попавший в один батч дважды, считался дважды. Теперь
входной список группируется по ``CorpusChunk.content_hash``, уникальный текст уходит в
модель один раз, а вектор раскладывается по всем его позициям.

Тесты фиксируют четыре свойства:

1. **Дедуп.** Одинаковый текст в батче (и в разных батчах без кэша) эмбеддится один раз.
2. **Порядок и побитовое равенство (A3).** Массив векторов остаётся порядком чанков и
   побайтово совпадает с расчётом без дедупа — тот же стаб-эмбеддер, тот же вход.
3. **Доказуемость экономии.** ``embed_dedup_saved``/``embed_dedup_pct`` публикуются в тех же
   ключах прогресса, что ``embed_cached``/``embed_hit_ratio_pct``.
4. **Провенанс.** ``corpus_chunks.embedded_at`` заполняется в :func:`_embed_and_build`
   (оркестратор читает часы), а чистый :func:`_embed_batched` часов не читает.

Нейтральные фикстуры: ``p1``, ``case``, ``fake-embedder``.
"""

from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from core.domain import CorpusChunk
from core.pipeline.corpus_index import (
    _embed_and_build,
    _embed_batched,
    _EmbedCensus,
    _EmbedThroughput,
    _make_embed_reporter,
    _VectorMatrix,
)
from core.pipeline.embed_cache import EmbedCache
from core.util.hashing import content_hash
from tests.fakes.ports import FakeEmbedder, FakeIndexBuilder

MODEL = "fake-embedder-1"
DEVICE = "cpu"
PRECISION = "fp32"
URL = "https://docs.test/p1/index"


def _chunk(text: str, *, chunk_id: str, hashed: bool = True) -> CorpusChunk:
    """Чанк одного продукта; ``hashed=False`` — без ``content_hash`` (дедуп не применим)."""
    return CorpusChunk(
        chunk_id=chunk_id,
        doc_id=f"doc-{chunk_id}",
        product_id="p1",
        url=URL,
        heading_path="h1",
        chunk_no=0,
        text=text,
        content_hash=content_hash(text) if hashed else None,
    )


def _fp16(values: Any) -> bytes:
    """Байты fp16-матрицы: та же величина, которую фиксирует сборщик индекса."""
    return np.asarray(values, dtype=np.float16).tobytes()


def _embed(
    chunks: list[CorpusChunk],
    *,
    embedder: FakeEmbedder | None = None,
    **kwargs: Any,
) -> list[list[float]]:
    """Вызов ``_embed_batched`` с объявленными осью и моделью (обязательные keyword)."""
    return _embed_batched(
        embedder if embedder is not None else FakeEmbedder(dim=4),
        chunks,
        embed_model_id=MODEL,
        embed_device=DEVICE,
        embed_precision=PRECISION,
        **kwargs,
    )


class _RecordingHeartbeat:
    """Стенд-in для ``Heartbeat``: считает точки жизни."""

    def __init__(self) -> None:
        self.touches = 0

    def touch(self, *, force: bool = False) -> bool:  # noqa: ARG002 — форма порта
        self.touches += 1
        return True


class _CountsStore:
    """Минимальный стор для колбэка: только агрегат сеанса."""

    def crawl_session_counts(self, session_id: str) -> dict[str, int]:  # noqa: ARG002
        return {}


# --------------------------------------------------------------------------- #
# 1. Дедуп по content_hash
# --------------------------------------------------------------------------- #


def test_same_text_in_one_batch_is_embedded_once() -> None:
    """``[a, b, a, c, b, a]`` → в эмбеддер уходит ровно ``[a, b, c]``."""
    texts = ["a", "b", "a", "c", "b", "a"]
    chunks = [_chunk(text, chunk_id=f"c{index}") for index, text in enumerate(texts)]
    embedder = FakeEmbedder(dim=4)

    vectors = _embed(chunks, embedder=embedder)

    assert embedder.calls == [["a", "b", "c"]]
    assert len(vectors) == len(chunks)
    baseline = [FakeEmbedder(dim=4).embed([chunk.text])[0] for chunk in chunks]
    assert _fp16(vectors) == _fp16(baseline)


def test_duplicate_across_batches_without_cache_is_embedded_once() -> None:
    """Без кэша повтор текста в следующем батче берётся из памяти вызова, а не из модели."""
    chunks = [
        _chunk("a", chunk_id="c0"),
        _chunk("b", chunk_id="c1"),
        _chunk("a", chunk_id="c2"),
    ]
    embedder = FakeEmbedder(dim=4)

    vectors = _embed(chunks, embedder=embedder, batch_size=2)

    # Первый батч — два уникальных текста; второй целиком обслужен дедупом.
    assert embedder.calls == [["a", "b"]]
    baseline = [FakeEmbedder(dim=4).embed([chunk.text])[0] for chunk in chunks]
    assert _fp16(vectors) == _fp16(baseline)


def test_chunks_without_content_hash_are_deduplicated_by_the_embed_input() -> None:
    """Ключ дедупа — хеш **входа эмбеддера**, а не ``content_hash`` текста.

    Раньше пустой ``content_hash`` отключал дедуп: ключ не различал тексты. Теперь ключ
    считается по строке, которую увидит модель, поэтому различает он всегда — и два
    одинаковых текста без ``content_hash`` дают **один** вектор (это верно: вектор —
    функция текста). Разные тексты по-прежнему не склеиваются.
    """
    chunks = [
        _chunk("same", chunk_id="c0", hashed=False),
        _chunk("same", chunk_id="c1", hashed=False),
        _chunk("other", chunk_id="c2", hashed=False),
    ]
    embedder = FakeEmbedder(dim=4)

    vectors = _embed(chunks, embedder=embedder)

    assert embedder.calls == [["same", "other"]]
    assert len(vectors) == 3


def test_same_text_with_different_headings_is_not_deduplicated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ось ``EMBED_INPUT_FIELDS`` исполняется: заголовок входит в вход — вход различает.

    Это защита от **тихой** ошибки: если группировать по ``content_hash`` (он у обоих
    чанков одинаков), второй получил бы вектор первого, посчитанный без его заголовка.
    Замеряемая цена такой ошибки — весь смысл O19.
    """
    monkeypatch.setattr(
        "core.pipeline.corpus_index.embed_input_text",
        lambda chunk: f"{chunk.heading_path}\n\n{chunk.text}",
    )
    chunks = [
        _chunk("same", chunk_id="c0"),
        _chunk("same", chunk_id="c1"),
    ]
    # Заголовки разные, текст один: ключи обязаны разойтись.
    chunks[1] = CorpusChunk(
        chunk_id="c1",
        doc_id="doc-c1",
        product_id="p1",
        url=URL,
        heading_path="h2",
        chunk_no=0,
        text="same",
        content_hash=content_hash("same"),
    )
    embedder = FakeEmbedder(dim=4)

    vectors = _embed(chunks, embedder=embedder)

    assert embedder.calls == [["h1\n\nsame", "h2\n\nsame"]]
    assert len(vectors) == 2


# --------------------------------------------------------------------------- #
# 2. Порядок и побитовое равенство пути без дедупа (A3)
# --------------------------------------------------------------------------- #


def test_vectors_stay_bitwise_equal_to_per_chunk_embedding() -> None:
    """Дедуп меняет число вызовов, но не массив векторов и не их порядок."""
    texts = ["x", "y", "x", "z", "y", "x", "z", "z"]
    chunks = [_chunk(text, chunk_id=f"c{index}") for index, text in enumerate(texts)]

    vectors = _embed(chunks)

    live = [FakeEmbedder(dim=4).embed([chunk.text])[0] for chunk in chunks]
    assert _fp16(vectors) == _fp16(live)


def test_cached_vectors_with_duplicates_are_bitwise_equal_to_live(
    tmp_path: Path,
) -> None:
    """Холодный и тёплый кэш на дедуплицированном входе дают те же байты, что живой расчёт."""
    cache = EmbedCache(tmp_path / "embed_cache")
    texts = ["x", "y", "x", "x", "z", "y"]
    chunks = [_chunk(text, chunk_id=f"c{index}") for index, text in enumerate(texts)]
    live = [FakeEmbedder(dim=4).embed([chunk.text])[0] for chunk in chunks]

    cold = _embed(chunks, cache=cache)
    warm = _embed(chunks, cache=cache)

    assert _fp16(cold) == _fp16(live)
    assert _fp16(warm) == _fp16(live)


def test_partition_keeps_chunk_ids_and_vector_order_with_duplicates() -> None:
    """Сборщик получает ``chunk_ids`` и векторы в порядке входа, даже при повторах."""
    texts = ["a", "b", "a", "c"]
    chunks = [_chunk(text, chunk_id=f"c{index}") for index, text in enumerate(texts)]
    builder = FakeIndexBuilder()
    errors: list[str] = []

    ok = _embed_and_build(
        FakeEmbedder(dim=4),
        builder,
        "p1",
        chunks,
        package_id="case",
        chunker_version="chunker-1",
        embed_model_id=MODEL,
        embed_device=DEVICE,
        embed_precision=PRECISION,
        extractor_version="extractor-1",
        lexical_engine_version="fake-bm25-1",
        vector_store="fake",
        product_content_token="token",
        snapshot_date="2026-01-01",
        partition_build_id="pb_fixture",
        errors=errors,
    )

    assert ok is True and errors == []
    assert [chunk.chunk_id for chunk in builder.chunks_by_product["p1"]] == [
        chunk.chunk_id for chunk in chunks
    ]
    live = [FakeEmbedder(dim=4).embed([chunk.text])[0] for chunk in chunks]
    assert _fp16(builder.vectors_by_product["p1"]) == _fp16(live)


# --------------------------------------------------------------------------- #
# 3. Наблюдаемость: сэкономленные вызовы эмбеддера
# --------------------------------------------------------------------------- #


def test_census_and_progress_name_saved_embedder_calls() -> None:
    """Дедуп публикует число снятых вызовов в тех же ключах прогресса, что и кэш."""
    chunks = [
        _chunk("a", chunk_id="c0"),
        _chunk("a", chunk_id="c1"),
        _chunk("b", chunk_id="c2"),
    ]
    census = _EmbedCensus()
    throughput = _EmbedThroughput()
    snapshots: list[dict[str, int]] = []
    report = _make_embed_reporter(
        heartbeat=_RecordingHeartbeat(),  # type: ignore[arg-type] — стенд-in формы
        progress=snapshots.append,
        store=_CountsStore(),
        session=None,
        throughput=throughput,
        census=census,
    )

    _embed(chunks, census=census, on_batch=report)

    # Уникальных текстов два, третий чанк снят дедупом — в модель ушло 2 вызова.
    assert census.embedded == 2
    assert census.deduplicated == 1
    assert census.processed == len(chunks)
    assert census.dedup_saved_pct == 33
    last = snapshots[-1]
    assert last["embed_dedup_saved"] == 1
    assert last["embed_dedup_pct"] == 33
    assert last["embed_chunks"] == len(chunks)


# --------------------------------------------------------------------------- #
# 4. Провенанс: embedded_at заполняется оркестратором, не чистым эмбеддингом
# --------------------------------------------------------------------------- #


def test_embed_batched_does_not_read_the_clock() -> None:
    """Чистая функция батча провенанс не пишет: времени у неё нет в аргументах."""
    chunks = [_chunk("a", chunk_id="c0"), _chunk("b", chunk_id="c1")]

    _embed(chunks)

    assert all(chunk.embedded_at is None for chunk in chunks)


def test_embed_and_build_stamps_embedded_at() -> None:
    """Все чанки продукт-цикла получают одно ISO-8601 (UTC) время эмбеддинга."""
    chunks = [_chunk("a", chunk_id="c0"), _chunk("b", chunk_id="c1")]
    builder = FakeIndexBuilder()
    errors: list[str] = []

    ok = _embed_and_build(
        FakeEmbedder(dim=4),
        builder,
        "p1",
        chunks,
        package_id="case",
        chunker_version="chunker-1",
        embed_model_id=MODEL,
        embed_device=DEVICE,
        embed_precision=PRECISION,
        extractor_version="extractor-1",
        lexical_engine_version="fake-bm25-1",
        vector_store="fake",
        product_content_token="token",
        snapshot_date="2026-01-01",
        partition_build_id="pb_fixture",
        errors=errors,
    )

    assert ok is True and errors == []
    stamps = {chunk.embedded_at for chunk in chunks}
    assert None not in stamps
    assert len(stamps) == 1
    parsed = _dt.datetime.fromisoformat(next(iter(stamps)))
    assert parsed.tzinfo is not None and parsed.utcoffset() == _dt.timedelta(0)

# -- накопитель векторов: байты партиции не меняются (план 26 O10) -----------


#: Значения, на которых приведение к ``float16`` различает «почти» и «точно»:
#: обычная дробь (0.1 не представима), субнормаль, крайние значения нормального
#: диапазона fp16 и точка ровно между двумя fp16-соседями (round-half-to-even).
_ADVERSARIAL_ROWS: list[list[float]] = [
    [1 / 3, -2 / 3, 0.1, 1e-8],
    [65504.0, -65504.0, 6.103515625e-05, 5.960464477539063e-08],
    [1.0, 1.0 + 2**-11, 1.0 + 3 * 2**-11, 0.0],
    [-1.0, -1.0 - 2**-11, 0.5, -0.5],
]


def test_vector_matrix_is_byte_identical_to_the_plain_list_cast() -> None:
    """Накопитель обязан дать те же байты, что прежний путь «список списков → fp16».

    Это и есть условие, при котором смена носителя не является ``content_change``:
    ``vectors.fp16.npy`` собирает сборщик через ``np.asarray(vectors, dtype=np.float16)``,
    и если накопитель приводит к ``float16`` раньше, но над теми же числами, байты
    обязаны совпасть. Значения выбраны так, чтобы разошлось при любом отличии в
    округлении (субнормали, граница 65504, точка ровно между соседями fp16).
    """
    total = len(_ADVERSARIAL_ROWS)
    sink = _VectorMatrix(total, dim=4)  # type: ignore[attr-defined]
    for row in _ADVERSARIAL_ROWS:
        sink.add([row])

    matrix = sink.matrix()
    reference = np.asarray(_ADVERSARIAL_ROWS, dtype=np.float16)
    assert matrix.shape == reference.shape == (total, 4)
    assert matrix.dtype == np.float16
    assert matrix.tobytes() == reference.tobytes()


def test_vector_matrix_rejects_ragged_rows_instead_of_padding() -> None:
    """Вектор другой длины — отказ, а не нули: иначе косинус по партиции ложен."""
    sink = _VectorMatrix(2, dim=3)  # type: ignore[attr-defined]
    sink.add([[1.0, 2.0, 3.0]])
    with pytest.raises(ValueError, match="рваных строк"):
        sink.add([[1.0, 2.0]])


def test_vector_matrix_learns_the_dim_from_the_first_vector() -> None:
    """Размерность не объявлена эмбеддером — берётся у первого вектора, не выдумывается."""
    sink = _VectorMatrix(2, dim=None)  # type: ignore[attr-defined]
    sink.add([[1.0, 2.0]])
    sink.add([[3.0, 4.0]])
    assert sink.matrix().tolist() == [[1.0, 2.0], [3.0, 4.0]]
