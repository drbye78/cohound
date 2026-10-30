"""Тесты почанкового кэша векторов эмбеддинга (``core/pipeline/embed_cache.py``).

Закрываемые дефекты/свойства:

1. **Переиспользование только целой партицией.** Смена ``extractor_version``/
   ``chunker_version`` или добавление документа даёт новый ``pb`` и пересчитывает эмбеддинг
   всего продукта, хотя текст большинства чанков не изменился. Кэш адресует вектор по
   ``(content_hash ТЕКСТА чанка, embed_model_id, embed_device)`` — поэтому одинаковый текст,
   полученный разными экстракторами, даёт **одно** попадание, вектор другой модели или
   другого устройства — ни одного.
2. **Порядок векторов (A3).** При смешанном батче хиты подставляются в свои позиции:
   порядок входа = порядок выхода, сборка индекса побитово равна живому эмбеддингу
   (fp16-квантизация кэша тождественна квантизации сборщика).
3. **Доказуемость экономии.** Счётчики ``embed_cached``/``embed_hit_ratio_pct`` в прогрессе и
   «из кэша N (X%)» в итоговой строке продукта: без числа фича недоказуема.
4. **Порча не роняет прогон.** Битый файл, неверная размерность, невалидный ключ — промах,
   а не исключение.
5. **Устройство различает запись (16 §5.3).** Векторы CPU и CUDA — разные числа, поэтому
   ``embed_device`` — обязательная компонента ключа, а запись без него — промах, а не хит.

Сквозные тесты (``_embed_batched``/``_embed_and_build``) передают ``embed_device=`` явно:
это тот же обязательный вход, что и у :class:`EmbedCache`, — ось объявляется в конвейере,
а не угадывается кэшем.

Нейтральные фикстуры: ``p1``, ``case``, ``fake-embedder``.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

import numpy as np

from core.domain import (
    AnchorRelation,
    Cohort,
    CohortEntry,
    CorpusChunk,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import (
    EMBED_BATCH_CHUNKS,
    build_corpus,
    format_embed_progress,
)
from core.pipeline.corpus_index import (
    _embed_and_build,
    _embed_batched,
    _EmbedCensus,
    _EmbedThroughput,
    _make_embed_reporter,
)
from core.pipeline.embed_cache import (
    ENVELOPE_FORMAT,
    SUFFIX,
    EmbedCache,
    embed_cache_key,
    model_dir_name,
)
from core.ports.types import Chunk, ExtractedDoc, ExtractionRoute, FetchPlan, RawPage
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash
from tests.fakes.ports import FakeEmbedder, FakeIndexBuilder

MODEL = "fake-embedder-1"
OTHER_MODEL = "another-embedder-1"
DEVICE = "cpu"
OTHER_DEVICE = "cuda"
RUN_ID = "run-embed-cache"
PACKAGE = "case"
TS = "2026-01-01T00:00:00+00:00"
URL_P1 = "https://docs.test/p1/index"
BODY = b"x" * 400


def _chunk(
    text: str,
    *,
    chunk_id: str = "c1",
    product_id: str = "p1",
    extractor_version: str | None = "extractor-1",
) -> CorpusChunk:
    """Чанк с авторитетным ``content_hash`` от чистого текста (как в конвейере, I19)."""
    return CorpusChunk(
        chunk_id=chunk_id,
        doc_id=f"doc-{chunk_id}",
        product_id=product_id,
        url=URL_P1,
        heading_path="h1",
        chunk_no=0,
        text=text,
        content_hash=content_hash(text),
        extractor_version=extractor_version,
    )


def _chunks(total: int, *, prefix: str = "chunk") -> list[CorpusChunk]:
    """``total`` различных чанков (разный текст — ни dedup, ни кэш-коллизий)."""
    return [_chunk(f"{prefix} text {i}", chunk_id=f"c{i}") for i in range(total)]


class _RecordingHeartbeat:
    """Стенд-in для :class:`~core.pipeline.liveness.Heartbeat`: считает точки жизни."""

    def __init__(self) -> None:
        self.touches = 0

    def touch(self, *, force: bool = False) -> bool:  # noqa: ARG002 — форма порта
        self.touches += 1
        return True


class _CountsStore:
    """Минимальный стор для колбэка: только агрегат сеанса."""

    def crawl_session_counts(self, session_id: str) -> dict[str, int]:  # noqa: ARG002
        return {}


def _fp16(values) -> bytes:  # noqa: ANN001 — вспомогательная проверка
    """Байты fp16-матрицы: та же величина, которую фиксирует ``index_npy``."""
    return np.asarray(values, dtype=np.float16).tobytes()


def _entry(root: Path, key: str, device: str = DEVICE) -> Path:
    """Путь записи кэша: ``{root}/{модель}/{устройство}/{h2}/{hash}.vec``."""
    return root / model_dir_name(MODEL) / device / key[:2] / f"{key}{SUFFIX}"


# --------------------------------------------------------------------------- #
# 1. Конверт: раскладка, fp16, независимость от версий производства
# --------------------------------------------------------------------------- #


def test_put_get_roundtrip_stores_two_bytes_per_axis(tmp_path: Path) -> None:
    """Запись — ``{model}/{device}/{h2}/{hash}.vec``: заголовок + ровно 2 байта на ось."""
    cache = EmbedCache(tmp_path / "embed_cache")
    vector = [0.5, -0.25, 0.125, 1.0]
    key = content_hash("текст чанка")
    cache.put(key, MODEL, vector, embed_device=DEVICE, dim=4)

    path = _entry(tmp_path / "embed_cache", key)
    raw = path.read_bytes()
    header, separator, payload = raw.partition(b"\n")
    assert separator == b"\n"
    assert b'"format":1' in header and b'"dim":4' in header
    assert b'"embed_device":"cpu"' in header
    assert len(payload) == 4 * 2  # fp16, а не JSON-числа

    # Чтение возвращает ровно fp16-значения (та же величина, что положит сборщик).
    assert cache.get(key, MODEL, embed_device=DEVICE, dim=4) == np.asarray(
        vector, dtype=np.float16
    ).astype(float).tolist()
    assert cache.stats() == {
        "hits": 1,
        "misses": 0,
        "writes": 1,
        "corrupt": 0,
        "evicted": 0,
        "invalid_key": 0,
        "scans": 0,  # без бюджета каталог не обходится вовсе
    }


def test_entry_is_addressed_by_text_model_and_device_only(tmp_path: Path) -> None:
    """Ключ несёт текст, модель и устройство — и ничего о версиях производства/чанке."""
    key = embed_cache_key(content_hash("t"), MODEL, DEVICE)
    assert key == f"{content_hash('t')}\x1f{MODEL}\x1f{DEVICE}"
    # Устройство — отдельная компонента ключа: тот же текст другой оси не адресует.
    assert embed_cache_key(content_hash("t"), MODEL, OTHER_DEVICE) != key
    # Санитизация каталога модели: путь-безопасное имя + хеш полного id (без коллизий).
    assert "/" not in model_dir_name("org/model:tag")
    assert model_dir_name("org/model:tag") != model_dir_name("org/model-tag")


def test_vectors_of_different_devices_do_not_mix(tmp_path: Path) -> None:
    """``put(cuda)`` не находится запросом ``cpu`` — и наоборот (16 §5.3).

    Векторы CPU и CUDA одной модели численно разные, поэтому «устройство в ключе» — это
    не провенанс, а часть значения: без него прогон, объявивший ``cuda``, молча получил бы
    ``cpu``-векторы.
    """
    cache = EmbedCache(tmp_path / "embed_cache")
    key = content_hash("текст")
    cache.put(key, MODEL, [1.0, 2.0], embed_device=OTHER_DEVICE, dim=2)

    assert cache.get(key, MODEL, embed_device=DEVICE, dim=2) is None
    assert cache.get(key, MODEL, embed_device=OTHER_DEVICE, dim=2) == [1.0, 2.0]
    # Записи живут раздельно: оси не перезаписывают друг друга (иначе каждый второй
    # прогон платил бы полный эмбеддинг).
    cache.put(key, MODEL, [3.0, 4.0], embed_device=DEVICE, dim=2)
    assert cache.get(key, MODEL, embed_device=OTHER_DEVICE, dim=2) == [1.0, 2.0]
    assert cache.get(key, MODEL, embed_device=DEVICE, dim=2) == [3.0, 4.0]


def test_entry_without_embed_device_is_a_miss(tmp_path: Path) -> None:
    """Старая запись без ``embed_device`` — **промах**, а не хит.

    Приписать ей устройство было бы догадкой: неизвестно, на чём посчитан вектор, а
    значения двух осей разные. Поэтому первый прогон после появления оси пере-эмбеддит
    корпус (цена названа в 16 §5.3), а не переиспользует чужое.
    """
    root = tmp_path / "embed_cache"
    cache = EmbedCache(root)
    key = content_hash("текст")
    path = _entry(root, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = np.asarray([0.5, 0.25], dtype=np.float16).tobytes()
    path.write_bytes(
        json.dumps(
            {
                "byteorder": "little",
                "content_hash": key,
                "dim": 2,
                "dtype": "float16",
                "embed_model_id": MODEL,
                "format": ENVELOPE_FORMAT,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        + b"\n"
        + payload
    )

    assert cache.get(key, MODEL, embed_device=DEVICE, dim=2) is None
    # Это не порча файла, а «не та ось»: счётчик порчи не растёт, растёт промах.
    assert cache.stats()["corrupt"] == 0
    assert cache.stats()["misses"] == 1


# --------------------------------------------------------------------------- #
# 2. Промах/попадание на уровне ``_embed_batched``
# --------------------------------------------------------------------------- #


def test_full_hit_never_calls_the_embedder(tmp_path: Path) -> None:
    """Все чанки в кэше — ``embed`` не вызывается ни разу (цель фичи)."""
    cache = EmbedCache(tmp_path / "embed_cache")
    chunks = _chunks(EMBED_BATCH_CHUNKS * 2 + 3)
    warm = FakeEmbedder(dim=4)
    _embed_batched(warm, chunks, cache=cache, embed_model_id=MODEL, embed_device=DEVICE)
    assert len(warm.calls) == 3

    cold = FakeEmbedder(dim=4)
    vectors = _embed_batched(
        cold, chunks, cache=cache, embed_model_id=MODEL, embed_device=DEVICE
    )
    assert cold.calls == []  # эмбеддер не тронут
    assert _fp16(vectors) == _fp16(warm.embed([c.text for c in chunks]))


def test_full_miss_embeds_everything_and_fills_the_cache(tmp_path: Path) -> None:
    """Пустой кэш — прежний путь: весь текст уходит в эмбеддер, кэш наполняется."""
    cache = EmbedCache(tmp_path / "embed_cache")
    chunks = _chunks(EMBED_BATCH_CHUNKS + 1)
    embedder = FakeEmbedder(dim=4)

    vectors = _embed_batched(
        embedder, chunks, cache=cache, embed_model_id=MODEL, embed_device=DEVICE
    )

    assert vectors == FakeEmbedder(dim=4).embed([c.text for c in chunks])
    assert [len(call) for call in embedder.calls] == [EMBED_BATCH_CHUNKS, 1]
    stats = cache.stats()
    assert stats["misses"] == len(chunks)
    assert stats["writes"] == len(chunks)


def test_mixed_batch_keeps_chunk_order(tmp_path: Path) -> None:
    """Часть батча в кэше, часть нет: порядок векторов остаётся порядком чанков (A3)."""
    cache = EmbedCache(tmp_path / "embed_cache")
    chunks = _chunks(10)
    # Прогреваем только чётные чанки: батч окажется смешанным.
    for index, chunk in enumerate(chunks):
        if index % 2 == 0:
            cache.put(
                chunk.content_hash or "",
                MODEL,
                [float(index)] * 4,
                embed_device=DEVICE,
                dim=4,
            )

    embedder = FakeEmbedder(dim=4)
    vectors = _embed_batched(
        embedder, chunks, cache=cache, embed_model_id=MODEL, embed_device=DEVICE
    )

    assert len(vectors) == len(chunks)
    # Промахи — ровно нечётные чанки, и только они ушли в эмбеддер.
    assert embedder.calls == [[chunks[i].text for i in (1, 3, 5, 7, 9)]]
    # Хиты стоят на своих позициях, свежие — побитово равны живому расчёту.
    live = FakeEmbedder(dim=4).embed([c.text for c in chunks])
    for index, vector in enumerate(vectors):
        if index % 2 == 0:
            assert vector == [float(index)] * 4
        else:
            assert _fp16(vector) == _fp16(live[index])


def test_models_do_not_mix(tmp_path: Path) -> None:
    """Вектор чужой модели не обслуживается: другая модель — полный промах."""
    cache = EmbedCache(tmp_path / "embed_cache")
    chunks = _chunks(3)
    _embed_batched(
        FakeEmbedder(dim=4), chunks, cache=cache, embed_model_id=MODEL,
        embed_device=DEVICE,
    )

    other = FakeEmbedder(dim=4, model_id=OTHER_MODEL)
    _embed_batched(
        other, chunks, cache=cache, embed_model_id=OTHER_MODEL, embed_device=DEVICE
    )
    assert len(other.calls) == 1  # эмбеддер вызван: векторов этой модели ещё нет
    # Обратно к первой модели — снова попадания (каталоги моделей не пересеклись).
    again = FakeEmbedder(dim=4)
    _embed_batched(
        again, chunks, cache=cache, embed_model_id=MODEL, embed_device=DEVICE
    )
    assert again.calls == []


def test_same_text_from_another_extractor_is_a_hit(tmp_path: Path) -> None:
    """Тот же текст, полученный другим экстрактором/чанкером — **одно** попадание.

    Это и есть цель кэша: смена механизма извлечения (или добавление документа) не
    заставляет пересчитывать вектор неизменившегося текста, хотя ``pb`` партиции меняется.
    """
    cache = EmbedCache(tmp_path / "embed_cache")
    original = _chunk("неизменившийся абзац", chunk_id="p1:docA:3")
    _embed_batched(
        FakeEmbedder(dim=4),
        [original],
        cache=cache,
        embed_model_id=MODEL,
        embed_device=DEVICE,
    )

    # Другой экстрактор, другой документ, другой chunk_id — текст тот же.
    reprint = _chunk(
        "неизменившийся абзац",
        chunk_id="p1:docB:0",
        product_id="p1",
        extractor_version="extractor-2",
    )
    embedder = FakeEmbedder(dim=4)
    vectors = _embed_batched(
        embedder, [reprint], cache=cache, embed_model_id=MODEL, embed_device=DEVICE
    )

    assert embedder.calls == []
    assert (
        cache.get(reprint.content_hash or "", MODEL, embed_device=DEVICE, dim=4)
        is not None
    )
    assert len(vectors) == 1

    # А изменившийся текст — промах (кэш не «схлопывает» разные тексты).
    changed = _chunk("изменившийся абзац", chunk_id="p1:docB:1")
    _embed_batched(
        embedder, [changed], cache=cache, embed_model_id=MODEL, embed_device=DEVICE
    )
    assert len(embedder.calls) == 1


# --------------------------------------------------------------------------- #
# 3. Порча и невалидные ключи — промах, не падение
# --------------------------------------------------------------------------- #


def test_corrupt_or_mismatched_entries_are_misses(tmp_path: Path) -> None:
    """Битый конверт, неверная размерность, чужой ключ внутри — промах без исключения."""
    root = tmp_path / "embed_cache"
    cache = EmbedCache(root)
    key = content_hash("текст")
    path = _entry(root, key)

    path.parent.mkdir(parents=True, exist_ok=True)
    # 1) не JSON и без разделителя заголовка
    path.write_bytes(b"\x00\x01\x02not-a-converter")
    assert cache.get(key, MODEL, embed_device=DEVICE, dim=4) is None
    # 2) заголовок валиден, payload обрезан (неверная длина)
    cache.put(key, MODEL, [0.0, 0.1, 0.2, 0.3], embed_device=DEVICE, dim=4)
    header, _, payload = path.read_bytes().partition(b"\n")
    path.write_bytes(header + b"\n" + payload[:-2])
    assert cache.get(key, MODEL, embed_device=DEVICE, dim=4) is None
    # 3) запись цела, но размерность не та, что ждёт модель
    cache.put(key, MODEL, [0.0, 0.1, 0.2, 0.3], embed_device=DEVICE, dim=4)
    assert cache.get(key, MODEL, embed_device=DEVICE, dim=8) is None
    # 4) несовместимая версия формата
    cache.put(key, MODEL, [0.0, 0.1, 0.2, 0.3], embed_device=DEVICE, dim=4)
    raw = path.read_bytes().partition(b"\n")[2]
    path.write_bytes(
        json.dumps(
            {
                "byteorder": "little",
                "content_hash": key,
                "dim": 4,
                "dtype": "float16",
                "embed_device": DEVICE,
                "embed_model_id": MODEL,
                "format": ENVELOPE_FORMAT + 1,
            }
        ).encode()
        + b"\n"
        + raw
    )
    assert cache.get(key, MODEL, embed_device=DEVICE, dim=4) is None
    # Все случаи учтены как порча/промах, и ничего не брошено.
    assert cache.stats()["corrupt"] >= 4

    # Испорченный файл не мешает прогону: промах уходит в эмбеддер.
    embedder = FakeEmbedder(dim=4)
    _embed_batched(
        embedder,
        [_chunk("текст")],
        cache=cache,
        embed_model_id=MODEL,
        embed_device=DEVICE,
    )
    assert len(embedder.calls) == 1


def test_invalid_key_is_a_miss_not_a_crash(tmp_path: Path) -> None:
    """Чанк без ``content_hash`` не может стать ключом — промах, без исключения."""
    cache = EmbedCache(tmp_path / "embed_cache")
    bare = CorpusChunk(
        chunk_id="c1", doc_id="d1", product_id="p1", text="без хеша"
    )
    embedder = FakeEmbedder(dim=4)
    vectors = _embed_batched(
        embedder, [bare], cache=cache, embed_model_id=MODEL, embed_device=DEVICE
    )
    assert len(vectors) == 1 and len(embedder.calls) == 1
    assert cache.stats()["invalid_key"] >= 1
    assert cache.get("", MODEL, embed_device=DEVICE) is None
    assert cache.get("../../etc/passwd", MODEL, embed_device=DEVICE) is None
    # Необъявленное устройство — тоже невалидный ключ (пустая метка не адресует запись).
    assert cache.get(content_hash("t"), MODEL, embed_device="") is None
    assert cache.get(content_hash("t"), MODEL, embed_device="../cuda") is None


def test_budget_evicts_oldest_entries(tmp_path: Path) -> None:
    """Бюджет байтов — управление диском: вытеснение видно в счётчиках."""
    cache = EmbedCache(tmp_path / "embed_cache")
    chunk = _chunk("текст")
    cache.put(chunk.content_hash or "", MODEL, [0.1] * 64, embed_device=DEVICE, dim=64)
    entry = next((tmp_path / "embed_cache").rglob(f"*{SUFFIX}"))
    entry_size = entry.stat().st_size

    bounded = EmbedCache(tmp_path / "embed_cache", max_bytes=entry_size * 2)
    for index in range(3):
        bounded.put(
            content_hash(f"t{index}"), MODEL, [0.1] * 64, embed_device=DEVICE, dim=64
        )

    remaining = list((tmp_path / "embed_cache").rglob(f"*{SUFFIX}"))
    assert len(remaining) == 2
    assert bounded.stats()["evicted"] >= 1
    assert bounded.max_bytes == entry_size * 2


def test_budget_scan_is_not_paid_per_write(tmp_path: Path) -> None:
    """Учёт объёма — счётчиком: обход каталога не выполняется на каждую запись.

    Иначе бюджет стоил бы O(n²) ``stat`` на кэше в десятки тысяч чанков — дороже
    эмбеддинга, ради экономии которого кэш и существует.
    """
    root = tmp_path / "embed_cache"
    probe = EmbedCache(root)
    probe.put(content_hash("seed"), MODEL, [0.1] * 64, embed_device=DEVICE, dim=64)
    entry_size = next(root.rglob(f"*{SUFFIX}")).stat().st_size

    # Бюджет заведомо больше 50 записей — обход нужен только чтобы узнать базу.
    cache = EmbedCache(root, max_bytes=entry_size * 100)
    for index in range(50):
        cache.put(
            content_hash(f"t{index}"), MODEL, [0.1] * 64, embed_device=DEVICE, dim=64
        )

    assert cache.stats()["writes"] == 50
    assert cache.stats()["scans"] == 1
    assert cache.stats()["evicted"] == 0


# --------------------------------------------------------------------------- #
# 4. Наблюдаемость: счётчики в прогрессе и итоговой строке
# --------------------------------------------------------------------------- #


def test_progress_publishes_cached_count_and_hit_ratio(tmp_path: Path) -> None:
    """``embed_cached``/``embed_hit_ratio_pct`` — в тех же ключах, что ``embed_*``."""
    cache = EmbedCache(tmp_path / "embed_cache")
    total = EMBED_BATCH_CHUNKS + 4
    chunks = _chunks(total)
    # Прогреваем половину: ожидаемая доля попаданий — ровно половина продукта.
    warm = chunks[: total // 2]
    _embed_batched(
        FakeEmbedder(dim=4), warm, cache=cache, embed_model_id=MODEL,
        embed_device=DEVICE,
    )

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

    _embed_batched(
        FakeEmbedder(dim=4),
        chunks,
        on_batch=report,
        cache=cache,
        embed_model_id=MODEL,
        embed_device=DEVICE,
        census=census,
    )

    last = snapshots[-1]
    assert last["embed_chunks"] == total
    assert last["embed_chunks_total"] == total
    assert last["embed_cached"] == total // 2
    assert last["embed_hit_ratio_pct"] == 50
    # Темп — по фактически посчитанным чанкам, а не по «обработанным» (A3-сравнимость).
    assert census.embedded == total - total // 2
    assert last["embed_chunks_per_min"] == round(throughput.rate * 60.0)
    # Итоговая строка продукта называет «сколько из скольких» и «сколько из кэша».
    line = format_embed_progress(
        total, total, throughput.rate, 0, cached=last["embed_cached"]
    )
    assert line.startswith(f"{total}/{total} чанков, из кэша {total // 2} (50%)")


def test_final_line_without_cache_is_unchanged() -> None:
    """Без кэша строка прежняя: ``cached=None`` не добавляет ни одного слова."""
    assert format_embed_progress(3000, 3000, 0.85, 0) == "3000/3000 чанков, 0.85 чанк/с"
    assert format_embed_progress(0, 10, None, None) == "0/10 чанков"
    # Полное попадание: эмбеддинга не было — темпа нет, но экономия названа.
    assert (
        format_embed_progress(10, 10, None, 0, cached=10)
        == "10/10 чанков, из кэша 10 (100%)"
    )


# --------------------------------------------------------------------------- #
# 5. Сборка индекса из кэша побитово равна живому эмбеддингу
# --------------------------------------------------------------------------- #


def _build(vectors_cache: EmbedCache | None) -> FakeIndexBuilder:
    builder = FakeIndexBuilder()
    chunks = _chunks(EMBED_BATCH_CHUNKS + 2)
    errors: list[str] = []
    ok = _embed_and_build(
        FakeEmbedder(dim=4),
        builder,
        "p1",
        chunks,
        package_id=PACKAGE,
        chunker_version="chunker-1",
        embed_model_id=MODEL,
        embed_device=DEVICE,
        extractor_version="extractor-1",
        lexical_engine_version="fake-bm25-1",
        vector_store="fake",
        product_content_token="token",
        snapshot_date="2026-01-01",
        partition_build_id="pb_fixture",
        errors=errors,
        cache=vectors_cache,
    )
    assert ok is True and errors == []
    return builder


def test_index_built_from_cache_is_bitwise_equal_to_live(tmp_path: Path) -> None:
    """fp16-квантизация кэша тождественна квантизации сборщика — партиция не меняется."""
    live = _build(None)
    cache = EmbedCache(tmp_path / "embed_cache")
    first = _build(cache)  # холодный кэш: векторы посчитаны и записаны
    second = _build(cache)  # тёплый кэш: все векторы из кэша

    assert _fp16(first.vectors_by_product["p1"]) == _fp16(
        live.vectors_by_product["p1"]
    )
    assert _fp16(second.vectors_by_product["p1"]) == _fp16(
        live.vectors_by_product["p1"]
    )


# --------------------------------------------------------------------------- #
# 6. Сквозной прогон: новый ``pb``, тот же текст — эмбеддинга нет
# --------------------------------------------------------------------------- #


def _plan() -> RunPlan:
    return RunPlan(
        run_id=RUN_ID,
        products=["p1"],
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a"],
        params=[],
        module_weights={"mod_a": 100.0},
        renormalized_weights={"mod_a": 1.0},
        cohort=Cohort(
            case_id=PACKAGE,
            frozen_at=_dt.date(2026, 1, 1),
            entries=[
                CohortEntry(
                    product_id="p1",
                    role_overlap=1.0,
                    selection_reason="fixture",
                    anchor_relation=AnchorRelation.ANCHOR,
                )
            ],
        ),
        golden=[],
    )


class _Crawler:
    version = "crawler-1"

    def __init__(self, pages: dict[str, bytes]) -> None:
        self.pages = dict(pages)
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта, FetchPlan frozen
        self.calls.append(url)
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at=TS,
            fetch_mode=plan.fetch,
            lang="en",
        )


class _Extractor:
    version = "extractor-1"

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        return ExtractedDoc(
            text=page.content.decode("utf-8", errors="replace"),
            content_hash=content_hash(page.content),
            extractor_version=self.version,
            page_count=1,
        )


class _Chunker:
    version = "chunker-1"

    def __init__(self, per_doc: int) -> None:
        self.per_doc = per_doc

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        return [
            Chunk(
                chunk_id=f"{document_id}:{i}",
                chunk_no=i,
                text=f"{doc.text} #{i}",
                heading_path="h1",
                context_prefix="ctx",
                content_hash=content_hash(f"{document_id}:{i}".encode()),
            )
            for i in range(self.per_doc)
        ]


def _run_corpus(
    store: SqliteStore,
    tmp_path: Path,
    *,
    index_root: Path,
    cache: EmbedCache,
    embedder,
) -> object:  # noqa: ANN001 — PhaseResult
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id=PACKAGE,
            anchor="p1",
            cohort=PACKAGE,
            products=["p1"],
            package=PACKAGE,
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
        ),
    )
    return build_corpus(
        _plan(),
        _Crawler({URL_P1: BODY}),
        _Extractor(),
        _Chunker(5),
        store,
        urls={"p1": [URL_P1]},
        raw_root=tmp_path / "raw",
        index_root=index_root,
        embedder=embedder,
        index_builder=FakeIndexBuilder(),
        package_id=PACKAGE,
        run_id=RUN_ID,
        embed_cache=cache,
    )


def test_new_partition_reuses_vectors_from_cache(tmp_path: Path) -> None:
    """Новая партиция (другой ``index_store``) + тот же текст ⇒ ноль эмбеддингов.

    Это ровно сценарий «сменился ``extractor_version``/добавился документ»: ``pb`` другой,
    переиспользовать партицию нельзя, а векторы всех неизменившихся чанков уже есть.
    """
    cache = EmbedCache(tmp_path / "embed_cache")

    store1 = SqliteStore(":memory:")
    try:
        first_embedder = FakeEmbedder(dim=4)
        first = _run_corpus(
            store1,
            tmp_path,
            index_root=tmp_path / "index_store_a",
            cache=cache,
            embedder=first_embedder,
        )
        assert first.status == "ok"  # type: ignore[attr-defined]
        assert len(first_embedder.calls) == 1  # холодный кэш — эмбеддинг был
        first_notes = [
            note for note in first.log_entries if isinstance(note, str)  # type: ignore[attr-defined]
        ]
        assert any("из кэша 0 (0%)" in note for note in first_notes), first_notes
    finally:
        store1.close()

    store2 = SqliteStore(":memory:")
    try:
        second_embedder = FakeEmbedder(dim=4)
        second = _run_corpus(
            store2,
            tmp_path,
            index_root=tmp_path / "index_store_b",  # другая сборка ⇒ пересборка партиции
            cache=cache,
            embedder=second_embedder,
        )
        assert second.status == "ok"  # type: ignore[attr-defined]
        # Все векторы взяты из кэша: эмбеддер не вызван ни разу.
        assert second_embedder.calls == []
        notes = [
            note
            for note in second.log_entries  # type: ignore[attr-defined]
            if isinstance(note, str) and "эмбеддинг p1" in note
        ]
        assert notes, second.log_entries  # type: ignore[attr-defined]
        assert "из кэша 5 (100%)" in notes[0]
    finally:
        store2.close()
