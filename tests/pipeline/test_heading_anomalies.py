"""Аномалии ``heading_path``: агрегат, топ-виновники и резолв ``cb_``→``pb_`` (O28).

Проверяется главный дефект: длинный ``heading_path`` (до 3 668 символов) и
контекст-тяжёлые чанки существуют в корпусе, но отчёт их не видел. Числа сверяются с
ручным подсчётом, а не с самой функцией.

Тесты нейтральны (§1.3): ни одного названия продукта/вендора.
"""

from __future__ import annotations

import csv
from collections.abc import Iterator
from pathlib import Path

import pytest

from core.domain import IndexManifestEntry
from core.pipeline.heading_anomalies import (
    CSV_COLUMNS,
    HEADING_CHAR_BUCKETS,
    heading_anomaly_summary,
    human_line,
    not_computed_note,
    write_heading_anomalies_artifact,
)
from core.ports.store import RunIndexRef
from core.ports.types import Chunk

RUN_ID = "run-heading"


def _chunk(chunk_id: str, heading: str, text: str = "t" * 1000, prefix: str = "") -> Chunk:
    return Chunk(
        chunk_id=chunk_id,
        chunk_no=0,
        text=text,
        heading_path=heading,
        context_prefix=prefix,
        content_hash="0" * 16,
    )


def _fixture_chunks() -> list[Chunk]:
    """Шесть чанков: пустой/короткий/длинный ``heading_path`` и тяжёлый контекст.

    Ручной подсчёт (``count_tokens = len``):

    * ``> 200``: ``dv_b`` (1) + ``dv_c`` (2) = **3 чанка на 2 документах**;
    * ``> 500``: ``dv_c`` (2) = **2 чанка на 1 документе**;
    * контекст тяжелее текста: ``dv_c`` оба (501/600 > 0.25×1000) + ``dv_d``
      (14 > 0.25×8) = **3 чанка**.
    """
    return [
        _chunk("p1:dv_a:0", "", text="abc"),
        _chunk("p1:dv_a:1", "x" * 200),
        _chunk("p1:dv_b:0", "y" * 201),
        _chunk("p2:dv_c:0", "z" * 501),
        _chunk("p2:dv_c:1", "w" * 600),
        _chunk("p3:dv_d:0", "short", text="abcdefgh", prefix="pppppppp"),
    ]


def _len_tokens(text: str) -> int:
    """Счётчик «одна позиция = один токен»: только для ручной сверки чисел."""
    return len(text)


def test_summary_counts_buckets_documents_and_top_offender() -> None:
    """Числа бакетов и состав топа совпадают с ручным подсчётом фикстуры."""
    summary = heading_anomaly_summary(
        _fixture_chunks(), count_tokens=_len_tokens, preview_chars=20
    )

    assert summary["chunks_total"] == 6
    assert summary["longest_heading_chars"] == 600

    over_200 = summary["buckets"]["200"]
    assert (over_200["chunks"], over_200["documents"]) == (3, 2)
    assert over_200["share"] == pytest.approx(3 / 6)
    # При равном числе чанков порядок задаёт doc_id (детерминированно).
    assert [entry["doc_id"] for entry in over_200["top"]] == ["p2:dv_c", "p1:dv_b"]
    assert over_200["top"][0]["chunks"] == 2
    assert over_200["top"][0]["max_heading_chars"] == 600
    assert over_200["top"][0]["longest_heading_preview"] == "w" * 20 + "…"
    assert over_200["top"][0]["preview_truncated"] is True
    assert over_200["top"][0]["sample_chunk_id"] == "p2:dv_c:1"

    over_500 = summary["buckets"]["500"]
    assert (over_500["chunks"], over_500["documents"]) == (2, 1)
    assert over_500["share"] == pytest.approx(2 / 6)
    assert [entry["doc_id"] for entry in over_500["top"]] == ["p2:dv_c"]


def test_summary_publishes_zero_not_missing() -> None:
    """Нет чанков за порогом — опубликованный ноль, а не отсутствующий ключ."""
    summary = heading_anomaly_summary(
        [_chunk("p1:dv_a:0", "короткий", text="t" * 100)], count_tokens=_len_tokens
    )

    for threshold in HEADING_CHAR_BUCKETS:
        bucket = summary["buckets"][str(threshold)]
        assert bucket["chunks"] == 0
        assert bucket["documents"] == 0
        assert bucket["share"] == 0.0
        assert bucket["top"] == []


def test_context_chars_are_measured_without_tokenizer() -> None:
    """Символьный замер есть всегда: ``count_tokens`` не нужен (ручной подсчёт фикстуры).

    Тяжелее текста (``len(context) > 0.25 × len(text)``): ``dv_c`` оба чанка
    (501/600 > 250) и ``dv_d`` (14 > 2) — **3 из 6**. ``dv_a:1`` (200 ≤ 250) и
    ``dv_b:0`` (201 ≤ 250) — нет; пустой контекст — нет.
    """
    summary = heading_anomaly_summary(_fixture_chunks())

    chars = summary["context"]["chars"]
    assert chars["chunks_total"] == 6
    assert chars["heavy_chunks"] == 3
    assert chars["heavy_share"] == pytest.approx(3 / 6)
    assert chars["limit"] == pytest.approx(0.25)


def test_context_without_counter_is_not_measured_not_zero() -> None:
    """Без счётчика токенов токенный замер не измерен: ``None`` + причина, а не ноль.

    Символьный замер при этом измерен (см. предыдущий тест) — блоки не смешиваются.
    """
    summary = heading_anomaly_summary(_fixture_chunks())

    tokens = summary["context"]["tokens"]
    assert tokens["measured"] is False
    assert tokens["heavy_chunks"] is None
    assert tokens["heavy_share"] is None
    assert tokens["reason"] == "tokenizer_unavailable"
    assert summary["context"]["chars"]["heavy_chunks"] == 3
    # Символьные бакеты от отсутствия токенизатора не страдают.
    assert summary["buckets"]["200"]["chunks"] == 3


def test_context_counts_chunks_heavier_than_text() -> None:
    """Формула харнесса: ``context_tokens > 0.25 × text_tokens`` (строгое ``>``)."""
    summary = heading_anomaly_summary(_fixture_chunks(), count_tokens=_len_tokens)

    tokens = summary["context"]["tokens"]
    assert tokens["measured"] is True
    assert tokens["heavy_chunks"] == 3
    assert tokens["heavy_share"] == pytest.approx(3 / 6)
    assert tokens["limit"] == pytest.approx(0.25)
    assert tokens["reason"] is None
    # Символьный замер считается независимо и даёт свой числитель.
    assert summary["context"]["chars"]["heavy_chunks"] == 3


def test_context_counter_failure_is_visible_not_zero() -> None:
    """Упавший счётчик — «не измерено» с причиной, а не выдуманный ноль."""
    def broken(_text: str) -> int:
        raise RuntimeError("tokenizer closed")

    summary = heading_anomaly_summary(_fixture_chunks(), count_tokens=broken)

    tokens = summary["context"]["tokens"]
    assert tokens["measured"] is False
    assert tokens["heavy_chunks"] is None
    assert tokens["heavy_share"] is None
    assert str(tokens["reason"]).startswith("tokenizer_failed: RuntimeError")
    # Символьный замер от сбоя токенизатора не страдает.
    assert summary["context"]["chars"]["heavy_chunks"] == 3


def test_preview_is_truncated_but_full_length_is_published() -> None:
    """Обрезка только в публикации: длина остаётся полной, строка — превью."""
    long_heading = "h" * 3668
    summary = heading_anomaly_summary(
        [_chunk("p1:dv_a:0", long_heading)], preview_chars=40
    )

    entry = summary["buckets"]["500"]["top"][0]
    assert entry["max_heading_chars"] == 3668
    assert entry["preview_truncated"] is True
    assert entry["longest_heading_preview"] == "h" * 40 + "…"


class _FakeStore:
    """Минимальный store: резолв ``cb_``→``pb_`` через манифест + чанки по партициям."""

    def __init__(
        self,
        *,
        composition: str = "cb_test",
        entries: list[IndexManifestEntry] | None = None,
        chunks_by_build: dict[str, list[Chunk]] | None = None,
        chunks_error: bool = False,
    ) -> None:
        self.composition = composition
        self.entries = entries or []
        self.chunks_by_build = chunks_by_build or {}
        self.chunks_error = chunks_error
        self.chunks_builds: list[str] = []

    def read_run_index_ref(self, run_id: str) -> RunIndexRef | None:
        return RunIndexRef(run_id=run_id, index_build_id=self.composition)

    def iter_index_manifest(self, index_build_id: str) -> Iterator[IndexManifestEntry]:
        assert index_build_id == self.composition
        yield from self.entries

    def iter_chunks(self, build_id: str, *, product_id: str | None = None) -> Iterator[Chunk]:
        self.chunks_builds.append(build_id)
        if self.chunks_error:
            raise RuntimeError("db closed")
        for chunk in self.chunks_by_build.get(build_id, []):
            if product_id is None or chunk.chunk_id.startswith(f"{product_id}:"):
                yield chunk

    def resolve_source_url(self, source_id: str) -> str | None:
        return f"https://docs.test/{source_id}"


def _entry(product_id: str, partition: str | None) -> IndexManifestEntry:
    return IndexManifestEntry(
        index_build_id="cb_test",
        partition_build_id=partition,
        product_id=product_id,
    )


def test_writer_resolves_composition_build_to_partition_chunks(tmp_path: Path) -> None:
    """``runs.index_build_id`` = ``cb_``, чанки лежат под ``pb_``: агрегат их находит."""
    store = _FakeStore(
        entries=[_entry("p1", "pb_1"), _entry("p2", "pb_2")],
        chunks_by_build={
            "pb_1": [_chunk("p1:dv_a:0", "y" * 201)],
            "pb_2": [_chunk("p2:dv_c:0", "z" * 501), _chunk("p2:dv_c:1", "w" * 501)],
        },
    )

    result = write_heading_anomalies_artifact(
        store, RUN_ID, tmp_path / "heading_anomalies.csv", count_tokens=_len_tokens
    )

    assert result["written"] is True
    assert result["reason"] is None
    assert result["path"] == str(tmp_path / "heading_anomalies.csv")
    summary = result["summary"]
    assert summary is not None
    assert summary["chunks_total"] == 3  # не ноль: чанки найдены под pb_
    assert summary["buckets"]["200"]["chunks"] == 3
    # Резолв шёл по партициям, а не по композиции.
    assert store.chunks_builds == ["pb_1", "pb_2"]
    assert "cb_test" not in store.chunks_builds
    # URL виновника разрешён по одному чанку на документ.
    assert summary["buckets"]["200"]["top"][0]["url"] == "https://docs.test/p2:dv_c:0"


def test_writer_reports_unresolved_build_as_not_computed(tmp_path: Path) -> None:
    """Сборка не разрешилась — «не посчитано» с причиной, а не ноль и не файл."""
    class _NoRef(_FakeStore):
        def read_run_index_ref(self, run_id: str) -> RunIndexRef | None:
            return None

    target = tmp_path / "heading_anomalies.csv"
    result = write_heading_anomalies_artifact(_NoRef(), RUN_ID, target)

    assert result["written"] is False
    assert result["summary"] is None
    assert result["path"] is None
    assert result["reason"] == "build_id_unknown: runs.index_build_id пуст"
    assert not target.exists()


def test_writer_reports_missing_manifest_as_not_computed(tmp_path: Path) -> None:
    """Пустой манифест — тоже «не посчитано», а не «аномалий нет»."""
    store = _FakeStore(entries=[])

    result = write_heading_anomalies_artifact(store, RUN_ID, tmp_path / "h.csv")

    assert result["written"] is False
    assert result["summary"] is None
    assert str(result["reason"]).startswith("manifest_empty")


def test_writer_survives_chunks_read_failure(tmp_path: Path) -> None:
    """``iter_chunks`` бросает — пустой результат без исключения (отчёт вторичен)."""
    store = _FakeStore(
        entries=[_entry("p1", "pb_1")],
        chunks_by_build={"pb_1": [_chunk("p1:dv_a:0", "y" * 201)]},
        chunks_error=True,
    )
    target = tmp_path / "heading_anomalies.csv"

    result = write_heading_anomalies_artifact(store, RUN_ID, target)

    assert result["written"] is False
    assert result["summary"] is None
    assert str(result["reason"]).startswith("chunks_read_failed: RuntimeError")
    assert not target.exists()


@pytest.mark.parametrize("reason", ["store_missing", "run_id_missing", "build_id_unknown: пусто"])
def test_not_computed_note_is_silent_when_there_is_no_subject(reason: str) -> None:
    """Нет корпусного индекса — нет предмета: замечание было бы шумом.

    Прогон без чанков не имеет ``heading_path``, который можно измерить. Строка
    «не посчитано» в каждом таком отчёте приучила бы не читать ``notes`` вовсе.
    """
    assert not_computed_note(reason) is None


@pytest.mark.parametrize(
    "reason",
    [
        "chunks_read_failed: RuntimeError: db closed",
        "chunks_reader_unavailable: у store нет iter_chunks",
        "manifest_unavailable: у store нет iter_index_manifest (build=cb_1)",
    ],
)
def test_not_computed_note_speaks_when_subject_exists(reason: str) -> None:
    """Предмет есть, числа нет — оператор обязан это увидеть, иначе «нет аномалий»."""
    note = not_computed_note(reason)

    assert note is not None
    assert "не посчитаны" in note
    assert reason in note
    assert "не ноль" in note


def test_not_computed_note_is_silent_when_computation_succeeded() -> None:
    """Посчитано (``reason=None``) — замечания нет."""
    assert not_computed_note(None) is None


def test_writer_writes_csv_with_bucket_rows_and_top_documents(tmp_path: Path) -> None:
    """CSV: строка на (порог × документ-виновник) + строка нуля для пустого бакета."""
    store = _FakeStore(
        entries=[_entry("p1", "pb_1")],
        chunks_by_build={
            "pb_1": [
                _chunk("p1:dv_b:0", "y" * 201),
                _chunk("p1:dv_c:0", "z" * 501),
                _chunk("p1:dv_c:1", "w" * 600),
            ]
        },
    )
    target = tmp_path / "heading_anomalies.csv"

    result = write_heading_anomalies_artifact(
        store, RUN_ID, target, count_tokens=_len_tokens
    )

    assert result["written"] is True
    with target.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert tuple(rows[0]) == CSV_COLUMNS
    # Бакет 200: два виновника (dv_c с 2 чанками, dv_b с 1).
    bucket_200 = [row for row in rows if row["bucket_chars"] == "200"]
    assert [row["doc_id"] for row in bucket_200] == ["p1:dv_c", "p1:dv_b"]
    assert bucket_200[0]["doc_chunks"] == "2"
    assert bucket_200[0]["max_heading_chars"] == "600"
    assert bucket_200[0]["url"] == "https://docs.test/p1:dv_c:1"
    assert bucket_200[0]["chunks_total"] == "3"
    assert bucket_200[0]["bucket_chunks"] == "3"
    assert bucket_200[0]["bucket_documents"] == "2"
    # Символьный и токенный замеры публикуются раздельно: здесь оба измерены.
    assert bucket_200[0]["context_limit"] == "0.25"
    assert bucket_200[0]["context_chars_heavy_chunks"] == "2"
    assert float(bucket_200[0]["context_chars_heavy_share"]) == pytest.approx(2 / 3)
    assert bucket_200[0]["context_tokens_heavy_chunks"] == "2"
    assert float(bucket_200[0]["context_tokens_heavy_share"]) == pytest.approx(2 / 3)
    # Бакет 500: один виновник.
    bucket_500 = [row for row in rows if row["bucket_chars"] == "500"]
    assert [row["doc_id"] for row in bucket_500] == ["p1:dv_c"]
    assert bucket_500[0]["doc_chunks"] == "2"


def test_writer_keeps_zero_buckets_in_csv(tmp_path: Path) -> None:
    """Бакет без виновников — строка с нулём, а не пропуск: ноль тоже измерение."""
    store = _FakeStore(
        entries=[_entry("p1", "pb_1")],
        chunks_by_build={"pb_1": [_chunk("p1:dv_a:0", "короткий", text="t" * 100)]},
    )
    target = tmp_path / "heading_anomalies.csv"

    result = write_heading_anomalies_artifact(store, RUN_ID, target)

    assert result["written"] is True
    with target.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert [row["bucket_chars"] for row in rows] == ["200", "500"]
    for row in rows:
        assert row["bucket_chunks"] == "0"
        assert row["bucket_documents"] == "0"
        assert row["doc_id"] == ""
        assert row["doc_rank"] == ""
        # Символьный замер есть всегда — здесь измеренный ноль.
        assert row["context_chars_heavy_chunks"] == "0"
        assert float(row["context_chars_heavy_share"]) == 0.0
        # Токенного замера не было: пустая строка («не измерено»), не ноль.
        assert row["context_tokens_heavy_chunks"] == ""
        assert row["context_tokens_heavy_share"] == ""


def test_human_line_names_offender_and_context() -> None:
    """Строка оператора называет бакеты, виновника и оба замера контекста."""
    summary = heading_anomaly_summary(
        _fixture_chunks(), count_tokens=_len_tokens, preview_chars=20
    )

    line = human_line(summary)

    assert line is not None
    assert "> 200 симв.: 3 чанков" in line
    assert "> 500 симв.: 2 чанков" in line
    assert "контекст длиннее текста (символы): 3 чанков" in line
    assert "контекст тяжелее текста (токены): 3 чанков" in line
    assert "виновник: p2:dv_c" in line


def test_human_line_without_tokenizer_names_chars_and_admits_tokens() -> None:
    """Без счётчика: символьный факт назван, токенный — «не измерен», не выдуман."""
    summary = heading_anomaly_summary(_fixture_chunks())

    line = human_line(summary)

    assert line is not None
    assert "контекст длиннее текста (символы): 3 чанков" in line
    assert "контекст тяжелее текста" not in line
    assert "контекст в токенах не измерен: tokenizer_unavailable" in line


def test_human_line_is_silent_without_chunks() -> None:
    """Нет чанков — нет строки; «не посчитано» сюда не подставляется."""
    assert human_line(None) is None
    assert human_line({"chunks_total": 0, "buckets": {}, "context": {}}) is None
