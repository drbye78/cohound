"""Сводка структурных блоков: ``runs.blocks_json`` (O36 фаза 2 плана ``26``).

Предмет — **два факта, которые нельзя складывать**: «блоки прочитаны» и «читатель блоков их
не даёт». Первое отвечает «сколько структуры в корпусе», второе — «на какой части корпуса
этот вопрос не задавался». Свести их к одному значило бы публиковать «блоков нет» там, где
их никто не искал.

Второй предмет — признак ``read|reconstructed``: он публикуется **раздельно**, потому что
смешать «прочитано» и «собрано моделью» значило бы потерять ровно тот ответ, ради которого
признак заведён.

Тесты нейтральны (§1.3): документы и адреса вымышленные, ``:memory:``-БД.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator

import pytest

from core.domain import (
    CorpusDoc,
    ExtractedBlock,
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
)
from core.pipeline.block_provenance import blocks_summary, human_line
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-block-summary"
PRODUCT = "p1"
OTHER_PRODUCT = "p2"
TEXT = "первая страница\n\nвторая страница"


def _block(index: int, start: int, end: int, **overrides: object) -> ExtractedBlock:
    fields: dict[str, object] = {
        "block_id": f"b{index}",
        "kind": "page",
        "char_start": start,
        "char_end": end,
        "order": index,
        "text_source": "read",
        "page_no": index + 1,
    }
    fields.update(overrides)
    return ExtractedBlock(**fields)  # type: ignore[arg-type]


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    """Корпус из четырёх документов: с блоками, с реконструкцией, без блоков и пустой."""
    with SqliteStore(":memory:") as instance:
        instance.create_run(
            RUN_ID,
            RunConfig(
                case_id="case",
                anchor=PRODUCT,
                cohort="cohort.yaml",
                products=[PRODUCT],
                package="pkg",
                focus=Focus(),
                snapshot_date=_dt.date(2026, 1, 1),
                mode=RunMode.STRICT,
                ranking_policy=RankingPolicy.CAPABILITY_RANKED,
            ),
        )
        instance.upsert_corpus_doc(
            CorpusDoc(
                doc_id=f"{PRODUCT}:h1",
                product_id=PRODUCT,
                url="https://docs.test/p1/one",
                content_hash="h1",
                extracted_chars=len(TEXT),
                blocks=(
                    _block(0, 0, 14),
                    _block(1, 16, 30),
                ),
            )
        )
        # Документ с реконструированным текстом (structured-механизм, если бы он давал блоки).
        instance.upsert_corpus_doc(
            CorpusDoc(
                doc_id=f"{PRODUCT}:h2",
                product_id=PRODUCT,
                url="https://docs.test/p1/two",
                content_hash="h2",
                extracted_chars=len(TEXT),
                blocks=(
                    _block(0, 0, 30, text_source="reconstructed"),
                ),
            )
        )
        # Читатель блоков не даёт вовсе (HTML/текст/Office/docling/шов).
        instance.upsert_corpus_doc(
            CorpusDoc(
                doc_id=f"{PRODUCT}:h3",
                product_id=PRODUCT,
                url="https://docs.test/p1/three",
                content_hash="h3",
                extracted_chars=len(TEXT),
                blocks=None,
            )
        )
        # Смотрел и не дал: «измерено, блоков нет» — ни в одной из двух категорий.
        instance.upsert_corpus_doc(
            CorpusDoc(
                doc_id=f"{PRODUCT}:h4",
                product_id=PRODUCT,
                url="https://docs.test/p1/four",
                content_hash="h4",
                extracted_chars=len(TEXT),
                blocks=(),
            )
        )
        yield instance


def test_summary_counts_blocks_kinds_and_sources(store: SqliteStore) -> None:
    """Числа считаются по фактам: документы с блоками, сами блоки, виды и происхождение."""
    summary = blocks_summary(store, RUN_ID, products=[PRODUCT, OTHER_PRODUCT])
    assert summary is not None
    assert summary["documents_with_blocks"] == 2
    assert summary["blocks"] == 3
    assert summary["documents_unmeasured"] == 1
    assert summary["by_text_source"] == {"read": 2, "reconstructed": 1}
    assert summary["by_kind"]["page"] == 3
    # Продукт плана без документов — нулями: «у него блоков нет» тоже факт (симметрия).
    assert summary["by_product"][OTHER_PRODUCT]["documents_with_blocks"] == 0
    assert summary["by_product"][OTHER_PRODUCT]["documents_unmeasured"] == 0
    assert summary["by_product"][PRODUCT]["blocks"] == 3


def test_empty_blocks_are_neither_measured_nor_unmeasured(store: SqliteStore) -> None:
    """``()`` — «измерено, блоков нет»: выдумывать ему категорию нельзя."""
    summary = blocks_summary(store, RUN_ID, products=[PRODUCT])
    assert summary is not None
    # Четыре документа: два с блоками, один ``None``, один ``()`` — второй ни в одной
    # категории, поэтому 2 + 1 ≠ 4.
    assert summary["documents_with_blocks"] + summary["documents_unmeasured"] == 3


def test_summary_is_none_without_corpus_documents() -> None:
    """Документов корпуса нет вовсе — ``None``, а не сводка с нулями."""
    with SqliteStore(":memory:") as empty:
        empty.create_run(
            RUN_ID,
            RunConfig(
                case_id="case",
                anchor=PRODUCT,
                cohort="cohort.yaml",
                products=[PRODUCT],
                package="pkg",
                focus=Focus(),
                snapshot_date=_dt.date(2026, 1, 1),
                mode=RunMode.STRICT,
                ranking_policy=RankingPolicy.CAPABILITY_RANKED,
            ),
        )
        assert blocks_summary(empty, RUN_ID, products=[PRODUCT]) is None


def test_unreadable_store_does_not_raise() -> None:
    """Сбой чтения — «не прочитали», а не падение отчёта (он вторичен к фазе)."""

    class _Broken:
        def iter_corpus_docs(self, run_id: str) -> object:
            raise RuntimeError("нет соединения")

    assert blocks_summary(_Broken(), RUN_ID) is None


def test_human_line_names_both_facts() -> None:
    """Строка оператора называет оба факта и различает происхождение текста."""
    line = human_line(
        {
            "blocks": 3,
            "documents_with_blocks": 2,
            "documents_unmeasured": 1,
            "by_text_source": {"read": 2, "reconstructed": 1},
        }
    )
    assert line is not None
    assert "3 в 2 документах" in line
    assert "прочитано 2" in line
    assert "реконструировано 1" in line
    assert "без разметки блоков 1" in line


def test_human_line_is_none_without_facts() -> None:
    """Сказать нечего — ``None``, а не пустая строка (нули не перечисляются)."""
    assert human_line(None) is None
    assert (
        human_line(
            {
                "blocks": 0,
                "documents_with_blocks": 0,
                "documents_unmeasured": 0,
                "by_text_source": {},
            }
        )
        is None
    )


def test_manifest_round_trips_blocks_summary(store: SqliteStore) -> None:
    """``blocks_json`` переживает запись и чтение; отсутствие читается как ``None``.

    ``None`` — «не записано» (документов не было либо строка прошлой схемы), ``{}``
    читалось бы как «сводка есть и она пуста», то есть как утверждение о блоках.
    """
    from core.domain.models import RunManifest

    def _manifest(**overrides: object) -> RunManifest:
        data: dict[str, object] = {
            "run_id": RUN_ID,
            "case_id": "case",
            "created_at": "2026-11-16T00:00:00+00:00",
            "mode": RunMode.STRICT,
            "methodology_version": "0.3.0",
            "package_version": "1.0.0",
            "catalog_version": "1.0.0",
            "status_dict_version": "1.0.0",
            "scoring_policy_version": "1.0.0",
            "anchor_product_id": PRODUCT,
        }
        data.update(overrides)
        return RunManifest(**data)  # type: ignore[arg-type]

    block = {
        "documents_with_blocks": 2,
        "blocks": 3,
        "documents_unmeasured": 1,
        "by_kind": {"page": 3},
        "by_text_source": {"read": 2, "reconstructed": 1},
        "by_product": {PRODUCT: {"blocks": 3}},
    }
    store.persist_manifest(RUN_ID, _manifest(blocks=block))
    stored = store.read_manifest(RUN_ID)
    assert stored is not None
    assert stored.blocks == block

    store.persist_manifest(RUN_ID, _manifest())
    stored = store.read_manifest(RUN_ID)
    assert stored is not None
    assert stored.blocks is None


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
