"""Карта страниц: от читателя до номера страницы чанка и до артефакта отчёта (O36).

Приёмочная проверка фазы 1b плана ``26`` — «страница чанка известна». Цепочка проверяется
**целиком**, а не по звеньям: PDF → читатель ``text_layer`` (карта) → строка ``corpus_docs``
→ чтение из БД → ``char_start`` чанка → номер страницы. Порознь каждое звено может быть
зелёным, а вместе они не сходятся — именно так и теряется провенанс.

Второй предмет — артефакт ``outputs/page_provenance.csv``: документ, у которого карты нет,
а страниц больше одной, обязан быть виден. Иначе «цитата не может назвать страницу»
невидимо, и отсутствие провенанса читается как его наличие.

Тесты нейтральны (§1.3): PDF собирается тем же помощником, что у тестов адаптера.
"""

from __future__ import annotations

import csv
import datetime as _dt
from collections.abc import Iterator
from pathlib import Path

import pytest

from core.adapters.chunker_text import TextChunkerPort
from core.adapters.extract_pdf_text import PdfiumDocumentExtractor
from core.domain import (
    CorpusDoc,
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
)
from core.domain.page_map import page_number_for
from core.pipeline.page_provenance import (
    CSV_COLUMNS,
    SCOPE_DOC,
    SCOPE_PRODUCT,
    write_page_provenance_artifact,
)
from core.store.sqlite_store import SqliteStore
from tests.adapters.test_extract_pdf_text import _make_pdf

_HAS_PDFIUM = __import__("importlib").util.find_spec("pypdfium2") is not None

pytestmark = pytest.mark.skipif(not _HAS_PDFIUM, reason="pypdfium2 не установлен")

RUN_ID = "run-page-provenance"


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    with SqliteStore(":memory:") as instance:
        instance.create_run(
            RUN_ID,
            RunConfig(
                case_id="case",
                anchor="p1",
                cohort="cohort.yaml",
                products=["p1"],
                package="pkg",
                focus=Focus(),
                snapshot_date=_dt.date(2026, 1, 1),
                mode=RunMode.STRICT,
                ranking_policy=RankingPolicy.CAPABILITY_RANKED,
            ),
        )
        yield instance


def _doc(doc_id: str, **overrides: object) -> CorpusDoc:
    base: dict[str, object] = {
        "doc_id": doc_id,
        "product_id": "p1",
        "url": f"https://docs.test/p1/{doc_id}",
    }
    base.update(overrides)
    return CorpusDoc(**base)  # type: ignore[arg-type]


def test_page_of_a_chunk_is_known_end_to_end(
    store: SqliteStore, tmp_path: Path
) -> None:
    """3-страничный PDF: чанк, чей ``char_start`` лежит на странице 2, получает номер 2.

    Путь тот же, что у прогона: читатель → ``CorpusDoc`` → store → чтение обратно →
    ``char_start`` настоящего чанка (``TextChunkerPort``). Если бы карта терялась в БД или
    смещения были бы в другом пространстве, номер уехал бы на единицу — а это и есть
    дефект, ради устранения которого заведена колонка.
    """
    path = tmp_path / "three.pdf"
    # Страницы длиннее бюджета абзацного чанкера (1500 символов): иначе весь документ —
    # один кусок с ``char_start = 0``, и «страница чанка» проверялась бы на одном числе.
    path.write_bytes(
        _make_pdf([f"{word} " * 170 for word in ("alpha", "beta", "gamma")])
    )

    extracted = PdfiumDocumentExtractor().extract_path(path, filename_hint="three.pdf")
    assert extracted.page_map is not None
    assert [entry[0] for entry in extracted.page_map] == [1, 2, 3]

    doc = _doc(
        f"p1:{extracted.content_hash}",
        content_hash=extracted.content_hash,
        extractor_version=extracted.extractor_version,
        page_count=extracted.page_count,
        extracted_chars=len(extracted.text),
        page_map=extracted.page_map,
    )
    store.upsert_corpus_doc(doc)

    stored = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}[doc.doc_id]
    assert stored.page_map == extracted.page_map

    chunks = TextChunkerPort().chunk(extracted, doc.doc_id)
    second_page = [chunk for chunk in chunks if "beta" in chunk.text]
    assert second_page, "чанкер не дал куска второй страницы — проверка была бы пустой"
    chunk = second_page[0]
    assert chunk.char_start is not None
    assert page_number_for(chunk.char_start, stored.page_map) == 2
    # Соседние страницы не поехали: первая и третья называются своими номерами.
    first_page = next(c for c in chunks if "alpha" in c.text)
    assert page_number_for(first_page.char_start, stored.page_map) == 1
    last_page = [c for c in chunks if "gamma" in c.text][-1]
    assert page_number_for(last_page.char_start, stored.page_map) == 3


# ---------------------------------------------------------------------------
# Артефакт: outputs/page_provenance.csv
# ---------------------------------------------------------------------------


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_artifact_aggregates_and_names_documents_without_a_map(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Агрегат по продукту + строка на документ без карты, у которого страниц > 1."""
    store.upsert_corpus_doc(
        _doc("p1:mapped", page_count=3, extracted_chars=16, page_map=((1, 0, 6), (2, 8, 8), (3, 10, 16)))
    )
    store.upsert_corpus_doc(
        _doc(
            "p1:many",
            page_count=300,
            pages_converted=300,
            partial=True,
            partial_reason="pdfium: страниц не прочитано 0 из 300; текст прочитанных страниц",
        )
    )
    store.upsert_corpus_doc(_doc("p1:single", page_count=1))
    # Страницы не сообщены: «не измерено» не даёт права сказать «их больше одной».
    store.upsert_corpus_doc(_doc("p1:unknown"))

    target = tmp_path / "outputs" / "page_provenance.csv"
    written = write_page_provenance_artifact(store, RUN_ID, target)

    assert written == target
    rows = _read_rows(target)
    products = [row for row in rows if row["scope"] == SCOPE_PRODUCT]
    assert len(products) == 1
    product = products[0]
    assert product["product_id"] == "p1"
    assert product["docs"] == "4"
    assert product["docs_with_page_map"] == "1"
    assert product["pages_mapped"] == "3"
    assert product["docs_without_page_map"] == "3"

    docs = [row for row in rows if row["scope"] == SCOPE_DOC]
    assert [row["doc_id"] for row in docs] == ["p1:many"]
    assert docs[0]["url"] == "https://docs.test/p1/p1:many"
    assert docs[0]["page_count"] == "300"
    # Агрегаты в строке документа — пустая ячейка, а не ноль: это другая форма строки.
    assert docs[0]["docs"] == ""


def test_artifact_is_not_written_without_documents(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Пустой корпус → ``None`` и никакого файла: пустая таблица не артефакт."""
    target = tmp_path / "outputs" / "page_provenance.csv"
    assert write_page_provenance_artifact(store, RUN_ID, target) is None
    assert not target.exists()


def test_artifact_survives_a_legacy_db_without_the_column(tmp_path: Path) -> None:
    """БД без колонки ``page_map`` (read-only прошлой версии) — «карт нет», не падение."""
    with SqliteStore(":memory:") as store:
        store.create_run(
            RUN_ID,
            RunConfig(
                case_id="case",
                anchor="p1",
                cohort="cohort.yaml",
                products=["p1"],
                package="pkg",
                focus=Focus(),
                snapshot_date=_dt.date(2026, 1, 1),
                mode=RunMode.STRICT,
                ranking_policy=RankingPolicy.CAPABILITY_RANKED,
            ),
        )
        store.upsert_corpus_doc(_doc("p1:legacy", page_count=2))
        with store.conn:
            # Имитация строки, записанной до 3.21.0: колонка есть (схема одна), значения нет.
            store.conn.execute(
                "UPDATE corpus_docs SET page_map = NULL WHERE doc_id = 'p1:legacy'"
            )
        target = write_page_provenance_artifact(
            store, RUN_ID, tmp_path / "page_provenance.csv"
        )
        assert target is not None
        rows = _read_rows(target)
        assert [row["doc_id"] for row in rows if row["scope"] == SCOPE_DOC] == [
            "p1:legacy"
        ]


def test_csv_header_is_the_declared_contract(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Порядок колонок стабилен: файл читают глазами и скриптом."""
    store.upsert_corpus_doc(_doc("p1:one", page_count=1))
    target = write_page_provenance_artifact(store, RUN_ID, tmp_path / "page_provenance.csv")
    assert target is not None
    assert target.read_text(encoding="utf-8").splitlines()[0] == ",".join(CSV_COLUMNS)
