"""Персистенция частичного извлечения: ``corpus_docs`` и блок манифеста (I2/I2c).

Признак обязан пережить запись и чтение обратно: если он теряется в БД, отчёт и
покрытие снова начинают видеть «прочитано полностью» там, где прочитана часть.
Тесты нейтральны (§1.3), работают на ``:memory:``-БД.
"""

from __future__ import annotations

import datetime as _dt
import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from core.domain import (
    CorpusDoc,
    Focus,
    RankingPolicy,
    ReuseKind,
    RunConfig,
    RunManifest,
    RunMode,
)
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-partial"

@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    with SqliteStore(":memory:") as instance:
        instance.create_run(
            RUN_ID,
            RunConfig(
                case_id="case",
                anchor="p1",
                cohort="cohort.yaml",
                products=["p1", "p2"],
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

def test_corpus_doc_partial_roundtrips(store: SqliteStore) -> None:
    """Частичность, причина и числа страниц/символов читаются обратно как записаны."""
    doc = _doc(
        "p1:partial",
        partial=True,
        partial_reason="docling: PARTIAL_SUCCESS, получено страниц 7 из 2763",
        page_count=2763,
        pages_converted=7,
        extracted_chars=79_810,
    )
    store.upsert_corpus_doc(doc)

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}
    assert read["p1:partial"] == doc
    assert read["p1:partial"].pages_converted == 7
    assert read["p1:partial"].extracted_chars == 79_810

def test_corpus_doc_full_read_roundtrips_without_partial_marks(
    store: SqliteStore,
) -> None:
    """Полный документ не обрастает признаками: поведение прежних строк сохранено."""
    doc = _doc("p1:full")
    store.upsert_corpus_doc(doc)

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}["p1:full"]
    assert read.partial is False
    assert read.partial_reason is None
    assert read.pages_converted is None
    # В БД — 0, а не NULL: читатель не должен различать «старая строка» и «полная».
    row = store.conn.execute(
        "SELECT partial, partial_reason FROM corpus_docs WHERE doc_id = ?",
        ("p1:full",),
    ).fetchone()
    assert row["partial"] == 0
    assert row["partial_reason"] is None

def test_corpus_docs_partial_columns_migrate_additively(tmp_path: Path) -> None:
    """Старая ``corpus_docs`` без колонок получает их аддитивно (``schema_version`` цел)."""
    path = tmp_path / "legacy.db"
    legacy = sqlite3.connect(path)
    legacy.execute(
        "CREATE TABLE corpus_docs ("
        " doc_id TEXT PRIMARY KEY, product_id TEXT NOT NULL, url TEXT NOT NULL,"
        " fetched_at TEXT, content_hash TEXT, raw_path TEXT,"
        " extractor_version TEXT, render_mode TEXT, lang TEXT)"
    )
    legacy.execute(
        "INSERT INTO corpus_docs (doc_id, product_id, url)"
        " VALUES ('p1:old', 'p1', 'https://docs.test/p1/old')"
    )
    legacy.commit()
    legacy.close()

    store = SqliteStore(path)
    try:
        cols = {r["name"] for r in store.conn.execute("PRAGMA table_info(corpus_docs)")}
        assert {
            "partial",
            "partial_reason",
            "page_count",
            "pages_converted",
            "extracted_chars",
        } <= cols
    finally:
        store.close()

    # Старая строка читается как «полная»: выдумывать частичность из отсутствия
    # колонки нельзя, а падать на исторической БД — тем более.
    store = SqliteStore(path)
    try:
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
        read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}
        assert read["p1:old"].partial is False
        assert read["p1:old"].partial_reason is None
    finally:
        store.close()

def test_manifest_partial_docs_block_roundtrips(store: SqliteStore) -> None:
    """Блок ``partial_docs`` манифеста переживает ``persist_manifest``→``read_manifest``."""
    manifest = RunManifest(
        run_id=RUN_ID,
        case_id="case",
        created_at="2026-01-01T00:00:00Z",
        mode=RunMode.STRICT,
        methodology_version="0.1.0",
        package_version="pkg",
        catalog_version="cat",
        status_dict_version="sd",
        scoring_policy_version="sp",
        anchor_product_id="p1",
        partial_docs={
            "documents": 1,
            "by_product": {"p1": 1, "p2": 0},
            "pages_converted": 7,
            "pages_declared": 2763,
            "chars": 79_810,
        },
    )
    store.persist_manifest(RUN_ID, manifest)

    read = store.read_manifest(RUN_ID)
    assert read.partial_docs == manifest.partial_docs

def test_manifest_without_partial_docs_reads_empty_block(store: SqliteStore) -> None:
    """Прогон без частичных документов: блок пуст, а не выдуман нулями."""
    manifest = RunManifest(
        run_id=RUN_ID,
        case_id="case",
        created_at="2026-01-01T00:00:00Z",
        mode=RunMode.STRICT,
        methodology_version="0.1.0",
        package_version="pkg",
        catalog_version="cat",
        status_dict_version="sd",
        scoring_policy_version="sp",
        anchor_product_id="p1",
    )
    store.persist_manifest(RUN_ID, manifest)

    assert store.read_manifest(RUN_ID).partial_docs == {}

def test_corpus_doc_observation_provenance_roundtrips(store: SqliteStore) -> None:
    """``http_status``/``reuse_kind`` переживают запись и чтение обратно (16 §3.3).

    Без этого 304-документ снова выглядел бы живым 200, а правило «``❌`` нельзя публиковать
    на неподтверждённом наблюдении» (09 §11.2) осталось бы неисполнимым: проверять было бы
    нечего.
    """
    doc = _doc("p1:revalidated", http_status=304, reuse_kind=ReuseKind.REVALIDATED)
    store.upsert_corpus_doc(doc)

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}
    assert read["p1:revalidated"].http_status == 304
    assert read["p1:revalidated"].reuse_kind is ReuseKind.REVALIDATED

def test_corpus_doc_reused_bytes_carry_their_own_classification(
    store: SqliteStore,
) -> None:
    """``reused`` (retained-байты, сервер не отвечал) отличим от ``revalidated`` и ``live``."""
    store.upsert_corpus_doc(
        _doc("p1:reused", http_status=None, reuse_kind=ReuseKind.REUSED)
    )
    store.upsert_corpus_doc(_doc("p1:live", http_status=200, reuse_kind=ReuseKind.LIVE))

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}
    assert read["p1:reused"].reuse_kind is ReuseKind.REUSED
    assert read["p1:reused"].http_status is None
    assert read["p1:live"].reuse_kind is ReuseKind.LIVE
    assert read["p1:live"].http_status == 200

def test_corpus_doc_without_observation_reads_as_not_recorded(
    store: SqliteStore,
) -> None:
    """Документ без записи о наблюдении читается ``None``/``None`` — «не записано», не «живой»."""
    store.upsert_corpus_doc(_doc("p1:plain"))

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}["p1:plain"]
    assert read.http_status is None
    assert read.reuse_kind is None

def test_corpus_doc_version_mapping_roundtrips(store: SqliteStore) -> None:
    """Отображение версии в документ переживает запись и чтение обратно (16 §6).

    Это вторая форма адреса документа: ``chunk_id`` и ``CellEvidence.source_id`` несут
    версию (``dv_...``), а ``doc_id`` остаётся ``product_id:content_hash``. Без отображения
    ``source_id`` не адресует строку корпуса, и правило «``❌`` требует
    ``live|revalidated``» снимало бы подтверждённый ``❌`` как «наблюдение не записано».
    """
    store.upsert_corpus_doc(_doc("p1:versioned"), doc_version_id="dv_abc123")

    assert store.iter_corpus_doc_versions(RUN_ID, "p1") == {"dv_abc123": "p1:versioned"}

def test_corpus_doc_keeps_every_version_of_one_document(store: SqliteStore) -> None:
    """Две версии одного ``doc_id`` — **обе** в отображении, а не одна на строку.

    ``dv`` включает уровень извлечения, ``doc_id`` — нет: один и тот же текст на уровнях
    ``text`` и ``layout`` даёт один документ корпуса и две версии. Носитель поэтому не
    колонка строки: там поместилась бы ровно одна версия, и evidence второй получал бы
    ложное «наблюдение не записано».
    """
    for version in ("dv_text", "dv_layout"):
        store.upsert_corpus_doc(_doc("p1:same-text"), doc_version_id=version)

    assert store.iter_corpus_doc_versions(RUN_ID, "p1") == {
        "dv_text": "p1:same-text",
        "dv_layout": "p1:same-text",
    }
    # Строка корпуса одна: дедуп по ``content_hash`` не тронут.
    assert len({item.doc_id for item in store.iter_corpus_docs(RUN_ID)}) == 1

def test_corpus_doc_version_mapping_is_idempotent(store: SqliteStore) -> None:
    """Повторная запись той же версии перекрывает строку, а не плодит её (resume)."""
    store.upsert_corpus_doc(_doc("p1:versioned"), doc_version_id="dv_abc123")
    store.upsert_corpus_doc(_doc("p1:versioned"), doc_version_id="dv_abc123")

    assert store.iter_corpus_doc_versions(RUN_ID, "p1") == {"dv_abc123": "p1:versioned"}

def test_corpus_doc_without_version_has_no_mapping(store: SqliteStore) -> None:
    """Документ без объявленной версии отображения не получает — не догадка."""
    store.upsert_corpus_doc(_doc("p1:plain-version"))

    assert store.iter_corpus_doc_versions(RUN_ID, "p1") == {}

def test_corpus_doc_version_mapping_is_per_product(store: SqliteStore) -> None:
    """Отображение фильтруется продуктом: версия чужого продукта не подмешивается."""
    store.upsert_corpus_doc(_doc("p1:mine"), doc_version_id="dv_mine")
    store.upsert_corpus_doc(
        _doc("p2:other", product_id="p2"), doc_version_id="dv_other"
    )

    assert store.iter_corpus_doc_versions(RUN_ID, "p1") == {"dv_mine": "p1:mine"}

def test_legacy_corpus_docs_without_observation_columns_read(tmp_path: Path) -> None:
    """БД ``3.13.0`` без новых колонок: документ читается, наблюдение — «не записано».

    Чтение идёт через ``_has_column``: колонка, добавленная позже, в read-only файле
    отсутствует, и отсутствие обязано значить «значение не записано», а не сбой чтения.
    """
    path = tmp_path / "legacy.db"
    store = SqliteStore(path)
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
    store.upsert_corpus_doc(_doc("p1:old"))
    store.close()

    raw = sqlite3.connect(path)
    raw.execute("ALTER TABLE corpus_docs DROP COLUMN http_status")
    raw.execute("ALTER TABLE corpus_docs DROP COLUMN reuse_kind")
    raw.execute("DROP TABLE corpus_doc_version")
    raw.commit()
    raw.close()

    legacy = SqliteStore(path, read_only=True)
    try:
        read = {item.doc_id: item for item in legacy.iter_corpus_docs(RUN_ID)}
        assert read["p1:old"].http_status is None
        assert read["p1:old"].reuse_kind is None
        # Таблицы отображения в этой БД нет: «версий не записано» — не сбой чтения.
        assert legacy.iter_corpus_doc_versions(RUN_ID, "p1") == {}
    finally:
        legacy.close()

def test_reused_rewrite_does_not_downgrade_confirmed_observation(
    store: SqliteStore,
) -> None:
    """Resume не стирает состоявшееся наблюдение (16 §3.3: ``live`` → ``reused`` остаётся ``live``).

    В том же ``run_id`` документ уже был получен от сервера; перезапуск хоста перечитывает
    его из retained-байтов (``_resume_page``), и вторая запись говорит ``reused``. Если бы
    она перекрывала первую, ``live`` исчезло бы — вместе с основанием публиковать ``❌``
    (09 §11.2), и ``❌`` стал бы недоказуем без причины.
    """
    store.upsert_corpus_doc(_doc("p1:seen", http_status=200, reuse_kind=ReuseKind.LIVE))
    store.upsert_corpus_doc(
        _doc("p1:seen", http_status=None, reuse_kind=ReuseKind.REUSED)
    )

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}["p1:seen"]
    assert read.reuse_kind is ReuseKind.LIVE
    # ``http_status`` — факт о **цикле**: NULL значит «сервер в этом цикле не отвечал», а не
    # понижение. Сохранение 200 приписало бы серверу ответ, которого не было.
    assert read.http_status is None

def test_revalidated_rewrite_does_not_downgrade_to_reused(store: SqliteStore) -> None:
    """Тот же класс для 304: ``revalidated`` не понижается до ``reused``."""
    store.upsert_corpus_doc(
        _doc("p1:304", http_status=304, reuse_kind=ReuseKind.REVALIDATED)
    )
    store.upsert_corpus_doc(
        _doc("p1:304", http_status=None, reuse_kind=ReuseKind.REUSED)
    )

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}["p1:304"]
    assert read.reuse_kind is ReuseKind.REVALIDATED

def test_observation_upgrade_from_reused_to_live_is_written(
    store: SqliteStore,
) -> None:
    """Обратное движение разрешено: ``reused`` → ``live`` становится ``live``."""
    store.upsert_corpus_doc(
        _doc("p1:later", http_status=None, reuse_kind=ReuseKind.REUSED)
    )
    store.upsert_corpus_doc(
        _doc("p1:later", http_status=200, reuse_kind=ReuseKind.LIVE)
    )

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}["p1:later"]
    assert read.reuse_kind is ReuseKind.LIVE
    assert read.http_status == 200

def test_first_observation_from_null_is_written(store: SqliteStore) -> None:
    """``NULL`` → ``reused`` становится ``reused``: первая запись наблюдения не теряется.

    Иначе правило «не понижать» превратилось бы в «не писать вообще» для документов, у
    которых прежней классификации нет, — а именно так выглядит первый resume-проход.
    """
    store.upsert_corpus_doc(_doc("p1:fresh"))
    store.upsert_corpus_doc(
        _doc("p1:fresh", http_status=None, reuse_kind=ReuseKind.REUSED)
    )

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}["p1:fresh"]
    assert read.reuse_kind is ReuseKind.REUSED

def test_stale_is_written_over_confirmed_observation(store: SqliteStore) -> None:
    """``stale`` — **не** понижение ``reused``: у него собственный смысл, и он пишется.

    Условный GET пропущен по потолку ревалидации — это отдельная классификация, а не
    «перечитали retained-байты»; подменять её прежним ``live`` значило бы решать за
    писателя то, чего он не утверждал.
    """
    store.upsert_corpus_doc(_doc("p1:stale", http_status=200, reuse_kind=ReuseKind.LIVE))
    store.upsert_corpus_doc(
        _doc("p1:stale", http_status=None, reuse_kind=ReuseKind.STALE)
    )

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}["p1:stale"]
    assert read.reuse_kind is ReuseKind.STALE
