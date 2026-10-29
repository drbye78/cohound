"""Граница фазы корпуса: результат не пересекает её в памяти (Д-2, работы A3/A4).

Дефект Д-2 (`docs/architecture/10-extraction-pipeline-architecture.md` §4) состоял в двух
вещах: ``_ProductIngest`` держал ``docs``/``chunks`` **всех** продуктов до конца фазы
(работа A3), а ``IndexBuild.docs`` вёз полный список документов в ``PhaseResult``, который
``Runner`` держит весь прогон (работа A4). Проверяется именно это, а не внутренняя
раскладка:

* к записи второго продукта чанки первого уже не удерживаются — пик памяти фазы
  определяется **одним** продуктом, а не их суммой (weakref: если бы список жил, ссылка
  не умерла бы);
* после возврата фазы не удерживается ни одного чанка, а ``IndexBuild`` не несёт списка
  документов вовсе (только счётчик ``doc_count``);
* документы и чанки читаются из стора, и провенанс сборки (``content_digest`` /
  ``partition_build_id``) **выводится заново** из персистентных строк — то есть
  освобождение произошло после того, как все входы ``pb`` посчитаны: идентичность сборки
  и ``run_id`` от правки не меняются;
* перенормировка сборки раунда (``serp_loop._normalize_build``) переносит счётчики, а
  документы берутся из стора (``store.iter_corpus_docs``), а не из фазы.

Харнесс переиспользуется из ``test_corpus`` (``tests/pipeline`` без ``__init__.py``,
поэтому pytest кладёт каталог в ``sys.path``). Данные нейтральны (§1.3).
"""

from __future__ import annotations

import gc
import weakref
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from test_corpus import RUN_ID, SqliteStore, _build, _build_of, _make_run

from core.domain.enums import EmbedDevice
from core.pipeline import corpus as corpus_module
from core.pipeline.corpus import (
    IndexBuild,
    _derive_partition_build_id,
    _product_content_token,
)
from core.pipeline.serp_loop import _normalize_build

URL_P1 = "https://docs.test/p1"
URL_P2 = "https://docs.test/p2"

def test_previous_product_is_released_before_the_next_is_persisted(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Пик памяти не растёт с числом продуктов: первый продукт отпущен до записи второго."""
    refs: dict[str, list[weakref.ref[Any]]] = {"p1": [], "p2": []}
    doc_refs: dict[str, list[weakref.ref[Any]]] = {"p1": [], "p2": []}
    #: Жив ли хоть один чанк/документ ``p1`` в момент записи чанков ``p2``.
    p1_alive_at_p2_persist: list[bool] = []

    real_persist_chunks = corpus_module._persist_chunks
    real_persist_docs = corpus_module._persist_docs

    def docs_spy(store: Any, docs: Any, errors: list[str], **kwargs: Any) -> None:
        # ``_persist_docs`` вызывается инкрементально, по документу: ссылка записывается,
        # пока документ ещё часть памяти продукта.
        doc_refs[docs[0].product_id].extend(weakref.ref(doc) for doc in docs)
        real_persist_docs(store, docs, errors, **kwargs)

    def spy(store: Any, chunks: Any, errors: list[str]) -> bool:
        product_chunks = list(chunks)
        if product_chunks:
            product_id = product_chunks[0].product_id
            if product_id == "p2":
                # Сборка мусора не обязательна для точного подсчёта ссылок, но делает
                # утверждение независимым от порядка освобождения циклов.
                gc.collect()
                p1_alive_at_p2_persist.append(
                    any(ref() is not None for ref in refs["p1"])
                    or any(ref() is not None for ref in doc_refs["p1"])
                )
            refs[product_id].extend(weakref.ref(chunk) for chunk in product_chunks)
        return real_persist_chunks(store, product_chunks, errors)

    monkeypatch.setattr(corpus_module, "_persist_chunks", spy)
    monkeypatch.setattr(corpus_module, "_persist_docs", docs_spy)

    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        result = _build(
            tmp_path,
            pages={URL_P1: b"one", URL_P2: b"two"},
            urls={"p1": [URL_P1], "p2": [URL_P2]},
            store=store,
            run_id=RUN_ID,
        )
        assert {doc.url for doc in store.iter_corpus_docs(RUN_ID)} == {
            URL_P1,
            URL_P2,
        }
        # `budget` — и очередь в p2 тоже отработала (иначе проверка была бы вакуумной).
        assert refs["p1"] and refs["p2"]
        # A3: чанки предыдущего продукта к записи следующего уже не удерживаются фазы.
        assert p1_alive_at_p2_persist and not any(p1_alive_at_p2_persist)
        # После возврата фазы не удерживается ни один чанк и ни один документ: они
        # живут только в БД.
        gc.collect()
        assert not any(
            ref() is not None for bucket in refs.values() for ref in bucket
        )
        assert not any(
            ref() is not None for bucket in doc_refs.values() for ref in bucket
        )
    finally:
        store.close()

    build = _build_of(result)
    # A4: документы не пересекают границу фазы — ни списком, ни полем.
    assert not hasattr(build, "docs")
    assert build.doc_count == 2
    assert build.chunk_count == 2

def test_build_provenance_is_recomputable_from_persisted_chunks(tmp_path: Path) -> None:
    """``content_digest``/``pb`` — функции персистентных чанков, а не памяти фазы.

    Если бы освобождение сдвинуло порядок/состав входов продукт-токена, пересчёт из
    ``corpus_chunks`` разошёлся бы с манифестом: побайтовое равенство ``pb`` и есть та
    проверка, которую требует правка («``pb`` и ``run_id`` не меняются»).
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        result = _build(
            tmp_path,
            pages={URL_P1: b"one", URL_P2: b"two"},
            urls={"p1": [URL_P1], "p2": [URL_P2]},
            store=store,
            run_id=RUN_ID,
        )
        build = _build_of(result)
        assert len(build.entries) == 2
        for entry in build.entries:
            rows = store.conn.execute(
                "SELECT chunk_id, content_hash FROM corpus_chunks "
                "WHERE product_id = ? ORDER BY chunk_id",
                (entry.product_id,),
            ).fetchall()
            assert rows, entry.product_id
            token = _product_content_token(
                [
                    SimpleNamespace(
                        chunk_id=row["chunk_id"], content_hash=row["content_hash"]
                    )
                    for row in rows
                ]
            )
            assert entry.content_digest == token
            assert entry.partition_build_id == _derive_partition_build_id(
                package_id=entry.package_id or "",
                product_id=entry.product_id,
                chunker_version=entry.chunker_version or "",
                embed_model_id=entry.embed_model_id or "",
                # 16 §5.1: устройство счёта входит в материал ``pb``. У ``IndexManifestEntry``
                # своего поля нет (это наблюдаемое поле партиции, не идентичность), а прогон
                # здесь не объявляет ось ⇒ действует объявленный дефолт ``cpu``.
                embed_device=EmbedDevice.CPU.value,
                extractor_version=entry.extractor_version or "",
                lexical_engine_version=entry.lexical_engine_version or "",
                vector_store=entry.vector_store or "",
                product_content_token=token,
            )
    finally:
        store.close()

def test_round_normalization_carries_counters_not_documents() -> None:
    """``serp_loop`` не переносит документы из фазы: раунд получает счётчики.

    Документы раунду доступны из стора (``store.iter_corpus_docs(run_id)``) — петля и так
    получает ``store``/``run_id``. Проверяется перенормировка, где раньше копировался
    ``build.docs`` (это и был единственный потребитель поля, Д-2/A4).
    """
    build = IndexBuild(index_build_id="ib_a", doc_count=7, chunk_count=9)
    normalized = _normalize_build(build, "ib_b", "pkg")
    assert normalized.index_build_id == "ib_b"
    assert normalized.doc_count == 7
    assert normalized.chunk_count == 9
    assert not hasattr(normalized, "docs")
