"""Граница ``runs`` — полоса «корпус» (16 §3.2/§3.3/§5/§6, 09 §6.3).

Проверяется ровно то, что полоса объявила измеримым, и каждый тест называет дефект,
который он закрывает:

* ``pb``/переиспользование: устройство счёта — часть условий сборки; отпечаток содержимого
  доказывается **пересчётом с диска**, а не манифестом; ``partition-1`` читается, но не
  переиспользуется;
* ``chunk_id`` адресует **версию документа** (``doc_version_id``), поэтому один текст на
  разных уровнях извлечения — разные адреса, а ``parse_source_id`` продолжает работать;
* ``corpus_docs.http_status``/``reuse_kind`` — происхождение наблюдения **этого** прогона
  (live/200, revalidated/304, reused/NULL);
* ``run_product`` — строка на границе продукта (``running``) и финальная после Pass B;
* дефект краула 1 (зарядка бюджета — владельцу, а не первому корню-претенденту),
  дефект краула 3 (заряженный URL обязан быть дренируемым — ``pangolin`` 600/600).

Харнесс — из ``tests/pipeline/test_corpus.py`` (те же детерминированные фейки портов):
``tests/pipeline`` без ``__init__.py``, поэтому pytest кладёт каталог в ``sys.path``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from test_corpus import (
    RUN_ID,
    URL_P1_A,
    URL_P2_A,
    SqliteStore,
    _BombCrawler,
    _build,
    _build_of,
    _Crawler,
    _docs,
    _Embedder,
    _Extractor,
    _items,
    _make_run,
    _StatefulCrawler,
    requires_numpy,
)

from core.adapters.index_npy import NpyIndexBuilder
from core.config.domain_profiles import DomainProfile
from core.domain import (
    CrawlRunConfig,
    DocumentLevel,
    EmbedDevice,
    EmbedPrecision,
    ReuseKind,
    RunProductStatus,
)
from core.pipeline.corpus_index import (
    _derive_partition_build_id,
    _partition_reuse_reason,
)
from core.ports.state import UrlStateRow
from core.store.state_store import SqliteStateStore
from core.util.ids import parse_source_id
from core.util.index_partition import CHUNK_IDS_FILE, VECTORS_FILE, partition_dir

# --------------------------------------------------------------------------- #
# 1. Идентичность партиции: устройство счёта и отпечаток содержимого (16 §5)
# --------------------------------------------------------------------------- #

def _built_partition(tmp_path: Path) -> tuple[Path, Any]:
    """Реальная партиция, построенная сборщиком: манифест и артефакты — как в прогоне."""
    index_root = tmp_path / "index_store"
    result = _build(
        tmp_path / "raw",
        pages={URL_P1_A: b"identity page"},
        urls={"p1": [URL_P1_A]},
        index_root=index_root,
        embedder=_Embedder(),
        index_builder=NpyIndexBuilder(index_root),
    )
    assert result.errors == []
    return index_root, _build_of(result).entries[0]


def _reuse_reason(index_root: Path, entry: Any, **overrides: Any) -> str | None:
    """Причина отказа в переиспользовании для **этой** партиции (аргументы — из входа)."""
    kwargs: dict[str, Any] = {
        "index_root": index_root,
        "package_id": entry.package_id or "",
        "partition_build_id": entry.partition_build_id or "",
        "product_id": "p1",
        "product_content_token": entry.content_digest or "",
        "chunker_version": entry.chunker_version or "",
        "embed_model_id": entry.embed_model_id or "",
        # Оси объявлены дефолтами: ``_build`` их не задаёт, а партиция посчитана ими.
        "embed_device": EmbedDevice.CPU.value,
        "embed_precision": EmbedPrecision.FP32.value,
        "extractor_version": entry.extractor_version or "",
        "lexical_engine_version": entry.lexical_engine_version or "",
        "vector_store": entry.vector_store or "",
    }
    kwargs.update(overrides)
    return _partition_reuse_reason(**kwargs)


@requires_numpy
def test_partition_of_another_device_is_not_reusable(tmp_path: Path) -> None:
    """16 §4.1/§5.1: партиция, посчитанная на другом устройстве, не переиспользуется.

    Даже при совпавшем ``pb`` (то есть при коллизии id) манифест называет устройство, и
    расхождение — видимая причина, а не «переиспользовали, потому что id тот же».
    CPU- и CUDA-векторы одной модели численно разные: молчаливое переиспользование
    вернуло бы прогону числа, которых он не считал.
    """
    index_root, entry = _built_partition(tmp_path)
    # Своё устройство — переиспользование разрешено (иначе тест доказывал бы отказ всего).
    assert _reuse_reason(index_root, entry) is None
    reason = _reuse_reason(index_root, entry, embed_device=EmbedDevice.CUDA.value)
    assert reason is not None
    assert "embed_device" in reason
    # ``pb`` различает устройства сам — это первый, дешёвый уровень (16 §5.1).
    base = dict(
        package_id=entry.package_id or "",
        product_id="p1",
        chunker_version=entry.chunker_version or "",
        embed_model_id=entry.embed_model_id or "",
        extractor_version=entry.extractor_version or "",
        lexical_engine_version=entry.lexical_engine_version or "",
        vector_store=entry.vector_store or "",
        product_content_token=entry.content_digest or "",
    )
    assert _derive_partition_build_id(
        **base, embed_device=EmbedDevice.CPU.value
    ) != _derive_partition_build_id(**base, embed_device=EmbedDevice.CUDA.value)


@requires_numpy
def test_partition_of_another_precision_is_not_reusable(tmp_path: Path) -> None:
    """Ось ``embed.precision``: партиция другой точности не переиспользуется.

    fp16- и fp32-векторы — разные числа под тем же содержимым (решение 06.11.2026).
    Манифест называет точность, которой считал сборщик, поэтому подмена точности в
    объявленных входах — видимая причина отказа, а не «id совпал, значит можно».
    """
    index_root, entry = _built_partition(tmp_path)
    assert _reuse_reason(index_root, entry) is None
    reason = _reuse_reason(index_root, entry, embed_precision=EmbedPrecision.FP16.value)
    assert reason is not None
    assert "embed_precision" in reason
    # ``pb`` различает точности сам — первый, дешёвый уровень (16 §5.1).
    base = dict(
        package_id=entry.package_id or "",
        product_id="p1",
        chunker_version=entry.chunker_version or "",
        embed_model_id=entry.embed_model_id or "",
        extractor_version=entry.extractor_version or "",
        lexical_engine_version=entry.lexical_engine_version or "",
        vector_store=entry.vector_store or "",
        product_content_token=entry.content_digest or "",
    )
    assert _derive_partition_build_id(
        **base, embed_precision=EmbedPrecision.FP32.value
    ) != _derive_partition_build_id(**base, embed_precision=EmbedPrecision.FP16.value)


@requires_numpy
def test_partition_1_is_readable_but_not_reusable(tmp_path: Path) -> None:
    """16 §5.2: отпечаток без ``content_hash`` в ``chunk_ids.jsonl`` недоказуем.

    Партиция исходного формата (``partition-1``) читается как обычно, но доказать её
    целостность нечем: пересчёт с диска невозможен. Поэтому она **не**
    переиспользуется, и причина названа — а не проглочена как «нет отпечатка».
    """
    index_root, entry = _built_partition(tmp_path)
    root = partition_dir(
        index_root,
        entry.package_id or "",
        entry.partition_build_id or "",
        "p1",
    )
    # Убираем ``content_hash`` — ровно то, чего не было в ``partition-1``.
    rows = [
        json.loads(line)
        for line in (root / CHUNK_IDS_FILE).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert rows and all("content_hash" in row for row in rows)
    (root / CHUNK_IDS_FILE).write_text(
        "".join(
            json.dumps({"chunk_id": row["chunk_id"]}, ensure_ascii=False) + "\n"
            for row in rows
        ),
        encoding="utf-8",
    )
    reason = _reuse_reason(index_root, entry)
    assert reason is not None
    assert "недоказуем" in reason and "partition-1" in reason


@requires_numpy
def test_tampered_vectors_are_not_reusable(tmp_path: Path) -> None:
    """16 §5.2: подмена ``vectors.fp16.npy`` видна — манифест не доказательство.

    Сборщик пишет ``vector_digest``, но при переиспользовании сборка не строится, значит
    ``_guard_immutable`` не исполняется. Единственный контроль — пересчёт с диска; без
    него подменённая матрица выглядела бы как валидная иммутабельная партиция.
    """
    import numpy as np  # type: ignore[import-not-found]

    index_root, entry = _built_partition(tmp_path)
    root = partition_dir(
        index_root,
        entry.package_id or "",
        entry.partition_build_id or "",
        "p1",
    )
    matrix = np.load(root / VECTORS_FILE)
    np.save(root / VECTORS_FILE, np.asarray(matrix, dtype=np.float16) + np.float16(1.0))

    reason = _reuse_reason(index_root, entry)
    assert reason is not None
    assert "vector_digest" in reason and "расходится" in reason


# --------------------------------------------------------------------------- #
# 2. Адрес чанка — версия документа (16 §6.1)
# --------------------------------------------------------------------------- #

def test_chunk_id_addresses_the_document_version_not_the_bytes(tmp_path: Path) -> None:
    """``chunk_id = product:doc_version_id:chunk_no``: уровень извлечения входит в адрес.

    09 §14.6 п. 11: один и тот же текст, извлечённый на разных уровнях, давал **один**
    ``chunk_id``, то есть chunk-сеты двух версий документа сливались. Теперь адрес несёт
    ``doc_version_id`` (``content_hash`` + версия экстрактора + уровень), и версии
    различаются. ``parse_source_id`` при этом продолжает разбирать адрес: сегментов
    по-прежнему три, а ``doc_id`` — идентификатор версии.
    """
    store = SqliteStore(":memory:")
    state = SqliteStateStore(":memory:")
    try:
        ids: dict[str, list[str]] = {}
        versions: dict[str, str] = {}
        for level in (DocumentLevel.TEXT, DocumentLevel.FULL):
            # Смена ``document_level`` — ``execution_change`` (§9), то есть **другой**
            # ``run_id`` и другой сеанс. Один ``run_id`` на два уровня был бы двумя
            # условиями измерения под одной идентичностью; после F5 это ещё и делит
            # durable-строку очереди (её ``level`` — условие корня), поэтому уровни
            # разводятся так же, как в реальном прогоне.
            level_run_id = f"{RUN_ID}-{level.value}"
            _make_run(store, level_run_id)
            result = _build(
                tmp_path / level.value,
                pages={URL_P1_A: b"same bytes"},
                urls={"p1": [URL_P1_A]},
                store=store,
                state=state,
                run_id=level_run_id,
                crawl_defaults=CrawlRunConfig(document_level=level),
            )
            assert result.errors == []
            ids[level.value] = sorted(
                row["chunk_id"]
                for row in store.conn.execute(
                    "SELECT chunk_id FROM corpus_chunks WHERE build_id = ?",
                    (_build_of(result).entries[0].partition_build_id,),
                )
            )
            version = state.get_url_state("p1", URL_P1_A)
            assert version is not None and version.doc_version_id
            versions[level.value] = version.doc_version_id
        # Разные версии документа — разные адреса чанков (иначе сеты слились бы).
        assert versions[DocumentLevel.TEXT.value] != versions[DocumentLevel.FULL.value]
        assert set(ids[DocumentLevel.TEXT.value]).isdisjoint(
            ids[DocumentLevel.FULL.value]
        )
        for level, chunk_ids in ids.items():
            assert chunk_ids, level
            for chunk_id in chunk_ids:
                ref = parse_source_id(chunk_id)
                assert ref.product_id == "p1"
                # ``doc_id`` — версия документа, а не хеш байтов.
                assert ref.doc_id == versions[level]
                assert ref.chunk_no == 0
    finally:
        state.close()
        store.close()


def test_corpus_docs_carry_the_same_document_version_as_chunk_ids(
    tmp_path: Path,
) -> None:
    """Строка ``corpus_docs`` несёт ту же версию, что и ``chunk_id`` (16 §6).

    Без неё ``source_id`` из evidence не отображается в строку корпуса: ``doc_id`` —
    content-hash, а ``dv_`` из него не выводится (``short_hash`` необратим). Тогда
    правило «``❌`` требует ``live|revalidated``» снимало бы **подтверждённый** ``❌``,
    выдавая это за «наблюдение не записано».
    """
    store = SqliteStore(":memory:")
    state = SqliteStateStore(":memory:")
    try:
        _make_run(store)
        result = _build(
            tmp_path / "raw",
            pages={URL_P1_A: b"same bytes"},
            urls={"p1": [URL_P1_A]},
            store=store,
            state=state,
            run_id=RUN_ID,
        )
        assert result.errors == []
        (doc,) = _docs(store)
        chunk_ids = [
            row["chunk_id"]
            for row in store.conn.execute("SELECT chunk_id FROM corpus_chunks")
        ]
        assert chunk_ids
        versions = {parse_source_id(chunk_id).doc_id for chunk_id in chunk_ids}
        assert len(versions) == 1, "один документ — одна версия в этом прогоне"
        assert store.iter_corpus_doc_versions(RUN_ID, "p1") == {
            versions.pop(): doc.doc_id
        }
        # Версия адресует строку корпуса: обратного отображения ``dv_`` → ``doc_id`` нет.
        assert store.resolve(chunk_ids[0]) is not None
    finally:
        state.close()
        store.close()


# --------------------------------------------------------------------------- #
# 3. Происхождение наблюдения: corpus_docs.http_status / reuse_kind (16 §3.3)
# --------------------------------------------------------------------------- #

def _doc_observation(store: SqliteStore, url: str) -> tuple[int | None, ReuseKind | None]:
    (doc,) = [row for row in _docs(store) if row.url == url]
    return doc.http_status, doc.reuse_kind


def test_corpus_docs_record_observation_origin(tmp_path: Path) -> None:
    """``live``/``revalidated``/``reused`` различаются, и ``http_status`` — факт цикла.

    Без этих колонок 304-документ выглядел как живой 200, а ``❌`` на retained-байтах без
    серверного подтверждения был невыразим (09 §11.2). ``NULL`` в ``http_status`` —
    «сервер в этом цикле не отвечал», а не понижение прежнего ответа.
    """
    # --- живой fetch: сервер ответил 200 -------------------------------- #
    live_store = SqliteStore(":memory:")
    live_state = SqliteStateStore(":memory:")
    try:
        _make_run(live_store)
        result = _build(
            tmp_path / "live",
            pages={URL_P1_A: b"page one"},
            urls={"p1": [URL_P1_A]},
            crawler=_StatefulCrawler({URL_P1_A: b"page one"}),
            store=live_store,
            state=live_state,
            run_id=RUN_ID,
        )
        assert result.errors == []
        assert _doc_observation(live_store, URL_P1_A) == (200, ReuseKind.LIVE)

        # Тот же стор, второй прогон: байты ретейнены — сервер не спрашивается.
        second = _build(
            tmp_path / "live",
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=_BombCrawler({}),
            store=live_store,
            run_id=RUN_ID,
        )
        assert second.errors == []
        # ``http_status`` — факт **цикла**: сервер не отвечал ⇒ NULL. А ``reuse_kind``
        # не понижается: resume не стирает состоявшееся наблюдение ``live``
        # (монотонное правило 16 §3.3 — понижает только новое ``reused``).
        assert _doc_observation(live_store, URL_P1_A) == (None, ReuseKind.LIVE)
    finally:
        live_state.close()
        live_store.close()

    # --- условный GET: сервер ответил 304 -------------------------------- #
    nm_store = SqliteStore(":memory:")
    nm_state = SqliteStateStore(":memory:")
    try:
        _make_run(nm_store)
        # Валидатор известен состоянию ⇒ ревалидация пойдёт условным GET. Версии документа
        # в субстрате нет (вытеснена), поэтому сервер подтверждает «не изменилось», а
        # переиспользовать нечего — обычная экстракция; для нас важно происхождение
        # наблюдения: сервер **ответил** 304.
        nm_state.upsert_url_state(
            UrlStateRow(
                product_id="p1",
                url=URL_P1_A,
                normalized_url=URL_P1_A,
                doc_version_id="dv_evicted",
                etag="etag-fixture",
            )
        )
        result = _build(
            tmp_path / "nm",
            pages={URL_P1_A: b"page one"},
            urls={"p1": [URL_P1_A]},
            crawler=_StatefulCrawler(
                {URL_P1_A: b"page one"},
                not_modified={URL_P1_A},
                etag="etag-fixture",
                last_modified=None,
            ),
            store=nm_store,
            state=nm_state,
            run_id=RUN_ID,
            product_modes={"p1": "incremental"},
        )
        assert any("304 без переиспользуемой версии" in err for err in result.errors)
        # 304 = сервер ответил (наблюдение подтверждено), но версия не изменилась.
        assert _doc_observation(nm_store, URL_P1_A) == (304, ReuseKind.REVALIDATED)
    finally:
        nm_state.close()
        nm_store.close()

    # --- resume из retained-байтов: сервер не отвечал -------------------- #
    # Сценарий, в котором ``reused`` действительно появляется в строке: первый прогон
    # упал на извлечении (строки ``corpus_docs`` нет), байты ретейнены, второй прогон
    # ре-извлекает их без сети. Тогда наблюдение — ровно «взяли retained-байты».
    resume_store = SqliteStore(":memory:")
    try:
        _make_run(resume_store)
        _build(
            tmp_path / "resume",
            pages={URL_P1_A: b"retained bytes"},
            urls={"p1": [URL_P1_A]},
            crawler=_StatefulCrawler({URL_P1_A: b"retained bytes"}),
            extractor=_Extractor(error_urls={URL_P1_A}),
            store=resume_store,
            run_id=RUN_ID,
        )
        assert _docs(resume_store) == []
        second = _build(
            tmp_path / "resume",
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=_BombCrawler({}),
            store=resume_store,
            run_id=RUN_ID,
        )
        assert second.errors == []
        # Retained-байты: ``reused`` и ``NULL`` (сервер в этом цикле не отвечал).
        assert _doc_observation(resume_store, URL_P1_A) == (None, ReuseKind.REUSED)
        # Идемпотентность: повторный resume того же документа не меняет классификацию.
        third = _build(
            tmp_path / "resume",
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=_BombCrawler({}),
            store=resume_store,
            run_id=RUN_ID,
        )
        assert third.errors == []
        assert _doc_observation(resume_store, URL_P1_A) == (None, ReuseKind.REUSED)
    finally:
        resume_store.close()

    # --- 200 с прежним content_hash: сервер ответил, версия та же --------- #
    same_state = SqliteStateStore(":memory:")
    same_store = SqliteStore(":memory:")
    second_store = SqliteStore(":memory:")
    try:
        _make_run(same_store)
        _build(
            tmp_path / "same1",
            pages={URL_P1_A: b"same text"},
            urls={"p1": [URL_P1_A]},
            crawler=_StatefulCrawler(
                {URL_P1_A: b"same text"}, etag=None, last_modified=None
            ),
            store=same_store,
            state=same_state,
            run_id=RUN_ID,
        )
        assert _doc_observation(same_store, URL_P1_A) == (200, ReuseKind.LIVE)

        # Второй прогон — **новый** стор прогона (сеанса с retained-байтами нет), поэтому
        # страница действительно добывается: 200 и тот же текст. Версия уже в durable-
        # субстрате ⇒ переиспользование версии (chunk-сет из ``chunk_store``), а наблюдение —
        # ``revalidated``: сервер ответил, контент не изменился (09 §6.3 ``classify``).
        _make_run(second_store)
        second = _build(
            tmp_path / "same2",
            pages={URL_P1_A: b"same text"},
            urls={"p1": [URL_P1_A]},
            crawler=_StatefulCrawler(
                {URL_P1_A: b"same text"}, etag=None, last_modified=None
            ),
            store=second_store,
            state=same_state,
            run_id=RUN_ID,
        )
        assert second.errors == []
        assert _doc_observation(second_store, URL_P1_A) == (
            200,
            ReuseKind.REVALIDATED,
        )
    finally:
        second_store.close()
        same_store.close()
        same_state.close()


# --------------------------------------------------------------------------- #
# 4. run_product: граница продукта и конец фазы (16 §3.2)
# --------------------------------------------------------------------------- #

class _RunProductSpy:
    """Прокси стора: журналирует записи ``run_product`` (порядок и статусы)."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.calls: list[tuple[str, str]] = []

    def save_run_product(self, run_id: str, product_id: str, **kwargs: Any) -> None:
        self.calls.append((product_id, str(kwargs.get("status"))))
        self._inner.save_run_product(run_id, product_id, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def test_run_product_is_written_at_the_product_boundary(tmp_path: Path) -> None:
    """16 §3.2: ``running`` до Pass A и финальный статус после Pass B — по продукту.

    Числа берутся из уже посчитанного фазой (документы, ``CorpusCoverage``), а не
    считаются заново; ``cells_total``/``cells_pending`` остаются нулями — на границе
    ``corpus`` ячеек ещё нет, их считает Ш6.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    spy = _RunProductSpy(store)
    try:
        result = _build(
            tmp_path,
            pages={URL_P1_A: b"one", URL_P2_A: b"two"},
            urls={"p1": [URL_P1_A], "p2": [URL_P2_A]},
            store=spy,
            run_id=RUN_ID,
        )
        assert result.errors == []
        # Порядок: продукт начинается (running) и закрывается (final) — попарно.
        assert [pid for pid, _status in spy.calls] == ["p1", "p1", "p2", "p2"]
        assert [status for _pid, status in spy.calls[::2]] == [
            RunProductStatus.RUNNING.value,
            RunProductStatus.RUNNING.value,
        ]
        assert all(
            status
            not in {RunProductStatus.RUNNING.value, RunProductStatus.PENDING.value}
            for _pid, status in spy.calls[1::2]
        )

        rows = list(store.iter_run_products(RUN_ID))
        assert [row["product_id"] for row in rows] == ["p1", "p2"]
        for row in rows:
            assert row["phase"] == "corpus"
            assert row["scope"] == "full"
            assert row["status"] in {
                RunProductStatus.COMPLETE.value,
                RunProductStatus.PARTIAL.value,
            }
            assert row["docs_total"] == 1
            assert row["docs_new"] == 1
            assert row["docs_reused"] == 0
            assert row["cells_total"] == 0 and row["cells_pending"] == 0
            assert row["coverage_complete"] is not None
    finally:
        store.close()


def test_run_product_closes_products_that_never_started(tmp_path: Path) -> None:
    """Продукт без записи ``urls`` не имеет границы — его закрывает конец фазы.

    Без этой строки «продукт не начинался» неотличим от «продукта не было в прогоне»
    (16 §3.2). Статус — ``blocked`` с названной причиной zero-source: материала не было,
    а не «не удалось».
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        result = _build(
            tmp_path,
            pages={URL_P2_A: b"two"},
            # p1 объявлен планом, но корпусного входа у него нет.
            urls={"p2": [URL_P2_A]},
            store=store,
            run_id=RUN_ID,
        )
        rows = {row["product_id"]: row for row in store.iter_run_products(RUN_ID)}
        assert set(rows) == {"p1", "p2"}
        assert rows["p2"]["status"] in {
            RunProductStatus.COMPLETE.value,
            RunProductStatus.PARTIAL.value,
        }
        assert rows["p1"]["status"] == RunProductStatus.BLOCKED.value
        assert rows["p1"]["error_kind"]
        assert result.errors == [] or all("p1" in e for e in result.errors)
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# 5. Дефект краула 1: зарядка бюджета — владельцу (09 §6.3)
# --------------------------------------------------------------------------- #

HOST_ROOT = "https://docs.test/"
DOCS_ROOT = "https://docs.test/docs/"


def test_budget_is_charged_to_the_most_specific_root(tmp_path: Path) -> None:
    """Потолок не умножается на число корней: URL заряжает **свой** корень.

    Дефект: реконструкция бюджета на resume заряжала ``max_documents`` первому
    корню-претенденту. Host-корень с префиксом ``/`` забирал зарядку у docs-корня, и
    потолок умножался на число перезапусков (измерено 1610 ``fetched`` при потолке
    800 = ``2 × 800``, 09 §6.3). Владелец — самый специфичный ``root_path_prefix``,
    тот же предикат, что у группировки ревалидации.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    host_page = "https://docs.test/about"
    docs_pages = [f"{DOCS_ROOT}page-{i}" for i in range(4)]
    try:
        # Первый прогон: host-корень обходит обе ветки — durable-элементы обоих владельцев.
        _build(
            tmp_path / "r1",
            pages={},
            urls={"p1": [HOST_ROOT]},
            crawler=_Crawler(
                {
                    HOST_ROOT: (
                        f'<a href="{host_page}">a</a><a href="{DOCS_ROOT}">docs</a>'
                    ).encode(),
                    host_page: b"about",
                    DOCS_ROOT: "".join(
                        f'<a href="{u}">d</a>' for u in docs_pages
                    ).encode(),
                    **{u: b"docs" for u in docs_pages},
                }
            ),
            domain_profiles={"docs.test": DomainProfile(max_depth=2)},
            store=store,
            run_id=RUN_ID,
        )
        assert {i.url for i in _items(store)} >= {host_page, DOCS_ROOT, *docs_pages}

        # Второй прогон: оба корня объявлены; потолки — host 5, docs 2.
        result = _build(
            tmp_path / "r2",
            pages={},
            urls={"p1": [HOST_ROOT, DOCS_ROOT]},
            crawler=_BombCrawler({}),
            domain_profiles={
                "docs.test": DomainProfile(max_documents=5),
                "docs.test/docs": DomainProfile(max_documents=2),
            },
            store=store,
            run_id=RUN_ID,
        )
        # Отказ seed'а docs-корня по потолку — штатный видимый сигнал, не сбой.
        assert all("достигнут max_documents" in e for e in result.errors), result.errors
        records = {rec["root"]: rec for rec in _build_of(result).truncation}
        # Host-корень заряжен только своими двумя (``/`` + ``/about``), а не всеми семью:
        # сумма по корням = число durable-элементов, каждый заряжен ровно один раз.
        assert records[HOST_ROOT]["pages_fetched"] == 2
        assert records[HOST_ROOT]["truncated"] is False
        # Docs-корень — своими пятью (корень + 4 страницы), потолок 2 исчерпан.
        assert records[DOCS_ROOT]["pages_fetched"] == 5
        assert records[DOCS_ROOT]["truncated"] is True
        assert records[DOCS_ROOT]["truncated_reason"] == "max_documents"
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# 6. Дефект краула 3: заряженный URL обязан быть дренируемым (09 §6.3)
# --------------------------------------------------------------------------- #

def test_charged_resume_urls_are_drained_even_when_the_root_is_truncated(
    tmp_path: Path,
) -> None:
    """``pangolin`` 600/600: корень заряжен в потолок ⇒ seed ``truncated`` ⇒ дренаж был пуст.

    Измерено: 509 документов давали 25 чанков при 4935 чанках в durable-субстрате — корпус
    терялся не сетью, а тем, что зарядка бюджета и работа были расцеплены: ``consumed``
    считался, а очередь дренажа оставалась пустой (``add_revalidate_seed`` строился только
    для ``incremental``). Теперь заряженный URL ставится depth-0 листом **независимо от
    режима продукта** и обязан дойти до корпуса.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    pages = [f"{DOCS_ROOT}page-{i}" for i in range(6)]
    try:
        _build(
            tmp_path / "r1",
            pages={},
            urls={"p1": [DOCS_ROOT]},
            crawler=_Crawler(
                {
                    DOCS_ROOT: "".join(
                        f'<a href="{u}">d</a>' for u in pages
                    ).encode(),
                    **{u: f"text {u}".encode() for u in pages},
                }
            ),
            domain_profiles={"docs.test/docs": DomainProfile(max_depth=1)},
            store=store,
            run_id=RUN_ID,
        )
        assert {i.url for i in _items(store)} >= {DOCS_ROOT, *pages}

        # Второй прогон: потолок корня уже занят durable-страницами (600/600 у pangolin),
        # сеть не нужна — корпус обязан восстановиться из retained-байтов.
        result = _build(
            tmp_path / "r2",
            pages={},
            urls={"p1": [DOCS_ROOT]},
            crawler=_BombCrawler({}),
            domain_profiles={"docs.test/docs": DomainProfile(max_documents=1)},
            store=store,
            run_id=RUN_ID,
        )
        build = _build_of(result)
        # Корень усечён — и это правда: потолок discovery исчерпан.
        record = next(
            rec for rec in build.truncation if rec["root"] == DOCS_ROOT
        )
        assert record["truncated"] is True
        assert record["truncated_reason"] == "max_documents"
        # Но заряженные URL дренированы: корень + все страницы в корпусе, а не «25 чанков».
        expected_docs = len(pages) + 1  # страницы + сам корень (в нём ссылки)
        assert build.doc_count == expected_docs
        assert build.chunk_count == expected_docs
        assert {doc.url for doc in _docs(store)} >= {DOCS_ROOT, *pages}
        # Сеть не тронута: всё пришло из retained-байтов (краулер-«бомба» не вызван).
        # Единственная запись — видимый отказ seed'а по потолку (не тихий дроп).
        assert all("достигнут max_documents" in e for e in result.errors), result.errors
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# 7. Регресс ``digital_q_database``: host-корень ``/`` не забирает чужую зарядку
# --------------------------------------------------------------------------- #

def test_dq_host_root_prefix_does_not_steal_charged_budget(tmp_path: Path) -> None:
    """``dq``: host-корень (``/``, потолок 10) не заряжает страницы docs-корня (потолок 800).

    Живой замер (09 §6.3): 2402 элемента, из них большинство — документация под
    ``/docs/``; host-корень с префиксом ``/`` забирал зарядку себе, и потолок умножался на
    число перезапусков. Масштаб здесь уменьшен намеренно (потолки 10/800 сохранены):
    проверяется арифметика владения, а не стоимость 2402 страниц.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    host_pages = [f"https://docs.test/site-{i}" for i in range(2)]
    docs_pages = [f"{DOCS_ROOT}page-{i}" for i in range(8)]
    try:
        _build(
            tmp_path / "r1",
            pages={},
            urls={"p1": [HOST_ROOT]},
            crawler=_Crawler(
                {
                    HOST_ROOT: "".join(
                        f'<a href="{u}">x</a>' for u in [*host_pages, DOCS_ROOT]
                    ).encode(),
                    DOCS_ROOT: "".join(
                        f'<a href="{u}">x</a>' for u in docs_pages
                    ).encode(),
                    **{u: f"body {u}".encode() for u in [*host_pages, *docs_pages]},
                }
            ),
            domain_profiles={"docs.test": DomainProfile(max_depth=2)},
            store=store,
            run_id=RUN_ID,
        )
        assert {i.url for i in _items(store)} >= {*host_pages, DOCS_ROOT, *docs_pages}

        result = _build(
            tmp_path / "r2",
            pages={},
            urls={"p1": [HOST_ROOT, DOCS_ROOT]},
            crawler=_BombCrawler({}),
            domain_profiles={
                "docs.test": DomainProfile(max_documents=10),
                "docs.test/docs": DomainProfile(max_documents=800),
            },
            store=store,
            run_id=RUN_ID,
        )
        assert result.errors == []
        records = {rec["root"]: rec for rec in _build_of(result).truncation}
        # Host-корень заряжен ровно своими тремя (корень + две страницы вне ``/docs/``),
        # а не всеми двенадцатью: чужая зарядка не переезжает к префиксу ``/``.
        assert records[HOST_ROOT]["pages_fetched"] == len(host_pages) + 1
        assert records[HOST_ROOT]["truncated"] is False
        # Docs-корень — своими девятью, потолок 800 не достигнут.
        assert records[DOCS_ROOT]["pages_fetched"] == len(docs_pages) + 1
        assert records[DOCS_ROOT]["truncated"] is False
    finally:
        store.close()
