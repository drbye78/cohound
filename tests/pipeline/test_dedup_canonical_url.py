"""17 §6 (F3): канонический адрес документа — ``min(url)`` среди равных ``doc_hash``.

Победитель контент-дедупа перестаёт быть «кто первым доехал до ``ingest_one``» (при
нескольких полосах это момент готовности страницы, то есть планировщик) и становится
**запросом** по durable-носителю ``crawl_session_items.doc_hash``. Здесь проверяется:

1. перестановка порядка ингеста даёт один и тот же ``corpus_docs.url`` — меньший адрес;
2. смена канонического адреса видна оператору (строка лога фазы + машинное поле
   ``product_end.canonical_url_changes``), а не подменяется молча;
3. повторный проход согласования идемпотентен (ничего не меняет и возвращает ``0``).

Порты подменены детерминированными заглушками (без сети), сторадж — реальный
``SqliteStore``: проверяется фактический артефакт (``corpus_docs``), а не промежуточный
список. Данные нейтральные (§1.3).

Почему порядок ингеста задаётся **seed-набором**, а не порядком ссылок: frontier идёт по
ключу ``(depth, url)`` (F3, полоса A), поэтому порядок ссылок внутри одной страницы уже не
управляет порядком наблюдения. Глубина — управляет: seed (depth 0) доезжает раньше ссылки
(depth 1). Один и тот же текст на двух хостах, seed — то один, то другой: в одном прогоне
документ пишется по большему адресу, в другом — по меньшему.
"""

from __future__ import annotations

import datetime as _dt
import json
import pathlib
from pathlib import Path
from typing import Any

from core.config.domain_profiles import DomainProfile
from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CrawlRunConfig,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import (
    CANONICAL_URL_CHANGED_NOTE,
    build_corpus,
    canonicalize_doc_urls,
)
from core.pipeline.crawl_session import default_session_id
from core.ports.types import (
    Chunk,
    ExtractedDoc,
    ExtractionRoute,
    FetchPlan,
    RawPage,
)
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash

FIXTURES = Path(__file__).parent / "fixtures"
TS = "2026-01-01T00:00:00+00:00"
#: Корень продукта: его хост — родитель объявленных поддоменов (scope корня — поддомены).
ROOT = "https://docs.test/a"
#: Меньший адрес: обязан стать каноническим при **любом** порядке ингеста.
URL_SMALL = "https://h0.docs.test/a"
#: Больший адрес: доезжает первым в прогоне ``large-first`` (он и пишет документ).
URL_LARGE = "https://h1.docs.test/a"
#: Одинаковые байты на обоих адресах ⇒ одинаковый извлечённый текст ⇒ один ``doc_hash``.
SAME = b"<html><p>same text</p></html>"
ROOT_PAGE = (
    b'<html><a href="https://h0.docs.test/a">a</a>'
    b'<a href="https://h1.docs.test/a">b</a>index</html>'
)


def _catalog() -> list[CatalogRow]:
    raw = json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))
    return [CatalogRow.model_validate(row) for row in raw["catalog"]]


def _plan(run_id: str) -> RunPlan:
    return RunPlan(
        run_id=run_id,
        products=["p1"],
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a"],
        params=_catalog(),
        module_weights={"mod_a": 100.0},
        renormalized_weights={"mod_a": 1.0},
        cohort=Cohort(
            case_id="case",
            frozen_at=_dt.date(2026, 1, 1),
            entries=[
                CohortEntry(
                    product_id="p1",
                    role_overlap=1.0,
                    selection_reason="anchor",
                    anchor_relation=AnchorRelation.ANCHOR,
                )
            ],
        ),
        golden=[],
    )


def _make_run(store: SqliteStore, run_id: str) -> None:
    store.create_run(
        run_id,
        RunConfig(
            case_id="case",
            anchor="p1",
            cohort="case",
            products=["p1"],
            package="pkg",
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
        ),
    )


class _Crawler:
    """Краулер ``url -> bytes`` (сеть не нужна: содержимое задано таблицей)."""

    version = "crawler-1"

    def __init__(self, pages: dict[str, bytes]) -> None:
        self.pages = dict(pages)
        self.calls: list[str] = []

    def fetch(self, url: str, fetch_plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта
        self.calls.append(url)
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at=TS,
            fetch_mode=fetch_plan.fetch,
            lang="en",
        )


class _Extractor:
    """Экстрактор: ``content_hash`` извлечённого текста = хеш байтов (стабильно)."""

    version = "extractor-1"

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        return ExtractedDoc(
            text=_page_bytes(page).decode("utf-8", errors="replace"),
            content_hash=content_hash(_page_bytes(page)),
            extractor_version=self.version,
            page_count=1,
        )


class _Chunker:
    version = "chunker-1"

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        return [
            Chunk(
                chunk_id=f"{document_id}:0",
                chunk_no=0,
                text=doc.text,
                heading_path="h1",
                context_prefix="ctx",
                content_hash=doc.content_hash,
            )
        ]


def _declared_hosts() -> dict[str, DomainProfile]:
    """Оба поддомена объявлены: без объявления ссылка вне scope корня (17 §6)."""
    return {"h0.docs.test": DomainProfile(), "h1.docs.test": DomainProfile()}


def _run(tmp_path: Path, run_id: str, *, own_seed: str) -> tuple[SqliteStore, Any]:
    """Прогон продукта ``p1``: корень + ``own_seed`` как собственный seed-корень.

    ``own_seed`` — адрес из пары одинаковых страниц, объявленный **отдельным** корнем
    продукта: он доезжает на глубине 0 (раньше ссылки). Корень ``ROOT`` разворачивает в
    ссылку второй адрес пары. Так порядок ингеста задаётся входом, а не планировщиком.
    """
    store = SqliteStore(":memory:")
    _make_run(store, run_id)
    result = build_corpus(
        _plan(run_id),
        _Crawler({ROOT: ROOT_PAGE, URL_SMALL: SAME, URL_LARGE: SAME}),
        _Extractor(),
        _Chunker(),
        store,
        urls={"p1": [ROOT, own_seed]},
        raw_root=tmp_path,
        run_id=run_id,
        domain_profiles=_declared_hosts(),
        crawl_defaults=CrawlRunConfig(max_depth=1),
    )
    return store, result


def _docs(store: SqliteStore, run_id: str) -> list[tuple[str, str]]:
    """Документы корпуса из стора: ``(doc_id, url)`` в детерминированном порядке."""
    return sorted((doc.doc_id, doc.url) for doc in store.iter_corpus_docs(run_id))


def _product_end(result: Any) -> dict[str, Any]:
    return next(
        entry
        for entry in result.log_entries
        if isinstance(entry, dict) and entry.get("event") == "product_end"
    )


def _notes(result: Any) -> list[str]:
    return [entry for entry in result.log_entries if isinstance(entry, str)]


def _same_doc_id(store: SqliteStore, run_id: str) -> str:
    """``doc_id`` документа с текстом ``SAME`` (единственного: алиасы схлопнуты)."""
    docs = [
        doc
        for doc in store.iter_corpus_docs(run_id)
        if doc.content_hash == content_hash(SAME)
    ]
    assert len(docs) == 1, [(doc.doc_id, doc.url) for doc in docs]
    return docs[0].doc_id


def _same_doc_url(store: SqliteStore, run_id: str) -> str:
    """Адрес единственного документа с текстом ``SAME`` (их два: текст и корневая страница)."""
    docs = [doc for doc in store.iter_corpus_docs(run_id) if doc.content_hash == content_hash(SAME)]
    assert len(docs) == 1, [(doc.doc_id, doc.url) for doc in store.iter_corpus_docs(run_id)]
    return docs[0].url


# --------------------------------------------------------------------------- #
# (1) Победитель дедупа — min(url) при любом порядке ингеста
# --------------------------------------------------------------------------- #


def test_same_document_has_min_url_regardless_of_ingest_order(tmp_path: Path) -> None:
    """Два адреса с одинаковым текстом → один документ, адрес = ``min(url)`` в обоих прогонах.

    ``large-first``: первым доехал ``URL_LARGE`` (он и записал документ), ``small-first``:
    первым доехал ``URL_SMALL``. Раньше это давало **разные** адреса в ``corpus_docs`` —
    состав корпуса зависел от того, какая страница стала готова раньше.

    **Что изменилось в F5 (причина названа).** Конверсия идёт порядком очереди
    ``(product_id, url)``, поэтому пара алиасов попадает в ``ingest_one`` **всегда** в
    каноническом порядке: документ пишется под меньшим адресом сразу, и проходу
    согласования переписывать нечего (``canonical_url_changes == 0`` в обоих прогонах).
    Раньше единица здесь была признаком того, что согласование сработало; теперь оно —
    **страховка** (документ мог быть записан прошлой схемой или неканоническим путём), и
    проверяется прямо, на строке корпуса, в тестах (2) и (3) ниже.
    """
    store_large, result_large = _run(
        tmp_path / "large", "run-canon-large", own_seed=URL_SMALL
    )
    store_small, result_small = _run(
        tmp_path / "small", "run-canon-small", own_seed=URL_LARGE
    )
    try:
        assert _same_doc_url(store_large, "run-canon-large") == URL_SMALL
        assert _same_doc_url(store_small, "run-canon-small") == URL_SMALL
        # Полный набор документов тоже совпал: согласование не создало и не потеряло строк.
        assert _docs(store_large, "run-canon-large") == _docs(store_small, "run-canon-small")

        # Документ записан по меньшему адресу в обоих прогонах — порядок конверсии
        # канонический, поэтому переписывать нечего.
        assert _same_doc_url(store_large, "run-canon-large") != URL_LARGE
        assert _product_end(result_large)["canonical_url_changes"] == 0
        assert _product_end(result_small)["canonical_url_changes"] == 0
    finally:
        store_large.close()
        store_small.close()


def test_alias_url_is_recorded_as_durable_doc_hash(tmp_path: Path) -> None:
    """Алиас несёт ``doc_hash`` в элементе сеанса — иначе ``min(url)`` неоткуда взять."""
    run_id = "run-canon-durable"
    store, _result = _run(tmp_path, run_id, own_seed=URL_LARGE)
    try:
        items = {
            item.url: item
            for item in store.iter_crawl_session_items(default_session_id(run_id))
        }
        hashes = {url: item.doc_hash for url, item in items.items()}
        # Оба адреса одного текста знают один и тот же ключ дедупа (алиас — тоже).
        assert hashes[URL_SMALL] == hashes[URL_LARGE] == content_hash(SAME)
        # У страницы другого текста — свой ключ: носитель не «один на продукт».
        assert hashes[ROOT] != hashes[URL_SMALL]
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (2) Смена адреса видна оператору (notes/лог фазы), а не молчит
# --------------------------------------------------------------------------- #


def test_canonical_url_change_is_visible_in_notes(tmp_path: Path) -> None:
    """Смена канонического адреса названа (продукт + число), а без смены строки нет.

    **Почему вызов прямой.** После F5 конверсия идёт каноническим порядком, и проход
    согласования внутри фазы не срабатывает (см. тест выше) — держать «срабатывание» на
    уровне фазы можно было бы только искусственным порядком, которого больше нет. Поэтому
    страховка проверяется на строке корпуса: адрес документа ставится в состояние
    «записан неканонически» (так выглядела бы строка прошлой схемы), проход его
    переписывает, а строка :data:`CANONICAL_URL_CHANGED_NOTE` называет продукт и число.
    """
    run_id = "run-canon-visible"
    store, result = _run(tmp_path, run_id, own_seed=URL_SMALL)
    try:
        assert result.errors == []
        # Ничего не переписано на фазе: порядок конверсии канонический.
        assert _product_end(result)["canonical_url_changes"] == 0
        assert not [note for note in _notes(result) if "канонический адрес" in note]

        # Ставим документ под больший адрес: ровно то, что должен исправить проход.
        store.set_corpus_doc_url(_same_doc_id(store, run_id), URL_LARGE)
        items = {
            (item.product_id, item.url): item
            for item in store.iter_crawl_session_items(default_session_id(run_id))
        }
        errors: list[str] = []
        changed = canonicalize_doc_urls(
            "p1", items=items, store=store, run_id=run_id, errors=errors
        )

        assert changed == 1
        assert _same_doc_url(store, run_id) == URL_SMALL
        # Ошибкой это не является: смена адреса — исправление порядка, а не сбой.
        assert errors == []
        note = CANONICAL_URL_CHANGED_NOTE.format(product_id="p1", count=changed)
        assert "p1" in note and "1 документов" in note
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (3) Повторный проход согласования идемпотентен
# --------------------------------------------------------------------------- #


def test_reconciliation_pass_is_idempotent(tmp_path: Path) -> None:
    """Второй проход по согласованному корпусу не пишет ничего и возвращает ``0``.

    **Почему после ручной правки.** Фаза конверсии пишет документ сразу по каноническому
    адресу (F5), поэтому первый ``canonicalize_doc_urls`` внутри фазы возвращает ``0``.
    Чтобы проверить идемпотентность, адрес сначала ставится неканоническим (как у строки
    прошлой схемы): первый проход исправляет, второй — нет.
    """
    run_id = "run-canon-idempotent"
    store, _result = _run(tmp_path, run_id, own_seed=URL_SMALL)
    try:
        store.set_corpus_doc_url(_same_doc_id(store, run_id), URL_LARGE)
        items = {
            (item.product_id, item.url): item
            for item in store.iter_crawl_session_items(default_session_id(run_id))
        }
        errors: list[str] = []
        first = canonicalize_doc_urls(
            "p1", items=items, store=store, run_id=run_id, errors=errors
        )
        assert first == 1
        before = _docs(store, run_id)
        assert _same_doc_url(store, run_id) == URL_SMALL

        again = canonicalize_doc_urls(
            "p1", items=items, store=store, run_id=run_id, errors=errors
        )

        assert again == 0
        assert _docs(store, run_id) == before
        assert errors == []
    finally:
        store.close()


def _page_bytes(page: Any) -> bytes:
    """Байты страницы: тело или retained-файл (потребитель очереди не держит тело в RAM).

    До F5 конверсия шла внутри краула, и ``RawPage.content`` страницы был непуст. Теперь
    документ конвертируется из строки очереди: ``content`` пуст, тело лежит по
    ``source_path`` (I19, ``corpus_raw/{hash}.bin``) — ровно так его получает воркер пула.
    Фейковый экстрактор обязан читать тот же носитель, что и адаптер, иначе он проверял бы
    не контракт порта, а прежний способ доставки байтов.
    """
    if page.content:
        return page.content
    path = getattr(page, "source_path", None)
    return pathlib.Path(path).read_bytes() if path else b""
