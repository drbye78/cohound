"""Подача ручных артефактов (13-manual-artifacts §3): вход → I19, отказ не стирается.

Проверяются ровно те свойства, ради которых шов существует:

* байты оператора входят в корпус **единственным** путём I19 (``RawPage`` → ``_retain``
  → экстракция → чанкер), а файл оператора остаётся на месте (его не «съедает»
  переезд staging-файла);
* ``robots_disallow`` не превращается в ``fetched``: отказ остаётся в
  ``crawl_session_items``, наблюдённость материала видна отдельно
  (``corpus_docs.origin``);
* покрытие различает «пробел закрыт артефактом» (``complete``) и «отказ был»
  (``blocked``/``partial_blocks``/``resolved_manually``);
* декларация отвергает подмену: чужой путь, чужой хост и несовпавший ``sha256`` —
  видимые ошибки, а не тихий пропуск;
* отсутствие файла — не ошибка: пустой набор и стабильный дайджест пустого множества.

Нейтрально: ``p1``/``p2``, ``docs.test`` — как во всём наборе тестов ядра (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import pathlib
from pathlib import Path
from typing import Any

import pytest

from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CrawlItemStatus,
    CrawlSessionItem,
    FailureKind,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import build_corpus, derive_corpus_coverage
from core.pipeline.crawl_session import default_session_id
from core.pipeline.digests import (
    EXECUTION_CONTRACT_FIELDS,
    manual_artifacts_digest,
)
from core.pipeline.manual_artifacts import (
    MANUAL_ARTIFACTS_FILE,
    ORIGIN_MANUAL,
    ManualArtifact,
    ManualArtifacts,
    load_manual_artifacts,
    manual_artifacts_summary,
)
from core.ports.types import (
    Chunk,
    ExtractedDoc,
    ExtractionRoute,
    FetchPlan,
    RawPage,
)
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash

RUN_ID = "run-manual"
SESSION_ID = default_session_id(RUN_ID)
TS = "2026-01-01T00:00:00+00:00"
FIXTURES = Path(__file__).parent / "fixtures"

URL_P1_A = "https://docs.test/p1/index"
URL_P2_A = "https://docs.test/p2/index"
URL_P1_B = "https://docs.test/p1/print"
#: Объявленный артефакт — документ, которого краулер не взял.
URL_MANUAL = "https://docs.test/p1/files/1C.pdf"
URL_FOREIGN = "https://other.test/p1/files/1C.pdf"
PAYLOAD = b"%PDF-1.4 manual certificate of the provided artifact"


# --------------------------------------------------------------------------- #
# Фикстуры/заглушки портов (нейтральные, без сети)
# --------------------------------------------------------------------------- #


def _catalog() -> list[CatalogRow]:
    import json

    raw = json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))
    return [CatalogRow.model_validate(row) for row in raw["catalog"]]


def _plan(products: tuple[str, ...] = ("p1", "p2")) -> RunPlan:
    return RunPlan(
        run_id=RUN_ID,
        products=list(products),
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a", "role_b"],
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
                ),
                CohortEntry(
                    product_id="p2",
                    role_overlap=0.5,
                    selection_reason="analog",
                    anchor_relation=AnchorRelation.ANALOG,
                ),
            ],
        ),
        golden=[],
    )


def _make_run(store: SqliteStore) -> None:
    """Строка прогона: нужна FK ``crawl_sessions.run_id`` и ``manual_artifacts.run_id``."""
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id="case",
            anchor="p1",
            cohort="case",
            products=["p1", "p2"],
            package="pkg",
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
        ),
    )


class _Crawler:
    """Краулер: ``url → bytes``; ``raising`` — URL, на которых поднимается сбой."""

    version = "crawler-1"

    def __init__(self, pages: dict[str, bytes], *, raising: set[str] | None = None) -> None:
        self.pages = dict(pages)
        self.raising = set(raising or ())
        self.calls: list[tuple[str, FetchPlan]] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта, FetchPlan frozen
        self.calls.append((url, plan))
        if url in self.raising:
            raise _RefusedError(f"robots.txt запрещает краул {url!r}")
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at=TS,
            fetch_mode=plan.fetch,
            lang="en",
        )


class _RefusedError(RuntimeError):
    """Robots-отказ: ``refusal_reason`` вместо ``failure_kind`` (D1/§10)."""

    refusal_reason = "robots_disallow"


class _Extractor:
    """Экстрактор: текст — декодированные байты (страница приходит с ``content``)."""

    version = "extractor-1"

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        text = _page_bytes(page).decode("utf-8", errors="replace")
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(_page_bytes(page)),
            extractor_version=self.version,
            page_count=1,
        )


class _Chunker:
    """Чанкер: один чанк на документ."""

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


def _build(
    tmp_path: Path,
    *,
    pages: dict[str, bytes],
    urls: dict[str, list[str]],
    store: SqliteStore,
    crawler: _Crawler | None = None,
    manual_artifacts: ManualArtifacts | None = None,
):
    return build_corpus(
        _plan(),
        crawler or _Crawler(pages),
        _Extractor(),
        _Chunker(),
        store,
        urls=urls,
        raw_root=tmp_path,
        run_id=RUN_ID,
        manual_artifacts=manual_artifacts,
    )


def _entry(
    tmp_path: Path,
    *,
    product_id: str = "p1",
    url: str = URL_MANUAL,
    payload: bytes = PAYLOAD,
    declared_path: str = "sources/manual/1C.pdf",
    reason: str | None = "robots_disallow",
) -> ManualArtifact:
    """Объявленная подача с **реальным** файлом внутри пакета (``tmp_path``)."""
    path = tmp_path / declared_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return ManualArtifact(
        product_id=product_id,
        url=url,
        path=path,
        declared_path=declared_path,
        sha256=hashlib.sha256(payload).hexdigest(),
        size=len(payload),
        reason=reason,
        provided_by="Roger",
        provided_at="2026-10-25",
    )


def _docs(store: SqliteStore) -> dict[str, Any]:
    return {doc.url: doc for doc in store.iter_corpus_docs(RUN_ID)}


def _items(store: SqliteStore) -> list[CrawlSessionItem]:
    return list(store.iter_crawl_session_items(SESSION_ID))


def _origin(store: SqliteStore, doc_id: str) -> str:
    row = store.conn.execute(
        "SELECT origin FROM corpus_docs WHERE doc_id = ?", (doc_id,)
    ).fetchone()
    return str(row["origin"])


# --------------------------------------------------------------------------- #
# 1. Артефакт входит в корпус путём I19, файл оператора цел
# --------------------------------------------------------------------------- #


def test_provided_artifact_enters_corpus_through_retain(tmp_path: Path) -> None:
    """``RawPage`` с байтами файла → ``_retain`` → экстракция → чанкер → ``corpus_docs``.

    Один путь байтов (I19): retained-файл назван хешем **этих** байтов, содержимое
    совпадает, происхождение — ``manual``. Файл оператора остаётся на месте: иначе
    повторный прогон не нашёл бы предоставленное.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    entry = _entry(tmp_path)
    crawler = _Crawler({URL_P1_A: b"page one"})
    try:
        result = _build(
            tmp_path,
            pages={URL_P1_A: b"page one"},
            urls={"p1": [URL_P1_A]},
            store=store,
            crawler=crawler,
            manual_artifacts=ManualArtifacts(entries=(entry,)),
        )
        assert result.status == "ok", result.errors
        doc = _docs(store)[URL_MANUAL]
        assert doc.product_id == "p1"
        raw = Path(doc.raw_path)
        assert raw.parent == tmp_path / "p1"
        # Имя retained-файла — хеш байтов (I19): тот же sha256, что объявлен и вычислен.
        assert raw.name == f"{entry.sha256}.bin"
        assert raw.read_bytes() == PAYLOAD
        # Файл оператора не потреблён и не изменён.
        assert entry.path.is_file()
        assert entry.path.read_bytes() == PAYLOAD
        # Происхождение байтов — отдельное поле, а не догадка по URL.
        assert _origin(store, doc.doc_id) == ORIGIN_MANUAL
        # Аудит предоставления: акт назван (кто, когда, что объявлено, что ретенировано).
        audit = list(store.iter_manual_artifacts(RUN_ID))
        assert len(audit) == 1
        assert audit[0]["doc_id"] == doc.doc_id
        assert audit[0]["raw_path"] == doc.raw_path
        assert audit[0]["declared_path"] == entry.declared_path
        assert audit[0]["provided_by"] == "Roger"
        assert audit[0]["byte_size"] == len(PAYLOAD)
        # Краул по этому URL не ходил вовсе: артефакт — не «повторная попытка фетча».
        assert URL_MANUAL not in [url for url, _plan in crawler.calls]
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# 2. Отказ краула не стирается (два факта сосуществуют)
# --------------------------------------------------------------------------- #


def test_refusal_row_survives_manual_provision(tmp_path: Path) -> None:
    """``failed`` + ``robots_disallow`` остаётся; наблюдённость — в ``corpus_docs``."""
    store = SqliteStore(":memory:")
    _make_run(store)
    entry = _entry(tmp_path, url=URL_P1_A)
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            store=store,
            crawler=_Crawler({}, raising={URL_P1_A}),
            manual_artifacts=ManualArtifacts(entries=(entry,)),
        )
        assert result.status == "ok", result.errors
        item = next(iter(_items(store)))
        assert item.url == URL_P1_A
        assert item.status is CrawlItemStatus.FAILED
        assert item.error_kind == FailureKind.ROBOTS_DISALLOW.value
        # Материал при этом наблюдён — и это видно отдельным фактом.
        doc = _docs(store)[URL_P1_A]
        assert _origin(store, doc.doc_id) == ORIGIN_MANUAL
        # Сеанс остался честным: ``fetched`` не появился из воздуха.
        assert not [i for i in _items(store) if i.status is CrawlItemStatus.FETCHED]
    finally:
        store.close()


def test_coverage_counts_gap_as_closed_but_is_not_a_walk(tmp_path: Path) -> None:
    """Подача сохраняет аудит и закрывает пробел материала, но **не** удостоверяет обход.

    F3 (план 20 §11.2, I2d): ``adopted``-материал наблюдён, поэтому ``resolved_manually``
    назван, ``blocked`` остаётся аудитом, а ``observed_sources`` — счётчиком **краула**
    (0). ``complete`` при этом ``False``: предоставление не является обходом корня, и
    «корень исчерпан» оно не утверждает. ``bounded``-ячейка на таком корпусе останется
    ``❓`` — это ужесточение §11.4 п.2, а не регресс аудита.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    entry = _entry(tmp_path, url=URL_P1_A)
    try:
        _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            store=store,
            crawler=_Crawler({}, raising={URL_P1_A}),
            manual_artifacts=ManualArtifacts(entries=(entry,)),
        )
        coverage = store.read_corpus_coverage(RUN_ID, "p1")
        assert coverage is not None
        assert coverage.blocked == {"robots_disallow": 1}
        assert coverage.partial_blocks == {"docs.test": {"robots_disallow": 1}}
        assert coverage.resolved_manually == 1
        assert coverage.observed_sources == 0
        assert coverage.complete is False
        assert coverage.unreadable_origins == []
        # Продукт без подачи — прежнее поведение: подача не размазывается по когорте.
        assert store.read_corpus_coverage(RUN_ID, "p2") is None
    finally:
        store.close()


def test_unresolved_refusal_keeps_coverage_incomplete(tmp_path: Path) -> None:
    """Без подачи тот же отказ по-прежнему делает покрытие неполным (регресс-пин)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            store=store,
            crawler=_Crawler({}, raising={URL_P1_A}),
        )
        coverage = store.read_corpus_coverage(RUN_ID, "p1")
        assert coverage is not None
        assert coverage.complete is False
        assert coverage.resolved_manually == 0
    finally:
        store.close()


def test_derive_coverage_resolution_is_bound_to_url_and_kind() -> None:
    """Единица: закрывается только **свой** URL и только он перестаёт быть потерей.

    ``not_found`` — разрешённое наблюдение отсутствия: он и без подачи не блокирует,
    поэтому в ``resolved_manually`` не попадает (иначе счётчик завышал бы «закрытые
    пробелы»).
    """
    robots = CrawlSessionItem(
        session_id=SESSION_ID,
        product_id="p1",
        url=URL_P1_A,
        status=CrawlItemStatus.FAILED,
        error_kind=FailureKind.ROBOTS_DISALLOW.value,
        attempts=1,
    )
    missing = CrawlSessionItem(
        session_id=SESSION_ID,
        product_id="p1",
        url="https://docs.test/p1/gone",
        status=CrawlItemStatus.FAILED,
        error_kind=FailureKind.NOT_FOUND.value,
    )
    fetched = CrawlSessionItem(
        session_id=SESSION_ID,
        product_id="p1",
        url=URL_P2_A,
        status=CrawlItemStatus.FETCHED,
    )
    coverage = derive_corpus_coverage(
        product_id="p1",
        items=[robots, missing, fetched],
        truncated=False,
        computed_at=TS,
        manual_urls=[URL_P1_A],
    )
    assert coverage.resolved_manually == 1
    assert coverage.blocked == {"not_found": 1, "robots_disallow": 1}
    assert coverage.partial_blocks == {"docs.test": {"robots_disallow": 1}}
    # F3: подача закрывает пробел материала, но не удостоверяет обход корня; корень без
    # снятия ``complete=1`` оставляет покрытие неполным.
    assert coverage.complete is False

    # Подача по **чужому** URL пробел не закрывает.
    other = derive_corpus_coverage(
        product_id="p1",
        items=[robots, fetched],
        truncated=False,
        computed_at=TS,
        manual_urls=["https://docs.test/p1/other.pdf"],
    )
    assert other.resolved_manually == 0
    assert other.complete is False

    # Неблокирующий класс пробела не открывал — вклад подачи не завышается: потеря
    # снята (материал наблюдён), но ``resolved_manually`` остаётся нулём.
    unavailable = CrawlSessionItem(
        session_id=SESSION_ID,
        product_id="p1",
        url=URL_P1_B,
        status=CrawlItemStatus.FAILED,
        error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
        attempts=1,
    )
    soft = derive_corpus_coverage(
        product_id="p1",
        items=[unavailable],
        truncated=False,
        computed_at=TS,
        manual_urls=[URL_P1_B],
    )
    assert soft.resolved_manually == 0
    assert soft.blocked == {"source_unavailable": 1}
    assert soft.partial_blocks == {"docs.test": {"source_unavailable": 1}}
    assert soft.unreadable_origins == []


# --------------------------------------------------------------------------- #
# 3. Декларация: сверка хеша, пути, хоста
# --------------------------------------------------------------------------- #


def _package_with_declaration(
    tmp_path: Path,
    *,
    url: str = URL_MANUAL,
    declared_path: str = "sources/manual/1C.pdf",
    sha256: str | None = None,
    product_id: str = "p1",
    write_artifact: bool = True,
) -> Path:
    """Пакет с ``cohort.yaml`` (p1/p2) и декларацией ``sources/manual_artifacts.yaml``."""
    import yaml

    package = tmp_path / "package"
    package.mkdir(parents=True)
    (package / "cohort.yaml").write_text(
        yaml.safe_dump(
            {
                "case_id": "case",
                "frozen_at": "2026-01-01",
                "entries": [
                    {
                        "product_id": "p1",
                        "role_overlap": 1.0,
                        "selection_reason": "anchor",
                        "anchor_relation": "anchor",
                    },
                    {
                        "product_id": "p2",
                        "role_overlap": 0.5,
                        "selection_reason": "analog",
                        "anchor_relation": "analog",
                    },
                ],
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    if write_artifact:
        target = package / declared_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(PAYLOAD)
    record: dict[str, Any] = {
        "product_id": product_id,
        "url": url,
        "path": declared_path,
        "reason": "robots_disallow",
    }
    if sha256 is not None:
        record["sha256"] = sha256
    declaration = package / MANUAL_ARTIFACTS_FILE
    declaration.parent.mkdir(parents=True, exist_ok=True)
    declaration.write_text(
        yaml.safe_dump(
            {"schema": "manual_artifacts/1", "provide": [record]},
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return package


def test_declaration_loads_and_path_resolves_against_package(tmp_path: Path) -> None:
    """Путь резолвится от каталога **пакета**, а не от ``cwd``; хеш сверяется."""
    package = _package_with_declaration(tmp_path)
    loaded = load_manual_artifacts(package)
    assert loaded.count == 1
    entry = loaded.entries[0]
    assert entry.path == (package / "sources/manual/1C.pdf").resolve()
    assert entry.sha256 == hashlib.sha256(PAYLOAD).hexdigest()
    assert loaded.digest() == manual_artifacts_digest(
        [("p1", URL_MANUAL, hashlib.sha256(PAYLOAD).hexdigest())]
    )


def test_sha256_mismatch_is_a_visible_error(tmp_path: Path) -> None:
    """Объявленный и несовпавший ``sha256`` — ошибка (сверка, не основание доверия)."""
    package = _package_with_declaration(tmp_path, sha256="0" * 64)
    with pytest.raises(ValueError) as excinfo:
        load_manual_artifacts(package)
    message = str(excinfo.value)
    assert "sha256" in message
    assert "0" * 64 in message


def test_path_outside_package_is_rejected(tmp_path: Path) -> None:
    """Абсолютный путь и выход за пакет отвергаются: декларация переезжает с пакетом."""
    package = _package_with_declaration(tmp_path, declared_path="../outside.pdf")
    (tmp_path / "outside.pdf").write_bytes(PAYLOAD)
    with pytest.raises(ValueError) as excinfo:
        load_manual_artifacts(package)
    assert "за каталог пакета" in str(excinfo.value)

    absolute = _package_with_declaration(
        tmp_path / "abs", declared_path="sources/manual/1C.pdf"
    )
    import yaml

    declaration = absolute / MANUAL_ARTIFACTS_FILE
    payload = yaml.safe_load(declaration.read_text(encoding="utf-8"))
    payload["provide"][0]["path"] = str((absolute / "sources/manual/1C.pdf").resolve())
    declaration.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )
    with pytest.raises(ValueError) as excinfo:
        load_manual_artifacts(absolute)
    assert "абсолютный" in str(excinfo.value)


def test_ssrf_url_is_rejected_at_load(tmp_path: Path) -> None:
    """Ссылка в облачную метадату не становится входом: SSRF-политика одна на прогон."""
    package = _package_with_declaration(
        tmp_path, url="http://169.254.169.254/latest/meta-data/"
    )
    with pytest.raises(ValueError):
        load_manual_artifacts(package)


def test_absent_declaration_is_empty_and_digest_is_stable(tmp_path: Path) -> None:
    """Нет файла — не ошибка: пустой набор и **стабильный** дайджест пустого множества."""
    package = _package_with_declaration(tmp_path, write_artifact=False)
    (package / MANUAL_ARTIFACTS_FILE).unlink()
    loaded = load_manual_artifacts(package)
    assert loaded.count == 0 and loaded.path is None
    assert loaded.digest() == manual_artifacts_digest(())
    # «Пакета нет», «файла нет» и «файл пуст» — один и тот же отпечаток пустого множества.
    assert load_manual_artifacts(None).digest() == loaded.digest()
    (package / MANUAL_ARTIFACTS_FILE).write_text("", encoding="utf-8")
    assert load_manual_artifacts(package).digest() == loaded.digest()


def test_digest_is_execution_contract_field() -> None:
    """Дайджест — поле контракта, и материал — ``(product_id, url, sha256)`` без провенанса."""
    assert "manual_artifacts_digest" in EXECUTION_CONTRACT_FIELDS
    base = [("p1", URL_MANUAL, "a" * 64)]
    assert manual_artifacts_digest(base) == manual_artifacts_digest(list(reversed(base)))
    # Провенанс записи (path/provided_by/provided_at/reason) в материал не входит.
    assert manual_artifacts_digest(base) == manual_artifacts_digest(
        [("p1", URL_MANUAL, "a" * 64)]
    )
    # Смена содержимого файла (sha256) меняет отпечаток → меняет run_id.
    assert manual_artifacts_digest(base) != manual_artifacts_digest(
        [("p1", URL_MANUAL, "b" * 64)]
    )
    # Продукт входит в материал: один файл двум продуктам — не то же измерение.
    assert manual_artifacts_digest(base) != manual_artifacts_digest(
        [("p2", URL_MANUAL, "a" * 64)]
    )


# --------------------------------------------------------------------------- #
# 4. Допуск: чужой хост и общая копия для двух продуктов
# --------------------------------------------------------------------------- #


def test_foreign_host_is_refused_but_audited(tmp_path: Path) -> None:
    """Хост вне объявленных — отказ: предоставление не расширяет scope краула."""
    store = SqliteStore(":memory:")
    _make_run(store)
    entry = _entry(tmp_path, url=URL_FOREIGN)
    try:
        result = _build(
            tmp_path,
            pages={URL_P1_A: b"page one"},
            urls={"p1": [URL_P1_A]},
            store=store,
            manual_artifacts=ManualArtifacts(entries=(entry,)),
        )
        assert any("вне объявленных хостов" in error for error in result.errors)
        assert URL_FOREIGN not in _docs(store)
        # Отказ виден в аудите: объявлено, но не допущено (не «тихо не было»).
        audit = list(store.iter_manual_artifacts(RUN_ID))
        assert len(audit) == 1 and audit[0]["doc_id"] is None
        assert audit[0]["url"] == URL_FOREIGN
    finally:
        store.close()


def test_same_file_for_two_products_keeps_two_docs_one_raw(tmp_path: Path) -> None:
    """Симметрия: один файл двум продуктам — два документа, по копии байтов на партицию.

    Retention продукт-партиционирован (``corpus_raw/{product_id}/{hash}.bin``, I17), поэтому
    «один хеш — один retained-файл» означает **внутри** партиции; имя файла в обеих
    партициях одно и то же — хеш байтов, а ``doc_id`` включает продукт, поэтому строки
    ``corpus_docs`` не схлопываются в чужую.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    first = _entry(tmp_path, product_id="p1")
    second = _entry(tmp_path, product_id="p2")
    try:
        result = _build(
            tmp_path,
            pages={URL_P1_A: b"page one", URL_P2_A: b"page two"},
            urls={"p1": [URL_P1_A], "p2": [URL_P2_A]},
            store=store,
            manual_artifacts=ManualArtifacts(entries=(first, second)),
        )
        assert result.status == "ok", result.errors
        docs = [doc for doc in store.iter_corpus_docs(RUN_ID) if doc.url == URL_MANUAL]
        assert len(docs) == 2
        # ``doc_id`` включает продукт: строки не схлопываются в одну чужую.
        assert {doc.product_id for doc in docs} == {"p1", "p2"}
        assert {Path(doc.raw_path).name for doc in docs} == {f"{first.sha256}.bin"}
        assert {Path(doc.raw_path).parent.name for doc in docs} == {"p1", "p2"}
        for doc in docs:
            assert Path(doc.raw_path).read_bytes() == PAYLOAD
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# 5. Манифест: доля ручного публикуется числом
# --------------------------------------------------------------------------- #


def test_manifest_summary_publishes_share_and_symmetry(tmp_path: Path) -> None:
    """``count``/``share_of_corpus``/``bytes``/``by_reason``/``by_product`` — из аудита."""
    store = SqliteStore(":memory:")
    _make_run(store)
    entry = _entry(tmp_path)
    try:
        _build(
            tmp_path,
            pages={URL_P1_A: b"page one"},
            urls={"p1": [URL_P1_A]},
            store=store,
            manual_artifacts=ManualArtifacts(entries=(entry,)),
        )
        summary = manual_artifacts_summary(store, RUN_ID, products=["p1", "p2"])
        assert summary["count"] == 1
        assert summary["declared"] == 1
        assert summary["bytes"] == len(PAYLOAD)
        assert summary["by_reason"] == {"robots_disallow": 1}
        # Симметрия видна: у p2 ноль, а не «ключа нет».
        assert summary["by_product"] == {"p1": 1, "p2": 0}
        total = sum(1 for _ in store.iter_corpus_docs(RUN_ID))
        assert total == 2
        assert summary["share_of_corpus"] == pytest.approx(1 / total)
    finally:
        store.close()


def test_manifest_summary_without_provisions_is_empty() -> None:
    """Нет подач — блок нулевой и с нулями по продуктам (не пустой словарь)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        summary = manual_artifacts_summary(store, RUN_ID, products=["p1"])
        assert summary["count"] == 0 and summary["declared"] == 0
        assert summary["share_of_corpus"] == 0.0
        assert summary["by_product"] == {"p1": 0}
        assert manual_artifacts_summary(None, RUN_ID, products=["p1"])["count"] == 0
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
