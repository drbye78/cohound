"""Страница у **цитаты**: пара страниц, причина отсутствия и публикация (O36 фаза 3).

Приёмка фазы 3 плана ``26`` — доказательство называет страницу, а не только документ, и
отсутствие страницы названо **причиной**, а не тишиной. Проверяется цепочка от
``corpus_docs.page_map`` и ``corpus_chunks.char_start`` до строки ``evidence_pages.csv``,
сводки манифеста и цитаты в battle card.

Три предмета, которые легко перепутать:

* **пара** ``page``/``page_end``: чанк, пересекающий границу страниц, обязан назвать
  обе — одно число объявило бы «цитата на стр. 2», когда половина её на 3;
* **причина** отсутствия: ``no_page_map`` (документ есть, карты нет), ``no_char_span``
  (чанк не точный срез), ``no_chunk`` (не корпусная цитата), ``no_doc`` (битая ссылка) —
  четыре разных факта, и «неизвестно» не подменяет ни один из них;
* **один расчёт**: сводка манифеста, файл и цитата матрицы считаются из одного набора
  строк, поэтому разойтись не могут.

Тесты нейтральны (§1.3): документы и адреса вымышленные, карта страниц собрана руками —
реальный PDF здесь не нужен, предмет — арифметика смещений и форма артефакта.
"""

from __future__ import annotations

import csv
import datetime as _dt
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from core.domain import CorpusDoc, Focus, RankingPolicy, RunConfig, RunMode
from core.domain.enums import CellState, EvidenceType, SourceKind, Status
from core.domain.models import Cell, CellEvidence, CorpusChunk, LLMEvidence, RunManifest
from core.pipeline.compute import compute
from core.pipeline.page_publication import (
    EVIDENCE_CSV_COLUMNS,
    PAGE_STATUS_MAPPED,
    PAGE_STATUS_NO_CHAR_SPAN,
    PAGE_STATUS_NO_CHUNK,
    PAGE_STATUS_NO_DOC,
    PAGE_STATUS_NO_PAGE_MAP,
    collect_evidence_pages,
    evidence_pages_summary,
    human_line,
    pages_by_chunk,
    write_evidence_pages_artifact,
)
from core.pipeline.report import render_battle_card
from core.store.sqlite_store import SqliteStore
from tests.pipeline.test_report import (
    PRODUCT_A,
    _artifact_text,
    _catalog,
    _CellView,
    _cfg,
    _Ev,
)

RUN_ID = "run-evidence-pages"
PRODUCT = "p1"
OTHER_PRODUCT = "p2"

#: Три «страницы» одного документа, разделённые ``"\n\n"`` — ровно так их склеивает
#: читатель ``text_layer``; карта страниц ниже построена по этим смещениям.
_PAGES = ("первая страница", "вторая страница", "третья страница")
DOC_TEXT = "\n\n".join(_PAGES)
_map: list[tuple[int, int, int]] = []
_offset = 0
for _number, _page in enumerate(_PAGES, start=1):
    _map.append((_number, _offset, _offset + len(_page)))
    _offset += len(_page) + 2  # разделитель ``"\n\n"`` — разрыв карты, не страница
DOC_PAGE_MAP: tuple[tuple[int, int, int], ...] = tuple(_map)

#: Смещения кусков, собранных **срезами** текста: ``CorpusChunk`` требует, чтобы длина
#: ``text`` равнялась длине диапазона, иначе это ложная провенанс-запись.
_FIRST_PAGE = (DOC_PAGE_MAP[0][1], DOC_PAGE_MAP[0][2])
_CROSSING = (DOC_PAGE_MAP[1][1] + 2, DOC_PAGE_MAP[2][1] + 4)


def _chunk(
    chunk_id: str,
    doc_id: str,
    span: tuple[int, int] | None,
    *,
    text: str | None = None,
    chunk_no: int = 0,
) -> CorpusChunk:
    """Строка ``corpus_chunks``; ``span=None`` — кусок не является точным срезом."""
    if span is None:
        return CorpusChunk(
            chunk_id=chunk_id,
            doc_id=doc_id,
            product_id=PRODUCT,
            chunk_no=chunk_no,
            text=text or "абзац без смещений",
        )
    start, end = span
    return CorpusChunk(
        chunk_id=chunk_id,
        doc_id=doc_id,
        product_id=PRODUCT,
        chunk_no=chunk_no,
        text=DOC_TEXT[start:end] if text is None else text,
        char_start=start,
        char_end=end,
    )


def _evidence(
    evidence_id: str,
    param_id: str,
    source_id: str,
    *,
    source_kind: SourceKind = SourceKind.DOCUMENTATION,
    source_url: str | None = None,
) -> CellEvidence:
    """Корпусный (по умолчанию) evidence; реестровому обязателен ``source_url`` (R1)."""
    return CellEvidence(
        evidence_id=evidence_id,
        run_id=RUN_ID,
        product_id=PRODUCT,
        param_id=param_id,
        source_kind=source_kind,
        evidence_type=EvidenceType.DIRECT,
        source_id=source_id,
        source_url=source_url,
        citation_quote="цитата",
        content_hash="hash" if source_kind is not SourceKind.REGISTRY else None,
    )


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    """Корпус из двух документов: с картой страниц и без неё, плюс пять видов цитат."""
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
        # Документ 1 — с картой страниц; версия ``dv_a`` отображается в его ``doc_id``.
        instance.upsert_corpus_doc(
            CorpusDoc(
                doc_id=f"{PRODUCT}:hash",
                product_id=PRODUCT,
                url="https://docs.test/p1/three",
                content_hash="hash",
                page_count=3,
                extracted_chars=len(DOC_TEXT),
                page_map=DOC_PAGE_MAP,
            ),
            doc_version_id="dv_a",
        )
        # Документ 2 — без карты (так выглядят HTML/Office/docling/``split``).
        instance.upsert_corpus_doc(
            CorpusDoc(
                doc_id=f"{PRODUCT}:hash2",
                product_id=PRODUCT,
                url="https://docs.test/p1/flat",
                content_hash="hash2",
                extracted_chars=len("кусок"),
            ),
            doc_version_id="dv_b",
        )
        instance.add_chunks_batch(
            [
                _chunk(f"{PRODUCT}:dv_a:0", f"{PRODUCT}:hash", _FIRST_PAGE, chunk_no=0),
                _chunk(f"{PRODUCT}:dv_a:1", f"{PRODUCT}:hash", _CROSSING, chunk_no=1),
                _chunk(f"{PRODUCT}:dv_a:2", f"{PRODUCT}:hash", None, chunk_no=2),
                _chunk(f"{PRODUCT}:dv_b:0", f"{PRODUCT}:hash2", (0, 5), text="кусок"),
                # Чанк с адресом версии, которой нет в ``corpus_doc_version``: документ по
                # адресу не разрешается — битая ссылка, а не «карты нет».
                _chunk(f"{PRODUCT}:dv_missing:0", f"{PRODUCT}:hash", _FIRST_PAGE),
            ]
        )
        for item in (
            _evidence("e1", "param.a", f"{PRODUCT}:dv_a:0"),
            _evidence("e2", "param.b", f"{PRODUCT}:dv_a:1"),
            _evidence("e3", "param.c", f"{PRODUCT}:dv_a:2"),
            _evidence("e4", "param.d", f"{PRODUCT}:dv_b:0"),
            _evidence(
                "e5",
                "param.e",
                f"{PRODUCT}:reg:0",
                source_kind=SourceKind.REGISTRY,
                source_url="https://registry.test/entry",
            ),
            _evidence("e6", "param.f", f"{PRODUCT}:dv_missing:0"),
        ):
            instance.add_evidence(item)
        yield instance


# ---------------------------------------------------------------------------
# (а) страница, пара страниц и четыре причины отсутствия
# ---------------------------------------------------------------------------


def test_page_of_a_quote_is_known(store: SqliteStore) -> None:
    """Чанк внутри страницы 1 называет страницу 1 (и конец — ту же страницу)."""
    rows, _summary_block = collect_evidence_pages(store, RUN_ID)
    row = next(item for item in rows if item["param_id"] == "param.a")
    assert (row["page"], row["page_end"]) == (1, 1)
    assert row["page_status"] == PAGE_STATUS_MAPPED
    assert row["doc_id"] == f"{PRODUCT}:hash"
    assert (row["char_start"], row["char_end"]) == _FIRST_PAGE


def test_quote_crossing_a_page_boundary_names_both_pages(store: SqliteStore) -> None:
    """Чанк, начинающийся на стр. 2 и кончающийся на 3, называет **диапазон** 2–3.

    Одно число здесь было бы ложью: половина цитаты лежит на следующей странице. Конец
    считается по ``char_end - 1`` — последнему символу куска (``char_end`` исключающий).
    """
    rows, _summary_block = collect_evidence_pages(store, RUN_ID)
    row = next(item for item in rows if item["param_id"] == "param.b")
    assert (row["page"], row["page_end"]) == (2, 3)
    assert row["page_status"] == PAGE_STATUS_MAPPED


def test_absence_of_a_page_names_its_own_reason(store: SqliteStore) -> None:
    """Четыре причины отсутствия различимы и не подменяют друг друга."""
    rows, _summary_block = collect_evidence_pages(store, RUN_ID)
    by_param = {item["param_id"]: item for item in rows}
    assert by_param["param.c"]["page_status"] == PAGE_STATUS_NO_CHAR_SPAN
    assert by_param["param.d"]["page_status"] == PAGE_STATUS_NO_PAGE_MAP
    assert by_param["param.e"]["page_status"] == PAGE_STATUS_NO_CHUNK
    assert by_param["param.f"]["page_status"] == PAGE_STATUS_NO_DOC
    # Пустая ячейка — «не известно», а не ноль: страница не выдумывается (A3).
    for param_id in ("param.c", "param.d", "param.e", "param.f"):
        assert by_param[param_id]["page"] is None
        assert by_param[param_id]["page_end"] is None


def test_rows_are_sorted_deterministically(store: SqliteStore) -> None:
    """Порядок — ``(product_id, param_id, source_id)``: файл сравнивают между прогонами."""
    rows, _summary_block = collect_evidence_pages(store, RUN_ID)
    keys = [(row["product_id"], row["param_id"], row["chunk_id"]) for row in rows]
    assert keys == sorted(keys)


def test_unreadable_store_does_not_raise() -> None:
    """Сбой чтения — пустой результат, а не падение отчёта (он вторичен к фазе)."""

    class _Broken:
        def iter_corpus_docs(self, run_id: str) -> Any:
            raise RuntimeError("нет соединения")

    assert collect_evidence_pages(_Broken(), RUN_ID) == ([], {})
    assert collect_evidence_pages(None, RUN_ID) == ([], {})


# ---------------------------------------------------------------------------
# (б) файл: колонки, порядок строк, пустой результат
# ---------------------------------------------------------------------------


def test_artifact_has_declared_columns_and_rows(
    store: SqliteStore, tmp_path: Path
) -> None:
    """CSV несёт объявленные колонки в объявленном порядке и строку на evidence."""
    target = tmp_path / "outputs" / "evidence_pages.csv"
    written = write_evidence_pages_artifact(store, RUN_ID, target)
    assert written == target
    with target.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        assert tuple(reader.fieldnames or ()) == EVIDENCE_CSV_COLUMNS
        rows = list(reader)
    assert [row["param_id"] for row in rows] == [
        "param.a",
        "param.b",
        "param.c",
        "param.d",
        "param.e",
        "param.f",
    ]
    # Диапазон печатается двумя числами; отсутствие — пустой ячейкой, не нулём.
    assert (rows[1]["page"], rows[1]["page_end"]) == ("2", "3")
    assert rows[2]["page"] == ""
    assert rows[2]["page_status"] == PAGE_STATUS_NO_CHAR_SPAN


def test_artifact_is_not_created_without_evidence(tmp_path: Path) -> None:
    """Цитат нет — файла нет: пустая таблица читалась бы как «посмотрели и не нашли»."""
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
        target = tmp_path / "evidence_pages.csv"
        assert write_evidence_pages_artifact(empty, RUN_ID, target) is None
        assert not target.exists()


# ---------------------------------------------------------------------------
# (в) сводка манифеста
# ---------------------------------------------------------------------------


def test_summary_counts_pages_and_reasons(store: SqliteStore) -> None:
    """``with_page`` и ``by_status`` считаются по фактам, а не по ожиданию."""
    summary = evidence_pages_summary(store, RUN_ID, products=[PRODUCT, OTHER_PRODUCT])
    assert summary is not None
    assert summary["evidence"] == 6
    assert summary["with_page"] == 2
    assert summary["by_status"] == {
        PAGE_STATUS_MAPPED: 2,
        PAGE_STATUS_NO_PAGE_MAP: 1,
        PAGE_STATUS_NO_CHAR_SPAN: 1,
        PAGE_STATUS_NO_CHUNK: 1,
        PAGE_STATUS_NO_DOC: 1,
    }
    # Продукт плана без цитат — нулями: «у него цитат нет» тоже факт (симметрия).
    assert summary["by_product"][OTHER_PRODUCT] == {"evidence": 0, "with_page": 0}
    assert summary["by_product"][PRODUCT] == {"evidence": 6, "with_page": 2}


def test_summary_is_none_without_evidence() -> None:
    """Цитат нет вовсе — ``None``, а не ноль: это разные факты."""
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
        assert evidence_pages_summary(empty, RUN_ID, products=[PRODUCT]) is None


# ---------------------------------------------------------------------------
# (г) манифест: запись и чтение
# ---------------------------------------------------------------------------


def _manifest(**overrides: Any) -> RunManifest:
    data: dict[str, Any] = {
        "run_id": RUN_ID,
        "case_id": "case",
        "created_at": "2026-01-01T00:00:00+00:00",
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


def test_manifest_round_trips_page_provenance(store: SqliteStore) -> None:
    """``page_provenance_json`` переживает запись и чтение без потерь."""
    block = {
        "evidence": 6,
        "with_page": 2,
        "by_status": {PAGE_STATUS_MAPPED: 2},
        "by_product": {PRODUCT: {"evidence": 6, "with_page": 2}},
    }
    store.persist_manifest(RUN_ID, _manifest(page_provenance=block))
    stored = store.read_manifest(RUN_ID)
    assert stored is not None
    assert stored.page_provenance == block


def test_manifest_without_page_provenance_reads_as_none(store: SqliteStore) -> None:
    """Манифест без поля (и строка прошлой схемы) читается как ``None``, не как ``{}``.

    ``None`` — «не записано либо цитат не было»; ``{}`` читалось бы как «сводка есть и она
    пуста», то есть как утверждение о цитатах. Колонка аддитивная, поэтому отсутствие
    значения — законное состояние, а не сбой чтения.
    """
    store.persist_manifest(RUN_ID, _manifest())
    stored = store.read_manifest(RUN_ID)
    assert stored is not None
    assert stored.page_provenance is None
    # Строка, у которой колонка пуста (манифест прошлой схемы), — тот же факт.
    store.conn.execute("UPDATE runs SET page_provenance_json = NULL")
    store.conn.commit()
    again = store.read_manifest(RUN_ID)
    assert again is not None
    assert again.page_provenance is None


# ---------------------------------------------------------------------------
# (д) battle card: страница в цитате
# ---------------------------------------------------------------------------


def _coverage_cell() -> Cell:
    """Ячейка, дающая бандлу Coverage: battle card без него не публикуется (I11).

    Параметр и продукт — из общей фикстуры каталога (``tests.pipeline.test_report``), чтобы
    вес ячейки был настоящим: предмет этой проверки — цитата, а не расчёт карты.
    """
    return Cell(
        run_id=RUN_ID,
        product_id=PRODUCT_A,
        param_id="param.ok",
        state=CellState.EVALUATED,
        status=Status.OK,
        evidence=[LLMEvidence(source_id="src.1", quote="цитата")],
    )


def test_battle_card_quote_names_the_page(tmp_path: Path) -> None:
    """Цитата в матрице получает ``(стр. N)`` либо ``(стр. N–M)`` из той же карты.

    Диапазон, а не одно число: чанк может пересекать границу страниц. Карта строится из
    строк артефакта (:func:`pages_by_chunk`) — тем же расчётом, что и файл.
    """
    rows = [
        {"chunk_id": "p1:dv_a:0", "page": 1, "page_end": 1},
        {"chunk_id": "p1:dv_a:1", "page": 2, "page_end": 3},
        {"chunk_id": "p1:dv_a:2", "page": None, "page_end": None},
    ]
    pages = pages_by_chunk(rows)
    assert pages == {"p1:dv_a:0": (1, 1), "p1:dv_a:1": (2, 3)}

    with SqliteStore(":memory:") as store:
        store.create_run(RUN_ID, _cfg())
        # Ячейка в сторе нужна только Coverage: без неё battle card не публикуется (I11),
        # а предмет проверки — цитата, а не расчёт карты.
        store.save_cell(_coverage_cell())
        bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})
        cells = [
            _CellView(
                product_id=PRODUCT,
                param_id="param.a",
                state=CellState.EVALUATED,
                status=Status.OK,
                evidence=[_Ev("p1:dv_a:1", "цитата с двух страниц")],
            )
        ]
        path = render_battle_card(
            bundle, cells, _catalog(), tmp_path / "battle_card.xlsx", pages=pages
        )
    text = _artifact_text(path)
    assert "(стр. 2–3)" in text


def test_battle_card_quote_without_page_adds_nothing(tmp_path: Path) -> None:
    """Нет карты — нет и «стр. ?»: неизвестная страница не превращается в знак."""
    with SqliteStore(":memory:") as store:
        store.create_run(RUN_ID, _cfg())
        # Та же причина: карта без Coverage не публикуется (I11).
        store.save_cell(_coverage_cell())
        bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})
        cells = [
            _CellView(
                product_id=PRODUCT,
                param_id="param.a",
                state=CellState.EVALUATED,
                status=Status.OK,
                evidence=[_Ev("p1:dv_a:0", "цитата без страницы")],
            )
        ]
        without = render_battle_card(
            bundle, cells, _catalog(), tmp_path / "no_pages.xlsx", pages=None
        )
        with_other = render_battle_card(
            bundle,
            cells,
            _catalog(),
            tmp_path / "other_pages.xlsx",
            pages={"p1:dv_a:9": (4, 4)},
        )
    assert "стр." not in _artifact_text(without)
    assert "стр." not in _artifact_text(with_other)


# ---------------------------------------------------------------------------
# человеческая строка (notes CLI)
# ---------------------------------------------------------------------------


def test_human_line_names_all_pages_when_every_quote_has_one() -> None:
    """Все цитаты назвали страницу — так и говорится, без перечисления причин."""
    summary = {
        "evidence": 12,
        "with_page": 12,
        "by_status": {PAGE_STATUS_MAPPED: 12},
        "by_product": {PRODUCT: {"evidence": 12, "with_page": 12}},
    }
    assert human_line(summary) == "цитаты: все 12 называют страницу"


def test_human_line_names_reasons_and_skips_zero_classes() -> None:
    """Часть цитат без страницы — причина по классам, нулевые классы не упоминаются.

    Без причины «5 из 8» читается как дефект прибора, хотя за ним стоит известное
    ограничение механизма. Битая ссылка — отдельным хвостом: это дефект данных, и
    смешивать его с «карты нет» значило бы прятать поломку в списке причин.
    """
    summary = {
        "evidence": 8,
        "with_page": 5,
        "by_status": {
            PAGE_STATUS_MAPPED: 5,
            PAGE_STATUS_NO_PAGE_MAP: 2,
            PAGE_STATUS_NO_DOC: 1,
        },
        "by_product": {PRODUCT: {"evidence": 8, "with_page": 5}},
    }
    line = human_line(summary)
    assert line == (
        "цитаты: 5 из 8 называют страницу; без карты документа 2, битая ссылка 1"
    )
    assert "без смещений чанка" not in line
    assert "вне корпуса" not in line


def test_human_line_is_silent_without_evidence() -> None:
    """Сказать нечего — строки нет (а не «0 из 0»)."""
    assert human_line(None) is None
    assert human_line({}) is None
    assert human_line({"evidence": 0, "with_page": 0}) is None


# ---------------------------------------------------------------------------
# строка в notes команды ``report``
# ---------------------------------------------------------------------------


def test_report_command_prints_evidence_pages_line(
    store: SqliteStore, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``hound report`` называет факт строкой, а не только файлом, который надо открыть.

    Строка идёт в ``notes`` — рядом с частичным извлечением и качеством поддержки: это
    тот же класс факта (наблюдение прибора, которое иначе видно лишь в ``outputs/``).
    """
    from core.cli.commands import report as report_command

    db_path = tmp_path / "cohound.db"
    with SqliteStore(str(db_path)) as persisted:
        persisted.create_run(RUN_ID, _cfg())
        persisted.upsert_corpus_doc(
            CorpusDoc(
                doc_id=f"{PRODUCT}:hash",
                product_id=PRODUCT,
                url="https://docs.test/p1/three",
                content_hash="hash",
                page_count=3,
                extracted_chars=len(DOC_TEXT),
                page_map=DOC_PAGE_MAP,
            ),
            doc_version_id="dv_a",
        )
        persisted.add_chunks_batch(
            [_chunk(f"{PRODUCT}:dv_a:0", f"{PRODUCT}:hash", _FIRST_PAGE)]
        )
        persisted.add_evidence(_evidence("e1", "param.a", f"{PRODUCT}:dv_a:0"))

    args = SimpleNamespace(
        db=str(db_path),
        runs_dir=None,
        run_id=RUN_ID,
        run_yaml=None,
        package=None,
        out=str(tmp_path / "out"),
        json=True,
        profile="local",
        env_file=None,
    )
    assert report_command.run(args) == 0
    payload = capsys.readouterr().out
    assert "цитаты: все 1 называют страницу" in payload
    assert (tmp_path / "out" / RUN_ID / "outputs" / "evidence_pages.csv").exists()


def test_report_without_evidence_publishes_no_summary(tmp_path: Path) -> None:
    """Цитат нет — сводки нет (``None``), а не сводка из нулей.

    Разница не косметическая: ``{"by_product": {...нули...}}`` без полей ``evidence``/
    ``with_page`` — это объект, у которого нет числа, и читатель артефакта не отличит
    «цитат не было» от «сводку не собрали». ``None`` говорит ровно одно: публиковать
    нечего, потому что публиковать нечего (а не потому что прибор сломался).
    """
    from core.pipeline.report import write_reports

    with SqliteStore(":memory:") as store:
        store.create_run(RUN_ID, _cfg())
        # Ячейка без evidence: прогон состоялся, корпусных цитат нет.
        store.save_cell(_coverage_cell())
        bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})
        artifacts = write_reports(
            bundle=bundle,
            cells=list(store.iter_cells(RUN_ID)),
            catalog=_catalog(),
            manifest=_manifest(),
            out_dir=tmp_path,
            run_id=RUN_ID,
            store=store,
        )
    assert artifacts.evidence_pages_summary is None
    assert artifacts.evidence_pages is None
