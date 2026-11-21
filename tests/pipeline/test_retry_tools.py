"""Путь повтора исполняет **то же** условие, что фаза: инструменты и табличная шапка.

Дефект, который здесь закрывается, был измерен, а не предположен: ``hound retry`` и
``hound revalidate`` идут не через :func:`evaluate_batch`, а через
:func:`_evaluate_cells_by_condition`, и этот путь не передавал ни профиль инструментов, ни
окно в корпус, ни транспорт, ни таблицы — и не писал трассу (``store=None``). Следствия
были ровно те, которые нельзя увидеть глазами:

* ``run_id`` объявлял ``tool_profile``, а повторённая ячейка исполнялась **без**
  инструментов — два прогона под одним идентификатором мерили разные условия;
* табличная шапка терялась именно на пути ремонта;
* ``cell_tool_calls`` у повторённых ячеек был пуст, то есть аудит «сколько вызовов стоил
  вердикт» видел ноль у самых дорогих ячеек.

Что проверяется и почему:

* включённый профиль без транспорта — отказ сборки (:class:`ToolWiringError`), а не тихий
  откат: профиль входит в ``run_id``, и прогон, объявивший его, обязан его исполнить;
* адрес таблицы доезжает до промпта повтора (строка ``столбцы:``) — это доказывает, что
  обогащение идёт **в вызывающем потоке** и до пула;
* трасса пишется в store, ровно один раз на ячейку, и **из вызывающего потока** даже при
  ``llm_concurrency > 1``: у SQLite один писатель (``check_same_thread``);
* окно в корпус строится **по продукту** (I17) и один раз на продукт, а не на ячейку;
* пустой профиль — прежнее поведение: один ``complete`` и ни одной строки трассы.

Работают на ``SqliteStore(":memory:")``; оценщик — скриптованный in-memory порт (I18).
Нейтральны к предметной категории (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import re
import threading
from collections.abc import Iterator, Sequence
from typing import Any

import pytest

from core.domain import (
    AbsenceRuleType,
    CatalogRow,
    Cell,
    CellState,
    CorpusChunk,
    CorpusDoc,
    Focus,
    Importance,
    LLMCellOutput,
    LLMEvidence,
    ParamType,
    RankingPolicy,
    RunConfig,
    RunMode,
    SourceKind,
    StaleReason,
    Status,
)
from core.domain.doc_tables import ExtractedTable, TableRef
from core.domain.tools import ToolInvocation, ToolProfile
from core.pipeline import Ports
from core.pipeline import evaluate as evaluate_module
from core.pipeline.evaluate import (
    ToolWiringError,
    reevaluate_stale,
    retry_failed_cells,
)
from core.pipeline.tool_corpus import StoreToolCorpus
from core.ports.llm import LLMRequest, LLMResponse, ToolTurn, ToolTurnRequest
from core.ports.vector_index import RetrievedChunk
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-retry-tools"
BUILD = "cb_new"
DOC_ID = "p1:hash1"
#: Фрагмент с текстом: из него берётся цитата (I22 сверяет ``text``).
PLAIN_CHUNK = "p1:dv_1:0"
#: Фрагмент таблицы: у него есть адрес внутри таблицы и шапка печатается по объекту.
TABLE_CHUNK = "p1:dv_2:0"
QUOTE = "Поддержка версии 18 подтверждена."
TABLE_TEXT = "| 18 | да |"

#: Профиль T1: ``expand_to_section`` с бюджетом два вызова на ячейку.
_PROFILE = ToolProfile(name="t1", tools=("expand_to_section",), max_calls=2)


# ---------------------------------------------------------------------------
# Данные
# ---------------------------------------------------------------------------


def _param(index: int = 0) -> CatalogRow:
    return CatalogRow(
        param_id=f"param_{index:02d}",
        title=f"Параметр {index}",
        module="mod_a",
        group="grp_1",
        importance=Importance.BASIC,
        param_type=ParamType.FACT,
        rule=f"Подтвердить наличие параметра {index}.",
        absence_rule=AbsenceRuleType.EXPLICIT,
        role_binding="role_a",
        concept_terms={"ru": [f"параметр {index}"], "en": [f"parameter {index}"]},
        search_langs=["ru"],
    )


def _cell(
    product_id: str = "p1",
    param_id: str = "param_00",
    *,
    state: CellState = CellState.FAILED,
    status: Status | None = None,
    index_build_id: str = BUILD,
) -> Cell:
    """Ячейка прогона; по умолчанию — без вердикта (ровно то, что отбирает повтор)."""
    # I1: ``✅``/``⚠️`` без evidence не существует — вердикт несёт цитату.
    evidence = (
        [LLMEvidence(source_id=PLAIN_CHUNK, quote=QUOTE)]
        if status is Status.OK
        else []
    )
    return Cell(
        run_id=RUN_ID,
        product_id=product_id,
        param_id=param_id,
        state=state,
        status=status,
        source_kind=SourceKind.DOCUMENTATION,
        evidence=evidence,
        index_build_id=index_build_id,
    )


def _chunk(
    chunk_id: str, *, product_id: str = "p1", text: str = QUOTE, heading: str = "Раздел 1"
) -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=chunk_id,
        product_id=product_id,
        text=text,
        content_hash=f"hash-{chunk_id}",
        heading_path=heading,
        build_id=BUILD,
        score=1.0,
    )


def _table() -> ExtractedTable:
    return ExtractedTable(
        table_id="t0",
        caption="Поддержка версий",
        header=("версия", "поддержка"),
        rows=(("18", "да"),),
    )


def _ok_output(chunk_id: str = PLAIN_CHUNK, quote: str = QUOTE) -> LLMCellOutput:
    return LLMCellOutput(
        status=Status.OK,
        justification="Свойство подтверждено документацией.",
        evidence=[{"source_id": chunk_id, "quote": quote}],
    )


def _seed_corpus(store: SqliteStore, product_id: str = "p1") -> None:
    """Документ с таблицей и два фрагмента: обычный текст и строка таблицы."""
    doc_id = DOC_ID if product_id == "p1" else f"{product_id}:hash1"
    plain = PLAIN_CHUNK if product_id == "p1" else f"{product_id}:dv_1:0"
    table = TABLE_CHUNK if product_id == "p1" else f"{product_id}:dv_2:0"
    store.upsert_corpus_doc(
        CorpusDoc(
            doc_id=doc_id,
            product_id=product_id,
            url=f"https://docs.test/{product_id}/d1",
            content_hash="hash1",
            tables=(_table(),),
        ),
        doc_version_id=f"{product_id}:dv_1",
    )
    store.upsert_corpus_doc(
        CorpusDoc(
            doc_id=doc_id,
            product_id=product_id,
            url=f"https://docs.test/{product_id}/d1",
            content_hash="hash1",
        ),
        doc_version_id=f"{product_id}:dv_2",
    )
    store.add_chunks_batch(
        [
            CorpusChunk(
                chunk_id=plain,
                doc_id=doc_id,
                product_id=product_id,
                chunk_no=0,
                text=QUOTE,
                heading_path="Раздел 1",
                content_hash=f"hash-{plain}",
            ),
            CorpusChunk(
                chunk_id=table,
                doc_id=doc_id,
                product_id=product_id,
                chunk_no=1,
                text=TABLE_TEXT,
                heading_path="Раздел 2",
                content_hash=f"hash-{table}",
                table_ref=TableRef("t0", 1, 1),
            ),
        ]
    )


class _TraceSpyStore(SqliteStore):
    """Хранилище, запоминающее **поток**, из которого пришла трасса вызовов.

    Запись из воркера в SQLite запрещена (``check_same_thread``), поэтому идентификатор
    потока — прямой признак соблюдения правила «один писатель»: проверяется он, а не факт
    отсутствия исключения.
    """

    def __init__(self) -> None:
        super().__init__(":memory:")
        self.trace_threads: list[int] = []

    def save_tool_calls(self, run_id: str, entries: Sequence[Any]) -> int:
        self.trace_threads.append(threading.get_ident())
        return super().save_tool_calls(run_id, entries)


@pytest.fixture()
def store() -> Iterator[_TraceSpyStore]:
    with _TraceSpyStore() as s:
        s.create_run(
            RUN_ID,
            RunConfig(
                case_id="case-1",
                anchor="p1",
                cohort="cohort.yaml",
                products=["p1", "p2"],
                package="pkg-1",
                focus=Focus(),
                languages=["ru"],
                snapshot_date=_dt.date(2026, 1, 1),
                mode=RunMode.STRICT,
                ranking_policy=RankingPolicy.CAPABILITY_RANKED,
            ),
        )
        s.set_run_index_build_id(RUN_ID, BUILD)
        _seed_corpus(s, "p1")
        yield s


# ---------------------------------------------------------------------------
# Двойники
# ---------------------------------------------------------------------------


class _LexicalStub:
    """Лексический порт: фиксированный срез по продуктам (R0 — без эмбеддера)."""

    engine_version = "stub-1"

    def __init__(self, build_id: str, chunks: Sequence[RetrievedChunk]) -> None:
        self.build_id = build_id
        self.chunks = list(chunks)

    def search(
        self,
        query: str,
        product_ids: list[str],
        top_k: int,
        lang: str | None = None,
    ) -> list[RetrievedChunk]:
        scoped = [c for c in self.chunks if c.product_id in set(product_ids)]
        return scoped[:top_k]


class _ToolEvaluator:
    """Оценщик с ``turn``: первый ход — вызов инструмента, второй — вердикт.

    Состояние — **по промпту**, а не по счётчику объекта: у каждой ячейки свой диалог, и
    общий счётчик дал бы вызов только первой из параллельных ячеек.
    """

    _RESULT_MARKER = "=== ЛОКАЛЬНЫЙ ИНСТРУМЕНТ"

    model_id = "fake-model"
    model_version = "1.0"
    seed = 0
    temperature = 0.0
    decoder = "none"

    def __init__(self, *, chunk_id: str = PLAIN_CHUNK) -> None:
        self._chunk_id = chunk_id
        self.calls: list[LLMRequest] = []
        self.requests: list[ToolTurnRequest] = []

    def complete(self, req: LLMRequest) -> LLMResponse:
        self.calls.append(req)
        output = _ok_output()
        return LLMResponse(
            output=output, raw=output.model_dump_json(), prompt_hash="ph", decoder="fake"
        )

    def turn(self, req: ToolTurnRequest) -> ToolTurn:
        self.requests.append(req)
        if self._RESULT_MARKER not in req.prompt:
            return ToolTurn(
                tool_calls=(
                    ToolInvocation(
                        name="expand_to_section",
                        arguments={"chunk_id": self._chunk_id},
                    ),
                ),
                raw="{}",
                finish_reason="tool_calls",
            )
        output = _ok_output()
        return ToolTurn(
            final=LLMResponse(
                output=output,
                raw=output.model_dump_json(),
                prompt_hash="ph-turn",
                decoder="none",
            ),
            raw=output.model_dump_json(),
            finish_reason="stop",
        )


class _PlainEvaluator:
    """Оценщик без ``turn``: путь без инструментов (один ``complete``).

    Цитата берётся из **первого адреса в промпте**, а не из поля двойника: набор может
    нести несколько продуктов, и жёстко заданный ``source_id`` дал бы ``validation_failed``
    у всех, кроме первого (I22 сверяет адрес с переданным срезом).
    """

    model_id = "fake-model"
    model_version = "1.0"
    seed = 0
    temperature = 0.0

    #: Адрес фрагмента в промпте (``prompt._render_chunk``): ``source_id: <chunk_id>``.
    _SOURCE_RE = re.compile(r"^source_id: (\S+)$", re.MULTILINE)

    def __init__(self) -> None:
        self.calls: list[LLMRequest] = []

    def complete(self, req: LLMRequest) -> LLMResponse:
        self.calls.append(req)
        match = self._SOURCE_RE.search(req.prompt)
        chunk_id = match.group(1) if match else PLAIN_CHUNK
        output = _ok_output(chunk_id)
        return LLMResponse(
            output=output, raw=output.model_dump_json(), prompt_hash="ph", decoder="fake"
        )


def _seed_failed(store: SqliteStore, cells: Sequence[Cell]) -> None:
    for cell in cells:
        store.save_cell(cell)
        store.record_cell_failure(
            RUN_ID, str(cell.product_id), str(cell.param_id), "evaluation_failed"
        )


def _ports(evaluator: Any, chunks: Sequence[RetrievedChunk]) -> Ports:
    return Ports(evaluator=evaluator, lexical_index=_LexicalStub(BUILD, chunks))


# ---------------------------------------------------------------------------
# 1. Сборка: включённый профиль требует транспорта
# ---------------------------------------------------------------------------


def test_enabled_profile_without_wiring_is_a_build_error(
    store: _TraceSpyStore,
) -> None:
    """Профиль объявлен, транспорта нет — отказ сборки, а не тихий откат.

    ``tool_profile`` входит в ``run_id``: прогон, объявивший набор инструментов, обязан его
    исполнить. Тихий откат сделал бы идентичность ложью — ровно тот дефект, из-за которого
    повтор и пере-оценка исполнялись другим промптом.
    """
    _seed_failed(store, [_cell()])
    ports = _ports(_PlainEvaluator(), [_chunk(PLAIN_CHUNK)])

    with pytest.raises(ToolWiringError) as caught:
        retry_failed_cells(
            store=store,
            run_id=RUN_ID,
            params={"param_00": _param()},
            ports=ports,
            tool_profile=_PROFILE,
        )

    assert "tool_caller" in str(caught.value)
    # Отказ — до исполнения: ячейка осталась без вердикта, трассы нет.
    assert list(store.iter_tool_calls(RUN_ID)) == []
    assert [c.state for c in store.iter_cells(RUN_ID)] == [CellState.FAILED]


# ---------------------------------------------------------------------------
# 2. Профиль включён: шапка таблицы, ход с инструментами, трасса
# ---------------------------------------------------------------------------


def test_retry_with_tools_prints_table_header_and_writes_trace(
    store: _TraceSpyStore,
) -> None:
    """Повтор исполняет условие прогона: шапка таблицы, ``turn`` и строка трассы.

    Проверяется именно **промпт**: адрес таблицы дочитывается в вызывающем потоке
    (``table_context``) и печатается строкой ``столбцы:``. Если обогащение пропало бы,
    ячейка всё равно получила бы вердикт — деградация была бы тихой, поэтому утверждение
    стоит на тексте входа, а не на факте вызова.
    """
    _seed_failed(store, [_cell()])
    evaluator = _ToolEvaluator(chunk_id=PLAIN_CHUNK)
    ports = _ports(
        evaluator,
        [_chunk(PLAIN_CHUNK), _chunk(TABLE_CHUNK, text=TABLE_TEXT, heading="Раздел 2")],
    )
    caller_ident = threading.get_ident()

    report = retry_failed_cells(
        store=store,
        run_id=RUN_ID,
        params={"param_00": _param()},
        ports=ports,
        tool_profile=_PROFILE,
        tool_caller=evaluator,
        tool_corpus=StoreToolCorpus(store, "p1"),
    )

    assert report.recovered == 1
    assert not [e for e in report.errors if "table_context" in e]
    # Шапка таблицы — часть входа оценщика (план 28 §4.0), а не украшение провенанса.
    assert "столбцы: версия | поддержка" in evaluator.requests[0].prompt
    # Транспорт исполнен: модель получила набор и попросила раздел.
    assert [spec.name for spec in evaluator.requests[0].allowed_tools] == [
        "expand_to_section"
    ]
    # Трасса — в store, по строке на вызов, и из вызывающего потока.
    rows = list(store.iter_tool_calls(RUN_ID))
    assert [(r.product_id, r.param_id, r.entry.seq, r.entry.name) for r in rows] == [
        ("p1", "param_00", 1, "expand_to_section")
    ]
    assert store.trace_threads == [caller_ident]


# ---------------------------------------------------------------------------
# 3. Профиль пуст: прежнее поведение (один ``complete``, трассы нет)
# ---------------------------------------------------------------------------


def test_empty_profile_keeps_one_complete_and_writes_no_trace(
    store: _TraceSpyStore,
) -> None:
    """Пустой профиль — ровно прежний путь: один ``complete`` и ни одной строки трассы.

    Шапка таблицы при этом **печатается**: она отдельная мера (§4.0) и в фазе печаталась
    всегда, поэтому повтор без неё был бы другим входом под тем же ``run_id``.
    """
    _seed_failed(store, [_cell()])
    evaluator = _PlainEvaluator()
    ports = _ports(
        evaluator,
        [_chunk(PLAIN_CHUNK), _chunk(TABLE_CHUNK, text=TABLE_TEXT, heading="Раздел 2")],
    )

    report = retry_failed_cells(
        store=store,
        run_id=RUN_ID,
        params={"param_00": _param()},
        ports=ports,
    )

    assert report.recovered == 1
    assert len(evaluator.calls) == 1
    assert "столбцы: версия | поддержка" in evaluator.calls[0].prompt
    # «Вызовов не было» и «трасса пустая» — разные факты; здесь именно первое.
    assert list(store.iter_tool_calls(RUN_ID)) == []
    assert store.trace_threads == []


# ---------------------------------------------------------------------------
# 4. Параллельный путь: трасса пишется из вызывающего потока
# ---------------------------------------------------------------------------


def test_trace_is_written_from_the_calling_thread(
    store: _TraceSpyStore,
) -> None:
    """``llm_concurrency > 1``: вердикты считаются в пуле, трасса пишется снаружи.

    У SQLite один писатель (``check_same_thread=True``): запись из воркера — не «медленнее»,
    а ``ProgrammingError``. Поэтому проверяется идентификатор потока, а не отсутствие
    исключения: иначе тест проходил бы и на пути, где трассу пишет воркер с чужого
    соединения.
    """
    _seed_failed(store, [_cell(param_id="param_00"), _cell(param_id="param_01")])
    evaluator = _ToolEvaluator(chunk_id=PLAIN_CHUNK)
    ports = _ports(
        evaluator,
        [_chunk(PLAIN_CHUNK), _chunk(TABLE_CHUNK, text=TABLE_TEXT, heading="Раздел 2")],
    )
    caller_ident = threading.get_ident()

    report = retry_failed_cells(
        store=store,
        run_id=RUN_ID,
        params={"param_00": _param(0), "param_01": _param(1)},
        ports=ports,
        tool_profile=_PROFILE,
        tool_caller=evaluator,
        tool_corpus=StoreToolCorpus(store, "p1"),
        llm_concurrency=2,
    )

    assert report.recovered == 2
    # Один вызов ``save_tool_calls`` на ячейку и оба — из вызывающего потока.
    assert store.trace_threads == [caller_ident, caller_ident]
    rows = list(store.iter_tool_calls(RUN_ID))
    assert sorted({(r.product_id, r.param_id) for r in rows}) == [
        ("p1", "param_00"),
        ("p1", "param_01"),
    ]


# ---------------------------------------------------------------------------
# 5. Окно в корпус — по продукту и один раз на продукт
# ---------------------------------------------------------------------------

def test_corpus_window_is_built_once_per_product(
    store: _TraceSpyStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``tool_corpus=None`` ⇒ окно строится **по продукту** (I17) и кэшируется.

    На ячейку это была бы цена за то же чтение (два SQL-запроса на каждую), а одно окно на
    набор — нарушением I17: чанк соседа по когорте стал бы доступен инструменту.
    """
    _seed_corpus(store, "p2")
    _seed_failed(
        store,
        [
            _cell(product_id="p1", param_id="param_00"),
            _cell(product_id="p1", param_id="param_01"),
            _cell(product_id="p2", param_id="param_00"),
            _cell(product_id="p2", param_id="param_01"),
        ],
    )
    built: list[str] = []
    real = evaluate_module.StoreToolCorpus

    def _factory(inner_store: Any, product_id: str) -> Any:
        built.append(str(product_id))
        return real(inner_store, product_id)

    monkeypatch.setattr(evaluate_module, "StoreToolCorpus", _factory)
    evaluator = _PlainEvaluator()
    ports = _ports(
        evaluator,
        [
            _chunk(PLAIN_CHUNK),
            _chunk("p2:dv_1:0", product_id="p2"),
        ],
    )

    report = retry_failed_cells(
        store=store,
        run_id=RUN_ID,
        params={"param_00": _param(0), "param_01": _param(1)},
        ports=ports,
    )

    assert report.recovered == 4
    # Построение — по одному на продукт, в порядке групп (``product_id`` сортируется).
    assert built == ["p1", "p2"]


# ---------------------------------------------------------------------------
# 6. Пере-оценка (``revalidate``) — тот же дефект, то же лечение
# ---------------------------------------------------------------------------


def test_revalidate_with_tools_writes_the_trace(store: _TraceSpyStore) -> None:
    """``reevaluate_stale`` исполняет профиль и пишет трассу — как и ``retry``.

    Обе команды идут одним шагом (:func:`_evaluate_cells_by_condition`), поэтому лечение
    одно. Проверяется отдельно, потому что у пере-оценки свой порядок сохранения
    (снятие ``stale_reason`` только при вердикте по текущей сборке), и трасса могла бы
    потеряться именно на её ветке «пометка сохранена».
    """
    stale = _cell(
        state=CellState.EVALUATED, status=Status.OK, index_build_id="cb_old"
    ).model_copy(update={"stale_reason": StaleReason.NEW_BUILD})
    store.save_cell(stale)
    evaluator = _ToolEvaluator(chunk_id=PLAIN_CHUNK)
    ports = _ports(
        evaluator,
        [_chunk(PLAIN_CHUNK), _chunk(TABLE_CHUNK, text=TABLE_TEXT, heading="Раздел 2")],
    )

    report = reevaluate_stale(
        store=store,
        run_id=RUN_ID,
        params={"param_00": _param()},
        ports=ports,
        tool_profile=_PROFILE,
        tool_caller=evaluator,
        current_build_id=BUILD,
    )

    assert report.reevaluated
    assert "столбцы: версия | поддержка" in evaluator.requests[0].prompt
    assert [r.entry.name for r in store.iter_tool_calls(RUN_ID)] == [
        "expand_to_section"
    ]
    # Одна запись трассы на ячейку и из вызывающего потока (у store один писатель).
    assert store.trace_threads == [threading.get_ident()]
    # Пометка снята: вердикт получен по текущей сборке.
    assert [c.stale_reason for c in store.iter_cells(RUN_ID)] == [None]

