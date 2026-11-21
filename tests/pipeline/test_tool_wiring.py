"""Проводка профиля инструментов и табличной шапки в Ш6 (план ``28`` §9, §4.0).

Что проверяется — и почему именно это:

* **отказ сборки вместо тихого отката.** Включённый ``tool_profile`` без ``tool_corpus``
  или без транспорта — :class:`ToolWiringError`: профиль входит в ``run_id``, и прогон,
  объявивший его, обязан его исполнить, иначе идентичность описывает неисполненное условие;
* **набор инструментов параметро-зависим.** ``subject_mention`` читает термины каталога,
  поэтому ``build_local_tools`` обязан вызываться **на ячейку** и получать ``param`` этой
  ячейки — один объект на батч дал бы инструменту чужие термины;
* **трасса пишется один раз и в вызывающем потоке.** Соединение SQLite открыто с
  ``check_same_thread=True``; запись из воркера — ``ProgrammingError``, поэтому трасса
  проходит через тот же единственный писатель, что и ячейки. Тест ловит и число записей
  на ячейку, и идентификатор потока;
* **адрес таблицы доезжает до промпта по цепочке** ``chunk_id → doc_version → doc_id →
  corpus_docs.tables → TableKey``. Ключ карты — ``(_document_id(chunk_id), table_id)``, а
  не ``corpus_chunks.doc_id``; если перепутать эти две строки, карта не совпадёт ни на
  одном чанке и деградация будет **тихой** («адрес известен, объект не передан» читается
  как «таблиц нет»). Поэтому тест утверждает наличие строки ``столбцы:`` в тексте промпта,
  а не только факт вызова;
* **шапка таблицы — отдельная мера, а не часть профиля**: при ``tool_profile=none`` и
  переданном корпусе печатается и шапка, и ровно один ``complete``;
* без ``tool_corpus`` шапки нет вовсе, и это не падение: адрес просто не появляется.

Сеть не используется, данные категорийно нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import threading
from collections.abc import Sequence
from typing import Any

import pytest

from core.domain import (
    AbsenceRuleType,
    CatalogRow,
    CellState,
    CorpusChunk,
    CorpusDoc,
    Focus,
    Importance,
    LLMCellOutput,
    ParamType,
    RankingPolicy,
    RunConfig,
    RunMode,
    Status,
)
from core.domain.doc_tables import ExtractedTable, TableRef
from core.domain.tools import ToolInvocation, ToolOutcome, ToolProfile, ToolSpec
from core.pipeline import evaluate as evaluate_module
from core.pipeline.evaluate import ToolWiringError, evaluate_batch
from core.pipeline.tool_corpus import StoreToolCorpus
from core.ports.llm import LLMRequest, LLMResponse, ToolTurn, ToolTurnRequest
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-tool-wiring"
PRODUCT_ID = "p1"
DOC_ID = "p1:hash1"
DOC_VERSION_CHUNK = "p1:dv_1:0"
#: Второй чанк того же документа: адрес таблицы есть, текст — строка таблицы.
TABLE_CHUNK_ID = "p1:dv_2:0"
QUOTE = "Поддержка версии 18 подтверждена."
TABLE_TEXT = "| 18 | да |"

_SPEC = ToolSpec(
    name="expand_to_section",
    description="Чанки того же раздела документа, что и адресованный чанк.",
    parameters={
        "type": "object",
        "properties": {"chunk_id": {"type": "string"}},
        "required": ["chunk_id"],
    },
)

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


def _chunk(chunk_id: str = DOC_VERSION_CHUNK, text: str = QUOTE) -> CorpusChunk:
    return CorpusChunk(
        chunk_id=chunk_id,
        doc_id=DOC_ID,
        product_id=PRODUCT_ID,
        chunk_no=0,
        text=text,
        heading_path="Раздел 1",
        content_hash=f"hash-{chunk_id}",
    )


def _table() -> ExtractedTable:
    return ExtractedTable(
        table_id="t0",
        caption="Поддержка версий",
        header=("версия", "поддержка"),
        rows=(("18", "да"),),
    )


def _ok_output(chunk_id: str = DOC_VERSION_CHUNK, quote: str = QUOTE) -> LLMCellOutput:
    return LLMCellOutput(
        status=Status.OK,
        justification="Свойство подтверждено документацией.",
        evidence=[{"source_id": chunk_id, "quote": quote}],
    )


def _store(*, with_table: bool = True) -> SqliteStore:
    """Боевой store в памяти: та же однопоточная дисциплина, что у прогона."""
    store = SqliteStore(":memory:")
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id="case-1",
            anchor=PRODUCT_ID,
            cohort="cohort.yaml",
            products=[PRODUCT_ID],
            package="pkg-1",
            focus=Focus(),
            languages=["ru"],
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
            ranking_policy=RankingPolicy.CAPABILITY_RANKED,
        ),
    )
    store.upsert_corpus_doc(
        CorpusDoc(
            doc_id=DOC_ID,
            product_id=PRODUCT_ID,
            url="https://docs.test/p1/d1",
            content_hash="hash1",
            # ``()`` — читатель таблиц искал и не нашёл; ``None`` — не даёт вовсе.
            tables=(_table(),) if with_table else (),
        )
    )
    store.add_chunks_batch(
        [
            _chunk(),
            # Табличный чанк: адрес есть, текст — строка таблицы.
            CorpusChunk(
                chunk_id=TABLE_CHUNK_ID,
                doc_id=DOC_ID,
                product_id=PRODUCT_ID,
                chunk_no=0,
                text=TABLE_TEXT,
                heading_path="Раздел 2",
                content_hash="hash-table",
                table_ref=TableRef("t0", 1, 1),
            ),
        ]
    )
    return store


# ---------------------------------------------------------------------------
# Двойники
# ---------------------------------------------------------------------------


class _ScriptedEvaluator:
    """``EvaluatorPort``-двойник: запоминает промпты, отвечает валидной цитатой.

    Цитата обязана быть подстрокой текста фрагмента (I22), иначе ячейка уходит в повторы
    и тест мерил бы исчерпание попыток, а не проводку. Поэтому цитата — параметр двойника.
    """

    model_id = "fake-model"
    model_version = "1.0"
    seed = 0
    temperature = 0.0
    decoder = "json_schema"

    def __init__(
        self, *, chunk_id: str = DOC_VERSION_CHUNK, quote: str = QUOTE
    ) -> None:
        self._chunk_id = chunk_id
        self._quote = quote
        self.calls: list[LLMRequest] = []

    def complete(self, req: LLMRequest) -> LLMResponse:
        self.calls.append(req)
        output = _ok_output(self._chunk_id, self._quote)
        return LLMResponse(
            output=output, raw=output.model_dump_json(), prompt_hash="ph", decoder="json_schema"
        )


class _ScriptedCaller:
    """``LocalToolCallerPort``-двойник: вызов на первом ходе ячейки, вердикт на втором.

    Состояние — **по промпту**, а не по счётчику объекта: у каждой ячейки свой диалог, и
    общий счётчик дал бы вызов только первой из параллельных ячеек. Признак второго хода —
    отрендеренный результат инструмента в промпте (он детерминирован по форме).
    """

    #: Признак того, что результат инструмента уже в промпте (``_render_tool_result``).
    _RESULT_MARKER = "=== ЛОКАЛЬНЫЙ ИНСТРУМЕНТ"

    def __init__(
        self, *, chunk_id: str = DOC_VERSION_CHUNK, quote: str = QUOTE
    ) -> None:
        self._chunk_id = chunk_id
        self._quote = quote
        self.requests: list[ToolTurnRequest] = []

    def turn(self, req: ToolTurnRequest) -> ToolTurn:
        self.requests.append(req)
        if self._RESULT_MARKER not in req.prompt:
            return ToolTurn(
                tool_calls=(
                    ToolInvocation(name="expand_to_section", arguments={"chunk_id": self._chunk_id}),
                ),
                raw="{}",
                finish_reason="tool_calls",
            )
        output = _ok_output(self._chunk_id, self._quote)
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


class _FakeTools:
    """``LocalToolPort``-двойник: объявленный набор и записанные вызовы."""

    def __init__(self, specs: Sequence[ToolSpec] = (_SPEC,)) -> None:
        self._specs = tuple(specs)
        self.calls: list[ToolInvocation] = []

    def specs(self) -> tuple[ToolSpec, ...]:
        return self._specs

    def invoke(self, invocation: ToolInvocation) -> ToolOutcome:
        self.calls.append(invocation)
        return ToolOutcome(name=invocation.name, content="Раздел 1 целиком.", ok=True)


def _run_batch(
    *,
    store: SqliteStore,
    evaluator: Any,
    params: Sequence[CatalogRow] | None = None,
    chunks_by_param: dict[str, Sequence[Any]] | None = None,
    tool_profile: ToolProfile | None = None,
    tool_caller: Any = None,
    tool_corpus: Any = None,
    llm_concurrency: int = 1,
) -> tuple[list[Any], list[str]]:
    """Батч с подставленным по умолчанию срезом; ``None`` — аргумент не передан."""
    resolved_params = list(params or [_param()])
    if chunks_by_param is None:
        chunks_by_param = {
            param.param_id: [StoreToolCorpus(store, PRODUCT_ID).chunk(DOC_VERSION_CHUNK)]
            for param in resolved_params
        }
    kwargs: dict[str, Any] = {}
    if tool_profile is not None:
        kwargs["tool_profile"] = tool_profile
    if tool_caller is not None:
        kwargs["tool_caller"] = tool_caller
    if tool_corpus is not None:
        kwargs["tool_corpus"] = tool_corpus
    return evaluate_batch(
        params=resolved_params,
        product_id=PRODUCT_ID,
        run_id=RUN_ID,
        evaluator=evaluator,
        chunks_by_param=chunks_by_param,
        store=store,
        llm_concurrency=llm_concurrency,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Сборка: включённый профиль требует окна и транспорта
# ---------------------------------------------------------------------------


def test_enabled_profile_without_corpus_is_a_build_error() -> None:
    """Профиль объявлен, окна в корпус нет — отказ сборки, а не тихий откат."""
    store = _store()
    try:
        with pytest.raises(ToolWiringError) as caught:
            _run_batch(
                store=store,
                evaluator=_ScriptedEvaluator(),
                tool_profile=_PROFILE,
                tool_caller=_ScriptedCaller(),
            )
        assert "tool_corpus" in str(caught.value)
    finally:
        store.close()


def test_enabled_profile_without_caller_is_a_build_error() -> None:
    """Профиль объявлен, ход исполнять нечем — тоже отказ сборки."""
    store = _store()
    try:
        with pytest.raises(ToolWiringError) as caught:
            _run_batch(
                store=store,
                evaluator=_ScriptedEvaluator(),
                tool_profile=_PROFILE,
                tool_corpus=StoreToolCorpus(store, PRODUCT_ID),
            )
        assert "tool_caller" in str(caught.value)
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Инструменты параметро-зависимы: build_local_tools на ячейку
# ---------------------------------------------------------------------------


def test_tools_are_built_per_cell_with_that_cells_param(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``build_local_tools`` вызывается на ячейку и получает **её** строку каталога."""
    store = _store()
    built: list[str] = []
    original = evaluate_module.build_local_tools

    def _recording(*, profile: ToolProfile, corpus: Any, param: CatalogRow) -> Any:
        built.append(param.param_id)
        return original(profile=profile, corpus=corpus, param=param)

    monkeypatch.setattr(evaluate_module, "build_local_tools", _recording)
    params = [_param(0), _param(1)]
    corpus = StoreToolCorpus(store, PRODUCT_ID)
    try:
        outcomes, errors = _run_batch(
            store=store,
            evaluator=_ScriptedEvaluator(),
            params=params,
            chunks_by_param={
                param.param_id: [corpus.chunk(DOC_VERSION_CHUNK)] for param in params
            },
            tool_profile=_PROFILE,
            tool_caller=_ScriptedCaller(),
            tool_corpus=corpus,
        )
        assert errors == []
        assert built == ["param_00", "param_01"]
        assert all(outcome.cell.state is CellState.EVALUATED for outcome in outcomes)
        # Трасса каждой ячейки — в store: инструмент был исполнен, а не только объявлен.
        records = list(store.iter_tool_calls(RUN_ID))
        assert sorted({(row.param_id, row.entry.name) for row in records}) == [
            ("param_00", "expand_to_section"),
            ("param_01", "expand_to_section"),
        ]
    finally:
        store.close()


def test_trace_is_written_once_per_cell_in_the_calling_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Трасса: одна запись на ячейку, из потока-владельца соединения (``check_same_thread``)."""
    store = _store()
    calls: list[tuple[tuple[str, ...], int]] = []
    original = store.save_tool_calls

    def _recording(run_id: str, records: Sequence[Any]) -> int:
        calls.append(
            (
                tuple(f"{row.param_id}#{row.entry.seq}" for row in records),
                threading.get_ident(),
            )
        )
        return original(run_id, records)

    monkeypatch.setattr(store, "save_tool_calls", _recording)
    params = [_param(0), _param(1), _param(2)]
    corpus = StoreToolCorpus(store, PRODUCT_ID)
    main_thread = threading.get_ident()
    try:
        outcomes, errors = _run_batch(
            store=store,
            evaluator=_ScriptedEvaluator(),
            params=params,
            chunks_by_param={
                param.param_id: [corpus.chunk(DOC_VERSION_CHUNK)] for param in params
            },
            tool_profile=_PROFILE,
            tool_caller=_ScriptedCaller(),
            tool_corpus=corpus,
            llm_concurrency=3,
        )
        assert errors == []
        assert len(outcomes) == 3
        # Ровно по одной записи на ячейку — не больше: повторная запись той же трассы
        # сделала бы «сколько вызовов стоил вердикт» неоднозначным.
        assert len(calls) == 3
        # Порядок записи — порядок готовности ячеек (так ячейка не теряется при падении
        # процесса), поэтому сравнивается набор, а не последовательность.
        assert sorted(entry for entries, _ in calls for entry in entries) == [
            f"param_{index:02d}#1" for index in range(3)
        ]
        # Все записи — из вызывающего потока: воркер соединения не касается.
        assert {ident for _, ident in calls} == {main_thread}
        assert len(list(store.iter_tool_calls(RUN_ID))) == 3
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Табличная шапка: адрес доезжает до текста промпта
# ---------------------------------------------------------------------------


def _table_chunks(store: SqliteStore) -> list[Any]:
    """Срез из одного табличного чанка — с адресом, но без карты объектов."""
    corpus = StoreToolCorpus(store, PRODUCT_ID)
    chunk = corpus.chunk(TABLE_CHUNK_ID)
    assert chunk is not None
    return [chunk]


def _table_evaluator() -> _ScriptedEvaluator:
    """Оценщик, чья цитата — текст табличного фрагмента (иначе I22 уводит в повторы)."""
    return _ScriptedEvaluator(chunk_id=TABLE_CHUNK_ID, quote=TABLE_TEXT)


def test_table_address_reaches_the_prompt_through_the_whole_chain() -> None:
    """``chunk_id → doc_version → doc_id → tables → TableKey`` замыкается: есть ``столбцы:``.

    Ключ карты — ``(_document_id(chunk_id), table_id)``. Если бы ``table_context`` положил
    в ключ ``corpus_chunks.doc_id`` (``p1:hash1``), промпт искал бы ``p1:dv_2`` и не нашёл
    бы ничего — молча, с пометкой «объект не передан».
    """
    store = _store()
    evaluator = _table_evaluator()
    try:
        chunks = _table_chunks(store)
        outcomes, errors = _run_batch(
            store=store,
            evaluator=evaluator,
            chunks_by_param={_param().param_id: chunks},
            tool_corpus=StoreToolCorpus(store, PRODUCT_ID),
        )
        assert errors == []
        assert outcomes[0].cell.state is CellState.EVALUATED
        prompt = evaluator.calls[0].prompt
        assert "столбцы: версия | поддержка" in prompt
        assert "таблица: Поддержка версий" in prompt
        assert "строка: 18 | да" in prompt
        # Пометка деградации — именно строка шапки, а не упоминание в шаблоне: шаблон
        # объясняет правило, и его текст не является свидетельством деградации.
        assert "таблица: адрес известен, объект не передан" not in prompt
    finally:
        store.close()


def test_without_corpus_the_address_stays_and_the_object_is_missing() -> None:
    """``tool_corpus=None`` — адрес уже в чанке, объекта нет: видимая деградация, не падение.

    Адрес таблицы живёт **в самом чанке** (``corpus_chunks.table_ref``), поэтому окно в
    корпус не может его «отнять»: без окна промпт честно печатает «адрес известен, объект
    не передан», а ячейка всё равно мерится. Проверяется именно эта форма: строки шапки
    ``таблица: <подпись>`` и ``столбцы:`` не появляются (объект не читали), а вердикт не
    зависит от деградации метаданных (I4 не про это).
    """
    store = _store()
    evaluator = _table_evaluator()
    try:
        chunks = _table_chunks(store)
        outcomes, errors = _run_batch(
            store=store,
            evaluator=evaluator,
            chunks_by_param={_param().param_id: chunks},
        )
        assert errors == []
        assert len(evaluator.calls) == 1
        assert outcomes[0].cell.state is CellState.EVALUATED
        prompt = evaluator.calls[0].prompt
        assert "таблица: адрес известен, объект не передан (t0)" in prompt
        assert "столбцы: версия | поддержка" not in prompt
        assert "таблица: Поддержка версий" not in prompt
    finally:
        store.close()


def test_empty_profile_with_corpus_calls_complete_once_and_still_prints_the_header() -> None:
    """Шапка таблицы — отдельная мера (§4.0): печатается и при ``tool_profile=none``."""
    store = _store()
    evaluator = _table_evaluator()
    caller = _ScriptedCaller()
    try:
        chunks = _table_chunks(store)
        outcomes, errors = _run_batch(
            store=store,
            evaluator=evaluator,
            chunks_by_param={_param().param_id: chunks},
            tool_caller=caller,
            tool_corpus=StoreToolCorpus(store, PRODUCT_ID),
        )
        assert errors == []
        # Пустой профиль — ровно прежний путь: один ``complete`` и ни одного хода.
        assert len(evaluator.calls) == 1
        assert caller.requests == []
        assert "столбцы: версия | поддержка" in evaluator.calls[0].prompt
        assert outcomes[0].tool_trace == ()
        # Инструментов не было — трассы нет вовсе (не пустая запись).
        assert list(store.iter_tool_calls(RUN_ID)) == []
    finally:
        store.close()


def test_table_context_of_a_product_without_tables_gives_no_header() -> None:
    """``corpus_docs.tables IS NULL`` — объекта нет; шапки нет, и это не падение."""
    store = _store(with_table=False)
    evaluator = _table_evaluator()
    try:
        chunks = _table_chunks(store)
        outcomes, errors = _run_batch(
            store=store,
            evaluator=evaluator,
            chunks_by_param={_param().param_id: chunks},
            tool_corpus=StoreToolCorpus(store, PRODUCT_ID),
        )
        assert errors == []
        assert outcomes[0].cell.state is CellState.EVALUATED
        # Адрес известен, объекта нет — видимая пометка, а не тишина.
        assert "адрес известен, объект не передан" in evaluator.calls[0].prompt
    finally:
        store.close()
