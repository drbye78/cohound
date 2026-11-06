"""Параллельные вызовы оценщика в Ш6 — объявленная ось исполнения ``--llm-concurrency``.

Замер, ради которого ось введена: прогон ``ru-pg-dbms-c2e7b860ac17`` — фаза evaluate
10 906 с на 656 вызовов (16.6 с на вызов) при локальной работе в доли секунды. То есть
фаза упирается **только** в латентность модели, и число вызовов в полёте — условие
исполнения (время), а не измерения (вердикт).

Что проверяется:

* (а) ``llm_concurrency=1`` — прежний результат: дефолт и явная единица дают **тот же**
  набор ячеек (побайтно по модели ячейки);
* (б) ``> 1`` — те же вердикты и **тот же порядок** ячеек, что при последовательном пути,
  и вызовы действительно перекрываются (иначе «параллельность» осталась бы в отчёте);
* (в) запись в store идёт **в одном потоке** — в том, что открыл соединение (двойник
  запоминает ``threading.get_ident()``): соединение SQLite открыто с
  ``check_same_thread=True``, и запись из воркера была бы ``ProgrammingError``;
* (г) ``429`` + ``Retry-After`` → пауза и успешный повтор, а не немедленный шторм; пауза
  видна в журнале вызовов (попытка, секунды, причина);
* (д) журнал вызовов при ``N > 1`` не теряет и не перемешивает строки: каждая строка —
  валидный JSON, число строк = числу вызовов.

Сеть не используется; данные категорийно нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
import re
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from core.adapters.llm_local import EvaluationError
from core.domain import (
    AbsenceRuleType,
    CatalogRow,
    CellState,
    CorpusCoverage,
    Importance,
    LLMCellOutput,
    ParamType,
    RankingPolicy,
    RunConfig,
    RunMode,
    Status,
)
from core.pipeline.evaluate import (
    LLMCallJournal,
    _CellWriteBuffer,
    evaluate_batch,
    evaluate_cell,
)
from core.ports.llm import LLMRequest, LLMResponse, TokenUsage
from core.ports.types import Chunk
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-concurrency-1"
PRODUCT_ID = "p1"
COVERAGE_TS = "2026-01-01T00:00:00+00:00"

#: Заголовок параметра в промпте: по нему оценщик-двойник понимает, **чей** это вызов.
_TITLE_RE = re.compile(r"Свойство (\d+)")


# ---------------------------------------------------------------------------
# Фикстуры (нейтральные)
# ---------------------------------------------------------------------------


def _param(index: int) -> CatalogRow:
    """Строка каталога № ``index`` — свой предмет у каждой ячейки."""
    return CatalogRow(
        param_id=f"param_{index:02d}",
        title=f"Свойство {index}",
        module="mod_a",
        group="grp_1",
        importance=Importance.CRITICAL,
        param_type=ParamType.FACT,
        rule=f"Подтвердить наличие свойства {index}.",
        absence_rule=AbsenceRuleType.EXPLICIT,
        role_binding="role_a",
        concept_terms={"ru": [f"свойство {index}"], "en": [f"feature {index}"]},
        search_langs=["ru", "en"],
    )


def _params(count: int) -> list[CatalogRow]:
    return [_param(index) for index in range(count)]


def _chunk(param: CatalogRow) -> Chunk:
    """Фрагмент, в котором цитата ответа действительно есть (I22 пройдёт)."""
    return Chunk(
        chunk_id=f"chunk_{param.param_id}",
        chunk_no=0,
        text=f"Свойство {param.title.split()[-1]} подтверждено документацией.",
        heading_path="Раздел 1",
        context_prefix="контекст",
        content_hash=f"hash_{param.param_id}",
    )


def _answer(index: int) -> LLMCellOutput:
    return LLMCellOutput(
        status=Status.OK,
        justification=f"Свойство {index} подтверждено документацией.",
        evidence=[
            {
                "source_id": f"chunk_param_{index:02d}",
                "quote": f"Свойство {index} подтверждено документацией.",
            }
        ],
    )


def _store() -> SqliteStore:
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
            languages=["ru"],
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
            ranking_policy=RankingPolicy.CAPABILITY_RANKED,
        ),
    )
    store.save_corpus_coverage(
        RUN_ID,
        CorpusCoverage(
            product_id=PRODUCT_ID,
            complete=True,
            expected_sources=1,
            observed_sources=1,
            computed_at=COVERAGE_TS,
        ),
    )
    return store


def _coverage() -> CorpusCoverage:
    return CorpusCoverage(
        product_id=PRODUCT_ID,
        complete=True,
        expected_sources=1,
        observed_sources=1,
        computed_at=COVERAGE_TS,
    )


class _TrackingEvaluator:
    """Оценщик-двойник: отвечает по предмету промпта и мерит **одновременность**.

    ``max_in_flight`` — самый большой счётчик вызовов, находившихся в работе
    одновременно. Именно он отличает «объявлено N» от «исполнено N»: пул из одного
    потока при ``max_workers=4`` дал бы ``1``.
    """

    model_id = "fake-model"
    model_version = "1.0"
    seed = 0
    temperature = 0.0

    def __init__(self, *, delay: float = 0.0) -> None:
        self.delay = delay
        self._lock = threading.Lock()
        self.calls = 0
        self.in_flight = 0
        self.max_in_flight = 0

    def complete(self, req: LLMRequest) -> LLMResponse:
        with self._lock:
            self.calls += 1
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            match = _TITLE_RE.search(req.prompt)
            assert match is not None, "промпт не назвал предмет — двойник не угадывает"
            index = int(match.group(1))
            if self.delay:
                # Задержка нужна, чтобы вызовы гарантированно перекрылись: без неё пул
                # может успеть исполнить их по очереди и тест ничего не докажет.
                time.sleep(self.delay)
            output = _answer(index)
        finally:
            with self._lock:
                self.in_flight -= 1
        return LLMResponse(
            output=output,
            raw=output.model_dump_json(),
            prompt_hash="ph",
            decoder="fake",
            usage=TokenUsage(),
        )


def _run_batch(
    *,
    concurrency: int | None,
    evaluator: Any,
    count: int = 4,
    store: Any = None,
    journal: LLMCallJournal | None = None,
) -> tuple[list[Any], list[str], Any]:
    """Батч по ``count`` ячейкам; ``concurrency=None`` — аргумент **не передан** вовсе."""
    params = _params(count)
    chunks_by_param = {param.param_id: (_chunk(param),) for param in params}
    own_store = store is None
    used_store = _store() if own_store else store
    kwargs: dict[str, Any] = {}
    if concurrency is not None:
        kwargs["llm_concurrency"] = concurrency
    try:
        outcomes, errors = evaluate_batch(
            params=params,
            product_id=PRODUCT_ID,
            run_id=RUN_ID,
            evaluator=evaluator,
            chunks_by_param=chunks_by_param,
            store=used_store,
            coverage=_coverage(),
            llm_journal=journal,
            **kwargs,
        )
    finally:
        if own_store:
            used_store.close()
    return outcomes, errors, params


def _snapshot(outcomes: list[Any]) -> list[dict[str, Any]]:
    """Сравнимый вид исхода: ячейка целиком (порядок списка — порядок ``params``)."""
    return [outcome.cell.model_dump() for outcome in outcomes]


# ---------------------------------------------------------------------------
# (а) llm_concurrency=1 — прежний результат
# ---------------------------------------------------------------------------


def test_explicit_single_concurrency_matches_default() -> None:
    """Дефолт и явная ``1`` дают один и тот же результат: поведение не изменилось.

    Сравниваются модели ячеек целиком (а не только статусы): при ``1`` путь прежний —
    вызовы последовательны, запись идёт в том же потоке, что и раньше.
    """
    default_outcomes, default_errors, _ = _run_batch(
        concurrency=None, evaluator=_TrackingEvaluator()
    )
    single_outcomes, single_errors, _ = _run_batch(
        concurrency=1, evaluator=_TrackingEvaluator()
    )

    assert default_errors == single_errors == []
    assert _snapshot(default_outcomes) == _snapshot(single_outcomes)


# ---------------------------------------------------------------------------
# (б) > 1 — те же вердикты и тот же порядок, вызовы перекрываются
# ---------------------------------------------------------------------------


def test_parallel_keeps_verdicts_and_result_order() -> None:
    """``N > 1`` не меняет вердикты и **порядок** ячеек; вызовы реально перекрываются."""
    sequential = _TrackingEvaluator(delay=0.02)
    parallel = _TrackingEvaluator(delay=0.02)

    seq_outcomes, seq_errors, params = _run_batch(concurrency=1, evaluator=sequential)
    par_outcomes, par_errors, _ = _run_batch(concurrency=4, evaluator=parallel)

    assert seq_errors == par_errors == []
    # Порядок результата — порядок каталога, а не порядок ответов модели.
    assert [outcome.cell.param_id for outcome in par_outcomes] == [
        param.param_id for param in params
    ]
    assert _snapshot(seq_outcomes) == _snapshot(par_outcomes)
    # Все вердикты — состоявшиеся: параллельность не «теряет» ячейки.
    assert [outcome.cell.status for outcome in par_outcomes] == [
        Status.OK
    ] * len(par_outcomes)
    # Исполнение, а не объявление: при 1 в полёте всегда один вызов, при 4 — больше.
    assert sequential.max_in_flight == 1
    assert parallel.max_in_flight > 1
    assert parallel.calls == sequential.calls == len(params)


def test_parallel_batch_isolates_failures_per_param() -> None:
    """Изоляция сбоев остаётся поштучной и на параллельном пути (I4).

    Сбой одного параметра не роняет батч и не крадёт вердикты у соседей: отказ
    адаптера превращается в ``failed``/``status=None`` **этой** ячейки
    (``evaluate_cell`` изолирует адаптер), а прочие ячейки оцениваются как обычно.
    """

    class _BrokenOne(_TrackingEvaluator):
        def complete(self, req: LLMRequest) -> LLMResponse:
            match = _TITLE_RE.search(req.prompt)
            assert match is not None
            if int(match.group(1)) == 2:
                raise RuntimeError("сбой одного параметра")
            return super().complete(req)

    outcomes, errors, params = _run_batch(concurrency=3, evaluator=_BrokenOne())

    # Батч не потерял ни одной ячейки: отказ изолирован внутри своей.
    assert len(outcomes) == len(params)
    assert errors == []
    assert [outcome.cell.param_id for outcome in outcomes] == [
        param.param_id for param in params
    ]
    by_param = {outcome.cell.param_id: outcome.cell for outcome in outcomes}
    broken = by_param["param_02"]
    assert broken.state is CellState.FAILED
    assert broken.status is None  # I4: failed никогда не несёт статус
    assert by_param["param_00"].status is Status.OK
    assert by_param["param_03"].status is Status.OK


def test_concurrency_must_be_positive() -> None:
    """``0`` вызовов в полёте — не исполнение: отказ, а не тихая единица (R5)."""
    params = _params(1)
    with pytest.raises(ValueError, match="≥ 1"):
        evaluate_batch(
            params=params,
            product_id=PRODUCT_ID,
            run_id=RUN_ID,
            evaluator=_TrackingEvaluator(),
            chunks_by_param={params[0].param_id: (_chunk(params[0]),)},
            llm_concurrency=0,
        )


# ---------------------------------------------------------------------------
# (в) один писатель в store — поток соединения
# ---------------------------------------------------------------------------


class _ThreadCheckedStore:
    """store-двойник: переадресует записи боевому store и запоминает их потоки.

    Проверяется ровно то, что было бы ``ProgrammingError`` у настоящего соединения:
    что ни одна запись не пришла из воркера пула.
    """

    def __init__(self, inner: SqliteStore) -> None:
        self.inner = inner
        self.main_thread = threading.get_ident()
        self.write_threads: list[int] = []

    def _checked(self) -> None:
        self.write_threads.append(threading.get_ident())
        assert threading.get_ident() == self.main_thread, "запись ячейки пришла из воркера"

    def save_cell(self, cell: Any) -> None:
        self._checked()
        self.inner.save_cell(cell)

    def set_cell_provenance(self, *args: Any, **kwargs: Any) -> None:
        self._checked()
        self.inner.set_cell_provenance(*args, **kwargs)

    def add_evidence_batch(self, evidence: Any) -> None:
        self._checked()
        self.inner.add_evidence_batch(evidence)

    def add_absence_evidence(self, records: Any) -> None:
        self._checked()
        self.inner.add_absence_evidence(records)

    def read_corpus_coverage(self, run_id: str, product_id: str) -> Any:
        return self.inner.read_corpus_coverage(run_id, product_id)

    def iter_cells(self, run_id: str) -> Any:
        return self.inner.iter_cells(run_id)


def test_store_is_written_from_the_calling_thread_only() -> None:
    """Запись ячейки/evidence идёт только из вызывающего потока (один писатель)."""
    inner = _store()
    double = _ThreadCheckedStore(inner)
    try:
        outcomes, errors, params = _run_batch(
            concurrency=4, evaluator=_TrackingEvaluator(delay=0.01), store=double
        )
        persisted = list(inner.iter_cells(RUN_ID))
    finally:
        inner.close()

    assert errors == []
    assert len(outcomes) == len(params)
    # Записи были и все — из потока вызывающего (он же владелец соединения).
    assert double.write_threads, "store не получил ни одной записи — тест вырожден"
    assert set(double.write_threads) == {double.main_thread}
    assert len(persisted) == len(params)  # ячейки действительно сохранены


def test_write_buffer_replays_on_the_calling_thread() -> None:
    """Буфер записи: воркер только копит вызовы, проигрывает их вызывающий.

    Прямая проверка механизма (без пула и SQLite): порядок вызовов сохраняется, а
    метод, которого у настоящего store нет, пропускается — как в ``_persist``.
    """

    class _Recorder:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def save_cell(self, cell: Any) -> None:
            self.calls.append(f"save_cell:{cell.param_id}")

        def add_evidence_batch(self, evidence: Any) -> None:
            self.calls.append(f"add_evidence_batch:{len(evidence)}")

    buffer = _CellWriteBuffer()
    buffer.add_evidence_batch([{"evidence_id": "ev_1"}])
    # ``set_cell_provenance`` у приёмника нет — пропуск, а не падение.
    buffer.set_cell_provenance(RUN_ID, PRODUCT_ID, "param_00", prompt_hash="h")
    buffer.save_cell(_param(0))  # type: ignore[arg-type] — нужен только ``param_id``
    assert buffer.ops  # воркер ничего не исполнил: только накопил

    recorder = _Recorder()
    buffer.replay(recorder)  # type: ignore[arg-type]

    assert recorder.calls == ["add_evidence_batch:1", "save_cell:param_00"]
    assert buffer.ops == []


# ---------------------------------------------------------------------------
# (г) backoff: Retry-After и экспоненциальная пауза
# ---------------------------------------------------------------------------


class _RetryAfterEvaluator:
    """Первая попытка — отказ провайдера (``429`` + ``Retry-After``), затем успех."""

    model_id = "fake-model"
    model_version = "1.0"
    seed = 0
    temperature = 0.0

    def __init__(self, *, retry_after: str | None, status: int = 429) -> None:
        self.retry_after = retry_after
        self.status = status
        self.calls = 0

    def complete(self, req: LLMRequest) -> LLMResponse:
        self.calls += 1
        if self.calls == 1:
            raise EvaluationError(
                f"HTTP-вызов оценщика не удался: HTTP {self.status}",
                reason="http_error",
                status_code=self.status,
                retry_after=self.retry_after,
            )
        output = _answer(0)
        return LLMResponse(
            output=output,
            raw=output.model_dump_json(),
            prompt_hash="ph",
            decoder="fake",
            usage=TokenUsage(),
        )


def _journal(tmp_path: Path) -> LLMCallJournal:
    return LLMCallJournal(tmp_path / "logs" / "llm_calls.jsonl", RUN_ID)


def _lines(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_retry_after_pauses_instead_of_hammering(tmp_path: Path) -> None:
    """``429`` + ``Retry-After: 7`` → пауза 7 с и успешный повтор; пауза видна в журнале."""
    param = _param(0)
    evaluator = _RetryAfterEvaluator(retry_after="7")
    waited: list[float] = []
    journal = _journal(tmp_path)

    outcome = evaluate_cell(
        param=param,
        product_id=PRODUCT_ID,
        run_id=RUN_ID,
        evaluator=evaluator,
        chunks=(_chunk(param),),
        llm_journal=journal,
        sleep=waited.append,
    )

    # Ровно одна пауза и ровно две попытки: шторма нет.
    assert waited == [7.0]
    assert evaluator.calls == 2
    assert outcome.cell.status is Status.OK

    lines = _lines(journal.path)
    assert [line["attempt"] for line in lines] == [1, 2]
    assert lines[0]["outcome"] == "http_error"
    assert lines[0]["retry_wait_s"] == 7.0
    assert lines[0]["retry_reason"] == "http_error:retry_after"
    assert lines[0]["retry_after_s"] == 7.0
    # Последняя попытка паузы не назначает: повторов больше нет.
    assert lines[1]["outcome"] == "ok"
    assert lines[1]["retry_wait_s"] is None
    assert lines[1]["retry_reason"] is None


def test_retry_after_is_capped(tmp_path: Path) -> None:
    """«Подожди час» — не пауза повтора: ``Retry-After`` усечён потолком политики."""
    from core.util.retry import MAX_RETRY_DELAY

    param = _param(0)
    waited: list[float] = []
    outcome = evaluate_cell(
        param=param,
        product_id=PRODUCT_ID,
        run_id=RUN_ID,
        evaluator=_RetryAfterEvaluator(retry_after="3600"),
        chunks=(_chunk(param),),
        llm_journal=_journal(tmp_path),
        sleep=waited.append,
    )

    assert waited == [MAX_RETRY_DELAY]
    assert outcome.cell.status is Status.OK


def test_transport_failure_backs_off_exponentially(tmp_path: Path) -> None:
    """Без ``Retry-After`` пауза экспоненциальная с джиттером, а не нулевая."""
    from core.util.retry import BASE_RETRY_DELAY

    param = _param(0)

    class _Flaky(_RetryAfterEvaluator):
        def complete(self, req: LLMRequest) -> LLMResponse:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("таймаут транспорта")
            return super().complete(req)

    waited: list[float] = []
    journal = _journal(tmp_path)
    evaluate_cell(
        param=param,
        product_id=PRODUCT_ID,
        run_id=RUN_ID,
        evaluator=_Flaky(retry_after=None),
        chunks=(_chunk(param),),
        llm_journal=journal,
        sleep=waited.append,
    )

    # Jitter аддитивный (до +50 % базы): диапазон первой паузы не пересекается со второй.
    assert len(waited) == 1
    assert BASE_RETRY_DELAY <= waited[0] <= BASE_RETRY_DELAY * 1.5
    first = _lines(journal.path)[0]
    assert first["outcome"] == "exception"
    assert first["retry_reason"] == "exception"
    assert first["retry_after_s"] is None
    assert first["retry_wait_s"] == pytest.approx(waited[0])


def test_answer_side_failure_is_retried_without_pause(tmp_path: Path) -> None:
    """Обрезание по потолку — отказ **по существу** ответа: повтор без паузы.

    Провайдер ответил (``raw`` есть), ждать нечего; пауза растянула бы фазу, ничего не
    изменив в шансах повтора. Видно и обратное: ``retry_wait_s=None`` у отказавшей строки,
    хотя попытки ещё остались.
    """
    param = _param(0)

    class _Truncated(_RetryAfterEvaluator):
        def complete(self, req: LLMRequest) -> LLMResponse:
            self.calls += 1
            raise EvaluationError(
                "генерация оборвана по потолку токенов",
                reason="length",
                raw="",
                finish_reason="length",
            )

    waited: list[float] = []
    journal = _journal(tmp_path)
    outcome = evaluate_cell(
        param=param,
        product_id=PRODUCT_ID,
        run_id=RUN_ID,
        evaluator=_Truncated(retry_after=None),
        chunks=(_chunk(param),),
        llm_journal=journal,
        sleep=waited.append,
    )

    assert waited == []
    assert outcome.cell.state.value == "failed"  # I4: статуса нет
    lines = _lines(journal.path)
    assert len(lines) == 3  # max_retries=2 → три попытки, все без паузы
    assert [line["outcome"] for line in lines] == ["length"] * 3
    assert all(line["retry_wait_s"] is None for line in lines)


# ---------------------------------------------------------------------------
# (д) журнал при N > 1: строки не теряются и не перемешиваются
# ---------------------------------------------------------------------------


def test_journal_under_parallelism_keeps_every_line_intact(tmp_path: Path) -> None:
    """8 вызовов в 4 полосы → 8 целых JSON-строк со своим ``cell_key`` и ``attempt``."""
    journal = _journal(tmp_path)
    outcomes, errors, params = _run_batch(
        concurrency=4,
        evaluator=_TrackingEvaluator(delay=0.01),
        count=8,
        journal=journal,
    )

    assert errors == []
    assert len(outcomes) == len(params)
    lines = _lines(journal.path)  # json.loads падает на перемешанной строке
    assert len(lines) == len(params) == journal.written
    assert {line["cell_key"] for line in lines} == {
        f"{PRODUCT_ID}/{param.param_id}" for param in params
    }
    assert [line["attempt"] for line in lines] == [1] * len(params)
    assert {line["outcome"] for line in lines} == {"ok"}
    # Каждая строка самодостаточна по адресу: порядок строк — порядок захвата замка,
    # поэтому проверяется состав, а не последовательность.
    assert {line["cell_key"] for line in lines} == {
        line["cell_key"] for line in lines
    }