"""Тесты CLI-команд ``discover``, ``review`` и ``promote`` (T10.4, ``03`` §D.6/E.1).

Покрывают:

* ``discover`` — инъектированный (фейковый) поисковый адаптер → кандидаты-гипотезы,
  ``--json``-форма и человекочитаемая таблица; недоступный адаптер → ``EXIT_ERROR`` без
  выдуманных кандидатов; unknown ``--search-adapter`` → ``EXIT_USAGE``; seed из
  ``doc_domains`` паспортов как подсказки; команда не создаёт файлов (evidence не
  пишется, I19);
* ``review`` — детерминированная очередь (preliminary + ``❓``), ``--json``-форма,
  явное «queue empty», резолв ``--runs-dir``;
* ``promote`` — отказ без ``--actor``; повышение с ``--status ❓`` и запись ``reviewed_by``;
  отказ при отсутствии вердикта (``I14``, вердикт не выдумывается) → ``EXIT_ERROR``;
  ``--no-validated-only`` (pending-вердикт); отказ на не-preliminary ячейке; фильтр
  ``--param``; ``--json``-форма.

Данные категорийно-нейтральны (§1.3): ``p1``/``p2``, ``case_x``, ``param_a``/``param_b``.
Нет сети: адаптер инъектируется, БД — ``tmp_path`` + ``SqliteStore``.
"""

from __future__ import annotations

import datetime as _dt
import importlib
import json
from pathlib import Path

import pytest

from core.cli.main import EXIT_ERROR, EXIT_OK, EXIT_USAGE, main
from core.domain.enums import CellState, RankingPolicy, RunMode, Status
from core.domain.models import Cell, Focus, LLMEvidence, RunConfig
from core.ports.search import SearchResult
from core.store.sqlite_store import SqliteStore
from tests.fakes.ports import FakeSearchAdapter

CONFIG_FIXTURES = Path(__file__).resolve().parent.parent / "config" / "fixtures"
PASSPORTS_FIXTURES = CONFIG_FIXTURES / "passports"

PRODUCT_A = "p1"
PRODUCT_B = "p2"
PARAM_A = "param_a"
PARAM_B = "param_b"
RUN_ID = "run.r"


# ---------------------------------------------------------------------------
# Помощники (категорийно нейтрально)
# ---------------------------------------------------------------------------


def _cfg() -> RunConfig:
    """Конфиг прогона с двумя продуктами и фокусом на один модуль."""
    return RunConfig(
        case_id="case_x",
        anchor=PRODUCT_A,
        cohort="cohort_case_x",
        products=[PRODUCT_A, PRODUCT_B],
        package="case_x",
        focus=Focus(modules=["mod_a", "mod_b"], params=[]),
        languages=["en"],
        snapshot_date=_dt.date(2026, 6, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def _cell(
    product_id: str,
    param_id: str,
    state: CellState,
    status: Status | None = None,
    *,
    run_id: str = RUN_ID,
) -> Cell:
    """Собирает ячейку заданного состояния/статуса (валидную под I1/I2)."""
    evidence = (
        [LLMEvidence(source_id="src.1", quote="q")]
        if status in {Status.OK, Status.PARTIAL}
        else []
    )
    absence = [{"source_scope": "partial"}] if status is Status.ABSENT else []
    return Cell(
        run_id=run_id,
        product_id=product_id,
        param_id=param_id,
        state=state,
        status=status,
        evidence=evidence,
        absence_evidence=absence,
    )


def _seed_run(db_path: Path, cells: list[Cell], *, run_id: str = RUN_ID) -> None:
    """Создаёт прогон и сохраняет ячейки (детерминированно)."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with SqliteStore(db_path) as store:
        store.create_run(run_id, _cfg())
        for cell in cells:
            store.save_cell(cell)
        store.conn.commit()


def _read_cell(db_path: Path, product_id: str, param_id: str, *, run_id: str = RUN_ID) -> Cell:
    """Читает ячейку из store после операции (проверка персистенции)."""
    with SqliteStore(db_path) as store:
        for cell in store.iter_cells(run_id):
            if cell.product_id == product_id and cell.param_id == param_id:
                return cell
    raise AssertionError(f"ячейка не найдена: {product_id}/{param_id}")


def _json_out(capsys: pytest.CaptureFixture[str]) -> dict:
    """Читает единственный JSON-объект из stdout."""
    captured = capsys.readouterr()
    return json.loads(captured.out.strip().splitlines()[-1])


def _fake_adapter(results: dict[str, list[SearchResult]] | None = None) -> FakeSearchAdapter:
    """Фейковый поисковый адаптер без сети (I19: только кандидаты)."""
    return FakeSearchAdapter(results=results)


class _UnavailableAdapter:
    """Поисковый адаптер с ``available=False`` (без сети/зависимостей)."""

    adapter_kind = "serp"
    provider = "unavailable"
    available = False

    def search(self, q):  # pragma: no cover — не должен вызываться
        raise AssertionError("недоступный адаптер не должен вызываться")

    def fetch(self, url):  # pragma: no cover
        raise AssertionError("недоступный адаптер не должен вызываться")


# ---------------------------------------------------------------------------
# discover
# ---------------------------------------------------------------------------


def test_discover_injected_adapter_lists_candidates_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    adapter = _fake_adapter(
        {
            "q1": [
                SearchResult(
                    url="https://ex.test/a", title="Alpha", snippet="snip", rank=1
                )
            ]
        }
    )

    code = main(["discover", "--query", "q1", "--json"], search_adapter=adapter)
    assert code == EXIT_OK
    payload = _json_out(capsys)
    assert payload["ok"] is True
    assert payload["phase"] == "discovery"
    assert payload["query"] == ["q1"]
    assert payload["search_calls"] == 1
    assert len(payload["candidates"]) == 1
    candidate = payload["candidates"][0]
    assert candidate["title"] == "Alpha"
    assert candidate["source_urls"] == ["https://ex.test/a"]
    assert candidate["status"] == "hypothesis"  # не promoted (G2 не автоматизирован)
    assert "I19" in payload["note"]
    assert payload["seeded_queries"] == []


def test_discover_human_output(capsys: pytest.CaptureFixture[str]) -> None:
    adapter = _fake_adapter(
        {"q1": [SearchResult(url="https://ex.test/a", title="Alpha", rank=1)]}
    )

    code = main(["discover", "--query", "q1"], search_adapter=adapter)
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "candidates: 1" in out
    assert "https://ex.test/a" in out
    assert "Alpha" in out
    assert "I19" in out


def test_discover_unavailable_adapter_is_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = main(["discover", "--query", "q1"], search_adapter=_UnavailableAdapter())
    assert code == EXIT_ERROR
    err = capsys.readouterr().err
    assert "недоступ" in err
    assert "I19" in err


def test_discover_no_query_is_usage_error() -> None:
    assert main(["discover"]) == EXIT_USAGE


def test_discover_unknown_adapter_is_usage_error() -> None:
    assert (
        main(["discover", "--query", "q1", "--search-adapter", "nope"]) == EXIT_USAGE
    )


def test_supported_search_adapters_include_glm_and_yandex() -> None:
    """Registry: адаптеры (serp/llm_grounding) собираемы и конформны порту (I19)."""
    from core.cli.wiring import SUPPORTED_SEARCH_ADAPTERS, build_search_adapter
    from core.ports.search import SearchAdapterPort

    assert set(SUPPORTED_SEARCH_ADAPTERS) >= {
        "yandex",
        "glm",
        "google",
        "duckduckgo",
        "open-websearch",
    }
    for name in SUPPORTED_SEARCH_ADAPTERS:
        adapter = build_search_adapter(name)
        assert isinstance(adapter, SearchAdapterPort), name
        # Кандидаты, не evidence: у адаптера нет поверхности записи/evidence (I19).
        assert not hasattr(adapter, "save_cell")
        assert not hasattr(adapter, "add_evidence")


def test_search_adapter_configured_keyless_adapters_allowed() -> None:
    """Бесключевые адаптеры (duckduckgo/open-websearch) не блокируются в discover.

    ``_search_adapter_configured`` требует ``api_key`` только у ключевых адаптеров
    (yandex/glm/google). Бесключевые атрибута ``api_key`` не имеют — для них достаточна
    ``available``; иначе ``hound discover --search-adapter duckduckgo`` ложно падал бы.
    """
    from core.cli.main import _search_adapter_configured

    class _Keyless:
        available = True
        api_key = None  # type: ignore[assignment]

    class _KeyedNoKey:
        available = True
        api_key = None

    class _KeyedWithKey:
        available = True
        api_key = "secret"

    class _NoApiKeyAttr:
        available = True

    class _Unavailable:
        available = False

    assert _search_adapter_configured(_NoApiKeyAttr()) is True  # duckduckgo/open-websearch
    assert _search_adapter_configured(_KeyedWithKey()) is True
    assert _search_adapter_configured(_KeyedNoKey()) is False  # yandex/glm/google без ключа
    assert _search_adapter_configured(_Unavailable()) is False
    # Явное присутствие атрибута api_key=None (даже у «keyless»-класса) → не готов.
    assert _search_adapter_configured(_Keyless()) is False


def test_discover_keyless_adapter_not_rejected_for_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``discover`` с бесключевым реальным адаптером не отбивается из-за отсутствия ключа.

    Проверяем, что после фикса бесключевой адаптер проходит проверку конфигурации:
    подменяем ``build_search_adapter`` на бесключевой фейк и убеждаемся, что доходит до
    вызова ``search`` (а не до ``EXIT_ERROR`` про «нет учётных данных»).
    """
    from core.cli import main as main_mod

    class _KeylessAdapter(FakeSearchAdapter):
        """Бесключевой адаптер: ``available=True`` и нет атрибута ``api_key``."""

        provider = "keyless"

    # ``core.cli`` экспортирует функцию ``main`` (не модуль) — берём модуль по имени.
    cli_main = importlib.import_module("core.cli.main")
    monkeypatch.setattr(
        cli_main, "build_search_adapter", lambda _name: _KeylessAdapter()
    )
    code = cli_main.main(["discover", "--query", "q1", "--json"])
    assert code == EXIT_OK



def test_discover_rejects_store_flags(capsys: pytest.CaptureFixture[str]) -> None:
    """У ``discover`` нет поверхности записи в store: ``--db`` — usage-ошибка (I19)."""
    asserted = main(["discover", "--query", "q1", "--db", "x.db"])
    assert asserted == EXIT_USAGE
    assert "usage" in capsys.readouterr().err


def test_discover_seeds_queries_from_passports(
    capsys: pytest.CaptureFixture[str],
) -> None:
    adapter = _fake_adapter()

    code = main(
        ["discover", "--passports-dir", str(PASSPORTS_FIXTURES), "--json"],
        search_adapter=adapter,
    )
    assert code == EXIT_OK
    payload = _json_out(capsys)
    # doc_domains паспорта p1: example.com → https-кандидат (только подсказка, I19).
    assert payload["seeded_queries"] == ["https://example.com/"]
    assert "https://example.com/" in payload["query"]
    assert payload["candidates"] == []


def test_discover_writes_no_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Discovery не пишет evidence: ни БД, ни файлов в рабочем каталоге (I19)."""
    monkeypatch.chdir(tmp_path)
    adapter = _fake_adapter(
        {"q1": [SearchResult(url="https://ex.test/a", title="Alpha", rank=1)]}
    )

    code = main(["discover", "--query", "q1"], search_adapter=adapter)
    assert code == EXIT_OK
    capsys.readouterr()
    assert list(tmp_path.iterdir()) == []


# ---------------------------------------------------------------------------
# review
# ---------------------------------------------------------------------------


def test_review_lists_preliminary_and_unknown(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(
        db,
        [
            _cell(PRODUCT_A, PARAM_A, CellState.PRELIMINARY, None),
            _cell(PRODUCT_B, PARAM_B, CellState.EVALUATED, Status.UNKNOWN),
            _cell(PRODUCT_A, PARAM_B, CellState.EVALUATED, Status.OK),
        ],
    )

    code = main(["review", "--db", str(db), "--run-id", RUN_ID, "--json"])
    assert code == EXIT_OK
    payload = _json_out(capsys)
    assert payload["ok"] is True
    assert payload["count"] == 2
    # Детерминированная сортировка по (product_id, param_id).
    assert [
        (item["product_id"], item["param_id"]) for item in payload["items"]
    ] == [(PRODUCT_A, PARAM_A), (PRODUCT_B, PARAM_B)]
    states = {item["state"] for item in payload["items"]}
    assert states == {"preliminary", "evaluated"}
    unknown = payload["items"][1]
    assert unknown["status"] == Status.UNKNOWN.value
    assert "I3" in unknown["reason"] or "unknown" in unknown["reason"].lower()


def test_review_empty_queue_message(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(db, [_cell(PRODUCT_A, PARAM_A, CellState.EVALUATED, Status.OK)])

    code = main(["review", "--db", str(db), "--run-id", RUN_ID])
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "queue empty" in out
    assert f"run_id: {RUN_ID}" in out


def test_review_empty_queue_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(db, [_cell(PRODUCT_A, PARAM_A, CellState.EVALUATED, Status.OK)])

    code = main(["review", "--db", str(db), "--run-id", RUN_ID, "--json"])
    assert code == EXIT_OK
    payload = _json_out(capsys)
    assert payload == {"ok": True, "run_id": RUN_ID, "count": 0, "items": []}


def test_review_runs_dir_resolution(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_dir = tmp_path / "runs"
    _seed_run(
        runs_dir / RUN_ID / "cohound.db",
        [_cell(PRODUCT_A, PARAM_A, CellState.PRELIMINARY, None)],
    )

    code = main(
        ["review", "--runs-dir", str(runs_dir), "--run-id", RUN_ID, "--json"]
    )
    assert code == EXIT_OK
    assert _json_out(capsys)["count"] == 1


# ---------------------------------------------------------------------------
# promote
# ---------------------------------------------------------------------------


def test_promote_missing_actor_is_usage_error() -> None:
    assert main(["promote", "--db", "x.db", "--run-id", RUN_ID]) == EXIT_USAGE


def test_promote_empty_actor_is_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(db, [_cell(PRODUCT_A, PARAM_A, CellState.PRELIMINARY, None)])

    code = main(
        [
            "promote",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--actor",
            "  ",
            "--status",
            Status.UNKNOWN.value,
        ]
    )
    assert code == EXIT_ERROR
    assert "I6/I7" in capsys.readouterr().err


def test_promote_with_unknown_status_sets_reviewed_by(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(db, [_cell(PRODUCT_A, PARAM_A, CellState.PRELIMINARY, None)])

    code = main(
        [
            "promote",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--actor",
            "human.reviewer",
            "--status",
            Status.UNKNOWN.value,
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = _json_out(capsys)
    assert payload["promoted"] == 1
    assert payload["requested"] == 1
    assert payload["promoted_cells"] == [
        {"product_id": PRODUCT_A, "param_id": PARAM_A, "status": Status.UNKNOWN.value}
    ]

    promoted = _read_cell(db, PRODUCT_A, PARAM_A)
    assert promoted.state is CellState.EVALUATED
    assert promoted.status is Status.UNKNOWN
    assert promoted.reviewed_by == "human.reviewer"


def test_promote_without_verdict_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--validated-only`` (дефолт) без ``--status``: вердикт не выдумывается (I14)."""
    db = tmp_path / "cohound.db"
    _seed_run(db, [_cell(PRODUCT_A, PARAM_A, CellState.PRELIMINARY, None)])

    code = main(
        [
            "promote",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--actor",
            "human.reviewer",
            "--json",
        ]
    )
    assert code == EXIT_ERROR
    payload = _json_out(capsys)
    assert payload["ok"] is False
    assert payload["promoted"] == 0
    assert payload["skipped"]
    assert "I14" in payload["skipped"][0]["reason"]

    unchanged = _read_cell(db, PRODUCT_A, PARAM_A)
    assert unchanged.state is CellState.PRELIMINARY
    assert unchanged.status is None


def test_promote_no_validated_only_allows_pending(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(db, [_cell(PRODUCT_A, PARAM_A, CellState.PRELIMINARY, None)])

    code = main(
        [
            "promote",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--actor",
            "human.reviewer",
            "--no-validated-only",
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = _json_out(capsys)
    assert payload["promoted"] == 1
    assert payload["validated_only"] is False
    pending = _read_cell(db, PRODUCT_A, PARAM_A)
    assert pending.state is CellState.EVALUATED
    assert pending.status is None


def test_promote_non_preliminary_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``reviewed``-ячейка в очереди, но повышению не подлежит → ``EXIT_ERROR``."""
    db = tmp_path / "cohound.db"
    _seed_run(db, [_cell(PRODUCT_A, PARAM_A, CellState.REVIEWED, Status.UNKNOWN)])

    code = main(
        [
            "promote",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--actor",
            "human.reviewer",
            "--status",
            Status.UNKNOWN.value,
            "--json",
        ]
    )
    assert code == EXIT_ERROR
    payload = _json_out(capsys)
    assert payload["promoted"] == 0
    assert payload["skipped"]
    assert "preliminary" in payload["skipped"][0]["reason"]


def test_promote_filter_by_param(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(
        db,
        [
            _cell(PRODUCT_A, PARAM_A, CellState.PRELIMINARY, None),
            _cell(PRODUCT_A, PARAM_B, CellState.PRELIMINARY, None),
        ],
    )

    code = main(
        [
            "promote",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--actor",
            "human.reviewer",
            "--param",
            PARAM_A,
            "--status",
            Status.UNKNOWN.value,
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = _json_out(capsys)
    assert payload["requested"] == 1
    assert payload["promoted"] == 1
    assert payload["promoted_cells"][0]["param_id"] == PARAM_A

    assert _read_cell(db, PRODUCT_A, PARAM_A).state is CellState.EVALUATED
    assert _read_cell(db, PRODUCT_A, PARAM_B).state is CellState.PRELIMINARY


def test_promote_human_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(db, [_cell(PRODUCT_A, PARAM_A, CellState.PRELIMINARY, None)])

    code = main(
        [
            "promote",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--actor",
            "human.reviewer",
            "--status",
            Status.UNKNOWN.value,
        ]
    )
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert f"run_id: {RUN_ID}" in out
    assert "actor: human.reviewer" in out
    assert "promoted: 1" in out
