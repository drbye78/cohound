"""Тесты CLI-поверхности ``hound crawl`` (L3b): run/status/list/resume/restart/cancel.

Команда — тонкая обёртка над durable-сеансом краула (Lane B/L1a): она не реализует
логику краула сама, а резолвит прогон/порты/фазы (как ``run``) и вызывает фазу корпуса Ш5
(:func:`core.pipeline.corpus.build_corpus`). Порты подменяются in-memory заглушками —
сеть не используется. Фикстуры категорийно-нейтральны (§1.3): ``p1/p2``, ``case_x``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from core.cli.commands import COMMANDS
from core.cli.main import (
    EXIT_NOT_FOUND,
    EXIT_OK,
    EXIT_USAGE,
    build_parser,
    main,
)
from core.pipeline import Ports
from core.ports.types import FetchPlan, RawPage
from tests.fakes.ports import FakeChunker, FakeExtractor
from tests.helpers.output import json_out
from tests.helpers.packages import valid_package

TS = "2026-01-01T00:00:00+00:00"
URL_P1 = "https://docs.test/p1"
URL_P2 = "https://docs.test/p2"


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


class _RecordingCrawler:
    """Краулер-заглушка, запоминающий URL'ы фетча (проверка idempotency/refetch)."""

    version = "recording-crawler-1"

    def __init__(self, pages: dict[str, RawPage] | None = None) -> None:
        self.pages = dict(pages or {})
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        """Помечает URL и возвращает заданную страницу (или пустые байты)."""
        self.calls.append(url)
        page = self.pages.get(url)
        if page is not None:
            return page
        return RawPage(
            url=url, content=b"", fetched_at=TS, fetch_mode=plan.fetch
        )


def _pages() -> dict[str, RawPage]:
    return {
        URL_P1: RawPage(url=URL_P1, content=b"one", fetched_at=TS),
        URL_P2: RawPage(url=URL_P2, content=b"two", fetched_at=TS),
    }


def _urls_file(tmp_path: Path) -> Path:
    path = tmp_path / "urls.json"
    path.write_text(
        f'{{"p1": ["{URL_P1}"], "p2": ["{URL_P2}"]}}', encoding="utf-8"
    )
    return path


def _ports(crawler: _RecordingCrawler) -> Ports:
    return Ports(crawler=crawler, extractor=FakeExtractor(), chunker=FakeChunker())


def _run_argv(
    package: Path,
    *,
    action: str,
    run_id: str,
    db: Path,
    urls: Path | None = None,
    extra: tuple[str, ...] = (),
) -> list[str]:
    """argv подкоманды ``crawl <action>`` для ``run``/``resume``/``restart``."""
    argv = [
        "crawl",
        action,
        "--run-yaml",
        str(package / "run.yaml"),
        "--package",
        str(package),
        "--run-id",
        run_id,
        "--db",
        str(db),
        "--json",
    ]
    if urls is not None:
        argv += ["--urls", str(urls)]
    argv += list(extra)
    return argv


# ---------------------------------------------------------------------------
# Регистрация команды
# ---------------------------------------------------------------------------


def test_crawl_registered_in_commands() -> None:
    assert any(module.__name__ == "core.cli.commands.crawl" for module in COMMANDS)


def test_crawl_appears_in_help(capsys: pytest.CaptureFixture[str]) -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):  # ``--help`` штатно выходит
        parser.parse_args(["crawl", "--help"])
    out = capsys.readouterr().out
    for sub in ("run", "status", "list", "resume", "restart", "cancel"):
        assert sub in out


def test_crawl_no_subcommand_is_usage_error() -> None:
    assert main(["crawl"]) == EXIT_USAGE


# ---------------------------------------------------------------------------
# status / list на пустой БД
# ---------------------------------------------------------------------------


def test_crawl_status_empty_session_returns_null(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Нет сеанса → ``EXIT_OK`` c ``status: null`` (пустой, но валидный ответ)."""
    db = tmp_path / "empty.db"
    code = main(
        ["crawl", "status", "--run-id", "r1", "--db", str(db), "--json"]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert payload["status"] is None
    assert payload["session_id"] == "crawl:r1"
    assert payload["counts"] == {}


def test_crawl_list_empty_db(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "empty.db"
    code = main(["crawl", "list", "--run-id", "r1", "--db", str(db), "--json"])
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["count"] == 0
    assert payload["sessions"] == []


# ---------------------------------------------------------------------------
# run → сеанс с элементами; status → счётчики
# ---------------------------------------------------------------------------


def test_crawl_run_creates_session_with_items(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    crawler = _RecordingCrawler(_pages())
    code = main(
        _run_argv(
            package, action="run", run_id="cr1", db=tmp_path / "cr1.db",
            urls=_urls_file(tmp_path),
        ),
        ports=_ports(crawler),
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert payload["session_id"] == "crawl:cr1"
    assert payload["crawl"]["fetched"] == 2
    assert sorted(crawler.calls) == [URL_P1, URL_P2]


def test_crawl_status_reports_counts_after_run(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr1.db"
    crawler = _RecordingCrawler(_pages())
    ports = _ports(crawler)
    assert main(
        _run_argv(package, action="run", run_id="cr1", db=db, urls=_urls_file(tmp_path)),
        ports=ports,
    ) == EXIT_OK
    capsys.readouterr()

    code = main(["crawl", "status", "--run-id", "cr1", "--db", str(db), "--json"])
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["status"] == "completed"
    assert payload["counts"]["fetched"] == 2
    assert payload["total"] == 2


def test_crawl_status_per_url_lists_items(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr1.db"
    crawler = _RecordingCrawler(_pages())
    ports = _ports(crawler)
    assert main(
        _run_argv(package, action="run", run_id="cr1", db=db, urls=_urls_file(tmp_path)),
        ports=ports,
    ) == EXIT_OK
    capsys.readouterr()

    code = main(
        ["crawl", "status", "--run-id", "cr1", "--db", str(db), "--per-url", "--json"]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    items = payload["items"]
    assert {row["product_id"] for row in items} == {"p1", "p2"}
    assert all(row["status"] == "fetched" for row in items)


# ---------------------------------------------------------------------------
# resume (идемпотентность) / restart (очистка)
# ---------------------------------------------------------------------------


def test_crawl_resume_is_idempotent_no_refetch(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr1.db"
    crawler = _RecordingCrawler(_pages())
    ports = _ports(crawler)
    argv = _run_argv(package, action="run", run_id="cr1", db=db, urls=_urls_file(tmp_path))
    assert main(argv, ports=ports) == EXIT_OK
    assert sorted(crawler.calls) == [URL_P1, URL_P2]
    capsys.readouterr()

    crawler.calls.clear()
    resume_argv = _run_argv(
        package, action="resume", run_id="cr1", db=db, urls=_urls_file(tmp_path)
    )
    assert main(resume_argv, ports=ports) == EXIT_OK
    payload = json_out(capsys)
    assert payload["mode"] == "resume"
    assert payload["crawl"]["fetched"] == 2
    # Resume не пере-фетчит уже FETCHED-элементы (recovery + retained raw).
    assert crawler.calls == []


def test_crawl_restart_clears_and_refetches(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr1.db"
    crawler = _RecordingCrawler(_pages())
    ports = _ports(crawler)
    assert main(
        _run_argv(package, action="run", run_id="cr1", db=db, urls=_urls_file(tmp_path)),
        ports=ports,
    ) == EXIT_OK
    capsys.readouterr()

    crawler.calls.clear()
    restart_argv = _run_argv(
        package,
        action="restart",
        run_id="cr1",
        db=db,
        urls=_urls_file(tmp_path),
        extra=("--from-scratch",),
    )
    assert main(restart_argv, ports=ports) == EXIT_OK
    payload = json_out(capsys)
    assert payload["mode"] == "restart"
    assert payload["cleared"] is True
    # Очистка сеанса → повторный фетч; --from-scratch задокументирован как Phase 4.
    assert sorted(crawler.calls) == [URL_P1, URL_P2]
    assert any("from-scratch" in note for note in payload["notes"])


def test_crawl_run_reset_clears_session(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr1.db"
    crawler = _RecordingCrawler(_pages())
    ports = _ports(crawler)
    assert main(
        _run_argv(package, action="run", run_id="cr1", db=db, urls=_urls_file(tmp_path)),
        ports=ports,
    ) == EXIT_OK
    capsys.readouterr()

    crawler.calls.clear()
    reset_argv = _run_argv(
        package, action="run", run_id="cr1", db=db, urls=_urls_file(tmp_path),
        extra=("--reset",),
    )
    assert main(reset_argv, ports=ports) == EXIT_OK
    payload = json_out(capsys)
    assert payload["cleared"] is True
    assert sorted(crawler.calls) == [URL_P1, URL_P2]


# ---------------------------------------------------------------------------
# cancel / list / --until
# ---------------------------------------------------------------------------


def test_crawl_cancel_sets_cancelled(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr1.db"
    crawler = _RecordingCrawler(_pages())
    ports = _ports(crawler)
    assert main(
        _run_argv(package, action="run", run_id="cr1", db=db, urls=_urls_file(tmp_path)),
        ports=ports,
    ) == EXIT_OK
    capsys.readouterr()

    code = main(["crawl", "cancel", "--run-id", "cr1", "--db", str(db), "--json"])
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["status"] == "cancelled"


def test_crawl_cancel_missing_session_not_found(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "empty.db"
    code = main(["crawl", "cancel", "--run-id", "r1", "--db", str(db), "--json"])
    assert code == EXIT_NOT_FOUND
    payload = json_out(capsys)
    assert payload["ok"] is False
    assert payload["status"] is None


def test_crawl_list_reports_session(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr1.db"
    crawler = _RecordingCrawler(_pages())
    ports = _ports(crawler)
    assert main(
        _run_argv(package, action="run", run_id="cr1", db=db, urls=_urls_file(tmp_path)),
        ports=ports,
    ) == EXIT_OK
    capsys.readouterr()

    code = main(["crawl", "list", "--run-id", "cr1", "--db", str(db), "--json"])
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["count"] == 1
    assert payload["sessions"][0]["session_id"] == "crawl:cr1"


def test_crawl_run_until_before_corpus_skips(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--until`` раньше corpus → краул не запускается (статус ``skipped``, ``EXIT_OK``)."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr1.db"
    crawler = _RecordingCrawler(_pages())
    ports = _ports(crawler)
    argv = _run_argv(
        package, action="run", run_id="cr1", db=db, urls=_urls_file(tmp_path),
        extra=("--until", "g1"),
    )
    assert main(argv, ports=ports) == EXIT_OK
    payload = json_out(capsys)
    assert payload["status"] == "skipped"
    assert crawler.calls == []
    assert payload["crawl"] == {}
