"""CLI-поверхность F2: флаг ``--crawl-only`` (17 §4).

Проверяется ровно граница входа:

1. ``crawl run --crawl-only`` принимается, публикует ``crawl_only: true`` и очередь
   ``extract_queue``, а корпус при этом **не** собирается;
2. ``crawl run --until evaluate --crawl-only`` — ``EXIT_USAGE``: режим не производит
   корпус, поэтому фазы после ``corpus`` обещали бы измерение по пустому корпусу;
3. у полного ``run`` флага нет (argparse отвергает) — иначе прогон печатал бы корпус
   без документов.

Порты подменяются in-memory заглушками; сеть не используется. Данные нейтральны (§1.3).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.cli.main import EXIT_OK, EXIT_USAGE, main
from core.pipeline import Ports
from core.ports.types import FetchPlan, RawPage
from core.store.sqlite_store import SqliteStore
from tests.fakes.ports import FakeChunker, FakeExtractor
from tests.helpers.output import json_out
from tests.helpers.packages import valid_package

TS = "2026-01-01T00:00:00+00:00"
URL_P1 = "https://docs.test/p1"
URL_P2 = "https://docs.test/p2"


class _RecordingCrawler:
    """Краулер-заглушка: отдаёт заданные байты и запоминает вызовы."""

    version = "recording-crawler-1"

    def __init__(self, pages: dict[str, RawPage] | None = None) -> None:
        self.pages = dict(pages or {})
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта
        self.calls.append(url)
        page = self.pages.get(url)
        if page is not None:
            return page
        return RawPage(url=url, content=b"", fetched_at=TS, fetch_mode=plan.fetch)


def _pages() -> dict[str, RawPage]:
    return {
        URL_P1: RawPage(url=URL_P1, content=b"one", fetched_at=TS),
        URL_P2: RawPage(url=URL_P2, content=b"two", fetched_at=TS),
    }


def _urls_file(tmp_path: Path) -> Path:
    path = tmp_path / "urls.json"
    path.write_text(f'{{"p1": ["{URL_P1}"], "p2": ["{URL_P2}"]}}', encoding="utf-8")
    return path


def _argv(
    package: Path,
    *,
    action: str,
    run_id: str,
    db: Path,
    urls: Path | None = None,
    extra: tuple[str, ...] = (),
    json_mode: bool = True,
) -> list[str]:
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
    ]
    if urls is not None:
        argv += ["--urls", str(urls)]
    if json_mode:
        argv.append("--json")
    argv += list(extra)
    return argv


def _ports(crawler: _RecordingCrawler) -> Ports:
    return Ports(crawler=crawler, extractor=FakeExtractor(), chunker=FakeChunker())


def _corpus_docs(db: Path) -> int:
    with SqliteStore(db, read_only=True) as store:
        row = store.conn.execute("SELECT COUNT(*) AS n FROM corpus_docs").fetchone()
        return int(row["n"])


# --------------------------------------------------------------------------- #
# (1) флаг принимается и режим виден в payload
# --------------------------------------------------------------------------- #


def test_crawl_run_crawl_only_publishes_queue(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr1.db"
    crawler = _RecordingCrawler(_pages())

    code = main(
        _argv(
            package,
            action="run",
            run_id="cr1",
            db=db,
            urls=_urls_file(tmp_path),
            extra=("--crawl-only",),
        ),
        ports=_ports(crawler),
    )

    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["crawl_only"] is True
    assert payload["extract_queue"] == {"pending": 2}
    assert payload["crawl"]["fetched"] == 2
    assert sorted(crawler.calls) == [URL_P1, URL_P2]
    # Краул добыл — корпус не собран (это и есть смысл режима).
    assert _corpus_docs(db) == 0


def test_crawl_run_publishes_phase_notes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Заметки фазы корпуса доходят до payload ``crawl run`` (17 §7.2).

    Регресс: в полном ``run`` строки ``log_entries`` видны как ``results[].notes``, а
    ``crawl run`` собирал свой payload и их терял — то есть измерительный протокол
    публиковал числа без того, чем они получены. Строка о полосе берётся настоящая:
    ось объявлена (3), host в продукте один ⇒ фаза пишет «полос не больше 1».
    """
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    crawler = _RecordingCrawler(_pages())

    code = main(
        _argv(
            package,
            action="run",
            run_id="cr3",
            db=tmp_path / "cr3.db",
            urls=_urls_file(tmp_path),
            extra=("--crawl-only", "--host-concurrency", "3"),
        ),
        ports=_ports(crawler),
    )

    assert code == EXIT_OK
    payload = json_out(capsys)
    lane = [
        note
        for note in payload["notes"]
        if note.startswith("crawl: продукт p1: host_concurrency=3 объявлен")
    ]
    assert lane, payload["notes"]
    assert "host'ов в продукте 1" in lane[0]
    # Строки фазы — наблюдения, а не сбои: в errors их нет.
    assert not any("host_concurrency" in error for error in payload["errors"])


def test_crawl_run_crawl_only_prints_human_line(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    crawler = _RecordingCrawler(_pages())

    code = main(
        _argv(
            package,
            action="run",
            run_id="cr2",
            db=tmp_path / "cr2.db",
            urls=_urls_file(tmp_path),
            extra=("--crawl-only",),
            json_mode=False,
        ),
        ports=_ports(crawler),
    )

    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "crawl_only: true" in out
    assert "extract_queue: pending=2" in out


def test_crawl_status_reports_queue(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """``crawl status`` показывает то же число: «добыто N, конверсия взяла 0»."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr3.db"
    assert main(
        _argv(
            package,
            action="run",
            run_id="cr3",
            db=db,
            urls=_urls_file(tmp_path),
            extra=("--crawl-only",),
        ),
        ports=_ports(_RecordingCrawler(_pages())),
    ) == EXIT_OK
    capsys.readouterr()

    assert main(["crawl", "status", "--run-id", "cr3", "--db", str(db), "--json"]) == EXIT_OK
    payload = json_out(capsys)
    assert payload["extract_queue"] == {"pending": 2}


# --------------------------------------------------------------------------- #
# (2) --until за пределы corpus — отказ входа
# --------------------------------------------------------------------------- #


def test_crawl_only_beyond_corpus_is_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    crawler = _RecordingCrawler(_pages())

    code = main(
        _argv(
            package,
            action="run",
            run_id="cr4",
            db=tmp_path / "cr4.db",
            urls=_urls_file(tmp_path),
            extra=("--crawl-only", "--until", "evaluate"),
            # Без ``--json``: сообщение об отказе — часть контракта режима
            # (в ``--json`` машинный вывод не засоряется, кода возврата достаточно).
            json_mode=False,
        ),
        ports=_ports(crawler),
    )

    assert code == EXIT_USAGE
    err = capsys.readouterr().err
    assert "crawl-only" in err and "корпус" in err
    assert crawler.calls == []  # отказ до работы: ни одного фетча


# --------------------------------------------------------------------------- #
# (3) у полного run флага нет
# --------------------------------------------------------------------------- #


def test_full_run_does_not_declare_crawl_only(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)

    code = main(
        [
            "run",
            "--run-yaml",
            str(package / "run.yaml"),
            "--package",
            str(package),
            "--crawl-only",
        ],
        ports=_ports(_RecordingCrawler(_pages())),
    )

    assert code == EXIT_USAGE
    assert "--crawl-only" in capsys.readouterr().err