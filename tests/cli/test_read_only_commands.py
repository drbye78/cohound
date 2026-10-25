"""Read-only команды CLI не создают БД и работают на живом прогоне (WAL).

Проверено вызовом (``12-cli-architecture-review.md`` §4.7): ``hound review --db
/tmp/ghost/x.db``, ``hound sensitivity --db …`` и ``hound golden --db …`` создавали файл на
233 472 байта — полную ``DDL``-схему. Причина — ``build_store`` делал ``mkdir`` и гонял
``DDL``. Последствие: при опечатке в пути пользователь получал не «не найден», а пустую БД,
и следующий ``run`` в тот же путь видел «прогон есть, он пустой».

Теперь читатели открывают БД через ``mode=ro``: файла нет → ``EXIT_NOT_FOUND`` (4),
каталогов не создаётся. Живой прогон (писатель держит WAL) читается без миграции схемы.

Данные категорийно нейтральны (§1.3); сеть не используется.
"""

from __future__ import annotations

import datetime as _dt
from pathlib import Path

import pytest

from core.cli.main import EXIT_NOT_FOUND, EXIT_OK, main
from core.domain.enums import RankingPolicy, RunMode
from core.domain.models import Focus, RunConfig
from core.store.sqlite_store import SqliteStore
from tests.helpers.output import json_out

RUN_ID = "run.1"


def _cfg() -> RunConfig:
    """Минимальный конфиг прогона (категорийно нейтральный)."""
    return RunConfig(
        case_id="case_x",
        anchor="p1",
        cohort="cohort_case_x",
        products=["p1"],
        package="case_x",
        focus=Focus(modules=["mod_a"], params=[]),
        languages=["en"],
        snapshot_date=_dt.date(2026, 6, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


#: Читающие команды: (описание, argv без пути к БД, как подставить ``--db``).
#: ``gate``/``promote``/``run``/``crawl run`` сюда не входят — они пишут.
_READ_ONLY_COMMANDS = [
    ("resolve", ["resolve", "src.1", "--db", "{db}"]),
    ("review", ["review", "--db", "{db}"]),
    ("sensitivity", ["sensitivity", "--db", "{db}"]),
    ("golden", ["golden", "--golden", "/nonexistent/golden.csv", "--db", "{db}"]),
    ("report", ["report", "--db", "{db}", "--run-id", RUN_ID, "--out", "{out}"]),
    ("diff", ["diff", "--db", "{db}", RUN_ID, RUN_ID]),
    ("crawl status", ["crawl", "status", "--run-id", RUN_ID, "--db", "{db}"]),
    ("crawl list", ["crawl", "list", "--db", "{db}"]),
]


@pytest.mark.parametrize(
    ("label", "argv"),
    _READ_ONLY_COMMANDS,
    ids=[label for label, _ in _READ_ONLY_COMMANDS],
)
def test_read_only_command_does_not_create_db(
    label: str,
    argv: list[str],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Отсутствующая БД — ``EXIT_NOT_FOUND``; ни файла, ни каталога на диске."""
    db = tmp_path / "ghost" / "x.db"
    out = tmp_path / "out"
    resolved = [part.format(db=db, out=out) for part in argv]

    code = main(resolved)

    assert code == EXIT_NOT_FOUND, label
    assert not db.exists(), label
    assert not db.parent.exists(), label
    captured = capsys.readouterr()
    assert "not_found" in captured.err
    assert "Traceback" not in captured.err


def test_review_reads_live_writer_wal(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Живой прогон: ``review`` читает закоммиченные данные, не ломаясь о WAL.

    Писатель (тот же процесс) держит ``cohound.db-wal``/``-shm``; читатель идёт через
    ``mode=ro``. Это ровно та ситуация, в которой находится оператор: ``run`` идёт, а
    статус хочется посмотреть сейчас.
    """
    db = tmp_path / "cohound.db"
    writer = SqliteStore(db)
    try:
        writer.create_run(RUN_ID, _cfg())
        writer.conn.commit()
        assert (tmp_path / "cohound.db-wal").exists()

        assert main(["review", "--db", str(db), "--run-id", RUN_ID, "--json"]) == EXIT_OK
        payload = json_out(capsys)
        assert payload["run_id"] == RUN_ID
        assert payload["count"] == 0
    finally:
        writer.close()


def test_report_reads_live_writer_wal(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``report`` тоже читатель: БД только читается, артефакты идут в файлы."""
    db = tmp_path / "cohound.db"
    out = tmp_path / "out"
    writer = SqliteStore(db)
    try:
        writer.create_run(RUN_ID, _cfg())
        writer.conn.commit()

        assert (
            main(
                [
                    "report",
                    "--db",
                    str(db),
                    "--run-id",
                    RUN_ID,
                    "--out",
                    str(out),
                    "--json",
                ]
            )
            == EXIT_OK
        )
        payload = json_out(capsys)
        assert payload["run_id"] == RUN_ID
        assert (out / RUN_ID / "outputs").is_dir()
    finally:
        writer.close()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
