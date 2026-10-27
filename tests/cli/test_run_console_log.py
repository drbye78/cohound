"""Консоль прогона дублируется в ``runs/{run_id}/logs/console.log``.

Дефект, который этот тест закрепляет, наблюдался на живом прогоне: хост выключили
посреди извлечения, и от пяти часов работы не осталось ни одной улики. В
``logs/`` лежат только пофазные ``jsonl`` — они пишутся на **границах** фаз, а
фаза ``corpus`` занимает часы. Вопрос «на чём он стоял» приходилось решать
реконструкцией по косвенным записям (mtime кэшей, heartbeat сеанса).

Проверяется ровно то, что делает фичу полезной, а не факт вызова функции:

* вывод идёт **и** в консоль, **и** в файл — иначе оператор потерял бы прогресс;
* файл открыт построчно и в режиме дозаписи, поэтому последняя строка переживает
  падение процесса и повторный запуск не затирает предыдущий;
* ``--dry-run`` не пишет ничего по контракту — лог для него не открывается;
* вторичный приёмник не ломает вывод: ошибка записи в файл не всплывает;
* ``tqdm``/``docling`` спрашивают у ``stderr`` ``isatty``/``fileno`` — отвечает
  первичный поток (терминал), иначе прогресс-бар перестал бы его распознавать.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import sys
from pathlib import Path

from core.cli.commands.run import (
    _append_console,
    _ConsoleTee,
    _install_console_log,
    _restore_console,
)


def test_writes_both_to_console_and_file(tmp_path: Path) -> None:
    """Строка доходит до обоих приёмников: консоль не теряет прогресс."""
    db_path = tmp_path / "runs" / "run-1" / "cohound.db"
    db_path.parent.mkdir(parents=True)

    marker = _install_console_log(db_path)
    assert marker is not None, "resolved-каталог ⇒ лог открывается"
    try:
        print("фаза corpus: идёт извлечение")
        sys.stdout.write("без перевода строки")
        sys.stdout.flush()
        sys.stderr.write("предупреждение\n")
        sys.stderr.flush()
    finally:
        _restore_console(marker)

    text = (db_path.parent / "logs" / "console.log").read_text(encoding="utf-8")
    assert "=== запуск " in text, "начало запуска помечено — файл переживает несколько запусков"
    assert "фаза corpus: идёт извлечение" in text
    assert "без перевода строки" in text
    assert "предупреждение" in text


def test_restore_returns_original_streams(tmp_path: Path) -> None:
    """``finally`` обязан вернуть исходные потоки — иначе весь остаток процесса пишет в файл."""
    db_path = tmp_path / "run-2" / "cohound.db"
    out_before, err_before = sys.stdout, sys.stderr

    marker = _install_console_log(db_path)
    assert marker is not None
    assert isinstance(sys.stdout, _ConsoleTee)
    assert isinstance(sys.stderr, _ConsoleTee)
    _restore_console(marker)

    assert sys.stdout is out_before
    assert sys.stderr is err_before


def test_appends_and_keeps_previous_run(tmp_path: Path) -> None:
    """Повторный запуск дописывает: история прогона не затирается."""
    db_path = tmp_path / "run-3" / "cohound.db"

    for note in ("первый запуск", "второй запуск"):
        marker = _install_console_log(db_path)
        assert marker is not None
        try:
            print(note)
        finally:
            _restore_console(marker)

    text = (db_path.parent / "logs" / "console.log").read_text(encoding="utf-8")
    assert text.count("=== запуск ") == 2, "каждый запуск помечен отдельно"
    assert "первый запуск" in text and "второй запуск" in text


def test_delegates_stream_probe_to_primary(tmp_path: Path) -> None:
    """``isatty``/``encoding`` отвечает первичный поток, а не файл.

    Это не косметика: ``tqdm`` (загрузка весов docling) и индикаторы прогресса
    решают по ``isatty``, рисовать ли полосу. Если бы отвечал файл, вывод изменился
    бы — оператор потерял бы прогресс, ради которого лог и не должен ничего менять.
    """

    class _Primary:
        encoding = "utf-8"

        def __init__(self) -> None:
            self.written: list[str] = []

        def write(self, data: str) -> int:
            self.written.append(data)
            return len(data)

        def flush(self) -> None:  # pragma: no cover - вызывается, но не проверяется
            pass

        def isatty(self) -> bool:
            return True

        def fileno(self) -> int:
            return 42

    primary = _Primary()
    secondary: list[str] = []

    class _Sink:
        def write(self, data: str) -> int:
            secondary.append(data)
            return len(data)

        def flush(self) -> None:  # pragma: no cover
            pass

    tee = _ConsoleTee(primary, _Sink())
    assert tee.isatty() is True, "признак терминала берётся у консоли, не у файла"
    assert tee.fileno() == 42
    assert tee.encoding == "utf-8"
    assert tee.write("x") == 1
    assert primary.written == ["x"] and secondary == ["x"]


def test_secondary_failure_does_not_break_output(tmp_path: Path) -> None:
    """Ошибка записи в лог не ломает прогон: наблюдаемость не условие исполнения."""

    class _Broken:
        def write(self, data: str) -> int:
            raise OSError("No space left on device")

        def flush(self) -> None:  # pragma: no cover
            raise OSError("No space left on device")

    class _Primary:
        def __init__(self) -> None:
            self.written: list[str] = []

        def write(self, data: str) -> int:
            self.written.append(data)
            return len(data)

        def flush(self) -> None:
            pass

    primary = _Primary()
    tee = _ConsoleTee(primary, _Broken())
    assert tee.write("важное") == len("важное")
    tee.flush()  # не бросает
    assert primary.written == ["важное"]


def test_unwritable_target_disables_log_without_failing(tmp_path: Path) -> None:
    """Недоступный каталог ⇒ ``None``: отсутствие лога не ошибка прогона."""
    # ``db_path.parent`` — файл, а не каталог: mkdir на его потомке невозможен.
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("", encoding="utf-8")

    marker = _install_console_log(blocker / "cohound.db")
    assert marker is None


def test_dry_run_flag_guards_install(tmp_path: Path) -> None:
    """``--dry-run`` ничего не пишет — лог не открывается.

    Проверяется на уровне решения ``run()``: лог включается только когда прогон
    пишущий. Тест держит инвариант «dry-run без записи» на случай, если кто-то
    перенесёт установку лога выше проверки флага.
    """
    source = Path("core/cli/commands/run.py").read_text(encoding="utf-8")
    assert "console = None if args.dry_run else _install_console_log(db_path)" in source, (
        "установка лога обязана быть под условием --dry-run"
    )


def test_failure_reason_reaches_the_file(tmp_path: Path) -> None:
    """Причина отказа попадает в лог, хотя печатает её ``main.py`` — уже после ``run``.

    Без этого файл сообщал бы «дошёл до фазы X» и молчал о том, почему остановился:
    самое частое чтение лога — как раз отказ (память, линт, гейт), а печать живёт
    снаружи функции. Проверяется, что строка не дублируется в консоль: пишем прямо
    в дескриптор, а не через ``print``.
    """
    db_path = tmp_path / "run-4" / "cohound.db"
    marker = _install_console_log(db_path)
    assert marker is not None
    console_before = sys.stdout
    try:
        _append_console(marker, "error: CliError: extraction does not fit memory budget")
        # Пишем в файл, но НЕ печатаем: иначе оператор увидел бы сообщение дважды.
        assert sys.stdout is console_before
    finally:
        _restore_console(marker)

    text = (db_path.parent / "logs" / "console.log").read_text(encoding="utf-8")
    assert "error: CliError: extraction does not fit memory budget" in text


def test_append_without_log_is_noop() -> None:
    """``None``-маркер (лог не открылся) не ломает путь отказа."""
    _append_console(None, "error: что угодно")  # не бросает

