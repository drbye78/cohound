"""Пины режима дрейфа ``scripts/check_doc_anchors.py --check-drift BASE``.

Гейт проверял только «строка по адресу непуста», и этого не хватило: правка
``retrieve.py`` (``+7`` строк) и ``evaluate.py`` (``+25``) сдвинула **55** якорей, а гейт
сказал «проблем: 0» — на новых адресах строки тоже были непустыми. Режим дрейфа отвечает
на другой вопрос: указывает ли якорь на **ту же** строку, что в ревизии ``BASE``.

Тесты гоняют настоящий CLI (копия скрипта в синтетическом репозитории ``tmp_path``), а не
внутренние функции: проверяются коды возврата и машинно-разбираемый вывод. Каждый тест
фиксирует свойство, поломка которого даёт **тихо неверный вердикт**:

* сдвиг виден и ``--strict`` даёт ``1`` — иначе гейт снова зеленеет на уехавших якорях;
* якорь на вставленный текст **не** дрейф — иначе новый якорь, добавленный вместе с кодом,
  ломал бы сборку;
* без флага поведение прежнее — на это завязан CI;
* отсутствие файла в ``BASE`` — предупреждение, а не проблема: «не с чем сравнить» ≠ «сломано»;
* пере-указанный якорь **не** дрейф: замер 16.11.2026 дал 54 таких сообщения из 64, и
  гейт с 84 % ложных срабатываний был бы выключен, а не починен;
* датированный отчёт называется отдельной категорией и код возврата не меняет: его якорь
  описывает код на дату записи, пере-указывать его запрещено;
* обрезка вывода на 50 строках при полном итоговом числе — иначе «первые 50» читаются как
  «всего 50».

Репозиторий синтетический: ``git init`` + ``commit`` с ``-c user.*`` и пустым глобальным
конфигом (``GIT_CONFIG_GLOBAL=/dev/null``), поэтому тесты не зависят от конфигурации хоста
— ни от ``user.email``, ни от ``init.defaultBranch``, ни от ``commit.gpgsign``.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "check_doc_anchors.py"

#: Базовый модуль: строка 5 — то, на что смотрит якорь в фикстуре. Строка 2 непустая
#: намеренно: после сдвига на неё попадает адрес якоря, и гейт «непустой строки» не должен
#: добавлять к отчёту о дрейфе постороннюю ошибку.
_MODULE = (
    "import os\n"
    "# заголовок\n"
    "\n"
    "def target():\n"
    "    return 'distinct-value'\n"
    "\n"
    "def other():\n"
    "    return 2\n"
)

_ANCHOR_LINE = 5


def _env() -> dict[str, str]:
    """Окружение без следов конфигурации хоста: git читает только переданное здесь."""
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update(
        {
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_SYSTEM": os.devnull,
            "GIT_AUTHOR_NAME": "coHound test",
            "GIT_AUTHOR_EMAIL": "test@example.invalid",
            "GIT_COMMITTER_NAME": "coHound test",
            "GIT_COMMITTER_EMAIL": "test@example.invalid",
        }
    )
    return env


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
        env=_env(),
    )


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _run(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(repo / "scripts" / "check_doc_anchors.py"), *args],
        cwd=repo,
        capture_output=True,
        text=True,
        env=_env(),
        timeout=120,
        check=False,
    )


@pytest.fixture()
def repo(tmp_path: Path) -> Path:
    """Репозиторий с одним коммитом: ``core/mod.py`` + документ с якорем на строку 5."""
    (tmp_path / "scripts").mkdir()
    shutil.copy2(_SCRIPT, tmp_path / "scripts" / "check_doc_anchors.py")
    _write(tmp_path / "core" / "mod.py", _MODULE)
    _write(tmp_path / "docs" / "a.md", f"Якорь: `core/mod.py:{_ANCHOR_LINE}`.\n")
    _git(tmp_path, "init")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-m", "base")
    return tmp_path


def _shift(repo: Path, lines: int = 3) -> None:
    """Вставляет строки **выше** якоря: содержание якорной строки уезжает вниз."""
    path = repo / "core" / "mod.py"
    prefix = "".join(f"# вставка {index}\n" for index in range(lines))
    path.write_text(prefix + path.read_text(encoding="utf-8"), encoding="utf-8")


def test_drift_detected_and_strict_fails(repo: Path) -> None:
    _shift(repo, 3)

    report = _run(repo, "--check-drift", "HEAD")
    assert report.returncode == 0, report.stdout + report.stderr
    assert "съехало на +3" in report.stdout
    assert f"сейчас :{_ANCHOR_LINE + 3}" in report.stdout
    assert "дрейф: 1," in report.stdout

    strict = _run(repo, "--strict", "--check-drift", "HEAD")
    assert strict.returncode == 1, strict.stdout + strict.stderr


def test_anchor_to_inserted_text_is_not_drift(repo: Path) -> None:
    """Строка 7 заменена на месте: пары в ``BASE`` нет, значит это не дрейф."""
    path = repo / "core" / "mod.py"
    text = path.read_text(encoding="utf-8").replace("def other():", "def replaced():")
    path.write_text(text, encoding="utf-8")
    # Первый якорь — прежний (строка документа не менялась), второй добавлен вместе с
    # правкой кода: именно он попадает в категорию «новый текст».
    _write(
        repo / "docs" / "a.md",
        f"Якорь: `core/mod.py:{_ANCHOR_LINE}`.\nНовый якорь: `core/mod.py:7`.\n",
    )

    report = _run(repo, "--strict", "--check-drift", "HEAD")

    assert report.returncode == 0, report.stdout + report.stderr
    assert "НОВЫЙ ТЕКСТ" in report.stdout
    assert "дрейф: 0," in report.stdout
    assert "якорей на вставленный/заменённый текст: 1" in report.stdout


def test_repointed_anchor_is_not_drift(repo: Path) -> None:
    """Номер в документе изменён вместе со сдвигом — адрес уже верный."""
    _shift(repo, 3)
    _write(repo / "docs" / "a.md", f"Якорь: `core/mod.py:{_ANCHOR_LINE + 3}`.\n")

    report = _run(repo, "--strict", "--check-drift", "HEAD")

    assert report.returncode == 0, report.stdout + report.stderr
    assert "дрейф: 0," in report.stdout
    assert "пере-указано (номер якоря изменён относительно HEAD): 1" in report.stdout


def test_without_flag_behaviour_unchanged(repo: Path) -> None:
    _shift(repo, 3)

    report = _run(repo)

    assert report.returncode == 0, report.stdout + report.stderr
    assert "ДРЕЙФ" not in report.stdout
    assert "проблем: 0" in report.stdout


def test_missing_file_in_base_is_warning(repo: Path) -> None:
    """Файл создан после коммита: сравнивать не с чем, это предупреждение."""
    _write(repo / "docs" / "b.md", "Якорь: `core/newmod.py:1`.\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "anchor before the file")
    _write(repo / "core" / "newmod.py", "value = 1\n")

    report = _run(repo, "--strict", "--check-drift", "HEAD")

    assert report.returncode == 0, report.stdout + report.stderr
    assert "файла нет в HEAD" in report.stdout
    assert "дрейф: 0," in report.stdout


def test_dated_report_drift_is_separate_category(repo: Path) -> None:
    """Датированный отчёт: дрейф называется, но пере-указанию не подлежит.

    Обе ветки: ``docs/reports/measurements/`` и корень ``docs/reports/``. План от 30.10
    датирован по той же причине, что и замер — его якорь описывает дерево на дату.
    """
    _write(repo / "docs" / "a.md", "Документ без якорей.\n")
    _write(
        repo / "docs" / "reports" / "measurements" / "2026-01-01-probe.md",
        f"Якорь: `core/mod.py:{_ANCHOR_LINE}`.\n",
    )
    _write(
        repo / "docs" / "reports" / "2026-01-02-plan.md",
        f"Якорь: `core/mod.py:{_ANCHOR_LINE}`.\n",
    )
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "dated reports")
    _shift(repo, 3)

    report = _run(repo, "--strict", "--check-drift", "HEAD")

    assert report.returncode == 0, report.stdout + report.stderr
    assert "ДРЕЙФ (датированный отчёт, пере-указанию не подлежит)" in report.stdout
    assert "measurements/2026-01-01-probe.md:1" in report.stdout
    assert "reports/2026-01-02-plan.md:1" in report.stdout
    assert "дрейф: 0," in report.stdout
    assert "дрейф в датированных отчётах: 2" in report.stdout


def test_ambiguous_basename_is_warning_not_drift(repo: Path) -> None:
    """Два файла с одним именем: якорь голым именем не сравнивается ни с одним.

    Выбор «первого кандидата» для «строка непуста» безобиден, для дрейфа — нет: сравнение
    с чужим файлом даёт ложный вердикт. Поэтому неоднозначность — предупреждение, а дрейф
    по такому якорю не считается.
    """
    _write(repo / "core" / "dup.py", "first = 1\n")
    _write(repo / "core" / "nested" / "dup.py", "second = 2\n")
    _write(repo / "docs" / "a.md", "Якорь: `dup.py:1`.\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "ambiguous basename")
    _write(repo / "core" / "nested" / "dup.py", "# сдвиг\nsecond = 2\n")

    report = _run(repo, "--strict", "--check-drift", "HEAD")

    assert report.returncode == 0, report.stdout + report.stderr
    assert "неоднозначно — dup.py (2 кандидатов); дрейф не считается" in report.stdout
    assert "дрейф: 0," in report.stdout


def test_deleted_file_anchor_is_warning_not_error(repo: Path) -> None:
    """Удалённый файл: доказательство описывает прошлое, живой адрес ему не нужен.

    Пин обеих сторон границы: голое имя без сегмента из ``REPO_ROOTS`` — предупреждение
    (так пишут датированную запись), а полный путь с ``core/`` — ошибка (так пишут якорь на
    живой код, и его отсутствие — сгнивший якорь). Граница по первому сегменту названа в
    docstring, а не выведена из «файла нет».
    """
    _write(
        repo / "docs" / "a.md",
        "Доказательство: `util/fusion.py:102` (удалён `aa4185b`).\n",
    )

    report = _run(repo, "--strict")

    assert report.returncode == 0, report.stdout + report.stderr
    assert "вне репозитория, не проверить — util/fusion.py" in report.stdout
    assert "проблем: 0" in report.stdout


def test_deleted_file_with_repo_root_prefix_is_error(repo: Path) -> None:
    """Тот же удалённый файл, но полным путём: ``core/`` — корень репозитория ⇒ ошибка."""
    _write(repo / "docs" / "a.md", "Якорь: `core/util/fusion.py:102`.\n")

    report = _run(repo, "--strict")

    assert report.returncode == 1, report.stdout + report.stderr
    assert "ОШИБКА  docs/a.md:1: файла нет — core/util/fusion.py" in report.stdout


def test_output_truncated_at_50_lines_with_full_count(repo: Path) -> None:
    """60 сдвинутых якорей: 50 строк вывода, итог — полный."""
    module = "".join(f"value_{index} = {index}\n" for index in range(1, 61))
    _write(repo / "core" / "mod.py", module)
    _write(
        repo / "docs" / "a.md",
        "".join(f"`core/mod.py:{index}`\n" for index in range(1, 61)),
    )
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "many anchors")
    _shift(repo, 5)

    report = _run(repo, "--check-drift", "HEAD")

    assert report.returncode == 0, report.stdout + report.stderr
    assert report.stdout.count("ДРЕЙФ  ") == 50
    assert "… ещё 10" in report.stdout
    assert "дрейф: 60," in report.stdout


def test_unknown_rev_is_usage_error(repo: Path) -> None:
    report = _run(repo, "--check-drift", "no-such-rev")

    assert report.returncode == 2, report.stdout + report.stderr
    assert "ревизия не разрешается" in report.stdout
