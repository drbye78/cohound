"""Пины прибора ``scripts/derive_llms_seeds.py`` — вывод seed-набора из индекса вендора.

Зачем прибор вообще существует (измерено 22.11.2026 на GaussDB): HTML-страница сайта
несёт только каркас (77 578 байт разметки, ~1 900 символов текста, ни одного абзаца
документации), а markdown-представление отдаёт текст. Обход по `<a href>` собрал бы
навигацию, поэтому набор seed'ов берётся из машинного индекса вендора (`llms.txt`) и
становится **статическим** входом пакета: `plan` считает `run_id` без сети.

Каждый тест фиксирует свойство, поломка которого даёт **тихо неверный корпус**:

* неизвестное имя раздела — отказ (код 2), а не пропуск: переименование раздела у
  вендора иначе уменьшило бы корпус, не изменив ни одного объявленного числа;
* `--section-match` и точные имена складываются, дубликаты схлопываются — иначе один
  документ оплачивался бы дважды;
* перегенерация **сохраняет авторскую шапку** блока: причина среза живёт в комментарии,
  и повторный запуск не вправе оставить числа без причины;
* перегенерация не трогает соседние продукты (их блоки и комментарии) — прибор правит
  один блок, а не файл;
* URL с учётными данными отвергается: seed — адрес, а не пропуск.

Тесты гоняют настоящий CLI на синтетическом индексе (`tmp_path`), сеть не нужна:
`--llms-file` читает локальную копию.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "derive_llms_seeds.py"

_INDEX = """# Индекс продукта

## 产品介绍
- [什么是产品](https://docs.example.com/pd/pd_01.md): описание
- [架构](https://docs.example.com/pd/pd_02.md): схема

## 开发指南（V2.0-10.x）
- [指南首页](https://docs.example.com/dev10/index.md): вход
- [SQL](https://docs.example.com/dev10/sql.md): синтаксис

## 开发指南（V2.0-8.x）
- [指南首页](https://docs.example.com/dev8/index.md): старая редакция

## 计费说明
- [价格](https://docs.example.com/price/p1.md): деньги
"""

_CORPUS_URLS = """# Авторский вход.
existing:
# Шапка существующего продукта — её прибор трогать не вправе.
- https://existing.example.com/docs/

gaussdb:
# Авторская шапка блока: почему срез такой.
- https://docs.example.com/old.md

tail:
- https://tail.example.com/
"""


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 — объявленный путь скрипта репозитория
        [sys.executable, str(SCRIPT), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def _index(tmp_path: Path) -> Path:
    path = tmp_path / "llms.txt"
    path.write_text(_INDEX, encoding="utf-8")
    return path


def _package(tmp_path: Path) -> Path:
    path = tmp_path / "corpus_urls.yaml"
    path.write_text(_CORPUS_URLS, encoding="utf-8")
    return path


def test_list_prints_section_census(tmp_path: Path) -> None:
    """``--list`` печатает перепись: «сколько всего» и «сколько в разделе» — разные числа."""
    result = _run("--llms-file", str(_index(tmp_path)), "--product", "gaussdb", "--list")
    assert result.returncode == 0, result.stderr
    assert "разделов: 4, документов: 6" in result.stdout
    assert "2  开发指南（V2.0-10.x）" in result.stdout
    assert "1  计费说明" in result.stdout


def test_unknown_section_is_refused_not_skipped(tmp_path: Path) -> None:
    """Переименованный раздел — отказ: иначе корпус тихо уменьшился бы."""
    result = _run(
        "--llms-file",
        str(_index(tmp_path)),
        "--product",
        "gaussdb",
        "--section",
        "开发指南（V2.0-11.x）",
    )
    assert result.returncode == 2
    assert "НЕИЗВЕСТНЫЕ разделы" in result.stderr


def test_sections_and_matches_compose_without_duplicates(tmp_path: Path) -> None:
    """Точные имена + подстрока складываются; дубликат оплачивается один раз."""
    result = _run(
        "--llms-file",
        str(_index(tmp_path)),
        "--product",
        "gaussdb",
        "--section",
        "产品介绍",
        "--section-match",
        "10.x",
        "--section-match",
        "10.x",
    )
    assert result.returncode == 0, result.stderr
    assert "итого: 4 уникальных" in result.stdout
    assert "снято дубликатов: 0" in result.stdout


def test_write_preserves_authored_header_and_other_products(tmp_path: Path) -> None:
    """Перегенерация меняет **только список**: шапка блока и соседи остаются байт-в-байт."""
    package = _package(tmp_path)
    result = _run(
        "--llms-file",
        str(_index(tmp_path)),
        "--product",
        "gaussdb",
        "--section",
        "产品介绍",
        "--write",
        str(package),
    )
    assert result.returncode == 0, result.stderr
    text = package.read_text(encoding="utf-8")
    assert "# Авторская шапка блока: почему срез такой." in text
    assert "- https://docs.example.com/pd/pd_01.md" in text
    assert "- https://docs.example.com/pd/pd_02.md" in text
    # Старый адрес снят: блок заменён целиком, а не дополнен.
    assert "old.md" not in text
    # Соседи не тронуты.
    assert "# Шапка существующего продукта — её прибор трогать не вправе." in text
    assert "- https://existing.example.com/docs/" in text
    assert "tail:\n- https://tail.example.com/" in text


def test_write_is_idempotent(tmp_path: Path) -> None:
    """Повторная запись тех же разделов не меняет файл — иначе diff шумел бы каждым запуском."""
    package = _package(tmp_path)
    args = (
        "--llms-file",
        str(_index(tmp_path)),
        "--product",
        "gaussdb",
        "--section",
        "产品介绍",
        "--write",
        str(package),
    )
    assert _run(*args).returncode == 0
    first = package.read_text(encoding="utf-8")
    assert _run(*args).returncode == 0
    assert package.read_text(encoding="utf-8") == first


def test_credentials_in_url_are_refused(tmp_path: Path) -> None:
    """Seed — адрес, а не пропуск: URL с учётными данными отвергается."""
    index = tmp_path / "llms.txt"
    index.write_text(
        "## 产品介绍\n- [x](https://user:pass@docs.example.com/pd_01.md)\n",
        encoding="utf-8",
    )
    result = _run(
        "--llms-file", str(index), "--product", "gaussdb", "--section", "产品介绍"
    )
    assert result.returncode == 2
    assert "непригодных URL" in result.stderr


def test_broken_vendor_link_is_excluded_and_counted(tmp_path: Path) -> None:
    """Битая ссылка индекса (в пути — целый URL) исключается, но счёт публикуется.

    Измерено 22.11.2026 на GaussDB: одна такая ссылка из 15 235 — `[ ](https://host/
    section/https://other.host/page?…)`. Взять её значит получить 404, корень с
    `complete=0` и потерять доказуемость отсутствия у всего продукта (I2d). Промолчать
    значит объявить корпус меньше, чем он есть, не изменив ни одного числа.
    """
    index = tmp_path / "llms.txt"
    index.write_text(
        "## 产品介绍\n"
        "- [x](https://docs.example.com/pd/pd_01.md)\n"
        "- [](https://docs.example.com/scene/https://other.example.com/page?p=1)\n",
        encoding="utf-8",
    )
    package = _package(tmp_path)
    result = _run(
        "--llms-file",
        str(index),
        "--product",
        "gaussdb",
        "--section",
        "产品介绍",
        "--write",
        str(package),
    )
    assert result.returncode == 0, result.stderr
    assert "итого: 1 уникальных (снято дубликатов: 0, исключено битых ссылок: 1)" in (
        result.stdout
    )
    assert "битые ссылки индекса" in result.stderr
    text = package.read_text(encoding="utf-8")
    assert "- https://docs.example.com/pd/pd_01.md" in text
    assert "other.example.com" not in text
