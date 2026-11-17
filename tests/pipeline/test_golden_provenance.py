"""Разбор ``sources/provenance.yaml`` — происхождение эталона (:mod:`core.pipeline.golden_status`).

Предмет — **второй объявленный файл состояния эталона**: ``golden_status.yaml`` объявляет,
годится ли набор, а ``provenance.yaml`` — откуда он взялся (источники, их отпечатки, дата
сборки). Число строк без даты читается как «сегодняшний эталон», поэтому дата обязана быть
разбираемой и видимой.

Проверяется:

* файла нет → ``None`` (у пакета может не быть объявленного происхождения — это не ошибка);
* корректный файл: порядок источников сохранён, ``snapshot_date`` разобран;
* строгость: неизвестный ключ, битая дата, битый ``sha256``, пустой ``sources`` — видимая
  ошибка пакета с ведущим ``provenance.yaml:``;
* ``generated_at`` — дата **сборки пакета**, а ``snapshot_date`` — дата **состояния
  источника**; совпадение значений не делает их одним фактом
  (:attr:`GoldenProvenance.distinct_snapshot_date`);
* реальный ``packages/ru-pg-dbms/sources/provenance.yaml`` грузится и даёт ``2026-10-28``
  (проверка объявленного входа, а не фикстуры).

Категорийно-нейтрально (§1.3): только ``tmp_path`` и общие имена файлов.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.pipeline.golden_status import (
    GoldenProvenance,
    provenance_path,
    read_provenance,
)

ROOT = Path(__file__).resolve().parents[2]

SHA = "6c7edf0d8219640bc5fe8d5e1d75a29fa0fb9973349ddad817ef677f0d95d183"

_HEADER = "case_id: case_x\n"

_BODY = (
    "generated_at: '2026-10-28T00:00:00Z'\n"
    "generator: build.py\n"
    "snapshot_date: '2026-10-28'\n"
    "sources:\n"
    f"- path: /tmp/golden_00.xlsx\n"
    "  role: сравнительная таблица\n"
    f"  sha256: {SHA}\n"
    "  sheet: Лист1\n"
)


def _write(tmp_path: Path, text: str) -> Path:
    """Пишет ``sources/provenance.yaml`` в пакет ``tmp_path/package``."""
    path = provenance_path(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Отсутствие файла — не ошибка
# ---------------------------------------------------------------------------


def test_missing_file_is_none(tmp_path: Path) -> None:
    """Файла нет → ``None``: выводить из отсутствия состояние запрещено."""
    assert read_provenance(tmp_path) is None


def test_empty_file_is_error(tmp_path: Path) -> None:
    """Пустой файл — незаполненное объявление, а не «нет объявления»."""
    _write(tmp_path, "# только комментарий\n")

    with pytest.raises(ValueError) as excinfo:
        read_provenance(tmp_path)

    assert str(excinfo.value).startswith("provenance.yaml:")


# ---------------------------------------------------------------------------
# Корректный файл
# ---------------------------------------------------------------------------


def test_loads_snapshot_and_preserves_source_order(tmp_path: Path) -> None:
    """Порядок ``sources`` значим: первый — тот, из которого собран эталон."""
    _write(
        tmp_path,
        _HEADER
        + _BODY.replace(
            "  sheet: Лист1\n",
            "  sheet: Лист1\n"
            "- path: /tmp/second.xlsx\n"
            "  role: второй\n"
            f"  sha256: '{'0' * 64}'\n",
        ),
    )

    provenance = read_provenance(tmp_path)

    assert provenance is not None
    assert provenance.case_id == "case_x"
    assert provenance.snapshot_date == "2026-10-28"
    assert [entry.name for entry in provenance.sources] == [
        "golden_00.xlsx",
        "second.xlsx",
    ]
    assert provenance.primary_source is not None
    assert provenance.primary_source.sha256 == SHA
    assert provenance.primary_source.sheet == "Лист1"


def test_generated_at_keeps_date_only(tmp_path: Path) -> None:
    """``generated_at`` — момент сборки; в вывод идёт дата (время — не факт об эталоне)."""
    _write(tmp_path, _HEADER + _BODY)

    provenance = read_provenance(tmp_path)

    assert provenance is not None
    assert provenance.generated_at == "2026-10-28"


def test_snapshot_equal_to_generated_at_is_not_a_distinct_fact(tmp_path: Path) -> None:
    """Совпадение дат — «одна дата объявлена дважды», а не дата источника."""
    _write(tmp_path, _HEADER + _BODY)

    provenance = read_provenance(tmp_path)

    assert provenance is not None
    assert provenance.snapshot_date == provenance.generated_at
    assert provenance.distinct_snapshot_date is None


def test_distinct_snapshot_is_published(tmp_path: Path) -> None:
    """Отличающаяся дата источника объявлена отдельно — и видна как отдельный факт."""
    _write(tmp_path, _HEADER + _BODY.replace("'2026-10-28'", "'2026-10-01'"))

    provenance = read_provenance(tmp_path)

    assert provenance is not None
    assert provenance.generated_at == "2026-10-28"
    assert provenance.distinct_snapshot_date == "2026-10-01"


def test_superseded_sources_need_no_sha256(tmp_path: Path) -> None:
    """Прежний источник объявляется без отпечатка: он не то, из чего собран эталон."""
    _write(
        tmp_path,
        _HEADER
        + _BODY
        + "superseded_sources:\n"
        "- path: /tmp/old.xlsx\n"
        "  role: прежний набор\n"
        "  sheet: data\n",
    )

    provenance = read_provenance(tmp_path)

    assert provenance is not None
    assert [entry.name for entry in provenance.superseded_sources] == ["old.xlsx"]
    assert provenance.superseded_sources[0].sha256 is None


def test_optional_fields_may_be_absent(tmp_path: Path) -> None:
    """``generated_at``/``generator``/``snapshot_date`` опциональны — ``None``, не выдумка."""
    _write(tmp_path, "case_id: case_x\nsources:\n- path: /tmp/a.xlsx\n" f"  sha256: {SHA}\n")

    provenance = read_provenance(tmp_path)

    assert provenance is not None
    assert provenance.generated_at is None
    assert provenance.generator is None
    assert provenance.snapshot_date is None
    assert provenance.distinct_snapshot_date is None


# ---------------------------------------------------------------------------
# Строгость: объявление с опечаткой хуже отсутствующего
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "fragment"),
    [
        (
            _HEADER + _BODY + "snapshot_dates: '2026-10-28'\n",
            "неизвестные ключи",
        ),
        (
            _HEADER + _BODY.replace("'2026-10-28'\n", "'28.10.2026'\n"),
            "не является датой ISO-8601",
        ),
        (
            _HEADER + _BODY.replace("'2026-10-28T00:00:00Z'", "'вчера'"),
            "не является датой ISO-8601",
        ),
        (
            _HEADER + _BODY.replace(SHA, "6c7edf0d"),
            "не является отпечатком sha256",
        ),
        (
            _HEADER + _BODY.replace(f"  sha256: {SHA}\n", ""),
            "требует sha256",
        ),
        (
            _HEADER + "sources: []\n",
            "sources обязателен и непуст",
        ),
        (
            "sources:\n- path: /tmp/a.xlsx\n  sha256: " + SHA + "\n",
            "case_id обязателен",
        ),
        (
            _HEADER
            + "sources:\n- role: сравнительная таблица\n"
            + f"  sha256: {SHA}\n",
            "требует path",
        ),
        (
            _HEADER + _BODY + "sources_extra: []\n",
            "неизвестные ключи",
        ),
    ],
)
def test_broken_declaration_is_visible_error(
    tmp_path: Path, text: str, fragment: str
) -> None:
    """Опечатка в объявлении — ``ValueError`` с ведущим ``provenance.yaml:``."""
    _write(tmp_path, text)

    with pytest.raises(ValueError) as excinfo:
        read_provenance(tmp_path)

    message = str(excinfo.value)
    assert message.startswith("provenance.yaml:")
    assert fragment in message


def test_unknown_key_inside_source_is_error(tmp_path: Path) -> None:
    """Запись источника строгая так же, как объявление: опечатка не пропускается."""
    _write(tmp_path, _HEADER + _BODY.replace("  sheet: Лист1\n", "  sheets: Лист1\n"))

    with pytest.raises(ValueError) as excinfo:
        read_provenance(tmp_path)

    assert str(excinfo.value).startswith("provenance.yaml:")
    assert "sources[1]" in str(excinfo.value)


def test_non_mapping_document_is_error(tmp_path: Path) -> None:
    """YAML-список вместо словаря — ошибка формы, а не «пустое объявление»."""
    _write(tmp_path, "- case_id: case_x\n")

    with pytest.raises(ValueError) as excinfo:
        read_provenance(tmp_path)

    assert "ожидался YAML-словарь" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Объявленный вход реального пакета, а не фикстура
# ---------------------------------------------------------------------------


def test_real_package_declares_snapshot_date() -> None:
    """``packages/ru-pg-dbms/sources/provenance.yaml`` разбирается и даёт 2026-10-01."""
    provenance: GoldenProvenance | None = read_provenance(ROOT / "packages" / "ru-pg-dbms")

    assert provenance is not None
    assert provenance.case_id == "ru-pg-dbms"
    assert provenance.snapshot_date == "2026-10-01"
    assert provenance.generated_at == "2026-10-28"
    # Даты источника и сборки **разные** — иначе читатель принимал бы оценку от 01.10
    # за текущую (решение владельца 17.11.2026: дата таблицы — 2026-10-01).
    assert provenance.distinct_snapshot_date == "2026-10-01"
    assert provenance.primary_source is not None
    assert provenance.primary_source.name == "golden_00.xlsx"
    assert provenance.primary_source.sha256 == SHA


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
