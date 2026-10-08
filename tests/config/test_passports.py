"""Тесты загрузчика паспортов продуктов (W5, §4.3).

Проверяют: выбор свежайшего паспорта по дате в имени файла (тай-брейк), парсинг
``upstream``/``satellites``, безопасный дефолт при отсутствии каталога, отсутствие
паспорта → ``None``, негативные случаи (некорректный YAML, несоответствие схеме),
вывод URL-кандидатов из ``doc_domains`` (схема/схема-less, «только кандидаты»),
детерминизм и категорийную нейтральность модуля (§1.3).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from core.config.passports import (
    PassportLoadError,
    latest_passport_path,
    load_passport,
    load_passports,
    passport_urls,
)
from core.domain.models import Passport

FIXTURES = Path(__file__).parent / "fixtures"
PASSPORTS_FIXTURE = FIXTURES / "passports"
MODULE_PATH = Path(__file__).resolve().parents[2] / "core" / "config" / "passports.py"


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _base(**overrides: Any) -> dict[str, Any]:
    """Возвращает минимальный валидный паспорт (§4.3) с переопределениями."""
    data: dict[str, Any] = {
        "product_id": "p1",
        "canonical_name": "Product One",
        "vendor": "Vendor One",
        "upstream": None,
        "edition_assessed": "Standard",
        "version": "1.0",
        "jurisdiction": "XX",
        "doc_domains": ["example.com"],
        "satellites": [],
        "snapshot_date": "2026-01-01",
    }
    data.update(overrides)
    return data


def _write_passport(root: Path, product_id: str, filename: str, data: dict[str, Any]) -> Path:
    """Пишет паспортный YAML в ``root/{product_id}/{filename}`` и возвращает путь."""
    product_dir = root / product_id
    product_dir.mkdir(parents=True, exist_ok=True)
    path = product_dir / filename
    path.write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Happy-path и фикстура
# ---------------------------------------------------------------------------


def test_load_fixture_passport() -> None:
    """Минимальная фикстура ``p1/2026-01-01.yaml`` парсится в ``Passport``."""
    passport = load_passport("p1", PASSPORTS_FIXTURE)
    assert passport is not None
    assert isinstance(passport, Passport)
    assert passport.product_id == "p1"
    assert passport.canonical_name == "Product One"
    assert passport.vendor == "Vendor One"
    assert passport.upstream is None
    assert passport.jurisdiction == "XX"
    assert passport.doc_domains == ["example.com"]
    assert passport.satellites == []
    assert passport.snapshot_date.isoformat() == "2026-01-01"


def test_load_passports_from_fixture_dir() -> None:
    """``load_passports`` обходит подкаталоги и возвращает словарь product_id → Passport."""
    result = load_passports(PASSPORTS_FIXTURE)
    assert set(result) == {"p1"}
    assert result["p1"].product_id == "p1"


# ---------------------------------------------------------------------------
# Выбор свежайшего паспорта и тай-брейк
# ---------------------------------------------------------------------------


def test_latest_by_date_selected(tmp_path: Path) -> None:
    """Выбирается паспорт с наибольшей датой в имени файла, а не первый по алфавиту/записи."""
    _write_passport(
        tmp_path, "p1", "2026-01-01.yaml", _base(version="1.0", snapshot_date="2026-01-01")
    )
    _write_passport(
        tmp_path, "p1", "2026-03-15.yaml", _base(version="2.0", snapshot_date="2026-03-15")
    )
    _write_passport(
        tmp_path, "p1", "2025-12-31.yaml", _base(version="0.9", snapshot_date="2025-12-31")
    )
    passport = load_passport("p1", tmp_path)
    assert passport is not None
    assert passport.version == "2.0"
    assert passport.snapshot_date.isoformat() == "2026-03-15"


def test_latest_passport_path_matches_selection(tmp_path: Path) -> None:
    """``latest_passport_path`` возвращает путь того же файла, что и ``load_passport``."""
    _write_passport(tmp_path, "p1", "2026-01-01.yaml", _base())
    newest = _write_passport(tmp_path, "p1", "2026-06-01.yaml", _base())
    assert latest_passport_path("p1", tmp_path) == newest


def test_non_passport_filenames_ignored(tmp_path: Path) -> None:
    """Файлы, не соответствующие ``YYYY-MM-DD.yaml``, не считаются паспортами."""
    _write_passport(tmp_path, "p1", "notes.yaml", _base(version="ignored"))
    _write_passport(tmp_path, "p1", "2026-01-01.yaml", _base(version="1.0"))
    passport = load_passport("p1", tmp_path)
    assert passport is not None
    assert passport.version == "1.0"


def test_product_dir_without_dated_file_is_skipped(tmp_path: Path) -> None:
    """Продукт без паспортного файла пропускается в ``load_passports``."""
    (tmp_path / "p1").mkdir()
    (tmp_path / "p1" / "readme.txt").write_text("no passport", encoding="utf-8")
    assert load_passports(tmp_path) == {}


def test_non_directory_entries_ignored(tmp_path: Path) -> None:
    """Файлы в корне ``passports_dir`` игнорируются (продукт — только подкаталог)."""
    (tmp_path / "loose.yaml").write_text(yaml.safe_dump(_base()), encoding="utf-8")
    assert load_passports(tmp_path) == {}


# ---------------------------------------------------------------------------
# Отсутствие данных → None / {}
# ---------------------------------------------------------------------------


def test_missing_dir_returns_empty_dict(tmp_path: Path) -> None:
    """Отсутствующий каталог паспортов → ``{}`` (безопасный дефолт)."""
    assert load_passports(tmp_path / "does_not_exist") == {}


def test_absent_product_returns_none(tmp_path: Path) -> None:
    """Отсутствующий продукт → ``None``."""
    assert load_passport("ghost", tmp_path) is None
    assert latest_passport_path("ghost", tmp_path) is None


# ---------------------------------------------------------------------------
# Разбор upstream и satellites
# ---------------------------------------------------------------------------


def test_upstream_and_satellites_parsed(tmp_path: Path) -> None:
    """``upstream`` и оба состояния сателлита (approved / not_found) парсятся."""
    data = _base(
        upstream={
            "baseline_id": "baseline-x",
            "version": "16",
            "evidence": {"url": "https://upstream.example/spec", "quote": "feature present"},
        },
        satellites=[
            {
                "role": "backup",
                "state": "approved",
                "component": "Component X",
                "origin": "third_party",
                "relationship": "bundled",
                "evidence_state": "verified",
                "relationship_evidence": {
                    "url": "https://vendor.example/backup",
                    "quote": "bundled with the platform",
                },
                "rejected_candidates": [{"name": "Other", "reason": "no evidence"}],
            },
            {"role": "management_console", "state": "not_found"},
        ],
    )
    _write_passport(tmp_path, "p1", "2026-01-01.yaml", data)
    passport = load_passport("p1", tmp_path)
    assert passport is not None

    assert passport.upstream is not None
    assert passport.upstream.baseline_id == "baseline-x"
    assert passport.upstream.version == "16"
    assert passport.upstream.evidence.url == "https://upstream.example/spec"

    assert len(passport.satellites) == 2
    approved, not_found = passport.satellites
    assert approved.role == "backup"
    assert approved.state.value == "approved"
    assert approved.component == "Component X"
    assert approved.origin is not None and approved.origin.value == "third_party"
    assert approved.relationship is not None and approved.relationship.value == "bundled"
    assert approved.evidence_state is not None and approved.evidence_state.value == "verified"
    assert approved.relationship_evidence is not None
    assert approved.rejected_candidates == [{"name": "Other", "reason": "no evidence"}]
    assert not_found.role == "management_console"
    assert not_found.state.value == "not_found"


# ---------------------------------------------------------------------------
# Негативные случаи
# ---------------------------------------------------------------------------


def test_malformed_yaml_raises_with_filename(tmp_path: Path) -> None:
    """Некорректный YAML → ``PassportLoadError``, сообщение называет файл."""
    path = tmp_path / "p1" / "2026-01-01.yaml"
    path.parent.mkdir(parents=True)
    path.write_text("product_id: [unclosed\n", encoding="utf-8")
    with pytest.raises(PassportLoadError, match="2026-01-01.yaml"):
        load_passport("p1", tmp_path)


def test_invalid_schema_raises_with_filename(tmp_path: Path) -> None:
    """Несоответствие схеме (лишнее поле, extra=forbid) → ``PassportLoadError``."""
    path = _write_passport(tmp_path, "p1", "2026-01-01.yaml", _base(unexpected=1))
    with pytest.raises(PassportLoadError, match=path.name):
        load_passport("p1", tmp_path)


def test_invalid_enum_raises(tmp_path: Path) -> None:
    """Недопустимое значение перечисления сателлита → ``PassportLoadError``."""
    data = _base(satellites=[{"role": "backup", "state": "not_found", "origin": "alien"}])
    _write_passport(tmp_path, "p1", "2026-01-01.yaml", data)
    with pytest.raises(PassportLoadError):
        load_passport("p1", tmp_path)


def test_empty_document_raises(tmp_path: Path) -> None:
    """Пустой документ → ``PassportLoadError``."""
    path = tmp_path / "p1" / "2026-01-01.yaml"
    path.parent.mkdir(parents=True)
    path.write_text("# только комментарий\n", encoding="utf-8")
    with pytest.raises(PassportLoadError, match="пустой"):
        load_passport("p1", tmp_path)


# ---------------------------------------------------------------------------
# URL-кандидаты из doc_domains
# ---------------------------------------------------------------------------


def test_passport_urls_schemeless_gets_https() -> None:
    """Схема-less домен → ``https://{domain}/``."""
    passport = Passport.model_validate(_base(doc_domains=["example.com"]))
    assert passport_urls(passport) == ["https://example.com/"]


def test_passport_urls_keeps_existing_scheme() -> None:
    """Запись со схемой сохраняется как есть (без достройки путей)."""
    passport = Passport.model_validate(
        _base(doc_domains=["http://docs.example.com/base", "https://other.example/x"])
    )
    assert passport_urls(passport) == [
        "http://docs.example.com/base",
        "https://other.example/x",
    ]


def test_passport_urls_preserves_order_and_dedupes() -> None:
    """Порядок ``doc_domains`` сохраняется; дубликаты схлопываются."""
    passport = Passport.model_validate(
        _base(doc_domains=["b.example", "a.example", "b.example"])
    )
    assert passport_urls(passport) == ["https://b.example/", "https://a.example/"]


def test_passport_urls_empty_when_no_domains() -> None:
    """Без ``doc_domains`` кандидатов нет."""
    passport = Passport.model_validate(_base(doc_domains=[]))
    assert passport_urls(passport) == []


def test_passport_urls_from_fixture() -> None:
    """Кандидаты выводятся и для загруженной фикстуры."""
    passport = load_passport("p1", PASSPORTS_FIXTURE)
    assert passport is not None
    assert passport_urls(passport) == ["https://example.com/"]


# ---------------------------------------------------------------------------
# Детерминизм
# ---------------------------------------------------------------------------


def test_loading_is_deterministic(tmp_path: Path) -> None:
    """Повторная загрузка даёт равные модели и одинаковый порядок ключей."""
    _write_passport(tmp_path, "p2", "2026-01-01.yaml", _base(product_id="p2"))
    _write_passport(tmp_path, "p1", "2026-01-01.yaml", _base(product_id="p1"))
    first = load_passports(tmp_path)
    second = load_passports(tmp_path)
    assert list(first) == ["p1", "p2"]
    assert first == second
    assert first["p1"].model_dump() == second["p1"].model_dump()


# ---------------------------------------------------------------------------
# Нейтральность (§1.3)
# ---------------------------------------------------------------------------


def test_module_is_category_neutral() -> None:
    """Исходник модуля не содержит предметных/вендорских терминов."""
    source = MODULE_PATH.read_text(encoding="utf-8").lower()
    forbidden = ("postgres", "pangolin", "sber", "сбер", "copywala", "платформа")
    for token in forbidden:
        assert token not in source, f"нейтральность §1.3 нарушена: «{token}»"
