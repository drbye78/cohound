"""Тесты :mod:`core.pipeline.drain_plan` — план докачки усечённых корней.

Проверяется то, что дороже всего ошибить молча: чтение **записанного** провенанса
(а не пересчёт), арифметика объявленного предела, подъём глубины только по своей
причине и правка ``domain_profiles.yaml`` без потери шапки и комментариев.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from core.pipeline.drain_plan import (
    DrainedRoot,
    DrainUpdate,
    MeasuredRoot,
    declared_depth,
    declared_limit,
    is_drainable,
    matches_profile_key,
    measured_roots,
    plan_updates,
    render_profile_limits,
    resolve_profile_key,
    root_key,
    truncated_roots,
)

ROOT = Path(__file__).resolve().parents[2]
ON_DISK_PROFILES = ROOT / "packages" / "ru-pg-dbms" / "domain_profiles.yaml"

_HEADER = """# Шапка: обоснование живых замеров, её терять нельзя.
# Вторая строка шапки.

"""

_PROFILES = """\
postgrespro.ru/docs:
  # Потолок объявлен по замеру 13.11.2026 (18 939, truncated: false).
  max_depth: 8
  max_documents: 20000  # замер: 18 939 документов
  fetch: http
jatoba.ru:
  max_depth: 2
  max_documents: 50
"""


# --------------------------------------------------------------------------- #
# root_key
# --------------------------------------------------------------------------- #


def test_root_key_strips_scheme_and_trailing_slash() -> None:
    assert root_key("https://postgrespro.ru/docs/") == "postgrespro.ru/docs"
    assert root_key("https://postgrespro.ru/docs") == "postgrespro.ru/docs"


def test_root_key_host_only() -> None:
    assert root_key("https://jatoba.ru/") == "jatoba.ru"
    assert root_key("http://jatoba.ru") == "jatoba.ru"


def test_root_key_keeps_www_prefix() -> None:
    # Ключ профиля — это **объявленный** хост: ``www.gaz-is.ru`` в пилоте объявлен
    # отдельным ключом, поэтому срезать ``www.`` нельзя (иначе правка уйдёт не в тот блок).
    assert root_key("https://www.jatoba.ru/") == "www.jatoba.ru"


def test_root_key_rejects_hostless() -> None:
    with pytest.raises(ValueError):
        root_key("/docs/18")


# --------------------------------------------------------------------------- #
# matches_profile_key / resolve_profile_key
# --------------------------------------------------------------------------- #


def test_matches_profile_key_host_wide_inherits_subdomain() -> None:
    assert matches_profile_key("https://example.org/", "example.org")
    assert matches_profile_key("https://www.example.org/x", "example.org")
    assert not matches_profile_key("https://example.org.evil.test/", "example.org")


def test_matches_profile_key_path_on_segment_boundary() -> None:
    assert matches_profile_key("https://e.test/docs/", "e.test/docs")
    assert matches_profile_key("https://e.test/docs/18/page", "e.test/docs")
    assert not matches_profile_key("https://e.test/docs-old/page", "e.test/docs")


def test_resolve_profile_key_prefers_longest_path() -> None:
    keys = ["postgrespro.ru", "postgrespro.ru/docs", "postgrespro.ru/products"]
    assert (
        resolve_profile_key("https://postgrespro.ru/docs/18/x", keys)
        == "postgrespro.ru/docs"
    )
    assert resolve_profile_key("https://postgrespro.ru/other", keys) == "postgrespro.ru"


def test_resolve_profile_key_returns_none_when_uncovered() -> None:
    assert resolve_profile_key("https://nope.test/", ["other.test"]) is None


# --------------------------------------------------------------------------- #
# truncated_roots
# --------------------------------------------------------------------------- #


def _provenance(*records: dict[str, object]) -> dict[str, object]:
    return {"truncated": True, "effective_limits": {"roots": list(records)}}


def test_truncated_roots_reads_recorded_facts() -> None:
    prov = _provenance(
        {
            "product_id": "pg",
            "root": "https://a.test/docs/",
            "truncated": True,
            "truncated_reason": "max_documents",
            "pages_fetched": 20000,
            "queue_remaining": 1234,
            "limits": {"max_depth": 8, "max_documents": 20000},
        }
    )
    (root,) = truncated_roots(prov, errors=[])
    assert root.product_id == "pg"
    assert root.key == "a.test/docs"
    assert root.reason == "max_documents"
    assert root.pages_fetched == 20000
    assert root.queue_remaining == 1234
    assert root.max_depth == 8
    assert root.max_documents == 20000


def test_truncated_roots_skips_complete_roots() -> None:
    prov = _provenance(
        {
            "product_id": "pg",
            "root": "https://a.test/docs/",
            "truncated": False,
            "truncated_reason": None,
            "pages_fetched": 10,
            "limits": {"max_depth": 8, "max_documents": 20000},
        }
    )
    assert truncated_roots(prov, errors=[]) == []


def test_truncated_roots_sorted_independently_of_completion_order() -> None:
    prov = _provenance(
        {
            "product_id": "b",
            "root": "https://z.test/",
            "truncated": True,
            "truncated_reason": "max_documents",
            "pages_fetched": 1,
            "limits": {},
        },
        {
            "product_id": "a",
            "root": "https://y.test/",
            "truncated": True,
            "truncated_reason": "max_depth",
            "pages_fetched": 2,
            "limits": {"max_depth": 3},
        },
    )
    assert [
        (item.product_id, item.root_url) for item in truncated_roots(prov, errors=[])
    ] == [
        ("a", "https://y.test/"),
        ("b", "https://z.test/"),
    ]


def test_truncated_roots_absent_provenance_is_empty() -> None:
    assert truncated_roots(None, errors=[]) == []
    assert truncated_roots({}, errors=[]) == []


def test_truncated_roots_names_record_without_url_instead_of_dropping_it() -> None:
    # synthetic-бакет ревалидации не авторский корень: в план он не идёт, но пропуск
    # обязан быть назван вызывающему (молчаливое отбрасывание — дефект).
    prov = _provenance(
        {"product_id": "pg", "truncated": True, "truncated_reason": "max_documents"}
    )
    errors: list[str] = []
    assert truncated_roots(prov, errors=errors) == []
    assert len(errors) == 1 and "без URL" in errors[0]


def test_truncated_roots_missing_limits_are_none_not_zero() -> None:
    prov = _provenance(
        {
            "product_id": "pg",
            "root": "https://a.test/",
            "truncated": True,
            "truncated_reason": "max_documents",
            "pages_fetched": 5,
        }
    )
    (root,) = truncated_roots(prov, errors=[])
    assert root.max_depth is None
    assert root.max_documents is None
    assert root.queue_remaining is None


# --------------------------------------------------------------------------- #
# measured_roots
# --------------------------------------------------------------------------- #


def test_measured_roots_documents_include_replayed_pages() -> None:
    # Под ``drain`` известные страницы читаются из байтов и в ``pages_fetched`` не
    # попадают: размер корня — сумма, иначе объявленный предел сравнивался бы
    # с числом в другой единице.
    prov = _provenance(
        {
            "product_id": "pg",
            "root": "https://a.test/docs/",
            "truncated": False,
            "pages_fetched": 12,
            "replayed": 18927,
            "limits": {"max_depth": 8, "max_documents": 20000},
        }
    )
    (root,) = measured_roots(prov, errors=[])
    assert root.documents == 18939
    assert root.replayed == 18927
    assert root.truncated is False


def test_measured_roots_old_provenance_has_no_replayed_key() -> None:
    # Строка прошлой схемы не несёт ``replayed``: 0 здесь — «не было переигровки»,
    # а не выдуманное измерение.
    prov = _provenance(
        {
            "product_id": "pg",
            "root": "https://a.test/",
            "truncated": True,
            "truncated_reason": "max_documents",
            "pages_fetched": 7,
            "limits": {"max_documents": 7},
        }
    )
    (root,) = measured_roots(prov, errors=[])
    assert root.documents == 7
    assert root.replayed == 0


# --------------------------------------------------------------------------- #
# declared_limit
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("measured", "margin", "expected"),
    [
        (18939, 0, 18939),
        (100, 10, 110),
        (1001, 10, 1102),  # ceil(1101.1)
        (0, 10, 1),
        (1, 100, 2),
    ],
)
def test_declared_limit_rounds_up(measured: int, margin: int, expected: int) -> None:
    assert declared_limit(measured, margin_percent=margin) == expected


def test_declared_limit_rejects_negative_inputs() -> None:
    with pytest.raises(ValueError):
        declared_limit(-1)
    with pytest.raises(ValueError):
        declared_limit(10, margin_percent=-5)


# --------------------------------------------------------------------------- #
# is_drainable / declared_depth
# --------------------------------------------------------------------------- #


def _measured(
    reason: str | None,
    *,
    truncated: bool = True,
    max_depth: int | None = 8,
    max_documents: int | None = 100,
    documents: int = 100,
) -> MeasuredRoot:
    return MeasuredRoot(
        product_id="pg",
        root_url="https://a.test/docs/",
        documents=documents,
        replayed=0,
        truncated=truncated,
        reason=reason,
        queue_remaining=5,
        max_depth=max_depth,
        max_documents=max_documents,
    )


def test_is_drainable_covers_budget_and_depth_only() -> None:
    assert is_drainable(_measured("max_documents"))
    assert is_drainable(_measured("max_depth"))
    # Причина не записана — неизвестное не объявляем неизлечимым.
    assert is_drainable(_measured(None))


@pytest.mark.parametrize("reason", ["round_budget", "disk_low_headroom", "blocked"])
def test_is_drainable_rejects_other_reasons(reason: str) -> None:
    # Ещё один проход потратил бы сеть, ничего не измерив: причина не про потолок.
    assert not is_drainable(_measured(reason))


def test_declared_depth_exhausted_at_walked_depth() -> None:
    # Дошли до 9 и исчерпали — объявляем 9, а не «прежний + шаг» (иначе новое усечение).
    assert declared_depth(_measured("max_depth", truncated=False), 9) == 9


def test_declared_depth_still_truncated_adds_step() -> None:
    assert declared_depth(_measured("max_depth", truncated=True), 9, depth_step=1) == 10


def test_declared_depth_other_reason_keeps_declared() -> None:
    assert declared_depth(_measured("max_documents"), None) == 8
    # Даже если проход шёл на глубине, причина не про глубину — предел не меняется.
    assert declared_depth(_measured("max_documents"), 12) == 8


def test_declared_depth_without_walk_is_not_invented() -> None:
    assert declared_depth(_measured("max_depth", max_depth=None), None) is None


def test_declared_depth_rejects_negative() -> None:
    with pytest.raises(ValueError):
        declared_depth(_measured("max_depth"), -1)


# --------------------------------------------------------------------------- #
# plan_updates
# --------------------------------------------------------------------------- #


def _drained(
    documents: int, *, max_depth: int | None = 8, previous_max_depth: int | None = 8
) -> DrainedRoot:
    return DrainedRoot(
        product_id="pg",
        root_url="https://a.test/docs/",
        documents=documents,
        max_depth=max_depth,
        previous_max_documents=100,
        previous_max_depth=previous_max_depth,
    )


def test_plan_updates_declares_measured_plus_margin() -> None:
    (update,) = plan_updates([_drained(18939)], ["a.test"], margin_percent=10)
    assert update.max_documents == 20833  # ceil(18939 × 1.1)
    assert update.measured == 18939
    assert update.previous_max_documents == 100
    # Ключ — действующий блок профиля (host-wide), а не выведенный из URL.
    assert update.key == "a.test"


def test_plan_updates_carries_decided_depth_verbatim() -> None:
    # Глубину решил тот, кто вёл лестницу; здесь — только перенос в правку.
    (update,) = plan_updates([_drained(120, max_depth=9)], ["a.test"])
    assert update.max_depth == 9
    assert update.previous_max_depth == 8


def test_plan_updates_requires_a_profile_block() -> None:
    # Правка не создаёт профиль домена: корень без блока — отказ, а не новая запись.
    with pytest.raises(ValueError):
        plan_updates([_drained(10)], ["other.test"])


# --------------------------------------------------------------------------- #
# render_profile_limits
# --------------------------------------------------------------------------- #


def _update(max_documents: int, max_depth: int | None = 8) -> DrainUpdate:
    return DrainUpdate(
        key="postgrespro.ru/docs",
        product_id="pg",
        root_url="https://postgrespro.ru/docs/",
        measured=max_documents,
        max_documents=max_documents,
        max_depth=max_depth,
        previous_max_documents=20000,
        previous_max_depth=8,
    )


def test_render_replaces_value_and_keeps_comment_and_header() -> None:
    raw = _HEADER + _PROFILES
    out = render_profile_limits(raw, [_update(25000)])
    assert out.startswith(_HEADER)
    assert "max_documents: 25000  # замер: 18 939 документов" in out
    assert "max_documents: 50" in out  # соседний корень не тронут
    data = yaml.safe_load(out[len(_HEADER) :])
    assert data["postgrespro.ru/docs"]["max_documents"] == 25000
    assert data["jatoba.ru"]["max_documents"] == 50


def test_render_noop_returns_input_verbatim() -> None:
    raw = _HEADER + _PROFILES
    assert render_profile_limits(raw, [_update(20000, max_depth=8)]) == raw


def test_render_inserts_missing_field_with_block_indent() -> None:
    raw = "a.test:\n  fetch: http\n"
    update = DrainUpdate(
        key="a.test",
        product_id="pg",
        root_url="https://a.test/",
        measured=10,
        max_documents=11,
        max_depth=2,
        previous_max_documents=None,
        previous_max_depth=None,
    )
    out = render_profile_limits(raw, [update])
    data = yaml.safe_load(out)
    assert data["a.test"] == {"fetch": "http", "max_documents": 11, "max_depth": 2}


def test_render_depth_field_can_be_skipped() -> None:
    raw = "a.test:\n  max_depth: 3\n  max_documents: 10\n"
    update = DrainUpdate(
        key="a.test",
        product_id="pg",
        root_url="https://a.test/",
        measured=10,
        max_documents=11,
        max_depth=4,
        previous_max_documents=10,
        previous_max_depth=3,
    )
    out = render_profile_limits(raw, [update], depth_field=False)
    data = yaml.safe_load(out)
    assert data["a.test"]["max_depth"] == 3
    assert data["a.test"]["max_documents"] == 11


def test_render_rejects_unknown_key() -> None:
    with pytest.raises(ValueError):
        render_profile_limits("a.test:\n  max_depth: 1\n", [_update(10)])


def test_render_on_disk_profiles_changes_only_target_key() -> None:
    raw = ON_DISK_PROFILES.read_text(encoding="utf-8")
    before = yaml.safe_load(raw)
    # Ключ пилота сужен 24.11.2026 до дерева объявленной редакции (решение владельца):
    # `postgrespro.ru/docs` был каталогом документации ВСЕХ продуктов вендора.
    key = "postgrespro.ru/docs/enterprise/18"
    assert key in before, "фикстура зависит от реального ключа пилота"
    measured = int(before[key]["max_documents"])
    update = DrainUpdate(
        key=key,
        product_id="postgrespro",
        root_url="https://postgrespro.ru/docs/enterprise/18/",
        measured=measured,
        max_documents=measured + 1,
        max_depth=before[key].get("max_depth"),
        previous_max_documents=measured,
        previous_max_depth=before[key].get("max_depth"),
    )
    out = render_profile_limits(raw, [update])
    after = yaml.safe_load(out)
    assert after[key]["max_documents"] == measured + 1
    for other in before:
        if other == key:
            continue
        assert after[other] == before[other]
    # Шапка (ведущие комментарии) сохраняется дословно.
    assert out.split("\npostgrespro.ru/docs/enterprise/18:", 1)[0] == raw.split(
        "\npostgrespro.ru/docs/enterprise/18:", 1
    )[0]
