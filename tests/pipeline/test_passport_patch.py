"""Правка корней сателлитов в тексте паспорта: сохранение байтов и идемпотентность.

Класс дефекта, который пин держит: паспорт — **авторский** артефакт, и его комментарии
несут обоснование (чем измерен размер корня, какая цитата подтверждает сателлита).
Полная перезапись через PyYAML стёрла бы их, а `passports_digest` считается по байтам —
то есть «применить разведку» превратилось бы в потерю доказательств. Поэтому правка
строчная: меняются только строки блока `doc_domains` нужного элемента.

Второй дефект, который пин ловит отдельно: комментарий **уровня списка** перед следующим
элементом относится к следующему сателлиту, и вставка поля после него приписала бы корень
чужому компоненту (это случилось на первом прогоне: `doc_domains` Kintsugi встал под
заголовком «Установлен: бэкапы с FUSE — роль backup»).
"""

from __future__ import annotations

import pytest
import yaml

from core.pipeline.passport_patch import patch_satellite_roots, satellite_root_blocks

SAMPLE = """\
# Паспорт: шапка с обоснованием (обязана сохраниться байт-в-байт).
product_id: demo
doc_domains:
  - "docs.example.com/v1"
satellites:
  # Комментарий следующего элемента.
  - role: management_console
    state: approved
    component: "Console"
    relationship_evidence:
      url: "https://example.com"
      quote: "Console (управление)"
  # Комментарий второго элемента.
  - role: backup
    state: approved
    component: "Backup"
    relationship_evidence:
      url: "https://example.com"
      quote: "Backup (бэкапы)"
snapshot_date: 2026-10-01
"""


def test_blocks_read_roles_and_domains() -> None:
    blocks = satellite_root_blocks(SAMPLE)
    assert [block.role for block in blocks] == ["management_console", "backup"]
    assert all(block.doc_domains == () for block in blocks)


def test_patch_inserts_root_without_touching_header_or_comments() -> None:
    patched = patch_satellite_roots(
        SAMPLE, {"management_console": ["example.com/docs/console"]}
    )
    # Шапка и комментарии на месте дословно.
    assert patched.startswith("# Паспорт: шапка с обоснованием")
    assert "  # Комментарий следующего элемента.\n" in patched
    assert "  # Комментарий второго элемента.\n" in patched
    # Значение читается доменной моделью.
    data = yaml.safe_load(patched)
    by_role = {item["role"]: item for item in data["satellites"]}
    assert by_role["management_console"]["doc_domains"] == ["example.com/docs/console"]
    assert "doc_domains" not in by_role["backup"]
    # Вставка не уехала под комментарий следующего элемента: корень Kintsugi стоит
    # внутри его блока, до заголовка второго.
    first_comment = patched.index("# Комментарий второго элемента.")
    assert patched.index("example.com/docs/console") < first_comment


def test_patch_is_idempotent() -> None:
    once = patch_satellite_roots(SAMPLE, {"backup": ["example.com/docs/backup"]})
    twice = patch_satellite_roots(once, {"backup": ["example.com/docs/backup"]})
    assert once == twice


def test_patch_replaces_existing_block() -> None:
    once = patch_satellite_roots(SAMPLE, {"backup": ["a.example/b", "a.example/c"]})
    replaced = patch_satellite_roots(once, {"backup": ["z.example/y"]})
    data = yaml.safe_load(replaced)
    by_role = {item["role"]: item for item in data["satellites"]}
    assert by_role["backup"]["doc_domains"] == ["z.example/y"]
    # Замена не задела соседа и его комментарии.
    assert "  # Комментарий второго элемента.\n" in replaced
    assert by_role["management_console"].get("doc_domains") is None


def test_patch_unknown_role_raises() -> None:
    with pytest.raises(KeyError):
        patch_satellite_roots(SAMPLE, {"nope": ["a.example"]})


def test_patch_without_satellites_raises() -> None:
    with pytest.raises(ValueError):
        patch_satellite_roots("product_id: demo\n", {"backup": ["a.example"]})
