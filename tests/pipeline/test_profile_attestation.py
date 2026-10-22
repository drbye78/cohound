"""Тесты аттестации **профиля** пакета-якоря (G1/G2): дайджест, state.db, логика гейта.

Покрывают:

* стабильность ``profile_digest``: одинаковый материал → одинаковый дайджест (в одном
  процессе и **между процессами**), изменение любого компонента → другой дайджест
  (таблично, по одному тесту на компонент);
* durable-строку ``profile_attestation``: round-trip, идемпотентный upsert по
  ``attestation_id``, ``superseded_at`` (отзыв не «воскресает» повторной записью),
  миграцию ``state.db`` schema 3 → 4 на существующем файле;
* логику гейта: нет ни строки ``gate_decisions``, ни аттестации → ``blocked``;
  действующая аттестация → ``ok`` без per-run строки; устаревший дайджест → ``blocked``
  **с названием изменившегося компонента**; per-run строка → ``ok`` без аттестации;
* состав ``PHASES``: ``g3``/``g4`` удалены, ``review``/``compute``/``report`` на месте,
  полный порядок фаз зафиксирован.

Категорийно нейтрально (§1.3): ``p1``/``p2``, ``case_x``, ``mod_a``/``mod_b``. Сеть и
диск (кроме ``tmp_path``/``passports``-фикстур) не используются.
"""

from __future__ import annotations

import datetime as _dt
import json
import subprocess
import sys
from pathlib import Path

import pytest

from core.config.passports import passport_content_digest, passport_content_digests
from core.domain import Focus, Gate, GatePolicy, RunConfig, RunMode
from core.domain.models import ProfileAttestationRow
from core.pipeline.gates import gate_g1, gate_g2
from core.pipeline.profile_attestation import (
    PROFILE_MATERIAL_KEYS,
    describe_material_diff,
    make_profile_gate,
    material_digest,
    profile_attestation_id,
    profile_digest,
    profile_material,
)
from core.pipeline.runner import (
    GATE_PHASES,
    PHASES,
    GateRuntime,
    PhaseResult,
    Ports,
    RunContext,
    _gate_phase,
)
from core.store.state_sqlite import STATE_SCHEMA_VERSION, connect_state
from core.store.sqlite_store import SqliteStore
from core.store.state_store import SqliteStateStore

RUN_ID = "run.p"
CASE_ID = "case_x"
PACKAGE_ID = "case_x"
ANCHOR = "p1"


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _material(**overrides: object) -> dict[str, object]:
    """Базовый материал профиля; ``overrides`` меняет один компонент за раз."""
    base: dict[str, object] = {
        "case_id": CASE_ID,
        "package_id": PACKAGE_ID,
        "anchor": ANCHOR,
        "cohort_digest": "cd_0001",
        "products": ["p1", "p2"],
        "focus_modules": ["mod_a"],
        "focus_params": ["param_x"],
        "seeds_digest": "sd_0001",
        "roles": ["role_r1", "role_r2"],
        "passports": {"p1": "pp_0001", "p2": None},
    }
    base.update(overrides)
    return profile_material(**base)  # type: ignore[arg-type]


def _digest(**overrides: object) -> str:
    material = _material(**overrides)
    return material_digest(material)


def _attestation(
    *,
    material: dict[str, object],
    digest: str,
    gate: Gate = Gate.G1,
    actor: str = "human-1",
    attested_at: str = "2026-10-01T00:00:00Z",
    notes: str | None = None,
) -> ProfileAttestationRow:
    return ProfileAttestationRow(
        attestation_id=profile_attestation_id(
            case_id=str(material["case_id"]),
            package_id=str(material["package_id"]),
            anchor=str(material["anchor"]),
            gate=gate,
            profile_digest=digest,
            attested_by=actor,
        ),
        case_id=str(material["case_id"]),
        package_id=str(material["package_id"]),
        anchor=str(material["anchor"]),
        gate=gate,
        profile_digest=digest,
        digest_material=material,
        attested_by=actor,
        attested_at=attested_at,
        notes=notes,
    )


def _state_store() -> SqliteStateStore:
    return SqliteStateStore(":memory:")


def _run_store() -> SqliteStore:
    store = SqliteStore(":memory:")
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id=CASE_ID,
            anchor=ANCHOR,
            cohort="cohort.yaml",
            products=["p1", "p2"],
            package=PACKAGE_ID,
            focus=Focus(),
            snapshot_date=_dt.date(2026, 10, 1),
            mode=RunMode.STRICT,
        ),
    )
    return store


def _ctx(store: SqliteStore) -> RunContext:
    return RunContext(run_id=RUN_ID, store=store, ports=Ports())


# ---------------------------------------------------------------------------
# Дайджест: стабильность и чувствительность к компонентам
# ---------------------------------------------------------------------------


def test_material_has_exactly_the_declared_components() -> None:
    material = _material()
    assert tuple(sorted(material)) == tuple(sorted(PROFILE_MATERIAL_KEYS))
    # Материал — то, что человек аттестует: ни меток времени, ни путей.
    assert "attested_at" not in material
    assert "path" not in json.dumps(material)


def test_digest_is_stable_across_two_computations() -> None:
    assert _digest() == _digest()


def test_digest_is_order_insensitive_for_lists_and_mappings() -> None:
    """Порядок входа не влияет: иначе дайджест «дрожал» бы от порядка файлов/словарей."""
    first = _digest(products=["p2", "p1"], roles=["role_r2", "role_r1"])
    second = _digest(products=["p1", "p2"], roles=["role_r1", "role_r2"])
    third = _digest(passports={"p2": None, "p1": "pp_0001"})
    assert first == second == third


def test_digest_is_stable_across_process_boundaries() -> None:
    """Тот же материал в отдельном процессе даёт тот же дайджест (нет скрытого состояния).

    Это ключевое свойство: аттестация пишется одним процессом (``hound profile attest``),
    а проверяется другим (``hound run``) — и, возможно, на другом хосте.
    """
    material = _material()
    expected = material_digest(material)
    code = (
        "import json, sys\n"
        "from core.pipeline.profile_attestation import material_digest\n"
        "print(material_digest(json.loads(sys.stdin.read())))\n"
    )
    root = Path(__file__).resolve().parents[2]
    proc = subprocess.run(
        [sys.executable, "-c", code],
        input=json.dumps(material),
        capture_output=True,
        text=True,
        cwd=str(root),
        env={"PYTHONPATH": str(root), "PATH": "/usr/bin:/bin"},
        check=True,
    )
    assert proc.stdout.strip() == expected


#: Изменения по одному компоненту: (компонент, override, ожидаемая подстрока в диффе).
_COMPONENT_CHANGES: tuple[tuple[str, dict[str, object], str], ...] = (
    ("case_id", {"case_id": "case_y"}, "case_id"),
    ("package_id", {"package_id": "case_y"}, "package_id"),
    ("anchor", {"anchor": "p2"}, "anchor"),
    ("cohort_digest", {"cohort_digest": "cd_0002"}, "cohort_digest"),
    ("products", {"products": ["p1", "p2", "p3"]}, "products"),
    ("focus_modules", {"focus_modules": ["mod_a", "mod_b"]}, "focus_modules"),
    ("focus_params", {"focus_params": ["param_y"]}, "focus_params"),
    ("seeds_digest", {"seeds_digest": "sd_0002"}, "seeds_digest"),
    ("roles", {"roles": ["role_r1"]}, "roles"),
    ("passports", {"passports": {"p1": "pp_0002", "p2": None}}, "passports"),
)


@pytest.mark.parametrize(
    ("component", "override", "key_in_diff"),
    _COMPONENT_CHANGES,
    ids=[item[0] for item in _COMPONENT_CHANGES],
)
def test_each_component_changes_the_digest(
    component: str, override: dict[str, object], key_in_diff: str
) -> None:
    """Любой компонент материала меняет дайджест и попадает в дифф по своему ключу."""
    baseline = _material()
    changed = _material(**override)
    assert set(changed) == set(baseline)

    baseline_digest = material_digest(baseline)
    changed_digest = material_digest(changed)
    assert changed_digest != baseline_digest, f"{component} не влияет на дайджест"

    diff = describe_material_diff(baseline, changed)
    assert diff, f"{component}: дифф обязан быть непустым"
    assert any(key_in_diff in line for line in diff), (
        f"{component}: дифф обязан называть компонент {key_in_diff!r}, получено {diff}"
    )
    # Обратное направление — тот же набор изменений (симметрия диагностики).
    reverse = describe_material_diff(changed, baseline)
    assert any(key_in_diff in line for line in reverse)


def test_diff_names_added_and_removed_entries() -> None:
    """Для списков дифф называет добавленные/убранные элементы, а не только ключ."""
    baseline = _material()
    changed = _material(products=["p1", "p3"], roles=["role_r1"])
    diff = describe_material_diff(baseline, changed)
    products_line = next(line for line in diff if line.startswith("products:"))
    assert "добавлены" in products_line and "p3" in products_line
    assert "убраны" in products_line and "p2" in products_line
    roles_line = next(line for line in diff if line.startswith("roles:"))
    assert "role_r2" in roles_line


def test_diff_is_empty_for_identical_material() -> None:
    assert describe_material_diff(_material(), _material()) == []


def test_profile_digest_returns_digest_and_material() -> None:
    digest, material = profile_digest(
        case_id=CASE_ID,
        package_id=PACKAGE_ID,
        anchor=ANCHOR,
        cohort_digest="cd_0001",
        products=["p1", "p2"],
        focus_modules=["mod_a"],
        focus_params=["param_x"],
        seeds_digest="sd_0001",
        roles=["role_r1", "role_r2"],
        passports={"p1": "pp_0001", "p2": None},
    )
    assert material == _material()
    assert digest == material_digest(material)
    assert len(digest) == 24


def test_passport_content_digest_tracks_authored_bytes(tmp_path: Path) -> None:
    """Дайджест паспорта считается по авторским байтам и меняется при правке файла."""
    root = tmp_path / "passports"
    product_dir = root / "p1"
    product_dir.mkdir(parents=True)
    first = product_dir / "2026-10-01.yaml"
    first.write_text("case_id: case_x\n", encoding="utf-8")

    digest_one = passport_content_digest("p1", root)
    assert digest_one is not None
    # Стабильность: повторное чтение тех же байтов — тот же дайджест.
    assert passport_content_digest("p1", root) == digest_one
    # Правка содержимого — другой дайджест.
    first.write_text("case_id: case_x\nfrozen_at: 2026-10-01\n", encoding="utf-8")
    assert passport_content_digest("p1", root) != digest_one
    # Появление второго паспортного файла — тоже другой дайджест (весь набор).
    digest_two = passport_content_digest("p1", root)
    (product_dir / "2026-11-01.yaml").write_text("case_id: case_x\n", encoding="utf-8")
    assert passport_content_digest("p1", root) != digest_two
    # Продукт без паспортов — наблюдаемое ``None``, а не «нет компонента».
    assert passport_content_digest("p2", root) is None
    assert passport_content_digests(["p2", "p1"], root) == {
        "p1": passport_content_digest("p1", root),
        "p2": None,
    }


def test_passport_appearance_changes_profile_digest() -> None:
    """Появление паспорта у продукта меняет дайджест профиля (паспорта фиксируются на цикл)."""
    without = _digest(passports={"p1": None, "p2": None})
    with_passport = _digest(passports={"p1": "pp_0001", "p2": None})
    assert without != with_passport


# ---------------------------------------------------------------------------
# Durable-строка ``profile_attestation`` в state.db
# ---------------------------------------------------------------------------


def test_profile_attestation_round_trip() -> None:
    material = _material()
    digest = material_digest(material)
    row = _attestation(material=material, digest=digest, notes="weekly cycle")

    with _state_store() as state:
        state.save_profile_attestation(row)
        found = state.find_profile_attestation(
            CASE_ID, PACKAGE_ID, ANCHOR, Gate.G1, digest
        )
        assert found is not None
        assert found.attestation_id == row.attestation_id
        assert found.profile_digest == digest
        assert found.digest_material == material
        assert found.attested_by == "human-1"
        assert found.superseded_at is None
        assert found.notes == "weekly cycle"
        assert [item.attestation_id for item in state.iter_profile_attestations(
            case_id=CASE_ID, package_id=PACKAGE_ID, anchor=ANCHOR, gate=Gate.G1
        )] == [row.attestation_id]
        # Другой гейт той же строкой не аттестуется.
        assert state.find_profile_attestation(
            CASE_ID, PACKAGE_ID, ANCHOR, Gate.G2, digest
        ) is None


def test_profile_attestation_upsert_is_idempotent() -> None:
    material = _material()
    digest = material_digest(material)
    row = _attestation(material=material, digest=digest)

    with _state_store() as state:
        state.save_profile_attestation(row)
        state.save_profile_attestation(row)
        rows = state.iter_profile_attestations(
            case_id=CASE_ID, package_id=PACKAGE_ID, anchor=ANCHOR, gate=Gate.G1
        )
        assert len(rows) == 1

        # Другой человек по тому же профилю — отдельное решение (аудит-след).
        other = _attestation(material=material, digest=digest, actor="human-2")
        assert other.attestation_id != row.attestation_id
        state.save_profile_attestation(other)
        assert len(
            state.iter_profile_attestations(
                case_id=CASE_ID, package_id=PACKAGE_ID, anchor=ANCHOR, gate=Gate.G1
            )
        ) == 2


def test_supersede_marks_attestation_and_survives_repeated_save() -> None:
    """Отзыв необратим повторной записью и не удаляет строку (аудит)."""
    material = _material()
    digest = material_digest(material)
    row = _attestation(material=material, digest=digest)

    with _state_store() as state:
        state.save_profile_attestation(row)
        state.supersede_profile_attestation(
            row.attestation_id, superseded_at="2026-10-05T00:00:00Z"
        )
        superseded = state.find_profile_attestation(
            CASE_ID, PACKAGE_ID, ANCHOR, Gate.G1, digest
        )
        assert superseded is not None
        assert superseded.superseded_at == "2026-10-05T00:00:00Z"
        # Отозванная не попадает в действующие…
        assert list(
            state.iter_profile_attestations(
                case_id=CASE_ID, package_id=PACKAGE_ID, anchor=ANCHOR, gate=Gate.G1
            )
        ) == []
        # …но видна в аудите.
        assert len(
            state.iter_profile_attestations(
                case_id=CASE_ID,
                package_id=PACKAGE_ID,
                anchor=ANCHOR,
                gate=Gate.G1,
                include_superseded=True,
            )
        ) == 1
        # Повторная подача того же решения не «воскрешает» аттестацию.
        state.save_profile_attestation(row)
        assert (
            state.find_profile_attestation(
                CASE_ID, PACKAGE_ID, ANCHOR, Gate.G1, digest
            ).superseded_at
            == "2026-10-05T00:00:00Z"
        )
        # Повторный отзыв не переписывает метку.
        state.supersede_profile_attestation(
            row.attestation_id, superseded_at="2026-12-31T00:00:00Z"
        )
        assert (
            state.find_profile_attestation(
                CASE_ID, PACKAGE_ID, ANCHOR, Gate.G1, digest
            ).superseded_at
            == "2026-10-05T00:00:00Z"
        )


def test_state_db_migrates_from_schema_3_to_4(tmp_path: Path) -> None:
    """Старая ``state.db`` (schema 3) открывается, получает ``profile_attestation``.

    БД строится текущим DDL и затем приведена к состоянию «до v4»: таблицы
    ``profile_attestation`` нет, версия схемы ``"3"``. Строки v3 обязаны сохраниться.
    """
    db_path = tmp_path / "state.db"
    conn = connect_state(db_path)
    conn.execute("DROP TABLE profile_attestation")
    conn.execute(
        "INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('schema_version', '3')"
    )
    conn.execute(
        "INSERT INTO product_state (product_id, canonical_name, onboarding_state,"
        " added_at, added_by) VALUES ('p1', 'Product One', 'ready', '2026-01-01', 'test')"
    )
    conn.commit()
    conn.close()

    migrated = connect_state(db_path)
    try:
        version = migrated.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()["value"]
        assert version == STATE_SCHEMA_VERSION == "4"
        tables = {
            row["name"]
            for row in migrated.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert "profile_attestation" in tables
        # Cell-level аттестация охвата (I2b) — отдельная таблица, не переиспользована.
        assert "attestation" in tables
        assert (
            migrated.execute(
                "SELECT canonical_name FROM product_state WHERE product_id = 'p1'"
            ).fetchone()["canonical_name"]
            == "Product One"
        )
        columns = {
            row["name"]
            for row in migrated.execute("PRAGMA table_info(profile_attestation)")
        }
        assert columns == {
            "attestation_id",
            "case_id",
            "package_id",
            "anchor",
            "gate",
            "profile_digest",
            "digest_material",
            "attested_by",
            "attested_at",
            "superseded_at",
            "notes",
        }
    finally:
        migrated.close()

    # Повторное открытие идемпотентно (миграция — no-op, версия та же).
    again = connect_state(db_path)
    try:
        assert (
            again.execute(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'"
            ).fetchone()["value"]
            == "4"
        )
    finally:
        again.close()

    # И запись аттестации в миграционно обновлённую БД работает.
    material = _material()
    with SqliteStateStore(db_path) as state:
        state.save_profile_attestation(
            _attestation(material=material, digest=material_digest(material))
        )
        assert state.find_profile_attestation(
            CASE_ID, PACKAGE_ID, ANCHOR, Gate.G1, material_digest(material)
        ) is not None


# ---------------------------------------------------------------------------
# Логика гейта: per-run строка ИЛИ действующая аттестация профиля
# ---------------------------------------------------------------------------


def test_gate_blocked_without_row_and_without_attestation() -> None:
    """(a) Ни строки ``gate_decisions``, ни аттестации → ``blocked`` с действенной подсказкой."""
    material = _material()
    digest = material_digest(material)
    with _state_store() as state, _run_store() as store:
        phase = _gate_phase("g1", Gate.G1, GateRuntime(profile_gate=make_profile_gate(state, material)))
        result = phase(_ctx(store))
        assert result.status == "blocked"
        assert result.phase == "g1"
        message = "\n".join(result.errors)
        assert "нет строки в gate_decisions" in message
        assert "нет аттестации профиля" in message
        assert "hound profile attest" in message
        assert digest[:12] in message


def test_gate_passes_with_matching_attestation_without_per_run_row() -> None:
    """(b) Действующая аттестация профиля → ``ok`` без единой строки ``gate_decisions``."""
    material = _material()
    digest = material_digest(material)
    with _state_store() as state, _run_store() as store:
        state.save_profile_attestation(_attestation(material=material, digest=digest))
        for gate, phase_name in ((Gate.G1, "g1"), (Gate.G2, "g2")):
            if gate is Gate.G2:
                state.save_profile_attestation(
                    _attestation(material=material, digest=digest, gate=Gate.G2)
                )
            result = _gate_phase(phase_name, gate, GateRuntime(profile_gate=make_profile_gate(state, material)))(
                _ctx(store)
            )
            assert result.status == "ok", result.errors
        assert list(store.iter_gate_decisions(RUN_ID)) == []


def test_gate_blocked_with_stale_digest_and_error_names_the_component() -> None:
    """(c) Устаревший дайджест → ``blocked``, и сообщение называет изменившийся компонент."""
    attested_material = _material()
    attested_digest = material_digest(attested_material)
    current_material = _material(cohort_digest="cd_9999", products=["p1", "p2", "p3"])

    with _state_store() as state, _run_store() as store:
        state.save_profile_attestation(
            _attestation(material=attested_material, digest=attested_digest)
        )
        result = _gate_phase(
            "g1",
            Gate.G1,
            GateRuntime(profile_gate=make_profile_gate(state, current_material)),
        )(_ctx(store))

        assert result.status == "blocked"
        message = "\n".join(result.errors)
        # Назван именно изменившийся компонент, а не «нет строки» без деталей.
        assert "cohort_digest" in message
        assert "cohort" in message
        # Изменение состава продуктов — с добавленным элементом.
        assert "products" in message
        assert "p3" in message
        # Аттестованный и текущий дайджесты различимы в сообщении.
        assert attested_digest[:12] in message
        assert material_digest(current_material)[:12] in message
        assert "hound profile attest" in message
        # Неизменившиеся компоненты не объявлены изменёнными.
        assert "seeds_digest" not in message


def test_gate_passes_with_per_run_row_without_attestation() -> None:
    """(d) Per-run строка ``gate_decisions`` проходит без аттестации (обратная совместимость)."""
    material = _material()
    with _state_store() as state, _run_store() as store:
        gate_g1(store, RUN_ID, actor="human-1")
        result = _gate_phase("g1", Gate.G1, GateRuntime(profile_gate=make_profile_gate(state, material)))(
            _ctx(store)
        )
        assert result.status == "ok"
        # Резолвер даже не понадобился: per-run решение приоритетно.
        assert list(
            state.iter_profile_attestations(
                case_id=CASE_ID, package_id=PACKAGE_ID, anchor=ANCHOR, gate=Gate.G1
            )
        ) == []


def test_gate_phase_without_resolver_keeps_previous_behaviour() -> None:
    """Без резолвера (dry-run/``--until plan``) гейт проверяет только ``gate_decisions``."""
    with _run_store() as store:
        blocked = _gate_phase("g1", Gate.G1)(_ctx(store))
        assert blocked.status == "blocked"
        # Первая часть сообщения — факт отсутствия решения; далее действенная подсказка.
        assert blocked.errors[0].startswith(
            "гейт G1 не пройден: нет строки в gate_decisions (S9)"
        )
        gate_g2(store, RUN_ID, actor="human-1")
        assert _gate_phase("g2", Gate.G2)(_ctx(store)).status == "ok"


def test_superseded_attestation_does_not_pass_the_gate() -> None:
    """Отозванная аттестация не проходит гейт и не используется как база диффа вслепую."""
    material = _material()
    digest = material_digest(material)
    with _state_store() as state, _run_store() as store:
        row = _attestation(material=material, digest=digest)
        state.save_profile_attestation(row)
        state.supersede_profile_attestation(
            row.attestation_id, superseded_at="2026-10-05T00:00:00Z"
        )
        result = _gate_phase("g1", Gate.G1, GateRuntime(profile_gate=make_profile_gate(state, material)))(
            _ctx(store)
        )
        assert result.status == "blocked"
        assert "нет аттестации профиля" in "\n".join(result.errors)


def test_runner_stops_at_g1_and_resumes_with_profile_attestation() -> None:
    """Сквозная проверка через ``Runner``: блок без аттестации, проход с ней."""
    from core.pipeline.runner import Runner

    material = _material()
    digest = material_digest(material)

    def _ok(phase: str):
        def _fn(ctx: RunContext) -> PhaseResult:
            return PhaseResult(status="ok", phase=phase)

        return _fn

    with _state_store() as state, _run_store() as store:
        resolver = make_profile_gate(state, material)
        phases = {
            name: _ok(name) for name in PHASES if name not in GATE_PHASES
        }
        # G3/G4 — точки интеграции: в этом тесте они делегированы (waived), чтобы
        # проверялся именно путь аттестации профиля для G1/G2.
        runtime = GateRuntime(
            profile_gate=resolver,
            policies={"G3": GatePolicy.WAIVED, "G4": GatePolicy.WAIVED},
        )
        for name, gate in GATE_PHASES.items():
            phases[name] = _gate_phase(name, gate, runtime)

        blocked = Runner(store, phases=dict(phases)).run(RUN_ID, until="g2")
        assert blocked.stopped_at == "g1"
        assert blocked.status == "blocked"

        # Аттестация профиля по обоим гейтам: прогон проходит без per-run решений.
        for gate in (Gate.G1, Gate.G2):
            state.save_profile_attestation(
                _attestation(material=material, digest=digest, gate=gate)
            )
        # Сброс durable-состояния фаз, иначе ``blocked`` пересчитается, но ``ok`` — нет;
        # для чистоты проверяем новый прогон того же профиля.
        second_run = "run.p2"
        store.create_run(
            second_run,
            RunConfig(
                case_id=CASE_ID,
                anchor=ANCHOR,
                cohort="cohort.yaml",
                products=["p1", "p2"],
                package=PACKAGE_ID,
                focus=Focus(),
                snapshot_date=_dt.date(2026, 10, 1),
                mode=RunMode.STRICT,
            ),
        )
        passed = Runner(store, phases=dict(phases)).run(second_run)
        assert passed.stopped_at is None
        assert passed.status == "ok"
        # Решений по G1/G2 нет; есть только waiver-строки делегированных G3/G4.
        assert [
            d.gate for d in store.iter_gate_decisions(second_run)
        ] == [Gate.G3, Gate.G4]


# ---------------------------------------------------------------------------
# Состав фаз: G1/G2 — внутренние гейты, G3/G4 — точки интеграции
# ---------------------------------------------------------------------------


def test_phases_keep_all_four_gates_and_review_compute_report() -> None:
    """G3/G4 остаются фазами: это точки **интеграции**, а не решения coHound."""
    for gate in ("g1", "g2", "g3", "g4"):
        assert gate in PHASES
    for kept in ("review", "compute", "report"):
        assert kept in PHASES
    assert set(GATE_PHASES) == {"g1", "g2", "g3", "g4"}


def test_full_phase_order_is_fixed() -> None:
    """Полный порядок фаз — инвариант: ревью экспортируется до G3, расчёт — до G4."""
    assert PHASES == (
        "normalize",
        "lint",
        "plan",
        "g1",
        "discovery",
        "g2",
        "registry",
        "inheritance",
        "corpus",
        "evaluate",
        "review",
        "g3",
        "compute",
        "g4",
        "report",
    )
    # Гейты стоят там, где решения действительно нужны: G1 — до разведки, G2 — до
    # реестров/наследования, G3 — после экспорта очереди ревью, G4 — после расчёта и до
    # публикации (отчёт).
    assert PHASES.index("g1") < PHASES.index("discovery")
    assert PHASES.index("g2") < PHASES.index("registry")
    assert PHASES.index("review") < PHASES.index("g3") < PHASES.index("compute")
    assert PHASES.index("compute") < PHASES.index("g4") < PHASES.index("report")


# ---------------------------------------------------------------------------
# Идентичность прогона: удаление гейтов НЕ меняет run_id
# ---------------------------------------------------------------------------


def test_gate_set_is_not_part_of_run_identity() -> None:
    """``run_id`` не зависит от набора гейтов: гейты — процедура, не условие измерения.

    Смена состава гейтов — не execution/methodology boundary (она не меняет измерение),
    поэтому ни ``EXECUTION_CONTRACT_FIELDS``, ни материал ``derive_run_id`` не содержат
    гейтов. Тест фиксирует это: добавление гейта в контракт потребовало бы нового
    ``run_id`` у всех прогонов и разорвало бы межцикловый diff.
    """
    from core.pipeline.digests import EXECUTION_CONTRACT_FIELDS
    from core.pipeline.plan import derive_run_id

    assert not [field for field in EXECUTION_CONTRACT_FIELDS if "gate" in field]
    assert "g1" not in EXECUTION_CONTRACT_FIELDS
    assert "g2" not in EXECUTION_CONTRACT_FIELDS
    # ``derive_run_id`` не принимает гейт ни позиционно, ни по имени.
    import inspect

    signature = inspect.signature(derive_run_id)
    assert not [
        name for name in signature.parameters if "gate" in name.lower()
    ], signature
    # Идентификатор стабилен при тех же аргументах (формула не зависит от гейтов).
    fixture = Path(__file__).resolve().parents[1] / "fixtures" / "e2e"
    from core.pipeline.normalize import load_inputs

    inputs = load_inputs(str(fixture / "run.yaml"), str(fixture))
    kwargs = {
        "cycle_id": "cycle-1",
        "cohort_digest": "cd",
        "seeds_digest": "sd",
        "execution_contract_digest": "ecd",
    }
    first = derive_run_id(inputs, **kwargs)
    second = derive_run_id(inputs, **kwargs)
    assert first == second
    assert first.startswith(f"{inputs.run.case_id}-")
