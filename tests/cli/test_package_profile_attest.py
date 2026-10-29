"""Тесты ``hound package profile attest|show``: производитель аттестации профиля (G1/G2).

Покрывают дефект, который эти команды закрывают: порт, стор и резолвер гейта были, а
**команды**, которую предлагало сообщение гейта, не существовало. Поэтому проверяется не
только «строка записалась», но и то, что записанное **действует**: прогонный
:func:`core.pipeline.profile_attestation.make_profile_gate` проходит с тем же материалом и
не проходит с изменённым — и называет изменившийся компонент.

Ещё один инвариант — **одна формула материала**: команда собирает материал тем же путём,
что ``plan``/``run`` (``load_inputs`` → ``load_effective_seeds`` → ``cohort_digest``/
``seeds_digest`` → ``build_plan`` → ``_profile_material_for_run``); тест сверяет материал
команды с материалом, собранным прогонным путём, а не «на глаз».

Сеть не используется: пакет создаётся из шаблона во временном каталоге, состояние —
``isolated_state_dir`` (``COHOUND_STATE_DIR``).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from core.cli.context import _profile_material_for_package, _profile_material_for_run
from core.cli.main import EXIT_OK, EXIT_USAGE, main
from core.config.paths import resolve_paths
from core.domain import Gate
from core.pipeline.digests import cohort_digest, seeds_digest
from core.pipeline.normalize import load_inputs
from core.pipeline.onboarding import load_effective_seeds
from core.pipeline.plan import plan as build_plan
from core.pipeline.profile_attestation import make_profile_gate
from core.store.state_sqlite import state_db_path
from core.store.state_store import SqliteStateStore

pytestmark = pytest.mark.usefixtures("isolated_state_dir")

_AT = "2026-10-01T10:00:00Z"
_ACTOR = "roger"


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _run(capsys: pytest.CaptureFixture[str], argv: list[str]) -> tuple[int, str, str]:
    """Запускает CLI и возвращает ``(код, stdout, stderr)``."""
    code = main(argv)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _new_package(
    capsys: pytest.CaptureFixture[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    """Создаёт пакет из шаблона в ``tmp_path`` (CWD — ``tmp_path``: паспортов нет)."""
    monkeypatch.chdir(tmp_path)
    code, out, _ = _run(
        capsys,
        [
            "package",
            "new",
            "--case-id",
            "profile_pkg",
            "--archetype",
            "B",
            "--dir",
            str(tmp_path),
            "--json",
        ],
    )
    assert code == EXIT_OK, out
    return tmp_path / "profile_pkg"


def _attest(
    capsys: pytest.CaptureFixture[str],
    package: Path,
    *,
    gate: str = "G1",
    actor: str | None = _ACTOR,
    at: str | None = _AT,
    extra: tuple[str, ...] = (),
) -> tuple[int, dict[str, object], str]:
    """``profile attest --json``; возвращает ``(код, payload, stderr)``."""
    argv = ["package", "profile", "attest", "--package", str(package), "--gate", gate]
    if actor is not None:
        argv.extend(["--actor", actor])
    if at is not None:
        argv.extend(["--attested-at", at])
    argv.extend(["--json", *extra])
    code, out, err = _run(capsys, argv)
    payload = json.loads(out) if out.strip().startswith("{") else {}
    return code, payload, err


def _show(
    capsys: pytest.CaptureFixture[str], package: Path
) -> tuple[int, dict[str, object]]:
    """``profile show --json``; возвращает ``(код, payload)``."""
    code, out, _ = _run(
        capsys, ["package", "profile", "show", "--package", str(package), "--json"]
    )
    return code, json.loads(out)


def _run_path_material(package: Path) -> dict[str, object]:
    """Материал профиля, собранный **прогонным** путём (как ``plan``/``run``).

    Второй формулы здесь нет: это ровно та последовательность, которой идёт прогон.
    Нужна как эталон для сверки с материалом команды.
    """
    inputs = load_inputs(str(package / "run.yaml"), str(package))
    run_plan = build_plan(inputs, run_id="parity")
    cohort = cohort_digest(
        inputs.cohort,
        case_id=inputs.run.case_id,
        cohort_ref=getattr(inputs.run, "cohort_ref", None),
    )
    seed_config, _ = load_effective_seeds(
        str(package),
        inputs.run.products,
        profiles=getattr(inputs, "domain_profiles", None) or {},
    )
    args = argparse.Namespace(package=str(package), paths=None, passports_dir=None)
    return _profile_material_for_run(
        args,
        inputs,
        run_plan,
        cohort_digest=cohort,
        seeds_digest=seeds_digest(seed_config),
    )


def _state_db(package: Path) -> Path:
    """Путь ``state/{package_id}/state.db`` — как его резолвит команда и прогон."""
    return state_db_path(resolve_paths().state, Path(package).name)


# ---------------------------------------------------------------------------
# attest: dry-run
# ---------------------------------------------------------------------------


def test_dry_run_writes_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    isolated_state_dir: Path,
) -> None:
    """``--dry-run`` не пишет ничего (ни ``state.db``, ни файла лока) и печатает материал.

    Обещание флага включает отсутствие **файла лока**: его создание тоже запись в
    каталог состояния оператора.
    """
    package = _new_package(capsys, tmp_path, monkeypatch)
    code, payload, err = _attest(capsys, package, extra=("--dry-run",))

    assert code == EXIT_OK, err
    assert payload["dry_run"] is True
    # Запись не выполнялась — «создано ли» неизвестно, а не выдуманное ``false``.
    assert payload["created"] is None
    assert payload["gate"] == Gate.G1.value
    assert payload["attestation_id"]
    assert payload["profile_digest"]
    material = payload["material"]
    assert {
        "case_id",
        "package_id",
        "anchor",
        "cohort_digest",
        "seeds_digest",
        "passports",
    } <= set(material)
    assert material["case_id"] == "profile_pkg"
    # Ни ``state.db``, ни ``.lock``: каталог состояния пакета не создан вовсе.
    assert not (isolated_state_dir / package.name).exists()
    assert not _state_db(package).exists()


# ---------------------------------------------------------------------------
# attest: запись
# ---------------------------------------------------------------------------


def test_attest_writes_row_visible_through_port(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Строка записана через порт: тот же дайджест, материал и метка времени.

    Материал команды сверяется с прогонным — иначе аттестованное и проверяемое разошлись
    бы молча (вторая формула дайджеста).
    """
    package = _new_package(capsys, tmp_path, monkeypatch)
    code, payload, err = _attest(capsys, package)
    assert code == EXIT_OK, err
    assert payload["created"] is True
    assert payload["dry_run"] is False

    material = payload["material"]
    assert material == _run_path_material(package)
    # Тот же материал даёт и хелпер, который команда вызывает (одна точка сборки).
    args = argparse.Namespace(package=str(package), paths=None, passports_dir=None)
    assert _profile_material_for_package(args) == material

    with SqliteStateStore(_state_db(package)) as state:
        row = state.find_profile_attestation(
            str(material["case_id"]),
            str(material["package_id"]),
            str(material["anchor"]),
            Gate.G1,
            str(payload["profile_digest"]),
        )
    assert row is not None
    assert row.attestation_id == payload["attestation_id"]
    assert row.digest_material == material
    assert row.attested_by == _ACTOR
    assert row.attested_at == _AT
    assert row.superseded_at is None


def test_repeat_submission_is_idempotent_no_op(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Повтор того же решения — no-op: ``created: false``, ``attested_at`` не переписан.

    Идемпотентность нужна потому, что профиль аттестуется редко, а команду легко
    повторить: вторая подача не должна ни плодить строки, ни сдвигать дату исходного
    решения (аудит отвечает «когда утвердили», а не «когда последний раз нажали»).
    """
    package = _new_package(capsys, tmp_path, monkeypatch)
    code, first, err = _attest(capsys, package)
    assert code == EXIT_OK, err
    assert first["created"] is True

    code, second, err = _attest(capsys, package, at="2026-10-05T10:00:00Z")
    assert code == EXIT_OK, err
    assert second["created"] is False
    assert second["attestation_id"] == first["attestation_id"]
    # Действует метка исходной подачи, а не переданная сейчас.
    assert second["attested_at"] == _AT
    assert any("no-op" in str(item) for item in second["warnings"])

    code, listing = _show(capsys, package)
    assert code == EXIT_OK
    assert len(listing["attestations"]) == 1


def test_attest_gate_g2_is_separate_row(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """G1 и G2 аттестуются отдельными строками (предмет решения разный)."""
    package = _new_package(capsys, tmp_path, monkeypatch)
    code, g1, err = _attest(capsys, package)
    assert code == EXIT_OK, err
    code, g2, err = _attest(capsys, package, gate="G2")
    assert code == EXIT_OK, err
    assert g2["created"] is True
    assert g2["attestation_id"] != g1["attestation_id"]

    code, listing = _show(capsys, package)
    assert code == EXIT_OK
    assert {row["gate"] for row in listing["attestations"]} == {"G1", "G2"}


# ---------------------------------------------------------------------------
# attest: отказы
# ---------------------------------------------------------------------------


def test_gate_outside_g1_g2_is_usage_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``--gate G3`` — ``EXIT_USAGE`` и ни одной строки в ``state.db``.

    G3/G4 решаются на прогон (``hound gate``): профиль там не предмет решения, и запись
    аттестации профиля под ними была бы решением о другом предмете.
    """
    package = _new_package(capsys, tmp_path, monkeypatch)
    code, _, err = _attest(capsys, package, gate="G3")

    assert code == EXIT_USAGE
    assert "G3" in err
    assert not _state_db(package).exists()


def test_actor_is_required(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Без ``--actor`` — ``EXIT_USAGE``: аттестация человеческая, анонимной не бывает."""
    package = _new_package(capsys, tmp_path, monkeypatch)
    code, payload, _ = _attest(capsys, package, actor=None)

    assert code == EXIT_USAGE
    # ``--json``: ошибка приходит машинным конвертом (``message``), не в stderr.
    assert "--actor" in str(payload["message"])
    assert not _state_db(package).exists()


def test_bad_attested_at_is_usage_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Кривой ``--attested-at`` — ``EXIT_USAGE``, а не «тихо текущее время»."""
    package = _new_package(capsys, tmp_path, monkeypatch)
    code, payload, _ = _attest(capsys, package, at="01.10.2026 10:00")

    assert code == EXIT_USAGE
    assert "--attested-at" in str(payload["message"])
    assert not _state_db(package).exists()


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------


def test_show_lists_written_attestation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``show --json`` видит записанную строку и называет поля аудита."""
    package = _new_package(capsys, tmp_path, monkeypatch)
    code, attested, err = _attest(capsys, package)
    assert code == EXIT_OK, err

    code, listing = _show(capsys, package)
    assert code == EXIT_OK
    assert listing["found"] is True
    assert listing["exists"] is True
    rows = listing["attestations"]
    assert len(rows) == 1
    row = rows[0]
    assert row["gate"] == Gate.G1.value
    assert row["profile_digest"] == attested["profile_digest"]
    assert row["attestation_id"] == attested["attestation_id"]
    assert row["attested_by"] == _ACTOR
    assert row["attested_at"] == _AT
    assert row["superseded_at"] is None


def test_show_without_attestations_answers_not_found_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    isolated_state_dir: Path,
) -> None:
    """Нет аттестаций — валидный ответ ``found: false``, а файл не создаётся.

    «Профиль ещё не аттестован» — состояние, а не ошибка входа: read-only команда не
    создаёт ``state.db`` (открытие штатным стором применило бы схему, то есть записало).
    """
    package = _new_package(capsys, tmp_path, monkeypatch)
    code, listing = _show(capsys, package)

    assert code == EXIT_OK
    assert listing["found"] is False
    assert listing["exists"] is False
    assert listing["attestations"] == []
    assert not (isolated_state_dir / package.name).exists()


# ---------------------------------------------------------------------------
# Главное: записанная аттестация действительно проходит гейт прогона
# ---------------------------------------------------------------------------


def test_attested_profile_passes_run_gate_and_names_changed_component(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """После ``attest`` прогонный гейт проходит; изменённый профиль — нет, с компонентом.

    Это и есть смысл команды: строка в ``state.db`` должна снимать необходимость
    per-run решения G1, а её расхождение с текущим профилем — называть **что** изменилось
    (иначе человек пере-аттестовывал бы вслепую).
    """
    package = _new_package(capsys, tmp_path, monkeypatch)
    code, attested, err = _attest(capsys, package)
    assert code == EXIT_OK, err

    material = _profile_material_for_package(
        argparse.Namespace(package=str(package), paths=None, passports_dir=None)
    )
    changed = dict(material, seeds_digest="deadbeefdeadbeefdeadbeef")

    with SqliteStateStore(_state_db(package)) as state:
        same = make_profile_gate(state, material)(Gate.G1)
        other = make_profile_gate(state, changed)(Gate.G1)

    assert same.passed is True
    assert same.detail is None
    assert other.passed is False
    assert other.detail is not None
    # Сообщение называет изменившийся компонент и работающую команду пере-аттестации.
    assert "seeds_digest" in other.detail
    assert "hound package profile attest" in other.detail
    assert str(attested["profile_digest"])[:12] in other.detail
