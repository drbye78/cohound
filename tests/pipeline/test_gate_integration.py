"""Точки интеграции G3/G4: предмет решения, запрос наружу, вердикт, waiver (S9, I6/I7).

G1/G2 — внутренняя аттестация (профиль, матрица роль×продукт). G3/G4 — **точки
интеграции**: strict-ревью и утверждение публикации выполняет внешняя система, coHound
экспортирует предмет решения и записывает ответ.

Покрывается:

* отпечаток предмета (G3 — очередь ревью, G4 — публикуемое содержимое) и его реакция на
  изменение состояния;
* файл запроса ``runs/{run_id}/gates/{gate}-request.json`` — детерминированный, с тем же
  ``subject_digest``, который обязано нести решение;
* внешнее одобрение и **внешний отказ** (включая отказ по устаревшему предмету);
* устаревшее одобрение (предмет изменился) с указанием изменившегося компонента;
* waiver (``run.yaml:gates.<gate>=waived``) — ``skipped`` + запись делегирования;
* обратную совместимость решений без привязки (legacy-строки);
* политику ``run.yaml:gates`` и её отсутствие в контракте исполнения.

Данные категорийно-нейтральны (§1.3): ``p1``/``p2``, ``param_a``, ``mod_a``.
"""

from __future__ import annotations

import datetime as _dt
import json
from collections.abc import Iterator
from pathlib import Path

import pytest

from core.domain import (
    CatalogRow,
    CellState,
    Focus,
    Gate,
    GatePolicy,
    GateSource,
    GateVerdict,
    Importance,
    ParamType,
    RunConfig,
    RunMode,
    SkipReason,
    Status,
)
from core.pipeline.compute import compute
from core.pipeline.gate_subject import (
    GateSubject,
    publication_subject,
    read_gate_request_digest,
    read_gate_request_subject,
    review_subject,
    write_gate_request,
)
from core.pipeline.gates import evaluate_gate, gate_g3, gate_g4, record_decision
from core.pipeline.review import ReviewItem
from core.pipeline.runner import GateRuntime, Ports, RunContext, _gate_phase
from core.store.sqlite_store import SqliteStore

RUN_ID = "run.g"


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case_x",
        anchor="p1",
        cohort="cohort_x",
        products=["p1", "p2"],
        package="case_x",
        focus=Focus(),
        snapshot_date=_dt.date(2026, 10, 1),
        mode=RunMode.STRICT,
    )


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    with SqliteStore(":memory:") as s:
        s.create_run(RUN_ID, _cfg())
        yield s


def _ctx(store: SqliteStore) -> RunContext:
    return RunContext(run_id=RUN_ID, store=store, ports=Ports())


def _item(
    product_id: str = "p1",
    param_id: str = "param_a",
    *,
    state: CellState = CellState.EVALUATED,
    status: Status | None = Status.UNKNOWN,
    reason: str = "strict-unknown: нужен человек",
) -> ReviewItem:
    return ReviewItem(
        product_id=product_id,
        param_id=param_id,
        state=state,
        status=status,
        reason=reason,
        evidence_count=0,
        index_build_id=None,
    )


def _catalog() -> dict[str, CatalogRow]:
    return {
        "param_a": CatalogRow(
            param_id="param_a",
            title="Параметр A",
            module="mod_a",
            group="grp",
            importance=Importance.IMPORTANT,
            param_type=ParamType.FACT,
            rule="правило",
        )
    }


def _bundle(store: SqliteStore, *, ranking: str = "capability_ranked"):
    return compute(
        store=store,
        run_id=RUN_ID,
        catalog=_catalog(),
        module_weights={"mod_a": 100.0},
        requested_ranking=ranking,
    )


def _g3_subject(items: list[ReviewItem] | None = None) -> GateSubject:
    return review_subject(items if items is not None else [_item()], run_id=RUN_ID)


def _runtime(
    subject: GateSubject | None,
    *,
    policies: dict[str, GatePolicy] | None = None,
    runs_dir: Path | None = None,
) -> GateRuntime:
    return GateRuntime(
        subject=lambda gate: subject if subject is not None and gate is subject.gate else None,
        policies=policies or {},
        runs_dir=runs_dir,
    )


# ---------------------------------------------------------------------------
# Предмет решения: отпечаток обязан следовать за состоянием
# ---------------------------------------------------------------------------


def test_g3_subject_digest_changes_with_the_queue() -> None:
    """Добавление ячейки в очередь ревью меняет отпечаток предмета."""
    base = _g3_subject([_item()])
    extra = _g3_subject([_item(), _item("p2", "param_b")])

    assert base.digest != extra.digest
    assert base.material["summary"]["total"] == 1
    assert extra.material["summary"]["total"] == 2
    # Отпечаток не зависит от порядка входа: очередь — множество, не список.
    assert _g3_subject([_item(), _item("p2", "param_b")]).digest == extra.digest


def test_g3_subject_digest_changes_with_the_status() -> None:
    """Смена статуса ячейки — другое состояние ревью, то есть другой предмет."""
    unknown = _g3_subject([_item(status=Status.UNKNOWN)])
    partial = _g3_subject([_item(status=Status.PARTIAL)])
    assert unknown.digest != partial.digest


def test_g4_subject_covers_publication_content(store: SqliteStore) -> None:
    """Предмет G4 — Coverage/Capability/Comparability/ранжирование и состав артефактов."""
    subject = publication_subject(_bundle(store), run_id=RUN_ID)

    assert subject.gate is Gate.G4
    assert set(subject.material) == {
        "run_id",
        "coverage",
        "capability",
        "modules",
        "comparability",
        "ranking",
        "failure_rate",
        "publish",
    }
    assert subject.material["coverage"]["defined"] in (True, False)
    assert "content_digest" in subject.material["publish"]
    names = [entry["name"] for entry in subject.material["publish"]["artifacts"]]
    assert "battle_card.xlsx" in names
    # Экспорт для внешней системы несёт то же, что и материал.
    assert subject.export["content_digest"] == subject.material["publish"]["content_digest"]


def test_g4_subject_digest_changes_with_ranking(store: SqliteStore) -> None:
    """Смена режима ранжирования меняет предмет публикации."""
    first = publication_subject(_bundle(store), run_id=RUN_ID)
    second = publication_subject(
        _bundle(store, ranking="coverage_fallback"), run_id=RUN_ID
    )
    assert first.digest != second.digest
    assert first.material["ranking"] != second.material["ranking"]


# ---------------------------------------------------------------------------
# Запрос на решение: детерминированный файл с отпечатком предмета
# ---------------------------------------------------------------------------


def test_blocked_gate_writes_deterministic_request(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Блок пишет ``gates/G3-request.json``: побайтно стабильный, с ``subject_digest``."""
    subject = _g3_subject()
    phase = _gate_phase("g3", Gate.G3, _runtime(subject, runs_dir=tmp_path))

    first = phase(_ctx(store))
    assert first.status == "blocked"
    path = tmp_path / RUN_ID / "gates" / "G3-request.json"
    assert path.is_file()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["gate"] == "G3"
    assert payload["run_id"] == RUN_ID
    assert payload["subject_digest"] == subject.digest
    assert payload["subject"] == subject.material
    assert payload["decision_contract"]["verdicts"] == ["approved", "rejected"]
    assert "external_ref" in payload["decision_contract"]["required_fields"]
    # Меток времени в запросе нет: тот же прогон в том же состоянии — те же байты.
    first_bytes = path.read_bytes()
    assert phase(_ctx(store)).status == "blocked"
    assert path.read_bytes() == first_bytes
    # Отпечаток читается обратно — именно его возвращает внешняя система.
    assert read_gate_request_digest(tmp_path, RUN_ID, Gate.G3) == subject.digest
    assert read_gate_request_subject(tmp_path, RUN_ID, Gate.G3) == subject.material


def test_blocked_gate_names_the_request_and_the_command(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Сообщение блока действенно: где запрос и какой командой ответить."""
    subject = _g3_subject()
    result = _gate_phase("g3", Gate.G3, _runtime(subject, runs_dir=tmp_path))(_ctx(store))

    message = "\n".join(result.errors)
    assert "нет строки в gate_decisions" in message
    assert "G3-request.json" in message
    assert f"--subject-digest {subject.digest}" in message
    assert "--verdict approved|rejected" in message


# ---------------------------------------------------------------------------
# Внешние решения: одобрение, отказ, устаревание
# ---------------------------------------------------------------------------


def test_external_approval_of_current_subject_passes(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Внешнее одобрение с совпадающим отпечатком проходит гейт и видно в аудите."""
    subject = _g3_subject()
    decision = record_decision(
        store,
        RUN_ID,
        Gate.G3,
        actor="review-system",
        notes="ревью пройдено",
        subject_digest=subject.digest,
        source=GateSource.EXTERNAL,
        external_ref="REV-17",
        verdict=GateVerdict.APPROVED,
    )
    assert decision.source is GateSource.EXTERNAL
    assert decision.external_ref == "REV-17"

    result = _gate_phase("g3", Gate.G3, _runtime(subject, runs_dir=tmp_path))(_ctx(store))
    assert result.status == "ok", result.errors


def test_external_rejection_of_current_subject_blocks_with_refusal(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Отказ по текущему предмету блокирует гейт и называет систему и причину."""
    subject = _g3_subject()
    record_decision(
        store,
        RUN_ID,
        Gate.G3,
        actor="review-system",
        notes="две ячейки не подтверждены источником",
        subject_digest=subject.digest,
        source=GateSource.EXTERNAL,
        external_ref="REV-18",
        verdict=GateVerdict.REJECTED,
    )

    result = _gate_phase("g3", Gate.G3, _runtime(subject, runs_dir=tmp_path))(_ctx(store))
    assert result.status == "blocked"
    message = "\n".join(result.errors)
    # Это не «нет решения»: отказ назван явно, с системой и причиной.
    assert "отклонён внешней системой" in message
    assert "REV-18" in message
    assert "две ячейки не подтверждены источником" in message
    assert "нет строки в gate_decisions" not in message


def test_external_rejection_of_stale_subject_says_it_is_stale(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Отказ по устаревшему предмету блокирует, но сообщает, что ревью было о прошлом."""
    old = _g3_subject([_item()])
    record_decision(
        store,
        RUN_ID,
        Gate.G3,
        actor="review-system",
        notes="старый состав",
        subject_digest=old.digest,
        source=GateSource.EXTERNAL,
        external_ref="REV-19",
        verdict=GateVerdict.REJECTED,
    )
    write_gate_request(tmp_path, RUN_ID, old)
    current = _g3_subject([_item(), _item("p2", "param_b")])

    result = _gate_phase(
        "g3", Gate.G3, _runtime(current, runs_dir=tmp_path)
    )(_ctx(store))
    assert result.status == "blocked"
    message = "\n".join(result.errors)
    assert "относится к устаревшему состоянию" in message
    assert "REV-19" in message
    assert "Повторное ревью" in message


def test_stale_approval_blocks_and_names_the_changed_component(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Одобрение прошлого предмета не действует: сообщение называет, что изменилось."""
    approved_state = _g3_subject([_item()])
    write_gate_request(tmp_path, RUN_ID, approved_state)
    record_decision(
        store,
        RUN_ID,
        Gate.G3,
        actor="review-system",
        notes="одобрено",
        subject_digest=approved_state.digest,
        source=GateSource.EXTERNAL,
        external_ref="REV-20",
    )

    current = _g3_subject([_item(), _item("p2", "param_b")])
    result = _gate_phase(
        "g3", Gate.G3, _runtime(current, runs_dir=tmp_path)
    )(_ctx(store))

    assert result.status == "blocked"
    message = "\n".join(result.errors)
    assert "устаревшему состоянию предмета" in message
    assert "cells" in message
    assert "param_b" in message
    # Запрос перезаписан текущим состоянием: следующее решение относится к нему.
    assert read_gate_request_digest(tmp_path, RUN_ID, Gate.G3) == current.digest


def test_legacy_unbound_decision_still_passes(store: SqliteStore, tmp_path: Path) -> None:
    """Решение без привязки (legacy-строка) продолжает проходить гейт."""
    gate_g3(store, RUN_ID, actor="human-legacy")
    subject = _g3_subject()
    result = _gate_phase("g3", Gate.G3, _runtime(subject, runs_dir=tmp_path))(_ctx(store))
    assert result.status == "ok", result.errors


def test_evaluate_gate_distinguishes_all_outcomes(store: SqliteStore) -> None:
    """``evaluate_gate``: нет решения / устарело / отказ / отказ по прошлому / пройдено."""
    assert evaluate_gate(store, RUN_ID, Gate.G4, "d1").status == "missing"

    gate_g4(store, RUN_ID, actor="h", subject_digest="d1")
    assert evaluate_gate(store, RUN_ID, Gate.G4, "d1").status == "passed"
    assert evaluate_gate(store, RUN_ID, Gate.G4, "d2").status == "stale"

    record_decision(
        store,
        RUN_ID,
        Gate.G4,
        actor="pub-system",
        notes="нет покрытия",
        subject_digest="d3",
        source=GateSource.EXTERNAL,
        external_ref="PUB-1",
        verdict=GateVerdict.REJECTED,
    )
    check = evaluate_gate(store, RUN_ID, Gate.G4, "d3")
    assert check.status == "rejected"
    assert check.decision is not None and check.decision.external_ref == "PUB-1"
    assert evaluate_gate(store, RUN_ID, Gate.G4, "d1").status == "passed"
    assert evaluate_gate(store, RUN_ID, Gate.G4, "d4").status == "rejected_stale"


def test_external_decision_requires_ref_and_notes(store: SqliteStore) -> None:
    """Внешнее решение без ссылки или без причины — не аудит-след (отказ модели)."""
    with pytest.raises(ValueError, match="external_ref"):
        record_decision(
            store,
            RUN_ID,
            Gate.G3,
            actor="sys",
            notes="причина",
            source=GateSource.EXTERNAL,
        )
    with pytest.raises(ValueError, match="notes"):
        record_decision(
            store,
            RUN_ID,
            Gate.G3,
            actor="sys",
            external_ref="REV-1",
            source=GateSource.EXTERNAL,
        )


# ---------------------------------------------------------------------------
# Waiver: делегирование видно, а не подразумевается
# ---------------------------------------------------------------------------


def test_waived_gate_is_skipped_and_records_the_delegation(
    store: SqliteStore, tmp_path: Path
) -> None:
    """``waived`` → ``skipped`` с причиной + waiver-строка (идемпотентно)."""
    subject = _g3_subject()
    runtime = _runtime(
        subject, policies={"G3": GatePolicy.WAIVED}, runs_dir=tmp_path
    )
    phase = _gate_phase("g3", Gate.G3, runtime)

    result = phase(_ctx(store))
    assert result.status == "skipped"
    assert result.skip_reason is SkipReason.GATE_DELEGATED_EXTERNAL
    assert "делегирован" in "\n".join(result.log_entries)

    rows = [d for d in store.iter_gate_decisions(RUN_ID) if d.gate is Gate.G3]
    assert len(rows) == 1
    assert rows[0].source is GateSource.EXTERNAL
    assert rows[0].external_ref == "waiver:run.yaml:gates.g3"
    assert "внутрисистемного одобрения нет" in (rows[0].notes or "")

    # Повторный resume не плодит waiver-строк.
    assert phase(_ctx(store)).status == "skipped"
    assert len([d for d in store.iter_gate_decisions(RUN_ID) if d.gate is Gate.G3]) == 1
    # Запрос наружу не пишется: решение не запрашивается, оно делегировано целиком.
    assert not (tmp_path / RUN_ID / "gates" / "G3-request.json").exists()


def test_waiver_is_not_available_for_internal_gates(store: SqliteStore) -> None:
    """``waived`` для G1/G2 не действует: аттестацию профиля нельзя «делегировать»."""
    runtime = GateRuntime(
        subject=lambda gate: None, policies={"G1": GatePolicy.WAIVED}
    )
    result = _gate_phase("g1", Gate.G1, runtime)(_ctx(store))
    assert result.status == "blocked"


def test_run_config_accepts_only_delegable_gate_policies() -> None:
    """``run.yaml:gates`` — только ``g3``/``g4`` и только ``external``/``waived``."""
    cfg = RunConfig(
        case_id="case_x",
        anchor="p1",
        cohort="cohort_x",
        products=["p1"],
        package="case_x",
        snapshot_date=_dt.date(2026, 10, 1),
        mode=RunMode.STRICT,
        gates={"g3": "waived", "g4": "external"},
    )
    assert cfg.gates == {"g3": GatePolicy.WAIVED, "g4": GatePolicy.EXTERNAL}

    with pytest.raises(ValueError, match="gates"):
        RunConfig(
            case_id="case_x",
            anchor="p1",
            cohort="cohort_x",
            products=["p1"],
            package="case_x",
            snapshot_date=_dt.date(2026, 10, 1),
            mode=RunMode.STRICT,
            gates={"g1": "waived"},
        )
    with pytest.raises(ValueError):
        RunConfig(
            case_id="case_x",
            anchor="p1",
            cohort="cohort_x",
            products=["p1"],
            package="case_x",
            snapshot_date=_dt.date(2026, 10, 1),
            mode=RunMode.STRICT,
            gates={"g3": "delegated"},
        )


def test_gate_policies_are_not_part_of_execution_contract() -> None:
    """Политика гейтов — процесс, не условие измерения: в контракт не входит (§13)."""
    from core.pipeline.digests import EXECUTION_CONTRACT_FIELDS

    assert not [name for name in EXECUTION_CONTRACT_FIELDS if "gate" in name]


def test_waiver_row_does_not_pass_the_gate_when_policy_is_external(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Делегирование ≠ одобрение: waiver-строка не проходит гейт при ``external``."""
    subject = _g3_subject()
    waived = _gate_phase(
        "g3",
        Gate.G3,
        _runtime(subject, policies={"G3": GatePolicy.WAIVED}, runs_dir=tmp_path),
    )
    assert waived(_ctx(store)).status == "skipped"

    # Политика снова ``external`` (например, процесс перестал делегировать): факт
    # делегирования не превращается в одобрение ревью.
    external = _gate_phase("g3", Gate.G3, _runtime(subject, runs_dir=tmp_path))
    result = external(_ctx(store))
    assert result.status == "blocked"
    assert evaluate_gate(store, RUN_ID, Gate.G3, subject.digest).status == "missing"


def test_manifest_records_policies_and_waivers(store: SqliteStore) -> None:
    """Манифест показывает делегирование: прогон без одобрения ревью не выглядит одобренным."""
    from types import SimpleNamespace

    from core.pipeline.phases import _gate_provenance

    _gate_phase(
        "g3",
        Gate.G3,
        _runtime(_g3_subject(), policies={"G3": GatePolicy.WAIVED}),
    )(_ctx(store))
    provenance = _gate_provenance(
        _ctx(store),
        SimpleNamespace(run_id=RUN_ID),
        SimpleNamespace(run=SimpleNamespace(gates={"g3": GatePolicy.WAIVED})),
    )
    assert provenance["gate_policies"] == {"g3": "waived"}
    assert [item["gate"] for item in provenance["gate_waivers"]] == ["G3"]
    assert provenance["gate_waivers"][0]["external_ref"] == "waiver:run.yaml:gates.g3"


def test_run_manifest_serializes_gate_waivers(tmp_path: Path) -> None:
    """``run_manifest.yaml`` несёт ``gate_waivers``: артефакт честен без чтения БД."""
    import yaml

    from core.domain import RunManifest
    from core.pipeline.report import write_run_manifest

    manifest = RunManifest(
        run_id=RUN_ID,
        case_id="case_x",
        created_at="2026-10-01T00:00:00Z",
        mode=RunMode.STRICT,
        methodology_version="2.4.0",
        package_version="0.1.0",
        catalog_version="0.1.0",
        status_dict_version="1.0.0",
        scoring_policy_version="1.0.0",
        anchor_product_id="p1",
        gate_policies={"g3": "waived"},
        gate_waivers=[
            {
                "gate": "G3",
                "external_ref": "waiver:run.yaml:gates.g3",
                "actor": "external-process",
                "notes": "внутрисистемного одобрения нет",
                "decided_at": "2026-10-01T00:00:00Z",
            }
        ],
    )
    path = write_run_manifest(manifest, tmp_path)
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert data["gate_policies"] == {"g3": "waived"}
    assert data["gate_waivers"][0]["gate"] == "G3"
