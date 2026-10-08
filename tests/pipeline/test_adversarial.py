"""Тесты adversarial-набора и регрессионного gate (T12.3, R8, §10/02 §C.2).

Проверяют: валидирующий загрузчик набора; что каждый известный класс атаки удерживается
(defended) — DoD «0 известных инъекций»; арифметику отчёта и ``to_manifest_json``;
что :meth:`AdversarialReport.gate` возбуждает :class:`AdversarialBreachError` на
синтетическом breach и молчит на известном наборе; детерминизм; и что реальные
инварианты (I10/I15/I22/§10) действительно стоят за классификацией (в т.ч. злонамеренный
резолвер, подсовывающий инъекционный URL, ловится как breach). Категорийно нейтрально (§1.3).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.domain.anchors import ResolvedSource, SourceResolver
from core.pipeline.adversarial import (
    DEFENSE_STAGES,
    KNOWN_ATTACKS,
    UNTRUSTED_CHUNK_ID,
    AdversarialBreachError,
    AdversarialCase,
    AdversarialReport,
    DefenseResult,
    build_seam_validator,
    check_chunk_is_data,
    check_injection_neutralized,
    load_adversarial,
    run_adversarial,
)
from core.util.hashing import content_hash
from core.util.sanitize import neutralize_prompt_injection_marks

FIXTURES = Path(__file__).parent / "fixtures"
FIXTURE = FIXTURES / "adversarial_prompts.json"

#: Классы-инъекции, у которых ожидается присутствие распознаваемого каркаса (§10).
_INJECTION_ATTACKS = {
    "system_override",
    "xml_tag_injection",
    "markdown_injection",
}


def _cases() -> list[AdversarialCase]:
    return load_adversarial(FIXTURE)


def _case(case_id: str) -> AdversarialCase:
    return next(case for case in _cases() if case.case_id == case_id)


def _write(tmp_path: Path, payload: object) -> Path:
    path = tmp_path / "adversarial.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _valid_item() -> dict:
    return {
        "case_id": "adv_x",
        "injected_text": "нейтральный текст",
        "injected_output": {"status": "❓", "justification": "нет данных", "evidence": []},
        "attack": "system_override",
        "expected_defense": "llm_contract",
    }


# ---------------------------------------------------------------------------
# Загрузчик
# ---------------------------------------------------------------------------


def test_load_adversarial_known_set_covers_all_attacks() -> None:
    cases = _cases()
    assert len(cases) == 9
    assert len({case.case_id for case in cases}) == len(cases)
    assert {case.attack for case in cases} == KNOWN_ATTACKS
    assert all(case.expected_defense in DEFENSE_STAGES for case in cases)


def test_loader_rejects_non_list(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="ожидается JSON-список"):
        load_adversarial(_write(tmp_path, {"case_id": "x"}))


def test_loader_rejects_duplicate_case_id(tmp_path: Path) -> None:
    item = _valid_item()
    with pytest.raises(ValueError, match="дубликат"):
        load_adversarial(_write(tmp_path, [item, dict(item)]))


def test_loader_rejects_missing_required_field(tmp_path: Path) -> None:
    item = _valid_item()
    del item["attack"]
    with pytest.raises(ValueError, match="отсутствуют обязательные поля"):
        load_adversarial(_write(tmp_path, [item]))


def test_loader_rejects_unknown_attack(tmp_path: Path) -> None:
    item = _valid_item()
    item["attack"] = "not_a_real_attack"
    with pytest.raises(ValueError, match="attack"):
        load_adversarial(_write(tmp_path, [item]))


def test_loader_rejects_unknown_defense_stage(tmp_path: Path) -> None:
    item = _valid_item()
    item["expected_defense"] = "firewall"
    with pytest.raises(ValueError, match="expected_defense"):
        load_adversarial(_write(tmp_path, [item]))


def test_loader_rejects_non_object_output(tmp_path: Path) -> None:
    item = _valid_item()
    item["injected_output"] = "не объект"
    with pytest.raises(ValueError, match="injected_output"):
        load_adversarial(_write(tmp_path, [item]))


# ---------------------------------------------------------------------------
# DoD R8: на известном наборе 0 инъекций
# ---------------------------------------------------------------------------


def test_known_set_has_zero_breaches() -> None:
    report = run_adversarial(_cases())
    assert report.total == report.defended == 9
    assert report.breached == 0
    assert report.breach_ids == []
    assert report.pass_rate == 1.0
    assert all(result.defended for result in report.results)


def test_each_attack_is_defended_by_declared_layer() -> None:
    report = run_adversarial(_cases())
    by_id = {result.case_id: result for result in report.results}
    assert by_id["adv_source_id_not_in_chunks"].stage == "echelon"
    assert by_id["adv_quote_not_substring"].stage == "echelon"
    assert by_id["adv_empty_quote"].stage == "echelon"
    assert by_id["adv_invalid_schema"].stage == "schema"
    for case_id in ("adv_system_override", "adv_xml_tag_injection", "adv_template_injection"):
        assert by_id[case_id].stage == "llm_contract", case_id


def test_gate_passes_on_known_set() -> None:
    run_adversarial(_cases()).gate()  # не должно возбуждать


# ---------------------------------------------------------------------------
# Арифметика отчёта и gate (синтетический breach)
# ---------------------------------------------------------------------------


def test_gate_raises_on_synthetic_breach() -> None:
    def breaker(case, chunks, output):  # noqa: ANN001
        return DefenseResult(False, "echelon", "synthetic breach")

    # Схемный кейс отвергается до вызова validator — берём кейс, доходящий до шва.
    report = run_adversarial([_case("adv_quote_not_substring")], validator=breaker)
    assert report.breached == report.total == 1
    with pytest.raises(AdversarialBreachError, match="R8"):
        report.gate()


def test_report_math_mixed_breach() -> None:
    target = "adv_empty_quote"

    def selective(case, chunks, output):  # noqa: ANN001
        if case.case_id == target:
            return DefenseResult(False, "echelon", "synthetic")
        return DefenseResult(True, "echelon", "ok")

    report = run_adversarial(_cases(), validator=selective)
    assert report.total == 9
    assert report.defended == 8
    assert report.breached == 1
    assert report.breach_ids == [target]
    assert report.pass_rate == pytest.approx(8 / 9)


def test_empty_report_is_deterministic_and_passes_gate() -> None:
    report = AdversarialReport()
    assert report.total == 0
    assert report.pass_rate == 0.0
    report.gate()


def test_injected_validator_receives_built_chunk_and_output() -> None:
    seen: dict = {}

    def spy(case, chunks, output):  # noqa: ANN001
        seen["case_id"] = case.case_id
        seen["chunk_id"] = next(iter(chunks))
        seen["quote"] = output.evidence[0].quote if output.evidence else None
        return DefenseResult(True, "echelon", "spy")

    run_adversarial([_case("adv_quote_not_substring")], validator=spy)
    assert seen["case_id"] == "adv_quote_not_substring"
    assert seen["chunk_id"] == UNTRUSTED_CHUNK_ID
    assert seen["quote"] == "экспорт в произвольный формат без ограничений"


# ---------------------------------------------------------------------------
# Манифест и детерминизм
# ---------------------------------------------------------------------------


def test_to_manifest_json_is_parseable() -> None:
    report = run_adversarial(_cases())
    payload = json.loads(report.to_manifest_json())
    assert payload["total"] == 9
    assert payload["breached"] == 0
    assert payload["pass_rate"] == 1.0
    assert payload["breach_ids"] == []
    assert len(payload["results"]) == 9


def test_manifest_json_is_deterministic() -> None:
    first = run_adversarial(_cases()).to_manifest_json()
    second = run_adversarial(_cases()).to_manifest_json()
    assert first == second


# ---------------------------------------------------------------------------
# Реальные инварианты за классификацией
# ---------------------------------------------------------------------------


def test_url_in_quote_not_promoted_with_benign_resolver() -> None:
    case = _case("adv_url_in_quote")
    resolver = _resolver_for(case, "https://docs.example.invalid/real-source")
    report = run_adversarial(
        [case], validator=build_seam_validator(resolver=resolver)
    )
    assert report.defended == 1


def test_url_in_quote_malicious_resolver_is_breach() -> None:
    """Если резолвер (вопреки I10/I15) вернёт URL из цитаты — это breach, а не защита."""
    case = _case("adv_url_in_quote")
    injected_url = "https://evil.example.invalid/leak"
    resolver = _resolver_for(case, injected_url)
    report = run_adversarial(
        [case], validator=build_seam_validator(resolver=resolver)
    )
    assert report.breached == 1
    assert report.breach_ids == [case.case_id]


def test_data_boundary_holds_for_every_case() -> None:
    for case in _cases():
        result = check_chunk_is_data(case)
        assert result.defended, f"{case.case_id}: {result.detail}"


def test_injection_scaffolds_are_neutralized() -> None:
    for case in _cases():
        result = check_injection_neutralized(case.injected_text)
        assert result.defended, f"{case.case_id}: {result.detail}"
    # для инъекционных кейсов каркас действительно был распознан и заменён маркером
    marker = "[injection-scaffold-removed]"
    for case in _cases():
        if case.attack in _INJECTION_ATTACKS:
            sanitized = neutralize_prompt_injection_marks(case.injected_text)
            assert marker in sanitized, case.case_id


def test_data_boundary_holds_with_alternate_product_id() -> None:
    """Структурная проверка не зависит от product_id: граница данных держится всегда."""
    case = _case("adv_template_injection")
    result = check_chunk_is_data(case, product_id="")
    assert result.defended


def _resolver_for(case: AdversarialCase, source_url: str) -> SourceResolver:
    """Детерминированный фейковый резолвер для одного недоверенного чанка (без сети)."""
    resolved = ResolvedSource(
        chunk_id=UNTRUSTED_CHUNK_ID,
        source_url=source_url,
        content_hash=content_hash(case.injected_text),
        build_id="build-adversarial",
    )

    class _Resolver:
        def resolve(self, source_id: str) -> ResolvedSource | None:
            return resolved if source_id == UNTRUSTED_CHUNK_ID else None

    return _Resolver()
