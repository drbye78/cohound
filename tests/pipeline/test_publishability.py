"""Профиль публикуемости (A1, §6): разложение неизвестности и класс карты.

Проверяется главное требование владельца — «не ограничивать, а измерять»: класс
выводится из **измеренных** чисел, рядом с классом публикуется разложение по причинам, а
сумма разложения объясняет знаменатель Coverage (инвариант, а не совпадение одной цифры).

Покрытие:

* каждый из четырёх классов достижим по построению (искусственные ячейки);
* граница порога: ровно на пороге, ε выше, ε ниже;
* ``Σ causes + measured_weight == coverage.denominator`` (инвариант разложения);
* ``N/A`` (I7) и ``pending``/``preliminary`` (I13) — вне знаменателя и отдельными бакетами;
* ``❌`` никогда не попадает в «неизвестность» — это опубликованное «нет»;
* ``bounded``-«нет» считается в ``weak_denials``, ``explicit``/``authoritative`` — нет;
* согласованность перечислений со словарём ``docs/status-dictionary.yaml``;
* живой прогон (read-only) — класс и разложение воспроизводятся детерминированно.

Категорийная нейтральность (§1.3): фикстуры синтетические, продуктов из живого прогона
в утверждениях нет — только в тесте воспроизведения, который читает прогон по пути.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import yaml

from core.domain.enums import (
    CellState,
    DenialStrength,
    ExcludedCellClass,
    Importance,
    PublishabilityClass,
    RunMode,
    Status,
    UnknownCause,
)
from core.domain.models import CatalogRow, Cell, EvidenceScope, LLMEvidence
from core.domain.scoring import (
    DEFAULT_UNKNOWN_SHARE_THRESHOLD,
    evidence_coverage,
    publishability_profile,
)
from core.pipeline.compute import (
    assert_publishable,
    compute,
    publish_warnings,
    scope_universe,
)

ROOT = Path(__file__).parents[2]
RUN_ID = "run-publishability"
PRODUCT_A = "prod.a"
PRODUCT_B = "prod.b"


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _row(param_id: str) -> CatalogRow:
    """Положительная строка каталога (вес 2.0 — важность ``Баз``)."""
    return CatalogRow(
        param_id=param_id,
        title=param_id,
        module="mod.one",
        group="grp",
        importance=Importance.BASIC,
        param_type="fact",
        rule="правило",
    )


def _catalog(count: int) -> dict[str, CatalogRow]:
    return {f"p{index:03d}": _row(f"p{index:03d}") for index in range(count)}


def _ok(product_id: str, param_id: str) -> Cell:
    return Cell(
        run_id=RUN_ID,
        product_id=product_id,
        param_id=param_id,
        state=CellState.EVALUATED,
        status=Status.OK,
        evidence=[LLMEvidence(source_id="src.1", quote="цитата")],
    )


def _unknown(
    product_id: str, param_id: str, strength: DenialStrength | None
) -> Cell:
    """``❓`` с указанной силой отрицания; ``None`` — историческая строка без ``EvidenceScope``."""
    scope = None
    if strength is not None:
        scope = EvidenceScope(denial_strength=strength, reason="наблюдённая причина")
    return Cell(
        run_id=RUN_ID,
        product_id=product_id,
        param_id=param_id,
        state=CellState.EVALUATED,
        status=Status.UNKNOWN,
        evidence_scope=scope,
    )


def _absent(product_id: str, param_id: str, strength: DenialStrength) -> Cell:
    """``❌`` с заданной силой отрицания (I2: ``❌`` без основания не публикуется)."""
    if strength is DenialStrength.EXPLICIT:
        scope = EvidenceScope(
            denial_strength=strength, citation_quote="функция не поддерживается"
        )
    elif strength is DenialStrength.BOUNDED:
        scope = EvidenceScope(
            denial_strength=strength,
            reason="конечный справочник, процедура полноты объявлена",
            scope_acceptance="reviewer",
        )
    else:
        scope = EvidenceScope(
            denial_strength=strength,
            enumerated=True,
            reason="реестровая энумерация полна по построению",
        )
    return Cell(
        run_id=RUN_ID,
        product_id=product_id,
        param_id=param_id,
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        absence_evidence=[{"rule": "explicit"}],
        evidence_scope=scope,
    )


def _na(product_id: str, param_id: str) -> Cell:
    return Cell(
        run_id=RUN_ID,
        product_id=product_id,
        param_id=param_id,
        state=CellState.EVALUATED,
        status=Status.NOT_APPLICABLE,
        reviewed_by="human",
    )


def _failed(product_id: str, param_id: str) -> Cell:
    return Cell(
        run_id=RUN_ID,
        product_id=product_id,
        param_id=param_id,
        state=CellState.FAILED,
        evidence_scope=EvidenceScope.for_instrument_failure("evaluation_failed"),
    )


def _pending(product_id: str, param_id: str) -> Cell:
    return Cell(
        run_id=RUN_ID,
        product_id=product_id,
        param_id=param_id,
        state=CellState.PENDING,
    )


def _preliminary(product_id: str, param_id: str) -> Cell:
    return Cell(
        run_id=RUN_ID,
        product_id=product_id,
        param_id=param_id,
        state=CellState.PRELIMINARY,
    )


def _profile(
    cells: list[Cell],
    catalog: dict[str, CatalogRow],
    *,
    universe: set[tuple[str, str]] | None = None,
    **overrides: object,
):
    """Считает Coverage и профиль по **одному** входу (как это делает ``compute``)."""
    coverage = evidence_coverage(cells, catalog, {}, universe=universe)
    profile = publishability_profile(
        cells,
        catalog,
        {},
        coverage=coverage,
        universe=universe,
        **overrides,  # type: ignore[arg-type]
    )
    return profile, coverage


# ---------------------------------------------------------------------------
# 1. Четыре класса достижимы по построению
# ---------------------------------------------------------------------------


def test_publishable_is_reachable() -> None:
    """Два продукта, всё измерено, противоречий нет → ``PUBLISHABLE``."""
    catalog = _catalog(4)
    params = sorted(catalog)
    universe = scope_universe(catalog, [PRODUCT_A, PRODUCT_B])
    cells = [_ok(product, param) for product in (PRODUCT_A, PRODUCT_B) for param in params]

    profile, coverage = _profile(
        cells, catalog, universe=universe, comparability_level="HIGH"
    )
    assert coverage.value == pytest.approx(1.0)
    assert profile.klass == PublishabilityClass.PUBLISHABLE.value
    assert profile.unknown_share == 0.0
    assert profile.caveats == []
    assert profile.dominant_cause is None


def test_publishable_with_caveat_is_reachable() -> None:
    """Однопродуктовая когорта — карта публикуется, но оговорка обязана быть названа."""
    catalog = _catalog(3)
    params = sorted(catalog)
    universe = scope_universe(catalog, [PRODUCT_A])
    cells = [_ok(PRODUCT_A, param) for param in params]

    profile, _ = _profile(
        cells, catalog, universe=universe, comparability_level="HIGH"
    )
    assert profile.klass == PublishabilityClass.PUBLISHABLE_WITH_CAVEAT.value
    assert profile.caveats == ["single_product_cohort"]
    assert "single_product_cohort" in profile.banner


def test_analytical_only_is_reachable() -> None:
    """Доля неизвестности выше порога: Capability есть, сравнение не публикуется."""
    catalog = _catalog(10)
    params = sorted(catalog)
    universe = scope_universe(catalog, [PRODUCT_A])
    cells = [_ok(PRODUCT_A, param) for param in params[:4]]
    cells += [
        _unknown(PRODUCT_A, param, DenialStrength.NOT_STATED) for param in params[4:]
    ]

    profile, coverage = _profile(cells, catalog, universe=universe)
    assert coverage.value == pytest.approx(0.4)
    assert profile.unknown_share == pytest.approx(0.6)
    assert profile.klass == PublishabilityClass.ANALYTICAL_ONLY.value
    assert profile.reasons == [
        "unknown_share_above_threshold",
        "coverage_below_floor",
    ]
    assert profile.dominant_cause == UnknownCause.NOT_STATED.value
    # Числа рядом с классом: порог, доля, доминирующая причинная доля.
    assert profile.unknown_share_threshold == DEFAULT_UNKNOWN_SHARE_THRESHOLD
    assert profile.causes[UnknownCause.NOT_STATED.value].share == pytest.approx(0.6)
    assert "60.0%" in profile.banner and "not_stated" in profile.banner


def test_not_comparable_when_coverage_undefined() -> None:
    """Пустая вселенная scope → ``UNDEFINED`` → сравнивать нечего."""
    catalog = _catalog(2)
    profile, coverage = _profile([], catalog, universe=set())
    assert coverage.defined is False
    assert profile.klass == PublishabilityClass.NOT_COMPARABLE.value
    assert "coverage_undefined" in profile.reasons
    assert "UNDEFINED" in profile.banner


def test_not_comparable_when_nothing_measured() -> None:
    """Ячейки есть, но ни одного измерения: ``❓``/``failed`` не делают карту сравнением."""
    catalog = _catalog(2)
    params = sorted(catalog)
    universe = scope_universe(catalog, [PRODUCT_A])
    cells = [
        _unknown(PRODUCT_A, params[0], DenialStrength.UNOBSERVED),
        _failed(PRODUCT_A, params[1]),
    ]
    profile, coverage = _profile(cells, catalog, universe=universe)
    assert coverage.defined is True  # знаменатель есть, измерений нет
    assert profile.klass == PublishabilityClass.NOT_COMPARABLE.value
    assert profile.reasons == ["no_measured_cells"]


# ---------------------------------------------------------------------------
# 2. Граница порога
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("unknown", "expected"),
    [
        (29, PublishabilityClass.PUBLISHABLE.value),  # ε ниже порога
        (30, PublishabilityClass.PUBLISHABLE.value),  # ровно на пороге
        (31, PublishabilityClass.ANALYTICAL_ONLY.value),  # ε выше порога
    ],
)
def test_threshold_boundary(unknown: int, expected: str) -> None:
    """Порог — граница измерения: ровно 30 % ещё публикуемо, 31 % — уже нет."""
    catalog = _catalog(100)
    params = sorted(catalog)
    universe = scope_universe(catalog, [PRODUCT_A, PRODUCT_B])
    cells = []
    for product in (PRODUCT_A, PRODUCT_B):
        cells += [_ok(product, param) for param in params[: 100 - unknown]]
        cells += [
            _unknown(product, param, DenialStrength.NOT_STATED)
            for param in params[100 - unknown :]
        ]

    profile, coverage = _profile(
        cells, catalog, universe=universe, comparability_level="HIGH"
    )
    assert profile.unknown_share == pytest.approx(unknown / 100)
    assert coverage.value == pytest.approx(1 - unknown / 100)
    assert profile.klass == expected


# ---------------------------------------------------------------------------
# 3. Инвариант: разложение объясняет знаменатель
# ---------------------------------------------------------------------------


def test_causes_explain_denominator() -> None:
    """``Σ causes + measured == coverage.denominator`` — по каждому продукту и в сумме."""
    catalog = _catalog(8)
    params = sorted(catalog)
    universe = scope_universe(catalog, [PRODUCT_A, PRODUCT_B])
    cells = [
        # prod.a: измерено/❓/failed
        _ok(PRODUCT_A, params[0]),
        _absent(PRODUCT_A, params[1], DenialStrength.EXPLICIT),
        _unknown(PRODUCT_A, params[2], DenialStrength.NOT_STATED),
        _unknown(PRODUCT_A, params[3], DenialStrength.UNOBSERVED),
        _failed(PRODUCT_A, params[4]),
        # prod.b: N/A (вне знаменателя), preliminary (вне знаменателя), pending
        _na(PRODUCT_B, params[0]),
        _preliminary(PRODUCT_B, params[1]),
        _pending(PRODUCT_B, params[2]),
    ]
    profile, coverage = _profile(cells, catalog, universe=universe)

    for product_id, product in profile.products.items():
        explained = product.measured_weight + sum(
            bucket.weight for bucket in product.causes.values()
        )
        assert explained == pytest.approx(product.denominator), product_id
    total = sum(product.measured_weight for product in profile.products.values()) + sum(
        bucket.weight for bucket in profile.causes.values()
    )
    assert total == pytest.approx(coverage.denominator)
    # И «неизвестное» ядро: знаменатель минус разрешённое.
    assert profile.unknown_share == pytest.approx(
        1 - (coverage.numerator / coverage.denominator)
    )


# ---------------------------------------------------------------------------
# 4. N/A и pending — вне знаменателя / отдельными бакетами (I7, I13)
# ---------------------------------------------------------------------------


def test_na_and_preliminary_are_outside_denominator() -> None:
    """``N/A`` (I7) и ``preliminary`` (I13) не объясняют знаменатель — они вне его."""
    catalog = _catalog(3)
    params = sorted(catalog)
    universe = scope_universe(catalog, [PRODUCT_A])
    cells = [
        _ok(PRODUCT_A, params[0]),
        _na(PRODUCT_A, params[1]),
        _preliminary(PRODUCT_A, params[2]),
    ]
    profile, coverage = _profile(cells, catalog, universe=universe)
    product = profile.products[PRODUCT_A]

    # Знаменатель — только одна измеренная ячейка (вес 2.0), N/A и preliminary вычтены.
    assert coverage.denominator == pytest.approx(2.0)
    assert product.denominator == pytest.approx(2.0)
    assert product.excluded[ExcludedCellClass.NOT_APPLICABLE.value].cells == 1
    assert product.excluded[ExcludedCellClass.PRELIMINARY.value].cells == 1
    # Ни один из них не попал в причины неизвестности.
    assert product.causes == {}
    assert ExcludedCellClass.NOT_APPLICABLE.value not in {
        cause.value for cause in UnknownCause
    }


def test_pending_is_separate_from_not_stated() -> None:
    """``pending`` — «ещё не смотрели»; это не «вендор молчит» (разные причины)."""
    catalog = _catalog(3)
    params = sorted(catalog)
    universe = scope_universe(catalog, [PRODUCT_A])
    cells = [
        _pending(PRODUCT_A, params[0]),
        _unknown(PRODUCT_A, params[1], DenialStrength.NOT_STATED),
    ]
    # params[2] — пара вселенной без строки: тоже pending, но с ``missing=1``.
    profile, coverage = _profile(cells, catalog, universe=universe)
    product = profile.products[PRODUCT_A]

    assert coverage.denominator == pytest.approx(6.0)  # три пары по весу 2.0
    pending = product.causes[UnknownCause.PENDING.value]
    assert pending.cells == 2
    assert pending.missing == 1
    assert product.causes[UnknownCause.NOT_STATED.value].cells == 1
    assert profile.dominant_cause == UnknownCause.PENDING.value


# ---------------------------------------------------------------------------
# 5. ❌ — не неизвестность
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "strength",
    [DenialStrength.EXPLICIT, DenialStrength.AUTHORITATIVE, DenialStrength.BOUNDED],
)
def test_absent_never_counts_as_unknown(strength: DenialStrength) -> None:
    """``❌`` — опубликованное «нет»: оно в измеренном, а не в долях неизвестности."""
    catalog = _catalog(2)
    params = sorted(catalog)
    universe = scope_universe(catalog, [PRODUCT_A])
    cells = [_absent(PRODUCT_A, params[0], strength), _ok(PRODUCT_A, params[1])]
    profile, _ = _profile(cells, catalog, universe=universe)

    assert profile.unknown_share == 0.0
    assert profile.causes == {}
    assert profile.products[PRODUCT_A].measured_cells == 2
    assert profile.products[PRODUCT_A].denials_weight == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# 6. Слабые «нет»: только bounded
# ---------------------------------------------------------------------------


def test_weak_denials_only_for_bounded() -> None:
    """``bounded`` — слабое основание; ``explicit``/``authoritative`` — нет."""
    catalog = _catalog(3)
    params = sorted(catalog)
    universe = scope_universe(catalog, [PRODUCT_A])
    cells = [
        _absent(PRODUCT_A, params[0], DenialStrength.BOUNDED),
        _absent(PRODUCT_A, params[1], DenialStrength.EXPLICIT),
        _absent(PRODUCT_A, params[2], DenialStrength.AUTHORITATIVE),
    ]
    profile, _ = _profile(cells, catalog, universe=universe)
    product = profile.products[PRODUCT_A]

    assert product.weak_denials.cells == 1
    assert product.weak_denials.weight == pytest.approx(2.0)
    assert product.weak_denials.share == pytest.approx(1 / 3)
    assert profile.weak_denials.cells == 1
    # Слабое «нет» в долю неизвестности НЕ подмешивается.
    assert profile.unknown_share == 0.0


def test_weak_denial_share_above_threshold_is_a_caveat() -> None:
    """Доля слабых «нет» выше объявленной → оговорка, а не другой класс."""
    catalog = _catalog(2)
    params = sorted(catalog)
    universe = scope_universe(catalog, [PRODUCT_A, PRODUCT_B])
    cells = [
        _absent(product, param, DenialStrength.BOUNDED)
        for product in (PRODUCT_A, PRODUCT_B)
        for param in params
    ]
    profile, _ = _profile(
        cells,
        catalog,
        universe=universe,
        comparability_level="HIGH",
        weak_denial_share_threshold=0.0,
    )
    assert profile.weak_denial_share == pytest.approx(1.0)
    assert "weak_denials_above_threshold" in profile.caveats
    assert profile.klass == PublishabilityClass.PUBLISHABLE_WITH_CAVEAT.value


# ---------------------------------------------------------------------------
# 7. Прочее: неизвестная причина не выдумывается, жёсткий гейт не тронут
# ---------------------------------------------------------------------------


def test_unspecified_cause_is_named_not_guessed() -> None:
    """Историческая строка без ``evidence_scope`` → ``unspecified``, а не чужая причина."""
    catalog = _catalog(2)
    params = sorted(catalog)
    universe = scope_universe(catalog, [PRODUCT_A])
    cells = [_unknown(PRODUCT_A, params[0], None), _ok(PRODUCT_A, params[1])]
    profile, _ = _profile(cells, catalog, universe=universe)

    assert profile.causes[UnknownCause.UNSPECIFIED.value].cells == 1
    assert profile.dominant_cause == UnknownCause.UNSPECIFIED.value


def test_universe_missing_pair_counts_as_pending() -> None:
    """Пара вселенной без строки — ``pending`` с ``missing=1`` (не «вендор молчит»)."""
    catalog = _catalog(2)
    universe = scope_universe(catalog, [PRODUCT_A])
    profile, coverage = _profile([], catalog, universe=universe)
    pending = profile.causes[UnknownCause.PENDING.value]
    assert pending.cells == 2
    assert pending.missing == 2
    assert coverage.denominator == pytest.approx(4.0)


def test_profile_is_serializable_and_deterministic() -> None:
    """Профиль — сериализуемые данные: без объектов, без зависимости от порядка входа."""
    catalog = _catalog(4)
    params = sorted(catalog)
    universe = scope_universe(catalog, [PRODUCT_A])
    cells = [_ok(PRODUCT_A, params[0]), _unknown(PRODUCT_A, params[1], DenialStrength.NOT_STATED)]
    first, _ = _profile(cells, catalog, universe=universe)
    second, _ = _profile(list(reversed(cells)), catalog, universe=universe)

    from dataclasses import asdict

    payload = asdict(first)
    assert json.loads(json.dumps(payload)) == payload
    assert asdict(second) == payload


def test_hard_gate_unchanged_for_analytical_only(store_bundle) -> None:  # noqa: ANN001
    """``publish_warnings`` (I11) остаётся узким: неизвестность артефакт не блокирует."""
    bundle = store_bundle
    assert publish_warnings(bundle) == []
    with pytest.warns(RuntimeWarning, match="ANALYTICAL_ONLY"):
        assert_publishable(bundle)


# ---------------------------------------------------------------------------
# Словарь — источник истины перечислений
# ---------------------------------------------------------------------------


def test_dictionary_mirrors_publishability_enums() -> None:
    data = yaml.safe_load((ROOT / "docs" / "status-dictionary.yaml").read_text("utf-8"))
    assert set(data["publishability_class"]) == {item.value for item in PublishabilityClass}
    assert set(data["unknown_cause"]) == {item.value for item in UnknownCause}
    assert set(data["excluded_cell_class"]) == {
        item.value for item in ExcludedCellClass
    }


# ---------------------------------------------------------------------------
# Живой прогон (read-only): класс и разложение воспроизводятся
# ---------------------------------------------------------------------------

LIVE_RUN_ID = "ru-pg-dbms-3edf2b06097c"


#: Явный указатель на каталог живых прогонов. Нужен потому, что тесты намеренно **не**
#: читают операторское окружение (``tests/conftest.py``: ``COHOUND_NO_DOTENV=1``), а
#: артефакты прогонов живут вне репозитория (03 §A.1). Без него проверка скипается —
#: честно и видимо; с ним исполняется на настоящем прогоне.
ENV_LIVE_RUNS_DIR = "COHOUND_LIVE_RUNS_DIR"


def _live_run_dir() -> Path:
    """Каталог живого прогона: ``COHOUND_LIVE_RUNS_DIR`` > resolved-раскладка.

    Прямое ``ROOT / "runs"`` (как было) находило бы каталог только у того, кто не
    настраивал раскладку, и у остальных тест молча скипался бы — «живой прогон проверен»
    было бы неправдой. Но и чтение операторского ``COHOUND_HOME`` из теста недопустимо:
    тогда прогон набора зависел бы от окружения хоста. Отсюда отдельная явная переменная.
    """
    from core.config.paths import resolve_paths

    override = (os.environ.get(ENV_LIVE_RUNS_DIR) or "").strip()
    base = Path(override) if override else resolve_paths().runs
    return base / LIVE_RUN_ID


LIVE_RUN_DIR = _live_run_dir()


def _live_bundle():
    from core.domain.models import Cohort
    from core.store.sqlite_store import SqliteStore

    plan = json.loads((LIVE_RUN_DIR / "plan_inputs.json").read_text(encoding="utf-8"))
    catalog = {row["param_id"]: CatalogRow(**row) for row in plan["params"]}
    weights = plan.get("renormalized_weights") or plan.get("module_weights") or {}
    cohort = Cohort(**plan["cohort"]) if plan.get("cohort") else None
    universe = (
        scope_universe(catalog, [entry.product_id for entry in cohort.entries])
        if cohort is not None
        else None
    )
    with SqliteStore(LIVE_RUN_DIR / "cohound.db", read_only=True) as store:
        return compute(
            store=store,
            run_id=LIVE_RUN_ID,
            catalog=catalog,
            module_weights=weights,
            cohort=cohort,
            universe=universe,
        )


@pytest.mark.skipif(
    not (LIVE_RUN_DIR / "cohound.db").exists(),
    reason=(
        "живого прогона нет: укажите COHOUND_LIVE_RUNS_DIR (каталог, где лежат "
        "runs/{run_id}/) — артефакты прогонов вне репозитория (03 §A.1)"
    ),
)
def test_live_run_class_and_decomposition() -> None:
    """Живой прогон: класс ``ANALYTICAL_ONLY``, доминирует ``instrument_failed``.

    Это тот самый прогон, из-за которого задача и появилась: 55 % ``❓``, coverage 0.208,
    comparability ``INSUFFICIENT`` — и прежний код называл его публикуемым, потому что
    проверял только «coverage определён».
    """
    first = _live_bundle()
    second = _live_bundle()
    profile = first.publishability

    assert profile is not None
    assert json.dumps(first.to_dict(), sort_keys=True) == json.dumps(
        second.to_dict(), sort_keys=True
    ), "профиль обязан воспроизводиться детерминированно"
    assert profile.klass == PublishabilityClass.ANALYTICAL_ONLY.value
    assert profile.dominant_cause == UnknownCause.INSTRUMENT_FAILED.value
    assert profile.unknown_share > profile.unknown_share_threshold
    assert profile.coverage is not None and profile.coverage < profile.coverage_floor
    # ``❌`` в этом прогоне нет — значит и слабых «нет» нет.
    assert profile.weak_denials.cells == 0
    # Разложение объясняет знаменатель и здесь.
    explained = sum(
        product.measured_weight for product in profile.products.values()
    ) + sum(bucket.weight for bucket in profile.causes.values())
    assert explained == pytest.approx(first.coverage.denominator)
    # Причина каждого «неизвестно» видна отдельным числом.
    causes = {cause: bucket.cells for cause, bucket in profile.causes.items()}
    assert causes[UnknownCause.INSTRUMENT_FAILED.value] > 0
    assert causes.get(UnknownCause.PENDING.value, 0) > 0


@pytest.fixture()
def store_bundle():
    """Бандл прогона, у которого доля неизвестности выше порога (для проверки гейта)."""
    import datetime as _dt

    from core.domain.models import Focus, RunConfig
    from core.store.sqlite_store import SqliteStore

    with SqliteStore(":memory:") as store:
        store.create_run(
            RUN_ID,
            RunConfig(
                case_id="case-1",
                anchor=PRODUCT_A,
                cohort="cohort.yaml",
                products=[PRODUCT_A],
                package="pkg-1",
                focus=Focus(),
                languages=["en"],
                snapshot_date=_dt.date(2026, 1, 1),
                mode=RunMode.STRICT,
            ),
        )
        catalog = _catalog(4)
        params = sorted(catalog)
        cells = [_ok(PRODUCT_A, params[0])]
        cells += [
            _unknown(PRODUCT_A, param, DenialStrength.NOT_STATED) for param in params[1:]
        ]
        for cell in cells:
            store.save_cell(cell)
        yield compute(
            store=store,
            run_id=RUN_ID,
            catalog=catalog,
            module_weights={},
            universe=scope_universe(catalog, [PRODUCT_A]),
        )
