"""Внешний endpoint оценщика — **объявленное** исключение стенда RnD (§10), не демотирование.

Проверяется три вещи, ради которых вводился признак:

* видимость: профиль публикуемости несёт ``rnd_endpoint``, хосты очереди и замечания;
* честность: без объявления замечание называет незакрытое состояние (а не молчит);
* **класс публикуемости не зависит от места исполнения** — пин на «не демотирует»:
  прогон на стенде — условие измерения, а не качество карты (A1/A3).

Категорийная нейтральность (§1.3): хосты в фикстурах синтетические.
"""

from __future__ import annotations

from core.domain.enums import CellState, Importance, PublishabilityClass, Status
from core.domain.models import CatalogRow, Cell, LLMEvidence
from core.domain.scoring import (
    endpoint_host_notices,
    evidence_coverage,
    publishability_profile,
)

RUN_ID = "run-endpoint"
PRODUCT = "prod.a"


def _row(param_id: str) -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title=param_id,
        module="mod.one",
        group="grp",
        importance=Importance.BASIC,
        param_type="fact",
        rule="правило",
    )


def _ok(param_id: str) -> Cell:
    return Cell(
        run_id=RUN_ID,
        product_id=PRODUCT,
        param_id=param_id,
        state=CellState.EVALUATED,
        status=Status.OK,
        evidence=[LLMEvidence(source_id="src.1", quote="цитата")],
    )


def _profile(*, rnd_endpoint: bool | None, llm_hosts: tuple[str, ...] = ()):
    catalog = {"p001": _row("p001"), "p002": _row("p002")}
    cells = [_ok(param) for param in sorted(catalog)]
    coverage = evidence_coverage(cells, catalog, {})
    return publishability_profile(
        cells,
        catalog,
        {},
        coverage=coverage,
        rnd_endpoint=rnd_endpoint,
        llm_hosts=llm_hosts,
    )


def test_declared_stand_sets_flag_and_hosts() -> None:
    """Объявленное исключение: ``rnd_endpoint`` виден полем, хосты — списком."""
    profile = _profile(rnd_endpoint=True, llm_hosts=("llm.stand.test",))

    assert profile.rnd_endpoint is True
    assert profile.rnd_endpoint_declared is True
    assert profile.llm_hosts == ["llm.stand.test"]
    # Замечание видимое и называет хост — «куда ушёл корпус» не остаётся догадкой.
    assert [notice["code"] for notice in profile.llm_host_notices] == [
        "rnd_endpoint_declared"
    ]
    assert "llm.stand.test" in profile.llm_host_notices[0]["message"]
    assert profile.llm_host_notices[0]["level"] == "warning"


def test_undeclared_host_is_a_notice_not_silence() -> None:
    """Хост объявлен, принадлежность периметру — нет: прогон заявляет производственный контур.

    Без этого замечания внешний хост неотличим от ошибки конфигурации (V6), и карта со
    стенда выглядела бы производственной публикацией.
    """
    profile = _profile(rnd_endpoint=None, llm_hosts=("llm.stand.test",))

    assert profile.rnd_endpoint is False
    assert profile.rnd_endpoint_declared is False
    assert [notice["code"] for notice in profile.llm_host_notices] == [
        "rnd_endpoint_not_declared"
    ]
    # Объявленное «внутри периметра» — декларация оператора, а не наблюдение: спорить нечем.
    internal = _profile(rnd_endpoint=False, llm_hosts=("llm.stand.test",))
    assert internal.llm_host_notices == []
    # Исключение без endpoint'а — тоже видимый факт (признак лишний, хоста нет).
    empty = _profile(rnd_endpoint=True)
    assert [notice["code"] for notice in empty.llm_host_notices] == [
        "rnd_endpoint_without_endpoint"
    ]


def test_publishability_class_does_not_depend_on_endpoint_place() -> None:
    """Пин на «не демотирует»: класс и оговорки одинаковы для всех трёх состояний.

    Место исполнения — условие стенда, а не качество измерения: демотировать карту за
    адрес шлюза значило бы назвать измерение плохим по провенансу (A1/A3).
    """
    profiles = [
        _profile(rnd_endpoint=None, llm_hosts=("llm.stand.test",)),
        _profile(rnd_endpoint=True, llm_hosts=("llm.stand.test",)),
        _profile(rnd_endpoint=False, llm_hosts=("llm.stand.test",)),
    ]

    assert {profile.klass for profile in profiles} == {
        PublishabilityClass.PUBLISHABLE_WITH_CAVEAT.value
    }
    assert {profile.unknown_share for profile in profiles} == {0.0}
    assert all(profile.caveats == ["single_product_cohort"] for profile in profiles)
    assert all("rnd_endpoint" not in caveat for profile in profiles for caveat in profile.caveats)


def test_hosts_are_sorted_and_deduplicated() -> None:
    """Детерминизм артефакта: порядок хостов не зависит от порядка строк в БД."""
    catalog = {"p001": _row("p001")}
    cells = [_ok("p001")]
    coverage = evidence_coverage(cells, catalog, {})

    first = publishability_profile(
        cells,
        catalog,
        {},
        coverage=coverage,
        rnd_endpoint=True,
        llm_hosts=("llm.b.test", "llm.a.test", "llm.b.test", " "),
    )
    second = publishability_profile(
        cells,
        catalog,
        {},
        coverage=coverage,
        rnd_endpoint=True,
        llm_hosts=("llm.a.test", "llm.b.test"),
    )

    assert first.llm_hosts == ["llm.a.test", "llm.b.test"]
    assert first.to_dict() == second.to_dict()


def test_notice_helper_is_pure_and_state_driven() -> None:
    """Функция замечаний читает только объявленные входы — никаких догадок по имени хоста."""
    assert endpoint_host_notices(rnd_endpoint=None, hosts=()) == []
    assert endpoint_host_notices(rnd_endpoint=False, hosts=()) == []
    assert endpoint_host_notices(rnd_endpoint=None, hosts=(" h2 ", "h1"))[0]["hosts"] == [
        "h1",
        "h2",
    ]
