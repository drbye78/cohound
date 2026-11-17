"""Тесты канонических дайджестов идентичности прогона (09 §6.2/§12.2/§12.5).

Офлайн, без I/O: только доменные модели и чистые функции ``core.pipeline.digests``.
Проверяется стабильность, чувствительность к каждому включённому полю и
нечувствительность к авторскому тексту/порядку.
"""

from __future__ import annotations

import datetime as _dt

import pytest

from core.domain.enums import (
    AnchorRelation,
    OnboardingState,
    SeedRole,
    SeedSource,
    SeedState,
)
from core.domain.models import Cohort, CohortEntry, ProductSeeds, SeedEntry, SeedsConfig
from core.pipeline.digests import (
    EXECUTION_CONTRACT_FIELDS,
    cohort_digest,
    execution_contract_digest,
    seeds_digest,
)
from core.pipeline.retrieve import (
    DEFAULT_CROSS_QUERY_FUSION,
    DEFAULT_POOL_MULTIPLIER,
    DEFAULT_RRF_K,
    MAX_QUERIES,
    retrieval_params,
    retrieval_params_json,
)


def _entry(
    product_id: str = "p1",
    *,
    anchor_relation: AnchorRelation = AnchorRelation.ANCHOR,
    role_overlap: float = 1.0,
    selection_reason: str | None = "якорь",
    comparison_base: bool = False,
) -> CohortEntry:
    return CohortEntry(
        product_id=product_id,
        role_overlap=role_overlap,
        selection_reason=selection_reason,
        anchor_relation=anchor_relation,
        comparison_base=comparison_base,
    )


def _cohort(
    *entries: CohortEntry,
    case_id: str = "case_x",
    frozen_at: _dt.date = _dt.date(2026, 6, 1),
) -> Cohort:
    rows = list(entries) or [_entry()]
    return Cohort(case_id=case_id, frozen_at=frozen_at, entries=rows)


def _seed(
    *,
    product_id: str = "p1",
    url: str = "https://p1.test/",
    role: SeedRole = SeedRole.PRIMARY,
    profile_key: str | None = "p1.test",
    enabled: bool = True,
) -> tuple[str, SeedEntry]:
    state = SeedState.ENABLED if enabled else SeedState.DISABLED
    entry = SeedEntry(
        url=url,
        normalized_url=url,
        role=role,
        source=SeedSource.OPERATOR,
        profile_key=profile_key,
        added_at=_dt.date(2026, 6, 1),
        added_by="test",
        enabled=enabled,
        state=state,
    )
    return product_id, entry


def _seeds(*rows: tuple[str, SeedEntry]) -> SeedsConfig:
    products: dict[str, ProductSeeds] = {}
    for product_id, seed in rows:
        products.setdefault(product_id, ProductSeeds(state=OnboardingState.PENDING_SEEDS, seeds=[]))
        products[product_id].seeds.append(seed)
    for product_id, product in products.items():
        products[product_id] = ProductSeeds(
            state=(
                OnboardingState.READY
                if any(seed.is_enabled for seed in product.seeds)
                else OnboardingState.PENDING_SEEDS
            ),
            seeds=list(product.seeds),
        )
    return SeedsConfig(products=products)


# ---------------------------------------------------------------------------
# cohort_digest
# ---------------------------------------------------------------------------


def test_cohort_digest_is_stable_and_short() -> None:
    cohort = _cohort(_entry("p1"), _entry("p2", anchor_relation=AnchorRelation.ANALOG, role_overlap=0.5))
    first = cohort_digest(cohort, case_id="case_x", cohort_ref="cohort-r1")
    second = cohort_digest(cohort, case_id="case_x", cohort_ref="cohort-r1")
    assert first == second
    assert len(first) == 24


def test_cohort_digest_ignores_selection_reason_and_order() -> None:
    left = _cohort(
        _entry("p1", selection_reason="якорь"),
        _entry("p2", anchor_relation=AnchorRelation.ANALOG, role_overlap=0.5, selection_reason="аналог"),
    )
    right = _cohort(
        _entry("p2", anchor_relation=AnchorRelation.ANALOG, role_overlap=0.5, selection_reason="другой текст"),
        _entry("p1", selection_reason="и ещё один"),
    )
    assert cohort_digest(left, case_id="case_x") == cohort_digest(right, case_id="case_x")


@pytest.mark.parametrize(
    ("left", "right"),
    [
        (
            _cohort(_entry("p1")),
            _cohort(_entry("p1", role_overlap=0.9)),
        ),
        (
            _cohort(_entry("p1"), _entry("p2", anchor_relation=AnchorRelation.ANALOG)),
            _cohort(_entry("p1", anchor_relation=AnchorRelation.ANALOG), _entry("p2")),
        ),
        (
            _cohort(_entry("p1")),
            _cohort(_entry("p1"), _entry("p2", anchor_relation=AnchorRelation.ANALOG, role_overlap=0.5)),
        ),
        (
            _cohort(_entry("p1"), frozen_at=_dt.date(2026, 6, 1)),
            _cohort(_entry("p1"), frozen_at=_dt.date(2026, 6, 2)),
        ),
    ],
)
def test_cohort_digest_is_sensitive_to_material(left: Cohort, right: Cohort) -> None:
    assert cohort_digest(left, case_id="case_x") != cohort_digest(right, case_id="case_x")


def test_cohort_digest_is_sensitive_to_case_id_and_cohort_ref() -> None:
    cohort = _cohort(_entry("p1"))
    base = cohort_digest(cohort, case_id="case_x", cohort_ref="r1")
    assert cohort_digest(cohort, case_id="case_y", cohort_ref="r1") != base
    assert cohort_digest(cohort, case_id="case_x", cohort_ref="r2") != base
    assert cohort_digest(cohort, case_id="case_x", cohort_ref=None) != base


def test_cohort_digest_without_comparison_base_is_pinned() -> None:
    """Пин: дайджест **тот же**, что до появления поля ``comparison_base``.

    Литерал ниже посчитан реализацией до правки
    (``git show HEAD:core/pipeline/digests.py``) на этой же фикстуре. Он держит главное
    свойство: ``comparison_base`` — представление (относительно кого печатать Δ), а не
    условие замера, поэтому материал не меняется ни при дефолте, ни при объявлении.
    """
    cohort = _cohort(
        _entry("p1"),
        _entry("p2", anchor_relation=AnchorRelation.ANALOG, role_overlap=0.5),
    )
    assert (
        cohort_digest(cohort, case_id="case_x", cohort_ref="r1")
        == "daf029ae9212643495f90a59"
    )


def test_cohort_digest_ignores_declared_comparison_base() -> None:
    """Объявленная точка отсчёта материал **не меняет**: иначе смена вида отчёта
    заставляла бы перезапускать прогон ради тех же ячеек (§7: политика гейтов — тоже
    процесс, а не условие замера, и её в контракте нет).

    Самоописательность сохраняется на артефакте: ``baseline_delta.yaml`` называет
    ``base_product``, поэтому два вида отчёта одного ``run_id`` не смешиваются.
    """
    left = _cohort(
        _entry("p1"),
        _entry("p2", anchor_relation=AnchorRelation.ANALOG, role_overlap=0.5),
    )
    right = _cohort(
        _entry("p1"),
        _entry(
            "p2",
            anchor_relation=AnchorRelation.ANALOG,
            role_overlap=0.5,
            comparison_base=True,
        ),
    )
    assert cohort_digest(left, case_id="case_x", cohort_ref="r1") == cohort_digest(
        right, case_id="case_x", cohort_ref="r1"
    )


# ---------------------------------------------------------------------------
# seeds_digest
# ---------------------------------------------------------------------------


def test_seeds_digest_is_order_sensitive_within_product() -> None:
    """Порядок корней **внутри продукта** — часть идентичности (§15, I19, D-7).

    Краул порядко-чувствителен: бюджет ``max_documents`` расходуется в порядке обхода,
    поэтому корень, идущий последним, может не выполниться вовсе (``_ordered_unique``).
    Перестановка двух корней одного продукта даёт **другой** корпус — значит и другой
    ``run_id``; сортировка только по URL скрывала бы это.
    """
    root_a = _seed(product_id="p1", url="https://p1.test/")
    root_b = _seed(product_id="p1", url="https://p1.test/docs", role=SeedRole.DOCS)
    assert seeds_digest(_seeds(root_a, root_b)) != seeds_digest(_seeds(root_b, root_a))


def test_seeds_digest_is_stable_and_canonicalizes_product_order() -> None:
    """Порядок **продуктов** в файле канонизируется: продукт-цикл отсортирован."""
    seed_a = _seed(product_id="p1", url="https://p1.test/")
    seed_b = _seed(product_id="p2", url="https://p2.test/", role=SeedRole.DOCS)
    first = seeds_digest(_seeds(seed_a, seed_b))
    second = seeds_digest(_seeds(seed_b, seed_a))
    assert first == second
    assert len(first) == 24


def test_seeds_digest_empty_set_is_stable_not_empty_string() -> None:
    digest = seeds_digest(SeedsConfig())
    assert digest
    assert len(digest) == 24
    assert digest == seeds_digest(SeedsConfig(products={}))


@pytest.mark.parametrize(
    ("left", "right"),
    [
        (
            _seeds(_seed(product_id="p1", url="https://p1.test/")),
            _seeds(_seed(product_id="p1", url="https://p1.test/", enabled=False)),
        ),
        (
            _seeds(_seed(product_id="p1", url="https://p1.test/")),
            _seeds(_seed(product_id="p1", url="https://p1.test/", role=SeedRole.DOCS)),
        ),
        (
            _seeds(_seed(product_id="p1", url="https://p1.test/", profile_key="p1.test")),
            _seeds(_seed(product_id="p1", url="https://p1.test/", profile_key="other.test")),
        ),
        (
            _seeds(_seed(product_id="p1", url="https://p1.test/")),
            _seeds(_seed(product_id="p1", url="https://p1.test/docs")),
        ),
        (
            _seeds(_seed(product_id="p1", url="https://p1.test/")),
            _seeds(_seed(product_id="p2", url="https://p1.test/")),
        ),
    ],
)
def test_seeds_digest_is_sensitive_to_material(left: SeedsConfig, right: SeedsConfig) -> None:
    assert seeds_digest(left) != seeds_digest(right)


# ---------------------------------------------------------------------------
# execution_contract_digest
# ---------------------------------------------------------------------------


def _contract_values() -> dict[str, object]:
    return {field: f"v-{field}" for field in EXECUTION_CONTRACT_FIELDS}


def test_execution_contract_is_stable_and_short() -> None:
    values = _contract_values()
    first = execution_contract_digest(values)
    second = execution_contract_digest(dict(reversed(list(values.items()))))
    assert first == second
    assert len(first) == 24


def test_execution_contract_includes_none_explicitly() -> None:
    values = _contract_values()
    values["chunker_version"] = None
    with_none = execution_contract_digest(values)

    other = _contract_values()
    other["chunker_version"] = "some-model"
    assert with_none != execution_contract_digest(other)

    # Пропущенный ключ — ошибка, а не «то же самое, что None»: иначе отсутствие
    # поля и явный null дали бы коллизию.
    missing = dict(values)
    del missing["chunker_version"]
    with pytest.raises(ValueError, match="отсутствуют поля"):
        execution_contract_digest(missing)


def test_execution_contract_rejects_unknown_fields() -> None:
    values = _contract_values()
    values["unexpected"] = "x"
    with pytest.raises(ValueError, match="неизвестные поля"):
        execution_contract_digest(values)


@pytest.mark.parametrize("field", EXECUTION_CONTRACT_FIELDS)
def test_execution_contract_is_sensitive_to_each_field(field: str) -> None:
    left = _contract_values()
    right = _contract_values()
    right[field] = "changed"
    assert execution_contract_digest(left) != execution_contract_digest(right)


def test_embed_precision_is_a_contract_condition() -> None:
    """``embed.precision`` — условие измерения, а не настройка вида (решение 06.11.2026).

    fp16- и fp32-векторы одного текста — разные числа, поэтому смена точности меняет
    корпус индекса и обязана менять ``run_id``: иначе два разных измерения жили бы под
    одним именем.
    """
    assert "embed_precision" in EXECUTION_CONTRACT_FIELDS
    fp32 = _contract_values()
    fp32["embed_precision"] = "fp32"
    fp16 = _contract_values()
    fp16["embed_precision"] = "fp16"
    assert execution_contract_digest(fp32) != execution_contract_digest(fp16)


def test_embed_spec_is_a_contract_condition() -> None:
    """``embed.model_id@embed.revision`` — условие измерения (план 26 O6).

    Две ревизии одной модели дают численно разные векторы, поэтому смена пина обязана
    менять ``run_id``: иначе два разных измерения жили бы под одним именем. Наблюдаемая
    строка ``embed_model_id`` при этом остаётся провенансом — тот же приём, что
    ``extractor_stack``/``extractor_version``.
    """
    assert "embed_spec" in EXECUTION_CONTRACT_FIELDS
    first = _contract_values()
    first["embed_spec"] = "org/model@" + "a" * 40
    second = _contract_values()
    second["embed_spec"] = "org/model@" + "b" * 40
    assert execution_contract_digest(first) != execution_contract_digest(second)

    # «Dense не объявлен» отличимо от «объявлен»: ``null`` — тоже значение контракта.
    absent = _contract_values()
    absent["embed_spec"] = None
    assert execution_contract_digest(absent) != execution_contract_digest(first)


# ---------------------------------------------------------------------------
# Профиль лексического анализатора — условие измерения (``lexical_spec``)
# ---------------------------------------------------------------------------


def test_lexical_spec_names_the_profile() -> None:
    """``lexical_spec`` называет профиль анализатора, а не только версию схемы.

    Профиль и есть выбор анализатора единственного поля: без него ``run_id`` не описывал
    бы, каким анализом получена лексическая выдача (``stemmed`` против ``identifier`` —
    разный ранг первого попадания, замер O16).
    """
    from core.adapters.lexical_tantivy import lexical_schema_spec

    spec = lexical_schema_spec()
    assert spec["profile"] == "stemmed"
    assert lexical_schema_spec("identifier")["profile"] == "identifier"
    # ``raw`` — псевдоним: каноническое имя одно.
    assert lexical_schema_spec("raw")["profile"] == "identifier"


def test_lexical_profile_is_a_contract_condition() -> None:
    """Смена профиля лексики меняет ``execution_contract_digest`` (значит ``run_id``)."""
    from core.adapters.lexical_tantivy import lexical_schema_spec

    assert "lexical_spec" in EXECUTION_CONTRACT_FIELDS
    stemmed = _contract_values()
    stemmed["lexical_spec"] = lexical_schema_spec("stemmed")
    identifier = _contract_values()
    identifier["lexical_spec"] = lexical_schema_spec("identifier")
    assert execution_contract_digest(stemmed) != execution_contract_digest(identifier)

    # Отсутствие оси отличимо от объявленной: ``null`` — тоже значение контракта.
    absent = _contract_values()
    absent["lexical_spec"] = None
    assert execution_contract_digest(absent) != execution_contract_digest(stemmed)


# ---------------------------------------------------------------------------
# O2 плана 26: retrieval_params_json = исполненные параметры
# ---------------------------------------------------------------------------
def test_retrieval_params_serializer_declares_exactly_executed_values() -> None:
    """Сериализатор называет ровно исполняемые параметры (и не выдумывает новых)."""
    params = retrieval_params(profile="R2", cutoff_k=10)
    assert set(params) == {
        "profile",
        "cutoff_K",
        "rrf_k",
        "pool_multiplier",
        "max_queries",
        "cross_query_fusion",
    }
    assert params["rrf_k"] == DEFAULT_RRF_K == 60
    assert params["pool_multiplier"] == DEFAULT_POOL_MULTIPLIER == 1
    assert params["max_queries"] == MAX_QUERIES == 3
    assert params["cross_query_fusion"] == DEFAULT_CROSS_QUERY_FUSION == "max"
    # Необъявленный K остаётся ``None`` — это отличимо от подставленного дефолта.
    assert params["cutoff_K"] == 10
    assert retrieval_params(profile=None, cutoff_k=None)["cutoff_K"] is None


def test_retrieval_rrf_k_changes_contract_digest() -> None:
    """Смена ``rrf_k`` меняет ``run_id``: это условие измерения, а не провенанс."""
    base = _contract_values()
    base["retrieval_params_json"] = retrieval_params_json(profile="R2", cutoff_k=10)
    other = dict(base)
    other["retrieval_params_json"] = retrieval_params_json(
        profile="R2", cutoff_k=10, rrf_k=77
    )
    assert execution_contract_digest(base) != execution_contract_digest(other)


def test_null_retrieval_params_is_not_the_defaults() -> None:
    """``null`` (старый контракт) отличим от явного набора исполненных параметров."""
    old = _contract_values()
    old["retrieval_params_json"] = None
    explicit = dict(old)
    explicit["retrieval_params_json"] = retrieval_params_json(
        profile=None, cutoff_k=None
    )
    assert execution_contract_digest(old) != execution_contract_digest(explicit)


# ---------------------------------------------------------------------------
# D9 (план 26): лимиты PDF — условие измерения, а не расписание
# ---------------------------------------------------------------------------


def test_pdf_limits_absent_do_not_change_crawl_config_digest() -> None:
    """Прогон без объявленных осей даёт **прежний** ``crawl_config_digest``.

    Это главная гарантия обратной совместимости: появление осей в коде не должно менять
    ``run_id`` у прогона, который их не объявлял (§13). Сравнение — с вызовом, где оси
    переданы как ``None`` явно: оба пути обязаны дать одну величину.
    """
    from core.config.deploy import CrawlConfig
    from core.domain.models import CrawlRunConfig
    from core.pipeline.report import crawl_control_digest

    implicit = crawl_control_digest(
        run_crawl=CrawlRunConfig(), app=CrawlConfig(), profiles=None
    )
    explicit = crawl_control_digest(
        run_crawl=CrawlRunConfig(pdf_max_pages=None, pdf_max_extracted_chars=None),
        app=CrawlConfig(),
        profiles=None,
    )
    assert implicit == explicit


def test_pdf_limits_declared_change_crawl_config_digest() -> None:
    """Объявленная ось меняет дайджест: состав текста документа — это состав корпуса."""
    from core.config.deploy import CrawlConfig
    from core.domain.models import CrawlRunConfig
    from core.pipeline.report import crawl_control_digest

    plain = crawl_control_digest(
        run_crawl=CrawlRunConfig(), app=CrawlConfig(), profiles=None
    )
    pages = crawl_control_digest(
        run_crawl=CrawlRunConfig(pdf_max_pages=250), app=CrawlConfig(), profiles=None
    )
    chars = crawl_control_digest(
        run_crawl=CrawlRunConfig(pdf_max_extracted_chars=1_000_000),
        app=CrawlConfig(),
        profiles=None,
    )
    assert len({plain, pages, chars}) == 3


def test_pdf_limits_product_layer_changes_crawl_config_digest() -> None:
    """Продуктовый слой (``product_axes``) — тоже условие измерения, а не провенанс."""
    from core.config.crawl_policy import ProductCrawlAxes
    from core.config.deploy import CrawlConfig
    from core.domain.models import CrawlRunConfig
    from core.pipeline.report import crawl_control_digest

    base = crawl_control_digest(
        run_crawl=CrawlRunConfig(), app=CrawlConfig(), profiles=None
    )
    with_product = crawl_control_digest(
        run_crawl=CrawlRunConfig(),
        app=CrawlConfig(),
        profiles=None,
        product_axes={"p1": ProductCrawlAxes(pdf_max_pages=250)},
    )
    other_limit = crawl_control_digest(
        run_crawl=CrawlRunConfig(),
        app=CrawlConfig(),
        profiles=None,
        product_axes={"p1": ProductCrawlAxes(pdf_max_pages=500)},
    )
    assert len({base, with_product, other_limit}) == 3
