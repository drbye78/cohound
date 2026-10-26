"""coHound — загрузка и проверка словаря ``docs/status-dictionary.yaml`` (§4.1).

Словарь — источник истины перечислений; этот модуль проверяет его целостность и синхронизацию
со схемами (S3). Нейтрален (§1.3).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from core.domain.enums import (
    ArchiveDescent,
    ArchiveDescentStatus,
    CapabilityPolicy,
    ChangeClass,
    CrawlItemStatus,
    CrawlSessionStatus,
    CrawlStrategy,
    DenialClaim,
    DenialStrength,
    DocumentLevel,
    ExcludedCellClass,
    ExtractionRoute,
    FailureKind,
    FetchMode,
    Gate,
    GatePolicy,
    GateSource,
    GateVerdict,
    IncrementalKind,
    OnboardingState,
    PdfPipeline,
    ProductMode,
    ProductScope,
    PublishabilityClass,
    RankingPolicy,
    RefreshPolicy,
    ReuseKind,
    RunScope,
    SearchKind,
    SeedRole,
    SeedSource,
    SeedState,
    SkipReason,
    StaleReason,
    Status,
    UnknownCause,
    UnknownPolicy,
)


def load_dictionary(path: str | Path) -> dict[str, Any]:
    """Загружает docs/status-dictionary.yaml."""
    text = Path(path).read_text(encoding="utf-8")
    data = yaml.safe_load(text)
    if not isinstance(data, dict):
        raise ValueError("status-dictionary.yaml: ожидался mapping верхнего уровня")
    return data


def assert_dictionary_integrity(path: str | Path) -> None:
    """Минимальная проверка целостности словаря относительно схем."""
    data = load_dictionary(path)
    required = [
        "status_dictionary_version",
        "statuses",
        "source_kind",
        "evidence_type",
        "absence_rule_type",
        # Вариант 2 (§2.3): сила отрицания — единственный носитель вопроса об отсутствии.
        "denial_strength",
        "denial_claim",
        "source_scope",
        "failure_kind",
        "cell_state",
        "run_mode",
        "satellite",
        "rule_r2",
        # S3: новые перечисления должны быть в словаре — источнике истины.
        "ranking_policy",
        "capability_policy",
        "unknown_policy",
        "gate",
        # §8: точки интеграции гейтов — кто решил, чем кончилось, какая политика.
        "gate_source",
        "gate_verdict",
        "gate_policy",
        "search_kind",
        "stale_reason",
        # D2: оси получения/извлечения (domain_profiles.yaml / FetchPlan).
        "fetch_mode",
        "extraction_route",
        # D4: стратегия краула (как используется браузер; FetchPlan).
        "crawl_strategy",
        # P5: уровень извлечения документных форматов (crawl.document_level).
        "document_level",
        # P7: механизм извлечения PDF (crawl.pdf_pipeline).
        "pdf_pipeline",
        # P6: глубина спуска в контейнеры (crawl.archive_descent).
        "archive_descent",
        # B3: исход спуска в один контейнер (archive_observations.status).
        "archive_descent_status",
        # Lane B: жизненный цикл сеанса краула и per-URL статус его элемента.
        "crawl_session_status",
        "crawl_item_status",
        # P2a: машинная причина пропуска фазы (PhaseStatus="skipped").
        "skip_reason",
        # MVP-1 (09 §3.1): онбординг продукта и seed-набор.
        "seed_role",
        "seed_source",
        "seed_state",
        "onboarding_state",
        "product_mode",
        "refresh_policy",
        "product_scope",
        # MVP-2b (09 §3.1/§12.1): идентичность прогона и класс изменения.
        "change_class",
        "run_scope",
        # MVP-3a (09 §6.3/§9): инкрементальный work-set и переиспользование.
        "incremental_kind",
        "reuse_kind",
        # A1/§6: класс публикуемости и разложение неизвестности — обязательные секции.
        "publishability_class",
        "unknown_cause",
        "excluded_cell_class",
    ]
    missing = [k for k in required if k not in data]
    if missing:
        raise ValueError(f"словарь неполон, нет секций: {missing}")
    for status, meta in data["statuses"].items():
        if meta["symbol"] not in {s.value for s in Status}:
            raise ValueError(f"статус {status}: символ {meta['symbol']} вне Status")
    if data["absence_rule_type"]["default"] not in data["absence_rule_type"]["values"]:
        raise ValueError("absence_rule_type.default отсутствует в values")
    # Синхронизация новых enum со словарём (S3).
    checks = {
        "ranking_policy": {v.value for v in RankingPolicy},
        "capability_policy": {v.value for v in CapabilityPolicy},
        "unknown_policy": {v.value for v in UnknownPolicy},
        "gate": {v.value for v in Gate},
        "gate_source": {v.value for v in GateSource},
        "gate_verdict": {v.value for v in GateVerdict},
        "gate_policy": {v.value for v in GatePolicy},
        "search_kind": {v.value for v in SearchKind},
        "stale_reason": {v.value for v in StaleReason},
        "fetch_mode": {v.value for v in FetchMode},
        "extraction_route": {v.value for v in ExtractionRoute},
        "crawl_strategy": {v.value for v in CrawlStrategy},
        "document_level": {v.value for v in DocumentLevel},
        "pdf_pipeline": {v.value for v in PdfPipeline},
        "archive_descent": {v.value for v in ArchiveDescent},
        "archive_descent_status": {v.value for v in ArchiveDescentStatus},
        "crawl_session_status": {v.value for v in CrawlSessionStatus},
        "crawl_item_status": {v.value for v in CrawlItemStatus},
        "skip_reason": {v.value for v in SkipReason},
        # MVP-1 (09 §3.1): онбординг продукта и seed-набор.
        "seed_role": {v.value for v in SeedRole},
        "seed_source": {v.value for v in SeedSource},
        "seed_state": {v.value for v in SeedState},
        "onboarding_state": {v.value for v in OnboardingState},
        "product_mode": {v.value for v in ProductMode},
        "refresh_policy": {v.value for v in RefreshPolicy},
        "product_scope": {v.value for v in ProductScope},
        # MVP-2b (09 §3.1/§12.1): идентичность прогона и класс изменения.
        "change_class": {v.value for v in ChangeClass},
        "run_scope": {v.value for v in RunScope},
        # MVP-3a (09 §6.3/§9): инкрементальный work-set и переиспользование.
        "incremental_kind": {v.value for v in IncrementalKind},
        "reuse_kind": {v.value for v in ReuseKind},
        # Ф0: классы сбоя краула/конвейера — иначе словарь и enum расходятся молча.
        "failure_kind": {v.value for v in FailureKind},
        # Вариант 2: что модель вправе заявить о своём «нет» (ровно два класса).
        "denial_claim": {v.value for v in DenialClaim},
        # A1/§6: класс публикуемости карты и разложение неизвестности. Без этих секций
        # общий гейт словаря не знал о них, и словарь мог разъехаться с enum незаметно
        # (тест профиля ловит это только в своём файле).
        "publishability_class": {v.value for v in PublishabilityClass},
        "unknown_cause": {v.value for v in UnknownCause},
        "excluded_cell_class": {v.value for v in ExcludedCellClass},
    }
    for section, expected in checks.items():
        actual = set(data[section])
        if actual != expected:
            raise ValueError(
                f"словарь.{section} рассинхронизирован со схемой: "
                f"словарь={sorted(actual)}, схема={sorted(expected)}"
            )
    # ``denial_strength`` несёт не только значения, но и аннотации (``verdicts``,
    # ``evaluator_can_claim``) — как ``absence_rule_type``. Поэтому проверяется не
    # равенство ключей, а полнота: каждый класс перечисления обязан быть назван.
    undocumented = {v.value for v in DenialStrength} - set(data["denial_strength"])
    if undocumented:
        raise ValueError(
            "словарь.denial_strength не документирует классы: "
            f"{sorted(undocumented)}"
        )
