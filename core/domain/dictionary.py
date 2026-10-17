"""coHound — загрузка и проверка словаря ``docs/status-dictionary.yaml`` (§4.1).

Словарь — источник истины перечислений; этот модуль проверяет его целостность и синхронизацию
со схемами (S3). Нейтрален (§1.3).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from core.domain.enums import (
    CapabilityPolicy,
    CrawlItemStatus,
    CrawlSessionStatus,
    CrawlStrategy,
    DocumentLevel,
    ExtractionRoute,
    FetchMode,
    Gate,
    RankingPolicy,
    SearchKind,
    SkipReason,
    StaleReason,
    Status,
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
        "search_kind",
        "stale_reason",
        # D2: оси получения/извлечения (domain_profiles.yaml / FetchPlan).
        "fetch_mode",
        "extraction_route",
        # D4: стратегия краула (как используется браузер; FetchPlan).
        "crawl_strategy",
        # P5: уровень извлечения документных форматов (crawl.document_level).
        "document_level",
        # Lane B: жизненный цикл сеанса краула и per-URL статус его элемента.
        "crawl_session_status",
        "crawl_item_status",
        # P2a: машинная причина пропуска фазы (PhaseStatus="skipped").
        "skip_reason",
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
        "search_kind": {v.value for v in SearchKind},
        "stale_reason": {v.value for v in StaleReason},
        "fetch_mode": {v.value for v in FetchMode},
        "extraction_route": {v.value for v in ExtractionRoute},
        "crawl_strategy": {v.value for v in CrawlStrategy},
        "document_level": {v.value for v in DocumentLevel},
        "crawl_session_status": {v.value for v in CrawlSessionStatus},
        "crawl_item_status": {v.value for v in CrawlItemStatus},
        "skip_reason": {v.value for v in SkipReason},
    }
    for section, expected in checks.items():
        actual = set(data[section])
        if actual != expected:
            raise ValueError(
                f"словарь.{section} рассинхронизирован со схемой: "
                f"словарь={sorted(actual)}, схема={sorted(expected)}"
            )
