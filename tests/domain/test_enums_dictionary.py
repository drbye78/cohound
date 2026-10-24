"""Синхронизация доменных перечислений со словарём §4.1 (W1/T1.5, S3)."""

from __future__ import annotations

from pathlib import Path

from core.domain import (
    CapabilityPolicy,
    CrawlStrategy,
    FailureKind,
    Gate,
    RankingPolicy,
    SearchKind,
    SkipReason,
    StaleReason,
    Status,
    UnknownPolicy,
    assert_dictionary_integrity,
)

DICT = Path(__file__).resolve().parent.parent.parent / "docs" / "status-dictionary.yaml"


def test_dictionary_integrity() -> None:
    assert_dictionary_integrity(DICT)


def test_status_symbols_are_dictionary_keys() -> None:
    from core.domain import load_dictionary

    data = load_dictionary(DICT)
    symbols = {meta["symbol"] for meta in data["statuses"].values()}
    assert symbols == {s.value for s in Status}


def test_new_enums_match_dictionary() -> None:
    from core.domain import load_dictionary

    data = load_dictionary(DICT)
    assert set(data["ranking_policy"]) == {v.value for v in RankingPolicy}
    assert set(data["capability_policy"]) == {v.value for v in CapabilityPolicy}
    assert set(data["unknown_policy"]) == {v.value for v in UnknownPolicy}
    assert set(data["gate"]) == {v.value for v in Gate}
    assert set(data["search_kind"]) == {v.value for v in SearchKind}
    assert set(data["stale_reason"]) == {v.value for v in StaleReason}
    assert set(data["crawl_strategy"]) == {v.value for v in CrawlStrategy}
    assert set(data["skip_reason"]) == {v.value for v in SkipReason}
    # Ф0: классы сбоя краула/конвейера — словарь и enum не могут разойтись молча.
    assert set(data["failure_kind"]) == {v.value for v in FailureKind}


def test_interrupted_is_operational_not_methodological() -> None:
    """F1: ``interrupted`` — операционное значение ``crawl_session_status``.

    Класс энума определяется его **семантикой**, а не файлом-носителем (§13, ``AGENTS.md``):
    словарь несёт и методологические, и операционные перечисления. Секция
    ``crawl_session_status`` операционная (жизненный цикл сеанса краула), поэтому добавление
    значения **не** двигает ``status_dictionary_version`` — а он входит в
    ``METHODOLOGY_FIELDS``, и его смена потребовала бы ``golden-full`` и пере-аннотации
    golden, которых в проекте не было ни разу. Версия словаря — литерал, а не отпечаток
    содержимого, поэтому значение добавлено, а версия не тронута.

    Пин версии осознанный: методологический бамп по другой причине обязан обновить эту
    строку вместе с решением (§13), а операционное значение — нет.
    """
    from core.domain import CrawlSessionStatus, load_dictionary
    from core.pipeline.report import METHODOLOGY_FIELDS

    data = load_dictionary(DICT)
    assert CrawlSessionStatus.INTERRUPTED.value == "interrupted"
    assert "interrupted" in data["crawl_session_status"]
    # Множество словаря и перечисление совпадают (зеркало проверяется и в selfcheck).
    assert set(data["crawl_session_status"]) == {v.value for v in CrawlSessionStatus}
    # Версия словаря не сдвинулась добавлением операционного значения.
    assert str(data["status_dictionary_version"]) == "2.2.0"
    # Причина, по которой «просто добавить значение» было небезопасно: версия в METHODOLOGY_FIELDS.
    assert "status_dict_version" in METHODOLOGY_FIELDS
