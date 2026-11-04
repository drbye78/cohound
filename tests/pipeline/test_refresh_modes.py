"""Политика наблюдения — **одна** таблица исполнения, без тихого фолбэка (план 19 §P3).

Дефект, который здесь закрыт: ветки ``policy is RefreshPolicy.OFFLINE`` жили в трёх
модулях, и новое значение enum'а молча получало старое поведение — «сеть разрешена»
(тихо в сеть) либо «корпус из чанков» (тихо в кэш). Оба исхода — ложь об условиях
измерения. Поэтому:

* каждое значение ``RefreshPolicy`` обязано иметь строку в ``_REFRESH_DISPATCH``
  (статическая проверка множества, а не поведения);
* неизвестное значение — отказ (``ValueError``), а не ``auto``;
* ось ``frozen_source`` действует только при ``frozen`` и канонизируется в ``n/a``
  иначе — иначе поданный, но не действующий флаг менял бы ``run_id``.
"""

from __future__ import annotations

import pytest

from core.domain.enums import CrawlCorpusSource, FrozenSource, RefreshPolicy
from core.pipeline.digests import EXECUTION_CONTRACT_FIELDS, execution_contract_digest
from core.pipeline.refresh_modes import (
    _FROZEN_SOURCE_TO_CORPUS,
    _REFRESH_DISPATCH,
    FROZEN_SOURCE_NOT_APPLICABLE,
    RefreshExecution,
    canonical_frozen_source,
    dispatch_refresh,
    resolve_frozen_source,
)


def test_every_refresh_policy_value_has_an_explicit_row() -> None:
    """Инвентарь: множество строк таблицы **равно** множеству значений enum'а.

    Это статическая проверка (не «вызвали и посмотрели»): добавление значения без
    строки падает здесь, а не в живом прогоне, где оно получило бы старое поведение.
    """
    assert set(_REFRESH_DISPATCH) == set(RefreshPolicy)
    for policy in RefreshPolicy:
        execution = dispatch_refresh(policy)
        assert isinstance(execution, RefreshExecution)


def test_dispatch_has_no_silent_default_for_unknown_policy() -> None:
    """Опечатка в политике — отказ; ``dict.get(policy, network)`` вернул бы сеть."""
    with pytest.raises(ValueError):
        dispatch_refresh("not-a-policy")
    with pytest.raises(ValueError):
        canonical_frozen_source("not-a-policy", "bytes")


def test_frozen_denies_network_and_dispatches_corpus_source() -> None:
    """``frozen`` — ноль сети для любой оси источника; ось лишь выбирает носитель."""
    for source in FrozenSource:
        execution = dispatch_refresh(RefreshPolicy.FROZEN, frozen_source=source)
        assert execution.network is False
        assert execution.discovery is False
        assert execution.validators is False
        assert execution.corpus_source is _FROZEN_SOURCE_TO_CORPUS[source]

    default = dispatch_refresh(RefreshPolicy.FROZEN)
    assert default.network is False
    assert default.corpus_source is CrawlCorpusSource.BYTES


@pytest.mark.parametrize(
    "policy",
    [RefreshPolicy.AUTO, RefreshPolicy.REVALIDATE, RefreshPolicy.REFRESH],
)
def test_network_policies_ignore_frozen_source(policy: RefreshPolicy) -> None:
    """При сетевой политике ось не читается: исполнение не зависит от флага."""
    assert (
        dispatch_refresh(policy, frozen_source="chunks").corpus_source
        is CrawlCorpusSource.NETWORK
    )


def test_frozen_source_canonicalization_makes_inactive_axis_neutral() -> None:
    """§6.3: ``n/a`` при любой политике, кроме ``frozen``; эффективное значение — при ней."""
    for policy in (
        RefreshPolicy.AUTO,
        RefreshPolicy.REVALIDATE,
        RefreshPolicy.REFRESH,
        RefreshPolicy.OFFLINE,
    ):
        assert canonical_frozen_source(policy, "bytes") == FROZEN_SOURCE_NOT_APPLICABLE
        assert canonical_frozen_source(policy, "chunks") == FROZEN_SOURCE_NOT_APPLICABLE
        assert canonical_frozen_source(policy, None) == FROZEN_SOURCE_NOT_APPLICABLE

    assert canonical_frozen_source(RefreshPolicy.FROZEN, None) == "bytes"
    assert canonical_frozen_source(RefreshPolicy.FROZEN, "bytes") == "bytes"
    assert canonical_frozen_source(RefreshPolicy.FROZEN, "chunks") == "chunks"


def test_frozen_source_rejects_unknown_value_instead_of_defaulting() -> None:
    """Неизвестный источник — отказ; тихого «возьмём байты» нет."""
    with pytest.raises(ValueError):
        resolve_frozen_source("tar")


def test_contract_digest_changes_for_frozen_but_not_for_inactive_axis() -> None:
    """Идентичность: ``frozen`` отличается от ``auto``/``offline``, а ``bytes`` при ``auto`` — нет.

    Сравниваются **контракты**, а не ``run_id`` целиком: ``refresh_policy`` входит в
    ``run_id`` отдельным слагаемым, поэтому здесь проверяется ровно вклад оси
    ``frozen_source`` в ``execution_contract_digest``.
    """
    def contract(frozen_source: str) -> dict[str, str]:
        return {field: "x" for field in EXECUTION_CONTRACT_FIELDS} | {
            "frozen_source": frozen_source
        }

    base = contract(FROZEN_SOURCE_NOT_APPLICABLE)
    auto_with_bytes = contract(
        canonical_frozen_source(RefreshPolicy.AUTO, "bytes")
    )
    assert execution_contract_digest(auto_with_bytes) == execution_contract_digest(base)

    frozen_bytes = contract(canonical_frozen_source(RefreshPolicy.FROZEN, "bytes"))
    frozen_chunks = contract(canonical_frozen_source(RefreshPolicy.FROZEN, "chunks"))
    assert execution_contract_digest(frozen_bytes) != execution_contract_digest(base)
    assert execution_contract_digest(frozen_chunks) != execution_contract_digest(frozen_bytes)
