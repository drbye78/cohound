"""coHound — разведка **списка** сателлитов объявленного продукта (не черновик паспорта).

Отличие от :mod:`core.pipeline.passport_scout`. Скаут строит черновик **всего** паспорта
по одной странице нового продукта (онбординг). Здесь у продукта паспорт уже есть, и вопрос
другой: «какие сателлиты этот продукт объявляет сегодня» — по его же документации, чтобы
список можно было сверить с объявленным и, при подтверждении человеком, обновить.

Почему это отдельный проход, а не поле в скауте. Сателлит — это **утверждение о связи**
(«Kintsugi — консоль управления, входит в поставку»), и оно опирается на цитату из
материала продукта. Скаут такой цитаты не требует (он собирает гипотезы), а здесь каждая
находка обязана нести ``relationship_evidence``; находка без цитаты остаётся в
``needs_human`` и **не** становится кандидатом.

Что возвращается. Три состояния, и они разные по смыслу:

* ``proposed`` — найдено в документации, в паспорте нет: кандидат на добавление;
* ``confirmed`` — найдено и объявлено: связь подтверждена повторно;
* ``not_rediscovered`` — объявлено (``approved``), а в собранных страницах не нашлось.
  Это **не** «сателлита нет»: страницы могли не содержать упоминания, а объявление
  опирается на другой источник. Поэтому состояние не понижается автоматически (I6/I7).

Ничего не пишется: результат — наблюдение и review-артефакт; паспорт правит человек
(``hound package satellites discover --apply --actor``).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from core.config.passports import passport_urls
from core.domain.enums import SatelliteState
from core.domain.models import Passport, Satellite
from core.pipeline.passport_scout import (
    DiscoveryPassportHypothesis,
    _build_prompt,
    _build_satellite,
    _fetch_one,
    _join_pages,
    _usage_dict,
)
from core.ports.discovery_llm import DiscoveryLLMRequest
from core.util.hashing import content_hash

__all__ = ["SatelliteDiscoveryReport", "discover_satellites"]

#: Сколько страниц продукта смотреть по умолчанию. Порог **наблюдения**: список сателлитов
#: объявляется на обзорных страницах («состав дистрибутива», «компоненты»), а не в глубине
#: справочника; больше страниц не расширяют набор, но платят фетчами.
DEFAULT_MAX_PAGES = 4

#: Предел символов текста, уходящего в промпт (тот же порядок, что у скаута).
DEFAULT_MAX_CHARS = 20000


@dataclass
class SatelliteDiscoveryReport:
    """Наблюдение списка сателлитов: кандидаты, подтверждения и расхождения."""

    product_id: str
    ok: bool = True
    proposed: list[Satellite] = field(default_factory=list)
    confirmed: list[dict[str, Any]] = field(default_factory=list)
    not_rediscovered: list[dict[str, Any]] = field(default_factory=list)
    needs_human: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    fetched: list[dict[str, Any]] = field(default_factory=list)
    prompt_hash: str = ""
    model_id: str = ""
    usage: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "product_id": self.product_id,
            "ok": self.ok,
            "proposed": [item.model_dump(mode="json") for item in self.proposed],
            "confirmed": list(self.confirmed),
            "not_rediscovered": list(self.not_rediscovered),
            "needs_human": list(self.needs_human),
            "errors": list(self.errors),
            "fetched": list(self.fetched),
            "prompt_hash": self.prompt_hash,
            "model_id": self.model_id,
            "usage": dict(self.usage),
        }


def _identity(role: str, component: str | None) -> tuple[str, str]:
    """Ключ сравнения: роль и компонент без учёта регистра (написание у вендора плавает)."""
    return (role.strip().lower(), (component or "").strip().lower())


def discover_satellites(
    passport: Passport,
    *,
    crawler: Any,
    extractor: Any,
    discovery_llm: Any,
    roles: Sequence[str] | None = None,
    max_pages: int = DEFAULT_MAX_PAGES,
    max_chars: int = DEFAULT_MAX_CHARS,
) -> SatelliteDiscoveryReport:
    """Собирает страницы продукта и спрашивает LLM, какие сателлиты он объявляет.

    Страницы берутся из объявленной области продукта (:func:`passport_urls`), а не из
    произвольного поиска: список сателлитов — утверждение о **продукте**, и опора на его же
    документацию делает находку проверяемой. Отсутствие объявленной области — ошибка
    наблюдения (``no_doc_domains``), а не пустой список: «не смотрели» ≠ «не нашли».

    :param roles: подсказка ролей из каталога пакета (не меняет инварианты).
    :param max_pages: сколько адресов области смотреть (первый — всегда первый объявленный).
    :param max_chars: предел символов текста в промпте.
    """
    report = SatelliteDiscoveryReport(product_id=passport.product_id)
    urls = passport_urls(passport)[: max(1, max_pages)]
    if not urls:
        report.ok = False
        report.errors.append("no_doc_domains: область продукта не объявлена")
        return report

    page_texts: dict[str, str] = {}
    page_errors: list[str] = []
    for url in urls:
        _fetch_one(
            url,
            crawler=crawler,
            extractor=extractor,
            fetched=report.fetched,
            page_texts=page_texts,
            page_errors=page_errors,
        )
    report.errors.extend(page_errors)
    if not page_texts:
        report.ok = False
        report.errors.append("fetch: ни одна страница продукта не извлечена")
        return report

    allowed: set[str] = set(page_texts) | set(urls)
    corpus_text = _join_pages(page_texts, max_chars)
    prompt = _build_prompt(
        product_name=passport.canonical_name,
        url=urls[0],
        roles=list(roles) if roles else None,
        pages=corpus_text,
    )
    report.prompt_hash = content_hash(prompt)
    report.model_id = str(getattr(discovery_llm, "model_id", "") or "")

    hypothesis = DiscoveryPassportHypothesis()
    try:
        response = discovery_llm.complete(
            DiscoveryLLMRequest(
                prompt=prompt,
                response_schema=DiscoveryPassportHypothesis,
                model_id=report.model_id,
            )
        )
        output = getattr(response, "output", None)
        if isinstance(output, DiscoveryPassportHypothesis):
            hypothesis = output
        elif output is not None and hasattr(output, "model_dump"):
            hypothesis = DiscoveryPassportHypothesis.model_validate(output.model_dump())
        report.prompt_hash = getattr(response, "prompt_hash", "") or report.prompt_hash
        report.usage = _usage_dict(
            getattr(response, "usage", None),
            int(getattr(response, "reasoning_tokens", 0) or 0),
        )
    except Exception as exc:  # noqa: BLE001 — сбой инференса фиксируется, не роняет отчёт
        report.ok = False
        report.errors.append(f"llm: {type(exc).__name__}: {exc}")
        return report

    declared = {_identity(sat.role, sat.component): sat for sat in passport.satellites}
    seen: set[tuple[str, str]] = set()
    provenance: list[dict[str, Any]] = []
    for hyp in hypothesis.satellites:
        satellite = _build_satellite(
            hyp,
            allowed=allowed,
            corpus_text=corpus_text,
            needs_human=report.needs_human,
            provenance=provenance,
        )
        if satellite.state is SatelliteState.NOT_FOUND:
            continue
        key = _identity(satellite.role, satellite.component)
        seen.add(key)
        existing = declared.get(key)
        if existing is None:
            # Без цитаты связи кандидатом не становится: утверждение о связи без
            # подтверждения — гипотеза. Отсечение обязано быть **видимым**: иначе
            # «модель не нашла» и «нашла, но нечем подтвердить» неотличимы в отчёте.
            if satellite.relationship_evidence is None:
                report.needs_human.append(
                    f"сателлит {satellite.component or satellite.role}: "
                    "связь не подтверждена цитатой — кандидатом не становится"
                )
                continue
            report.proposed.append(satellite)
            continue
        report.confirmed.append(
            {
                "role": existing.role,
                "component": existing.component,
                "declared_state": existing.state.value,
                "evidence_confirmed": satellite.relationship_evidence is not None,
                "quote": (
                    satellite.relationship_evidence.quote
                    if satellite.relationship_evidence is not None
                    else None
                ),
            }
        )

    for key, satellite in declared.items():
        if key in seen:
            continue
        if satellite.state not in {
            SatelliteState.APPROVED,
            SatelliteState.EDITED_BY_HUMAN,
        }:
            continue
        report.not_rediscovered.append(
            {
                "role": satellite.role,
                "component": satellite.component,
                "declared_state": satellite.state.value,
                "note": "не найдено на собранных страницах; состояние не понижается (I6/I7)",
            }
        )
    return report
