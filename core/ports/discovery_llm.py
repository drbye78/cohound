"""coHound — порт LLM-разведки (Discovery LLM) — ``DiscoveryLLMPort`` (§7.3, I18).

Отдельный от :class:`~core.ports.llm.EvaluatorPort` контракт для сценария
**разведки паспорта** (``passport-scout``): модель получает только собранный
промпт (текст страниц + инструкция) и возвращает **гипотезу** паспорта, а не
публикуемый статус ячейки. Выход — кандидат, продвигаемый человеческим решением
G2 (A3); порт не пишет evidence и не импортирует ``LLMCellOutput``/``Cell``/
``CellEvidence``.

**I18 — структурная.** В интерфейсе физически нет ``tools``/``web_search``: метод
``complete`` принимает только ``DiscoveryLLMRequest``. Search-enabled модели живут
за другим портом (``ports/search.py``); смешивать контуры запрещено (§7.2/§7.4).

Нейтрален (§1.3): без названий продуктов/вендоров и предметных терминов.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from pydantic import BaseModel

from core.ports.llm import TokenUsage

__all__ = [
    "DiscoveryLLMPort",
    "DiscoveryLLMRequest",
    "DiscoveryLLMResponse",
]


@dataclass(frozen=True)
class DiscoveryLLMRequest:
    """Запрос к LLM-разведке.

    ``response_schema`` — pydantic-модель **гипотезы** (например,
    ``DiscoveryPassportHypothesis``), а не ``LLMCellOutput``: это выход контура
    разведки, не оценщика. ``seed``/``temperature`` задают детерминизм сэмплирования.
    ``max_tokens=0`` — «взять дефолт адаптера» (для reasoning-моделей дефолт с запасом).
    """

    prompt: str
    #: Класс pydantic-модели гипотезы — **не** произвольный ``type``: адаптер зовёт у него
    #: ``model_json_schema()`` (constrained decoding) и ``__name__`` (имя схемы в запросе).
    #: Голый ``type`` объявлял контракт, которого нет.
    response_schema: type[BaseModel]
    model_id: str
    seed: int = 0
    temperature: float = 0.0
    max_tokens: int = 0


@dataclass(frozen=True)
class DiscoveryLLMResponse:
    """Ответ LLM-разведки: разобранная гипотеза + аудит-метаданные (§7).

    ``reasoning_tokens`` — токены «размышления» reasoning-модели (``0``, если не
    применимо). Они входят в ``usage.completion_tokens``, но выделяются отдельно для
    sidecar-провенанса (стоимость кейса, §15).
    """

    output: BaseModel
    raw: str
    prompt_hash: str
    decoder: str = "none"
    # ``default_factory``, а не общий ``TokenUsage()``: не про алиасинг (``TokenUsage``
    # объявлен ``frozen=True`` и мутировать его нельзя), а про форму — ruff-правило
    # RUF009 требует ``default_factory`` для вызываемого дефолта, и один канон в обоих
    # портах дешевле двух. Видимое значение дефолта то же (нули).
    usage: TokenUsage = field(default_factory=TokenUsage)
    reasoning_tokens: int = 0


@runtime_checkable
class DiscoveryLLMPort(Protocol):
    """LLM-разведка паспорта. **Без** инструментов/веб-поиска (I18)."""

    def complete(self, req: DiscoveryLLMRequest) -> DiscoveryLLMResponse: ...
