"""Порт оценщика (LLM) — ``EvaluatorPort`` (§7, I18).

Гарантия I18 (без веб-поиска) — **структурная**: в интерфейсе физически нет параметров
``tools``/``web_search``; метод ``complete`` принимает только промпт и схему ответа.
Search-enabled модели обслуживаются другим портом (``ports/search.py``).

Типы выходного контракта — из домена (``LLMCellOutput``). Нейтрален (§1.3).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from core.domain import LLMCellOutput


@dataclass(frozen=True)
class TokenUsage:
    """Расход токенов вызова (стоимость кейса, §15)."""

    prompt_tokens: int = 0
    completion_tokens: int = 0


@dataclass(frozen=True)
class LLMRequest:
    """Запрос к оценщику. ``scenario`` всегда evaluator для этого порта (I21)."""

    prompt: str
    response_schema: type
    model_id: str
    model_version: str = ""
    seed: int | None = None
    temperature: float | None = None


@dataclass(frozen=True)
class LLMResponse:
    """Ответ оценщика: разобранный выход + аудит-метаданные (§7)."""

    output: LLMCellOutput
    raw: str
    prompt_hash: str
    decoder: str = "none"
    # Общий дефолт безопасен и оставлен намеренно: ``TokenUsage`` объявлен
    # ``frozen=True`` (проверяется тестом), поэтому экземпляр неизменяем, а правила
    # RUF009/B008 защищают от **мутабельных** дефолтов — мутировать здесь нечем. Директиву
    # подавления не ставлю: ruff это место не помечает (RUF009 не видит дефолт, объявленный
    # в том же модуле), а noqa, которая ничего не подавляет, врёт о наличии исключения.
    usage: TokenUsage = TokenUsage()


@runtime_checkable
class EvaluatorPort(Protocol):
    """Self-host оценщик с constrained decoding. **Без** инструментов/веб-поиска (I18)."""

    def complete(self, req: LLMRequest) -> LLMResponse: ...
