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
    #: Потолок генерации (V5). ``None`` — дефолт адаптера/провайдера. Условие измерения,
    #: а не «настройка вида»: без потолка сервер выбирает сам, и обрезание приходит как
    #: ``finish_reason=length`` с пустым текстом, неотличимое от невалидного JSON.
    max_tokens: int | None = None


@dataclass(frozen=True)
class LLMResponse:
    """Ответ оценщика: разобранный выход + аудит-метаданные (§7)."""

    output: LLMCellOutput
    raw: str
    prompt_hash: str
    decoder: str = "none"
    #: Причина остановки генерации от провайдера (``stop``/``length``/…), если он её
    #: назвал. ``None`` — провайдер метку не отдаёт (шов ``generate``, часть серверов):
    #: «не названо» не выдаётся за ``stop``. Поле — провенанс исполнения (журнал вызовов),
    #: в вердикт и ``run_id`` не входит.
    finish_reason: str | None = None
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
