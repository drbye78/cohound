"""Порт оценщика (LLM) — ``EvaluatorPort`` (§7, I18).

Гарантия I18 (без веб-поиска) — **структурная**: в интерфейсе физически нет параметров
``tools``/``web_search``; метод ``complete`` принимает только промпт и схему ответа.
Search-enabled модели обслуживаются другим портом (``ports/search.py``).

Типы выходного контракта — из домена (``LLMCellOutput``). Нейтрален (§1.3).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from pydantic import BaseModel

from core.domain import LLMCellOutput
from core.domain.enums import ReasoningEffort


@dataclass(frozen=True)
class TokenUsage:
    """Расход токенов вызова (стоимость кейса, §15).

    ``reasoning_tokens`` — токены **рассуждения**, если провайдер их называет
    (``usage.completion_tokens_details.reasoning_tokens`` либо
    ``completion_thinking_tokens``). Это провенанс исполнения, а не условие измерения:
    он не входит ни в ``run_id``, ни в вердикт, но показывает, действовал ли объявленный
    уровень усилия (``llm.reasoning_effort``) и стоил ли он денег. ``0`` — «провайдер не
    назвал» (не «рассуждения не было»): не названное не выдаётся за измеренный ноль.
    """

    prompt_tokens: int = 0
    completion_tokens: int = 0
    reasoning_tokens: int = 0


@dataclass(frozen=True)
class LLMRequest:
    """Запрос к оценщику. ``scenario`` всегда evaluator для этого порта (I21)."""

    prompt: str
    #: Класс pydantic-модели выходного контракта — **не** произвольный ``type``.
    #: Адаптер зовёт у него ``model_json_schema()`` (constrained decoding) и отвергает
    #: не-модель видимой ошибкой; голый ``type`` объявлял контракт, которого нет, и
    #: заставлял каждого потребителя либо глушить проверку, либо падать в рантайме.
    response_schema: type[BaseModel]
    model_id: str
    model_version: str = ""
    seed: int | None = None
    temperature: float | None = None
    #: Потолок генерации (V5). ``None`` — дефолт адаптера/провайдера. Условие измерения,
    #: а не «настройка вида»: без потолка сервер выбирает сам, и обрезание приходит как
    #: ``finish_reason=length`` с пустым текстом, неотличимое от невалидного JSON.
    max_tokens: int | None = None
    #: Объявленное усилие рассуждения (ось ``llm.reasoning_effort``). ``None`` — ось не
    #: объявлена ни на одном слое; ``off`` — объявлено «рассуждение не запрошено». Адаптер
    #: переводит уровень в тело запроса по семейству модели и **отказывает**, если уровень
    #: для этой модели невыразим: тихий пропуск параметра выдал бы «не просили» за
    #: «уровень не действовал».
    reasoning_effort: ReasoningEffort | None = None


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
    #: Текст рассуждения, если провайдер его отдал (``message.reasoning_content``).
    #: Провенанс **диагностики**: на успехе в журнал пишется только длина и хеш, на
    #: отказе — полный текст (см. :class:`core.pipeline.evaluate.LLMCallJournal`).
    #: ``None`` — не отдал, и это не выдумывается как пустая строка.
    reasoning: str | None = None


@runtime_checkable
class EvaluatorPort(Protocol):
    """Self-host оценщик с constrained decoding. **Без** инструментов/веб-поиска (I18)."""

    def complete(self, req: LLMRequest) -> LLMResponse: ...
