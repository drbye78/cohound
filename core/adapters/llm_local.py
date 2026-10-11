"""Оценщик — реализация ``EvaluatorPort`` (W9/T9.2, §7.1).

Адаптер обслуживает сценарий **evaluator** (I21): модель, доступная через
OI-совместимый API корпоративного облака, с **constrained decoding**
(xgrammar/XGrammar) и детерминированным сэмплированием (greedy при
``temperature == 0.0`` + ``seed``). Модель работает **только по переданным
чанкам** промпта и не вызывает внешний поиск — это гарантия I18.

**Не self-host.** Это клиент OI-совместимого API корпоративного облака (не установка
vLLM/SGLang у нас): vLLM/SGLang могут быть бэкендом облака, но не нашим стеком/зависимостью.

**I18 — структурная.** Порт ``EvaluatorPort`` физически не принимает ``tools``/
``web_search``, и конструктор/``complete`` этого адаптера их тоже не принимают и не
передают в HTTP-запрос. Search-enabled модели живут за другим портом
(``ports/search.py``); смешивать контуры запрещено (§7.2/§7.4). В HTTP-payload
отправляются только ``model``/``messages``/``temperature``/``seed``/``response_format``.

**Constrained decoding — форма, не семантика.** Реальный путь передаёт JSON Schema
ячейки (``response_format={"type": "json_schema", ...}``), из которой OI-совместимый
сервер (например, на бэкенде vLLM/SGLang облака) строит грамматику xgrammar и навязывает
её на уровне сэмплера. Это гарантирует только
*форму*; механическая валидация (source_id→url, цитата-подстрока, достаточность) —
отдельный эшелон §10 (``pipeline/validate.py``). Поле ``decoder`` в ``LLMResponse``
фиксирует механизм (``"xgrammar"``/``"json_schema"``/``"none"``) для манифеста (R7).

**Детерминизм.** ``seed`` и ``temperature`` из запроса (или дефолты адаптера) уходят
в запрос; ``temperature == 0.0`` — жадный режим. ``prompt_hash`` детерминированно
вычисляется из промпта (``content_hash``): канонический ``prompt_hash`` (шаблон + rule
+ absence_rule + отсортированные ``chunk_id``) компилируется в ``pipeline/prompt.py``
(T9.1) и в данный порт полем не передаётся, поэтому адаптер хэширует сам промпт.

**Ленивый тяжёлый импорт.** Модуль импортируется и тестируется без ``httpx``:
HTTP-клиент подтягивается только в реальном пути, а ``available`` показывает готовность.

**Изолированный шов для тестов.** Конструктор принимает ``generate`` — callable
``(prompt, response_schema) -> str`` (сырой JSON). Если он задан, реальный путь не
задействуется: тесты не требуют сети. Если ``generate is None`` — реальный HTTP-путь
к openai-совместимому ``/v1/chat/completions``.

**Ошибки.** Невалидный JSON/форма/нарушение инвариантов модели (I1/I7) →
``EvaluationError``; pipeline (T9.3) отображает его в ``evaluation_failed`` с retry ≤2
(§7.1). Отсутствие HTTP-зависимости без внедрённого ``generate`` → ``EvaluatorUnavailable``.

Нейтрален (§1.3): идентификаторы моделей/движков — данные, переопределяемые параметрами.
"""

from __future__ import annotations

import importlib.util
import json
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, ValidationError

from core.domain import LLMCellOutput
from core.ports.llm import EvaluatorPort, LLMRequest, LLMResponse, TokenUsage
from core.util.hashing import content_hash

__all__ = [
    "EvaluationError",
    "EvaluatorUnavailable",
    "LocalEvaluator",
    "GenerateFn",
    "DEFAULT_MODEL_ID",
    "DEFAULT_ENDPOINT_KIND",
    "DEFAULT_DECODER",
    "DEFAULT_BASE_URL",
    "DEFAULT_TIMEOUT",
    "CHAT_COMPLETIONS_PATH",
]

#: Оценщик по умолчанию (§7.4, MVP): модель из корпоративного облака. Переопределяется ``model_id``.
DEFAULT_MODEL_ID = "deepseek-v4.1-flash:cxbc"
#: Профиль зоны (07): API корпоративного облака (OI-совместимый), не self-host инсталляция.
DEFAULT_ENDPOINT_KIND = "api"
#: Механизм constrained decoding по умолчанию: без навязанной грамматики.
DEFAULT_DECODER = "none"
#: Плейсхолдер адреса OI-совместимого API (не боевой endpoint). Реальный внутренний
#: endpoint периметра всегда передаёт composition root (``build_evaluator_llm``); оценщик
#: конструируется только при сконфигурированном endpoint. Значение непустое, т.к.
#: конструктор отвергает пустой ``base_url``.
DEFAULT_BASE_URL = "http://localhost:8000"
#: Таймаут HTTP-запроса, с.
DEFAULT_TIMEOUT = 120.0
#: Openai-совместимый путь генерации.
CHAT_COMPLETIONS_PATH = "/v1/chat/completions"

#: Внедряемая функция генерации: (промпт, схема ответа) → сырой JSON-строкой.
GenerateFn = Callable[[str, type], str]


class EvaluationError(RuntimeError):
    """Оценщик вернул невалидный JSON/форму или нарушил инварианты ячейки (§7.1).

    Pipeline отображает ошибку в ``evaluation_failed`` с retry ≤2 (не в статус — I4).
    """


class EvaluatorUnavailable(RuntimeError):
    """Оценщик недоступен: нет HTTP-зависимости и не внедрён ``generate``."""


def _httpx_available() -> bool:
    """Установлен ли ``httpx`` (без импорта пакета)."""
    return importlib.util.find_spec("httpx") is not None


class LocalEvaluator:
    """Оценщик за ``EvaluatorPort`` (OI-совместимый API корпоративного облака).

    :param generate: шов для тестов; ``None`` → реальный HTTP-путь.
    :param model_id: идентификатор модели по умолчанию (переопределяется ``req.model_id``).
    :param model_version: версия весов (манифест §13); по умолчанию пусто.
    :param endpoint_kind: профиль зоны (``api``/``self_host``), данные манифеста.
    :param decoder: записываемый механизм constrained decoding
        (``"xgrammar"``/``"json_schema"``/``"none"``). ``"none"`` → без ``response_format``.
    :param seed: seed по умолчанию (детерминизм); переопределяется ``req.seed``.
    :param temperature: температура по умолчанию; ``0.0`` — greedy.
    :param base_url: база OI-совместимого API (реальный путь).
    :param api_key: необязательный bearer-токен (ключ корпоративного облака).
    :param timeout: таймаут HTTP-запроса, с.
    """

    def __init__(
        self,
        generate: GenerateFn | None = None,
        *,
        model_id: str = DEFAULT_MODEL_ID,
        model_version: str = "",
        endpoint_kind: str = DEFAULT_ENDPOINT_KIND,
        decoder: str = DEFAULT_DECODER,
        seed: int = 0,
        temperature: float = 0.0,
        base_url: str = DEFAULT_BASE_URL,
        api_key: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        if not model_id:
            raise ValueError("model_id обязан быть непустым")
        if not endpoint_kind:
            raise ValueError("endpoint_kind обязан быть непустым")
        if not base_url:
            raise ValueError("base_url обязан быть непустым")
        if timeout <= 0:
            raise ValueError(f"timeout обязан быть положительным, получено {timeout!r}")
        if seed < 0:
            raise ValueError(f"seed не может быть отрицательным, получено {seed!r}")
        if temperature < 0.0:
            raise ValueError(f"temperature не может быть отрицательной, получено {temperature!r}")
        # I18: здесь намеренно НЕТ и не будет параметров tools/web_search.
        self.model_id = model_id
        self.model_version = model_version
        self.endpoint_kind = endpoint_kind
        self.decoder = decoder
        self.seed = seed
        self.temperature = temperature
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self._generate = generate

    # ------------------------------------------------------------------
    # Контракт порта
    # ------------------------------------------------------------------

    @property
    def available(self) -> bool:
        """Готов ли адаптер: внедрён ``generate`` либо установлен ``httpx``."""
        if self._generate is not None:
            return True
        return _httpx_available()

    def complete(self, req: LLMRequest) -> LLMResponse:
        """Выполняет один вызов оценщика и разбирает выход в ``LLMCellOutput`` (§7.1).

        :raises EvaluationError: невалидный JSON/форма/инварианты ячейки.
        :raises EvaluatorUnavailable: нет ``generate`` и нет ``httpx``.
        """
        seed = req.seed if req.seed is not None else self.seed
        temperature = req.temperature if req.temperature is not None else self.temperature
        usage = TokenUsage()
        if self._generate is not None:
            raw = self._generate(req.prompt, req.response_schema)
        else:
            raw, usage = self._http_complete(req, seed=seed, temperature=temperature)
        raw = raw if isinstance(raw, str) else str(raw)
        output = self._parse(req.response_schema, raw)
        return LLMResponse(
            output=output,
            raw=raw,
            prompt_hash=content_hash(req.prompt),
            decoder=self.decoder,
            usage=usage,
        )

    # ------------------------------------------------------------------
    # Разбор / валидация
    # ------------------------------------------------------------------

    def _parse(self, schema: type, raw: str) -> LLMCellOutput:
        """Разбирает сырой JSON в модель ``schema`` и проверяет форму/inварианты (§7.1)."""
        if not (isinstance(schema, type) and issubclass(schema, BaseModel)):
            raise EvaluationError(
                f"response_schema обязан быть pydantic-моделью, получено {schema!r}"
            )
        try:
            parsed = schema.model_validate_json(raw)
        except ValidationError as exc:
            raise EvaluationError(
                f"оценщик вернул невалидный JSON/форму ({exc.error_count()} ошибок): {exc}"
            ) from exc
        except (ValueError, TypeError) as exc:  # защитный слой (не JSON, None и т.п.)
            raise EvaluationError(f"не удалось разобрать выход оценщика: {exc}") from exc
        if not isinstance(parsed, LLMCellOutput):
            raise EvaluationError(
                f"схема {schema.__name__} не является LLMCellOutput (контракт §7.1)"
            )
        return parsed

    # ------------------------------------------------------------------
    # Реальный HTTP-путь (openai-совместимый /v1/chat/completions)
    # ------------------------------------------------------------------

    def _http_complete(
        self, req: LLMRequest, *, seed: int, temperature: float
    ) -> tuple[str, TokenUsage]:
        """Реальный вызов OI-совместимого API. Схема → constrained decoding; без tools (I18)."""
        httpx = self._import_http()
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        payload: dict[str, Any] = {
            "model": req.model_id or self.model_id,
            "messages": [{"role": "user", "content": req.prompt}],
            "temperature": temperature,  # 0.0 ⇒ greedy (детерминизм)
            "seed": seed,
            "stream": False,
        }
        if self.decoder != "none":
            # Форма, не семантика: сервер строит грамматику xgrammar из JSON Schema (§7.1).
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": "llm_cell_output",
                    "schema": req.response_schema.model_json_schema(),
                    "strict": True,
                },
            }
        # I18: tools/web_search в запрос НЕ добавляются — здесь их нет по построению.
        try:
            response = httpx.post(
                f"{self.base_url}{CHAT_COMPLETIONS_PATH}",
                json=payload,
                headers=headers,
                timeout=self.timeout,
            )
            response.raise_for_status()
            data = response.json()
        except Exception as exc:  # httpx.HTTPError/JSON-ошибки транспорта
            raise EvaluationError(f"HTTP-вызов оценщика не удался: {exc}") from exc
        return self._extract(data)

    def _extract(self, data: Any) -> tuple[str, TokenUsage]:
        """Достаёт ``choices[0].message.content`` и ``usage`` из ответа сервера."""
        try:
            message = data["choices"][0]["message"]
            content = message["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise EvaluationError(f"неожиданная форма ответа сервера: {data!r}") from exc
        if not isinstance(content, str):
            raise EvaluationError(
                f"choices[0].message.content обязан быть строкой, получено {type(content).__name__}"
            )
        usage_raw = data.get("usage") or {}
        usage = TokenUsage(
            prompt_tokens=int(usage_raw.get("prompt_tokens", 0) or 0),
            completion_tokens=int(usage_raw.get("completion_tokens", 0) or 0),
        )
        return content, usage

    def _import_http(self) -> Any:
        """Лениво импортирует ``httpx``; при отсутствии — ``EvaluatorUnavailable``."""
        if not _httpx_available():
            raise EvaluatorUnavailable(
                "httpx не установлен: реальный HTTP-путь оценщика недоступен. "
                "Установите extra 'crawl' (05) или передайте generate."
            )
        import httpx

        return httpx
