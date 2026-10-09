"""Discovery LLM — реализация ``DiscoveryLLMPort`` (W14, §7.3, I18).

Тонкий openai-совместимый адаптер для сценария **разведки паспорта**
(``hound passport-scout``): получает промпт с текстом страниц и возвращает
pydantic-модель **гипотезы** паспорта. Адаптер не пишет evidence, не импортирует
``LLMCellOutput``/``Cell``/``CellEvidence`` и не принимает ``tools``/``web_search``
(I18 — граница структурная, как у :mod:`core.adapters.llm_local`).

**Одна попытка (бюджет).** Адаптер не ретраит: инференс — единственный вызов.
Невалидный JSON/форма → ``DiscoveryLLMError``; частичный/выдуманный результат не
возвращается.

**Разбор выхода.** Сначала ``schema.model_validate_json(raw)``; при провале из текста
вырезается первый **сбалансированный** JSON-объект (с учётом вложенности и строк) и
разбор повторяется. Повторный провал → ``DiscoveryLLMError``.

**Ленивый тяжёлый импорт.** ``httpx`` подтягивается только в реальном пути;
``available`` показывает готовность. Конструктор принимает ``generate`` — шов
``(prompt, schema) -> str`` для тестов/оффлайна (как в ``LocalEvaluator``): если он
задан, сеть не задействуется.

Нейтрален (§1.3): идентификаторы моделей/движков — данные, переопределяемые
параметрами.
"""

from __future__ import annotations

import importlib.util
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, ValidationError

from core.ports.discovery_llm import (
    DiscoveryLLMRequest,
    DiscoveryLLMResponse,
)
from core.ports.llm import TokenUsage
from core.util.hashing import content_hash

__all__ = [
    "DiscoveryLLMError",
    "DiscoveryLLMUnavailable",
    "DiscoveryLLM",
    "GenerateFn",
    "DEFAULT_DECODER",
    "DEFAULT_TIMEOUT",
    "DEFAULT_MAX_TOKENS",
    "CHAT_COMPLETIONS_PATH",
]

#: Механизм декодирования по умолчанию. ``json_object`` подтверждён рабочим на
#: целевом openai-совместимом сервере; ``json_schema`` — best-effort (не подтверждён).
DEFAULT_DECODER = "json_object"
#: Таймаут HTTP-запроса, с.
DEFAULT_TIMEOUT = 120.0
#: Потолок генерации по умолчанию. Целевая модель — REASONING: thinking-токены
#: списываются из ``max_tokens``, поэтому дефолт с запасом (thinking + JSON-ответ).
DEFAULT_MAX_TOKENS = 4096
#: Openai-совместимый путь генерации (относительно ``base_url``).
CHAT_COMPLETIONS_PATH = "/chat/completions"

#: Внедряемая функция генерации: (промпт, схема ответа) → сырой JSON-строкой.
GenerateFn = Callable[[str, type], str]


class DiscoveryLLMError(RuntimeError):
    """LLM-разведка вернула невалидный JSON/форму либо сбой транспорта."""


class DiscoveryLLMUnavailable(DiscoveryLLMError):
    """LLM-разведка недоступна: нет ``httpx`` и не внедрён ``generate``."""


def _httpx_available() -> bool:
    """Установлен ли ``httpx`` (без импорта пакета)."""
    return importlib.util.find_spec("httpx") is not None


def _extract_json_object(text: str) -> str | None:
    """Вырезает первый сбалансированный JSON-объект из текста (``{...}``).

    Учитывает вложенность, строковые литералы и экранирование (``\\"``), поэтому не
    обрывается на первой закрывающей скобке внутри строки. Возвращает ``None``, если
    сбалансированный объект не найден.
    """
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(text)):
        ch = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    return None


class DiscoveryLLM:
    """Openai-совместимый клиент LLM-разведки за ``DiscoveryLLMPort``.

    :param base_url: база сервера (например ``http://localhost:8000/v1``); пустая —
        адаптер недоступен (кроме случая внедрённого ``generate``).
    :param api_key: необязательный bearer-токен.
    :param model_id: идентификатор модели по умолчанию (переопределяется ``req.model_id``).
    :param timeout: таймаут HTTP-запроса, с.
    :param decoder: ``json_schema`` | ``json_object`` | ``none``.
    :param max_tokens: потолок генерации по умолчанию.
    :param generate: шов для тестов/оффлайна; ``None`` → реальный HTTP-путь.
    """

    def __init__(
        self,
        base_url: str = "",
        *,
        api_key: str | None = None,
        model_id: str = "",
        timeout: float = DEFAULT_TIMEOUT,
        decoder: str = DEFAULT_DECODER,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        generate: GenerateFn | None = None,
    ) -> None:
        if timeout <= 0:
            raise ValueError(f"timeout обязан быть положительным, получено {timeout!r}")
        if max_tokens <= 0:
            raise ValueError(f"max_tokens обязан быть положительным, получено {max_tokens!r}")
        # I18: здесь намеренно НЕТ и не будет параметров tools/web_search.
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model_id = model_id
        self.timeout = timeout
        self.decoder = decoder
        self.max_tokens = max_tokens
        self._generate = generate

    # ------------------------------------------------------------------
    # Контракт порта
    # ------------------------------------------------------------------

    @property
    def available(self) -> bool:
        """Готов ли адаптер: внедрён ``generate`` ИЛИ (есть ``httpx`` и непустой ``base_url``)."""
        if self._generate is not None:
            return True
        return _httpx_available() and bool(self.base_url)

    def complete(self, req: DiscoveryLLMRequest) -> DiscoveryLLMResponse:
        """Выполняет один вызов LLM-разведки и разбирает гипотезу.

        :raises DiscoveryLLMError: невалидный JSON/форма либо сбой транспорта.
        :raises DiscoveryLLMUnavailable: нет ``generate`` и нет ``httpx``.
        """
        usage = TokenUsage()
        reasoning_tokens = 0
        if self._generate is not None:
            raw = self._generate(req.prompt, req.response_schema)
        else:
            raw, usage, reasoning_tokens = self._http_complete(req)
        raw = raw if isinstance(raw, str) else str(raw)
        output = self._parse(req.response_schema, raw)
        return DiscoveryLLMResponse(
            output=output,
            raw=raw,
            prompt_hash=content_hash(req.prompt),
            decoder=self.decoder,
            usage=usage,
            reasoning_tokens=reasoning_tokens,
        )

    # ------------------------------------------------------------------
    # Разбор
    # ------------------------------------------------------------------

    def _parse(self, schema: type, raw: str) -> BaseModel:
        """Разбирает сырой JSON в модель ``schema`` (с fallback-вырезанием объекта)."""
        if not (isinstance(schema, type) and issubclass(schema, BaseModel)):
            raise DiscoveryLLMError(
                f"response_schema обязан быть pydantic-моделью, получено {schema!r}"
            )
        try:
            return schema.model_validate_json(raw)
        except (ValidationError, ValueError, TypeError):
            pass
        candidate = _extract_json_object(raw)
        if candidate is not None and candidate != raw:
            try:
                return schema.model_validate_json(candidate)
            except (ValidationError, ValueError, TypeError) as exc:
                raise DiscoveryLLMError(
                    f"LLM-разведка вернула невалидную гипотезу: {exc}"
                ) from exc
        raise DiscoveryLLMError("LLM-разведка вернула невалидный/не-JSON выход")

    # ------------------------------------------------------------------
    # Реальный HTTP-путь (openai-совместимый /chat/completions)
    # ------------------------------------------------------------------

    def _http_complete(
        self, req: DiscoveryLLMRequest
    ) -> tuple[str, TokenUsage, int]:
        """Реальный вызов сервера. Поддерживает ``json_schema``/``json_object``/``none``."""
        httpx = self._import_http()
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        payload: dict[str, Any] = {
            "model": req.model_id or self.model_id,
            "messages": [{"role": "user", "content": req.prompt}],
            "temperature": req.temperature,
            "seed": req.seed,
            "max_tokens": req.max_tokens or self.max_tokens,
            "stream": False,
        }
        if self.decoder == "json_schema":
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": req.response_schema.__name__,
                    "schema": req.response_schema.model_json_schema(),
                    "strict": True,
                },
            }
        elif self.decoder == "json_object":
            payload["response_format"] = {"type": "json_object"}
        # decoder == "none": response_format не добавляется.
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
            raise DiscoveryLLMError(f"HTTP-вызов LLM-разведки не удался: {exc}") from exc
        return self._extract(data)

    def _extract(self, data: Any) -> tuple[str, TokenUsage, int]:
        """Достаёт ``choices[0].message.content``/``finish_reason`` и ``usage``.

        Для REASONING-моделей thinking-токены списываются из ``max_tokens``: если весь
        бюджет ушёл в reasoning, приходит ``finish_reason=="length"`` с пустым
        ``content``. Это не «угадывается» — поднимается понятная ошибка с просьбой
        увеличить ``max_tokens`` (сравните с ``reasoning_content`` в ответе).
        """
        try:
            choice = data["choices"][0]
            content = choice["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise DiscoveryLLMError(f"неожиданная форма ответа сервера: {data!r}") from exc
        finish_reason = choice.get("finish_reason") if isinstance(choice, dict) else None
        if (content is None or content == "") and finish_reason == "length":
            raise DiscoveryLLMError(
                "модель израсходовала max_tokens на reasoning; "
                "увеличьте COHOUND_LLM_MAX_TOKENS (или max_tokens)"
            )
        if not isinstance(content, str):
            raise DiscoveryLLMError(
                f"choices[0].message.content обязан быть строкой, получено {type(content).__name__}"
            )
        usage_raw = data.get("usage") or {}
        usage = TokenUsage(
            prompt_tokens=int(usage_raw.get("prompt_tokens", 0) or 0),
            completion_tokens=int(usage_raw.get("completion_tokens", 0) or 0),
        )
        details = usage_raw.get("completion_tokens_details") or {}
        reasoning_tokens = int(
            details.get("reasoning_tokens", usage_raw.get("completion_thinking_tokens", 0))
            or 0
        )
        return content, usage, reasoning_tokens

    def _import_http(self) -> Any:
        """Лениво импортирует ``httpx``; при отсутствии — ``DiscoveryLLMUnavailable``."""
        if not _httpx_available():
            raise DiscoveryLLMUnavailable(
                "httpx не установлен: реальный HTTP-путь LLM-разведки недоступен. "
                "Установите extra 'crawl' (05) или передайте generate."
            )
        import httpx

        return httpx
