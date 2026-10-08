"""Self-host оценщик на CPU — реализация ``EvaluatorPort`` через ``llama-cpp-python`` (W9/T9.6, §7).

Fallback-путь к основному vLLM/SGLang-адаптеру (``adapters/llm_local.py``, T9.2): тот же
порт ``EvaluatorPort``, но инференс идёт по GGUF-весам на CPU через ``llama-cpp-python``
(05 §2.3). Нужен, чтобы интеграционно проверять контур оценщика **без GPU** и без сети
(DoD T9.6), а также как аварийный CPU-путь.

**I18 — структурная.** Порт ``EvaluatorPort`` физически не принимает параметров поиска, и
конструктор/``complete`` этого адаптера их тоже не принимают и не передают в бэкенд.
Модель работает только по переданному промпту (чанки уже вкомпилированы в промпт
``pipeline/prompt.py``, T9.1). Поисковые модели живут за другим портом
(``ports/search.py``); смешивать контуры запрещено (§7.2/§7.4).

**Constrained decoding — форма, не семантика.** Когда ``decoder != "none"``, из JSON Schema
ячейки (``req.response_schema``) строится GBNF-грамматика
(``LlamaGrammar.from_json_schema``) и навязывается сэмплеру llama.cpp. Это гарантирует
только *форму*; семантическая валидация (source_id→url, цитата-подстрока, достаточность) —
отдельный эшелон §10 (``pipeline/validate.py``). Поле ``decoder`` в ``LLMResponse``
фиксирует механизм (по умолчанию ``"gbnf"``) для манифеста (R7) и **отличается** от
``xgrammar``/``json_schema`` основного адаптера — провенанс различим.

**Детерминизм.** ``seed`` и ``temperature`` из запроса (или дефолты адаптера) уходят в
llama.cpp; ``temperature == 0.0`` — жадный режим. ``prompt_hash`` детерминированно
вычисляется из промпта (``content_hash``): канонический ``prompt_hash`` (шаблон + rule +
absence_rule + отсортированные ``chunk_id``) компилируется в ``pipeline/prompt.py`` (T9.1)
и в порт полем не передаётся, поэтому адаптер хэширует сам промпт.

**Ленивый тяжёлый импорт.** Модуль импортируется и тестируется без ``llama_cpp``: пакет
импортируется только в реальном пути загрузки модели, а ``available`` показывает
готовность (пакет установлен **и** файл GGUF существует).

**Изолированный шов для тестов.** Конструктор принимает ``generate`` — callable
``(LLMRequest) -> str`` (сырой JSON). Если он задан, реальный путь не задействуется: тесты
не требуют ни пакета, ни GGUF-модели, ни GPU (зеркало шва ``generate`` в ``llm_local``).

**Ошибки.** Невалидный JSON/форма/нарушение инвариантов ячейки (I1/I7) → ``EvaluationError``
(тот же тип, что у основного адаптера, — pipeline обрабатывает uniform, T9.3). Сбой бэкенда
llama.cpp → ``LlamaCppError``. Отсутствие пакета/модели без внедрённого ``generate`` →
``EvaluatorUnavailable``.

Нейтрален (§1.3): идентификаторы модели/пути — данные, переопределяемые параметрами.
"""

from __future__ import annotations

import importlib.util
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError

from core.adapters.llm_local import EvaluationError
from core.domain import LLMCellOutput
from core.ports.llm import EvaluatorPort, LLMRequest, LLMResponse, TokenUsage
from core.util.hashing import content_hash

__all__ = [
    "LlamaCppError",
    "EvaluationError",
    "EvaluatorUnavailable",
    "LlamaCppEvaluator",
    "GenerateFn",
    "DEFAULT_MODEL_ID",
    "DEFAULT_ENDPOINT_KIND",
    "DEFAULT_DECODER",
    "DEFAULT_N_CTX",
    "DEFAULT_MAX_TOKENS",
]

#: Запасное имя модели, если ``model_path`` не задан (реальное имя — данные конфигурации).
DEFAULT_MODEL_ID = "local-gguf"
#: Профиль зоны (07): self-host контур.
DEFAULT_ENDPOINT_KIND = "self_host"
#: Механизм constrained decoding по умолчанию: GBNF-грамматика llama.cpp.
DEFAULT_DECODER = "gbnf"
#: Размер контекста llama.cpp по умолчанию, токенов.
DEFAULT_N_CTX = 4096
#: Лимит генерации по умолчанию, токенов.
DEFAULT_MAX_TOKENS = 1024

#: Внедряемая функция генерации: запрос оценщика → сырой JSON-строкой.
GenerateFn = Callable[[LLMRequest], str]


class LlamaCppError(RuntimeError):
    """Сбой CPU/GGUF-бэкенда оценщика (загрузка модели, вызов, форма ответа)."""


class EvaluatorUnavailable(LlamaCppError):
    """Оценщик недоступен: нет ``llama-cpp-python``/GGUF-модели и не внедрён ``generate``."""


def _llama_cpp_available() -> bool:
    """Установлен ли ``llama-cpp-python`` (без импорта тяжёлого пакета)."""
    return importlib.util.find_spec("llama_cpp") is not None


class LlamaCppEvaluator:
    """CPU/GGUF оценщик за ``EvaluatorPort``. Без инструментов/поиска (I18).

    :param generate: шов для тестов/CPU-ветки; ``None`` → реальный путь llama.cpp.
    :param model_path: путь к GGUF-файлу (реальный путь); ``None`` — не сконфигурирован.
    :param model_id: идентификатор модели; если ``None`` и задан ``model_path`` — берётся
        имя файла (config-derived, не хардкод), иначе запасной ``DEFAULT_MODEL_ID``.
    :param model_version: версия весов (манифест §13); по умолчанию пусто.
    :param endpoint_kind: профиль зоны (``self_host``/``api``), данные манифеста.
    :param decoder: записываемый механизм constrained decoding; ``"none"`` → без грамматики.
    :param seed: seed по умолчанию (детерминизм); переопределяется ``req.seed``.
    :param temperature: температура по умолчанию; ``0.0`` — greedy.
    :param n_ctx: размер контекста llama.cpp, токенов.
    :param max_tokens: лимит генерации, токенов.
    :param n_threads: число CPU-потоков (``None`` — дефолт llama.cpp).
    """

    def __init__(
        self,
        generate: GenerateFn | None = None,
        *,
        model_path: str | None = None,
        model_id: str | None = None,
        model_version: str = "",
        endpoint_kind: str = DEFAULT_ENDPOINT_KIND,
        decoder: str = DEFAULT_DECODER,
        seed: int = 0,
        temperature: float = 0.0,
        n_ctx: int = DEFAULT_N_CTX,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        n_threads: int | None = None,
    ) -> None:
        # model_id выводится из конфигурации (имя GGUF-файла), а не хардкодится в коде.
        if model_id is None:
            model_id = Path(model_path).name if model_path else DEFAULT_MODEL_ID
        if not model_id:
            raise ValueError("model_id обязан быть непустым")
        if not endpoint_kind:
            raise ValueError("endpoint_kind обязан быть непустым")
        if model_path is not None and not model_path:
            raise ValueError("model_path обязан быть непустым, если задан")
        if seed < 0:
            raise ValueError(f"seed не может быть отрицательным, получено {seed!r}")
        if temperature < 0.0:
            raise ValueError(f"temperature не может быть отрицательной, получено {temperature!r}")
        if n_ctx <= 0:
            raise ValueError(f"n_ctx обязан быть положительным, получено {n_ctx!r}")
        if max_tokens <= 0:
            raise ValueError(f"max_tokens обязан быть положительным, получено {max_tokens!r}")
        if n_threads is not None and n_threads <= 0:
            raise ValueError(f"n_threads обязан быть положительным, получено {n_threads!r}")
        # I18: здесь намеренно НЕТ и не будет параметров внешнего поиска.
        self.model_path = model_path
        self.model_id = model_id
        self.model_version = model_version
        self.endpoint_kind = endpoint_kind
        self.decoder = decoder
        self.seed = seed
        self.temperature = temperature
        self.n_ctx = n_ctx
        self.max_tokens = max_tokens
        self.n_threads = n_threads
        self._generate = generate
        self._model: Any = None

    # ------------------------------------------------------------------
    # Контракт порта
    # ------------------------------------------------------------------

    @property
    def available(self) -> bool:
        """Готов ли адаптер: внедрён ``generate`` либо есть пакет **и** файл GGUF."""
        if self._generate is not None:
            return True
        return _llama_cpp_available() and self._model_path_ok()

    def complete(self, req: LLMRequest) -> LLMResponse:
        """Выполняет один вызов оценщика и разбирает выход в ``LLMCellOutput`` (§7.1).

        :raises EvaluationError: невалидный JSON/форма/инварианты ячейки.
        :raises LlamaCppError: сбой бэкенда llama.cpp.
        :raises EvaluatorUnavailable: нет ``generate``, пакета и/или GGUF-модели.
        """
        seed = req.seed if req.seed is not None else self.seed
        temperature = req.temperature if req.temperature is not None else self.temperature
        usage = TokenUsage()
        if self._generate is not None:
            raw = self._generate(req)
        else:
            raw, usage = self._llm_complete(req, seed=seed, temperature=temperature)
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
        """Разбирает сырой JSON в модель ``schema`` и проверяет форму/инварианты (§7.1)."""
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
    # Реальный CPU/GGUF-путь (llama-cpp-python)
    # ------------------------------------------------------------------

    def _llm_complete(
        self, req: LLMRequest, *, seed: int, temperature: float
    ) -> tuple[str, TokenUsage]:
        """Реальный вызов llama.cpp. Схема → GBNF-грамматика; без внешнего поиска (I18)."""
        llm = self._load_model()
        kwargs: dict[str, Any] = {
            "max_tokens": self.max_tokens,
            "temperature": temperature,  # 0.0 ⇒ greedy (детерминизм)
            "seed": seed,
        }
        if self.decoder != "none":
            # Форма, не семантика: GBNF-грамматика строится из JSON Schema ячейки (§7.1).
            kwargs["grammar"] = self._build_grammar(req.response_schema)
        # I18: аргументы внешнего поиска сюда НЕ добавляются — их здесь нет по построению.
        try:
            result = llm(req.prompt, **kwargs)
        except Exception as exc:  # сбой бэкенда llama.cpp
            raise LlamaCppError(f"вызов llama.cpp не удался: {exc}") from exc
        return self._extract(result)

    def _extract(self, data: Any) -> tuple[str, TokenUsage]:
        """Достаёт ``choices[0].text`` и ``usage`` из ответа llama.cpp."""
        try:
            content = data["choices"][0]["text"]
        except (KeyError, IndexError, TypeError) as exc:
            raise LlamaCppError(f"неожиданная форма ответа llama.cpp: {data!r}") from exc
        if not isinstance(content, str):
            raise LlamaCppError(
                f"choices[0].text обязан быть строкой, получено {type(content).__name__}"
            )
        usage_raw = data.get("usage") or {}
        usage = TokenUsage(
            prompt_tokens=int(usage_raw.get("prompt_tokens", 0) or 0),
            completion_tokens=int(usage_raw.get("completion_tokens", 0) or 0),
        )
        return content, usage

    def _build_grammar(self, schema: type) -> Any:
        """Строит GBNF-грамматику из JSON Schema pydantic-модели (constrained decoding)."""
        if not (isinstance(schema, type) and issubclass(schema, BaseModel)):
            raise EvaluationError(
                f"response_schema обязан быть pydantic-моделью, получено {schema!r}"
            )
        try:
            from llama_cpp import LlamaGrammar  # type: ignore[import-not-found]

            return LlamaGrammar.from_json_schema(
                json.dumps(schema.model_json_schema(), ensure_ascii=False)
            )
        except Exception as exc:
            raise LlamaCppError(f"не удалось построить JSON-грамматику llama.cpp: {exc}") from exc

    def _model_path_ok(self) -> bool:
        """Существует ли сконфигурированный файл GGUF."""
        return bool(self.model_path) and Path(self.model_path).is_file()

    def _load_model(self) -> Any:
        """Лениво импортирует ``llama_cpp`` и загружает GGUF; при отсутствии — typed error."""
        if self._model is not None:
            return self._model
        if not _llama_cpp_available():
            raise EvaluatorUnavailable(
                "llama-cpp-python не установлен: CPU/GGUF-путь оценщика недоступен. "
                "Установите пакет (05 §2.3) или передайте generate."
            )
        if not self._model_path_ok():
            raise EvaluatorUnavailable(
                f"GGUF-модель не найдена по пути {self.model_path!r}: "
                "укажите существующий model_path или передайте generate."
            )
        from llama_cpp import Llama  # type: ignore[import-not-found]

        kwargs: dict[str, Any] = {
            "model_path": self.model_path,
            "n_ctx": self.n_ctx,
            "seed": self.seed,
            "verbose": False,
        }
        if self.n_threads is not None:
            kwargs["n_threads"] = self.n_threads
        self._model = Llama(**kwargs)
        return self._model
