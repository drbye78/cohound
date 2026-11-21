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
``complete`` отправляются только ``model``/``messages``/``temperature``/``seed``/
``response_format``.

**Локальные инструменты — отдельный метод, а не расширение ``complete``.** Метод
:meth:`LocalEvaluator.turn` обслуживает порт ``LocalToolCallerPort``: он отправляет
провайдеру **ровно** объявленные схемы инструментов (``tools[].function``) и
``tool_choice="auto"``. Инструменты **локальные**: их исполняет ядро
(``core/pipeline/local_tools.py``), адаптер только передаёт запрос модели и возвращает
её просьбы; ни ``web_search``, ни какого-либо другого входа в сеть, кроме endpoint'а
LLM, у адаптера нет и здесь не появляется. Отдельный метод, а не поле в ``LLMRequest``,
держит I18 структурно: наличие ``turn`` не делает ``complete`` способным вызывать
инструменты. ``response_format`` в режиме инструментов **не отправляется**: грамматика
constrained decoding и tool-calling конфликтуют в одном запросе у OI-совместимых
серверов, поэтому провенанс такого хода честно называет ``decoder="none"``.

**Constrained decoding — форма, не семантика.** Реальный путь передаёт JSON Schema
ячейки (``response_format={"type": "json_schema", ...}``), из которой OI-совместимый
сервер (например, на бэкенде vLLM/SGLang облака) строит грамматику xgrammar и навязывает
её на уровне сэмплера. Это гарантирует только
*форму*; механическая валидация проверяет **форму ответа, ``source_id`` и
цитату-подстроку** (``pipeline/validate.py``, §7.1 п. 4). Достаточность доказательства —
не шаг ``validate.py``: её делает видимой отдельная **аномалия** по замеру (план 15, V1) —
«доказательство не о предмете» публикуется в операторском ``nda_questions.csv`` строкой
``anomaly_kind=unrelated_support`` и статус ячейки не меняет; маркерная проверка класса
утверждения (отрицание/условность в цитате против заявленного ✅) не построена — она
требует замера шума, а не порога из головы. Поле ``decoder`` в ``LLMResponse``
фиксирует механизм (``"xgrammar"``/``"json_schema"``/``"none"``) для манифеста (R7).

**Детерминизм.** ``seed`` и ``temperature`` из запроса (или дефолты адаптера) уходят
в запрос; ``temperature == 0.0`` — жадный режим. Это **не гарантия** одинакового ответа:
провайдер вправе игнорировать ``seed``. Замер 26.10.2026: при идентичном ``prompt_hash``
статус сменили 4.2 % ячеек (5 из 119) —
``docs/reports/measurements/2026-10-26-verdict-reproducibility.md``. Это остаточный
недетерминизм; повтор его ограничивает, но не устраняет (см. ``evaluation_policy``).
``prompt_hash`` детерминированно
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
``EvaluationError``; pipeline (T9.3) отображает его в ``evaluation_failed`` с retry ≤ ``max_retries``
(§7.1). Отсутствие HTTP-зависимости без внедрённого ``generate`` → ``EvaluatorUnavailable``.
Ошибка несёт провенанс ответа (``finish_reason``/``raw``/``usage``): по артефактам прогона
видно, **что** вернула модель на отказе, а не только что JSON не разобрался.

**``finish_reason`` и шов.** HTTP-путь заполняет метку остановки из ответа провайдера
(``LLMResponse.finish_reason``). Шов ``generate`` возвращает только текст, поэтому метка
остаётся ``None`` — «провайдер не назвал», а не выдуманный ``stop``.

Нейтрален (§1.3): идентификаторы моделей/движков — данные, переопределяемые параметрами.
"""

from __future__ import annotations

import importlib.util
import json
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from pydantic import BaseModel, ValidationError

from core.domain import LLMCellOutput
from core.domain.enums import Decoder, ReasoningEffort
from core.domain.tools import ToolInvocation, ToolSpec
from core.ports.llm import (
    LLMRequest,
    LLMResponse,
    TokenUsage,
    ToolTurn,
    ToolTurnRequest,
)
from core.util.hashing import content_hash
from core.util.retry import retry_after_header

__all__ = [
    "CHAT_COMPLETIONS_PATH",
    "DEFAULT_BASE_URL",
    "DEFAULT_DECODER",
    "DEFAULT_ENDPOINT_KIND",
    "DEFAULT_MODEL_ID",
    "DEFAULT_TIMEOUT",
    "EVALUATOR_DEFAULT_SEED",
    "EVALUATOR_DEFAULT_TEMPERATURE",
    "EvaluationError",
    "EvaluatorUnavailable",
    "GenerateFn",
    "LocalEvaluator",
    "ReasoningWireUnsupported",
    "ToolCallRefused",
    "decoder_wire_type",
    "endpoint_host",
    "normalize_api_base",
    "reasoning_wire",
]

#: Модель оценщика по умолчанию — **только fallback прямой конструкции** (§7.4).
#: Не авторитет: composition root всегда передаёт объявленную модель
#: (``--llm-model`` > ``deploy.llm.<slot>.model_id``), а при её отсутствии
#: ``build_evaluator_llm`` отказывает (``llm``/``EXIT_USAGE``), не подставляя это значение.
#: Значение держится равным ``config/deploy.yaml:llm.<slot>.model_id`` (пин в
#: ``tests/config/test_deploy.py``): источник истины — конфиг, а не константа адаптера.
DEFAULT_MODEL_ID = "deepseek-v4.1-flash-1017:yellow"
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
#: Openai-совместимый путь генерации (дописывается к канонической базе).
CHAT_COMPLETIONS_PATH = "/v1/chat/completions"
#: Потолок тела HTTP-отказа в тексте исключения/атрибуте ``provider_body``, символов.
#: Тело объясняет причину (401 — «неверный ключ», 402 — «средства исчерпаны»), но
#: целиком в сообщение его класть нельзя: провайдер вправе ответить HTML-страницей на
#: сотни килобайт, и такое сообщение уедет в ``validation_notes`` каждой ячейки.
PROVIDER_BODY_LIMIT = 500


def provider_body_text(response: Any) -> str | None:
    """Тело HTTP-отказа как одна строка: без переводов строк, усечённое, либо ``None``.

    Читается **только тело**: ключ и заголовки запроса в отказ не попадают ни при каких
    условиях (это уже так, и это не должно измениться). Пустое тело — ``None``, чтобы
    вызывающий не склеивал сообщение с ``": "`` и пустотой.
    """
    try:
        text = response.text
    except Exception:  # noqa: BLE001 — декодирование тела не должно подменять отказ
        return None
    if not isinstance(text, str):
        return None
    flat = " ".join(text.split())
    if not flat:
        return None
    return flat[:PROVIDER_BODY_LIMIT]


def normalize_api_base(base_url: str) -> str:
    """Каноническая база OI-совместимого API: без хвоста ``/v1`` и без ``/`` на конце.

    Хвост ``/v1`` — самый частый способ записать тот же endpoint: операторы копируют его
    из ``COHOUND_LLM_BASE_URL`` привычного вида ``https://host/v1``. Без нормализации
    такой адрес склеивался в ``/v1/v1/chat/completions`` (провайдер терпит, но это
    терпимость, а не контракт), и — важнее — сравнение объявленного endpoint'а с
    env-значением в :func:`core.cli.wiring.llm_env_notices` давало ложное «прогон идёт не
    на объявленном endpoint»: предупреждение, которое горит всегда, перестают читать.

    Функция нужна **одна**: по ней сравниваются объявленный и env-endpoint, и по ней же
    собирается URL запроса. Иначе оценка «тот же endpoint» и фактический адрес разъезжаются.
    """
    base = base_url.rstrip("/")
    if base.endswith("/v1"):
        base = base[: -len("/v1")].rstrip("/")
    return base

#: Внедряемая функция генерации: (промпт, схема ответа) → сырой JSON-строкой.
GenerateFn = Callable[[str, type], str]


class EvaluationError(RuntimeError):
    """Оценщик вернул невалидный JSON/форму или нарушил инварианты ячейки (§7.1).

    Pipeline отображает ошибку в ``evaluation_failed`` с retry ≤ ``max_retries`` (дефолт 2; не в статус — I4).

    Отказ несёт **провенанс ответа** — что именно вернул провайдер: ``finish_reason``
    (метка остановки генерации), ``raw`` (сырой текст ответа, если он был) и ``usage``.
    Без них путь отказа неразбираем: обрезание по потолку и невалидный JSON выглядели
    одинаково, а самого ответа модели не оставалось нигде (журнал вызовов пишет их —
    ``logs/llm_calls.jsonl``). ``reason`` — машинный класс отказа
    (``length``/``invalid_json``/``schema``/``http_error``), чтобы читатель не разбирал
    текст сообщения.

    ``status_code``/``retry_after`` — HTTP-ответ, из которого следует **пауза** повтора:
    провайдер говорит «подожди N секунд» заголовком ``Retry-After`` (секунды или
    HTTP-date, как есть — разбирает вызывающий), а статус (``429``/``5xx``) называет
    причину. Без них повтор шёл бы немедленно, и при нескольких вызовах в полёте лимит
    провайдера превратился бы в шторм повторов.

    ``provider_body`` — **тело** HTTP-отказа (усечённое до :data:`PROVIDER_BODY_LIMIT`
    и без переводов строк). Провайдер объясняет отказ именно там: замер 14.11.2026 на
    ``HTTP 401`` тело было ``{"error":{"message":"Отсутствует или неверный API-ключ.",…}}``,
    а до правки оно выбрасывалось и 401 был неотличим от 403/402 по артефактам. Ключ и
    заголовки сюда не попадают: читается только тело ответа.
    """

    def __init__(
        self,
        message: str,
        *,
        reason: str | None = None,
        finish_reason: str | None = None,
        raw: str | None = None,
        usage: TokenUsage | None = None,
        status_code: int | None = None,
        retry_after: str | None = None,
        provider_body: str | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.finish_reason = finish_reason
        self.raw = raw
        self.usage = usage
        #: HTTP-статус отказа (``None`` — отказа не было/транспортный сбой).
        self.status_code = status_code
        #: Заголовок ``Retry-After`` **как есть** (``None`` — провайдер паузу не назвал).
        self.retry_after = retry_after
        #: Тело HTTP-отказа (``None`` — отказ не HTTP либо тело пусто).
        self.provider_body = provider_body
        #: Текст рассуждения, если провайдер успел его отдать. Заполняется путями отказа
        #: (:meth:`LocalEvaluator._extract`) и :meth:`LocalEvaluator.complete`: на отказе
        #: это единственный диагностический след, и журнал пишет его целиком.
        self.reasoning: str | None = None


class EvaluatorUnavailable(RuntimeError):
    """Оценщик недоступен: нет HTTP-зависимости и не внедрён ``generate``."""


class ToolCallRefused(EvaluationError):
    """Модель попросила вызвать инструмент, которого **нет** в объявленном наборе.

    Отказ, а не «пропустим и продолжим». Набор инструментов — объявленный вход
    измерения (:class:`core.domain.tools.ToolProfile`): вызов имени вне набора означал бы,
    что модель исполняет то, чего прогон не объявлял, и вердикт перестал бы быть
    функцией объявленных входов. Молчаливый пропуск выглядел бы как «модель ничего не
    просила», то есть как другой ход при том же входе.

    Наследует :class:`EvaluationError`: это отказ **формы ответа**, и путь обработки у
    него тот же (``evaluation_failed`` с retry ≤ ``max_retries``, §7.1), но машинный
    ``reason="tool_not_allowed"`` отличает его от невалидного JSON в журнале вызовов.
    """


def _reasoning_tokens_of(usage_raw: dict[str, Any]) -> int:
    """Токены рассуждения из ``usage`` (0 — провайдер не назвал).

    Две подтверждённые формы: ``completion_tokens_details.reasoning_tokens``
    (OI-совместимая, отдаётся замеренной моделью) и плоское
    ``completion_thinking_tokens`` (часть совместимых серверов; образец —
    :mod:`core.adapters.llm_discovery`). Порядок — от точной формы к запасной; нечисловое
    значение не выдумывается как ноль, а читается как «не названо».
    """
    details = usage_raw.get("completion_tokens_details")
    candidates: list[Any] = []
    if isinstance(details, dict):
        candidates.append(details.get("reasoning_tokens"))
    candidates.append(usage_raw.get("completion_thinking_tokens"))
    for value in candidates:
        if isinstance(value, bool) or value is None:
            continue
        try:
            number = int(value)
        except (TypeError, ValueError):
            continue
        return max(0, number)
    return 0


def _httpx_available() -> bool:
    """Установлен ли ``httpx`` (без импорта пакета)."""
    return importlib.util.find_spec("httpx") is not None


def _tool_wire(spec: ToolSpec) -> dict[str, Any]:
    """Схема объявленного инструмента в форме OI-совместимого ``tools[]``.

    Переводится **объявленный** :class:`ToolSpec`, а не реализация инструмента: иначе
    набор доступных модели вызовов описывался бы кодом, который эти вызовы исполняет, и
    менялся бы без смены идентичности прогона.
    """
    return {
        "type": "function",
        "function": {
            "name": spec.name,
            "description": spec.description,
            "parameters": dict(spec.parameters),
        },
    }


def _tool_arguments(
    raw: Any,
    *,
    name: str,
    finish_reason: str | None,
    usage: TokenUsage,
) -> dict[str, Any]:
    """Аргументы вызова из ``function.arguments`` (OI отдаёт JSON-**строкой**).

    Три допустимые формы: отсутствие аргументов (``{}`` — инструмент без параметров),
    уже разобранное отображение (часть совместимых серверов) и JSON-строка. Невалидный
    JSON и не-объект — отказ: ``ToolInvocation`` с угаданными аргументами означал бы
    вызов, которого модель не просила.
    """
    if raw is None or raw == "":
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if not isinstance(raw, str):
        raise EvaluationError(
            f"аргументы вызова «{name}» обязаны быть JSON-объектом или строкой, "
            f"получено {type(raw).__name__}",
            reason="invalid_json",
            finish_reason=finish_reason,
            usage=usage,
        )
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise EvaluationError(
            f"аргументы вызова «{name}» — невалидный JSON: {exc}",
            reason="invalid_json",
            raw=raw,
            finish_reason=finish_reason,
            usage=usage,
        ) from exc
    if not isinstance(parsed, dict):
        raise EvaluationError(
            f"аргументы вызова «{name}» обязаны быть JSON-объектом, "
            f"получено {type(parsed).__name__}",
            reason="invalid_json",
            raw=raw,
            finish_reason=finish_reason,
            usage=usage,
        )
    return parsed


#: Seed оценщика по умолчанию — то же значение, что передаёт composition root
#: (``build_evaluator_llm``), и то же, что записывается в ``llm_scenarios.seed``.
#: Одна константа на обе роли: иначе провенанс и провод разъедутся молча.
EVALUATOR_DEFAULT_SEED = 0
#: Температура оценщика по умолчанию (``0.0`` — greedy). См. выше: одна константа.
EVALUATOR_DEFAULT_TEMPERATURE = 0.0

def endpoint_host(base_url: str) -> str | None:
    """Хост endpoint'а без схемы/пути/учётных данных (V6): факт «куда ушёл корпус».

    Возвращает ``None`` для пустой строки и для строки без хоста. Порт сохраняется:
    один хост на разных портах — разные шлюзы. Секрет сюда попасть не может: берётся
    только ``hostname`` (+порт), логин/пароль/путь отбрасываются.
    """
    try:
        parsed = urlsplit(base_url or "")
    except ValueError:
        return None
    host = parsed.hostname
    if not host:
        return None
    return f"{host}:{parsed.port}" if parsed.port else host

class ReasoningWireUnsupported(RuntimeError):
    """Объявленный уровень рассуждения **невыразим** на проводе этой модели.

    Отказ, а не тихий пропуск параметра: пропуск выглядел бы как «уровень не выбран», и
    прогон жил бы под ``run_id``, называющим уровень, который никуда не ушёл. Класс
    ошибки отдельный, чтобы composition root мог отобразить его в ``EXIT_USAGE`` с именем
    модели и уровня (это ошибка входа, а не сбой исполнения).
    """

    def __init__(self, message: str, *, model_id: str, effort: ReasoningEffort | None = None
    ) -> None:
        super().__init__(message)
        self.model_id = model_id
        self.effort = effort


@dataclass(frozen=True)
class _ReasoningFamily:
    """Одно семейство моделей и способ выразить уровень рассуждения на его проводе.

    ``wire is None`` — механизм семейства **известен, но не подтверждён замером** на этом
    стенде; такой уровень — отказ, а не догадка о теле запроса (A3: «принято» ≠
    «действует», и выдуманный ключ — тем более).

    Таблица нужна одна: второй список семейств разошёлся бы с первым молча.
    """

    name: str
    #: Подстроки ``model_id`` (в нижнем регистре), по которым семейство опознаётся.
    markers: tuple[str, ...]
    #: Провод уровня → фрагмент тела запроса. ``None`` — не подтверждён замером.
    wire: Callable[[ReasoningEffort], dict[str, Any]] | None
    #: Что известно о механизме семейства (для текста отказа/докстринга).
    mechanism: str


#: Таблица «семейство модели → провод усилия рассуждения». Единственная.
#:
#: Замер 06.11.2026 (4 вызова, ``https://plusvibeapi.ru/v1``, модель
#: ``deepseek-v4.1-flash-1017:yellow``): ``reasoning_effort`` принимается в теле запроса
#: без ошибки; ``usage.completion_tokens_details.reasoning_tokens`` отдаётся (17–25 на
#: тривиальном вопросе), ``choices[0].message.reasoning_content`` присутствует. На
#: тривиальном промпте уровни неразличимы (low 20 / high 17 reasoning-токенов) — то есть
#: действенность уровня **измеряется**, а не предполагается; это публикует журнал вызовов.
#:
#: ``off`` на этом проводе выражается **отсутствием параметра**: «не просим» — иная
#: форма, чем «просим уровень off», и выдумывать её ключ запрещено. Разницу между
#: объявленным ``off`` и фактическим рассуждением видно в журнале (``usage.reasoning``).
#:
#: ``qwen``/``glm`` перечислены, потому что их механизмы известны, но **замером здесь не
#: подтверждены**: ``qwen`` — ``enable_thinking`` (+ бюджет через ``chat_template_kwargs``),
#: ``glm`` — ``thinking: {type: enabled|disabled}``. Пока не снят замер, запрос такого
#: уровня — отказ (``ReasoningWireUnsupported``), а не догадка. Точка расширения: снять
#: замер, заполнить ``wire`` — и всё, что выше, начнёт действовать без правок вызывающих.
_REASONING_FAMILIES: tuple[_ReasoningFamily, ...] = (
    _ReasoningFamily(
        name="deepseek",
        markers=("deepseek",),
        wire=lambda level: (
            {} if level is ReasoningEffort.OFF else {"reasoning_effort": level.value}
        ),
        mechanism="reasoning_effort в теле запроса (подтверждено замером 06.11.2026)",
    ),
    _ReasoningFamily(
        name="qwen",
        markers=("qwen",),
        wire=None,
        mechanism=(
            "enable_thinking (+ бюджет через chat_template_kwargs); замером на этом "
            "стенде не подтверждено"
        ),
    ),
    _ReasoningFamily(
        name="glm",
        markers=("glm", "chatglm"),
        wire=None,
        mechanism=(
            "thinking: {type: enabled|disabled}; замером на этом стенде не подтверждено"
        ),
    ),
)


def reasoning_family(model_id: str) -> _ReasoningFamily | None:
    """Семейство модели по ``model_id`` (``None`` — не опознано).

    Опознание — по подстроке маркера в нижнем регистре: варианты и версии
    (``deepseek-v4.1-flash-1017:yellow``, ``...:purple``) принадлежат одному семейству,
    поэтому ``model_id`` целиком в таблицу не заводится (иначе каждая новая версия
    требовала бы правки).
    """
    text = str(model_id or "").strip().lower()
    for family in _REASONING_FAMILIES:
        if any(marker in text for marker in family.markers):
            return family
    return None


def reasoning_wire(model_id: str, effort: ReasoningEffort | None) -> dict[str, Any]:
    """Фрагмент тела запроса для объявленного уровня рассуждения.

    ``{}`` — уровня нет (``None``) или объявлено ``off``. ``off`` выражается
    **отсутствием параметра** и потому выразимо для **любой** модели: «не просим» не
    требует знания ключа семейства. Отличить объявленное ``off`` от фактического
    рассуждения (провайдер мог думать по своему дефолту) призван журнал
    (``usage.reasoning``) — это провенанс исполнения, а не обещание.

    Иначе — ключи тела запроса по таблице семейств.

    :raises ReasoningWireUnsupported: семейство не опознано, либо уровень для него не
        подтверждён замером. Молча вернуть ``{}`` значило бы выдать «уровень не действует»
        за «уровень объявлен» — ложный провенанс в ``llm_scenarios`` и журнале.
    """
    if effort is None or effort is ReasoningEffort.OFF:
        return {}
    family = reasoning_family(model_id)
    if family is None:
        raise ReasoningWireUnsupported(
            f"усилие рассуждения «{effort.value}» для модели «{model_id}» невыразимо: "
            "семейство модели не опознано, а угадывать ключ тела запроса запрещено "
            "(A3). Объявите llm.reasoning_effort: off либо модель семейства, для "
            "которого провод подтверждён замером",
            model_id=model_id,
            effort=effort,
        )
    if family.wire is None:
        raise ReasoningWireUnsupported(
            f"усилие рассуждения «{effort.value}» для модели «{model_id}» невыразимо: "
            f"механизм семейства {family.name} — {family.mechanism}. Запрос не отправлен: "
            "выдуманный ключ объявил бы уровень, которого модель не получала",
            model_id=model_id,
            effort=effort,
        )
    return family.wire(effort)


def decoder_wire_type(decoder: str) -> str | None:
    """Механизм, который реально уходит по проводам для объявленной метки (V4).

    Одна формула на две роли: адаптер строит по ней ``response_format``, а провенанс
    (``llm_scenarios.constrained_decoding``) записывает **фактический** механизм. До
    правки эти две роли расходились: метка бралась из конфига как есть, а по проводам
    на любое значение ≠ ``none`` уходил ``json_schema``.

    ``None`` — механизм не запрошен (``decoder=none``): схема не навязывается, форма
    ответа держится только pydantic-парсом.
    """
    kind = Decoder(decoder)
    if kind is Decoder.NONE:
        return None
    return kind.value

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
    :param reasoning_effort: объявленное усилие рассуждения (ось
        ``llm.reasoning_effort``); переопределяется ``req.reasoning_effort``. Уровень
        переводится в тело запроса таблицей семейств (:func:`reasoning_wire`);
        невыразимый уровень — :class:`ReasoningWireUnsupported`, а не тихий пропуск.
    :param max_connections: потолок соединений HTTP-клиента (число вызовов в полёте
        объявлено осью исполнения ``--llm-concurrency``). Клиент **один** на адаптер:
        ``httpx.post`` на каждый вызов делал бы лишнее TLS-рукопожатие. ``None`` —
        дефолт ``httpx`` (``100``), он заведомо выше любого объявленного параллелизма;
        значение ниже объявленного числа потоков заставило бы воркеры ждать соединения,
        то есть «параллельность» осталась бы в отчёте, а не в исполнении.
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
        max_tokens: int | None = None,
        reasoning_effort: ReasoningEffort | str | None = None,
        max_connections: int | None = None,
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
        if max_tokens is not None and max_tokens <= 0:
            raise ValueError(f"max_tokens обязан быть положительным, получено {max_tokens!r}")
        if max_connections is not None and max_connections <= 0:
            raise ValueError(
                f"max_connections обязан быть положительным, получено {max_connections!r}"
            )
        try:
            resolved_decoder = Decoder(decoder)
        except ValueError as exc:  # V4: метка вне перечисления — отказ, а не «включим схему»
            allowed = ", ".join(item.value for item in Decoder)
            raise ValueError(
                f"decoder={decoder!r} вне перечисления ({allowed}); "
                "метка обязана совпадать с исполняемым механизмом"
            ) from exc
        # I18: здесь намеренно НЕТ и не будет параметров tools/web_search.
        self.model_id = model_id
        self.model_version = model_version
        self.endpoint_kind = endpoint_kind
        self.decoder = resolved_decoder
        self.seed = seed
        self.temperature = temperature
        # Хранится **как объявлено** (провенанс: эту строку задал оператор), а
        # нормализуется при склейке URL — там, где она становится адресом запроса.
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        #: Потолок генерации (V5): ``None`` — серверный дефолт провайдера, и это надо
        #: знать: без потолка обрезание приходит как ``finish_reason=length``.
        self.max_tokens = max_tokens
        #: Усилие рассуждения — объявленный вход (входит в ``llm_scenarios``). Хранится
        #: как объявлено (enum), чтобы провенанс (``model_config_digest``) и провод
        #: читали одно значение, а не две её копии.
        self.reasoning_effort = (
            None if reasoning_effort is None else ReasoningEffort(reasoning_effort)
        )
        self._generate = generate
        # Счётчики стоимости (§15/M1): заполняются в :meth:`complete`. Нужны потому, что
        # ``metrics.json`` обязан называть цену прогона, а факт вызова и расход токенов
        # известны **только** здесь: выше по стеку ячейка несёт ``model_id``, но не
        # ``usage``. Ноль в этих счётчиках означает «вызовов не было», а не «не считали».
        #
        # Счётчики инкрементируются из пула потоков (число вызовов в полёте — объявленная
        # ось исполнения), поэтому ``+=`` на ``int`` здесь недостаточно: два потока прочли
        # бы одно значение и один вызов потерялся бы. Замок охватывает и запись, и чтение
        # (:meth:`usage_summary`): цена прогона — наблюдённый факт, а не приблизительный.
        self._calls = 0
        self._prompt_tokens = 0
        self._completion_tokens = 0
        self._reasoning_tokens = 0
        self._usage_lock = threading.Lock()
        #: Потолок соединений одного клиента (``None`` — дефолт ``httpx``).
        self.max_connections = max_connections
        # Клиент создаётся **лениво** (первый реальный вызов): модуль импортируется без
        # ``httpx``, и конструктор обязан работать без него. Замок — потому что ленивое
        # создание идёт из пула: два потока создали бы два клиента, и пул соединений
        # перестал бы быть одним.
        self._client: Any = None
        self._client_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Наблюдаемость стоимости
    # ------------------------------------------------------------------

    def usage_summary(self) -> dict[str, Any]:
        """Расход оценщика за время жизни адаптера: ``{"calls", "tokens"}``.

        ``tokens`` — ``{"prompt", "completion"}`` либо пустая карта, если провайдер не
        вернул ``usage`` (шов ``generate``, часть серверов): «неизвестно» не выдаётся за
        измеренный ноль, поэтому пустая карта, а не нули. ``calls`` считается всегда —
        сам факт вызова известен независимо от того, что ответил сервер.

        Числа не входят ни в ``execution_contract_digest``, ни в ``pb``: это измерение
        исполнения (стоимость), а не условие измерения (A3/§13).
        """
        with self._usage_lock:
            calls = self._calls
            prompt_tokens = self._prompt_tokens
            completion_tokens = self._completion_tokens
            reasoning_tokens = self._reasoning_tokens
        tokens: dict[str, int] = {}
        if prompt_tokens or completion_tokens:
            tokens = {
                "prompt": prompt_tokens,
                "completion": completion_tokens,
            }
            # Рассуждение — отдельная статья расхода (часть ``completion``); не
            # смешивается с ним, иначе «сколько стоило думать» неотличимо от ответа.
            if reasoning_tokens:
                tokens["reasoning"] = reasoning_tokens
        return {"calls": calls, "tokens": tokens}

    def close(self) -> None:
        """Закрывает HTTP-клиент адаптера (``None`` — клиент не создавался).

        Один клиент на адаптер означает владельца: пул соединений держит сокеты, и после
        прогона их надо вернуть. Метод идемпотентен (повторный вызов — no-op).
        """
        with self._client_lock:
            client, self._client = self._client, None
        if client is not None:
            client.close()

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
        finish_reason: str | None = None
        reasoning: str | None = None
        # Счётчик вызовов растёт **до** запроса: провалившийся вызов тоже стоил времени
        # (а часто и токенов — HTTP-ошибка после отправки). Токены прибавляются после
        # ответа: их знает только сервер. Инкремент под замком — вызовы идут из пула.
        with self._usage_lock:
            self._calls += 1
        if self._generate is not None:
            raw = self._generate(req.prompt, req.response_schema)
        else:
            raw, usage, finish_reason, reasoning = self._http_complete(
                req, seed=seed, temperature=temperature
            )
        with self._usage_lock:
            self._prompt_tokens += usage.prompt_tokens
            self._completion_tokens += usage.completion_tokens
            self._reasoning_tokens += usage.reasoning_tokens
        raw = raw if isinstance(raw, str) else str(raw)
        try:
            output = self._parse(req.response_schema, raw)
        except EvaluationError as exc:
            # Путь отказа обязан назвать ответ: ``_parse`` знает текст, но не знает
            # метку остановки/расход; дополняем недостающее, не затирая названное.
            if exc.raw is None:
                exc.raw = raw
            if exc.usage is None:
                exc.usage = usage
            if exc.finish_reason is None:
                exc.finish_reason = finish_reason
            # Текст рассуждения на отказе — единственный диагностический след: именно он
            # объясняет, почему модель не дошла до JSON (журнал пишет его целиком).
            if exc.reasoning is None:
                exc.reasoning = reasoning
            raise
        return LLMResponse(
            output=output,
            raw=raw,
            prompt_hash=content_hash(req.prompt),
            decoder=self.decoder,
            finish_reason=finish_reason,
            usage=usage,
            # Рассуждение — провенанс диагностики: журнал пишет длину на успехе и полный
            # текст на отказе (иначе файл растёт на десятки МБ).
            reasoning=reasoning,
        )

    def turn(self, req: ToolTurnRequest) -> ToolTurn:
        """Один ход диалога с **локальными** инструментами (``LocalToolCallerPort``, §7).

        Провайдеру уходят **ровно** объявленные схемы (``req.allowed_tools``) и
        ``tool_choice="auto"``: набор инструментов — объявленный вход измерения, а не то,
        что модель выберет по ходу. Просьба вызвать имя вне набора —
        :class:`ToolCallRefused`, а не «пропустим»: иначе ход описывал бы исполнение,
        которого прогон не объявлял.

        **Инструменты локальные.** Их исполняет ядро (``core/pipeline/local_tools.py``) по
        корпусу прогона; адаптер только передаёт просьбы модели. Ни ``web_search``, ни
        другого входа в сеть, кроме endpoint'а LLM, здесь нет — I18 остаётся структурной.

        **``response_format`` не отправляется.** Constrained decoding (грамматика из JSON
        Schema) и tool-calling конфликтуют в одном запросе у OI-совместимых серверов,
        поэтому форма ответа держится pydantic-парсом, а провенанс хода честно называет
        ``decoder="none"`` (:attr:`LLMResponse.decoder` у финального ответа). Это
        видимый факт, а не тихое понижение: ``TurnRequest`` не несёт метки механизма, и
        выдумывать её нельзя.

        Ответ разбирается одним путём с :meth:`complete`: ``choices[0].message.content``
        проходит через :meth:`_parse` (та же проверка формы/инвариантов и те же
        ``raw``/``finish_reason``/``usage`` в отказе). Пустой ход (ни вердикта, ни
        вызовов) — отказ формы: ячейка не должна молча стать ``❓`` из-за пустого ответа.

        :raises ToolCallRefused: имя инструмента вне ``req.allowed_tools``.
        :raises EvaluationError: невалидный JSON/форма, пустой ход, обрыв по потолку.
        :raises EvaluatorUnavailable: нет ``httpx`` (шов ``generate`` ход не обслуживает:
            он возвращает текст, а не структуру вызова, и подделывать её здесь нельзя).
        """
        specs = tuple(req.allowed_tools)
        allowed = frozenset(spec.name for spec in specs)
        if len(allowed) != len(specs):
            raise ValueError(
                "ToolTurnRequest.allowed_tools: имена инструментов обязаны быть уникальны"
            )
        seed = req.seed if req.seed is not None else self.seed
        temperature = req.temperature if req.temperature is not None else self.temperature
        model_id = req.model_id or self.model_id
        effort = (
            req.reasoning_effort
            if req.reasoning_effort is not None
            else self.reasoning_effort
        )
        payload: dict[str, Any] = {
            "model": model_id,
            "messages": [{"role": "user", "content": req.prompt}],
            "temperature": temperature,
            "seed": seed,
            "stream": False,
        }
        payload.update(reasoning_wire(model_id, effort))
        max_tokens = req.max_tokens if req.max_tokens is not None else self.max_tokens
        if max_tokens is not None:
            payload["max_tokens"] = int(max_tokens)
        if specs:
            # Ровно объявленный набор, в объявленном порядке: набор — вход измерения.
            payload["tools"] = [_tool_wire(spec) for spec in specs]
            payload["tool_choice"] = "auto"
        # ``response_format`` намеренно отсутствует: грамматика и tool-calling не
        # совмещаются в одном запросе у OI-совместимых серверов. Форму держит ``_parse``.
        with self._usage_lock:
            self._calls += 1
        data = self._post_chat(payload)
        return self._turn_from_response(data, req=req, allowed=allowed)

    def _turn_from_response(
        self, data: Any, *, req: ToolTurnRequest, allowed: frozenset[str]
    ) -> ToolTurn:
        """Разбирает ответ провайдера в :class:`ToolTurn` (вызовы либо вердикт).

        Оба пусты — отказ формы (``reason="empty_turn"``), а не пустой ``ToolTurn``:
        вызывающий обязан увидеть, что модель не ответила ни тем, ни другим, а не
        получить объект, из которого непонятно, что произошло.
        """
        message, finish_reason, usage, reasoning = self._envelope(data)
        with self._usage_lock:
            self._prompt_tokens += usage.prompt_tokens
            self._completion_tokens += usage.completion_tokens
            self._reasoning_tokens += usage.reasoning_tokens
        raw_message = json.dumps(message, ensure_ascii=False, default=str)
        calls_raw = message.get("tool_calls") if isinstance(message, dict) else None
        if calls_raw:
            calls = tuple(
                self._tool_invocation(
                    item,
                    allowed=allowed,
                    finish_reason=finish_reason,
                    usage=usage,
                )
                for item in calls_raw
            )
            return ToolTurn(
                tool_calls=calls,
                raw=raw_message,
                finish_reason=finish_reason,
            )
        content = message.get("content") if isinstance(message, dict) else None
        if (content is None or content == "") and finish_reason == "length":
            limit = self.max_tokens if self.max_tokens is not None else "серверный"
            raise EvaluationError(
                "генерация оценщика оборвана по потолку токенов "
                f"(finish_reason=length, max_tokens={limit}): увеличьте "
                "llm.<slot>.max_tokens — модель израсходовала бюджет на reasoning",
                reason="length",
                finish_reason=finish_reason,
                raw=content if isinstance(content, str) else "",
                usage=usage,
            )
        if not isinstance(content, str) or not content.strip():
            raise EvaluationError(
                "ход не содержит ни вердикта, ни вызова инструмента "
                f"(content={type(content).__name__}, finish_reason={finish_reason!r})",
                reason="empty_turn",
                finish_reason=finish_reason,
                raw=raw_message,
                usage=usage,
            )
        try:
            output = self._parse(req.response_schema, content)
        except EvaluationError as exc:
            # Путь отказа обязан назвать ответ — как у :meth:`complete`.
            if exc.raw is None:
                exc.raw = content
            if exc.usage is None:
                exc.usage = usage
            if exc.finish_reason is None:
                exc.finish_reason = finish_reason
            if exc.reasoning is None:
                exc.reasoning = reasoning
            raise
        return ToolTurn(
            final=LLMResponse(
                output=output,
                raw=content,
                prompt_hash=content_hash(req.prompt),
                # В режиме инструментов грамматика не навязывалась — провенанс это
                # называет, а не приписывает адаптеру объявленный механизм.
                decoder=Decoder.NONE.value,
                finish_reason=finish_reason,
                usage=usage,
                reasoning=reasoning,
            ),
            raw=content,
            finish_reason=finish_reason,
        )

    def _tool_invocation(
        self,
        item: Any,
        *,
        allowed: frozenset[str],
        finish_reason: str | None,
        usage: TokenUsage,
    ) -> ToolInvocation:
        """Элемент ``tool_calls`` → :class:`ToolInvocation` (имя проверяется по набору)."""
        function = item.get("function") if isinstance(item, dict) else None
        if not isinstance(function, dict):
            raise EvaluationError(
                f"tool_calls: элемент без function: {item!r}",
                reason="invalid_json",
                raw=json.dumps(item, ensure_ascii=False, default=str),
                finish_reason=finish_reason,
                usage=usage,
            )
        name = function.get("name")
        if not isinstance(name, str) or not name:
            raise EvaluationError(
                f"tool_calls: пустое имя инструмента: {item!r}",
                reason="invalid_json",
                raw=json.dumps(item, ensure_ascii=False, default=str),
                finish_reason=finish_reason,
                usage=usage,
            )
        if name not in allowed:
            declared = ", ".join(sorted(allowed)) or "набор пуст"
            raise ToolCallRefused(
                f"модель запросила инструмент «{name}» вне объявленного набора "
                f"({declared}): вызов не исполняется, потому что набор инструментов — "
                "объявленный вход измерения",
                reason="tool_not_allowed",
                raw=json.dumps(item, ensure_ascii=False, default=str),
                finish_reason=finish_reason,
                usage=usage,
            )
        return ToolInvocation(
            name=name,
            arguments=_tool_arguments(
                function.get("arguments"),
                name=name,
                finish_reason=finish_reason,
                usage=usage,
            ),
        )

    # ------------------------------------------------------------------
    # Предпроверка ключа (health-check, не измерение)
    # ------------------------------------------------------------------

    def probe(self) -> None:
        """Проверяет живость ключа/endpoint'а **одним** минимальным вызовом.

        Зачем отдельный метод, если есть :meth:`complete`. Прогон начинается с
        многочасового краула, и мёртвый ключ обнаруживался только на Ш6 — после того как
        корпус уже скачан (замер 14.11.2026: провайдер отвечает ``401`` на отсутствующий
        ключ, и это выяснялось на первой ячейке). Предпроверка переводит такую ошибку в
        ошибку **входа**: прогон не должен начинаться, если мерить нечем.

        Путь — тот же, что у боевого вызова: тот же URL (:meth:`_completions_url`), те же
        заголовки (:meth:`_headers`), та же объявленная модель и тот же уровень
        рассуждения. Отличается только объём: ``max_tokens=1`` и механический промпт —
        проба не претендует на ответ и ничего не пишет ни в кэш, ни в БД.

        :raises EvaluationError: отказ провайдера — со ``status_code``/``provider_body``,
            как у боевого вызова; ответ ``200`` без ``choices`` — тоже отказ (пустой
            ответ не доказывает, что ключ принят и модель отвечает).
        """
        payload: dict[str, Any] = {
            "model": self.model_id,
            "messages": [{"role": "user", "content": "ping"}],
            "temperature": 0.0,
            "max_tokens": 1,
            "stream": False,
        }
        # Уровень рассуждения — та же ось, что у боевого вызова: проба, отправившая
        # другой провод, проверяла бы другую конфигурацию.
        payload.update(reasoning_wire(self.model_id, self.reasoning_effort))
        client = self._http_client()
        try:
            response = client.post(
                self._completions_url(),
                json=payload,
                headers=self._headers(),
                timeout=self.timeout,
            )
        except Exception as exc:  # noqa: BLE001 — транспортный сбой — тоже провал пробы
            raise EvaluationError(
                f"проверка ключа оценщика не удалась: {type(exc).__name__}: {exc}",
                reason="http_error",
            ) from exc
        if response.status_code >= 400:
            body = provider_body_text(response)
            detail = f"HTTP {response.status_code}"
            if body:
                detail = f"{detail}: {body}"
            raise EvaluationError(
                f"проверка ключа оценщика не удалась: {detail}",
                reason="http_error",
                status_code=response.status_code,
                retry_after=retry_after_header(response),
                provider_body=body,
            )
        try:
            data = response.json()
        except Exception as exc:  # noqa: BLE001 — не-JSON ответ не доказывает живость
            raise EvaluationError(
                f"проверка ключа оценщика не удалась: тело ответа не JSON: {exc}",
                reason="invalid_json",
                status_code=response.status_code,
            ) from exc
        choices = data.get("choices") if isinstance(data, dict) else None
        if not choices:
            raise EvaluationError(
                "проверка ключа оценщика не удалась: ответ без choices "
                "(ключ принят, но модель не ответила)",
                reason="invalid_json",
                status_code=response.status_code,
                raw=json.dumps(data, ensure_ascii=False) if data is not None else None,
            )

    # ------------------------------------------------------------------
    # Разбор / валидация
    # ------------------------------------------------------------------

    def _parse(self, schema: type, raw: str) -> LLMCellOutput:
        """Разбирает сырой JSON в модель ``schema`` и проверяет форму/inварианты (§7.1)."""
        if not (isinstance(schema, type) and issubclass(schema, BaseModel)):
            raise EvaluationError(
                f"response_schema обязан быть pydantic-моделью, получено {schema!r}",
                reason="schema",
                raw=raw,
            )
        try:
            parsed = schema.model_validate_json(raw)
        except ValidationError as exc:
            raise EvaluationError(
                f"оценщик вернул невалидный JSON/форму ({exc.error_count()} ошибок): {exc}",
                reason="invalid_json",
                raw=raw,
            ) from exc
        except (ValueError, TypeError) as exc:  # защитный слой (не JSON, None и т.п.)
            raise EvaluationError(
                f"не удалось разобрать выход оценщика: {exc}",
                reason="invalid_json",
                raw=raw,
            ) from exc
        if not isinstance(parsed, LLMCellOutput):
            raise EvaluationError(
                f"схема {schema.__name__} не является LLMCellOutput (контракт §7.1)",
                reason="schema",
                raw=raw,
            )
        return parsed

    # ------------------------------------------------------------------
    # Реальный HTTP-путь (openai-совместимый /v1/chat/completions)
    # ------------------------------------------------------------------

    def _http_complete(
        self, req: LLMRequest, *, seed: int, temperature: float
    ) -> tuple[str, TokenUsage, str | None, str | None]:
        """Реальный вызов OI-совместимого API. Схема → constrained decoding; без tools (I18).

        Возвращает ``(content, usage, finish_reason, reasoning_content)``: метка остановки
        генерации — провенанс ответа (``length`` иначе неотличим от невалидного JSON),
        текст рассуждения — провенанс диагностики (журнал).

        **Усилие рассуждения** (ось ``llm.reasoning_effort``) переводится в тело запроса
        таблицей семейств (:func:`reasoning_wire`). Невыразимый уровень —
        :class:`ReasoningWireUnsupported` **до** отправки: тихий пропуск параметра выдал бы
        «уровень не выбран» за «уровень объявлен».
        """
        model_id = req.model_id or self.model_id
        effort = (
            req.reasoning_effort
            if req.reasoning_effort is not None
            else self.reasoning_effort
        )
        payload: dict[str, Any] = {
            "model": model_id,
            "messages": [{"role": "user", "content": req.prompt}],
            "temperature": temperature,  # 0.0 ⇒ greedy (детерминизм)
            "seed": seed,
            "stream": False,
        }
        # Уровень рассуждения — условие генерации (входит в llm_scenarios → run_id).
        # Таблица одна на все семейства; невыразимый уровень — отказ, а не пропуск.
        payload.update(reasoning_wire(model_id, effort))
        # V5: потолок генерации — условие измерения, а не «настройка вида». До правки
        # ``max_tokens`` из слота не доезжал до провода вообще, поэтому потолок задавал
        # сервер своим дефолтом, а обрезанная генерация была неотличима от невалидного
        # JSON (оба приходили как один ``EvaluationError``).
        max_tokens = req.max_tokens if req.max_tokens is not None else self.max_tokens
        if max_tokens is not None:
            payload["max_tokens"] = int(max_tokens)
        if self.decoder != Decoder.NONE:
            # V4: метка и провод — одно и то же. До правки любое значение ≠ none
            # отправляло ``json_schema``, а в провенанс уходила запрошенная метка
            # (в живом прогоне: манифест ``json_object``, провод ``json_schema``).
            payload["response_format"] = self._response_format(req)
        # I18: tools/web_search в запрос НЕ добавляются — здесь их нет по построению.
        return self._extract(self._post_chat(payload))

    def _post_chat(self, payload: dict[str, Any]) -> Any:
        """Один POST на ``/v1/chat/completions`` и разбор тела ответа в ``data``.

        Один путь на боевой вызов (:meth:`_http_complete`) и на ход с инструментами
        (:meth:`turn`): иначе два места по-своему решали бы, что считать отказом
        транспорта, а что — ответом провайдера, и журнал вызовов читался бы по-разному в
        зависимости от того, был ли включён профиль инструментов.

        Один клиент на адаптер: ``httpx.post`` создаёт клиент на вызов, а при
        параллельных вызовах это лишнее TLS-рукопожатие на каждый. Пул соединений
        ограничен ``max_connections`` (≥ объявленного числа вызовов в полёте).
        """
        client = self._http_client()
        try:
            response = client.post(
                self._completions_url(),
                json=payload,
                headers=self._headers(),
                timeout=self.timeout,
            )
        except Exception as exc:  # httpx.HTTPError/JSON-ошибки транспорта
            raise EvaluationError(
                f"HTTP-вызов оценщика не удался: {type(exc).__name__}: {exc}",
                reason="http_error",
            ) from exc
        if response.status_code >= 400:
            # Статус и ``Retry-After`` едут в отказе: вызывающий (Ш6) по ним отличает
            # «подожди и повтори» (``429``/``5xx``) от «ответ по существу» и ставит паузу,
            # а не повторяет немедленно. Текст сообщения — без заголовков и ключей.
            #
            # Тело ответа — тоже в отказе: провайдер объясняет причину именно там
            # (замер 14.11.2026: ``401`` + ``{"error":{"message":"Отсутствует или неверный
            # API-ключ.",…}}``). Без тела 401/402/403 неотличимы по артефактам, а
            # оператор ищет причину в консоли. Тело добавляется, только если оно непусто.
            body = provider_body_text(response)
            detail = f"HTTP {response.status_code}"
            if body:
                detail = f"{detail}: {body}"
            raise EvaluationError(
                f"HTTP-вызов оценщика не удался: {detail}",
                reason="http_error",
                status_code=response.status_code,
                retry_after=retry_after_header(response),
                provider_body=body,
            )
        try:
            return response.json()
        except Exception as exc:  # JSON-ошибка транспорта
            raise EvaluationError(
                f"HTTP-вызов оценщика не удался: {type(exc).__name__}: {exc}",
                reason="http_error",
                status_code=response.status_code,
            ) from exc

    def _completions_url(self) -> str:
        """Адрес генерации: каноническая база + ``/v1/chat/completions``.

        Одна формула на боевой вызов и на :meth:`probe`: предпроверка ключа обязана идти
        **тем же** путём, иначе она проверяет не то, чем меряют.
        """
        return f"{normalize_api_base(self.base_url)}{CHAT_COMPLETIONS_PATH}"

    def _headers(self) -> dict[str, str]:
        """Заголовки запроса: JSON + bearer, если ключ объявлен.

        Одна формула на боевой вызов и на :meth:`probe` — по той же причине, что и у
        :meth:`_completions_url`. Ключ живёт только в заголовке и никуда не логируется.
        """
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _http_client(self) -> Any:
        """Ленивый единый HTTP-клиент адаптера (пул соединений на все вызовы).

        ``httpx`` импортируется лениво: модуль обязан импортироваться без него
        (:func:`_httpx_available`). Лимит соединений — ``max_connections`` (дефолт
        ``httpx`` — 100, заведомо выше объявленного параллелизма): значение ниже числа
        потоков заставило бы воркеры ждать соединения, и «параллельность» осталась бы в
        отчёте, а не в исполнении.
        """
        with self._client_lock:
            if self._client is None:
                httpx = self._import_http()
                limits = (
                    httpx.Limits(
                        max_connections=self.max_connections,
                        max_keepalive_connections=self.max_connections,
                    )
                    if self.max_connections is not None
                    else httpx.Limits()
                )
                self._client = httpx.Client(limits=limits)
            return self._client

    def _response_format(self, req: LLMRequest) -> dict[str, Any]:
        """``response_format`` под объявленный механизм (зеркало discovery-адаптера).

        ``json_schema`` — грамматика из JSON Schema (сильнее); ``json_object`` — только
        «верни объект»; ``none`` сюда не доходит. Различать их обязательно: спека §7.1
        называет **два** механизма, и провайдеры поддерживают их по-разному, поэтому
        подмена одного другим — это другое исполнение, а не то же самое под другим именем.
        """
        if decoder_wire_type(self.decoder) == Decoder.JSON_OBJECT.value:
            return {"type": "json_object"}
        return {
            "type": "json_schema",
            "json_schema": {
                "name": "llm_cell_output",
                "schema": req.response_schema.model_json_schema(),
                "strict": True,
            },
        }

    def _extract(
        self, data: Any
    ) -> tuple[str, TokenUsage, str | None, str | None]:
        """Достаёт ``choices[0].message.content``, ``usage`` и ``finish_reason``.

        V5: ``finish_reason == "length"`` с пустым текстом — это **исчерпанный потолок
        генерации**, а не «невалидный JSON». Различать обязательно: иначе обрезание
        уходит в ``validation_failed`` → retry ≤ ``max_retries`` → ``failed``, и причина отказа не
        называется ни в ячейке, ни в отчёте (так же поступает discovery-адаптер,
        ``llm_discovery.py:256-272``).

        Метка остановки и ``usage`` читаются **до** проверок: отказ по потолку тоже
        израсходовал токены, и путь отказа обязан их назвать (журнал вызовов).

        Оболочка ответа разбирается **одним** местом (:meth:`_envelope`) — её же читает
        ход с инструментами: два разбора одного ответа разошлись бы молча.
        """
        message, finish_reason, usage, reasoning = self._envelope(data)
        content = message.get("content") if isinstance(message, dict) else None
        if (content is None or content == "") and finish_reason == "length":
            limit = self.max_tokens if self.max_tokens is not None else "серверный"
            error = EvaluationError(
                "генерация оценщика оборвана по потолку токенов "
                f"(finish_reason=length, max_tokens={limit}): увеличьте "
                "llm.<slot>.max_tokens — модель израсходовала бюджет на reasoning",
                reason="length",
                finish_reason=finish_reason,
                raw=content if isinstance(content, str) else "",
                usage=usage,
            )
            # Рассуждение — единственный текст, который успела выдать модель: без него
            # путь отказа неразбираем (в ответе ``content`` пуст).
            error.reasoning = reasoning
            raise error
        if not isinstance(content, str):
            error = EvaluationError(
                f"choices[0].message.content обязан быть строкой, получено {type(content).__name__}",
                reason="invalid_json",
                finish_reason=finish_reason,
                usage=usage,
            )
            error.reasoning = reasoning
            raise error
        return content, usage, finish_reason, reasoning

    def _envelope(
        self, data: Any
    ) -> tuple[Any, str | None, TokenUsage, str | None]:
        """Оболочка ответа провайдера: ``message``, ``finish_reason``, ``usage``, reasoning.

        Единственный разбор ``choices[0]`` на модуль: им пользуются и обычный вызов
        (:meth:`_extract`), и ход с инструментами (:meth:`_turn_from_response`).
        ``content`` здесь **не** требуется: у хода с вызовами его может не быть вовсе,
        и это не «неожиданная форма», а другой вид ответа.
        """
        try:
            choice = data["choices"][0]
            message = choice["message"]
        except (KeyError, IndexError, TypeError) as exc:
            raise EvaluationError(
                f"неожиданная форма ответа сервера: {data!r}", reason="http_error"
            ) from exc
        finish_reason = choice.get("finish_reason") if isinstance(choice, dict) else None
        if not isinstance(finish_reason, str):
            finish_reason = None
        usage_raw = (data.get("usage") or {}) if isinstance(data, dict) else {}
        if not isinstance(usage_raw, dict):
            # Неожиданная форма расхода — «не названо», а не измеренный ноль.
            usage_raw = {}
        # Рассуждение провайдер называет по-разному: ``completion_tokens_details.
        # reasoning_tokens`` (OI-совместимая форма) либо ``completion_thinking_tokens``
        # (плоское поле части совместимых серверов). Оба — часть ``completion``, поэтому
        # не прибавляются к нему, а публикуются отдельной статьёй (провенанс стоимости).
        usage = TokenUsage(
            prompt_tokens=int(usage_raw.get("prompt_tokens", 0) or 0),
            completion_tokens=int(usage_raw.get("completion_tokens", 0) or 0),
            reasoning_tokens=_reasoning_tokens_of(usage_raw),
        )
        # Текст рассуждения, если провайдер его отдал (замер 06.11.2026: отдаёт).
        reasoning = message.get("reasoning_content") if isinstance(message, dict) else None
        if not isinstance(reasoning, str) or not reasoning:
            reasoning = None
        return message, finish_reason, usage, reasoning

    def _import_http(self) -> Any:
        """Лениво импортирует ``httpx``; при отсутствии — ``EvaluatorUnavailable``."""
        if not _httpx_available():
            raise EvaluatorUnavailable(
                "httpx не установлен: реальный HTTP-путь оценщика недоступен. "
                "Установите extra 'crawl' (05) или передайте generate."
            )
        import httpx

        return httpx
