"""Generic config-driven реестровый fetcher — реализация ``RegistryFetcherPort``.

Механизм: конкретный внешний реестр описывается **данными** (YAML-конфиг
``registries/{id}.yaml``: URL-шаблон, формат ``json|html``, маппинг полей), а этот
адаптер интерпретирует конфиг. Никакой предметной специфики в ядре нет (§1.3,
архитектура 01 §7; 02 §D.1).

**R1.** Каждый возвращаемый ``RegistryFact`` несёт непустой ``source_url`` — это
фактический URL запроса (для GET он же — URL источника).

**Внедрение HTTP-клиента.** Сеть не хардкодится: конструктор принимает
``http_get``. Поддерживаются две формы:

* callable ``(url, headers) -> bytes`` — таймаут/ретраи/TLS задаёт сам клиент при
  построении (``config.timeout`` доступен composition root);
* объект с методом ``get(url, headers, timeout) -> bytes`` — тогда ``config.timeout``
  передаётся ему явно.

**Ошибки.** Сеть/парсинг/конфиг не «проглатываются»: поднимаются подклассы
``RegistryError`` (``RegistryConfigError``, ``RegistryFetchError``, ``RegistryParseError``).
Вызывающий pipeline ловит ``RegistryError`` и отображает на ``source_unavailable``.

**Маппинг.** Пути разрешаются относительно записи (record-relative). Поддерживается
подмножество dot-path: ключи, числовые индексы ``[0]`` и раскрытие списка ``[*]``;
ведущие ``$``/``$.`` отбрасываются. Для ``json`` записи выделяются ``records_path``
(иначе — верхнеуровневый список или весь объект как одна запись). Для ``html``
записи — элементы с тегом ``records_path``; в путях маппинга ``@attr`` — атрибут,
``#text``/``text`` — нормализованный текст, ``tag`` — имя тега.

Нейтрален (§1.3): в модуле и докстрингах нет названий продуктов/вендоров/reestr'ов.
"""

from __future__ import annotations

import json
import re
import string
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Literal, Protocol, cast, runtime_checkable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from core.domain import RegistryFact
from core.ports.types import ProductRef

__all__ = [
    "GenericRegistryFetcher",
    "HttpClient",
    "RegistryConfig",
    "RegistryConfigError",
    "RegistryError",
    "RegistryFetchError",
    "RegistryParseError",
    "load_registry_config",
]


# ---------------------------------------------------------------------------
# Ошибки адаптера
# ---------------------------------------------------------------------------


class RegistryError(Exception):
    """Базовая ошибка реестрового адаптера. Pipeline отображает её на ``source_unavailable``."""


class RegistryConfigError(RegistryError):
    """Некорректный/неполный конфиг реестра (YAML/шаблон/маппинг)."""


class RegistryFetchError(RegistryError):
    """Сбой сети/транспорта при получении ответа реестра."""


class RegistryParseError(RegistryError):
    """Ответ получен, но не разобран (пустой/битый/неожиданная структура)."""


# ---------------------------------------------------------------------------
# Конфиг реестра
# ---------------------------------------------------------------------------


class RegistryConfig(BaseModel):
    """Конфиг одного реестра (``registries/{registry_id}.yaml``, 03 §D.4).

    Специфика внешнего источника живёт здесь как данные: URL-шаблон, метод, формат,
    маппинг. Соответствует эскизу 03 §D.4 (поля ``registry_id``/``jurisdiction``/
    ``endpoint``/``method``/``format``/``mapping``) с добавленными ``records_path``,
    ``query_param``, ``headers`` и ``timeout``.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    registry_id: str
    jurisdiction: str
    endpoint: str
    format: Literal["json", "html"]
    method: str = "GET"
    mapping: dict[str, str] = Field(default_factory=dict)
    query_param: str | None = None
    records_path: str | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    timeout: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def _check(self) -> RegistryConfig:
        if not self.registry_id:
            raise ValueError("registry_id обязателен")
        if not self.jurisdiction:
            raise ValueError("jurisdiction обязателен")
        if not self.endpoint:
            raise ValueError("endpoint обязателен")
        if self.method.upper() != "GET":
            raise ValueError("поддерживается только метод GET (адаптер не шлёт тело запроса)")
        if not self.mapping:
            raise ValueError("mapping не может быть пустым: нечего извлекать")
        return self


def load_registry_config(
    source: str | Path | Mapping[str, Any],
) -> RegistryConfig:
    """Загружает ``RegistryConfig`` из YAML-файла или уже разобранного словаря.

    ``source`` — путь к YAML-файлу либо mapping (например, после ``yaml.safe_load``).
    Любая проблема чтения/структуры/валидации поднимается как ``RegistryConfigError``.
    """
    if isinstance(source, Mapping):
        data: Any = dict(source)
    else:
        path = Path(source)
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise RegistryConfigError(f"не удалось прочитать конфиг реестра: {path}") from exc
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise RegistryConfigError(f"некорректный YAML конфига реестра: {path}") from exc

    if not isinstance(data, Mapping):
        raise RegistryConfigError("конфиг реестра обязан быть отображением (mapping)")

    try:
        return RegistryConfig(**data)
    except ValidationError as exc:
        raise RegistryConfigError(f"некорректный конфиг реестра: {exc}") from exc


# ---------------------------------------------------------------------------
# HTTP-клиент (внедряемая зависимость)
# ---------------------------------------------------------------------------


@runtime_checkable
class HttpClient(Protocol):
    """Минимальный контракт внедряемого клиента: объект с ``get``.

    ``Callable[[str, dict], bytes]`` также принимается конструктором как более
    простая форма (тогда ``config.timeout`` — забота вызывающей стороны).
    """

    def get(
        self, url: str, headers: Mapping[str, str], timeout: float | None = None
    ) -> bytes: ...


# ---------------------------------------------------------------------------
# Разбор путей маппинга
# ---------------------------------------------------------------------------

_MISSING: Any = object()
_PATH_PART_RE = re.compile(r"^([^\[\]]*)((?:\[[^\[\]]*\])*)$")


def _split_path(path: str) -> list[tuple[str, str]]:
    """Разбирает dot-path на сегменты ``("key"|"idx"|"wild", value)``.

    Поддерживаются ``a.b``, ``a[0].b``, ``a[*].b``; ведущие ``$``/``$.`` отбрасываются.
    """
    cleaned = path.strip()
    if cleaned.startswith("$"):
        cleaned = cleaned[1:]
    cleaned = cleaned.lstrip(".")
    if not cleaned:
        return []

    segments: list[tuple[str, str]] = []
    for part in cleaned.split("."):
        match = _PATH_PART_RE.match(part)
        if match is None:
            raise RegistryConfigError(f"некорректный путь маппинга: {path!r}")
        base, brackets = match.group(1), match.group(2)
        if base:
            segments.append(("key", base))
        for index in re.findall(r"\[([^\[\]]*)\]", brackets):
            index = index.strip()
            if index == "*":
                segments.append(("wild", index))
            elif index:
                segments.append(("idx", index))
    return segments


def _resolve_path(obj: Any, path: str) -> Any:
    """Разрешает dot-path относительно ``obj``; ``_MISSING`` — если ничего не найдено."""
    current: list[Any] = [obj]
    for kind, value in _split_path(path):
        nxt: list[Any] = []
        for item in current:
            if kind == "key":
                if isinstance(item, Mapping) and value in item:
                    nxt.append(item[value])
            elif kind == "wild":
                if isinstance(item, (list, tuple)):
                    nxt.extend(item)
                elif isinstance(item, Mapping):
                    nxt.extend(item.values())
            else:  # idx
                try:
                    position = int(value)
                except ValueError as exc:
                    raise RegistryConfigError(f"нечисловой индекс в пути: {path!r}") from exc
                if isinstance(item, (list, tuple)) and -len(item) <= position < len(item):
                    nxt.append(item[position])
        current = nxt
        if not current:
            return _MISSING
    if len(current) == 1:
        return current[0]
    return current


def _resolve_html_value(node: _HtmlNode, source_path: str) -> Any:
    """Разрешает путь маппинга HTML-записи.

    ``@attr`` — атрибут, ``#text``/``text``/``.`` — нормализованный текст, ``tag`` —
    имя тега. Голое имя трактуется как атрибут.
    """
    key = source_path.strip()
    if key in ("", ".", "#text", "text"):
        return node.text
    if key == "tag":
        return node.tag
    if key.startswith("@"):
        return node.attrs.get(key[1:], _MISSING)
    return node.attrs.get(key, _MISSING)


# ---------------------------------------------------------------------------
# HTML-разбор (stdlib, без внешних зависимостей)
# ---------------------------------------------------------------------------


class _HtmlNode:
    """Собранный элемент: тег, атрибуты и накопленный текст (включая вложенный)."""

    __slots__ = ("tag", "attrs", "text_parts")

    def __init__(self, tag: str, attrs: Mapping[str, str]) -> None:
        self.tag = tag
        self.attrs: dict[str, str] = dict(attrs)
        self.text_parts: list[str] = []

    @property
    def text(self) -> str:
        """Нормализованный текст (склейка и сжатие пробелов)."""
        return " ".join(" ".join(self.text_parts).split())


class _HtmlCollector(HTMLParser):
    """Минимальный ``HTMLParser``: собирает элементы, атрибуты и агрегированный текст.

    Текст приписывается всем открытым элементам — так ``tr`` видит текст вложенной
    ``td``. Грациозен к незакрытым тегам (стек обрезается по совпадению имени).
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.nodes: list[_HtmlNode] = []
        self._stack: list[_HtmlNode] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        node = _HtmlNode(tag, {key: (val or "") for key, val in attrs})
        self.nodes.append(node)
        self._stack.append(node)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.nodes.append(_HtmlNode(tag, {key: (val or "") for key, val in attrs}))

    def handle_data(self, data: str) -> None:
        for node in self._stack:
            node.text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        for position in range(len(self._stack) - 1, -1, -1):
            if self._stack[position].tag == tag:
                del self._stack[position:]
                return


# ---------------------------------------------------------------------------
# Адаптер
# ---------------------------------------------------------------------------


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class GenericRegistryFetcher:
    """Config-driven реестровый fetcher; структурно реализует ``RegistryFetcherPort``.

    Один экземпляр обслуживает один реестр (``config.registry_id``). Сеть — через
    внедрённый ``http_get``; часы — через ``clock`` (для детерминизма в тестах).
    """

    def __init__(
        self,
        config: RegistryConfig,
        http_get: Callable[..., bytes] | HttpClient,
        *,
        clock: Callable[[], str] | None = None,
    ) -> None:
        self.config = config
        self.registry_id = config.registry_id
        self._http_get = http_get
        self._clock = clock or _utcnow

    # -- публичный контракт порта ---------------------------------------- #

    def fetch(self, product: ProductRef, scope: dict[str, Any]) -> list[RegistryFact]:
        """Получает и нормализует факты реестра для продукта.

        ``scope`` может нести ``run_id`` и произвольные ключи, подставляемые в URL-шаблон
        (в т.ч. ``query``). Каждый факт: ``fact_json`` (JSON-строка маппинга записи),
        ``product_id``, ``registry_id``, ``source_url`` (R1) и ``fetched_at``.
        """
        scope = dict(scope or {})
        url = self._build_url(product, scope)
        raw = self._request(url)
        records = self._parse(raw)
        fetched_at = self._clock()
        run_id = str(scope.get("run_id", ""))

        facts: list[RegistryFact] = []
        for record in records:
            facts.append(
                RegistryFact(
                    run_id=run_id,
                    product_id=product.product_id,
                    registry_id=self.config.registry_id,
                    fact_json=json.dumps(record, ensure_ascii=False, sort_keys=True),
                    source_url=url,
                    fetched_at=fetched_at,
                )
            )
        return facts

    # -- построение URL -------------------------------------------------- #

    def _build_url(self, product: ProductRef, scope: Mapping[str, Any]) -> str:
        values = self._template_values(product, scope)
        url = self._render(self.config.endpoint, values)
        if self.config.query_param:
            url = self._append_query_param(url, self.config.query_param, values["query"])
        if not url:
            raise RegistryConfigError("URL запроса пуст после подстановки шаблона")
        return url

    @staticmethod
    def _template_values(product: ProductRef, scope: Mapping[str, Any]) -> dict[str, str]:
        values: dict[str, str] = {
            "product_id": product.product_id,
            "canonical_name": product.canonical_name,
            "vendor": product.vendor,
            "jurisdiction": product.jurisdiction,
        }
        for key, value in product.extra.items():
            values.setdefault(str(key), "" if value is None else str(value))
        for key, value in scope.items():
            values[str(key)] = "" if value is None else str(value)
        if not values.get("query"):
            values["query"] = values.get("canonical_name") or values.get("product_id", "")
        return values

    @staticmethod
    def _render(template: str, values: Mapping[str, str]) -> str:
        fields = [
            field_name
            for _, field_name, _, _ in string.Formatter().parse(template)
            if field_name
        ]
        missing = [
            field_name
            for field_name in fields
            if field_name.split(".")[0].split("[")[0] not in values
        ]
        if missing:
            raise RegistryConfigError(
                f"в URL-шаблоне не хватает значений: {sorted(set(missing))}"
            )
        try:
            rendered = template.format(**values)
        except (KeyError, IndexError, ValueError) as exc:
            raise RegistryConfigError(f"не удалось подставить URL-шаблон: {template!r}") from exc
        if "{" in rendered or "}" in rendered:
            raise RegistryConfigError(f"в URL остались неподставленные плейсхолдеры: {rendered!r}")
        return rendered

    @staticmethod
    def _append_query_param(url: str, name: str, value: str) -> str:
        parts = urlsplit(url)
        pairs = parse_qsl(parts.query, keep_blank_values=True)
        if any(key == name for key, _ in pairs):
            return url
        pairs.append((name, value))
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(pairs), parts.fragment))

    # -- транспорт ------------------------------------------------------- #

    def _request(self, url: str) -> bytes | str:
        headers = dict(self.config.headers)
        client: Any = self._http_get
        getter = getattr(client, "get", None)
        try:
            if callable(getter):
                return cast(bytes | str, getter(url, headers, self.config.timeout))
            if callable(client):
                return cast(bytes | str, client(url, headers))
            raise RegistryConfigError("http_get обязан быть callable или объектом с методом get")
        except RegistryError:
            raise
        except Exception as exc:  # noqa: BLE001 — грациозная граница транспорта
            raise RegistryFetchError(f"сбой HTTP-запроса к реестру: {url}") from exc

    # -- разбор ответа --------------------------------------------------- #

    def _parse(self, raw: bytes | str) -> list[dict[str, Any]]:
        if self.config.format == "json":
            return self._parse_json(raw)
        return self._parse_html(raw)

    def _parse_json(self, raw: bytes | str) -> list[dict[str, Any]]:
        text = raw.decode("utf-8", errors="strict") if isinstance(raw, bytes) else raw
        if not text or not text.strip():
            raise RegistryParseError("пустой JSON-ответ реестра")
        try:
            payload = json.loads(text)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise RegistryParseError("некорректный JSON-ответ реестра") from exc
        records = self._extract_records(payload)
        return [self._map_record(record) for record in records]

    def _extract_records(self, payload: Any) -> list[Any]:
        if self.config.records_path:
            found = _resolve_path(payload, self.config.records_path)
            if found is _MISSING:
                raise RegistryParseError(
                    f"records_path {self.config.records_path!r} не найден в ответе"
                )
            if isinstance(found, (list, tuple)):
                return list(found)
            return [found]
        if isinstance(payload, (list, tuple)):
            return list(payload)
        if isinstance(payload, Mapping):
            if not payload:
                return []
            return [payload]
        return [payload]

    def _map_record(self, record: Any) -> dict[str, Any]:
        mapped: dict[str, Any] = {}
        for field_name, source_path in self.config.mapping.items():
            value = _resolve_path(record, source_path)
            if value is _MISSING:
                continue
            mapped[field_name] = value
        return mapped

    def _parse_html(self, raw: bytes | str) -> list[dict[str, Any]]:
        text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
        if not text or not text.strip():
            return []
        collector = _HtmlCollector()
        try:
            collector.feed(text)
            collector.close()
        except Exception as exc:  # noqa: BLE001 — html.parser не должен ронять конвейер
            raise RegistryParseError("сбой разбора HTML-ответа реестра") from exc
        return [self._map_html_node(node) for node in self._html_records(collector.nodes)]

    def _html_records(self, nodes: list[_HtmlNode]) -> list[_HtmlNode]:
        wanted = (self.config.records_path or "").strip().lower()
        if not wanted:
            return list(nodes)
        return [node for node in nodes if node.tag.lower() == wanted]

    def _map_html_node(self, node: _HtmlNode) -> dict[str, Any]:
        mapped: dict[str, Any] = {}
        for field_name, source_path in self.config.mapping.items():
            value = _resolve_html_value(node, source_path)
            if value is _MISSING:
                continue
            mapped[field_name] = value
        return mapped
