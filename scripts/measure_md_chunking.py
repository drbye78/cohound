"""Приёмка нарезки markdown: инварианты P1–P13 на реальном корпусе, а не на примерах.

Инструмент отвечает на вопрос, который нельзя закрыть юнит-тестами: **что нарезка делает
с настоящими документами**. Он не «смотрит глазами» — он проверяет свойства, объявленные
в ``docs/architecture/18-markdown-chunker-plan.md`` §3, и печатает числа, включая нули.

**Вход** — сохранённая БД прогона (``cohound.db``), читается **только на чтение**:
страницы берутся из ``crawl_session_items``, тела — по ``raw_path``, текст — тем же
экстрактором, что и в прогоне (``HtmlExtractor``), чтобы мерить именно то, что увидит
чанкер. Сеть, запись в БД и правки в ``core/`` — вне контракта инструмента.

**Метрики** (определения зафиксированы):

``p1_violations``
    Документы, где ``"".join(chunk.text)`` ≠ нормализованный вход. Инвариант: **0**.
``p4_violations``
    Документы, где текст чанка не является подстрокой входа. Инвариант: **0**.
``p8_violations``
    Чанки, начинающиеся с закрывающей пунктуации **из-за реза**. Инвариант: **0**.
    Начало на границе блока не считается: если абзац источника сам начинается с точки
    (``<p>. Согласно стандарту…</p>``), чанкер по P1 не имеет права эту точку сдвинуть.
    Такие случаи считаются отдельно (``source_punct``) — метрика не прячет их, а
    отделяет причину.
``over_budget``
    Чанки длиннее ``chunk_size``. Ноль — цель; ненулевое значение означает, что хвост
    приклеен к предыдущему куску (объявленный компромисс, см. ``_absorb_tail``).
``tiny``
    Чанки короче ``min_chunk_size``: самостоятельно объявленные единицы документа
    (однострочная секция). Не ошибка, но число публикуется — «мелкий мусор» проверяем.
``without_heading``
    Чанки без ``heading_path`` (документ без заголовков или текст до первого).
``with_context``
    Чанки, чей ``context_prefix`` отличается от ``heading_path``: продолжения таблиц,
    fence и цитат (P9/P10).
``mid_sentence``
    Границы, оборвавшие предложение: предыдущий чанк не кончается знаком конца
    предложения, следующий начинается с буквы/иероглифа. Считается только для прозы
    (не для кода и таблиц), поэтому это доля от ``boundaries``.
``unknown_lines``
    Строки, не покрытые ни одним блоком (P13). Инвариант: **0**.

**Три необязательных измерения** (флаги независимы; по умолчанию считаются инварианты и
перепись, тяжёлые токены — нет):

``--tokens``
    Геометрия в **информационных единицах**: токены ``chunk.text`` (это вход эмбеддера —
    ``EMBED_INPUT_FIELDS=("text",)``) и токены ``heading_path + context_prefix + text``
    (нижняя граница входа промпта: инструкции промпта не в счёт). Токенизатор — тот же,
    что у эмбеддера, из локального кэша (``local_files_only=True``). Модель берётся из
    манифеста (``runs.embed_model_id``); если манифест молчит — это **допущение замера**,
    и оно печатается, а не замалчивается. Символьный прокси вместо токенов не
    подставляется ни при каких условиях: он мерил бы другую величину.
``--census`` (по умолчанию включена)
    Перепись **кандидатов** pandoc-конструкций регексами по сырому извлечённому тексту
    документа (не по чанкам): документов, вхождений, до 3 примеров ``url:строка``.
    Это кандидаты: регекс отвечает «похоже», а решение о грамматике принимается по числу
    и по прочитанным примерам.
``--skeleton``
    Скелетные формы чанков ровно по определению замера 2026-10-26
    (``docs/reports/measurements/2026-10-26-chunk-content-policy.md``): сколько чанков
    входят в повторяющиеся группы (посылка P4b), а не повтор прежнего замера на старом
    наборе.

**Чего метрики НЕ измеряют:** качество эмбеддингов и ретривала (это отдельный замер на
индексной сборке), κ оценщика, стоимость эмбеддинга. Здесь — только геометрия нарезки.
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import importlib.util
import json
import math
import re
import statistics
import sys
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any

from core.adapters.chunker_markdown import MIN_CHUNK_SIZE, MarkdownChunker
from core.adapters.extract_html import HtmlExtractor
from core.adapters.md_ast import MdDocument, parse_markdown
from core.ports.types import Chunk, RawPage
from core.util.document_formats import FAMILY_TEXT
from core.util.text_utils import CLOSING_CHARS


def _load_sibling(name: str):
    """Подгружает соседний скрипт как модуль (переиспользование раскладки и выборки)."""
    path = Path(__file__).with_name(f"{name}.py")
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"не удалось загрузить {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_mhe = _load_sibling("measure_html_extraction")

#: Знаки конца предложения: западные и полноширинные (CJK-пунктуация).
_SENTENCE_END = frozenset(".!?;…。！？；…:：)\"»”」』")


#: Блоки, где начало чанка с закрывающей пунктуации — норма (код, таблица, формула).
_NON_PROSE = frozenset({"fence", "code", "html", "math", "table_row", "reference", "front_matter"})


@dataclass
class _Spans:
    """Все блоки документа: ``(начало, конец, вид)`` — для вопроса «что здесь за текст»."""

    spans: list[tuple[int, int, str]]

    @classmethod
    def of(cls, document: MdDocument) -> _Spans:
        return cls(
            [
                (block.span.start_char, block.span.end_char, block.kind)
                for top in document.blocks
                for block in top.walk()
            ]
        )

    def starts_block(self, offset: int) -> bool:
        """Начинается ли в ``offset`` блок документа (то есть рез не создал эту границу)."""
        return any(start == offset for start, _end, _kind in self.spans)

    def kind_at(self, offset: int) -> str | None:
        """Вид самого глубокого блока, содержащего смещение."""
        best: tuple[int, str] | None = None
        for start, end, kind in self.spans:
            if start <= offset < end and (best is None or end - start < best[0]):
                best = (end - start, kind)
        return best[1] if best else None

    def paragraph_at(self, offset: int) -> tuple[int, int] | None:
        """Спан абзаца, содержащего смещение (для отличения реза внутри абзаца)."""
        best: tuple[int, int, int] | None = None
        for start, end, kind in self.spans:
            if kind != "paragraph" or not start <= offset < end:
                continue
            if best is None or end - start < best[0]:
                best = (end - start, start, end)
        return (best[1], best[2]) if best else None


@dataclass
class DocResult:
    """Замер одного документа. ``None`` — величина не определена (не ноль)."""

    host: str
    url: str
    chars: int
    chunks: list[Chunk]
    boundaries: int
    mid_sentence: int
    unknown_lines: int
    p1_violation: bool
    p4_violation: bool
    prose_p8: int = 0
    source_punct: int = 0
    examples: list[str] = field(default_factory=list)
    constructions: dict[str, int] = field(default_factory=dict)


def _host_of(url: str) -> str:
    return url.split("://", 1)[-1].split("/", 1)[0].split(":", 1)[0].lower()


def _mid_sentence_breaks(chunks: Sequence[Chunk], spans: _Spans) -> tuple[int, int]:
    """``(разрывы посреди предложения, резы внутри абзаца)``.

    Считаются только резы **внутри одного абзаца**: граница между абзацами — объявленная
    единица структуры, и «обрывом предложения» она не является. Иначе метрика мерила бы
    долю абзацных границ и ничего не говорила бы о качестве разрезов.
    """
    breaks = 0
    boundaries = 0
    position = 0
    for index in range(len(chunks) - 1):
        current, following = chunks[index], chunks[index + 1]
        position += len(current.text)
        left_paragraph = spans.paragraph_at(position - 1)
        right_paragraph = spans.paragraph_at(position)
        if left_paragraph is None or left_paragraph != right_paragraph:
            continue
        left = current.text.rstrip()
        right = following.text.lstrip()
        if not left or not right:
            continue
        boundaries += 1
        if left[-1] not in _SENTENCE_END and (right[0].isalnum() or _is_cjk(right[0])):
            breaks += 1
    return breaks, boundaries


def _is_cjk(char: str) -> bool:
    """Иероглиф/кандзи/хангыль — письменность без пробелов между словами."""
    code = ord(char)
    return (
        0x3040 <= code <= 0x30FF
        or 0x3400 <= code <= 0x4DBF
        or 0x4E00 <= code <= 0x9FFF
        or 0xAC00 <= code <= 0xD7AF
    )


def _measure(url: str, text: str, chunker: MarkdownChunker) -> DocResult:
    document = parse_markdown(text, chunker.profile)
    chunks = chunker.chunk(url, text)
    spans = _Spans.of(document)
    joined = "".join(chunk.text for chunk in chunks)
    breaks, boundaries = _mid_sentence_breaks(chunks, spans)
    examples: list[str] = []
    prose_p8 = 0
    source_punct = 0
    position = 0
    for chunk in chunks:
        kind = spans.kind_at(position)
        at_block_start = spans.starts_block(position)
        position += len(chunk.text)
        if chunk.text[:1] not in CLOSING_CHARS or kind in _NON_PROSE:
            continue
        if at_block_start:
            # Знак пришёл из источника: по P1 чанкер его не двигает. Не дефект реза.
            source_punct += 1
            continue
        prose_p8 += 1
        if len(examples) < 3:
            examples.append(f"P8 [{kind}]: {url} :: {chunk.text[:60]!r}")
    return DocResult(
        host=_host_of(url),
        url=url,
        chars=len(document.text),
        chunks=chunks,
        boundaries=boundaries,
        mid_sentence=breaks,
        unknown_lines=int(document.counts.get("unknown_lines") or 0),
        p1_violation=joined != document.text,
        p4_violation=any(chunk.text not in document.text for chunk in chunks),
        prose_p8=prose_p8,
        source_punct=source_punct,
        examples=examples,
        constructions={
            key: int(document.counts.get(key) or 0)
            for key in ("table", "fence", "list", "blockquote", "code", "footnote_def")
        },
    )


# ---------------------------------------------------------------------------
# Информационные единицы: токенизатор того же эмбеддера, без прокси
# ---------------------------------------------------------------------------

#: Окно эмбеддера bge-m3 (05 §2.3): чанк длиннее не входит в вектор целиком.
EMBED_CONTEXT_WINDOW = 8192

#: Границы гистограммы: интервал ``[граница, следующая)``.
TOKEN_BIN_EDGES: tuple[int, ...] = (64, 128, 256, 512, 1024, 2048, 4096)

#: Подписи гистограммы — на одну больше, чем границ.
TOKEN_BIN_LABELS: tuple[str, ...] = (
    "<64",
    "64–128",
    "128–256",
    "256–512",
    "512–1024",
    "1024–2048",
    "2048–4096",
    ">4096",
)

#: Порог класса «письменность без пробелов»: доля CJK среди непробельных символов.
CJK_SHARE_THRESHOLD = 0.20

#: Доля контекста в тексте чанка, выше которой чанк считается контекст-тяжёлым.
CONTEXT_SHARE_LIMIT = 0.25


class TokenizerUnavailable(RuntimeError):
    """Токенизатор недоступен; символьный прокси вместо токенов не подставляется."""


class TokenizerModelMismatch(RuntimeError):
    """Манифест прогона объявляет другую модель: её токенизатором мерить нельзя."""


def _import_auto_tokenizer() -> Any:
    """Ленивый импорт ``transformers``: без ``--tokens`` тяжёлая зависимость не нужна."""
    from transformers import AutoTokenizer  # type: ignore[import-not-found]  # noqa: PLC0415

    return AutoTokenizer


def _load_tokenizer(model_id: str) -> Any:
    """Загружает токенизатор из **локального** кэша; иначе — видимый отказ.

    ``local_files_only=True`` — часть контракта замера: сеть запрещена, и отсутствие
    снапшота должно выглядеть как ошибка, а не как тихий поход за весами. Прокси
    (символы вместо токенов) здесь не появляется ни в каком виде: он мерил бы другую
    величину под тем же именем.
    """
    try:
        factory = _import_auto_tokenizer()
    except Exception as exc:  # noqa: BLE001 - ImportError и его окружение равнозначны
        raise TokenizerUnavailable(
            f"transformers недоступен ({exc}); --tokens невозможен. "
            "Символьный прокси вместо токенов не подставляется."
        ) from exc
    try:
        return factory.from_pretrained(model_id, local_files_only=True)
    except Exception as exc:  # noqa: BLE001 - сеть/кэш/битый снапшот — один класс отказа
        raise TokenizerUnavailable(
            f"токенизатор {model_id!r} не загружен из локального кэша "
            f"(local_files_only=True): {exc}"
        ) from exc


def _count_tokens(tokenizer: Any, text: str) -> int:
    """Токены текста так, как их увидит модель: со служебными токенами, без усечения."""
    encoded = tokenizer(text, add_special_tokens=True, truncation=False)
    return len(encoded["input_ids"])


def _sha256(path: Path) -> str:
    """sha256 файла: провенанс токенизатора, а не «какой-то кэш»."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _tokenizer_file(model_id: str, tokenizer: Any) -> Path | None:
    """Путь к ``tokenizer.json``, который реально загружен (``None`` — не найден).

    Сначала — каталог, который назвал сам токенизатор. В ``transformers`` 5.x
    ``name_or_path`` для загруженного из кэша объекта равен **repo id**, а не пути,
    поэтому второй шаг — ``try_to_load_from_cache``: он смотрит только в локальный кэш
    и в сеть не ходит (то же ограничение, что у ``local_files_only=True``). Третий шаг —
    ничего: путь не выдумывается, и провенанс печатает ``None``.
    """
    root = Path(str(getattr(tokenizer, "name_or_path", "") or "")).expanduser()
    candidate = root / "tokenizer.json"
    if candidate.is_file():
        return candidate
    try:
        from huggingface_hub import try_to_load_from_cache  # noqa: PLC0415

        cached = try_to_load_from_cache(model_id, "tokenizer.json")
    except Exception:  # noqa: BLE001 - нет huggingface_hub — провенанс без пути
        return None
    if isinstance(cached, str) and Path(cached).is_file():
        return Path(cached)
    return None


def _tokenizer_provenance(model_id: str, tokenizer: Any) -> dict[str, Any]:
    """Провенанс: модель, путь к файлу словаря, его sha256, размеры словаря."""
    path = _tokenizer_file(model_id, tokenizer)
    try:
        vocabulary_size: int | None = len(tokenizer)
    except TypeError:  # токенизатор без ``__len__`` — размер не выдумываем
        vocabulary_size = None
    vocab_size = getattr(tokenizer, "vocab_size", None)
    return {
        "model_id": model_id,
        "tokenizer_class": type(tokenizer).__name__,
        "tokenizer_json": str(path) if path is not None else None,
        "tokenizer_json_sha256": _sha256(path) if path is not None else None,
        "vocab_size": vocab_size,
        "vocabulary_size": vocabulary_size,
        "path_seen_by_tokenizer": str(getattr(tokenizer, "name_or_path", "") or ""),
    }


def _declared_embed_model(conn: Any) -> str | None:
    """``runs.embed_model_id`` прогона (``None`` — манифест молчит)."""
    try:
        row = conn.execute("SELECT embed_model_id FROM runs LIMIT 1").fetchone()
    except Exception:  # noqa: BLE001 - БД без runs не должна валить замер
        return None
    if row is None or row[0] is None:
        return None
    value = str(row[0]).strip()
    return value or None


def _repo_part(model_id: str) -> str:
    """Идентификатор модели без пина ревизии (``repo@sha`` → ``repo``)."""
    return model_id.split("@", 1)[0].strip()


def _resolve_tokenizer_model(declared: str | None, requested: str) -> dict[str, Any]:
    """Согласует модель замера с объявленной в манифесте.

    Различие ревизий (``repo@sha``) допускается: это тот же словарь, а его sha256
    публикуется в провенансе. Различие **моделей** — отказ: токенизатор другой модели
    даёт другие токены, и число перестало бы описывать вход эмбеддера прогона.
    """
    if declared is None:
        return {"declared_embed_model_id": None, "model_id": requested, "assumed": True}
    if _repo_part(declared) != _repo_part(requested):
        raise TokenizerModelMismatch(
            f"манифест прогона объявляет embed_model_id={declared!r}, "
            f"а замер просит {requested!r}: мерить токенизатором другой модели нельзя. "
            f"Передайте --tokenizer-model {declared!r}."
        )
    return {
        "declared_embed_model_id": declared,
        "model_id": requested,
        "assumed": False,
        "revision_pinned": declared != requested,
    }


def _percentile(ordered: Sequence[int], quantile: float) -> float:
    """Перцентиль линейной интерполяцией по позиции ``q/100 * (n-1)``.

    Определение зафиксировано, чтобы «p90» в двух отчётах означало одно и то же;
    ``ordered`` обязан быть отсортирован.
    """
    if not ordered:
        raise ValueError("перцентиль пустого ряда не определён")
    if len(ordered) == 1:
        return float(ordered[0])
    position = (len(ordered) - 1) * quantile / 100.0
    low = math.floor(position)
    high = math.ceil(position)
    if low == high:
        return float(ordered[low])
    weight = position - low
    return float(ordered[low]) + (float(ordered[high]) - float(ordered[low])) * weight


def _distribution(values: Sequence[int]) -> dict[str, Any]:
    """``n/p50/p90/p99/max`` по ряду; пустой ряд — ``None``, а не нули."""
    if not values:
        return {"n": 0, "p50": None, "p90": None, "p99": None, "max": None}
    ordered = sorted(values)
    return {
        "n": len(ordered),
        "p50": _percentile(ordered, 50),
        "p90": _percentile(ordered, 90),
        "p99": _percentile(ordered, 99),
        "max": ordered[-1],
    }


def _histogram(values: Sequence[int]) -> dict[str, int]:
    """Гистограмма длин по объявленным границам (интервалы полуоткрытые)."""
    counts = dict.fromkeys(TOKEN_BIN_LABELS, 0)
    for value in values:
        index = bisect.bisect_right(TOKEN_BIN_EDGES, value)
        counts[TOKEN_BIN_LABELS[min(index, len(TOKEN_BIN_LABELS) - 1)]] += 1
    return counts


def _cjk_share(text: str) -> float:
    """Доля CJK среди непробельных символов (0.0 — непробельных нет)."""
    visible = [char for char in text if not char.isspace()]
    if not visible:
        return 0.0
    return sum(1 for char in visible if _is_cjk(char)) / len(visible)


def _script_class(text: str) -> str:
    """Класс письменности чанка: ``ZH`` при доле CJK ≥ порога, иначе ``LAT``."""
    return _script_class_of(_cjk_share(text))


def _script_class_of(share: float) -> str:
    """Класс по уже вычисленной доле CJK (порог включающий)."""
    return "ZH" if share >= CJK_SHARE_THRESHOLD else "LAT"


def _median_or_none(values: Sequence[float]) -> float | None:
    """Медиана по определённым значениям (``None`` — «не измерено», не ноль)."""
    return statistics.median(values) if values else None


@dataclass
class TokenGeometry:
    """Геометрия чанков в токенах: текст (вход эмбеддера) и промпт (нижняя граница)."""

    model_id: str
    text_tokens: list[int] = field(default_factory=list)
    prompt_tokens: list[int] = field(default_factory=list)
    context_tokens: list[int] = field(default_factory=list)
    heading_chars: list[int] = field(default_factory=list)
    heading_tokens: list[int] = field(default_factory=list)
    prefix_chars: list[int] = field(default_factory=list)
    prefix_tokens: list[int] = field(default_factory=list)
    chars_per_token: list[float] = field(default_factory=list)
    script_tokens: dict[str, list[int]] = field(default_factory=dict)
    script_chars_per_token: dict[str, list[float]] = field(default_factory=dict)
    max_cjk_share: float = 0.0
    chunks_with_cjk: int = 0
    context_heavy: int = 0

    def add(self, chunk: Chunk, count: Callable[[str], int]) -> None:
        """Добавляет чанк: токены текста, промпта, контекста и класс письменности."""
        heading = chunk.heading_path or ""
        prefix = chunk.context_prefix or ""
        text_tokens = count(chunk.text)
        heading_tokens = count(heading) if heading else 0
        prefix_tokens = count(prefix) if prefix else 0
        context = "\n".join(part for part in (heading, prefix) if part)
        context_tokens = count(context) if context else 0
        prompt = "\n".join(part for part in (heading, prefix, chunk.text) if part)

        self.text_tokens.append(text_tokens)
        self.prompt_tokens.append(count(prompt))
        self.context_tokens.append(context_tokens)
        self.heading_chars.append(len(heading))
        self.heading_tokens.append(heading_tokens)
        self.prefix_chars.append(len(prefix))
        self.prefix_tokens.append(prefix_tokens)
        if text_tokens:
            self.chars_per_token.append(len(chunk.text) / text_tokens)
        if context_tokens > CONTEXT_SHARE_LIMIT * text_tokens:
            self.context_heavy += 1

        script = _script_class(chunk.text)
        share = _cjk_share(chunk.text)
        self.max_cjk_share = max(self.max_cjk_share, share)
        self.chunks_with_cjk += int(share > 0.0)
        self.script_tokens.setdefault(script, []).append(text_tokens)
        if text_tokens:
            self.script_chars_per_token.setdefault(script, []).append(
                len(chunk.text) / text_tokens
            )

    def summary(self) -> dict[str, Any]:
        """Сводка: распределения, гистограмма, разрез по письменности, контекст."""
        chunks = len(self.text_tokens)
        text = _distribution(self.text_tokens)
        text.update(
            {
                "histogram": _histogram(self.text_tokens),
                "over_512": sum(1 for value in self.text_tokens if value > 512),
                "over_window": sum(
                    1 for value in self.text_tokens if value > EMBED_CONTEXT_WINDOW
                ),
                "chars_per_token_median": _median_or_none(self.chars_per_token),
            }
        )
        prompt = _distribution(self.prompt_tokens)
        prompt.update(
            {
                "histogram": _histogram(self.prompt_tokens),
                "over_512": sum(1 for value in self.prompt_tokens if value > 512),
                "over_window": sum(
                    1 for value in self.prompt_tokens if value > EMBED_CONTEXT_WINDOW
                ),
            }
        )
        script: dict[str, Any] = {}
        for name in sorted(self.script_tokens):
            values = self.script_tokens[name]
            distribution = _distribution(values)
            distribution["share"] = len(values) / (chunks or 1)
            distribution["chars_per_token_median"] = _median_or_none(
                self.script_chars_per_token.get(name, [])
            )
            script[name] = distribution
        return {
            "model_id": self.model_id,
            "chunks": chunks,
            "text": text,
            "prompt": prompt,
            "script": script,
            "script_diagnostics": {
                "threshold": CJK_SHARE_THRESHOLD,
                "max_cjk_share": self.max_cjk_share,
                "chunks_with_cjk": self.chunks_with_cjk,
            },
            "context": {
                "heading_chars": _distribution(self.heading_chars),
                "heading_tokens": _distribution(self.heading_tokens),
                "prefix_chars": _distribution(self.prefix_chars),
                "prefix_tokens": _distribution(self.prefix_tokens),
                "context_tokens": _distribution(self.context_tokens),
                "heavy_chunks": self.context_heavy,
                "heavy_share": self.context_heavy / (chunks or 1),
                "heavy_limit": CONTEXT_SHARE_LIMIT,
            },
        }


# ---------------------------------------------------------------------------
# Перепись кандидатов pandoc-конструкций (регексы по сырому тексту документа)
# ---------------------------------------------------------------------------

#: grid table: рамка таблицы из ``+---+``/``+===+``.
_GRID_LINE = re.compile(r"^\s*\+[-=+]{3,}\+\s*$")
#: line block: строка, начинающаяся с ``| `` (ровно одна черта — проверяется отдельно).
_LINE_BLOCK_LINE = re.compile(r"^\|\s")
#: fenced div.
_FENCED_DIV = re.compile(r"^\s*:::{3,}")
#: bracketed span.
_BRACKETED_SPAN = re.compile(r"\[[^\]\n]*\]\{[.#]")
#: citation.
_CITATION = re.compile(r"\[-?@[A-Za-z0-9_:.#$%&+?<>~/-]+")
#: example list.
_EXAMPLE_LIST = re.compile(r"^\s*\(@[A-Za-z0-9_-]*\)")
#: raw TeX: начало окружения.
_RAW_TEX_ENV = re.compile(r"\\begin\{")
#: raw TeX: команда (``\alpha``).
_RAW_TEX_CMD = re.compile(r"\\[A-Za-z]+")
#: implicit figure: одиночная картинка отдельной строкой.
_IMPLICIT_FIGURE = re.compile(r"^\s*!\[[^\]\n]*\]\([^)\n]*\)\s*$")
#: startnum.
_STARTNUM = re.compile(r"^\s*\d{1,9}\)\s")
#: fancy list marker (широкий регекс — примеры обязательны при чтении).
_FANCY_MARKER = re.compile(r"^\s*\(?[a-zA-ZivxlcdmIVXLCDM]{1,4}[.)]\s")
#: emoji.
_EMOJI = re.compile(r":[a-z0-9_+-]{2,}:")

#: Виды переписи и их подписи; порядок фиксирован (порядок в отчёте тоже).
CENSUS_TITLES: dict[str, str] = {
    "grid_table": "grid table (`+---+`)",
    "line_block": "line block (`| …`)",
    "fenced_div": "fenced div (`:::`)",
    "bracketed_span": "bracketed span (`[]{.x}`)",
    "citation": "citation (`[@key]`)",
    "example_list": "example list (`(@)`)",
    "toml_metadata": "TOML metadata (`+++`)",
    "raw_tex_env": "raw TeX: окружение (`\\begin{`)",
    "raw_tex_cmd": "raw TeX: команда (`\\cmd`)",
    "implicit_figure": "implicit figure (одиночный `![…](…)`)",
    "startnum": "startnum (`1) …`)",
    "fancy_list_marker": "fancy list marker (`a)` / `iv.`)",
    "emoji": "emoji (`:name:`)",
}

#: Виды переписи в порядке отчёта.
CENSUS_KINDS: tuple[str, ...] = tuple(CENSUS_TITLES)

#: Регексы, искомые по всему тексту (номер строки восстанавливается по смещению).
_CENSUS_INLINE: dict[str, re.Pattern[str]] = {
    "bracketed_span": _BRACKETED_SPAN,
    "citation": _CITATION,
    "raw_tex_env": _RAW_TEX_ENV,
    "raw_tex_cmd": _RAW_TEX_CMD,
    "emoji": _EMOJI,
}

#: Регексы, искомые построчно.
_CENSUS_LINE: dict[str, re.Pattern[str]] = {
    "fenced_div": _FENCED_DIV,
    "example_list": _EXAMPLE_LIST,
    "startnum": _STARTNUM,
    "fancy_list_marker": _FANCY_MARKER,
}


def _line_hits(pattern: re.Pattern[str], text: str) -> list[tuple[int, str]]:
    """Совпадения построчного регекса: ``(номер строки, строка)``."""
    return [
        (number, line)
        for number, line in enumerate(text.splitlines(), start=1)
        if pattern.search(line)
    ]


def _inline_hits(pattern: re.Pattern[str], text: str) -> list[tuple[int, str]]:
    """Совпадения регекса по всему тексту с номером строки каждого попадания."""
    lines = text.splitlines()
    starts: list[int] = []
    offset = 0
    for line in lines:
        starts.append(offset)
        offset += len(line) + 1
    hits: list[tuple[int, str]] = []
    for match in pattern.finditer(text):
        index = bisect.bisect_right(starts, match.start()) - 1
        if 0 <= index < len(lines):
            hits.append((index + 1, lines[index]))
    return hits


def _grid_table_hits(text: str) -> list[tuple[int, str]]:
    """grid table — кандидат только при **двух и более** строках рамки."""
    hits = _line_hits(_GRID_LINE, text)
    return hits if len(hits) >= 2 else []


def _line_block_hits(text: str) -> list[tuple[int, str]]:
    """line block — два и более подряд идущих строк с ``| `` и ровно одной чертой."""
    hits: list[tuple[int, str]] = []
    run: list[tuple[int, str]] = []
    for number, line in enumerate(text.splitlines(), start=1):
        if _LINE_BLOCK_LINE.search(line) and line.count("|") == 1:
            run.append((number, line))
            continue
        if len(run) >= 2:
            hits.extend(run)
        run = []
    if len(run) >= 2:
        hits.extend(run)
    return hits


def _toml_hits(text: str) -> list[tuple[int, str]]:
    """TOML metadata: документ начинается с ``+++``."""
    lines = text.splitlines()
    if lines and lines[0].strip().startswith("+++"):
        return [(1, lines[0])]
    return []


def _implicit_figure_hits(text: str) -> list[tuple[int, str]]:
    """implicit figure: одиночная картинка между двумя пустыми строками."""
    lines = text.splitlines()
    hits: list[tuple[int, str]] = []
    for index, line in enumerate(lines):
        if not _IMPLICIT_FIGURE.search(line):
            continue
        if index == 0 or index + 1 >= len(lines):
            continue
        if lines[index - 1].strip() or lines[index + 1].strip():
            continue
        hits.append((index + 1, line))
    return hits


def _census_hits(kind: str, text: str) -> list[tuple[int, str]]:
    """Вхождения одного вида переписи в тексте документа."""
    if kind in _CENSUS_INLINE:
        return _inline_hits(_CENSUS_INLINE[kind], text)
    if kind in _CENSUS_LINE:
        return _line_hits(_CENSUS_LINE[kind], text)
    if kind == "grid_table":
        return _grid_table_hits(text)
    if kind == "line_block":
        return _line_block_hits(text)
    if kind == "toml_metadata":
        return _toml_hits(text)
    if kind == "implicit_figure":
        return _implicit_figure_hits(text)
    raise KeyError(kind)


@dataclass
class Census:
    """Перепись кандидатов: документов, вхождений, до трёх примеров на конструкцию."""

    documents: Counter[str] = field(default_factory=Counter)
    occurrences: Counter[str] = field(default_factory=Counter)
    examples: dict[str, list[str]] = field(default_factory=dict)

    def add(self, url: str, text: str) -> None:
        """Считает все виды по одному документу (сырой текст, не чанки)."""
        for kind in CENSUS_KINDS:
            hits = _census_hits(kind, text)
            if not hits:
                continue
            self.documents[kind] += 1
            self.occurrences[kind] += len(hits)
            bucket = self.examples.setdefault(kind, [])
            for number, line in hits[: max(0, 3 - len(bucket))]:
                bucket.append(f"{url}:{number}: {' '.join(line.split())[:80]}")

    def as_dict(self) -> dict[str, Any]:
        """Машинная форма: все виды, включая нулевые (ноль — измеренный факт)."""
        return {
            kind: {
                "title": CENSUS_TITLES[kind],
                "documents": self.documents.get(kind, 0),
                "occurrences": self.occurrences.get(kind, 0),
                "examples": self.examples.get(kind, []),
            }
            for kind in CENSUS_KINDS
        }


# ---------------------------------------------------------------------------
# Скелетные формы чанков (посылка P4b замера 2026-10-26)
# ---------------------------------------------------------------------------


def _skeleton(text: str) -> str:
    """Скелет текста: пробелы схлопнуты, повторы знаков и числа обезличены.

    Ровно определение замера ``2026-10-26-chunk-content-policy.md`` (``d2c.py``):
    ``\\s+`` → один пробел, ``([^\\w])\\1+`` → один символ, ``\\b(\\d+)\\b`` → ``0``.
    Бланк потому и бланк, что он одинаков в каждом документе: скелет делает
    ``| + |`` любой ширины одним классом, а строку матрицы возможностей — нет.
    """
    collapsed = re.sub(r"\s+", " ", text or "").strip()
    squeezed = re.sub(r"([^\w])\1+", r"\1", collapsed)
    return re.sub(r"\b(\d+)\b", "0", squeezed)


@dataclass
class SkeletonStats:
    """Повторяемость скелетов: группы ≥2/≥3, доля чанков и символов, топ-10 форм."""

    counts: Counter[str] = field(default_factory=Counter)
    chars: Counter[str] = field(default_factory=Counter)
    example: dict[str, str] = field(default_factory=dict)
    chunks: int = 0
    total_chars: int = 0

    def add(self, text: str) -> None:
        """Добавляет чанк: скелет, его символы и первый пример текста."""
        key = _skeleton(text)
        self.counts[key] += 1
        self.chars[key] += len(text)
        self.example.setdefault(key, " ".join(text.split())[:100])
        self.chunks += 1
        self.total_chars += len(text)

    def _group(self, threshold: int) -> dict[str, Any]:
        """Группы скелетов размера ≥ ``threshold``: число групп, чанков, доли."""
        keys = [key for key, count in self.counts.items() if count >= threshold]
        chunks = sum(self.counts[key] for key in keys)
        chars = sum(self.chars[key] for key in keys)
        return {
            "threshold": threshold,
            "groups": len(keys),
            "chunks": chunks,
            "chunk_share": chunks / (self.chunks or 1),
            "char_share": chars / (self.total_chars or 1),
        }

    def summary(self) -> dict[str, Any]:
        """Сводка: уникальные скелеты, группы ≥2/≥3, топ-10 по частоте."""
        return {
            "chunks": self.chunks,
            "total_chars": self.total_chars,
            "unique_skeletons": len(self.counts),
            "groups_ge2": self._group(2),
            "groups_ge3": self._group(3),
            "top": [
                {
                    "skeleton": key,
                    "count": count,
                    "chars": self.chars[key],
                    "example": self.example.get(key, ""),
                }
                for key, count in self.counts.most_common(10)
            ],
        }


@dataclass
class Totals:
    """Сводка по корпусу: счётчики, а не средние — средние по документам обманывают."""

    documents: int = 0
    chars: int = 0
    chunks: int = 0
    p1_violations: int = 0
    p4_violations: int = 0
    p8_violations: int = 0
    source_punct: int = 0
    over_budget: int = 0
    tiny: int = 0
    without_heading: int = 0
    with_context: int = 0
    unknown_lines: int = 0
    boundaries: int = 0
    mid_sentence: int = 0
    constructions: dict[str, int] = field(default_factory=dict)
    worst: list[tuple[int, str]] = field(default_factory=list)

    def add(self, result: DocResult, chunk_size: int, min_chunk_size: int) -> None:
        self.documents += 1
        self.chars += result.chars
        self.chunks += len(result.chunks)
        self.p1_violations += int(result.p1_violation)
        self.p4_violations += int(result.p4_violation)
        self.unknown_lines += result.unknown_lines
        self.boundaries += result.boundaries
        self.mid_sentence += result.mid_sentence
        for key, value in result.constructions.items():
            self.constructions[key] = self.constructions.get(key, 0) + value
        self.p8_violations += result.prose_p8
        self.source_punct += result.source_punct
        for chunk in result.chunks:
            self.over_budget += int(len(chunk.text) > chunk_size)
            self.tiny += int(len(chunk.text.strip()) < min_chunk_size)
            self.without_heading += int(not chunk.heading_path)
            self.with_context += int(chunk.context_prefix != chunk.heading_path)
        if result.p1_violation:
            self.worst.append((0, f"P1: {result.url}"))
        for message in result.examples:
            self.worst.append((1, message))

    @property
    def share(self) -> dict[str, float]:
        chunks = self.chunks or 1
        return {
            "over_budget": self.over_budget / chunks,
            "tiny": self.tiny / chunks,
            "without_heading": self.without_heading / chunks,
            "with_context": self.with_context / chunks,
            "mid_sentence": self.mid_sentence / (self.boundaries or 1),
        }


def _print_report(
    totals: Totals,
    *,
    chunk_size: int,
    min_chunk_size: int,
    profile: str,
    hosts: dict[str, int],
) -> None:
    share = totals.share
    print(f"markdown-чанкер: профиль {profile}, бюджет {chunk_size}, порог {min_chunk_size}")
    print(f"документов: {totals.documents} ({totals.chars} символов)")
    print(f"чанков: {totals.chunks}")
    print("")
    print("инварианты (цель — 0):")
    print(f"  P1  конкатенация ≠ вход      : {totals.p1_violations}")
    print(f"  P4  чанк не подстрока входа   : {totals.p4_violations}")
    print(f"  P8  чанк начинается с пунктуации из-за реза : {totals.p8_violations}")
    print(f"  (источник сам начинается со знака — P1 : {totals.source_punct})")
    print(f"  P13 строки вне блоков         : {totals.unknown_lines}")
    print("")
    print("геометрия (число чанков и доля):")
    print(f"  длиннее бюджета      : {totals.over_budget} ({share['over_budget']:.3%})")
    print(f"  короче порога        : {totals.tiny} ({share['tiny']:.3%})")
    print(f"  без heading_path     : {totals.without_heading} ({share['without_heading']:.3%})")
    print(f"  с контекстом продолж.: {totals.with_context} ({share['with_context']:.3%})")
    print(
        f"  обрыв предложения    : {totals.mid_sentence} из {totals.boundaries} границ "
        f"({share['mid_sentence']:.3%})"
    )
    print("")
    print("конструкции в корпусе:", ", ".join(f"{k}={v}" for k, v in sorted(totals.constructions.items())))
    if hosts:
        top = sorted(hosts.items(), key=lambda item: -item[1])[:10]
        print("документов по хостам:", ", ".join(f"{host}={count}" for host, count in top))
    for _, message in totals.worst[:5]:
        print(f"  ! {message}")


def _fmt_dist(dist: dict[str, Any]) -> str:
    """Распределение одной строкой: ``p50/p90/p99/max`` (``n/a`` — не измерено)."""
    parts = []
    for key in ("p50", "p90", "p99"):
        value = dist.get(key)
        parts.append(f"{key}={value:.1f}" if isinstance(value, float) else f"{key}={value}")
    parts.append(f"max={dist.get('max')}")
    return " ".join(parts)


def _print_token_report(
    summary: dict[str, Any],
    provenance: dict[str, Any],
    resolution: dict[str, Any],
) -> None:
    """Токены на чанк, разрез по письменности и геометрия контекста — в шапке провенанс."""
    print("")
    print("информационные единицы (токенизатор эмбеддера, local_files_only=True):")
    print(
        f"  модель: {summary['model_id']} ({provenance.get('tokenizer_class')}, "
        f"словарь {provenance.get('vocabulary_size')} записей, "
        f"vocab_size={provenance.get('vocab_size')})"
    )
    print(f"  tokenizer.json: {provenance.get('tokenizer_json')}")
    print(f"  sha256: {provenance.get('tokenizer_json_sha256')}")
    if resolution.get("assumed"):
        print(
            "  ! манифест прогона не объявляет embed_model_id; замер идёт против объявленной "
            "по умолчанию модели `BAAI/bge-m3` (core/adapters/embed_local.py:DEFAULT_MODEL_ID) "
            "— это допущение замера, а не свойство прогона"
        )
    elif resolution.get("revision_pinned"):
        print(
            f"  манифест объявляет embed_model_id={resolution.get('declared_embed_model_id')!r}: "
            "тот же словарь, пин ревизии зафиксирован; sha256 словаря указан выше"
        )
    else:
        print(f"  манифест объявляет embed_model_id={resolution.get('declared_embed_model_id')!r}")

    text = summary["text"]
    prompt = summary["prompt"]
    print(f"  чанков: {summary['chunks']}")
    print("  текст (вход эмбеддера, embed_input=('text',)):")
    print(f"    {_fmt_dist(text)}")
    print(
        "    гистограмма: "
        + ", ".join(f"{label}={count}" for label, count in text["histogram"].items())
    )
    rate = text["chars_per_token_median"]
    rate_text = f"{rate:.3f}" if isinstance(rate, float) else "n/a"
    print(
        f"    >512: {text['over_512']} ({text['over_512'] / (summary['chunks'] or 1):.3%})"
        f"   >{EMBED_CONTEXT_WINDOW} (окно bge-m3): {text['over_window']}"
        f"   медиана символов/токен: {rate_text}"
    )
    print("  промпт (нижняя граница: heading_path + context_prefix + text; инструкции промпта не в счёт):")
    print(f"    {_fmt_dist(prompt)}")
    print(
        "    гистограмма: "
        + ", ".join(f"{label}={count}" for label, count in prompt["histogram"].items())
    )
    print(
        f"    >512: {prompt['over_512']} ({prompt['over_512'] / (summary['chunks'] or 1):.3%})"
        f"   >{EMBED_CONTEXT_WINDOW}: {prompt['over_window']}"
    )

    print("  разрез по письменности (доля CJK среди непробельных ≥0.20 → ZH):")
    for name in ("ZH", "LAT"):
        dist = summary["script"].get(name)
        if dist is None:
            print(f"    {name}: чанков 0")
            continue
        rate = dist["chars_per_token_median"]
        rate_text = f"{rate:.3f}" if isinstance(rate, float) else "n/a"
        print(
            f"    {name}: чанков {dist['n']} ({dist['share']:.3%}); {_fmt_dist(dist)}; "
            f"медиана символов/токен: {rate_text}"
        )
    diagnostics = summary["script_diagnostics"]
    print(
        f"    (диагностика: чанков с любым CJK — {diagnostics['chunks_with_cjk']}, "
        f"максимальная доля CJK в чанке — {diagnostics['max_cjk_share']:.4f})"
    )

    context = summary["context"]
    print("  геометрия контекста (символы и токены):")
    print(f"    heading_path:   симв {_fmt_dist(context['heading_chars'])}")
    print(f"                    токенов {_fmt_dist(context['heading_tokens'])}")
    print(f"    context_prefix: симв {_fmt_dist(context['prefix_chars'])}")
    print(f"                    токенов {_fmt_dist(context['prefix_tokens'])}")
    print(f"    heading+context токенов {_fmt_dist(context['context_tokens'])}")
    print(
        f"    токены(heading+context) > {context['heavy_limit']:.0%} токенов текста: "
        f"{context['heavy_chunks']} ({context['heavy_share']:.3%})"
    )


def _print_census_report(census: Census) -> None:
    """Перепись кандидатов: документов, вхождений, пример. Это кандидаты, не вердикт."""
    print("")
    print("перепись КАНДИДАТОВ pandoc-конструкций (регекс по сырому тексту документа):")
    print(f"  {'конструкция':<44}{'документов':>11}{'вхождений':>11}")
    for kind in CENSUS_KINDS:
        print(
            f"  {CENSUS_TITLES[kind]:<44}"
            f"{census.documents.get(kind, 0):>11}{census.occurrences.get(kind, 0):>11}"
        )
    for kind in CENSUS_KINDS:
        examples = census.examples.get(kind, [])
        if not examples:
            continue
        print(f"  примеры [{CENSUS_TITLES[kind]}]:")
        for example in examples:
            print(f"    {example}")
    print(
        "  ! это кандидаты: регекс отвечает «похоже», а не «есть». Решение о грамматике "
        "(L3) принимается по числу и по прочитанным примерам, не по одному регексу."
    )


def _print_skeleton_report(summary: dict[str, Any]) -> None:
    """Скелетные формы: доля чанков/символов в повторяющихся группах и топ-10."""
    print("")
    print("скелетные формы (P4b-посылка: скелет схлопывает пробелы, повторы знаков, числа):")
    print(
        f"  чанков: {summary['chunks']}, уникальных скелетов: {summary['unique_skeletons']}"
    )
    for key in ("groups_ge2", "groups_ge3"):
        group = summary[key]
        print(
            f"  группы ≥{group['threshold']}: {group['groups']} скелетов, "
            f"чанков {group['chunks']} ({group['chunk_share']:.3%}), "
            f"символов {group['char_share']:.3%}"
        )
    print("  топ-10 по частоте:")
    for item in summary["top"]:
        print(f"    {item['count']:>4}× [{item['skeleton'][:80]}] ← {item['example'][:100]!r}")


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="measure_md_chunking.py",
        description=(
            "Приёмка markdown-чанкера на сохранённом корпусе (read-only): "
            "инварианты P1/P4/P8/P13 и геометрия нарезки."
        ),
        epilog=(
            "Примеры:\n"
            "  python3 scripts/measure_md_chunking.py --db runs/{run_id}/cohound.db\n"
            "  python3 scripts/measure_md_chunking.py --db runs/{run_id}/cohound.db \\\n"
            "      --host docs.example.org --limit 50 --chunk-size 800 --out-json /tmp/m.json\n"
            "\n"
            "--tokens требует локального кэша модели (сеть запрещена): без него инструмент\n"
            "работает, но информационные единицы не измеряет — прокси не подставляется.\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", required=True, help="cohound.db прогона (только чтение)")
    parser.add_argument(
        "--host", action="append", default=[], metavar="SUBSTR",
        help="фильтр по URL, подстрока (можно повторять)",
    )
    parser.add_argument("--limit", type=int, default=300, help="страниц на хост (дефолт 300)")
    parser.add_argument("--chunk-size", type=int, default=1500, help="бюджет куска")
    parser.add_argument("--min-chunk-size", type=int, default=MIN_CHUNK_SIZE, help="порог хвоста")
    parser.add_argument(
        "--profile", choices=("commonmark", "gfm", "pandoc"), default="gfm",
        help="профиль грамматики (дефолт gfm — как у страниц и офисных документов)",
    )
    parser.add_argument(
        "--tokens",
        action="store_true",
        help="считать токены тем же токенизатором, что у эмбеддера (дефолт: выкл)",
    )
    parser.add_argument(
        "--tokenizer-model",
        default=None,
        metavar="MODEL_ID",
        help=(
            "модель токенизатора; по умолчанию DEFAULT_MODEL_ID из "
            "core/adapters/embed_local.py (BAAI/bge-m3)"
        ),
    )
    parser.add_argument(
        "--census",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="перепись кандидатов pandoc-конструкций по сырому тексту (дефолт: вкл)",
    )
    parser.add_argument(
        "--skeleton",
        action="store_true",
        help="скелетные формы чанков: посылка P4b на текущем наборе (дефолт: выкл)",
    )
    parser.add_argument("--out-json", metavar="PATH", help="куда записать машинный отчёт")
    return parser.parse_args(argv)


def _default_tokenizer_model() -> str:
    """Объявленный дефолт эмбеддера — из самого адаптера, а не копия строки."""
    from core.adapters.embed_local import DEFAULT_MODEL_ID  # noqa: PLC0415

    return DEFAULT_MODEL_ID


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.limit < 1:
        print("measure: --limit должен быть ≥ 1", file=sys.stderr)
        return 2
    if not Path(args.db).expanduser().is_file():
        print(f"measure: БД не найдена: {args.db}", file=sys.stderr)
        return 2

    extractor = HtmlExtractor()
    if not extractor.available:
        print("measure: trafilatura не установлена (extra 'extract')", file=sys.stderr)
        return 2

    base = _mhe._raw_base()
    conn = _mhe._ro_connect(args.db)
    try:
        declared_model = _declared_embed_model(conn) if args.tokens else None
        items, skipped_no_raw, skipped_non_html = _mhe._load_items(conn, args.host, args.limit)
    finally:
        conn.close()

    # Токенизатор грузится ДО обхода корпуса: отказ должен быть виден за секунду, а не
    # после двух минут извлечения, и он не имеет права превратиться в символьный прокси.
    tokenizer: Any = None
    counter: Callable[[str], int] | None = None
    resolution: dict[str, Any] = {}
    provenance: dict[str, Any] = {}
    geometry: TokenGeometry | None = None
    if args.tokens:
        requested = args.tokenizer_model or _default_tokenizer_model()
        try:
            resolution = _resolve_tokenizer_model(declared_model, requested)
            tokenizer = _load_tokenizer(requested)
        except (TokenizerModelMismatch, TokenizerUnavailable) as exc:
            print(f"measure: {exc}", file=sys.stderr)
            return 2
        provenance = _tokenizer_provenance(requested, tokenizer)
        geometry = TokenGeometry(model_id=requested)
        counter = partial(_count_tokens, tokenizer)

    chunker = MarkdownChunker(chunk_size=args.chunk_size, min_chunk_size=args.min_chunk_size, profile=args.profile)
    totals = Totals()
    census = Census()
    skeletons = SkeletonStats()
    hosts: dict[str, int] = {}
    skipped_text, skipped_error = 0, 0

    for item in items:
        path = _mhe._resolve_raw(item.raw_path, base)
        if path is None:
            skipped_no_raw += 1
            continue
        page = RawPage(url=item.url, content=path.read_bytes(), fetched_at="", content_type=item.content_type)
        extracted = extractor.extract(page)
        if extracted.error or not extracted.text:
            skipped_error += 1
            continue
        # HTML/офисные документы идут в markdown-чанкер (``FamilyChunker`` выбирает
        # абзацный только для семейства ``text``), поэтому пропускается ровно оно.
        if extracted.family == FAMILY_TEXT:
            skipped_text += 1
            continue
        result = _measure(item.url, extracted.text, chunker)
        totals.add(result, args.chunk_size, args.min_chunk_size)
        hosts[result.host] = hosts.get(result.host, 0) + 1
        # Перепись идёт по сырому тексту документа, а не по чанкам: вопрос «есть ли в
        # корпусе такая конструкция» не зависит от того, как её нарезали.
        if args.census:
            census.add(item.url, extracted.text)
        if geometry is not None and counter is not None:
            for chunk in result.chunks:
                geometry.add(chunk, counter)
        if args.skeleton:
            for chunk in result.chunks:
                skeletons.add(chunk.text)

    _print_report(
        totals,
        chunk_size=args.chunk_size,
        min_chunk_size=args.min_chunk_size,
        profile=args.profile,
        hosts=hosts,
    )
    token_summary: dict[str, Any] | None = None
    if geometry is not None:
        token_summary = geometry.summary()
        _print_token_report(token_summary, provenance, resolution)
    if args.census:
        _print_census_report(census)
    skeleton_summary: dict[str, Any] | None = None
    if args.skeleton:
        skeleton_summary = skeletons.summary()
        _print_skeleton_report(skeleton_summary)
    print("")
    print(
        f"пропущено: без сырья={skipped_no_raw}, не-HTML={skipped_non_html}, "
        f"ошибка извлечения={skipped_error}, семейство text={skipped_text}"
    )

    if args.out_json:
        payload: dict[str, Any] = {
            "profile": args.profile,
            "chunk_size": args.chunk_size,
            "min_chunk_size": args.min_chunk_size,
            "documents": totals.documents,
            "chars": totals.chars,
            "chunks": totals.chunks,
            "invariants": {
                "p1_violations": totals.p1_violations,
                "p4_violations": totals.p4_violations,
                "p8_violations": totals.p8_violations,
                "source_punct": totals.source_punct,
                "unknown_lines": totals.unknown_lines,
            },
            "geometry": {
                "over_budget": totals.over_budget,
                "tiny": totals.tiny,
                "without_heading": totals.without_heading,
                "with_context": totals.with_context,
                "mid_sentence": totals.mid_sentence,
                "boundaries": totals.boundaries,
            },
            "share": totals.share,
            "constructions": totals.constructions,
            "hosts": hosts,
            "skipped": {
                "no_raw": skipped_no_raw,
                "non_html": skipped_non_html,
                "extract_error": skipped_error,
                "text_family": skipped_text,
            },
            "census": census.as_dict() if args.census else None,
            "skeleton": skeleton_summary,
            "tokens": token_summary,
            "tokenizer": (
                {**provenance, **resolution} if token_summary is not None else None
            ),
        }
        target = Path(args.out_json).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(f"json: {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())