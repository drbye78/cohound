"""Диспетчер чанкеров по семейству формата — композит ``ChunkerPort``.

Текстовые форматы — первый класс (решение владельца): у plain text своя единица смысла
(абзац), у markdown/HTML/EPUB — заголовок. Поэтому в composition root строится **один**
порт-чанкер, который выбирает реализацию по ``ExtractedDoc.family``:

* ``"text"`` → :class:`core.adapters.chunker_text.TextChunker` (абзацы/предложения);
* ``"markdown"``/``"epub"``/``None`` → :class:`core.adapters.chunker_markdown.MarkdownChunker`
  (заголовки; ``None`` — HTML и docling, оба отдают markdown).

**``chunker_version`` — композит, а не версия одного чанкера.** Поле
``chunker_version`` входит в ``EXECUTION_CONTRACT_FIELDS``
(:mod:`core.pipeline.digests`) и в ``partition_build_id``/``index_build_id``: с появлением
второго чанкера набор чанков определяется **парой** правил, поэтому объявленная версия —
``MarkdownChunker.version + "+" + TextChunker.version``
(:func:`composite_chunker_version`). Markdown-компонент сам является стеком (грамматика +
движок разбора + политика), поэтому подмена зависимости меняет ``run_id``. Это версионная
граница класса ``execution_versions`` (``execution_change``, §13): меняется ``run_id``,
методология (``METHODOLOGY_VERSION``) — нет.

**Токенный ограничитель — надстройка, а не второй чанкер (O21).** Семейный чанкер режет по
структуре и считает бюджет в **символах**; сколько из этого получится токенов, зависит от
алфавита (замер: 39.888 % plain-text чанков длиннее 512 токенов при символьном бюджете,
``docs/reports/measurements/2026-11-04-text-chunker-exact-slices.md``). Поэтому поверх
результата применяется :class:`core.adapters.token_limit.TokenLimiter`: он **делит**
слишком длинные чанки, не меняя того, что задаёт структуру. Условие объявлено модульной
константой :data:`TOKEN_LIMIT` и попадает и в ``chunker_version`` (суффикс), и в
``chunking_spec`` (ключи ``token_limit*``) — но только когда ограничитель объявлен:
``None`` — «условие не объявлено», и тогда ни версия, ни спека не меняются, иначе
включение/выключение меняло бы ``run_id``, не меняя корпуса. Применение всегда видно в
логе: молчаливая правка состава корпуса — тот класс дефектов, из-за которого
``chunking_spec`` и появился.

Нейтрален (§1.3).
"""

from __future__ import annotations

import logging

from core.adapters.chunker_markdown import MarkdownChunker, MarkdownChunkerPort
from core.adapters.chunker_text import TextChunker, TextChunkerPort
from core.adapters.token_limit import (
    TOKEN_LIMIT_POLICY,
    HfTokenCounter,
    TokenLimiter,
)
from core.ports.types import Chunk, ExtractedDoc
from core.util.document_formats import FAMILY_TEXT
from core.util.embed_input import EMBED_INPUT_FIELDS as _EMBED_INPUT_FIELDS
from core.util.embed_input import embed_input_text

__all__ = [
    "EMBED_INPUT_FIELDS",
    "TOKEN_LIMIT",
    "FamilyChunker",
    "composite_chunker_version",
    "composite_chunking_spec",
    "declared_token_limiter",
    "embed_input_text",
]

_log = logging.getLogger(__name__)


#: Объявленный токенный ограничитель: ``(лимит в токенах, "model_id@revision")`` или
#: ``None``.
#:
#: Лимит — **условие измерения** (гранулярность корпуса), а не потолок безопасности:
#: он меняет состав чанков, значит меняет ``run_id`` и требует пере-эмбеддинга, а не
#: «защищает» от чего-то. Поэтому он объявлен здесь, рядом с версией чанкера, а не
#: приходит флагом из окружения.
#:
#: ``None`` — ограничитель не применяется, и в :func:`composite_chunking_spec` ключей
#: ограничителя **нет**: иначе включение и выключение меняли бы ``run_id``, не меняя
#: корпуса (``None`` — «условие не объявлено», а не «объявлено выключенным»).
#:
#: ``model_id@revision`` — та же конвенция, что ``embed_spec`` (O6): веса токенизатора
#: задают разбиение на подслова, то есть само число токенов.
TOKEN_LIMIT: tuple[int, str] | None = None

def declared_token_limiter() -> TokenLimiter | None:
    """Ограничитель, объявленный :data:`TOKEN_LIMIT`, либо ``None``.

    Единственная точка, где объявление превращается в исполнение: composition root зовёт
    её, а не собирает ограничитель сам — иначе объявленный лимит и фактический счётчик
    могли бы разъехаться. ``None`` не строит ничего (никакого импорта ``transformers`` на
    пути пайплайна).
    """
    if TOKEN_LIMIT is None:
        return None
    limit, spec = TOKEN_LIMIT
    model_id, _, revision = spec.rpartition("@")
    if not model_id:  # ревизия не объявлена — пин не выдумываем
        model_id, revision = spec, ""
    return TokenLimiter(HfTokenCounter(model_id, revision or None), limit)


#: Из каких полей чанка собирается текст, уходящий в эмбеддер — реэкспорт из
#: :mod:`core.util.embed_input`. Живёт там, а не здесь, потому что исполняет состав слой
#: ``pipeline`` (``core.pipeline.corpus_index``), которому импорт адаптера запрещён
#: матрицей зависимостей (§4.1). Здесь — только объявление в ``chunking_spec``.
EMBED_INPUT_FIELDS: tuple[str, ...] = _EMBED_INPUT_FIELDS


def composite_chunking_spec() -> dict[str, object]:
    """Геометрия нарезки и состав входа эмбеддера — объявленная часть контракта (C3).

    **Зачем это отдельно от ``chunker_version``.** ``chunker_version`` — метка-строка
    (композит версий чанкеров), а не геометрия. Правка ``chunk_size`` на
    другое число давала другой корпус чанков при том же ``run_id``: тот же класс дефекта,
    за который проект ругает себя в ``embed_model_id``. Здесь геометрия **читается из
    самих чанкеров** (не из копии констант), поэтому разъехаться с фактической нарезкой
    она не может: поменяли дефолт в чанкере — поменялся контракт.

    Значения — только объявленные дефолты конструкторов. Свойства конкретного прогона
    (наблюдаемые размеры чанков) остаются провенансом и в контракт не входят.

    **Ключи ограничителя условны, и это осознанно.** Пока :data:`TOKEN_LIMIT` равен
    ``None``, ключей ``token_limit*`` в спеке **нет**: ограничитель не применяется, корпус
    прежний, и появление ключа меняло бы ``run_id`` без изменения корпуса — ложный
    ``execution_change``. Когда ограничитель объявлен, ключи появляются все три: лимит,
    модель счётчика (``model_id@revision`` — веса задают разбиение на подслова) и версия
    правил ограничителя.
    """
    markdown = MarkdownChunkerPort()
    text = TextChunkerPort()
    # Версии правил сюда **не** входят: их несёт ``chunker_version`` (композит
    # ``"md-...+text-..."``). Дублировать их значило бы получить две оси, меняющиеся
    # вместе, и потерять различение «изменились правила нарезки» vs «изменилась
    # геометрия» — а это разные вопросы при разборе расхождения прогонов.
    spec: dict[str, object] = {
        "embed_input": list(EMBED_INPUT_FIELDS),
        "markdown": {
            "chunk_size": getattr(markdown, "chunk_size", None),
            "chunk_overlap": getattr(markdown, "chunk_overlap", None),
            "min_chunk_size": getattr(markdown, "min_chunk_size", None),
            # Профиль грамматики — условие измерения, а не деталь реализации: ``gfm`` и
            # ``pandoc`` дают разные блоки на одном тексте (таблицы, сноски, формулы).
            "profile": getattr(markdown, "profile", None),
        },
        "text": {
            "chunk_size": getattr(text, "chunk_size", None),
            "min_chunk_size": getattr(text, "min_chunk_size", None),
        },
    }
    if TOKEN_LIMIT is not None:
        # Лимит в токенах — геометрия другой единицы: символьные бюджеты выше его не
        # описывают (сколько токенов в символе — свойство алфавита).
        limit, counter_spec = TOKEN_LIMIT
        spec["token_limit"] = limit
        spec["token_limit_model"] = counter_spec
        spec["token_limit_policy"] = TOKEN_LIMIT_POLICY
    return spec

def composite_chunker_version() -> str:
    """Объявленная ``chunker_version``: версия markdown-чанкера + версия текстового.

    Единая формула для декларации (:mod:`core.cli.execution_axes`) и для самого композита
    (:attr:`FamilyChunker.version`): два разных источника разошлись бы с фактической
    нарезкой и сломали бы воспроизводимость контракта (R7 §11.7).

    При объявленном ограничителе добавляется суффикс ``+token-limit-1:<limit>``: правила
    ограничителя меняют состав чанков, то есть это ``execution_change``, и версия обязана
    его называть. При ``None`` строка прежняя — байт-в-байт, чтобы выключенный ограничитель
    не менял ``run_id``.
    """
    base = f"{MarkdownChunker.version}+{TextChunker.version}"
    if TOKEN_LIMIT is None:
        return base
    return f"{base}+{TOKEN_LIMIT_POLICY}:{TOKEN_LIMIT[0]}"


class FamilyChunker:
    """``ChunkerPort``-композит: семейство ``text`` → абзацный чанкер, иначе markdown.

    Реализует порт структурно. ``markdown``/``text`` — точки подмены (тесты, AB); по
    умолчанию собираются порт-обёртки, поэтому сигнатура ``chunk(doc, document_id)``
    одна.

    :param token_limiter: токенный ограничитель поверх структурной нарезки. Обязан
        соответствовать объявленному :data:`TOKEN_LIMIT`: объявленный лимит без счётчика —
        это тихо неисполненное условие измерения, поэтому ``ValueError``, а не «настройка
        по умолчанию». Обратный случай (счётчик без объявления) разрешён, но **виден**
        предупреждением: состав корпуса тогда меняется, а ``run_id`` — нет.
    """

    def __init__(
        self,
        markdown: MarkdownChunkerPort | None = None,
        text: TextChunkerPort | None = None,
        token_limiter: TokenLimiter | None = None,
    ) -> None:
        self._markdown = markdown if markdown is not None else MarkdownChunkerPort()
        self._text = text if text is not None else TextChunkerPort()
        if token_limiter is None and TOKEN_LIMIT is not None:
            raise ValueError(
                "TOKEN_LIMIT объявлен, но ограничитель не передан: объявленное условие "
                "измерения осталось бы неисполненным. Передайте "
                "declared_token_limiter() в FamilyChunker(token_limiter=...)."
            )
        if token_limiter is not None and TOKEN_LIMIT is None:
            # Явная инъекция при необъявленном условии: куски изменятся, а версия и
            # ``chunking_spec`` этого не назовут. Не отказ (тесты/AB), но видимое
            # предупреждение — молчаливой правки состава корпуса быть не должно.
            _log.warning(
                "token_limit_undeclared: ограничитель передан явно, но TOKEN_LIMIT=None — "
                "состав чанков изменится под прежним chunker_version/chunking_spec"
            )
        self._token_limiter = token_limiter

    @property
    def version(self) -> str:
        """Композитная версия правил нарезки (:func:`composite_chunker_version`)."""
        return composite_chunker_version()

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        """``ExtractedDoc`` → чанки тем чанкером, чьё семейство указал экстрактор.

        Поверх результата применяется токенный ограничитель (если объявлен); отчёт
        пишется в лог одной строкой — применение не бывает молчаливым.
        """
        chunker = (
            self._text
            if getattr(doc, "family", None) == FAMILY_TEXT
            else self._markdown
        )
        chunks = chunker.chunk(doc, document_id)
        if self._token_limiter is None:
            return chunks
        limited, report = self._token_limiter.apply(chunks)
        _log.info(
            "token_limit: document=%s chunks_in=%d chunks_out=%d split=%d "
            "absorbed_tails=%d unsplittable=%d over_limit=%d max_tokens_after=%d",
            document_id,
            report.chunks_in,
            report.chunks_out,
            report.split_chunks,
            report.absorbed_tails,
            report.unsplittable,
            report.over_limit,
            report.max_tokens_after,
        )
        return limited

