"""Лексический индекс на tantivy — реализация ``LexicalIndexPort`` (W8/T8.3, §3.2).

Роль в coHound: лексическое плечо ретривала (профиль **R0**, 02 §B.3). В отличие от
``lexical_bm25.py`` (in-process baseline, test double), здесь используется настоящий
поисковый движок с BM25 и нативными RU/EN-анализаторами (проверка стека **V1**,
05 §6: ``Filter.stemmer("russian")``/``Filter.stopword("russian")`` доступны в
tantivy 0.26).

**Двойной профиль (§B.3 / T8.3).** Индекс хранит поля:

* ``body_stem_ru`` — RU-анализатор: lowercase → russian stemmer → russian stopword;
* ``body_stem_en`` — EN-анализатор: lowercase → english stemmer → english stopword;
* ``body_identifier`` — identifier-профиль: whitespace-токенизация **без** lowercase
  и стемминга, поэтому точные идентификаторы (``API-2024_X``) различают регистр и
  сохраняют пунктуацию/подчёркивания;
* ``body_ngram`` — ngram-профиль: символьные n-граммы (``Tokenizer.ngram``,
  ``min_gram=3``, ``max_gram=3``) по lowercase-тексту, для fuzzy/подстрочного
  совпадения по фрагменту идентификатора (``"2024"`` находит ``API-2024_X``).

Профиль выбирается параметром ``profile`` конструктора: ``"stemmed"`` (по умолчанию,
RU/EN), ``"identifier"`` (псевдоним ``"raw"``) либо ``"ngram"``. Методы
:meth:`search_identifier`/:meth:`search_ngram` позволяют выполнить поиск по
соответствующему профилю независимо от выбранного при конструировании.

**Границы ngram-профиля (честно).** Это **не** лексический fuzzy по расстоянию
редактирования (опечатки/Левенштейн) и не ngram-per-token: ``Tokenizer.ngram`` в
tantivy 0.26 нарезает n-граммы по всему тексту поля, включая пробелы и пунктуацию.
Профиль даёт подстрочный матч по общим n-граммам, поэтому, как и любой ngram-поиск,
может давать паразитные совпадения по общим граммам; короткие запросы короче
``min_gram`` не находят ничего. Параметры min/max gram зафиксированы детерминированно;
их абляция — отдельная задача R9/R10.

**Продукт-скоупед (I17).** Поиск ограничен переданными ``product_ids``: и на уровне
запроса (term-set по ``product_id``), и защитной пост-фильтрацией. Пустой список
``product_ids`` даёт пустой результат — ретривал не выходит за явно запрошенный скоуп.

**Детерминизм (R7).** Результаты сортируются по ``(-score, chunk_id)``.

**Ленивый импорт.** ``tantivy`` импортируется только в реальном пути; модуль
импортируется и конструируется без него. Недоступность зависимости поднимает
``LexicalIndexUnavailable`` (подкласс ``RuntimeError``) — адаптер **не** подменяет
отсутствие движка пустыми результатами.

Происхождение идеи ленивого шва и ``available`` — ``adapters/embed_local.py``
(локальная практика тестирования адаптеров coHound); сторонний код не копировался.
Нейтрален (§1.3).
"""

from __future__ import annotations

import importlib.util
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.ports.vector_index import RetrievedChunk

__all__ = [
    "LexicalIndexUnavailable",
    "LexicalChunk",
    "TantivyLexicalIndex",
    "tantivy_available",
    "tantivy_engine_version",
]

#: Имена полей схемы (держатся в одном месте).
FIELD_CHUNK_ID = "chunk_id"
FIELD_PRODUCT = "product_id"
FIELD_STEM_RU = "body_stem_ru"
FIELD_STEM_EN = "body_stem_en"
FIELD_IDENTIFIER = "body_identifier"
FIELD_NGRAM = "body_ngram"

_RU_TOKENIZER = "cohound_ru"
_EN_TOKENIZER = "cohound_en"
_NGRAM_TOKENIZER = "cohound_ngram"

#: Границы n-грамм ngram-профиля (символы). Фиксированы детерминированно; триграмма —
#: компромисс между подстрочной чувствительностью и объёмом инвертированного индекса.
_NGRAM_MIN = 3
_NGRAM_MAX = 3

#: Допустимые профили поиска (``"raw"`` — псевдоним identifier-профиля).
_PROFILES = {"stemmed", "identifier", "raw", "ngram"}

#: Поля, обязательные в индексируемой записи.
_REQUIRED_FIELDS = ("chunk_id", "product_id", "text")


def tantivy_available() -> bool:
    """Установлен ли ``tantivy`` (проверка без импорта тяжёлого движка)."""
    return importlib.util.find_spec("tantivy") is not None


def tantivy_engine_version() -> str:
    """Версия нативного движка (``tantivy-<ver>``) для провенанса сборки (§13:676).

    Значение входит в ``partition_build_id`` и в манифест партиции, поэтому оно
    непусто и стабильно в любом состоянии: ``"tantivy-unavailable"`` — зависимость
    не установлена, ``"tantivy-unknown"`` — установлена, но версия не распознана.
    Читается без конструирования индекса (никаких временных каталогов).
    """
    if not tantivy_available():
        return "tantivy-unavailable"
    import tantivy

    raw = str(getattr(tantivy, "__version__", ""))
    # ``__version__`` вида "tantivy v0.26.2, index_format v7" → "0.26.2".
    token = next((p for p in raw.replace(",", " ").split() if any(ch.isdigit() for ch in p)), "")
    version = token.lstrip("vV")
    return f"tantivy-{version}" if version else "tantivy-unknown"


class LexicalIndexUnavailable(RuntimeError):
    """Движок tantivy недоступен: нет зависимости или не построен индекс."""


@dataclass(frozen=True)
class LexicalChunk:
    """Индексируемая запись лексического контура.

    ``ports.types.Chunk`` не несёт ``product_id``, а ``RetrievedChunk`` требует
    провенанс (``content_hash``/``build_id``/``heading_path``), поэтому адаптер
    принимает собственный record-тип. Допускаются также ``Mapping`` и любые объекты
    с одноимёнными атрибутами (например, ``lexical_bm25.LexicalDoc``) — они
    приводятся к :class:`LexicalChunk` функцией :func:`_coerce_record`.
    """

    chunk_id: str
    product_id: str
    text: str
    heading_path: str = ""
    content_hash: str = ""
    build_id: str = ""


def _coerce_record(record: Any) -> LexicalChunk:
    """Приводит ``Mapping``/объект/запись к :class:`LexicalChunk`.

    :raises ValueError: если отсутствует обязательное поле (``chunk_id``/``product_id``/``text``).
    """
    if isinstance(record, LexicalChunk):
        return record
    if isinstance(record, Mapping):
        values = {name: record.get(name, "") for name in LexicalChunk.__dataclass_fields__}
    else:
        values = {
            name: getattr(record, name, "")
            for name in LexicalChunk.__dataclass_fields__
        }
    for name in _REQUIRED_FIELDS:
        if not values.get(name):
            raise ValueError(
                f"запись лексического индекса: отсутствует обязательное поле {name!r}"
            )
    return LexicalChunk(
        chunk_id=str(values["chunk_id"]),
        product_id=str(values["product_id"]),
        text=str(values["text"]),
        heading_path=str(values.get("heading_path") or ""),
        content_hash=str(values.get("content_hash") or ""),
        build_id=str(values.get("build_id") or ""),
    )


class TantivyLexicalIndex:
    """BM25-поиск на tantivy с RU/EN-стеммингом, identifier- и ngram-профилями (§3.2, I16/I17).

    :param index_path: каталог индекса; ``None`` → временный каталог (авто-удаление).
    :param profile: ``"stemmed"`` (RU/EN, по умолчанию), ``"identifier"``/``"raw"``,
        ``"ngram"`` (символьные триграммы для подстрочного/fuzzy-совпадения).

    Атрибуты ``engine_version``/``build_id`` доступны до построения индекса;
    ``build_id`` заполняется при :meth:`index` (из параметра либо из записей).
    """

    def __init__(
        self,
        index_path: str | Path | None = None,
        *,
        profile: str = "stemmed",
    ) -> None:
        normalized = profile.strip().lower()
        if normalized not in _PROFILES:
            raise ValueError(
                f"неизвестный профиль {profile!r} (ожидается stemmed|identifier|raw|ngram)"
            )
        self.profile = normalized
        self.build_id: str = ""
        self._owns_path = index_path is None
        self._tmp: tempfile.TemporaryDirectory[str] | None = None
        self._path: Path = self._resolve_path(index_path)
        self._schema: Any = None
        self._index: Any = None
        self._searcher: Any = None
        self._records_by_id: dict[str, LexicalChunk] = {}
        self._built = False

    # -- зависимость ---------------------------------------------------- #

    @property
    def available(self) -> bool:
        """Готов ли движок (установлен ли ``tantivy``)."""
        return tantivy_available()

    @property
    def engine_version(self) -> str:
        """Версия нативного движка (для манифеста/провенанса, §13)."""
        return tantivy_engine_version()

    # -- build ---------------------------------------------------------- #

    def index(
        self,
        chunks: Sequence[Any],
        *,
        build_id: str | None = None,
    ) -> None:
        """Строит лексический индекс по записям (полная перестройка).

        :param chunks: последовательность :class:`LexicalChunk`/``Mapping``/объектов.
        :param build_id: идентификатор сборки; ``None`` → берётся из записей.
        :raises LexicalIndexUnavailable: если ``tantivy`` не установлен.
        :raises ValueError: дубли ``chunk_id`` или отсутствие обязательных полей.
        """
        records = [_coerce_record(record) for record in chunks]
        mapping: dict[str, LexicalChunk] = {}
        for record in records:
            if record.chunk_id in mapping:
                raise ValueError(f"дублирующийся chunk_id в индексе: {record.chunk_id!r}")
            mapping[record.chunk_id] = record

        self._records_by_id = mapping
        self.build_id = build_id if build_id is not None else _derive_build_id(records)

        tantivy = _require_tantivy()
        schema = self._build_schema(tantivy)
        index = self._open_index(tantivy, schema)
        writer = index.writer()
        writer.delete_all_documents()
        for record in records:
            writer.add_document(
                tantivy.Document(
                    **{
                        FIELD_CHUNK_ID: record.chunk_id,
                        FIELD_PRODUCT: record.product_id,
                        FIELD_STEM_RU: record.text,
                        FIELD_STEM_EN: record.text,
                        FIELD_IDENTIFIER: record.text,
                        FIELD_NGRAM: record.text,
                    }
                )
            )
        writer.commit()
        index.reload()

        self._schema = schema
        self._index = index
        self._searcher = index.searcher()
        self._built = True

    # -- search (LexicalIndexPort.shape) -------------------------------- #

    def search(
        self,
        query: str,
        product_ids: list[str],
        top_k: int,
        lang: str | None = None,
    ) -> list[RetrievedChunk]:
        """BM25-поиск, продукт-скоупед (I17), с детерминированным tie-break (R7).

        :param lang: ``"ru"``/``"en"`` выбирает анализатор языка (свой стеммер/стоп-слова);
            ``None`` ищет по обоим полям. Это не языковой firewall: стеммер чужого
            скрипта может пропустить токен дословно.
        """
        if self.profile == "stemmed":
            return self._search(query, product_ids, top_k, profile="stemmed", lang=lang)
        if self.profile == "ngram":
            return self._search(query, product_ids, top_k, profile="ngram", lang=None)
        return self._search(query, product_ids, top_k, profile="identifier", lang=None)

    def search_identifier(
        self,
        query: str,
        product_ids: list[str],
        top_k: int,
    ) -> list[RetrievedChunk]:
        """Поиск по identifier-профилю независимо от ``profile`` конструктора."""
        return self._search(query, product_ids, top_k, profile="identifier", lang=None)

    def search_ngram(
        self,
        query: str,
        product_ids: list[str],
        top_k: int,
    ) -> list[RetrievedChunk]:
        """Поиск по ngram-профилю независимо от ``profile`` конструктора."""
        return self._search(query, product_ids, top_k, profile="ngram", lang=None)

    # ------------------------------------------------------------------ #
    # Внутреннее
    # ------------------------------------------------------------------ #

    def _resolve_path(self, index_path: str | Path | None) -> Path:
        if index_path is None:
            self._tmp = tempfile.TemporaryDirectory(prefix="cohound-tantivy-")
            return Path(self._tmp.name)
        path = Path(index_path)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _build_schema(self, tantivy: Any) -> Any:
        builder = tantivy.SchemaBuilder()
        builder.add_text_field(FIELD_CHUNK_ID, stored=True, tokenizer_name="raw")
        builder.add_text_field(FIELD_PRODUCT, tokenizer_name="raw")
        builder.add_text_field(FIELD_STEM_RU, tokenizer_name=_RU_TOKENIZER)
        builder.add_text_field(FIELD_STEM_EN, tokenizer_name=_EN_TOKENIZER)
        builder.add_text_field(FIELD_IDENTIFIER, tokenizer_name="whitespace")
        builder.add_text_field(FIELD_NGRAM, tokenizer_name=_NGRAM_TOKENIZER)
        return builder.build()

    def _open_index(self, tantivy: Any, schema: Any) -> Any:
        index = tantivy.Index(schema, path=str(self._path))
        index.register_tokenizer(_RU_TOKENIZER, _analyzer(tantivy, "russian"))
        index.register_tokenizer(_EN_TOKENIZER, _analyzer(tantivy, "english"))
        index.register_tokenizer(_NGRAM_TOKENIZER, _ngram_analyzer(tantivy))
        return index

    def _search(
        self,
        query: str,
        product_ids: Sequence[str],
        top_k: int,
        *,
        profile: str,
        lang: str | None,
    ) -> list[RetrievedChunk]:
        if not self._built or self._searcher is None:
            raise LexicalIndexUnavailable(
                "TantivyLexicalIndex: перед поиском вызовите index()"
            )
        if top_k <= 0:
            return []
        allowed = list(dict.fromkeys(str(pid) for pid in product_ids))
        if not allowed:
            return []
        base = self._query_for_profile(query, profile=profile, lang=lang)
        if base is None:
            return []

        tantivy = _require_tantivy()
        product_filter = tantivy.Query.term_set_query(self._schema, FIELD_PRODUCT, allowed)
        combined = tantivy.Query.boolean_query(
            [(tantivy.Occur.Must, base), (tantivy.Occur.Must, product_filter)]
        )
        hits = self._searcher.search(combined, max(1, top_k))

        allowed_set = set(allowed)
        out: list[RetrievedChunk] = []
        for score, address in hits.hits:
            document = self._searcher.doc(address)
            chunk_id = document.get_first(FIELD_CHUNK_ID)
            record = self._records_by_id.get(chunk_id)
            if record is None or record.product_id not in allowed_set:
                continue
            out.append(
                RetrievedChunk(
                    chunk_id=record.chunk_id,
                    product_id=record.product_id,
                    text=record.text,
                    content_hash=record.content_hash,
                    heading_path=record.heading_path,
                    build_id=record.build_id or self.build_id,
                    score=float(score),
                )
            )
        out.sort(key=lambda chunk: (-chunk.score, chunk.chunk_id))
        return out[:top_k]

    def _query_for_profile(self, query: str, *, profile: str, lang: str | None) -> Any:
        """Строит разобранный запрос под профиль (``None`` — пустой запрос)."""
        if profile == "identifier":
            return self._identifier_query(query)
        if profile == "ngram":
            return self._ngram_query(query)
        return self._stemmed_query(query, lang)

    def _stemmed_query(self, query: str, lang: str | None) -> Any:
        if not query or not query.strip():
            return None
        language = (lang or "").strip().lower()
        if language == "ru":
            fields = [FIELD_STEM_RU]
        elif language == "en":
            fields = [FIELD_STEM_EN]
        else:
            fields = [FIELD_STEM_RU, FIELD_STEM_EN]
        # lenient: невалидные фрагменты превращаются в match-nothing, а не в исключение.
        parsed, _errors = self._index.parse_query_lenient(query, default_field_names=fields)
        return parsed

    def _identifier_query(self, query: str) -> Any:
        tokens = [token for token in (query or "").split() if token]
        if not tokens:
            return None
        tantivy = _require_tantivy()
        clauses = [
            (
                tantivy.Occur.Should,
                tantivy.Query.term_query(self._schema, FIELD_IDENTIFIER, token),
            )
            for token in tokens
        ]
        return tantivy.Query.boolean_query(clauses, 1)

    def _ngram_query(self, query: str) -> Any:
        """Запрос по ngram-полю: тот же анализатор, что при индексации.

        Парсер сам нарезает строку запроса на n-граммы анализатором поля (триграммы)
        и объединяет их дизъюнкцией (tantivy query parser, дефолт OR), поэтому
        совпадение по фрагменту токена/идентификатора даёт ненулевой BM25-скор.
        """
        if not query or not query.strip():
            return None
        parsed, _errors = self._index.parse_query_lenient(
            query, default_field_names=[FIELD_NGRAM]
        )
        return parsed

    def close(self) -> None:
        """Освобождает временный каталог индекса (если он принадлежит адаптеру)."""
        if self._tmp is not None:
            self._tmp.cleanup()
            self._tmp = None
        self._index = None
        self._searcher = None
        self._built = False

    def __enter__(self) -> TantivyLexicalIndex:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def _derive_build_id(records: Sequence[LexicalChunk]) -> str:
    """Берёт первый непустой ``build_id`` из записей (иначе пустая строка)."""
    for record in records:
        if record.build_id:
            return record.build_id
    return ""


def _analyzer(tantivy: Any, language: str) -> Any:
    """Анализатор: lowercase → stemmer(language) → stopword(language)."""
    return (
        tantivy.TextAnalyzerBuilder(tantivy.Tokenizer.simple())
        .filter(tantivy.Filter.lowercase())
        .filter(tantivy.Filter.stemmer(language))
        .filter(tantivy.Filter.stopword(language))
        .build()
    )


def _ngram_analyzer(tantivy: Any) -> Any:
    """Анализатор ngram-профиля: ``Tokenizer.ngram(min,max)`` → lowercase.

    Тот же анализатор применяется и при индексации, и при разборе запроса — иначе
    n-граммы документа и запроса не совпадут. Стемминг/стоп-слова намеренно не
    используются: профиль отвечает за подстрочный, а не морфологический матч.
    """
    return (
        tantivy.TextAnalyzerBuilder(
            tantivy.Tokenizer.ngram(min_gram=_NGRAM_MIN, max_gram=_NGRAM_MAX)
        )
        .filter(tantivy.Filter.lowercase())
        .build()
    )


def _require_tantivy() -> Any:
    """Возвращает модуль ``tantivy`` или поднимает ``LexicalIndexUnavailable``."""
    if not tantivy_available():
        raise LexicalIndexUnavailable(
            "tantivy не установлен: лексический индекс недоступен. "
            "Установите зависимость 'retrieval' (05 §2.3) — адаптер не подменяет движок."
        )
    import tantivy

    return tantivy
