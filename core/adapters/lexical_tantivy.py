"""Лексический индекс на tantivy — реализация ``LexicalIndexPort`` (W8/T8.3, §3.2).

Роль в coHound: лексическое плечо ретривала (профиль **R0**, 02 §B.3). В отличие от
``lexical_bm25.py`` (in-process baseline, test double), здесь используется настоящий
поисковый движок с BM25 и нативными RU/EN-анализаторами (проверка стека **V1**,
05 §6: ``Filter.stemmer("russian")``/``Filter.stopword("russian")`` доступны в
tantivy 0.26).

**Одно текстовое поле, анализатор выбирает профиль (C8).** Схема — ``chunk_id`` и
``product_id`` (служебные, ``raw``) плюс **одно** текстовое поле ``body``. Профиль
конструктора задаёт **анализатор** этого поля, а не набор полей:

* ``"stemmed"`` (по умолчанию, RU/EN) — ``lowercase`` → ``stemmer(russian)`` →
  ``stemmer(english)`` → ``stopword(russian)`` → ``stopword(english)``. Цепочка двух
  стеммеров — не «два языка в одном токене»: snowball-стеммеры работают в своём
  алфавите, поэтому кириллический токен проходит русский стеммер и не меняется
  английским, латинский — наоборот. Это то же сближение форм, что раньше давали два
  отдельных поля;
* ``"identifier"`` (псевдоним ``"raw"``) — ``whitespace`` без lowercase и стемминга:
  точные идентификаторы (``API-2024_X``) различают регистр и сохраняют пунктуацию;
* ``"ngram"`` — символьные триграммы (``Tokenizer.ngram``, ``min_gram=3``,
  ``max_gram=3``) по lowercase-тексту, для подстрочного совпадения по фрагменту
  идентификатора (``"2024"`` находит ``API-2024_X``).

**Почему одно поле (замер, а не вкус).** До C8 схема держала **четыре** текстовых поля
с одним и тем же текстом (``body_stem_ru``, ``body_stem_en``, ``body_identifier``,
``body_ngram``), чтобы один индекс отвечал на все профили сразу. Прогон запрашивает
только ``stemmed``: ``wiring.seed_lexical_index`` строит ``TantivyLexicalIndex()`` без
профиля, а ``search_identifier``/``search_ngram`` в ``core/`` не вызываются ни разу.
То есть три поля из четырёх платились работой анализаторов и объёмом индекса и не
читались. Замер на корпусе прогона ``ru-pg-dbms-3edf2b06097c`` (9398 чанков, 7.2 МБ
текста), одна машина, один процесс, по 3 сборки на схему:

==================  ============================  ================
схема               объём индекса (устоявшийся)   сборка, медиана
==================  ============================  ================
4 поля (до C8)      33.60 МБ (32.0 MiB)           1.10 с
1 поле (после C8)   3.66 МБ (3.49 MiB)            0.25 с
==================  ============================  ================

Пик RSS процесса сборки: 295 → 151 MiB. Объём мерян через 5 с после ``commit``:
движок сливает сегменты в фоновых потоках, и «сразу после сборки» даёт нестабильные
25.1 МБ против 2.12 МБ (устоявшееся значение совпадает на +5 с и +20 с).

**Что это меняет в выдаче (измерено, а не предположено).** Ранжирование при
``lang=None`` (а именно так зовёт ``pipeline.retrieve`` из ``evaluate``: все три вызова
``retrieve_for_cell`` — два в ``evaluate``, один в ``evaluator_benchmark`` — не передают
``lang``) раньше складывало баллы **двух** полей с
одним текстом: тот же термин учитывался дважды, а русские стоп-слова через английский
фильтр оставались термами — «и» индексировалось как обычный токен в 3981 чанке из 9398
(42 % корпуса). На 14 пробных запросах корпуса top-10 при ``lang=None``: средний
Jaccard 0.56, при ``lang="ru"``/``"en"`` — 0.94 и совпадение множеств в 10 случаях из
14. То есть смена схемы — **не переупаковка**: при ``lang=None`` выдача меняется, и это
исправление двойного счёта, а не регрессия.

**Провенанс (разрыв, названный явно).** Схема лексического индекса **не входит** ни в
``partition_build_id``, ни в ``index_build_id``: туда идёт ``lexical_engine_version``
(``tantivy-<ver>``, версия движка), а не состав полей. Поэтому смена схемы не двигает
``run_id``/``pb_``/``cb_`` — это тот же класс, что «геометрия чанка вне контракта»
(C3), и решение о версионной границе принимает владелец, а не этот модуль.

**Цена решения, названная явно.** Профили больше **не сосуществуют** в одной сборке:
``identifier``/``ngram`` доступны только как явно построенный индекс с этим профилем.
Кросс-профильный поиск (``search_identifier``/``search_ngram`` по стенмленному индексу)
убран: именно он и требовал держать все четыре поля сразу. Профиль — это выбор
**анализа**, а не выбор поля в готовом индексе.

**Язык запроса.** Языковая ось больше не делит индекс на поля: единственный анализатор
применяет оба стеммера, поэтому ``lang`` не сужает выдачу (параметр принимается ради
контракта ``LexicalIndexPort`` и остаётся осью запроса в ``pipeline.retrieve``). Разница
с прежним поведением односторонняя: латинские словоформы теперь сближаются и в
«русском» запросе, потому что латинский токен документа анализируется английским
стеммером. Языкового firewall порт и не обещал (см. ниже).

**Время жизни индекса — процесс прогона (объявлено, C8).** Индекс строится в
временном каталоге (``tempfile.TemporaryDirectory``, префикс ``cohound-tantivy-``) и
удаляется в :meth:`TantivyLexicalIndex.close` (или GC); персиста нет, каждый прогон
пересобирает индекс из персистентных чанков сборки. Это решение, а не недосмотр:
источник чанков уже персистентен (``corpus_chunks``/партиции ``index_store/``), а
персист лексического индекса потребовал бы версии схемы индекса и миграции при каждой
смене профиля или движка — при цене 0.25 с на 9398 чанков хранение и инвалидация
дороже пересборки. Каталог виден оператору как ``cohound-tantivy-*`` в ``$TMPDIR``
на время фазы оценки.

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
    "LEXICAL_SCHEMA_VERSION",
    "LexicalChunk",
    "LexicalIndexUnavailable",
    "TantivyLexicalIndex",
    "lexical_schema_spec",
    "tantivy_available",
    "tantivy_engine_version",
]

#: Версия **схемы** лексического индекса — то, из чего получается выдача.
#:
#: Отдельно от ``tantivy_engine_version``: версия движка приходит из установленного
#: wheel'а (наблюдаемая, хост-зависимая), а схема объявлена кодом. Именно поэтому схема
#: входит в контракт (``lexical_spec``), а версия движка — нет (правило §13: хеширование
#: наблюдаемой версии сделало бы ``run_id`` хост-зависимым).
#:
#: ``single-body-stem-ru-en-v1`` — C8: одно текстовое поле вместо четырёх. Профиль задаёт
#: анализатор поля, а не набор полей. Замер на живом корпусе (9398 чанков): объём индекса
#: 31.8 → 3.5 MiB, сборка 1.10 → 0.25 с. **Смена выдачи, а не оптимизация:** при
#: ``lang=None`` (единственный продовый путь) прежняя схема складывала баллы двух полей с
#: одним текстом и держала русские стоп-слова термами (средний Jaccard top-10 0.56 против
#: 0.94 при явном языке). Значит это условие измерения и требует новой версии.
LEXICAL_SCHEMA_VERSION = "single-body-stem-ru-en-v1"

def lexical_schema_spec() -> dict[str, object]:
    """Объявленная схема лексического индекса — значение поля контракта ``lexical_spec``.

    Материал — только то, что определяет **выдачу**: состав полей и цепочка анализатора.
    Версия движка сюда не входит (она наблюдаемая, см. ``LEXICAL_SCHEMA_VERSION``).
    Хранимые-но-неиндексируемые поля (``chunk_id``) названы явно: их наличие тоже часть
    схемы, а не деталь реализации.
    """
    return {
        "version": LEXICAL_SCHEMA_VERSION,
        #: Единственное индексируемое текстовое поле: выдача определяется им одним.
        "indexed_fields": [FIELD_BODY],
        #: Фильтр-поля (не участвуют в скоринге, но входят в схему).
        "filter_fields": [FIELD_PRODUCT],
        "stored_fields": [FIELD_CHUNK_ID],
        #: Цепочка анализатора ``stemmed`` — порядок значим (lowercase до стеммеров,
        #: стоп-слова после), поэтому перечисление, а не множество.
        "stemmed_analyzer": [
            "tokenizer:simple",
            "filter:lowercase",
            "filter:stemmer:russian",
            "filter:stemmer:english",
            "filter:stopword:russian",
            "filter:stopword:english",
        ],
    }

#: Имена полей схемы (держатся в одном месте). Текстовое поле **одно** (C8): профиль
#: задаёт его анализатор, а не добавляет поле.
FIELD_CHUNK_ID = "chunk_id"
FIELD_PRODUCT = "product_id"
FIELD_BODY = "body"

#: Токенайзеры профилей. Один действующий на сборку: ``identifier`` использует встроенный
#: ``whitespace``, поэтому своих имён у него нет.
_STEM_TOKENIZER = "cohound_stem_ru_en"
_NGRAM_TOKENIZER = "cohound_ngram"

#: Границы n-грамм ngram-профиля (символы). Фиксированы детерминированно; триграмма —
#: компромисс между подстрочной чувствительностью и объёмом инвертированного индекса.
_NGRAM_MIN = 3
_NGRAM_MAX = 3

#: Допустимые профили поиска (``"raw"`` — псевдоним identifier-профиля).
_PROFILES = {"stemmed", "identifier", "raw", "ngram"}

#: Поля, обязательные в индексируемой записи.
_REQUIRED_FIELDS = ("chunk_id", "product_id", "text")

#: Стартовый запас выдачи сверх ``top_k`` при поиске (R7: детерминизм границы).
#:
#: Tantivy-коллектор держит кучу ровно на запрошенное число документов, поэтому при
#: **равных баллах на границе** выживает тот, кого выбрал движок, а не тот, кто меньше
#: по ``chunk_id``. Замер (корпус ``jatoba``, K=10, 189 запросов): у 12 запросов (6.3 %)
#: группа равных баллов на границе шире K, максимум — 12 документов. Наблюдёмый дефект:
#: два процесса на той же сборке вернули на позиции 9 разные чанки при балле 16.671051,
#: из-за чего ``prompt_hash`` ячеек разошёлся.
#:
#: Запас — не потолок, а старт: :meth:`TantivyLexicalIndex._deterministic_hits`
#: расширяет окно вдвое, пока граница не выйдет из группы ничьих, поэтому результат
#: точен при любом размере группы. Число нужно только чтобы обычный запрос не платил
#: за полный перебор партиции.
_TIE_HEADROOM = 16

#: Потолок расширения окна (документов). Достигается только на патологической партиции,
#: где ничья тянется до самого конца; тогда выдача усекается по ``(-score, chunk_id)``
#: среди всех выданных — детерминированно, хотя и без полной гарантии полноты группы.
_TIE_WINDOW_MAX = 4096


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
    """BM25-поиск на tantivy: одно текстовое поле, анализатор по профилю (§3.2, I16/I17).

    :param index_path: каталог индекса; ``None`` → временный каталог (авто-удаление по
        ``close``/GC). Время жизни индекса в прогоне — процесс (см. докстринг модуля).
    :param profile: анализатор **единственного** текстового поля: ``"stemmed"`` (RU+EN,
        по умолчанию), ``"identifier"``/``"raw"`` (точный токен, регистр значим),
        ``"ngram"`` (символьные триграммы для подстрочного совпадения). Профиль — свойство
        сборки: индекс обслуживает один профиль (C8).

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
                        FIELD_BODY: record.text,
                    }
                )
            )
        writer.commit()
        index.reload()
        # Writer отпускается сразу: он держит lock-файлы каталога индекса, а следующий
        # шаг — создание searcher'а, которому writer не нужен. Без явного ``del``
        # момент финализации объекта движка непредсказуем (GC), и уборка каталога в
        # ``close()`` может соревноваться с ещё живым writer'ом.
        del writer

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
        """BM25-поиск по профилю сборки, продукт-скоупед (I17), tie-break (R7).

        :param lang: язык запроса (``"ru"``/``"en"``). Анализатор единственного поля
            несёт **оба** стеммера, поэтому ``lang`` не сужает выдачу — параметр
            принимается ради контракта ``LexicalIndexPort`` (ось объявлена в
            ``pipeline.retrieve``). Это не языковой firewall: стеммер чужого скрипта
            проходит токен дословно.

        Профиль поиска — свойство **сборки** (``profile`` конструктора): кросс-профильного
        поиска по готовому индексу нет, для другого профиля строится другой индекс (C8).
        """
        return self._search(query, product_ids, top_k, lang=lang)

    # ------------------------------------------------------------------ #
    # Внутреннее
    # ------------------------------------------------------------------ #

    def _resolve_path(self, index_path: str | Path | None) -> Path:
        if index_path is None:
            self._tmp = tempfile.TemporaryDirectory(
                prefix="cohound-tantivy-", ignore_cleanup_errors=True
            )
            return Path(self._tmp.name)
        path = Path(index_path)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _field_tokenizer(self) -> str:
        """Токенайзер единственного текстового поля под профиль сборки (C8)."""
        if self.profile == "identifier":
            return "whitespace"
        if self.profile == "ngram":
            return _NGRAM_TOKENIZER
        return _STEM_TOKENIZER

    def _build_schema(self, tantivy: Any) -> Any:
        builder = tantivy.SchemaBuilder()
        builder.add_text_field(FIELD_CHUNK_ID, stored=True, tokenizer_name="raw")
        builder.add_text_field(FIELD_PRODUCT, tokenizer_name="raw")
        builder.add_text_field(FIELD_BODY, tokenizer_name=self._field_tokenizer())
        return builder.build()

    def _open_index(self, tantivy: Any, schema: Any) -> Any:
        index = tantivy.Index(schema, path=str(self._path))
        tokenizer = self._field_tokenizer()
        if tokenizer == _STEM_TOKENIZER:
            index.register_tokenizer(_STEM_TOKENIZER, _stem_analyzer(tantivy))
        elif tokenizer == _NGRAM_TOKENIZER:
            index.register_tokenizer(_NGRAM_TOKENIZER, _ngram_analyzer(tantivy))
        return index

    def _search(
        self,
        query: str,
        product_ids: Sequence[str],
        top_k: int,
        *,
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
        base = self._build_query(query, lang)
        if base is None:
            return []

        tantivy = _require_tantivy()
        product_filter = tantivy.Query.term_set_query(self._schema, FIELD_PRODUCT, allowed)
        combined = tantivy.Query.boolean_query(
            [(tantivy.Occur.Must, base), (tantivy.Occur.Must, product_filter)]
        )
        out = self._deterministic_hits(combined, top_k, allowed)
        out.sort(key=lambda chunk: (-chunk.score, chunk.chunk_id))
        return out[:top_k]

    def _deterministic_hits(
        self, combined: Any, top_k: int, allowed: Sequence[str]
    ) -> list[RetrievedChunk]:
        """Выдача tantivy, у которой граница ``top_k`` **не разрезает группу равных баллов**.

        Tantivy-коллектор отдаёт ровно запрошенное число документов, и при равенстве
        баллов на границе выбор зависит от внутреннего порядка движка — он не обязан
        совпадать между процессами (наблюдено на позиции 9: балл 16.671051 у двух чанков,
        в разных процессах выживал разный). Это ломает провенанс: ``prompt_hash`` ячейки
        считается по ``chunk_ids``, поэтому «тот же промпт» переставал быть тем же.

        Поэтому окно запрашивается с запасом и **расширяется вдвое**, пока балл на
        позиции ``top_k-1`` строго больше балла последнего полученного документа: это
        доказательство, что группа ничьих целиком внутри окна и усечение по
        ``(-score, chunk_id)`` даёт одно и то же множество в любом процессе. Плата за
        обычный запрос — один лишний шаг кучи, а не перебор партиции.

        Симметрия с dense-плечом: :func:`core.adapters.vector_npy._ranked_rows` держит то
        же свойство («равные баллы на границе не теряются»), здесь оно достигается
        расширением окна, потому что tantivy не умеет «дай всё с баллом ≥ X».
        """
        allowed_set = set(allowed)
        window = max(1, top_k) + _TIE_HEADROOM
        while True:
            hits = self._searcher.search(combined, window)
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
            # Окно исчерпало движок (все совпадения внутри) либо потолок — расширять
            # больше нечем: усечение остаётся детерминированным по (-score, chunk_id).
            if len(out) <= top_k or len(hits.hits) < window:
                return out
            ordered = sorted(out, key=lambda chunk: (-chunk.score, chunk.chunk_id))
            if ordered[top_k - 1].score > ordered[-1].score:
                return out  # граница top_k строго выше дна окна — ничья не разрезана
            if window >= _TIE_WINDOW_MAX:
                return out
            window = min(window * 2, _TIE_WINDOW_MAX)

    def _build_query(self, query: str, lang: str | None) -> Any:
        """Разобранный запрос по профилю сборки (``None`` — пустой запрос).

        ``lang`` не меняет поле (оно одно) и не выбирает анализатор: анализатор — свойство
        сборки, а не запроса. Языковая ось остаётся осью **запроса** в ``pipeline.retrieve``
        и объявлена в контракте порта, поэтому параметр принимается и документируется как
        несужающий (см. :meth:`search`).
        """
        del lang  # ось запроса, не ось индекса: единственный анализатор несёт оба стеммера
        if self.profile == "identifier":
            return self._identifier_query(query)
        if self.profile == "ngram":
            return self._ngram_query(query)
        if not query or not query.strip():
            return None
        # lenient: невалидные фрагменты превращаются в match-nothing, а не в исключение.
        parsed, _errors = self._index.parse_query_lenient(
            query, default_field_names=[FIELD_BODY]
        )
        return parsed

    def _identifier_query(self, query: str) -> Any:
        tokens = [token for token in (query or "").split() if token]
        if not tokens:
            return None
        tantivy = _require_tantivy()
        clauses = [
            (
                tantivy.Occur.Should,
                tantivy.Query.term_query(self._schema, FIELD_BODY, token),
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
            query, default_field_names=[FIELD_BODY]
        )
        return parsed

    def close(self) -> None:
        """Освобождает временный каталог индекса (если он принадлежит адаптеру).

        **Порядок важен, и он не косметика.** Сначала отпускаются объекты движка
        (``searcher`` держит читателей сегментов, ``index`` — meta.json и lock-файлы),
        и только потом удаляется каталог. В обратном порядке ``rmtree`` соревнуется с
        движком и падает ``OSError[39] Directory not empty``; исключение всплывало из
        ``__exit__`` контекстного менеджера, т.е. из самого ``close()`` — это ресурсный
        флак, который в тестах выглядел как падение проверки, а не как проблема уборки.

        Уборка каталога терпима к гонке (``ignore_cleanup_errors``): недоделанный
        каталог во временной директории — цена, которую дешевле заплатить, чем уронить
        прогон на выходе из фазы.
        """
        self._index = None
        self._searcher = None
        self._built = False
        if self._tmp is not None:
            self._tmp.cleanup()
            self._tmp = None

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


def _stem_analyzer(tantivy: Any) -> Any:
    """Анализатор профиля ``stemmed``: lowercase → оба стеммера → оба набора стоп-слов.

    Два стеммера в одной цепочке — не суперпозиция языков на токен: snowball-стеммер
    ``russian`` работает с кириллицей, ``english`` — с латиницей, поэтому каждый токен
    проходит ровно «свой» стеммер и остаётся нетронутым чужим. Это воспроизводит
    поведение двух прежних полей (``body_stem_ru``/``body_stem_en``) в одном поле (C8),
    сохраняя сближение словоформ в каждом языке. Порядок фильтров фиксирован: lowercase
    до стеммеров, стоп-слова после (как было в пофайловых анализаторах).
    """
    return (
        tantivy.TextAnalyzerBuilder(tantivy.Tokenizer.simple())
        .filter(tantivy.Filter.lowercase())
        .filter(tantivy.Filter.stemmer("russian"))
        .filter(tantivy.Filter.stemmer("english"))
        .filter(tantivy.Filter.stopword("russian"))
        .filter(tantivy.Filter.stopword("english"))
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
