#!/usr/bin/env python3
"""Подготовка документного retrieval-эталона (O14 плана 26).

Что утверждает документный эталон: «для ячейки ``{product_id}/{param_id}`` документ,
процитированный в comparative table (``source_url``), должен попасть в top-K».
Утверждение не зависит от нарезки: документ адресуется URL-ом (устойчив к смене
чанкера и переэкстракции), а ``doc_id`` — это лишь его резолв **в конкретном
корпусе** (``doc_id = product_id:content_hash``, то есть меняется вместе с текстом).

Два правила разрешения — **механические**, разметка человека там не нужна, потому что
доказательство уже лежит в данных:

**Правило C — только объявленный хост.** Если хост ``source_url`` не объявлен в
``doc_domains`` паспорта продукта, пара получает статус ``foreign_host`` и кандидата не
ищет. Это не «страница недоступна», а «таблица цитирует не документацию продукта»:
``github.com``, ``v8.1c.ru``, ``support.postgrespro.ru`` — решение о такой ссылке
принимает человек по таблице, а не прибор по корпусу. Замер 17.11.2026: 20 пар из 287.

**Правило A — разрешение по цитате.** Если пара не разрешилась по адресу, а хост объявлен,
ищется **текст цитаты** её ячеек в чанках того же продукта (I17). Цитата — готовое
доказательство: она уже проверена оценщиком против чанка, поэтому выравнивание по ней не
требует разметки. Пара разрешается, только если **все** её цитаты найдены **в одном**
документе; цитаты в двух и более документах — ``ambiguous`` (человек видит, какие это
документы и сколько цитат в каждом); цитата не найдена — ``missing``/``quote_not_found``.
Никакой похожести, косинусов и «одного имени файла»: только буквальное вхождение
нормализованной цитаты (нормализация — общая с чанковым прибором,
:mod:`core.pipeline.quote_text`).

Новый вид совпадения — ``moved_quote``: «страница, в которой цитата лежит буквально».
Это **другое утверждение**, чем ``exact``/``canonical`` («страница, на которую ссылается
таблица»), поэтому ``match`` не подменяется, а старые числа остаются воспроизводимыми:
``--no-quotes`` выключает правило A.

Подкоманды:

``build``
    Строит машинный отчёт по всем парам ``(product_id, source_url)`` из
    ``golden_00_cells.csv`` (CSV: одна строка на пару). URL резолвится в документ
    корпуса **по БД прогона**, а не по похожести строк. Машинные исходы:
    ``resolved`` (ровно один документ), ``ambiguous`` (несколько), ``missing`` (нет),
    ``foreign_host`` (хост не объявлен — правило C). Ничего не додумывается:
    ``missing`` остаётся ``missing``.

``annotate``
    Пересобирает **лист на разметку** — тот, в котором работает человек. Выход по
    расширению: ``.xlsx`` (шапка с фильтром, выпадающий список решений, залитые строки,
    где решение не механическое, второй лист с инструкцией) или CSV. Дописывает
    подсказки: ``titles``/``claims`` (что утверждает ячейка) и ``cited_from`` (откуда
    цитата фактически взята). Решения человека из прежнего листа переносятся.
    ``--moved-out`` дополнительно пишет **второй лист** — только новые пары
    (``moved_quote`` и цитатную ``ambiguous``) с найденным адресом, числом цитат и
    текстом цитаты: это те строки, где решение не механическое, и читать их среди
    287 строк незачем. Решения из этого листа переносятся в основной при следующем
    ``annotate``.

``compile``
    Превращает лист (XLSX или CSV — формат по расширению) после правки человеком в
    JSON-эталон. Включаются только строки с ``decision=accept``; нерезолвленная строка
    с ``accept`` — отказ; ``foreign_host`` с ``accept`` — отказ (прибор не выбирает
    документ за пределами объявленного хоста).

``validate``
    Проверяет готовый JSON против БД прогона: строгая схема, существование каждого
    ``doc_id``, принадлежность продукту ячейки (I17) и совпадение его URL с
    ``source_url`` ячейки. Для ``moved_quote``/``quote`` совпадения адресов не
    требуется — требуется ``corpus_url``, который резолвится в тот же документ и
    принадлежит продукту; запись с ``match=foreign_host`` — отказ. Нерезолвленный
    ``doc_id`` — отказ (код 2), не молчаливый пропуск.

``batches``
    Печатает пачки на подтверждение (``URL | документ | что утверждается | цитата
    фактически из | ячеек | статус``) по ~50 строк — тот же лист, но читаемый в
    терминале и в переписке.

Почему скрипт, а не ``core/util/retrieval_eval.py``: это подготовка входа, а не
оценка. Формат ещё не принят пайплайном (файл-кандидат в пайплайн не подключён), а
``core/`` — категориально-нейтральный движок под гейтами mypy/нейтральности; класть
туда неподключённый формат рано. Когда формат будет принят, загрузчик переедет в
``core`` вместе с правкой контракта оценки.

Исключение — **правило «тот же документ»**: канонизация URL и индексы документов
корпуса живут в :mod:`core.pipeline.doc_refs` (``DocIndex``). Измерение документного
recall@K обязано резолвить URL тем же правилом, что и эталон; две копии канонизации
разъехались бы молча и дали бы два разных ответа на один вопрос.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

# Корень репозитория на sys.path: скрипт запускается как `python scripts/…py`, и тогда
# sys.path[0] — каталог scripts, а не корень (плоская раскладка пакета `core`).
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.config.passports import load_passport, passport_urls  # noqa: E402 — после правки sys.path
from core.pipeline.doc_refs import (  # noqa: E402 — после правки sys.path
    DocIndex,
    canonical_url,
    host_key,
    root_matches,
)
from core.pipeline.quote_text import normalize  # noqa: E402 — одна нормализация на два прибора

GOLDEN_FORMAT = "cohound-retrieval-golden-docs/1"
BATCH_SIZE = 50

CANDIDATE_FIELDS = [
    "product_id",
    "param_ids",
    "cells",
    "source_url",
    "status",
    "match",
    "corpus_url",
    "doc_id",
    "decision",
    "note",
    # Живость адреса (проверка 09.11.2026, `2026-11-09-golden-url-liveness.json`).
    # Это **не** утверждение о релевантности: 301 говорит «страница переехала», 404 —
    # «её нет». Решение `accept`/`reject` всё равно за человеком: переехавшая редакция
    # (17_5 → 17_11) — тот же раздел, но другой текст.
    "live_kind",
    "live_code",
    "live_url",
    # Правила C и A: на объявленном ли хосте пара и что нашлось по цитате. Колонки
    # добавлены **в конец** намеренно: лист, размеченный до них (100 решений человека),
    # обязан оставаться читаемым, иначе `_carry_decisions` молча вернул бы ноль.
    "host_declared",
    "quotes_found",
    "quotes_total",
    "quote_docs",
    "quote_len",
    "quote_sample",
]

#: Колонки, без которых лист не читается. Новые (правила C/A) — необязательные: лист,
#: размеченный до них, читается с пустыми значениями, а не отвергается целиком.
QUOTE_FIELDS = ["host_declared", "quotes_found", "quotes_total", "quote_docs", "quote_len", "quote_sample"]
REQUIRED_FIELDS = [field for field in CANDIDATE_FIELDS if field not in QUOTE_FIELDS]

#: Колонки-подсказки для разметчика (`annotate`): что именно утверждает ячейка.
#: Не вход `compile` — это чтение для человека, а не решение; `compile` их игнорирует.
ANNOTATION_FIELDS = ["titles", "claims", "cited_from"]

#: Колонки, которые заполняет **человек**. `comment` отделена от `note` намеренно:
#: `note` машинная (для `moved` там `moved_from=…`, для `ambiguous` — два `doc_id`), и
#: пересборка листа её перезаписывает. Одна колонка на два владельца означала бы, что
#: комментарий разметчика исчезает при первом же `annotate`.
HUMAN_FIELDS = ["decision", "comment"]

#: Значения ``decision``, которые пишет человек. Пусто = ещё не рассмотрено.
DECISIONS = {"accept", "reject"}


# ---------------------------------------------------------------------------
# Чтение входов
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class CellRow:
    product_id: str
    param_id: str
    source_url: str
    status: str


def read_cells(path: Path) -> list[CellRow]:
    """Ячейки с непустым ``source_url``; строки без URL в эталон не входят.

    Ячейка ``❌`` цитирует документ как свидетельство **отсутствия**; утверждение
    «этот документ релевантен» для неё неверно, поэтому в положительный эталон она не
    входит (её URL всё равно виден в отчёте). Ячейки ``✅``/``⚠️`` входят.
    """
    rows: list[CellRow] = []
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            url = (row.get("source_url") or "").strip()
            if not url:
                continue
            rows.append(
                CellRow(
                    product_id=row["product_id"].strip(),
                    param_id=row["param_id"].strip(),
                    source_url=url,
                    status=(row.get("normalized_status") or "").strip(),
                )
            )
    return rows


class CorpusIndex:
    """Документы прогона, прочитанные точечно (``corpus_docs`` — тысячи строк).

    Разрешение «URL → документ» — правило :class:`~core.pipeline.doc_refs.DocIndex`;
    здесь остаётся то, что делает именно подготовка эталона: покрытие, журнал краула и
    политика. Публичные имена индексов документов сохранены прежними (``exact``,
    ``canonical``, ``canonical_noquery``, ``doc_url``, ``doc_product``), чтобы остальной
    скрипт не знал, где живёт правило.
    """

    def __init__(self, db_path: Path) -> None:
        self._conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        self._conn.row_factory = sqlite3.Row
        self.docs = DocIndex(self._conn)
        self._session_index: dict[tuple[str, str], sqlite3.Row] | None = None

    def close(self) -> None:
        self._conn.close()

    # --- документы и их URL (правило — в core) -------------------------------
    @property
    def exact(self) -> dict[tuple[str, str], list[str]]:
        return self.docs.exact

    @property
    def canonical(self) -> dict[tuple[str, str], list[tuple[str, str]]]:
        return self.docs.canonical

    @property
    def canonical_noquery(self) -> dict[tuple[str, str], list[tuple[str, str]]]:
        return self.docs.canonical_noquery

    # --- проверки для валидатора -------------------------------------------
    def doc_url(self, doc_id: str) -> str | None:
        return self.docs.doc_url(doc_id)

    def doc_product(self, doc_id: str) -> str | None:
        return self.docs.doc_product(doc_id)

    def declared_roots(self) -> dict[str, list[tuple[str, bool]]]:
        """Объявленные корни продукта и полнота их обхода (из покрытия прогона)."""
        roots: dict[str, list[tuple[str, bool]]] = defaultdict(list)
        for row in self._conn.execute(
            "SELECT product_id, roots_json FROM corpus_coverage"
        ):
            for root in json.loads(row["roots_json"] or "[]"):
                url = root.get("root_url")
                if url:
                    roots[row["product_id"]].append((url, bool(root.get("complete"))))
        return dict(roots)

    def truncated_products(self) -> dict[str, bool]:
        """Продукты, чей обход упёрся в бюджет (``corpus_coverage.truncated``)."""
        return {
            row["product_id"]: bool(row["truncated"])
            for row in self._conn.execute(
                "SELECT product_id, truncated FROM corpus_coverage"
            )
        }

    def unreadable_origins(self) -> dict[str, set[str]]:
        """Хосты, которые обход объявил недоступными (``unreadable_origins_json``)."""
        return {
            row["product_id"]: set(json.loads(row["unreadable_origins_json"] or "[]"))
            for row in self._conn.execute(
                "SELECT product_id, unreadable_origins_json FROM corpus_coverage"
            )
        }

    def policy_skips(self) -> dict[str, list[dict]]:
        """Причины пропуска по политике краула (из манифеста прогона)."""
        row = self._conn.execute(
            "SELECT crawl_policy_json FROM runs LIMIT 1"
        ).fetchone()
        if row is None or not row["crawl_policy_json"]:
            return {}
        payload = json.loads(row["crawl_policy_json"])
        skips: dict[str, list[dict]] = defaultdict(list)
        for skip in payload.get("policy_skips", []):
            skips[skip.get("product_id", "")].append(skip)
        return dict(skips)

    def session_item(self, product_id: str, url: str) -> sqlite3.Row | None:
        """Строка журнала краула для URL (по канону), если она есть.

        Журнал читается один раз и индексируется по канону: точечный запрос на каждый
        ``missing``-URL дал бы полный скан 6 192 строк × 257 раз.
        """
        if self._session_index is None:
            self._session_index = {}
            for row in self._conn.execute(
                "SELECT product_id, url, status, http_status, error_kind "
                "FROM crawl_session_items"
            ):
                key = (row["product_id"], canonical_url(row["url"]))
                self._session_index.setdefault(key, row)
        return self._session_index.get((product_id, canonical_url(url)))

    # --- правило A: поиск цитаты по тексту корпуса --------------------------
    def quote_search(
        self, needles: dict[tuple[str, str], tuple[str, ...]]
    ) -> QuoteIndex:
        """Строит индекс цитат на соединении прибора (одно чтение, только на чтение)."""
        return QuoteIndex.build(self._conn, needles)


# ---------------------------------------------------------------------------
# Правило C: только объявленный хост
# ---------------------------------------------------------------------------
class DeclaredHosts:
    """Объявленные хосты продукта (``doc_domains`` паспорта) — правило C.

    **Что взято из** :mod:`core.pipeline.doc_refs`: :func:`~core.pipeline.doc_refs.host_key` —
    сравнение хостов с эквивалентностью ``www.``. **Что не взято и почему:**
    :func:`~core.pipeline.doc_refs.root_matches` и ``Frontier.host_in_scope``
    (:mod:`core.pipeline.crawl_frontier`) добавляют к хосту префикс пути, потому что там
    вопрос другой — «вправе ли этот корень тратить бюджет обхода». Здесь вопрос —
    «принадлежит ли страница документации продукта», и префикс пути на него не отвечает.
    Замер 17.11.2026 на 187 неразрешённых парах: **76** лежат на объявленном хосте, но вне
    объявленного префикса — pangolin ``platformv.sbertech.ru/docs/public/PSQ/7.1.1/…``
    против объявленного ``…/PSQ/8.1.0/``, tantor ``docs.tantorlabs.ru/tp/6.0/…`` против
    ``…/tdb/``. Это тот же продукт и та же страница **другой версии** (именно их и находит
    правило A), а не чужой хост; назвать их ``foreign_host`` значило бы выбросить
    доказательство по признаку, который к владению не относится.

    Паспорта читаются один раз на продукт; продукт без паспорта — видимая ошибка: правило C
    без объявленных хостов не решает ничего, и тихое отключение выглядело бы как «чужих
    хостов нет».
    """

    def __init__(self, passports_dir: Path, products: set[str]) -> None:
        if not passports_dir.is_dir():
            raise SystemExit(
                f"build: --passports-dir {passports_dir} не каталог — правило C "
                "(только объявленный хост) без паспортов не исполняется"
            )
        self._hosts: dict[str, frozenset[str]] = {}
        for product_id in sorted(products):
            passport = load_passport(product_id, passports_dir)
            if passport is None:
                raise SystemExit(
                    f"build: у продукта {product_id!r} нет паспорта в {passports_dir} — "
                    "объявленные хосты неизвестны, правило C применить нельзя"
                )
            hosts = frozenset(
                host_key(url) for url in passport_urls(passport) if host_key(url)
            )
            if not hosts:
                raise SystemExit(
                    f"build: паспорт {product_id!r} не объявляет ни одного doc_domain — "
                    "правило C не с чем сверять"
                )
            self._hosts[product_id] = hosts

    def is_declared(self, product_id: str, url: str) -> bool:
        """Хост URL объявлен паспортом продукта (``www.`` ≡ без ``www.``)."""
        return host_key(url) in self._hosts.get(product_id, frozenset())


# ---------------------------------------------------------------------------
# Правило A: разрешение по цитате
# ---------------------------------------------------------------------------
class QuoteIndex:
    """Где лежат цитаты пар: ``(product, url) → {doc_id: {индексы найденных цитат}}``.

    Хранятся **индексы**, а не счётчик: «сколько цитат найдено» и «в скольких документах»
    — разные вопросы, и один и тот же текст, встреченный в 179 документах, не должен
    выглядеть как 179 найденных цитат.

    Поиск — **буквальное вхождение** нормализованной цитаты (нормализация общая с
    чанковым прибором, :mod:`core.pipeline.quote_text`) в текст документа, собранный из
    его чанков. Никакой похожести, расстояний и «одного имени файла»: разрешение по
    цитате — это факт «цитата здесь есть», а не оценка близости.

    **Почему одним проходом.** В run-DB нет индекса по ``corpus_chunks.doc_id``, поэтому
    ``WHERE doc_id = ?`` на каждый документ — полный скан 253 306 строк (замер 17.11.2026:
    40 документов не уложились в 120 с). Здесь чанки читаются один раз на продукт в
    порядке ``doc_id, chunk_no`` (есть индекс ``idx_chunks_product``), текст документа
    собирается из его чанков — ровно как в чанковом приборе, где чанк является точным
    срезом текста.

    Соединение чужое: класс его не открывает и не закрывает.
    """

    def __init__(self, found: dict[tuple[str, str], dict[str, set[int]]]) -> None:
        self._found = found

    @classmethod
    def build(
        cls, conn: sqlite3.Connection, needles: dict[tuple[str, str], tuple[str, ...]]
    ) -> QuoteIndex:
        """Ищет цитаты пар по корпусу. ``needles`` — уже дедуплицированные цитаты пары."""
        found: dict[tuple[str, str], dict[str, set[int]]] = defaultdict(
            lambda: defaultdict(set)
        )
        by_product: dict[str, dict[str, list[tuple[tuple[str, str], int]]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for key, quotes in needles.items():
            for index, quote in enumerate(quotes):
                needle, _ = normalize(quote)
                if needle:
                    by_product[key[0]][needle].append((key, index))
        for product_id in sorted(by_product):
            owners = by_product[product_id]
            current: str | None = None
            parts: list[str] = []
            for doc_id, text in conn.execute(
                "SELECT doc_id, text FROM corpus_chunks WHERE product_id = ? "
                "ORDER BY doc_id, chunk_no",
                (product_id,),
            ):
                if doc_id != current:
                    if current is not None:
                        _collect(parts, current, owners, found)
                    current = str(doc_id)
                    parts = []
                parts.append(str(text or ""))
            if current is not None:
                _collect(parts, current, owners, found)
        return cls(
            {key: {doc: set(indexes) for doc, indexes in value.items()} for key, value in found.items()}
        )

    def docs(self, product_id: str, url: str) -> dict[str, int]:
        """``doc_id → сколько цитат пары найдено в нём``; пусто — цитат в корпусе нет."""
        return {
            doc_id: len(indexes)
            for doc_id, indexes in self._found.get((product_id, url), {}).items()
        }

    def quotes_found(self, product_id: str, url: str) -> int:
        """Сколько **разных** цитат пары найдено хоть где-то (не сумма вхождений)."""
        found = self._found.get((product_id, url), {})
        return len(set().union(*found.values())) if found else 0

    def multi_doc_quotes(self, product_id: str, url: str) -> int:
        """Сколько цитат пары встречается **более чем в одном** документе.

        Отличает две разные неоднозначности: «одна цитата лежит в N документах» (общий
        текст, boilerplate) и «цитаты разошлись по документам» (таблица цитирует две
        страницы). Числа в примечании без этого признака неразличимы.
        """
        per_quote: dict[int, int] = defaultdict(int)
        for indexes in self._found.get((product_id, url), {}).values():
            for index in indexes:
                per_quote[index] += 1
        return sum(1 for count in per_quote.values() if count > 1)


def _collect(
    parts: list[str],
    doc_id: str,
    owners: dict[str, list[tuple[tuple[str, str], int]]],
    found: dict[tuple[str, str], dict[str, set[int]]],
) -> None:
    """Ищет все цитаты продукта в тексте одного документа (нормализация — один раз)."""
    haystack, _ = normalize("".join(parts))
    for needle, pairs in owners.items():
        if needle not in haystack:
            continue
        for key, index in pairs:
            found[key][doc_id].add(index)


#: Сегмент пути, считающийся версией: ``8.1.0``, ``v17.5``, ``7``, ``15_12``,
#: ``versiya-174``. Формы выбраны по замеру 17.11.2026: документация пишет версию и
#: подчёркиванием (``tdb/ru/15_12/`` против ``tdb/ru/18_6/``), и словом с числом
#: (``postgresql/versiya-174/`` против ``q-database/versiya-180/``). Риск ложного
#: срабатывания (номер раздела вместо версии) назван в примечании как признак, а не как
#: основание — решение всё равно за человеком.
_VERSION_SEGMENT = re.compile(r"^(?:v?\d+(?:[._-]\d+)*|[a-z]+[-_]\d+)$")


def _version_split(url: str) -> tuple[str, str] | None:
    """``(версия, хвост пути после неё)`` или ``None``, если сегмента-версии нет."""
    segments = [part for part in urlsplit(url.strip()).path.split("/") if part]
    for index, segment in enumerate(segments):
        if _VERSION_SEGMENT.match(segment):
            return segment, "/".join(segments[index + 1 :])
    return None


def _version_hint(source_url: str, corpus_url: str) -> str:
    """Признак «подстановка версии возможна» — **для человека**, не основание решения.

    Сравнивается хвост пути после сегмента-версии: ``PSQ/7.1.1/common/x.html`` и
    ``PSQ/8.1.0/common/x.html`` дают ``version=7.1.1->8.1.0``, то есть «та же страница
    другой версии». Несовпавший хвост — ``version=tail_differs``: это другая страница
    того же продукта, и человек должен видеть разницу.
    """
    source = _version_split(source_url)
    corpus = _version_split(corpus_url)
    if source is None or corpus is None:
        return "version=none"
    if source[1] != corpus[1]:
        return "version=tail_differs"
    return f"version={source[0]}->{corpus[0]}"


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Verdict:
    """Исход пары: статус, вид совпадения и то, что нужно листу разметки.

    ``quotes_found``/``quote_docs``/``quote_len``/``quote_sample`` заполняются правилом A:
    без них лист на разметку показывал бы «неоднозначно» без ответа на вопрос «сколько
    документов и сколько цитат в каждом».
    """

    status: str
    match: str = ""
    corpus_url: str = ""
    doc_id: str = ""
    note: str = ""
    quotes_found: int = 0
    quotes_total: int = 0
    quote_docs: int = 0
    quote_len: int = 0
    quote_sample: str = ""


def _ambiguous_note(corpus: CorpusIndex, doc_ids: list[str]) -> str:
    """Кандидаты для ``ambiguous``: URL + doc_id, чтобы человек выбрал, а не гадал."""
    parts = []
    for doc_id in doc_ids:
        parts.append(f"{corpus.doc_url(doc_id)} ({doc_id})")
    return f"ambiguous: {len(doc_ids)} docs: " + " | ".join(parts)


def _quote_ambiguous_note(
    corpus: CorpusIndex, docs: dict[str, int], total: int, multi: int
) -> str:
    """Цитаты разошлись по документам: число документов, до трёх URL и счёт цитат.

    Счёт — «сколько из скольких цитат пары лежит в этом документе». ``multi`` — сколько
    цитат встречается более чем в одном документе: без него «2 docs» неотличимо от
    «обе цитаты лежат в обоих» и от «одна цитата общая для двух страниц».
    """
    ordered = sorted(docs.items(), key=lambda item: (-item[1], item[0]))
    parts = [
        f"{corpus.doc_url(doc_id)} ({doc_id}, {count}/{total})"
        for doc_id, count in ordered[:3]
    ]
    more = "" if len(ordered) <= 3 else f" +{len(ordered) - 3}"
    return f"quote_ambiguous: {len(ordered)} docs; multi={multi}: " + " | ".join(parts) + more


def _quote_verdict(
    corpus: CorpusIndex,
    url: str,
    quotes: tuple[str, ...],
    found: dict[str, int],
    found_any: int,
    multi: int = 0,
) -> Verdict | None:
    """Правило A: исход пары по найденным цитатам; ``None`` — ни одна цитата не найдена.

    Разрешение — только когда **все** цитаты пары лежат **в одном** документе. Цитата,
    найденная в двух документах, и цитаты, разошедшиеся по документам, — ``ambiguous``:
    «таблица ссылается на одну страницу, а цитирует две» — факт, который решает человек.
    """
    shortest = min(quotes, key=len)
    sample = _one_line(shortest)
    common = {
        "quotes_found": found_any,
        "quotes_total": len(quotes),
        "quote_docs": len(found),
        "quote_len": len(shortest),
        "quote_sample": sample,
    }
    if not found:
        return None
    if len(found) > 1:
        return Verdict(
            status="ambiguous",
            match="quote",
            note=_quote_ambiguous_note(corpus, found, len(quotes), multi),
            **common,
        )
    doc_id, count = next(iter(found.items()))
    doc_url = corpus.doc_url(doc_id) or ""
    if count < len(quotes):
        # Часть цитат нашлась, часть — нет: разрешением это не является, но и «ничего не
        # нашли» было бы неправдой. Причина — та же `quote_not_found`, в примечании видно,
        # где лежит найденная часть.
        return Verdict(
            status="missing",
            note=f"quote_not_found:found={count}/{len(quotes)};doc={doc_url}",
            **common,
        )
    sha = hashlib.sha256(shortest.encode("utf-8")).hexdigest()[:16]
    note = (
        f"moved_quote:source={url};sha256={sha};quotes={count}/{len(quotes)};"
        f"doc={doc_url};{_version_hint(url, doc_url)}"
    )
    return Verdict(
        status="resolved",
        match="moved_quote",
        corpus_url=doc_url,
        doc_id=doc_id,
        note=note,
        **common,
    )


def classify(
    corpus: CorpusIndex,
    roots: dict[str, list[tuple[str, bool]]],
    skips: dict[str, list[dict]],
    truncated: dict[str, bool],
    unreadable: dict[str, set[str]],
    product_id: str,
    url: str,
    *,
    declared: DeclaredHosts | None = None,
    quotes: tuple[str, ...] = (),
    quote_index: QuoteIndex | None = None,
) -> Verdict:
    """Возвращает исход пары продукт+URL: статус, вид совпадения, документ, примечание.

    Порядок решений — от факта к догадке:

    1. адрес (``exact`` → ``canonical`` → ``canonical_query``) — как было;
    2. правило C: хост не объявлен паспортом → ``foreign_host``, кандидата не ищем;
    3. правило A: буквальное вхождение цитаты в текст документа продукта;
    4. ``missing`` с названной причиной.

    ``missing`` не маскируется: причина называется машинным токеном, но сам URL остаётся
    в списке на решение человека.
    """
    exact = corpus.exact.get((product_id, url), [])
    if len(exact) == 1:
        doc_url = corpus.doc_url(exact[0]) or url
        return Verdict("resolved", "exact", doc_url, exact[0])
    if len(exact) > 1:
        return Verdict("ambiguous", "exact", note=_ambiguous_note(corpus, exact))

    canonical = corpus.canonical.get((product_id, canonical_url(url)), [])
    if len(canonical) == 1:
        doc_url, doc_id = canonical[0]
        return Verdict("resolved", "canonical", doc_url, doc_id)
    if len(canonical) > 1:
        return Verdict(
            "ambiguous",
            "canonical",
            note=_ambiguous_note(corpus, [doc_id for _, doc_id in canonical]),
        )

    noquery = corpus.canonical_noquery.get(
        (product_id, canonical_url(url, drop_query=True)), []
    )
    if len(noquery) == 1:
        doc_url, doc_id = noquery[0]
        return Verdict("resolved", "canonical_query", doc_url, doc_id, "query differs")
    if len(noquery) > 1:
        return Verdict(
            "ambiguous",
            "canonical_query",
            note=_ambiguous_note(corpus, [doc_id for _, doc_id in noquery]),
        )

    # --- правило C: таблица цитирует не документацию продукта -----------------
    if declared is not None and not declared.is_declared(product_id, url):
        return Verdict("foreign_host", note=f"foreign_host:{host_key(url)}")

    # --- правило A: разрешение по цитате -------------------------------------
    if quote_index is not None and quotes:
        found = quote_index.docs(product_id, url)
        verdict = _quote_verdict(
            corpus,
            url,
            quotes,
            found,
            quote_index.quotes_found(product_id, url),
            quote_index.multi_doc_quotes(product_id, url),
        )
        if verdict is not None and verdict.status != "missing":
            return verdict
        if verdict is not None:
            base = _missing_note(corpus, roots, skips, truncated, unreadable, product_id, url)
            return replace(
                verdict,
                note=verdict.note + (f";{base}" if base else ""),
            )

    note = _missing_note(corpus, roots, skips, truncated, unreadable, product_id, url)
    if quote_index is not None and quotes:
        # Цитаты искали и не нашли ни одной: это причина, а не отсутствие причины.
        note = ";".join(part for part in ("quote_not_found", note) if part)
    return Verdict("missing", note=note)


def _missing_note(
    corpus: CorpusIndex,
    roots: dict[str, list[tuple[str, bool]]],
    skips: dict[str, list[dict]],
    truncated: dict[str, bool],
    unreadable: dict[str, set[str]],
    product_id: str,
    url: str,
) -> str:
    """Причина ``missing``: назвать, а не превратить в догадку о документе."""
    item = corpus.session_item(product_id, url)
    if item is not None:
        detail = f"{item['status']}"
        if item["http_status"] is not None:
            detail += f"/http={item['http_status']}"
        if item["error_kind"]:
            detail += f"/{item['error_kind']}"
        return f"visited_no_doc:{detail}"

    for skip in skips.get(product_id, []):
        root = skip.get("root") or ""
        if root and root_matches(url, root):
            reasons = "+".join(skip.get("reasons") or [])
            return f"policy_skipped:{reasons}"

    if host_key(url) in unreadable.get(product_id, set()):
        return "unreadable_origin"

    matching = [
        (root, complete)
        for root, complete in roots.get(product_id, [])
        if root_matches(url, root)
    ]
    if matching:
        # Самый специфичный корень информативнее: `/docs/` говорит больше, чем `/`.
        root, complete = max(matching, key=lambda item: len(urlsplit(item[0]).path))
        return (
            f"in_scope_not_reached:root={root};complete={int(complete)};"
            f"truncated={int(truncated.get(product_id, False))}"
        )

    return "out_of_scope"


def _load_liveness(path: str | None) -> dict[str, dict[str, object]]:
    """Живость адресов из файла замера: ``{url: {code, final_url, kind}}``.

    Файла нет — словарь пуст: ``build`` работает как раньше, а колонки ``live_*``
    остаются пустыми. Подставлять «живой» по умолчанию нельзя: это была бы догадка.
    """
    if not path:
        return {}
    with open(path, encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise SystemExit(f"--liveness: ожидался объект URL→факт, получено {type(payload)}")
    return payload


def _pair_quotes(
    run_db: Path, cells: list[CellRow]
) -> dict[tuple[str, str], tuple[str, ...]]:
    """Цитаты пар ``(product, url)`` — по **положительным** ячейкам, которые ссылаются на URL.

    Цитата ``❌``-ячейки — свидетельство отсутствия, и подставлять её как «страница
    релевантна» нельзя: это перевернуло бы смысл доказательства. Поэтому берутся только
    ``✅``/``⚠️``.

    Одна цитата на пару упоминается один раз: одинаковый текст в двух ячейках — это одна
    цитата, и считать её дважды значило бы требовать «все цитаты» от одной и той же.
    """
    by_cell: dict[tuple[str, str], list[str]] = defaultdict(list)
    for cell in cells:
        if cell.status != "❌":
            by_cell[(cell.product_id, cell.param_id)].append(cell.source_url)
    conn = sqlite3.connect(f"file:{run_db}?mode=ro", uri=True)
    try:
        evidence: dict[tuple[str, str], list[str]] = defaultdict(list)
        for product_id, param_id, quote in conn.execute(
            "SELECT product_id, param_id, citation_quote FROM cell_evidence"
            " WHERE citation_quote IS NOT NULL AND trim(citation_quote) <> ''"
        ):
            evidence[(product_id, param_id)].append(str(quote))
    finally:
        conn.close()
    quotes: dict[tuple[str, str], list[str]] = defaultdict(list)
    for (product_id, param_id), urls in by_cell.items():
        for url in urls:
            for quote in evidence.get((product_id, param_id), []):
                if quote not in quotes[(product_id, url)]:
                    quotes[(product_id, url)].append(quote)
    return {key: tuple(value) for key, value in quotes.items()}


def cmd_build(args: argparse.Namespace) -> int:
    cells = read_cells(Path(args.cells))
    liveness = _load_liveness(getattr(args, "liveness", None))
    grouped: dict[tuple[str, str], list[str]] = defaultdict(list)
    absence: dict[tuple[str, str], list[str]] = defaultdict(list)
    for cell in cells:
        bucket = absence if cell.status == "❌" else grouped
        bucket[(cell.product_id, cell.source_url)].append(cell.param_id)

    # Правило C исполняется всегда: без объявленных хостов пара не отличается от
    # «страницы нет», а это разные факты. `--no-quotes` выключает только правило A.
    declared = DeclaredHosts(Path(args.passports_dir), {cell.product_id for cell in cells})
    quotes = _pair_quotes(Path(args.run_db), cells) if args.quotes else {}

    corpus = CorpusIndex(Path(args.run_db))
    try:
        roots = corpus.declared_roots()
        skips = corpus.policy_skips()
        truncated = corpus.truncated_products()
        unreadable = corpus.unreadable_origins()
        verdicts: dict[tuple[str, str], Verdict] = {}
        # Пары, где цитату есть где искать: адрес не разрешился, хост объявлен. Индекс
        # цитат строится по ним одним проходом — разрешённые по адресу в нём не нужны.
        pending: dict[tuple[str, str], tuple[str, ...]] = {}
        for (product_id, url), _param_ids in sorted(grouped.items()):
            live = liveness.get(url) or {}
            verdict = classify(
                corpus,
                roots,
                skips,
                truncated,
                unreadable,
                product_id,
                url,
                declared=declared,
            )
            # Переехавший адрес: пробуем конечный. Это отдельный ``match``, чтобы
            # подмена адреса была видна, а не выглядела точным попаданием.
            live_url = str(live.get("final_url") or "")
            if verdict.status == "missing" and live_url and live_url != url:
                moved = classify(
                    corpus,
                    roots,
                    skips,
                    truncated,
                    unreadable,
                    product_id,
                    live_url,
                    declared=declared,
                )
                if moved.status in {"resolved", "ambiguous"}:
                    verdict = replace(
                        moved,
                        match=f"moved_{moved.match or 'canonical'}",
                        note=";".join(
                            part
                            for part in (f"moved_from={url}", moved.note)
                            if part
                        ),
                    )
            verdicts[(product_id, url)] = verdict
            if verdict.status == "missing" and quotes.get((product_id, url)):
                pending[(product_id, url)] = quotes[(product_id, url)]

        if pending:
            index = corpus.quote_search(pending)
            for (product_id, url), pair_quotes in pending.items():
                verdicts[(product_id, url)] = classify(
                    corpus,
                    roots,
                    skips,
                    truncated,
                    unreadable,
                    product_id,
                    url,
                    declared=declared,
                    quotes=pair_quotes,
                    quote_index=index,
                )

        rows = []
        for (product_id, url), param_ids in sorted(grouped.items()):
            live = liveness.get(url) or {}
            verdict = verdicts[(product_id, url)]
            note = verdict.note
            excluded = sorted(absence.get((product_id, url), []))
            if excluded:
                note = ";".join(
                    part
                    for part in (note, f"absence_cells_excluded={';'.join(excluded)}")
                    if part
                )
            rows.append(
                {
                    "product_id": product_id,
                    "param_ids": ";".join(sorted(param_ids)),
                    "cells": str(len(param_ids)),
                    "source_url": url,
                    "status": verdict.status,
                    "match": verdict.match,
                    "corpus_url": verdict.corpus_url,
                    "doc_id": verdict.doc_id,
                    "decision": "",
                    "note": note,
                    "live_kind": str(live.get("kind") or ""),
                    "live_code": (
                        "" if not live else str(live.get("code", ""))
                    ),
                    "live_url": str(live.get("final_url") or ""),
                    "host_declared": "yes" if declared.is_declared(product_id, url) else "no",
                    "quotes_found": str(verdict.quotes_found),
                    "quotes_total": str(verdict.quotes_total),
                    "quote_docs": str(verdict.quote_docs),
                    "quote_len": str(verdict.quote_len),
                    "quote_sample": verdict.quote_sample,
                }
            )
    finally:
        corpus.close()

    out = Path(args.out)
    with out.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CANDIDATE_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    _print_summary(rows, cells, out, quotes=args.quotes)
    return 0


def _summary_buckets(rows: list[dict[str, str]]) -> dict[str, int]:
    """Состав пар по статусу и виду совпадения (``resolved/exact`` и т. п.)."""
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        if row["status"] == "missing":
            key = f"missing:{row['note'].split(':', 1)[0].split(';', 1)[0]}"
        elif row["status"] == "resolved":
            key = f"resolved/{row['match'] or '?'}"
        elif row["status"] == "ambiguous":
            key = f"ambiguous/{row['match'] or '?'}"
        else:
            key = row["status"]
        counts[key] += 1
    return counts


#: Виды совпадения по адресу — «до» правил A/C в таблице отчёта.
_ADDRESS_MATCHES = frozenset({"exact", "canonical", "canonical_query"})


def _product_table(rows: list[dict[str, str]]) -> dict[str, dict[str, int]]:
    """Пары и ячейки по продуктам: сколько разрешилось адресом, цитатой и чем не вышло.

    ``ambiguous`` печатается одной колонкой (как в отчёте), но внутри разделена на
    адресную и цитатную: «до/после» — это адресное разрешение против адресного плюс
    цитатного, и смешать их в одном счётчике значило бы выдать правило A за правило C.
    """
    table: dict[str, dict[str, int]] = {}
    for row in rows:
        cells = int(row["cells"])
        item = table.setdefault(row["product_id"], _empty_product_row())
        item["pairs"] += 1
        item["cells"] += cells
        if row["host_declared"] == "yes":
            item["pairs_declared"] += 1
            item["cells_declared"] += cells
        if row["status"] == "resolved" and row["match"] in _ADDRESS_MATCHES:
            item["pairs_addr"] += 1
            item["cells_addr"] += cells
        elif row["status"] == "resolved":
            item["pairs_quote"] += 1
            item["cells_quote"] += cells
        elif row["status"] == "ambiguous":
            item["pairs_ambiguous"] += 1
            item["cells_ambiguous"] += cells
            if row["match"] in _ADDRESS_MATCHES:
                item["pairs_addr_ambiguous"] += 1
                item["cells_addr_ambiguous"] += cells
            else:
                item["pairs_quote_ambiguous"] += 1
                item["cells_quote_ambiguous"] += cells
        elif row["status"] == "foreign_host":
            item["pairs_foreign"] += 1
            item["cells_foreign"] += cells
        else:
            item["pairs_missing"] += 1
            item["cells_missing"] += cells
    return table


def _empty_product_row() -> dict[str, int]:
    """Счётчики строки таблицы: пары и ячейки по каждому исходу."""
    return dict.fromkeys(
        (
            "pairs",
            "pairs_declared",
            "pairs_addr",
            "pairs_quote",
            "pairs_ambiguous",
            "pairs_addr_ambiguous",
            "pairs_quote_ambiguous",
            "pairs_missing",
            "pairs_foreign",
            "cells",
            "cells_declared",
            "cells_addr",
            "cells_quote",
            "cells_ambiguous",
            "cells_addr_ambiguous",
            "cells_quote_ambiguous",
            "cells_missing",
            "cells_foreign",
        ),
        0,
    )


def _print_summary(
    rows: list[dict[str, str]], cells: list[CellRow], out: Path, *, quotes: bool
) -> None:
    counts = _summary_buckets(rows)
    positive = sum(1 for cell in cells if cell.status != "❌")
    print(f"cells with source_url: {len(cells)} (positive {positive}, ❌ {len(cells) - positive})")
    print(f"unique (product, url) pairs: {len(rows)}")
    print(f"positive cells in pairs: {sum(int(row['cells']) for row in rows)}")
    print(f"правило A (цитата): {'включено' if quotes else 'выключено (--no-quotes)'}")
    for key in sorted(counts):
        print(f"  {key}: {counts[key]}")

    table = _product_table(rows)
    header = (
        f"{'product':20} {'pairs':>6} {'decl':>5} {'addr':>5} {'quote':>5} "
        f"{'amb':>4} {'miss':>5} {'foreign':>7} | {'cells':>6} {'addr':>5} "
        f"{'quote':>5} {'amb':>4} {'miss':>5} {'foreign':>7}"
    )
    print()
    print(header)
    totals = dict.fromkeys(table[next(iter(table))], 0) if table else {}
    for product_id in sorted(table):
        item = table[product_id]
        for key in totals:
            totals[key] += item[key]
        print(
            f"{product_id:20} {item['pairs']:6} {item['pairs_declared']:5} "
            f"{item['pairs_addr']:5} {item['pairs_quote']:5} {item['pairs_ambiguous']:4} "
            f"{item['pairs_missing']:5} {item['pairs_foreign']:7} | {item['cells']:6} "
            f"{item['cells_addr']:5} {item['cells_quote']:5} {item['cells_ambiguous']:4} "
            f"{item['cells_missing']:5} {item['cells_foreign']:7}"
        )
    if totals:
        print(
            f"{'ИТОГО':20} {totals['pairs']:6} {totals['pairs_declared']:5} "
            f"{totals['pairs_addr']:5} {totals['pairs_quote']:5} {totals['pairs_ambiguous']:4} "
            f"{totals['pairs_missing']:5} {totals['pairs_foreign']:7} | {totals['cells']:6} "
            f"{totals['cells_addr']:5} {totals['cells_quote']:5} {totals['cells_ambiguous']:4} "
            f"{totals['cells_missing']:5} {totals['cells_foreign']:7}"
        )
        _print_share(table, totals)
    print(f"written: {out}")


def _print_share(table: dict[str, dict[str, int]], totals: dict[str, int]) -> None:
    """Доля крупнейшего продукта — «до» и «после».

    Печатается потому, что агрегат по эталону читается как агрегат по продуктам, а он
    держится одним продуктом: 17.11.2026 — 115 из 132 ячеек ``postgrespro``.
    """
    if not table:
        return
    biggest = max(table, key=lambda product_id: table[product_id]["cells_addr"])
    item = table[biggest]
    before_pairs = totals["pairs_addr"] + totals["pairs_addr_ambiguous"]
    before_cells = totals["cells_addr"] + totals["cells_addr_ambiguous"]
    after_pairs = before_pairs + totals["pairs_quote"] + totals["pairs_quote_ambiguous"]
    after_cells = before_cells + totals["cells_quote"] + totals["cells_quote_ambiguous"]
    own_before_pairs = item["pairs_addr"] + item["pairs_addr_ambiguous"]
    own_before_cells = item["cells_addr"] + item["cells_addr_ambiguous"]
    own_after_pairs = own_before_pairs + item["pairs_quote"] + item["pairs_quote_ambiguous"]
    own_after_cells = own_before_cells + item["cells_quote"] + item["cells_quote_ambiguous"]
    print()
    for label, pairs, cells, own_pairs, own_cells in (
        (
            "до (адрес: resolved + ambiguous по адресу)",
            before_pairs,
            before_cells,
            own_before_pairs,
            own_before_cells,
        ),
        (
            "после (адрес + правило A)",
            after_pairs,
            after_cells,
            own_after_pairs,
            own_after_cells,
        ),
    ):
        pair_share = 100.0 * own_pairs / pairs if pairs else 0.0
        cell_share = 100.0 * own_cells / cells if cells else 0.0
        print(
            f"{label}: пар {pairs}, ячеек {cells}; {biggest} — "
            f"{own_pairs} пар ({pair_share:.0f} %), "
            f"{own_cells} ячеек ({cell_share:.0f} %)"
        )


# ---------------------------------------------------------------------------
# compile
# ---------------------------------------------------------------------------
#: Колонки листа в порядке чтения: сначала то, по чему принимается решение (адреса и
#: уровень совпадения), затем две человеческие колонки рядом, затем машинный `note` и
#: подсказки. `compile` читает по именам, порядок нужен только глазам.
ALL_FIELDS = [
    *CANDIDATE_FIELDS[: CANDIDATE_FIELDS.index("decision")],
    "decision",
    "comment",
    *CANDIDATE_FIELDS[CANDIDATE_FIELDS.index("decision") + 1 :],
    *ANNOTATION_FIELDS,
]


def read_candidate(path: Path) -> list[dict[str, str]]:
    """Лист на разметку → строки. Формат — по расширению: ``.xlsx`` или CSV.

    XLSX — рабочий формат разметчика (Excel: выпадающий список решений, фильтр,
    закреплённая шапка), CSV — формат обмена и диффа в git. Оба читаются здесь, чтобы
    `compile` и `batches` не знали, в чём именно размечали: иначе выбор формата протекал
    бы в валидатор и в отчёт.
    """
    if path.suffix.lower() == ".xlsx":
        return _read_xlsx(path)
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = [field for field in REQUIRED_FIELDS if field not in (reader.fieldnames or [])]
        if missing:
            raise SystemExit(f"{path}: нет колонок {missing}")
        return list(reader)


def read_moved(path: Path) -> list[dict[str, str]]:
    """Лист новых пар (``moved_quote``/``quote``) → строки: у него свой набор колонок."""
    if path.suffix.lower() == ".xlsx":
        return _read_xlsx(path, required=MOVED_FIELDS)
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = [field for field in MOVED_FIELDS if field not in (reader.fieldnames or [])]
        if missing:
            raise SystemExit(f"{path}: нет колонок {missing}")
        return list(reader)


def _openpyxl() -> object:
    """`openpyxl` — зависимость extra `extract` (её тянет Office-путь docling).

    Импорт ленивый и с внятным отказом: скрипт обязан работать в окружении без extra
    `extract` (CSV-путь), а не падать на импорте при запуске `build`/`validate`.
    """
    try:
        import openpyxl
    except ImportError as error:  # pragma: no cover - зависит от окружения
        raise SystemExit(
            "XLSX требует openpyxl (extra `extract`: uv sync --extra extract); "
            "без него используйте CSV"
        ) from error
    return openpyxl


def _read_xlsx(path: Path, *, required: list[str] | None = None) -> list[dict[str, str]]:
    """XLSX → строки листа. Первая строка — шапка, значения читаются как текст.

    Числа и пустые ячейки приводятся к строке: `compile` сравнивает `decision` со
    множеством значений и не должен зависеть от того, как Excel сохранил ячейку.
    ``required`` — обязательные колонки (у основного листа и у листа новых пар они разные).
    """
    fields = REQUIRED_FIELDS if required is None else required
    openpyxl = _openpyxl()
    book = openpyxl.load_workbook(path, read_only=True, data_only=True)  # type: ignore[attr-defined]
    try:
        sheet = book.worksheets[0]
        rows = sheet.iter_rows(values_only=True)
        try:
            header = [("" if cell is None else str(cell)).strip() for cell in next(rows)]
        except StopIteration:
            raise SystemExit(f"{path}: пустой лист") from None
        missing = [field for field in fields if field not in header]
        if missing:
            raise SystemExit(f"{path}: нет колонок {missing}")
        out: list[dict[str, str]] = []
        for values in rows:
            if not any(value not in (None, "") for value in values):
                continue
            row = {name: "" for name in header if name}
            for index, name in enumerate(header):
                if not name or index >= len(values):
                    continue
                value = values[index]
                row[name] = "" if value is None else str(value).strip()
            out.append(row)
        return out
    finally:
        book.close()


def _write_xlsx(path: Path, rows: list[dict[str, str]]) -> None:
    """Строки листа → XLSX, пригодный для разметки руками.

    Что делает файл рабочим, а не «CSV в обёртке»:

    * шапка закреплена и с фильтром — 102 строки иначе листаются вслепую;
    * колонка `decision` — выпадающий список ``accept``/``reject``: значение нельзя
      напечатать с опечаткой, а `compile` отказывает именно на неизвестном решении;
    * строки, где решение **не** механическое (адрес переехал, документ неоднозначен,
      совпадение не точное), залиты цветом — это те 14 строк, которые требуют человека;
    * `titles`/`claims`/`cited_from` переносятся по словам и имеют ширину: иначе
      подсказка обрезана и разметка превращается в угадывание.

    Второй лист — инструкция: что означает `accept` и что делать с переехавшими
    адресами. Она в файле, а не в переписке, потому что файл живёт дольше переписки.
    """
    openpyxl = _openpyxl()
    from openpyxl.styles import Alignment, Font, PatternFill  # noqa: PLC0415 - ленивый импорт
    from openpyxl.worksheet.datavalidation import DataValidation  # noqa: PLC0415

    book = openpyxl.Workbook()  # type: ignore[attr-defined]
    sheet = book.active
    sheet.title = "лист"
    sheet.append(ALL_FIELDS)
    for cell in sheet[1]:
        cell.font = Font(bold=True)
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = f"A1:{_column_letter(len(ALL_FIELDS))}1"

    # Ширины по назначению колонки, а не «по умолчанию»: адреса длинные, решения короткие.
    widths = {
        "product_id": 18,
        "param_ids": 40,
        "cells": 7,
        "source_url": 55,
        "status": 11,
        "match": 17,
        "corpus_url": 55,
        "doc_id": 30,
        "decision": 12,
        "comment": 34,
        "note": 30,
        "live_kind": 10,
        "live_code": 10,
        "live_url": 55,
        "titles": 45,
        "claims": 30,
        "cited_from": 55,
        "host_declared": 12,
        "quotes_found": 12,
        "quotes_total": 12,
        "quote_docs": 11,
        "quote_len": 10,
        "quote_sample": 60,
    }
    for index, name in enumerate(ALL_FIELDS, start=1):
        sheet.column_dimensions[_column_letter(index)].width = widths.get(name, 16)
    wrap = {
        "param_ids",
        "titles",
        "claims",
        "cited_from",
        "note",
        "comment",
        "doc_id",
        "quote_sample",
    }
    decision_index = ALL_FIELDS.index("decision") + 1

    needs_human = PatternFill("solid", start_color="FFF2CC")
    for row_index, row in enumerate(rows, start=2):
        sheet.append([row.get(name, "") for name in ALL_FIELDS])
        for index, name in enumerate(ALL_FIELDS, start=1):
            if name in wrap:
                sheet.cell(row=row_index, column=index).alignment = Alignment(
                    wrap_text=True, vertical="top"
                )
        if _needs_human(row):
            for index in range(1, len(ALL_FIELDS) + 1):
                sheet.cell(row=row_index, column=index).fill = needs_human

    validation = DataValidation(
        type="list",
        formula1=f'"{",".join(sorted(DECISIONS))}"',
        allow_blank=True,
        showDropDown=False,
    )
    validation.error = "решение — accept или reject; пусто = ещё не рассмотрено"
    validation.errorTitle = "Неизвестное решение"
    sheet.add_data_validation(validation)
    last = max(2, len(rows) + 1)
    validation.add(
        f"{_column_letter(decision_index)}2:{_column_letter(decision_index)}{last}"
    )

    guide = book.create_sheet("как размечать")
    for line in _GUIDE_LINES:
        guide.append([line])
    guide.column_dimensions["A"].width = 110
    for cell in guide["A"]:
        cell.alignment = Alignment(wrap_text=True, vertical="top")

    book.save(path)


#: Что означает решение. Лежит вторым листом книги: файл переживает переписку.
_GUIDE_LINES: tuple[str, ...] = (
    "Размечается ОДНА колонка — decision: accept или reject (выпадающий список). "
    "Пусто = ещё не рассмотрено. Рядом колонка comment — для ваших пометок; "
    "note — машинная (её пишет build), не правьте её: при пересборке листа она "
    "пересчитывается, а при правке на месте остаётся как была.",
    "",
    "accept значит: документ корпуса (corpus_url) — это тот самый документ, на который "
    "ссылается ячейка таблицы (source_url), и он должен попасть в top-K. Это утверждение "
    "про адрес источника, а не про то, нравится ли страница.",
    "",
    "titles/claims — что утверждает ячейка (параметр и записанное значение).",
    "cited_from — откуда цитата фактически взята в прогоне. Совпадения с corpus_url "
    "меньшинство: доказательство часто лежит в другом документе. Это справка; решение "
    "принимается по corpus_url.",
    "live_kind — живость адреса на дату замера: live / moved (переехал) / gone / auth.",
    "",
    "Залитые строки — те, где решение не механическое:",
    "  moved — адрес переехал. Тот же раздел, но другой текст: решает человек.",
    "  canonical_query (ambiguous) — две редакции одного раздела. При accept впишите в "
    "колонку doc_id один из двух (они в колонке note).",
    "  canonical — совпало после канонизации (фрагмент #..., хвостовой слэш, http/https, "
    "%кодирование). Обычно accept; reject — если фрагмент указывает на другой раздел.",
    "",
    "Дальше — одна команда (compile сам прогонит валидатор и откажет на нерезолвленном "
    "doc_id, чужом продукте и несовпавшем URL).",
)


def _column_letter(index: int) -> str:
    """1 → ``A``, 27 → ``AA`` (openpyxl такой утилиты в публичном API не даёт)."""
    letters = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters = chr(ord("A") + remainder) + letters
    return letters


#: Колонки листа «новые пары» (правила A/C). Отдельный лист, а не колонки основного:
#: строк, где решение не механическое, десятки, а в основном листе их 287 — читать
#: решение по ``moved_quote`` среди них значит листать вслепую.
MOVED_FIELDS = [
    "product_id",
    "source_url",
    "corpus_url",
    "match",
    "quotes_found",
    "quotes_total",
    "quote_docs",
    "quote_len",
    "quote_sample",
    "note",
    "decision",
    "comment",
]

#: Что означает решение в листе «новые пары». Второй лист книги — как у основного листа:
#: файл переживает переписку.
_MOVED_GUIDE_LINES: tuple[str, ...] = (
    "Это пары, которые НЕ разрешились по адресу, а разрешились (или не разрешились) по "
    "цитате. Решение — одна колонка decision: accept или reject (выпадающий список). "
    "Пусто = ещё не рассмотрено.",
    "",
    "moved_quote значит: все цитаты пары лежат буквально в одном документе (corpus_url), "
    "но это НЕ та страница, на которую ссылается таблица (source_url). Для retrieval это "
    "утверждение сильнее адресного — цитата в этом документе есть, — но это другое "
    "утверждение. Признак подстановки версии — в колонке note: version=7.1.1->8.1.0 — "
    "тот же хвост пути после сегмента версии (та же страница другой версии); "
    "version=tail_differs — другая страница того же продукта; version=none — на одной "
    "из сторон нет сегмента версии, сравнивать не с чем.",
    "",
    "ambiguous значит: цитаты пары нашлись, но не в одном документе (см. quotes_found/"
    "quotes_total и quote_docs) либо одна цитата лежит в нескольких документах. Такой "
    "строке нужен выбранный документ: впишите его doc_id в основной лист (колонка doc_id) "
    "и поставьте там accept — иначе accept здесь не к чему привязать.",
    "",
    "quote_len — длина САМОЙ КОРОТКОЙ цитаты пары: короткая цитата и даёт неоднозначность. "
    "Порог «слишком короткая» не вводится: распределение длин публикуется, решение — за "
    "человеком.",
    "",
    "Решения из этого листа переносятся в основной при следующем `annotate --moved-out`.",
)


def moved_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Только новые пары: ``moved_quote`` и цитатная ``ambiguous``."""
    return [
        row
        for row in rows
        if (row.get("match") or "") in {"moved_quote", "quote"}
    ]


def write_moved_sheet(path: Path, rows: list[dict[str, str]]) -> int:
    """Пишет лист «новые пары» (XLSX или CSV по расширению); возвращает число строк."""
    picked = moved_rows(rows)
    if path.suffix.lower() != ".xlsx":
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=MOVED_FIELDS, lineterminator="\n")
            writer.writeheader()
            for row in picked:
                writer.writerow({key: row.get(key, "") for key in MOVED_FIELDS})
        return len(picked)
    _write_moved_xlsx(path, picked)
    return len(picked)


def _write_moved_xlsx(path: Path, rows: list[dict[str, str]]) -> None:
    """XLSX для разметки новых пар: фильтр, список решений, перенос текста цитаты."""
    openpyxl = _openpyxl()
    from openpyxl.styles import Alignment, Font  # noqa: PLC0415 - ленивый импорт
    from openpyxl.worksheet.datavalidation import DataValidation  # noqa: PLC0415

    book = openpyxl.Workbook()  # type: ignore[attr-defined]
    sheet = book.active
    sheet.title = "новые пары"
    sheet.append(MOVED_FIELDS)
    for cell in sheet[1]:
        cell.font = Font(bold=True)
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = f"A1:{_column_letter(len(MOVED_FIELDS))}1"
    widths = {
        "product_id": 18,
        "source_url": 55,
        "corpus_url": 55,
        "match": 14,
        "quotes_found": 12,
        "quotes_total": 12,
        "quote_docs": 11,
        "quote_len": 10,
        "quote_sample": 70,
        "note": 40,
        "decision": 12,
        "comment": 34,
    }
    for index, name in enumerate(MOVED_FIELDS, start=1):
        sheet.column_dimensions[_column_letter(index)].width = widths.get(name, 14)
    wrap = {"source_url", "corpus_url", "quote_sample", "note", "comment"}
    for row_index, row in enumerate(rows, start=2):
        sheet.append([row.get(name, "") for name in MOVED_FIELDS])
        for index, name in enumerate(MOVED_FIELDS, start=1):
            if name in wrap:
                sheet.cell(row=row_index, column=index).alignment = Alignment(
                    wrap_text=True, vertical="top"
                )

    validation = DataValidation(
        type="list",
        formula1=f'"{",".join(sorted(DECISIONS))}"',
        allow_blank=True,
        showDropDown=False,
    )
    validation.error = "решение — accept или reject; пусто = ещё не рассмотрено"
    validation.errorTitle = "Неизвестное решение"
    sheet.add_data_validation(validation)
    decision = _column_letter(MOVED_FIELDS.index("decision") + 1)
    validation.add(f"{decision}2:{decision}{max(2, len(rows) + 1)}")

    guide = book.create_sheet("как размечать")
    for line in _MOVED_GUIDE_LINES:
        guide.append([line])
    guide.column_dimensions["A"].width = 110
    for cell in guide["A"]:
        cell.alignment = Alignment(wrap_text=True, vertical="top")
    book.save(path)


def _needs_human(row: dict[str, str]) -> bool:
    """Решение не механическое: переезд адреса, неоднозначность, неточное совпадение."""
    if (row.get("status") or "") != "resolved":
        return True
    if (row.get("live_kind") or "") == "moved":
        return True
    return (row.get("match") or "") != "exact"


def cmd_compile(args: argparse.Namespace) -> int:
    rows = read_candidate(Path(args.candidate))
    problems: list[str] = []
    entries: list[dict[str, object]] = []
    seen_cells: dict[str, int] = {}

    for lineno, row in enumerate(rows, start=2):
        decision = (row.get("decision") or "").strip()
        if decision not in DECISIONS:
            if decision:
                problems.append(f"строка {lineno}: неизвестное decision {decision!r}")
            continue
        if decision == "reject":
            continue
        # accept: строка обязана указывать документ — либо машинно разрешённый, либо
        # **вручную пришпиленный** человеком. Ручной пин нужен ровно там, где машина
        # честно не выбрала: ``ambiguous`` — два документа одного раздела (языковые
        # редакции ``?lang=en``/``?lang=ru-en``). Без него единственным записываемым
        # решением остаётся ``reject``, то есть ячейка выпадает из эталона из-за
        # ограничения прибора, а не из-за факта — а решение человека обязано быть
        # записываемым. Подмена не проходит молча: валидатор всё равно сверяет, что
        # ``doc_id`` резолвится в корпусе прогона, принадлежит продукту ячейки (I17) и
        # его URL совпадает с ``source_url`` (для ``canonical_query`` — без query).
        status = row.get("status")
        doc_id = (row.get("doc_id") or "").strip()
        if status == "foreign_host":
            # Прибор не выбирает документ за пределами объявленного хоста: строка не
            # «недоступна», она про чужой источник, и решение по ней — про таблицу.
            problems.append(
                f"строка {lineno}: decision=accept при status=foreign_host "
                f"({row.get('source_url')}) — таблица цитирует не документацию продукта; "
                "решение принимает человек по таблице, а не прибор"
            )
            continue
        if status != "resolved" and not (status == "ambiguous" and doc_id):
            problems.append(
                f"строка {lineno}: decision=accept при status={status!r} "
                f"({row.get('source_url')})"
                + (
                    " — у ambiguous впишите doc_id выбранного документа в колонку doc_id"
                    if status == "ambiguous"
                    else ""
                )
            )
            continue
        if not doc_id:
            problems.append(f"строка {lineno}: decision=accept без doc_id")
            continue
        match = row.get("match") or ""
        if match in {"moved_quote", "quote"} and not (row.get("corpus_url") or "").strip():
            # `moved_quote` — утверждение о документе, которого нет в `source_url`;
            # без его адреса запись не говорит ничего проверяемого.
            problems.append(
                f"строка {lineno}: match={match} без corpus_url ({row.get('source_url')})"
            )
            continue
        for param_id in (row.get("param_ids") or "").split(";"):
            param_id = param_id.strip()
            if not param_id:
                continue
            cell = f"{row['product_id']}/{param_id}"
            if cell in seen_cells:
                problems.append(
                    f"строка {lineno}: ячейка {cell} уже объявлена строкой {seen_cells[cell]}"
                )
                continue
            seen_cells[cell] = lineno
            entry = {
                "cell": cell,
                "product_id": row["product_id"],
                "param_id": param_id,
                "source_url": row["source_url"],
                "expected_doc_ids": [doc_id],
                "match": match,
            }
            if match in {"moved_quote", "quote"}:
                entry["corpus_url"] = (row.get("corpus_url") or "").strip()
            entries.append(entry)

    if problems:
        for problem in problems:
            print(f"ОТКАЗ: {problem}", file=sys.stderr)
        return 2
    if not entries:
        print(
            "ОТКАЗ: ни одна строка не помечена decision=accept — эталон пуст",
            file=sys.stderr,
        )
        return 2

    entries.sort(key=lambda item: (str(item["product_id"]), str(item["param_id"])))
    payload = {
        "format": GOLDEN_FORMAT,
        "case_id": args.case_id,
        "source": args.source,
        "corpus_run_id": args.run_id,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "entries": entries,
    }
    out = Path(args.out)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"written: {out} ({len(entries)} entries)")

    # Компиляция без проверки бессмысленна: сразу гоним валидатор.
    args.golden = str(out)
    return cmd_validate(args)


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------
def _check_schema(payload: object) -> tuple[list[str], list[dict]]:
    problems: list[str] = []
    if not isinstance(payload, dict):
        return ["верхний уровень — не объект"], []
    for field in ("format", "case_id", "source", "corpus_run_id", "entries"):
        if field not in payload:
            problems.append(f"нет поля {field}")
    if payload.get("format") != GOLDEN_FORMAT:
        problems.append(f"format={payload.get('format')!r}, ожидается {GOLDEN_FORMAT!r}")
    entries = payload.get("entries")
    if not isinstance(entries, list):
        problems.append("entries — не список")
        return problems, []
    expected_keys = {
        "cell",
        "product_id",
        "param_id",
        "source_url",
        "expected_doc_ids",
        "match",
    }
    #: Необязательное поле: адрес документа, в котором буквально лежит цитата. Обязательно
    #: для ``moved_quote``/``quote`` — там документ **не** тот, на который ссылается
    #: таблица, и без адреса запись не говорит ничего проверяемого.
    optional_keys = {"corpus_url"}
    quote_matches = {"moved_quote", "quote"}
    for index, entry in enumerate(entries):
        where = f"entries[{index}]"
        if not isinstance(entry, dict):
            problems.append(f"{where}: не объект")
            continue
        extra = set(entry) - expected_keys - optional_keys
        absent = expected_keys - set(entry)
        if extra:
            problems.append(f"{where}: лишние поля {sorted(extra)}")
        if absent:
            problems.append(f"{where}: нет полей {sorted(absent)}")
            continue
        match = entry["match"]
        if match == "foreign_host":
            problems.append(
                f"{where}: match=foreign_host не может входить в эталон "
                "(таблица цитирует не документацию продукта)"
            )
        if match in quote_matches and not str(entry.get("corpus_url") or "").strip():
            problems.append(f"{where}: match={match} без corpus_url")
        doc_ids = entry["expected_doc_ids"]
        if not isinstance(doc_ids, list) or not doc_ids:
            problems.append(f"{where}: expected_doc_ids пуст")
        for field in ("cell", "product_id", "param_id", "source_url"):
            if not isinstance(entry[field], str) or not entry[field].strip():
                problems.append(f"{where}: {field} пуст")
    return problems, entries


def validate_payload(payload: object, corpus: CorpusIndex) -> list[str]:
    problems, entries = _check_schema(payload)
    if problems:
        return problems
    seen: dict[str, int] = {}
    for index, entry in enumerate(entries):
        where = f"entries[{index}] {entry.get('cell')}"
        cell = entry["cell"]
        if cell in seen:
            problems.append(f"{where}: дубль ячейки (первая — entries[{seen[cell]}])")
        seen[cell] = index
        expected_cell = f"{entry['product_id']}/{entry['param_id']}"
        if cell != expected_cell:
            problems.append(f"{where}: cell не совпадает с {expected_cell}")
        for doc_id in entry["expected_doc_ids"]:
            if not isinstance(doc_id, str) or not doc_id:
                problems.append(f"{where}: пустой doc_id")
                continue
            doc_url = corpus.doc_url(doc_id)
            if doc_url is None:
                problems.append(
                    f"{where}: doc_id {doc_id} не резолвится в corpus_docs "
                    f"(прогон {entry.get('corpus_run_id', '?')})"
                )
                continue
            product = corpus.doc_product(doc_id)
            if product != entry["product_id"]:
                problems.append(
                    f"{where}: doc_id {doc_id} принадлежит {product!r}, "
                    f"ячейка — {entry['product_id']!r} (I17)"
                )
            source = canonical_url(entry["source_url"])
            actual = canonical_url(doc_url)
            if entry.get("match") == "canonical_query":
                source = canonical_url(entry["source_url"], drop_query=True)
                actual = canonical_url(doc_url, drop_query=True)
            if entry.get("match") in {"moved_quote", "quote"}:
                # Здесь документ **по построению** не тот, на который ссылается таблица:
                # утверждение — «цитата лежит буквально в нём». Проверяется не равенство
                # адресов, а что `corpus_url` ведёт в тот же документ и в тот же продукт
                # (I17): иначе запись перестала бы быть проверяемой.
                corpus_url = str(entry.get("corpus_url") or "")
                candidates, _level = corpus.docs.resolve(entry["product_id"], corpus_url)
                if doc_id not in candidates:
                    problems.append(
                        f"{where}: corpus_url {corpus_url} не резолвится в документ "
                        f"{doc_id} продукта {entry['product_id']!r} (I17)"
                    )
                if canonical_url(corpus_url) != canonical_url(doc_url):
                    problems.append(
                        f"{where}: corpus_url {corpus_url} не совпадает с адресом "
                        f"документа {doc_url}"
                    )
                continue
            if actual != source:
                problems.append(
                    f"{where}: URL документа {doc_url} не совпадает с source_url "
                    f"{entry['source_url']} (match={entry.get('match')!r})"
                )
    return problems


def cmd_validate(args: argparse.Namespace) -> int:
    golden = Path(args.golden)
    payload = json.loads(golden.read_text(encoding="utf-8"))
    corpus = CorpusIndex(Path(args.run_db))
    try:
        problems = validate_payload(payload, corpus)
    finally:
        corpus.close()
    if problems:
        for problem in problems:
            print(f"ОТКАЗ: {problem}", file=sys.stderr)
        print(f"{golden}: {len(problems)} проблем(ы)", file=sys.stderr)
        return 2
    entries = payload["entries"]
    print(f"OK: {golden}: {len(entries)} записей, все doc_id резолвятся")
    return 0


# ---------------------------------------------------------------------------
# batches
# ---------------------------------------------------------------------------
def _batch_status(row: dict[str, str]) -> str:
    status = row["status"]
    note = (row.get("note") or "").strip()
    if status == "resolved":
        return "resolved" if row.get("match") == "exact" else f"resolved ({row['match']})"
    if status == "missing" and note:
        reason = note.split(":", 1)[0].split(";", 1)[0]
        return f"missing ({reason})"
    return status


def _batch_live(row: dict[str, str]) -> str:
    """Живость адреса из ячейки — короткой пометкой для человека.

    Колонка нужна, чтобы разметчик не сверялся с 54-килобайтным JSON: решение
    ``accept``/``reject`` зависит от того, жив ли адрес, и «переехал» от «умер»
    отличается последствием. ``moved`` называет конечный адрес: подмена адреса должна
    быть видна в таблице, а не только в CSV.
    """
    kind = (row.get("live_kind") or "").strip()
    if not kind:
        return "—"
    code = (row.get("live_code") or "").strip()
    label = f"{kind} {code}".strip() if code else kind
    final = (row.get("live_url") or "").strip()
    if kind == "moved" and final and final != row["source_url"]:
        return f"{label} → {final}"
    return label


def _batch_cited(row: dict[str, str]) -> str:
    """Откуда фактически взята цитата: ``= объявлено``, иначе адрес(а) или «—».

    ``= объявлено`` печатается вместо повторения адреса: совпадение — это факт, и он
    должен читаться мгновенно. Несколько адресов — через ``<br>``, иначе markdown-строка
    распухает и таблица перестаёт читаться.
    """
    cited = (row.get("cited_from") or "").strip()
    if not cited:
        return "—"
    if cited == (row.get("corpus_url") or "").strip():
        return "= объявлено"
    return "<br>".join(part.strip() for part in cited.split(" ; ") if part.strip()).replace(
        "|", "\\|"
    )


def _batch_claim(row: dict[str, str]) -> str:
    """Утверждение ячейки для таблицы: «параметр: значение», иначе «—».

    Печатается только когда аннотация есть (`annotate`); вертикальная черта внутри
    текста заменяется, иначе она разрежет markdown-таблицу.
    """
    titles = (row.get("titles") or "").strip()
    claims = (row.get("claims") or "").strip()
    if not titles and not claims:
        return "—"
    text = f"{titles}: {claims}" if titles and claims else (titles or claims)
    return text.replace("|", "\\|")


def cmd_annotate(args: argparse.Namespace) -> int:
    """Дописывает в лист утверждение ячейки: ``titles`` и ``claims``.

    Зачем. В листе на разметку были только адреса: разметчик видел URL и документ
    корпуса, но не видел, **что** утверждается цитатой, — а решение ``accept`` означает
    именно «утверждение этой ячейки взято с этого документа». Без текста утверждения
    сверять нечего, и разметка превращается в сверку похожести адресов.

    Откуда берётся. ``golden_00_cells.csv`` — исходная comparative table: ``excel_title``
    («что за параметр») и ``raw_value`` («что записано в ячейке»). Присоединение — по
    ``(product_id, param_id)``, по **всем** ``param_ids`` строки (страница цитируется
    несколькими ячейками, и решение принимается по всем сразу).

    Чего не делает. Не решает и не додумывает: ячейка, которой нет в исходной таблице,
    остаётся с пустыми колонками. Многострочный текст сводится к одной строке и
    обрезается по ``MAX_ANNOTATION_CHARS`` — колонка читается глазами в таблице, а полный
    текст лежит в исходном CSV.
    """
    rows = read_candidate(Path(args.candidate))
    # Строки `missing` не размечаются: документа нет в корпусе, и решение по ним было бы
    # решением о покрытии, а не об эталоне. Раньше это отсекалось руками — то есть
    # правило жило в переписке, а не в приборе. `--keep-missing` возвращает их, если
    # нужен полный машинный отчёт в том же формате.
    total = len(rows)
    if not args.keep_missing:
        rows = [row for row in rows if (row.get("status") or "") != "missing"]
    cells = _read_cells(Path(args.cells))
    cited = _read_citations(Path(args.run_db)) if args.run_db else {}
    missing: list[str] = []
    for row in rows:
        titles: list[str] = []
        claims: list[str] = []
        for param_id in (row.get("param_ids") or "").split(";"):
            param_id = param_id.strip()
            if not param_id:
                continue
            cell = cells.get((row["product_id"], param_id))
            if cell is None:
                # Имя, а не только счётчик: «1 пара не нашлась» не говорит, какую править.
                missing.append(f"{row['product_id']}/{param_id}")
                continue
            titles.append(_one_line(cell.get("excel_title") or ""))
            claims.append(_one_line(cell.get("raw_value") or ""))
        row["titles"] = " | ".join(t for t in titles if t)
        row["claims"] = " | ".join(c for c in claims if c)
        urls: list[str] = []
        for param_id in (row.get("param_ids") or "").split(";"):
            for url in cited.get((row["product_id"], param_id.strip()), []):
                if url not in urls:
                    urls.append(url)
        row["cited_from"] = " ; ".join(urls)
    out = Path(args.out)
    carried = _carry_decisions(out, rows)
    moved_out = Path(args.moved_out) if getattr(args, "moved_out", None) else None
    carried_moved = 0
    if moved_out is not None:
        carried_moved, conflicts = _carry_moved(moved_out, rows)
        for conflict in conflicts:
            print(f"ВНИМАНИЕ: решение в двух листах расходится: {conflict}", file=sys.stderr)
    if out.suffix.lower() == ".xlsx":
        _write_xlsx(out, rows)
    else:
        fields = list(ALL_FIELDS)
        with out.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
            writer.writeheader()
            for row in rows:
                writer.writerow({key: row.get(key, "") for key in fields})
    print(f"written: {out} ({len(rows)} строк, колонки {', '.join(ANNOTATION_FIELDS)})")
    if len(rows) != total:
        print(
            f"отброшено {total - len(rows)} строк status=missing — документов нет в корпусе; "
            "это провал покрытия, а не эталона (вернуть: --keep-missing)"
        )
    if carried:
        print(f"сохранено решений из прежнего листа: {carried} (decision/comment)")
    if carried_moved:
        print(f"сохранено решений из листа новых пар: {carried_moved}")
    if moved_out is not None:
        written = write_moved_sheet(moved_out, rows)
        print(
            f"written: {moved_out} ({written} новых пар: moved_quote и цитатная ambiguous)"
        )
    if args.run_db:
        with_citation = sum(1 for row in rows if row.get("cited_from"))
        print(f"цитата есть у {with_citation} из {len(rows)} строк (адрес — в cited_from)")
    if missing:
        print(
            f"ВНИМАНИЕ: {len(missing)} пар (product, param_id) нет в исходной таблице — "
            "колонки-подсказки у них пусты: " + ", ".join(missing),
            file=sys.stderr,
        )
    return 0


#: Потолок длины подсказки в листе: колонка читается глазами, полный текст — в исходном CSV.
MAX_ANNOTATION_CHARS = 160


def _carry_moved(path: Path, rows: list[dict[str, str]]) -> tuple[int, list[str]]:
    """Переносит решения человека из листа новых пар в основной лист.

    Лист новых пар — не украшение: разметчик ставит решение там, где строк десятки, а не
    сотни, и решение обязано доехать до `compile`. Ключ тот же — ``(product, source_url)``.
    Расхождение с решением основного листа не перетирается молча: оно возвращается списком
    и печатается.
    """
    if not path.exists():
        return 0, []
    try:
        previous = read_moved(path)
    except SystemExit:
        return 0, []
    by_key = {(row["product_id"], row["source_url"]): row for row in previous}
    carried = 0
    conflicts: list[str] = []
    for row in rows:
        old = by_key.get((row["product_id"], row["source_url"]))
        if old is None:
            continue
        decision = (old.get("decision") or "").strip()
        if not decision:
            continue
        if (row.get("decision") or "").strip():
            if row["decision"] != decision:
                conflicts.append(f"{row['product_id']} {row['source_url']}")
            continue
        row["decision"] = decision
        comment = (old.get("comment") or "").strip()
        if comment:
            row["comment"] = comment
        carried += 1
    return carried, conflicts


def _carry_decisions(out: Path, rows: list[dict[str, str]]) -> int:
    """Переносит решения человека из прежнего листа в новый; возвращает их число.

    Зачем. `annotate` пересобирает подсказки после смены корпуса или прибора, и без
    переноса повторный запуск стёр бы разметку — работу, которую машина не повторяет.
    Ключ — ``(product_id, source_url)``: он же единица листа. Строка, которой в новом
    листе нет, просто не переносится (её документ выпал из корпуса) — и это видно по
    счётчику, а не молча.

    Переносится ``decision`` и ``comment``. ``doc_id`` — только у строк, где машина его не
    выбрала (``status != resolved``): там его вписал человек (выбор редакции при
    ``ambiguous``), и потерять его значит потерять решение. У ``resolved`` значение
    пересчитывается `build` заново, и перенос старого закрепил бы устаревший адрес.

    ``note`` здесь **не** трогается: он машинный, и его источник — `build`. При правке
    листа на месте (кандидат = выход) он просто читается и записывается обратно; при
    пересборке из свежего `build` он пересчитывается вместе со всей строкой. Поэтому
    правило для разметчика — «`note` не править», а не «`annotate` его обновит».
    """
    if not out.exists():
        return 0
    try:
        previous = read_candidate(out)
    except SystemExit:
        return 0
    by_key = {(row["product_id"], row["source_url"]): row for row in previous}
    carried = 0
    for row in rows:
        old = by_key.get((row["product_id"], row["source_url"]))
        if old is None:
            continue
        for field in HUMAN_FIELDS:
            value = (old.get(field) or "").strip()
            if value:
                row[field] = value
        if (old.get("status") or "") != "resolved":
            pinned = (old.get("doc_id") or "").strip()
            if pinned:
                row["doc_id"] = pinned
        if (row.get("decision") or "").strip() or (row.get("comment") or "").strip():
            carried += 1
    return carried


def _one_line(text: str) -> str:
    """Многострочный текст ячейки → одна строка, обрезанная по ``MAX_ANNOTATION_CHARS``."""
    collapsed = " ".join(str(text).split())
    if len(collapsed) <= MAX_ANNOTATION_CHARS:
        return collapsed
    return collapsed[: MAX_ANNOTATION_CHARS - 1] + "…"


def _read_citations(run_db: Path) -> dict[tuple[str, str], list[str]]:
    """``(product_id, param_id)`` → адреса документов, из которых **фактически** взята цитата.

    Зачем это в листе. Эталон объявляет один документ на ячейку — `source_url` из
    comparative table. Оценщик же берёт цитату из любого документа корпуса, и адрес
    цитаты виден в `cell_evidence` → `corpus_chunks` (там `content_hash` **чанка**;
    `corpus_docs.content_hash` не подходит — это хеш документа, и join по нему даёт ноль).

    Расхождение — не дефект: ячейка таблицы и документ корпуса адресуют одно и то же
    утверждение по-разному, и разметчику нужно видеть оба, иначе `accept` превращается в
    сверку похожести адресов. Само решение всё равно за человеком.
    """
    conn = sqlite3.connect(f"file:{run_db}?mode=ro", uri=True)
    try:
        by_chunk = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT content_hash, url FROM corpus_chunks WHERE url IS NOT NULL"
            )
        }
        out: dict[tuple[str, str], list[str]] = defaultdict(list)
        for product_id, param_id, chunk_hash in conn.execute(
            "SELECT product_id, param_id, content_hash FROM cell_evidence"
            " WHERE citation_quote IS NOT NULL AND citation_quote != ''"
        ):
            url = by_chunk.get(chunk_hash)
            if url is None:
                continue
            key = (product_id, param_id)
            if url not in out[key]:
                out[key].append(url)
        return out
    finally:
        conn.close()


def _read_cells(path: Path) -> dict[tuple[str, str], dict[str, str]]:
    """Исходная comparative table: ``(product_id, param_id)`` → строка ячейки."""
    with path.open(encoding="utf-8", newline="") as handle:
        return {
            (row["product_id"], row["param_id"]): row for row in csv.DictReader(handle)
        }


def cmd_batches(args: argparse.Namespace) -> int:
    rows = read_candidate(Path(args.candidate))
    by_product: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_product[row["product_id"]].append(row)
    for product_id in sorted(by_product):
        product_rows = sorted(by_product[product_id], key=lambda r: r["source_url"])
        for start in range(0, len(product_rows), BATCH_SIZE):
            batch = product_rows[start : start + BATCH_SIZE]
            print(f"### {product_id} — пачка {start // BATCH_SIZE + 1} "
                  f"({len(batch)} строк)")
            print()
            annotated = any(row.get("claims") or row.get("titles") for row in batch)
            cited = any((row.get("cited_from") or "").strip() for row in batch)
            header = ["URL из ячейки", "адрес", "документ в корпусе"]
            if annotated:
                header.append("что утверждается")
            if cited:
                header.append("цитата фактически из")
            header += ["ячеек", "статус"]
            print("| " + " | ".join(header) + " |")
            print("|" + "---|" * len(header))
            for row in batch:
                doc = row.get("corpus_url") or "—"
                cells = [row["source_url"], _batch_live(row), doc]
                if annotated:
                    cells.append(_batch_claim(row))
                if cited:
                    cells.append(_batch_cited(row))
                cells += [row["cells"], _batch_status(row)]
                print("| " + " | ".join(cells) + " |")
            print()
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build", help="URL → документ по БД прогона")
    build.add_argument("--cells", required=True)
    build.add_argument("--run-db", required=True)
    build.add_argument("--out", required=True)
    build.add_argument(
        "--passports-dir",
        required=True,
        help="каталог паспортов: объявленные doc_domains продукта — правило C "
        "(только объявленный хост). Обязателен: без него правило C не исполняется",
    )
    build.add_argument(
        "--quotes",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="правило A (разрешение по цитате); --no-quotes воспроизводит прежнюю "
        "классификацию — адрес и ничего больше",
    )
    build.add_argument(
        "--liveness",
        default=None,
        help="файл замера живости адресов (URL→{code,final_url,kind}); "
        "без него колонки live_* пусты",
    )
    build.set_defaults(func=cmd_build)

    annotate = sub.add_parser(
        "annotate", help="дописать в лист утверждение ячейки (titles/claims)"
    )
    annotate.add_argument("--candidate", required=True)
    annotate.add_argument("--cells", required=True)
    annotate.add_argument("--out", required=True)
    annotate.add_argument(
        "--keep-missing",
        action="store_true",
        help="не отбрасывать строки status=missing (по умолчанию они не размечаются)",
    )
    annotate.add_argument(
        "--run-db",
        default=None,
        help="БД прогона: добавит cited_from — адрес документа, из которого фактически "
        "взята цитата (без него колонка пуста)",
    )
    annotate.add_argument(
        "--moved-out",
        default=None,
        help="куда писать лист новых пар (moved_quote и цитатная ambiguous): "
        "XLSX или CSV по расширению",
    )
    annotate.set_defaults(func=cmd_annotate)

    compile_ = sub.add_parser("compile", help="кандидатный CSV → JSON-эталон")
    compile_.add_argument("--candidate", required=True)
    compile_.add_argument("--out", required=True)
    compile_.add_argument("--run-db", required=True)
    compile_.add_argument("--case-id", default="ru-pg-dbms")
    compile_.add_argument(
        "--source", default="packages/ru-pg-dbms/sources/golden_00_cells.csv"
    )
    compile_.add_argument("--run-id", default="")
    compile_.set_defaults(func=cmd_compile)

    validate = sub.add_parser("validate", help="проверить JSON-эталон против БД")
    validate.add_argument("--golden", required=True)
    validate.add_argument("--run-db", required=True)
    validate.set_defaults(func=cmd_validate)

    batches = sub.add_parser("batches", help="пачки на подтверждение (markdown)")
    batches.add_argument("--candidate", required=True)
    batches.set_defaults(func=cmd_batches)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
