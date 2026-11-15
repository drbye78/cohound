"""Разрешение «URL ↔ документ корпуса» — одно определение на эталон и измерение.

**Зачем отдельный модуль.** Документный эталон (``scripts/prepare_retrieval_golden_docs.py``)
и будущее измерение документного recall@K задают один и тот же вопрос: «тот же это
документ?». Пока правило канонизации лежало в скрипте, у измерения была бы вторая копия,
и две копии разъезжаются молча — тогда эталон и измерение отвечают на один вопрос
по-разному, а расхождение выглядит как провал ретривала, а не как расхождение прибора.
Поэтому правило живёт здесь, в нейтральном слое, и оба потребителя берут его отсюда.

**Что не меняет документ по HTTP-семантике** (снимается канонизацией): регистр схемы и
хоста, порт по умолчанию, фрагмент (якорь внутри страницы), повторные и хвостовой ``/``,
percent-кодирование пути. ``http`` и ``https`` считаются одной формой: корпус пишет
``https``, comparative table иногда ``http``.

**Уровни совпадения** — от сильного к слабому:

``exact``
    URL документа буквально равен запрошенному. Спорить не о чем.
``canonical``
    Совпало после канонизации. Обычно тот же документ; фрагмент ``#...`` мог указывать
    на другой раздел, поэтому это утверждение сильнее, чем кажется, но не буквальное.
``canonical_query``
    Совпало, если отбросить query-строку. Самый слабый уровень, потому что query бывает
    двух разных природ: ``?h=...`` — подсветка внутри той же страницы (документ тот же), а
    ``?lang=ru`` — **языковая редакция** (текст другой). Отличить одно от другого без
    чтения страницы нельзя, поэтому такой матч не выдаётся за точный: выбор между
    кандидатами — решение вызывающего (человек в эталоне, отказ или «нерезолвленные» в
    измерении).

**Владение соединением.** :class:`DocIndex` принимает готовое ``sqlite3.Connection`` и
никогда его не закрывает: владелец — вызывающий (скрипт открывает read-only соединение на
время команды, прибор — своё). Класс только читает ``corpus_docs``; схему не создаёт и
не проверяет.

Нейтрален к предметной категории (§1.3): ни названий продуктов, ни вендоров.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from urllib.parse import unquote, urlsplit, urlunsplit

__all__ = [
    "DocIndex",
    "canonical_url",
    "host_key",
    "root_matches",
]


def canonical_url(url: str, *, drop_query: bool = False) -> str:
    """Каноническая форма URL для сопоставления с корпусом.

    Снимает только то, что не меняет документ по HTTP-семантике: регистр схемы и
    хоста, порт по умолчанию, фрагмент (якорь внутри страницы), повторные и хвостовой
    ``/``, percent-кодирование пути. ``http`` и ``https`` считаются одной формой:
    корпус пишет ``https``, comparative table иногда ``http``.

    ``drop_query`` — отдельный, более грубый уровень: query-строка может быть как
    подсказкой подсветки (``?h=...``), так и языковой редакцией (``?lang=ru``).
    Поэтому такой матч помечается ``canonical_query`` и уходит человеку на проверку.
    """
    parts = urlsplit(url.strip())
    host = (parts.hostname or "").lower()
    port = parts.port
    netloc = host if port in (None, 80, 443) else f"{host}:{port}"
    path = unquote(parts.path) or "/"
    while "//" in path:
        path = path.replace("//", "/")
    if len(path) > 1 and path.endswith("/"):
        path = path[:-1]
    query = "" if drop_query else parts.query
    return urlunsplit(("https", netloc, path, query, ""))


def host_key(url: str) -> str:
    """Хост без ``www.`` — так ``www.`` и голый хост считаются одной формой."""
    host = (urlsplit(url.strip()).hostname or "").lower()
    return host[4:] if host.startswith("www.") else host


def root_matches(url: str, root: str) -> bool:
    """Лежит ли URL под объявленным корнем (хост + префикс пути по сегменту).

    Семантика та же, что у краула (AGENTS §6): поддомен не наследует право обхода,
    поэтому хост сравнивается точно (с эквивалентностью ``www.``), а префикс пути —
    только на границе сегмента.
    """
    if host_key(url) != host_key(root):
        return False
    root_path = urlsplit(root.strip()).path or "/"
    if not root_path.endswith("/"):
        root_path += "/"
    path = urlsplit(url.strip()).path or "/"
    if not path.endswith("/"):
        path += "/"
    return path == root_path or path.startswith(root_path)


class DocIndex:
    """Документы прогона и их URL, прочитанные точечно (``corpus_docs`` — тысячи строк).

    Строит три индекса по ``(product_id, ключ)``: буквальный URL (``exact``),
    канонический URL (``canonical``) и канонический без query (``canonical_query``).
    В ``canonical``/``canonical_query`` значение — пары ``(url, doc_id)``: вызывающему
    нужен не только документ, но и его собственный адрес (эталон публикует его как
    ``corpus_url``).

    Соединение чужое: класс его не открывает и не закрывает (см. докстринг модуля).
    ``row_factory`` вызывающего не важен — строки читаются по позиции.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self.exact: dict[tuple[str, str], list[str]] = defaultdict(list)
        self.canonical: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
        self.canonical_noquery: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(
            list
        )
        for product_id, url, doc_id in conn.execute(
            "SELECT product_id, url, doc_id FROM corpus_docs"
        ):
            self.exact[(product_id, url.strip())].append(doc_id)
            self.canonical[(product_id, canonical_url(url))].append((url, doc_id))
            self.canonical_noquery[(product_id, canonical_url(url, drop_query=True))].append(
                (url, doc_id)
            )

    def resolve(self, product_id: str, url: str) -> tuple[list[str], str]:
        """Документы продукта для URL и уровень совпадения.

        Возвращает **все** кандидаты: несколько документов на одном уровне — не ошибка
        прибора, а факт корпуса (две языковые редакции одного раздела). Что делать с
        неоднозначностью, решает вызывающий: эталон спрашивает человека, измерение
        отказывается угадывать. Уровень — ``exact`` | ``canonical`` |
        ``canonical_query``; при отсутствии документов — пустой список и ``""``.
        """
        exact = self.exact.get((product_id, url.strip()), [])
        if exact:
            return list(exact), "exact"
        canonical = self.canonical.get((product_id, canonical_url(url)), [])
        if canonical:
            return [doc_id for _, doc_id in canonical], "canonical"
        noquery = self.canonical_noquery.get(
            (product_id, canonical_url(url, drop_query=True)), []
        )
        if noquery:
            return [doc_id for _, doc_id in noquery], "canonical_query"
        return [], ""

    def doc_url(self, doc_id: str) -> str | None:
        """Адрес документа по ``doc_id``; ``None`` — такого документа в корпусе нет."""
        row = self._conn.execute(
            "SELECT url FROM corpus_docs WHERE doc_id = ?", (doc_id,)
        ).fetchone()
        return None if row is None else str(row[0])

    def doc_product(self, doc_id: str) -> str | None:
        """Продукт документа по ``doc_id``; ``None`` — документа нет (I17 проверяет URL)."""
        row = self._conn.execute(
            "SELECT product_id FROM corpus_docs WHERE doc_id = ?", (doc_id,)
        ).fetchone()
        return None if row is None else str(row[0])
