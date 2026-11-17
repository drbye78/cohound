#!/usr/bin/env python3
"""Сколько семантической смежности теряет нарезка на живом корпусе (план 26, O9).

Зачем прибор. `tests/adapters/test_chunker_semantics.py` доказал, что четыре из пяти
семантических свойств нарезки нарушены, — но доказал **на четырёх фикстурах**. Фикстура
показывает, что дефект возможен, и ничего не говорит о том, как часто он встречается:
правка нарезки — это `execution_change` (новый chunk-set ⇒ новый `run_id` ⇒
пере-эмбеддинг), поэтому цена решения зависит от **частоты**, а не от существования
дефекта. Прибор считает частоту на готовом прогоне: чанки читаются из `corpus_chunks`
одним проходом `ORDER BY doc_id, chunk_no` (индекса по `doc_id` в run-DB нет, поэтому
запрос на документ — полный скан 253k строк; так уже теряли 120 с на 40 документах).

**Каждый детектор — прокси, и это главное, что о нём надо знать.** Детектор видит
**только границу двух соседних чанков** и текст в них; он не видит исходного документа,
не знает, был ли разрез осмысленным, и не отличает «связь потеряна» от «связь потеряна,
но восстанавливается из `context_prefix`». Поэтому публикуются не только счётчики, но и
**примеры** (человек оценивает ложные срабатывания сам) и **уточнения** — разбивка
срабатываний на классы, часть которых дефектом не является. Число детектора — оценка
сверху, а не приговор нарезке.

Что прибор **не** измеряет:

* **D4 «отрицание оторвано от предмета» — не измерен.** Честного текстового прокси для
  него нет: дефект из фикстуры состоит в том, что отрицание осталось в предыдущем чанке,
  а глагол открыл следующий, — и в данных чанка **нет ни одного признака**, помечающего
  открывающий глагол как нуждающийся в отрицании. Прокси «последний токен чанка — частица
  `не`/`ни`/`not`/`no`» видит только разрез **между частицей и её глаголом**; он звучит,
  но измеряет другой (более узкий) класс, и потому публикуется отдельной строкой с явной
  пометкой. Вместо числа по D4 прибор публикует **необходимое условие** класса: сколько
  разрезов вообще попадает **внутрь предложения** (`sentence_internal_cuts`) — без такого
  разреза дефект невозможен, но далеко не каждый такой разрез его создаёт. Это верхняя
  граница класса, а не его размер.
* **качество нарезки** (хороша ли она) — это O19/O14, здесь считаются нарушения
  названных свойств;
* **что фикс улучшит retrieval** — цена пере-эмбеддинга считается как **верхняя граница**
  (см. `--price`), а эффект на выдачу требует отдельного замера.

Запуск (стенд, только чтение; гейт хранилища не используется — файл старой схемы):

    .venv/bin/python scripts/measure_chunk_adjacency.py \
      --db ~/.cohound/stands/ru-pg-dbms/runs/ru-pg-dbms-145ad66ca3f9/cohound.db \
      --run-id ru-pg-dbms-145ad66ca3f9 \
      --out-json docs/reports/measurements/2026-11-17-chunk-adjacency.json
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

#: Пропускная способность эмбеддера `bge-m3`, `cuda`/`fp16`, батч 32 (продакшен-путь).
#: Замер 16.11.2026, дымовой срез 3000 чанков, `--power-mode ac-barrel` — план 26 §O19
#: (`docs/architecture/26-open-review-items-plan.md`, «Тот же срез пере-мерен на постоянном
#: питании»). Отдельного файла замера `2026-11-16-embedding-throughput.md` **нет** — числа
#: живут в тексте плана, и это названо в отчёте, а не выдано за опубликованный прибор.
EMBED_CHUNKS_PER_S_AC = 8.16
#: Тот же срез на USB-C-питании: 438.0 с на 3000 чанков = 6.85 чанк/с (план 26 §O19).
#: Режим питания программно не отличим, поэтому оба числа публикуются рядом (AGENTS §1.1).
EMBED_CHUNKS_PER_S_USB_C = 6.85

#: Сколько примеров на детектор печатать/писать.
DEFAULT_EXAMPLES = 5
#: Длина обрывка текста в примере: два обрывка по 60 символов ≈ 120 на пример.
SNIPPET_CHARS = 60
#: Порог «строка похожа на заголовок» для D2 — назван в докстринге детектора.
HEADING_MAX_CHARS = 80

_BULLETS = ("- ", "* ", "+ ", "•", "–", "—")
_NUMBERED = re.compile(r"^\d{1,2}[.)]\s")
_ATX = re.compile(r"^#{1,6}\s")
_QUOTE = re.compile(r"^(?:>\s*)+")
#: Знаки, которыми заканчивается утверждение; их отсутствие — признак «строка не закончена».
TERMINAL_CHARS = frozenset(".!?;:,…\"»)")
#: Частицы отрицания: прокси D4 (см. докстринг `detect_dangling_negation`).
NEGATION_TOKENS = frozenset({"не", "ни", "not", "no"})
#: С чего начинается примечание (D3). Сравнение идёт по началу строки после снятия `>` и
#: разметки, поэтому список — это префиксы, а не точные строки.
NOTE_PREFIXES = ("примечание", "внимание", "note", "nb:", "nb.", "nb ", "n.b.")


@dataclass(frozen=True, slots=True)
class ChunkRow:
    """Чанк в том виде, в каком он нужен детекторам: адрес + текст + контекст."""

    doc_id: str
    chunk_no: int
    text: str
    context_prefix: str


@dataclass(frozen=True, slots=True)
class Hit:
    """Срабатывание детектора: адрес пары, разбивка на классы и вес срабатывания.

    ``primary=False`` — детектор сработал, но по **второй** (рыхлой) ветке, которая
    дефектом не признана: счётчик публикуется отдельно, в цену и в объединение задетых
    чанков такое срабатывание не входит. Так сделано у D2: «ATX-заголовок в хвосте» и
    «короткая строка без точки» — разные утверждения, и складывать их значило бы выдать
    одно за другое.
    """

    prev: ChunkRow
    cur: ChunkRow
    flags: dict[str, bool]
    primary: bool = True


# --------------------------------------------------------------------------- #
# Разбор строк
# --------------------------------------------------------------------------- #


def nonempty_lines(text: str) -> list[str]:
    """Непустые строки без ведущих/хвостовых пробелов (порядок сохраняется)."""
    return [line.strip() for line in text.splitlines() if line.strip()]


def bare(line: str) -> str:
    """Строка без маркеров цитаты и пробелов: `> **Note.**` → `**Note.**`."""
    return _QUOTE.sub("", line.strip()).strip()


def is_list_item(line: str) -> bool:
    """Элемент списка: `-`, `*`, `+`, `•`, `–`, `1.`, `1)` — после снятия `>`."""
    text = bare(line)
    if not text:
        return False
    if text in {"-", "*", "+", "•", "–", "—"}:
        return True
    return text.startswith(_BULLETS) or bool(_NUMBERED.match(text))


def is_table_row(line: str) -> bool:
    """Строка таблицы GFM: начинается с вертикальной черты."""
    return bare(line).startswith("|")


def is_fence(line: str) -> bool:
    """Ограда кода: ``` или ~~~ в любом количестве (≥3)."""
    text = bare(line)
    return text.startswith("```") or text.startswith("~~~")


def is_atx_heading(line: str) -> bool:
    """ATX-заголовок: 1–6 решёток и пробел."""
    return bool(_ATX.match(bare(line)))


def heading_text(line: str) -> str:
    """Текст заголовка без решёток и хвостовых решёток — для сверки с `context_prefix`."""
    text = bare(line)
    text = _ATX.sub("", text)
    return text.strip().rstrip("#").strip()


def looks_like_heading(line: str) -> bool:
    """Строка **похожа** на заголовок: ATX либо короткая строка без конца утверждения.

    Прокси, а не разбор разметки: у нарезки нет флага «это заголовок» в тексте чанка,
    поэтому D2 узнаёт заголовок по форме. Что он не видит: заголовок Setext (подчёркнутый
    `===`/`---`), заголовок, заканчивающийся точкой, и **любую короткую строку-подпись**
    (например, «Схема 1», «Пример») — она попадёт в D2 ложно. Что он видит сверх ATX:
    короткую строку без завершающей пунктуации, которая в markdown-корпусе почти всегда
    является заголовком раздела или термином определения.
    """
    text = bare(line)
    if not text:
        return False
    if is_atx_heading(text):
        return True
    if is_list_item(text) or is_table_row(text) or is_fence(text):
        return False
    if len(text) > HEADING_MAX_CHARS:
        return False
    return text[-1] not in TERMINAL_CHARS


def is_note_start(line: str) -> bool:
    """Строка открывает примечание: `Примечание`/`Внимание`/`Note`/`NB` (D3).

    Прокси. Видит: пометку в начале строки после снятия `>` и `**`/`*`/`_`-разметки, с
    границей слова (`Note:` — да, `Notebook` — нет). Не видит: примечание внутри абзаца
    («…, примечание: …»), примечание в ячейке таблицы и примечание, набранное как
    `> Note` с последующим текстом на той же строке без разделителя. Ложно срабатывает
    на английском `Note` в начале строки, если это часть обычного предложения («Note that
    the server …» — здесь срабатывание верное по форме, но объект рядом), и на
    «Внимание» как заголовке раздела.
    """
    text = bare(line)
    while text[:1] in {"*", "_", "`"}:
        text = text[1:].lstrip()
    lowered = text.lower()
    for prefix in NOTE_PREFIXES:
        if not lowered.startswith(prefix):
            continue
        rest = lowered[len(prefix) :]
        # Граница слова: `Notebook` не примечание, `Note:`/`Note.`/`Note ` — да.
        if rest and rest[0].isalnum():
            continue
        return True
    return False


def is_sentence_end(line: str) -> bool:
    """Строка кончается знаком конца утверждения (для `sentence_internal_cuts`)."""
    text = bare(line)
    return bool(text) and text[-1] in TERMINAL_CHARS


def is_block_start(line: str) -> bool:
    """Строка открывает новый блок: заголовок, элемент списка, таблица, ограда, цитата."""
    text = bare(line)
    if not text:
        return False
    return (
        is_atx_heading(text)
        or is_list_item(text)
        or is_table_row(text)
        or is_fence(text)
        or line.strip().startswith(">")
    )


def _snippet(text: str, *, tail: bool) -> str:
    """Обрывок текста ≤ `SNIPPET_CHARS`: хвост или начало, с многоточием."""
    flat = " ".join(text.split())
    if len(flat) <= SNIPPET_CHARS:
        return flat
    return "…" + flat[-SNIPPET_CHARS:] if tail else flat[:SNIPPET_CHARS] + "…"


def _fence_open(text: str) -> bool:
    """Открыта ли ограда кода к концу чанка (нечётное число строк-оград)."""
    return sum(1 for line in nonempty_lines(text) if is_fence(line)) % 2 == 1


# --------------------------------------------------------------------------- #
# Детекторы: каждый — прокси, ограничения названы в докстринге
# --------------------------------------------------------------------------- #


def detect_list_intro(prev: ChunkRow, cur: ChunkRow) -> Hit | None:
    """D1. Вводная строка списка оторвана от элементов.

    Условие: последняя непустая строка чанка `i` кончается на `:`/`：`, а первый непустой
    блок чанка `i+1` — элемент списка. Это форма фикстуры
    `test_list_intro_line_stays_with_its_items`.

    Видит: разрез ровно на границе «вводная строка | список». Не видит: (а) что список
    в чанке `i+1` относится именно к этой вводной строке — рядом мог стоять другой абзац;
    (б) что элементы списка не были повторены в следующем чанке перекрытием.
    Ложно срабатывает: на `:` в конце **элемента списка** (`- Параметр:`) и на `:` внутри
    ограды кода (YAML/JSON: `key:` и список ниже) — оба класса считаются отдельно
    (`intro_is_list_item`, `intro_in_fence`), а не вычитаются молча.
    """
    prev_lines = nonempty_lines(prev.text)
    cur_lines = nonempty_lines(cur.text)
    if not prev_lines or not cur_lines:
        return None
    last = prev_lines[-1]
    if not last.endswith((":", "：")):
        return None
    if not is_list_item(cur_lines[0]):
        return None
    intro = last.rstrip("：:").strip()
    repeated = bool(intro) and intro in cur.text
    in_list_item = is_list_item(last)
    in_fence = _fence_open(prev.text)
    return Hit(
        prev=prev,
        cur=cur,
        flags={
            "intro_is_list_item": in_list_item,
            "intro_in_fence": in_fence,
            "prose_like": not in_list_item and not in_fence,
            "repeated_in_next": repeated,
        },
    )


def detect_dangling_heading(prev: ChunkRow, cur: ChunkRow) -> Hit | None:
    """D2. Чанк заканчивается заголовком, тело — в следующем чанке.

    Условие: последняя непустая строка чанка `i` похожа на заголовок
    (`looks_like_heading`), и до неё в чанке есть содержательный блок (то есть заголовок
    действительно висит в хвосте, а не составляет чанк целиком).

    **Две ветки, и они не равны.** Корпус — markdown, и заголовок в нём это ATX-строка
    (`## Описание`): ветка `atx` — единственное, что можно назвать заголовком по факту.
    Ветка `loose` («короткая строка без завершающей точки») — форма фикстуры, но на живом
    корпусе она ловит **код и определения**: измерено 17.11.2026, из 33 657 срабатываний
    ATX-ветка дала **559**, а остальные — строки вида `… CREATE TABLE`, `} ` и
    `ALTER DOMAIN — изменение определения домена`. Поэтому в счёт свойства и в цену входит
    только `atx`; `loose` публикуется отдельным числом с разбивкой `code_like`/
    `open_fence`, чтобы её размер был виден, но не выдавался за размер дефекта.

    Видит: разрез сразу после строки-заголовка. Не видит: был ли заголовок осмысленной
    границей (в корпусе есть документы, где раздел — это строка заголовка и строка тела;
    там такой разрез безвреден). Ложно срабатывает на короткой подписи («Пример»,
    «Схема 1») и на термине определения в хвосте списка — это и есть `loose`.

    Срабатывания разложены на три класса, и **дефектом является только третий**:
    `repeated_in_next` — заголовок повторён в начале следующего чанка (перекрытие, связь
    не теряется); `named_in_next_context` — заголовок виден в `context_prefix` следующего
    чанка (читатель его получает, потеря мягкая); `silent` — ни того, ни другого, связь
    теряется полностью.
    """
    prev_lines = nonempty_lines(prev.text)
    if len(prev_lines) < 2:
        return None
    last = prev_lines[-1]
    if not looks_like_heading(last):
        return None
    atx = is_atx_heading(last)
    in_fence = _fence_open(prev.text)
    title = heading_text(last)
    repeated = bool(title) and title in cur.text
    named = bool(title) and (title in (cur.context_prefix or ""))
    return Hit(
        prev=prev,
        cur=cur,
        flags={
            "atx": atx,
            "atx_in_fence": atx and in_fence,
            "loose": not atx,
            "repeated_in_next": repeated,
            "named_in_next_context": named,
            "silent": not repeated and not named,
            "open_fence": in_fence,
            "code_like": _code_like(last),
        },
        primary=atx and not in_fence,
    )


def _code_like(line: str) -> bool:
    """Строка похожа на код/разметку, а не на заголовок: скобки, `=`, `;`, `|`, backtick."""
    return any(char in line for char in "{}[]()=;|<>`$\\")


def detect_note_without_object(prev: ChunkRow, cur: ChunkRow) -> Hit | None:
    """D3. Примечание открывает чанк, то есть потеряло объект из предыдущего чанка.

    Условие: первый непустой блок чанка `i+1` — примечание (`is_note_start`), и чанк `i+1`
    не первый в документе (у первого чанка предыдущего объекта нет вовсе). Это форма
    фикстуры `test_note_keeps_its_own_object`.

    Видит: примечание в голове чанка. Не видит: относится ли оно к объекту из `i` (тогда
    потеря мягкая — объект рядом) или к объекту, который в `i+1` стоит **после** примечания
    (тогда примечание приклеено к чужому объекту — это и есть дефект фикстуры). Второй
    случай измеряется уточнением `followed_by_other_text`: после строки примечания в чанке
    есть строки вне цитаты, то есть примечание стоит **перед** содержанием, а не после.

    Ложно срабатывает на примечании, которое законно открывает раздел (его объект — сам
    раздел, и он назван в `context_prefix`): такие случаи видны по уточнению
    `named_in_context`.
    """
    cur_lines = nonempty_lines(cur.text)
    if not cur_lines:
        return None
    if cur.chunk_no == 0:
        return None
    if not is_note_start(cur_lines[0]):
        return None
    note_line = cur_lines[0]
    # Строки после примечания, не являющиеся продолжением цитаты: примечание стоит перед
    # чужим содержанием, а не после своего объекта.
    following = [line for line in cur_lines[1:] if not line.lstrip().startswith(">")]
    object_name = _note_object_name(note_line)
    return Hit(
        prev=prev,
        cur=cur,
        flags={
            "followed_by_other_text": bool(following),
            "named_in_context": bool(object_name) and object_name in (cur.context_prefix or ""),
        },
    )


def _note_object_name(line: str) -> str:
    """Текст примечания без пометки — для сверки с `context_prefix` (пустая строка — нет)."""
    text = bare(line)
    while text[:1] in {"*", "_", "`"}:
        text = text[1:].lstrip()
    lowered = text.lower()
    for prefix in NOTE_PREFIXES:
        if lowered.startswith(prefix):
            return text[len(prefix) :].strip(" .:—–-*_`")
    return ""


def detect_dangling_negation(prev: ChunkRow, cur: ChunkRow) -> Hit | None:
    """D4. **Не измеряет** класс «отрицание оторвано от предмета» — считает узкий прокси.

    Дефект из фикстуры `test_negation_stays_with_its_object`: разрез встаёт между «не» и
    глаголом, чанк `i+1` начинается с «поддерживает …» и утверждает обратное источнику.
    Признака, по которому **открывающий глагол** нуждается в отрицании, в данных нет:
    чанк `i+1` выглядит как обычное положительное утверждение, а чанк `i` — как обычный
    абзац. Поэтому честного прокси у этого класса нет, и число по нему **не выдаётся**.

    Что считается вместо: последний токен чанка `i` — частица отрицания (`не`, `ни`,
    `not`, `no`). Это разрез **внутри** отрицаемой группы, то есть тот же дефект в самой
    грубой форме. Прокси узкий: он пропускает все случаи, где отрицание уехало целиком
    («не поддерживает» осталось в `i`, а предмет — в `i+1`), и все случаи, где разрез
    прошёл после частицы, но с переносом строки. Число по нему — **нижняя** граница
    другого класса, и в отчёте оно названо так, а не «размером D4».
    """
    prev_lines = nonempty_lines(prev.text)
    if not prev_lines:
        return None
    tokens = prev_lines[-1].split()
    if not tokens:
        return None
    token = tokens[-1].strip(".,;:!?()[]{}\"'«»").lower()
    if token not in NEGATION_TOKENS:
        return None
    next_lines = nonempty_lines(cur.text)
    block_boundary = is_block_start(next_lines[0]) if next_lines else False
    return Hit(
        prev=prev,
        cur=cur,
        flags={"block_boundary": block_boundary},
        primary=not block_boundary,
    )


def is_sentence_internal_cut(prev: ChunkRow, cur: ChunkRow) -> bool:
    """Необходимое условие класса D4: разрез попал **внутрь** предложения.

    Не нарушение и не детектор: разрез внутри предложения бывает осмысленным (длинный
    абзац, продолжение перечисления). Считается как **верхняя граница** класса D4 — без
    такого разреза оторвать отрицание невозможно, но большинство таких разрезов ничего не
    теряют. Признак: последняя строка чанка `i` не заканчивается утверждением и не
    открывает блок, а чанк `i+1` начинается со строчной буквы (то есть продолжает фразу).
    """
    prev_lines = nonempty_lines(prev.text)
    if not prev_lines:
        return False
    last = prev_lines[-1]
    if is_block_start(last) or is_sentence_end(last):
        return False
    head = cur.text.lstrip()[:1]
    return bool(head) and head.islower()


DETECTORS: tuple[tuple[str, str, Any], ...] = (
    ("D1", "вводная строка списка", detect_list_intro),
    ("D2", "висячий заголовок", detect_dangling_heading),
    ("D3", "примечание без объекта", detect_note_without_object),
    ("D4", "частица отрицания в хвосте (прокси, класс НЕ измерен)", detect_dangling_negation),
)


# --------------------------------------------------------------------------- #
# Чтение прогона
# --------------------------------------------------------------------------- #


def open_readonly(db_path: Path) -> sqlite3.Connection:
    """Открыть БД прогона **только на чтение** сырым `sqlite3`.

    Гейт хранилища здесь не используется намеренно: стенд несёт файл схемы 3.20.0, а код
    ждёт 3.26.0, и `SchemaVersionMismatch` — правильный отказ записи, но не чтения
    готового корпуса прибором. `mode=ro` гарантирует, что прибор не тронет ни байта.
    """
    return sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True)


def iter_documents(conn: sqlite3.Connection) -> Iterator[list[ChunkRow]]:
    """Документы по одному: чанки отсортированы по `chunk_no`, а не по порядку строк.

    Один проход `ORDER BY doc_id, chunk_no` и накопитель на текущий документ: индекса по
    `doc_id` нет, поэтому запрос на документ был бы полным сканом 253k строк. Сортировка
    по `chunk_no` делается **в Python**, чтобы порядок не зависел от того, как БД вернула
    строки (проверяется тестом).
    """
    cursor = conn.execute(
        "SELECT doc_id, chunk_no, text, context_prefix FROM corpus_chunks "
        "ORDER BY doc_id, chunk_no"
    )
    current: list[ChunkRow] = []
    current_id: str | None = None
    for doc_id, chunk_no, text, context_prefix in cursor:
        if doc_id != current_id:
            if current:
                yield sorted(current, key=lambda row: row.chunk_no)
            current = []
            current_id = doc_id
        current.append(
            ChunkRow(
                doc_id=str(doc_id),
                chunk_no=int(chunk_no),
                text=str(text or ""),
                context_prefix=str(context_prefix or ""),
            )
        )
    if current:
        yield sorted(current, key=lambda row: row.chunk_no)


def adjacent_pairs(chunks: Iterable[ChunkRow]) -> Iterator[tuple[ChunkRow, ChunkRow]]:
    """Пары соседних чанков одного документа в порядке `chunk_no`."""
    previous: ChunkRow | None = None
    for chunk in chunks:
        if previous is not None:
            yield previous, chunk
        previous = chunk


# --------------------------------------------------------------------------- #
# Свод
# --------------------------------------------------------------------------- #


def measure(conn: sqlite3.Connection, *, examples: int = DEFAULT_EXAMPLES) -> dict[str, Any]:
    """Пройти корпус одним проходом и собрать счётчики, доли и примеры."""
    started = time.monotonic()
    documents = 0
    documents_with_pairs = 0
    chunks_total = 0
    pairs_total = 0
    stats: dict[str, dict[str, Any]] = {
        code: {
            "code": code,
            "title": title,
            "pairs": 0,
            "documents": set(),
            "chunks": set(),
            "flags": {},
            "examples": [],
            "secondary_pairs": 0,
            "secondary_documents": set(),
            "secondary_chunks": set(),
            "secondary_flags": {},
            "secondary_examples": [],
        }
        for code, title, _ in DETECTORS
    }
    sentence_internal_pairs = 0
    sentence_internal_docs: set[str] = set()
    doc_chunk_counts: dict[str, int] = {}
    affected_documents: set[str] = set()

    for doc_chunks in iter_documents(conn):
        documents += 1
        chunks_total += len(doc_chunks)
        doc_chunk_counts[doc_chunks[0].doc_id] = len(doc_chunks)
        has_pairs = False
        for prev, cur in adjacent_pairs(doc_chunks):
            has_pairs = True
            pairs_total += 1
            for code, _title, detector in DETECTORS:
                hit = detector(prev, cur)
                if hit is None:
                    continue
                entry = stats[code]
                prefix = "" if hit.primary else "secondary_"
                if hit.primary:
                    affected_documents.add(prev.doc_id)
                entry[f"{prefix}pairs"] += 1
                entry[f"{prefix}documents"].add(prev.doc_id)
                entry[f"{prefix}chunks"].add((prev.doc_id, prev.chunk_no))
                entry[f"{prefix}chunks"].add((cur.doc_id, cur.chunk_no))
                flags = entry[f"{prefix}flags"]
                for key, value in hit.flags.items():
                    if value:
                        flags[key] = flags.get(key, 0) + 1
                bucket = entry[f"{prefix}examples"]
                if len(bucket) < examples:
                    bucket.append(
                        {
                            "doc_id": prev.doc_id,
                            "chunk_no": prev.chunk_no,
                            "next_chunk_no": cur.chunk_no,
                            "prev_tail": _snippet(prev.text, tail=True),
                            "next_head": _snippet(cur.text, tail=False),
                            "flags": sorted(key for key, value in hit.flags.items() if value),
                        }
                    )
            if is_sentence_internal_cut(prev, cur):
                sentence_internal_pairs += 1
                sentence_internal_docs.add(prev.doc_id)
        if has_pairs:
            documents_with_pairs += 1

    elapsed = time.monotonic() - started
    all_affected: set[tuple[str, int]] = set()
    secondary_affected: set[tuple[str, int]] = set()
    detectors: dict[str, Any] = {}
    for code, _title, _detector in DETECTORS:
        entry = stats[code]
        affected: set[tuple[str, int]] = entry["chunks"]
        all_affected |= affected
        secondary_affected |= entry["secondary_chunks"]
        detectors[code] = {
            "code": code,
            "title": entry["title"],
            "pairs": entry["pairs"],
            "documents": len(entry["documents"]),
            "chunks_to_rechunk": len(affected),
            "pair_share": _share(entry["pairs"], pairs_total),
            "document_share": _share(len(entry["documents"]), documents_with_pairs),
            "chunk_share": _share(len(affected), chunks_total),
            "flags": dict(entry["flags"]),
            "examples": entry["examples"],
            "secondary": {
                "pairs": entry["secondary_pairs"],
                "documents": len(entry["secondary_documents"]),
                "chunks_to_rechunk": len(entry["secondary_chunks"]),
                "pair_share": _share(entry["secondary_pairs"], pairs_total),
                "flags": dict(entry["secondary_flags"]),
                "examples": entry["secondary_examples"],
            },
        }

    return {
        "corpus": {
            "documents": documents,
            "documents_with_pairs": documents_with_pairs,
            "chunks": chunks_total,
            "pairs": pairs_total,
        },
        "detectors": detectors,
        "sentence_internal_cuts": {
            "pairs": sentence_internal_pairs,
            "documents": len(sentence_internal_docs),
            "pair_share": _share(sentence_internal_pairs, pairs_total),
            "note": "не нарушение: необходимое условие класса D4, верхняя граница, а не размер",
        },
        "negation_proxy_pairs": detectors["D4"]["pairs"],
        "affected_chunks_union": len(all_affected),
        "affected_chunks_union_secondary": len(secondary_affected),
        "affected_documents_union": len(affected_documents),
        "chunks_in_affected_documents": sum(
            doc_chunk_counts.get(doc_id, 0) for doc_id in affected_documents
        ),
        "elapsed_s": round(elapsed, 1),
    }


def _share(part: int, whole: int) -> float | None:
    """Доля; `None`, если знаменатель нулевой (не «0»: не измерено ≠ ноль)."""
    return round(part / whole, 6) if whole else None


def price(summary: dict[str, Any]) -> dict[str, Any]:
    """Цена исправления: две границы, узкая и широкая, в часах пере-эмбеддинга.

    **Узкая граница** — оба чанка каждой задетой пары. Это верхняя оценка для фикса,
    который **переносит** текст между двумя соседними чанками и не двигает остальные.
    Она завышена, потому что: (а) пере-эмбеддинг нужен только чанкам, чей текст меняется,
    а фикс часто сохраняет текст одного из двух; (б) `embed_cache` вернёт векторы для
    кусков с тем же текстом и тем же `embed_input` (измеренная доля попаданий в кэш —
    10 %, план 26 §O19: выигрыш мал, но он есть); (в) ложные срабатывания детекторов не
    вычтены (см. примеры и уточнения).

    **Широкая граница** — все чанки задетых документов. Это верхняя оценка для фикса,
    который меняет **выравнивание окна**: сдвиг одной границы пере-нарезает хвост
    документа, и тогда пере-эмбеддинг нужен всему документу, а не двум чанкам. Какая из
    двух границ ближе к правде — вопрос реализации фикса, а не замера; поэтому публикуются
    обе, а не одна «средняя».
    """
    chunks_total = int(summary["corpus"]["chunks"])
    affected = int(summary["affected_chunks_union"])
    affected_documents_chunks = int(summary.get("chunks_in_affected_documents", 0))
    return {
        "chunks_total": chunks_total,
        "chunks_affected_union": affected,
        "chunks_affected_union_secondary": int(
            summary.get("affected_chunks_union_secondary", 0)
        ),
        "affected_documents_union": int(summary.get("affected_documents_union", 0)),
        "chunks_in_affected_documents": affected_documents_chunks,
        "affected_share": _share(affected, chunks_total),
        "documents_share": _share(
            int(summary.get("affected_documents_union", 0)), int(summary["corpus"]["documents"])
        ),
        "rate_chunks_per_s_ac": EMBED_CHUNKS_PER_S_AC,
        "rate_chunks_per_s_usb_c": EMBED_CHUNKS_PER_S_USB_C,
        "hours_affected_ac": round(affected / EMBED_CHUNKS_PER_S_AC / 3600, 3),
        "hours_affected_usb_c": round(affected / EMBED_CHUNKS_PER_S_USB_C / 3600, 3),
        "hours_documents_ac": round(affected_documents_chunks / EMBED_CHUNKS_PER_S_AC / 3600, 3),
        "hours_documents_usb_c": round(affected_documents_chunks / EMBED_CHUNKS_PER_S_USB_C / 3600, 3),
        "hours_full_corpus_ac": round(chunks_total / EMBED_CHUNKS_PER_S_AC / 3600, 3),
        "hours_full_corpus_usb_c": round(chunks_total / EMBED_CHUNKS_PER_S_USB_C / 3600, 3),
    }


def print_table(summary: dict[str, Any], price_block: dict[str, Any]) -> None:
    """Таблица «детектор × пары / документы / чанки / доли» и цена."""
    corpus = summary["corpus"]
    print(
        f"корпус: {corpus['documents']} документов, {corpus['chunks']} чанков, "
        f"{corpus['pairs']} пар; проход {summary['elapsed_s']} с"
    )
    print(
        f"{'детектор':34} {'пар':>8} {'док':>7} {'чанков':>8} "
        f"{'доля пар':>10} {'доля док':>10}"
    )
    for code, entry in summary["detectors"].items():
        pair_share = entry["pair_share"]
        doc_share = entry["document_share"]
        print(
            f"{code + ' ' + entry['title']:34} {entry['pairs']:8d} {entry['documents']:7d} "
            f"{entry['chunks_to_rechunk']:8d} "
            f"{(pair_share or 0) * 100:9.3f}% {(doc_share or 0) * 100:9.3f}%"
        )
        if entry["flags"]:
            print(f"{'':34} уточнения: {entry['flags']}")
        secondary = entry["secondary"]
        if secondary["pairs"]:
            print(
                f"{'':34} вторая ветка (не дефект): {secondary['pairs']} пар, "
                f"{secondary['documents']} док, {secondary['chunks_to_rechunk']} чанков; "
                f"{secondary['flags']}"
            )
    cuts = summary["sentence_internal_cuts"]
    print(
        f"{'разрез внутри предложения':34} {cuts['pairs']:8d} {cuts['documents']:7d} "
        f"{'—':>8} {(cuts['pair_share'] or 0) * 100:9.3f}%        —  (не нарушение: граница D4)"
    )
    print(
        f"объединение задетых чанков: {price_block['chunks_affected_union']} "
        f"({(price_block['affected_share'] or 0) * 100:.3f} % корпуса); "
        f"вторая ветка D2 отдельно: {price_block['chunks_affected_union_secondary']}"
    )
    print(
        f"цена узкой границы: {price_block['hours_affected_ac']} ч (БП, "
        f"{EMBED_CHUNKS_PER_S_AC} чанк/с) / {price_block['hours_affected_usb_c']} ч (USB-C, "
        f"{EMBED_CHUNKS_PER_S_USB_C} чанк/с)"
    )
    print(
        f"цена широкой границы: {price_block['hours_documents_ac']} ч (БП) / "
        f"{price_block['hours_documents_usb_c']} ч (USB-C) — "
        f"{price_block['chunks_in_affected_documents']} чанков в "
        f"{price_block['affected_documents_union']} документах"
    )
    print(
        f"полный пере-эмбеддинг корпуса: {price_block['hours_full_corpus_ac']} ч (БП) / "
        f"{price_block['hours_full_corpus_usb_c']} ч (USB-C)"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n")[0])
    parser.add_argument("--db", required=True, help="cohound.db прогона (только чтение)")
    parser.add_argument("--run-id", default=None, help="run_id для отчёта (по умолчанию — имя каталога)")
    parser.add_argument("--out-json", default=None, help="куда писать машинный отчёт")
    parser.add_argument(
        "--examples", type=int, default=DEFAULT_EXAMPLES, help="примеров на детектор"
    )
    args = parser.parse_args(argv)

    db_path = Path(args.db).expanduser()
    if not db_path.is_file():
        print(f"measure: БД не найдена: {db_path}", file=sys.stderr)
        return 2
    run_id = args.run_id or db_path.parent.name

    conn = open_readonly(db_path)
    try:
        summary = measure(conn, examples=args.examples)
    finally:
        conn.close()
    price_block = price(summary)
    payload = {
        "run_id": run_id,
        "db": str(db_path),
        "measured_at": time.strftime("%Y-%m-%d"),
        "price_note": (
            "верхняя граница: оба чанка пары, ложные срабатывания и кэш не вычтены"
        ),
        **summary,
        "price": price_block,
    }
    print(f"run_id: {run_id}")
    print_table(summary, price_block)
    if args.out_json:
        target = Path(args.out_json).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
        print(f"json: {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
