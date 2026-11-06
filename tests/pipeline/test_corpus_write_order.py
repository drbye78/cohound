"""О3: порядок записи в store — оракул для декомпозиции `corpus.py`.

Зачем это существует. Рефакторинг крупных модулей (план
`docs/reports/2026-10-30-large-modules-refactoring-plan.md`) переносит код между файлами и
переписывает замыкания `_build_corpus`. Утверждение «поведение не изменилось» проверяемо
только если виден **порядок побочных эффектов**: ячейки, манифест и покрытие пишутся в
конкретной последовательности, и перестановка двух операторов даёт другой отчёт при зелёных
функциональных тестах.

Механизм. На соединение ставится встроенный ``sqlite3.Connection.set_trace_callback``: он
видит **фактические** операторы в порядке исполнения, включая ``BEGIN``/``COMMIT``, — то
есть транзакционные границы тоже под наблюдением. Обёртка над ``StorePort`` этого не даёт:
она видела бы вызовы методов, а не то, что из них следует.

Нормировка — три вещи, которые различаются между прогонами по природе, а не по поведению:
  * литералы-таймстампы (``created_at``, ``fetched_at``, ``heartbeat_at``);
  * владелец сеанса ``host/pid/tid`` — свидетельство живости, своё у каждого процесса;
  * корень ``tmp_path`` (он попадает в ``raw_path``).

**Детерминизм замерен, а не предположен.** Спайк: 4 отдельных процесса дают один и тот же
дайджест нормированной последовательности (178 операторов). Поэтому эталон здесь — не
наблюдение «как получилось», а зафиксированный контракт.

Как обновлять. Изменение последовательности — **осознанное решение**, а не побочный эффект:
пересобрать эталон можно, но в том же коммите должно быть сказано, почему порядок записи
изменился. Иначе этот тест превращается в штамп.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

import pytest

import tests.pipeline.test_corpus_parity as parity
from core.store.sqlite_store import SqliteStore

#: Литерал таймстампа (``2026-10-29T23:01:38.426686+00:00``) внутри SQL.
_TS = re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2})?")

#: Владелец сеанса: ``host/pid/tid`` — своё у каждого процесса, поведением не является.
_OWNER = re.compile(r"'[A-Za-z0-9_.\-]+/\d+/\d+'")

#: Дайджест нормированной последовательности **записей** (см. :func:`_is_write`): ловит
#: изменение внутри оператора (список колонок, ``ON CONFLICT``-ветка) и перестановку двух
#: писателей, которое сводка «BEGIN/INSERT …» не покажет. Это и есть claim модуля: порядок
#: побочных эффектов не изменился.
#:
#: **Пересобран 2026-11-01 осознанно** (F6 плана ``17``, §4.2 п.3-4), по двум причинам и
#: ни по какой другой:
#:   1. переход ``pending → claimed`` стал **отдельным** ``UPDATE extract_queue``
#:      (``expect=PENDING`` + ``claimed_at``/``heartbeat_at``): аренда — это запись, и
#:      конвертация больше не закрывает строку «одним махом»;
#:   2. перед проходом читается ``SELECT session_id, … FROM extract_queue`` — просроченные
#:      аренды (``heartbeat_at`` старше порога) возвращаются в ``pending`` до чтения списка.
#: Операторов стало 195 против 178, записей 162 против 147: ровно по ``UPDATE`` на
#: сконвертированную строку плюс чтение восстановления. Порядок остальных писателей не
#: изменился — блоки ``INSERT corpus_docs``/``corpus_chunks`` идут там же, где были.
#:
#: **Пересобран 2026-11-01 осознанно** (F5 плана ``17``, слаг семантики ``crawl-3``), по
#: трём названным причинам и ни по какой другой:
#:   1. краул больше **не конвертирует**: на каждый добытый документ он пишет
#:      ``INSERT extract_queue`` (строку очереди) вместо ``INSERT corpus_docs`` +
#:      ``INSERT corpus_doc_version``;
#:   2. конверсия идёт на границе продукта, порядком очереди ``(product_id, url)``:
#:      блоки ``INSERT corpus_docs``/``INSERT corpus_chunks`` сдвинулись **после** волн
#:      краула, и порядок документов внутри блока — порядок очереди, а не фетча;
#:   3. появился ``UPDATE extract_queue`` на каждую сконвертированную строку (переход
#:      ``pending → done`` в durable-очереди) — ровно по одному на документ.
#: Число операторов и записей при этом **изменилось**: 146 → 178 и 117 → 147 (замерено
#: пересбором на этом дереве, а не по памяти). Так и должно быть: строка очереди — это
#: отдельный ``INSERT`` на документ, к которому добавляется ``UPDATE extract_queue`` на
#: каждую сконвертированную строку. Поэтому «эталон сошёлся» здесь не аргумент — новым
#: контрактом стал сам эталон, и он запинен именно в этом составе.
#:
#: **Пересобран 2026-11-03 осознанно** (L1b-2b-1 плана ``18``), по одной причине: аддитивные
#: колонки ``corpus_chunks.char_start``/``char_end`` (смещения куска — провенанс врезки)
#: изменили текст оператора ``INSERT OR REPLACE INTO corpus_chunks``. Число операторов и
#: записей **не изменилось** (195 и 162 — те же, что в эталоне 2026-11-01), порядок писателей
#: тот же. Проверено не на глаз: если из нового оператора убрать две добавленные колонки и их
#: значения, дайджест совпадает с прежним эталоном
#: ``fadd988c828c99f06447887ad4d71053243ac8a917871cd5b76e25e0360be71e`` побайтно — то есть
#: сдвинулся **текст одного INSERT**, а не последовательность побочных эффектов.
#:
#: **Пересобран 2026-10-31** (F3 плана ``17``, слаг ``crawl-2``): порядок обхода frontier
#: стал ключом ``(depth, url)`` (два документа ингестились в обратном порядке), и
#: аддитивная колонка ``crawl_session_items.doc_hash`` изменила текст каждого ``INSERT``
#: этой таблицы.
#:
#: **Пересобран 2026-11-05 осознанно** (F1 плана ``20``), по трём названным причинам и ни по
#: какой другой: фаза ``corpus`` теперь считает снимок корпуса (провенанс, не вход), поэтому
#:   1. читает durable-корпус продукта — ``SELECT 1 FROM runs`` + ``SELECT * FROM corpus_docs``
#:      (адрес документа) и ``PRAGMA``/``SELECT doc_version_id, doc_id FROM corpus_doc_version``
#:      (адрес версии): без них ``snapshot_product_id`` пришлось бы выдумывать;
#:   2. пишет снимок прогона в ``runs.corpus_snapshot_json`` — один ``UPDATE runs`` после
#:      закрытия всех продуктов (манифест Ш9 читает его оттуда, а не пересчитывает);
#:   3. не сдвигает ни одну прежнюю запись: порядок ``INSERT corpus_docs``/``corpus_chunks``/
#:      ``corpus_coverage``/``run_product`` тот же. Операторов стало 203 против 195, записей
#:      165 против 162 — ровно чтения снимка и один его ``UPDATE``.
#:
#: **Пересобран 2026-11-05 осознанно** (F2 плана ``20``, §F2), по одной причине: на границе
#: продукта пишется проекция дат артефактов в БД прогона — ``DELETE`` + ``INSERT``
#: ``corpus_artifact`` в **одной** транзакции (замена набора продукта: resume/``--reset`` не
#: копит строки). Место — между ``INSERT corpus_coverage`` и ``INSERT run_product``: там же,
#: где считаются снятия и даты, из которых строится снимок продукта. Операторов стало 211
#: против 203, записей 173 против 165: ``BEGIN``/``COMMIT`` + ``DELETE`` на продукт и
#: ``INSERT`` на каждый документ (5 в этом харнессе). Порядок остальных писателей не
#: изменился, новых чтений нет — поэтому сдвинулся ровно блок ``corpus_artifact``.
#:
#: **Пересобран 2026-11-05 осознанно** (F3 плана ``20``, SCHEMA 3.20.0), по одной причине:
#: у ``corpus_coverage`` появилась колонка ``roots_json`` — снятия объявленных корней,
#: по которым теперь решается ``complete`` (§11.2). Она вошла в текст ``INSERT
#: corpus_coverage``, поэтому сдвинулся дайджест **записи**; ни один оператор не
#: переставлен и ни одно чтение не добавлено (снятия читаются из ``state.db``, а не из
#: run-БД, и в трассу store не попадают). ``cells.absence_as_of`` тоже добавлена в
#: ``_CELL_COLUMNS``, но ячеек этот харнесс не пишет — на этот пин она не влияет.
#: Повторно 2026-11-05: ``policy_digest``/``policy_matched`` в ``roots_json`` меняют
#: JSON-литерал в ``INSERT corpus_coverage`` (``trace_callback`` раскрывает параметры);
#: порядок и число записей не изменились.
#:
#: **Пересобран 2026-11-05 осознанно** (дефект провенанса, найденный замером F3 плана
#: ``20``), по одной причине: реестр ``index_manifest`` пишется **после** исполненного
#: ``commit()``, а не до проверки эмбеддера/билдера. Харнесс паритета не подаёт ни
#: эмбеддера, ни сборщика, поэтому сборка в нём не исполняется, и блок
#: ``BEGIN/INSERT index_manifest/COMMIT`` из следа **исчез** — число операторов
#: 211 → 208, записей 173 → 170, ровно один блок. Ни один прежний оператор не
#: переставлен. Новый порядок (``commit()`` раньше записи реестра) запинен отдельно:
#: ``tests/pipeline/test_corpus.py::test_manifest_is_written_after_commit``.
#:
#: **Пересобран 2026-11-06 осознанно** (ось ``embed.precision``, ``partition-3``), по одной
#: причине: ``_derive_partition_build_id`` получил слагаемое ``embed_precision`` и сменил
#: схему формулы (``partition-2`` → ``partition-3``), поэтому в ``INSERT OR REPLACE INTO
#: corpus_chunks`` изменился **литерал** ``build_id``: ``pb_7abc32db2fd866c8ccd455bf`` →
#: ``pb_3a9fbb8e4b770456bc4594d9`` (пять строк — по одной на документ харнесса).
#: Ни один оператор не переставлен и ни один не добавлен: 208 операторов / 170 записей —
#: те же, что в прежнем эталоне, и читаемый пин :data:`EXPECTED_SEQUENCE` **не менялся**.
#: Проверено не на глаз: если в новом следе вернуть прежний ``pb``, оба дайджеста совпадают
#: с прежними эталонами побайтно — то есть сдвинулось значение одного слагаемого
#: идентичности партиции, а не последовательность побочных эффектов.
EXPECTED_WRITE_DIGEST = "6385d7a8cec8b41d8ee59d8bc166d2619198dec6203c87a82935f24524293e72"

#: Дайджест **полного** трассированного следа, включая ``SELECT``. Второй пин, а не первый:
#: он видит то, чего пин записей не видит по построению, — появление нового чтения (N+1) и
#: смену текста read-проекции. Смена **только** этого дайджеста при неизменном
#: ``EXPECTED_WRITE_DIGEST`` означает «запись не тронута, чтение изменилось» — другой
#: диагноз и другая реакция, чем «поехал порядок записи».
#:
#: **Пересобран 2026-11-03** вместе с писательским пином и по той же причине: те же две
#: колонки вошли в read-проекцию ``Chunk`` (``_CHUNK_COLUMNS``). Новых чтений нет — сдвинулся
#: текст проекции, что и есть ожидаемая цена аддитивной колонки.
#:
#: **Пересобран 2026-11-05** вместе с писательским пином и по той же причине (F3 плана
#: ``20``): ``roots_json`` вошла в текст ``INSERT corpus_coverage``. Новых чтений нет —
#: текст одного оператора, что и есть цена аддитивной колонки.
#:
#: **Пересобран 2026-11-05 ещё раз** (та же F3): в ``roots_json`` добавлены
#: ``policy_digest``/``policy_matched`` — сверка политики снятия с объявленной (§11.2).
#: ``trace_callback`` видит ``INSERT`` с раскрытыми параметрами, поэтому JSON-литерал
#: изменился; состав и порядок операторов — нет (211/173, как и было).
#:
#: **Пересобран 2026-11-05** вместе с писательским пином и по той же причине: блок
#: ``index_manifest`` из следа ушёл (сборки в харнессе нет ⇒ реестр не пишется). Новых
#: чтений нет — 208/170 против 211/173.
#:
#: **Пересобран 2026-11-06** вместе с писательским пином и по той же причине (ось
#: ``embed.precision``): изменился литерал ``build_id`` в ``INSERT corpus_chunks``
#: (``partition-3`` + ``embed_precision`` в формуле ``pb``). Новых чтений нет, состав и
#: порядок операторов — прежние (208/170): сдвинулся текст записи, а не чтение.
EXPECTED_TRACE_DIGEST = "1b2ac7108a7ba3837f8e0cad9b4861b35eeab5c37b5ceb1404f302e17eaa6c21"

#: Глаголы, меняющие данные или схему. Транзакционные границы входят в пин записей: без них
#: «две записи в одной транзакции» и «две записи в разных» дали бы один дайджест.
_WRITE_VERBS = frozenset({"INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP"})
_TXN_VERBS = frozenset({"BEGIN", "COMMIT", "ROLLBACK"})

#: Последовательность в сжатом виде: ``(сколько раз подряд, оператор)``. Сводка нужна,
#: чтобы падение было читаемым: видно, **где** разошлось, а не только «дайджест другой».
EXPECTED_SEQUENCE: tuple[tuple[int, str], ...] = (
    (1, 'BEGIN'),
    (1, 'INSERT runs'),
    (1, 'COMMIT'),
    (2, 'SELECT *'),
    (1, 'BEGIN'),
    (1, 'INSERT crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'UPDATE crawl_session_items'),
    (1, 'SELECT status,'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (2, 'SELECT *'),
    (1, 'BEGIN'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT run_product'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT crawl_session_items'),
    (1, 'SELECT status,'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT extract_queue'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT crawl_session_items'),
    (1, 'SELECT status,'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT extract_queue'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT crawl_session_items'),
    (1, 'SELECT status,'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT crawl_session_items'),
    (1, 'SELECT status,'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT extract_queue'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT crawl_session_items'),
    (1, 'SELECT status,'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT extract_queue'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT crawl_session_items'),
    (1, 'SELECT status,'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT extract_queue'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'SELECT *'),
    (1, 'SELECT session_id,'),
    (1, 'BEGIN'),
    (1, 'UPDATE extract_queue'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT corpus_docs'),
    (1, 'INSERT corpus_doc_version'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT crawl_session_items'),
    (1, 'SELECT status,'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'UPDATE extract_queue'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT corpus_docs'),
    (1, 'INSERT corpus_doc_version'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT crawl_session_items'),
    (1, 'SELECT status,'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'UPDATE extract_queue'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT corpus_docs'),
    (1, 'INSERT corpus_doc_version'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT crawl_session_items'),
    (1, 'SELECT status,'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'UPDATE extract_queue'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT corpus_docs'),
    (1, 'INSERT corpus_doc_version'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT crawl_session_items'),
    (1, 'SELECT status,'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'UPDATE extract_queue'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT corpus_docs'),
    (1, 'INSERT corpus_doc_version'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT crawl_session_items'),
    (1, 'SELECT status,'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'UPDATE extract_queue'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'UPDATE extract_queue'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'UPDATE extract_queue'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'UPDATE extract_queue'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'UPDATE extract_queue'),
    (1, 'COMMIT'),
    (2, 'SELECT *'),
    (1, 'SELECT status,'),
    (1, 'BEGIN'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'SELECT 1'),
    (1, 'SELECT *'),
    (1, 'SELECT 1'),
    (1, 'PRAGMA'),
    (1, 'SELECT doc_version_id,'),
    (1, 'BEGIN'),
    (5, 'INSERT corpus_chunks'),
    (1, 'COMMIT'),
    (1, 'SELECT *'),
    (1, 'BEGIN'),
    (1, 'INSERT corpus_coverage'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'DELETE corpus_artifact'),
    (5, 'INSERT corpus_artifact'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'INSERT run_product'),
    (1, 'COMMIT'),
    (1, 'BEGIN'),
    (1, 'UPDATE runs'),
    (1, 'COMMIT'),
    (1, 'SELECT *'),
    (1, 'BEGIN'),
    (1, 'INSERT corpus_coverage'),
    (1, 'COMMIT'),
    (2, 'PRAGMA'),
    (1, 'SELECT evidence_id,'),
    (1, 'SELECT run_id,'),
    (1, 'SELECT status,'),
    (1, 'SELECT *'),
    (1, 'BEGIN'),
    (1, 'UPDATE crawl_sessions'),
    (1, 'COMMIT'),
    (1, 'SELECT *'),
    (1, 'SELECT 1'),
    (2, 'SELECT *'),
)


def _normalize(sql: str, root: Path) -> str:
    """Убирает то, что различается между прогонами по природе, а не по поведению."""
    out = _OWNER.sub("'<OWNER>'", _TS.sub("<TS>", sql))
    return " ".join(out.replace(str(root), "<ROOT>").split())


def _summary(sql: str) -> str:
    """``INSERT corpus_docs`` вместо всего оператора: сводка для читаемого падения."""
    tokens = sql.split()
    verb = tokens[0]
    if verb not in {"INSERT", "UPDATE", "DELETE", "CREATE", "SELECT", "ALTER", "DROP"}:
        return verb
    skip = {"or", "into", "replace", "from", "set", "table", "index", "view", "trigger"}
    for token in tokens[1:]:
        if token.lower() in skip:
            continue
        return f"{verb} {token.rstrip('(')}"
    return verb


def _compress(ops: list[str]) -> tuple[tuple[int, str], ...]:
    out: list[tuple[int, str]] = []
    for op in ops:
        if out and out[-1][1] == op:
            out[-1] = (out[-1][0] + 1, op)
        else:
            out.append((1, op))
    return tuple(out)


def _capture(root: Path) -> list[str]:
    """Прогоняет детерминированный харнесс паритета и снимает последовательность SQL."""
    trace: list[str] = []

    class _TracingStore(SqliteStore):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self.conn.set_trace_callback(trace.append)

    original = parity.SqliteStore
    parity.SqliteStore = _TracingStore  # type: ignore[misc]
    try:
        pages, final_url = parity._site_full_bfs()
        parity._run_local(
            root,
            pages=pages,
            final_url=final_url,
            crawl_defaults=parity.FULL_BFS_DEFAULTS,
            urls={"p1": [parity.P_ROOT]},
        )
    finally:
        parity.SqliteStore = original  # type: ignore[misc]
    return trace


def _run_and_normalize(root: Path) -> list[str]:
    """Один прогон фазы: снять последовательность и сразу нормировать по тому же корню.

    Корень передаётся в нормировку тот же, что и в прогон: ``raw_path`` попадает в
    литералы SQL, и разные каталоги дали бы ложное расхождение.
    """
    return [_normalize(sql, root) for sql in _capture(root)]


@pytest.fixture(scope="module")
def write_order(tmp_path_factory: pytest.TempPathFactory) -> list[str]:
    """Последовательность операторов одного прогона фазы corpus (нормированная)."""
    return _run_and_normalize(tmp_path_factory.mktemp("write_order") / "root")


def _is_write(sql: str) -> bool:
    """Меняет ли оператор данные/схему (или открывает/закрывает транзакцию).

    ``SELECT`` сюда не входит намеренно: чтение не является побочным эффектом, а пин
    записей должен отвечать ровно на вопрос «порядок записи не изменился». Чтения
    наблюдаются отдельным пином (:data:`EXPECTED_TRACE_DIGEST`).
    """
    tokens = sql.split()
    if not tokens:
        return False
    verb = tokens[0].upper()
    return verb in _WRITE_VERBS or verb in _TXN_VERBS


def test_write_order_matches_the_pinned_digest(write_order: list[str]) -> None:
    """Последовательность **записей** не изменилась — ни порядком, ни составом операторов."""
    writes = [sql for sql in write_order if _is_write(sql)]
    digest = hashlib.sha256("\n".join(writes).encode()).hexdigest()
    assert digest == EXPECTED_WRITE_DIGEST, (
        "последовательность записей в store изменилась. Если это осознанно — пересоберите "
        "эталон и объясните смену порядка в сообщении коммита (см. докстринг модуля). "
        f"записей: {len(writes)} из {len(write_order)} операторов"
    )


def test_full_trace_matches_the_pinned_digest(write_order: list[str]) -> None:
    """Полный след (включая чтения) не изменился — второй пин, отдельный от записей.

    Зачем отдельный. Аддитивная колонка в read-проекции (``SELECT … FROM cell_evidence``)
    меняет текст оператора, не меняя ни одного побочного эффекта. Одним дайджестом это
    неотличимо от «поехал порядок записи», а реакция на эти события разная: первое —
    ожидаемая цена аддитивной миграции, второе — дефект. Разделение пинов даёт диагноз
    до чтения диффа: при неизменном ``EXPECTED_WRITE_DIGEST`` и изменившемся этом —
    смотреть надо на проекции чтения, и наоборот.
    """
    digest = hashlib.sha256("\n".join(write_order).encode()).hexdigest()
    assert digest == EXPECTED_TRACE_DIGEST, (
        "полный след операторов store изменился. Проверьте сначала "
        "test_write_order_matches_the_pinned_digest: если он зелёный — изменилось чтение "
        "(проекция/новый запрос), а не запись. "
        f"операторов: {len(write_order)}"
    )


def test_write_order_matches_the_pinned_sequence(write_order: list[str]) -> None:
    """Читаемая сверка: где именно разошлось, а не только «дайджест другой»."""
    actual = _compress([_summary(sql) for sql in write_order])
    assert actual == EXPECTED_SEQUENCE


def test_write_order_is_stable_across_runs(
    write_order: list[str], tmp_path: Path
) -> None:
    """Нормировка действительно снимает всё, что различается между прогонами.

    Без этой проверки предыдущие две недоказуемы: эталон мог совпасть случайно, а любая
    невычищенная случайность (``host/pid/tid``, таймстамп, путь) делала бы тест флаки —
    то есть выключенным на практике.
    """
    second = _run_and_normalize(tmp_path / "root")
    assert second == write_order
