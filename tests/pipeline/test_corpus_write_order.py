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
дайджест нормированной последовательности (146 операторов). Поэтому эталон здесь — не
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
EXPECTED_WRITE_DIGEST = "0db7f593fd0e75511f43793e51ec72207dd77edfc57d46c4998a05e717bbab8e"

#: Дайджест **полного** трассированного следа, включая ``SELECT``. Второй пин, а не первый:
#: он видит то, чего пин записей не видит по построению, — появление нового чтения (N+1) и
#: смену текста read-проекции. Смена **только** этого дайджеста при неизменном
#: ``EXPECTED_WRITE_DIGEST`` означает «запись не тронута, чтение изменилось» — другой
#: диагноз и другая реакция, чем «поехал порядок записи».
EXPECTED_TRACE_DIGEST = "711077e0442cf015d4b8de5eaa14e8a86c6cd522a69632460cc9c88a2ae39111"

#: Глаголы, меняющие данные или схему. Транзакционные границы входят в пин записей: без них
#: «две записи в одной транзакции» и «две записи в разных» дали бы один дайджест.
_WRITE_VERBS = frozenset({"INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP"})
_TXN_VERBS = frozenset({"BEGIN", "COMMIT", "ROLLBACK"})

#: Последовательность в сжатом виде: ``(сколько раз подряд, оператор)``. Сводка нужна,
#: чтобы падение было читаемым: видно, **где** разошлось, а не только «дайджест другой».
EXPECTED_SEQUENCE: tuple[tuple[int, str], ...] = (
    (1, "BEGIN"),
    (1, "INSERT runs"),
    (1, "COMMIT"),
    (2, "SELECT *"),
    (1, "BEGIN"),
    (1, "INSERT crawl_sessions"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "UPDATE crawl_session_items"),
    (1, "SELECT status,"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (2, "SELECT *"),
    (1, "BEGIN"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT run_product"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT crawl_session_items"),
    (1, "SELECT status,"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT corpus_docs"),
    (1, "INSERT corpus_doc_version"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT crawl_session_items"),
    (1, "SELECT status,"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT crawl_session_items"),
    (1, "SELECT status,"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT corpus_docs"),
    (1, "INSERT corpus_doc_version"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT crawl_session_items"),
    (1, "SELECT status,"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT crawl_session_items"),
    (1, "SELECT status,"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT corpus_docs"),
    (1, "INSERT corpus_doc_version"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT crawl_session_items"),
    (1, "SELECT status,"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT crawl_session_items"),
    (1, "SELECT status,"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT corpus_docs"),
    (1, "INSERT corpus_doc_version"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT crawl_session_items"),
    (1, "SELECT status,"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT crawl_session_items"),
    (1, "SELECT status,"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT crawl_session_items"),
    (1, "SELECT status,"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT corpus_docs"),
    (1, "INSERT corpus_doc_version"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT crawl_session_items"),
    (1, "SELECT status,"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (1, "SELECT status,"),
    (1, "BEGIN"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (5, "INSERT corpus_chunks"),
    (1, "COMMIT"),
    (1, "SELECT *"),
    (1, "BEGIN"),
    (1, "INSERT corpus_coverage"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT run_product"),
    (1, "COMMIT"),
    (1, "SELECT *"),
    (1, "BEGIN"),
    (1, "INSERT corpus_coverage"),
    (1, "COMMIT"),
    (1, "BEGIN"),
    (1, "INSERT index_manifest"),
    (1, "COMMIT"),
    (2, "PRAGMA"),
    (1, "SELECT evidence_id,"),
    (1, "SELECT run_id,"),
    (1, "SELECT status,"),
    (1, "SELECT *"),
    (1, "BEGIN"),
    (1, "UPDATE crawl_sessions"),
    (1, "COMMIT"),
    (1, "SELECT *"),
    (1, "SELECT 1"),
    (2, "SELECT *"),
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
