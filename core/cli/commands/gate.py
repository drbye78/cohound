"""``gate``: запись решения гейта G1–G4 (S9/I6/I7; ``03`` §E.2, точки интеграции).

Вынесено по образцу :mod:`core.cli.commands.review`. Модуль экспортирует
:func:`configure` (регистрация аргументов подкоманды) и :func:`run` (тело команды).

**Два типа гейтов, одна команда.**

* ``G1``/``G2`` — внутренняя аттестация: человек утверждает профиль пакета-якоря и матрицу
  роль×продукт.
* ``G3``/``G4`` — точки интеграции: strict-ревью и утверждение публикации выполняет
  **внешняя система**. coHound не принимает эти решения — он пишет запрос
  (``runs/{run_id}/gates/{gate}-request.json``), а эта команда **записывает** полученный
  ответ (``--source external --external-ref <id>``).

**Решение привязано к предмету.** ``subject_digest`` — отпечаток того, о чём решали.
По умолчанию он берётся из файла запроса (внешняя система отвечает на конкретный запрос);
``--subject-digest`` позволяет передать его явно. Решение без отпечатка не записывается:
именно «строка без привязки» проходила гейт вечно, независимо от состояния прогона.

**Вердикт обязателен.** ``--verdict rejected`` — такая же запись, как одобрение, и гейт
её уважает: прогон останавливается с явным «отклонён внешней системой (ref): notes».
Без вердикта отказ выражался бы молчанием, неотличимым от «ещё никто не смотрел».

Повторная подача того же решения (run/gate/target/actor/notes/digest/source/ref/verdict)
идемпотентно перезаписывает ту же строку (детерминированный ``decision_id``).

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import argparse

from core.cli.context import (
    _add_common,
    _infer_run_id,
    _print_gate,
    _print_json,
    _resolve_db_path,
)
from core.cli.errors import EXIT_OK, EXIT_USAGE, CliError
from core.cli.wiring import build_store
from core.domain import Gate, GateSource, GateVerdict
from core.pipeline import read_gate_request_digest, record_decision


def _runs_dir_for(
    args: argparse.Namespace, db_path: str | Path, run_id: str
) -> Path | None:
    """Каталог прогонов для поиска файла запроса (``runs/{run_id}/gates/...``).

    Явный ``--runs-dir`` приоритетен. Иначе выводится из ``--db``, если БД лежит в
    штатной раскладке ``runs/{run_id}/cohound.db`` (имя каталога совпадает с ``run_id``).
    Иначе ``None`` — запроса не найти, и отпечаток придётся передать явно.
    """
    explicit = getattr(args, "runs_dir", None)
    if explicit:
        return Path(explicit)
    path = Path(db_path)
    if path.name == "cohound.db" and path.parent.name == run_id:
        return path.parent.parent
    return None


def _resolve_subject_digest(
    args: argparse.Namespace, db_path: str | Path, run_id: str, gate: Gate
) -> str:
    """``subject_digest`` решения: ``--subject-digest`` либо отпечаток из файла запроса.

    :raises CliError: отпечатка нет ни там, ни там (:data:`EXIT_USAGE`). Записывать
        решение без привязки нельзя: такая строка проходила бы гейт при любом состоянии
        прогона. Запрос пишет сам прогон, когда останавливается на гейте.
    """
    explicit = getattr(args, "subject_digest", None)
    if explicit:
        return str(explicit).strip()
    runs_dir = _runs_dir_for(args, db_path, run_id)
    digest = (
        read_gate_request_digest(runs_dir, run_id, gate)
        if runs_dir is not None
        else None
    )
    if digest:
        return digest
    raise CliError(
        f"нет отпечатка предмета для гейта {gate.value}: запрос на решение "
        f"(gates/{gate.value}-request.json) не найден, --subject-digest не задан. "
        "Укажите --runs-dir <каталог прогонов> (запрос лежит в "
        f"{{runs_dir}}/{{run_id}}/gates/{gate.value}-request.json) или сам отпечаток. "
        "Сначала выполните hound run — он пишет запрос, когда останавливается на гейте; "
        "внешняя система возвращает subject_digest из этого запроса.",
        code="gate_subject",
        exit_code=EXIT_USAGE,
    )


def run(args: argparse.Namespace) -> int:
    """``gate``: записывает решение гейта G1–G4 (S9/I6/I7, ``03`` §E.2).

    Резолвит хранилище как ``sensitivity`` (``--db`` приоритетнее ``--runs-dir`` +
    ``--run-id``; ``run_id`` выводится, если в БД ровно один прогон), валидирует
    существование прогона через :meth:`~core.store.sqlite_store.SqliteStore.read_manifest`,
    определяет ``subject_digest`` (:func:`_resolve_subject_digest`) и персистит решение
    через :func:`core.pipeline.gates.record_decision`. ``actor`` обязателен (I6/I7);
    пустой ``actor`` — :class:`ValueError` → ``EXIT_USAGE``.

    Для ``--source external`` обязательны ``--external-ref`` и ``--notes``: решение без
    ссылки на решающую систему и без причины — не аудит-след (проверяется и здесь, и в
    доменной модели).
    """
    db_path = _resolve_db_path(args)
    run_id = args.run_id
    gate = Gate(args.gate)
    source = GateSource(args.source)
    verdict = GateVerdict(args.verdict)
    if not (args.actor or "").strip():
        # Доменная проверка (``record_decision``) остаётся инвариантом; здесь — ранний
        # отказ с внятным сообщением, до обращения к файлу запроса.
        raise CliError(
            f"гейт {gate.value} не может быть пройден без actor (человек, I6/I7)",
            code="gate_actor",
            exit_code=EXIT_USAGE,
        )
    if source is GateSource.EXTERNAL:
        if not (getattr(args, "external_ref", None) or "").strip():
            raise CliError(
                "--source external требует --external-ref: решение внешней системы "
                "обязано ссылаться на неё",
                code="gate_external_ref",
                exit_code=EXIT_USAGE,
            )
        if not (args.notes or "").strip():
            raise CliError(
                "--source external требует --notes: решение без причины — не аудит-след",
                code="gate_notes",
                exit_code=EXIT_USAGE,
            )
    payload: dict[str, Any] = {}
    with build_store(db_path, profile=args.profile) as store:
        if run_id is None:
            run_id = _infer_run_id(store)
        store.read_manifest(run_id)  # валидация: прогон существует
        subject_digest = _resolve_subject_digest(args, db_path, run_id, gate)
        decision = record_decision(
            store,
            run_id,
            gate,
            actor=args.actor,
            target=args.target,
            notes=args.notes,
            subject_digest=subject_digest,
            source=source,
            external_ref=getattr(args, "external_ref", None),
            verdict=verdict,
        )
        payload = {
            "ok": True,
            "run_id": run_id,
            "gate": decision.gate.value,
            "decision_id": decision.decision_id,
            "actor": decision.actor,
            "decided_at": decision.decided_at,
            "target": decision.target,
            "notes": decision.notes,
            "subject_digest": decision.subject_digest,
            "source": decision.source.value,
            "external_ref": decision.external_ref,
            "verdict": decision.verdict.value,
        }

    if args.json:
        _print_json(payload)
    else:
        _print_gate(payload)
    return EXIT_OK


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``gate``."""
    p_gate = sub.add_parser(
        "gate",
        help=(
            "записать решение гейта G1–G4 (G1/G2 — аттестация, G3/G4 — ответ внешней "
            "системы; S9/I6/I7)"
        ),
    )
    gate_source = p_gate.add_mutually_exclusive_group(required=True)
    gate_source.add_argument("--db", default=None, help="путь к cohound.db прогона")
    gate_source.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db; требует --run-id)",
    )
    p_gate.add_argument(
        "--run-id",
        default=None,
        help="обязателен с --runs-dir; с --db выводится, если прогон один",
    )
    p_gate.add_argument(
        "--gate",
        required=True,
        choices=[g.value for g in Gate],
        help=(
            "гейт G1–G4: G1/G2 — внутренняя аттестация, G3/G4 — точки интеграции "
            "(ответ внешней системы)"
        ),
    )
    p_gate.add_argument(
        "--actor",
        required=True,
        help="человек или система, принявшая решение (I6/I7)",
    )
    p_gate.add_argument(
        "--source",
        default=GateSource.INTERNAL.value,
        choices=[s.value for s in GateSource],
        help=(
            "кто решил: internal — человек в coHound (по умолчанию), external — внешняя "
            "система (требует --external-ref и --notes)"
        ),
    )
    p_gate.add_argument(
        "--external-ref",
        default=None,
        help="идентификатор решения во внешней системе (обязателен для --source external)",
    )
    p_gate.add_argument(
        "--verdict",
        default=GateVerdict.APPROVED.value,
        choices=[v.value for v in GateVerdict],
        help="вердикт: approved (по умолчанию) или rejected — отказ блокирует прогон",
    )
    p_gate.add_argument(
        "--subject-digest",
        default=None,
        help=(
            "отпечаток предмета решения; по умолчанию берётся из "
            "runs/{run_id}/gates/{gate}-request.json"
        ),
    )
    p_gate.add_argument(
        "--target",
        default=None,
        help="что подтверждено: cohort/cell/candidate_id (для G2 — per-candidate)",
    )
    p_gate.add_argument("--notes", default=None, help="произвольная заметка к решению")
    _add_common(p_gate)
    p_gate.set_defaults(handler=run)
