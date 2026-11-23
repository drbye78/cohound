"""``hound crawl drain``: докачать усечённые корни и обновить объявленные пределы.

Зачем одна команда. Докачка корня до исчерпания источников — это три операции, которые
до сих пор делались руками и каждая по-своему опасно: (1) понять, **какие** корни
усечены и на каком числе; (2) докачать их, не перекачивая уже добытое; (3) переписать
``max_documents``/``max_depth`` в ``domain_profiles.yaml`` так, чтобы следующий прогон не
усекал на той же отметке. Пока это три шага, они расходятся: предел поднимают «на глаз»,
а усечение остаётся незамеченным.

Команда ничего не выдумывает: список корней читается из **записанного** провенанса
прогона (``effective_limits.roots``), докачка исполняется той же фазой корпуса под
политикой ``drain`` (известные страницы — из сохранённых байтов, живьём только
неизвестный остаток), а новое значение предела — измеренное число плюс **объявленный**
запас (``--margin-percent``, печатается рядом с замером).

Почему это не «тихая правка конфига». Правка ``domain_profiles.yaml`` меняет
``crawl_config_digest``, то есть ``run_id`` следующего прогона (``execution_change``):
два разных корпуса не должны жить под одной идентичностью. Поэтому команда печатает
каждый проход со своим ``run_id``, а артефакт правки (``sources/crawl_drain/…json``)
несёт измеренное число, запас, прежние и новые значения и имя человека.

**Что объявляется, а что нет.** Предел пишется только для корня, который **докачан**
(обход исчерпан). Корень, который докачать не удалось (лестница глубины не дошла,
причина усечения не лечится докачкой, проход упал), попадает в ``remaining`` с названной
причиной и профиль не трогает: предел по недокачанному наблюдению был бы объявлением
меньше измеренного — то есть гарантированным новым усечением.

Нейтрален к предметной категории (§1.3): домены и ключи — данные.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import sys
from pathlib import Path
from typing import Any

from core.cli.context import _add_common, _print_json
from core.cli.errors import EXIT_ERROR, EXIT_OK, EXIT_USAGE, CliError
from core.cli.run_overview import effective_runs_dir, iter_run_dbs
from core.cli.wiring import build_store, run_db_path
from core.pipeline.crawl_session import default_session_id, read_provenance
from core.pipeline.drain_plan import (
    DEFAULT_DEPTH_STEP,
    DEFAULT_MARGIN_PERCENT,
    DrainedRoot,
    DrainUpdate,
    TruncatedRoot,
    declared_depth,
    drainable_reason,
    measured_roots,
    plan_updates,
    render_profile_limits,
    truncated_roots,
)
from core.util.atomic_write import atomic_write_text

#: Сколько проходов делает лестница глубины за один вызов. Число **объявлено** и
#: печатается: это не потолок измерения, а граница одного вызова — каждый проход
#: публикует свой ``run_id``, и остановка видна, а не молчалива.
DEFAULT_MAX_PASSES = 3

#: Причина усечения, которую лечит подъём глубины (остальные — снятием потолка документов).
_DEPTH_REASON = "max_depth"


# --------------------------------------------------------------------------- #
# Регистрация
# --------------------------------------------------------------------------- #


def configure(crawl: Any) -> None:
    """Регистрирует подкоманду ``crawl drain`` (вызывается из ``crawl.configure``)."""
    p = crawl.add_parser(
        "drain",
        help="докачать усечённые корни до исчерпания и обновить пределы профиля",
    )
    p.add_argument(
        "--package",
        required=True,
        help="каталог пакета (он же источник run.yaml/domain_profiles.yaml)",
    )
    p.add_argument(
        "--run-yaml",
        dest="run_yaml",
        default=None,
        help="run.yaml прогона (дефолт — {package}/run.yaml)",
    )
    p.add_argument(
        "--urls",
        default=None,
        help="корпусный вход прогона (дефолт — {package}/sources/corpus_urls.yaml, "
        "если файл есть)",
    )
    p.add_argument(
        "--from-run",
        dest="from_run",
        default=None,
        help="чей провенанс читать (дефолт — последний прогон пакета по started_at)",
    )
    p.add_argument(
        "--product",
        action="append",
        default=None,
        metavar="ID",
        help="ограничить продукты (повторяемо; дефолт — все усечённые)",
    )
    p.add_argument(
        "--root",
        action="append",
        default=None,
        metavar="HOST[/PATH]",
        help="ограничить корни (повторяемо; форма ключа domain_profiles)",
    )
    p.add_argument(
        "--margin-percent",
        dest="margin_percent",
        type=int,
        default=DEFAULT_MARGIN_PERCENT,
        help=f"запас к измеренному числу, проценты (дефолт {DEFAULT_MARGIN_PERCENT}); "
        "запас объявлен и печатается, а не прячется",
    )
    p.add_argument(
        "--depth-step",
        dest="depth_step",
        type=int,
        default=DEFAULT_DEPTH_STEP,
        help=f"шаг подъёма max_depth при усечении глубиной (дефолт {DEFAULT_DEPTH_STEP})",
    )
    p.add_argument(
        "--max-passes",
        dest="max_passes",
        type=int,
        default=DEFAULT_MAX_PASSES,
        help=f"проходов лестницы глубины за вызов (дефолт {DEFAULT_MAX_PASSES})",
    )
    p.add_argument(
        "--list",
        dest="list_only",
        action="store_true",
        help="только офлайн-отчёт по усечённым корням (без сети и без правки)",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="записать пределы в domain_profiles.yaml (требует --actor)",
    )
    p.add_argument(
        "--actor",
        default=None,
        help="человек, изменивший объявленный предел (обязателен с --apply)",
    )
    p.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (дефолт — resolved раскладка, иначе runs)",
    )
    _add_common(p)
    p.set_defaults(handler=run)


# --------------------------------------------------------------------------- #
# Тело команды
# --------------------------------------------------------------------------- #


def run(args: argparse.Namespace) -> int:
    """``crawl drain``: план → проходы ``drain`` → правка пределов → артефакт.

    :returns: :data:`EXIT_OK` — докачано и (при ``--apply``) записано;
        :data:`EXIT_USAGE` — нечего/некуда (нет прогона, нет ключа профиля, нет
        ``--actor`` при ``--apply``); :data:`EXIT_ERROR` — проход краула вернул ошибку.
    """
    if bool(getattr(args, "apply", False)) and not getattr(args, "actor", None):
        raise CliError(
            "--apply требует --actor (человек, изменяющий объявленный предел)",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    package_dir = Path(args.package)
    profiles_path = package_dir / "domain_profiles.yaml"
    if not profiles_path.is_file():
        raise CliError(
            f"{profiles_path}: нет domain_profiles.yaml — пределы объявляются только там",
            code="package",
            exit_code=EXIT_USAGE,
        )
    if getattr(args, "run_yaml", None) is None:
        args.run_yaml = str(package_dir / "run.yaml")
    if getattr(args, "urls", None) is None:
        candidate = package_dir / "sources" / "corpus_urls.yaml"
        args.urls = str(candidate) if candidate.is_file() else None

    runs_root = Path(effective_runs_dir(args))
    from_run = getattr(args, "from_run", None) or _latest_run_id(runs_root)
    if from_run is None:
        raise CliError(
            f"{runs_root}: ни одного прогона — докачивать нечего (сначала `hound run`)",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    provenance, prov_errors = _read_provenance(runs_root, from_run)
    if prov_errors:
        # Провенанс не прочитан — плана нет. «Ноль усечённых корней» здесь читался бы
        # как измерение («докачивать нечего»), хотя мы просто не смотрели (A3).
        raise CliError(
            "провенанс прогона не прочитан — докачка не планируется: "
            + "; ".join(prov_errors),
            code="usage",
            exit_code=EXIT_USAGE,
        )
    errors: list[str] = []
    roots = truncated_roots(provenance, errors=errors)
    selected = _select(
        roots,
        products=getattr(args, "product", None),
        keys=getattr(args, "root", None),
    )
    remaining: list[dict[str, Any]] = []
    drainable = [root for root in selected if drainable_reason(root.reason)]
    for root in selected:
        if not drainable_reason(root.reason):
            remaining.append(
                {
                    "root": root.root_url,
                    "product_id": root.product_id,
                    "reason": f"not_drainable:{root.reason}",
                    "note": _not_drainable_note(root.reason),
                }
            )
    if getattr(args, "list_only", False):
        payload = {
            "ok": True,
            "from_run": from_run,
            "package": str(package_dir),
            "list_only": True,
            "applied": False,
            "changed": False,
            "margin_percent": int(args.margin_percent),
            "depth_step": int(args.depth_step),
            "truncated_roots": [root.as_dict() for root in roots],
            "selected": [root.as_dict() for root in selected],
            "drainable": [root.as_dict() for root in drainable],
            "drained": [],
            "updates": [],
            "passes": [],
            "remaining": remaining,
            "artifact": None,
            "notes": errors,
        }
        _emit(args, payload)
        return EXIT_OK

    if not drainable:
        payload = {
            "ok": True,
            "from_run": from_run,
            "package": str(package_dir),
            "applied": False,
            "changed": False,
            "margin_percent": int(args.margin_percent),
            "depth_step": int(args.depth_step),
            "truncated_roots": [root.as_dict() for root in roots],
            "selected": [root.as_dict() for root in selected],
            "drained": [],
            "updates": [],
            "passes": [],
            "message": "докачивать нечего",
            "notes": errors,
            "remaining": remaining,
            "artifact": None,
        }
        _emit(args, payload)
        return EXIT_OK

    profile_keys = _profile_keys(profiles_path)
    depth_step = int(args.depth_step)
    #: Что объявить по глубине — решает результат прохода, а не прежнее объявление.
    walked: dict[tuple[str, str], int] = {}
    #: Требуемая глубина следующего прохода: ``None`` — глубина не меняется (лечится
    #: снятием потолка документов), число — объявленный ``max_depth`` + шаг.
    pending: dict[tuple[str, str], int | None] = {
        (root.product_id, root.root_url): (
            root.max_depth + depth_step
            if root.reason == _DEPTH_REASON and root.max_depth is not None
            else None
        )
        for root in drainable
    }
    by_id: dict[tuple[str, str], TruncatedRoot] = {
        (root.product_id, root.root_url): root for root in drainable
    }
    drained: list[DrainedRoot] = []
    passes: list[dict[str, Any]] = []

    for _attempt in range(max(1, int(args.max_passes))):
        if not pending:
            break
        for depth, keys in _group_by_depth(pending):
            code, payload = _execute_pass(
                args,
                keys=[by_id[key].key for key in keys],
                depth=depth,
                runs_root=runs_root,
            )
            run_id = str(payload.get("run_id") or "")
            passes.append(
                {
                    "run_id": run_id,
                    "drain_roots": [by_id[key].key for key in keys],
                    "drain_max_depth": depth,
                    "session_status": payload.get("session_status"),
                    "crawl": payload.get("crawl"),
                    "exit_code": code,
                }
            )
            new_prov, new_errors = _read_provenance(runs_root, run_id)
            errors.extend(new_errors)
            fresh = {
                (root.product_id, root.root_url): root
                for root in measured_roots(new_prov, errors=errors)
            }
            for key in keys:
                root = by_id[key]
                seen = fresh.get(key)
                if seen is None:
                    # Корень не попал в провенанс прохода: число не подменяем прежним.
                    remaining.append(
                        {
                            "root": root.root_url,
                            "product_id": key[0],
                            "reason": "drain_root_not_in_provenance",
                            "run_id": run_id,
                        }
                    )
                    pending.pop(key, None)
                    continue
                if depth is not None:
                    walked[key] = depth
                if not seen.truncated:
                    drained.append(
                        DrainedRoot(
                            product_id=root.product_id,
                            root_url=root.root_url,
                            documents=seen.documents,
                            max_depth=declared_depth(seen, walked.get(key)),
                            previous_max_documents=root.max_documents,
                            previous_max_depth=root.max_depth,
                            replayed=seen.replayed,
                        )
                    )
                    pending.pop(key, None)
                    continue
                if seen.reason == _DEPTH_REASON and depth is not None:
                    # Лестница: следующий проход — глубже. Шаг печатается в ``passes``.
                    pending[key] = depth + depth_step
                    continue
                remaining.append(
                    {
                        "root": root.root_url,
                        "product_id": key[0],
                        "reason": seen.reason or "truncated_reason_unknown",
                        "queue_remaining": seen.queue_remaining,
                        "run_id": run_id,
                    }
                )
                pending.pop(key, None)

    for key in list(pending):
        remaining.append(
            {
                "root": by_id[key].root_url,
                "product_id": key[0],
                "reason": "depth_ladder_exhausted",
                "note": f"проходов израсходовано: {len(passes)}; повторите с --max-passes",
            }
        )

    updates = plan_updates(
        drained,
        profile_keys,
        margin_percent=int(args.margin_percent),
    )
    raw = profiles_path.read_text(encoding="utf-8")
    rendered = render_profile_limits(raw, updates)
    changed = rendered != raw
    artifact: Path | None = None
    if getattr(args, "apply", False) and changed:
        _validate_profile_text(profiles_path, rendered)
        atomic_write_text(profiles_path, rendered)
        artifact = _write_artifact(
            package_dir,
            actor=str(args.actor),
            from_run=from_run,
            margin_percent=int(args.margin_percent),
            updates=updates,
            passes=passes,
            remaining=remaining,
        )

    payload = {
        "ok": True,
        "from_run": from_run,
        "package": str(package_dir),
        "applied": bool(getattr(args, "apply", False)) and changed,
        "changed": changed,
        "margin_percent": int(args.margin_percent),
        "depth_step": depth_step,
        "selected": [root.as_dict() for root in selected],
        "drained": [root.as_dict() for root in drained],
        "updates": [update.as_dict() for update in updates],
        "passes": passes,
        "remaining": remaining,
        "artifact": None if artifact is None else str(artifact),
        "notes": errors,
    }
    _emit(args, payload)
    return EXIT_OK


# --------------------------------------------------------------------------- #
# Проход краула
# --------------------------------------------------------------------------- #


def _pass_argv(
    args: argparse.Namespace, *, keys: list[str], depth: int | None, runs_root: Path
) -> list[str]:
    """Аргументы одного прохода ``crawl run --refresh drain``.

    ``runs_root`` передаётся **всегда**, а не «если флаг задан»: дочерний Namespace
    собирается парсером без ``args.paths`` (его кладёт composition root), поэтому
    опущенный ``--runs-dir`` увёл бы проход в литерал ``runs`` — то есть в другой
    каталог, чем тот, чей провенанс читает команда. Это тихая потеря связи между
    проходом и планом, а не удобство.

    ``--crawl-only --until corpus`` — проход наполняет очередь и **не** конвертирует:
    докачка десятков тысяч страниц не должна тянуть экстракцию и оценку. Конверсию
    делает следующий обычный прогон.
    """
    argv = [
        "crawl",
        "run",
        "--run-yaml",
        str(args.run_yaml),
        "--package",
        str(args.package),
        "--refresh",
        "drain",
        "--crawl-only",
        "--until",
        "corpus",
        "--runs-dir",
        str(runs_root),
    ]
    if getattr(args, "urls", None):
        argv += ["--urls", str(args.urls)]
    if getattr(args, "profile", None):
        argv += ["--profile", str(args.profile)]
    if getattr(args, "env_file", None):
        argv += ["--env-file", str(args.env_file)]
    for key in keys:
        argv += ["--drain-root", key]
    if depth is not None:
        argv += ["--drain-max-depth", str(int(depth))]
    return argv


def _execute_pass(
    args: argparse.Namespace, *, keys: list[str], depth: int | None, runs_root: Path
) -> tuple[int, dict[str, Any]]:
    """Один проход ``crawl run --refresh drain`` для набора корней.

    Namespace собирается **настоящим** парсером (``build_parser``), а не копией
    родительского: копия не несла бы всех флагов, которые читает ``crawl run``, и
    недостающий атрибут выглядел бы как «ось не задана». Печать payload'а — у
    вызывающего: здесь нужен ``run_id``, а не текст.
    """
    from core.cli.commands.crawl import _execute_crawl
    from core.cli.parser import build_parser

    namespace = build_parser().parse_args(
        _pass_argv(args, keys=keys, depth=depth, runs_root=runs_root)
    )
    # Payload нужен целиком (``run_id``/``crawl``/``session_status``) — печать своя.
    namespace.json = True
    return _execute_crawl(namespace, mode="run", clear_first=False)


def _group_by_depth(
    pending: dict[tuple[str, str], int | None],
) -> list[tuple[int | None, list[tuple[str, str]]]]:
    """Группировка корней по требуемой глубине: ось ``drain_max_depth`` одна на проход.

    Ключ корня — ``(product_id, root_url)``: ось исполнения адресует **корень**, а не
    блок профиля, и два продукта на одном блоке — разные корни.

    Корни с ``None`` (лечится снятием потолка документов) идут первой группой: им
    глубина не меняется, и подставлять им чужое число значило бы расширить
    пространство без причины. Порядок внутри группы — ``(product_id, root)``, чтобы
    команда не зависела от порядка завершения корней в прошлом прогоне.
    """
    groups: dict[int | None, list[tuple[str, str]]] = {}
    for key in sorted(pending):
        groups.setdefault(pending[key], []).append(key)
    return sorted(groups.items(), key=lambda item: (item[0] is not None, item[0] or 0))


# --------------------------------------------------------------------------- #
# Прогоны, провенанс, файлы
# --------------------------------------------------------------------------- #


#: Почему корень не докачивается и что вместо этого делать. Причина называется
#: поимённо: «нужен другой механизм» без указания какого — это отказ без выхода.
_NOT_DRAINABLE_NOTES: dict[str, str] = {
    "crawl_policy_skipped": "корень не обходился по решению политики (reuse): снимите "
    "политику (`hound crawl policy clear`) или запустите `--refresh refresh`",
    "crawl_network_denied": "сеть запрещена политикой: `hound crawl policy clear` либо "
    "`--refresh refresh`",
    "round_budget": "бюджет раунда исполнителя, а не потолок корня: `max_documents` "
    "его не лечит",
    "disk_low_headroom": "не хватило места на диске: освободите место и повторите",
    "blocked": "источник отказал (robots/auth): байты вносятся как объявленный вход — "
    "`hound artifacts fetch`",
}


def _not_drainable_note(reason: str | None) -> str:
    """Объяснение отказа по причине усечения (``None`` — причина не записана)."""
    if reason is None:
        return "причина усечения не записана — докачка не берётся за неизвестное"
    return _NOT_DRAINABLE_NOTES.get(
        reason, "докачка не лечит эту причину: она не про потолок корня"
    )


def _read_provenance(
    runs_root: Path, run_id: str
) -> tuple[dict[str, Any], list[str]]:
    """Провенанс сеанса краула прогона: ``(provenance, notes)``.

    Читается **только на чтение** и только записанный факт: пересчитывать «был ли корень
    усечён» этой командой значило бы отвечать на вопрос прежней формулы. Сбой чтения —
    названная заметка, а не исключение: пустой провенанс честно означает «докачивать
    нечего по этому прогону».
    """
    db_path = run_db_path(runs_root, run_id)
    notes: list[str] = []
    if not db_path.is_file():
        return {}, [f"{db_path}: БД прогона отсутствует — провенанс не прочитан"]
    try:
        with build_store(db_path, read_only=True) as store:
            session = store.get_crawl_session(default_session_id(run_id))
            return (read_provenance(session) if session is not None else {}), notes
    except Exception as exc:  # noqa: BLE001 — чтение провенанса best-effort
        notes.append(f"{db_path}: провенанс не прочитан: {type(exc).__name__}: {exc}")
        return {}, notes


def _latest_run_id(runs_root: Path) -> str | None:
    """Последний прогон по **артефакту** (``crawl_sessions.started_at``).

    Не по ``mtime`` каталога: время файла говорит о копировании, а не о прогоне.
    """
    best: tuple[str, str] | None = None
    for db_path in iter_run_dbs(runs_root):
        try:
            with build_store(db_path, read_only=True) as store:
                session = store.get_crawl_session(
                    default_session_id(db_path.parent.name)
                )
                if session is None or not session.started_at:
                    continue
                stamp = str(session.started_at)
        except Exception:  # noqa: BLE001, S112 — битый/чужой файл не участвует в выборе
            continue
        if best is None or stamp > best[1]:
            best = (db_path.parent.name, stamp)
    return None if best is None else best[0]


def _profile_keys(path: Path) -> list[str]:
    """Ключи ``domain_profiles.yaml`` (порядок файла — авторский)."""
    import yaml

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if data is None:
        return []
    if not isinstance(data, dict):
        raise CliError(
            f"{path}: ожидался YAML-словарь верхнего уровня",
            code="package",
            exit_code=EXIT_USAGE,
        )
    return [str(key) for key in data]


def _select(
    roots: list[TruncatedRoot], *, products: list[str] | None, keys: list[str] | None
) -> list[TruncatedRoot]:
    """Отбор усечённых корней по ``--product``/``--root`` (пусто — все).

    Запрошенный, но отсутствующий в провенансе корень — отказ, а не пустой отбор:
    «такого усечённого корня нет» и «я его не нашёл» — разные ответы.
    """
    wanted_products = {str(item) for item in (products or [])}
    wanted_keys = {str(item) for item in (keys or [])}
    selected = [
        root
        for root in roots
        if (not wanted_products or root.product_id in wanted_products)
        and (not wanted_keys or root.key in wanted_keys)
    ]
    if wanted_keys:
        missing = sorted(wanted_keys - {root.key for root in roots})
        if missing:
            raise CliError(
                "--root: таких усечённых корней нет в провенансе: " + ", ".join(missing),
                code="usage",
                exit_code=EXIT_USAGE,
            )
    return selected


def _validate_profile_text(path: Path, text: str) -> None:
    """Проверяет отрендеренный текст **до** записи (загрузчик — тот же, что у прогона)."""
    from core.config.domain_profiles import load_domain_profiles

    probe = path.with_name(f".{path.name}.drain-check")
    try:
        probe.write_text(text, encoding="utf-8")
        load_domain_profiles(probe)
    except ValueError as exc:
        raise CliError(
            f"{path}: правка сделала файл невалидным ({exc}); файл не изменён",
            code="error",
            exit_code=EXIT_ERROR,
        ) from exc
    finally:
        probe.unlink(missing_ok=True)


def _write_artifact(
    package_dir: Path,
    *,
    actor: str,
    from_run: str,
    margin_percent: int,
    updates: list[DrainUpdate],
    passes: list[dict[str, Any]],
    remaining: list[dict[str, Any]],
) -> Path:
    """Пишет артефакт правки: что измерено, что объявлено, кем и по какому прогону."""
    today = _dt.date.today().isoformat()
    target = package_dir / "sources" / "crawl_drain" / f"{today}-{actor}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "kind": "crawl_drain",
        "date": today,
        "actor": actor,
        "from_run": from_run,
        "margin_percent": margin_percent,
        "updates": [update.as_dict() for update in updates],
        "passes": passes,
        "remaining": remaining,
    }
    atomic_write_text(target, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return target


# --------------------------------------------------------------------------- #
# Печать
# --------------------------------------------------------------------------- #


def _emit(args: argparse.Namespace, payload: dict[str, Any]) -> None:
    """Печать результата: ``--json`` ‖ человекочитаемая форма."""
    if getattr(args, "json", False):
        _print_json(payload)
        return
    lines = [
        f"from_run: {payload['from_run']}",
        f"margin: {payload['margin_percent']} % (объявленный запас, не замер)",
    ]
    if payload.get("list_only"):
        lines.append(
            f"усечённых корней: {len(payload['truncated_roots'])}; "
            f"докачиваемых: {len(payload['drainable'])}"
        )
        # Сводка по причинам: у продукта с тысячами корней одна и та же причина
        # повторяется тысячу раз, и детальная строка на каждый корень прячет форму.
        # Это **оформление**, не потолок: полный список — `--json` (ничего не скрыто).
        by_reason: dict[str, int] = {}
        for root in payload["truncated_roots"]:
            reason = str(root["truncated_reason"])
            by_reason[reason] = by_reason.get(reason, 0) + 1
        for reason, count in sorted(by_reason.items()):
            note = _not_drainable_note(None if reason == "None" else reason)
            lines.append(f"  причина {reason}: {count} — {note}")
        for root in payload["drainable"]:
            lines.append(
                f"  докачать {root['product_id']}/{root['key']}: "
                f"добыто={root['pages_fetched']}, "
                f"остаток_очереди={root['queue_remaining']}, "
                f"max_depth={root['max_depth']}, max_documents={root['max_documents']}"
            )
        if payload["truncated_roots"] and len(payload["truncated_roots"]) > len(
            payload["drainable"]
        ):
            lines.append(
                "  (по-корневой список непригодных к докачке — `--json`; "
                "сводка выше называет причины и что делать)"
            )
    if payload.get("message"):
        lines.append(str(payload["message"]))
    for update in payload.get("updates") or ():
        depth_note = ""
        if update["max_depth"] is not None and (
            update["max_depth"] != update["previous_max_depth"]
        ):
            depth_note = (
                f", max_depth {update['previous_max_depth']} → {update['max_depth']}"
            )
        lines.append(
            f"  {update['key']}: измерено {update['measured']} → max_documents "
            f"{update['previous_max_documents']} → {update['max_documents']}"
            f"{depth_note}"
        )
    for item in payload.get("passes") or ():
        lines.append(
            f"  pass {item['run_id']}: roots={len(item['drain_roots'])} "
            f"depth={item['drain_max_depth']} status={item['session_status']}"
        )
    # «Не докачан» печатается по причинам: у продукта с тысячами корней одна причина
    # повторяется тысячу раз и прячет форму результата. Полный список — `--json`.
    refused: dict[str, list[dict[str, Any]]] = {}
    for item in payload.get("remaining") or ():
        refused.setdefault(str(item["reason"]), []).append(item)
    for reason, items in sorted(refused.items()):
        if reason.startswith("not_drainable:"):
            lines.append(
                f"  НЕ докачан: {len(items)} корн. — {reason} "
                f"({items[0].get('note') or 'без объяснения'})"
            )
            continue
        for item in items:
            lines.append(
                f"  НЕ докачан: {item['product_id']}/{item['root']} ({reason})"
            )
    if payload.get("applied"):
        lines.append(f"записано: {payload['package']}/domain_profiles.yaml")
        lines.append(f"артефакт: {payload['artifact']}")
        lines.append(
            "execution_change: следующий прогон профиля получит новый run_id "
            "(изменён crawl_config_digest)"
        )
    elif payload.get("list_only"):
        pass
    elif payload.get("changed") is False:
        lines.append("пределы уже соответствуют измерению — файл не изменён")
    elif not payload.get("applied"):
        lines.append("--dry-run: правка не записана (повторите с --apply --actor)")
    for note in payload.get("notes") or ():
        lines.append(f"  note: {note}")
    sys.stdout.write("\n".join(lines) + "\n")
