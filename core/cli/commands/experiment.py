"""``experiment``: эксперименты фиксации параметров **измерением** (A3, T8.6, R9/R10).

Тонкая пост-хок поверхность (A1): открывает БД прогона, разрешает **зафиксированную**
индексную сборку из строки ``runs`` (``runs.index_build_id``/``runs.package_id`` через
:func:`core.cli.wiring.resolve_experiment_target`), собирает порты ретривала P4a-сборкой
(:func:`core.cli.wiring.build_adapters`) и прогоняет retrieval-golden по профилям
R0/R1/R2 (:func:`core.pipeline.experiments.run_retrieval_experiment`).

Результат измерения **не является тупиком** (A3 «дефолт → измерение»): выбранный профиль,
``retrieval_cutoff_K``, ``rrf_k`` и ``embed_model_id`` персистятся в строку ``runs``
(``retrieval_profile``/``retrieval_cutoff_K``/``retrieval_params_json``/``embed_model_id``)
вместе с ``settings_provenance_json`` = ``{"…": {"source": "measured", "experiment_id": …}}``.
Это наблюдают ``hound report`` (манифест) и ``hound diff`` (граница ``execution_change``
§13); версии — класс ``execution_versions``, бампа ``methodology_version`` нет.

**NB1 (эмбеддер ↔ сборка).** Перед прогоном ``embedder.model_id`` сверяется с
``embed_model_id`` сборки (из on-disk ``manifest.json``): равенства ``dim`` недостаточно —
две одинаково размерных разных модели дали бы бессмысленный скор. Расхождение —
**отказ** (:class:`core.cli.wiring.EmbedderBuildMismatch`), а не выдуманное число.

**Честная деградация (N3).** Отсутствие предпосылки (нет сборки в прогоне, нет эмбеддера,
нет продукт-партиций, не поднимаются порты) — явное ``skipped`` с причиной и кодом
возврата ``0`` (пропуск — INFO, не WARN); ранжирование не фабрикуется и команда не падает.
`core/` остаётся категорийно-нейтральным (§1.3): идентификаторы моделей — данные адаптеров.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from core.cli.context import (
    _add_common,
    _infer_run_id,
    _positive_int,
    _print_json,
    _resolve_db_path,
)
from core.cli.errors import EXIT_ERROR, EXIT_OK, EXIT_USAGE, CliError
from core.cli.wiring import (
    EmbedderBuildMismatch,
    assert_embedder_matches_build,
    build_adapters,
    build_store,
    resolve_experiment_target,
)
from core.domain.enums import SkipReason
from core.pipeline import load_retrieval_golden, run_retrieval_experiment
from core.pipeline.evaluate import DEFAULT_RETRIEVAL_CUTOFF_K
from core.pipeline.retrieve import DEFAULT_RRF_K

__all__ = ["run", "configure"]

#: Настройки, фиксируемые измерением (класс ``execution_versions``, §13, A3).
#: Пишутся в ``runs.settings_provenance_json`` как ``source: measured``.
MEASURED_SETTINGS: tuple[str, ...] = (
    "retrieval_profile",
    "retrieval_cutoff_K",
    "embed_model_id",
    "rrf_k",
)

#: Причины пропуска (командный уровень, N3). ``no_embedder`` переиспользует словарь фаз.
SKIP_NO_INDEX_BUILD = "no_index_build"
SKIP_NO_EMBEDDER = SkipReason.NO_EMBEDDER.value
SKIP_NO_INDEX_PORTS = "no_index_ports"
SKIP_UNVERIFIED_EMBEDDER = "embedder_unverified"


def _index_root(args: argparse.Namespace) -> str:
    """Корень ``index_store`` из resolved-раскладки (composition root ``03`` §A.1)."""
    paths = getattr(args, "paths", None)
    return str(paths.index_store) if paths is not None else "index_store"


def _resolve_ports(args: argparse.Namespace, store: Any, target: Any) -> Any:
    """Собирает порты ретривала: инъекция (``args.ports``) либо P4a-сборка (A1).

    Продукт-скоуп (I17) обеспечивает сам ``NpyVectorIndex``/лексический индекс; сборка
    получает ``index_build_id`` из строки ``runs`` (не из сканирования каталогов).
    """
    injected = getattr(args, "ports", None)
    if injected is not None:
        return injected
    return build_adapters(
        profile=args.profile,
        package=target.package,
        index_root=_index_root(args),
        store=store,
        chunk_lookup=store.get_chunk,
        index_build_id=target.build_id,
    )


def _fixation_manifest(
    manifest: Any, report: Any, *, embed_model_id: str | None, experiment_id: str
) -> Any:
    """Манифест с зафиксированным измерением: профиль/K/params + ``provenance: measured``.

    Читает текущий манифест прогона и возвращает копию с измеренными ретривал-полями и
    дополненным (не затёртым) ``settings_provenance``. Никаких side-файлов: запись идёт
    через существующий путь ``StorePort.persist_manifest`` (его читают ``report``/``diff``).
    """
    provenance = dict(getattr(manifest, "settings_provenance", None) or {})
    for name in MEASURED_SETTINGS:
        provenance[name] = {"source": "measured", "experiment_id": experiment_id}
    return manifest.model_copy(
        update={
            "retrieval_profile": report.selected_profile,
            "retrieval_cutoff_K": report.k,
            "retrieval_params_json": report.to_manifest_json(),
            "embed_model_id": embed_model_id,
            "settings_provenance": provenance,
        }
    )


def _emit_skip(args: argparse.Namespace, run_id: str, reason: str, message: str) -> int:
    """Печатает явное ``skipped`` (N3) и возвращает ``EXIT_OK`` — пропуск не сбой."""
    payload = {
        "ok": False,
        "status": "skipped",
        "run_id": run_id,
        "skip_reason": reason,
        "message": message,
    }
    if args.json:
        _print_json(payload)
    else:
        sys.stdout.write(
            f"run_id: {run_id}\nstatus: skipped\nskip_reason: {reason}\n{message}\n"
        )
    return EXIT_OK


def _emit_measured(args: argparse.Namespace, payload: dict[str, Any]) -> None:
    """Печатает результат измерения (человеку либо ``--json``)."""
    if args.json:
        _print_json(payload)
        return
    lines = [
        f"run_id: {payload['run_id']}",
        "status: measured",
        f"package: {payload['package']}",
        f"index_build_id: {payload['index_build_id']}",
        f"embed_model_id: {payload['embed_model_id']}",
        f"cutoff_k: {payload['cutoff_k']}",
        f"rrf_k: {payload['rrf_k']}",
        f"selected_profile: {payload['selected_profile']}",
        f"selection_basis: {payload['selection_basis']}",
    ]
    for name, metrics in sorted(payload["metrics"].items()):
        lines.append(
            f"profile {name}: recall@K={metrics['recall_at_k']:.4f} "
            f"mrr={metrics['mrr']:.4f} hit@K={metrics['hit_at_k']:.4f}"
        )
    for name, reason in sorted(payload.get("skipped", {}).items()):
        lines.append(f"skipped {name}: {reason}")
    lines.append(f"experiment_id: {payload['experiment_id']}")
    sys.stdout.write("\n".join(lines) + "\n")


def run(args: argparse.Namespace) -> int:
    """``hound experiment retrieval``: прогон R0/R1/R2 и фиксация измерения (A3, I20).

    Шаги: открыть БД прогона → разрешить зафиксированную сборку из ``runs`` → собрать порты
    ретривала → загрузить retrieval-golden → прогнать R0/R1/R2 на едином ``cutoff_k`` (I20)
    → персистить выбранный профиль/K/``rrf_k``/``embed_model_id`` и ``provenance: measured``.

    K берётся из ``--cutoff-k``, иначе из зафиксированного ``runs.retrieval_cutoff_K``,
    иначе :data:`DEFAULT_RETRIEVAL_CUTOFF_K` (I20: cut-off golden = pipeline-K).

    :raises CliError: эмбеддер запроса не совпадает со сборкой (NB1) — отказ (§13),
        называется парой идентификаторов. Прочее отсутствие — ``skipped`` (N3).
    """
    db_path = _resolve_db_path(args)
    run_id = args.run_id

    with build_store(db_path, profile=args.profile) as store:
        if run_id is None:
            run_id = _infer_run_id(store)
        manifest = store.read_manifest(run_id)  # валидация: прогон существует

        target = resolve_experiment_target(store, run_id, index_root=_index_root(args))
        if target is None:
            return _emit_skip(
                args,
                run_id,
                SKIP_NO_INDEX_BUILD,
                "в прогоне не зафиксирована индексная сборка (runs.index_build_id пуст) "
                "или не разрешён пакет — эксперимент не запускается (N3)",
            )
        if not target.product_ids:
            return _emit_skip(
                args,
                run_id,
                SKIP_NO_INDEX_BUILD,
                f"сборка {target.build_id!r} не несёт продукт-партиций — оценивать нечего (N3)",
            )

        ports = _resolve_ports(args, store, target)
        embedder = getattr(ports, "embedder", None)
        query_model_id = getattr(embedder, "model_id", None) if embedder is not None else None
        if not query_model_id:
            return _emit_skip(
                args,
                run_id,
                SKIP_NO_EMBEDDER,
                "нет доступного эмбеддера запроса (нет локального снапшота модели / "
                "зависимости) — dense-плечо не выдумывается (I19/A3)",
            )
        if not target.embed_model_id:
            return _emit_skip(
                args,
                run_id,
                SKIP_UNVERIFIED_EMBEDDER,
                f"сборка {target.build_id!r} не несёт embed_model_id — согласованность "
                "«эмбеддер запроса ↔ сборка» недоказуема (NB1)",
            )
        # NB1: сверка ДО прогона. Расхождение — отказ с обоими идентификаторами (не skip).
        try:
            assert_embedder_matches_build(str(query_model_id), str(target.embed_model_id))
        except EmbedderBuildMismatch as exc:
            raise CliError(str(exc), code="§13", exit_code=EXIT_ERROR) from exc

        if (
            getattr(ports, "vector_index", None) is None
            and getattr(ports, "lexical_index", None) is None
        ):
            return _emit_skip(
                args,
                run_id,
                SKIP_NO_INDEX_PORTS,
                f"не удалось поднять порты ретривала для сборки {target.build_id!r} "
                "(нет плотного и лексического индекса) — профили не выдумываются (N3)",
            )

        golden = load_retrieval_golden(args.golden)
        cutoff_k = args.cutoff_k or manifest.retrieval_cutoff_K or DEFAULT_RETRIEVAL_CUTOFF_K
        report = run_retrieval_experiment(
            golden,
            product_ids=list(target.product_ids),
            cutoff_k=int(cutoff_k),
            lexical_index=getattr(ports, "lexical_index", None),
            vector_index=getattr(ports, "vector_index", None),
            embed=lambda text: embedder.embed([text])[0],
            rrf_k=DEFAULT_RRF_K,
            min_delta=args.min_delta,
        )

        experiment_id = f"retrieval/{target.build_id}/K{report.k}"
        updated = _fixation_manifest(
            manifest,
            report,
            embed_model_id=target.embed_model_id,
            experiment_id=experiment_id,
        )
        store.persist_manifest(run_id, updated)

    metrics = json.loads(report.to_manifest_json())["metrics"]
    payload: dict[str, Any] = {
        "ok": True,
        "status": "measured",
        "run_id": run_id,
        "package": target.package,
        "index_build_id": target.build_id,
        "embed_model_id": target.embed_model_id,
        "cutoff_k": report.k,
        "rrf_k": report.rrf_k,
        "selected_profile": report.selected_profile,
        "selection_basis": report.selection_basis,
        "metrics": metrics,
        "skipped": dict(report.skipped),
        "experiment_id": experiment_id,
        "provenance": "measured",
    }
    _emit_measured(args, payload)
    return EXIT_OK


def _no_subcommand(args: argparse.Namespace) -> int:  # noqa: ARG001 — сигнатура handler
    """``hound experiment`` без вида: печатает usage и возвращает :data:`EXIT_USAGE`."""
    sys.stderr.write("usage: hound experiment retrieval ...\n")
    return EXIT_USAGE


def configure(sub: Any) -> None:
    """Регистрирует подпакет ``experiment`` (вид ``retrieval``)."""
    p_exp = sub.add_parser(
        "experiment", help="эксперименты фиксации параметров измерением (A3)"
    )
    exp_sub = p_exp.add_subparsers(dest="experiment_command", metavar="ВИД")
    p_ret = exp_sub.add_parser(
        "retrieval",
        help="прогон R0/R1/R2 на retrieval-golden и фиксация профиля/K (T8.6, I20)",
    )
    source = p_ret.add_mutually_exclusive_group(required=True)
    source.add_argument("--db", default=None, help="путь к cohound.db прогона")
    source.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db; требует --run-id)",
    )
    p_ret.add_argument(
        "--run-id",
        default=None,
        help="run_id (обязателен с --runs-dir; с --db выводится, если прогон один)",
    )
    p_ret.add_argument(
        "--golden",
        required=True,
        help="путь к retrieval-golden (JSON: [{query, expected_ids, lang?}])",
    )
    p_ret.add_argument(
        "--cutoff-k",
        dest="cutoff_k",
        type=_positive_int,
        default=None,
        metavar="K",
        help="retrieval_cutoff_K (I20); дефолт — из runs.retrieval_cutoff_K либо "
        f"{DEFAULT_RETRIEVAL_CUTOFF_K}",
    )
    p_ret.add_argument(
        "--min-delta",
        dest="min_delta",
        type=float,
        default=0.0,
        metavar="DELTA",
        help="порог превосходства над baseline R0 по recall@K, ниже которого сохраняется R0 "
        "(A3); дефолт 0.0",
    )
    _add_common(p_ret)
    p_ret.set_defaults(handler=run)
    # ``hound experiment`` без вида — usage-ошибка, а не трейсбек отсутствующего handler.
    p_exp.set_defaults(handler=_no_subcommand)
