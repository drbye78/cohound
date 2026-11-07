"""``experiment``: эксперименты фиксации параметров **измерением** (A3, T8.6, R9/R10).

Тонкая пост-хок поверхность (A1): открывает БД прогона, разрешает **зафиксированную**
индексную сборку из строки ``runs`` (``runs.index_build_id``/``runs.package_id`` через
:func:`core.cli.wiring.resolve_experiment_target`), собирает порты ретривала P4a-сборкой
(:func:`core.cli.wiring.build_adapters`) и прогоняет retrieval-golden по профилям
R0/R1/R2 (:func:`core.pipeline.experiments.run_retrieval_experiment`).

Результат измерения **не является тупиком** (A3 «дефолт → измерение»): выбранный профиль,
``retrieval_cutoff_K``, ``rrf_k`` и ``embed_model_id`` персистятся в строку ``runs``
(``retrieval_profile``/``retrieval_cutoff_K``/``retrieval_params_json``/``embed_model_id``)
вместе с ``settings_provenance_json``. Это наблюдают ``hound report`` (манифест) и
``hound diff`` (граница ``execution_change`` §13); версии — класс ``execution_versions``,
бампа ``methodology_version`` нет.

**A3 (что именно «измерено»).** ``source: measured`` получает только то, что этот прогон
действительно измерил: ``retrieval_profile``, и лишь когда сравнение было **полным** (все
профили реально прогнаны). K задан извне, ``rrf_k`` экспериментом не варьируется,
``embed_model_id`` прочитан из сборки — они записываются как ``source: default``.
Неполное сравнение (часть профилей ``skipped``) тоже даёт ``default``: выбор профиля по
усечённому сравнению — не измерение, и выдавать его за измерение запрещает A3.

**NB1 (эмбеддер ↔ сборка).** Перед прогоном ``embedder.model_id`` сверяется с
``embed_model_id`` сборки (из on-disk ``manifest.json``): равенства ``dim`` недостаточно —
две одинаково размерных разных модели дали бы бессмысленный скор. Расхождение —
**отказ** (:class:`core.cli.wiring.EmbedderBuildMismatch`), а не выдуманное число.

**Честная деградация (N3).** Отсутствие предпосылки (нет сборки в прогоне, нет эмбеддера,
нет продукт-партиций, не поднимаются порты) — явное ``skipped`` с причиной и кодом
возврата ``0`` (пропуск — INFO, не WARN); ранжирование не фабрикуется и команда не падает.
`core/` остаётся категорийно-нейтральным (§1.3): идентификаторы моделей — данные адаптеров.

**``experiment evaluator-benchmark``** (A3, Ш6) — второй вид: измеряет **модели оценщика**
при фиксированном всём остальном (сборка, профиль/K/``rrf_k``, промпт, срез чанков,
``seed=0``/``temperature=0.0``/``max_retries=2``/``strict``). Ретривал выполняется один раз
до моделей, ``evaluate_cell`` идёт с ``store=None`` (измерение, не прогон). Модель берётся
**только** из ``--model``: env ``COHOUND_LLM_*`` даёт лишь endpoint/ключ/декодер — run
identity намеренно игнорирует env, и benchmark не должен воспроизводить это расхождение.
Артефакт — ``runs/{run_id}/benchmark/evaluator-<timestamp>.json``; код возврата ``0`` даже
при плохом счёте модели, ненулевой — только при отсутствии предпосылки.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import sys
from pathlib import Path
from typing import Any

from core.cli.context import (
    _add_common,
    _concise,
    _infer_run_id,
    _load_golden_arg,
    _positive_int,
    _print_json,
    _resolve_db_path,
    _restore_plan_inputs,
)
from core.cli.errors import EXIT_ERROR, EXIT_OK, EXIT_USAGE, CliError
from core.cli.wiring import (
    EmbedderBuildMismatch,
    assert_embedder_matches_build,
    build_adapters,
    build_evaluator_llm,
    build_store,
    resolve_experiment_target,
)
from core.config.deploy import LLM_SLOTS, load_deploy_config
from core.domain.enums import ReasoningEffort, SkipReason
from core.pipeline import (
    BENCHMARK_CONTRACT,
    DEFAULT_PROFILES,
    PRECONDITION_GOLDEN_DECLINED,
    BenchmarkPreconditionError,
    ModelUnderTest,
    load_retrieval_golden,
    prepare_cases,
    run_benchmark,
    run_retrieval_experiment,
)
from core.pipeline.evaluate import DEFAULT_RETRIEVAL_CUTOFF_K
from core.pipeline.golden_status import GoldenStatus, read_golden_status
from core.pipeline.retrieve import DEFAULT_RRF_K

__all__ = ["configure", "run", "run_evaluator_benchmark"]

#: Настройки-предпосылки: попадают в артефакт, но НЕ измерены этим прогоном.
#: ``retrieval_cutoff_K`` задан оператором/зафиксирован ранее, ``rrf_k`` не варьируется
#: экспериментом, ``embed_model_id`` прочитан из сборки. Помечать их ``measured`` —
#: ровно тот подлог, который запрещает A3 (§13: предположение ≠ измерение).
INPUT_SETTINGS: tuple[str, ...] = (
    "retrieval_cutoff_K",
    "rrf_k",
    "embed_model_id",
)

#: Единственная настройка, которую этот эксперимент действительно измеряет (A3).
MEASURED_SETTING = "retrieval_profile"

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
    manifest: Any,
    report: Any,
    *,
    embed_model_id: str | None,
    experiment_id: str,
    profile_measured: bool,
) -> Any:
    """Манифест с зафиксированным результатом: профиль + честный провенанс (A3, §13).

    Читает текущий манифест прогона и возвращает копию с ретривал-полями и дополненным
    (не затёртым) ``settings_provenance``. Никаких side-файлов: запись идёт через
    существующий путь ``StorePort.persist_manifest`` (его читают ``report``/``diff``).

    ``source: measured`` ставится **только** тому, что этот прогон действительно измерил:
    ``retrieval_profile`` — и лишь при ``profile_measured`` (все три профиля реально
    прогнаны). Иначе записывается ``default`` с картой ``skipped``: выбор профиля по
    неполному сравнению — не измерение, и выдавать его за измерение запрещает A3.
    Настройки-предпосылки (:data:`INPUT_SETTINGS`) всегда ``default``: K задан извне,
    ``rrf_k`` экспериментом не варьируется, ``embed_model_id`` прочитан из сборки.
    """
    provenance = dict(getattr(manifest, "settings_provenance", None) or {})
    provenance[MEASURED_SETTING] = (
        {"source": "measured", "experiment_id": experiment_id}
        if profile_measured
        else {"source": "default", "skipped": dict(report.skipped)}
    )
    for name in INPUT_SETTINGS:
        provenance[name] = {"source": "default"}
    return manifest.model_copy(
        update={
            "retrieval_profile": report.selected_profile,
            "retrieval_cutoff_K": report.k,
            "retrieval_params_json": report.to_manifest_json(),
            "embed_model_id": embed_model_id,
            "settings_provenance": provenance,
        }
    )


def _emit_skip(
    args: argparse.Namespace,
    run_id: str,
    reason: str,
    message: str,
    *,
    exit_code: int = EXIT_OK,
    details: dict[str, Any] | None = None,
) -> int:
    """Печатает явное ``skipped`` (N3) и возвращает ``exit_code`` (по умолчанию успех).

    Пропуск ретривал-эксперимента — INFO (``EXIT_OK``); для benchmark отсутствие
    предпосылки (нет чанков/golden) — ненулевой код: измерение не состоялось.

    ``details`` — машинно-читаемое разложение второго уровня (напр. причины пропуска по
    строкам golden). Главная причина остаётся в ``skip_reason``: ``details`` её не
    переопределяют и печатаются только в ``--json`` (человеку хватает ``message``).
    """
    payload: dict[str, Any] = {
        "ok": False,
        "status": "skipped",
        "run_id": run_id,
        "skip_reason": reason,
        "message": message,
    }
    if details:
        payload["details"] = details
    if args.json:
        _print_json(payload)
    else:
        sys.stdout.write(
            f"run_id: {run_id}\nstatus: skipped\nskip_reason: {reason}\n{message}\n"
        )
    return exit_code


def _emit_measured(args: argparse.Namespace, payload: dict[str, Any]) -> None:
    """Печатает результат измерения (человеку либо ``--json``)."""
    if args.json:
        _print_json(payload)
        return
    lines = [
        f"run_id: {payload['run_id']}",
        "status: measured",
        f"provenance: {payload.get('provenance', 'measured')}",
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
        # A3: профиль считается измеренным только если сравнение было **полным** — прогнаны
        # все запрошенные профили (не только «нет skipped»: сужаем запрос — тоже неполнота).
        # Частичное сравнение → provenance: default (выбор по усечённому сравнению ≠ измерение).
        profile_measured = not report.skipped and len(report.by_profile) == len(DEFAULT_PROFILES)
        updated = _fixation_manifest(
            manifest,
            report,
            embed_model_id=target.embed_model_id,
            experiment_id=experiment_id,
            profile_measured=profile_measured,
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
        "provenance": "measured" if profile_measured else "default",
    }
    _emit_measured(args, payload)
    return EXIT_OK


# ---------------------------------------------------------------------------
# ``experiment evaluator-benchmark``: измерение моделей оценщика (A3, Ш6)
# ---------------------------------------------------------------------------

#: Кандидаты по умолчанию (данные, не решение LLM; A3): три модели на одном
#: [OI]-совместимом endpoint и одном ключе. Список переопределяется ``--model``.
DEFAULT_BENCHMARK_MODELS: tuple[str, ...] = (
    "qwen3.8-omni-flash:cxa",
    "gpt-6-luna:purple",
    "deepseek-v4.1-flash-1017:cxb",
)

#: Подкаталог артефакта в каталоге прогона: ``runs/{run_id}/benchmark/``.
BENCHMARK_ARTIFACT_DIR = "benchmark"

def _build_benchmark_models(
    args: argparse.Namespace, deploy_llm: Any
) -> list[ModelUnderTest]:
    """Собирает кандидатов: ``model_id`` — **из флага**, endpoint/ключ/декодер — из слота.

    ``COHOUND_LLM_MODEL`` намеренно не участвует: run identity игнорирует env
    (``execution_axes``), и benchmark, меняющий модель через среду, воспроизвёл бы ровно
    тот баг, от которого защищает эта граница. Из окружения/слота берутся только
    ``base_url``/``api_key``/``decoder``/``timeout``.

    **Уровень рассуждения — тот же путь, что в прогоне.** ``--reasoning-effort`` уходит
    в :func:`core.cli.wiring.build_evaluator_llm` как ``reasoning_effort`` и проверяется
    :func:`core.cli.wiring.ensure_reasoning_wire`: невыразимый для модели уровень —
    ``CliError`` (``EXIT_USAGE``), а не тихий пропуск параметра. ``--max-tokens`` —
    объявленный потолок генерации: он ставится полю адаптера, откуда его читает провод
    (``LocalEvaluator._http_complete``), как и потолок из слота в прогоне.

    :raises CliError: endpoint не сконфигурирован — измерять не на чем (:data:`EXIT_USAGE`).
    """
    model_ids = list(args.model or DEFAULT_BENCHMARK_MODELS)
    reasoning_effort = getattr(args, "reasoning_effort", None)
    max_tokens = getattr(args, "max_tokens", None)
    models: list[ModelUnderTest] = []
    for model_id in model_ids:
        evaluator = build_evaluator_llm(
            deploy_llm=deploy_llm,
            slot=getattr(args, "llm_slot", None),
            generate=getattr(args, "evaluator_generate", None),
            model_id=model_id,
            # Версия весов не берётся из слота: слот описывает активную модель прогона, а
            # здесь их несколько. Неизвестная версия честно пуста, а не чужая (§13).
            model_version="",
            # Контракт измерения: greedy + фиксированный seed (A3/§7.1).
            seed=int(BENCHMARK_CONTRACT["seed"]),
            temperature=float(BENCHMARK_CONTRACT["temperature"]),
            # Уровень рассуждения резолвится и проверяется той же функцией, что в прогоне;
            # невыразимый уровень даёт EXIT_USAGE до первого вызова.
            reasoning_effort=reasoning_effort,
        )
        if evaluator is None:
            raise CliError(
                "LLM endpoint оценщика не сконфигурирован: задайте base_url в слоте "
                f"deploy.llm.{getattr(args, 'llm_slot', None) or '<slot>'} или "
                "COHOUND_LLM_BASE_URL; model_id берётся из --model, не из env",
                code="llm",
                exit_code=EXIT_USAGE,
            )
        if max_tokens is not None:
            # Явный потолок генерации перекрывает потолок слота: это ось замера, а не
            # настройка вида. ``setattr`` — потому что порт оценщика потолок не объявляет
            # (он в ``LLMRequest``/адаптере), а ``evaluator`` здесь конкретный адаптер.
            setattr(evaluator, "max_tokens", int(max_tokens))  # noqa: B010 — поле адаптера
        models.append(
            ModelUnderTest(
                model_id=model_id,
                label=model_id,
                evaluator=evaluator,
                model_version=str(getattr(evaluator, "model_version", "") or ""),
                decoder=str(getattr(evaluator, "decoder", "none") or "none"),
                endpoint_kind=str(getattr(evaluator, "endpoint_kind", "api") or "api"),
            )
        )
    return models

def _benchmark_artifact_path(db_path: str | Path) -> Path:
    """``runs/{run_id}/benchmark/evaluator-<timestamp>.json`` рядом с БД прогона."""
    stamp = _dt.datetime.now(_dt.UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return Path(str(db_path)).parent / BENCHMARK_ARTIFACT_DIR / f"evaluator-{stamp}.json"

def _fmt_ci_dict(data: dict[str, Any] | None) -> str:
    """CI из JSON-словаря: ``[low;high]`` либо ``n/a`` (расчёт не определён)."""
    if not data or data.get("low") is None or data.get("high") is None:
        return "n/a"
    return f"[{data['low']:.2f};{data['high']:.2f}]"

def _benchmark_table(payload: dict[str, Any]) -> str:
    """Markdown-таблица измерений (модель | validity | κ | abstention | …).

    Колонки ``reasoning/cell``, ``length`` и ``invalid JSON`` показывают цену уровня
    рассуждения: доля обрезаний по потолку и невалидных ответов, приходящихся на него.
    """
    lines = [
        "| model | validity (95% CI) | κ (95% CI) | abstention | fabricated | "
        "determinism | tokens/cell | reasoning/cell | length | invalid JSON | p95 ms | "
        "reasoning_effort | max_tokens |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for model in payload["models"]:
        validity = model["validity"]
        agreement = model["agreement"]
        abstention = model["abstention"]
        determinism = model["determinism"]
        kappa = agreement["kappa"]
        kappa_text = (
            f"{kappa:.2f} {_fmt_ci_dict(agreement['kappa_ci95'])}"
            if kappa is not None
            else f"n/a ({agreement['undefined_reason'] or 'не определён'})"
        )
        absent_rate = abstention["absent_rate"]
        abstention_text = (
            f"{absent_rate:.2f} ({abstention['answered_absent']}/{abstention['golden_absent']})"
            if absent_rate is not None
            else "n/a"
        )
        if determinism["measured"] and determinism["stable_fraction"] is not None:
            determinism_text = f"{determinism['stable_fraction']:.2f}"
        else:
            determinism_text = "n/a"
        length_fraction = model["finish_reason"]["length_fraction"]
        length_text = "n/a" if length_fraction is None else f"{length_fraction:.2f}"
        invalid_fraction = model["invalid_json"]["fraction"]
        invalid_text = "n/a" if invalid_fraction is None else f"{invalid_fraction:.2f}"
        reasoning_mean = model["reasoning"]["mean"]
        axes = model["axes"]
        lines.append(
            "| {label} | {rate:.2f} {ci} | {kappa} | {abst} | {fab}/{ok} | {det} | "
            "{tokens:.0f} | {reasoning:.0f} | {length} | {invalid} | {p95:.1f} | "
            "{effort} | {max_tokens} |".format(
                label=model["label"],
                effort=axes.get("reasoning_effort") or "n/a",
                max_tokens=axes.get("max_tokens") or "n/a",
                rate=validity["rate"],
                ci=_fmt_ci_dict(validity["ci95"]),
                kappa=kappa_text,
                abst=abstention_text,
                fab=abstention["fabricated_support"],
                ok=abstention["golden_ok"],
                det=determinism_text,
                tokens=model["tokens"]["per_cell_mean"],
                reasoning=reasoning_mean,
                length=length_text,
                invalid=invalid_text,
                p95=model["latency_ms"]["p95"],
            )
        )
    return "\n".join(lines)

def _emit_benchmark(args: argparse.Namespace, payload: dict[str, Any]) -> None:
    """Печатает результат: ``--json`` — payload, иначе компактная сводка + таблица."""
    if args.json:
        _print_json(payload)
        return
    lines = [
        f"run_id: {payload['run_id']}",
        "status: measured",
        f"index_build_id: {payload['index_build_id']}",
        "retrieval: profile={profile} K={cutoff_k} rrf_k={rrf_k}".format(
            **payload["retrieval"]
        ),
        f"n_cells: {payload['n_cells']} (products: {payload['n_products']})",
        f"concurrency: {payload['contract']['concurrency']}",
        f"input_digest: {payload['input_digest']}",
        f"skipped: {len(payload['skipped'])}",
        f"artifact: {payload['artifact']}",
    ]
    if payload["limits"]:
        lines.append("limits:")
        lines.extend(f"  - {limit}" for limit in payload["limits"])
    lines.append("")
    lines.append(_benchmark_table(payload))
    sys.stdout.write("\n".join(lines) + "\n")

def _benchmark_golden_status(args: argparse.Namespace) -> GoldenStatus | None:
    """Объявленное состояние эталона для benchmark (P6b).

    Источник — файл пакета ``sources/golden_status.yaml`` (носитель авторского входа,
    :mod:`core.pipeline.golden_status`). Каталог пакета: явный ``--package``; без него —
    каталог ``--golden`` (``golden.csv`` лежит в корне пакета, §4.2), иначе самая частая
    форма вызова теряла бы объявление и снова отчитывалась бы ``no_cases``.

    Битое/противоречивое объявление — **видимая** ошибка (``EXIT_USAGE``, код ``golden``):
    трактовать его как «объявления нет» значило бы вернуть ровно ту потерю, ради которой
    файл и заведён.
    """
    package_dir: Path | None = None
    if getattr(args, "package", None):
        package_dir = Path(str(args.package))
    elif getattr(args, "golden", None):
        package_dir = Path(str(args.golden)).parent
    if package_dir is None:
        return None
    try:
        return read_golden_status(package_dir)
    except (OSError, ValueError) as exc:
        raise CliError(_concise(exc), code="golden", exit_code=EXIT_USAGE) from exc


def run_evaluator_benchmark(args: argparse.Namespace) -> int:
    """``hound experiment evaluator-benchmark``: измерение моделей на сценарии Ш6 (A3).

    Всё, кроме модели, фиксировано и берётся из прогона: сборка/композиция, профиль
    ретривала, ``retrieval_cutoff_K``, ``rrf_k``, промпт и **один и тот же** срез чанков
    на ячейку (ретривал выполняется один раз). ``store=None`` у оценщика: это измерение,
    а не прогон — ни ячейки, ни evidence не пишутся.

    Код возврата: ``0`` — измерение состоялось, даже если модель плоха; ненулевой —
    только при отсутствии предпосылки (нет сборки/продуктов/чанков/golden) с видимой
    причиной (честная деградация N3). Объявленный отказ эталона
    (``sources/golden_status.yaml: state: declined``) — отдельный исход
    ``golden_declined`` с автором/датой/причиной и ``EXIT_USAGE``: это не сбой прибора и
    не «всё хорошо», а отозванный человеком вход (как в ``hound golden``).
    """
    db_path = _resolve_db_path(args)
    run_id = args.run_id
    with build_store(db_path, profile=args.profile) as store:
        if run_id is None:
            run_id = _infer_run_id(store)
        store.read_manifest(run_id)  # валидация: прогон существует

        target = resolve_experiment_target(store, run_id, index_root=_index_root(args))
        if target is None:
            return _emit_skip(
                args,
                run_id,
                SKIP_NO_INDEX_BUILD,
                "в прогоне не зафиксирована индексная сборка (runs.index_build_id пуст) "
                "или не разрешён пакет — измерять нечего",
                exit_code=EXIT_ERROR,
            )
        if not target.product_ids:
            return _emit_skip(
                args,
                run_id,
                SKIP_NO_INDEX_BUILD,
                f"сборка {target.build_id!r} не несёт продукт-партиций — измерять нечего",
                exit_code=EXIT_ERROR,
            )

        ports = _resolve_ports(args, store, target)
        if (
            getattr(ports, "vector_index", None) is None
            and getattr(ports, "lexical_index", None) is None
        ):
            return _emit_skip(
                args,
                run_id,
                SKIP_NO_INDEX_PORTS,
                f"не удалось поднять порты ретривала для сборки {target.build_id!r} "
                "(нет плотного и лексического индекса) — срез чанков не выдумывается",
                exit_code=EXIT_ERROR,
            )

        golden = _load_golden_arg(args.golden)
        # P6b: объявленное состояние эталона читается из пакета, а не угадывается по
        # `no_cases`: инструмент измерения точности обязан назвать **объявленный** отказ
        # эталона, а не растворять его в причинах уровня строк.
        golden_status = _benchmark_golden_status(args)
        catalog, _weights, _cohort, notes = _restore_plan_inputs(
            args, run_id, purpose="evaluator-benchmark", store=store
        )
        try:
            prepared = prepare_cases(
                store=store,
                run_id=run_id,
                golden=golden,
                catalog=catalog,
                product_ids=target.product_ids,
                build_id=target.build_id,
                lexical_index=getattr(ports, "lexical_index", None),
                vector_index=getattr(ports, "vector_index", None),
                embedder=getattr(ports, "embedder", None),
                limit=args.limit,
                golden_status=golden_status,
            )
        except BenchmarkPreconditionError as exc:
            return _emit_skip(
                args,
                run_id,
                exc.reason,
                str(exc),
                # Отозванный эталон — «вход не выполним» (EXIT_USAGE), как в `hound
                # golden`: это не сбой прибора (EXIT_ERROR) и не «всё хорошо» (EXIT_OK).
                exit_code=(
                    EXIT_USAGE
                    if exc.reason == PRECONDITION_GOLDEN_DECLINED
                    else EXIT_ERROR
                ),
                details=exc.details,
            )

        models = _build_benchmark_models(args, load_deploy_config(None).llm)
        report = run_benchmark(
            prepared.cases,
            models,
            repeats=args.repeats,
            clock=getattr(args, "clock", None),
            skipped=prepared.skipped,
            notes=tuple(notes) + prepared.notes,
            concurrency=int(getattr(args, "concurrency", 1) or 1),
        )

    payload = report.to_dict()
    artifact = _benchmark_artifact_path(db_path)
    artifact.parent.mkdir(parents=True, exist_ok=True)
    payload["artifact"] = str(artifact)
    artifact.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    _emit_benchmark(args, payload)
    return EXIT_OK

def _no_subcommand(args: argparse.Namespace) -> int:  # noqa: ARG001 — сигнатура handler
    """``hound experiment`` без вида: печатает usage и возвращает :data:`EXIT_USAGE`."""
    sys.stderr.write(
        "usage: hound experiment {retrieval|evaluator-benchmark} ...\n"
    )
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

    p_bench = exp_sub.add_parser(
        "evaluator-benchmark",
        help="измерение моделей оценщика (Ш6) при фиксированных прочих условиях (A3)",
    )
    source_bench = p_bench.add_mutually_exclusive_group(required=True)
    source_bench.add_argument("--db", default=None, help="путь к cohound.db прогона")
    source_bench.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db; требует --run-id)",
    )
    p_bench.add_argument(
        "--run-id",
        default=None,
        help="run_id (обязателен с --runs-dir; с --db выводится, если прогон один)",
    )
    p_bench.add_argument(
        "--golden",
        required=True,
        help="путь к golden.csv (§4.2/§11.6) — эталонные ожидания для всех моделей",
    )
    p_bench.add_argument(
        "--package",
        default=None,
        help="каталог пакета (опц.; каталог параметров обычно восстанавливается из БД прогона)",
    )
    p_bench.add_argument(
        "--run-yaml",
        dest="run_yaml",
        default=None,
        help="run.yaml (опц.; вместе с --package задаёт явный источник каталога)",
    )
    p_bench.add_argument(
        "--model",
        action="append",
        default=None,
        metavar="MODEL_ID",
        help="кандидат (можно повторять); дефолт — "
        + ", ".join(DEFAULT_BENCHMARK_MODELS)
        + ". model_id берётся ТОЛЬКО из флага, не из COHOUND_LLM_MODEL",
    )
    p_bench.add_argument(
        "--llm-slot",
        dest="llm_slot",
        choices=LLM_SLOTS,
        default=None,
        help="слот deploy.llm, откуда взять base_url/api_key/decoder/timeout "
        f"({', '.join(LLM_SLOTS)})",
    )
    p_bench.add_argument(
        "--repeats",
        type=_positive_int,
        default=1,
        metavar="N",
        help="число повторов для пробы детерминизма (N>=2 включает пробу); дефолт 1",
    )
    p_bench.add_argument(
        "--reasoning-effort",
        dest="reasoning_effort",
        choices=[effort.value for effort in ReasoningEffort],
        default=None,
        metavar="LEVEL",
        help="объявленный уровень усилия рассуждения оценщика "
        f"({', '.join(effort.value for effort in ReasoningEffort)}); уходит тем же путём, "
        "что в прогоне (reasoning_wire), невыразимый для модели уровень — EXIT_USAGE. "
        "Дефолт — из слота (llm.<slot>.reasoning_effort)",
    )
    p_bench.add_argument(
        "--max-tokens",
        dest="max_tokens",
        type=_positive_int,
        default=None,
        metavar="N",
        help="объявленный потолок генерации (перекрывает llm.<slot>.max_tokens); "
        "дефолт — потолок слота",
    )
    p_bench.add_argument(
        "--limit",
        type=_positive_int,
        default=None,
        metavar="N",
        help="smoke-подмножество: не более N ячеек (детерминированно по golden)",
    )
    p_bench.add_argument(
        "--concurrency",
        type=_positive_int,
        default=1,
        metavar="K",
        help="сколько вызовов оценщика держать в полёте (ось исполнения "
        "--llm-concurrency фазы Ш6); дефолт 1 — последовательно, байт-в-байт. "
        "Точки одного замера снимаются по одной: провайдер общий",
    )
    _add_common(p_bench)
    p_bench.set_defaults(handler=run_evaluator_benchmark)

    # ``hound experiment`` без вида — usage-ошибка, а не трейсбек отсутствующего handler.
    p_exp.set_defaults(handler=_no_subcommand)
