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

**Нерезолвленный эталон (O4).** Эталон привязан к ``chunk_id`` конкретной сборки. Когда
сборка пересобрана, а голден — нет, ``recall@K`` даёт честный ноль, неотличимый от провала
поиска. Команда различает это по хранилищу прогона: id, которого там нет, не разрешится
никогда, и печатает ``unresolved_expected_ids: N/M не разрешены`` (пример + число в
``--json``). Код возврата остаётся ``0``: **измерение состоялось**, подозрителен вход.
``EXIT_USAGE`` превращал бы «старый эталон» в «сломанный инструмент» и ломал бы CI-смоук на
заведомо устаревшем голдене; пересборка эталона — O14 (нужна человеческая разметка).

**``experiment evaluator-benchmark``** (A3, Ш6) — второй вид: измеряет **модели оценщика**
при фиксированном всём остальном (сборка, профиль/K/``rrf_k``, промпт, срез чанков,
``seed=0``/``temperature=0.0``/объявленный ``max_retries``/``strict``). Ретривал выполняется один раз
до моделей, ``evaluate_cell`` идёт с ``store=None`` (измерение, не прогон). Модель берётся
**только** из ``--model``: env ``COHOUND_LLM_*`` даёт лишь endpoint/ключ/декодер — run
identity намеренно игнорирует env, и benchmark не должен воспроизводить это расхождение.
Артефакт — ``runs/{run_id}/benchmark/evaluator-<timestamp>.json``; код возврата ``0`` даже
при плохом счёте модели, ненулевой — только при отсутствии предпосылки.

**``experiment extraction``** (O38б, D10) — третий вид: A/B **механизмов PDF-извлечения**
(``structured`` ↔ ``text_layer``) на размеченном наборе извлечения
(``cohound-extraction-golden/1``). Набор готовит ``scripts/prepare_extraction_golden.py``
из корпуса прогона, размечает человек (D10) — команда ничего не размечает и не выдумывает.
Механизмы собираются **той же** фабрикой, что и воркеры прогона
(:func:`core.cli.wiring.build_worker_extractor`), сравнение — :func:`compare_extractors`
(``auto_switch`` всегда ``False``: A/B — измерение, A3). Артефакт —
``runs/{run_id}/extraction/extraction-<timestamp>.json``; код возврата ``0`` — измерение
состоялось (даже если оба механизма плохи), ``EXIT_USAGE`` — нет набора / 0 кейсов /
объявленный механизм не исполним (с числами, без тихого фолбэка).
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import sys
import time
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
    build_worker_extractor,
    resolve_experiment_target,
)
from core.config.deploy import LLM_SLOTS, load_deploy_config
from core.domain.enums import (
    DocumentLevel,
    PdfDevice,
    PdfPipeline,
    ReasoningEffort,
    SkipReason,
)
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
from core.pipeline.extraction_eval import (
    compare_extractors,
    load_extraction_golden_document,
)
from core.pipeline.golden_status import GoldenStatus, read_golden_status
from core.pipeline.retrieve import DEFAULT_RRF_K

__all__ = ["configure", "run", "run_evaluator_benchmark", "run_extraction"]

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


def _short_id(cid: str) -> str:
    """Короткая форма ``chunk_id`` для предупреждения: ``product:hash[:8]…:chunk_no``.

    Полный id — 64 hex-символа; в строке предупреждения он вытесняет само число.
    Форма грамматики ``product:<doc_version_id>:<chunk_no>`` сохраняется, чтобы адрес
    оставался узнаваемым, а hash — усечён.
    """
    parts = cid.split(":")
    if len(parts) == 3 and len(parts[1]) > 8:
        return f"{parts[0]}:{parts[1][:8]}…:{parts[2]}"
    if len(cid) <= 32:
        return cid
    return f"{cid[:20]}…{cid[-8:]}"


def _unresolved_warning(report: Any) -> str | None:
    """Строка-предупреждение о неразрешённых id эталона (O4) либо ``None``.

    Это **наблюдение о входе**, а не отказ: измерение состоялось, и код возврата остаётся
    ``EXIT_OK``. Молчать нельзя — иначе устаревший голден выглядит как провал поиска.
    """
    unresolved = int(getattr(report, "unresolved_expected_ids", 0) or 0)
    if unresolved <= 0:
        return None
    total = int(getattr(report, "expected_ids_total", 0) or 0)
    examples = tuple(getattr(report, "unresolved_expected_examples", ()) or ())
    example = f" (пример: {_short_id(examples[0])})" if examples else ""
    return (
        f"unresolved_expected_ids: {unresolved}/{total} не разрешены{example} — "
        "ожидаемых id нет в корпусе прогона: recall@K описывает рассинхрон эталона, "
        "а не качество поиска (O4/O14)"
    )


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
    if payload.get("warning"):
        lines.append(f"warning: {payload['warning']}")
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
        # ``embedder is None`` в условии — то же отсутствие, что и пустой ``model_id``
        # (иначе ``query_model_id`` не был бы получен): отказ и причина прежние, но
        # ``embedder`` сужается до не-``None`` для вызова ``embed``.
        if embedder is None or not query_model_id:
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
        # O4: эталон привязан к ``chunk_id`` сборки. Спрашиваем хранилище прогона об
        # ожидаемых id — без этого «поиск не нашёл» и «такого id нет» неразличимы, и
        # устаревший голден выглядит как recall 0. Вопрос точечный (N ≤ размера голдена),
        # а не выгрузка корпуса.
        expected_ids = sorted({cid for item in golden for cid in item.expected_ids})
        known_ids = set(store.get_chunks(expected_ids)) if expected_ids else set()
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
            known_ids=known_ids,
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

        # Машинный вывод собирается здесь же: ``target`` сужен проверками выше и в
        # пределах ``with`` (после закрытия БД mypy уже не видит его как
        # ``ExperimentTarget``, а ``None`` в payload был бы выдумкой).
        metrics = json.loads(report.to_manifest_json())["metrics"]
        warning = _unresolved_warning(report)
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
            # O4: эталон ↔ сборка. Число и примеры — в машинном выводе, а не только
            # в предупреждении: «17/17 не разрешены» — факт измерения, который
            # потребитель обязан увидеть, не разбирая человеческую строку.
            "unresolved_expected_ids": report.unresolved_expected_ids,
            "expected_ids_total": report.expected_ids_total,
            "unresolved_expected_examples": list(report.unresolved_expected_examples),
            "experiment_id": experiment_id,
            "provenance": "measured" if profile_measured else "default",
        }
        if warning is not None:
            payload["warning"] = warning
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

# ---------------------------------------------------------------------------
# ``experiment extraction``: A/B механизмов PDF-экстракции (O38б, D10, A3)
# ---------------------------------------------------------------------------

#: Каталог артефакта рядом с БД прогона (как у benchmark).
EXTRACTION_ARTIFACT_DIR = "extraction"

#: Объявленные механизмы PDF (``PdfPipeline``): ``structured`` — модели разметки (docling),
#: ``text_layer`` — текстовый слой файла (pdfium). Значения — данные, не решение.
EXTRACTION_MECHANISMS: tuple[str, ...] = (PdfPipeline.STRUCTURED.value, PdfPipeline.TEXT_LAYER.value)


def _resolve_extraction_golden(args: argparse.Namespace, case_id: str | None) -> Path:
    """Путь к набору: ``--golden`` > ``--package``/sources > ``packages/{case_id}/sources``.

    Третья форма — та же раскладка, что у пакета (§4.2): ``sources/`` лежит в каталоге
    кейса, и имя набора там фиксировано. Раскладка резолвится объявленным путём
    (:func:`core.config.paths.resolve_paths`), а не ``Path("packages")`` от CWD: на хосте с
    ``COHOUND_HOME`` пакеты живут вне репозитория.
    """
    if getattr(args, "golden", None):
        return Path(str(args.golden)).expanduser()
    if getattr(args, "package", None):
        return Path(str(args.package)).expanduser() / "sources" / "extraction_golden.json"
    if not case_id:
        raise CliError(
            "набор не назван: передайте --golden либо --package (прогон не несёт case_id)",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    from core.config.paths import resolve_paths  # noqa: PLC0415 — только для дефолта пути

    # Composition root уже разрешил раскладку (``args.paths``): второй резолв читал бы env
    # ещё раз и мог разойтись с тем, что видели остальные команды.
    paths = getattr(args, "paths", None)
    if paths is None:
        try:
            paths = resolve_paths()
        except ValueError as exc:
            raise CliError(
                f"раскладка не прочитана ({_concise(exc)}): передайте --golden",
                code="usage",
                exit_code=EXIT_USAGE,
            ) from exc
    return paths.packages / case_id / "sources" / "extraction_golden.json"


def _require_mechanism_available(
    extractor: Any, mechanism: str, *, level: DocumentLevel, device: PdfDevice
) -> None:
    """Объявленный механизм обязан быть **исполним** — отказ, а не тихая подмена.

    ``build_worker_extractor`` при ``structured`` без docling возвращает композит, у
    которого документного читателя нет: PDF ушёл бы в HTML-читатель, и измерение назвало
    бы механизм, которого не было (тот же класс, что тихий CPU-фолбэк при ``cuda``).
    Композит объявляет это честно: ``document_level is None`` — документного адаптера нет.
    """
    if mechanism != PdfPipeline.STRUCTURED.value:
        return
    if getattr(extractor, "document_level", None) is not None:
        return
    raise CliError(
        f"механизм structured не исполним: docling не установлен (level={level.value}, "
        f"device={device.value}), PDF ушёл бы в HTML-читатель — измерение назвало бы не тот "
        "движок. Объявите --mechanism-a text_layer либо установите extra 'extract-pdf'",
        code="wiring",
        exit_code=EXIT_USAGE,
    )


def _extraction_artifact_path(db_path: str | Path) -> Path:
    """``runs/{run_id}/extraction/extraction-<timestamp>.json`` рядом с БД прогона."""
    stamp = _dt.datetime.now(_dt.UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return Path(str(db_path)).parent / EXTRACTION_ARTIFACT_DIR / f"extraction-{stamp}.json"


def _extraction_table(payload: dict[str, Any]) -> str:
    """Markdown-таблица: кейс | исход A | исход B | символы A/B | страницы A/B | мс A/B."""
    lines = [
        "| case_id | A | B | chars A/B | pages A/B | ms A/B |",
        "|---|---|---|---|---|---|",
    ]
    for row in payload["per_case"]:
        arm_a = row["a"]
        arm_b = row["b"]

        def _fmt_pair(left: Any, right: Any, fmt: str = "{}") -> str:
            text_left = "—" if left is None else fmt.format(left)
            text_right = "—" if right is None else fmt.format(right)
            return f"{text_left}/{text_right}"

        lines.append(
            "| {case} | {a} | {b} | {chars} | {pages} | {ms} |".format(
                case=row["case_id"],
                a=arm_a.get("outcome", "absent"),
                b=arm_b.get("outcome", "absent"),
                chars=_fmt_pair(arm_a.get("chars"), arm_b.get("chars")),
                pages=_fmt_pair(arm_a.get("pages"), arm_b.get("pages")),
                ms=_fmt_pair(arm_a.get("elapsed_ms"), arm_b.get("elapsed_ms"), "{:.0f}"),
            )
        )
    return "\n".join(lines)


def _emit_extraction(args: argparse.Namespace, payload: dict[str, Any]) -> None:
    """Печатает результат: ``--json`` — payload, иначе сводка + таблица."""
    if args.json:
        _print_json(payload)
        return
    summary = payload["summary"]
    lines = [
        f"run_id: {payload['run_id']}",
        f"golden: {payload['golden']} (формат {payload['golden_format'] or 'не объявлен'}, "
        f"status {payload['golden_status']})",
        f"кейсов: {payload['total']}",
        "A: {mechanism} level={level} device={device} version={extractor_version}".format(
            **payload["mechanisms"]["a"]
        ),
        "B: {mechanism} level={level} device={device} version={extractor_version}".format(
            **payload["mechanisms"]["b"]
        ),
        f"прошло под A: {summary['a_passed']}/{payload['total']}, "
        f"под B: {summary['b_passed']}/{payload['total']}",
        f"только A: {summary['a_only']}, только B: {summary['b_only']}, "
        f"оба: {summary['both']}, ни один: {summary['neither']}",
        f"recommendation: {payload['recommendation']['suggested']} "
        f"(auto_switch={payload['recommendation']['auto_switch']})",
        f"artifact: {payload['artifact']}",
        "",
        _extraction_table(payload),
    ]
    sys.stdout.write("\n".join(lines) + "\n")


def run_extraction(args: argparse.Namespace) -> int:
    """``hound experiment extraction``: A/B механизмов PDF-извлечения на наборе (O38б).

    Набор — файл формата ``cohound-extraction-golden/1`` (его готовит
    ``scripts/prepare_extraction_golden.py``, размечает человек — D10). Байты кейсов
    читаются по ``raw_path`` из набора: этот путь — тот же ``corpus_docs.raw_path`` прогона,
    поэтому второго способа найти документ не заводится.

    Механизмы собираются **той же** фабрикой, что и воркеры прогона
    (:func:`core.cli.wiring.build_worker_extractor`), — иначе измерялся бы не тот экстрактор,
    который исполняется в корпусе. Сравнение — :func:`core.pipeline.extraction_eval.compare_extractors`:
    ``auto_switch`` всегда ``False`` (A3), выбор механизма фиксирует человек.

    Код возврата: ``0`` — измерение состоялось, даже если оба механизма плохи; ``EXIT_USAGE``
    — нет набора, в наборе 0 кейсов, объявленный механизм не исполним (с числами, без
    тихого фолбэка) или A и B — один и тот же механизм.
    """
    db_path = _resolve_db_path(args)
    run_id = args.run_id
    case_id: str | None = None
    with build_store(db_path, profile=args.profile) as store:
        if run_id is None:
            run_id = _infer_run_id(store)
        manifest = store.read_manifest(run_id)  # валидация: прогон существует
        # ``case_id`` — имя пакета в раскладке (§4.2); ``package_id`` у манифеста нет, а
        # придумывать его из ничего нельзя: без ``case_id`` команда потребует --golden.
        case_id = getattr(manifest, "case_id", None) or None

    golden_path = _resolve_extraction_golden(args, case_id)
    if not golden_path.is_file():
        raise CliError(
            f"набора нет: {golden_path} — соберите его "
            "`scripts/prepare_extraction_golden.py candidates|compile` и разметьте (D10)",
            code="golden",
            exit_code=EXIT_USAGE,
        )
    try:
        document = load_extraction_golden_document(golden_path)
    except ValueError as exc:
        raise CliError(_concise(exc), code="golden", exit_code=EXIT_USAGE) from exc
    cases = list(document.cases)
    if args.limit is not None:
        cases = cases[: args.limit]
    if not cases:
        raise CliError(
            f"в наборе 0 кейсов ({golden_path}) — измерять нечего: пустой набор не "
            "превращается в измерение",
            code="golden",
            exit_code=EXIT_USAGE,
        )

    mechanism_a = PdfPipeline(str(args.mechanism_a))
    mechanism_b = PdfPipeline(str(args.mechanism_b))
    if mechanism_a is mechanism_b:
        raise CliError(
            f"A и B — один механизм ({mechanism_a.value}): сравнение механизма с самим "
            "собой ничего не измеряет",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    level = DocumentLevel(str(args.level))
    device = PdfDevice(str(args.device))

    extractor_a = build_worker_extractor(
        level=level, pdf_pipeline=mechanism_a, pdf_device=device
    )
    _require_mechanism_available(extractor_a, mechanism_a.value, level=level, device=device)
    extractor_b = build_worker_extractor(
        level=level, pdf_pipeline=mechanism_b, pdf_device=device
    )
    _require_mechanism_available(extractor_b, mechanism_b.value, level=level, device=device)

    comparison = compare_extractors(cases, extractor_a, extractor_b, clock=time.perf_counter)
    totals = comparison["totals"]
    payload: dict[str, Any] = {
        "ok": True,
        "run_id": run_id,
        "case_id": case_id,
        "golden": str(golden_path),
        "golden_format": document.format,
        "golden_status": document.status,
        "golden_reviewed_by": document.reviewed_by,
        "total": len(cases),
        "cases": [
            {
                "case_id": case.case_id,
                "format": case.format,
                "doc_id": case.doc_id,
                "raw_path": case.raw_path,
                "expect_error": case.expect_error,
            }
            for case in cases
        ],
        "mechanisms": {
            "a": {
                "mechanism": mechanism_a.value,
                "level": level.value,
                "device": device.value,
                "extractor_version": comparison["a_version"],
            },
            "b": {
                "mechanism": mechanism_b.value,
                "level": level.value,
                "device": device.value,
                "extractor_version": comparison["b_version"],
            },
        },
        "extractor_version": {
            "a": comparison["a_version"],
            "b": comparison["b_version"],
        },
        "per_case": comparison["cases"],
        "summary": {
            "a_passed": comparison["a_passed"],
            "b_passed": comparison["b_passed"],
            "a_pass_rate": comparison["a_pass_rate"],
            "b_pass_rate": comparison["b_pass_rate"],
            **totals,
            "classification": comparison["summary"],
        },
        "recommendation": comparison["recommendation"],
    }
    artifact = (
        Path(str(args.out_json)).expanduser()
        if getattr(args, "out_json", None)
        else _extraction_artifact_path(db_path)
    )
    artifact.parent.mkdir(parents=True, exist_ok=True)
    payload["artifact"] = str(artifact)
    artifact.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    _emit_extraction(args, payload)
    return EXIT_OK


def _no_subcommand(args: argparse.Namespace) -> int:  # noqa: ARG001 — сигнатура handler
    """``hound experiment`` без вида: печатает usage и возвращает :data:`EXIT_USAGE`."""
    sys.stderr.write(
        "usage: hound experiment {retrieval|evaluator-benchmark|extraction} ...\n"
    )
    return EXIT_USAGE


def configure(sub: Any) -> None:
    """Регистрирует подпакет ``experiment`` (виды ``retrieval``/``evaluator-benchmark``/``extraction``)."""
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

    p_ext = exp_sub.add_parser(
        "extraction",
        help="A/B механизмов PDF-извлечения на наборе извлечения (O38б, D10)",
    )
    source_ext = p_ext.add_mutually_exclusive_group(required=True)
    source_ext.add_argument("--db", default=None, help="путь к cohound.db прогона")
    source_ext.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db; требует --run-id)",
    )
    p_ext.add_argument(
        "--run-id",
        default=None,
        help="run_id (обязателен с --runs-dir; с --db выводится, если прогон один)",
    )
    p_ext.add_argument(
        "--golden",
        default=None,
        help="набор извлечения (JSON, формат cohound-extraction-golden/1); по умолчанию — "
        "{package}/sources/extraction_golden.json",
    )
    p_ext.add_argument(
        "--package",
        default=None,
        help="каталог пакета (опц.; из него берётся набор, если --golden не задан)",
    )
    p_ext.add_argument(
        "--mechanism-a",
        dest="mechanism_a",
        choices=list(EXTRACTION_MECHANISMS),
        default=PdfPipeline.STRUCTURED.value,
        help=f"механизм A ({', '.join(EXTRACTION_MECHANISMS)}); дефолт "
        f"{PdfPipeline.STRUCTURED.value}",
    )
    p_ext.add_argument(
        "--mechanism-b",
        dest="mechanism_b",
        choices=list(EXTRACTION_MECHANISMS),
        default=PdfPipeline.TEXT_LAYER.value,
        help=f"механизм B ({', '.join(EXTRACTION_MECHANISMS)}); дефолт "
        f"{PdfPipeline.TEXT_LAYER.value}",
    )
    p_ext.add_argument(
        "--level",
        choices=[level.value for level in DocumentLevel],
        default=DocumentLevel.FULL.value,
        help="уровень документной экстракции для structured "
        f"({', '.join(level.value for level in DocumentLevel)}); дефолт "
        f"{DocumentLevel.FULL.value}",
    )
    p_ext.add_argument(
        "--device",
        choices=[device.value for device in PdfDevice],
        default=PdfDevice.CPU.value,
        help="устройство моделей для structured (cpu|cuda; cuda без устройства — отказ, "
        "не тихий CPU)",
    )
    p_ext.add_argument(
        "--out-json",
        dest="out_json",
        default=None,
        help="куда писать артефакт замера (по умолчанию "
        "runs/{run_id}/extraction/extraction-<timestamp>.json)",
    )
    p_ext.add_argument(
        "--limit",
        type=_positive_int,
        default=None,
        metavar="N",
        help="smoke-подмножество: не более N кейсов (детерминированно по набору)",
    )
    _add_common(p_ext)
    p_ext.set_defaults(handler=run_extraction)

    # ``hound experiment`` без вида — usage-ошибка, а не трейсбек отсутствующего handler.
    p_exp.set_defaults(handler=_no_subcommand)
