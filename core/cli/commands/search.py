"""``search``: поиск по корпусу прогона и по reference-корпусу — разведка, не evidence (A3/I17/I18).

Зачем команда существует. Разметка терминов каталога и разбор ложных ``present`` в
``hound baseline recon`` требуют ответа на вопрос «что вообще есть в корпусе по этому
понятию». До этого такой ответ добывался одноразовыми скриптами поверх
``corpus_chunks`` — то есть **мимо** продукта: другой токенизатор, другое слияние
запросов, никакого провенанса. Здесь тот же путь, что у оценщика: ``retrieve()`` по
зафиксированной сборке, продукт-скоупед (I17), с объявленным профилем.

Граница с оценщиком — в одном месте и намеренно:

============================  ==============================  ============================
                              оценщик (Ш6)                    ``search``
============================  ==============================  ============================
вход                          параметр каталога               свободный текст либо ``--param``
что делает                    запросы → чанки → LLM           запросы → чанки (→ LLM, ``--answer``)
выход                         **вердикт** ✅/⚠️/❌ + цитата     **материал**: чанки с адресами
роль                          evidence (I1/I22)               разведка (не пишется никуда)
============================  ==============================  ============================

``--param`` показывает ровно то, что увидит оценщик: термины берутся
:func:`core.pipeline.prompt.render_queries` — та же функция, что строит запросы Ш6.
Поэтому «почему параметр подтвердился благодарностью в release notes» видно за минуту,
а не за прогон.

**Две стороны выдачи (``--scope``, решение владельца 18.11.2026).** Команда отвечает на
тот же вопрос по двум разным корпусам:

* ``cohort`` (дефолт, прежнее поведение) — корпус прогона, лексическое плечо tantivy,
  профиль объявляет оператор (``--retrieval-profile``);
* ``reference`` — документация предка (план 27), ``Bm25Index``, **только R0**;
* ``both`` — обе стороны в одной выдаче, каждая своим блоком.

Три правила, без которых стороны смешались бы в одно неверное число:

1. **Счета не сливаются.** Tantivy и ``Bm25Index`` — разные шкалы; упорядочивание по ним
   описывало бы разницу нормировок, а не релевантность. Поэтому попадание несёт
   ``side``, а слияние и сортировка идут только **внутри** стороны.
2. **Разные постановки задачи у ``--param``.** Для когорты это ``render_queries`` (запросы
   Ш6 — то, что увидит оценщик), для предка — :func:`core.pipeline.reference_corpus.
   parameter_terms` (термины recon: ``title`` + ``concept_terms``, без служебного
   ``rule``). Шапка печатает, какой построитель использован.
3. **Reference — только лексика.** В reference-БД нет ``index_manifest`` и
   ``embed_model_id`` = NULL: векторов не существует, поэтому dense-плечо там не
   собирается. Это замер, а не ограничение: объявленная модель и пере-эмбеддинг —
   отдельная работа, а не флаг.

Ответ LLM (``--answer``) идёт через порт **разведки**
(:class:`~core.ports.discovery_llm.DiscoveryLLMPort`), а не через оценщика: у него
другой контракт (свободный ответ, а не ``LLMCellOutput``), и он не может попасть в
``cell_evidence``. Модель разведки — операционная настройка (env/слот), а не условие
измерения: выход не является доказательством (§7.4). Ответ строится только по стороне
когорты: у reference-корпуса нет своего оценщика, и ответ по чужому корпусу смешал бы
две постановки.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any

from core.adapters.lexical_bm25 import Bm25Index, LexicalDoc
from core.cli.context import (
    _add_common,
    _add_llm_model_flag,
    _add_llm_slot_flag,
    _concise,
    _positive_int,
    _print_json,
    _resolve_paths,
)
from core.cli.errors import EXIT_NOT_FOUND, EXIT_OK, EXIT_USAGE, CliError
from core.cli.wiring import (
    RetrievalPorts,
    _llm_configured,
    build_discovery_llm,
    build_retrieval_ports,
)
from core.config import load_catalog
from core.config.deploy import load_deploy_config
from core.domain.models import CatalogRow, ReferenceProduct, ReferenceProducts
from core.pipeline.corpus_search import (
    SIDE_COHORT,
    SIDE_REFERENCE,
    Hit,
    SearchAnswer,
    build_answer_prompt,
    merge_hits,
    render_hits,
)
from core.pipeline.prompt import render_queries
from core.pipeline.reference_corpus import (
    ReferenceBuildAmbiguous,
    ReferenceBuildMissing,
    ReferenceCorpusLayout,
    iter_reference_chunks,
    parameter_terms,
    read_manifest,
    reference_layout,
    resolve_build,
)
from core.pipeline.reference_products import (
    REFERENCE_PRODUCTS_RELPATH,
    load_package_reference_products,
)
from core.pipeline.retrieve import RetrievalProfile, retrieve
from core.ports.discovery_llm import DiscoveryLLMRequest
from core.ports.vector_index import RetrievedChunk
from core.store.corpus_reader import CorpusReader, CorpusReaderError
from core.util.schema_gate import SchemaVersionMismatch

#: Дефолт среза выдачи, когда в прогоне не записан ``retrieval_cutoff_K``.
DEFAULT_TOP_K = 10

#: Стороны выдачи (``--scope``). Строки, а не ``StrEnum``: это ось **выдачи команды**, а
#: не доменный статус, и в словаре §4.1 ей нечего зеркалировать.
SCOPE_COHORT = "cohort"
SCOPE_REFERENCE = "reference"
SCOPE_BOTH = "both"
SCOPES: tuple[str, ...] = (SCOPE_COHORT, SCOPE_REFERENCE, SCOPE_BOTH)

#: Профиль reference-стороны — всегда ``R0``: векторов в reference-БД нет (§8.4 плана 27).
REFERENCE_PROFILE = RetrievalProfile.R0

#: Имя построителя запросов в payload: чем именно поставлена задача стороне.
BUILDER_RENDER_QUERIES = "render_queries"
BUILDER_PARAMETER_TERMS = "parameter_terms"
BUILDER_FREE_TEXT = "free_text"


def run(args: argparse.Namespace) -> int:
    """``search``: материал из корпуса по запросу (разведка, не evidence).

    Читает **только корпус и строку прогона** (:class:`~core.store.corpus_reader.
    CorpusReader`) либо reference-корпус (``--scope reference``): ячейки, вердикты и отчёт
    не открываются, поэтому команда ничего не может «подтвердить» по ошибке. Ничего не
    пишет: ни в БД, ни в каталог прогона.

    :returns: :data:`EXIT_OK` — выдача построена (пустая выдача — тоже валидный
        результат); :data:`EXIT_USAGE` — необъявленный профиль/продукт, чужая схема БД
        без ``--allow-legacy-schema``, неготовый LLM, отсутствующая/неоднозначная
        reference-сборка; :data:`EXIT_NOT_FOUND` — прогона или БД нет.
    """
    payload = _collect(args)
    if args.json:
        _print_json(payload)
    else:
        _print_text(payload)
    return EXIT_OK


def _collect(args: argparse.Namespace) -> dict[str, Any]:
    """Собирает payload: сторона(ы) → сборка → плечи → запросы → слияние → (ответ)."""
    scope = args.scope
    _check_scope_flags(args, scope)
    paths = _resolve_paths(args)
    if scope == SCOPE_REFERENCE:
        # Прогон не открывается вовсе: reference-корпус живёт вне ``runs/`` и переживает
        # прогоны, поэтому и пакет здесь задаётся явно (``--package``), а не выводится из
        # строки прогона.
        return _collect_reference(args, paths)
    db_path, run_id = _resolve_db(args, paths)
    try:
        corpus = CorpusReader(
            db_path, run_id=run_id, allow_foreign_schema=args.allow_legacy_schema
        )
    except SchemaVersionMismatch as exc:
        # Сообщение гейта уже называет обе версии и запрет молчаливого чтения; здесь —
        # только то, чего гейт не знает: у поиска есть явный выход, и он не читает
        # вердиктов.
        raise CliError(
            f"{_concise(exc)} Для поиска по старому прогону: --allow-legacy-schema "
            "(читаются только чанки корпуса и строка прогона)",
            code="schema",
            exit_code=EXIT_USAGE,
        ) from exc
    except CorpusReaderError as exc:
        raise CliError(_concise(exc), code="not_found", exit_code=EXIT_NOT_FOUND) from exc
    try:
        return _collect_from(corpus, args, paths, scope)
    finally:
        corpus.close()


def _check_scope_flags(args: argparse.Namespace, scope: str) -> None:
    """Отказы по сочетанию ``--scope`` с флагами — до открытия любой БД.

    Проверка стоит здесь, а не в середине сборки, чтобы отказ не зависел от того, есть ли
    прогон: «R2 + reference» неверно само по себе, и выяснять это после чтения БД значило
    бы делать вид, что сочетание могло бы сработать.
    """
    profile = RetrievalProfile(args.retrieval_profile)
    if scope == SCOPE_REFERENCE and profile is not RetrievalProfile.R0:
        raise CliError(
            f"reference-корпус ищется только лексикой (R0), а запрошен {profile.value}: "
            "векторов у reference-корпуса нет (в его БД нет index_manifest, "
            "embed_model_id = NULL), поэтому dense-плечо не собирается. Либо "
            "--scope cohort, либо объявите модель и пере-эмбеддьте reference-корпус — "
            "это замер, а не флаг",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    if scope == SCOPE_REFERENCE and args.answer:
        raise CliError(
            "ответ разведки строится только по корпусу когорты: у reference-корпуса нет "
            "своего оценщика, а ответ по чужому корпусу смешал бы две постановки. "
            "Задайте --scope cohort, либо --scope both (ответ будет по стороне когорты)",
            code="usage",
            exit_code=EXIT_USAGE,
        )


def _collect_from(
    corpus: CorpusReader, args: argparse.Namespace, paths: Any, scope: str
) -> dict[str, Any]:
    ref = corpus.run_ref()
    if not ref.index_build_id:
        raise CliError(
            f"в прогоне {ref.run_id!r} не записана индексная сборка (runs.index_build_id "
            "пуст) — искать не в чем: сборка появляется после фазы corpus",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    partitions = corpus.partitions(ref.index_build_id)
    known = [partition.product_id for partition in partitions]
    products = list(args.product or known)
    unknown = [product for product in products if product not in known]
    if unknown:
        raise CliError(
            f"продуктов нет в сборке {ref.index_build_id}: {', '.join(unknown)}; "
            f"есть: {', '.join(known) or '—'}",
            code="usage",
            exit_code=EXIT_USAGE,
        )

    profile = RetrievalProfile(args.retrieval_profile)
    top_k = args.top_k if args.top_k is not None else (ref.retrieval_cutoff_k or DEFAULT_TOP_K)
    package_dir = _package_dir(args, ref.package_id, paths)
    row = _query_row(args, package_dir)
    queries = _cohort_queries(args, row)
    deploy = load_deploy_config(None)
    ports = build_retrieval_ports(
        index_root=paths.index_store,
        package=ref.package_id,
        store=corpus,
        build_id=ref.index_build_id,
        deploy=deploy,
        dense=profile in (RetrievalProfile.R1, RetrievalProfile.R2),
    )
    _check_arms(profile, ports, ref.embed_model_id)

    per_query = [
        retrieve(
            query_text=query,
            query_vector=_query_vector(query, profile, ports),
            product_ids=products,
            profile=profile,
            cutoff_k=top_k,
            lexical_index=ports.lexical_index,
            vector_index=ports.vector_index,
        ).chunks
        for query in queries
    ]
    hits = merge_hits(per_query)[:top_k]
    meta = corpus.chunk_meta([hit.chunk_id for hit in hits])
    urls = {chunk_id: values.get("url", "") for chunk_id, values in meta.items()}

    payload: dict[str, Any] = {
        "ok": True,
        # Разведка, а не evidence: поле стоит в корне и в JSON, чтобы выдача не могла
        # быть принята за вердикт ни человеком, ни скриптом (A3/I1).
        "reconnaissance": True,
        "scope": scope,
        "run_id": ref.run_id,
        "package_id": ref.package_id,
        "build_id": ref.index_build_id,
        "profile": profile.value,
        "declared_profile": ref.retrieval_profile,
        "top_k": top_k,
        "products": products,
        "schema": {
            "file": ref.schema_version,
            "expected": corpus.expected_schema_version,
            "mismatch": corpus.mismatch,
        },
        "queries": queries,
        "reference_queries": [],
        "param": _param_payload(row),
        "builders": {
            "cohort": _cohort_builder(args),
            "reference": None,
        },
        "explain": bool(args.explain),
        "hits": [_hit_payload(hit, urls) for hit in hits],
        "notices": list(ports.notices),
        "reference": None,
        "answer": None,
    }
    if scope == SCOPE_BOTH:
        side = _reference_side(
            args, paths, package_dir, row=row, top_k=top_k, cohort_side=True
        )
        payload["reference"] = side["meta"]
        payload["reference_queries"] = side["queries"]
        payload["builders"]["reference"] = side["builder"]
        # Два блока подряд: счета разных движков несравнимы, поэтому список не
        # пересортировывается между сторонами — только внутри каждой.
        payload["hits"] = payload["hits"] + side["hits"]
    if args.answer:
        # Ответ — только по стороне когорты: у reference-корпуса своего оценщика нет.
        payload["answer"] = _answer(args, queries[0], hits, deploy)
    return payload


def _collect_reference(args: argparse.Namespace, paths: Any) -> dict[str, Any]:
    """Payload стороны reference-корпуса: прогон не читается вовсе."""
    package_dir = _package_dir(args, None, paths)
    row = _query_row(args, package_dir)
    top_k = args.top_k if args.top_k is not None else DEFAULT_TOP_K
    side = _reference_side(
        args, paths, package_dir, row=row, top_k=top_k, cohort_side=False
    )
    return {
        "ok": True,
        "reconnaissance": True,
        "scope": SCOPE_REFERENCE,
        "run_id": None,
        "package_id": package_dir.name,
        "build_id": None,
        "profile": REFERENCE_PROFILE.value,
        "declared_profile": None,
        "top_k": top_k,
        "products": [],
        "schema": None,
        "queries": [],
        "reference_queries": side["queries"],
        "param": _param_payload(row),
        "builders": {"cohort": None, "reference": side["builder"]},
        "explain": bool(args.explain),
        "hits": side["hits"],
        "notices": [],
        "reference": side["meta"],
        "answer": None,
    }


def _reference_side(
    args: argparse.Namespace,
    paths: Any,
    package_dir: Path,
    *,
    row: CatalogRow | None,
    top_k: int,
    cohort_side: bool,
) -> dict[str, Any]:
    """Сторона reference-корпуса: линия → версия → сборка → BM25 → попадания (R0).

    Движок индекса выбирает composition root (``pipeline`` не импортирует ``adapters``,
    матрица 01 §1), и его версия публикуется: «чем искали» — часть наблюдения. Тот же
    ``Bm25Index``, что у ``hound baseline recon``, — иначе разведка и recon показывали бы
    разные фрагменты одного корпуса.
    """
    line = _reference_line(args, package_dir, cohort_side=cohort_side)
    version = _reference_version(args, line, package_dir)
    layout = reference_layout(
        Path(paths.reference),
        package_id=package_dir.name,
        product_id=line.product_id,
        version=version,
    )
    build_id = _reference_build_id(args, layout, package_dir, line, version)
    db_path = layout.db_path(build_id)
    if not db_path.is_file():
        raise CliError(
            f"сборка {build_id} объявлена, но файла корпуса нет: {db_path}. "
            f"Соберите: {_build_hint(package_dir, line, version)}",
            code="reference",
            exit_code=EXIT_USAGE,
        )
    manifest = read_manifest(layout.manifest_path(build_id))
    chunks = list(iter_reference_chunks(db_path))
    if not chunks:
        raise CliError(
            f"reference-корпус пуст: {db_path} (0 чанков). Пересоберите: "
            f"{_build_hint(package_dir, line, version)}",
            code="reference",
            exit_code=EXIT_USAGE,
        )
    queries = _reference_queries(args, row)

    index = Bm25Index()
    index.index(
        [
            LexicalDoc(chunk_id=chunk.chunk_id, product_id=line.product_id, text=chunk.text)
            for chunk in chunks
        ]
    )
    by_id = {chunk.chunk_id: chunk for chunk in chunks}
    per_query: list[list[RetrievedChunk]] = []
    for query in queries:
        per_query.append(
            [
                RetrievedChunk(
                    chunk_id=hit.chunk_id,
                    product_id=hit.product_id,
                    text=by_id[hit.chunk_id].text,
                    # Провенанс reference-сборки: хеш текста и ``build_id`` настоящие, а не
                    # подставленные — иначе выдача описывала бы не тот корпус.
                    content_hash=by_id[hit.chunk_id].content_hash or "",
                    heading_path=by_id[hit.chunk_id].heading_path or "",
                    build_id=build_id,
                    score=hit.score,
                )
                for hit in index.search(query, top_k=top_k)
                if hit.chunk_id in by_id
            ]
        )
    hits = merge_hits(per_query, side=SIDE_REFERENCE)[:top_k]
    urls = {chunk.chunk_id: chunk.url or "" for chunk in chunks}
    complete = bool(manifest.complete) if manifest is not None else False
    notice = None if complete else (
        "reference: сборка неполная (complete=false) — «не нашли» относится к проходу, "
        "а не к документации"
    )
    return {
        "meta": {
            "product_id": line.product_id,
            "version": version,
            "build_id": build_id,
            "complete": complete,
            "chunks": len(chunks),
            "profile": REFERENCE_PROFILE.value,
            "engine": index.engine_version,
            "notice": notice,
        },
        "queries": queries,
        "hits": [_hit_payload(hit, urls) for hit in hits],
        "builder": _reference_builder(args),
    }


def _reference_line(
    args: argparse.Namespace, package_dir: Path, *, cohort_side: bool
) -> ReferenceProduct:
    """Линия предка: одна объявленная, либо названная ``--product``.

    Несколько линий без названной — отказ с перечнем, а не «первая в файле»: выбор по
    порядку объявления сделал бы ответ функцией правки YAML.
    """
    declaration_path = package_dir / REFERENCE_PRODUCTS_RELPATH
    declaration: ReferenceProducts | None = load_package_reference_products(package_dir)
    if declaration is None or not declaration.products:
        raise CliError(
            f"в {declaration_path} не объявлено ни одной reference-линии — искать у предка "
            "нечего. Объявите линию в sources/reference_products.yaml; что уже объявлено — "
            "hound baseline list",
            code="reference",
            exit_code=EXIT_USAGE,
        )
    declared = [product.product_id for product in declaration.products]
    if len(declared) == 1:
        line = declaration.products[0]
        if not cohort_side:
            # ``--product`` относится к продуктам когорты; на стороне предка он может
            # только совпасть с продуктом линии. Молча игнорировать чужой продукт значило
            # бы ответить не на тот вопрос, который задали.
            foreign = [
                product for product in (args.product or []) if product != line.product_id
            ]
            if foreign:
                raise CliError(
                    f"--product {', '.join(foreign)} не называет reference-линию: у "
                    f"reference-корпуса продукт {line.product_id!r} "
                    f"(объявлены: {', '.join(declared)}). --product фильтрует продукты "
                    "когорты, а с --scope reference он обязан совпадать с продуктом линии",
                    code="usage",
                    exit_code=EXIT_USAGE,
                )
        return line
    chosen = [product for product in (args.product or []) if product in declared]
    if len(chosen) == 1:
        return next(
            product for product in declaration.products if product.product_id == chosen[0]
        )
    raise CliError(
        f"в {declaration_path} объявлено несколько reference-линий: "
        f"{', '.join(declared)} — назовите линию через --product "
        f"({'|'.join(declared)})",
        code="reference",
        exit_code=EXIT_USAGE,
    )


def _reference_version(
    args: argparse.Namespace, line: ReferenceProduct, package_dir: Path
) -> str:
    """Версия линии: ``--reference-version`` > ``current_version`` объявленной линии.

    Необъявленная версия — отказ с перечнем: путь ``reference/{pkg}/{product}/{version}``
    можно собрать для любой строки, и «сборок нет» на опечатке читалось бы как «ещё не
    собрали», а не как «такой версии профиль не объявлял».
    """
    declared = [version.version for version in line.versions]
    version = args.reference_version or line.current_version
    if version not in declared:
        raise CliError(
            f"версия {version!r} не объявлена у линии {line.product_id!r} в "
            f"{package_dir / REFERENCE_PRODUCTS_RELPATH} (объявлены: {', '.join(declared)}; "
            f"current_version={line.current_version})",
            code="reference",
            exit_code=EXIT_USAGE,
        )
    return version


def _reference_build_id(
    args: argparse.Namespace,
    layout: ReferenceCorpusLayout,
    package_dir: Path,
    line: ReferenceProduct,
    version: str,
) -> str:
    """Сборка: ``--reference-build`` > ``current.json`` > единственная > отказ с перечнем.

    Выбор по ``mtime`` запрещён раскладкой ``reference-corpus/2``: время файла — не
    объявленный вход и не наблюдение о содержимом, поэтому «какая сборка свежее» не
    является ответом на вопрос «какая измерена».
    """
    try:
        return resolve_build(layout, explicit=args.reference_build)
    except ReferenceBuildMissing as exc:
        raise CliError(
            f"reference-корпус не найден: {exc}. Соберите его: "
            f"{_build_hint(package_dir, line, version)}; либо назовите сборку "
            "--reference-build",
            code="reference",
            exit_code=EXIT_USAGE,
        ) from exc
    except ReferenceBuildAmbiguous as exc:
        raise CliError(
            f"сборка не выбрана — {exc}. Есть: {', '.join(exc.builds)}; укажите "
            "--reference-build (выбор по mtime запрещён: раскладка требует current.json "
            "или явного выбора)",
            code="reference",
            exit_code=EXIT_USAGE,
        ) from exc


def _build_hint(package_dir: Path, line: ReferenceProduct, version: str) -> str:
    """Команда, которая создаёт корпус: её печатает любая ошибка «нечего читать»."""
    return (
        f"hound baseline build --package {package_dir} "
        f"--baseline {line.baseline_id} --version {version}"
    )


def _query_vector(query: str, profile: RetrievalProfile, ports: RetrievalPorts) -> Any:
    """Вектор запроса — только для dense-профилей; иначе ``None`` (R0 его не читает)."""
    if profile not in (RetrievalProfile.R1, RetrievalProfile.R2):
        return None
    embedder = ports.embedder
    if embedder is None:  # уже проверено в ``_check_arms``; здесь — сужение типа
        raise CliError(
            "dense-плечо не собрано: объявите embed.model_id и revision в deploy.yaml",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    return embedder.embed([query])[0]


def _check_arms(
    profile: RetrievalProfile, ports: RetrievalPorts, corpus_model: str | None
) -> None:
    """Отказ вместо тихой подмены: чего не хватает — назвать, а не понизить профиль.

    «R2 не собрался, взяли R0» — худший исход: выдача выглядела бы как гибридная, а
    считалась бы лексикой. Поэтому отказ, и в сообщении — что именно отсутствует и как
    это получить.
    """
    if profile in (RetrievalProfile.R0, RetrievalProfile.R2) and ports.lexical_index is None:
        raise CliError(
            "лексическое плечо не собрано: нужен tantivy (extra `crawl`) и разрешённая "
            f"сборка в {profile.value}",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    if profile in (RetrievalProfile.R1, RetrievalProfile.R2):
        if ports.vector_index is None:
            raise CliError(
                "dense-плечо не собрано: нет разрешённой сборки индекса (index_store) "
                "или numpy",
                code="usage",
                exit_code=EXIT_USAGE,
            )
        if ports.embedder is None:
            raise CliError(
                "dense-плечо не собрано: модель весов не объявлена "
                "(embed.model_id + embed.revision в deploy.yaml) — вектор запроса "
                "считать нечем",
                code="usage",
                exit_code=EXIT_USAGE,
            )
        embedder_model = getattr(ports.embedder, "model_id", "")
        # Сравнивается **имя модели**, не полная строка: ``embedder.model_id`` несёт пин
        # ревизии (``model_id@sha``, ось ``embed_spec``), а ``embed_model_id`` в манифесте —
        # наблюдаемая строка без ревизии (провенанс). Ревизию по манифесту не сверить: её
        # там нет, и делать вид, что сверили, — выдуманная проверка.
        if (
            corpus_model
            and embedder_model
            and _base_model(embedder_model) != _base_model(corpus_model)
        ):
            raise CliError(
                f"векторы корпуса посчитаны моделью {corpus_model!r}, а объявлена "
                f"{embedder_model!r}: близость между разными пространствами не имеет "
                "смысла — приведите deploy.yaml к модели прогона",
                code="usage",
                exit_code=EXIT_USAGE,
            )


def _base_model(model_id: str) -> str:
    """Имя модели без пина ревизии (``BAAI/bge-m3@sha`` → ``BAAI/bge-m3``)."""
    return model_id.split("@", 1)[0].strip()


def _query_row(args: argparse.Namespace, package_dir: Path) -> CatalogRow | None:
    """Строка каталога по ``--param``; ``None`` — свободный запрос.

    Один вход на две стороны: и когорта, и reference читают **тот же** параметр каталога,
    а различаются построителем запросов (``render_queries`` / ``parameter_terms``).
    """
    if args.param and args.query:
        raise CliError(
            "задайте либо свободный запрос, либо --param (это разные входы)",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    if not args.param:
        if not args.query:
            raise CliError(
                "нужен запрос: свободный текст позиционным аргументом или --param PARAM_ID",
                code="usage",
                exit_code=EXIT_USAGE,
            )
        return None
    return _catalog_row(args, package_dir)


def _cohort_queries(args: argparse.Namespace, row: CatalogRow | None) -> list[str]:
    """Запросы стороны когорты: ``render_queries`` — те же, что уйдёт в Ш6.

    Язык берётся из того же резолвера, что у прогона (``--query-lang`` > ``deploy.retrieval.query_lang``
    > ``all``): разведка обязана спрашивать те же запросы, что Ш6, иначе она отвечает на
    другой вопрос, чем измерение. Флаг здесь — **разведка, а не ось**: он не входит в
    ``run_id`` и нужен ровно для того, чтобы увидеть разницу языков на одном корпусе.
    """
    if row is None:
        return [args.query]
    from core.cli.execution_axes import resolve_query_lang

    queries = render_queries(
        row,
        query_lang=resolve_query_lang(
            getattr(args, "query_lang", None), load_deploy_config(None)
        ),
    )
    if not queries:
        raise CliError(
            f"у параметра {row.param_id!r} пусты title и concept_terms — искать не по чему",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    return queries


def _reference_queries(args: argparse.Namespace, row: CatalogRow | None) -> list[str]:
    """Запросы стороны reference: ``parameter_terms`` — та же постановка, что у recon.

    Термины склеиваются в **один** запрос, как в ``hound baseline recon``
    (``index.search_scores(" ".join(terms))``): это опубликованный прибор разведки предка,
    и второй формулы «что спросить у документации» быть не должно.
    """
    if row is None:
        return [args.query]
    terms = parameter_terms(row)
    if not terms:
        raise CliError(
            f"у параметра {row.param_id!r} пусты title и concept_terms — искать не по чему",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    return [" ".join(terms)]


def _catalog_row(args: argparse.Namespace, package_dir: Path) -> CatalogRow:
    """Строка каталога по ``--param``: каталог пакета из раскладки или ``--package``."""
    catalog_path = package_dir / "catalog.csv"
    if not catalog_path.is_file():
        raise CliError(
            f"каталог не найден: {catalog_path} — укажите --package с пакетом прогона",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    rows = load_catalog(catalog_path)
    row = next((item for item in rows if item.param_id == args.param), None)
    if row is None:
        raise CliError(
            f"параметр {args.param!r} не найден в {catalog_path} ({len(rows)} параметров)",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    return row


def _package_dir(args: argparse.Namespace, package_id: str | None, paths: Any) -> Path:
    """Каталог пакета: ``--package`` > ``paths.packages/{package_id}``.

    ``--scope reference`` прогон не открывает, поэтому ``package_id`` взять неоткуда —
    там каталог пакета задаётся явно, а не угадывается по раскладке.
    """
    if args.package:
        return Path(args.package)
    if not package_id:
        raise CliError(
            "не из чего взять пакет: --package не задан, а package_id прогона пуст. "
            "С --scope reference прогон не открывается, поэтому каталог пакета задаётся "
            "явно: --package PATH",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    return Path(paths.packages) / package_id


def _param_payload(row: CatalogRow | None) -> dict[str, Any] | None:
    """Параметр в payload: чем он был назван (``None`` — свободный запрос)."""
    if row is None:
        return None
    return {
        "param_id": row.param_id,
        "title": row.title,
        "rule": row.rule,
        "absence_rule": row.absence_rule.value,
        "concept_terms": row.concept_terms,
    }


def _cohort_builder(args: argparse.Namespace) -> str:
    """Имя построителя запросов стороны когорты (печатается и публикуется)."""
    return BUILDER_RENDER_QUERIES if args.param else BUILDER_FREE_TEXT


def _reference_builder(args: argparse.Namespace) -> str:
    """Имя построителя запросов стороны reference (``parameter_terms`` — у recon)."""
    return BUILDER_PARAMETER_TERMS if args.param else BUILDER_FREE_TEXT


def _answer(
    args: argparse.Namespace, question: str, hits: list[Hit], deploy: Any
) -> dict[str, Any]:
    """Ответ LLM-разведки по найденным фрагментам. **Не evidence** (I18/§7.4)."""
    if not hits:
        raise CliError(
            "фрагментов нет — отвечать не по чему (--answer требует непустую выдачу)",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    llm = build_discovery_llm(
        model_id=args.llm_model,
        slot=getattr(args, "llm_slot", None),
        deploy_llm=deploy.llm,
    )
    if not _llm_configured(llm):
        raise CliError(
            "LLM-разведка не сконфигурирована: задайте --llm-model либо "
            "COHOUND_LLM_BASE_URL/COHOUND_LLM_MODEL (или слот deploy.llm)",
            code="llm",
            exit_code=EXIT_USAGE,
        )
    response = llm.complete(
        DiscoveryLLMRequest(
            prompt=build_answer_prompt(question, hits),
            response_schema=SearchAnswer,
            model_id=args.llm_model or getattr(llm, "model_id", ""),
        )
    )
    answer = SearchAnswer.model_validate(response.output.model_dump())
    valid = {number for number in answer.citations if 1 <= number <= len(hits)}
    dropped = [number for number in answer.citations if number not in valid]
    return {
        "reconnaissance": True,
        "answer": answer.answer,
        "insufficient": answer.insufficient,
        "citations": [
            {"n": number, "chunk_id": hits[number - 1].chunk_id, "product_id": hits[number - 1].product_id}
            for number in sorted(valid)
        ],
        # Ссылка на несуществующий фрагмент — факт о модели, а не повод её «поправить»
        # молча: выбросить номер и промолчать значило бы выдать ответ за проверенный.
        "citations_dropped": dropped,
        "model_id": getattr(llm, "model_id", ""),
        "prompt_hash": response.prompt_hash,
        "note": "разведка: ответ не является evidence и не публикуется (I1/I22)",
    }


def _hit_payload(hit: Hit, urls: dict[str, str]) -> dict[str, Any]:
    return {
        "chunk_id": hit.chunk_id,
        "product_id": hit.product_id,
        "side": hit.side,
        "url": urls.get(hit.chunk_id, ""),
        "heading_path": hit.heading_path,
        "score": round(hit.score, 6),
        "queries": list(hit.queries),
        "content_hash": hit.content_hash,
        "build_id": hit.build_id,
        "text": hit.text,
    }


def _print_text(payload: dict[str, Any]) -> None:
    """Человеческая выдача: шапка с условиями, блоки сторон, затем ответ.

    Блоки печатаются в фиксированном порядке (когорта, затем reference) и **не**
    сортируются между собой: счета двух движков — разные шкалы, и общий рейтинг по ним
    описывал бы нормировку, а не релевантность.
    """
    reference = payload["reference"]
    head: list[str] = []
    if payload["run_id"] is not None:
        head.append(
            f"прогон {payload['run_id']} · сборка {payload['build_id']} · "
            f"профиль {payload['profile']} · top-{payload['top_k']}"
        )
        head.append(f"продукты: {', '.join(payload['products'])}")
    else:
        head.append(
            f"reference-корпус {payload['package_id']}/{reference['product_id']} "
            f"{reference['version']} · сборка {reference['build_id']} · "
            f"профиль {reference['profile']} · top-{payload['top_k']}"
        )
    if reference is not None and payload["run_id"] is not None:
        head.append(
            f"reference: {reference['product_id']} {reference['version']} · "
            f"сборка {reference['build_id']} · {reference['profile']} (векторов нет)"
        )
    head.append("РАЗВЕДКА: выдача — материал для решения, не evidence (I1/I22)")
    schema = payload["schema"]
    if schema is not None and schema["mismatch"]:
        head.append(
            f"СХЕМА: файл {schema['file']}, код {schema['expected']} — чтение разрешено "
            "явно (--allow-legacy-schema); вердикты не читаются"
        )
    if payload["param"]:
        param = payload["param"]
        head.append(f"параметр {param['param_id']}: {param['title']}")
        head.append(f"  absence_rule={param['absence_rule']}")
    if payload["run_id"] is not None:
        if payload["param"]:
            head.append("запросы Ш6 (те же, что увидит оценщик):")
        for number, query in enumerate(payload["queries"], start=1):
            head.append(f"  {number}. {query}")
    if reference is not None:
        if payload["param"]:
            head.append(
                "термины recon (parameter_terms — та же постановка, что у "
                "hound baseline recon):"
            )
        else:
            head.append("запрос reference-стороны:")
        for number, query in enumerate(payload["reference_queries"], start=1):
            head.append(f"  {number}. {query}")
    print("\n".join(head))
    if payload["notices"]:
        print("заметки сборки: " + "; ".join(payload["notices"]))
    if reference is not None and reference["notice"]:
        print(reference["notice"])
    print()

    blocks: list[tuple[str, str, list[dict[str, Any]]]] = []
    if payload["run_id"] is not None:
        blocks.append(
            (
                SIDE_COHORT,
                "КОРПУС КОГОРТЫ",
                [item for item in payload["hits"] if item["side"] == SIDE_COHORT],
            )
        )
    if reference is not None:
        blocks.append(
            (
                SIDE_REFERENCE,
                "REFERENCE-КОРПУС",
                [item for item in payload["hits"] if item["side"] == SIDE_REFERENCE],
            )
        )
    for index, (side, title, items) in enumerate(blocks):
        if len(blocks) > 1:
            print(("" if index == 0 else "\n") + title)
        if not items:
            where = "в корпусе прогона" if side == SIDE_COHORT else "в reference-корпусе"
            print(f"попаданий нет — это результат, а не ошибка: {where} такого нет")
        else:
            print(render_hits(_hits_from(items), urls=_urls_from(items)))
        if payload["explain"] and items:
            print("\nкакой запрос вернул фрагмент (порядок запросов выше):")
            for number, item in enumerate(items, start=1):
                used = ", ".join(str(index_ + 1) for index_ in item["queries"]) or "—"
                print(f"  {number:2d}. {item['chunk_id']} ← {used}")
    if payload["answer"] is not None:
        answer = payload["answer"]
        print("\n" + "=" * 78)
        print("ОТВЕТ РАЗВЕДКИ (не evidence; модель " + str(answer["model_id"]) + ")")
        print("=" * 78)
        if reference is not None:
            print("(ответ построен только по корпусу когорты: у reference-корпуса нет "
                  "своего оценщика)")
        print(answer["answer"])
        if answer["insufficient"]:
            print("(модель отметила: во фрагментах ответа нет)")
        for citation in answer["citations"]:
            print(f"  ← [{citation['n']}] {citation['chunk_id']} ({citation['product_id']})")
        if answer["citations_dropped"]:
            print(f"  ссылки на несуществующие фрагменты отброшены: {answer['citations_dropped']}")


def _hits_from(items: list[dict[str, Any]]) -> list[Hit]:
    return [
        Hit(
            chunk_id=item["chunk_id"],
            product_id=item["product_id"],
            text=item["text"],
            heading_path=item["heading_path"],
            context_prefix="",
            content_hash=item["content_hash"],
            build_id=item["build_id"],
            score=item["score"],
            queries=tuple(item["queries"]),
            side=item.get("side", SIDE_COHORT),
        )
        for item in items
    ]


def _urls_from(items: list[dict[str, Any]]) -> dict[str, str]:
    return {item["chunk_id"]: item["url"] for item in items}


def _resolve_db(args: argparse.Namespace, paths: Any) -> tuple[Path, str | None]:
    """БД прогона и ``run_id``: ``--db`` > ``--runs-dir`` (+``--run-id``) > раскладка.

    Без ``--run-id`` берётся **последний** прогон каталога: максимальный ``created_at``
    из строк ``runs`` (не ``mtime`` файла — правило «выбор никогда по mtime» держится и
    здесь). Равные отметки разрешаются ``run_id``, чтобы выбор был воспроизводимым.
    """
    if args.db is not None:
        return Path(args.db), args.run_id
    runs_dir = Path(args.runs_dir) if args.runs_dir else Path(paths.runs)
    if args.run_id:
        return runs_dir / args.run_id / "cohound.db", args.run_id
    return runs_dir / _latest_run_id(runs_dir) / "cohound.db", None


def _latest_run_id(runs_dir: Path) -> str:
    """Последний прогон каталога по ``runs.created_at`` (детерминированно)."""
    if not runs_dir.is_dir():
        raise CliError(
            f"каталог прогонов не найден: {runs_dir} — укажите --runs-dir или --db",
            code="not_found",
            exit_code=EXIT_NOT_FOUND,
        )
    stamps: list[tuple[str, str]] = []
    for db_path in sorted(runs_dir.glob("*/cohound.db")):
        run_id = db_path.parent.name
        try:
            conn = sqlite3.connect(f"file:{db_path.absolute().as_posix()}?mode=ro", uri=True)
            try:
                row = conn.execute(
                    "SELECT created_at FROM runs WHERE run_id = ?", (run_id,)
                ).fetchone()
            finally:
                conn.close()
        except sqlite3.DatabaseError:
            # Битый файл — не повод падать: он не участвует в выборе, и это видно по
            # отсутствию его run_id в кандидатах.
            continue
        if row is not None and row[0]:
            stamps.append((str(row[0]), run_id))
    if not stamps:
        raise CliError(
            f"в {runs_dir} нет ни одного прогона — укажите --db",
            code="not_found",
            exit_code=EXIT_NOT_FOUND,
        )
    return max(stamps)[1]


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``search``."""
    p_search = sub.add_parser(
        "search",
        help=(
            "поиск по корпусу прогона и/или по reference-корпусу предка: свободный "
            "запрос или параметр каталога (разведка)"
        ),
    )
    p_search.add_argument(
        "query",
        nargs="?",
        default=None,
        help="запрос на естественном языке (либо --param, но не вместе)",
    )
    p_search.add_argument(
        "--param",
        default=None,
        metavar="PARAM_ID",
        help="param_id каталога: искать по терминам, которые уйдут в Ш6 (render_queries)",
    )
    p_search.add_argument(
        "--package",
        default=None,
        help=(
            "каталог пакета для --param и для --scope reference (по умолчанию — из "
            "раскладки по package_id прогона)"
        ),
    )
    p_search.add_argument(
        "--scope",
        choices=list(SCOPES),
        default=SCOPE_COHORT,
        help=(
            "сторона выдачи: cohort — корпус прогона (дефолт), reference — документация "
            "предка (план 27, только R0), both — обе сторонами, без слияния счетов"
        ),
    )
    p_search.add_argument(
        "--reference-version",
        dest="reference_version",
        default=None,
        metavar="VER",
        help=(
            "версия линии предка; по умолчанию — current_version из "
            "sources/reference_products.yaml"
        ),
    )
    p_search.add_argument(
        "--reference-build",
        dest="reference_build",
        default=None,
        metavar="BUILD_ID",
        help=(
            "сборка reference-корпуса; по умолчанию — current.json, иначе единственная "
            "(выбор по mtime запрещён)"
        ),
    )
    search_source = p_search.add_mutually_exclusive_group()
    search_source.add_argument("--db", default=None, help="путь к cohound.db прогона")
    search_source.add_argument(
        "--runs-dir", default=None, help="каталог прогонов (по умолчанию — из раскладки)"
    )
    p_search.add_argument(
        "--run-id",
        default=None,
        help="run_id; по умолчанию — последний прогон по runs.created_at",
    )
    p_search.add_argument(
        "--product",
        action="append",
        default=None,
        metavar="PRODUCT_ID",
        help=(
            "продукт-партиция когорты (можно повторять); по умолчанию все продукты "
            "сборки. С --scope reference обязан совпадать с продуктом линии предка"
        ),
    )
    p_search.add_argument(
        "--retrieval-profile",
        dest="retrieval_profile",
        choices=[profile.value for profile in RetrievalProfile],
        default=RetrievalProfile.R0.value,
        help=(
            "профиль ретривала когорты: R0 — лексический (дефолт: без загрузки весов), "
            "R1 — dense, R2 — гибрид. Reference-сторона всегда R0 (векторов там нет)"
        ),
    )
    p_search.add_argument(
        "--query-lang",
        dest="query_lang",
        choices=["ru", "en", "all"],
        default=None,
        metavar="LANG",
        help=(
            "язык терминов при --param (ru|en|all; по умолчанию — "
            "deploy.retrieval.query_lang): разведка спрашивает те же запросы, что Ш6, "
            "поэтому язык можно переключить и сравнить выдачу"
        ),
    )
    p_search.add_argument(
        "--top-k",
        type=_positive_int,
        default=None,
        help="срез выдачи; по умолчанию — retrieval_cutoff_K прогона",
    )
    p_search.add_argument(
        "--explain",
        action="store_true",
        help="показать, какие запросы вернули каждый фрагмент, и заметки сборки",
    )
    p_search.add_argument(
        "--answer",
        action="store_true",
        help=(
            "LLM-ответ по найденным фрагментам когорты (разведка, не evidence; нужен "
            "LLM-слот). С --scope reference не работает"
        ),
    )
    p_search.add_argument(
        "--allow-legacy-schema",
        dest="allow_legacy_schema",
        action="store_true",
        help=(
            "разрешить чтение корпуса прогона с чужой schema_version (решение владельца "
            "18.11.2026): читаются только корпус и строка прогона, вердикты — нет"
        ),
    )
    _add_llm_model_flag(p_search)
    _add_llm_slot_flag(p_search)
    _add_common(p_search)
    p_search.set_defaults(handler=run)
