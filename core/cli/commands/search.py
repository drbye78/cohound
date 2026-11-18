"""``search``: поиск по корпусу прогона — разведка, не evidence (A3/I17/I18).

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

Ответ LLM (``--answer``) идёт через порт **разведки**
(:class:`~core.ports.discovery_llm.DiscoveryLLMPort`), а не через оценщика: у него
другой контракт (свободный ответ, а не ``LLMCellOutput``), и он не может попасть в
``cell_evidence``. Модель разведки — операционная настройка (env/слот), а не условие
измерения: выход не является доказательством (§7.4).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any

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
from core.domain.models import CatalogRow
from core.pipeline.corpus_search import (
    Hit,
    SearchAnswer,
    build_answer_prompt,
    merge_hits,
    render_hits,
)
from core.pipeline.prompt import render_queries
from core.pipeline.retrieve import RetrievalProfile, retrieve
from core.ports.discovery_llm import DiscoveryLLMRequest
from core.store.corpus_reader import CorpusReader, CorpusReaderError
from core.util.schema_gate import SchemaVersionMismatch

#: Дефолт среза выдачи, когда в прогоне не записан ``retrieval_cutoff_K``.
DEFAULT_TOP_K = 10


def run(args: argparse.Namespace) -> int:
    """``search``: материал из корпуса по запросу (разведка, не evidence).

    Читает **только корпус и строку прогона** (:class:`~core.store.corpus_reader.
    CorpusReader`): ячейки, вердикты и отчёт не открываются, поэтому команда ничего не
    может «подтвердить» по ошибке. Ничего не пишет: ни в БД, ни в каталог прогона.

    :returns: :data:`EXIT_OK` — выдача построена (пустая выдача — тоже валидный
        результат); :data:`EXIT_USAGE` — необъявленный профиль/продукт, чужая схема БД
        без ``--allow-legacy-schema``, неготовый LLM; :data:`EXIT_NOT_FOUND` — прогона
        или БД нет.
    """
    payload = _collect(args)
    if args.json:
        _print_json(payload)
    else:
        _print_text(payload)
    return EXIT_OK


def _collect(args: argparse.Namespace) -> dict[str, Any]:
    """Собирает payload: прогон → сборка → плечи → запросы → слияние → (ответ)."""
    paths = _resolve_paths(args)
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
        return _collect_from(corpus, args, paths)
    finally:
        corpus.close()


def _collect_from(
    corpus: CorpusReader, args: argparse.Namespace, paths: Any
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
    queries, param = _queries(args, ref, paths)
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
        "param": param,
        "explain": bool(args.explain),
        "hits": [_hit_payload(hit, urls) for hit in hits],
        "notices": list(ports.notices),
        "answer": None,
    }
    if args.answer:
        payload["answer"] = _answer(args, queries[0], hits, deploy)
    return payload


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


def _queries(
    args: argparse.Namespace, ref: Any, paths: Any
) -> tuple[list[str], dict[str, Any] | None]:
    """Запросы выдачи: свободный текст либо термины каталога по ``--param``."""
    if args.param and args.query:
        raise CliError(
            "задайте либо свободный запрос, либо --param (это разные входы)",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    if args.param:
        row = _catalog_row(args, ref, paths)
        queries = render_queries(row)
        if not queries:
            raise CliError(
                f"у параметра {row.param_id!r} пусты title и concept_terms — искать не по чему",
                code="usage",
                exit_code=EXIT_USAGE,
            )
        return queries, {
            "param_id": row.param_id,
            "title": row.title,
            "rule": row.rule,
            "absence_rule": row.absence_rule.value,
            "concept_terms": row.concept_terms,
        }
    if not args.query:
        raise CliError(
            "нужен запрос: свободный текст позиционным аргументом или --param PARAM_ID",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    return [args.query], None


def _catalog_row(args: argparse.Namespace, ref: Any, paths: Any) -> CatalogRow:
    """Строка каталога по ``--param``: каталог пакета из раскладки или ``--package``."""
    package_dir = (
        Path(args.package)
        if args.package
        else Path(paths.packages) / (ref.package_id or "")
    )
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
        "url": urls.get(hit.chunk_id, ""),
        "heading_path": hit.heading_path,
        "score": round(hit.score, 6),
        "queries": list(hit.queries),
        "content_hash": hit.content_hash,
        "build_id": hit.build_id,
        "text": hit.text,
    }


def _print_text(payload: dict[str, Any]) -> None:
    """Человеческая выдача: шапка с условиями, затем фрагменты, затем ответ."""
    schema = payload["schema"]
    head = [
        f"прогон {payload['run_id']} · сборка {payload['build_id']} · "
        f"профиль {payload['profile']} · top-{payload['top_k']}",
        f"продукты: {', '.join(payload['products'])}",
        "РАЗВЕДКА: выдача — материал для решения, не evidence (I1/I22)",
    ]
    if schema["mismatch"]:
        head.append(
            f"СХЕМА: файл {schema['file']}, код {schema['expected']} — чтение разрешено "
            "явно (--allow-legacy-schema); вердикты не читаются"
        )
    if payload["param"]:
        param = payload["param"]
        head.append(f"параметр {param['param_id']}: {param['title']}")
        head.append(f"  absence_rule={param['absence_rule']}")
        head.append("запросы Ш6 (те же, что увидит оценщик):")
        for number, query in enumerate(payload["queries"], start=1):
            head.append(f"  {number}. {query}")
    print("\n".join(head))
    if payload["notices"]:
        print("заметки сборки: " + "; ".join(payload["notices"]))
    print()
    if not payload["hits"]:
        print("попаданий нет — это результат, а не ошибка: в корпусе прогона такого нет")
    else:
        print(render_hits(_hits_from(payload), urls=_urls_from(payload)))
    if payload["explain"] and payload["hits"]:
        print("\nкакой запрос вернул фрагмент (порядок запросов выше):")
        for number, item in enumerate(payload["hits"], start=1):
            used = ", ".join(str(index + 1) for index in item["queries"]) or "—"
            print(f"  {number:2d}. {item['chunk_id']} ← {used}")
    if payload["answer"] is not None:
        answer = payload["answer"]
        print("\n" + "=" * 78)
        print("ОТВЕТ РАЗВЕДКИ (не evidence; модель " + str(answer["model_id"]) + ")")
        print("=" * 78)
        print(answer["answer"])
        if answer["insufficient"]:
            print("(модель отметила: во фрагментах ответа нет)")
        for citation in answer["citations"]:
            print(f"  ← [{citation['n']}] {citation['chunk_id']} ({citation['product_id']})")
        if answer["citations_dropped"]:
            print(f"  ссылки на несуществующие фрагменты отброшены: {answer['citations_dropped']}")


def _hits_from(payload: dict[str, Any]) -> list[Hit]:
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
        )
        for item in payload["hits"]
    ]


def _urls_from(payload: dict[str, Any]) -> dict[str, str]:
    return {item["chunk_id"]: item["url"] for item in payload["hits"]}


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
        help="поиск по корпусу прогона: свободный запрос или параметр каталога (разведка)",
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
        help="каталог пакета для --param (по умолчанию — из раскладки по package_id прогона)",
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
        help="продукт-партиция (можно повторять); по умолчанию все продукты сборки",
    )
    p_search.add_argument(
        "--retrieval-profile",
        dest="retrieval_profile",
        choices=[profile.value for profile in RetrievalProfile],
        default=RetrievalProfile.R0.value,
        help=(
            "профиль ретривала: R0 — лексический (дефолт: без загрузки весов), "
            "R1 — dense, R2 — гибрид. Это ось выдачи, а не объявление прогона"
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
        help="LLM-ответ по найденным фрагментам (разведка, не evidence; нужен LLM-слот)",
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
