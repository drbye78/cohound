"""coHound — CLI reference-продуктов профиля: ``hound baseline …`` (план 27 §5).

Группа ``hound baseline`` **отдельно** от ``hound package``: там — авторские CRUD над
пакетом, здесь — измерение (декларация reference-продуктов, факты о документации предка,
разведка и отчёт). Ни одна подкоманда не пишет в ``runs/`` и не трогает прогон: факты и
декларация живут в пакете, reference-корпус — в ``COHOUND_HOME/reference/…``.

Порядок работы — цепочка, и каждая команда называет **следующую** (шаг, который не
сделан, обязан быть виден из вывода, а не из документации):

```
hound baseline build    # корпус версии: краул → экстракция → чанки → сборка
hound baseline recon    # факты по каталогу из сборки (детерминированно, без LLM)
hound baseline review   # подпись человека под фактами (наследует только accepted)
hound baseline propose  # наблюдения → строки baselines.csv (на разметку человеком)
hound baseline report   # что унаследовано и кем (+ --with-map)
hound baseline map      # карта reference-продукта: Capability/Coverage
```

Правила, общие для всех подкоманд (§5):

* ``--actor`` обязателен для любой записи — подпись без автора недоказуема;
* ``--dry-run`` ничего не пишет и **говорит**, что записал бы;
* запись идёт через ``.authoring/apply_log.jsonl`` (существующий контур I1);
* сборка выбирается **явно** (``--build``) либо разрешается раскладкой
  (``current.json`` > единственная сборка > отказ с перечнем) — «последняя по mtime»
  не выбирается никогда (A3).

Нейтрален к предметной категории (§1.3): ни одного названия продукта или вендора.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from core.cli.context import (
    _add_common,
    _document_level,
    _non_negative_int,
    _pdf_pipeline,
    _positive_int,
    _print_json,
)
from core.cli.errors import EXIT_BLOCKED, EXIT_ERROR, EXIT_OK, EXIT_USAGE, CliError
from core.config.run_config import (
    load_baselines,
    load_catalog,
    load_package_manifest,
)
from core.domain.enums import FactReview, FactStatus
from core.domain.models import BaselineFact, CatalogRow
from core.pipeline.inheritance import major_version
from core.pipeline.package_admin import PackageAdminError
from core.pipeline.reference_corpus import (
    ChunkText,
    ReferenceCorpusLayout,
    derive_facts,
    iter_reference_chunks,
    list_builds,
    parameter_phrases,
    parameter_terms,
    read_current_build,
    read_manifest,
    reference_build_id,
    reference_content_digest,
    reference_layout,
    resolve_build,
    select_quote,
)
from core.pipeline.reference_map import (
    MapChunk,
    fact_map_agreement,
    reference_map_id,
)
from core.pipeline.reference_products import (
    apply_refresh,
    baseline_roles,
    carry_signed_facts,
    fact_rows,
    inheritance_report,
    load_package_facts,
    load_package_passports,
    load_package_reference_products,
    refresh_diff,
    review_facts,
    write_facts,
)
from core.pipeline.reference_propose import (
    apply_candidates,
    candidates_path,
    propose_baselines,
    write_candidates,
)

__all__ = ["configure", "run"]


#: Сколько кандидатов лексический индекс отдаёт на **один** параметр разведки.
#: Разведка ищет не «самый похожий» чанк, а **любой**, из которого берётся дословная
#: цитата: кандидат, где терминов правила нет (стемминг лексики и разбор ``parameter_terms``
#: режут текст по-разному), цитаты не даёт, и цикл идёт к следующему. Замер 17.11.2026
#: (``scripts/measure_recon_top_k.py``, корпус пилота 253 306 чанков, 181 правило каталога,
#: линейный проход нашёл цитату для всех 181): ``K=1`` → 178 цитат, ``K=2`` → **181**,
#: дальше не меняется; на срезах 2 000 и 6 000 чанков — то же (`K=2` достаточно).
#: Публикация: ``docs/reports/measurements/2026-11-17-recon-top-k.md``.
#:
#: Взято 20, а не 2: цена линейна по ``K`` (одно обращение к индексу на параметр), а запас
#: нужен для корпусов, где разбор терминов и токенизация расходятся сильнее. **Граница
#: замера:** мерился корпус пилота, а не reference-корпус версии; при промахах первым делом
#: мерить ``K``, а не поднимать его на глаз.
RECON_TOP_K = 20

#: Классификатор разведки: чем решается вопрос «описан ли параметр в документации версии».
#:
#: ``phrase`` — фразовый гейт (``derive_facts``/``select_quote``): BM25 отбирает
#: кандидатов, цитата засчитывается по дословной фразе. Оставлен как **явный legacy-режим**
#: и по умолчанию: он ничего не стоит (нет LLM) и остаётся контрольной линией.
#:
#: ``llm`` — тот же оценщик, что у карты (``evaluate_cell``, промпт ``evaluator_v6``), но
#: на **кандидатах recon** — объединении словесного и фразового наборов
#: (``core/pipeline/recon_candidates.py``). Замер 19.11.2026: ни один набор не является
#: надмножеством другого (49/53 словесный, 28/53 фразовый, 53/53 объединение), поэтому
#: recall даёт объединение, а precision — оценщик. Факт — проекция его вердикта
#: (``core/pipeline/fact_projection.py``).
CLASSIFIER_PHRASE = "phrase"
CLASSIFIER_LLM = "llm"
CLASSIFIERS: tuple[str, ...] = (CLASSIFIER_PHRASE, CLASSIFIER_LLM)

#: Провенанс фактов фразового гейта (``BaselineFact.observed_by``): правило зачёта по
#: фразе, версия 1. Смена правила зачёта — новая метка, иначе две проекции одного прибора
#: по разным правилам выглядели бы одним наблюдением.
PHRASE_OBSERVER = "phrase-1"


def _classifier(args: argparse.Namespace) -> str:
    """Классификатор разведки из ``--classifier`` (по умолчанию — фразовый гейт)."""
    value = str(getattr(args, "classifier", None) or CLASSIFIER_PHRASE)
    if value not in CLASSIFIERS:
        raise CliError(
            f"неизвестный классификатор {value!r} (есть: {', '.join(CLASSIFIERS)})",
            code="baseline",
            exit_code=EXIT_USAGE,
        )
    return value


# ---------------------------------------------------------------------------
# Общие помощники
# ---------------------------------------------------------------------------


def _package_dir(args: argparse.Namespace) -> Path:
    """Каталог пакета из ``--package`` (обязателен для всех подкоманд)."""
    raw = getattr(args, "package", None)
    if not raw:
        raise CliError("--package обязателен", code="baseline", exit_code=EXIT_USAGE)
    path = Path(raw)
    if not path.is_dir():
        raise CliError(
            f"каталог пакета не найден: {path}", code="baseline", exit_code=EXIT_USAGE
        )
    return path


def _reference_base(args: argparse.Namespace) -> Path:
    """Корень reference-корпуса: ``--reference-dir`` > раскладка (``args.paths``).

    Раскладку разрешает composition root (03 §A.1): ``reference`` — обычное поле
    ``HARNESS_FIELDS``, поэтому ``COHOUND_HOME``/``COHOUND_REFERENCE_DIR``/``paths.yaml``
    действуют на него так же, как на ``runs``/``index_store``. Собственный дефолт от CWD
    здесь был бы вторым резолвом раскладки и разошёлся бы с остальными командами.
    Имена каталогов внутри корня — правило ``reference_corpus.reference_layout``.
    """
    root = getattr(args, "reference_dir", None)
    paths = getattr(args, "paths", None)
    return Path(root) if root else (
        Path(paths.reference) if paths is not None else Path("reference")
    )


def _layout(
    args: argparse.Namespace, product: Any, version: str
) -> ReferenceCorpusLayout:
    """Раскладка reference-корпуса продукта/версии (``reference-corpus/2``).

    Путь разрешает composition root (``_reference_base``), а имена каталогов внутри —
    ``reference_corpus.reference_layout``: две копии правила «где лежит сборка» разошлись
    бы при первом же изменении раскладки.
    """
    return reference_layout(
        _reference_base(args),
        package_id=_package_dir(args).name,
        product_id=product.product_id,
        version=version,
    )


def _build_hint(args: argparse.Namespace, product: Any, version: str) -> str:
    """Команда, которая создаёт корпус: её печатает любая ошибка «нечего читать».

    Ошибка без следующего шага заставляет читать документацию, чтобы узнать, чего не
    хватает; поэтому подсказка собирается **здесь**, а не пишется в каждом сообщении.
    """
    return (
        f"hound baseline build --package {_package_dir(args)} "
        f"--baseline {product.baseline_id} --version {version}"
    )


def _resolve_build_id(
    args: argparse.Namespace, layout: ReferenceCorpusLayout, product: Any, *, purpose: str
) -> str:
    """Выбор сборки: ``--build`` > ``current.json`` > единственная > отказ с перечнем.

    Разрешение — дело раскладки (``resolve_build``), здесь только перевод отказа в
    ``CliError`` с адресом: явный выбор не подменяется угадыванием, а неоднозначность не
    решается ни временем файла, ни порядком каталогов (A3 — не выбирать измерение за
    оператора).
    """
    from core.pipeline.reference_corpus import (
        ReferenceBuildAmbiguous,
        ReferenceBuildMissing,
    )

    explicit = getattr(args, "build", None)
    try:
        return resolve_build(layout, explicit=explicit)
    except ReferenceBuildMissing as exc:
        raise CliError(
            f"{purpose}: reference-корпус не найден — {exc}. Соберите его: "
            f"{_build_hint(args, product, layout.version)}",
            code="baseline",
            exit_code=EXIT_USAGE,
        ) from exc
    except ReferenceBuildAmbiguous as exc:
        raise CliError(
            f"{purpose}: сборка не выбрана — {exc}. Укажите --build явно",
            code="baseline",
            exit_code=EXIT_USAGE,
        ) from exc


def _reason_counts(items: Any) -> dict[str, int]:
    """Пропуски по причинам: ``{причина: сколько}`` (детерминированно, по убыванию).

    Форма ``skipped`` принадлежит сборке (``ReferenceBuildResult``) и может быть как
    отображением «причина → счётчик», так и списком записей с полем ``reason``. Печать
    обязана показывать причины, а не длину списка, поэтому читаются обе формы.
    """
    if not items:
        return {}
    if isinstance(items, Mapping):
        return {str(key): int(value) for key, value in sorted(items.items())}
    counts: Counter[str] = Counter()
    for item in items:
        reason = getattr(item, "reason", None)
        if reason is None and isinstance(item, Mapping):
            reason = item.get("reason")
        counts[str(reason or "unknown")] += 1
    return dict(sorted(counts.items(), key=lambda pair: (-pair[1], pair[0])))


def _corpus_state(layout: ReferenceCorpusLayout) -> dict[str, Any]:
    """Состояние корпуса версии для дашборда ``list``: сборки, текущая, объёмы, охват.

    Охват берётся из манифеста **сборки** (``complete``/``incomplete_reasons``/
    ``skipped``/``errors``): это наблюдение прохода, а не свойство продукта, и при
    отсутствии сборки публикуется ``None`` — «неизвестно», а не «полный» (A3: отсутствие
    факта не факт).
    """
    builds = list_builds(layout)
    current = read_current_build(layout)
    state: dict[str, Any] = {
        "version": layout.version,
        "builds": len(builds),
        "build_ids": list(builds),
        "current": current,
        "docs": None,
        "chunks": None,
        "complete": None,
        "incomplete_reasons": [],
        "errors": None,
        "skipped": {},
        "roots": [],
        "db_path": None,
    }
    if current is None:
        return state
    manifest = read_manifest(layout.manifest_path(current))
    state["db_path"] = str(layout.db_path(current))
    if manifest is None:
        return state
    state["docs"] = manifest.docs
    state["chunks"] = manifest.chunks
    state["complete"] = manifest.complete
    state["incomplete_reasons"] = list(manifest.incomplete_reasons)
    state["errors"] = list(manifest.errors)
    state["skipped"] = _reason_counts(manifest.skipped)
    state["roots"] = list(manifest.roots)
    return state


def _coverage_text(state: Mapping[str, Any]) -> str:
    """Охват одной фразой: полный / неполный с причинами / неизвестно."""
    complete = state.get("complete")
    if complete is None:
        return "охват неизвестен"
    if complete:
        return "охват полный"
    reasons = list(state.get("incomplete_reasons") or ())
    if not reasons:
        errors = state.get("errors") or []
        skipped = state.get("skipped") or {}
        if errors:
            reasons.append(f"ошибок {len(errors)}")
        if skipped:
            reasons.append("пропусков " + str(sum(int(v) for v in skipped.values())))
    return "охват неполный" + (f" ({', '.join(reasons)})" if reasons else "")


def _declaration(args: argparse.Namespace) -> Any:
    """Декларация reference-продуктов; отсутствие — ``CliError`` с адресом файла."""
    declaration = load_package_reference_products(_package_dir(args))
    if declaration is None:
        raise CliError(
            "sources/reference_products.yaml отсутствует: reference-продукты не объявлены",
            code="baseline",
            exit_code=EXIT_USAGE,
        )
    return declaration


def _resolve_baseline(args: argparse.Namespace, declaration: Any) -> Any:
    """Reference-продукт по ``--baseline`` (обязателен: без него нечего измерять)."""
    baseline_id = getattr(args, "baseline", None)
    if not baseline_id:
        raise CliError("--baseline обязателен", code="baseline", exit_code=EXIT_USAGE)
    for product in declaration.products:
        if product.baseline_id == baseline_id:
            return product
    raise CliError(
        f"baseline {baseline_id!r} не объявлен в sources/reference_products.yaml "
        f"(объявлены: {', '.join(p.baseline_id for p in declaration.products) or '—'})",
        code="baseline",
        exit_code=EXIT_USAGE,
    )


def _shorten(text: str, limit: int = 120) -> str:
    """Обрезает цитату для однострочного вывода (полная — в ``--json``)."""
    collapsed = " ".join(str(text).split())
    return collapsed if len(collapsed) <= limit else collapsed[: limit - 1].rstrip() + "…"


def _catalog(package: Path) -> list[Any]:
    return load_catalog(package / "catalog.csv")


def _passports(args: argparse.Namespace) -> dict[str, Any]:
    return load_package_passports(getattr(args, "passports_dir", None))


# ---------------------------------------------------------------------------
# build: reference-корпус версии (краул → экстракция → чанки)
# ---------------------------------------------------------------------------


def _roots_for_version(product: Any, version: str) -> list[str]:
    """Корни документации версии — из **декларации**, а не из флагов и не из сети.

    ``sources/reference_products.yaml`` — единственный источник корней (план 27 §3.1):
    корпус описывает документацию объявленной версии объявленного продукта, и ``--urls``
    здесь был бы вторым, независимым объявлением того же предмета. Версия вне декларации —
    отказ с перечнем объявленных, а не «соберём что-нибудь».
    """
    for entry in product.versions:
        if str(entry.version) == version:
            roots = [str(root) for root in entry.docs_roots]
            if not roots:
                raise CliError(
                    f"версия {version} объявлена без docs_roots: добавьте корни "
                    "документации в sources/reference_products.yaml",
                    code="baseline",
                    exit_code=EXIT_USAGE,
                )
            return roots
    raise CliError(
        f"версия {version!r} не объявлена для {product.baseline_id!r} "
        f"(объявлены: {', '.join(str(v.version) for v in product.versions) or '—'})",
        code="baseline",
        exit_code=EXIT_USAGE,
    )


def _rate_limit(raw: str) -> str:
    """``argparse``-тип ``--rate-limit``: канонический темп ``uniform(min=…,max=…)``.

    Сборка объявляет темп **канонической строкой** (``ReferenceBuildLimits.rate_limit`` —
    та же форма, что у ``CrawlLimits``): две записи одного темпа сделали бы сравнение
    сборок строковым лотерейным билетом. Для удобства принимается и одно число секунд
    (``--rate-limit 0.5`` ⇒ ``uniform(min=0.5,max=0.5)``), но канонизирует его
    :func:`~core.domain.models.canonical_rate_limit`, а не строка флага.

    :raises argparse.ArgumentTypeError: ни число, ни каноническая форма.
    """
    from core.domain.models import RateLimitSpec, canonical_rate_limit, parse_rate_limit

    text = (raw or "").strip()
    try:
        seconds = float(text)
    except ValueError:
        seconds = None
    if seconds is not None:
        if seconds < 0:
            raise argparse.ArgumentTypeError(
                f"задержка обязана быть неотрицательной, получено {raw!r}"
            )
        return str(
            canonical_rate_limit(RateLimitSpec(min_s=seconds, max_s=seconds))
        )
    try:
        return str(canonical_rate_limit(parse_rate_limit(text)))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"ожидалось число секунд либо каноническая форма "
            f"'uniform(min=0.5,max=0.5)', получено {raw!r}"
        ) from exc


def _build_limits(args: argparse.Namespace) -> Any:
    """Лимиты сборки из флагов (``None`` — ось не объявлена, действует дефолт сборки)."""
    from core.domain.enums import DocumentLevel, PdfPipeline
    from core.pipeline.reference_build import ReferenceBuildLimits

    depth = getattr(args, "max_depth", None)
    return ReferenceBuildLimits(
        max_documents=getattr(args, "max_documents", None),
        # ``None`` — глубины нет: граница пространства — path-prefix объявленного корня
        # (``docs_roots``), а не выдуманный потолок. Явный ``--max-depth 0`` остаётся
        # объявлением «только корни» и даёт ``complete=False`` (усечение видно).
        max_depth=int(depth) if depth is not None else None,
        rate_limit=getattr(args, "rate_limit", None),
        exclude_paths=tuple(getattr(args, "exclude_paths", None) or ()),
        document_level=getattr(args, "document_level", None) or DocumentLevel.FULL,
        pdf_pipeline=getattr(args, "pdf_pipeline", None) or PdfPipeline.STRUCTURED,
    )


def _package_domain_profiles(package: Path) -> Any:
    """``domain_profiles.yaml`` пакета (если файл есть); иначе ``None`` — осей нет.

    Доменный профиль — авторское объявление осей хоста (``fetch``/``extract``/лимиты), и
    сборка reference-корпуса читает его **того же** пакета: иначе документация предка
    читалась бы другим механизмом, чем документация продукта когорты.
    """
    path = package / "domain_profiles.yaml"
    if not path.is_file():
        return None
    from core.config.domain_profiles import load_domain_profiles

    try:
        return load_domain_profiles(path)
    except ValueError as exc:
        raise CliError(
            f"domain_profiles.yaml пакета не читается: {exc}",
            code="baseline",
            exit_code=EXIT_USAGE,
        ) from exc


def _build_ports(args: argparse.Namespace) -> tuple[Any, Any, Any, Any, Any]:
    """Порты сборки: краулер, экстрактор, чанкер, robots и доменные профили.

    Движки не угадываются здесь, а строятся теми же фабриками, что у прогона:
    ``build_scout_crawler`` (краулер по ``deploy.yaml``/env), ``build_worker_extractor``
    (объявленные ``document_level``/``pdf_pipeline``) и ``FamilyChunker`` с **объявленным**
    ограничителем (``declared_token_limiter``). Инъекция (``args.crawler``/``extractor``/
    ``chunker``/``robots``) — для тестов и будущего AB: она не меняет путь по умолчанию.
    """
    from core.adapters.chunker_family import FamilyChunker, declared_token_limiter
    from core.cli.wiring import (
        _robots_gate,
        build_scout_crawler,
        build_worker_extractor,
    )
    from core.config.deploy import load_deploy_config
    from core.domain.enums import DocumentLevel, PdfPipeline

    deploy = load_deploy_config(None)
    package = _package_dir(args)
    domain_profiles = _package_domain_profiles(package)
    level = getattr(args, "document_level", None) or DocumentLevel.FULL
    pipeline = getattr(args, "pdf_pipeline", None) or PdfPipeline.STRUCTURED

    crawler = getattr(args, "crawler", None)
    if crawler is None:
        crawler = build_scout_crawler(
            deploy=deploy,
            cache=None,
            domain_profiles=domain_profiles,
            run_crawl=None,
            cli_max_depth=getattr(args, "max_depth", None),
        )
    extractor = getattr(args, "extractor", None)
    if extractor is None:
        extractor = build_worker_extractor(level, pipeline)
    chunker = getattr(args, "chunker", None)
    if chunker is None:
        chunker = FamilyChunker(token_limiter=declared_token_limiter())
    robots = getattr(args, "robots", None)
    if robots is None:
        # Тот же гейт, что у прогона: ``robots.txt`` читается лениво и **тем же**
        # краулером, поэтому инъекция краулера в тестах не ходит в сеть сама по себе.
        robots = _robots_gate(crawler, user_agent=deploy.crawl.user_agent)
    return crawler, extractor, chunker, robots, domain_profiles


def run_build(args: argparse.Namespace) -> int:
    """``hound baseline build``: собрать reference-корпус объявленной версии (шаг 1 §8.1).

    Корни — из декларации (``reference_products.yaml:versions[].docs_roots``), механизм —
    тот же, что у корпуса когорты: краулер → экстрактор → чанкер → ``content_hash``.
    Сборка **иммутабельна** и адресуется содержимым (``build_id``), поэтому повторный
    запуск на тех же байтах даёт ту же сборку, а изменившаяся документация — новую.

    ``--dry-run`` печатает план (корни, лимиты, уровень, механизм) и **не** ходит в сеть:
    проверка декларации не обязана стоить запросов.
    """
    package = _package_dir(args)
    declaration = _declaration(args)
    product = _resolve_baseline(args, declaration)
    version = str(getattr(args, "version", None) or product.current_version)
    roots = _roots_for_version(product, version)
    layout = _layout(args, product, version)
    limits = _build_limits(args)
    dry_run = bool(getattr(args, "dry_run", False))

    if dry_run:
        payload: dict[str, Any] = {
            "package": str(package),
            "baseline_id": product.baseline_id,
            "product_id": product.product_id,
            "version": version,
            "roots": roots,
            "limits": asdict(limits),
            "reference_dir": str(layout.root),
            "builds": list_builds(layout),
            "current": read_current_build(layout),
            "dry_run": True,
            "next_command": _build_hint(args, product, version),
        }
        if args.json:
            _print_json(payload)
        else:
            lines = [
                f"build (dry-run): {product.baseline_id} → {product.product_id} @{version}",
                f"  корни ({len(roots)}): " + ", ".join(roots),
                f"  лимиты: max_documents={limits.max_documents}, "
                f"max_depth={limits.max_depth if limits.max_depth is not None else 'без ограничения (граница — корень)'}, "
                f"rate_limit={limits.rate_limit}, "
                f"exclude_paths={list(limits.exclude_paths) or '—'}",
                f"  уровень: {limits.document_level or 'full'}, "
                f"механизм PDF: {limits.pdf_pipeline or 'structured'}",
                f"  раскладка: {layout.root}",
                "сеть не тронута, на диск ничего не записано",
                f"дальше: {_build_hint(args, product, version)} (без --dry-run)",
            ]
            sys.stdout.write("\n".join(lines) + "\n")
        return EXIT_OK

    from core.pipeline.reference_build import (
        ReferenceBuildBusy,
        ReferenceBuildConflict,
        ReferenceBuildError,
        build_reference_corpus,
    )
    from core.store.sqlite_store import SqliteStore

    crawler, extractor, chunker, robots, domain_profiles = _build_ports(args)
    try:
        result = build_reference_corpus(
            layout=layout,
            roots=roots,
            crawler=crawler,
            extractor=extractor,
            chunker=chunker,
            limits=limits,
            domain_profiles=domain_profiles,
            robots=robots,
            store_factory=lambda path: SqliteStore(path),
        )
    except ReferenceBuildBusy as exc:
        raise CliError(
            f"версию уже собирает другой процесс: {exc}. Дождитесь его или снимите "
            f"лок {layout.lock_path}",
            code="baseline",
            exit_code=EXIT_USAGE,
        ) from exc
    except ReferenceBuildConflict as exc:
        raise CliError(
            f"сборка не подтвердила идентичность: {exc}. Удалите каталог сборки "
            f"{layout.builds_dir} и повторите: {_build_hint(args, product, version)}",
            code="baseline",
            exit_code=EXIT_USAGE,
        ) from exc
    except ReferenceBuildError as exc:
        raise CliError(
            f"сборка не удалась: {exc}", code="baseline", exit_code=EXIT_USAGE
        ) from exc

    skipped = _reason_counts(result.skipped)
    next_command = (
        f"hound baseline recon --package {package} --baseline {product.baseline_id} "
        f"--version {version} --build {result.build_id}"
    )
    payload = {
        "package": str(package),
        "baseline_id": product.baseline_id,
        "product_id": product.product_id,
        "version": version,
        "build_id": result.build_id,
        "content_digest": result.content_digest,
        "docs": result.docs,
        "chunks": result.chunks,
        "skipped": skipped,
        "errors": result.errors,
        "truncated": list(result.truncated),
        "complete": bool(result.complete),
        "roots": list(result.roots),
        "reused": result.reused,
        "db_path": str(result.db_path),
        "manifest_path": str(layout.manifest_path(result.build_id)),
        "current": read_current_build(layout),
        "next_command": next_command,
    }
    if args.json:
        _print_json(payload)
    else:
        state = _corpus_state(layout)
        lines = [
            f"build {result.build_id} — {product.product_id} @{version}"
            + (" (уже была: содержимое то же, ничего не перезаписано)" if result.reused else ""),
            f"  документов {result.docs}, чанков {result.chunks}, "
            f"корней {len(payload['roots'])}",
            f"  {_coverage_text(state)}",
        ]
        if skipped:
            lines.append(
                "  пропуски: " + ", ".join(f"{name} {count}" for name, count in skipped.items())
            )
        if payload["errors"]:
            lines.append(f"  ошибок: {len(payload['errors'])}")
            for error in payload["errors"][:10]:
                lines.append(f"    {error}")
        lines.append(f"  сборка: {payload['db_path']}")
        lines.append(f"текущая сборка версии: {payload['current']}")
        lines.append(f"дальше: {next_command}")
        sys.stdout.write("\n".join(lines) + "\n")
    return EXIT_OK


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def _fact_counts(facts: Sequence[BaselineFact], baseline_id: str) -> dict[str, int]:
    """Факты одной линии по статусам и подписям (для дашборда ``list``)."""
    rows = [fact for fact in facts if fact.baseline_id == baseline_id]
    return {
        "total": len(rows),
        "present": sum(1 for fact in rows if fact.status is FactStatus.PRESENT),
        "absent": sum(1 for fact in rows if fact.status is FactStatus.ABSENT),
        "unclear": sum(1 for fact in rows if fact.status is FactStatus.UNCLEAR),
        "accepted": sum(1 for fact in rows if fact.review is FactReview.ACCEPTED),
        "pending": sum(1 for fact in rows if fact.review is FactReview.PENDING),
        "rejected": sum(1 for fact in rows if fact.review is FactReview.REJECTED),
    }


def _map_count(layout: ReferenceCorpusLayout, build_id: str | None) -> int:
    """Сколько карт посчитано по сборке (0 — карт нет)."""
    if build_id is None:
        return 0
    maps_dir = layout.maps_dir(build_id)
    if not maps_dir.is_dir():
        return 0
    return sum(1 for path in maps_dir.iterdir() if path.is_dir())


def _next_command(
    args: argparse.Namespace,
    package: Path,
    product: Any,
    version: str,
    state: Mapping[str, Any],
    counts: Mapping[str, int],
    maps: int,
) -> dict[str, str]:
    """Следующий шаг цепочки для продукта: ``{"step": …, "command": …}``.

    Один шаг, а не список: дашборд отвечает на вопрос «что делать сейчас», и цепочка
    строится по **наблюдаемому** состоянию (сборка → факты → подпись → карта → отчёт),
    а не по номеру этапа плана.
    """
    package_arg = str(package)
    build_cmd = _build_hint(args, product, version)
    if state["current"] is None:
        return {"step": "build", "command": build_cmd}
    recon_cmd = (
        f"hound baseline recon --package {package_arg} --baseline {product.baseline_id} "
        f"--version {version} --build {state['current']}"
    )
    if not counts["total"]:
        return {"step": "recon", "command": recon_cmd}
    if counts["pending"]:
        return {
            "step": "review",
            "command": (
                f"hound baseline review --package {package_arg} "
                f"--baseline {product.baseline_id} --version {version} "
                "--accept --actor {кто}"
            ),
        }
    # Наблюдения собраны и подписаны, а политики нет: это и есть разрыв, который закрывает
    # ``propose``. Без этого шага цепочка после ``review`` уходила в карту, и факты
    # оставались наблюдением, которое никто не превращал в строки наследования.
    if counts.get("accepted", 0) and not counts.get("baselines", 0):
        return {
            "step": "propose",
            "command": (
                f"hound baseline propose --package {package_arg} "
                f"--baseline {product.baseline_id}"
            ),
        }
    if not maps:
        return {
            "step": "map",
            "command": (
                f"hound baseline map --package {package_arg} "
                f"--baseline {product.baseline_id} --version {version} "
                f"--build {state['current']}"
            ),
        }
    return {
        "step": "report",
        "command": (
            f"hound baseline report --package {package_arg} --with-map --json"
        ),
    }


def _list_payload(args: argparse.Namespace) -> dict[str, Any]:
    package = _package_dir(args)
    declaration = load_package_reference_products(package)
    cohort_products: list[str] = []
    cohort_path = package / "cohort.yaml"
    if cohort_path.is_file():
        from core.config.run_config import load_cohort

        cohort_products = [
            str(entry.product_id) for entry in load_cohort(cohort_path).entries
        ]
    roles = baseline_roles(
        declaration, _passports(args), cohort_products=cohort_products
    )
    facts = load_package_facts(package)
    baselines = load_baselines(package / "baselines.csv")
    corpus: dict[str, Any] = {}
    fact_summary: dict[str, Any] = {}
    next_commands: dict[str, Any] = {}
    for product in declaration.products if declaration is not None else ():
        version = str(product.current_version)
        layout = _layout(args, product, version)
        state = _corpus_state(layout)
        counts = _fact_counts(facts, product.baseline_id)
        counts["baselines"] = sum(
            1 for row in baselines if row.baseline_id == product.baseline_id
        )
        maps = _map_count(layout, state["current"])
        corpus[product.baseline_id] = {**state, "maps": maps}
        fact_summary[product.baseline_id] = counts
        next_commands[product.baseline_id] = _next_command(
            args, package, product, version, state, counts, maps
        )
    primary = _primary_next(next_commands)
    payload: dict[str, Any] = {
        "package": str(package),
        "declared": declaration is not None,
        "products": roles,
        "baselines_rows": len(baselines),
        "facts": len(facts),
        "cohort_products": cohort_products,
        "corpus": corpus,
        "fact_summary": fact_summary,
        "next_commands": next_commands,
        "next": primary,
    }
    return payload


def _primary_next(next_commands: Mapping[str, Any]) -> dict[str, str] | None:
    """Шаг «дальше» для текущего продукта: первый незавершённый в порядке ``baseline_id``.

    Если незавершённых нет, печатается шаг первой линии (``report``) — дашборд не
    оставляет читателя без следующего действия.
    """
    if not next_commands:
        return None
    ordered = sorted(next_commands.items())
    for baseline_id, item in ordered:
        if item["step"] != "report":
            return {"baseline_id": baseline_id, **item}
    baseline_id, item = ordered[0]
    return {"baseline_id": baseline_id, **item}


def _corpus_column(state: Mapping[str, Any]) -> str:
    """Состояние корпуса одной фразой: сборки, текущая, объёмы, охват."""
    if not state["builds"]:
        return "сборок 0 (корпус не собран)"
    docs = state["docs"] if state["docs"] is not None else "?"
    chunks = state["chunks"] if state["chunks"] is not None else "?"
    return (
        f"сборок {state['builds']}, текущая {state['current'] or '—'}, "
        f"{docs} док / {chunks} чанков, {_coverage_text(state)}"
    )


def _print_list(payload: dict[str, Any]) -> None:
    lines: list[str] = []
    if not payload["declared"]:
        lines.append(
            "reference_products: не объявлены (sources/reference_products.yaml отсутствует)"
        )
    products_payload: dict[str, dict[str, Any]] = dict(payload["products"])
    for baseline_id, data in sorted(products_payload.items()):
        who = (
            "ancestor of: " + ", ".join(data["ancestors_of"])
            if data["ancestors_of"]
            else "parallel (не предок когорты)"
        )
        state = payload["corpus"].get(baseline_id, {})
        counts = payload["fact_summary"].get(baseline_id, {})
        lines.append(
            f"{baseline_id:<14} {data['product_id']:<14} "
            f"current {data['current_version']:<4} "
            f"versions [{', '.join(data['versions'])}]  {who}  "
            f"корпус: {_corpus_column(state)}  "
            f"факты: {counts.get('total', 0)} (present {counts.get('present', 0)}, "
            f"absent {counts.get('absent', 0)}, unclear {counts.get('unclear', 0)}; "
            f"accepted {counts.get('accepted', 0)}, pending {counts.get('pending', 0)}, "
            f"rejected {counts.get('rejected', 0)})  "
            f"baselines.csv: {counts.get('baselines', 0)} строк"
        )
    lines.append(
        f"итого: baselines.csv {payload['baselines_rows']} строк; "
        f"baseline_facts.csv {payload['facts']} фактов"
    )
    primary = payload.get("next")
    if primary is None:
        lines.append(
            "дальше: объявите reference-продукт в sources/reference_products.yaml "
            "(schema reference_products/1), затем `hound baseline build …`"
        )
    else:
        lines.append(f"дальше ({primary['baseline_id']}): {primary['command']}")
    sys.stdout.write("\n".join(lines) + "\n")


def run_list(args: argparse.Namespace) -> int:
    """``hound baseline list``: что объявлено и в каком состоянии (роль вычисляется)."""
    payload = _list_payload(args)
    if args.json:
        _print_json(payload)
    else:
        _print_list(payload)
    return EXIT_OK


# ---------------------------------------------------------------------------
# facts
# ---------------------------------------------------------------------------


def run_facts(args: argparse.Namespace) -> int:
    """``hound baseline facts``: факты с фильтрами (read-only, ничего не пишет)."""
    package = _package_dir(args)
    rows = fact_rows(
        load_package_facts(package),
        baseline_id=getattr(args, "baseline", None),
        version=getattr(args, "version", None),
        param_id=getattr(args, "param", None),
        review=getattr(args, "review", None),
        status=getattr(args, "status", None),
    )
    payload = {
        "package": str(package),
        "count": len(rows),
        "facts": [
            {
                "baseline_id": fact.baseline_id,
                "param_id": fact.param_id,
                "version": fact.version,
                "status": fact.status.value,
                "review": fact.review.value,
                "url": fact.url,
                "quote": fact.quote,
                "content_hash": fact.content_hash,
                "observed_at": fact.observed_at,
                "reviewed_by": fact.reviewed_by,
                "note": fact.note,
            }
            for fact in rows
        ],
    }
    if args.json:
        _print_json(payload)
    else:
        lines = [f"facts: {len(rows)}"]
        for fact in rows:
            lines.append(
                f"  {fact.baseline_id} {fact.param_id} @{fact.version}: "
                f"{fact.status.value}/{fact.review.value}"
                + (f"  {fact.url}" if fact.url else "")
                + (f"  — {_shorten(fact.quote)}" if fact.quote else "")
            )
        sys.stdout.write("\n".join(lines) + "\n")
    return EXIT_OK


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------


def run_report(args: argparse.Namespace) -> int:
    """``hound baseline report``: что унаследовано, кем и от какого предка (без прогона)."""
    package = _package_dir(args)
    baselines = load_baselines(package / "baselines.csv")
    facts = load_package_facts(package)
    passports = _passports(args)
    cohort_path = package / "cohort.yaml"
    products: list[str] = []
    if cohort_path.is_file():
        from core.config.run_config import load_cohort

        products = [str(entry.product_id) for entry in load_cohort(cohort_path).entries]
    declaration = load_package_reference_products(package)
    report = inheritance_report(
        baselines=baselines,
        products=products,
        passports=passports,
        facts=facts,
        params=_catalog(package),
    )
    payload: dict[str, Any] = {
        "package": str(package),
        "reference_products": (
            {
                product.baseline_id: {
                    "product_id": product.product_id,
                    "current_version": product.current_version,
                    "versions": [version.version for version in product.versions],
                }
                for product in (declaration.products if declaration else ())
            }
        ),
        "facts": {
            "total": len(facts),
            "accepted": sum(1 for f in facts if f.review is FactReview.ACCEPTED),
            "pending": sum(1 for f in facts if f.review is FactReview.PENDING),
            "rejected": sum(1 for f in facts if f.review is FactReview.REJECTED),
            "present": sum(1 for f in facts if f.status is FactStatus.PRESENT),
            "absent": sum(1 for f in facts if f.status is FactStatus.ABSENT),
            "unclear": sum(1 for f in facts if f.status is FactStatus.UNCLEAR),
        },
        "inheritance": report,
    }
    with_map = bool(getattr(args, "with_map", False))
    if with_map:
        payload["map"] = _map_section(args, package, declaration, facts)
    if args.json:
        _print_json(payload)
    else:
        lines = [f"package: {package}"]
        reference_payload: dict[str, dict[str, Any]] = dict(
            payload["reference_products"]
        )
        for baseline_id, data in sorted(reference_payload.items()):
            lines.append(
                f"reference {baseline_id}: {data['product_id']} "
                f"current {data['current_version']} "
                f"versions [{', '.join(data['versions'])}]"
            )
        facts_summary = payload["facts"]
        lines.append(
            "факты: "
            f"{facts_summary['total']} (present {facts_summary['present']}, "
            f"absent {facts_summary['absent']}, unclear {facts_summary['unclear']}; "
            f"accepted {facts_summary['accepted']}, pending {facts_summary['pending']}, "
            f"rejected {facts_summary['rejected']})"
        )
        for line in report["summary"]:
            lines.append(f"  {line}")
        lines.append(f"inherited cells: {report['inherited_cells']}")
        for error in report["errors"]:
            lines.append(f"error: {error}")
        if with_map:
            lines.extend(_format_map_section(payload["map"]))
        sys.stdout.write("\n".join(lines) + "\n")
    return EXIT_OK


def _map_dir(
    args: argparse.Namespace, product: Any, version: str, *, purpose: str = "report --with-map"
) -> Path | None:
    """Каталог ``maps/`` **выбранной сборки** версии (``None`` — сборки ещё нет).

    Карта адресуется сборкой: смена модели/промпта меняет карту, но не корпус, и раскладка
    держит карты внутри сборки (``builds/{build_id}/maps/``). Сборки нет — карты тоже:
    это не ошибка отчёта, а состояние, о котором отчёт говорит подсказкой. Неоднозначность
    сборок, наоборот, **не** глотается: отчёт обязан отказать так же, как ``recon``.
    """
    from core.pipeline.reference_corpus import ReferenceBuildMissing

    layout = _layout(args, product, version)
    try:
        build_id = _resolve_build_id(args, layout, product, purpose=purpose)
    except CliError as exc:
        cause = exc.__cause__
        if isinstance(cause, ReferenceBuildMissing):
            return None
        raise
    return layout.maps_dir(build_id)


def _pick_map(
    args: argparse.Namespace, maps_dir: Path | None, *, flag: str = "--map"
) -> Path | None:
    """Выбирает карту: явный ``--map`` либо **единственную** имеющуюся.

    Несколько карт и нет флага — отказ с их списком, а не «последняя по mtime»:
    выбор измерения по времени файла выдал бы одну карту за другую (A3 — не угадывать).
    ``flag`` — имя флага у вызывающей подкоманды: подсказка обязана называть тот флаг,
    который у неё есть, иначе читатель ищет несуществующую опцию.
    """
    if maps_dir is None:
        return None
    explicit = getattr(args, "map_id", None)
    if explicit:
        path = maps_dir / explicit
        if not path.is_dir():
            raise CliError(
                f"карта не найдена: {path}. Посчитайте её: "
                f"`hound baseline map --package {_package_dir(args)} --baseline "
                f"{getattr(args, 'baseline', '…')}`",
                code="baseline",
                exit_code=EXIT_USAGE,
            )
        return path
    if not maps_dir.is_dir():
        return None
    candidates = sorted(p for p in maps_dir.iterdir() if p.is_dir())
    if not candidates:
        return None
    if len(candidates) > 1:
        names = ", ".join(path.name for path in candidates)
        raise CliError(
            f"карт несколько ({names}): укажите {flag}, чтобы не угадывать измерение",
            code="baseline",
            exit_code=EXIT_USAGE,
        )
    return candidates[0]


def _map_section(
    args: argparse.Namespace,
    package: Path,
    declaration: Any,
    facts: list[Any],
) -> dict[str, Any] | None:
    """Секция карты для отчёта: сводка ``manifest`` + согласие факт↔карта (§9.5)."""
    from core.pipeline.reference_map import (
        fact_map_agreement,
        read_map_cells,
        read_map_manifest,
    )

    products = list(declaration.products) if declaration is not None else []
    baseline_id = getattr(args, "baseline", None)
    if baseline_id:
        products = [p for p in products if p.baseline_id == baseline_id]
    out: list[dict[str, Any]] = []
    for product in products:
        version = str(getattr(args, "version", None) or product.current_version)
        maps_dir = _map_dir(args, product, version)
        chosen = _pick_map(args, maps_dir)
        if chosen is None:
            out.append(
                {
                    "baseline_id": product.baseline_id,
                    "version": version,
                    "map_id": None,
                    "note": (
                        f"карты нет: `hound baseline map --package {package} "
                        f"--baseline {product.baseline_id} --version {version}` "
                        f"(сборка — `{_build_hint(args, product, version)}`)"
                    ),
                }
            )
            continue
        manifest = read_map_manifest(chosen / "manifest.json")
        rows = read_map_cells(chosen / "cells.jsonl")
        score_path = chosen / "score.json"
        score = (
            json.loads(score_path.read_text(encoding="utf-8"))
            if score_path.is_file()
            else None
        )
        agreement = fact_map_agreement(
            [fact for fact in facts if fact.baseline_id == product.baseline_id],
            rows,
            version=version,
        )
        out.append(
            {
                "baseline_id": product.baseline_id,
                "version": version,
                "map_id": chosen.name,
                "manifest": manifest.as_dict() if manifest is not None else None,
                "counts": dict(manifest.counts) if manifest is not None else {},
                "score": score,
                "fact_map_agreement": agreement,
            }
        )
    if not out:
        return None
    return out[0] if len(out) == 1 else {"maps": out}


def _format_map_section(section: Any) -> list[str]:
    """Человекочитаемая сводка карты: числа + согласие, без интерпретации."""
    if section is None:
        return ["карта: reference-продукты не объявлены"]
    items: list[dict[str, Any]] = (
        list(section["maps"]) if "maps" in section else [section]
    )
    lines: list[str] = []
    for item in items:
        if item.get("map_id") is None:
            lines.append(f"карта {item['baseline_id']} @{item['version']}: {item['note']}")
            continue
        counts = ", ".join(f"{name} {count}" for name, count in sorted(item["counts"].items()))
        lines.append(
            f"карта {item['baseline_id']} @{item['version']}: {item['map_id']} "
            f"({counts or 'пусто'})"
        )
        agreement = item["fact_map_agreement"]
        # Факты других версий в κ не входят: наблюдение @11 — не наблюдение @18.
        other = agreement.get("skipped_other_versions") or 0
        version_note = f", фактов других версий отброшено {other}" if other else ""
        if agreement["kappa"] is not None:
            lines.append(
                f"  согласие факт↔карта @{item['version']}: κ = {agreement['kappa']:.4f} "
                f"(n={agreement['n']}{version_note}), "
                f"расхождений {len(agreement['disagreements'])}"
                f" (факт present, карта ❌: {agreement['fact_present_map_absent']})"
            )
        else:
            lines.append(
                f"  согласие факт↔карта @{item['version']}: undefined "
                f"({agreement['undefined_reason']}{version_note})"
            )
    return lines


# ---------------------------------------------------------------------------
# review
# ---------------------------------------------------------------------------


def run_review(args: argparse.Namespace) -> int:
    """``hound baseline review``: человеческая подпись под фактом (§5, §3.3)."""
    accept: bool | None
    if getattr(args, "accept", False):
        accept = True
    elif getattr(args, "reject", False):
        accept = False
    elif getattr(args, "clear", False):
        accept = None
    else:
        raise CliError(
            "нужен один из флагов: --accept | --reject | --clear",
            code="baseline",
            exit_code=EXIT_USAGE,
        )
    baseline_id = str(getattr(args, "baseline", None) or "")
    if not baseline_id:
        raise CliError("--baseline обязателен", code="baseline", exit_code=EXIT_USAGE)
    try:
        payload = review_facts(
            _package_dir(args),
            baseline_id=baseline_id,
            actor=getattr(args, "actor", None) or "",
            param_id=getattr(args, "param", None),
            version=getattr(args, "version", None),
            accept=accept,
            note=getattr(args, "note", None),
            dry_run=bool(getattr(args, "dry_run", False)),
        )
    except ValueError as exc:
        raise CliError(str(exc), code="baseline", exit_code=EXIT_USAGE) from exc
    if args.json:
        _print_json(payload)
    else:
        verb = "принято" if accept is True else ("отклонено" if accept is False else "снято")
        lines = [
            f"review: {payload['review']} ({verb}), изменено {len(payload['changed'])}, "
            f"без изменений {payload['skipped']}"
        ]
        for item in payload["changed"]:
            lines.append(
                f"  {item['param_id']} @{item['version']}: {item['from']} → {item['to']}"
            )
        if payload["dry_run"]:
            lines.append("dry-run: файл не записан")
        else:
            lines.append(f"записано: {payload['path']}")
        sys.stdout.write("\n".join(lines) + "\n")
    return EXIT_OK


# ---------------------------------------------------------------------------
# refresh
# ---------------------------------------------------------------------------


def run_refresh(args: argparse.Namespace) -> int:
    """``hound baseline refresh``: перемер новой версии и diff, без записи политики (§13).

    ``removed`` не применяется никогда; ``min_version`` выводится **предложением** —
    ``baselines.csv`` эта команда не трогает вовсе (неверный пол молча пере- или
    недо-наследует, а решение о применимости строки — авторское).

    ``--apply`` доводит до файла то, что раньше только печаталось: ``apply_refresh``
    применяет ``added``/``changed`` к ``baseline_facts.csv`` (``removed`` — нет). Без
    ``--apply`` поведение прежнее байт-в-байт: ничего не пишется.
    """
    package = _package_dir(args)
    declaration = _declaration(args)
    product = _resolve_baseline(args, declaration)
    version = str(getattr(args, "version", None) or product.current_version)
    existing = load_package_facts(package)
    previous = [
        fact
        for fact in existing
        if fact.baseline_id == product.baseline_id and fact.version != version
    ]
    current = [
        fact
        for fact in existing
        if fact.baseline_id == product.baseline_id and fact.version == version
    ]
    diff = refresh_diff(previous, current)
    previous_versions = sorted({fact.version for fact in previous})
    apply = bool(getattr(args, "apply", False))
    if apply:
        actor = (getattr(args, "actor", None) or "").strip()
        if not actor:
            raise CliError(
                "--apply требует --actor: запись без автора недоказуема",
                code="baseline",
                exit_code=EXIT_USAGE,
            )
        payload = apply_refresh(
            package,
            baseline_id=product.baseline_id,
            version=version,
            actor=actor,
            dry_run=bool(getattr(args, "dry_run", False)),
            current_facts=current,
        )
        payload["package"] = str(package)
        payload["note"] = (
            "removed не применяется: регрессия/переезд/дефект различает человек; "
            "baselines.csv не тронут — строки политики предлагает `hound baseline propose`"
        )
    else:
        payload = {
            "package": str(package),
            "baseline_id": product.baseline_id,
            "version": version,
            "previous_versions": previous_versions,
            "diff": diff,
            "proposed_min_version": [item["param_id"] for item in diff["added"]],
            "applied": [],
            "dry_run": True,
            "note": (
                "refresh не пишет факты и не меняет baselines.csv: новая версия разведуется "
                "через `hound baseline recon --version …`, а min_version остаётся "
                "предложением автору профиля (§27.13)"
            ),
        }
    if args.json:
        _print_json(payload)
    else:
        lines = [
            f"refresh {product.baseline_id} @{version} против "
            f"[{', '.join(previous_versions) or '—'}]",
        ]
        for kind in ("added", "removed", "changed", "unchanged", "unclear"):
            items = payload["diff"][kind]
            lines.append(f"  {kind}: {len(items)}")
            for item in items[:20]:
                lines.append(f"    {item['param_id']}")
            if len(items) > 20:
                lines.append(f"    … ещё {len(items) - 20}")
        if payload.get("proposed_min_version"):
            lines.append(
                "предложение min_version="
                f"{version} для: {', '.join(payload['proposed_min_version'][:20])}"
            )
        lines.append("removed не применяется: регрессия/переезд/дефект различает человек")
        if apply:
            applied = payload["applied"]
            lines.append(
                ("dry-run: " if payload["dry_run"] else "")
                + f"применено фактов: {len(applied)}"
                + (
                    " — " + ", ".join(item["param_id"] for item in applied[:20])
                    if applied
                    else ""
                )
            )
            if payload["dry_run"]:
                lines.append("файл фактов не записан")
        sys.stdout.write("\n".join(lines) + "\n")
    return EXIT_OK


# ---------------------------------------------------------------------------
# propose: наблюдения → строки политики наследования (шаг между review и report)
# ---------------------------------------------------------------------------


def _propose_map(
    args: argparse.Namespace, product: Any, version: str
) -> tuple[list[dict[str, Any]], str | None]:
    """Карта версии для предложения (``--with-map``): строки ``cells.jsonl`` и версия.

    Карта нужна ровно для одного решения — «цитата найдена, но она не о предмете»
    (``support_not_about_subject``). Её отсутствие не ошибка предложения: без карты режим
    ``conditional`` не выводится, потому что условие не наблюдали. Сборка выбирается так
    же, как у ``report --with-map`` (``current.json`` > единственная > отказ), поэтому
    карта читается с той сборки, которую оператор и считал текущей.
    """
    if not getattr(args, "with_map", False):
        return [], None
    from core.pipeline.reference_map import read_map_cells

    maps_dir = _map_dir(args, product, version, purpose="propose --with-map")
    chosen = _pick_map(args, maps_dir)
    if chosen is None:
        return [], None
    return read_map_cells(chosen / "cells.jsonl"), version


def run_propose(args: argparse.Namespace) -> int:
    """``hound baseline propose``: строки политики из подписанных наблюдений (§13).

    Два режима и разная запись:

    * без ``--apply`` — строит ``sources/baselines.candidate.csv`` (предложение на
      разметку); строки политики **не** пишутся: ``decision`` заполняет человек;
    * ``--apply --actor`` — читает размеченный файл и пишет ``baselines.csv`` только по
      ``accept``. Незаполненная строка — отказ: разметку не выдумываем.

    Строки ``baselines.csv`` входят в ``baselines_digest``, а тот — в
    ``execution_contract_digest``: применение предложения меняет ``run_id``. Это печатается
    сообщением, но не вычисляется здесь: идентичность считает ``plan``.
    """
    package = _package_dir(args)
    declaration = _declaration(args)
    product = _resolve_baseline(args, declaration)
    dry_run = bool(getattr(args, "dry_run", False))
    apply = bool(getattr(args, "apply", False))

    if apply:
        try:
            result = apply_candidates(
                package,
                baseline_id=product.baseline_id,
                actor=getattr(args, "actor", None) or "",
                dry_run=dry_run,
            )
        except ValueError as exc:
            raise CliError(str(exc), code="baseline", exit_code=EXIT_USAGE) from exc
        except PackageAdminError as exc:
            raise CliError(
                str(exc),
                code=getattr(exc, "code", "baseline"),
                exit_code=EXIT_USAGE if exc.kind == "usage" else EXIT_BLOCKED,
            ) from exc
        payload: dict[str, Any] = {
            "package": str(package),
            "baseline_id": product.baseline_id,
            "apply": result,
            "dry_run": dry_run,
            "execution_change": True,
            "note": (
                "строки baselines.csv входят в baselines_digest ⇒ execution_change: "
                "новый run_id (посчитает `hound plan`)"
            ),
        }
        if args.json:
            _print_json(payload)
        else:
            lines = [
                f"propose --apply: {product.baseline_id}, файл {result['path']}",
                f"  принято {result['counts']['accepted']}, пропущено "
                f"{result['counts']['skipped']} (reject/skip/existing не пишутся)",
            ]
            for item in result["added"]:
                lines.append(
                    f"  + {item['param_id']} min_version={item['min_version']} "
                    f"{item['inheritance_mode']} ({item['reason']})"
                )
            lines.append(
                "dry-run: baselines.csv не записан"
                if dry_run
                else "execution_change: baselines_digest изменился ⇒ новый run_id"
            )
            sys.stdout.write("\n".join(lines) + "\n")
        return EXIT_OK

    versions = [str(entry.version) for entry in product.versions]
    map_cells, map_version = _propose_map(
        args, product, str(getattr(args, "version", None) or product.current_version)
    )
    proposal = propose_baselines(
        baseline_id=product.baseline_id,
        facts=load_package_facts(package),
        versions=versions,
        catalog=_catalog(package),
        baselines=load_baselines(package / "baselines.csv"),
        map_cells=map_cells,
        map_version=map_version,
    )
    path: Path | None = None
    if not dry_run:
        path = write_candidates(package, product.baseline_id, proposal["rows"])
    payload = {
        "package": str(package),
        **proposal,
        "path": (
            str(path)
            if path is not None
            else str(candidates_path(package, product.baseline_id))
        ),
        "written": path is not None,
        "dry_run": dry_run,
        "next_command": (
            f"hound baseline propose --package {package} "
            f"--baseline {product.baseline_id} --apply --actor {{кто}}"
        ),
    }
    if args.json:
        _print_json(payload)
    else:
        counts = proposal["counts"]
        lines = [
            f"propose {product.baseline_id}: каталог {counts['catalog']}, "
            f"объявлено версий {len(versions)}"
            + (
                f", карта @{map_version} применена"
                if proposal["map_applied"]
                else ""
            ),
            f"  предлагается {counts['proposed']} "
            f"(direct {counts['direct']}, conditional {counts['conditional']})",
            f"  нужна подпись: {counts['needs_signature']}, "
            f"уже в baselines.csv: {counts['existing']}, "
            f"без доказательств: {counts['no_evidence']}",
        ]
        for param_id in proposal["proposed"][:20]:
            lines.append(f"  → {param_id}")
        if len(proposal["proposed"]) > 20:
            lines.append(f"  … ещё {len(proposal['proposed']) - 20}")
        if proposal["needs_signature"]:
            lines.append(
                "  без подписи (прибор не подписывает за человека): "
                + ", ".join(proposal["needs_signature"][:10])
                + (" …" if len(proposal["needs_signature"]) > 10 else "")
            )
        lines += [
            (
                "dry-run: файл предложения не записан"
                if dry_run
                else f"записано: {payload['path']} (decision пуста — заполняет человек)"
            ),
            f"дальше: {payload['next_command']}",
        ]
        sys.stdout.write("\n".join(lines) + "\n")
    return EXIT_OK


# ---------------------------------------------------------------------------
# recon
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ReconDoc:
    """Чанк в форме ``LexicalDocLike``: индексу нужны идентичность, партиция и текст.

    Адрес/хеш индексу не нужны, а факту — нужны, поэтому исходный чанк держится рядом:
    ``select_quote``/``derive_facts`` читают его, индекс — только текст.
    """

    chunk_id: str
    product_id: str
    text: str
    chunk: ChunkText


def _recon_facts(
    *,
    baseline_id: str,
    version: str,
    params: Sequence[CatalogRow],
    docs: Sequence[_ReconDoc],
    index: Any,
    corpus_build_id: str,
    observed_at: str,
    coverage_complete: bool = False,
    query_lang: str = "all",
) -> list[BaselineFact]:
    """Факты по каталогу: кандидатов отбирает индекс, статус ставит ``derive_facts``.

    Прежний линейный проход читал **все** чанки на каждый параметр (181 правило × корпус
    версии) и брал первый подходящий в порядке ``doc_id, chunk_no`` — то есть порядок
    документа, а не релевантность. Теперь кандидаты приходят из лексического индекса
    (``Bm25Index`` за портом ``LexicalSearcherPort``) в порядке убывания счёта с
    tie-break по ``chunk_id``, и цикл идёт по ним, пока ``select_quote`` не найдёт
    дословную цитату.

    **Статус факта остаётся за ``derive_facts``** (единственный источник семантики
    ``present``/``unclear``/``absent``, I15/I22): здесь выбирается только **что читать**.
    Пустой список кандидатов — тот же исход «не нашли», что у линейного прохода, с той же
    причиной (``coverage_incomplete``), поэтому неполнота охвата не подменяется
    релевантностью.

    ``coverage_complete`` — **наблюдение сборки**, а не решение разведки: он приходит из
    ``manifest.complete``. При полном охвате «не нашли» — это ``absent`` (утверждение о
    документации версии), при неполном — ``unclear`` с причиной ``coverage_incomplete``.
    Без этого параметра даже аттестованная сборка публиковала ``unclear``, и ``absent``
    не появлялся никогда.

    **Запрос и зачёт — разные построители, и это не дублирование.** Кандидатов даёт
    ``parameter_terms`` (recall: BM25 токенизирует всё равно, и слова дают больше
    попаданий), а цитату засчитывает ``parameter_phrases`` (точность: термин целиком).
    Слить их в один список значило бы либо потерять кандидатов, либо вернуть зачёт по
    слову — замер 18.11.2026: 165 ``present`` по слову против 37 по фразе при одном и том
    же наборе кандидатов.
    """
    by_id = {doc.chunk_id: doc for doc in docs}
    facts: list[BaselineFact] = []
    for param in params:
        terms = parameter_terms(param, query_lang=query_lang)
        phrases = parameter_phrases(param)
        candidates = index.search_scores(" ".join(terms), top_k=RECON_TOP_K) if terms else []
        fact: BaselineFact | None = None
        for chunk_id, _score in candidates:
            doc = by_id.get(chunk_id)
            if doc is None or select_quote(doc.chunk.text, phrases) is None:
                continue
            fact = derive_facts(
                baseline_id=baseline_id,
                version=version,
                chunks=[doc.chunk],
                params=[param],
                corpus_build_id=corpus_build_id,
                observed_at=observed_at,
                coverage_complete=coverage_complete,
            )[0]
            break
        if fact is None:
            fact = derive_facts(
                baseline_id=baseline_id,
                version=version,
                chunks=[],
                params=[param],
                corpus_build_id=corpus_build_id,
                observed_at=observed_at,
                coverage_complete=coverage_complete,
            )[0]
        facts.append(fact)
    return facts


def _quote_stats(facts: Sequence[BaselineFact]) -> dict[str, Any]:
    """Сколько ``present`` и сколько среди них **различных** цитат (видимость дефекта).

    Зачёт по слову давал 45 ``present`` при 30 уникальных цитатах, и одна цитата
    обслуживала 8 параметров (замер 18.11.2026): цитата, названная однажды, «подтверждала»
    параметры, которые она не называет. Числа ``quotes``/``unique_quotes`` и список
    ``shared_quotes`` делают этот класс дефекта **видимым** в выводе, вместо того чтобы он
    тихо наследовался следующим замером. ``unique_quote_ratio`` — ``None`` при нуле
    ``present``: «нечего делить» и «всё делится» — разные наблюдения, и один ноль их не
    различает.
    """
    present = [fact for fact in facts if fact.status is FactStatus.PRESENT]
    by_quote: dict[str, list[str]] = {}
    for fact in present:
        if fact.quote:
            by_quote.setdefault(fact.quote, []).append(str(fact.param_id))
    shared = [
        {"quote": quote[:120], "params": sorted(param_ids)}
        for quote, param_ids in by_quote.items()
        if len(param_ids) >= 2
    ]
    # По убыванию числа параметров, затем по тексту: порядок не должен зависеть от
    # порядка обхода фактов (R7 — повторный recon даёт тот же вывод).
    shared.sort(key=lambda item: (-len(item["params"]), item["quote"]))
    quotes = len(present)
    return {
        "quotes": quotes,
        "unique_quotes": len(by_quote),
        "shared_quotes": shared,
        "unique_quote_ratio": (len(by_quote) / quotes) if quotes else None,
    }


def _same_fact_version(left: str, right: str) -> bool:
    """Совпадают ли версии факта и сборки — по **мажору**, как и везде в наследовании.

    Факт записан версией как в документации (``18.4.1``), а сборка адресуется объявленным
    мажором (``18``); сравнение строками пропустило бы подпись, записанную подробной
    версией, и она потерялась бы молча — ровно тот дефект, который здесь и закрывается.
    """
    left_major = major_version(left)
    right_major = major_version(right)
    if left_major is None or right_major is None:
        return left == right
    return left_major == right_major


def _llm_model_id(evaluator: Any) -> str:
    """Объявленная модель оценщика: ``--llm-model`` > слот ``deploy.yaml: llm`` > отказ.

    Та же дисциплина, что у карты и прогона (I21): модель — объявленный вход, а не
    окружение. Разведка без модели не «попробует дефолт», а называет, чего не хватает.
    """
    model_id = getattr(evaluator, "model_id", None)
    if not model_id:
        raise CliError(
            "оценщик без объявленной модели (model_id пуст): укажите --llm-model либо "
            "слот deploy.yaml: llm — иначе провенанс факта не назвал бы модель",
            code="baseline",
            exit_code=EXIT_USAGE,
        )
    return str(model_id)


def _llm_facts(
    args: argparse.Namespace,
    *,
    db_path: Path,
    manifest: Any,
    build_id: str,
    chunks: Sequence[ChunkText],
    catalog: Sequence[CatalogRow],
    product: Any,
    version: str,
    observed_at: str,
    query_lang: str,
) -> tuple[list[BaselineFact], dict[str, Any]]:
    """Факты версии — оценщик на **кандидатах recon** (``--classifier llm``).

    Кандидатов даёт :func:`core.pipeline.recon_candidates.recon_candidates` (объединение
    словесного и фразового наборов: замер 19.11.2026 — 49/53 и 28/53 по отдельности против
    53/53 вместе), а решение о наличии принимает **тот же** :func:`evaluate_cell`, что у
    карты и Ш6: промпт ``evaluator_v6``, ``strict=True``, тот же эшелон валидации (I22).
    Второй формулы оценки нет — иначе расхождение двух приборов мерило бы разницу
    реализаций, а не разницу путей.

    Даты наблюдений и покрытие передаются из **самой сборки**
    (``read_artifact_dates``/``coverage_from_manifest``): без них ``❌`` публиковался бы
    без датированного носителя (I2c/I2d).

    :returns: ``(facts, meta)`` — факты в порядке каталога и провенанс прогона
        (``model_id``, ``candidate_policy``, число кандидатов), который печатает команда.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from core.adapters.lexical_bm25 import Bm25Index
    from core.pipeline.evaluate import evaluate_cell
    from core.pipeline.fact_projection import facts_from_cells
    from core.pipeline.prompt import TEMPLATE_VERSION
    from core.pipeline.recon_candidates import CANDIDATE_POLICY, recon_candidates
    from core.pipeline.reference_corpus import (
        coverage_from_manifest,
        read_artifact_dates,
    )
    from core.ports.vector_index import RetrievalResult, RetrievedChunk
    from core.util.hashing import content_hash

    evaluator = _evaluator(args)
    model_id = _llm_model_id(evaluator)
    index = Bm25Index()
    docs = [
        _ReconDoc(
            chunk_id=chunk.chunk_id,
            product_id=product.product_id,
            text=chunk.text,
            chunk=chunk,
        )
        for chunk in chunks
    ]
    index.index(docs)
    by_id = {doc.chunk_id: doc for doc in docs}
    # Адрес и хеш — у чанка корпуса: проекция привязывает цитату к **тексту**, и хеш
    # чанка — единственное, что делает её проверяемой. Фолбэк хеша — как у ``derive_facts``.
    lookup: dict[str, tuple[str | None, str | None]] = {
        chunk.chunk_id: (chunk.url, chunk.content_hash or content_hash(chunk.text))
        for chunk in chunks
    }
    run_id = f"recon:{product.baseline_id}:{version}"
    artifact_dates = read_artifact_dates(db_path, product.product_id)
    coverage = coverage_from_manifest(manifest, product.product_id)
    candidates_per_param: dict[str, int] = {}

    def _cell_for(param: CatalogRow) -> Any:
        """Одна ячейка: кандидаты recon → оценка тем же ``evaluate_cell``, что у карты."""
        pairs = recon_candidates(
            param,
            index=index,
            product_id=product.product_id,
            query_lang=query_lang,
        )
        candidates_per_param[str(param.param_id)] = len(pairs)
        retrieved = [
            RetrievedChunk(
                chunk_id=chunk_id,
                product_id=product.product_id,
                text=by_id[chunk_id].text,
                content_hash=lookup[chunk_id][1] or "",
                heading_path="",
                build_id=build_id,
                score=score,
            )
            for chunk_id, score in pairs
            if chunk_id in by_id
        ]
        retrieval = RetrievalResult(
            chunks=retrieved,
            profile="R0",
            cutoff_K=len(retrieved),
            build_id=build_id,
        )
        outcome = evaluate_cell(
            param=param,
            product_id=product.product_id,
            run_id=run_id,
            evaluator=evaluator,
            chunks=retrieved,
            retrievals=retrieval,
            strict=True,
            model_id=model_id,
            artifact_dates=artifact_dates,
            coverage=coverage,
        )
        return outcome.cell

    cells: dict[str, Any] = {}
    failed: list[str] = []
    concurrency = int(getattr(args, "llm_concurrency", 1) or 1)
    total = len(catalog)
    started = _dt.datetime.now(_dt.UTC)

    def _record(param_id: str, cell: Any, done: int) -> None:
        cells[param_id] = cell
        elapsed = (_dt.datetime.now(_dt.UTC) - started).total_seconds()
        rate = done / elapsed if elapsed > 0 else 0.0
        left = (total - done) / rate if rate > 0 else 0.0
        sys.stderr.write(
            f"[recon {done}/{total}] {param_id[:40]:40} "
            f"{elapsed:6.0f} с, осталось ≈{left / 60:.1f} мин\n"
        )
        sys.stderr.flush()

    done = 0
    if concurrency == 1:
        for param in catalog:
            try:
                cell = _cell_for(param)
            except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                failed.append(f"{param.param_id}: {type(exc).__name__}: {exc}")
                continue
            done += 1
            _record(str(param.param_id), cell, done)
    else:
        with ThreadPoolExecutor(
            max_workers=concurrency, thread_name_prefix="recon-llm"
        ) as pool:
            futures = {pool.submit(_cell_for, param): param for param in catalog}
            for future in as_completed(futures):
                param = futures[future]
                try:
                    cell = future.result()
                except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                    failed.append(f"{param.param_id}: {type(exc).__name__}: {exc}")
                    continue
                done += 1
                _record(str(param.param_id), cell, done)

    if failed:
        # Частичный набор фактов выдал бы себя за полный: недостающая ячейка дала бы
        # ``unclear``, неотличимый от «оценщик не нашёл». Ошибки названы все сразу.
        raise CliError(
            f"разведка не завершена: упало ячеек {len(failed)} (всего {total}); "
            f"первая: {failed[0]}. Факты не записаны",
            code="baseline",
            exit_code=EXIT_ERROR,
        )

    # Провенанс наблюдения: модель, набор кандидатов и **версия шаблона промпта**. Версия
    # входит потому, что правка шаблона меняет вердикты (``v3`` перестал выдумывать
    # ``❌``, ``v4`` вернул различение частичного утверждения, ``v6`` добавил группу и
    # модуль): без неё два разных правила оценки выглядели бы одним наблюдением, а
    # ``baseline_facts_digest`` намеренно не видит провенанс и не различил бы их.
    observed_by = f"llm:{model_id}:{CANDIDATE_POLICY}:{TEMPLATE_VERSION}"
    facts = facts_from_cells(
        cells=cells,
        params=catalog,
        baseline_id=product.baseline_id,
        version=version,
        corpus_build_id=build_id,
        observed_at=observed_at,
        observed_by=observed_by,
        chunk_lookup=lambda chunk_id: lookup.get(chunk_id, (None, None)),
    )
    counts = sorted(candidates_per_param.values())
    meta = {
        "model_id": model_id,
        "candidate_policy": CANDIDATE_POLICY,
        "observed_by": observed_by,
        # Цена вызова: сколько кандидатов в среднем уходило в промпт на параметр. Число
        # печатается, а не подразумевается: от него зависит и цена, и что видит модель.
        "candidates_mean": (sum(counts) / len(counts)) if counts else None,
        "candidates_max": counts[-1] if counts else None,
        "candidates_min": counts[0] if counts else None,
    }
    return facts, meta


def _recon_version(
    args: argparse.Namespace,
    product: Any,
    *,
    version: str,
    catalog: Sequence[CatalogRow],
    previous: Sequence[Any] | None = None,
) -> dict[str, Any]:
    """Разведка **одной** версии: сборка → чанки → факты (без записи).

    Отдельная функция, потому что ``--all-versions`` обходит все объявленные версии, а
    запись у него одна: наблюдение версии возвращается вызывающему, и он решает, что и
    когда писать. Отказ версии (нет сборки, пустой корпус, нет оценщика) поднимается как
    ``CliError`` — вызывающий либо пробрасывает его, либо публикует строкой «пропущено».

    Два классификатора (``--classifier``) и один корпус:

    * ``phrase`` — кандидатов отбирает лексический индекс, цитату засчитывает
      ``derive_facts`` (прежний путь, байт-в-байт по набору фактов);
    * ``llm`` — кандидатов даёт объединение словесного и фразового наборов
      (``recon_candidates``), вердикт — тот же ``evaluate_cell``, что у карты, факт —
      проекция вердикта (``fact_projection``).
    """
    layout = _layout(args, product, version)
    classifier = _classifier(args)
    explicit_db = getattr(args, "corpus_db", None)
    if explicit_db and getattr(args, "build", None):
        # Два адреса одной сборки: молча выиграть один значило бы измерить не то, что
        # назвал оператор. Это отказ, а не приоритет.
        raise CliError(
            "--corpus-db и --build называют разные адреса корпуса: выберите один "
            "(--build — сборку раскладки, --corpus-db — чужой файл)",
            code="baseline",
            exit_code=EXIT_USAGE,
        )
    if explicit_db:
        # Явный путь — аварийный вход (чужая БД, разбор): сборка тогда адресуется
        # содержимым, потому что манифеста у такого файла может не быть.
        db_path = Path(explicit_db)
        manifest = None
        if not db_path.is_file():
            raise CliError(
                f"reference-корпус не найден: {db_path} (--corpus-db). Соберите его: "
                f"{_build_hint(args, product, version)}",
                code="baseline",
                exit_code=EXIT_USAGE,
            )
    else:
        build_id = _resolve_build_id(args, layout, product, purpose="recon")
        db_path = layout.db_path(build_id)
        manifest = read_manifest(layout.manifest_path(build_id))
        if not db_path.is_file():
            raise CliError(
                f"сборка {build_id} объявлена, но файла корпуса нет: {db_path}. "
                f"Пересоберите: {_build_hint(args, product, version)}",
                code="baseline",
                exit_code=EXIT_USAGE,
            )
    chunks = list(iter_reference_chunks(db_path))
    if not chunks:
        raise CliError(
            f"reference-корпус пуст: {db_path} (0 чанков). Пересоберите: "
            f"{_build_hint(args, product, version)}",
            code="baseline",
            exit_code=EXIT_USAGE,
        )
    # Без манифеста сборка адресуется **содержимым**, а не путём: путь — раскладка хоста,
    # и хеш от него сделал бы две одинаковые сборки разными (§4).
    build_id = (
        manifest.build_id
        if manifest is not None
        else reference_build_id(
            product.product_id, version, reference_content_digest(chunks)
        )
    )
    # Охват — **наблюдение сборки**: ``complete`` манифеста аттестует исчерпание
    # объявленного пространства, и только тогда «не нашли» становится ``absent``.
    # Манифеста нет (``--corpus-db``) — полнота неизвестна, значит ``unclear``.
    coverage_complete = bool(manifest.complete) if manifest is not None else False
    observed_at = _dt.datetime.now(_dt.UTC).replace(microsecond=0).isoformat()
    # Резолв языка — до вызова: тот же резолвер, что у прогона и карты
    # (``--query-lang`` > ``deploy.retrieval.query_lang`` > ``all``).
    from core.cli.execution_axes import resolve_query_lang
    from core.config.deploy import load_deploy_config

    query_lang = resolve_query_lang(
        getattr(args, "query_lang", None), load_deploy_config(None)
    )
    engine: str | None = None
    meta: dict[str, Any] = {}
    if classifier == CLASSIFIER_LLM:
        facts, meta = _llm_facts(
            args,
            db_path=db_path,
            manifest=manifest,
            build_id=build_id,
            chunks=chunks,
            catalog=catalog,
            product=product,
            version=version,
            observed_at=observed_at,
            query_lang=query_lang,
        )
    else:
        # Движок лексики — адаптер, и выбирает его composition root: `pipeline` не
        # импортирует `adapters` (матрица 01 §1). Индекс строится один раз на прогон, а не
        # на параметр.
        from core.adapters.lexical_bm25 import Bm25Index

        index = Bm25Index()
        docs = [
            _ReconDoc(
                chunk_id=chunk.chunk_id,
                product_id=product.product_id,
                text=chunk.text,
                chunk=chunk,
            )
            for chunk in chunks
        ]
        index.index(docs)
        engine = index.engine_version
        facts = _recon_facts(
            baseline_id=product.baseline_id,
            version=version,
            params=catalog,
            docs=docs,
            index=index,
            corpus_build_id=build_id,
            observed_at=observed_at,
            coverage_complete=coverage_complete,
            # Язык терминов — объявленное условие (``--query-lang`` > deploy > ``all``):
            # на английском reference-корпусе русские термины вытесняли английские из
            # бюджета ``MAX_TERMS``, и непустой top-10 давали 88 параметров из 181.
            query_lang=query_lang,
        )
        # Провенанс наблюдения: каким прибором получен факт. ``derive_facts`` о приборе не
        # знает (он — правило зачёта), поэтому метку ставит вызывающий.
        facts = [
            fact.model_copy(update={"observed_by": PHRASE_OBSERVER}) for fact in facts
        ]
    # Подписи переживают пере-разведку, если их доказательство на месте: прибор не вправе
    # стирать решение человека молча (§3.3), а человек не вправе держать подпись под
    # цитатой, которой в корпусе больше нет.
    carried: list[dict[str, str]] = []
    dropped: list[dict[str, str]] = []
    if previous is not None:
        texts = [chunk.text for chunk in chunks]

        def _quote_resolves(fact: Any) -> bool:
            """Цитата факта дословно встречается в корпусе **этой** версии."""
            quote = fact.quote
            return bool(quote) and any(quote in text for text in texts)

        kept = carry_signed_facts(
            [fact for fact in previous if _same_fact_version(fact.version, version)],
            facts,
            quote_resolves=_quote_resolves,
        )
        facts = kept["facts"]
        carried = kept["carried"]
        dropped = kept["dropped"]
    return {
        "version": version,
        "build_id": build_id,
        "classifier": classifier,
        **meta,
        "chunks": len(chunks),
        "docs": len({chunk.doc_id for chunk in chunks}),
        "engine": engine,
        "coverage_complete": coverage_complete,
        "facts": facts,
        "review_carried": carried,
        "review_dropped": dropped,
    }


def _recon_version_line(item: Mapping[str, Any]) -> dict[str, Any]:
    """Сводка версии для вывода: сборка, объёмы, исходы (без самих фактов)."""
    facts = list(item["facts"])
    return {
        "version": item["version"],
        "build_id": item["build_id"],
        "classifier": item.get("classifier"),
        "model_id": item.get("model_id"),
        "candidate_policy": item.get("candidate_policy"),
        "chunks": item["chunks"],
        "docs": item["docs"],
        "coverage_complete": item["coverage_complete"],
        "present": sum(1 for fact in facts if fact.status is FactStatus.PRESENT),
        "unclear": sum(1 for fact in facts if fact.status is FactStatus.UNCLEAR),
        "absent": sum(1 for fact in facts if fact.status is FactStatus.ABSENT),
        # Параметров, которых оценщик не оценивал: не отказ, но видимое число — иначе
        # «unclear» смешал бы «ячейки нет» и «оценщик смотрел и не признал».
        "llm_missing_cell": sum(
            1 for fact in facts if (fact.note or "").startswith("llm_missing_cell")
        ),
        # Подписи, пережившие пере-разведку, и подписи, чьё доказательство исчезло:
        # и то и другое — видимое число, а не тишина.
        "review_carried": len(item.get("review_carried") or []),
        "review_dropped": len(item.get("review_dropped") or []),
    }


def run_recon(args: argparse.Namespace) -> int:
    """``hound baseline recon``: факты по каталогу из reference-корпуса (шаг 7a плана).

    Разведка **детерминированная**: кандидат — чанк, найденный лексическим индексом по
    терминам правила; цитата обязана быть дословной подстрокой текста чанка. LLM здесь не
    участвует, и это осознанно: факт — провенанс, а провенанс не должен зависеть от
    модели. Движок индекса выбирает composition root, и его версия печатается: «чем
    искали» — часть наблюдения.

    ``absent`` публикуется **только** при аттестованном охвате (``manifest.complete``):
    проход по неполному корпусу не даёт утверждения об отсутствии (I2/I2c), поэтому «не
    нашли» там — ``unclear`` с причиной ``coverage_incomplete``.

    ``--all-versions`` обходит все **объявленные** версии линии: у каждой своя сборка
    (``current.json`` > единственная > пропуск), а запись одна на конце — слияние сохраняет
    прочие версии и параметры. Версия без сборки печатается строкой «сборки нет,
    пропущено» и считается в итоге: одна из восьми может быть не собрана, и это не повод
    отказывать всей разведке, но и не повод молчать.

    ``--classifier llm`` меняет **источник** наблюдения, а не его форму: кандидатов даёт
    объединение словесного и фразового наборов (``core/pipeline/recon_candidates.py``),
    вердикт — тот же ``evaluate_cell``, что у карты и Ш6, а факт — проекция вердикта
    (``core/pipeline/fact_projection.py``). В выводе публикуются ``classifier``,
    ``model_id`` и ``candidate_policy`` — читатель обязан видеть, чем и на каком наборе
    кандидатов снято наблюдение. ``--classifier phrase`` (по умолчанию) — прежний
    фразовый гейт.
    """
    package = _package_dir(args)
    declaration = _declaration(args)
    product = _resolve_baseline(args, declaration)
    classifier = _classifier(args)
    all_versions = bool(getattr(args, "all_versions", False))
    if all_versions:
        if getattr(args, "version", None):
            raise CliError(
                "--version и --all-versions взаимоисключающие: выберите одну версию либо "
                "все объявленные",
                code="baseline",
                exit_code=EXIT_USAGE,
            )
        if getattr(args, "corpus_db", None) or getattr(args, "build", None):
            raise CliError(
                "--corpus-db/--build адресуют одну сборку и несовместимы с --all-versions: "
                "у каждой версии своя сборка",
                code="baseline",
                exit_code=EXIT_USAGE,
            )
        versions = [str(entry.version) for entry in product.versions]
        if not versions:
            raise CliError(
                f"{product.baseline_id}: в sources/reference_products.yaml не объявлено ни "
                "одной версии — разведывать нечего",
                code="baseline",
                exit_code=EXIT_USAGE,
            )
    else:
        versions = [str(getattr(args, "version", None) or product.current_version)]
    catalog = _catalog(package)
    only = getattr(args, "param", None)
    if only:
        catalog = [row for row in catalog if str(row.param_id) == only]
        if not catalog:
            raise CliError(
                f"param {only!r} отсутствует в каталоге", code="baseline", exit_code=EXIT_USAGE
            )

    observed: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    # Прежние факты читаются **до** обхода: по ним решается, какие подписи переживают
    # пере-разведку, и это решение принимается на версию, а не после записи.
    previous_facts = load_package_facts(package)
    for version in versions:
        try:
            observed.append(
                _recon_version(
                    args, product, version=version, catalog=catalog, previous=previous_facts
                )
            )
        except CliError as exc:
            if not all_versions:
                raise
            # Версия не разведана — видимая строка, а не тихий пропуск: причина называется
            # текстом отказа (нет сборки / неоднозначная сборка / пустой корпус).
            skipped.append({"version": version, "reason": str(exc)})

    facts = [fact for item in observed for fact in item["facts"]]
    dry_run = bool(getattr(args, "dry_run", False))
    # Слияние по ключу ``(baseline, мажор версии, param)``: обновляются только перемеренные
    # строки, всё остальное (другие версии, другие параметры) остаётся как было. Мажор, а не
    # строка: факт мог быть записан подробной версией (``18.4.1``), и сравнение строками
    # оставило бы рядом две строки одного параметра.
    fresh_keys = {
        (fact.baseline_id, major_version(fact.version) or fact.version, fact.param_id)
        for fact in facts
    }
    merged = [
        fact
        for fact in previous_facts
        if (fact.baseline_id, major_version(fact.version) or fact.version, fact.param_id)
        not in fresh_keys
    ]
    written = False
    if facts and not dry_run:
        write_facts(package, [*merged, *facts])
        written = True
    primary = observed[0] if observed else None
    current_version = str(product.current_version)
    payload_carried = [entry for item in observed for entry in item["review_carried"]]
    payload_dropped = [entry for item in observed for entry in item["review_dropped"]]
    payload: dict[str, Any] = {
        "package": str(package),
        "baseline_id": product.baseline_id,
        "version": primary["version"] if (primary is not None and not all_versions) else None,
        "corpus_build_id": (
            primary["build_id"] if (primary is not None and not all_versions) else None
        ),
        "build": primary["build_id"] if (primary is not None and not all_versions) else None,
        "engine": primary["engine"] if primary is not None else None,
        # Чем снято наблюдение: фразовый гейт или оценщик на кандидатах recon. Вместе с
        # ``model_id``/``candidate_policy`` это провенанс набора фактов — без него
        # читатель не отличит два правила по имени.
        "classifier": classifier,
        "model_id": (
            primary.get("model_id") if (primary is not None and not all_versions) else None
        ),
        "candidate_policy": (
            primary.get("candidate_policy")
            if (primary is not None and not all_versions)
            else None
        ),
        # Параметров, которых оценщик не оценивал: не отказ — они дали
        # ``unclear``/``llm_missing_cell``, — но число печатается.
        "llm_missing_cell": sum(
            1 for fact in facts if (fact.note or "").startswith("llm_missing_cell")
        ),
        # Цена вызова: сколько кандидатов в среднем уходило в промпт на параметр. Число
        # печатается, а не подразумевается: от него зависит и цена, и то, что видит модель.
        "candidates_mean": (
            primary.get("candidates_mean") if (primary is not None and not all_versions) else None
        ),
        "candidates_min": (
            primary.get("candidates_min") if (primary is not None and not all_versions) else None
        ),
        "candidates_max": (
            primary.get("candidates_max") if (primary is not None and not all_versions) else None
        ),
        "top_k": RECON_TOP_K,
        "chunks": sum(item["chunks"] for item in observed),
        "docs": sum(item["docs"] for item in observed),
        "params": len(catalog),
        "present": sum(1 for fact in facts if fact.status is FactStatus.PRESENT),
        "unclear": sum(1 for fact in facts if fact.status is FactStatus.UNCLEAR),
        "absent": sum(1 for fact in facts if fact.status is FactStatus.ABSENT),
        **_quote_stats(facts),
        # Параметр без терминов не ищется вовсе, и это не «не нашли в корпусе»:
        # назвать число дешевле, чем оставить оператора догадываться, почему ячейка
        # пустая. Термины даёт title+concept_terms (rule исключён, см.
        # ``parameter_terms``), поэтому пустой список — дефект каталога, а не корпуса.
        "params_without_terms": sorted(
            str(row.param_id) for row in catalog if not parameter_terms(row)
        ),
        "facts_path": str(package / "sources" / "baseline_facts.csv"),
        "written": written,
        "dry_run": dry_run,
        "all_versions": all_versions,
        "versions": [_recon_version_line(item) for item in observed],
        "skipped_versions": skipped,
        # Подписи, пережившие пере-разведку и потерянные (цитаты в корпусе больше нет).
        # Без этих двух списков повторный recon молча стирал бы решение человека.
        "review_carried": payload_carried,
        "review_dropped": payload_dropped,
        "next_command": (
            f"hound baseline review --package {package} "
            f"--baseline {product.baseline_id} --version {current_version} "
            "--accept --actor {кто}"
        ),
        "note": (
            "факты записаны с review=pending: наследует только accepted, подпись — "
            "`hound baseline review --accept --actor …`"
            + (
                f"; подписей сохранено {len(payload_carried)}, потеряно {len(payload_dropped)}"
                if (payload_carried or payload_dropped)
                else ""
            )
        ),
    }
    # Подписи — видимым числом: «сохранено» и «снято» печатаются всегда, когда есть что
    # сказать, а не только в JSON. Молчаливая потеря подписи — тот самый дефект.
    signature_lines: list[str] = []
    if payload_carried:
        signature_lines.append(
            f"подписи сохранены (доказательство на месте): {len(payload_carried)} — "
            + ", ".join(entry["param_id"] for entry in payload_carried[:3])
            + (" …" if len(payload_carried) > 3 else "")
        )
    if payload_dropped:
        signature_lines.append(
            f"подписи сняты (цитаты в корпусе нет): {len(payload_dropped)} — "
            + ", ".join(entry["param_id"] for entry in payload_dropped[:3])
            + (" …" if len(payload_dropped) > 3 else "")
        )
    if args.json:
        _print_json(payload)
    elif all_versions:
        lines = [
            f"recon {product.baseline_id}: объявлено версий {len(versions)}, "
            f"разведано {len(observed)}, пропущено {len(skipped)}; "
            f"classifier {classifier}"
            + (
                f" (top_k={RECON_TOP_K}, индекс {payload['engine'] or '—'})"
                if classifier == CLASSIFIER_PHRASE
                else f" (модель {payload['model_id']}, кандидаты {payload['candidate_policy']})"
            ),
        ]
        for item in payload["versions"]:
            lines.append(
                f"  @{item['version']}: сборка {item['build_id']}, {item['chunks']} чанков, "
                f"{item['docs']} документов — present {item['present']}, "
                f"unclear {item['unclear']}, absent {item['absent']}"
                + (f", модель {item['model_id']}" if item.get("model_id") else "")
                + ("" if item["coverage_complete"] else " (охват неполный)")
            )
        for item in skipped:
            lines.append(f"  @{item['version']}: сборки нет, пропущено — {item['reason']}")
        lines += [
            f"итого: present {payload['present']}, unclear {payload['unclear']}, "
            f"absent {payload['absent']}; цитат {payload['quotes']}, "
            f"уникальных {payload['unique_quotes']}",
            *(
                [
                    f"без ячейки оценщика (unclear/llm_missing_cell): "
                    f"{payload['llm_missing_cell']}"
                ]
                if payload["llm_missing_cell"]
                else []
            ),
            *signature_lines,
            (
                "dry-run: ничего не записано"
                if dry_run
                else (
                    f"записано: {payload['facts_path']} (review=pending)"
                    if written
                    else "записывать нечего: ни одна версия не разведана"
                )
            ),
            f"дальше: {payload['next_command']}",
        ]
        sys.stdout.write("\n".join(lines) + "\n")
    else:
        ratio = payload["unique_quote_ratio"]
        # «—» при нуле present: доли нет, потому что делить нечего (а не потому что 0.0).
        ratio_text = "—" if ratio is None else f"{ratio:.2f}"
        source_text = (
            f"модель {payload['model_id']}, кандидаты {payload['candidate_policy']}"
            if classifier == CLASSIFIER_LLM
            else f"индекс {payload['engine']} (top_k={RECON_TOP_K})"
        )
        lines = [
            f"recon {product.baseline_id} @{payload['version']}: {payload['chunks']} чанков, "
            f"{payload['docs']} документов, сборка {payload['build']}, "
            f"classifier {classifier} ({source_text})",
            f"параметров: {payload['params']}; present {payload['present']}, "
            f"unclear {payload['unclear']}, absent {payload['absent']}",
            f"цитат: {payload['quotes']}, уникальных {payload['unique_quotes']} "
            f"(доля {ratio_text})",
        ]
        if classifier == CLASSIFIER_LLM and primary is not None:
            lines.append(
                f"кандидатов на параметр: "
                f"среднее {primary.get('candidates_mean') or 0:.1f}, "
                f"мин {primary.get('candidates_min')}, макс {primary.get('candidates_max')}"
            )
        if payload["llm_missing_cell"]:
            lines.append(
                f"без ячейки оценщика (unclear/llm_missing_cell): "
                f"{payload['llm_missing_cell']}"
            )
        shared = payload["shared_quotes"]
        if shared:
            shown = "; ".join(
                f"{', '.join(item['params'])}: {item['quote']}" for item in shared[:3]
            )
            lines.append(
                f"общих цитат: {len(shared)} — {shown}"
                + (f" … и ещё {len(shared) - 3}" if len(shared) > 3 else "")
            )
        without = payload["params_without_terms"]
        if without and classifier == CLASSIFIER_PHRASE:
            lines.append(
                f"без терминов (не искались, дефект каталога): {len(without)} — "
                + ", ".join(without[:5])
                + (" …" if len(without) > 5 else "")
            )
        lines += [
            *signature_lines,
            (
                "dry-run: ничего не записано"
                if dry_run
                else f"записано: {payload['facts_path']} (review=pending)"
            ),
            f"дальше: {payload['next_command']}",
        ]
        sys.stdout.write("\n".join(lines) + "\n")
    return EXIT_OK


# ---------------------------------------------------------------------------
# map: карта reference-продукта (этап 9 плана)
# ---------------------------------------------------------------------------


def _module_weights(package: Path) -> dict[str, float]:
    """Объявленные веса модулей пакета (``package.yaml: modules``).

    Карта считается **тем же** расчётом, что бандл когорты, а веса — его вход. Выдумать
    равные веса значило бы посчитать другую Capability, чем у продуктов; отсутствие
    ``package.yaml`` — отказ с адресом, а не тихий ноль.
    """
    manifest = load_package_manifest(package / "package.yaml")
    return {str(module.module_id): float(module.weight) for module in manifest.modules}


def _evaluator(args: argparse.Namespace) -> Any:
    """Оценщик карты: инъекция (``args.ports``/тесты) либо сборка из LLM-слота.

    Модель — **объявленный** вход (``--llm-model`` > слот), как у прогона: карта без
    названной модели не измерение (входит в ``map_id``).
    """
    injected = getattr(args, "evaluator", None)
    if injected is not None:
        return injected
    from core.cli.wiring import build_evaluator_llm
    from core.config.deploy import load_deploy_config

    deploy = load_deploy_config(None)
    evaluator = build_evaluator_llm(
        deploy_llm=getattr(deploy, "llm", None),
        model_id=getattr(args, "llm_model", None),
    )
    if evaluator is None:
        raise CliError(
            "оценщик не сконфигурирован: `hound baseline map` и "
            "`hound baseline recon --classifier llm` требуют LLM-слот (deploy.yaml: llm) "
            "либо --llm-model; детерминированные факты строит "
            "`hound baseline recon --classifier phrase`",
            code="baseline",
            exit_code=EXIT_USAGE,
        )
    return evaluator


def run_map(args: argparse.Namespace) -> int:
    """``hound baseline map``: Capability/Coverage reference-продукта по каталогу (§9).

    Карта **не** входит в сравнение когорты: артефакт лежит в
    ``reference/…/builds/{build_id}/maps/{map_id}/`` и имеет свою идентичность. Считается
    тем же расчётом, что бандл прогона, по ячейкам оценщика на reference-корпусе — второй
    формулы оценки нет.
    """
    from core.pipeline.digests import baseline_facts_digest, catalog_digest
    from core.pipeline.reference_corpus import (
        coverage_from_manifest,
        iter_reference_chunks,
        read_artifact_dates,
    )
    from core.pipeline.reference_map import (
        ReferenceMapIncomplete,
        ReferenceMapLayout,
        build_reference_map,
        cell_row,
        write_map,
    )
    from core.pipeline.run_identity import prompt_template_identity

    package = _package_dir(args)
    declaration = _declaration(args)
    product = _resolve_baseline(args, declaration)
    version = str(getattr(args, "version", None) or product.current_version)
    corpus_layout = _layout(args, product, version)
    explicit_db = getattr(args, "corpus_db", None)
    if explicit_db and getattr(args, "build", None):
        # Два адреса одной сборки: молча выиграть один значило бы измерить не то, что
        # назвал оператор. Это отказ, а не приоритет.
        raise CliError(
            "--corpus-db и --build называют разные адреса корпуса: выберите один "
            "(--build — сборку раскладки, --corpus-db — чужой файл)",
            code="baseline",
            exit_code=EXIT_USAGE,
        )
    if explicit_db:
        db_path = Path(explicit_db)
        manifest = None
        if not db_path.is_file():
            raise CliError(
                f"reference-корпус не найден: {db_path} (--corpus-db). Соберите его: "
                f"{_build_hint(args, product, version)}",
                code="baseline",
                exit_code=EXIT_USAGE,
            )
    else:
        build_id = _resolve_build_id(args, corpus_layout, product, purpose="map")
        db_path = corpus_layout.db_path(build_id)
        manifest = read_manifest(corpus_layout.manifest_path(build_id))
        if not db_path.is_file():
            raise CliError(
                f"сборка {build_id} объявлена, но файла корпуса нет: {db_path}. "
                f"Пересоберите: {_build_hint(args, product, version)}",
                code="baseline",
                exit_code=EXIT_USAGE,
            )
    catalog = _catalog(package)
    facts = [fact for fact in load_package_facts(package) if fact.baseline_id == product.baseline_id]
    chunks = [
        MapChunk(
            chunk_id=chunk.chunk_id,
            doc_id=chunk.doc_id,
            url=chunk.url,
            text=chunk.text,
            content_hash=chunk.content_hash or "",
        )
        for chunk in iter_reference_chunks(db_path)
    ]
    if not chunks:
        raise CliError(
            f"reference-корпус пуст: {db_path} (0 чанков). Пересоберите: "
            f"{_build_hint(args, product, version)}",
            code="baseline",
            exit_code=EXIT_USAGE,
        )
    # Та же дисциплина, что в recon: без манифеста сборка адресуется содержимым.
    build_id = (
        manifest.build_id
        if manifest is not None
        else reference_build_id(
            product.product_id,
            version,
            reference_content_digest(
                ChunkText(
                    chunk_id=chunk.chunk_id,
                    doc_id=chunk.doc_id,
                    url=chunk.url,
                    text=chunk.text,
                    content_hash=chunk.content_hash,
                )
                for chunk in chunks
            ),
        )
    )
    evaluator = _evaluator(args)
    # Движок лексики — адаптер, и выбирает его composition root: `pipeline` не импортирует
    # `adapters` (матрица 01 §1).
    from core.adapters.lexical_bm25 import Bm25Index

    lexical_index = Bm25Index()
    # Язык запросов карты — объявленное условие (``--query-lang`` > deploy > ``all``),
    # а не порядок в коде: reference-корпус английский, и замер 19.11.2026 показал
    # ``present`` 13 при RU-запросах против 42 при EN.
    from core.cli.execution_axes import resolve_query_lang
    from core.config.deploy import load_deploy_config

    query_lang = resolve_query_lang(
        getattr(args, "query_lang", None), load_deploy_config(None)
    )
    # Шаблон промпта — **условие измерения**, и он вычисляется из того шаблона, которым
    # карта действительно будет оценивать (``имя@хеш`` содержимого), а не берётся из
    # флага. Свободный ``--prompt-template`` позволял объявить одно, а исполнить другое:
    # манифесты всех восьми карт 19.11.2026 несли ``prompt_template: null`` при реально
    # исполненном ``evaluator_v6``, то есть терм ``map_id`` был пуст, и правка шаблона
    # прошла бы под прежним адресом. Это тот же класс, что модель оценщика из окружения.
    prompt_template = prompt_template_identity()
    map_id = reference_map_id(
        reference_build_id=build_id,
        baseline_facts_digest=baseline_facts_digest(facts),
        catalog_version=catalog_digest(catalog),
        llm_scenarios={"model_id": getattr(evaluator, "model_id", None)},
        prompt_template=prompt_template,
        evaluation_policy=None,
        query_lang=query_lang,
    )
    map_layout = ReferenceMapLayout(
        root=corpus_layout.map_dir(build_id, map_id), map_id=map_id
    )
    concurrency = int(getattr(args, "llm_concurrency", 1) or 1)
    total_params = len(catalog)
    started = _dt.datetime.now(_dt.UTC)

    def _progress(done: int, total: int, param_id: str) -> None:
        """Прогресс в stderr: stdout занят результатом (``--json``), а прогон длинный."""
        elapsed = (_dt.datetime.now(_dt.UTC) - started).total_seconds()
        rate = done / elapsed if elapsed > 0 else 0.0
        left = (total - done) / rate if rate > 0 else 0.0
        sys.stderr.write(
            f"[map {done}/{total}] {param_id[:40]:40} "
            f"{elapsed:6.0f} с, осталось ≈{left / 60:.1f} мин\n"
        )
        sys.stderr.flush()

    try:
        result = build_reference_map(
            layout=map_layout,
            product_id=product.product_id,
            version=version,
            reference_build_id=build_id,
            chunks=chunks,
            params=catalog,
            evaluator=evaluator,
            lexical_index=lexical_index,
            module_weights=_module_weights(package),
            baseline_facts_digest=baseline_facts_digest(facts),
            catalog_version=catalog_digest(catalog),
            llm_scenarios={"model_id": getattr(evaluator, "model_id", None)},
            prompt_template=prompt_template,
            cutoff_k=int(getattr(args, "cutoff_k", 10) or 10),
            query_lang=query_lang,
            model_id=getattr(evaluator, "model_id", None),
            observed_at=_dt.datetime.now(_dt.UTC).replace(microsecond=0).isoformat(),
            llm_concurrency=concurrency,
            progress=_progress,
            # I2c/I2d на карте: даты наблюдений — из самой сборки, покрытие — из её
            # манифеста. Без них `❌` публиковался бы без датированного носителя.
            artifact_dates=read_artifact_dates(db_path, product.product_id),
            coverage=coverage_from_manifest(manifest, product.product_id),
        )
    except ReferenceMapIncomplete as exc:
        # Частичная карта не пишется (см. ``ReferenceMapIncomplete``): артефакт обязан
        # описывать каталог целиком либо не существовать. Ошибки уже названы.
        raise CliError(
            f"карта {map_id} не собрана: упало ячеек {len(exc.failed)} "
            f"(всего {total_params}); первая: {exc.failed[0]}",
            code="baseline",
            exit_code=EXIT_ERROR,
        ) from exc
    agreement = fact_map_agreement(
        facts, [cell_row(cell) for cell in result.cells], version=version
    )
    dry_run = bool(getattr(args, "dry_run", False))
    paths: dict[str, str] = {}
    if not dry_run:
        paths = {
            name: str(path)
            for name, path in write_map(
                map_layout, result, extra_score={"fact_map_agreement": agreement}
            ).items()
        }
    payload = {
        "package": str(package),
        "baseline_id": product.baseline_id,
        "product_id": product.product_id,
        "version": version,
        "map_id": map_id,
        "reference_build_id": build_id,
        "params": result.manifest.params,
        "counts": result.counts,
        "capability": asdict(result.bundle.capability),
        "coverage": asdict(result.bundle.coverage),
        "fact_map_agreement": agreement,
        "paths": paths,
        "dry_run": dry_run,
    }
    if args.json:
        _print_json(payload)
    else:
        lines = [
            f"map {map_id} — {product.product_id} @{version}, сборка {build_id}",
            f"параметров: {payload['params']}; "
            + ", ".join(f"{name} {count}" for name, count in sorted(payload["counts"].items())),
            f"Capability: {payload['capability']['value']:.4f} "
            f"(n_used={payload['capability']['n_used']})",
            f"Coverage: {payload['coverage']['value']} "
            f"(defined={payload['coverage']['defined']}, "
            f"universe={payload['coverage']['universe']:.0f})",
            (
                f"согласие факт↔карта: κ = {agreement['kappa']:.4f} (n={agreement['n']})"
                if agreement["kappa"] is not None
                else f"согласие факт↔карта: undefined ({agreement['undefined_reason']})"
            ),
        ]
        if dry_run:
            lines.append("dry-run: артефакты не записаны")
        else:
            for name, path in sorted(paths.items()):
                lines.append(f"  {name}: {path}")
        sys.stdout.write("\n".join(lines) + "\n")
    return EXIT_OK


# ---------------------------------------------------------------------------
# Парсер
# ---------------------------------------------------------------------------


def configure(sub: Any) -> None:
    """Регистрирует группу ``hound baseline`` (план 27 §5)."""
    parser = sub.add_parser(
        "baseline",
        help="reference-продукты профиля: сборка корпуса, факты о документации предка, "
        "разведка, ревью, refresh, карта и отчёт (план 27)",
    )
    inner = parser.add_subparsers(dest="baseline_command", metavar="ПОДКОМАНДА")

    def _package_arg(target: argparse.ArgumentParser) -> None:
        target.add_argument(
            "--package", required=True, help="каталог пакета категории (packages/{case_id})"
        )
        target.add_argument(
            "--passports-dir",
            dest="passports_dir",
            default=None,
            help="каталог паспортов (по умолчанию — не читать: роль предка тогда "
            "вычисляется по пустому набору)",
        )
        _add_common(target)

    p_build = inner.add_parser(
        "build",
        help="собрать reference-корпус версии: краул → экстракция → чанки (шаг 1 §8.1)",
    )
    _package_arg(p_build)
    p_build.add_argument("--baseline", required=True, help="baseline_id")
    p_build.add_argument(
        "--version",
        default=None,
        help="версия (по умолчанию — current_version); корни берутся из "
        "sources/reference_products.yaml:versions[].docs_roots",
    )
    p_build.add_argument(
        "--max-documents",
        dest="max_documents",
        type=_positive_int,
        default=None,
        help="потолок документов сборки (по умолчанию — без лимита)",
    )
    p_build.add_argument(
        "--max-depth",
        dest="max_depth",
        type=_non_negative_int,
        default=None,
        help=(
            "глубина обхода от корней; по умолчанию не ограничена — граница это "
            "объявленный корень (path-prefix). 0 — только сами корни (охват "
            "публикуется неполным)"
        ),
    )
    p_build.add_argument(
        "--rate-limit",
        dest="rate_limit",
        type=_rate_limit,
        default=None,
        metavar="SECONDS",
        help="задержка между запросами: число секунд либо каноническая форма "
        "uniform(min=…,max=…) (по умолчанию — политика deploy.yaml)",
    )
    p_build.add_argument(
        "--exclude-path",
        dest="exclude_paths",
        action="append",
        default=None,
        metavar="GLOB",
        help="glob блэклиста по пути URL (повторяемый)",
    )
    p_build.add_argument(
        "--document-level",
        dest="document_level",
        type=_document_level,
        default=None,
        metavar="LEVEL",
        help="уровень извлечения документов: off|text|layout|layout_tables|full "
        "(по умолчанию full)",
    )
    p_build.add_argument(
        "--pdf-pipeline",
        dest="pdf_pipeline",
        type=_pdf_pipeline,
        default=None,
        metavar="PIPELINE",
        help="механизм извлечения PDF: structured|text_layer (по умолчанию structured)",
    )
    p_build.add_argument(
        "--reference-dir",
        dest="reference_dir",
        default=None,
        help="корень reference-корпуса (по умолчанию — раскладка хоста)",
    )
    p_build.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="напечатать план (корни, лимиты, уровень, механизм) и не ходить в сеть",
    )
    p_build.set_defaults(handler=run_build)

    p_list = inner.add_parser("list", help="что объявлено и в каком состоянии")
    _package_arg(p_list)
    p_list.set_defaults(handler=run_list)

    p_facts = inner.add_parser("facts", help="факты с фильтрами (read-only)")
    _package_arg(p_facts)
    p_facts.add_argument("--baseline", default=None, help="baseline_id")
    p_facts.add_argument("--version", default=None, help="версия (как в файле)")
    p_facts.add_argument("--param", default=None, help="param_id")
    p_facts.add_argument(
        "--review",
        default=None,
        choices=[review.value for review in FactReview],
        help="фильтр по подписи",
    )
    p_facts.add_argument(
        "--status",
        default=None,
        choices=[status.value for status in FactStatus],
        help="фильтр по статусу наблюдения",
    )
    p_facts.set_defaults(handler=run_facts)

    p_report = inner.add_parser(
        "report", help="что унаследовано, кем и от какого предка (без прогона)"
    )
    _package_arg(p_report)
    p_report.add_argument(
        "--with-map",
        dest="with_map",
        action="store_true",
        help="добавить карту reference-продукта и согласие факт↔карта (§9.5)",
    )
    p_report.add_argument("--map", dest="map_id", default=None, help="map_id карты")
    p_report.add_argument(
        "--build",
        dest="build",
        default=None,
        help="build_id сборки для --with-map (по умолчанию — текущая)",
    )
    p_report.add_argument("--baseline", default=None, help="только эта линия")
    p_report.add_argument(
        "--version", default=None, help="версия reference-корпуса (по умолчанию current)"
    )
    p_report.add_argument(
        "--reference-dir",
        dest="reference_dir",
        default=None,
        help="корень reference-корпуса (по умолчанию ./reference)",
    )
    p_report.set_defaults(handler=run_report)

    p_review = inner.add_parser("review", help="подпись человека под фактом")
    _package_arg(p_review)
    p_review.add_argument("--baseline", required=True, help="baseline_id")
    p_review.add_argument("--param", default=None, help="param_id (по умолчанию — все)")
    p_review.add_argument("--version", default=None, help="версия (по умолчанию — все)")
    p_review.add_argument("--actor", required=True, help="кто подписывает (обязателен)")
    group = p_review.add_mutually_exclusive_group(required=True)
    group.add_argument("--accept", action="store_true", help="принять факт")
    group.add_argument("--reject", action="store_true", help="отклонить факт")
    group.add_argument(
        "--clear", action="store_true", help="снять подпись (вернуть в pending)"
    )
    p_review.add_argument("--note", default=None, help="комментарий к решению")
    p_review.add_argument(
        "--dry-run", dest="dry_run", action="store_true", help="ничего не писать"
    )
    p_review.set_defaults(handler=run_review)

    p_refresh = inner.add_parser(
        "refresh", help="diff фактов новой версии; min_version — предложение, не запись"
    )
    _package_arg(p_refresh)
    p_refresh.add_argument("--baseline", required=True, help="baseline_id")
    p_refresh.add_argument(
        "--version", default=None, help="версия (по умолчанию — current_version)"
    )
    p_refresh.add_argument(
        "--apply",
        dest="apply",
        action="store_true",
        help="применить added/changed к sources/baseline_facts.csv (removed не пишется)",
    )
    p_refresh.add_argument(
        "--actor", default=None, help="кто применяет (обязателен вместе с --apply)"
    )
    p_refresh.add_argument(
        "--dry-run", dest="dry_run", action="store_true", help="ничего не писать"
    )
    p_refresh.set_defaults(handler=run_refresh)

    p_recon = inner.add_parser(
        "recon", help="факты по каталогу из reference-корпуса (детерминированно)"
    )
    _package_arg(p_recon)
    p_recon.add_argument("--baseline", required=True, help="baseline_id")
    p_recon.add_argument(
        "--version", default=None, help="версия (по умолчанию — current_version)"
    )
    p_recon.add_argument("--param", default=None, help="только один param_id")
    p_recon.add_argument(
        "--build",
        dest="build",
        default=None,
        help="build_id сборки (по умолчанию — текущая из раскладки)",
    )
    p_recon.add_argument(
        "--corpus-db",
        dest="corpus_db",
        default=None,
        help="путь к БД корпуса (аварийный вход: сборка тогда адресуется содержимым)",
    )
    p_recon.add_argument(
        "--reference-dir",
        dest="reference_dir",
        default=None,
        help="корень reference-корпуса (по умолчанию — раскладка хоста)",
    )
    p_recon.add_argument(
        "--dry-run", dest="dry_run", action="store_true", help="ничего не писать"
    )
    p_recon.add_argument(
        "--all-versions",
        dest="all_versions",
        action="store_true",
        help="обойти все объявленные версии линии (у каждой своя сборка; запись одна)",
    )
    p_recon.add_argument(
        "--query-lang",
        dest="query_lang",
        choices=["ru", "en", "all"],
        default=None,
        metavar="LANG",
        help="язык терминов запроса ru|en|all (условие измерения: при `all` бюджет "
        "MAX_TERMS тратится на все языки сразу; для английского reference-корпуса — en)",
    )
    p_recon.add_argument(
        "--classifier",
        dest="classifier",
        choices=list(CLASSIFIERS),
        default=CLASSIFIER_PHRASE,
        metavar="{phrase,llm}",
        help="чем решается «описан ли параметр»: phrase — фразовый гейт (по умолчанию, "
        "без LLM), llm — оценщик (тот же, что у карты) на объединении словесных и "
        "фразовых кандидатов",
    )
    p_recon.add_argument(
        "--llm-model",
        dest="llm_model",
        default=None,
        help="объявленная модель оценщика для --classifier llm "
        "(иначе — слот deploy.yaml: llm)",
    )
    p_recon.add_argument(
        "--llm-concurrency",
        dest="llm_concurrency",
        type=_positive_int,
        default=1,
        metavar="N",
        help="сколько вызовов оценщика в полёте (по умолчанию 1; ось исполнения, "
        "не входит в идентичность)",
    )
    p_recon.set_defaults(handler=run_recon)

    p_propose = inner.add_parser(
        "propose",
        help="предложить строки baselines.csv из подписанных фактов (на разметку человеком)",
    )
    _package_arg(p_propose)
    p_propose.add_argument("--baseline", required=True, help="baseline_id")
    p_propose.add_argument(
        "--version",
        default=None,
        help="версия карты для --with-map (по умолчанию — current_version)",
    )
    p_propose.add_argument(
        "--with-map",
        dest="with_map",
        action="store_true",
        help="учесть карту версии: support_not_about_subject делает строку conditional",
    )
    p_propose.add_argument("--map", dest="map_id", default=None, help="map_id карты")
    p_propose.add_argument(
        "--build",
        dest="build",
        default=None,
        help="build_id сборки для --with-map (по умолчанию — текущая)",
    )
    p_propose.add_argument(
        "--apply",
        dest="apply",
        action="store_true",
        help="записать baselines.csv по заполненному decision (только accept)",
    )
    p_propose.add_argument(
        "--actor", default=None, help="кто применяет (обязателен вместе с --apply)"
    )
    p_propose.add_argument(
        "--dry-run", dest="dry_run", action="store_true", help="ничего не писать"
    )
    p_propose.set_defaults(handler=run_propose)

    p_map = inner.add_parser(
        "map",
        help="карта reference-продукта: Capability/Coverage по каталогу профиля",
    )
    _package_arg(p_map)
    p_map.add_argument("--baseline", required=True, help="baseline_id")
    p_map.add_argument(
        "--version", default=None, help="версия (по умолчанию — current_version)"
    )
    p_map.add_argument(
        "--build",
        dest="build",
        default=None,
        help="build_id сборки (по умолчанию — текущая из раскладки)",
    )
    p_map.add_argument(
        "--corpus-db",
        dest="corpus_db",
        default=None,
        help="путь к БД корпуса (аварийный вход: сборка тогда адресуется содержимым)",
    )
    p_map.add_argument(
        "--reference-dir",
        dest="reference_dir",
        default=None,
        help="корень reference-корпуса (по умолчанию — раскладка хоста)",
    )
    p_map.add_argument(
        "--llm-model",
        dest="llm_model",
        default=None,
        help="объявленная модель оценщика (иначе — слот deploy.yaml: llm)",
    )
    p_map.add_argument(
        "--cutoff-k",
        dest="cutoff_k",
        type=int,
        default=10,
        help="глубина ретривала карты (top-K фрагментов на параметр)",
    )
    p_map.add_argument(
        "--query-lang",
        dest="query_lang",
        choices=["ru", "en", "all"],
        default=None,
        metavar="LANG",
        help="язык запросов ретривала карты ru|en|all (условие измерения, входит в "
        "map_id; для английского reference-корпуса — en)",
    )
    p_map.add_argument(
        "--llm-concurrency",
        dest="llm_concurrency",
        type=_positive_int,
        default=1,
        help="сколько вызовов оценщика держать в полёте (1 — последовательно; ось "
        "исполнения, в map_id не входит)",
    )
    p_map.add_argument(
        "--dry-run", dest="dry_run", action="store_true", help="ничего не писать"
    )
    p_map.set_defaults(handler=run_map)

    # Без подкоманды ``handler`` — сам диспетчер: он и объяснит, чего не хватает.
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """Диспетчер группы ``hound baseline`` (без подкоманды — ошибка использования)."""
    handler = getattr(args, "handler", None)
    if handler is None or handler is run or not getattr(args, "baseline_command", None):
        raise CliError(
            "hound baseline: нужна подкоманда "
            "(build|list|facts|report|review|refresh|recon|propose|map); порядок работы — "
            "build → recon → review → propose → report → map",
            code="baseline",
            exit_code=EXIT_USAGE,
        )
    return handler(args)
