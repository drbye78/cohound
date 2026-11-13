"""``artifacts``: недоступные артефакты прогона — список и ручное предоставление (13 §4).

Группа ``hound artifacts`` — тонкая CLI-поверхность над двумя **готовыми** механизмами:
отчётом ``artifacts_gap.yaml`` (:mod:`core.pipeline.artifacts_gap`) и объявленным входом
байтов ``sources/manual_artifacts.yaml`` (:mod:`core.pipeline.manual_artifacts`). Ни один
из них здесь не переписан: команда только резолвит прогон/пакет и печатает результат.

Семантика подкоманд:

* ``list`` — какие артефакты краул не взял и почему. Источник — **БД прогона**
  (``crawl_session_items``): ``outputs/artifacts_gap.yaml`` вторичен и мог быть перезаписан
  следующим ``report``, поэтому команда его не читает, а собирает отчёт тем же генератором
  (:func:`core.pipeline.artifacts_gap.collect_artifact_gaps`). Пустой список — валидный
  ответ (``EXIT_OK``), а не ошибка.
* ``fetch`` — забрать артефакт **явно** и объявить его в ``manual_artifacts.yaml``.
  Обход robots/CAPTCHA/платных барьеров запрещён (§10), поэтому команда не «обходит
  отказ»: она делает действие человека **видимым** — ``provided_by``, ``robots_override``
  и запись в декларации, меняющей ``run_id`` следующего прогона (``execution_change``
  ``manual_artifacts_digest``).

Резолв прогона: ``--db`` > ``--run-id`` > ``--package`` (последний читаемый прогон
пакета). Это **не** :func:`core.cli.context._resolve_db_path`: там нет пакетного фолбэка,
а ``--runs-dir`` без ``--run-id`` — ошибка.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from core.adapters.crawl_http import DEFAULT_TIMEOUT, HttpCrawler
from core.cli.context import _add_common, _resolve_paths
from core.cli.errors import EXIT_ERROR, EXIT_NOT_FOUND, EXIT_OK, EXIT_USAGE, CliError
from core.cli.output import print_json
from core.cli.run_overview import effective_runs_dir, scan_runs
from core.cli.wiring import build_store, run_db_path
from core.config.deploy import load_deploy_config
from core.config.domain_profiles import load_domain_profiles
from core.config.run_config import load_run_config
from core.config.seeds import load_corpus_urls
from core.domain.enums import FailureKind
from core.pipeline.artifact_fetch import (
    FetchOutcome,
    artifact_relpath,
    fetch_artifact,
)
from core.pipeline.artifacts_gap import ARTIFACTS_GAP_FILENAME
from core.pipeline.blocked_artifacts import (
    attribute_root,
    blocked_listing,
)
from core.pipeline.crawl_frontier import resolve_crawl_limits
from core.pipeline.manual_artifacts import (
    ManualArtifactEntry,
    load_manual_artifacts,
    write_manual_artifacts,
)
from core.util.redirects import system_resolver

__all__ = ["configure", "run_fetch", "run_list"]

#: Код ошибок команды: адресует оператору объект (прогон/пакет), а не инвариант.
_ARTIFACTS_CODE = "artifacts"

#: Класс отказа по умолчанию: самый частый и самый интересный оператору (robots).
_DEFAULT_KIND = "robots_disallow"

#: Размер порции потокового чтения при переиспользовании уже скачанного файла (1 MiB).
_READ_CHUNK = 1024 * 1024


# ---------------------------------------------------------------------------
# Резолв прогона/пакета
# ---------------------------------------------------------------------------


def _runs_dir(args: argparse.Namespace) -> Path:
    """Эффективный каталог прогонов: ``--runs-dir`` > раскладка > ``runs``."""
    return Path(effective_runs_dir(args))


def _paths(args: argparse.Namespace) -> Any:
    """Раскладка из composition root либо её резолв (прямой вызов хендлера)."""
    paths = getattr(args, "paths", None)
    return paths if paths is not None else _resolve_paths(args)


def _package_dir(args: argparse.Namespace) -> Path | None:
    """Каталог пакета из ``--package`` (путь **или** ``case_id``); ``None`` — флага нет.

    :raises CliError: каталог не найден ни как путь, ни под ``paths.packages`` —
        явный флаг, указывающий в никуда, ошибка входа, а не «пакет без корней».
    """
    raw = getattr(args, "package", None)
    if not raw:
        return None
    candidate = Path(str(raw))
    if candidate.is_dir():
        return candidate
    fallback = Path(_paths(args).packages) / str(raw)
    if fallback.is_dir():
        return fallback
    raise CliError(
        f"каталог пакета не найден: {raw} (ни как путь, ни как {fallback})",
        code=_ARTIFACTS_CODE,
        exit_code=EXIT_USAGE,
    )


def _require_db(path: Path) -> Path:
    """Проверяет наличие БД прогона до открытия (сообщение с путём, а не ``StoreNotInitialized``)."""
    if not path.is_file():
        raise CliError(
            f"БД прогона не найдена: {path}",
            code="not_found",
            exit_code=EXIT_NOT_FOUND,
        )
    return path


def _read_run_id(db_path: Path, *, profile: str) -> str:
    """``run_id`` из БД (``--db`` без ``--run-id``): последний записанный прогон.

    ``run_id`` нужен для имени сеанса краула (``crawl:{run_id}``), поэтому выдумывать его
    из имени каталога нельзя: в общей ``--db`` каталог и прогон — разные вещи.
    """
    with build_store(db_path, profile=profile, read_only=True) as store:
        rows = store.conn.execute(
            "SELECT run_id FROM runs ORDER BY created_at DESC, run_id DESC"
        ).fetchall()
    if not rows:
        raise CliError(
            f"в БД нет ни одного прогона: {db_path}",
            code=_ARTIFACTS_CODE,
            exit_code=EXIT_USAGE,
        )
    return str(rows[0]["run_id"])


@dataclass(frozen=True)
class _RunTarget:
    """Разрешённый адрес прогона: ``run_id``, БД и (если известен) каталог пакета."""

    run_id: str
    db_path: Path
    package_dir: Path | None


def _run_target_for_package(args: argparse.Namespace, package_dir: Path) -> _RunTarget:
    """Последний **читаемый** прогон пакета (``--package`` без ``--run-id``/``--db``).

    Нечитаемые БД (чужая схема, не-SQLite) в выбор не идут: их ``skipped_reason`` — это
    не «прогон пакета», а отказ чтения. Пустой выбор называется числом просмотренных
    прогонов, а не «ничего не найдено».
    """
    runs_dir = _runs_dir(args)
    package_id = package_dir.name
    summaries = scan_runs(runs_dir)
    readable = [
        summary
        for summary in summaries
        if summary.package_id == package_id and summary.skipped_reason is None
    ]
    if not readable:
        raise CliError(
            f"в {runs_dir} нет читаемых прогонов пакета {package_id!r} "
            f"(просмотрено прогонов: {len(summaries)})",
            code=_ARTIFACTS_CODE,
            exit_code=EXIT_USAGE,
        )
    best = max(readable, key=lambda summary: (summary.created_at or "", summary.run_id))
    return _RunTarget(
        run_id=best.run_id, db_path=Path(best.db), package_dir=package_dir
    )


def _resolve_run_target(args: argparse.Namespace) -> _RunTarget:
    """Резолвит прогон: ``--db`` > ``--run-id`` > ``--package``.

    :raises CliError: не задан ни один адрес (:data:`EXIT_USAGE`); БД не найдена
        (:data:`EXIT_NOT_FOUND`); у пакета нет читаемых прогонов (:data:`EXIT_USAGE`).
    """
    package_dir = _package_dir(args)

    db_arg = getattr(args, "db", None)
    if db_arg is not None:
        db_path = _require_db(Path(str(db_arg)))
        run_id = getattr(args, "run_id", None)
        if run_id is None:
            run_id = _read_run_id(db_path, profile=args.profile)
        return _RunTarget(
            run_id=str(run_id), db_path=db_path, package_dir=package_dir
        )

    run_id = getattr(args, "run_id", None)
    if run_id:
        db_path = _require_db(Path(run_db_path(_runs_dir(args), str(run_id))))
        return _RunTarget(
            run_id=str(run_id), db_path=db_path, package_dir=package_dir
        )

    if package_dir is not None:
        return _run_target_for_package(args, package_dir)

    raise CliError(
        "нужен --run-id, --db или --package",
        code="usage",
        exit_code=EXIT_USAGE,
    )


def _package_id_from_store(store: Any, run_id: str) -> str | None:
    """``package_id`` прогона из снимка плановых входов (``None`` — снимка нет)."""
    try:
        raw = store.read_plan_inputs(run_id)
    except Exception:  # noqa: BLE001 — снимок входов не обязателен для чтения элементов
        return None
    if isinstance(raw, Mapping):
        value = raw.get("package_id")
        if isinstance(value, str) and value:
            return value
    return None


def _package_dir_from_id(args: argparse.Namespace, package_id: str | None, run_id: str) -> Path:
    """Каталог пакета по ``package_id`` прогона (``paths.packages`` — операционная раскладка).

    :raises CliError: идентичность пакета неизвестна либо каталога нет — запись
        декларации в несуществующий пакет была бы тихой потерей (13 §3.1).
    """
    if not package_id:
        raise CliError(
            f"пакет прогона {run_id} неизвестен: в БД нет снимка плановых входов — "
            "укажите --package",
            code=_ARTIFACTS_CODE,
            exit_code=EXIT_USAGE,
        )
    candidate = Path(_paths(args).packages) / package_id
    if not candidate.is_dir():
        raise CliError(
            f"каталог пакета не найден: {candidate}",
            code=_ARTIFACTS_CODE,
            exit_code=EXIT_USAGE,
        )
    return candidate


# ---------------------------------------------------------------------------
# Фильтры
# ---------------------------------------------------------------------------


def _filters(args: argparse.Namespace) -> dict[str, Any]:
    """Фильтры CLI → именованные аргументы ``blocked_listing`` (пустое — без фильтра)."""
    return {
        "products": tuple(getattr(args, "product", None) or ()),
        "roots": tuple(getattr(args, "root", None) or ()),
        "paths": tuple(getattr(args, "path", None) or ()),
        "masks": tuple(getattr(args, "mask", None) or ()),
    }


def _kinds(args: argparse.Namespace) -> tuple[str, ...]:
    """Классы отказа для ``list``: объявленные оператором либо ``robots_disallow``.

    ``gaps`` уже содержит **только** блокирующие классы, поэтому ``--kind`` — сужение
    ответа, а не фильтр безопасности. Неизвестное значение — ошибка входа: молча пустой
    список читался бы как «таких отказов нет».
    """
    raw = tuple(getattr(args, "kind", None) or ())
    if not raw:
        return (_DEFAULT_KIND,)
    valid = {kind.value for kind in FailureKind}
    unknown = [value for value in raw if value not in valid]
    if unknown:
        raise CliError(
            f"неизвестный --kind: {', '.join(unknown)} "
            f"(допустимо: {', '.join(sorted(valid))})",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    return raw


def _roots_by_product(package_dir: Path | None) -> dict[str, list[str]]:
    """Корни продукта из ``sources/corpus_urls.yaml``; пакета нет — корней нет."""
    if package_dir is None:
        return {}
    return load_corpus_urls(package_dir / "sources" / "corpus_urls.yaml")


def _provided_pairs(package_dir: Path | None) -> set[tuple[str, str]] | None:
    """``(product_id, url)`` уже объявленных подач; ``None`` — пакет неизвестен.

    Декларация читается тем же загрузчиком, что и прогон: сломанная декларация —
    видимая ошибка (``EXIT_USAGE``), а не «подач нет».
    """
    if package_dir is None:
        return None
    declared = load_manual_artifacts(package_dir)
    return {(entry.product_id, entry.url) for entry in declared.entries}


def _counts(values: Sequence[str]) -> dict[str, int]:
    """``value → count`` с сортированными ключами (детерминизм вывода)."""
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return {key: counts[key] for key in sorted(counts)}


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def run_list(args: argparse.Namespace) -> int:
    """``artifacts list``: блокирующие отказы прогона по фильтрам (``EXIT_OK`` и на пустом).

    :returns: :data:`EXIT_OK` — список (в том числе пустой) это ответ, а не ошибка.
    :raises CliError: прогон не резолвится/БД нет; неизвестный ``--kind``; сломанная
        декларация пакета.
    """
    target = _resolve_run_target(args)
    roots_by_product = _roots_by_product(target.package_dir)
    provided = _provided_pairs(target.package_dir)
    kinds = _kinds(args)
    with build_store(target.db_path, profile=args.profile, read_only=True) as store:
        selected, summary = blocked_listing(
            store, target.run_id, kinds=kinds, **_filters(args)
        )

    records: list[dict[str, Any]] = []
    for artifact in selected:
        roots = roots_by_product.get(artifact.product_id, [])
        records.append(
            {
                "product_id": artifact.product_id,
                "url": artifact.url,
                "kind": artifact.kind,
                "host": _host_of(artifact.url),
                "path": _path_of(artifact.url),
                "root": attribute_root(artifact.url, roots) if roots else None,
                "depth": artifact.depth,
                "http_status": artifact.http_status,
                "detail": artifact.detail,
                "provided": (
                    None
                    if provided is None
                    else (artifact.product_id, artifact.url) in provided
                ),
            }
        )

    gap_file = target.db_path.parent / "outputs" / ARTIFACTS_GAP_FILENAME
    if args.json:
        print_json(
            {
                "ok": True,
                "run_id": target.run_id,
                "db": str(target.db_path),
                "package": (
                    target.package_dir.name if target.package_dir is not None else None
                ),
                "package_dir": (
                    str(target.package_dir) if target.package_dir is not None else None
                ),
                "count": len(records),
                "items": records,
                "summary": summary,
            }
        )
        return EXIT_OK

    out = sys.stdout
    for record in records:
        out.write(
            f"{record['product_id']:22} {record['kind']:16} {record['url']}\n"
        )
    by_product = _counts([str(record["product_id"]) for record in records])
    summary_line = f"всего: {len(records)}"
    if by_product:
        summary_line += "; по продуктам: " + ", ".join(
            f"{key}={value}" for key, value in by_product.items()
        )
    out.write(summary_line + "\n")
    excluded = summary.get("excluded_by_kind") or {}
    if isinstance(excluded, Mapping) and excluded:
        out.write(
            "не входят (не блокирующие): "
            + ", ".join(f"{kind}={count}" for kind, count in excluded.items())
            + "\n"
        )
    if records:
        out.write(
            f"скачать: hound artifacts fetch --run-id {target.run_id} "
            "--mask '*.pdf' --actor <кто>\n"
        )
    out.write(
        f"отчёт: {gap_file} (БД первична, файл мог быть перезаписан)\n"
    )
    return EXIT_OK


# ---------------------------------------------------------------------------
# fetch
# ---------------------------------------------------------------------------


def _explicit_pairs(
    urls: Sequence[str], products: Sequence[str]
) -> list[tuple[str, str]]:
    """Пары ``(product_id, url)`` для явных ``--url``.

    Один продукт распространяется на все URL; иначе списки обязаны совпадать по длине —
    угадывать принадлежность URL продукту по домену здесь нельзя (это решение человека).

    :raises CliError: ``--url`` без соразмерного ``--product`` (:data:`EXIT_USAGE`).
    """
    if not urls:
        return []
    if len(products) == 1:
        return [(products[0], url) for url in urls]
    if len(products) == len(urls):
        return list(zip(products, urls, strict=True))
    raise CliError(
        "--url требует --product: один продукт на все URL либо список той же длины "
        f"(url: {len(urls)}, продуктов: {len(products)})",
        code="usage",
        exit_code=EXIT_USAGE,
    )


def _sha256_file(path: Path) -> tuple[str, int]:
    """``(sha256, size)`` файла потоково — байты не поднимаются в RAM целиком."""
    hasher = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(_READ_CHUNK), b""):
            hasher.update(chunk)
            size += len(chunk)
    return hasher.hexdigest(), size


def _reused_result(product_id: str, url: str, declared: str, path: Path) -> FetchOutcome:
    """Результат «файл уже на месте»: сети не было, хеш пересчитан из байтов.

    Переиспользование, а не повторная загрузка: ``artifact_relpath`` детерминирован по
    ``(product_id, url)``, поэтому файл по этому пути — тот же артефакт. ``--force``
    существует ровно для случая «перекачать, даже если файл уже есть».
    """
    digest, size = _sha256_file(path)
    return FetchOutcome(
        product_id=product_id,
        requested_url=url,
        final_url=None,
        status="unchanged",
        reason="already_present",
        declared_path=declared,
        sha256=digest,
        size=size,
        content_type=None,
        status_code=None,
    )


def _fetch_one(
    product_id: str,
    url: str,
    *,
    package_dir: Path,
    crawler: Any,
    limits_for: Any,
    staging_dir: Path,
    profiles: Any,
    roots: Sequence[str],
    force: bool,
    dry_run: bool,
) -> FetchOutcome:
    """Один артефакт: переиспользование существующего файла либо ``fetch_artifact``.

    ``dry_run`` уходит в ядро: там байты скачиваются по-настоящему (иначе хеш и размер
    были бы выдуманы), но в пакет не пишутся — исход ``planned``.
    """
    declared = artifact_relpath(product_id, url)
    existing = package_dir / declared
    if not force and not dry_run and existing.is_file():
        return _reused_result(product_id, url, declared, existing)
    return fetch_artifact(
        package_dir=package_dir,
        product_id=product_id,
        url=url,
        crawler=crawler,
        limits_for=limits_for,
        staging_dir=staging_dir,
        roots=roots,
        domain_profiles=profiles,
        dry_run=dry_run,
    )


def _is_ok(result: FetchOutcome) -> bool:
    """Исход считается успешным: файл записан, уже был или запланирован (``dry_run``)."""
    return result.status in ("fetched", "unchanged", "planned")


def _host_of(url: str) -> str:
    """Хост URL — то же поле, что в сводке ``blocked_summary`` (``www.`` не сворачивается)."""
    return (urlsplit(url).netloc or "").lower()


def _path_of(url: str) -> str:
    """Сырой путь URL (как его видит оператор в БД); пустой — ``/``."""
    return urlsplit(url).path or "/"


def _result_record(result: FetchOutcome, *, max_bytes: int | None) -> dict[str, Any]:
    """JSON-запись одного артефакта (порядок ключей не важен: ``print_json`` сортирует)."""
    return {
        "product_id": result.product_id,
        "url": result.requested_url,
        "status": result.status,
        "ok": _is_ok(result),
        "reason": result.reason,
        "final_url": result.final_url,
        "declared_path": result.declared_path,
        "sha256": result.sha256,
        "size": result.size,
        "max_bytes": max_bytes,
        "content_type": result.content_type,
        "http_status": result.status_code,
    }


def run_fetch(args: argparse.Namespace) -> int:
    """``artifacts fetch``: скачать выбранные артефакты и объявить их в пакете.

    :returns: :data:`EXIT_OK` — все артефакты получены; :data:`EXIT_ERROR` — есть отказы.
    :raises CliError: пустой ``--actor``, нечего скачивать, пакет/корни/декларация не
        резолвятся (все — :data:`EXIT_USAGE`).
    """
    actor = str(getattr(args, "actor", "") or "").strip()
    if not actor:
        raise CliError(
            "--actor обязателен: предоставление артефакта подписывается человеком "
            "(13 §9), пустое значение — не подпись",
            code="usage",
            exit_code=EXIT_USAGE,
        )

    urls = tuple(dict.fromkeys(getattr(args, "url", None) or ()))
    products = tuple(dict.fromkeys(getattr(args, "product", None) or ()))
    explicit = _explicit_pairs(urls, products)
    # Прогон нужен, когда адрес прогона назван явно либо набор берётся из отказов.
    # ``--package`` + ``--url`` — законная пара: пакет здесь место записи, а не адрес
    # прогона (иначе «скачать один известный файл» требовал бы существующего прогона).
    has_run = bool(getattr(args, "run_id", None) or getattr(args, "db", None)) or (
        not urls and getattr(args, "package", None) is not None
    )

    target = _resolve_run_target(args) if has_run else None
    pairs: dict[tuple[str, str], None] = {}
    package_id: str | None = None
    if target is not None:
        with build_store(target.db_path, profile=args.profile, read_only=True) as store:
            # Явные ``--url`` и набор из отказов **не складываются**: оператор, назвавший
            # URL, просит ровно его. Иначе ``--url X --product p`` скачал бы весь продукт
            # p (``--product`` — фильтр листинга), и это было бы тихой подменой запроса.
            # Класс отбора — тот же дефолт, что у ``list`` (``robots_disallow``): иначе
            # «скачать отказы продукта» тянуло бы и ``not_found``/``source_unavailable``
            # — адреса, которые документом не станут. Явный ``--kind`` расширяет набор.
            selected = (
                []
                if urls
                else blocked_listing(
                    store, target.run_id, kinds=_kinds(args), **_filters(args)
                )[0]
            )
            package_id = _package_id_from_store(store, target.run_id)
        # ``--kind`` здесь намеренно не применяется: оператор, назвавший маску/URL,
        # просит скачать именно это, а не «только блокирующий класс».
        for artifact in selected:
            pairs[(artifact.product_id, artifact.url)] = None
    for product_id, url in explicit:
        pairs[(product_id, url)] = None
    if not pairs:
        raise CliError(
            "нечего скачивать: фильтры не выбрали ни одного артефакта",
            code="usage",
            exit_code=EXIT_USAGE,
        )

    if target is not None:
        package_dir = target.package_dir
        if package_dir is None:
            package_dir = _package_dir_from_id(args, package_id, target.run_id)
    else:
        package_dir = _package_dir(args)
        if package_dir is None:
            raise CliError(
                "fetch без прогона требует --package: декларация пишется в пакет (13 §3.1)",
                code="usage",
                exit_code=EXIT_USAGE,
            )

    roots_path = package_dir / "sources" / "corpus_urls.yaml"
    if not roots_path.is_file():
        raise CliError(
            f"нет {roots_path}: корни продуктов объявлены в sources/corpus_urls.yaml",
            code=_ARTIFACTS_CODE,
            exit_code=EXIT_USAGE,
        )
    roots_by_product = load_corpus_urls(roots_path)
    profiles_path = package_dir / "domain_profiles.yaml"
    profiles = load_domain_profiles(profiles_path) if profiles_path.is_file() else {}

    deploy = load_deploy_config(None)
    crawl_app = deploy.crawl
    run_crawl = None
    run_yaml = package_dir / "run.yaml"
    if run_yaml.is_file():
        run_crawl = load_run_config(run_yaml).crawl
    # RobotsGate не передаётся намеренно: команда — действие человека, а не краул, и
    # молчаливый обход robots адаптером не должен происходить «сам» (13 §10).
    crawler = HttpCrawler(
        resolver=system_resolver,
        user_agent=crawl_app.user_agent,
        timeout=(
            crawl_app.timeout_s if crawl_app.timeout_s is not None else DEFAULT_TIMEOUT
        ),
        max_response_bytes=crawl_app.max_response_bytes,
    )

    def _limits_for(url: str) -> Any:
        return resolve_crawl_limits(
            url, app=crawl_app, run=run_crawl, profiles=profiles
        )

    reason = str(getattr(args, "reason", None) or _DEFAULT_KIND)
    dry_run = bool(getattr(args, "dry_run", False))
    force = bool(getattr(args, "force", False))
    provided_at = datetime.now(UTC).isoformat()

    results: list[FetchOutcome] = []
    entries: list[ManualArtifactEntry] = []
    # Staging-каталог нужен ядру всегда: терминальный артефакт приходит файлом, а не
    # телом в RAM. Живёт только на время команды.
    with tempfile.TemporaryDirectory(prefix="cohound-artifacts-") as tmp:
        staging_dir = Path(tmp)
        for product_id, url in pairs:
            results.append(
                _fetch_one(
                    product_id,
                    url,
                    package_dir=package_dir,
                    crawler=crawler,
                    limits_for=_limits_for,
                    staging_dir=staging_dir,
                    profiles=profiles,
                    roots=roots_by_product.get(product_id, ()),
                    force=force,
                    dry_run=dry_run,
                )
            )
        # Уже объявленная подача не переписывается: повторный запуск не должен менять
        # ``provided_at`` (иначе «ничего не изменилось» выглядело бы правкой аудита).
        declared_before = _provided_pairs(package_dir) or set()
        entries = [
            ManualArtifactEntry(
                product_id=result.product_id,
                url=result.requested_url,
                path=str(result.declared_path),
                sha256=result.sha256,
                reason=reason,
                provided_by=actor,
                provided_at=provided_at,
                robots_override=True,
            )
            for result in results
            if _is_ok(result)
            and result.declared_path
            and (result.product_id, result.requested_url) not in declared_before
        ]
        digest_before = load_manual_artifacts(package_dir).digest()
        declaration_path, changed = write_manual_artifacts(
            package_dir, entries, dry_run=dry_run
        )
        # Материал дайджеста — ``(product_id, url, sha256)``: только его смена даёт
        # ``execution_change``. Правка провенанса (``provided_by``/``provided_at``)
        # переписывает файл, но ``run_id`` не двигает — и говорить обратное нельзя.
        digest_changed = (
            digest_before != load_manual_artifacts(package_dir).digest()
            and not dry_run
        )

    fetched = sum(1 for result in results if _is_ok(result))
    failed = len(results) - fetched
    total_bytes = sum(int(result.size or 0) for result in results if _is_ok(result))
    by_reason = _counts(
        [
            str(result.reason or ("ok" if _is_ok(result) else "unknown"))
            for result in results
        ]
    )

    if args.json:
        print_json(
            {
                "ok": failed == 0,
                "run_id": target.run_id if target is not None else None,
                "package": package_dir.name,
                "package_dir": str(package_dir),
                "dry_run": dry_run,
                "items": [
                    _result_record(
                        result, max_bytes=_limits_for(result.requested_url).max_artifact_bytes
                    )
                    for result in results
                ],
                "summary": {
                    "fetched": fetched,
                    "failed": failed,
                    "bytes": total_bytes,
                    "by_reason": by_reason,
                },
                "declaration": {
                    "path": str(declaration_path),
                    "changed": int(changed),
                    "digest_changed": bool(digest_changed),
                },
            }
        )
    else:
        out = sys.stdout
        for result in results:
            tail = result.declared_path or f"{result.reason or 'error'}".rstrip()
            out.write(
                f"{result.status:9} {result.product_id:22} "
                f"{result.requested_url} -> {tail}\n"
            )
        verb = "запланировано" if dry_run else "скачано"
        out.write(
            f"{verb}: {fetched}, пропущено/отказов: {failed}, байт: {total_bytes}\n"
        )
        if dry_run:
            out.write("dry-run: ничего не записано\n")
        else:
            out.write(
                f"декларация: {declaration_path} (записей изменено: {int(changed)})\n"
            )
            if digest_changed:
                out.write(
                    "декларация изменена ⇒ следующий прогон получит новый run_id "
                    "(execution_change: manual_artifacts_digest)\n"
                )
            elif changed:
                out.write(
                    "декларация переписана, но материал (product_id, url, sha256) не "
                    "изменился ⇒ run_id прежний\n"
                )

    return EXIT_OK if failed == 0 else EXIT_ERROR


# ---------------------------------------------------------------------------
# Регистрация
# ---------------------------------------------------------------------------


def _no_subcommand(args: argparse.Namespace) -> int:
    """``hound artifacts`` без подкоманды: понятная ошибка использования (``EXIT_USAGE``)."""
    raise CliError(
        "не указана подкоманда artifacts (list|fetch)",
        code="usage",
        exit_code=EXIT_USAGE,
    )


def _add_run_flags(parser: argparse.ArgumentParser) -> None:
    """Адрес прогона: ``--db`` > ``--run-id`` > ``--package`` (последний прогон пакета)."""
    parser.add_argument("--run-id", default=None, help="run_id прогона")
    parser.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (дефолт — раскладка paths.runs)",
    )
    parser.add_argument(
        "--db", default=None, help="путь к cohound.db прогона (приоритет над --run-id)"
    )
    parser.add_argument(
        "--package",
        default=None,
        help="каталог пакета или case_id: корни продуктов и декларация подач",
    )


def _add_filter_flags(parser: argparse.ArgumentParser) -> None:
    """Отбор артефактов: продукт/корень/путь внутри корня/маска URL (все — повторяемые)."""
    parser.add_argument(
        "--product", action="append", default=None, help="продукт (можно несколько)"
    )
    parser.add_argument(
        "--root", action="append", default=None, help="URL объявленного корня"
    )
    parser.add_argument(
        "--path", action="append", default=None, help="путь внутри корня (префикс)"
    )
    parser.add_argument(
        "--mask", action="append", default=None, help="маска URL, например '*.pdf'"
    )


def configure(sub: Any) -> None:
    """Регистрирует группу ``artifacts`` с подкомандами ``list`` и ``fetch``."""
    p_artifacts = sub.add_parser(
        "artifacts", help="недоступные артефакты прогона: list | fetch"
    )
    actions = p_artifacts.add_subparsers(
        dest="artifacts_command", metavar="ДЕЙСТВИЕ"
    )

    p_list = actions.add_parser(
        "list", help="какие артефакты краул не взял (источник — БД прогона)"
    )
    _add_run_flags(p_list)
    _add_filter_flags(p_list)
    p_list.add_argument(
        "--kind",
        action="append",
        default=None,
        help=(
            "класс отказа (можно несколько; дефолт "
            f"{_DEFAULT_KIND}); сужает ответ, а не отбор"
        ),
    )
    _add_common(p_list)
    p_list.set_defaults(handler=run_list)

    p_fetch = actions.add_parser(
        "fetch",
        help="скачать артефакт явно и объявить его в sources/manual_artifacts.yaml",
    )
    _add_run_flags(p_fetch)
    _add_filter_flags(p_fetch)
    p_fetch.add_argument(
        "--actor",
        required=True,
        help="кто предоставил артефакт (обязателен: попадает в provided_by)",
    )
    p_fetch.add_argument(
        "--reason",
        default=_DEFAULT_KIND,
        help=f"почему краул не смог (дефолт {_DEFAULT_KIND})",
    )
    p_fetch.add_argument(
        "--url",
        action="append",
        default=None,
        help=(
            "явный URL (требует --product: один продукт или список той же длины); "
            "названные URL скачиваются вместо отбора из отказов, а не вместе с ним"
        ),
    )
    p_fetch.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="скачать во временный каталог, ничего не записывая в пакет",
    )
    p_fetch.add_argument(
        "--force",
        action="store_true",
        help="перекачать, даже если файл уже есть и хеш совпал",
    )
    _add_common(p_fetch)
    p_fetch.set_defaults(handler=run_fetch)

    p_artifacts.set_defaults(handler=_no_subcommand)
