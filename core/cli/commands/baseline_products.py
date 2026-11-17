"""coHound — CLI reference-продуктов профиля: ``hound baseline …`` (план 27 §5).

Группа ``hound baseline`` **отдельно** от ``hound package``: там — авторские CRUD над
пакетом, здесь — измерение (декларация reference-продуктов, факты о документации предка,
разведка и отчёт). Ни одна подкоманда не пишет в ``runs/`` и не трогает прогон: факты и
декларация живут в пакете, reference-корпус — в ``COHOUND_HOME/reference/…``.

Правила, общие для всех подкоманд (§5):

* ``--actor`` обязателен для любой записи — подпись без автора недоказуема;
* ``--dry-run`` ничего не пишет и **говорит**, что записал бы;
* запись идёт через ``.authoring/apply_log.jsonl`` (существующий контур I1).

Нейтрален к предметной категории (§1.3): ни одного названия продукта или вендора.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

from core.cli.context import _add_common, _print_json
from core.cli.errors import EXIT_OK, EXIT_USAGE, CliError
from core.config.run_config import (
    load_baselines,
    load_catalog,
    load_package_manifest,
)
from core.domain.enums import FactReview, FactStatus
from core.pipeline.reference_corpus import (
    ChunkText,
    ReferenceManifest,
    derive_facts,
    iter_reference_chunks,
    read_manifest,
    reference_build_id,
    reference_content_digest,
    write_manifest,
)
from core.pipeline.reference_map import (
    MapChunk,
    fact_map_agreement,
    reference_map_id,
)
from core.pipeline.reference_products import (
    baseline_roles,
    fact_rows,
    inheritance_report,
    load_package_facts,
    load_package_passports,
    load_package_reference_products,
    refresh_diff,
    review_facts,
    write_facts,
)

__all__ = ["configure", "run"]


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


def _reference_dir(args: argparse.Namespace, product_id: str, version: str) -> Path:
    """Каталог reference-корпуса: ``--reference-dir`` > раскладка (``args.paths``).

    Раскладку разрешает composition root (03 §A.1): ``reference`` — обычное поле
    ``HARNESS_FIELDS``, поэтому ``COHOUND_HOME``/``COHOUND_REFERENCE_DIR``/``paths.yaml``
    действуют на него так же, как на ``runs``/``index_store``. Собственный дефолт от CWD
    здесь был бы вторым резолвом раскладки и разошёлся бы с остальными командами.
    """
    root = getattr(args, "reference_dir", None)
    paths = getattr(args, "paths", None)
    base = Path(root) if root else (
        Path(paths.reference) if paths is not None else Path("reference")
    )
    package_id = _package_dir(args).name
    return base / package_id / product_id / version


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
# list
# ---------------------------------------------------------------------------


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
    payload: dict[str, Any] = {
        "package": str(package),
        "declared": declaration is not None,
        "products": roles,
        "baselines_rows": len(baselines),
        "facts": len(facts),
        "cohort_products": cohort_products,
    }
    return payload


def _print_list(payload: dict[str, Any]) -> None:
    lines: list[str] = []
    if not payload["declared"]:
        lines.append(
            "reference_products: не объявлены (sources/reference_products.yaml отсутствует)"
        )
    products_payload: dict[str, dict[str, Any]] = dict(payload["products"])
    for baseline_id, data in sorted(products_payload.items()):
        role = data["role"]
        who = (
            "ancestor of: " + ", ".join(data["ancestors_of"])
            if data["ancestors_of"]
            else "parallel (ни один продукт когорты не объявляет его предком)"
        )
        lines.append(
            f"{baseline_id:<16} {data['product_id']:<16} current {data['current_version']:<6} "
            f"versions [{', '.join(data['versions'])}]  {who}"
            if role == "ancestor"
            else f"{baseline_id:<16} {data['product_id']:<16} {who}"
        )
    lines.append(
        f"baselines.csv: {payload['baselines_rows']} строк; "
        f"baseline_facts.csv: {payload['facts']} фактов"
    )
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


def _map_dir(args: argparse.Namespace, product: Any, version: str) -> Path:
    """Каталог ``maps/`` версии reference-корпуса (раскладка — та же, что у ``map``)."""
    return _reference_dir(args, product.product_id, version) / "maps"


def _pick_map(args: argparse.Namespace, maps_dir: Path) -> Path | None:
    """Выбирает карту: явный ``--map`` либо **единственную** имеющуюся.

    Несколько карт и нет ``--map`` — отказ с их списком, а не «последняя по mtime»:
    выбор измерения по времени файла выдал бы одну карту за другую (A3 — не угадывать).
    """
    explicit = getattr(args, "map_id", None)
    if explicit:
        path = maps_dir / explicit
        if not path.is_dir():
            raise CliError(
                f"карта не найдена: {path}", code="baseline", exit_code=EXIT_USAGE
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
            f"карт несколько ({names}): укажите --map, чтобы не угадывать измерение",
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
                    "note": "карты нет: `hound baseline map --baseline …`",
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
        if agreement["kappa"] is not None:
            lines.append(
                f"  согласие факт↔карта: κ = {agreement['kappa']:.4f} "
                f"(n={agreement['n']}), расхождений {len(agreement['disagreements'])}"
                f" (факт present, карта ❌: {agreement['fact_present_map_absent']})"
            )
        else:
            lines.append(
                f"  согласие факт↔карта: undefined ({agreement['undefined_reason']})"
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
    payload = {
        "package": str(package),
        "baseline_id": product.baseline_id,
        "version": version,
        "previous_versions": sorted({fact.version for fact in previous}),
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
            f"[{', '.join(payload['previous_versions']) or '—'}]",
        ]
        for kind in ("added", "removed", "changed", "unchanged", "unclear"):
            items = diff[kind]
            lines.append(f"  {kind}: {len(items)}")
            for item in items[:20]:
                lines.append(f"    {item['param_id']}")
            if len(items) > 20:
                lines.append(f"    … ещё {len(items) - 20}")
        if payload["proposed_min_version"]:
            lines.append(
                "предложение min_version="
                f"{version} для: {', '.join(payload['proposed_min_version'][:20])}"
            )
        lines.append("removed не применяется: регрессия/переезд/дефект различает человек")
        sys.stdout.write("\n".join(lines) + "\n")
    return EXIT_OK


# ---------------------------------------------------------------------------
# recon
# ---------------------------------------------------------------------------


def run_recon(args: argparse.Namespace) -> int:
    """``hound baseline recon``: факты по каталогу из reference-корпуса (шаг 7a плана).

    Разведка **детерминированная**: кандидат — чанк, где встречается термин правила;
    цитата обязана быть дословной подстрокой текста чанка. LLM здесь не участвует, и это
    осознанно: факт — провенанс, а провенанс не должен зависеть от модели.

    ``absent`` не выводится: проход по объявленному корпусу не аттестует **полноту**
    охвата (I2/I2c), поэтому «не нашли» публикуется как ``unclear`` с причиной
    ``coverage_incomplete``. Аттестация охвата — отдельное человеческое действие.
    """
    package = _package_dir(args)
    declaration = _declaration(args)
    product = _resolve_baseline(args, declaration)
    version = str(getattr(args, "version", None) or product.current_version)
    reference_dir = _reference_dir(args, product.product_id, version)
    db_path = Path(getattr(args, "corpus_db", None) or reference_dir / "reference.db")
    if not db_path.is_file():
        raise CliError(
            f"reference-корпус не найден: {db_path}. Соберите его (crawl + extract + "
            "chunk в reference.db) либо укажите --corpus-db",
            code="baseline",
            exit_code=EXIT_USAGE,
        )
    manifest = read_manifest(reference_dir / "manifest.json")
    catalog = _catalog(package)
    only = getattr(args, "param", None)
    if only:
        catalog = [row for row in catalog if str(row.param_id) == only]
        if not catalog:
            raise CliError(
                f"param {only!r} отсутствует в каталоге", code="baseline", exit_code=EXIT_USAGE
            )
    chunks = list(iter_reference_chunks(db_path))
    if not chunks:
        raise CliError(
            f"reference-корпус пуст: {db_path} (0 чанков)", code="baseline", exit_code=EXIT_USAGE
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
    observed_at = _dt.datetime.now(_dt.UTC).replace(microsecond=0).isoformat()
    facts = derive_facts(
        baseline_id=product.baseline_id,
        version=version,
        chunks=chunks,
        params=catalog,
        corpus_build_id=build_id,
        observed_at=observed_at,
    )
    dry_run = bool(getattr(args, "dry_run", False))
    merged = [
        fact
        for fact in load_package_facts(package)
        if not (
            fact.baseline_id == product.baseline_id
            and fact.version == version
            and any(item.param_id == fact.param_id for item in facts)
        )
    ]
    written = False
    if not dry_run:
        write_facts(package, [*merged, *facts])
        # Манифест сборки фиксирует, чем прочитан текст: без него «откуда цитата»
        # отвечалось бы догадкой (версии читателя/чанкера — провенанс, не identity).
        write_manifest(
            reference_dir / "manifest.json",
            ReferenceManifest(
                schema="reference-corpus/1",
                build_id=build_id,
                package_id=package.name,
                product_id=product.product_id,
                version=version,
                content_digest=build_id.rsplit("_", 1)[-1],
                docs=len({chunk.doc_id for chunk in chunks}),
                chunks=len(chunks),
                observed_at=observed_at,
            ),
        )
        written = True
    payload = {
        "package": str(package),
        "baseline_id": product.baseline_id,
        "version": version,
        "corpus_build_id": build_id,
        "chunks": len(chunks),
        "docs": len({chunk.doc_id for chunk in chunks}),
        "params": len(catalog),
        "present": sum(1 for fact in facts if fact.status is FactStatus.PRESENT),
        "unclear": sum(1 for fact in facts if fact.status is FactStatus.UNCLEAR),
        "facts_path": str(package / "sources" / "baseline_facts.csv"),
        "written": written,
        "dry_run": dry_run,
        "note": (
            "факты записаны с review=pending: наследует только accepted, подпись — "
            "`hound baseline review --accept --actor …`"
        ),
    }
    if args.json:
        _print_json(payload)
    else:
        lines = [
            f"recon {product.baseline_id} @{version}: {payload['chunks']} чанков, "
            f"{payload['docs']} документов, сборка {build_id}",
            f"параметров: {payload['params']}; present {payload['present']}, "
            f"unclear {payload['unclear']}",
            (
                "dry-run: ничего не записано"
                if dry_run
                else f"записано: {payload['facts_path']} (review=pending)"
            ),
            payload["note"],
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
            "оценщик не сконфигурирован: карта требует LLM-слот (deploy.yaml: llm) "
            "либо --llm-model; детерминированные факты строит `hound baseline recon`",
            code="baseline",
            exit_code=EXIT_USAGE,
        )
    return evaluator


def run_map(args: argparse.Namespace) -> int:
    """``hound baseline map``: Capability/Coverage reference-продукта по каталогу (§9).

    Карта **не** входит в сравнение когорты: артефакт лежит в ``reference/…/maps/{map_id}/``
    и имеет свою идентичность. Считается тем же расчётом, что бандл прогона, по ячейкам
    оценщика на reference-корпусе — второй формулы оценки нет.
    """
    from core.pipeline.digests import baseline_facts_digest, catalog_digest
    from core.pipeline.reference_corpus import iter_reference_chunks
    from core.pipeline.reference_map import (
        ReferenceMapLayout,
        build_reference_map,
        cell_row,
        write_map,
    )

    package = _package_dir(args)
    declaration = _declaration(args)
    product = _resolve_baseline(args, declaration)
    version = str(getattr(args, "version", None) or product.current_version)
    reference_dir = _reference_dir(args, product.product_id, version)
    db_path = Path(getattr(args, "corpus_db", None) or reference_dir / "reference.db")
    if not db_path.is_file():
        raise CliError(
            f"reference-корпус не найден: {db_path}",
            code="baseline",
            exit_code=EXIT_USAGE,
        )
    manifest = read_manifest(reference_dir / "manifest.json")
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
            f"reference-корпус пуст: {db_path} (0 чанков)",
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
    map_id = reference_map_id(
        reference_build_id=build_id,
        baseline_facts_digest=baseline_facts_digest(facts),
        catalog_version=catalog_digest(catalog),
        llm_scenarios={"model_id": getattr(evaluator, "model_id", None)},
        prompt_template=getattr(args, "prompt_template", None),
        evaluation_policy=None,
    )
    layout = ReferenceMapLayout(root=reference_dir / "maps" / map_id, map_id=map_id)
    result = build_reference_map(
        layout=layout,
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
        prompt_template=getattr(args, "prompt_template", None),
        cutoff_k=int(getattr(args, "cutoff_k", 10) or 10),
        model_id=getattr(evaluator, "model_id", None),
        observed_at=_dt.datetime.now(_dt.UTC).replace(microsecond=0).isoformat(),
    )
    agreement = fact_map_agreement(facts, [cell_row(cell) for cell in result.cells])
    dry_run = bool(getattr(args, "dry_run", False))
    paths: dict[str, str] = {}
    if not dry_run:
        paths = {
            name: str(path)
            for name, path in write_map(
                layout, result, extra_score={"fact_map_agreement": agreement}
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
        help="reference-продукты профиля: декларация, факты о документации предка, "
        "разведка, ревью, refresh и отчёт (план 27)",
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
        "--corpus-db", dest="corpus_db", default=None, help="путь к reference.db"
    )
    p_recon.add_argument(
        "--reference-dir",
        dest="reference_dir",
        default=None,
        help="корень reference-корпуса (по умолчанию ./reference)",
    )
    p_recon.add_argument(
        "--dry-run", dest="dry_run", action="store_true", help="ничего не писать"
    )
    p_recon.set_defaults(handler=run_recon)

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
        "--corpus-db", dest="corpus_db", default=None, help="путь к reference.db"
    )
    p_map.add_argument(
        "--reference-dir",
        dest="reference_dir",
        default=None,
        help="корень reference-корпуса (по умолчанию ./reference)",
    )
    p_map.add_argument(
        "--llm-model",
        dest="llm_model",
        default=None,
        help="объявленная модель оценщика (иначе — слот deploy.yaml: llm)",
    )
    p_map.add_argument(
        "--prompt-template",
        dest="prompt_template",
        default=None,
        help="имя@хеш шаблона промпта (условие измерения, входит в map_id)",
    )
    p_map.add_argument(
        "--cutoff-k",
        dest="cutoff_k",
        type=int,
        default=10,
        help="глубина ретривала карты (top-K фрагментов на параметр)",
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
            "(list|facts|report|review|refresh|recon|map)",
            code="baseline",
            exit_code=EXIT_USAGE,
        )
    return handler(args)
