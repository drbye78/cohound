#!/usr/bin/env python3
"""Прибор: что проход без сети (`--refresh offline`/`frozen`) увидит в подложке.

Вопрос замера. Проход без сети собирает корпус из durable-субстрата
(``url_state`` → ``document_version`` → chunk-set) и **не** ходит в сеть. Значит его
корпус — функция того, что ``state.db`` держит в этот момент, а не того, что объявлено
пакетом. Замер 23.11.2026 на стенде ``ru-pg-dbms``: прогон ``--refresh offline``
(`ru-pg-dbms-902c0b71c419`) напечатал 1 590 строк «у URL нет активной версии документа;
документ не вошёл в корпус» — и это были **четыре разных факта**, из которых недостачей
подложки является только один. Разбирать их по журналу нельзя (журнал — не артефакт
подложки и у следующего прогона будет другой), поэтому классификация считается по
первичным таблицам.

Классы (порядок — приоритет, первый подошедший выигрывает):

``outside_space``
    URL вне **объявленного** пространства продукта: ни один авторский корень не допускает
    его (:meth:`core.pipeline.crawl_frontier.Frontier.scope_admission` — объявленный host,
    path-prefix по границе сегмента, SSRF). Строка ``url_state`` живёт вечно, а корень
    можно вывести из пакета; под сетью такие URL отсекает ``Frontier``, под политикой без
    сети — фильтр набора работы. Это **не** недостача корпуса: материал вне объявленного
    пространства в корпус не входит по определению.
``provided``
    URL объявлен ручной подачей (``sources/manual_artifacts.yaml``): байты предъявляются
    позже в том же прогоне, поэтому «версии нет» о нём — утверждение о моменте.
``container``
    URL — контейнер (``container_state``): у контейнера версии документа нет по
    построению, члены переиспользуются по своим URL.
``refused:<kind>``
    Причина записана наблюдением (``url_state.last_error_kind``): сайт отказал
    (``robots_disallow`` — граница сайта), сервер не ответил (``source_unavailable``),
    документ не разобран и т. п. Это ответ подложки, а не её пробел.
``superseded``
    Версия есть, но вытеснена живым прогоном: переиспользовать её значило бы вернуть в
    корпус вытесненное.
``pointer_missing``
    **Дефект подложки:** ``url_state.doc_version_id`` пуст, а ``document_version`` того же
    URL держит ``active`` версию. Проход без сети читает версию **по указателю**, поэтому
    такой документ не переиспользуется, хотя он есть.
``no_version``
    Версии нет и причины не записано. Только этот класс и является недостачей подложки.

Использование:

    python scripts/measure_substrate_reuse.py \\
        --package packages/ru-pg-dbms \\
        --state-db ~/.cohound/stands/ru-pg-dbms/state/ru-pg-dbms/state.db \\
        [--product postgrespro] [--json]
"""

from __future__ import annotations

import argparse
import collections
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.pipeline.crawl_frontier import (  # noqa: E402
    CrawlLimits,
    Frontier,
    _path_within,
    decoded_scope_path,
    normalize_scope_host,
)
from core.pipeline.manual_artifacts import MANUAL_ARTIFACTS_FILE  # noqa: E402

__all__ = ["Census", "census", "classify", "declared_scopes", "main"]

#: Порядок классов в печати — от «не потеря» к «потеря».
CLASS_ORDER = (
    "has_version",
    "outside_space",
    "provided",
    "container",
    "refused",
    "superseded",
    "pointer_missing",
    "no_version",
)


@dataclass
class Census:
    """Счётчики одного продукта: класс → число, плюс разбивка отказов по виду."""

    product_id: str
    counts: collections.Counter[str] = field(default_factory=collections.Counter)
    refused_by_kind: collections.Counter[str] = field(
        default_factory=collections.Counter
    )
    examples: dict[str, list[str]] = field(default_factory=dict)

    def add(self, cls: str, url: str, kind: str = "") -> None:
        self.counts[cls] += 1
        if cls == "refused" and kind:
            self.refused_by_kind[kind] += 1
        bucket = self.examples.setdefault(cls, [])
        if len(bucket) < 3:
            bucket.append(url)

    @property
    def total(self) -> int:
        return sum(self.counts.values())

    def as_dict(self) -> dict[str, Any]:
        return {
            "product_id": self.product_id,
            "total": self.total,
            "counts": {
                cls: self.counts[cls] for cls in CLASS_ORDER if self.counts[cls]
            },
            "refused_by_kind": dict(sorted(self.refused_by_kind.items())),
            "examples": self.examples,
        }


def _read_yaml(path: Path) -> Any:
    if not path.exists():
        return None
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def declared_scopes(
    package: Path,
) -> tuple[dict[str, list[str]], dict[str, frozenset[str]]]:
    """Корни продуктов и допущенные поддомены — **тем же** правилом, что у краула.

    Копия построения из :func:`core.pipeline.corpus.build_corpus` намеренная: прибор
    обязан считать допуск тем же предикатом (:meth:`Frontier.scope_admission`), но не
    тянуть за собой весь конвейер. Расхождение с конвейером ловится тестом
    ``tests/scripts/test_measure_substrate_reuse.py``: он сверяет классы на наборе URL,
    где ответ известен.
    """
    url_map = _read_yaml(package / "sources" / "corpus_urls.yaml") or {}
    profiles = _read_yaml(package / "domain_profiles.yaml") or {}
    roots = {
        str(pid): [str(url) for url in (urls or [])] for pid, urls in url_map.items()
    }
    all_root_hosts = {
        normalize_scope_host(urlsplit(url).hostname or "")
        for urls in roots.values()
        for url in urls
    }
    profile_hosts = {
        normalize_scope_host(str(key).split("/", 1)[0]) for key in profiles
    }
    declared_hosts = (all_root_hosts | profile_hosts) - {""}
    scope_hosts = {
        pid: frozenset(
            declared_hosts
            - {normalize_scope_host(urlsplit(url).hostname or "") for url in urls}
        )
        for pid, urls in roots.items()
    }
    return roots, scope_hosts


def _in_space(
    url: str,
    frontiers: list[Frontier],
    host_cache: dict[tuple[str, str], bool],
) -> bool:
    """Допускает ли URL хоть один объявленный корень продукта (host + path-prefix).

    Предикат тот же, что у краула (:meth:`Frontier.scope_admission`), но без
    ``validate_url``: SSRF — гейт **фетча**, а не определение пространства, и адрес,
    который его не прошёл, в ``url_state`` не попадал (записывается он только после
    допуска). Прогонять SSRF на 36 630 адресах значило бы платить 10 минут за ответ,
    который уже известен.
    """
    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower()
    if not host:
        return False
    decoded = decoded_scope_path(parsed.path or "/")
    if decoded is None:
        return False
    for frontier in frontiers:
        key = (frontier.root_host, host)
        allowed = host_cache.get(key)
        if allowed is None:
            allowed = frontier.host_in_scope(host)
            host_cache[key] = allowed
        if allowed and _path_within(decoded, frontier.path_prefix):
            return True
    return False


def classify(
    *,
    url: str,
    pointer: str | None,
    kind: str | None,
    frontiers: list[Frontier],
    host_cache: dict[tuple[str, str], bool],
    provided: frozenset[str],
    containers: frozenset[str],
    version_status: dict[str, str],
    active_urls: set[tuple[str, str]],
    product_id: str,
) -> tuple[str, str]:
    """Класс одного URL ``url_state`` и вид отказа (если класс ``refused``).

    Всё, что нужно для решения, читается **один раз** на продукт (``census``):
    ``frontiers`` кэшируют разбор корня, ``version_status``/``active_urls`` — выборку
    ``document_version``. Запрос на каждый из 36 630 URL стоил бы минуты там, где нужен
    один проход.
    """
    if not _in_space(url, frontiers, host_cache):
        return "outside_space", ""
    if url in provided:
        return "provided", ""
    if url in containers:
        return "container", ""
    if kind:
        return "refused", str(kind)
    if pointer:
        if version_status.get(pointer) == "active":
            return "has_version", ""
        return "superseded", ""
    # Указателя нет. Версия могла существовать и не быть связанной — это дефект подложки.
    if (product_id, url) in active_urls:
        return "pointer_missing", ""
    return "no_version", ""


def _load_provided(package: Path) -> dict[str, set[str]]:
    """URL объявленных подач по продуктам — адреса, а не дайджесты.

    Читается YAML напрямую, а не :func:`~core.pipeline.manual_artifacts.load_manual_artifacts`:
    тот требует наличных файлов и сверяет хеши (это его работа — допуск байтов), а прибору
    нужен только список адресов, объявленных пакетом.
    """
    provided: dict[str, set[str]] = collections.defaultdict(set)
    declared = _read_yaml(package / MANUAL_ARTIFACTS_FILE) or {}
    for entry in (declared or {}).get("provide") or ():
        if isinstance(entry, dict) and entry.get("url"):
            provided[str(entry.get("product_id") or "")].add(str(entry["url"]))
    return provided


def census(package: Path, state_db: Path, products: list[str]) -> list[Census]:
    roots, scope_hosts = declared_scopes(package)
    provided = _load_provided(package)
    state = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True)
    try:
        containers: dict[str, set[str]] = collections.defaultdict(set)
        for product_id, url in state.execute(
            "SELECT product_id, normalized_url FROM container_state"
        ):
            containers[str(product_id)].add(str(url))
        version_status = {
            str(dv): str(status)
            for dv, status in state.execute(
                "SELECT doc_version_id, status FROM document_version"
            )
        }
        active_urls = {
            (str(pid), str(url))
            for pid, url in state.execute(
                "SELECT product_id, url FROM document_version WHERE status = 'active'"
            )
        }
        rows = state.execute(
            "SELECT product_id, normalized_url, doc_version_id, last_error_kind "
            "FROM url_state ORDER BY product_id, normalized_url"
        ).fetchall()
        frontiers_by_product: dict[str, list[Frontier]] = {}
        host_caches: dict[str, dict[tuple[str, str], bool]] = {}
        reports: dict[str, Census] = {}
        for product_id, url, pointer, kind in rows:
            pid = str(product_id)
            if products and pid not in products:
                continue
            frontiers = frontiers_by_product.get(pid)
            if frontiers is None:
                hosts = scope_hosts.get(pid, frozenset())
                frontiers = [
                    Frontier(root, CrawlLimits(), scope_hosts=hosts)
                    for root in roots.get(pid, [])
                ]
                frontiers_by_product[pid] = frontiers
                host_caches[pid] = {}
            report = reports.setdefault(pid, Census(product_id=pid))
            cls, refusal = classify(
                url=str(url),
                pointer=(str(pointer) if pointer else None),
                kind=(str(kind) if kind else None),
                frontiers=frontiers,
                host_cache=host_caches[pid],
                provided=frozenset(provided.get(pid, set())),
                containers=frozenset(containers.get(pid, set())),
                version_status=version_status,
                active_urls=active_urls,
                product_id=pid,
            )
            report.add(cls, str(url), refusal)
        return [reports[pid] for pid in sorted(reports)]
    finally:
        state.close()


def _print(reports: list[Census]) -> None:
    for report in reports:
        print(f"\n== {report.product_id}: URL в url_state {report.total}")
        for cls in CLASS_ORDER:
            if not report.counts[cls]:
                continue
            extra = ""
            if cls == "refused" and report.refused_by_kind:
                extra = (
                    " ("
                    + ", ".join(
                        f"{kind}={count}"
                        for kind, count in sorted(report.refused_by_kind.items())
                    )
                    + ")"
                )
            print(f"   {cls:<16}{report.counts[cls]:>7}{extra}")
        if report.counts["no_version"]:
            print("   ПРИМЕРЫ no_version: " + ", ".join(report.examples["no_version"]))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", required=True, type=Path)
    parser.add_argument("--state-db", required=True, type=Path)
    parser.add_argument(
        "--product", action="append", default=[], help="ограничить одним продуктом"
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    reports = census(args.package, args.state_db, args.product)
    if args.json:
        print(
            json.dumps(
                [report.as_dict() for report in reports], ensure_ascii=False, indent=1
            )
        )
    else:
        _print(reports)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
