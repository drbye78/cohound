"""Почему `policy_matched = false`: отпечаток пространства как функция **области прогона**.

Зачем прибор. Прогон `ru-pg-dbms-48bf65507223` (`--only-product postgrespro
--pdf-pipeline text_layer`) получил `corpus_coverage.policy_matched = false` на всех
четырёх корнях продукта при тех же снятиях, что и полный прогон `2a5b51af5778`, где
`policy_matched = true`. Следствие не косметическое: `complete=false` ⇒ датированный
носитель `❌` (I2d) не публикуется. Две причины выглядели равноправными — сужение
области (`--only-product`) и смена механизма PDF (`pdf_pipeline` входит в
`crawl_config_digest`). Этот прибор разделяет их **вычислением**, а не прогоном.

Что он делает. Собирает объявленные корни пакета (`load_effective_seeds` + `--urls`),
читает записи политики операторского `state.db` (**только чтение**) и считает
`crawl_space_digest` — ту же величину, что несёт снятие корня
(:func:`core.pipeline.crawl_policy.resolve_crawl_policy`). Три материала:

* полная когорта + `crawl_config_digest` канонического прогона;
* только один продукт + тот же `crawl_config_digest`;
* только один продукт + конфиг не объявлен.

Печать: отпечаток, число решений и совпадение с отпечатком снятия (если задан
`--capture-digest`). Совпадение первой строки до последнего знака означает, что модель
воспроизведена, а не подогнана: дальше видно, что именно меняет отпечаток.

Второй, независимый повод проверить отдельно: `pdf_pipeline`/`document_level` входят в
`CrawlLimits.as_dict()`, а тот — материал отпечатка. Прибор печатает и это: два конфига,
различающиеся **только** читателем PDF, при одном и том же пространстве URL.

Запуск (стенд канонический: записи политики живут в его `state.db`):

    .venv/bin/python scripts/measure_crawl_space_digest.py \
      --package packages/ru-pg-dbms \
      --state-db ~/.cohound/stands/ru-pg-dbms/state/ru-pg-dbms/state.db \
      --product postgrespro \
      --config-digest a28ac62830945aa1c2811d3e \
      --capture-digest 4badc0e1ed6f976793dbb764

Границы прибора: он не запускает обход и не читает `url_state` (наблюдения не входят в
материал отпечатка по построению); «продукт» он берёт из `--product`, а не из плана
прогона. Число решений — не число корней: решение появляется только там, где корень
совпал с записью политики (пакет или операторский `state.db`).
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import sys
from pathlib import Path

import yaml

from core.config.crawl_policy import CrawlPolicyConfig, load_crawl_policy
from core.pipeline.crawl_policy import declared_roots, resolve_crawl_policy
from core.pipeline.digests import crawl_space_digest, short_hash
from core.pipeline.onboarding import load_effective_seeds
from core.store.state_store import read_crawl_policy_rows

#: Ключи, которые `CrawlLimits.as_dict()` отдаёт в дайджест, — минимальный набор для
#: второго опыта (меняется только читатель PDF). Значения взяты из объявленных осей
#: пилота, чтобы отпечаток был сопоставим с прогонами, а не с выдумкой.
_BASE_LIMITS: dict[str, object] = {
    "max_workers": 10,
    "max_depth": 8,
    "max_documents": 800,
    "exclude_paths": [],
    "exclude_documents": [],
    "robots": True,
    "semantics": "crawl-3",
}


def _load_urls(package: Path) -> dict[str, list[str]]:
    """Корпусный вход прогона (`sources/corpus_urls.yaml`) — как `--urls`."""
    path = package / "sources" / "corpus_urls.yaml"
    if not path.is_file():
        raise SystemExit(f"{path}: файла нет — прибор ждёт пакет с корпусным входом")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise SystemExit(f"{path}: ожидался словарь продукт → список URL")
    return {str(k): [str(u) for u in v or []] for k, v in data.items()}


def _space(
    *,
    package: Path,
    urls: dict[str, list[str]],
    policy: CrawlPolicyConfig,
    rows: list[object],
    products: list[str],
    config_digest: str | None,
    now: _dt.datetime,
) -> tuple[str, int, int]:
    """Отпечаток пространства, число решений и число объявленных корней."""
    seeds, _ = load_effective_seeds(str(package), products, urls_override=urls)
    roots = declared_roots(seeds)
    resolved = resolve_crawl_policy(
        package_policy=policy,
        operator_rows=rows,
        roots=roots,
        now=now,
        observed_at={},
        requested=(),
        crawl_config_digest=config_digest,
    )
    return resolved.space_digest, len(resolved.decisions), len(roots)


def _reader_only_digest(pdf_pipeline: str, document_level: str) -> str:
    """`crawl_config_digest` для набора осей, где читатель PDF — единственная разница."""
    payload = dict(_BASE_LIMITS)
    payload["pdf_pipeline"] = pdf_pipeline
    payload["document_level"] = document_level
    return short_hash(json.dumps(payload, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--package", required=True)
    parser.add_argument("--state-db", default=None, help="операторский state.db (только чтение)")
    parser.add_argument("--product", default=None, help="продукт для суженного материала")
    parser.add_argument("--config-digest", default=None, help="crawl_config_digest прогона")
    parser.add_argument("--capture-digest", default=None, help="policy_digest снятия корня")
    parser.add_argument("--now", default=None, help="ISO-момент резолва (по умолчанию сегодня)")
    args = parser.parse_args(argv)

    package = Path(args.package)
    urls = _load_urls(package)
    if not urls:
        raise SystemExit("корпусный вход пуст — считать нечего")
    policy_path = package / "crawl_policy.yaml"
    policy = load_crawl_policy(policy_path) if policy_path.is_file() else CrawlPolicyConfig()
    rows = read_crawl_policy_rows(Path(args.state_db)) if args.state_db else []
    now = (
        _dt.datetime.fromisoformat(args.now)
        if args.now
        else _dt.datetime.now(_dt.UTC)
    )

    products_all = sorted(urls)
    product = args.product or products_all[0]
    print(f"пакет: {package}")
    print(f"продукты: {len(products_all)}, записей политики: {len(rows)}")
    print()

    cases: list[tuple[str, list[str], str | None]] = [
        ("полная когорта, конфиг объявлен", products_all, args.config_digest),
        (f"только {product}, тот же конфиг", [product], args.config_digest),
        (f"только {product}, конфиг не объявлен", [product], None),
    ]
    for label, products, config_digest in cases:
        digest, decisions, roots = _space(
            package=package,
            urls=urls,
            policy=policy,
            rows=rows,
            products=products,
            config_digest=config_digest,
            now=now,
        )
        match = ""
        if args.capture_digest:
            match = "  ← совпадает со снятием" if digest == args.capture_digest else ""
        print(
            f"{label:42} корней {roots:2}  решений {decisions:2}  "
            f"space={digest}{match}"
        )

    print()
    print("читатель PDF как отдельный повод (пространство URL одно и то же):")
    for pdf_pipeline, document_level in (
        ("structured", "layout_tables"),
        ("text_layer", "layout_tables"),
        ("structured", "text"),
    ):
        config_digest = _reader_only_digest(pdf_pipeline, document_level)
        space = crawl_space_digest(
            (("p", "root", "example.invalid", "/docs/", "auto"),),
            crawl_config_digest=config_digest,
        )
        print(
            f"  pdf_pipeline={pdf_pipeline:11} document_level={document_level:13} "
            f"config={config_digest} space={space}"
        )
    print()
    print(
        "Вывод прибора: отпечаток зависит от ОБЛАСТИ прогона (какие корни попали в "
        "решения), а не только от осей; корень без записи политики в материал не входит."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
