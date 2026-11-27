#!/usr/bin/env python3
"""Прибор: поддерживает ли хост объявленный ``strategy: hybrid`` (A3, замер а не догадка).

**Вопрос прибора.** Ключ ``domain_profiles.yaml`` с ``fetch: browser`` объявляет, что
страницы требуют браузера. ``strategy`` говорит, **как** браузер используется: ``honest``
рендерит каждый URL, ``hybrid`` прогревает хост один раз и дальше идёт лёгким HTTP-GET с
cookie-мостом (см. ``core/adapters/crawl_browser.py``). Перевести ключ в ``hybrid``
законно только по наблюдению: мост должен отдавать **тот же материал**, что рендер.

**Что мерится (парно, по каждому URL).** Сначала ``hybrid``-фетч, затем ``honest``-рендер
того же адреса. Пара даёт три факта, которых не даёт один агрегат:

* ``mode`` — кто доставил байты (``http`` = мост сработал, ``browser`` = рендер, то есть
  мост не снял челлендж/отдал заглушку);
* ``text_chars`` — выход **реального** экстрактора на обоих байтах: «200 от моста» ещё не
  значит «тот же документ»;
* ``warms`` из ``BrowserCrawler.hybrid_state()`` — сколько раз браузер платил за хост:
  именно эта величина и есть цена режима.

**Выборка.** URL берутся из durable-подложки (``state.db: url_state``) по объявленному
ключу профиля (host + префикс пути), детерминированно: сортировка по URL и равномерный
шаг. Выборка — измерение, а не «весь корень»: по правилу владельца публикуется интервал
(доля моста даётся вместе с числом наблюдений), а не заявка на полноту.

**Границы прибора, названные явно.**

* Состояние ``hybrid`` живёт в движке, а движок — на поток; прибор идёт одним потоком, то
  есть наблюдает **лучший** случай (в прогоне с ``browser_workers > 1`` каждый воркер
  прогревает хост сам — цена выше).
* ``robots.txt`` уважается тем же гейтом, что в прогоне; отказ печатается как отказ.
* Замер не пишет ни в ``state.db``, ни в корпус: кэш отключён, байты не сохраняются.

Выход — YAML-наблюдение (``--out``) плюс человекочитаемая таблица в stdout.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:  # запуск как `python scripts/…` без установки пакета
    sys.path.insert(0, str(REPO_ROOT))

from core.adapters.crawl_browser import BrowserCrawler  # noqa: E402
from core.adapters.crawl_http import HttpCrawler  # noqa: E402
from core.cli.wiring import (  # noqa: E402
    LOCAL_PROFILE,
    _browser_crawler_kwargs,
    _http_crawler_kwargs,
    build_adapters,
)
from core.config.deploy import load_deploy_config  # noqa: E402
from core.config.domain_profiles import load_domain_profiles  # noqa: E402
from core.domain.enums import CrawlStrategy, ExtractionRoute, FetchMode  # noqa: E402
from core.ports.types import FetchPlan  # noqa: E402
from core.util.document_formats import document_suffix  # noqa: E402
from core.util.rate_limit import RateLimiter  # noqa: E402
from core.util.robots import RobotsGate  # noqa: E402

#: Сколько URL на ключ по умолчанию. Мало — потому что цена одного URL это браузерный
#: рендер (секунды), а вопрос прибора бинарный на ключ; число наблюдений публикуется.
DEFAULT_PER_KEY = 6


@dataclass
class UrlRow:
    """Наблюдение одного URL: hybrid-фетч и (опционально) honest-рендер того же адреса."""

    url: str
    artifact: bool
    hybrid_mode: str | None = None
    hybrid_status: int | None = None
    hybrid_bytes: int = 0
    hybrid_chars: int = 0
    hybrid_ms: int = 0
    hybrid_error: str | None = None
    honest_chars: int | None = None
    honest_ms: int | None = None
    honest_error: str | None = None

    @property
    def bridged(self) -> bool:
        """Мост сработал: байты пришли лёгким GET, а не рендером."""
        return self.hybrid_mode == FetchMode.HTTP.value

    def as_dict(self) -> dict[str, Any]:
        """Сериализация для артефакта (без производных свойств — они считаются заново)."""
        return {
            "url": self.url,
            "artifact": self.artifact,
            "hybrid": {
                "mode": self.hybrid_mode,
                "status": self.hybrid_status,
                "bytes": self.hybrid_bytes,
                "text_chars": self.hybrid_chars,
                "ms": self.hybrid_ms,
                "error": self.hybrid_error,
            },
            "honest": {
                "text_chars": self.honest_chars,
                "ms": self.honest_ms,
                "error": self.honest_error,
            },
        }


@dataclass
class KeyRow:
    """Наблюдение ключа профиля: строки URL + счётчики движка по его хосту."""

    key: str
    fetch: str
    strategy: str
    wait_for: str | None
    urls: list[UrlRow] = field(default_factory=list)
    warms: int = 0
    http_fetches: int = 0
    challenges: int = 0
    artifacts: int = 0

    @property
    def bridged(self) -> int:
        return sum(1 for row in self.urls if row.bridged)

    @property
    def measured(self) -> int:
        """URL, где hybrid-фетч вообще дал байты (ошибка транспорта — не наблюдение)."""
        return sum(1 for row in self.urls if row.hybrid_error is None and row.hybrid_mode)

    def text_ratio(self) -> float | None:
        """Медиана ``hybrid_chars / honest_chars`` по парам, где рендер дал текст."""
        ratios = [
            row.hybrid_chars / row.honest_chars
            for row in self.urls
            if row.honest_chars and row.honest_chars > 0
        ]
        if not ratios:
            return None
        ratios.sort()
        mid = len(ratios) // 2
        return ratios[mid] if len(ratios) % 2 else (ratios[mid - 1] + ratios[mid]) / 2

    def as_dict(self) -> dict[str, Any]:
        ratio = self.text_ratio()
        return {
            "key": self.key,
            "declared": {"fetch": self.fetch, "strategy": self.strategy, "wait_for": self.wait_for},
            "engine": {
                "warms": self.warms,
                "http_fetches": self.http_fetches,
                "challenges": self.challenges,
                "artifacts": self.artifacts,
            },
            "measured_urls": self.measured,
            "bridged_urls": self.bridged,
            "text_ratio_median": None if ratio is None else round(ratio, 4),
            "urls": [row.as_dict() for row in self.urls],
        }


def _key_matches(key: str, url: str) -> bool:
    """Совпадает ли ``url`` с ключом профиля ``host[/path]`` (та же семантика, что резолв).

    Хост сравнивается без учёта ``www.`` (как ``fetch_plan``); префикс пути — по границе
    сегмента, иначе ``/products/dbms`` затянул бы ``/products/dbms-old``.
    """
    host_key, _, path_key = key.partition("/")
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    if host.removeprefix("www.") != host_key.strip().lower().removeprefix("www."):
        return False
    if not path_key:
        return True
    prefix = "/" + path_key.strip("/")
    path = parts.path or "/"
    return path == prefix or path.startswith(prefix + "/")


def _select_urls(db: Path, key: str, per_key: int) -> list[str]:
    """Детерминированная выборка URL ключа из ``url_state`` (равномерный шаг по сортировке).

    Равномерный шаг, а не первые N: первые N — это одна ветка дерева (TOC и его соседи),
    а вопрос «отдаётся ли хост лёгким GET» задаётся по всему поддереву. Порядок сортировки
    лексикографический, поэтому повторный запуск даёт те же адреса.
    """
    host = key.partition("/")[0].lower()
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT url FROM url_state WHERE url LIKE ? ORDER BY url", (f"https://%{host}%",)
        ).fetchall()
    finally:
        conn.close()
    matched = sorted({str(row[0]) for row in rows if _key_matches(key, str(row[0]))})
    if not matched:
        return []
    if len(matched) <= per_key:
        return matched
    step = len(matched) / per_key
    picked = [matched[min(int(index * step), len(matched) - 1)] for index in range(per_key)]
    return list(dict.fromkeys(picked))


def _extract_chars(extractor: Any, page: Any) -> tuple[int, str | None]:
    """Символы текста из страницы реальным экстрактором (пригодность по выходу, не по байтам)."""
    try:
        doc = extractor.extract(page, ExtractionRoute.AUTO)
    except Exception as exc:  # noqa: BLE001 — изоляция по контракту прибора
        return 0, f"extract: {type(exc).__name__}: {exc}"
    error = getattr(doc, "error", None)
    return len(getattr(doc, "text", "") or ""), (f"extract: {error}" if error else None)


def _fetch_hybrid(browser: Any, url: str, wait_for: str | None, rate_limit: Any) -> Any:
    """Один hybrid-фетч объявленным планом (``wait_for`` — авторское, не синтезируется)."""
    plan = FetchPlan(
        FetchMode.BROWSER,
        strategy=CrawlStrategy.HYBRID,
        wait_for=wait_for,
        rate_limit=rate_limit,
    )
    return browser.fetch(url, plan)


def _fetch_honest(browser: Any, url: str, wait_for: str | None, rate_limit: Any) -> Any:
    """Один honest-рендер того же адреса — эталон пары."""
    plan = FetchPlan(
        FetchMode.BROWSER,
        strategy=CrawlStrategy.HONEST,
        wait_for=wait_for,
        rate_limit=rate_limit,
    )
    return browser.fetch(url, plan)


def measure(
    *,
    profiles: dict[str, Any],
    keys: list[str],
    db: Path,
    per_key: int,
    extractor: Any,
    browser: Any,
    http: Any,
    robots: RobotsGate | None,
    honest_pair: bool,
    verbose: bool,
) -> list[KeyRow]:
    """Прогоняет пары «hybrid / honest» по выборке URL каждого ключа.

    Один ``browser`` на весь прогон: состояние липкого hybrid живёт в движке, и прогон
    ключей одного хоста обязан видеть его так же, как увидел бы живой краул.
    """
    rows: list[KeyRow] = []
    for key in keys:
        profile = profiles[key]
        strategy = getattr(profile, "strategy", None)
        row = KeyRow(
            key=key,
            fetch=str(getattr(profile, "fetch", "")),
            strategy=str(strategy) if strategy is not None else "auto",
            wait_for=getattr(profile, "wait_for", None),
        )
        urls = _select_urls(db, key, per_key)
        if not urls:
            print(f"  {key}: в url_state нет адресов под ключом — нечего мерить")
        rate_limit = getattr(profile, "rate_limit", None)
        before = browser.hybrid_state().get(key.partition("/")[0].lower(), {})
        for url in urls:
            item = UrlRow(url=url, artifact=document_suffix(url) is not None)
            if robots is not None:
                verdict = robots.verdict(url, enabled=True)
                if not verdict.allowed:
                    item.hybrid_error = f"robots: {verdict.reason or verdict.state.value}"
                    row.urls.append(item)
                    continue
            started = time.monotonic()
            try:
                page = _fetch_hybrid(browser, url, row.wait_for, rate_limit)
            except Exception as exc:  # noqa: BLE001 — сбой одного URL не рушит замер
                item.hybrid_error = f"{type(exc).__name__}: {exc}"
            else:
                item.hybrid_ms = int((time.monotonic() - started) * 1000)
                item.hybrid_mode = str(getattr(page, "fetch_mode", "") or "") or None
                item.hybrid_status = getattr(page, "status_code", None)
                item.hybrid_bytes = len(getattr(page, "content", b"") or b"")
                item.hybrid_chars, item.hybrid_error = _extract_chars(extractor, page)
            if honest_pair and item.hybrid_error is None:
                started = time.monotonic()
                try:
                    page = _fetch_honest(browser, url, row.wait_for, rate_limit)
                except Exception as exc:  # noqa: BLE001
                    item.honest_error = f"{type(exc).__name__}: {exc}"
                else:
                    item.honest_ms = int((time.monotonic() - started) * 1000)
                    item.honest_chars, error = _extract_chars(extractor, page)
                    item.honest_error = error
            row.urls.append(item)
            if verbose:
                print(
                    f"    {item.hybrid_mode or 'ERR':7s} {item.hybrid_status or '---':>4} "
                    f"{item.hybrid_chars:6d}ch/{item.honest_chars if item.honest_chars is not None else -1:6d}ch "
                    f"{item.hybrid_ms:5d}ms {url}"
                )
        after = browser.hybrid_state().get(key.partition("/")[0].lower(), {})
        row.warms = int(after.get("warms", 0)) - int(before.get("warms", 0))
        row.http_fetches = int(after.get("http_fetches", 0)) - int(before.get("http_fetches", 0))
        row.challenges = int(after.get("challenges", 0)) - int(before.get("challenges", 0))
        row.artifacts = int(after.get("artifacts", 0)) - int(before.get("artifacts", 0))
        rows.append(row)
    return rows


def _verdict(row: KeyRow) -> str:
    """Вердикт по ключу: переводить ли в ``hybrid``.

    Три исхода, а не два: ``hybrid`` — мост отдал материал; ``honest`` — мост не сработал
    (или материал хуже); ``недостаточно наблюдений`` — ни одного пригодного URL, и тогда
    режим не меняется (A3: отсутствие факта — не факт).
    """
    if row.measured == 0:
        return "недостаточно наблюдений"
    ratio = row.text_ratio()
    if row.bridged == row.measured and (ratio is None or ratio >= 0.9):
        return "hybrid"
    return "honest"


def _print_table(rows: list[KeyRow]) -> None:
    print()
    print(f"{'ключ':44s} {'URL':>4} {'мост':>5} {'warms':>6} {'ratio':>6}  вердикт")
    for row in rows:
        ratio = row.text_ratio()
        print(
            f"{row.key:44s} {row.measured:4d} {row.bridged:5d} {row.warms:6d} "
            f"{('—' if ratio is None else f'{ratio:.2f}'):>6}  {_verdict(row)}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--package", required=True, help="каталог пакета (domain_profiles.yaml)")
    parser.add_argument("--state-db", required=True, help="durable state.db (url_state: выборка)")
    parser.add_argument(
        "--keys", nargs="*", default=None, help="ключи профиля (по умолчанию — все fetch: browser)"
    )
    parser.add_argument("--per-key", type=int, default=DEFAULT_PER_KEY, help="URL на ключ")
    parser.add_argument("--out", default=None, help="файл YAML-наблюдения")
    parser.add_argument("--no-honest-pair", action="store_true", help="не рендерить эталон")
    parser.add_argument("--verbose", action="store_true", help="печатать каждый URL")
    args = parser.parse_args(argv)

    profiles = load_domain_profiles(Path(args.package) / "domain_profiles.yaml")
    keys = args.keys or [
        key for key, profile in profiles.items() if str(getattr(profile, "fetch", "")) == "browser"
    ]
    missing = [key for key in keys if key not in profiles]
    if missing:
        print(f"ключи вне профиля: {missing}", file=sys.stderr)
        return 2

    deploy = load_deploy_config(None)
    http = HttpCrawler(cache=None, **_http_crawler_kwargs(deploy))
    browser = BrowserCrawler(
        cache=None,
        rate_limiter=RateLimiter(),
        **_browser_crawler_kwargs(deploy),
    )
    extractor = build_adapters(profile=LOCAL_PROFILE, package=str(Path(args.package))).extractor
    robots = RobotsGate(_robots_fetcher(http))

    print(f"ключей: {len(keys)}; URL на ключ: {args.per_key}; эталон honest: {not args.no_honest_pair}")
    try:
        rows = measure(
            profiles=profiles,
            keys=keys,
            db=Path(args.state_db),
            per_key=args.per_key,
            extractor=extractor,
            browser=browser,
            http=http,
            robots=robots,
            honest_pair=not args.no_honest_pair,
            verbose=args.verbose,
        )
    finally:
        browser.close()

    _print_table(rows)
    if args.out:
        payload = {
            "instrument": "scripts/measure_hybrid_hosts.py",
            "package": str(args.package),
            "state_db": str(args.state_db),
            "per_key": args.per_key,
            "honest_pair": not args.no_honest_pair,
            "keys": [dict(row.as_dict(), verdict=_verdict(row)) for row in rows],
        }
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"\nнаблюдение: {out}")
    return 0


def _robots_fetcher(http_crawler: Any) -> Any:
    """Фетчер ``robots.txt`` через HTTP-краулер — та же политика, что в прогоне.

    ``404``/``410`` пробрасывается как :class:`CrawlNotFound` (гейт выведет ``not_found``,
    то есть «ленты нет» — наблюдённый факт), прочий сбой — ``None`` (``unavailable``).
    """
    from core.ports.crawl_errors import CrawlNotFound

    def fetch(origin: str) -> str | None:
        try:
            page = http_crawler.fetch(f"{origin}/robots.txt", FetchPlan(FetchMode.HTTP))
        except CrawlNotFound:
            raise
        except Exception:  # noqa: BLE001 — наблюдения нет
            return None
        return page.content.decode("utf-8", errors="replace")

    return fetch


if __name__ == "__main__":
    raise SystemExit(main())
