#!/usr/bin/env python3
"""Прибор: сколько сетевых запросов делает **один honest-рендер** против одного лёгкого GET.

**Зачем.** ``RateLimiter`` списывает **один слот на попытку фетча**
(``corpus_ingest._fetch_with_paced_retries``), а не на сетевой запрос. Honest-рендер
браузером — это документ + JS + CSS + картинки + XHR, то есть десятки запросов к хосту;
лёгкий GET (липкий ``hybrid``) — один. Значит объявленный ``rate_limit`` для честного
плеча и для основного означают **разную нагрузку**, и вопрос «какой интервал вежлив для
каждого» решается числом запросов, а не предвидением (A3).

**Что мерится.** По каждому адресу — honest-рендер тем же движком, что в прогоне
(``_PlaywrightBackend``: те же флаги запуска, headed, ignore-default-args — иначе
антибот-класс мерился бы на другой конфигурации), с подпиской на события страницы:

* ``requests_total`` — все запросы рендера;
* ``requests_same_origin`` — только к хосту запрошенного адреса (это и есть нагрузка,
  которую считает лимитер: он ключуется по ``normalize_host``);
* ``requests_third_party`` — CDN/аналитика/шрифты (другой origin — другой счётчик);
* разбивка по ``resource_type`` (``document``/``script``/``stylesheet``/``image``/
  ``xhr``/``font``/``other``), чтобы «десятки» были не одним числом;
* байты ответов, если браузер их сообщает.

Рядом — лёгкий GET того же адреса через ``httpx`` с теми же cookie, с подсчётом запросов
**включая редиректы**: редирект — тоже запрос к хосту, и лимитер его не видит отдельно.

**Границы прибора, названные явно.**

* Один поток, один новый контекст на адрес: кэш браузера не переиспользуется, поэтому
  число запросов — холодного рендера (в прогоне часть ресурсов может прийти из кэша
  контекста, и тогда честное плечо дешевле измеренного).
* ``wait_until=domcontentloaded`` как в адаптере; запросы, начатые после этого момента
  (ленивая подгрузка по скроллу), в счёт не попадают — их нет и в прогоне.
* Реклама/аналитика может блокироваться сетью хоста — тогда число третьесторонних
  запросов ниже реального; на вывод о same-origin это не влияет.
* Замер **не** отвечает на вопрос «какой интервал терпит хост»: он даёт множитель
  между плечами. Терпимость проверяется живым краулом.

Выход — JSON (``--out``) и таблица в stdout.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:  # запуск как `python scripts/…` без установки пакета
    sys.path.insert(0, str(REPO_ROOT))

from core.adapters.crawl_browser import _PlaywrightBackend  # noqa: E402
from core.cli.wiring import _browser_crawler_kwargs  # noqa: E402
from core.config.deploy import load_deploy_config  # noqa: E402
from core.config.domain_profiles import load_domain_profiles  # noqa: E402
from core.util.rate_limit import normalize_host  # noqa: E402

DEFAULT_PER_KEY = 3

#: Типы ресурсов, которые Playwright сообщает в ``request.resource_type``.
RESOURCE_TYPES = ("document", "script", "stylesheet", "image", "xhr", "font", "media", "other")


@dataclass
class RenderRow:
    """Наблюдение одного honest-рендера и лёгкого GET того же адреса."""

    url: str
    key: str
    #: Запросы рендера: всего / к хосту адреса / к чужим origin.
    requests_total: int = 0
    requests_same_origin: int = 0
    requests_third_party: int = 0
    by_type: dict[str, int] = field(default_factory=dict)
    bytes_total: int = 0
    render_ms: int = 0
    render_error: str | None = None
    #: Лёгкий GET: запросы (документ + редиректы), байты, время.
    light_requests: int = 0
    light_bytes: int = 0
    light_ms: int = 0
    light_status: int | None = None
    light_error: str | None = None

    @property
    def ratio(self) -> float | None:
        """Во сколько раз рендер дороже лёгкого GET по числу запросов к тому же origin."""
        if not self.light_requests or self.requests_same_origin is None:
            return None
        return self.requests_same_origin / self.light_requests

    def as_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "key": self.key,
            "render": {
                "requests_total": self.requests_total,
                "requests_same_origin": self.requests_same_origin,
                "requests_third_party": self.requests_third_party,
                "by_type": dict(sorted(self.by_type.items())),
                "bytes_total": self.bytes_total,
                "ms": self.render_ms,
                "error": self.render_error,
            },
            "light": {
                "requests": self.light_requests,
                "bytes": self.light_bytes,
                "ms": self.light_ms,
                "status": self.light_status,
                "error": self.light_error,
            },
            "ratio": None if self.ratio is None else round(self.ratio, 3),
        }


def _select_urls(db: Path, key: str, per_key: int) -> list[str]:
    """Детерминированная выборка URL ключа из ``url_state`` (равномерный шаг).

    Та же выборка, что у ``measure_hybrid_hosts.py``: два прибора, сравнивающие плечи,
    обязаны смотреть на одни адреса, иначе их числа не складываются.
    """
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from measure_hybrid_hosts import _select_urls as select  # noqa: PLC0415 — переиспользование

    return select(db, key, per_key)


def _cookie_header(cookies: list[dict[str, Any]]) -> str:
    """``Cookie``-заголовок из списка cookie браузера (имя=значение;…)."""
    return "; ".join(
        f"{cookie.get('name')}={cookie.get('value')}"
        for cookie in cookies
        if cookie.get("name")
    )


def _render(
    backend: Any, url: str, wait_for: str | None, timeout_ms: int
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Honest-рендер с подсчётом запросов; возвращает наблюдение и cookie контекста.

    Подписка на события — на ``page`` **до** навигации: запросы, начатые самой навигацией,
    иначе не попадут в счёт. ``session._page`` — приватный шов адаптера; прибор им
    пользуется намеренно, потому что повторять запуск движка своими руками значило бы
    мерить другую конфигурацию (флаги антибота, headed, ignore-default-args).
    """
    host = normalize_host(urlsplit(url).hostname or "")
    stats: dict[str, Any] = {"total": 0, "same": 0, "third": 0, "by_type": {}, "bytes": 0}
    session = backend.open()
    try:
        page = getattr(session, "_page", None)
        if page is None:
            raise RuntimeError("сессия не отдаёт page: прибор не может подписаться на запросы")

        def on_request(request: Any) -> None:
            stats["total"] += 1
            request_host = normalize_host(urlsplit(getattr(request, "url", "")).hostname or "")
            if request_host == host:
                stats["same"] += 1
            else:
                stats["third"] += 1
            kind = str(getattr(request, "resource_type", "") or "other")
            key = kind if kind in RESOURCE_TYPES else "other"
            stats["by_type"][key] = stats["by_type"].get(key, 0) + 1

        def on_response(response: Any) -> None:
            try:
                length = int(response.headers.get("content-length") or 0)
            except (TypeError, ValueError):
                length = 0
            stats["bytes"] += length

        page.on("request", on_request)
        page.on("response", on_response)
        started = time.monotonic()
        session.navigate(url, wait_for)
        stats["ms"] = int((time.monotonic() - started) * 1000)
        cookies = list(session.cookies(url) or [])
        return stats, cookies
    finally:
        session.close()


def _light_get(url: str, cookie: str, timeout_s: float) -> dict[str, Any]:
    """Лёгкий GET как в липком ``hybrid``: тот же cookie-мост, редиректы считаются.

    Запросы считаются хуком ``httpx``: редирект — тоже запрос к хосту, а лимитер видит
    только попытку фетча, поэтому «1» здесь было бы занижением там, где сайт редиректит.
    """
    import httpx  # noqa: PLC0415 — тяжёлый импорт только в этом приборе

    count = {"requests": 0}

    def on_request(_request: Any) -> None:
        count["requests"] += 1

    headers = {"User-Agent": "cohound-hybrid-probe/1"}
    if cookie:
        headers["Cookie"] = cookie
    started = time.monotonic()
    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=timeout_s,
            headers=headers,
            event_hooks={"request": [on_request]},
        ) as client:
            response = client.get(url)
        return {
            "requests": count["requests"],
            "bytes": len(response.content),
            "ms": int((time.monotonic() - started) * 1000),
            "status": response.status_code,
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001 — сбой одного адреса не рушит замер
        return {
            "requests": count["requests"],
            "bytes": 0,
            "ms": int((time.monotonic() - started) * 1000),
            "status": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


def measure(
    *,
    profiles: dict[str, Any],
    keys: list[str],
    db: Path,
    per_key: int,
    timeout_s: float,
    verbose: bool,
) -> list[RenderRow]:
    """Рендерит и спрашивает лёгким GET каждый выбранный адрес; один движок на прогон."""
    deploy = load_deploy_config(None)
    kwargs = _browser_crawler_kwargs(deploy)
    backend = _PlaywrightBackend(
        cdp_url=kwargs.get("cdp_url"),
        user_data_dir=kwargs.get("user_data_dir"),
        headless=kwargs.get("headless", False),
        channel=kwargs.get("channel"),
        timeout_ms=kwargs.get("timeout_ms", 30000),
    )
    rows: list[RenderRow] = []
    try:
        for key in keys:
            profile = profiles[key]
            wait_for = getattr(profile, "wait_for", None)
            for url in _select_urls(db, key, per_key):
                row = RenderRow(url=url, key=key)
                try:
                    stats, cookies = _render(backend, url, wait_for, kwargs.get("timeout_ms", 30000))
                except Exception as exc:  # noqa: BLE001
                    row.render_error = f"{type(exc).__name__}: {exc}"
                else:
                    row.requests_total = int(stats["total"])
                    row.requests_same_origin = int(stats["same"])
                    row.requests_third_party = int(stats["third"])
                    row.by_type = dict(stats["by_type"])
                    row.bytes_total = int(stats["bytes"])
                    row.render_ms = int(stats["ms"])
                    light = _light_get(url, _cookie_header(cookies), timeout_s)
                    row.light_requests = int(light["requests"])
                    row.light_bytes = int(light["bytes"])
                    row.light_ms = int(light["ms"])
                    row.light_status = light["status"]
                    row.light_error = light["error"]
                rows.append(row)
                if verbose:
                    print(
                        f"    {row.key:34s} render {row.requests_same_origin:3d}+"
                        f"{row.requests_third_party:<3d} light {row.light_requests:2d} "
                        f"×{row.ratio if row.ratio is not None else '—'} {row.render_ms:5d}ms {url}"
                    )
    finally:
        backend.close()
    return rows


def _summarise(rows: list[RenderRow]) -> dict[str, dict[str, Any]]:
    """Агрегат по ключу: медиана запросов рендера и отношения к лёгкому GET."""
    by_key: dict[str, list[RenderRow]] = {}
    for row in rows:
        by_key.setdefault(row.key, []).append(row)
    out: dict[str, dict[str, Any]] = {}
    for key, items in by_key.items():
        ok = [row for row in items if row.render_error is None]
        same = sorted(row.requests_same_origin for row in ok)
        ratios = sorted(row.ratio for row in ok if row.ratio is not None)
        out[key] = {
            "urls": len(items),
            "rendered": len(ok),
            "same_origin_median": same[len(same) // 2] if same else None,
            "same_origin_min": same[0] if same else None,
            "same_origin_max": same[-1] if same else None,
            "third_party_median": (
                sorted(row.requests_third_party for row in ok)[len(ok) // 2] if ok else None
            ),
            "light_requests_median": (
                sorted(row.light_requests for row in ok)[len(ok) // 2] if ok else None
            ),
            "ratio_median": ratios[len(ratios) // 2] if ratios else None,
            "render_ms_median": (
                sorted(row.render_ms for row in ok)[len(ok) // 2] if ok else None
            ),
            "light_ms_median": (
                sorted(row.light_ms for row in ok)[len(ok) // 2] if ok else None
            ),
        }
    return out


def _print_table(summary: dict[str, dict[str, Any]]) -> None:
    print()
    print(
        f"{'ключ':34s} {'URL':>4} {'рендер':>12} {'light':>6} {'×':>6} "
        f"{'рендер ms':>10} {'light ms':>9}"
    )
    for key, row in summary.items():
        same = row["same_origin_median"]
        spread = (
            f"{same} [{row['same_origin_min']}…{row['same_origin_max']}]"
            if same is not None
            else "—"
        )
        print(
            f"{key:34s} {row['rendered']:4d} {spread:>12} {row['light_requests_median'] or 0:6d} "
            f"{row['ratio_median'] or 0:6.2f} {row['render_ms_median'] or 0:10d} "
            f"{row['light_ms_median'] or 0:9d}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--package", required=True, help="каталог пакета (domain_profiles.yaml)")
    parser.add_argument("--state-db", required=True, help="durable state.db (выборка URL)")
    parser.add_argument("--keys", nargs="*", default=None, help="ключи профиля (дефолт — fetch: browser)")
    parser.add_argument("--per-key", type=int, default=DEFAULT_PER_KEY, help="URL на ключ")
    parser.add_argument("--timeout", type=float, default=30.0, help="таймаут лёгкого GET, с")
    parser.add_argument("--out", default=None, help="файл JSON-наблюдения")
    parser.add_argument("--verbose", action="store_true", help="печатать каждый адрес")
    args = parser.parse_args(argv)

    profiles = load_domain_profiles(Path(args.package) / "domain_profiles.yaml")
    keys = args.keys or [
        key
        for key, profile in profiles.items()
        if str(getattr(profile, "fetch", "")) == "browser"
    ]
    missing = [key for key in keys if key not in profiles]
    if missing:
        print(f"ключи вне профиля: {missing}", file=sys.stderr)
        return 2

    print(f"ключей: {len(keys)}; URL на ключ: {args.per_key}")
    rows = measure(
        profiles=profiles,
        keys=keys,
        db=Path(args.state_db),
        per_key=args.per_key,
        timeout_s=args.timeout,
        verbose=args.verbose,
    )
    summary = _summarise(rows)
    _print_table(summary)
    if args.out:
        payload = {
            "instrument": "scripts/measure_render_requests.py",
            "package": str(args.package),
            "state_db": str(args.state_db),
            "per_key": args.per_key,
            "summary": summary,
            "urls": [row.as_dict() for row in rows],
        }
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"\nнаблюдение: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
