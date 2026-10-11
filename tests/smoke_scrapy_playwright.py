#!/usr/bin/env python3
"""coHound — smoke-тест scrapy-playwright (V5, §3.2).

Проверяет связку Scrapy + scrapy-playwright + Playwright на реальном браузере,
изолированно (локальный HTTP-сервер, без внешней сети и антибота):

  1. JS-контент НЕ виден обычным HTTP-запросом (доказывает необходимость рендера);
  2. JS-контент ВИДЕН через scrapy-playwright (chromium) — рендер работает;
  3. (best-effort) рендер в контексте Firefox, если бинарь установлен;
  4. (``--multi``) несколько URL в ОДНОМ процессе через ``ScrapyCrawler``: два HTTP
     и один browser-рендер — авторитетная проверка «N URL в одном процессе без
     ``ReactorNotRestartable``», а также семантики кэша (HTTP кэшируется, browser — нет).

Режимы 1–3 запускают каждый браузер в отдельном процессе (Twisted-реактор не
перезапускаем). Режим ``--multi`` использует общий реактор процесса
(``core/adapters/crawl_scrapy.py``). Требуется Scrapy ≥2.16 на Python 3.14 (в 2.19 у
`Spider` удалён `start_requests`, поэтому используется `async def start()`).

Запуск (отдельное окружение, требует браузерных бинарей):
    pip install 'scrapy>=2.19' 'scrapy-playwright>=0.0.48' 'playwright>=1.63'
    playwright install chromium firefox
    python3 tests/smoke_scrapy_playwright.py            # режимы 1–3
    python3 tests/smoke_scrapy_playwright.py --multi    # режим 4

Коды выхода: 0 — пройдено; 1 — провал; 2 — окружение не готово (skip).
Это ручной/full-CI тест: не часть tests/selfcheck.py (тот — stdlib+pydantic, без сети).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 8765
JS_TEXT = "JS-RENDERED-CONTENT-7f3a"
STATIC_TEXT = "STATIC-ONLY-1b2c"

PAGE_JS = f"""<!doctype html><html><body>
<div id="app">loading</div>
<script>
  setTimeout(function() {{
    document.getElementById('app').textContent = ['JS','RENDERED','CONTENT','7f3a'].join('-');
  }}, 50);
</script>
</body></html>"""

PAGE_PLAIN = f"""<!doctype html><html><body><p>{STATIC_TEXT}</p></body></html>"""


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        body = PAGE_JS if self.path.startswith("/js") else PAGE_PLAIN
        payload = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args) -> None:  # noqa: A002 - сигнатура базового класса
        return


def _start_server() -> ThreadingHTTPServer:
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), _Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def _run_browser(browser_type: str) -> dict[str, str]:
    """Прогон одного браузера (в текущем процессе). Возвращает {label: text}."""
    from scrapy import Request, Spider
    from scrapy.crawler import CrawlerProcess
    from scrapy_playwright.page import PageMethod

    results: dict[str, str] = {}

    class _SmokeSpider(Spider):
        name = "smoke"

        async def start(self):
            # dont_filter=True: /js запрашивается дважды (HTTP vs render) с одним URL;
            # иначе Scrapy дедуплицирует второй запрос и рендер-проверка не выполнится.
            yield Request(
                f"http://127.0.0.1:{PORT}/plain", callback=self._save,
                cb_kwargs={"label": "plain_http"}, dont_filter=True,
            )
            yield Request(
                f"http://127.0.0.1:{PORT}/js", callback=self._save,
                cb_kwargs={"label": "js_no_render"}, dont_filter=True,
            )
            yield Request(
                f"http://127.0.0.1:{PORT}/js",
                callback=self._save,
                cb_kwargs={"label": "js_render"},
                dont_filter=True,
                meta={
                    "playwright": True,
                    "playwright_include_page": False,
                    # ждём завершения JS (setTimeout) — иначе снимок берётся раньше инъекции
                    "playwright_page_methods": [
                        PageMethod(
                            "wait_for_function",
                            f"() => document.getElementById('app').textContent.includes('{JS_TEXT}')",
                            timeout=10000,
                        )
                    ],
                },
            )

        def _save(self, response, label: str) -> None:
            results[label] = response.text

    settings = {
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "PLAYWRIGHT_BROWSER_TYPE": browser_type,
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 15000,
        "ROBOTSTXT_OBEY": True,
        "LOG_LEVEL": os.environ.get("SMOKE_LOG_LEVEL", "ERROR"),
        "CONCURRENT_REQUESTS": 4,
    }
    # CrawlerProcess.start() запускает реактор; один браузер — один процесс.
    process = CrawlerProcess(settings=settings)
    process.crawl(_SmokeSpider)
    process.start()
    return results


def _run_browser_subprocess(browser_type: str) -> dict[str, str]:
    """Запускает один браузер в отдельном процессе, возвращает результаты."""
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tf:
        out_path = tf.name
    env = dict(os.environ)
    proc = subprocess.run(
        [sys.executable, __file__, "--browser", browser_type, "--out", out_path],
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
    )
    try:
        with open(out_path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError):
        data = {"_error": (proc.stderr or proc.stdout or "")[-400:]}
    finally:
        try:
            os.unlink(out_path)
        except OSError:
            pass
    return data


def _run_multi() -> int:
    """``--multi``: N URL (HTTP + browser) в ОДНОМ процессе через ``ScrapyCrawler``.

    Авторитетная проверка дефекта ``ReactorNotRestartable``: два HTTP-фетча и один
    браузерный рендер идут подряд в одном процессе/реакторе. Дополнительно
    проверяется семантика кэша R7: HTTP-страница кэшируется, browser — нет.

    Использует SSRF-допустимое DNS-имя ``localhost`` (литеральный ``127.0.0.1``
    был бы отклонён ``validate_url``). Возвращает 0/1.
    """
    from pathlib import Path

    from core.adapters.crawl_scrapy import CrawlError, ScrapyCrawler
    from core.ports.types import RenderMode
    from core.util.cache import ContentAddressedCache, http_key

    base = f"http://localhost:{PORT}"
    plain_url = f"{base}/plain"
    plain_url2 = f"{base}/plain?second=1"
    js_url = f"{base}/js"
    wait_for = (
        "() => document.getElementById('app').textContent.includes('" + JS_TEXT + "')"
    )

    print("== smoke scrapy-playwright (multi-URL, один процесс) ==")
    failures: list[str] = []

    with tempfile.TemporaryDirectory() as tmp:
        cache = ContentAddressedCache(tmp)
        # Профиль без `render`: режим берётся из аргумента fetch, а `wait_for`
        # подхватывается для browser-рендера (см. _profile_wait_for).
        crawler = ScrapyCrawler(
            cache=cache,
            domain_profiles={"localhost": {"wait_for": wait_for}},
        )
        try:
            plain = crawler.fetch(plain_url, RenderMode.HTTP)
            js = crawler.fetch(js_url, RenderMode.BROWSER)
            plain2 = crawler.fetch(plain_url2, RenderMode.HTTP)
        except CrawlError as exc:
            print(f"FAIL  краул упал (ReactorNotRestartable?): {exc}")
            return 1
        finally:
            crawler.close()  # останавливает общий реактор (idempotent)

        def check(label: str, content: bytes, needle: str) -> None:
            text = content.decode("utf-8", "replace")
            ok = needle in text
            mark = "  ok  " if ok else "FAIL  "
            print(f"{mark}{label}: {'содержит' if ok else 'не содержит'} {needle!r}")
            if not ok:
                failures.append(label)

        check("plain_http", plain.content, STATIC_TEXT)
        check("js_browser", js.content, JS_TEXT)
        check("plain_http_second", plain2.content, STATIC_TEXT)

        http_file = Path(cache.path_for("http", http_key(plain_url), ".bin"))
        js_file = Path(cache.path_for("http", http_key(js_url), ".bin"))
        if http_file.is_file():
            print("  ok  cache: HTTP-страница закэширована")
        else:
            print("FAIL  cache: нет файла кэша для HTTP-страницы")
            failures.append("cache_http")
        if not js_file.is_file():
            print("  ok  cache: browser-рендер НЕ закэширован")
        else:
            print("FAIL  cache: browser-рендер попал в кэш")
            failures.append("cache_browser")

    if failures:
        print("\nMULTI SMOKE FAILED: " + ", ".join(failures))
        return 1
    print("\nMULTI SMOKE PASSED")
    return 0


def main(argv: list[str]) -> int:
    if "--browser" in argv:  # дочерний процесс: один браузер
        browser_type = argv[argv.index("--browser") + 1]
        out_path = argv[argv.index("--out") + 1] if "--out" in argv else None
        results = _run_browser(browser_type)
        if out_path:
            with open(out_path, "w", encoding="utf-8") as fh:
                json.dump(results, fh)
        return 0

    try:
        import playwright  # noqa: F401
        import scrapy  # noqa: F401
        import scrapy_playwright  # noqa: F401
    except ImportError as exc:
        print(f"SKIP: окружение не готово ({exc}). См. docstring.")
        return 2

    if "--multi" in argv:  # режим 4: несколько URL в одном процессе
        srv = _start_server()
        try:
            return _run_multi()
        finally:
            srv.shutdown()

    srv = _start_server()
    try:
        res = _run_browser_subprocess("chromium")
        firefox = _run_browser_subprocess("firefox")
    finally:
        srv.shutdown()

    failures: list[str] = []

    def expect(results: dict[str, str], label: str, needle: str, should_contain: bool, optional: bool = False) -> None:
        if "_error" in results:
            if optional:
                print(f"  skip  {label}: браузер недоступен")
                return
            failures.append(f"{label}: {results['_error']}")
            print(f"FAIL  {label}: процесс браузера упал")
            return
        text = results.get(label)
        if text is None:
            msg = f"  skip  {label}: нет ответа" if optional else f"FAIL  {label}: нет ответа"
            print(msg)
            if not optional:
                failures.append(f"{label}: нет ответа")
            return
        has = needle in text
        ok = has == should_contain
        mark = "  ok  " if ok else "FAIL  "
        verb = "содержит" if has else "не содержит"
        print(f"{mark}{label}: {verb} {needle!r}{'' if ok else ' (ожидалось иное)'}")
        if not ok:
            failures.append(f"{label}: ожидалось should_contain={should_contain}, текст={text[:80]!r}")

    print("== smoke scrapy-playwright ==")
    expect(res, "plain_http", STATIC_TEXT, True)
    expect(res, "js_no_render", JS_TEXT, False)   # обычный HTTP не видит JS-инъекцию
    expect(res, "js_render", JS_TEXT, True)       # playwright/chromium видит
    expect(firefox, "js_render", JS_TEXT, True, optional=True)  # Firefox best-effort

    if failures:
        print("\nSMOKE FAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nSMOKE PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
