# coHound — архитектура: реестр зависимостей

**Документ:** `docs/architecture/05-dependency-stack.md`
**Статус:** реестр решений (снимок 2026-10-08)
**Опирается на:** `00-overview.md` (AD1, AD10), `01-modules-and-contracts.md` (порты/адаптеры)
**Источник истины:** `cohound-spec-v2.md` §3.2. При расхождении — регламент главнее.

Назначение: зафиксировать **какие внешние пакеты берём, какие откладываем и какие запрещены**,
с обоснованием и лицензией. Реестр — вход для «пин+лицензии» M0.
Версии/лицензии/`requires_python` — из первичных источников (PyPI JSON API, GitHub API, офф. docs)
на 2026-10-08.

Легенда вердикта: **a** — брать (MVP) · **b** — кандидат/опционально · **c** — только идея ·
**d** — не брать. Пометки достоверности: ✅ подтверждено · 🟡 частично · ❓ не подтверждено.

---

## 1. Базлайн окружения: Python 3.14

**Решение (2026-10-08): базлайн проекта — Python 3.14** (не 3.11). Обоснование: greenfield,
контролируемое развёртывание (свой контур), один разработчик; базлайн 3.14 убирает матрицу версий и
legacy-шимы и совпадает с целевым окружением. Условие допустимости — **все зависимости стека имеют
готовые колёса под `cp314`** (или `abi3`/`py3`, forward-совместимые) — проверено (ниже).

**Матрица колёс под 3.14 (PyPI JSON API, 2026-10-08):**

| Тип колеса | Пакеты | Следствие |
|---|---|---|
| `cp314`-колёса | `numpy`, `torch`, `scipy`, `scikit-learn`, `pydantic-core`, `lxml`, `pillow`, `cryptography`, `regex`, `cffi`, `markupsafe`, `pyyaml`, `aiohttp`(+`multidict`/`yarl`/`frozenlist`/`propcache`), `greenlet`, `psutil`, `msgpack`, `zstandard`, `brotli`, `lz4`, `tantivy`, `nh3`, `curl-cffi`, `faiss-cpu`, `docling-parse` | ✅ ставятся из wheel без сборки |
| `abi3` (forward-compat) | `tokenizers`, `safetensors` (оба `cp310-abi3`), `charset-normalizer` | ✅ abi3-колёса работают на 3.14 |
| `py3-none-*` (Python-независимые) | `rtree`, `twisted` (чистый Python), `scrapy`, `trafilatura`, `docling-slim`, `html5lib`, `model2vec` | ✅ |

**Отсеяно базлайном 3.14:** `unstructured` (`<3.14`) → d; `outlines` (`<3.14`) → c.
`vllm` (`<3.15`) / `sglang` — **не наша зависимость**: возможные движки инференса в корпоративном
облаке; coHound потребляет модели по [OI]-совместимому API и в стек не входит.

**Следствие:** тяжёлые ML-зависимости эмбеддинг-контура (`torch`) не обязаны разделять venv с CLI.
CLI-ядро (`pydantic`+`pyyaml`+stdlib) ставится везде; индекс-контур — изолированное окружение/образ,
а LLM-инференс вообще внешний (корп. облако, §2.3). Это согласуется с **AD1** (commodity за портами)
и A1.

**Проверено резолвом и установкой на Python 3.14 (2026-10-08):** все лёгкие extras (ядро + `retrieval`,
`crawl`, `extract`, `safety`, `ops`, `dev`, `ann`, `fallback`, `embed-lite`) разрешаются и
устанавливаются; `torch`/`sentence-transformers` (`embed`) — тоже (колёса `cp314` есть). Полный
совместный набор — 137 пакетов, без torch по умолчанию. `extract-pdf` намеренно тяжёлый (torch+CUDA).
`requires-python = ">=3.14"` в `pyproject.toml`; CI резолвит на 3.14.

**Escape hatch.** Если появится вынужденно старый целевой хост (без возможности поставить 3.14 через
`uv`/контейнер), понижение базлайна до 3.13/3.12 — точечная правка `requires-python` после проверки
колёс под понижённую версию; код не должен использовать 3.14-only синтаксис инкрементально
(фиксируется по мере появления).

**Находка (docling):** пакет `docling` = ровно `docling-slim[standard]` и тянет
**torch+torchvision+CUDA** (~гигабайты). Для Office (DOCX/PPTX/XLSX) это не нужно:
`docling-slim[format-office]` ставит только python-docx/python-pptx/openpyxl/pylatexenc.
`docling-slim` предоставляет namespace-импорт `docling` (`docling/document_converter.py`) —
адаптер `extract_document.py` менять не нужно. Разделены extras `extract` (лёгкий) и
`extract-pdf` (тяжёлый, отдельное окружение).

---

## 2. Реестр по категориям

### 2.1. Ядро (обязательно, уже принято)

| Пакет | Версия | Лицензия | Вердикт | Зачем |
|---|---|---|---|---|
| `pydantic` | 2.13.x | MIT ✅ | **a** | схемы §4.2/4.3/4.5, инварианты, `extra="forbid"` |
| `pyyaml` | 6.0.x | MIT ✅ | **a** | `run.yaml`, `package.yaml`, `cohort.yaml` |
| `numpy` | 2.5.x | BSD-3 ✅ | **a** | `.npy`-шарды, brute-force cosine |
| `csv`, `argparse`, `sqlite3`, `json` | stdlib | PSF | **a** | отчёты, CLI, хранилище, сериализация |

### 2.2. Ретривал: лексика и вектора

| Пакет | Версия | Лицензия | Вердикт | Обоснование |
|---|---|---|---|---|
| `tantivy` | 0.26.x | MIT ✅ | **a** | актуальное имя официальных Python-биндингов (Rust, BM25). **НЕ `tantivy-py`** — это старое альфа-имя (0.11.0-rc.7) → d |
| `faiss-cpu` | 1.15.x | MIT ✅ | **b** | следующий шаг ANN, когда product-партиции перестают влезать в RAM: 3M×1024 fp32 ≈ 12.3 ГБ. `.npy` остаётся source of truth |
| `usearch` | 2.26.x | Apache-2.0 ✅ | **b** | лёгкая альтернатива ANN (3 зависимости) |
| `qdrant-client` | 1.19.x | Apache-2.0 ✅ | **c** | требует сервиса; отложено по §14 |
| `chromadb` | 1.5.x | Apache-2.0 ✅ | **d** | 31 зависимость (onnxruntime, opentelemetry, uvicorn) — против A1 |
| `hnswlib` | 0.8.0 | Apache-2.0 ✅ | **c** | релиз 2023, вялый; проигрывает faiss/usearch |
| `bm25s` | 0.3.x | MIT ✅ | **b** | быстрый BM25 на numpy, но тянет `numba` (JIT/LLVM) — против минимализма |
| `rank-bm25` | 0.2.2 | Apache-2.0 ✅ | **c** | только юнит-тесты на крошечных корпусах |
| `whoosh` / `pylucene` | — | BSD-2 / Apache-2.0 | **d** | whoosh мёртв (2016); pylucene требует Java/JCC |

⚠️ **Проверка RU/EN-стемминга tantivy на retrieval-golden** (§11.2): встроенный стеммер ограничен;
для русского может потребоваться `snowballstemmer`/`PyStemmer` на индексации/запросе. 🟡

### 2.3. Эмбеддинги и LLM-инференс

| Пакет | Версия | Лицензия | Вердикт | Обоснование |
|---|---|---|---|---|
| `sentence-transformers` | 6.1.x | Apache-2.0 ✅ | **a** | инференс bge-m3 / multilingual-e5, локально |
| `BAAI/bge-m3` | — | MIT ✅ | **a** | **основной кандидат** (приоритет RU, EN хорошо): 8192 контекста, dense(+sparse/colbert), 33.7M загрузок HF |
| `intfloat/multilingual-e5-large` | — | MIT ✅ | **b** | альтернатива (512 контекста, нужны префиксы query:/passage:) |
| `deepvk/USER-bge-m3` | — | Apache-2.0 ✅ | **b** | RU-специализированный файнтюн bge-m3 (**только ru**, 86★) — кандидат R9/R10, если RU-recall надо усилить; EN просядет → нужен билингвальный напарник |
| `ai-forever/ru-en-RoSBERTa` | — | MIT ✅ | **b** | ru+en, MIT — альтернативный билингвальный кандидат R9/R10 |
| `model2vec` | 0.9.x | MIT ✅ | **b** | лёгкий fallback без torch (статичные эмбеддинги, CPU); качество RU 🟡 — как абляция |
| `FlagEmbedding` | 1.4.x | MIT ✅ | **d** | training-тулкит (deepspeed, flash-attn); для инференса избыточен |
| `llama-cpp-python` | 0.3.x | MIT ✅ | ~~**b**/**a**~~ | **снято (2026-10, см. `06` §5.W9/§5.13):** офлайн-fallback для тестов порта не реализован и вытеснен [OI]-совместимым LLM-клиентом внутреннего периметра (`core/adapters/llm_local.py`). Строка сохранена как исторический след решения, конвейер путь не использует. ⚠️ PyPI отдаёт **только sdist** (без wheels) → потребовал бы C/C++/CMake-тулчейн; build-free альтернатива — first-party `llama.cpp` `llama-server` |
| `transformers` | 5.19.x | Apache-2.0 ✅ | **c** | не для сервинга; приходит через ST |
| `text-generation-inference` | — | Apache-2.0 | **d** | **архивирован 2026-03** ✅ — не строить |

> **Облачный backend — не наша зависимость.** `vLLM`/`SGLang` — лишь *возможные* движки инференса
> на стороне корпоративного облака. coHound **не ставит и не развёртывает** их: модели потребляются
> по [OI]-совместимому API (endpoint `api`); выбор и версия движка — забота облака (commodity по A2).

**Constrained decoding:** схема ячейки (`status/justification/evidence[]`) генерируется из
`pydantic`-модели → JSON Schema → grammar-constrained decoding **на стороне облачного endpoint**
(конкретный движок — backend корпоративного облака). Отдельная зависимость не нужна.

| Пакет | Вердикт | Обоснование |
|---|---|---|
| `xgrammar` | **—** (на стороне облака) | прямой вызов из кода не требуется; решает endpoint |
| `instructor` | **b** | клиентские ретраи поверх OpenAI-совместимого API; у нас своя pydantic-валидация §10 |
| `outlines` | **c** | `<3.14` + тянет SDK провайдеров |
| `guidance`, `lm-format-enforcer` | **c** | избыточны при серверном xgrammar |
| `jsonformer` | **d** | мёртв (2023) |

**Reranker (опц.):** порт `reranker` есть, по умолчанию выключен. Кандидат — `bge-reranker-v2-m3`
(Apache-2.0 ✅) через `sentence-transformers` CrossEncoder → **b**.

### 2.4. Сбор корпуса: HTML/PDF/Office

| Пакет | Версия | Лицензия | Вердикт | Обоснование |
|---|---|---|---|---|
| `trafilatura` | 2.3.x | Apache-2.0 ✅ | **a** | main-content → markdown, заголовки/списки/таблицы |
| `docling-slim[format-office]` | 2.135.x | MIT ✅ | **a** | DOCX/PPTX/XLSX нативно (python-docx/-pptx/openpyxl), **без torch**. `rtree` для Office **не нужен** (нужен только PDF-пути spatial/reading-order) и импорт `DocumentConverter` без него работает |
| `docling-slim[standard]` | 2.135.x | MIT ✅ | **a** (отдельное окружение) | PDF layout+tables; **тянет torch+torchvision+CUDA**; обязателен для PDF-контура (блокер M0) |
| `lxml` | 6.1.x | BSD-3 ✅ | **a** (транзитивно) | уже в дереве trafilatura/Scrapy |
| `openpyxl` | 3.1.x | MIT ✅ | **a** | XLSX-отчёты; Office-бэкенд docling |
| `python-docx` / `python-pptx` | 1.2.x / 1.0.x | MIT ✅ | **b** (через docling) | отдельно не нужны |
| `markitdown` (Microsoft) | 0.1.x | MIT ✅ | **b** | fallback экзотики (EPUB/msg/xls); PDF не layout-aware |
| `pypdf` / `pdfplumber` | 6.19.x / 0.11.x | BSD-3 / MIT ✅ | **b** | лёгкая работа с PDF без layout |
| `selectolax` / `beautifulsoup4` | 1.0.0 / 4.15.x | MIT ✅ | **b** | fallback-парсинг и извлечение иерархии `h1–h6` под `heading_path` |
| `readability-lxml` / `markdownify` | 0.9 / 1.2.x | Apache-2.0 / MIT ✅ | **b** | fallback main-content / HTML→MD |
| `camelot-py` / `tabula-py` | 2.0 / 2.10 | MIT ✅ | **c** | тяжёлые (opencv+pandas / JVM); таблицами занимается docling |
| `PyMuPDF` | 1.28.x | **AGPL** ✅ | **d** | AGPL исключён (курс проекта) |
| `unstructured` | 0.27.x | Apache-2.0 ✅ | **d** | `<3.14` + 24+ зависимостей + телеметрия on-by-default; OSS-таблицы слабые (0.426 vs 0.866) |
| `html2text` | 2025.4.x | **GPL-3.0** ✅ | **d** | GPL несовместим с permissive-курсом; закрывается `markdownify` |

⚠️ **docling offline:** веса нужны **только PDF-контуру** (Office — без ML-моделей). Штатный
офлайн-механизм — pipeline `artifacts_path` / env `DOCLING_ARTIFACTS_PATH`, заполняемый
`docling-tools models download` (или `download_models()`); `HF_HUB_OFFLINE=1` — валидный, но
вспомогательный hub-level guard, а не основной переключатель (§7).
**Лицензии весов** ❓ — проверить перед редистрибуцией (лицензия кода ≠ лицензия весов).

### 2.5. Краулинг

| Пакет | Версия | Лицензия | Вердикт | Обоснование |
|---|---|---|---|---|
| `Scrapy` | 2.19.x | BSD-3 ✅ | **a** | `DOWNLOAD_SLOTS` per-domain, AutoThrottle, HttpCache |
| `scrapy-playwright` | 0.0.48 | BSD-3 ✅ | **a** (с пином + smoke) | per-request `playwright=True` для JS-доменов. **V5: зрелый де-факто стандарт** (org `scrapy-plugins`, активные PR Zyte 2026, py3.14 поддержан PR #375); ярлык `0.0.x` = конвенция автора (classifier Beta), не заброшенность |
| `playwright` | 1.63.x | Apache-2.0 ✅ | **a** (пин + smoke) | headless-рендер; **два пути краула:** движок `scrapy-playwright` и **прямой** sync-API `BrowserCrawler` (`adapters/crawl_browser.py`, honest/hybrid, без Scrapy/Twisted; выбор — сменой адаптера за портом `CrawlerPort`, A2). Нужен бинарь `playwright install chromium`; браузеры качаются с CDN — в закрытом контуре зеркало (`PLAYWRIGHT_DOWNLOAD_HOST`), изоляция — `PLAYWRIGHT_BROWSERS_PATH` |
| `httpx` | 0.28.x | BSD ✅ | **a** | общий HTTP-клиент (search API, tests). **Watch-item (2026-10):** апстрим-активность снизилась; Pydantic ведёт преемник `httpx2` (Scrapy 2.18+ использует его). `httpx` ещё не EOL; миграция — отдельное решение по триггеру (§14). `httpx2` — кандидат в этот реестр |
| `curl_cffi` | 0.16.x | MIT ✅ | **b** | «дешёвый первый ярус» для антибот-доменов без JS (имитация TLS/JA3); не замена браузеру |
| `trafilatura`/`courlan` (fetch) | 2.3.x | Apache-2.0 ✅ | **b** | простой политкорректный fetch |
| `crawl4ai` | 0.9.x | Apache-2.0 ✅ | **c** | LLM-first, тянет litellm/patchright/stealth — избыточно (A1) |
| `crawlee-python` | 1.10.x | Apache-2.0 ✅ | **c** | активен, но **нет интеграции со Scrapy** — это замена фреймворка целиком (ломает A2) |
| `scrapy-selenium` / `scrapy-splash` | 0.0.7 / 0.11.1 | BSD/Apache | **d** | selenium мёртв (релиз 2019); splash требует внешнего сервера (Docker/Lua) |
| `selenium` | 4.50.x | Apache-2.0 ✅ | **c** | legacy-рендер |
| `firecrawl` | — | **AGPL-3.0** | **d** | AGPL + облачный дефолт |

⚠️ **Безопасность (важно, ✅):** при встраивании Scrapy **как библиотеки** его дефолт —
`ROBOTSTXT_OBEY = False`. Нужно **явно выставить `True`** (этическая норма §10, курс на robots-дефолт).
robots.txt-запрос оставлять на дефолтном HTTP-handler (не ставить ему `playwright`-meta).

**V5 — зрелость `scrapy-playwright` и оговорки (2026-10-08):**
- **Незакрытые прогоны требуют настройки:** утечка JS-heap Node (upstream) → `NODE_OPTIONS=--max-old-space-size=…`;
  `playwright_include_page=True` без закрытия Page вешает краул → всегда errback-close; concurrency>1 требует
  `--reactor=asyncio`. Плюс согласовать `PLAYWRIGHT_MAX_CONTEXTS`/`PLAYWRIGHT_MAX_PAGES_PER_CONTEXT` с `CONCURRENT_REQUESTS_PER_DOMAIN`.
- **`DOWNLOAD_SLOTS` не затрагивается** (0 issue) — политика на домен остаётся за Scrapy core.
- **Антибот плагин не решает** — профили контекстов (UA/прокси) + `curl_cffi` как первый ярус.
- **Bus-factor ≈1** (elacuesta, 344 из ~355 коммитов), но проект в официальной org; изоляция за портом
  `crawler` делает замену сменой адаптера, не переписыванием (A2).
- **Совместимость 1.63/2.19 не подтверждена мейнтейнером** (оба вышли позже релиза 0.0.48; upstream CI пинит
  playwright 1.57) → **обязательный smoke-тест** (plain render + download + firefox-контекст) перед пином;
  фолбэк при провале — `playwright==1.57`.
- ✅ **Smoke-тест пройден (2026-10-08):** `tests/smoke_scrapy_playwright.py` на связке
  Scrapy 2.19.0 + scrapy-playwright 0.0.48 + playwright 1.63.0, браузеры chromium и firefox
  (Python 3.14). Проверены: HTTP-запрос не видит JS, рендер видит JS (chromium и firefox).
  **Пин `playwright==1.63` подтверждён; фолбэк на 1.57 не нужен.**
- ℹ️ **Неочевидные факты, вскрытые тестом (учесть в адаптере краула):**
  (1) в **Scrapy 2.19 у `Spider` удалён `start_requests`** — стартовые запросы задаются
  `async def start()` (иначе паук завершается с 0 запросов); (2) одинаковые URL в стартовых
  запросах **дедуплицируются** — для параллельных запросов одного URL нужен `dont_filter=True`;
  (3) рендер-снимок берётся до отложенного JS — нужен `playwright_page_methods`
  (`PageMethod("wait_for_function", …)`).

### 2.6. Безопасность контента / санитайзинг (§10)

| Пакет | Версия | Лицензия | Вердикт | Обоснование |
|---|---|---|---|---|
| `nh3` | 0.3.x | MIT ✅ | **a** | современный (Rust/ammonia), 0 зависимостей; против prompt-injection |
| `defusedxml` | 0.7.x | PSF ✅ | **a** | безопасный XML (уже транзитивно) |
| `bleach` | 6.4.x | Apache-2.0 ✅ | **d** | **EOL с 2026-06-05**, релизов безопасности не будет |

⚠️ Санитайзинг не должен менять текстовые узлы — иначе ломается строгая проверка
«цитата — подстрока чистого текста чанка» (§10/C.2). Чистим структуру, сохраняем символы;
сырые байты — в `corpus_raw` (I19).

### 2.7. Утилиты, логи, тесты

| Пакет | Версия | Лицензия | Вердикт | Обоснование |
|---|---|---|---|---|
| `tenacity` | 9.2.x | Apache-2.0 ✅ | **a** | ровно под §7 «retry ≤2 → failed» и HTTP-адаптеры |
| `structlog` | 26.1.x | Apache-2.0 ✅ | **a** | структурные JSON-логи для воспроизводимого прогона |
| `pytest` | 9.1.x | MIT ✅ | **a** | §11.6 CI (сейчас не установлен) |
| `hypothesis` | 6.168.x | MPL-2.0 ✅ | **a** | property-based проверка инвариантов I1–I23 и парсеров |
| `respx` | 0.23.x | BSD-3 ✅ | **a** | мок `httpx` для LLM/SERP-адаптеров без сети |
| `pytest-cov` | 7.1.x | MIT ✅ | **b** | покрытие |
| `pytest-xdist` | 3.8.x | MIT ✅ | **b** | параллельный прогон (`-n auto --dist worksteal`); CI ускоряется ~2x. Транзитивно тянет `execnet` (MIT) |
| `rich` / `typer` / `click` | 15.0 / 0.27 / 8.5 | MIT / BSD-3 ✅ | **b** | `argparse` хватает для MVP (A1); typer — при росте подкоманд |
| `pydantic-settings` | 2.15.x | MIT ✅ | **b** | конфиг — YAML, поэтому опц.; **не ставить `[all]`** (boto3/azure) |
| `orjson` | 3.13.x | Apache-2.0 ✅ | **b** | скорость сериализации; stdlib `json` достаточен |
| `tqdm` | 4.70.x | MPL-2.0 AND MIT ✅ | **b** | прогресс длинных фаз |
| `xlsxwriter` | 3.2.x | BSD-2 ✅ | **b** | богаче форматирование отчётов, чем openpyxl |
| `hishel` | 1.4.x | BSD ✅ | **b** | RFC-9111 кэш поверх httpx (свой content-addressed уже есть, R7) |
| `pandas` | 3.0.x | BSD-3 ✅ | **d** | матрицу/скоры считает `domain/scoring.py`; отчёты — openpyxl+csv |
| `python-slugify` / `Unidecode` | 9.1 / 1.4 | MIT / **GPL-2.0+** 🟡 | **d** | транзитив GPL; свой `sanitize_slug` уже есть |
| `requests-cache` | 1.3.x | BSD-2 ✅ | **c** | ориентирован на `requests`, у нас httpx |

### 2.8. Отчёты

`openpyxl` (MIT, **a**) + stdlib `csv` (**a**); `xlsxwriter` (**b**) — если нужно форматирование.

### 2.9. Транспорт DMZ-шва (07)

Профиль `split` связывает ядро (Z0) и DMZ (Z1) через application-layer boundary. Дополнительных
тяжёлых зависимостей не требуется:

| Пакет | Вердикт | Роль |
|---|---|---|
| `httpx` | **a** | клиент remote-прокси (core→edge); уже в стеке |
| stdlib `http.server` / ASGI-микрофреймворк | **a** | сервер gateway в `edge/`; для MVP достаточно stdlib/`uvicorn`+`Starlette`(опц.) |
| TLS/mTLS | stdlib `ssl` | взаимная аутентификация ядра↔edge (сертификаты вне git); **без certificate pinning** — чтобы не блокировать корп. TLS-инспекцию (07 §8.1) |
| `jsonschema` | **b** | валидация запроса/ответа шва по опубликованной JSON-Schema (07 §8.3); можно stdlib-руками |
| `pyjwt` / `authlib` | **b** | OIDC/JWT-идентичность шва, если корп. IDP требует bearer-токены вместо mTLS (07 §8.2) |

`edge/` — отдельное окружение (тот же `crawl`+`extract` extras), сетевая изоляция — на уровне L7-фаервола
(политики — `07` §5), не пакета. Сервер gateway — тонкий; допускается `Starlette`/`uvicorn` (Apache-2.0/MIT)
как **b**, но не обязателен (A1). Контракты шва публикуются как **OpenAPI + JSON-Schema** (`schema_version`)
для IDP/DLP/IDS-политик — `07` §8.

---

## 3. Явно отклонённые: оркестраторы и hosted-парсеры

### 3.1. LangChain / LangGraph / LlamaIndex — **d / d / c**

| Пакет | Версия | Лицензия | Вердикт |
|---|---|---|---|
| `langchain` | 1.4.x | MIT | **d** |
| `llama-index` | 0.14.x | MIT | **d** |
| `langgraph` | 1.2.x | MIT | **c** (только идея) |

**Почему не берём в core:**
1. **A2** — moat в методологии, не в AI-пайплайне; фреймворк навязывает свои абстракции поверх
   evidence-модели и `source_id`.
2. **Дублирование портов** — `chunker`/`index_builder`/`search`/`store`/`crawler`/`extractor` уже
   спроектированы (`01-modules-and-contracts.md`).
3. **Инварианты** — I1–I23 должны обеспечиваться Pydantic + SQL-триггерами; скрытая оркестрация
   мешает сквозному аудиту и воспроизводимости (`run_manifest.yaml`).
4. **I18** — LangChain-tools легко «протаскивают» веб-агентов в оценщик; прямой риск инварианта.
5. **Тяжёлые транзитивные зависимости** — 22 (langchain) / core+openai+nltk (llama-index).

**LangGraph** концептуально близок к идее durable-конвейера Ш1–Ш9, но у coHound уже есть
персистентность (SQLite `runs.state`) и явный `pipeline/` на чистом Python; LangGraph добавил бы
дублирующий checkpoint-слой и `langchain-core`. → оставляем как **источник идей**, не зависимость.

### 3.2. Облачные/hosted-парсеры — **все d** (РФ-контур)

Требование «данные не покидают периметр» **несовместимо с SaaS-моделью**: документ/URL уходит на
чужой сервер. Проверено:

| Сервис | Модель | Данные наружу | Вывод |
|---|---|---|---|
| **LlamaParse** (LlamaIndex) | Cloud SaaS (Global/EU) | **Да** | **d**; «local cloud deployment» — только Enterprise, проприетарно 🟡 |
| unstructured.io Transform API | Cloud | **Да** | **d** ($0.015/стр) |
| Firecrawl Cloud | Cloud (US) | **Да** | **d**; свой хостинг — AGPL-3.0 |
| Jina Reader | SaaS `r.jina.ai` | **Да** | SaaS **d**; OSS-хостинг Apache-2.0, но **Node.js** и дублирует наш стек → **c** |
| Reducto | Cloud | **Да** | **d**; VPC/air-gapped — проприетарно, вне РФ-юрисдикции 🟡 |

Итог: **LlamaParse в coHound не применяется** — не по качеству, а по контуру данных. Локальную
роль «облачного парсера» закрывает docling (+trafilatura), полностью офлайн.

---

## 4. Предлагаемый набор MVP

**Runtime (`a`):** `pydantic` · `pyyaml` · `numpy` · `tantivy` · `trafilatura` ·
`docling-slim[format-office]` · `Scrapy`+`scrapy-playwright`+`playwright` · `httpx` · `nh3` ·
`defusedxml` · `openpyxl` · `tenacity` · `structlog` · stdlib (`sqlite3`, `csv`, `argparse`, `json`).
Собирается как extras в `pyproject.toml`: `retrieval`, `crawl`, `extract`, `safety`, `ops`.

**Отдельные окружения (не в CLI-venv):**
- **PDF layout:** `docling-slim[standard]` (torch+CUDA) — extra `extract-pdf`, блокер M0.
- **Индекс-контур:** `sentence-transformers` (+torch) — extra `embed`, изолированный venv/образ.
- **LLM-инференс:** модели корпоративного облака по [OI]-совместимому API — **вне наших окружений**
  (не ставим и не развёртываем; `vLLM`/`SGLang` — возможный backend облака, §2.3).
- **Опц. CPU-инференс:** ~~`llama-cpp-python` (GGUF) для тестов порта~~ — **снято** (не реализовано;
  вытеснено [OI]-совместимым клиентом внутреннего периметра, см. `06` §5.W9/§5.13).

**Dev (`a`):** `pytest` · `pytest-xdist` · `hypothesis` · `respx` · `pytest-cov`.

**Опционально (`b`):** `faiss-cpu`/`usearch` (ANN при росте), `model2vec` (лёгкая эмбеддинг-ветка),
`markitdown`/`pypdf`/`pdfplumber` (fallback-экстракция), `selectolax`/`bs4`/`markdownify`,
`bge-reranker-v2-m3` (reranker), `rich`/`typer`, `orjson`/`tqdm`, `xlsxwriter`, `hishel`.

**Не брать (`d`):** LangChain, LlamaIndex, все hosted-парсеры, `unstructured`, `PyMuPDF`,
`html2text`, `bleach`, `chromadb`, `whoosh`, `pylucene`, `jsonformer`, TGI, `pandas`,
`Unidecode`/`python-slugify`, `FlagEmbedding` (для инференса).

---

## 5. Версионные границы (связь с §13)

Смена **эмбеддинг-модели**, **чанкера** или **профиля ретривала** — версионная граница:
bump `methodology_version`, обязателен `golden-full` (§13). Поэтому `sentence-transformers` и
`tantivy` пинятся в `run_manifest.yaml` вместе с моделью/стеммером/RRF-параметрами.

---

## 6. Открытые вопросы верификации (вход для M0)

| # | Вопрос | Метод проверки |
|---|---|---|
| V1 | RU/EN-стемминг `tantivy` из Python-биндингов | ✅ **закрыт (2026-10-08):** русский стеммер и стоп-слова доступны нативно — `Filter.stemmer("russian")`, `Filter.stopword("russian")` (tantivy 0.26.2); end-to-end индекс+поиск на смешанном RU/EN корпусе прошёл («экспорт» находит «Экспорт»/«экспорта»). Внешний `snowballstemmer` не нужен |
| V2 | Колёса `torch`/`sentence-transformers` под Python 3.14 | ✅ **закрыт (2026-10-08):** `torch 2.14.1` и `sentence-transformers 6.1.0` имеют колёса `cp314`; резолв `uv` на 3.14 прошёл. См. `pyproject.toml` |
| V3 | Выбор RU/EN-эмбеддинга | 🟡 **решение:** RU — абсолютный приоритет, EN — очень хорошо; **основной — `BAAI/bge-m3`** (MIT, 8192 контекста). Подтверждение и сравнение с RU-файнтюнами (`deepvk/USER-bge-m3`, `ai-forever/ru-en-RoSBERTa`) — R9/R10 |
| V4 | docling offline-веса в контуре + лицензии весов | ⚠️ **механика уточнена:** штатный офлайн-механизм — `DOCLING_ARTIFACTS_PATH` / pipeline `artifacts_path`, заполняемый `docling-tools models download` (см. §7); `HF_HUB_OFFLINE=1` — вспомогательный guard; Office-форматы весов не требуют. Открыт аудит лицензий весов |
| V5 | `scrapy-playwright` 0.0.x на сложных антибот-доменах | ✅ **закрыт smoke-тестом (2026-10-08):** зрелый де-факто стандарт (org `scrapy-plugins`, BSD-3, py3.14). Smoke `tests/smoke_scrapy_playwright.py` пройден на Scrapy 2.19.0 + scrapy-playwright 0.0.48 + playwright **1.63.0** (chromium + firefox) → **оставляем с жёстким пином**; связка 1.63/2.19 подтверждена, фолбэк 1.57 не нужен. Изолируется за портом `crawler`. Альтернативы не берём (см. §2.5) |

---

## 7. V4 — офлайн-веса docling и лицензии моделей

**Суть.** docling — это не только код (MIT). Для PDF-конвейера он использует ML-модели
(layout-анализ, распознавание таблиц TableFormer, OCR), которые **скачиваются с Hugging Face
Hub при первом прогоне**. В РФ-контуре (данные/инфраструктура не выходят за периметр, возм.
air-gap) сетевого доступа к HF нет — либо загрузка упадёт, либо произойдёт нежелательное
обращение наружу.

**Механика (подтверждена в коде `docling 2.135.0`; штатный офлайн — через artifacts-путь):**
- штатный офлайн-switch — pipeline `artifacts_path` / env `DOCLING_ARTIFACTS_PATH`; бандл
  заполняется `docling-tools models download` (или `download_models()`);
- `HF_HUB_OFFLINE=1` (и `TRANSFORMERS_OFFLINE=1`) — валидный **hub-level** guard, но не основной
  переключатель Docling: он лишь запрещает обращения к HF-hub, полагаясь на локальный кэш;
- (внутренний путь загрузки) `docling/models/utils/hf_model_download.py` → `huggingface_hub.snapshot_download(repo_id=…, revision=…)`;
- дефолтные репозитории моделей — `docling-project/docling-models`, `docling-project/TableFormerV2`,
  `docling-project/CodeFormulaV2`; OCR/VLM-варианты — `ibm-granite/*`, `nvidia/nemotron-ocr-v2` и др.;
- часть моделей пинится по `revision` (напр. TableFormer `v2.3.0`) — плюс к воспроизводимости;
- кэш — стандартный HF (`HF_HOME`), полем `local_dir` можно задать свой путь.

**Статус:** ✅ план **подтверждён владельцем (2026-10-08), механика уточнена 2026-10-12**
(предзакачка бандла → `DOCLING_ARTIFACTS_PATH` + `HF_HUB_OFFLINE=1` как guard → revision-пины →
аудит лицензий весов).

**Что делать (план, закрывает V4):**
1. **Предзакачать** нужные веса (`docling-tools models download`) на машине с доступом к сети →
   положить в артефакт-бандл контура; указать путь через `DOCLING_ARTIFACTS_PATH`/`artifacts_path`.
2. Дополнительно выставить `HF_HUB_OFFLINE=1` (и `TRANSFORMERS_OFFLINE=1`) как **guard**: даже при
   неверно настроенном artifacts-пути HF-hub не выйдет в сеть. Это и тест «офлайн реально работает».
   (Office-форматы весов не требуют — офлайн из коробки.)
3. Указать кэш/пути через `HF_HOME`/`local_dir`; зафиксировать `revision` каждой модели в
   `run_manifest.yaml` (§13, воспроизводимость R7).
4. **Аудит лицензий весов** — лицензия кода (MIT) ≠ лицензия весов. Для каждой модели в бандле
   зафиксировать repo_id + revision + лицензию в `run_manifest.yaml` до редистрибуции.

**Область действия:** V4 касается **только PDF-контура** (`docling-slim[standard]`, extra
`extract-pdf`). Office-форматы (DOCX/PPTX/XLSX, extra `extract`, `docling-slim[format-office]`)
парсятся **без ML-моделей** — офлайн из коробки, веса не нужны. Поэтому блокер M0 по PDF
(тест на 3–5 мануалах, §3.2/§15) включает и проверку офлайн-бандла весов.
