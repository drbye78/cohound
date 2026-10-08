# coHound — архитектура: реестр зависимостей

**Документ:** `docs/architecture/05-dependency-stack.md`
**Статус:** реестр решений (снимок 2026-10-08)
**Опирается на:** `00-overview.md` (D1, D10), `01-modules-and-contracts.md` (порты/адаптеры)
**Источник истины:** `cohound-spec-v2.md` §3.2. При расхождении — регламент главнее.

Назначение: зафиксировать **какие внешние пакеты берём, какие откладываем и какие запрещены**,
с обоснованием и лицензией. Реестр — вход для «пин+лицензии» M0 и для `THIRD_PARTY_NOTICES.md`.
Версии/лицензии/`requires_python` — из первичных источников (PyPI JSON API, GitHub API, офф. docs)
на 2026-10-08.

Легенда вердикта: **a** — брать (MVP) · **b** — кандидат/опционально · **c** — только идея ·
**d** — не брать. Пометки достоверности: ✅ подтверждено · 🟡 частично · ❓ не подтверждено.

---

## 1. Ограничение окружения: Python 3.14

В проекте **Python 3.14.6**. Это уже отсеивает часть популярных пакетов:

| Пакет | `requires_python` | Следствие |
|---|---|---|
| `unstructured` | `>=3.11,<3.14` ✅ | **не устанавливается на 3.14** → d |
| `outlines` | `<3.14` ✅ | **не устанавливается** → c (и не нужен, см. §5) |
| `torch` / `sentence-transformers` | совместимость с 3.14 🟡 | проверить колёса перед пином; при отсутствии — venv на 3.12/3.13 для индекс-контура |
| `vllm` | `<3.15` ✅ | совместим (но живёт в отдельном GPU-Docker, не в основном venv) |

**Следствие:** тяжёлые ML-зависимости (torch/vLLM/docling) не обязаны разделять venv с CLI.
CLI-ядро (`pydantic`+`pyyaml`+stdlib) ставится везде; индекс- и GPU-контуры —
изолированные окружения/образы. Это согласуется с D1 (commodity за портами) и A1.

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
| `qdrant-client` | 1.19.x | Apache-2.0 ✅ | **c** | требует сервиса; отложено по §15 |
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
| модели: `BAAI/bge-m3`, `intfloat/multilingual-e5-large` | — | MIT ✅ | **a** | RU/EN, длинный контекст; финал — R9/R10 (A3) |
| `model2vec` | 0.9.x | MIT ✅ | **b** | лёгкий fallback без torch (статичные эмбеддинги, CPU); качество RU 🟡 — как абляция |
| `FlagEmbedding` | 1.4.x | MIT ✅ | **d** | training-тулкит (deepspeed, flash-attn); для инференса избыточен |
| `vllm` | 0.31.x | Apache-2.0 ✅ | **a** | self-host сервер, встроенный xgrammar, reproducibility-режимы |
| `sglang` | 0.5.x | Apache-2.0 ✅ | **a** | альтернатива vLLM; per-request seed, day-0 поддержка целевых моделей |
| `llama-cpp-python` | 0.3.x | MIT ✅ | **b**/**a** | CPU/GGUF-путь (полезен без большого GPU и для интеграционных тестов порта) |
| `transformers` | 5.19.x | Apache-2.0 ✅ | **c** | не для сервинга; приходит через ST |
| `text-generation-inference` | — | Apache-2.0 | **d** | **архивирован 2026-03** ✅ — не строить |

**Constrained decoding:** схема ячейки (`status/justification/evidence[]`) генерируется из
`pydantic`-модели → JSON Schema → grammar-constrained decoding **на сервере** (`xgrammar` —
движок-по-умолчанию внутри vLLM/SGLang). Отдельная зависимость не нужна.

| Пакет | Вердикт | Обоснование |
|---|---|---|
| `xgrammar` | **a** (транзитивно, через сервер) | прямой вызов из кода не требуется |
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
| `docling` (+`docling-slim[format-office]`) | 2.135.x | MIT ✅ | **a** | PDF layout+tables; DOCX/PPTX/XLSX нативно (бэкенды python-docx/-pptx/openpyxl) |
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

⚠️ **docling offline:** при первом PDF-прогоне docling подтягивает веса с Hugging Face 🟡 — в
РФ-контуре нужен предварительно закешированный артефакт (`HF_HUB_OFFLINE=1`).
**Лицензии весов** ❓ — проверить перед редистрибуцией (лицензия кода ≠ лицензия весов).

### 2.5. Краулинг

| Пакет | Версия | Лицензия | Вердикт | Обоснование |
|---|---|---|---|---|
| `Scrapy` | 2.19.x | BSD-3 ✅ | **a** | `DOWNLOAD_SLOTS` per-domain, AutoThrottle, HttpCache |
| `scrapy-playwright` | 0.0.48 | BSD-3 ✅ | **a** | per-request `playwright=True` для JS-доменов (зрелость 0.0.x 🟡) |
| `playwright` | 1.63.x | Apache-2.0 ✅ | **a** | headless-рендер; браузеры качаются с CDN — в закрытом контуре нужен зеркальный кэш |
| `httpx` | 0.28.x | BSD ✅ | **a** | общий HTTP-клиент (search API, тесты) |
| `trafilatura`/`courlan` (fetch) | 2.3.x | Apache-2.0 ✅ | **b** | простой политкорректный fetch |
| `crawl4ai` | 0.9.x | Apache-2.0 ✅ | **c** | LLM-first, тянет litellm/patchright/stealth — избыточно (A1) |
| `selenium` | 4.50.x | Apache-2.0 ✅ | **c** | legacy-рендер |
| `firecrawl` | — | **AGPL-3.0** | **d** | AGPL + облачный дефолт |

⚠️ **Безопасность (важно, ✅):** при встраивании Scrapy **как библиотеки** его дефолт —
`ROBOTSTXT_OBEY = False`. Нужно **явно выставить `True`** (этическая норма §10, курс на robots-дефолт).

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
| Firecrawl Cloud | Cloud (US) | **Да** | **d**; self-host — AGPL-3.0 |
| Jina Reader | SaaS `r.jina.ai` | **Да** | SaaS **d**; OSS self-host Apache-2.0, но **Node.js** и дублирует наш стек → **c** |
| Reducto | Cloud | **Да** | **d**; VPC/air-gapped — проприетарно, вне РФ-юрисдикции 🟡 |

Итог: **LlamaParse в coHound не применяется** — не по качеству, а по контуру данных. Локальную
роль «облачного парсера» закрывает docling (+trafilatura), полностью офлайн.

---

## 4. Предлагаемый набор MVP

**Runtime (`a`):** `pydantic` · `pyyaml` · `numpy` · `tantivy` · `trafilatura` · `docling`
(+`docling-slim[format-office]`) · `Scrapy`+`scrapy-playwright`+`playwright` · `httpx` · `nh3` ·
`defusedxml` · `openpyxl` · `tenacity` · `structlog` · stdlib (`sqlite3`, `csv`, `argparse`, `json`).

**Отдельные окружения (не в CLI-venv):**
- **Индекс-контур:** `sentence-transformers` (+torch) — изолированный venv/образ.
- **GPU-инференс:** `vLLM`/`SGLang` — отдельный Docker-образ на GPU.
- **Опц. CPU-инференс:** `llama-cpp-python` (GGUF) для тестов порта.

**Dev (`a`):** `pytest` · `hypothesis` · `respx` · `pytest-cov`.

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
| V1 | RU/EN-стемминг `tantivy` из Python-биндингов | retrieval-golden (§11.2) |
| V2 | Колёса `torch`/`sentence-transformers` под Python 3.14 | пробная установка; иначе venv 3.12/3.13 |
| V3 | RU-качество bge-m3 vs multilingual-e5 | R9/R10 (§16) |
| V4 | docling offline-веса в контуре + лицензии весов | предзакачка в кэш HF + аудит лицензий |
| V5 | `scrapy-playwright` 0.0.x на сложных антибот-доменах | пилотный краул 3–5 доменов |
