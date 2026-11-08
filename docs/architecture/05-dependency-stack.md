# coHound — архитектура: реестр зависимостей

> **Статус открытых вопросов на 2026-11-08** (HEAD `2af6d1e`, сверено с кодом и `uv.lock`): 3 закрыто (V1, V2, V5), 2 открыто (V3, V4). Реестр §2 сверен с `pyproject.toml`/`uv.lock`; отклонения с замером — решения, а не долг.
> Поправка к V4: `DOCLING_ARTIFACTS_PATH` в docling 2.135.0 **читается**
> (`.venv/…/docling/datamodel/settings.py:62-71`), поэтому офлайн-механизм исполним; открыты лицензии весов и `repo_id`/`revision` в манифесте.

**Документ:** `docs/architecture/05-dependency-stack.md`
**Статус:** реестр решений (снимок 2026-10-08; сверено с `uv.lock` и кодом 2026-11-08)
**Опирается на:** `00-overview.md` (AD1, AD10), `01-modules-and-contracts.md` (порты/адаптеры)
**Источник истины:** `cohound-spec-v2.md` §3.2. При расхождении — регламент главнее.

Назначение: зафиксировать **какие внешние пакеты берём, какие откладываем и какие запрещены**,
с обоснованием и лицензией. Реестр — вход для «пин+лицензии» M0.
Версии/лицензии/`requires_python` — из первичных источников (PyPI JSON API, GitHub API, офф. docs)
на 2026-10-08; установленный набор сверен с `uv.lock` на 2026-11-08.

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
CLI-ядро (`pydantic`+`pyyaml`+`pypdfium2`+`markdown-it-py`/`mdit-py-plugins`+stdlib) ставится везде; индекс-контур — изолированное окружение/образ,
а LLM-инференс вообще внешний (корп. облако, §2.3). Это согласуется с **AD1** (commodity за портами)
и A1.

**Проверено резолвом и установкой на Python 3.14 (2026-10-08):** все лёгкие extras (ядро + `retrieval`,
`crawl`, `extract`, `safety`, `ops`, `dev`, `ann`, `pdf-measure`, `embed-lite`) разрешаются и
устанавливаются; `torch`/`sentence-transformers` (`embed`) — тоже (колёса `cp314` есть). Полный
совместный набор — **213** записей `uv.lock` на 2026-11-08 (прежние «137» — счётчик 08.10, другой прибор),
без torch по умолчанию. `extract-pdf` намеренно тяжёлый (`torch`+`torchvision` — см. §1: `cu130` на Linux, CPU на macOS).
`requires-python = ">=3.14"` в `pyproject.toml`; CI резолвит на 3.14.

**Воспроизводимость установки (`uv.lock`, проверено 2026-10-23).** `uv lock --check` проходит;
`extract-archives` **присутствует** в локе (`py7zr` 1.1.4, `rarfile` 4.5), поэтому
`uv sync --frozen --extra extract-archives` разрешается (`--dry-run` проверен). Аудит `a4f67af`
фиксировал обратное (лок без этих пакетов) — с тех пор лок пересобран. Правило остаётся:
после правки `pyproject.toml` лок надо пересобрать (`uv lock`), иначе `--frozen` поставит
устаревший набор; без пере-лока гарантированный путь — `uv pip install -e '.[dev,retrieval,
crawl,extract]'` (+ `extract-archives`, если нужен спуск в 7z/rar).

**Escape hatch.** Если появится вынужденно старый целевой хост (без возможности поставить 3.14 через
`uv`/контейнер), понижение базлайна до 3.13/3.12 — точечная правка `requires-python` после проверки
колёс под понижённую версию; код не должен использовать 3.14-only синтаксис инкрементально
(фиксируется по мере появления).

**Находка (docling):** пакет `docling` = ровно `docling-slim[standard]` и тянет
**torch+torchvision** (~гигабайты). Для Office (DOCX/PPTX/XLSX) это не нужно:
`docling-slim[format-office]` ставит только python-docx/python-pptx/openpyxl/pylatexenc —
**но `rtree` в него не входит**, а импорт `DocumentConverter` без `rtree` не проходит
(цепочка `pipeline→models.factories→base_ocr_model→datamodel.spatial`). Поэтому extra
`extract` объявляет `rtree` **явно** (§2.4), и это не «PDF-only» зависимость.
`docling-slim` предоставляет namespace-импорт `docling` (`docling/document_converter.py`) —
адаптер `extract_document.py` менять не нужно. Разделены extras `extract` (лёгкий) и
`extract-pdf` (тяжёлый, отдельное окружение).

**Решение (2026-10-17, уточнено 2026-10-28): PyTorch — декларативно из индекса, одним
источником для `torch` и `torchvision`.** Контуры `extract-pdf` (PDF-layout) и `embed`
(индекс-контур) используют сборки PyTorch, объявленные в `pyproject.toml`:
`torch 2.14.1+cu130` / `torchvision 0.29.1+cu130` из индекса `pytorch-cu130`
(`https://download.pytorch.org/whl/cu130`) на `sys_platform != 'darwin'` и
`torch 2.14.1+cpu` / `torchvision 0.29.1+cpu` из `pytorch-cpu` на macOS (колёс `cu130` под
darwin не существует). Пиннинг —
декларативный и единственно возможный на `uv 0.11.16`: два `[[tool.uv.index]]`
(`pytorch-cpu`, `pytorch-cu130`, оба `explicit = true`) + `[tool.uv.sources]`
`torch`/`torchvision` → эти индексы по маркеру платформы; ядра объявлены **прямыми**
зависимостями `extract-pdf`, иначе транзитивный `docling` притянул бы колесо `torchvision`
с pypi.org (ABI-несовместимость: «operator torchvision::nms does not exist» → пустой текст
PDF). Оба пакета обязаны приходить из **одного** индекса — ABI-пара собирается только из
одной сборки; это и есть причина, по которой они объявлены парой.
`[tool.uv.sources]` действует **проектно-глобально**: `embed`
(`sentence-transformers` → `torch`) тоже резолвит те же ядра — это **осознанно и
объявлено здесь**, а не побочный эффект. `[tool.uv] torch-backend = "cpu"` резолвером проекта
(`lock`/`sync`) **молча игнорируется**; `--torch-backend` есть только у `uv pip` — поэтому
index+sources и есть единственная декларативная опция. Новый supply-chain host —
`download.pytorch.org` (редиректит на `download-r2.pytorch.org`) — при зеркалировании закрытого
контура учитывать как отдельный источник.

**Решение (2026-10-28): CUDA-колёса и ось `crawl.pdf_device` (P9).** GPU внесён как
**объявленная ось**, а не как неявный выбор по установленным колёсам: `deploy.yaml`/
`run.yaml:crawl.pdf_device: cpu | cuda`, дефолт `cpu`. `auto` не существует намеренно
(R5, `10-extraction-pipeline-architecture.md` §1): объявленная ось обязана действовать, а
`auto` = «устройство выбрал движок», тот же класс, что чтение модели оценщика из env (§7.4).
Устройство подставляется в `accelerator_options` конвертера, входит в `extractor_version`
(`docling-doc-3:<level>/<device>/docling-<build>/<policy>/<finalize>`) и в `extractor_stack` — значит меняет
ключ `extract_cache` и `run_id` (`execution_change`, §13). `pdf_device: cuda` при
недоступном устройстве — `EXIT_USAGE` с числами, **не** тихий CPU.

Замер на целевом контуре — `docs/reports/measurements/2026-10-28-gpu-vs-cpu-extraction.md`,
разбор и все числа — `10-extraction-pipeline-architecture.md` §3.4. Здесь остаётся только то,
что обосновывает **выбор зависимости**, а не стоимость извлечения: `layout_tables` ускоряется
на **8.45–11.73×** (22 / 181 / 665 стр., против одного CPU-воркера), уровень `text` (нативный
pdfium) GPU **не ускоряет** — отсюда обе механизмы PDF живут рядом, и `cuda` не вытесняет
`pypdfium2`. Параллель на карте бессмысленна (2 × 181 стр. — **1.10×** при ΣRSS 4787 MB и
ΣVRAM 1344 MB: связывает host), поэтому правильная конфигурация — **один GPU-воркер**;
пики VRAM/RSS по уровням — `10` §3. Воспроизведение: `uv sync --frozen --all-extras`;
`python -c "import torch; print(torch.__version__, torch.cuda.is_available())"` →
`2.14.1+cu130 True`. Один и тот же 7-стр. PDF под `cpu` и `cuda` даёт **разный** текст
(12 203 и 12 199 симв.) — это и есть причина, по которой устройство стоит в ключе
`extract_cache`, а не только в логе.

**Текущая конфигурация хоста — `cuda`** (`config/deploy.yaml:crawl.pdf_device: cuda`,
решение владельца 2026-10-28): устройство принадлежит машине, поэтому объявлено в деплое, а не
в `run.yaml` продукта. Проверено сквозняком через сам пайплайн (не отдельным скриптом):
`build_worker_extractor(LAYOUT_TABLES, pdf_device=cuda)` → `extract(...)` на реальной
181-стр. книге корпуса дал **189 006 симв. за 31.7 с**, пик VRAM **478 MB**, `error=None`,
и документ назвался `docling-doc-3:layout_tables/cuda/docling-2.135.0` (на 2026-10-28 — до появления `/<policy>/<finalize>` в строке версии) — то есть устройство
доехало до конвертера и до провенанса. Окно воркеров на этом хосте — **1** по host-памяти
(доступно 5 266 MB при пике уровня 3 679 MB), VRAM-слагаемое его не ограничивает (3 583 MB
доступной карты против 817 MB на воркер), что совпадает с замером §3.6: связывает host.

Выбор одного индекса на Linux (вместо двух сборок) опирается на то, что колесо `cu130`
**импортируется без доступного устройства**: проверено в этом же venv с `CUDA_VISIBLE_DEVICES=""` —
`torch.cuda.is_available()` → `False`, `cuda_available()` → `False`, извлечение
`layout_tables` прошло штатно (7 стр., 12 203 симв.) и назвалось `/cpu/`. Поэтому
`pdf_device: cuda` на машине без карты отвергается preflight'ом (`EXIT_USAGE` с числами), а
`cpu` — штатный режим. ❔ **не проверено:** поведение в чистом CI-контейнере **без**
установленных CUDA-библиотек дистрибутива (локально они приходят pip-пакетами `nvidia-*`
из того же индекса; на голом образе импорт может упасть на загрузке `libcuda`-зависимых
модулей — тогда Linux-полосе понадобится CPU-индекс как второй источник).
Проверено в этом venv: до установки колесо было `+cpu`, после — `+cu130` с
`cuda_available=True`, матричное умножение на устройстве проходит (`max_memory_allocated`
10 MB на пробном тензоре 512×512); `uv lock --check` сходится, `uv sync --frozen` — тоже.

---

## 2. Реестр по категориям

### 2.1. Ядро (обязательно, уже принято)

| Пакет | Версия | Лицензия | Вердикт | Зачем |
|---|---|---|---|---|
| `pydantic` | 2.14.x (диапазон `>=2.13,<3`; в `uv.lock` — 2.14.0) | MIT ✅ | **a** | схемы §4.2/4.3/4.5, инварианты, `extra="forbid"` |
| `pyyaml` | 6.0.x | MIT ✅ | **a** | `run.yaml`, `package.yaml`, `cohort.yaml` |
| `csv`, `argparse`, `sqlite3`, `json` | stdlib | PSF | **a** | отчёты, CLI, хранилище, сериализация |

> Базовые `dependencies` (5) — `pydantic` (MIT), `pyyaml` (MIT), `pypdfium2` (BSD-3/Apache-2.0; механизм
> `text_layer` оси `crawl.pdf_pipeline`), `markdown-it-py`+`mdit-py-plugins` (MIT; Markdown AST чанкера,
> план 18) — `pyproject.toml:18-27`. `numpy` **не** входит в ядро: живёт в extras `retrieval`/`embed-lite`
> и импортируется **лениво** (`adapters/index_format.py`, `index_npy.py`, `vector_npy.py`, `embed_model2vec.py`);
> `.npy`-шарды и brute-force cosine требуют `retrieval`; `marko` 2.2.4 приходит с docling (0 импортов).

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
| `model2vec` | 0.9.x | MIT ✅ | **b** | лёгкий fallback без torch (статичные эмбеддинги, CPU); качество RU 🟡 — как абляция. Extra `embed-lite` объявляет `huggingface_hub` (1.33.0, Apache-2.0) явно: адаптер резолвит пин ревизии через `snapshot_download` |
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
| `docling-slim[format-office]` | 2.135.x | MIT ✅ | **a** | DOCX/PPTX/XLSX нативно (python-docx/-pptx/openpyxl), **без torch**. `rtree` для Office **обязателен**: extra `[format-office]` его не покрывает, а цепочка импорта `DocumentConverter` (`pipeline→models.factories→base_ocr_model→datamodel.spatial`) требует `rtree` — поэтому он объявлен в extra `extract` явно (`pyproject.toml`), а `doctor` проверяет его в группе `deps:extract`; группа опциональна — отсутствие даёт `warn`, а не `fail` (`core/cli/doctor.py:121,354-365`) |
| `docling-slim[standard]` | 2.135.x | MIT ✅ | **a** (отдельное окружение) | PDF layout+tables; **тянет torch+torchvision** (из индексов `pytorch-cu130`/`pytorch-cpu`, §1); обязателен для PDF-контура (блокер M0). OCR-движок `rapidocr` (Apache-2.0) держится прямым пином `>=3.9.1,<3.10` (`pyproject.toml:71`): 3.10.0 удалил `PP_OCRV6_LANGS` из `rapidocr.utils.model_resolver`, а метаданные docling (`rapidocr<4,>=3.9.1`) пропускают 3.10.0 — и OCR при `document_level=full` молча выключается |
| `lxml` | 6.1.x | BSD-3 ✅ | **a** (транзитивно) | уже в дереве trafilatura/Scrapy |
| `openpyxl` | 3.1.x | MIT ✅ | **a** | XLSX-отчёты; Office-бэкенд docling |
| `python-docx` / `python-pptx` | 1.2.x / 1.0.x | MIT ✅ | **b** (через docling) | отдельно не нужны |
| `markitdown` (Microsoft) | 0.1.x | MIT ✅ | **b**, не устанавливается | fallback экзотики (EPUB/msg/xls); PDF не layout-aware. В коде импортов **ноль**, fallback-путей в адаптерах нет |
| `pypdf` / `pdfplumber` | 6.19.x / 0.11.x | BSD-3 / MIT ✅ | **b** — extra `pdf-measure` | Сравнительные измерения; не являются runtime fallback-путём |
| `selectolax` / `beautifulsoup4` | 1.0.0 / 4.15.x | MIT ✅ | **b**, не устанавливаются | `selectolax` не устанавливается; `beautifulsoup4` 4.15.0 приходит транзитивно с `docling-slim[standard]` (`extract-pdf`), но импортов в адаптерах ноль. Заявлены как fallback-парсинг и извлечение `h1–h6` под `heading_path`; иерархию заголовков сегодня даёт свой чанкер (`chunker_markdown`) |
| `readability-lxml` / `markdownify` | 0.9 / 1.2.x | Apache-2.0 / MIT ✅ | **b**, не устанавливаются | fallback main-content / HTML→MD; main-content даёт `trafilatura` |
| `camelot-py` / `tabula-py` | 2.0 / 2.10 | MIT ✅ | **c** | тяжёлые (opencv+pandas / JVM); таблицами занимается docling |
| `PyMuPDF` | 1.28.x | **AGPL** ✅ | **d** | AGPL исключён (курс проекта) |
| `unstructured` | 0.27.x | Apache-2.0 ✅ | **d** | `<3.14` + 24+ зависимостей + телеметрия on-by-default; OSS-таблицы слабые (0.426 vs 0.866) |
| `html2text` | 2025.4.x | **GPL-3.0** ✅ | **d** | GPL несовместим с permissive-курсом; закрывается `markdownify` |

**Текстовые форматы и EPUB — без новых зависимостей (22.10).** Plain text/markdown и
EPUB не добавляют в стек ни одного пакета: текст читается stdlib-декодированием и
нормализацией (`core/adapters/extract_text.py`), EPUB — stdlib `zipfile` + `xml.etree`
через уже принятый субстрат контейнеров (`core/adapters/extract_epub.py`,
`core/adapters/container_zip.py`, `core/util/container_policy.py`). `markitdown` (b) для
EPUB **не** используется (лишний слой конвертации поверх того же ZIP); LlamaIndex/LangChain
остаются **d** (§3.1) — EPUB-разбор не повод их подключать. Разбор Markdown в чанкере — отдельная ось: `markdown-it-py`+`mdit-py-plugins` (базовые, план 18, §2.1).

**Extra `pdf-measure` содержит только измерительные библиотеки.** `pypdf` и `pdfplumber`
используются скриптом `scripts/measure_pdf_backends.py` для сравнительных замеров; они не
образуют runtime fallback-путь. Остальные кандидаты (`markitdown`, `selectolax`,
`beautifulsoup4`, `markdownify`) не подключены к адаптерам и не устанавливаются этим extra.

**Контейнеры-артефакты (P6, спуск в zip/7z/rar).** Слой допуска/retention (стриминг в
staging-файл, класс-aware потолки) реализован **без** этих пакетов — `zipfile` stdlib и
магия байтов. **Спуск реализован**: порт `core/ports/container.py` (`ContainerPort`),
адаптеры `core/adapters/container_{zip,7z,rar,any}.py`, исполнение —
`core/pipeline/archive_descent.py`, сборка — `core/cli/wiring.py` (`container_for`).
`zip` читается stdlib; `7z`/`rar` — ленивыми импортами из extra `extract-archives`.
Деградация **честная и видимая**: без пакета `list_members` отдаёт
`ContainerListing(unreadable="backend_unavailable: py7zr не установлен (extra
extract-archives)")`, `open_member` — `ContainerUnavailable`, а спуск пишет
`container_reader_unavailable` — «пустого архива» не бывает. Базовый extra `extract` не
меняется.

| Пакет | Версия | Лицензия | Вердикт | Обоснование |
|---|---|---|---|---|
| `zipfile` (stdlib) | 3.14 | PSF ✅ | **a** | ZIP-члены без внешних бинарей; **ZipCrypto read-only**, AES не поддержан; **bomb-guard нет** — границы задаёт ось `archive_max_*` (член/суммарный распакованный объём/число членов), не библиотека |
| `py7zr` | 1.1.4 | **LGPL-2.1-or-later** ✅ | **a** (extra `extract-archives`) | 7z (LZMA2/BCJ) на чистом Python; **реализован** (`container_7z.py`, ленивый импорт). LGPL — **явное исключение**, см. ниже |
| `rarfile` | 4.5 | ISC ✅ | **a** (extra `extract-archives`) | RAR4/RAR5-разбор; **реализован** (`container_rar.py`). **Сжатые** члены требуют внешней утилиты (`bsdtar`/`unrar`/`7z`). `unrar` (проприетарный) в образ **не тащим**: отсутствие утилиты = видимая причина, не тихий пустой текст |

⚠️ **LGPL-исключение (`py7zr`) — единственное в проекте.** Политика: LGPL-код
**не копируется** в репозиторий (перенос кода запрещён), используется только как
**unmodified wheel** из индекса; пакет вынесен в отдельный extra `extract-archives`,
не входит в базовый `extract`/runtime-набор; за портом `ContainerPort` он **swap-able**
(альтернатива — внешний `7z`-бинарь за тем же портом, без смены методологии). Это
commodity-слой (A2): замена читателя контейнеров не меняет измерение. Прочие LGPL/GPL
пакеты по-прежнему **d** (см. §3); `patool`/`pyth`/`pyunpack` — **d** (GPL-3 и/или
неподдерживаемые: тонкие обёртки над бинарями, ломают воспроизводимость и
контроль границ распаковки). Установка этого extra и текущее состояние лока — §1
(«Воспроизводимость установки»).

⚠️ **docling offline:** веса нужны **только PDF-контуру** (Office — без ML-моделей). Штатный
офлайн-механизм — pipeline `artifacts_path` / env `DOCLING_ARTIFACTS_PATH`, заполняемый
`docling-tools models download` (или `download_models()`); `HF_HUB_OFFLINE=1` — валидный, но
вспомогательный hub-level guard, а не основной переключатель (§7).
**Лицензии весов** ❓ — проверить перед редистрибуцией (лицензия кода ≠ лицензия весов).

⚠️ **docling: память PDF-пайплайна — `queue_max_size` (замер, не предвидение).**
Главный драйвер памяти `StandardPdfPipeline` — не «весь PDF», а число страниц **в полёте**:
продюсер плюс шесть очередей по `queue_max_size` (дефолт docling 2.135 — **100**). Адаптер
(`core/adapters/extract_document.py`, `_pdf_options`) ставит **`queue_max_size=1`** для всех
уровней `PdfPipelineOptions` (`layout`/`layout_tables`/`full`); ветка `text`
(`NativePdfPipelineOptions`) ручки не имеет и не меняется.

| Замер (уровень `layout_tables`) | `queue_max_size` | s/стр. | peak RSS | md5 markdown |
|---|---|---|---|---|
| синтетика, 68 стр. | 100 (дефолт) | 1.72 | 3160 MB | A |
| синтетика, 68 стр. | 8 | 1.72 | 2952 MB | A |
| синтетика, 68 стр. | **1** | **1.60** | **1775 MB** | **A (идентичен)** |
| реальный PDF, 81 стр. | **1** | 1.87 | 2303 MB | совпал с кэшем |
| реальный PDF, 665 стр. | 100 (дефолт) | 1.46 | 4174 MB | A |
| реальный PDF, 665 стр. | **1** | **1.30** | **2126 MB** | **A (идентичен)** |

Итог: **−44%** peak RSS на синтетике и **−49%** на реальном 665-стр. документе, markdown
**байт-в-байт тот же** (md5 совпал на 81- и 665-стр. документах против `extract_cache`),
статус docling — `SUCCESS` с полным набором страниц (665/665, ни одного `PARTIAL_SUCCESS`),
время на страницу **не выросло** (1.46 → 1.30 s/стр.), то есть далеко от
`_EXTRACT_TASK_TIMEOUT_S = 1800` (866 с на 665 стр.).

Класс изменения — **`execution_change`** (§13), не `methodology_change`: `extractor_version`
(`docling-doc-3:<level>/<device>/docling-<build>/<policy>/<finalize>`) **не включает** эту ручку и от замера не меняется,
поэтому записи `extract_cache/` (ключ — `(sha256 байтов, extractor_version, level)`)
**остаются валидными**, `golden-full` не требуется. Значение ручки публикуется в `settings_provenance` манифеста
(`document_extract.queue_max_size`, `source: measured`) — провенанс замера, не граница
контракта (`EXECUTION_CONTRACT_FIELDS` её не содержит).

### 2.5. Краулинг

| Пакет | Версия | Лицензия | Вердикт | Обоснование |
|---|---|---|---|---|
| `Scrapy` | 2.19.x | BSD-3 ✅ | **a** | `DOWNLOAD_SLOTS` per-domain, AutoThrottle, HttpCache |
| `scrapy-playwright` | 0.0.48 | BSD-3 ✅ | **a** (диапазон + smoke) | per-request `playwright=True` для JS-доменов. **V5: зрелый де-факто стандарт** (org `scrapy-plugins`, активные PR Zyte 2026, py3.14 поддержан PR #375); ярлык `0.0.x` = конвенция автора (classifier Beta), не заброшенность |
| `playwright` | 1.63.x | Apache-2.0 ✅ | **a** (диапазон + smoke) | headless-рендер; **два пути краула:** движок `scrapy-playwright` и **прямой** sync-API `BrowserCrawler` (`adapters/crawl_browser.py`, honest/hybrid, без Scrapy/Twisted; выбор — сменой адаптера за портом `CrawlerPort`, A2). Нужен бинарь `playwright install chromium`; браузеры качаются с CDN — в закрытом контуре зеркало (`PLAYWRIGHT_DOWNLOAD_HOST`), изоляция — `PLAYWRIGHT_BROWSERS_PATH` |
| `httpx` | 0.28.x | BSD ✅ | **a** | общий HTTP-клиент (search API, tests). **Watch-item (08.11.2026):** `httpx` 0.28.1 в локе, `httpx2` в графе нет; Scrapy 2.19.0 от `httpx` не зависит. Миграция — по триггеру §14 |
| `curl_cffi` | 0.16.x | MIT ✅ | **b** — **не подключён** | Не устанавливается extra `crawl`: «дешёвый первый ярус» остаётся заявкой, а не работающим fetch-адаптером |
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
  **Диапазон `playwright>=1.63,<2` (`pyproject.toml:41`) подтверждён; пина `==` нет, фолбэк на 1.57 не нужен.**
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
| `tenacity` | 9.2.x | Apache-2.0 ✅ | **b** (заявка, не устанавливается) | **ноль импортов** в `core/`/`tests/`: ретраи реализованы вручную через `max_attempts`/`attempts_exhausted` (`core/pipeline/corpus.py`) |
| `structlog` | 26.1.x | Apache-2.0 ✅ | **a** | структурные JSON-логи для воспроизводимого прогона (**опциональный** backend рендеринга: `HAS_STRUCTLOG` в `core/util/logging.py`; без него — свой JSON-рендерер) |
| `pytest` | 9.1.x (в `uv.lock` — 9.1.1) | MIT ✅ | **a** | §11.6 CI; входит в extra `dev` (`pyproject.toml`) и в `uv.lock` — установлен в dev-окружении, `scripts/ci.sh` — единая точка входа |
| `hypothesis` | 6.168.x | MPL-2.0 ✅ | **a** | property-based проверка инвариантов I1–I23 и парсеров |
| `respx` | 0.23.x | BSD-3 ✅ | **a** | мок `httpx` для LLM/SERP-адаптеров без сети |
| `pytest-cov` | 7.1.x | MIT ✅ | **b** | покрытие |
| `pytest-xdist` | 3.8.x | MIT ✅ | **b** | параллельный прогон (`-n auto --dist worksteal`); CI ускоряется ~2x. Транзитивно тянет `execnet` (MIT) |
| `ruff` | 0.17.3 (в `uv.lock` — 0.17.3) | MIT ✅ | **a** | линтер, гейт `scripts/ci.sh`; extra `dev`. Набор правил и обоснования — `pyproject.toml` `[tool.ruff.lint]` и §2.7.1 |
| `mypy` | 2.4.0 | MIT ✅ | **a** (`core/domain`) / **b** (остальной `core/`) | type-checker; extra `dev`. Гейт «ноль» на `core/domain`, зафиксированный baseline на остальном ядре — §2.7.1 |
| `rich` / `typer` / `click` | 15.0 / 0.26.8 / 8.5 | MIT / BSD-3 ✅ | **b** | `argparse` хватает для MVP (A1); typer — при росте подкоманд. Все три уже стоят транзитивно (docling/`huggingface_hub`), прямыми зависимостями не объявлены |
| `pydantic-settings` | 2.15.x | MIT ✅ | **b** | конфиг — YAML, поэтому опц.; **не ставить `[all]`** (boto3/azure) |
| `orjson` | 3.13.x | Apache-2.0 ✅ | **b** | скорость сериализации; stdlib `json` достаточен |
| `tqdm` | 4.70.x | MPL-2.0 AND MIT ✅ | **b** | прогресс длинных фаз |
| `xlsxwriter` | 3.2.x | BSD-2 ✅ | **b** | богаче форматирование отчётов, чем openpyxl |
| `hishel` | 1.4.x | BSD ✅ | **b** | RFC-9111 кэш поверх httpx (свой content-addressed уже есть, R7) |
| `pandas` | 3.0.x | BSD-3 ✅ | **d** | матрицу/скоры считает `domain/scoring.py`; отчёты — openpyxl+csv. Транзитивно приходит с `docling-slim[standard]` (`docling-core` 2.101.0), но прямой зависимости нет |
| `python-slugify` / `Unidecode` | 9.1 / 1.4 | MIT / **GPL-2.0+** 🟡 | **d** | транзитив GPL; свой `sanitize_slug` уже есть |
| `requests-cache` | 1.3.x | BSD-2 ✅ | **c** | ориентирован на `requests`, у нас httpx |

#### 2.7.1. Линтер и type-checker: что гейтится и почему не «`--strict` на всё»

До 2026-10-27 в проекте не было ни линтера, ни type-checker'а. Это был **молчаливый пробел**, а
не решение: реестр разбирает `rich`, `typer`, `pandas`, `hishel`, `python-slugify`, но не их.
Заменой считались `tests/selfcheck.py` (113 проверок на 08.11.2026, включая AST-проверки
импорт-графа §1 и нейтральности §1.3), Pydantic `extra="forbid"` + `StrEnum` (runtime-валидация) и
property-тесты `hypothesis`. Замер показал, чего эта замена не покрывает.

**Замер до включения** (27.10.2026: `core/` — 212 файлов, 101 781 строка; `tests/` — 234 файла;
на 08.11.2026 — 258 файлов / 151 571 строка и 340 файлов, `git ls-files`):

| инструмент | результат | автофиксится |
|---|---|---|
| `ruff` (набор `E4,E7,E9,F,B,BLE,I,UP,SIM,S,RUF`) | **782** замечания в `core/`+`tests/`+`scripts/` | 427 |
| `mypy` (обычный режим, не `--strict`) | **127** ошибок в 36 файлах из 212 | — |

Типизация в коде **уже была**: функций без аннотации возврата — 2 из 2938 (0.1 %), аргументов без
аннотации — 5 из 6891. Её просто никогда не проверяли машиной.

**Набор правил — узкий и названный.** Он ловит не стиль, а классы дефектов, которые проект
объявляет главными: молчание вместо видимого отказа (`S110` try/except/pass, `BLE`), позднее
связывание в замыкании цикла (`B023`), мёртвый код и неиспользуемые имена (`F401`, `F841`, `F811`,
`F821`), склейку строк и вложенные условия (`ISC`/`SIM`).

Что **выключено** и почему:

* `ruff format` не включён: переформатирование 100k строк смешало бы предметные правки с
  косметикой и сделало бы диффы нечитаемыми, а `git blame` — бесполезным.
* `RUF001/002/003` (ambiguous-unicode): докстринги и комментарии — на русском, идентификаторы — на
  английском. Смешение алфавитов и кириллическая типографика здесь норма проекта (9.5k срабатываний,
  ноль настоящих дефектов).
* `RUF100` (unused-noqa): набор правил **уже**, чем конвенция подавлений в коде (`ANN`, `ARG`,
  `SLF`, `D`, `ERA`, `PLC` не включены). «Неиспользуемая» в терминах ruff noqa часто подавляет
  правило, которое в проекте действует как конвенция (278 из 400 — `BLE001`), поэтому автоудаление
  стёрло бы намерение. Мёртвые `# noqa: F401` (15 шт.) сняты вручную, когда они действительно
  ничего не подавляли.
* `SIM300` (yoda-conditions) — **дефект инструмента, а не стиль**. В ruff 0.17.3 автофикс
  переворачивает сравнение, когда один из операндов — **вызов**: `X = frozenset({"a"});
  X == frozenset({"a"})` диагностируется как «Yoda condition» и после `--fix` превращается в
  `frozenset({"a"}) == X`, то есть в настоящую йоду. Из 14 срабатываний фикс дал 4 инверсии
  (в `tests/edge/`); они возвращены вручную, правило вынесено в `ignore` с обоснованием.
  Вернуть, когда фикс перестанет инвертировать операнды: правило, чей фикс ухудшает код, вреднее
  его отсутствия.
* `S101` отключён **только в `tests/`** (`per-file-ignores`): там `assert` — механизм, а не забытая
  проверка. В `core/` правило действует.

**Почему mypy не гейтил весь `core/` сразу (решение 27.10.2026).** 2357 упоминаний `Any` и 1497
вызовов `getattr` в `core/` (08.11.2026; на 27.10 было 1581 и 1047) заражают типы, поэтому «127» —
нижняя граница, а `--strict` даст существенно больше.
Гейт «весь `core/` в ноль» означал бы либо месяцы разбора, либо `# type: ignore` везде — второе
хуже отсутствия проверки, потому что создаёт видимость. Поэтому проверка разделена по смыслу:

* `core/domain` — **гейт «ноль»**. Это чистый слой без I/O, где живут инварианты и scoring: ошибка
  типа там — не долг, а неверная модель. Сборка падает.
* остальной `core/` — **зафиксированный baseline** (`docs/architecture/mypy-baseline.txt`).
  Долг назван числом, его рост блокирует сборку, уменьшение видно (и печатается как «baseline
  устарел»), но починка ошибки не ломает CI — иначе её перестали бы чинить.
* ключ сравнения — `путь: сообщение [код]` **без номера строки**: baseline должен переживать
  правку выше по файлу, иначе его перегенерируют не читая.

**Долг закрыт 08.11.2026, и флаг подавлений теперь ловит протухание.** Два пасса, оба — измерением,
а не растяжкой baseline (`a801b9f` поверх `4905cfe`): 112 ошибок → 0 честными типами (сужение,
протоколы, `cast` с комментарием об инварианте там, где тип чужой), затем `# type: ignore` → **0**.
Baseline в `docs/architecture/mypy-baseline.txt` пуст, шапка `# Записей: 0` совпадает с фактом;
`any`/`getattr`-заражение осталось, но оно больше не даёт ошибок.

Второй пасс стал возможен только потому, что `mypy --warn-unused-ignores` **измерил** протухание:
64 подавления из 90 не были нужны вовсе. При `warn_unused_ignores = false` этот класс дефектов
невидим: гейт проверяет синтаксис, а не нужность, и любой `# type: ignore` живёт в коде вечно —
в том числе тот, что когда-то глушил ошибку, давно исправленную. Поэтому флаг включён
(`pyproject.toml`): лишнее подавление теперь **ошибка сборки** (`unused-ignore`), а не частная
заметка при чтении. Запас на будущее калибруется не порогом, а правилом: новое подавление
допустимо только с причиной в строке и только там, где ограничение чужое (стабы сторонней
библиотеки); собственный `Any`-хвост чинится типом.

**Как запускать:**

```bash
.venv/bin/ruff check core tests scripts          # линтер
.venv/bin/python scripts/mypy_gate.py            # домен + baseline
.venv/bin/python scripts/mypy_gate.py --update   # перегенерировать baseline (осознанно)
bash scripts/ci.sh                               # всё вместе: ruff → mypy → selfcheck → pytest
```

Оба инструмента — в extra `dev` и в `uv.lock`; при их отсутствии `scripts/ci.sh` печатает
предупреждение и продолжает (окружение остаётся провайдер-нейтральным), но CI-окружение проекта
их содержит.

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
| `jsonschema` | **a (dev)** | валидация запросов/ответов шва по опубликованной JSON-Schema (07 §8.3); обязателен в dev-тестах, не runtime-зависимость |
| `pyjwt` / `authlib` | **b** | OIDC/JWT-идентичность шва, если корп. IDP требует bearer-токены вместо mTLS (07 §8.2) |

`edge/` — отдельное окружение (тот же `crawl`+`extract` extras), сетевая изоляция — на уровне L7-фаервола
(политики — `07` §5), не пакета. Сервер gateway — тонкий; допускается `Starlette`/`uvicorn` (Apache-2.0/MIT)
как **b**, но не обязателен (A1). Контракты шва публикуются как **OpenAPI + JSON-Schema** (`schema_version`)
для IDP/DLP/IDS-политик — `07` §8.

---

### 2.10. Скопированный сторонний код (не зависимости)

Реестр выше описывает **пакеты**. Отдельный класс — код, **перенесённый копированием** из
других проектов (правило `AGENTS.md`: переносить копированием, а не зависимостью; сохранять
лицензию/copyright в шапке файла; код без явной лицензии не копировать).

**Где живёт реестр.** Отдельного файла-реестра нет — и это решение, а не пробел. Единственный
источник провенанса — **шапка самого файла**: проект-источник, путь источника, лицензия,
copyright, перечень изменений. Такой реестр нельзя рассинхронизировать с кодом: удалили файл —
исчезла и запись, а копия шапки в отдельном файле пережила бы оригинал и стала бы ложной.
Обзор ниже — сводка по шапкам, а не второй реестр; при расхождении верна шапка.

Текущее состояние: **четыре файла** адаптированы из проекта **gag5** под **MIT License**
(Copyright (c) 2024), каждый — с шапкой происхождения:

| файл в coHound | источник в gag5 |
|---|---|
| `core/util/security.py` | `core/security.py` |
| `core/util/text_utils.py` | `core/text_utils.py` |
| `core/adapters/chunker_markdown.py` | `ingestion/structural_chunker.py` |
| `core/adapters/extract_document.py` | `documents/parse.py` (перенесён **не файл**, а идея graceful-degradation и способ вызова `DocumentConverter.export_to_markdown()`) |

**Вторая категория — внутренний источник.** Ещё два файла несут шапку «адаптировано из
проекта **gtm** (`scripts/kb_poc.py`, собственная разработка)»: `core/util/retrieval_eval.py`
(метрики hit@K/MRR/recall@K) и `core/adapters/lexical_bm25.py` (baseline BM25). Это не
сторонний код: лицензионных обязательств перед третьей стороной нет. Шапка сохранена ради
провенанса: видно, что код пришёл из PoC, а не написан под coHound.

Правила для новых копий: (1) у источника есть явная лицензия, допускающая копирование;
(2) шапка файла несёт проект, путь источника, лицензию, copyright и перечень изменений —
это и есть запись в реестре; (3) строка добавляется в таблицу выше; (4) не-MIT лицензия
(например LGPL у `py7zr`, §2.4) — отдельным разделом с указанием обязательств.
**Отличие от LGPL-исключения:** `py7zr` используется как **unmodified wheel** из индекса,
его код в репозиторий не переносится.

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

**Runtime (`a`):** `pydantic` · `pyyaml` · `pypdfium2` · `markdown-it-py`+`mdit-py-plugins` ·
`numpy`+`tantivy` (extra `retrieval`) · `trafilatura` ·
`docling-slim[format-office]`+`rtree` (extra `extract`) · `Scrapy`+`scrapy-playwright`+`playwright`
(extra `crawl`) · `httpx` · `nh3`+`defusedxml` (extra `safety`) · `openpyxl` · `structlog` (extra
`ops`) · stdlib (`sqlite3`, `csv`, `argparse`, `json`).
Собирается как extras в `pyproject.toml`: `retrieval`, `crawl`, `extract`, `safety`, `ops`.
Базовые `dependencies` (5): `pydantic`, `pyyaml`, `pypdfium2`, `markdown-it-py`, `mdit-py-plugins`
(`pyproject.toml:18-27`). `.npy`/лексика требуют `retrieval`; CLI-ядро ставится без numpy/tantivy (см. §2.1).

**Отдельные окружения (не в CLI-venv):**
- **PDF layout:** `docling-slim[standard]` (torch+torchvision; `cu130` на Linux, CPU на macOS — §1) — extra `extract-pdf`, блокер M0.
- **Индекс-контур:** `sentence-transformers` (+torch, те же ядра через проектно-глобальные `[tool.uv.sources]`, §1) — extra `embed`, изолированный venv/образ.
- **LLM-инференс:** модели корпоративного облака по [OI]-совместимому API — **вне наших окружений**
  (не ставим и не развёртываем; `vLLM`/`SGLang` — возможный backend облака, §2.3).
- **Спуск в 7z/rar:** extra `extract-archives` (`py7zr`, `rarfile`) — ставится по
  требованию; без него контейнеры 7z/rar не раскрываются (видимая причина, §2.4).
- **Опц. CPU-инференс:** ~~`llama-cpp-python` (GGUF) для тестов порта~~ — **снято** (не реализовано;
  вытеснено [OI]-совместимым клиентом внутреннего периметра, см. `06` §5.W9/§5.13).

**Dev (`a`):** `pytest` · `pytest-xdist` · `hypothesis` · `respx` · `pytest-cov` · `jsonschema` (контрактные тесты шва).

**Опционально (`b`):** `faiss-cpu`/`usearch` (ANN при росте), `model2vec` (лёгкая эмбеддинг-ветка),
`markitdown`/`selectolax`/`bs4`/`markdownify` (fallback-экстракция не подключена),
`pypdf`/`pdfplumber` (измерительный extra `pdf-measure`, §2.4), `curl_cffi` (не подключён и не входит в `crawl`),
`bge-reranker-v2-m3` (reranker), `rich`/`typer`, `orjson`/`tqdm`, `xlsxwriter`, `hishel`,
`tenacity` (ретраи пока ручные, не устанавливается), `py7zr`/`rarfile` (спуск в контейнеры, extra
`extract-archives`; `py7zr` — LGPL-исключение §2.4).

**Не брать (`d`):** LangChain, LlamaIndex, все hosted-парсеры, `unstructured`, `PyMuPDF`,
`html2text`, `bleach`, `chromadb`, `whoosh`, `pylucene`, `jsonformer`, TGI, `pandas`,
`Unidecode`/`python-slugify`, `FlagEmbedding` (для инференса), `patool`/`pyth`/`pyunpack`
(GPL-3 и/или мёртвые обёртки над внешними бинарями).

---

## 5. Версионные границы (связь с §13)

Класс границы определяется **не типом зависимости**, а тем, входит ли величина в
`execution_contract_digest` / `METHODOLOGY_FIELDS` (методология 0.3.0 — `core/domain/models.py:162`). Таблица — зеркало
`03` §C.1/§C.2 и `core/pipeline/digests.py`:

| Что меняется | Класс | Следствие |
|---|---|---|
| `embed_model_id`, `lexical_engine_version`, `vector_store`, `extractor_version` | **`content_change`** | наблюдаемые величины: **вне** контракта (не выводятся из входа — per-product композит корпуса, диск, wheel'ы). Новая `index_build_id`, diff **разрешён** с баннером и doc-delta |
| `chunker_version`, `extractor_stack` | **`execution_change`** | входят в контракт → новый `run_id`, diff блокируется; `methodology_version` **не** меняется |
| `crawl.document_level` | **`execution_change`** | входит в `CrawlLimits.as_dict()` **безусловно** (дефолт `full`) → `crawl_config_digest` → новый `run_id`. Дополнительно наблюдается в `extractor_version` (`docling-doc-3:<level>/<device>/docling-<build>/<policy>/<finalize>`) → новая сборка; но идентичность разошлась раньше, поэтому класс — `execution_change` |
| `retrieval_profile`, `retrieval_cutoff_K`, `retrieval_params_json` (`rrf_k`/fusion) | **`execution_change`** | входят в контракт; `index_build_id` **не** меняется (ретривал-конфиг не влияет на чанкинг/векторы), обязателен retrieval-golden |
| `scoring_policy_version`, `status_dict_version`, `capability_policy`, `unknown_policy`, `partial_score` (`METHODOLOGY_FIELDS`) | **`methodology_change`** | bump `methodology_version` + `golden-full` **и пере-аннотация golden** (§13) |
| модель под сценарий оценщика (`llm_scenarios`) | **`execution_change`** | новый `run_id`; `methodology_version` не меняется (`core/config/deploy.py`) |

Поэтому `sentence-transformers`/`tantivy` пинятся в `run_manifest.yaml` (провенанс: модель,
стеммер, RRF-параметры), но **сами по себе** версию методологии не двигают: их смена видна
через `pb`/`index_build_id`. Bump `methodology_version` — только по `METHODOLOGY_FIELDS`.

**Апгрейд `docling` внутри диапазона `>=2.135,<3` — что происходит с `extract_cache`.** Пин
задан диапазоном, поэтому минорное обновление проходит без правки кода; `extractor_version`
документа — `docling-doc-3:<level>/<device>/docling-<build>/<policy>/<finalize>`, где `<build>` — версия установленной
библиотеки (единственный источник — `docling_engine_build()` в
`core/adapters/extract_document.py`). Отсюда: (1) `extractor_stack` (в контракте) меняется ⇒
новый `run_id` — прогон честно объявляет новый движок; (2) та же сборка входит в ключ
`extract_cache` (`(content_hash, extractor_version, level)`) ⇒ записи прежнего движка **не
находятся**, документы переизвлекаются один раз; (3) `pb`/`corpus_docs.extractor_version`
описывают **фактически прочитанный** текст, а не объявленный движок. Это не дефект, а цена
честного провенанса: переиспользовать текст, произведённый другим парсером, значит выдать
`run_id`, который называет движок, которого не было (Д-8, `10` §5.8). Обратная совместимость
старых конвертов (ключ без сборки) намеренно не поддерживается — они просто не находятся.

---

## 6. Открытые вопросы верификации (вход для M0)

| # | Вопрос | Метод проверки |
|---|---|---|
| V1 | RU/EN-стемминг `tantivy` из Python-биндингов | ✅ **закрыт (2026-10-08):** русский стеммер и стоп-слова доступны нативно — `Filter.stemmer("russian")`, `Filter.stopword("russian")` (tantivy 0.26.2); end-to-end индекс+поиск на смешанном RU/EN корпусе прошёл («экспорт» находит «Экспорт»/«экспорта»). Подтверждён кодом: `core/adapters/lexical_tantivy.py:878-881` (`stemmer("russian")` + `stopword("russian")`). Внешний `snowballstemmer` не нужен |
| V2 | Колёса `torch`/`sentence-transformers` под Python 3.14 | ✅ **закрыт (2026-10-08):** `torch 2.14.1` и `sentence-transformers 6.1.0` имеют колёса `cp314`; резолв `uv` на 3.14 прошёл. Подтверждён установкой: `uv.lock` (08.11.2026) держит ровно эти версии. См. `pyproject.toml` |
| V3 | Выбор RU/EN-эмбеддинга | 🟡 **решение:** RU — абсолютный приоритет, EN — очень хорошо; **основной — `BAAI/bge-m3`** (MIT, 8192 контекста). Подтверждение и сравнение с RU-файнтюнами (`deepvk/USER-bge-m3`, `ai-forever/ru-en-RoSBERTa`) — R9/R10. ⬜ **ОТКРЫТО (08.11.2026)** — `bge-m3` зафиксирован кодом как дефолт (`core/adapters/embed_local.py:74`), но **сравнения с RU-файнтюнами не было**: ни одного замера по `USER-bge-m3`/`RoSBERTa` в `docs/reports/measurements/` нет |
| V4 | docling offline-веса в контуре + лицензии весов | ⬜ **ОТКРЫТО (08.11.2026), но посылка прежней записи ложна:** `DOCLING_ARTIFACTS_PATH` в docling 2.135.0 **читается** — `env_prefix="DOCLING_"` (`.venv/…/docling/datamodel/settings.py:62-71`), а `BasePipeline` берёт `settings.artifacts_path` как fallback (`…/docling/pipeline/base_pipeline.py:67-68`). Механика офлайн-бандла исполниме: env → `artifacts_path`; `HF_HUB_OFFLINE=1` — вспомогательный guard; Office-форматы весов не требуют. coHound передаёт `artifacts_path` только в OCR-пробу (`core/adapters/extract_document.py:643-660`). Остаётся: лицензии весов и `repo_id`/`revision` в манифесте |
| V5 | `scrapy-playwright` 0.0.x на сложных антибот-доменах | ✅ **закрыт smoke-тестом (2026-10-08):** зрелый де-факто стандарт (org `scrapy-plugins`, BSD-3, py3.14). Smoke `tests/smoke_scrapy_playwright.py` пройден на Scrapy 2.19.0 + scrapy-playwright 0.0.48 + playwright **1.63.0** (chromium + firefox) → **оставляем с диапазоном `playwright>=1.63,<2`** (пина `==` нет); связка 1.63/2.19 подтверждена, фолбэк 1.57 не нужен. Изолируется за портом `crawler`. Альтернативы не берём (см. §2.5) |

---

## 7. V4 — офлайн-веса docling и лицензии моделей

**Суть.** docling — это не только код (MIT). Для PDF-конвейера он использует ML-модели
(layout-анализ, распознавание таблиц TableFormer, OCR), которые **скачиваются с Hugging Face
Hub при первом прогоне**. В РФ-контуре (данные/инфраструктура не выходят за периметр, возм.
air-gap) сетевого доступа к HF нет — либо загрузка упадёт, либо произойдёт нежелательное
обращение наружу.

**Механика (подтверждена в коде `docling 2.135.0`; штатный офлайн — через artifacts-путь):**
- штатный офлайн-switch — pipeline `artifacts_path` / env `DOCLING_ARTIFACTS_PATH`; env **читается**:
  `env_prefix="DOCLING_"` (`…/docling/datamodel/settings.py:62-71`), а `BasePipeline` берёт
  `settings.artifacts_path` как fallback, когда опция пайплайна не задана
  (`…/docling/pipeline/base_pipeline.py:67-68`). Бандл заполняется `docling-tools models download`
  (или `download_models()`);
- `HF_HUB_OFFLINE=1` (и `TRANSFORMERS_OFFLINE=1`) — валидный **hub-level** guard, но не основной
  переключатель Docling: он лишь запрещает обращения к HF-hub, полагаясь на локальный кэш;
- (внутренний путь загрузки) `docling/models/utils/hf_model_download.py` → `huggingface_hub.snapshot_download(repo_id=…, revision=…)`;
- дефолтные репозитории моделей — `docling-project/docling-models`, `docling-project/TableFormerV2`,
  `docling-project/CodeFormulaV2`; OCR/VLM-варианты — `ibm-granite/*`, `nvidia/nemotron-ocr-v2` и др.;
- часть моделей пинится по `revision` (напр. TableFormer `v2.3.0`) — плюс к воспроизводимости;
- кэш — стандартный HF (`HF_HOME`), полем `local_dir` можно задать свой путь.

**Статус:** ✅ план **подтверждён владельцем (2026-10-08), механика уточнена 2026-10-12,
посылка исправлена 2026-11-08**
(предзакачка бандла → `DOCLING_ARTIFACTS_PATH` + `HF_HUB_OFFLINE=1` как guard → revision-пины →
аудит лицензий весов). ⬜ не сделано — исполнение ниже не подтверждается репозиторием: шаг 1
(бандл весов) — операция окружения и в репозитории не виден; ш. 3–4 отсутствуют — манифест несёт
`extractor_stack` (константы + версия docling, `core/pipeline/run_identity.py:92`), но **не**
repo_id/revision/лицензию весов; env-переключатель `DOCLING_ARTIFACTS_PATH` есть, но остаётся
передать `artifacts_path` в `PdfPipelineOptions` экстракции явно, чтобы путь был объявленным
входом, а не env (`core/adapters/extract_document.py:643-660` — сегодня только OCR-проба).

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
