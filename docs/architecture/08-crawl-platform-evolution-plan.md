# План развития краулинг-платформы

> Статус документа: целевой инженерный план, пересмотрен 2026-10-19.
>
> Область: split-развёртывание, корпоративный контроль трафика и контента,
> горизонтальное масштабирование, асинхронная обработка PDF и одновременная
> обработка нескольких crawl-профилей.

## 1. Краткий вывод

Текущая реализация уже является содержательным **single-node split MVP**, но не
кластерной платформой. Поэтому дальнейшая работа должна быть не расширением
количества crawler-движков, а расщеплением execution plane на независимые и
контролируемые плоскости.

Целевая архитектура:

```text
Z0 core
  ├─ Crawl Coordinator
  ├─ immutable CrawlPlan / waves
  ├─ distributed leases, quotas, host policy
  ├─ core validation + rehash
  └─ corpus/index/evaluation
          │ inspected HTTP/JSON seam
          ▼
Z1 edge control plane
  ├─ stateless API replicas
  ├─ durable task store / queue
  ├─ profile admission and authorization
  └─ audit/event projection
          │ leases
          ├───────────────┬────────────────┐
          ▼               ▼                ▼
   fetch workers    PDF workers       result ingesters
   http/scrapy/    text/layout/OCR    manifest + hash
   browser              │
          └─────── mandatory egress/content gateway ───────┐
                                                           ▼
                                                  corporate IDS/IDP/DLP
                                                           ▼
                                                        Internet
```

Главные принципы:

1. **Core владеет намерением и политикой.** Edge исполняет только выданные
   задания и не создаёт собственную методологическую семантику.
2. **Каждый outbound-запрос проходит через контролируемый egress-контур.**
   Прямой Internet-доступ worker-процессов запрещён.
3. **Большой входящий контент передаётся структурированно.** Manifest, MIME,
   hash, размер, классификация и correlation IDs видны корпоративным системам;
   непрозрачная ссылка на локальный blob не является достаточным контрактом.
4. **План неизменяем, исполнение возобновляемо.** Retry создаёт новую attempt,
   а не переписывает исходный факт.
5. **Fetch и PDF extraction — разные ресурсные классы.** PDF workers могут
   масштабироваться независимо от HTTP/browser workers.
6. **Crawl profile — это ресурсная и security-изоляция, а не строковое поле
   запроса.** Deployment profile (`local`/`split`) и workload profile должны
   быть разными сущностями.

Обратную совместимость с текущим `/v1` seam и локальной моделью edge-state
сохранять не требуется. При этом полезные текущие реализации можно оставить
как single-node adapter для разработки и тестов.

## 2. Что уже сделано в текущем HEAD

Рабочее дерево на момент аудита чистое; ниже оценивается фактически
закоммиченная реализация, а не гипотетический diff.

### 2.1. Реально реализовано

| Область | Состояние | Доказательство |
|---|---|---|
| Z0/Z1 boundary | Реализовано | `docs/architecture/07-deployment-topology.md:24-88` |
| L7 HTTP/JSON seam и схемы | Реализовано как контракт | `contracts/seam/`, `core/edge/server.py:823-913` |
| Core-side remote execution | Реализовано | `core/adapters/remote_crawl_executor.py:100-114` |
| Edge durable queue | Реализовано для одного узла | `core/edge/scheduler.py:161-209, 946-1041` |
| Recovery/cancel/collection | Реализовано для одного узла | `core/edge/scheduler.py:948-1041` |
| Core-side hash verification | Реализовано | `core/adapters/remote_crawl_executor.py:1-15` |
| Split BFS waves | Реализовано, waves последовательны | `core/pipeline/corpus.py:2668-2839` |
| Local bounded extraction | Реализовано | `core/cli/wiring.py:1253-1271` |
| Browser process isolation | Реализовано на edge-узле | `core/edge/scheduler.py:17-22, 860-895` |
| Feed/channel/sitemap routing | Реализовано с текущими ограничениями | `core/pipeline/corpus.py`, `core/pipeline/crawl_sources.py` |

### 2.2. Реализовано частично

1. **IDP/auth/audit.** Библиотеки и конфигурационные примитивы существуют
   (`core/edge/auth.py`, `core/edge/audit.py`, `core/config/deploy.py`), но
   текущий transport handler в `core/edge/server.py:890-902` вызывает
   `route_request` без видимого общего middleware, которое гарантированно
   выполняет authentication, authorization, classification и audit до dispatch.
   Нельзя считать runtime enforcement завершённым до появления такого тестируемого
   middleware.
2. **PDF concurrency.** Локальный `ProcessPoolExecutor` и FIFO-commit полезны,
   но в split extraction не является отдельной долговечной очередью. Документы
   всё ещё обрабатываются в рамках foreground pipeline (`core/cli/commands/crawl.py:272-326`).
3. **Structured seam.** Core↔edge сообщения структурированы, но outbound
   edge→Internet-запросы не представлены отдельным обязательным egress-протоколом.
4. **Profile.** Поля конфигурации и seam-профили есть, но не образуют независимый
   workload namespace с квотами, очередями, egress identity и storage isolation.

### 2.3. Пока отсутствует

- shared durable queue/metadata store для нескольких edge-узлов;
- cluster-wide leases, heartbeats, reclaim и идемпотентная запись результата;
- shared immutable object storage как часть correctness-модели;
- глобальный per-host scheduler между worker-процессами и узлами;
- отдельная PDF extraction service/queue/deployment;
- first-class workload profile isolation;
- обязательный egress gateway без прямого Internet bypass;
- доказанный runtime enforcement auth/audit на реальном HTTP boundary;
- detached durable crawl job вместо исполнения в CLI-процессе.

Важно: текущий edge scheduler обеспечивает вертикальную масштабируемость одного
узла, но `core/edge/config.py:49-78` и `core/edge/scheduler.py:161-209`
показывают локальные SQLite/blob paths. Load-balancer перед несколькими такими
узлами не превращает их в кластер: job state и blob могут оказаться на разных
репликах.

## 3. Целевые границы доверия и масштабирования

### 3.1. Z0 — trusted core

Z0 содержит:

- run/session/profile orchestration;
- CrawlPlan и source waves;
- URL canonicalization, scope, deduplication;
- retry/refusal/partial policy;
- distributed host policy;
- core-side retention, rehash и принятие контента;
- индексы, evaluator и отчёты.

Z0 не имеет общего Internet route. Единственный путь к Z1 — опубликованный
HTTP/JSON seam с authentication, authorization и audit.

### 3.2. Z1 control plane

Z1 control plane должен быть stateless:

- несколько API replicas за load balancer;
- schema validation и request-size limits;
- authentication/authorization middleware;
- profile admission;
- task creation и lease API;
- structured audit events;
- отсутствие локального состояния, необходимого для корректности.

Текущая `EdgeScheduler` может быть временно использована как local executor,
но в целевой архитектуре authoritative queue не должна жить внутри одного
процесса.

### 3.3. Z1 execution plane

Разделить deployment units:

| Unit | Масштабирование |
|---|---|
| HTTP fetch | concurrent requests / network |
| Scrapy | process slots / memory |
| Browser | contexts / RAM |
| PDF text | documents/sec / CPU |
| PDF layout/table | pages/sec / RAM |
| OCR | pages/sec / CPU/GPU |
| Egress gateway | requests/sec / inspection capacity |
| Result ingester | bytes/sec / storage throughput |

Для нагрузки 10–15 продуктов × 3 сайта это не требует немедленно Kafka или
полноценного workflow engine. Базовый вариант: PostgreSQL-compatible metadata
store с `FOR UPDATE SKIP LOCKED`, delayed retry и leases, плюс S3-compatible
immutable object storage. Queue abstraction должна быть портом, чтобы позже
заменить PostgreSQL-backed queue специализированным broker без изменения
доменной модели.

## 4. Enterprise control plane: IDS/IDP/DLP

### 4.1. Обязательный маршрут outbound

```text
worker → Egress Policy Gateway → corporate proxy/IDS/DLP → Internet
```

Worker не получает прямого маршрута к Internet. Это должно быть проверяемо
сетевой политикой, а не только обещанием в конфиге.

Egress gateway является policy enforcement point и обязан:

- принимать только schema-valid structured requests;
- применять allow/deny по profile, host, URL scope и capability;
- выполнять DNS/IP/redirect safety checks;
- контролировать robots, pacing и byte/time limits;
- сохранять policy decision и policy version;
- fail closed при собственной недоступности;
- возвращать structured response или structured refusal.

### 4.2. Egress request envelope

Минимальные поля:

```json
{
  "schema_version": "egress.v1",
  "request_id": "req-...",
  "parent_request_id": "task-...",
  "run_id": "run-...",
  "session_id": "session-...",
  "profile_id": "profile-...",
  "product_id": "product-...",
  "site_id": "site-...",
  "task_id": "task-...",
  "url": "https://docs.example/manual.pdf",
  "method": "GET",
  "transport": "http",
  "classification": "untrusted-public",
  "policy_hash": "sha256:...",
  "scope": {"origin": "https://docs.example", "path_prefix": "/p1/"},
  "limits": {"timeout_s": 30, "max_bytes": 50000000, "max_redirects": 5}
}
```

Ответ обязан содержать:

- `request_id` и `task_id`;
- decision (`allowed`, `blocked`, `unavailable`, `failed`);
- policy version и audit event ID;
- requested/final URL и полный redirect chain;
- status, content type, size и content hash;
- resolved addresses и egress identity;
- timestamps и byte counters;
- content transfer manifest либо structured refusal.

Не разрешать silent fallback на прямой fetch, если gateway или DLP недоступны.

### 4.3. Контентный envelope

Для маленьких сообщений допустим JSON envelope. Для PDF и других больших
объектов нужен inspectable `multipart/related` или chunked protocol:

1. JSON manifest: task/profile/run IDs, MIME, size, whole hash, chunk hashes,
   classification, source and policy decisions.
2. Binary content stream/chunks.
3. Финальный manifest с assembled hash и DLP decision.

Object-store URI не может быть единственным контрактом: он скрывает содержимое
от L7 DLP. После передачи получатель пересчитывает hash; несоответствие ведёт
в quarantine, а не в extraction.

### 4.4. Browser

Browser worker должен быть помещён в network namespace без direct Internet
egress. Через egress gateway проходят:

- main-frame navigation;
- redirects;
- XHR/fetch;
- downloads;
- subresources;
- WebSocket — запрещён по умолчанию.

Нужно явно ограничить third-party origins и не считать проверку только исходного
URL достаточной.

## 5. Distributed coordination

### 5.1. Immutable plan

Добавить доменные сущности:

```text
CrawlPlan       immutable intent + policy snapshot
PlanEntry       immutable URL/source/scope/fetch policy
Wave            immutable source/discovery batch
Attempt         append-only execution fact
Lease           ownership with expiry
ExecutionState  materialized current projection
Event           auditable state transition
```

`plan_hash` должен включать:

- canonical seeds и source digest;
- effective scope/limits;
- robots, retry, redirect и host policy;
- selected capabilities/transport;
- profile policy hash;
- schema/engine versions.

Изменение эффективной policy создаёт новый plan/execution version, а не меняет
старый plan.

### 5.2. Lease protocol

Каждое задание имеет:

```text
task_id, plan_hash, profile_id, attempt_no,
worker_id, lease_id, claimed_at, lease_expires_at,
heartbeat_at, idempotency_key
```

Worker должен heartbeat-ить lease и коммитить результат идемпотентно. После
expiry задание reclaimable. Duplicate delivery допустима; duplicate accepted
artifact — нет.

### 5.3. Host scheduler

Scheduler должен быть глобальным для cluster, а не process-local:

- `origin = scheme + normalized host + effective port`;
- concurrency и минимальный inter-request delay;
- retry backoff и `Retry-After`;
- cluster/profile/run/product budgets;
- fairness между профилями;
- host circuit breaker;
- robots coordination;
- queue/lease age metrics.

Profile quotas независимы, но host politeness общая: два профиля не должны
суммарно обходить один origin быстрее установленного лимита.

## 6. Независимая PDF-плоскость

### 6.1. Lifecycle

```text
FETCH_REQUESTED
  → FETCHED_RAW
  → CONTENT_SCANNED
  → EXTRACTION_QUEUED
  → EXTRACTING
  → EXTRACTED
  → EXTRACTION_SCANNED
  → ACCEPTED_BY_CORE
```

Отдельные terminal/error states:

```text
FETCH_FAILED
DLP_BLOCKED
EXTRACTION_FAILED
EXTRACTION_TIMEOUT
EXTRACTION_OOM
QUARANTINED
```

Fetch считается завершённым после получения, DLP decision, immutable retention
и hash verification. Extraction не блокирует durable raw artifact и может быть
повторена независимо.

### 6.2. Очереди и deployment units

Минимальный набор:

```text
extract.pdf.text
extract.pdf.layout
extract.pdf.tables
extract.pdf.ocr
extract.office
deadletter.extract
```

Минимум text и layout/OCR должны быть разными resource classes. Тяжёлые
Docling/OCR jobs не должны вытеснять дешёвое text extraction.

PDF workers:

- не имеют Internet route;
- читают raw object только read-only;
- пишут только extraction artifact namespace;
- имеют CPU/RSS/wall-clock/temp-disk limits;
- не имеют доступа к evaluator или crawl policy;
- возвращают extractor version, source content hash, result hash и usage.

Пример результата:

```json
{
  "task_id": "extract-...",
  "content_hash": "sha256:...",
  "extractor": "docling",
  "extractor_version": "...",
  "document_level": "layout_tables",
  "status": "succeeded",
  "result_hash": "sha256:...",
  "result_ref": "...",
  "page_count": 42,
  "resource_usage": {"peak_rss_bytes": 1900000000, "wall_time_ms": 88000}
}
```

Текущий `extract_workers` следует сохранить как local adapter, но документация
должна называть его bounded foreground extraction, пока не появятся отдельные
очереди и worker deployments.

## 7. Workload profiles

Развести термины:

| Термин | Значение |
|---|---|
| `deploy_profile` | где работает система: `local`, `split` |
| `crawl_profile` | policy, egress, quotas и worker pools для workload |
| `domain_profile` | scope/source/document rules конкретного сайта |
| `retrieval_profile` | настройки retrieval/evaluation |

`crawl_profile_id` выбирается из trusted registry, а не принимается как
произвольная policy от caller.

Профиль должен определять:

- allowed transports и capabilities;
- egress identity;
- queue namespaces;
- per-profile inflight/bytes/PDF quotas;
- host delay/concurrency defaults;
- storage namespace и retention;
- credentials reference, если она вообще разрешена;
- browser state namespace.

Scheduler hierarchy:

```text
cluster → crawl_profile → run → product → site → origin → resource class
```

Два профиля должны обрабатываться одновременно, но иметь независимые quotas,
queues, cancellation, metrics и browser state. При этом origin politeness остаётся
общей.

## 8. Фазы реализации

### Phase 0 — зафиксировать контракт и acceptance baseline

**Цель:** убрать неоднозначность до distributed refactor.

- Утвердить `deploy_profile`/`crawl_profile`/`domain_profile` naming.
- Зафиксировать scope sitemap и source-wave semantics.
- Описать state machine fetch/extraction/attempt/lease.
- Определить fail-closed policy для egress/DLP/robots.
- Сохранить baseline focused tests и текущие известные ограничения.
- Исправить stale note `.slim/deepwork/crawl-control/progress.md`, где split
  BFS ещё помечен как недоступный, хотя `core/pipeline/corpus.py:2668-2839`
  уже реализует последовательные split waves.

**Gate:** схема состояний и acceptance matrix утверждены; нет противоречия
между code, docs и tests.

### Phase 1 — фактическое enforcement на seam

**Цель:** сделать IDP/auth/audit реальным runtime boundary.

- Ввести единый middleware вокруг `route_request`/HTTP handler.
- До dispatch выполнить authentication, authorization, schema и size validation.
- Сформировать classification и correlation context.
- Emit audit event на allow/deny/error, включая identity, operation, policy
  version и payload hashes.
- Запретить endpoint-specific обход middleware.
- Добавить tests с fake mTLS/OIDC claims и deny cases.

**Gate:** unauthenticated/unauthorized request не достигает gateway; каждый
allow/deny имеет audit event; oversized/invalid content не попадает дальше.

### Phase 2 — egress/content policy gateway

**Цель:** сделать outbound request и inbound content контролируемыми IDS/DLP.

- Ввести `EgressRequest`, `EgressDecision`, `ContentManifest`, `ContentChunk`.
- Запретить direct Internet sockets у fetch/browser workers на deployment уровне.
- Реализовать URL/DNS/IP/redirect checks на каждом hop.
- Реализовать multipart/chunked transfer с whole/chunk hashes.
- Вернуть policy version, decision ID, redirect chain, resolved addresses и
  content metadata.
- Fail closed при unavailable gateway/inspection path.
- Добавить browser subrequest interception.

**Gate:** proxy/DLP harness видит structured request и полный content stream;
response tampering выявляется core rehash; bypass route отсутствует.

### Phase 3 — distributed control plane

**Цель:** превратить single-node queue в cluster-capable execution plane.

- Ввести ports для metadata store, task queue, object store и lease store.
- Реализовать immutable plans/waves/entries.
- Перенести authoritative task state из edge-local SQLite в shared store.
- Добавить leases, heartbeats, reclaim, delayed retry, DLQ.
- Вынести raw blobs в shared immutable object storage.
- Сделать edge API stateless и безопасным за load balancer.
- Оставить текущий SQLite scheduler только как local adapter.

**Gate:** две API replicas и два worker nodes безопасно обрабатывают один run;
crash/reclaim/duplicate result tests проходят; local filesystem не нужен для
correctness.

### Phase 4 — cluster-wide scheduler и profile quotas

**Цель:** эффективная одновременная обработка 45 сайтов и нескольких profiles.

- Реализовать global origin scheduler.
- Добавить cluster/profile/run/product/site quotas.
- Добавить weighted fair scheduling и reserved capacity.
- Разделить queues по engine/resource class/profile namespace.
- Ввести profile admission из trusted registry.
- Сохранить общую origin politeness между профилями.

**Gate:** concurrent load test с несколькими профилями не нарушает per-origin
limits; profile A не потребляет reserved capacity profile B; cancellation и
metrics независимы.

### Phase 5 — asynchronous PDF plane

**Цель:** независимое масштабирование extraction.

- Разделить fetch completion и extraction completion.
- Ввести `ExtractionTask`, `ExtractionAttempt`, extraction leases и results.
- Добавить text/layout/tables/OCR queues.
- Добавить отдельный process/container deployment для PDF workers.
- Принудительно передавать document level, extractor version и source hash.
- Добавить resource limits, quarantine и retry/DLQ.
- Поддержать scale-out PDF workers без scale-out HTTP workers.

**Gate:** killed/OOM PDF worker не теряет raw artifact; layout queue масштабируется
отдельно; extraction output проверяется по hash и DLP decision.

### Phase 6 — unified fetch adapters

**Цель:** одинаковые semantics для HTTP/Scrapy/browser.

- Ввести structured `FetchAttemptResult`.
- Перенести retry/refusal decision из adapter defaults в core policy.
- Сделать redirects manual и проверяемыми per-hop.
- Унифицировать robots decision и error semantics.
- Реализовать resolved-IP validation/pinning либо явно документированный proxy
  delegated-resolution mode.
- Сохранить process isolation browser.

**Gate:** одинаковый fake-server сценарий даёт одинаковую классификацию ошибок,
redirects, robots decisions и provenance во всех выбранных транспортах.

### Phase 7 — source waves и split parity

**Цель:** не потерять корректность текущего corpus pipeline.

- Превратить sitemap/feed/channel expansion в явные child waves.
- Core валидирует и deduplicates все returned URLs.
- Явно различать `scope_rejected`, `blacklisted`, `ssrf_rejected`, `duplicate`
  и `budget_exhausted`.
- Проверить parity local/clustered split по content set, hashes, truncation и
  statuses.
- Сохранить правило: feed/sitemap не являются evidence автоматически.

**Gate:** один plan в local adapter и clustered split даёт одинаковый accepted
corpus при одинаковых fake responses и effective policy.

### Phase 8 — эксплуатация, миграция и документация

- Добавить metrics: queue depth/age, lease age, retries, DLQ, bytes, PDF RSS,
  DLP blocks, redirect rejects, policy mismatches и orphan blobs.
- Добавить `inspect`, `pause`, `resume`, `cancel`, `reconcile`, `replay`,
  `explain-entry`.
- Обновить `02-subsystems`, `03-data-config-cli`, `04-reliability-adr`,
  `06-implementation-strategy`, `07-deployment-topology`, README и seam schemas.
- Удалить или явно переименовать устаревшие `/v1` contracts; compatibility не
  является ограничением.
- CI должен fail, а не skip, если pytest/coverage/security integration tests
  отсутствуют.

**Gate:** production-like deployment drill с двумя edge replicas, отдельным
PDF pool, двумя crawl profiles и корпоративным proxy/DLP harness.

## 9. Приёмочная матрица

### Enterprise control

- Z0 не имеет произвольного Internet route.
- Worker не имеет прямого Internet route.
- Все navigation, redirect, download и browser subrequest проходят gateway.
- IDS/DLP видит structured metadata и content stream.
- DLP block предотвращает передачу в parser/core.
- IDP scopes ограничивают operation/profile.
- Gateway unavailable не вызывает direct-fetch fallback.
- Core обнаруживает изменение body через rehash.

### Cluster

- Две и более API replicas не теряют и не двойным образом не владеют task.
- Worker crash приводит к reclaim после lease expiry.
- Duplicate result commit идемпотентен.
- Shared queue/store/object storage являются источником истины.
- Queue age, leases, retries, DLQ и per-profile usage наблюдаемы.
- Cluster execution эквивалентен single-node execution при одной policy.

### PDF

- Fetch worker не выполняет тяжёлую PDF extraction.
- Text/layout/OCR pools масштабируются независимо.
- Raw PDF сохраняется при parser crash/OOM/timeout.
- Extractor version, source hash, result hash и resource usage записаны.
- Parser worker не имеет Internet route.
- Failed extraction не превращается в пустой документ или ложное отсутствие.

### Profiles

- Два профиля запускаются одновременно.
- Quotas, queues, cancellation и metrics независимы.
- Profile policy выбирается server-side и авторизуется через IDP.
- Browser state, credentials, egress identity и storage namespace изолированы.
- Общая origin politeness действует между profiles.

### Методологическая честность

- `partial`, truncation, blocked и failed видны в session provenance.
- Ни один failed fetch не создаёт absence автоматически.
- Cached/raw reuse отличим от live fetch.
- Смена effective crawl policy создаёт execution change.
- После технической приёмки всё ещё выполняются реальные PDF/Office, golden,
  adversarial и manual-baseline проверки из `cohound-spec-v2.md:581-615`.

## 10. Не делать

1. Не превращать текущий edge SQLite в псевдокластер через shared filesystem.
2. Не считать `profile` в текущем crawl schema доказательством изоляции.
3. Не передавать PDF только как opaque object-store URL, если требуется DLP
   inspection.
4. Не оставлять прямой Internet route как «fallback» для gateway.
5. Не смешивать PDF OCR/layout jobs с HTTP queue.
6. Не добавлять Kafka/Kubernetes/workflow engine до измерения bottleneck
   PostgreSQL-backed coordination.
7. Не переносить source routing, scope, retry или methodological decisions в edge.
8. Не делать stealth/CAPTCHA bypass/residential proxy features.
9. Не называть foreground `extract_workers` асинхронной распределённой PDF
   платформой.

## 11. Итоговая последовательность приоритетов

1. Runtime auth/audit middleware и строгие seam contracts.
2. Mandatory egress/content gateway с fail-closed policy.
3. Immutable plan + shared leases/metadata/object storage.
4. Cluster-wide scheduler, quotas и profile isolation.
5. Независимая PDF extraction plane.
6. Adapter parity, safe redirects/DNS и source-wave parity.
7. Load, failure-injection, corporate inspection и real-data acceptance.

До завершения пунктов 1–4 проект следует описывать как **single-node split
MVP с вертикальной параллельностью**, а не как горизонтально масштабируемую
платформу. После пунктов 1–6 он сможет претендовать на кластерное исполнение;
после пунктов 7 и реальных acceptance runs — на подтверждённую эксплуатационную
готовность для целевых когорт.
