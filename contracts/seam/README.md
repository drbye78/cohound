# Контракты шва core↔edge (`/v1/*`)

Машиночитаемые контракты **единственного** канала между доверенным ядром (Z0) и DMZ
(Z1/edge): HTTP/JSON на уровне L7, версионированный через `schema_version`
(`docs/architecture/07-deployment-topology.md` §3, §8). Ядро **инициирует**, edge
**отвечает**.

Эти файлы — публикуемая спецификация: по ним пишутся политики DLP/IDS/IDP, и по ним
же контракт-тесты (`tests/edge/`, `core/edge/seam_schema.py`) сверяют фактические
ответы.

## Состав

| Файл | Что описывает |
|---|---|
| `envelope.schema.json` | Конверт ответа `{schema_version, request_id, status, payload, error}`; форма запроса (`$defs.request`, `status="request"`) |
| `fetch.schema.json` | `POST /v1/fetch` — запрос `{url, fetch, extract, wait_for, lang}` и ответ (сырые байты `raw` / markdown `text`; извлечение best-effort, D2) |
| `search.schema.json` | `POST /v1/search` — запрос `{query, lang, limit, phase}` и ответ-список `{url, title, snippet, rank}` |
| `registry.schema.json` | `POST /v1/registry` — запрос `{registry_id, product_ref, scope}` и ответ-список `{fact_json, source_url, fetched_at}` (R1: `source_url` обязателен) |
| `crawl.schema.json` | `POST /v1/crawl/{submit,status,cancel,collect,poll}` — постановка задания краула (`items`), прогресс (счётчики), отмена, постраничный забор результатов (`items`+`next_cursor`+`done`) и опрос каталогового документа (`url`+`kind` ∈ `{feed,channel,sitemap}` → `entries`+`count`+`truncated`, ссылки без байтов); контент/ссылки, не evidence (I19) |
| `capabilities.schema.json` | `GET /v1/capabilities` → `{extractor_version, crawler_version, search_engine, supported_formats}` |
| `health.schema.json` | `GET /v1/health` → `{status}` |
| `openapi.json` | OpenAPI 3.1 по всем эндпоинтам; конверт — схема ответа, `schema_version` документирован |

Схемы — JSON Schema draft 2020-12.

## Опциональные поля crawl (execution-класс, §13)

Все нижеперечисленные поля — **аддитивно-опциональные**: старые продюсеры, их не
передающие, остаются схемо-валидными, а старые потребители игнорируют незнакомые ключи.
`SEAM_SCHEMA_VERSION` при этом **не меняется** (остаётся `"1"`), новых операций
`/v1/*` нет.

- `submit_item.robots` (boolean) — флаг egress-вежливости: включать ли проверку
  `robots.txt` для этого элемента. Это **политика**, а не методология; `false`/отсутствие
  означает «robots не трогается» (обратная совместимость — лишних fetch `robots.txt` нет).
- `submit_item.rate_limit` (`{min_s, max_s}` | null) — **резолвнутый per-item** диапазон
  задержек egress (`RateLimitSpec`). Отсутствие/`null` ⇒ без пейсинга. Значения —
  структурные числа (не строка `canonical_rate_limit`), чтобы исполнитель восстановил
  `RateLimitSpec`.
- `submit_request.user_agent` (string) — значение `User-Agent` для egress/robots-политики
  исполнителя (та же операционная настройка, что `deploy.yaml:crawl.user_agent`).
- `collect_item.final_url` (`string` | null) — финальный URL после редиректов. Ядро трактует
  его как `RawPage.url` (база ссылок, redirect-scope); ключ элемента сеанса остаётся
  **запрошенным** URL. Отсутствие трактуется как запрошенный URL (обратная совместимость
  со старым edge).

### Pacing — честная формулировка остатка (§10/§11.3)

`rate_limit` — это **per-item** и **per executing context** пейсинг, **не** глобальный
per-host rate-гейт. Под `split` он исполняется на стороне edge: один стателесс лимитер в
процессе планировщика для потоковых движков и по одному в каждом воркер-процессе для
`scrapy`/`browser`. При `workers>1` параллельные фетчи одного хоста не ограничены сверху,
а кросс-процессный пейсинг не разделяет состояние. Полное соблюдение `rate_limit` как
глобального per-host лимита **не заявляется**. Локальный путь имеет тот же характер при
`max_workers>1`; тайминг не нормируется ни в одном профиле, контент не затрагивается.

## Использование для DLP / IDS / IDP (07 §8)

**Что обеспечивается и при какой конфигурации.** Аутентификация, авторизация,
аудит и лимит тела реализованы в пути запроса, но **включаются конфигурацией**
`edge.yaml`; дефолт локального профиля — без аутентификации. Таблица ниже — это
контракт, а не намерение:

| Механизм | Когда действует | Дефолт |
|---|---|---|
| Аутентификация | `auth.mode: mtls` или `auth.mode: oidc` | `none` |
| Авторизация по операции | там же (allowlist `core.edge.auth`) | нет |
| Аудит | `audit.sink: file` | `none` |
| Лимит тела | **всегда** (`max_body_bytes`) | 1 MiB |

- **IDP.** При `auth.mode: mtls` идентичность — субъект клиентского сертификата
  от доверенного терминирующего прокси (`auth.subject_header`; реестр
  `auth.subjects: subject → [scopes]`). При `auth.mode: oidc` — bearer-JWT
  (`Authorization`); проверяются подпись (верификатор-шов; `pyjwt` —
  опциональная зависимость, без неё fail-closed), `iss`, `aud`, `exp`, scopes.
  Авторизация — по операции: `fetch`→`fetch`, `search`→`search`,
  `registry`→`registry`, `capabilities`→`capabilities`, все `crawl/*`→`crawl`;
  `health` — публичная диагностика. Отказ: **401** (не аутентифицирован) или
  **403** (идентичность есть, scope нет); конверт `status="error"`.
- **Профиль `local` (петля) — неаутентифицирован по замыслу.** `auth.mode: none`
  допустим только на петлевом адресе: `127.0.0.1`, любой `127.0.0.0/8`, `::1`
  или литерал `localhost` (DNS-имена не разрешаются). Это локальный профиль, и
  защита идентичности здесь **не заявляется**.
- **Fail-fast.** Bind на не-loopback адрес (`0.0.0.0`, `::`, внешний
  интерфейс/имя) при `auth.mode: none` **отвергается при старте** с ошибкой,
  называющей адрес. Публикация неаутентифицированного шва в маршрутизируемый
  интерфейс — фактическая угроза, поэтому «забыли auth» не должно молча
  превращаться в открытый endpoint.
- **DLP.** Конверт несёт явные поля контента (`raw.bytes_b64`, `text.content`) —
  политика инспекции пишется по `fetch.schema.json` без разбора нестандартного
  фрейминга (только L7/JSON). Метка класса данных — `classification` из
  `core.edge.audit.Classification` (`internal` по умолчанию, операторский
  `override`); она попадает в аудит-событие и **не** в конверт. Инспекция не
  мутирует содержимое: ядро пересчитывает `content_hash` (I19).
- **IDS.** Метаданные L7-трафика — `request_id`, `op` (путь), `schema_version`,
  размеры — видны по OpenAPI. При `audit.sink: file` каждый вызов даёт ровно
  одну JSON-строку (`ts`, `request_id`, `principal`, `op`, `status`
  ∈ `{allow, deny, error}`, `classification`, `remote`), коррелируемую с логами
  ядра по `request_id` (07 §8.1/§8.4). Отказы 401/403/413 аудируются тоже
  (`deny`/`error`) — аудит-трейл не «теряет» именно отклонённые вызовы.
- **Лимит тела.** `max_body_bytes` (дефолт 1 MiB) действует всегда:
  `Content-Length` сверх лимита отвергается **до чтения** (413), чтение
  ограничено `max_body_bytes + 1`, на 413 соединение закрывается
  (`Connection: close`) — непрочитанное тело не может быть разобрано как
  следующий запрос. 1 MiB обоснован крупнейшей легитимной формой:
  `crawl/submit` с волной из 64 элементов (~128 КБ при URL ~2 КБ), см.
  `core.edge.server.DEFAULT_MAX_BODY_BYTES`.
- **TLS-инспекция не блокируется:** никакого certificate pinning; доверие к
  корпоративному CA настраивается на стороне развёртывания (07 §8.1).

Аттестация против конкретных средств ИБ — вне рамок проекта (07 §8).

## Версионирование (§13)

- `schema_version` — часть контракта. Текущее значение: **`"1"`**
  (`core/edge/seam_schema.py: SEAM_SCHEMA_VERSION`; совпадает с
  `DEFAULT_SCHEMA_VERSION` клиента `core/adapters/remote_gateway.py`).
- **Несовместимое изменение контракта → bump `schema_version`** — версионная граница
  §13. Используемая версия фиксируется в `run_manifest.yaml` (`edge_schema_version`)
  для воспроизводимости R7.
- **HTTP-коды 401/403/413 аддитивны, `schema_version` не меняется.** Форма конверта
  (`{schema_version, request_id, status, payload, error}`) прежняя: новые ответы —
  тот же конверт с `status="error"`; клиент, уже обрабатывающий любой не-200,
  получает их без изменений. Появление новых статусов не ломает ни один
  существующий разбор, поэтому это не версионная граница. Повышение
  `schema_version` — только при несовместимом изменении **формы** конверта или
  семантики существующих полей.
- Детерминированный отпечаток опубликованных контрактов —
  `core.edge.seam_schema.contract_digest()` (sha256 по `contracts/seam/*.json`),
  пригоден для пиннинга версии.

## Прямая валидация

```python
from core.edge import seam_schema

seam_schema.load_schema("envelope")            # dict опубликованной схемы
seam_schema.validate_envelope(response)        # [] = конверт валиден
seam_schema.validate_payload("registry", items)  # [] = payload валиден
seam_schema.contract_digest()                  # "sha256:…"
```

`validate_envelope` / `validate_payload` — stdlib-only (рекурсивная проверка
required/типов/enum-ов). Внешний `jsonschema` — необязательная зависимость
(вердикт **b**, `docs/architecture/05-dependency-stack.md` §2.9) и может валидировать
эти же файлы напрямую.
