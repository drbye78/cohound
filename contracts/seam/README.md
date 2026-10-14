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
| `crawl.schema.json` | `POST /v1/crawl/{submit,status,cancel,collect,poll}` — постановка задания краула (`items`), прогресс (счётчики), отмена, постраничный забор результатов (`items`+`next_cursor`+`done`) и опрос фида/канала (`url`+`kind` → `entries`+`count`+`truncated`, ссылки без байтов); контент/ссылки, не evidence (I19) |
| `capabilities.schema.json` | `GET /v1/capabilities` → `{extractor_version, crawler_version, search_engine, supported_formats}` |
| `health.schema.json` | `GET /v1/health` → `{status}` |
| `openapi.json` | OpenAPI 3.1 по всем эндпоинтам; конверт — схема ответа, `schema_version` документирован |

Схемы — JSON Schema draft 2020-12.

## Использование для DLP / IDS / IDP (07 §8)

- **DLP.** Конверт несёт явные поля контента (`raw.bytes_b64`, `text.content`) — политика
  инспекции пишется по `fetch.schema.json`, без разбора нестандартного фрейминга
  (только L7/JSON). Класс данных (`classification`, 07 §8.5) планируется в T13.9.
  Инспекция не мутирует содержимое: ядро пересчитывает `content_hash` (I19).
- **IDS.** Метаданные L7-трафика — `request_id`, `op` (путь), `schema_version`, размеры —
  видны по OpenAPI; коррелируются с аудитом по `request_id` (07 §8.1/§8.4).
- **IDP.** Идентичность — mTLS workload-identity или OIDC/JWT bearer (07 §8.2);
  авторизация по операции (`fetch`/`search`/`registry`/`crawl`). Схемы `openapi.json`
  фиксируют allowlist путей и заголовки `X-Request-Id` / `X-Schema-Version`.
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
