# Реестровые конфиги (`registries/`)

**Назначение (W6/T6.2).** Здесь лежат **данные**, описывающие внешние реестры для
generic-адаптера `core/adapters/registry_generic.py`. Ядро категорийно-нейтрально (§1.3):
названия реестров, вендоров и предметные термины в `core/` не попадают — вся специфика
внешнего источника живёт в этих YAML-файлах (архитектура `01` §7, `02` §D.1, `03` §D.4).

> **Статус данных.** Реальные endpoint'ы, заголовки и маппинги полей конкретных реестров
> **поставляет владелец домена** (по аналогии с пакетом PG, T6.5). В репозитории намеренно
> нет «настоящих» адресов: примеры из регламента не имеют подтверждённых живых endpoint'ов
> и payload'ов, поэтому мы их **не выдумываем**. Файлы `example_*.yaml` — это шаблоны с
> заведомо несуществующим TLD `.invalid`; они проходят валидацию схемы, но не являются
> рабочими конфигами и не должны использоваться в прогоне.

## Схема файла `registries/{registry_id}.yaml`

Обязательные и опциональные поля полностью совпадают с pydantic-моделью
`RegistryConfig` в `core/adapters/registry_generic.py` (`extra="forbid"` — лишние ключи
считаются ошибкой; строки обрезаются по краям).

| Поле | Тип | Обяз. | Дефолт | Смысл и как используется движком |
|---|---|---|---|---|
| `registry_id` | str | да | — | Идентификатор реестра; непустой. Совпадает с именем файла. Уходит в `RegistryFact.registry_id`. |
| `jurisdiction` | str | да | — | Юрисдикция реестра (например код региона); непустая. Метаданные/подстановка. |
| `endpoint` | str | да | — | URL-шаблон. Подстановка через Python `str.format`: `{query}`, `{product_id}`, `{canonical_name}`, `{vendor}`, `{jurisdiction}`, ключи `product.extra` и `scope`. Если `query` не задан явно — берётся `canonical_name` либо `product_id`. |
| `format` | `json` \| `html` | да | — | Выбор парсера ответа. Другие значения отклоняются. |
| `method` | str | нет | `GET` | Поддерживается **только** `GET` (адаптер не шлёт тело запроса). |
| `mapping` | dict[str,str] | да | — | Непустое отображение «имя поля → путь в записи». См. ниже. |
| `query_param` | str | нет | `null` | Имя query-параметра. Если задано, к URL добавляется `?<query_param>=<query>` (когда такого параметра ещё нет). |
| `records_path` | str | нет | `null` | Как выделить записи. JSON: dot-path (напр. `items`, `data.records[*]`); при отсутствии — верхнеуровневый список либо весь объект как одна запись. HTML: имя тега (напр. `tr`); при отсутствии — все узлы. |
| `headers` | dict[str,str] | нет | `{}` | HTTP-заголовки, передаются внедрённому клиенту. |
| `timeout` | float > 0 | нет | `null` | Таймаут (сек). Передаётся клиенту-объекту с методом `get(url, headers, timeout)`. |

### Синтаксис путей маппинга

Пути **относительны записи** (record-relative).

* **JSON** — подмножество dot-path: ключи (`a.b`), числовые индексы (`items[0]`),
  раскрытие списка (`items[*]`). Ведущие `$` и `$.` отбрасываются, поэтому допустимы
  оба стиля: `name` и `$.name`. Не найденный путь — поле просто опускается в результате.
* **HTML** — значение относится к выбранному элементу (`records_path`):
  * `@attr` — значение атрибута (голое имя атрибута трактуется так же);
  * `#text` / `text` / `.` / пусто — нормализованный агрегированный текст узла;
  * `tag` — имя тега.

### R1 (`source_url`)

Для **каждого** возвращаемого `RegistryFact` движок проставляет `source_url` — это
фактический URL запроса (для `GET` он же — URL источника). Поле **не берётся из
маппинга**: даже если в `mapping` есть ключ `source_url`, фактический `source_url` факта
всегда равен запрошенному URL. Именно поэтому любой конфиг с шаблоном, порождающим
непустой URL, удовлетворяет R1 (100% фактов с `source_url`). Абсолютный URL-адрес
полезной ссылки на запись, если он есть в payload, можно дополнительно вынести
отдельным ключом маппинга (напр. `detail_url`).

## Пример JSON-конфига

```yaml
registry_id: example_json
jurisdiction: XX
endpoint: "https://example.invalid/api/search/{query}"
method: GET
format: json
records_path: items
headers:
  Accept: application/json
timeout: 30.0
mapping:
  name: "name"
  version: "version"
  registration_id: "id"
  detail_url: "detail_url"
```

## Пример HTML-конфига

```yaml
registry_id: example_html
jurisdiction: XX
endpoint: "https://example.invalid/registry/list"
method: GET
format: html
records_path: tr
query_param: name
mapping:
  name: "#text"
  version: "@data-version"
  detail_url: "@data-href"
```

## Как добавить реальный реестр

1. Скопируйте `example_json.yaml` или `example_html.yaml` в `registries/{registry_id}.yaml`.
2. Замените `registry_id`, `jurisdiction` и `endpoint` на подтверждённые владельцем домена
   значения; при необходимости заполните `headers`/`timeout`.
3. Приведите `records_path` и `mapping` к реальной структуре ответа.
4. Прогоните валидацию: `tests/registries/test_registry_configs.py` загружает **каждый**
   `registries/*.yaml` через `load_registry_config` и проверяет, что файл соответствует
   схеме `RegistryConfig`.

Движок: `core/adapters/registry_generic.py`. Документация: `docs/architecture/01` §7,
`docs/architecture/02` §D.1, `docs/architecture/03` §D.4, `docs/architecture/06` W6/T6.2.
