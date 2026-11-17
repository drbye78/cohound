# Шаблон паспорта продукта (`passports/_template/`)

**Назначение (§4.3).** Это **скелет** паспорта продукта — «максимально полный» пример,
показывающий все поля схемы `Passport` и установленного сателлита (`state: approved`
со всеми обязательными полями и `relationship_evidence`).

> **Статус данных.** Реальные данные **поставляет владелец домена**; здесь только
> плейсхолдеры `TODO`. `upstreams` ссылается на линию, объявленную в
> `packages/_template/sources/reference_products.yaml` (`baseline_id: upstream-line`) —
> это паспорт архетипа **B** (кросс-линейное сравнение). `doc_domains` указывают на
> зарезервированный TLD `.invalid` (RFC 2606) — это заведомо несуществующие адреса, не
> «живые» endpoint'ы.

## Раскладка

```
passports/{product_id}/{YYYY-MM-DD}.yaml
```

Загрузчик `core/config/passports.py::load_passport` выбирает **самый свежий** файл по
ISO-дате в имени. Каталог `_template/` нейтрален: имя продукта `example_product`
синтетическое. `load_passports('passports')` обходит подкаталоги, поэтому шаблон
валиден как обычный паспорт.

## Обязательные поля

| Поле | Смысл |
|---|---|
| `product_id` | Иммутабельный slug; совпадает с именем каталога. |
| `canonical_name`, `vendor` | Каноническое имя и вендор. |
| `upstreams` | Предки продукта (план 27 §3.2): список `{baseline_id, version, evidence: {url, quote}}`; **порядок = приоритет** (наследует первый, у кого есть строка политики и принятый факт). Архетип C — пустой список. В шаблоне одна линия-плейсхолдер `upstream-line`, объявленная в `packages/_template/sources/reference_products.yaml`. |
| `edition_assessed`, `version` | Оцениваемая редакция и версия. |
| `jurisdiction` | Юрисдикция (`XX` — фиктивный код). |
| `doc_domains` | Домены документации → URL-кандидаты (не evidence, I19). |
| `satellites` | Сателлиты: `draft`/`approved`/`edited_by_human`/`not_found`. |
| `snapshot_date` | Дата снимка. |

Для `state ∈ {approved, edited_by_human}` обязательны `component`, `origin`,
`relationship`, `evidence_state` и `relationship_evidence` (`{url, quote}`).
