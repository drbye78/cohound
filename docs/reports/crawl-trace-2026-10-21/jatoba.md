# Трасса краула — `jatoba`

Прогон `ru-pg-dbms-e957c5e8fe49`, продукт `jatoba` (когорта `ru-pg-dbms`).
Срез: **2026-10-21T20:40Z**. Источник: `runs/ru-pg-dbms-e957c5e8fe49/cohound.db`,
`state/ru-pg-dbms/state.db`.

Итог одной строкой: обход **закончился исчерпанием фронта** — не бюджетом и не
глубиной. Документация продукта в корпус не попала, и причины этого лежат **вне**
краула (scope, блэклист, потолок payload), а не в исчерпании обхода.

---

## 1. Вход: seeds и разрешённые пределы

`--urls packages/ru-pg-dbms/sources/corpus_urls.yaml` полностью заменяет seeds
(`core/cli/context.py:_load_corpus_urls` → `corpus.py:url_map`). Три seed-URL:

| seed | профиль (`domain_profiles.yaml`) | `max_depth` | `max_documents` | sitemap | fetch |
|---|---|---|---|---|---|
| `https://jatoba.ru/` | `jatoba.ru` | 2 | 50 | `https://jatoba.ru/sitemap.xml` | http |
| `https://www.gaz-is.ru/` | `gaz-is.ru` | **0** | 10 | — | http |
| `https://www.gaz-is.ru/produkty/inform-sistemy/subd-jatoba/` | `gaz-is.ru/produkty/inform-sistemy/subd-jatoba` | 8 | 200 | — | http |

`max_depth: 0` у host-корня — не экономия, а защита I17: обход корня компании
затянул бы в партицию `jatoba` страницы соседних продуктов. Продуктовый обход живёт
на path-ключе.

Проверено исполнением (`Frontier.add_seed` на реальных лимитах): все три seed получают
`queued`, ни один не зачёркнут блэклистом, ни один не усечён на входе.

## 2. Лестница глубины

11 элементов сеанса: **10 `fetched`, 1 `failed`**, `pending`/`running` = 0.

| глубина | fetched | failed | что это |
|---|---|---|---|
| `None` | 1 | 0 | `sitemap.xml` — не страница (нет `raw_path`, I19) |
| 0 | 4 | 0 | три seed + `<loc>`-seed `politic.pdf` из sitemap |
| 1 | 5 | 1 | ссылки главной `jatoba.ru` + единственный отказ |

Максимальная достигнутая глубина — **1** при потолке 2 у `jatoba.ru` и 8 у path-ключа.
Потолок глубины не достигнут ни на одном корне: `Frontier.offer` на границе не
срабатывал, `truncated_reason="max_depth"` не выставлялся.

## 3. Декодированные отказы

Отказ ровно один — и он декодируется полностью:

```
URL:    https://jatoba.ru/download/documentationJatoba18_4.zip
класс:  source_unavailable          ← записано
факт:   payload_too_large           ← что произошло на самом деле
текст:  "Expected to receive 125727643 bytes which is larger than download max size (20971520)"
```

125 727 643 B против `DOWNLOAD_MAXSIZE` = 20 971 520 B (`crawl.max_response_bytes`,
20 MiB). То есть файл отвергнут **по размеру**, а не «источник недоступен».

Причина потери класса — `core/adapters/crawl_scrapy.py:_is_oversize_failure` (`:503`):
предикат ищет подстроки `maxsize`, `maximum size`, `downloaded more than`, `too large`,
`filesize`, `stopdownload`, а Scrapy формулирует иначе — `larger than download max size`.
Проверено прямым вызовом: на точной строке Scrapy предикат возвращает `False`, и
`_record_failure` (`:322-345`) падает в `source_unavailable`.

Почему это не косметика: `payload_too_large` и `source_unavailable` **оба** не входят в
`BLOCKING_FAILURE_KINDS` (`core/domain/enums.py:110`), поэтому здесь потеря класса не
меняет покрытие — но она лишает оператора единственного сигнала «файл есть, он просто
больше потолка» и делает невидимой стоимость поднятия потолка.

**Побочное следствие для будущего спуска в архивы:** это тот самый архив документации
(125.7 MB, 47 членов, все PDF). Он не «недоступен» — он отсечён осью
`max_response_bytes`. При `crawl.max_artifact_bytes = 256 MiB` тот же URL пройдёт как
артефакт, а не как страница.

## 4. Документы: 9 из 10

`corpus_docs` для `jatoba` = **9**: `jatoba.ru` 7, `www.gaz-is.ru` 2.

Учёт fetched → doc (по нормализованному URL, схлопывая хвостовой `/`):

| | шт. | что |
|---|---|---|
| URL совпал с документом | 9 | все 9 документов |
| пустой текст | 1 | `https://jatoba.ru/sitemap.xml` |

Единственный «fetched без документа» — sitemap: элемент `kind="sitemap"` пишется
`FETCHED` без `raw_path` намеренно (не контент, I19). Это не потеря.

Состав документов (по `extractor_version`): 3 × `trafilatura-html-2` (три HTML-страницы:
`jatoba.ru/`, `www.gaz-is.ru/`, `www.gaz-is.ru/…/subd-jatoba/`), 6 ×
`docling-doc-3:layout_tables` (PDF). Смешанные
`fetched_at` (2026-10-11 … 2026-10-18) — документы переиспользованы из `state.db`, а не
извлечены в этом прогоне.

## 5. Чем именно остановился обход

**Исчерпание фронта, а не потолки.** Проверено по счётчикам:

- `jatoba.ru` — 7 из `max_documents` 50, глубина 1 из 2;
- `www.gaz-is.ru` — 2 из 10, глубина 0 (потолок `max_depth: 0` штатный: обходится
  только сам корень);
- path-ключ `.../subd-jatoba` — **0 документов** при потолке 200 и глубине 8.

Sitemap не дал обхода: `https://jatoba.ru/sitemap.xml` содержит ровно два `<loc>` —
`/` и `/politic.pdf`, оба с `lastmod` 2022-07-13. То есть `<loc>`-seeds дали ноль
новых путей.

## 6. Почему документация продукта не попала в корпус

Это не отказ краула, а два независимых барьера — оба вне логики обхода.

**(а) path-scope.** Страница `https://www.gaz-is.ru/produkty/inform-sistemy/subd-jatoba/`
содержит **189 ссылок**; из них под префиксом корня `/produkty/inform-sistemy/subd-jatoba`
лежит ровно **одна** — сама эта страница. Все остальные — соседние продукты
(`/produkty/inform-sistemy/monitor3s-2-0.html`, `/produkty/upravlenie-ib/ankey-idm.html`, …),
общесайтовые разделы (`/o-kompanii/...`), внешние хосты (`academy.gaz-is.ru`,
`docs.cntd.ru`, `habr.com`, …). Поэтому `Frontier.discover` не поставил ни одного
ребёнка: **0 элементов** в scope на глубинах 1+.

При этом на той же странице лежат **17 прямых PDF** `/files/products/jatoba/*.pdf` и
**14 ссылок Joomla-компонента** `/component/jdownloads/?task=download.send&id=…` — все
вне path-scope корня. То есть документация физически доступна и лежит на один клик
глубже, но архитектура scope (`root_path_prefix`, `crawl_frontier.py:452`) её отсекает.
Это корректное поведение по I17: тот же `/files/products/` содержит документы других
продуктов.

**(б) блэклист.** На главной `https://jatoba.ru/` есть ссылка
`https://jatoba.ru/download/documentationJatoba18_4.zip`. До 21.10 она отбрасывалась
`exclude_documents: ['*.zip','*.rar','*.7z',…]` (`blacklist_match` по basename). Глоб
удалён коммитом `069476a` (24 → 21 глоб), но в прогоне `e957c5e8fe49` архив всё равно
не добрался: он отсечён на шаге (в) — потолком размера.

**(в) потолок payload.** 125.7 MB > 20 MiB ⇒ отказ (см. §3). Это тот барьер, который
снимает `crawl.max_artifact_bytes`.

Отдельно: ссылок `jdownloads` **14**, а не 12 — число в предыдущей заметке владельца
было занижено; суффикса у них нет, документ определяется только по заголовкам ответа
(`content-type` + `content-disposition`).

## 7. Наблюдения, требующие решения (не факты о продукте)

1. **Предикат oversize не знает формулировку Scrapy.** Живое измерение: 1 отказ здесь и
   19 на `edu.postgrespro.ru` (см. `postgrespro.md`). Правка предиката — `execution_change`
   (таксономия отказов), тест обязателен.
2. **`http_status` не пишется на FAILED-пути.** У всех 11 элементов `http_status = NULL`,
   включая успешные: успешный путь пишет его только когда адаптер вернул статус, а
   scrapy-путь для jatoba его не пронёс. Для единственного отказа это скрывает, что ответ
   был 200 с `Content-Length` 125.7 MB.
3. **Потолок размера не оставляет следа в провенансе усечения.** Отказ виден как
   `failed`-элемент, но `truncated`/`truncated_reason` продукта не поднимаются: потеря
   документа по размеру не влияет на `complete` (класс не блокирующий). При переходе на
   артефакты это надо пересмотреть отдельно — иначе «архив не влез» останется
   невидимым для гейта покрытия.

## 8. Открытые вопросы

- Точный список URL, которые отсек `Frontier.discover` по блэклисту, в прогоне **не
  сохраняется** (`skipped_blacklisted` живёт в памяти `Frontier` и попадает только в
  `IndexBuild`-провенанс, который не был записан). Числа в §6(б) восстановлены по
  ретенции главной страницы, а не из провенанса прогона.
- Сколько из 17 PDF `/files/products/jatoba/` относятся именно к `jatoba`, а не к
  соседним продуктам, по данным прогона не определить: URL не фетчились, метаданных нет.
  Это вопрос к оператору/паспорту, а не к трассе.
