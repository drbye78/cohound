# Размер корней когорты `ru-pg-dbms` (пере-разведка, 09–13.11.2026)

Задача: заменить «пробы размера» (`max_documents: 5000` с оговоркой «до прогона с
`truncated: false` любое число — нижняя граница») **измеренными** размерами корней.

- **Дата замеров:** 09.11.2026 22:00 MSK — 13.11.2026 00:20 MSK (UTC 19:00 09.11 — 21:20 12.11).
- **Прибор:** `core.cli crawl run --crawl-only --from-scratch --json` (frontier + фетч +
  retention; конверсии нет, документы остаются в `extract_queue`). Плюс независимый
  статический BFS на `curl` (`/tmp/opencode/recon/bfs_dump3.py`) — он считает **нижнюю
  границу** размера сайта и не зависит ни от бюджета, ни от frontier.
- **Стенд:** `~/.cohound/stands/root-recon` (свои `runs/`, `state/`, `corpus_raw/`) +
  **копия** пакета в `/tmp/opencode/recon/packages/ru-pg-dbms`. Причина: канонический
  стенд `stands/ru-pg-dbms` держал пакетный лок чужим прогоном (эмбеддинг корпуса
  postgrespro, pid 423920), и `state.db` того стенда — предмет другого замера.
  `packages/ru-pg-dbms/**` в рабочем дереве не менялся ни в одном прогоне, кроме итоговой
  правки бюджетов (см. §8).
- **Гейт по памяти:** каждый запуск проверял `MemAvailable >= 1500 MB` (OOM-killer на
  этом хосте выбирает процесс с наибольшим RSS, то есть чужой прогон). Гейт сработал
  один раз (09.11, 678 MB — запуск отложен), остальные запуски прошли.
- **Конверсия:** все прогоны — `--pdf-pipeline text_layer --document-level text`.
  Это **не** влияет на число документов (краул без конверсии), но без этого preflight
  отказывает на 7 GB хосте: `layout_tables` требует 3679 MB на воркер.

## 1. Главный результат

| корень | документов (живой краул) | `truncated` | стена | запросов | код корня |
|---|---:|---|---:|---:|---|
| `postgrespro.ru/docs/` | **18 939** | false | 2 995 с | 18 941 | 200 |
| `tantorlabs.ru/` (host-корень, depth 3) | **9 162** (не завершён) | — | 2 362 с | 9 188 | 200 |
| `database.diasoft.ru/docs/ru/q-database/versiya-180/` | **1 226** (из них 1 184 страницы + 42 мусорных `mailto:`) | false | 254 с | 1 641 | 200 |
| `docs.tantorlabs.ru/tdb/` | **1 207** | false | 253 с | 1 216 | 301→200 |
| `platformv.sbertech.ru/docs/public/PSQ/8.1.0/` | **1 155** | false | 190 с | 1 156 | 200 |
| `edu.postgrespro.ru/` | **900** | false | 617 с | 915 | 200 |
| `pangolin.sbertech.ru/` (browser) | **572** (не завершён; статически 3 025) | — | — | — | 401→200 |
| `jatoba.ru/` | **7** | false | 8 с | 9 | 200 |
| `tantorlabs.ru/products/dbms/` | **3** | false | 11 с | 3 | 200 |
| `www.gaz-is.ru/produkty/inform-sistemy/subd-jatoba/` | **1** HTML + 17 PDF отбиты robots | false | 59 с | 18 | 200 |
| `wiki.astralinux.ru/tandocs/` | **0** (403 от Scrapy-UA, см. §5) | — | 3 с | 1 | 403 |
| `www.gaz-is.ru/`, `database.diasoft.ru/`, `postgrespro.ru/products/postgrespro/enterprise/` | **1** каждый (`max_depth: 0` по объявлению) | false | ≤10 с | 1 | 200 |
| `postgrespro.ru/` (host-корень) | 3 996 (прерван: 980×`429` на `/list/*`) | — | 1 403 с | 5 041 | 200 |
| `sbertech.ru/documents/platform-v-pangolin-se/` | не измерен (401 обычному HTTP, нужен browser) | — | — | — | 401 |

**Три числа, которые меняют бюджеты:**

1. `postgrespro.ru/docs/` — **18 939 документов**, `truncated: false`, корень исчерпан.
   Прежний бюджет 800 усекал его в **23 раза**; корпус цикла `2a5b51af5778` держит 949
   документов этого корня.
2. `database.diasoft.ru/.../versiya-180/` — **1 184 страницы** (не «≥1187»: статический BFS
   нашёл ровно те же 1 184 адреса, что и краул, — расхождение 0). Прежняя оценка «≥1187»
   была верна с точностью до страницы.
3. `platformv.sbertech.ru/.../PSQ/8.1.0/` — **1 155** документов (`truncated: false`), а не
   «≥1299». Статическая сумма ссылок 15 разделов = **1299**, статический BFS = **1159**;
   разница 140 — перекрёстные ссылки на страницы **других** разделов, которые считались
   дважды. Живой краул дедуплицирует по `content_hash` и даёт 1 155.

## 2. Статика: нижние границы, не зависящие от бюджета

| корень | sitemap `<loc>` | TOC-ссылок | robots `Disallow` | код корня | нижняя граница |
|---|---:|---:|---:|---|---:|
| `postgrespro.ru/docs/` | 404 | 55 индексов редакций | 4 | 200 | **18 939** (живой) |
| `postgrespro.ru/docs/enterprise/18` | — | 111 | — | 200 | **1 267** (полный BFS) |
| `postgrespro.ru/docs/enterprise/10` | — | 100 | — | 200 | **1 147** (полный BFS) |
| `postgrespro.ru/docs/enterprise/9.6` | — | 97 | — | 200 | **1 098** (полный BFS) |
| `database.diasoft.ru/.../versiya-180/` | 8 (83 `<loc>`, под `/docs/` — ноль) | 701 (+465 со стр. 2) | 36 (`/*PAGEN`) | 200 | **1 184** (живой = статика) |
| `platformv.../PSQ/8.1.0/` | 818 (под PSQ — ноль) | 1 299 (сумма 15 разделов) | 32 | 200 | **1 155** (живой) |
| `docs.tantorlabs.ru/tdb/` | 404 (редирект на чужой хост) | 894 (`ru/18_6/se`) | 0 | 301→200 | **1 207** (живой) |
| `docs.tantorlabs.ru/tdb/ru/17_11/se/` | — | 893 | — | 200 | **1 209** (полный BFS) |
| `docs.tantorlabs.ru/tdb/ru/16_15/se/` | — | — | — | 200 | **1 217** (BFS) |
| `docs.tantorlabs.ru/tdb/ru/15_19/se/` | — | — | — | 200 | **1 214** (BFS) |
| `docs.tantorlabs.ru/tdb/ru/14_24/se/` | — | — | — | 200 | **1 178** (BFS) |
| `docs.tantorlabs.ru/tp/7.1.0/` | — | 171 | — | 200 | **171** |
| `edu.postgrespro.ru/` | 404 | 505 HTML + 533 вложения | 0 | 200 | **638** (HTML-ссылок) |
| `pangolin.sbertech.ru/` | 25 | — | 32 | 401 | **25** + `/center/docs/*` |
| `sbertech.ru/` | 745 (`/documents` 499, pangolin 27) | — | 32 | 401 | **27** (pangolin) |
| `tantorlabs.ru/` | 61 | 49 | 108 | 200 | **61** + поддеревья |
| `tantorlabs.ru/products/dbms/` | 3 | 9 | 108 | 200 | **3** |
| `www.gaz-is.ru/produkty/.../subd-jatoba/` | 500 | 3 (2 в scope) | 53 (`*.pdf$`) | 200 | **2** HTML |
| `jatoba.ru/` | 2 | 7 | 14 | 200 | **7** |
| `wiki.astralinux.ru/tandocs/` | 404 | 66 | 0 | 200 (`curl`) / 403 (Scrapy) | **73** (BFS) |

## 3. Как читать «`truncated: false`»

`truncated: false` в `crawl_truncation` означает: **фронт исчерпан**, а не «бюджет не
достигнут». Для четырёх корней это доказано двумя независимыми приборами:

- `postgrespro.ru/docs/` — 18 939 при бюджете 20 000 (`queue_remaining = 0`);
- `PSQ/8.1.0` — 1 155 при бюджете 20 000; статический BFS 1 159 (разница — 4 страницы,
  схлопнутые по `content_hash`);
- `diasoft versiya-180` — 1 184 при бюджете 20 000; статический BFS 1 184;
- `docs.tantorlabs.ru/tdb/` — 1 207 при бюджете 20 000; статический BFS 1 210.

Остальные корни упирались не в бюджет, а в **свойство сайта**:

- `postgrespro.ru/` (host-корень) — **980 ответов `429`** на `/list/*` за 20 минут;
  бюджет не достигнут, прерван. Это единственный измеренный случай, где хост сам
  ограничивает темп (объявленный `{0.05, 0.2}` = 5–20 запросов/с).
- `tantorlabs.ru/` — 9 162 за 39 минут, 20×`504`; прерван по таймауту прибора, не по
  бюджету. Корень — **весь хост**, поэтому число включает `/perseydb`, `/tp`, `/pipelinedb`,
  `/dlh`, `/xdata`, `/patroni`, `/polardb` — то есть чужие продукты (I17).
- `pangolin.sbertech.ru/` — browser, `in_flight_max == 1`, 572 за 40 минут; 518 из них —
  `/center/docs/*`. Прерван таймаутом. Статика даёт 3 025 страниц (`/center/docs/7` 2 307 +
  kintsugi 227 + copywala 491) — это нижняя граница, которую краул не прошёл.
- `edu.postgrespro.ru/` — 0.9 запроса/с (измерено 07.11): **900 документов за 617 с**,
  `truncated: false`. Статическая оценка 638 HTML была нижней границей по ссылкам:
  краул добрал 252 `.pdf`, 140 `.zip`, 74 `.txt` (12 отказов `payload_too_large`).

## 4. Что не измерено и почему

| корень | почему |
|---|---|
| `sbertech.ru/documents/platform-v-pangolin-se/` | 401 обычному HTTP; нужен browser-рендер, а browser сериализован. Статика даёт 27 `<loc>` pangolin в host-sitemap. |
| `postgrespro.ru/products/postgrespro/enterprise/` | лендинг: `max_depth: 8`, но страница не содержит in-scope ссылок; 1 документ. Документация продукта живёт на `/docs/enterprise/*` — другом корне. |
| `tantorlabs.ru/` | прерван таймаутом прибора (39 мин). 9 162 — **нижняя граница**, не размер. |
| `pangolin.sbertech.ru/` | прерван таймаутом (40 мин). 572 — нижняя граница; статика 3 025 — тоже нижняя граница (browser-краул до неё не дошёл). |
| `postgrespro.ru/` | 429 от хоста; размер host-корня не является предметом (это не корень продукта). |

## 5. Найденные факты, которые меняют объявление корней

### 5.1 `wiki.astralinux.ru` отдаёт 403 **по User-Agent**, а не «закрыт»

Матрица (13.11.2026, один и тот же URL):

| User-Agent | `/tandocs/` | `/tandocs/baza-znanij-tantor-238752972.html` |
|---|---|---|
| `Scrapy/2.19.0 (+https://scrapy.org)` | **403** | **403** |
| `coHound-crawler/0.1` | 200 | 200 |
| `Mozilla/5.0 … Chrome/120` | 200 | 200 |

`COHOUND_SCOUT_CRAWLER=scrapy` в `.env` (строка 33), а `ScrapyCrawler` **не задаёт
`USER_AGENT`** (`DEFAULT_SCRAPY_SETTINGS`, `core/adapters/crawl_scrapy.py:159` — ключа нет),
поэтому Scrapy отправляет свой дефолт. `HttpCrawler`/`BrowserCrawler` отправляют
`coHound-crawler/0.1` (`crawl_http.py:132`, `crawl_browser.py:159`).

Следствие: прогон 07.11 записал «сид отвечает 403 обычному HTTP» — это верно **для
Scrapy-пути**, но не для HTTP-пути. Корень живой (73 страницы, статический BFS),
и 403 — дефект провенанса User-Agent, а не свойство сайта.

### 5.2 `docs.tantorlabs.ru/tdb/` покрывает **одно** поддерево, а не корень

Сид `/tdb/` отдаёт `301 → /tdb/ru/18_6/se/differences.html`. Дальше BFS идёт по ссылкам
страницы, а они относительные (`admin.html`), то есть остаются внутри `/tdb/ru/18_6/se/`.
Перекрёстных ссылок на `en/`, `se1c/`, `be/` в странице **нет** (проверено: 0 из 1 068
`href`). Поэтому:

- измеренный размер корня = **1 207** (`ru/18_6/se`), а не 5 027 и не 2 063;
- в прогоне `2a5b51af5778` 2 063 элемента — это 1 168 страниц дерева **плюс 895 плоских
  `/tdb/*.html`**, которые все отдают `301` на ту же `differences.html` и схлопываются в
  **один** `doc_hash` (`b89ba3f…`, 895 алиасов). Корпус получил 1 168 документов
  (1 167 `ru/18_6/se` + 1 корень), а не 2 063;
- деревья `se1c`/`be`/`en` достижимы только **отдельными** сидами: `en/18_6` — 3 537
  страниц (se 1 206 + se1c 1 174 + be 1 157), `ru/18_6/se1c` — 865, `ru/18_6/be` — 848.

### 5.3 Переехавшие редакции tantor

| адрес из эталона | сейчас | живая редакция | страниц |
|---|---|---|---|
| `/tdb/ru/17_5/se/*` | 301 | `/tdb/ru/17_11/se/` | **1 209** |
| `/tdb/ru/16_8/se/*`, `/tdb/ru/16_10/*` | 301 | `/tdb/ru/16_15/se/` | **1 217** |
| `/tdb/ru/15_12/*` | 301 | `/tdb/ru/15_19/se/` | **1 214** |
| `/tdb/ru/14_17/*` | 301 | `/tdb/ru/14_24/se/` | **1 178** |
| `/tp/6.0/*` | 200, но `/tp/6.0/instances/*` → 302 | `/tp/7.1.0/` | **171** |

`/tdb/ru/17_5/` больше не существует (301), но `/tdb/ru/17_11/` жив и полон — 44 из 66
ячеек tantor в `in_scope_not_reached` указывают именно на `17_5`.

### 5.4 `database.diasoft.ru` — два разных адреса

Эталон ссылается на `/docs/ru/postgresql/versiya-174/` (**жив, 200**, TOC 701),
а `corpus_urls.yaml` объявляет `/docs/ru/q-database/versiya-180/` (1 184 страницы).
Это не «документа нет» и не «пере-обход»: это **другой объявленный корень**. 3 ячейки
`in_scope_not_reached` у `digital_q_database` — ровно этот случай.

### 5.5 robots закрывает то, что объявлено

- `database.diasoft.ru/robots.txt`: `Disallow: /*PAGEN` ⇒ 59 попыток второй страницы
  TOC отбиты (`robots_disallow`), но все 1 184 страницы первой страницы получены.
- `www.gaz-is.ru/robots.txt`: `Disallow: *.pdf$` ⇒ 17 PDF продукта jatoba **не могут быть
  получены вовсе** (это и есть документы продукта: сертификаты, «Jatoba Security Vision»).
  Корень даёт 1 HTML-страницу, `complete=False`.
- `sbertech.ru`/`platformv.sbertech.ru`: 32 `Disallow`, ни один не задевает PSQ/`/documents`.

### 5.6 Мёртвые редакции (подтверждено)

| адрес | код | `Location` | ячеек эталона |
|---|---|---|---:|
| `platformv.sbertech.ru/docs/public/PSQ/7.1.1/` | 302 | `/docs/public/PSQ/7.1.1/404.html` | **53** |
| `platformv.sbertech.ru/docs/public/DBM/1.9.1/` | 302 | `/docs/public/DBM/1.9.1/404.html` | **6** |
| `platformv.sbertech.ru/docs/public/DBM/1.9.1-std/` | 302 | `…/404.html` | **1** |

Это не «сайт недоступен», а «редакции больше нет»: 60 ячеек эталона не восстанавливаются
никаким обходом — только другой редакцией или другим источником.

## 6. Политика vs размер

В прогоне `2a5b51af5778` политика `scheduled 168 ч` (операторский `state.db`,
`crawl_policy`) отменила сеть для **всех** корней `jatoba` (3) и `pangolin` (5):
`crawl_policy_skipped`, 0 элементов. В том цикле их корпус не обновлялся вовсе.

Измерено 09–13.11 (сеть разрешена, политики в изолированном стенде нет):

| продукт | корень | документов | ячеек эталона в `policy_skipped` |
|---|---|---:|---:|
| pangolin | `pangolin.sbertech.ru/center/docs/7` | 200 (жив) | 9 |
| pangolin | `pangolin.sbertech.ru/center/docs/copywala` | 200 (жив) | 8 |
| pangolin | `pangolin.sbertech.ru/center/docs/kintsugi` | 200 (жив) | 11 |
| jatoba | `jatoba.ru/` (документация `documentationJatoba18.zip`) | 200 (жив) | 2 |
| pangolin | `sbertech.ru/about/legal-info/...` | 401 | 1 |

**18 из 31 ячейки `policy_skipped` — это живые страницы `pangolin.sbertech.ru/center/docs/*`**
(продуктовая документация в Confluence-подобном центре). Политика запретила сеть живому
корню: это аргумент к `on_request`/`--refresh-scope`, а не к «сайт пустой».

## 7. Что восстановимо: ячейки эталона по причинам

Источник: `packages/ru-pg-dbms/sources/retrieval_golden_docs.candidate.csv`
(287 пар источников, **391 ячейка**; с 15.11 полный машинный отчёт воспроизводится
командой `build`, а лист на разметку — XLSX с 102 решаемыми строками).

| причина | пар | ячеек | восстановимо обходом? |
|---|---:|---:|---|
| `resolved` | 28 | 36 | уже в корпусе |
| `in_scope_not_reached` | 139 | **192** | **да**, если бюджет/сид покрывает дерево |
| `out_of_scope` | 96 | **129** | часть — да (объявить корень), часть — нет (мёртвые редакции, чужие сайты) |
| `policy_skipped` | 20 | **31** | да — снять политику для живых корней |
| `ambiguous` / `query differs` / `moved_from` / `unreadable_origin` | 4 | 5 | частично |

`in_scope_not_reached` по семействам:

| продукт | семейство | ячеек | что говорит замер |
|---|---|---:|---|
| postgrespro | `docs/enterprise` | **71** | корень 18 939 док.; в корпусе enterprise/18 = **12** при живых 1 267 |
| tantor | `tdb/ru` | **66** | 44 из них — `17_5`, живая замена `17_11` = 1 209 |
| postgrespro | `docs/ppem` | **32** | `ppem/2` — 95 страниц, корень тот же `/docs/` |
| postgrespro | `docs/pg_probackup3` | **14** | `3.2` — 49 страниц |
| digital_q_database | `docs/ru` | **3** | `versiya-174` жив (701), объявлен `versiya-180` |
| postgrespro | прочие (`postgrespro`, `ora2pgpro`, `shardman`, `roadmap`) | 5 | входят в 18 939 |
| tantor | `tantor-se-1c/tpost` | 1 | живой блог-пост |

`out_of_scope` по корням:

| корень | ячеек | статус |
|---|---:|---|
| `platformv…/PSQ/7.1.1/` | **53** | мёртв (302 на 404) |
| `docs.tantorlabs.ru/tp/6.0` | **36** | жив, но переехал на `/tp/7.1.0/` (171 стр.) |
| `pgbackrest.org` (чужой сайт) | 9 | вне когорты |
| `platformv…/DBM/1.9.1*` | 7 | мёртв |
| `github.com/TantorLabs/pg_cluster` | 4 | чужой сайт |
| `v8.1c.ru` (×3 продукта) | 3 | чужой сайт |
| `www.postgresql.org`, `lk-new.astralinux.ru`, `machine.postgrespro.ru`, `support.postgrespro.ru`, `platform.gov.ru`, `www.rubackup.ru` | по 1 | чужие/вне когорты |

## 8. Правки бюджетов (`packages/ru-pg-dbms/domain_profiles.yaml`)

Бюджет ставится **по измеренному размеру**: выше `pages_fetched` при `truncated: false`,
с запасом на новую редакцию. Где корень не продуктовый и размер не исчерпан, бюджет —
объявленная граница приоритета, и это сказано в комментарии (не выдаётся за размер).
Каждая правка несёт в файле дату, команду и `truncated`.

| ключ | было | стало | основание |
|---|---:|---:|---|
| `postgrespro.ru/docs` | 3667 | **20000** | 18 939, `truncated: false` (09.11, `--crawl-only`) |
| `database.diasoft.ru/docs/ru/q-database/versiya-180` | 5000 | **1500** | 1 184 страницы + 59 `robots_disallow`; статика = краул (расхождение 0) |
| `platformv.sbertech.ru/docs/public/PSQ` | 5000 | **1500** | 1 155, `truncated: false`; статика 1 159 |
| `docs.tantorlabs.ru` | 5000 | **1500** | 1 207, `truncated: false`; статика 1 210 |
| `edu.postgrespro.ru` | 700 | **1500** | 900, `truncated: false` (12.11); 700 резало корень |
| `pangolin.sbertech.ru` | 600 | **3500** | статическая нижняя граница 3 025; живой краул не завершён (572), `truncated: false` не подтверждён |
| `tantorlabs.ru` | 300 | **300** (без изменений) | host-корень даёт 9 162 чужих страниц; бюджет 300 — граница I17, не размера |
| `tantorlabs.ru/products/dbms` | 200 | **50** | 3 документа, `truncated: false` |
| `jatoba.ru` | 50 | **50** (без изменений) | 7 документов, `truncated: false` |
| `gaz-is.ru/produkty/inform-sistemy/subd-jatoba` | 200 | **50** | 1 HTML в scope + 17 PDF отбиты robots |
| `sbertech.ru/documents` | 600 | **100** | 27 `<loc>` pangolin в host-sitemap (краул не проводился) |
| `wiki.astralinux.ru/tandocs` | 300 | **150** | 73 страницы (BFS, 13.11) |
| `postgrespro.ru` (host) | 800 | **800** (без изменений) | портал остановлен хостом: 3 996 док. и 980×`429`; 800 — приоритет, не размер |
| `platformv.sbertech.ru` (host) | 300 | **300** (без изменений) | 401 HTTP; маркетинговые страницы |
| `sbertech.ru`, `database.diasoft.ru`, `gaz-is.ru`, `wiki.astralinux.ru` (host) | 10 | **10** (без изменений) | depth 0 по объявлению |

Значения живут в **двух** местах: `domain_profiles.yaml` и генератор
`packages/ru-pg-dbms/sources/build_package.py::build_domain_profiles`. Правка только
файла ломает пин `test_on_disk_profiles_body_is_exactly_generator_dump` — обновлены оба,
и ожидаемые значения в `tests/packages/test_build_package.py` (это пин бюджетов, а не
ослабление проверки: изменилось само объявленное значение).

Правка `domain_profiles.yaml` меняет `crawl_config_digest` ⇒ **новый `run_id`**
(`execution_change`). Это ожидаемо и является целью задачи.

## 9. Что объявить, что убрать

**Объявить (новые/изменённые корни — решение владельца, не моё):**

1. `postgrespro.ru/docs/` — бюджет 20 000 (измерено 18 939). Это снимает 71 ячейку
   `docs/enterprise` и 32 `ppem` + 14 `pg_probackup3`.
2. `docs.tantorlabs.ru/tdb/ru/17_11/` — отдельный сид: 1 209 страниц, 44 ячейки `17_5`.
3. `docs.tantorlabs.ru/tp/7.1.0/` — 171 страница, 36 ячеек `tp/6.0`.
4. `docs.tantorlabs.ru/tdb/ru/16_15/`, `/15_19/`, `/14_24/` — если нужны старые редакции
   (1 217 / 1 214 / 1 178).
5. `pangolin.sbertech.ru/center/docs/` — политика `on_request` или снятие `scheduled`
   (18 ячеек `policy_skipped`, страницы живы).
6. `database.diasoft.ru/docs/ru/postgresql/versiya-174/` — либо переключить корень
   digital_q_database с `versiya-180` на `versiya-174` (эталон ссылается на 174),
   либо объявить оба (3 ячейки).

**Убрать/не объявлять:**

1. `platformv…/PSQ/7.1.1/`, `/DBM/1.9.1/`, `/DBM/1.9.1-std/` — мертвы (302 на 404).
   60 ячеек эталона не восстанавливаются обходом; нужен другой источник.
2. `docs.tantorlabs.ru/tdb/` **в нынешнем виде** — он покрывает только `ru/18_6/se`
   (1 207), а не весь `/tdb/`. Оставить как есть можно, но нельзя ждать от него `se1c`/`be`/`en`.
3. `postgrespro.ru/` (host-корень) — 980×`429`; не корень продукта, оставить `max_depth: 0`.
4. `tantorlabs.ru/` (host-корень) — 9 162 страницы чужих продуктов; `max_depth: 3`
   затягивает `/perseydb`, `/pipelinedb`, `/dlr`, `/polardb`. Бюджет 300 — правильная
   граница, но лучше `max_depth: 1` + явные сиды продуктов.

## 10. Что это значит для документного эталона

- **`in_scope_not_reached` (192 ячейки) — это не «документа нет», а «корпус взял долю
  процента дерева».** Ярче всего `enterprise/18`: живой корень 1 267 страниц, в корпусе
  **12** (0.9 %). Бюджет 800 на `/docs/` (дерево 18 939) — причина, а не отсутствие
  документа.
- **`out_of_scope` (129) распадается на две неравные части:** 60 ячеек — мёртвые
  редакции PSQ/DBM (не восстановимы), 36 — живой `/tp/7.1.0` (восстановим объявлением),
  33 — чужие сайты (вне когорты).
- **`policy_skipped` (31) — почти весь живой** (18 ячеек `pangolin/center/docs`).
- Итого восстановимо обходом: **192 + 36 + 31 = 259 ячеек** из 391 (66 %), при условии
  объявления корней и снятия политики; не восстановимы обходом 60 (мёртвые редакции) +
  33 (чужие сайты) = 93.

## 11. Точные команды

```bash
# Изолированный стенд (свои runs/state; пакет — копия)
export COHOUND_HOME=/home/roger/.cohound/stands/root-recon
export COHOUND_PATHS_FILE=/home/roger/.cohound/stands/root-recon/paths.yaml
export COHOUND_SERP=0            # SERP-контур не нужен: измеряется корень, не поиск

# Один корень (файл --urls: {product: [url]})
.venv/bin/python -m core.cli crawl run --crawl-only \
  --run-yaml /tmp/opencode/recon/packages/ru-pg-dbms/run.yaml \
  --package  /tmp/opencode/recon/packages/ru-pg-dbms \
  --urls /tmp/opencode/recon/<root>.yaml --only-product <product> \
  --run-id recon-<root> --from-scratch \
  --pdf-pipeline text_layer --document-level text --json
```

`--pdf-pipeline text_layer --document-level text` — не влияние на размер, а условие
запуска на 7 GB хосте (preflight отказывает `layout_tables`: 3679 MB на воркер).

Статика (независимый прибор):

```bash
python3 /tmp/opencode/recon/bfs_dump3.py "<url>" 4000 0.1 /tmp/out.txt
# 4 воркера, ≥0.1 с между запросами; вложения (pdf/zip/ova) считаются по ссылкам и не качаются
```

## 12. Границы прибора (честно)

1. **Статический BFS не соблюдает robots** — он читает индексы, а не собирает корпус.
   Числа §2 — нижние границы размера сайта, а не «что разрешено собрать».
2. **`--crawl-only` не конвертирует**, поэтому число документов не включает контейнеры
   (zip/ova) и их членов. Для `edu` это существенно: 533 вложения, часть из них — архивы
   с десятками членов.
3. **Browser-корни сериализованы** (`in_flight_max == 1`): 572 документа за 40 минут —
   это темп, а не размер.
4. **Три корня прерваны таймаутом** (`tantorlabs.ru`, `pangolin.sbertech.ru`, `edu`), их
   числа — нижние границы, помечены в §1.
5. **`postgrespro.ru/` прерван хостом** (980×`429`): это факт о хосте, а не о размере.
6. **Один прогон статического BFS был убит OOM** (`.ova` на несколько ГБ в `edu`):
   после этого вложения не скачиваются, только считаются. Пик RSS зафиксирован
   (`dmesg`: `Killed process … anon-rss:5257972kB`).
