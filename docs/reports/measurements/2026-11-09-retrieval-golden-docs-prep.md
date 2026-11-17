# Документный retrieval-эталон: формат, сопоставление URL→документ, пачки на подтверждение

**Дата:** 2026-11-09
**Пункт плана:** `docs/architecture/26-open-review-items-plan.md` §4, O14 («Эталон на уровне
документа и цитаты»); дефект текущего эталона — O4 там же.
**Прогон-источник:** `ru-pg-dbms-2a5b51af5778`,
`~/.cohound/stands/ru-pg-dbms/runs/ru-pg-dbms-2a5b51af5778/cohound.db` (470 MB; 8 115
документов, 154 664 чанка, 5 продуктов). Открытие — `sqlite3 …?mode=ro`, сеть не
использовалась, `core.cli run`/`crawl` не запускались.
**Входы:** `packages/ru-pg-dbms/sources/golden_00_cells.csv` (905 ячеек),
`core/util/retrieval_eval.py` (что сейчас понимает оценка).
`packages/ru-pg-dbms/sources/build_from_golden_00.py` прочитан, но не переиспользован: он
требует XLSX-исходник `/mnt/c/Work/htap/golden_00.xlsx` и пересобирает весь пакет, а здесь
нужен один производный артефакт.
**Артефакты:** `scripts/prepare_retrieval_golden_docs.py`,
`packages/ru-pg-dbms/sources/retrieval_golden_docs.candidate.csv` (287 строк; в пайплайн
**не** подключён).

> **Уточнение 15.11.2026 (позже замера).** Файл, о котором говорит этот отчёт, больше не
> существует в той форме: лист на разметку переехал в
> `packages/ru-pg-dbms/sources/retrieval_golden_docs.candidate.xlsx` и содержит **102**
> решаемые строки (185 пар `missing` в лист не попадают — это провал покрытия корпуса, а не
> эталона). Полный машинный отчёт по 287 парам воспроизводится одной командой `build`
> (выход — CSV). Числа ниже — про 09.11 и остаются верными для того замера.
**Не менялось:** `retrieval_golden.json` (остаётся вторичной проверкой, пересборка —
отдельное решение), `golden_00_cells.csv`.
*(Поправка 17.11.2026: `retrieval_golden.json` удалён — пересборка состоялась, замена
`sources/retrieval_golden_chunks.json`, см. `2026-11-17-retrieval-golden-chunks.md`.)*

## 1. Что утверждает документный эталон

Одно утверждение на ячейку: «для ячейки `{product_id}/{param_id}` документ, процитированный
в comparative table (`source_url`), должен попасть в top-K документной выдачи».

Это **нижняя граница** релевантности, а не полный набор релевантных документов: таблица
цитирует один источник на ячейку, и эталон говорит только «вот этот документ релевантен».
Документов, которые тоже релевантны, но не процитированы, эталон не перечисляет — и это
названо, а не замаскировано.

Утверждение не зависит от нарезки: адрес утверждения — URL, а `doc_id` — только его резолв
в конкретном корпусе (`doc_id = product_id:content_hash`, `core/pipeline/corpus_persist.py:471`;
текстовый хеш, поэтому при правке страницы `doc_id` меняется — см. §8). Смена чанкера,
переэкстракция или новая сборка индекса эталон не ломают; смена **текста** документа —
ломает резолв, и это должно быть видно, а не проглочено.

## 2. Формат

Стиль — как у `retrieval_golden.json`: запись на утверждение, `expected_*` — список
идентификаторов. Отличие: у записи нет поля `query`. Поисковый запрос ячейки выводит
пайплайн из каталога (`title`/`concept_terms`), и дублировать его в эталоне значило бы
завести второй источник истины, который разъедется с каталогом молча.

```json
{
  "format": "cohound-retrieval-golden-docs/1",
  "case_id": "ru-pg-dbms",
  "source": "packages/ru-pg-dbms/sources/golden_00_cells.csv",
  "corpus_run_id": "ru-pg-dbms-2a5b51af5778",
  "generated_at": "2026-11-09T15:42:09+00:00",
  "entries": [
    {
      "cell": "pangolin/xlsx_118_rekomendacii_po_optimalnoi_konfiguracii_v_zavisimost",
      "product_id": "pangolin",
      "param_id": "xlsx_118_rekomendacii_po_optimalnoi_konfiguracii_v_zavisimost",
      "source_url": "https://pangolin.sbertech.ru/center/docs/7/administration-guide/pangolin-tuner",
      "expected_doc_ids": ["pangolin:050dbb634c2d68a1f4be7c704943959989f4e769585cad119522bac2f2230326"],
      "match": "canonical"
    }
  ]
}
```

- `source_url` — само утверждение (устойчиво к пересборке корпуса).
- `expected_doc_ids` — резолв в объявленном снимке корпуса (`corpus_run_id`); на другом
  корпусе валидатор откажет и потребует перерезолвить, а не «подставит похожий».
- `match` — как URL сопоставлен (`exact` / `canonical` / `canonical_query`, §3); нужен для
  аудита, а не для оценки.
- `cell` — `{product_id}/{param_id}`, он же ключ уникальности.

## 3. Сопоставление URL → документ

Единственный машинный факт — «этот URL есть в корпусе прогона как документ `Y`», проверенный
запросом к `corpus_docs` (не по похожести строк). Пары строятся по `(product_id, source_url)`:
выдача продукт-скоупная (I17), поэтому URL, лежащий в корпусе другого продукта, для этого
продукта — `missing`, а не «нашли у соседа».

Три уровня сопоставления, каждый — проверка по БД:

1. `exact` — строка URL совпала с `corpus_docs.url` побайтно.
2. `canonical` — снято только то, что не меняет документ по HTTP-семантике: регистр схемы и
   хоста, порт по умолчанию, фрагмент (якорь), повторные и хвостовой `/`, percent-кодирование
   пути, `http`↔`https`.
3. `canonical_query` — дополнительно отброшена query-строка. Это **грубее**: `?h=…` —
   подсказка подсветки, а `?lang=ru` — языковая редакция страницы. Такой матч помечается и
   уходит человеку; в текущем срезе он один (§5).

Похожесть путей не используется: страница той же темы в другой редакции (например,
`…/17_5/…` против `…/18_6/…`) — это другой документ, и она остаётся `missing`. Для человека
в §5 названо, сколько таких «одноимённых в другой редакции» URL есть, но это подсказка для
решения, а не сопоставление.

Ячейки `❌` в положительный эталон не входят: для них цитируемый документ — свидетельство
**отсутствия**, и утверждение «релевантен» было бы ложным. Их три (§4), они названы.

## 4. Числа

Вход (09.11.2026):

| | значение |
|---|---|
| ячеек в `golden_00_cells.csv` | 905 |
| ячеек с непустым `source_url` | **394** (43.5 %) |
| из них `✅` / `⚠️` / `❌` | 341 / 50 / **3** |
| уникальных строк URL | **285** (число плана подтверждено) |
| пар `(product_id, url)` | **287** (две строки URL цитируются двумя-тремя продуктами) |
| положительных ячеек, входящих в эталон | **391** (394 − 3 `❌`) |

Три `❌`-ячейки, исключённые из положительного эталона (их цитируемый документ —
свидетельство **отсутствия**): `pangolin/xlsx_021_podderzhka_minimum_treh_mazhornyh_versii_v_sertifici`
→ `https://pangolin.sbertech.ru/compatibility`; `pangolin/xlsx_155_sozdanie_udalenie_redaktirovanie_tablespace`
и `pangolin/xlsx_156_sozdanie_udalenie_redaktirovanie_obektov_shemy_bd` → страница DBM/1.9.1
`…/user-guide-dbcm/scenarios/instructions-clusters-open-connect-db.html`. Последняя страница
цитируется ещё двумя `⚠️`-ячейками (`xlsx_154…`, `xlsx_171…`), поэтому её пара в эталоне
осталась — с двумя положительными ячейками и пометкой `absence_cells_excluded` в CSV.

Резолв в корпус прогона:

| исход | пар | ячеек | доля положительных ячеек |
|---|---|---|---|
| `resolved` | 30 | 37 | **9.5 %** |
| `ambiguous` | 1 | 1 | 0.3 % |
| `missing` | 256 | 353 | 90.3 % |

`resolved` по типу матча: `exact` 21, `canonical` 7, `canonical_query` 1, `moved_exact` 1
(последний — из проверки живости §9; до неё таблица давала 29 пар / 36 ячеек).

По продуктам (ячейки `resolved` / все положительные ячейки с URL):

| продукт | resolved | всего | пар |
|---|---|---|---|
| digital_q_database | 2 | 14 | 1/7 |
| jatoba | 5 | 9 | 4/8 |
| pangolin | 2 | 99 | 2/76 |
| postgrespro | 20 | 149 | 16/113 |
| tantor | 9 | 120 | 8/83 |

**Главный вывод, который надо назвать до всякой оценки:** на текущем корпусе документный
эталон покрывает **38 из 391** положительной ячейки (37 `resolved` + 1 `ambiguous`).
`recall@K` по нему — это замер на 38 ячейках, а не на «сотнях»; для остальных 353 `missing`
цитируемого документа в корпусе нет, и качество поиска по ним не определено. Это не дефект сопоставления
(§5 объясняет каждую причину адресом), а состояние корпуса относительно цитат.

## 5. Почему `missing`: классы и адреса

Классификация детерминирована и считается из `corpus_coverage.roots_json`,
`runs.crawl_policy_json`, `corpus_coverage.unreadable_origins_json` и
`crawl_session_items`; токен причины пишется в колонку `note` кандидатного CSV.

| причина | пар | ячеек | что это значит |
|---|---|---|---|
| `in_scope_not_reached` | 139 | 192 | URL под объявленным корнем, но в очередь краула не попал |
| `out_of_scope` | 96 | 129 | URL вне объявленных корней (другой хост или другой префикс/редакция) |
| `policy_skipped` | 20 | 31 | корень пропущен политикой краула (`crawl_policy_skipped` + `crawl_on_request_not_requested`) |
| `unreadable_origin` | 1 | 1 | хост объявлен, но обход объявил его недоступным |
| `visited_no_doc` | 1 | 1 | URL обойдён, извлечение отмечено `done`, строки документа нет |
| `ambiguous` | 1 | 1 | URL соответствует двум документам (языковые редакции) |

Адреса по продуктам:

- **postgrespro — 90 пар `in_scope_not_reached`: 89 под `https://postgrespro.ru/docs/`, 1 —
  `https://postgrespro.ru/roadmap` под корнем `/`.**
  Объявленный корень документации имеет `complete=false` и `truncated=true` (покрытие
  прогона), бюджет корня — `max_documents: 600` (`packages/ru-pg-dbms/run.yaml:76`). В корпусе
  документация enterprise распределена так: 16 — 249 док., 15 — 127, 10 — 117, 13 — 91,
  11 — 69, 12 — 59, 14 — 56, 17 — 31, **18 — 12**. Таблица цитирует в основном 18-ю редакцию:
  обход остановился внутри неё. Ещё 7 пар — вне корней (`support.postgrespro.ru` 2,
  `www.postgresql.org`, `machine.postgrespro.ru`, `platform.gov.ru`, `www.rubackup.ru`,
  `v8.1c.ru` по 1).
- **tantor — 46 пар `in_scope_not_reached`, 28 `out_of_scope`.**
  Корень `https://docs.tantorlabs.ru/tdb/` обойдён с `complete=true`, но в корпусе только
  редакция `18_6` (1 167 док.). Таблица цитирует `17_5` (57 ячеек), `15_12` (4), `16_8` (3),
  `16_10` (1), `14_17` (1) — этих редакций в корпусе нет: 45 пар под `/tdb/` плюс одна пара
  `tantorlabs.ru/…` в объявленном корне, но не достигнутая. Ещё 24 пары — `…/tp/6.0/…`: это
  другой продукт на том же хосте, вне префикса `/tdb/`, то есть `out_of_scope`; 4 пары — на
  хостах, которых в корпусе нет (`github.com`, `v8.1c.ru`, `lk-new.astralinux.ru`,
  `www.postgresql.org`). Отдельно — `wiki.astralinux.ru` (см. ниже).
- **pangolin — 56 пар `out_of_scope`, 18 `policy_skipped`.** Объявленный корень документации —
  `https://platformv.sbertech.ru/docs/public/PSQ/8.1.0/`, а таблица цитирует `PSQ/7.1.1`
  (53 ячейки) и `DBM/1.9.1` (7 ячеек): другой префикс ⇒ `out_of_scope`; ещё 4 ячейки — прочие
  страницы `platformv.sbertech.ru` вне `/docs/` (`release-policy`, курсы, PDF-сертификат).
  Корень `https://pangolin.sbertech.ru/` пропущен политикой (сеть запрещена, корпус взят из
  adopted-снимка) ⇒ 18 пар `policy_skipped`; остальные 4 пары `out_of_scope` — на хостах,
  которых в корпусе нет (`client.sbertech.ru`, `cloud.ru`, `platform-docs.v-serv.ru`,
  `v8.1c.ru`).
- **digital_q_database — 3 `in_scope_not_reached`** (`…/docs/ru/postgresql/versiya-174/…`:
  корпус содержит `versiya-180`) и 3 `out_of_scope` (`pgbackrest.org`).
- **jatoba — 2 `policy_skipped`** (продукт пропущен политикой: `crawl_policy_skipped` +
  `crawl_on_request_not_requested`) и 2 `out_of_scope` (`github.com`, `v8.1c.ru`). Пять
  ячеек при этом `resolved` — из adopted-снимка.

Хосты, которых в корпусе продукта нет вовсе и которые не объявлены корнями (20 пар на 12
хостах): `v8.1c.ru` (4), `pgbackrest.org` (3), `github.com` (2), `support.postgrespro.ru` (2),
`www.postgresql.org` (2), `client.sbertech.ru`, `cloud.ru`, `platform-docs.v-serv.ru`,
`machine.postgrespro.ru`, `platform.gov.ru`, `www.rubackup.ru`, `lk-new.astralinux.ru` — по 1.
Это внешние ссылки таблицы (чужие сайты, личные кабинеты), а не объявленные корни; расширение
scope — это `execution_change` и новый `run_id`, а не правка настройки. Ещё одна пара —
`wiki.astralinux.ru`: корень объявлен, но origin недоступен (см. ниже).

Подсказка «другая редакция той же страницы»: у 132 из 257 `missing`-пар последний сегмент
пути встречается в корпусе того же продукта (pangolin 51, tantor 43, postgrespro 36,
digital_q 2). Это **не** сопоставление: страница `…/17_5/se/hydra.html` и
`…/18_6/se/hydra.html` — разные документы, и выбор между ними решает человек.

Две особые строки:

- `postgrespro/xlsx_118_…` → `https://postgrespro.ru/docs/enterprise/18/app-pgpro-tune`:
  `ambiguous`, два кандидата — `?lang=en` (10 485 симв.) и `?lang=ru-en` (21 310 симв.).
  Голый URL в корпусе отсутствует; сервер отдаёт языковую редакцию, и какую из них считать
  документом ячейки — решение человека.
- `tantor/xlsx_…` → `https://tantorlabs.ru/products/dlh`: URL обойдён (`fetched`),
  `extract_queue.state=done`, но строки `corpus_docs` нет. Причина из БД не восстанавливается
  (дедуп по тексту оставил бы другую строку с тем же текстовым хешем — такой строки тоже нет,
  значит вероятнее пустой текст); повторная экстракция для проверки не запускалась.

Отдельно, как факт качества данных покрытия: корень `https://wiki.astralinux.ru/tandocs/`
имеет `complete=true` в `roots_json`, при этом `unreadable_origins_json` содержит
`wiki.astralinux.ru` и документов с этого хоста в корпусе **ноль**. `complete=true` здесь
нельзя читать как «страницу смотрели»; поэтому причина классифицирована как
`unreadable_origin`, а не «обойдено и не найдено».

## 6. Валидатор

`scripts/prepare_retrieval_golden_docs.py` — один файл, четыре подкоманды (с 15.11 их
пять: добавлена `annotate`, пересобирающая лист на разметку в XLSX). Скрипт, а не
функция в `core/util/retrieval_eval.py`: это подготовка входа, а не оценка, и формат ещё не
принят пайплайном; `core/` — категориально-нейтральный движок под гейтами, класть туда
неподключённый формат рано. Загрузчик переедет в `core` вместе с правкой контракта оценки.

Проверки `validate` (и те же — на выходе `compile`):

1. строгая схема: `format` совпадает, лишних полей нет, обязательные непусты;
2. каждый `doc_id` резолвится в `corpus_docs` — иначе **отказ**;
3. документ принадлежит продукту ячейки (I17);
4. канонический URL документа совпадает с `source_url` (для `match=canonical_query`
   сравнение идёт без query);
5. нет дублей ячеек, `cell == product_id/param_id`;
6. пустой эталон (`entries: []`) — отказ.

Коды: `0` — принято; `2` — отказ (использование/валидация), сообщение на каждый дефект.

Проверено на «плохих» входах (все — `exit=2`):

```text
# нерезолвленный doc_id
ОТКАЗ: entries[0] digital_q_database/xlsx_005_…: doc_id jatoba:000…0 не резолвится в corpus_docs
# чужой URL у реального документа
ОТКАЗ: entries[1] …: URL документа https://database.diasoft.ru/ не совпадает с source_url https://example.com/other
# документ другого продукта
ОТКАЗ: entries[2] …: doc_id jatoba:df38… принадлежит 'jatoba', ячейка — 'postgrespro' (I17)
# decision=accept на нерезолвленной строке (compile)
ОТКАЗ: строка 3: decision=accept при status='missing' (https://database.diasoft.ru/docs/ru/postgresql/versiya-174/)
```

Демонстрация на принятых строках (36 записей из 29 `resolved`-пар):

```bash
.venv/bin/python scripts/prepare_retrieval_golden_docs.py compile \
  --candidate /tmp/opencode/candidate_accepted.csv \
  --out /tmp/opencode/retrieval_golden_docs.json \
  --run-db ~/.cohound/stands/ru-pg-dbms/runs/ru-pg-dbms-2a5b51af5778/cohound.db \
  --run-id ru-pg-dbms-2a5b51af5778
# written: … (36 entries)
# OK: …: 36 записей, все doc_id резолвятся        → exit=0
```

## 7. Что требует решения человека

Файл `packages/ru-pg-dbms/sources/retrieval_golden_docs.candidate.csv` — 287 строк, по строке
на пару `(product_id, source_url)`; колонки `param_ids`, `cells`, `status`, `match`,
`corpus_url`, `doc_id`, `decision`, `note`. (С 15.11 лист на разметку — XLSX и только
решаемые строки; колонки добавились: `comment` — пометка разметчика, `titles`/`claims` —
что утверждает ячейка, `cited_from` — откуда цитата фактически взята.) Правка построчная: `doc_id` можно исправить
руками, `decision` — `accept`/`reject`; пересборка CSV при этом не нужна, `compile` читает
файл как есть.

1. **30 `resolved`-строк (37 ячеек).** Проверить соответствие URL↔документ (в `corpus_url`
   видно фактический адрес; 7 — `canonical`, 1 — `canonical_query`, 1 — `moved_exact`) и
   поставить `accept` или `reject`.
2. **1 `ambiguous`.** Выбрать редакцию: `?lang=en` или `?lang=ru-en` (`…/18/app-pgpro-tune`).
3. **139 `in_scope_not_reached` (192 ячейки).** Решение из двух: (а) пере-обход с бюджетом,
   покрывающим цитируемые редакции, — это `execution_change` и новый `run_id`; (б) принять,
   что эталон по этим ячейкам не строится на этом корпусе, и назвать это числом.
4. **96 `out_of_scope` (129 ячеек).** Объявить новые корни (чужие хосты и другие редакции —
   `execution_change`) или исключить эти ячейки из эталона.
5. **20 `policy_skipped` (31 ячейка).** Запросить обновление этих корней у политики краула
   (`on_request`/`scheduled`) или принять исключение.
6. **1 особая строка** (`app-pgpro-tune`, выбор редакции) — см. §5; `products/dlh`
   перестала быть особой: адрес переехал на `/products/di`, документ в корпусе есть, строка
   `resolved` с `match=moved_exact` (§9).
7. **3 ячейки `❌`** — вне этого эталона по определению: он покрывает положительные
   доказательства, а не поиск отсутствия. Их качество этим планом не закрывается.

После правок `decision` эталон собирается командой `compile` (§6) и сразу же проходит
валидатор; файл-кандидат в пайплайн не подключён.

## 8. Ограничения и что дальше

- **Покрытие.** 38 из 391 ячейки на этом корпусе (37 `resolved` + 1 `ambiguous`, §4).
  Валидатор это не лечит — он лишь не даёт опубликовать нерезолвленный эталон. Причина
  разрыва — покрытие корпуса, а не сопоставление: 271 ячейка существует в мире, но не
  обойдена (§9).
- **Только положительные свидетельства.** Одна цитата на ячейку, без градаций релевантности
  и без списка «тоже релевантных». `❌`-ячейки исключены (§1, §7 п.7).
- **`doc_id` контентно-адресный.** `doc_id = product_id:content_hash` — текстовый хеш
  (`core/pipeline/corpus_persist.py:471`, `core/ports/store.py:364`). После правки страницы
  или переэкстракции тот же URL даёт другой `doc_id`, и валидатор на новом корпусе **откажет**
  — это и есть требуемое поведение: эталон надо перерезолвить (`build`) и заново
  подтвердить, а не «подставить похожий id».
- **Снимок корпуса — точка во времени.** `corpus_run_id` в шапке фиксирует, против какого
  корпуса резолвились id.
- **Сторона оценки не сделана.** `evaluate_retrieval` (`core/util/retrieval_eval.py:68`)
  сравнивает идентификаторы как есть; для документного `recall@K` нужен проектор «хит-чанк →
  `doc_id`» (дедуп по документу, порядок первого вхождения) и K по документной выдаче. Это
  следующая часть O14, не входившая в эту задачу.

## 9. Живость адресов: `missing` — это не одно и то же (09.11.2026)

`missing` говорит «документа нет в корпусе», но не отвечает, **есть ли он в мире**. Это
разные факты с разными последствиями: «страница переехала» лечится переписыванием адреса,
«страницы нет» — исключением ячейки, «не обошли» — пере-обходом. Проверено запросом к 284
уникальным URL (`GET`, до 5 хопов, тело не скачивается), результат —
`2026-11-09-golden-url-liveness.json`.

| класс | что это | пар | ячеек |
|---|---|---:|---:|
| `live` 200 | адрес отвечает сегодня | 161 | 221 |
| `moved` | 301/308/302 на **живой** адрес | 60 | 87 |
| `gone` | 404 или 302 на `404.html` | 60 | 76 |
| `auth` | 401/403 обычному HTTP | 3 | 4 |
| `unreachable` | TLS/таймаут (повторная проба возможна) | 3 | 3 |

Класс `moved` — не «та же страница»: `docs.tantorlabs.ru/tdb/ru/17_5/…` отдаёт 301 на
`…/17_11/…`, то есть **другая редакция**, и решает человек. Класс `gone` — это не «сайт
лёг»: `platformv.sbertech.ru/docs/public/PSQ/7.1.1/…` (53 ячейки) отдаёт 302 на
собственный `404.html`, потому что редакция 7.1.1 снята; `pangolin.sbertech.ru/center/docs/…`
(13 ячеек) — честный 404.

**Что это меняет в плане.** Из 391 положительной ячейки:
- **37** резолвятся на текущем корпусе (30 пар) плюс **1** `ambiguous` — итого 38;
- **271** (187 `live` + 84 `moved`) существуют в мире, но не в корпусе ⇒ лечатся
  пере-обходом, а не эталоном;
- **82** (`gone` 76 + `auth` 4 + `unreachable` 2) не существуют или недоступны ⇒
  исключаются из эталона числом.

37 + 1 + 271 + 82 = 391. До проверки живости резолвилось **36** ячеек (29 пар); замер
добавил одну: `tantorlabs.ru/products/dlh` отдаёт 301 на `/products/di`, и этот документ в
корпусе есть — он помечен отдельным `match=moved_exact`, чтобы подмена адреса была видна.

То есть приёмочный критерий O14 («recall@K на сотнях запросов») **достижим** — но не на
текущем корпусе и не правкой эталона. Раньше этот вывод звучал как «36 из 391», и он
читался как провал эталона; на самом деле это провал **покрытия корпуса**: обход встал
внутри редакций (`postgrespro` enterprise/18 — 12 документов из 391 ячейки, `tantor` — только
`18_6`, `pangolin` — только `PSQ/8.1.0`).

**Колонки `live_*` в кандидатном CSV** (`live_kind`, `live_code`, `live_url`) — факт, а не
решение: `build --liveness <файл>` пробует конечный адрес переехавшей страницы и помечает
такое попадание отдельным `match` (`moved_*`), чтобы подмена адреса была видна. Разметку
`accept`/`reject` по-прежнему ставит человек (D5).

**Приложение A показывает живость рядом с адресом.** Колонка «адрес» — `live`/`moved`/
`gone`/`auth`/`unreachable` с кодом ответа, у `moved` назван конечный адрес. Без неё
разметчику пришлось бы сверяться с 54-килобайтным JSON на каждую строку, а решение
`accept`/`reject` зависит именно от этого: «страница переехала в другую редакцию» и
«страницы больше нет» — разные последствия для эталона.

## Приложение A. Пачки на подтверждение

Формат: `URL из ячейки | адрес | документ в корпусе | ячеек | статус`. `doc_id` — в
`retrieval_golden_docs.candidate.csv` (колонка `doc_id`); здесь документ показан адресом в
корпусе, потому что проверять надо именно его. Для `missing` документ — «—».
Колонка «адрес» — живость из §9: `live`/`moved`/`gone`/`auth`/`unreachable` и код ответа,
у `moved` назван конечный адрес. Это факт для разметчика, а не решение.

### digital_q_database — пачка 1 (7 строк)

| URL из ячейки | адрес | документ в корпусе | ячеек | статус |
|---|---|---|---|---|
| https://database.diasoft.ru/#solution | live 206 | https://database.diasoft.ru/ | 2 | resolved (canonical) |
| https://database.diasoft.ru/docs/ru/postgresql/versiya-174/ | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://database.diasoft.ru/docs/ru/postgresql/versiya-174/functions-json/ | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://database.diasoft.ru/docs/ru/postgresql/versiya-174/monitoring-locks/ | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://pgbackrest.org/command.html#command-check | live 206 | — | 1 | missing (out_of_scope) |
| https://pgbackrest.org/configuration.html#section-general/option-compress-type | live 206 | — | 1 | missing (out_of_scope) |
| https://pgbackrest.org/user-guide.html | live 206 | — | 7 | missing (out_of_scope) |

### jatoba — пачка 1 (8 строк)

| URL из ячейки | адрес | документ в корпусе | ячеек | статус |
|---|---|---|---|---|
| https://github.com/wal-g/wal-g/blob/master/docs/STORAGES.md | live 206 | — | 1 | missing (out_of_scope) |
| https://jatoba.ru/#gallery-75 | live 200 | https://jatoba.ru/ | 1 | resolved (canonical) |
| https://jatoba.ru/assets/_cms/uploads/tinymce/source/certificateFSTEK.jpg | gone 404 | — | 1 | missing (policy_skipped) |
| https://jatoba.ru/download/documentationJatoba18.zip | live 206 | — | 1 | missing (policy_skipped) |
| https://jatoba.ru/jatoba_vkcloud.pdf | live 206 | https://jatoba.ru/jatoba_vkcloud.pdf | 1 | resolved |
| https://jatoba.ru/Программа-ТП-2023.pdf | unreachable -1 | https://jatoba.ru/%D0%9F%D1%80%D0%BE%D0%B3%D1%80%D0%B0%D0%BC%D0%BC%D0%B0-%D0%A2%D0%9F-2023.pdf | 1 | resolved (canonical) |
| https://v8.1c.ru/tekhnologii/systemnye-trebovaniya-1s-predpriyatiya-8/subd/ | live 200 | — | 1 | missing (out_of_scope) |
| https://www.gaz-is.ru/produkty/inform-sistemy/subd-jatoba | live 200 | https://www.gaz-is.ru/produkty/inform-sistemy/subd-jatoba/ | 2 | resolved (canonical) |

### pangolin — пачка 1 (50 строк)

| URL из ячейки | адрес | документ в корпусе | ячеек | статус |
|---|---|---|---|---|
| https://client.sbertech.ru/ | unreachable -1 | — | 1 | missing (out_of_scope) |
| https://cloud.ru/offers/developers | live 200 | — | 1 | missing (out_of_scope) |
| https://pangolin.sbertech.ru/center/docs/7/administration-guide/administration-scenarios | gone 404 | — | 2 | missing (policy_skipped) |
| https://pangolin.sbertech.ru/center/docs/7/administration-guide/pangolin-tuner | moved 206 → https://pangolin.sbertech.ru/center/docs/7/administration-guide/pangolin-tuner/ | https://pangolin.sbertech.ru/center/docs/7/administration-guide/pangolin-tuner/ | 1 | resolved (canonical) |
| https://pangolin.sbertech.ru/center/docs/7/extensions/pg_dbms_stats | moved 206 → https://pangolin.sbertech.ru/center/docs/7/extensions/pg_dbms_stats/ | — | 1 | missing (policy_skipped) |
| https://pangolin.sbertech.ru/center/docs/7/functional-administration/optimization-1c/1c-temp-tables-optimization | gone 404 | — | 2 | missing (policy_skipped) |
| https://pangolin.sbertech.ru/center/docs/7/functional-administration/recovery-control/copywala | gone 404 | — | 1 | missing (policy_skipped) |
| https://pangolin.sbertech.ru/center/docs/7/functional-administration/recovery-control/copywala/copywala-create-backup-generic-folder | gone 404 | — | 1 | missing (policy_skipped) |
| https://pangolin.sbertech.ru/center/docs/7/functional-administration/recovery-control/copywala/copywala-fuse | gone 404 | — | 1 | missing (policy_skipped) |
| https://pangolin.sbertech.ru/center/docs/7/functional-administration/security/password-policies | gone 404 | — | 1 | missing (policy_skipped) |
| https://pangolin.sbertech.ru/center/docs/copywala/1.2.0/about/advantages | gone 404 | — | 2 | missing (policy_skipped) |
| https://pangolin.sbertech.ru/center/docs/copywala/1.2.0/about/conceptual-model | gone 404 | — | 1 | missing (policy_skipped) |
| https://pangolin.sbertech.ru/center/docs/copywala/1.2.0/administration-guide/scenarios/copywala-delta-backups | gone 404 | — | 1 | missing (policy_skipped) |
| https://pangolin.sbertech.ru/center/docs/copywala/1.2.0/administration-guide/scenarios/copywala-restore | gone 404 | — | 3 | missing (policy_skipped) |
| https://pangolin.sbertech.ru/center/docs/copywala/1.2.0/administration-guide/scenarios/copywala-retention-policy-set | gone 404 | — | 1 | missing (policy_skipped) |
| https://pangolin.sbertech.ru/center/docs/kintsugi/1.9/administration-guide/administration-scenarios-custom-metrics | moved 206 → https://pangolin.sbertech.ru/center/docs/kintsugi/ent/administration-guide/administration-scenarios-custom-metrics/ | — | 1 | missing (policy_skipped) |
| https://pangolin.sbertech.ru/center/docs/kintsugi/1.9/user-guide-dbcm/access#роли-пользователей | moved 206 → https://pangolin.sbertech.ru/center/docs/kintsugi/ent/user-guide-dbcm/access/ | — | 1 | missing (policy_skipped) |
| https://pangolin.sbertech.ru/center/docs/kintsugi/1.9/user-guide-dbcm/description-metrics | moved 206 → https://pangolin.sbertech.ru/center/docs/kintsugi/ent/user-guide-dbcm/description-metrics/ | — | 7 | missing (policy_skipped) |
| https://pangolin.sbertech.ru/center/docs/kintsugi/1.9/user-guide-dbcm/description-metrics#панель-мониторинга-логи | moved 206 → https://pangolin.sbertech.ru/center/docs/kintsugi/ent/user-guide-dbcm/description-metrics/ | — | 1 | missing (policy_skipped) |
| https://pangolin.sbertech.ru/center/docs/kintsugi/1.9/user-guide-dbcm/description-settings-alerting | moved 206 → https://pangolin.sbertech.ru/center/docs/kintsugi/ent/user-guide-dbcm/description-settings-alerting/ | — | 1 | missing (policy_skipped) |
| https://pangolin.sbertech.ru/center/knowledge-base/ | live 206 | https://pangolin.sbertech.ru/center/knowledge-base/ | 1 | resolved |
| https://platform-docs.v-serv.ru/online-documentation/home/user/data/pangolin-se/6.4.0 | live 200 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/DBM/1.9.1-std/common/ | moved 206 → https://platformv.sbertech.ru/docs/public/404.html | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/DBM/1.9.1/common/documents/administration-guide/metrics-mapping-on-save-request.html?h=promet | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/DBM/1.9.1/common/documents/user-guide-dbcm/description-clusters.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/DBM/1.9.1/common/documents/user-guide-dbcm/description-operations-center.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/DBM/1.9.1/common/documents/user-guide-dbcm/scenarios/instructions-bulk-load-monitoring-objects.html#id1 | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/DBM/1.9.1/common/documents/user-guide-dbcm/scenarios/instructions-clusters-open-connect-db.html | gone 302 | — | 2 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/ | moved 206 → https://platformv.sbertech.ru/docs/public/404.html | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/about/functions.html?h=json_table | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/architecture-components/patroni/ha_multi_dc.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/architecture-components/patroni/patroni-main.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/architecture-components/patroni/watchdog.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/description-of-functionality/index.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/extensions/citus.html | gone 302 | — | 2 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/extensions/ora2pg.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/extensions/pg_background.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/extensions/pg_cron.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/extensions/pg_profile.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/extensions/pg_stat_statements.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/extensions/pg_variables.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/extensions/pgse_backup.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/extensions/psql_bfile.html | gone 302 | — | 2 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/extensions/psql_quotolin.html | gone 302 | — | 2 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/extensions/rum.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/extensions/timescaledb.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/extensions/vector.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/authentication/authentication-proxy-pooler.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/authentication/authorization-ldap.html?h=ldap | gone 302 | — | 2 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/diagnostic/blocks-monitoring.html | gone 302 | — | 1 | missing (out_of_scope) |

### pangolin — пачка 2 (26 строк)

| URL из ячейки | адрес | документ в корпусе | ячеек | статус |
|---|---|---|---|---|
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/diagnostic/diagnostic-report.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/diagnostic/diff-time.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/diagnostic/req-plans.html | gone 302 | — | 2 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/diagnostic/resource-consumptions-limit.html#id1 | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/global-indexes.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/msq.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/native-partitioning.html | gone 302 | — | 2 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/recovery-control/adress-load.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/recovery-control/copywala.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/recovery-control/db-integrity-control.html | gone 302 | — | 2 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/recovery-control/wal-replica-integrity-control.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/security/audit.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/security/data-obfuscation.html | gone 302 | — | 2 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/security/password-policies.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/security/privileged-users.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/security/req-masking.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/security/tde.html | gone 302 | — | 2 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/functional-administration/trace-sessions.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/installation-guide/docker-image-pangolin.html | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/installation-guide/installation/installation-automatic.html | gone 302 | — | 2 | missing (out_of_scope) |
| https://platformv.sbertech.ru/docs/public/PSQ/7.1.1/common/documents/installation-guide/other-information/description-rpm-packages.html#pangolin-manager | gone 302 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/education/online-courses/subd-pangolin-dba-0-1 | auth 401 | — | 1 | missing (out_of_scope) |
| https://platformv.sbertech.ru/release-policy | auth 401 | — | 2 | missing (out_of_scope) |
| https://platformv.sbertech.ru/strapi/api/media/Sertifika_ot_07_11_2024_1d5305f4d5.pdf | gone 404 | — | 1 | missing (out_of_scope) |
| https://sbertech.ru/about/legal-info/klientam-i-partneram/moduli-tipovyh-uslovij | auth 401 | — | 1 | missing (policy_skipped) |
| https://v8.1c.ru/tekhnologii/systemnye-trebovaniya-1s-predpriyatiya-8/subd/ | live 200 | — | 1 | missing (out_of_scope) |

### postgrespro — пачка 1 (50 строк)

| URL из ячейки | адрес | документ в корпусе | ячеек | статус |
|---|---|---|---|---|
| https://machine.postgrespro.ru | moved 200 → https://postgrespro.ru/products/machine | — | 1 | missing (out_of_scope) |
| https://platform.gov.ru/dopolnitelnye-servisy/subd-postgres-pro-enterprise/ | unreachable -1 | — | 1 | missing (out_of_scope) |
| https://postgrespro.ru/docs | live 200 | https://postgrespro.ru/docs | 1 | resolved |
| https://postgrespro.ru/docs/axe/1/postgres-pro-axe-overview | live 200 | https://postgrespro.ru/docs/axe/1/postgres-pro-axe-overview | 1 | resolved |
| https://postgrespro.ru/docs/enterprise/16/storage-toast | live 200 | https://postgrespro.ru/docs/enterprise/16/storage-toast | 1 | resolved |
| https://postgrespro.ru/docs/enterprise/17/citus | live 200 | — | 2 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/17/in-memory | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/17/release-proee-17-7-1 | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/17/restrict-dbms-admin-data-access | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/app-ldap2pg | live 200 | — | 2 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/app-pgpro-tune | live 200 | — | 1 | ambiguous |
| https://postgrespro.ru/docs/enterprise/18/app-pgprobackup#PBK-COMPRESSION-OPTS | live 200 | https://postgrespro.ru/docs/enterprise/18/app-pgprobackup | 1 | resolved (canonical) |
| https://postgrespro.ru/docs/enterprise/18/aqe | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/aqo | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/atx | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/autoprepare | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/background-freezer | live 200 | — | 2 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/biha-architecture | live 200 | — | 2 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/biha-architecture#BIHA-REFEREE | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/biha-solution | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/binary-installation-on-linux | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/cfs | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/dbms-lob | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/ddl-partitioning | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/dump-stat | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/functions-json | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/hypopg | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/intro-pgpro-vs-pg | live 200 | — | 2 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/mamonsu | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/memory-purge-cert | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/multimaster | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/pg-integrity-check | live 200 | — | 2 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/pg-proaudit | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/pg-query-state | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/pg-variables | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/pg-wait-sampling | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/pgpro-anonymizer | live 200 | — | 2 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/pgpro-autopart | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/pgpro-bfile | live 200 | https://postgrespro.ru/docs/enterprise/18/pgpro-bfile?lang=ru | 2 | resolved (canonical_query) |
| https://postgrespro.ru/docs/enterprise/18/pgpro-gbtree | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/pgpro-ilm | live 200 | — | 2 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/pgpro-multiplan | live 200 | — | 3 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/pgpro-pwr | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/pgpro-queue | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/pgpro-result-cache | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/pgpro-rp | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/pgpro-scheduler | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/pgpro-sfile | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/pgpro-stats | live 200 | — | 2 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/pgpro-usage | live 200 | — | 2 | missing (in_scope_not_reached) |

### postgrespro — пачка 2 (50 строк)

| URL из ячейки | адрес | документ в корпусе | ячеек | статус |
|---|---|---|---|---|
| https://postgrespro.ru/docs/enterprise/18/pgtransfer | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/plpgsql-package | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/proxima | live 200 | — | 4 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/proxima#PROXIMA-KVIK | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/release-proee-18-1-1 | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/routine-vacuuming | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/rum | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/runtime-config-resource | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/sql-altertable#SQL-ALTERTABLE-MERGE-PARTITIONS | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/sql-createprofile | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/sql-createtable | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/tde | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/wal-restoration | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/enterprise/18/warm-standby | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ora2pgpro/24/index | live 200 | https://postgrespro.ru/docs/ora2pgpro/24/index | 2 | resolved |
| https://postgrespro.ru/docs/ora2pgpro/24/ora2pgpro-exporting-associative-arrays | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/pg_probackup3/3.2/app-pgprobackup | live 200 | — | 4 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/pg_probackup3/3.2/app-pgprobackup#PBK-FUSE | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/pg_probackup3/3.2/pbk-key-features | live 200 | — | 2 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/pg_probackup3/3.2/pbk-new-features | live 200 | — | 6 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/pg_probackup3/3.2/pbk-restoring-a-cluster | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/postgrespro/current/monitoring-locks | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/postgrespro/current/textsearch-dictionaries | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/index | live 200 | https://postgrespro.ru/docs/ppem/2/index | 1 | resolved |
| https://postgrespro.ru/docs/ppem/2/ppem-activity | live 200 | — | 3 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-alerts | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-architecture | live 200 | — | 2 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-authentication | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-backup | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-clusters | live 200 | — | 2 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-configuration-presets | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-control-center | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-databases | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-extensions | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-features | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-instance-roles | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-instances | live 200 | — | 2 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-integration-with-openldap-and-active-directory | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-message-log | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-metrics | live 200 | — | 2 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-profiler | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-query-plan-visualization | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-schemas | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-sql-statistics | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-tables | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-tablespaces | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-task-console | live 200 | — | 2 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/ppem/2/ppem-user-management | live 200 | — | 2 | missing (in_scope_not_reached) |
| https://postgrespro.ru/docs/shardman/17/intro-whatis | live 200 | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/education/cert | live 200 | https://postgrespro.ru/education/cert | 1 | resolved |

### postgrespro — пачка 3 (13 строк)

| URL из ячейки | адрес | документ в корпусе | ячеек | статус |
|---|---|---|---|---|
| https://postgrespro.ru/education/courses | live 200 | https://postgrespro.ru/education/courses | 1 | resolved |
| https://postgrespro.ru/products/PPEM | live 200 | https://postgrespro.ru/products/PPEM | 1 | resolved |
| https://postgrespro.ru/products/postgrespro/enterprisecertified | live 200 | https://postgrespro.ru/products/postgrespro/enterprisecertified | 2 | resolved |
| https://postgrespro.ru/products/progate | live 200 | https://postgrespro.ru/products/progate | 1 | resolved |
| https://postgrespro.ru/release-policy | live 200 | https://postgrespro.ru/release-policy | 1 | resolved |
| https://postgrespro.ru/roadmap | moved 200 → https://postgrespro.ru | — | 1 | missing (in_scope_not_reached) |
| https://postgrespro.ru/services/support | live 200 | https://postgrespro.ru/services/support | 2 | resolved |
| https://postgrespro.ru/support/sw | moved 200 → https://postgrespro.ru:443/services/support/sw | https://postgrespro.ru/support/sw | 1 | resolved |
| https://support.postgrespro.ru/ | live 206 | — | 1 | missing (out_of_scope) |
| https://support.postgrespro.ru/articles | live 206 | — | 1 | missing (out_of_scope) |
| https://v8.1c.ru/tekhnologii/systemnye-trebovaniya-1s-predpriyatiya-8/subd/ | live 200 | — | 1 | missing (out_of_scope) |
| https://www.postgresql.org/community/contributors/ | live 206 | — | 1 | missing (out_of_scope) |
| https://www.rubackup.ru/documentation/certificates/ | live 200 | — | 1 | missing (out_of_scope) |

### tantor — пачка 1 (50 строк)

| URL из ячейки | адрес | документ в корпусе | ячеек | статус |
|---|---|---|---|---|
| https://docs.tantorlabs.ru/tdb/ru/14_17/se1c/sql-createstatistics.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/14_24/se1c/sql-createstatistics.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/15_12/certified/pg_integrity_verifier.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/15_19/certified/pg_integrity_verifier.html | — | 2 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/15_12/se/citus.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/15_19/se/citus.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/15_12/se/orafce.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/15_19/se/orafce.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/16_10/certified/runtime-config-file-locations.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/16_15/certified/runtime-config-file-locations.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/16_8/se/pg_archive.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/16_15/se/pg_archive.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/16_8/se/pgbouncer.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/16_15/se/pgbouncer.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/16_8/se1c/ldap2pg.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/16_15/se1c/ldap2pg.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/be/backup.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/be/backup.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/app-pgdump.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/app-pgdump.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/autonomous-transactions.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/autonomous-transactions.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/backup-dump.html#25-1-3-%D0%BE%D0%B1%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%BA%D0%B0-%D0%B1%D0%BE%D0%BB%D1%8C%D1%88%D0%B8%D1%85-%D0%B1%D0%B0%D0%B7-%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85 | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/backup-dump.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/bgworker.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/bgworker.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/binary-download-execute.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/binary-download-execute.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/credcheck.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/credcheck.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/ddl-partitioning.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/ddl-partitioning.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/differences.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/differences.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/functions-json.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/functions-json.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/hstore.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/hstore.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/hydra.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/hydra.html | — | 4 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/hypopg.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/hypopg.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/kernel-resources.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/kernel-resources.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/libpq-connect.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/libpq-connect.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/monitoring-locks.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/monitoring-locks.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/orafce.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/orafce.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/page_repair.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/page_repair.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/pg-anon.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/differences.html | — | 3 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/pg_background.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/pg_background.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/pg_sec_check.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/pg_sec_check.html | — | 3 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/pg_tde.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/pg_tde.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/pg_throttle.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/pg_throttle.html | — | 2 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/pg_trace.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/pg_trace.html | — | 3 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/pg_variables.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/pg_variables.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/pgaudit.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/pgaudit.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/pgbouncer.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/pgbouncer.html | — | 2 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/planner-optimizer.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/planner-optimizer.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/routine-vacuuming.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/routine-vacuuming.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/runtime-config-query.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/runtime-config-query.html | — | 2 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/runtime-config-resource.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/runtime-config-resource.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/sql-createrule.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/sql-createrule.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/textsearch-dictionaries.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/textsearch-dictionaries.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/transp_anon.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/transp_anon.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/vector.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/vector.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se/wal-g.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se/wal-g.html | — | 9 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tdb/ru/17_5/se1c/runtime-config-resource.html | moved 206 → https://docs.tantorlabs.ru/tdb/ru/17_11/se1c/runtime-config-resource.html | — | 1 | missing (in_scope_not_reached) |
| https://docs.tantorlabs.ru/tp/6.0/admin/admin_diagnostic_utility.html | live 206 | — | 1 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/admin/admin_reports.html | live 206 | — | 1 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/admin/admin_tokens.html | live 206 | — | 1 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/admin/admin_users.html | live 206 | — | 1 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/admin/backup_info.html | live 206 | — | 1 | missing (out_of_scope) |

### tantor — пачка 2 (33 строк)

| URL из ячейки | адрес | документ в корпусе | ячеек | статус |
|---|---|---|---|---|
| https://docs.tantorlabs.ru/tp/6.0/admin/group_page.html | live 206 | — | 1 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/admin/ldap.html | live 206 | — | 2 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/admin/monitoring_with_prometheus_and_grafana.html | live 206 | — | 1 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/index.html | live 206 | — | 1 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/instances/DB_browser.html | live 206 | — | 7 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/instances/activity.html | live 206 | — | 4 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/instances/activity.html#id4 | live 206 | — | 1 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/instances/admin_scheduler.html | live 206 | — | 1 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/instances/charts.html | live 206 | — | 1 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/instances/instance_adding.html | live 206 | — | 1 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/instances/overview.html | live 206 | — | 2 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/instances/param_groups.html | live 206 | — | 2 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/instances/pg_monitor/pg_m_Logs.html | live 206 | — | 1 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/instances/pg_monitor/pg_monitor.html | live 206 | — | 1 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/instances/query_profiler.html | live 206 | — | 1 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/instances/replication.html | live 206 | — | 1 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/instances/ug_clusters_pages.html | live 206 | — | 1 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/platform_architecture.html | live 206 | — | 1 | missing (out_of_scope) |
| https://docs.tantorlabs.ru/tp/6.0/requirements.html | live 206 | — | 1 | missing (out_of_scope) |
| https://github.com/TantorLabs/pg_cluster/tree/master | live 206 | — | 4 | missing (out_of_scope) |
| https://lk-new.astralinux.ru/ | moved 206 → https://lk.astra.ru/ | — | 1 | missing (out_of_scope) |
| https://tantorlabs.ru/certified | live 200 | https://tantorlabs.ru/certified | 1 | resolved |
| https://tantorlabs.ru/docs/support | live 200 | http://tantorlabs.ru/docs/support | 1 | resolved (canonical) |
| https://tantorlabs.ru/edu | live 200 | https://tantorlabs.ru/edu | 1 | resolved |
| https://tantorlabs.ru/edu/atc-dbms | live 200 | https://tantorlabs.ru/edu/atc-dbms | 1 | resolved |
| https://tantorlabs.ru/pgconfigurator | live 200 | https://tantorlabs.ru/pgconfigurator | 1 | resolved |
| https://tantorlabs.ru/products/dbms | live 200 | https://tantorlabs.ru/products/dbms | 1 | resolved |
| https://tantorlabs.ru/products/dlh | moved 200 → https://tantorlabs.ru/products/di | https://tantorlabs.ru/products/di | 1 | resolved (moved_exact) |
| https://tantorlabs.ru/products/xdata | live 200 | https://tantorlabs.ru/products/xdata | 2 | resolved |
| https://tantorlabs.ru/tantor-se-1c/tpost/1ldz1eb691-tantor-labs-vipustila-visokoproizvoditel | moved 200 → https://tantorlabs.ru/news/tpost/1ldz1eb691-tantor-labs-vipustila-visokoproizvoditel | — | 1 | missing (in_scope_not_reached) |
| https://v8.1c.ru/platforma/krossplatformennost-rabota-s-razlichnymi-subd/ | live 200 | — | 1 | missing (out_of_scope) |
| https://wiki.astralinux.ru/tandocs/baza-znanij-tantor-238752972.html | live 200 | — | 1 | missing (unreadable_origin) |
| https://www.postgresql.org/community/contributors/ | live 206 | — | 1 | missing (out_of_scope) |

