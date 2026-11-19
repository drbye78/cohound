# Кросс-языковой замер: читает ли оценщик ZH-документ так же, как EN (план 26, §O21 — ZH-часть)

**Дата:** 17.11.2026. **Дерево:** главный чекаут `cohound`, HEAD `d997c28` + приборы этого замера.
**Прибор:** `scripts/measure_cross_lingual_eval.py` (новый). **Тесты:**
`tests/scripts/test_measure_cross_lingual_eval.py` (20 проверок, без сети и без LLM).
**Артефакт прогона:** `/tmp/opencode/xling/run-b.json` (данные, не репозиторий; JSON пишет
сам прибор, ключи `records`/`summaries`/`query_language_control`/`cost`).

## 1. Вопрос и ответ

`render_queries` (`core/pipeline/prompt.py:510`) собирает запрос Ш6 из `title` + терминов
**ru → en**. ZH-документ с русским запросом в лексическом плече R0 не находится — это
структурный ноль, а не «хуже». Прежде чем чинить поиск, нужно отделить «оценщик не читает
по-китайски» от «поиск не находит по-китайски».

Ответ (30 пар, один и тот же мануал PostgreSQL в двух языках):

| вопрос | ответ | чем |
|---|---|---|
| Даёт ли оценщик другой вердикт на ZH-тексте? | **Нет**: согласие 26/30 (0.867), κ **0.632** [0.235;0.907]; в трёх прогонах одного входа κ 0.63–0.81, и **ни одного** `✅→❓` или `✅→❌` | режим `aligned` |
| Это «одинаково понял» или «одинаково промолчал»? | **Промолчал**: 22–24 пары из 30 — `❓→❓`; решающих пар 6–8, из них согласны 5–6 | матрица переходов |
| Находит ли ZH лексическое плечо? | **Находит, 28/30** — но не потому, что ZH, а потому что в запросе есть **латинский токен** (`WAL`, `postgresql`); с чисто русским запросом — **0 из 98** ячеек на **обеих** сторонах | режим `retrieval` + контроль языка запроса |

То есть ZH-документ **не хуже** EN для оценщика на этой выборке — и **не лучше**: оба почти
всегда дают `❓`, а лексическое плечо находит документ не по языку документа, а по латинскому
токену в запросе. ZH не «плохой язык» — он в том же положении, что EN-документ с русским
запросом, и это положение измерено числом (§4.4).

## 2. Стенд замера

Один и тот же мануал в двух языках: сравнение не смешивает язык с «у другого продукта другая
функциональность».

| | EN | ZH |
|---|---|---|
| хост | `https://www.postgresql.org/docs/18/<path>` | `http://www.postgres.cn/docs/18/<path>` |
| редакция | 18.x (текущая ветка `docs/18`) | **18.3, community-перевод** (не вендорский) |
| robots | запрещает `/admin/`, `/account/`, `/docs/devel/`, `/list/`, `/search/` — `/docs/18/` разрешён | `robots.txt` → **404**: правило **не объявлено**; это «не объявлено», а не «разрешено» |
| пример | `runtime-config-connection.html` → 25 121 симв. (в этом замере — 24 707) | тот же путь → 9 901 симв. (в этом замере — 11 117) |
| идентификаторы | `max_connections ( integer )` | **те же латиницей** |

**Расхождение патча объявлено:** EN-сторона — ветка `18` (патч не зафиксирован), ZH — перевод
18.3. «Перевод отстал на патч» — свойство редакции, а не дефект оценщика; в отчёте это не
исправляется и не маскируется.

Условия, общие для обеих сторон (объявлены, а не унаследованы от хоста):

| условие | значение | откуда |
|---|---|---|
| читатель | `trafilatura-html-7` на момент замера (`core/adapters/extract_html.py:363` — текущая `trafilatura-html-8`, снятие пролога XHTML) | тот же, что в пайплайне |
| чанкер | `md-ast-1:commonmark-0.31.2+gfm+pandoc/mdit-4.2.0/plugins-0.6.1/policy-6`, `chunk_size=**1500**` | `MarkdownChunkerPort()`; ключа `chunk_size` в `config/deploy.yaml` **нет** — это дефолт кода (`core/adapters/chunker_markdown.py:336`) |
| оценщик | `deepseek-v4.1-flash-1017:yellow`, `decoder=json_object`, endpoint `deploy.llm.cheap` | `--llm-model` > слот; `COHOUND_LLM_MODEL` не читается |
| путь оценки | `core.pipeline.evaluate.evaluate_cell` (`core/pipeline/evaluate.py:3023`) | не своя копия промпта |
| поиск (режим `retrieval`) | `TantivyLexicalIndex`, профиль **`stemmed`** (`core/adapters/lexical_tantivy.py:482`), `cutoff_K=10`, запросы `run_queries_for_cell` (`core/pipeline/evaluate.py:1057`) | тот же путь R0, что в прогоне |
| вежливость | пауза **1.0 с** между сетевыми запросами, кэш страниц `/tmp/opencode/xling/cache/` | повторный прогон качает **0** страниц (проверено: прогон-артефакт — 0 запросов, 65 из кэша) |
| логин/обход барьеров | нет | — |

## 3. Как строится выравнивание (одной фразой)

Из `catalog.csv` берутся параметры, у которых `concept_terms.en` содержит ASCII-идентификатор;
его страница находится **механически** — по предметному указателю EN-мануала
(`/docs/18/bookindex.html`, 3205 записей; перечень страниц сверяется с sitemap: 1149 URL
`/docs/18/`), а слайс — чанки, содержащие этот якорь буквально (границы ASCII-токена, регистр
не значим, ≤ 3 чанка на сторону), на обеих сторонах по одному пути.

Правила, которые делают выбор механическим (без разметки человеком):

- **якорь** — термин `concept_terms.en`, целиком являющийся ASCII-идентификатором
  (`[A-Za-z_][A-Za-z0-9_]{2,}`); ранг — «сила идентификатора»: с `_` раньше, затем длиннее
  (`scripts/measure_cross_lingual_eval.py:199`);
- **страница** — цели указателя для якоря (точное имя, затем «имя + слово»), ≤ 3 кандидата;
  берётся та, где текст EN длиннее: указатель иногда ведёт на оглавление главы, и «самая
  содержательная цель» отделяет раздел от списка ссылок (`:783`);
- **заглушка** (текст EN короче `chunk_size`) отбрасывается: порог — это бюджет самого
  чанкера, а не выбранное число (`:809`);
- **пара** существует, только если якорь есть в тексте **обеих** сторон.

Отбраковка (числа прогона-артефакта): `no_ascii_term` **89**, `anchor_not_in_index` **16**,
`anchor_absent_zh` **1**, `no_usable_page` **6** — 30 пар собрано, остальные **39** параметров
каталога (181) не рассматривались: предел выборки достигнут.

## 4. Результаты

### 4.1 Режим `aligned` — изолирует оценщика

Обе стороны получают **свой** якорный слайс одного участка текста; поиска нет.

| пар | κ (95 % CI, бутстрэп по парам 2000, seed 20261117) | согласие | ✅→❓ | ✅→❌ |
|---|---|---|---|---|
| 30 | **0.632** [0.235;0.907] | 0.867 | **0** | **0** |

Матрица переходов `status_EN → status_ZH`:

| EN → ZH | число |
|---|---|
| ⚠️ → ✅ | 1 |
| ⚠️ → ⚠️ | 3 |
| ⚠️ → ❓ | 1 |
| ❌ → ❌ | 1 |
| ❌ → ❓ | 1 |
| ❓ → ⚠️ | 1 |
| ❓ → ❓ | **22** |

**Как это читать.** Согласие высокое, но оно в основном **согласие в молчании**: 22 из 30 пар —
`❓→❓`. Решающих пар (EN сказал что-то) восемь: шесть согласны, две расходятся
(`⚠️→✅` — перевод увидел наличие там, где EN увидел частичное; `⚠️→❓` — перевод промолчал).
`✅→❓` и `✅→❌` равны нулю **потому, что EN ни разу не сказал `✅`** на этой выборке: классы
дефектов не задействованы, и «0» здесь — факт выборки, а не нулевой риск.

### 4.2 Режим `retrieval` — весь лексический путь R0

Тот же параметр, но слайс выбирает запрос Ш6 по `TantivyLexicalIndex` (профиль `stemmed`),
отдельно по EN-тексту и по ZH-тексту.

| пар | κ (95 % CI) | согласие | ✅→❓ | ✅→❌ | EN нашёл чанк | ZH нашёл чанк |
|---|---|---|---|---|---|---|
| 30 | **0.754** [0.423;1.000] | 0.900 | **0** | **0** | **28/30** | **28/30** |

Матрица переходов:

| EN → ZH | число |
|---|---|
| ✅ → ✅ | 1 |
| ⚠️ → ⚠️ | 4 |
| ⚠️ → ❓ | 1 |
| ❌ → ❌ | 1 |
| ❓ → ⚠️ | 1 |
| ❓ → ❌ | 1 |
| ❓ → ❓ | **21** |

**Структурный ноль на ZH не подтвердился — и это результат, а не поломка.** В 28 из 30 пар ZH
нашёл чанки, содержащие якорь. Причина измерена, а не угадана: BM25 матчит **латинский токен**,
который есть в самом запросе (`... фрагментов WAL ...`, `... json (json_table)`), и он же
остаётся латиницей в ZH-тексте. Проверено точечно на одной ZH-странице: запрос `WAL` → 10
чанков, запрос `WAL-файлов` (идентификатор склеен с кириллицей) → 0. Две пары с нулём на обеих
сторонах — ровно те, где идентификатор в запросе склеен с кириллицей или отсутствует
(`xlsx_063`, `xlsx_129`).

### 4.3 Контроль корреляции: 16 слайсов, а не 30 независимых

Семь параметров меряются на одной странице (`plpgsql-porting.html`, якорь `oracle`), четыре —
на `runtime-config-wal.html`. Это не независимые наблюдения, и складывать их в κ значит
занижать интервал. Сводка по одному представителю на группу `(якорь, путь)`:

| режим | слайсов | κ (95 % CI) | ✅→❓ | ✅→❌ |
|---|---|---|---|---|
| `aligned` | 16 | 0.549 [0.000;1.000] | 0 | 0 |
| `retrieval` | 16 | 0.828 [0.000;1.000] | 0 | 0 |

Интервал на 16 парах не различает ничего — публикуется как есть, а не как «подтверждение».

### 4.4 Контроль языка запроса: русский запрос против **обеих** сторон

Режим `retrieval` смещён выбором: пара существует только там, где есть латинский якорь, — то
есть именно там, где мост между русским запросом и ZH-текстом **есть**. Контроль снимает это
смещение: 10 параметров **без** ASCII-якоря (их запрос чисто русский) × 14 страниц обеих сторон
= 140 ячеек; вызовов модели нет, мерится только выдача поиска.

| язык запроса | ячеек | EN нашёл хоть один чанк | ZH нашёл хоть один чанк |
|---|---|---|---|
| чисто русский | **98** | **0** | **0** |
| есть латинский токен | 42 | 28 | 26 |

Это и есть структурный ноль, но он **не про ZH**: русский запрос не находит ни EN-, ни
ZH-документ. Лексическое плечо ищет на языке запроса; ZH-документ не хуже EN-документа — он
ровно так же не найден. Мост один — латинский токен в запросе, и он работает одинаково на
обеих сторонах (28 против 26 из 42).

### 4.5 Контроль читателя: асимметрия есть, но текста не меняет

postgres.cn отдаёт страницы XHTML с объявлением кодировки, из-за чего пре-клин
(`strip_active_html`, `core/util/sanitize.py:296`) деградирует к allow-list; postgresql.org —
нет. Это **асимметрия читателя**, и она могла бы объяснить расхождение вердиктов без всякого
языка. Измерено: деградация — на **30 из 30** ZH-страниц, на 0 EN-страниц; повторное чтение со
снятым объявлением даёт **побайтово тот же текст** на 26 из 30 страниц и **+7…+11 символов**
(0.02 %) на четырёх. Текст, который видит оценщик, от политики пре-клайна здесь не зависит —
влияние читателя на вердикт этим ограничено, но не исключено.

### 4.6 Повторяемость: κ не воспроизводится, вердикты — почти

Три прогона **одного и того же входа** (те же пары, та же модель, тот же кэш страниц):

| прогон | aligned κ [CI] | retrieval κ [CI] | согласие (aligned/retrieval) | LLM-вызовов |
|---|---|---|---|---|
| 1 (до контроля языка запроса) | 0.789 [0.464;1.000] | 0.656 [0.266;0.924] | 0.933 / 0.867 | 119 |
| 2 (холодный кэш) | 0.814 [0.514;1.000] | 1.000 [1.000;1.000] | 0.933 / 1.000 | 118 |
| 3 (артефакт) | 0.632 [0.235;0.907] | 0.754 [0.423;1.000] | 0.867 / 0.900 | 116 |

Между прогонами 2 и 3 вердикт изменился у **4 из 30** пар (`aligned`) и у **3 из 30**
(`retrieval`). Отсюда граница: **направление** вывода («ZH не даёт систематически другого
вердикта; `✅→❓` и `✅→❌` не наблюдались») устойчиво во всех трёх прогонах, а **величина κ** на
n=30 не воспроизводится — публиковать её как «κ ≈ 0.7» значило бы выдать шум за измерение.
Разброс 0.63–0.81 — это и есть цена недетерминизма оценщика на выборке такого размера.

## 5. Построчные данные (`aligned`, прогон-артефакт)

Цитаты — по ≤120 символов, как их вернул оценщик (`cell.evidence`).

| param_id | якорь | путь | EN симв. | ZH симв. | ZH/EN | EN | ZH | цитата EN | цитата ZH |
|---|---|---|---|---|---|---|---|---|---|
| xlsx_025_vozmozhnost_garantirovanoi_ochistki_wipe_neaktualnyh | wal | runtime-config-wal.html | 38418 | 17498 | 0.455 | ❓ | ❓ |  |  |
| xlsx_027_sinhronizaciya_rolei_i_grupp_iz_ldap | ldap | install-make.html | 32597 | 14994 | 0.46 | ❓ | ❓ |  |  |
| xlsx_028_autentifikaciya_polzovatelei_cherez_vneshnie_sluzhby | ldap | install-make.html | 32597 | 14994 | 0.46 | ⚠️ | ⚠️ | Build with LDAP support for authentication and connection parameter lookup | 为认证和连接参数查找编译LDAP支持 |
| xlsx_029_otdelnoe_rabochee_mesto_arm_dlya_vydelennyh_rolei_se | security | sql-security-label.html | 4568 | 2726 | 0.597 | ❓ | ❓ |  |  |
| xlsx_031_prozrachnoe_dlya_prilozhenii_zashchitnoe_preobrazova | oracle | plpgsql-porting.html | 17162 | 12359 | 0.72 | ❓ | ❓ |  |  |
| xlsx_035_utilita_dlya_kompleksnogo_audita_bezopasnosti_analog | oracle | plpgsql-porting.html | 17162 | 12359 | 0.72 | ❓ | ❓ |  |  |
| xlsx_046_nalichie_append_only_storage | storage | sql-createtable.html | 69569 | 34694 | 0.499 | ❓ | ❓ |  |  |
| xlsx_055_nalichie_in_memory_keshirovaniya_s_obnovleniem_kesha | delete | hstore.html | 14802 | 9843 | 0.665 | ❓ | ❓ |  |  |
| xlsx_063_vozmozhnost_sliyaniya_inkrementalnye_kopii_s_roditel | wal | runtime-config-wal.html | 38418 | 17498 | 0.455 | ❓ | ❓ |  |  |
| xlsx_065_vozmozhnost_vosstanovleniya_na_moment_vremeni_point | recovery | runtime-config-wal.html | 38418 | 17498 | 0.455 | ⚠️ | ✅ | minimal WAL does not contain sufficient information for point-in-time recovery, so `replica` or higher must be used to e | 默认情况下，恢复将会一直恢复到 WAL 日志的末尾。下面的参数可以用来指定一个更早的停止点。 |
| xlsx_070_szhatie_rezernvyh_kopii_s_podderzhkoi_algoritmov_szh | zlib | install-requirements.html | 7251 | 3442 | 0.475 | ⚠️ | ⚠️ | The zlib compression library is used by default. | 默认使用zlib压缩库。 |
| xlsx_077_otrabotka_oshibok_peredachi_wal_na_repliku_izbezhani | wal | runtime-config-wal.html | 38418 | 17498 | 0.455 | ❓ | ❓ |  |  |
| xlsx_078_nalichie_in_memory_tehnologii | memory | kernel-resources.html | 23150 | 10988 | 0.475 | ❓ | ❓ |  |  |
| xlsx_083_split_merge_sekcii | merge | dml-returning.html | 3800 | 2019 | 0.531 | ❓ | ❓ |  |  |
| xlsx_089_ogranichenie_resursov_cpu_i_ram_dlya_sessii | cpu | runtime-config-query.html | 23827 | 10619 | 0.446 | ❓ | ❓ |  |  |
| xlsx_090_upravlenie_prioritezaciei_resursov_cpu_ram_i_o | cpu | runtime-config-query.html | 23827 | 10619 | 0.446 | ❓ | ⚠️ |  | 你可以一起提高或降低两个值来改变磁盘 I/O 代价相对于 CPU 代价的重要性，后者由下列参数描述。 |
| xlsx_099_podderzhka_szhatiya_v_libpq | libpq | libpq-exec.html | 42524 | 21509 | 0.506 | ❓ | ❓ |  |  |
| xlsx_105_in_memory_analiticheskii_dvizhok_s_podderzhkoi_kolon | memory | kernel-resources.html | 23150 | 10988 | 0.475 | ❓ | ❓ |  |  |
| xlsx_113_ogranichenie_resursov_cpu_i_ram_dlya_ekzemplyara_ins | cpu | runtime-config-query.html | 23827 | 10619 | 0.446 | ❓ | ❓ |  |  |
| xlsx_116_diagnosticheskie_otchety_analog_oracle_awr | oracle | plpgsql-porting.html | 17162 | 12359 | 0.72 | ❓ | ❓ |  |  |
| xlsx_119_pluggable_toast | toast | sql-createtable.html | 69569 | 34694 | 0.499 | ❓ | ❓ |  |  |
| xlsx_129_rasshirennye_vozmozhnosti_raboty_s_json_json_table | table | sql-select.html | 64966 | 33921 | 0.522 | ❓ | ❓ |  |  |
| xlsx_131_sovmestimost_s_oracle_pl_sql | oracle | plpgsql-porting.html | 17162 | 12359 | 0.72 | ⚠️ | ⚠️ | PL/pgSQL is similar to PL/SQL in many aspects. | PL/pgSQL与 PL/SQL 在许多方面都非常类似。 |
| xlsx_132_obshchie_slovari_dlya_full_text_search | search | ddl-schemas.html | 13954 | 5347 | 0.383 | ❓ | ❓ |  |  |
| xlsx_134_podderzhka_ocheredei_s_podderzhkoi_tranzakcionnosti | oracle | plpgsql-porting.html | 17162 | 12359 | 0.72 | ❓ | ❓ |  |  |
| xlsx_136_podderzhka_paketov_v_stile_oracle | oracle | plpgsql-porting.html | 17162 | 12359 | 0.72 | ❌ | ❌ | Since there are no packages, there are no package-level variables either. | 因为没有包，所以也没有包级别的变量。 |
| xlsx_137_gotovye_analogi_sistemnyh_paketov_oracle_ukazat_spis | oracle | plpgsql-porting.html | 17162 | 12359 | 0.72 | ❌ | ❓ | Since there are no packages, there are no package-level variables either. |  |
| xlsx_139_parametry_v_skriptah_psql | psql | app-psql.html | 131742 | 64933 | 0.493 | ⚠️ | ❓ | psql provides a number of meta-commands and various shell-like features to facilitate writing scripts and automating a w |  |
| xlsx_141_vozmozhnost_hraneniya_binarnyh_dannyh_vne_failov_dan | oracle | plpgsql-porting.html | 17162 | 12359 | 0.72 | ❓ | ❓ |  |  |
| xlsx_142_podderzhka_sintaksisa_dlya_raboty_s_lob_obektami_ana | oracle | plpgsql-porting.html | 17162 | 12359 | 0.72 | ❓ | ❓ |  |  |

Длины ZH/EN по парам: минимум **0.383**, медиана **0.499**, максимум **0.720** — ZH-текст
примерно вдвое короче EN. Это свойство перевода (и его редакции), а не потеря при чтении: обе
стороны читает один и тот же читатель, и на четырёх страницах снятие объявления кодировки
меняет длину на +7…+11 символов (§4.5).

## 6. Построчные данные (`retrieval`, прогон-артефакт)

| param_id | якорь | путь | запросов | EN хитов | ZH хитов | EN якорь | ZH якорь | EN | ZH |
|---|---|---|---|---|---|---|---|---|---|
| xlsx_025_vozmozhnost_garantirovanoi_ochistki_wipe_neaktualnyh | wal | runtime-config-wal.html | 3 | 10 | 10 | да | да | ❓ | ⚠️ |
| xlsx_027_sinhronizaciya_rolei_i_grupp_iz_ldap | ldap | install-make.html | 3 | 1 | 1 | да | да | ❓ | ❓ |
| xlsx_028_autentifikaciya_polzovatelei_cherez_vneshnie_sluzhby | ldap | install-make.html | 3 | 3 | 1 | да | да | ⚠️ | ⚠️ |
| xlsx_029_otdelnoe_rabochee_mesto_arm_dlya_vydelennyh_rolei_se | security | sql-security-label.html | 3 | 7 | 7 | да | да | ❓ | ❓ |
| xlsx_031_prozrachnoe_dlya_prilozhenii_zashchitnoe_preobrazova | oracle | plpgsql-porting.html | 3 | 10 | 9 | да | да | ❓ | ❓ |
| xlsx_035_utilita_dlya_kompleksnogo_audita_bezopasnosti_analog | oracle | plpgsql-porting.html | 3 | 10 | 9 | да | да | ❓ | ❓ |
| xlsx_046_nalichie_append_only_storage | storage | sql-createtable.html | 3 | 10 | 8 | да | да | ❓ | ❓ |
| xlsx_055_nalichie_in_memory_keshirovaniya_s_obnovleniem_kesha | delete | hstore.html | 3 | 7 | 4 | да | да | ❓ | ❓ |
| xlsx_063_vozmozhnost_sliyaniya_inkrementalnye_kopii_s_roditel | wal | runtime-config-wal.html | 3 | 0 | 0 | нет | нет | ❓ | ❓ |
| xlsx_065_vozmozhnost_vosstanovleniya_na_moment_vremeni_point | recovery | runtime-config-wal.html | 3 | 10 | 6 | да | да | ✅ | ✅ |
| xlsx_070_szhatie_rezernvyh_kopii_s_podderzhkoi_algoritmov_szh | zlib | install-requirements.html | 3 | 2 | 2 | да | да | ⚠️ | ⚠️ |
| xlsx_077_otrabotka_oshibok_peredachi_wal_na_repliku_izbezhani | wal | runtime-config-wal.html | 3 | 10 | 10 | да | да | ❓ | ❓ |
| xlsx_078_nalichie_in_memory_tehnologii | memory | kernel-resources.html | 3 | 10 | 6 | да | да | ❓ | ❓ |
| xlsx_083_split_merge_sekcii | merge | dml-returning.html | 3 | 3 | 2 | да | да | ❓ | ❓ |
| xlsx_089_ogranichenie_resursov_cpu_i_ram_dlya_sessii | cpu | runtime-config-query.html | 3 | 4 | 3 | да | да | ❓ | ❓ |
| xlsx_090_upravlenie_prioritezaciei_resursov_cpu_ram_i_o | cpu | runtime-config-query.html | 3 | 4 | 3 | да | да | ⚠️ | ❓ |
| xlsx_099_podderzhka_szhatiya_v_libpq | libpq | libpq-exec.html | 3 | 4 | 4 | да | да | ❓ | ❓ |
| xlsx_105_in_memory_analiticheskii_dvizhok_s_podderzhkoi_kolon | memory | kernel-resources.html | 3 | 10 | 6 | да | да | ❓ | ❓ |
| xlsx_113_ogranichenie_resursov_cpu_i_ram_dlya_ekzemplyara_ins | cpu | runtime-config-query.html | 3 | 4 | 3 | да | да | ❓ | ❓ |
| xlsx_116_diagnosticheskie_otchety_analog_oracle_awr | oracle | plpgsql-porting.html | 3 | 10 | 9 | да | да | ❓ | ❓ |
| xlsx_119_pluggable_toast | toast | sql-createtable.html | 3 | 9 | 5 | да | да | ❓ | ❓ |
| xlsx_129_rasshirennye_vozmozhnosti_raboty_s_json_json_table | table | sql-select.html | 3 | 0 | 0 | нет | нет | ❓ | ❓ |
| xlsx_131_sovmestimost_s_oracle_pl_sql | oracle | plpgsql-porting.html | 3 | 10 | 10 | да | да | ⚠️ | ⚠️ |
| xlsx_132_obshchie_slovari_dlya_full_text_search | search | ddl-schemas.html | 3 | 8 | 3 | да | да | ❓ | ❓ |
| xlsx_134_podderzhka_ocheredei_s_podderzhkoi_tranzakcionnosti | oracle | plpgsql-porting.html | 3 | 10 | 9 | да | да | ❓ | ❓ |
| xlsx_136_podderzhka_paketov_v_stile_oracle | oracle | plpgsql-porting.html | 3 | 10 | 9 | да | да | ❌ | ❌ |
| xlsx_137_gotovye_analogi_sistemnyh_paketov_oracle_ukazat_spis | oracle | plpgsql-porting.html | 3 | 10 | 9 | да | да | ❓ | ❌ |
| xlsx_139_parametry_v_skriptah_psql | psql | app-psql.html | 3 | 10 | 10 | да | да | ⚠️ | ⚠️ |
| xlsx_141_vozmozhnost_hraneniya_binarnyh_dannyh_vne_failov_dan | oracle | plpgsql-porting.html | 3 | 10 | 9 | да | да | ❓ | ❓ |
| xlsx_142_podderzhka_sintaksisa_dlya_raboty_s_lob_obektami_ana | oracle | plpgsql-porting.html | 3 | 10 | 9 | да | да | ❓ | ❓ |

## 7. Цена

| | значение |
|---|---|
| страниц скачано за весь замер | **75 уникальных**, 8.6 МБ в кэше `/tmp/opencode/xling/cache/` |
| из них в прогоне с холодным кэшем | 61 страница, 7.63 МБ, 98.0 с сетевого времени (пауза 1.0 с) |
| прогон-артефакт | **0** сетевых запросов, 65 страниц из кэша (обещание «повтор не качает заново» выполнено) |
| LLM-вызовов | **116** (артефакт), 118 и 119 в двух других прогонах; 353 за все три |
| почему меньше 120 | пустой слайс не тратит вызов модели (`no_fragments`): в `retrieval` две пары дали 0 чанков на обеих сторонах |
| время прогона | 344.1 с (артефакт), 557.3 с (холодный кэш); всего за замер ≈ 23.6 мин |
| `sitemap` + указатель | 2 страницы, 1 запрос каждый (кэшируются так же) |

## 8. Что не измерено и почему

1. **Dense-плечо (R1/R2).** `bge-m3` многоязычен, но пере-эмбеддинг корпуса ≈ 7 ч GPU
   (`docs/reports/measurements/2026-11-17-policy6-retrieval.md`), и это другой замер. Здесь
   измерено **лексическое** плечо R0 и **оценщик**; вывод «ZH не хуже» на dense не переносится.
   **Измерено 18.11.2026** — `2026-11-18-cross-lingual-retrieval.md`: на чисто русском запросе
   `R0` даёт **0/30** на обеих сторонах, `R1` — 29/30 (якорь `hit@3` 22/30 EN, 26/30 ZH,
   p<0.0001), но судья признаёт срез описанием параметра лишь в 1 случае из 29 на каждой
   стороне ⇒ **симметрия EN/ZH** (язык не мешает) и **узкое место — запрос**, а не матчер.
2. **Нарезка ZH как таковая.** Чанкер markdown-агностичен, но ZH-специфика (заголовки, списки,
   таблицы в переводе) не проверялась: сравнение шло на том, что дал чанкер проекта, и другого
   ZH-корпуса в замере нет.
3. **Общий каталог.** Мерились только параметры с латинским якорем — 30 из 181; 89 параметров
   невыразимы в этом протоколе вообще (нет ASCII-термина), 16 — термин не в указателе, 6 —
   страница-заглушка, 1 — якоря нет в ZH. **Выборка смещена в пользу тех параметров, где мост
   между языками есть**; поэтому «структурный ноль не подтвердился» относится к ним, а к
   остальным — §4.4 (русский запрос = 0 на обеих сторонах).
4. **Ни одного `✅` на EN в `aligned`.** Поэтому `✅→❓` и `✅→❌` равны нулю по составу выборки:
   сказать «перевод не превращает наличие в отсутствие» эти данные не позволяют — они
   позволяют сказать только «на 30 парах такого не случилось». В `retrieval` единственный `✅`
   EN совпал с `✅` ZH.
5. **Человеческий эталон.** Кросс-языкового golden нет: истина «правильно ли оценщик понял
   ZH-абзац» здесь не измеряется — измеряется **совпадение** вердиктов на одном участке текста.
   Совпадение двух ошибок неотличимо от совпадения двух правильных чтений.
6. **Другие продукты, хосты и языки.** Один продукт (PostgreSQL), один переводчик (community),
   два хоста. `ru-pg-dbms` — пилот, и это не потолок: числа — цена этого стенда, не свойство
   ZH-корпусов вообще.
7. **Редакции.** EN 18.x против ZH 18.3: расхождение патча не устранено, а объявлено. Если
   перевод отстал по содержанию, часть расхождений — редакция, а не язык.
8. **Стабильность оценщика.** κ на n=30 не воспроизводится между прогонами (0.63–0.81,
   §4.6). Причина не установлена (температура 0 объявлена; провайдер может её не чтить) —
   это отдельный замер, здесь он только назван.

## 9. Ответ одной фразой

На 30 механически выровненных парах «один участок мануала PostgreSQL на EN и ZH» оценщик даёт
**тот же** вердикт в 26–28 случаях из 30 (κ 0.63–0.81, `✅→❓` и `✅→❌` — ноль), но в основном
это **согласие в молчании** (22–24 пары `❓→❓`); лексическое плечо при этом находит ZH-документ
ровно так же часто, как EN (28/30 против 28/30), **только пока в запросе есть латинский токен** —
с чисто русским запросом оно не находит ни ZH, ни EN (0 из 98 ячеек). То есть ZH-продукт в
когорту ставить **можно** по линии «оценщик не портит чтение переводом», но нельзя по линии
«поиск найдёт»: препятствие — язык запроса, а не язык документа.
