# Pandoc: правила блоков (пустая строка, line block, fancy/example lists)

**Дата:** 04.11.2026. **Класс:** `execution_change` — правила блочного разбора профиля
`pandoc` меняют набор блоков, а значит и границы чанков (`chunker_version`
`policy-3` → **`policy-4`**). Методология не меняется. **Срез:** рабочее дерево на
`91e3748` + правки этой полосы; базовый замер — коммит `91e3748`.
**Инструмент:** `scripts/conformance_pandoc.py` (режим `types-only`). **Оракул:**
`/usr/bin/pandoc` 2.17.1.1 — только на хосте замера/в CI.

## Что сделано

Первая половина L3 (`docs/architecture/18-markdown-chunker-plan.md` §L3): три класса
конструкций, для которых Pandoc markdown расходится с CommonMark/GFM. Таблицы, raw TeX
и нативный HTML `Div` **не трогались** — это вторая половина.

| класс | правило | где |
|---|---|---|
| семантика пустой строки | список/цитата сразу после строки абзаца — продолжение абзаца | `core/adapters/md_dialect/blank_lines.py` |
| line block `\| строка` | блок из строк; единица нарезки — строка, продолжение — `\| ` | `core/adapters/md_dialect/line_block.py` |
| fancy/example lists | `a)`, `A)`, `i.`, `iv.`, `#.`, `(@)`, `(@label)` — списки | `core/adapters/md_dialect/fancy_list.py` |

Правила применяются **только** к профилю `pandoc` (регистрация — `parse_markdown` в
`core/adapters/md_ast.py`); в `commonmark`/`gfm` они не исполняются. Порядок объявлен
списком `RULES`: сначала склейка «прерванных» абзацев, затем деление абзацев на line
block и списки.

Все три правила **не двигают текст**: они перекладывают уже разобранные блоки в другую
форму, спаны остаются отрезками тех же строк. Объединение спанов уровня не меняется,
поэтому P1/P4/P13 сохраняются по построению; чанкер дополнительно режет line block по
строкам и печатает маркер пункта/строки в `context_prefix`.

### Почему именно так — поведение оракула

Каждое решение взято из `pandoc -f markdown -t json`, а не из догадки:

- **Что абзац поглощает** (после того как список/цитата его «прервали»): заголовок,
  тематический разрыв, pipe-таблицу, definition list — да; fenced code и HTML — нет.
  Измерено: `text\n- a\n# H` → один `Para`; `text\n- a\n---` → один `Para`;
  `text\n- a\n| x | y |` → один `Para`; `text\n- a\n```…` → `Para` + `CodeBlock`.
  Множество закреплено как `ABSORBABLE` и покрыто тестами.
- **Соседство по строкам с оговоркой**: `markdown-it` включает замыкающую пустую строку
  в спан списка/цитаты (`- a\n\n---`: `map=[0,2]`), поэтому «следующий блок начинается
  ровно на `end_line`» ещё не значит «нет пустой строки». Проверка
  `_ends_with_content` — цена этой ловушки, найденной первым прогоном оракула
  (без неё на дереве появились 19 ложных `pandoc_only:paragraph`).
- **Line block**: `|a` (без пробела) — текст; отступ на верхнем уровне (`  | a`) — текст;
  строка после абзаца без пустой — текст; `> | a` внутри цитаты — line block.
- **Fancy lists**: `i.` как первый маркер — римский, `v.`/`x.`/`l.` — альфа (измерено:
  `v. one` → `LowerAlpha` со значением 22); одна заглавная буква с точкой (`A.`, `I.`,
  `X.`) — **не** маркер (оракул отдаёт `Para`), с скобкой (`A)`, `I)`) — маркер;
  `iv.`/`v.` — один римский список; `a.`/`b.` через пустую строку — один список,
  `a)`/`iv.` — два (стиль первого маркера решает, `_fits`).

### Границы реализации (объявленные, не забытые)

- Вложенность внутри пункта fancy-списка не разбирается: маркер на любой строке
  начинает следующий пункт того же уровня. На верхнеуровневое сравнение это не влияет,
  текст остаётся точным срезом.
- Рекурсия правил идёт в `blockquote`, но **не** в пункты списков: там нужен отступ
  содержимого по маркеру пункта. Верхний уровень при этом остаётся `list`, поэтому
  оракул этого не видит; чанкер внутри пункта работает как раньше.
- Стиль fancy-списка выводится из первого маркера; продолжение внутри абзаца
  принимает любой маркер (измерено: `a. one\ni. two` — один альфа-список).

## Числа до/после — `scripts/conformance_pandoc.py`

Команда (дословно):

```bash
.venv/bin/python scripts/conformance_pandoc.py --out-json /tmp/pandoc-after.json
```

Код возврата — **1** и до, и после (остаются таблицы/raw/Div). 59 документов
(40 отслеживаемых git файлов дерева + 19 сгенерированных форм), блоков Pandoc — **2 341**.
Срез дерева в этом проходе — 40 из **71** файла (`git ls-files`; в базовом замере было
68 файлов, поэтому число блоков списков отличается).

| класс | до | после | что осталось |
|---|---:|---:|---|
| `sequence_length` — всего | 38 блоков / 10 док. | **10 блоков / 3 док.** | только сгенерированные таблицы и нативный `Div` |
| `mdast_only:list_without_blank_line` | **26** (40 в базовом замере) | **0** | — |
| `mdast_only:blockquote_without_blank_line` | 0 (1 в базовом замере) | **0** | — |
| `unparsed_construct` — всего | 11 блоков / 9 док. | **7 блоков / 7 док.** | таблицы, raw TeX, `Div`, `line_block` (дефект гейта, ниже) |
| `unparsed_construct:example_list` | **2** | **0** | — |
| `unparsed_construct:fancy_list` | **2** | **0** | — |
| `unparsed_construct:line_block` | 1 | 1 | разобран, но гейт не знает отображения (ниже) |
| `unparsed_construct:grid_table` | 1 | 1 | вторая половина L3 |
| `unparsed_construct:multiline_table` | 1 | 1 | вторая половина L3 |
| `unparsed_construct:table` (simple) | 1 | 1 | вторая половина L3 |
| `unparsed_construct:table_caption` | 1 | 1 | вторая половина L3 |
| `unparsed_construct:raw_tex` | 1 | 1 | вторая половина L3 |
| `unparsed_construct:html_div` | 1 | 1 | вторая половина L3 |
| `type_mismatch` | 0 | **0** | — |
| `oracle_error` | 0 | **0** | — |

**Разделение по источникам.** На 40 документах дерева в этом проходе (выборка 40 из 71
файла) было **28 расхождений в 7 документах** — 26 `list_without_blank_line`, по одному
`mdast_only:paragraph` и `pandoc_only:paragraph`; после правки — **0**. В базовом замере
(`91e3748`, 68 файлов, `2026-11-04-pandoc-differential-baseline.md`) на 40 документах было
**42 блока в 9 документах**; разница — состав выборки, а не регресс. Остальные 10
`sequence_length` и 7 `unparsed_construct` — сгенерированные формы, то есть список работы
второй половины L3. Из 19 форм блочно совпадали 10, теперь **12** (13 с учётом дефекта
гейта `line_block`): добавились `line_block`, `example list`, `fancy list`.

### Дефект гейта: `line_block` не отображён в `_MDAST_KINDS`

`scripts/conformance_pandoc.py` (запрещён к правке этой полосе) объявляет канонический
класс для `LineBlock` на стороне Pandoc, но в таблице `_MDAST_KINDS` нет строки для
нашего вида `line_block`, поэтому пара `LineBlock` ↔ `line_block` попадает в
`unparsed_construct:line_block`, хотя конструкция разобрана (в отчёте видно
`mdast=line_block`). Проверено симуляцией **одной строки** без правки файла:

```python
cp._MDAST_KINDS["line_block"] = "line_block"
```

С ней `unparsed_construct` падает до **6 блоков / 6 документов** и `line_block`
исчезает из счётчика совсем; `sequence_length` не меняется. Правка — одна строка в
гейте, а не в разборе.

## Числа до/после — `scripts/conformance_md.py` (commonmark/gfm не сдвинулись)

```bash
.venv/bin/python scripts/conformance_md.py
```

| профиль | сопоставимо | совпало | расхождения | P13 | P1 |
|---|---:|---:|---:|---:|---:|
| CommonMark 0.31.2 | 635 | **635** | **0** | **652/652** | **652/652** |
| GFM | 653 | **653** | **0** | **670/670** | **670/670** |

Числа совпадают с документированными в §7 плана — это и есть приёмка «байт-в-байт».

### Чанк-сет `gfm` на корпусе: 0 различий на 1 929 документах

Живой срез (`ru-pg-dbms-208472049479`) пишется идущим краулом, поэтому два прогона
харнесса подряд видят разные документы — сравнивать «до/после» по двум прогонам
нельзя. Сделано иначе: извлечённые тексты **заморожены** в один снимок
(`1929 документов, 21 447 619 символов`), и по нему прогнаны чанкер из `HEAD`
(`policy-3`) и из рабочего дерева (`policy-4`) с **одними и теми же** входами.

| профиль | документов | чанков (old → new) | документов с отличиями |
|---|---:|---:|---:|
| `gfm` | 1 929 | 23 666 → 23 666 | **0** |
| `pandoc` | 1 929 | 23 665 → 23 648 | 35 |

Для `gfm` сравнение пошло по полному набору полей куска (`text`, `heading_path`,
`context_prefix`, `char_start`, `char_end`) — совпадение посимвольное, а не «по числу
чанков». Процедура снимка (команды дословно, из рабочего дерева):

```bash
# 1. Заморозить входы: извлечь тексты тем же HtmlExtractor, что и пайплайн.
.venv/bin/python /tmp/snapshot_corpus.py          # -> /tmp/md-corpus-snapshot.json
# 2. Прогнать чанкер HEAD (policy-3) и рабочего дерева (policy-4) на одних входах.
cd /tmp/oldtree && PYTHONPATH=/tmp/oldtree \
  /home/roger/src/cohound/.venv/bin/python /tmp/chunk_snapshot.py gfm /tmp/old-gfm.json
cd /home/roger/src/cohound && \
  .venv/bin/python /tmp/chunk_snapshot.py gfm /tmp/new-gfm.json
# 3. Сравнить наборы кусков по всем полям.
```

Скрипты снимка — разовые (в репозиторий не входят); штатный инструмент инвариантов —
`scripts/measure_md_chunking.py`:

```bash
.venv/bin/python scripts/measure_md_chunking.py \
  --db ~/.cohound/data/runs/ru-pg-dbms-208472049479/cohound.db --out-json /tmp/md-after.json
```

Для `pandoc` 35 документов реально содержат новые конструкции: 1 929
документов дают **46 line block (815 строк) в 26 документах** и **9 fancy-списков
(25 пунктов) в 3 документах**; `unknown_lines=0`, `gaps=0` на всём снимке.

**Инварианты харнесса** (`scripts/measure_md_chunking.py`, профиль `gfm`, живой срез,
1 929 документов): P1/P4/P8/P13/P14 = **0**, `unknown_lines=0`. Но состав документов
между двумя прогонами харнесса **меняется** (при том же числе 1 929 документов сумма
символов была 21 648 914 и 20 935 871) — БД и `corpus_raw` пишет идущий краул,
`pgrep` подтверждает `hound.py run … --follow`. Поэтому равенство чанк-сета
доказывается не двумя прогонами харнесса, а замороженным снимком входов (выше):
харнесс подтверждает инварианты, снимок — отсутствие изменений.

## Зубы тестов (мутация)

| мутация | упало |
|---|---|
| `merged = blocks` (правило пустой строки отключено) | 4: `test_pandoc_list_does_not_interrupt_a_paragraph`, `test_pandoc_blockquote_does_not_interrupt_a_paragraph`, `test_pandoc_paragraph_keeps_absorbing_until_blank_line`, `test_new_pandoc_blocks_keep_p1_p4_p8_p13_on_a_mixed_document` |
| `line_blocks = merged` (line block отключён) | 4: `test_pandoc_line_block_is_one_block_of_lines`, `test_line_block_tail_becomes_a_paragraph`, `test_line_block_is_cut_by_lines_and_continuation_carries_marker`, смешанный документ |
| `return line_blocks` (fancy/example отключены) | 6: `test_pandoc_fancy_and_example_lists_are_lists`, `test_uppercase_single_letter_with_period_is_not_a_marker`, `test_fancy_lists_merge_across_a_blank_line_only_when_style_fits`, `test_fancy_list_item_is_a_unit_and_marker_stays_in_context`, `test_example_list_item_is_a_unit_and_label_stays_in_context`, смешанный документ |
| `context=None` у line block (чанкер) | 1: `test_line_block_is_cut_by_lines_and_continuation_carries_marker` |
| старый `_LIST_MARKER_RE` (чанкер) | 2: `test_fancy_list_item_is_a_unit_and_marker_stays_in_context`, `test_example_list_item_is_a_unit_and_label_stays_in_context` |

После каждой мутации файл возвращён побайтно (`diff -q` → идентичны).

## Версия и границы замера

- `chunker_version` — стек `md-ast-1:commonmark-0.31.2+gfm+pandoc/mdit-4.2.0/plugins-0.6.1/policy-4`
  (версия движка из метаданных пакета). `execution_change`: в профиле `pandoc` меняется
  набор блоков, то есть границы чанков и `prompt_sha256`; `commonmark`/`gfm` — без
  изменений, что и доказано выше. `run_id` нового прогона будет другим.
- **Проверяется**: последовательность видов блоков верхнего уровня (как в базовом
  замере); точка — инлайновая структура, атрибуты блоков, спаны и вложенность этим
  инструментом не проверяются (в JSON 2.17.1.1 нет `sourcepos`).
- **Не проверено корпусом**: fancy-списки в реальном корпусе — 9 списков / 25 пунктов в
  3 документах, и это `a)`/`b)` внутри текста LGPL (`license-third-party`); примеров на
  `(@label)` и `#.` в корпусе нет, они подтверждены только оракулом на сгенерированных
  формах. Line block'и на корпусе — 46 блоков / 815 строк в 26 документах, они проверены
  оракулом по типам, но не по спанам.
- **Корпусный прогон оракула — ориентир, не приёмка.** Тот же `types-only` по всем 1 929
  документам снимка: **11 857 → 11 479** расхождений (`mdast_only:list_without_blank_line`
  265 → 164, `mdast_only:blockquote_without_blank_line` 13 → 0, `mdast_only:fancy_list`
  26 → 10, `unparsed_construct:fancy_list` 7 → 0). Остаток
  доминируют классы вне этой полосы (`pandoc_only:paragraph` 5 633,
  `pandoc_only:heading` 1 252, `mdast_only:fence` 694 и т. п.) — извлечённый trafilatura
  markdown не является Pandoc-документом, и `SequenceMatcher` каскадирует сдвиг после
  каждого изменённого блока. Поэтому приёмка — дерево + сгенерированные формы, а не это
  число. С одной строкой гейта (`line_block`) из `unparsed_construct` уходят ещё 46
  блоков (11 479 → 11 433 арифметикой по посчитанному классу).
- **Живой срез**: БД `ru-pg-dbms-208472049479` пишется краулом; числа харнесса между
  двумя прогонами меняются по составу документов. Это ограничение замера, а не
  результата: снимок входов и сравнение на нём лежат в этой работе.