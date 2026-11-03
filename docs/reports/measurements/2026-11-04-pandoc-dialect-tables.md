# Pandoc: таблицы, raw TeX и нативный `Div` (вторая половина L3)

**Дата:** 04.11.2026. **Класс:** `execution_change` — правила блочного разбора профиля
`pandoc` меняют набор блоков, а значит и границы чанков (`chunker_version`
`policy-4` → **`policy-5`**). Методология не меняется. **Срез:** рабочее дерево поверх
`91e3748` (правки первой половины L3 не закоммичены); базовый замер — `91e3748` и
`2026-11-04-pandoc-dialect-blocks.md`. **Инструмент:**
`scripts/conformance_pandoc.py` (режим `types-only`). **Оракул:** `/usr/bin/pandoc`
2.17.1.1 — только на хосте замера/в CI.

## Что сделано

Вторая половина L3 (`docs/architecture/18-markdown-chunker-plan.md` §L3) — три класса
конструкций, которые Pandoc markdown разбирает, а CommonMark/GFM и `markdown-it-py` нет.

| класс | правило | где |
|---|---|---|
| grid / multiline / simple таблицы | блок `table` с реальными спанами `table_row` и флагом `header`; шапка и выравнивание — в `info["header_pipe"]` (pipe-вид, для контекста) | `core/adapters/md_dialect/tables.py` |
| подпись таблицы `Table: …` / `: …` | входит в **спан** таблицы (`info["caption"]`, `caption_text`), отдельным блоком не остаётся | там же (`attach_captions`) |
| pipe-таблица: граница Pandoc | таблица кончается **перед** первой строкой без `\|`, остаток переразбирается | там же (`trim_pipe_tables`) |
| raw TeX | блок `raw` с `raw_format="tex"` и `marker` для `context_prefix` | `core/adapters/md_dialect/raw_tex.py` |
| нативный HTML `Div` | блок `container` со спаном на весь элемент и детьми из внутреннего среза | `core/adapters/md_dialect/html_div.py` |

Порядок объявлен списком `RULES` (`core/adapters/md_dialect/__init__.py`):
`blank_lines` → `line_block` → `fancy_list` → **`tables` → `raw_tex` → `html_div`**.
Правила применяются **только** к профилю `pandoc`; в `commonmark`/`gfm` они не
исполняются (пин `test_gfm_and_commonmark_do_not_see_pandoc_tables` и снимок чанк-сета
ниже). Чанкер (`core/adapters/chunker_markdown.py`) получил `_pandoc_table_cuts`: строка
Pandoc-таблицы — минимальная единица, `context_prefix` продолжения = подпись + pipe-шапка;
у `raw` продолжение несёт маркер (`\begin{env}` / `\command`).

### Почему именно так — поведение оракула

Каждое решение взято из `pandoc -f markdown -t json`, а не из догадки:

- **Выравнивание grid** задаётся двоеточиями **в разделителе шапки** (`+:==+==:+`);
  двоеточия верхней рамки оракул игнорирует. Ячейка может занимать несколько физических
  строк; таблица, отступленная на 4+ пробела, — абзац.
- **Multiline**: верхняя линия дефисов, 1+ строк шапки, разделитель из групп дефисов,
  тело через пустую строку, нижняя линия дефисов. **Simple**: ровно одна строка шапки,
  разделитель, по строке на ряд, конец — первая пустая строка.
- **Подпись**: `Table: …` **перед** таблицей требует пустой строки между подписью и
  таблицей (без неё — один `Para`); `Table:`/`:` **после** пустой строки не требует.
  Оракул отдаёт подпись внутри `Table`, а не отдельным блоком — поэтому она в спане.
- **Pipe-таблица (GFM multiline cell)**: `| 1 | 2 |` + строка без `|` + `| 3 | 4 |` →
  `Table` + один `Para`. GFM продолжает ячейку, Pandoc — нет. Без этого правила на дереве
  оставалось ровно одно расхождение (`pandoc_only:paragraph` в
  `docs/reports/measurements/2026-11-03-md-chunk-geometry-tokens.md`).
- **Raw TeX**: параграф, начинающийся с `\` + буквенной команды → `RawBlock tex`;
  `\alpha beta` (хвост на той же строке) — `Para`; `\alpha\nbeta` — `RawBlock` + `Para`;
  экранированная пунктуация (`\%`, `\$`, `\\`) — не команда; математические окружения
  (`align`, `equation`, `gather`, `multline`, `displaymath`, `math`, `eqnarray`, `split`)
  оракул raw-блоком **не** считает. Исключение — множество `MATH_ENVIRONMENTS`.
- **`Div`**: `<div …>` + пустая строка + текст + пустая строка + `</div>` — один `Div` с
  абзацем внутри, а не три блока верхнего уровня.

### Дефект, найденный замером, и его цена

Первый прогон снимка (1 929 документов) дал **одну** документную разницу против
`policy-4`-подобного поведения: `https://postgrespro.ru/docs/enterprise/16/citus?lang=en`
(377 → 377 чанков, сдвиг одного куска). Причина — **ложная simple-таблица** в блоке,
который `markdown-it` не увидел как забор: строка шапки начинается с `-`, то есть это
тематический разрыв/дамп, а не таблица. Добавлена проверка первой значимой строки
(`tables.py:281`); после неё набор кусков на всём снимке **совпал побайтно** (ниже).
Это же правило убирает и вырожденные «таблицы» из консольного вывода.

### Границы реализации (объявленные, не забытые)

- Таблицы, raw TeX и `Div` распознаются только на верхнем уровне: внутри `blockquote`
  (`quoted=True`) правило возвращает блоки без изменений — там пришлось бы снимать
  префикс `> ` построчно и пересчитывать смещения.
- Одноколоночные dash-таблицы не распознаются: разделитель неотличим от тематического
  разрыва, отдельного синтаксиса оракул для них не даёт.
- `Div`: вложенность глубже одного уровня не обещается — закрывающий тег ищется как
  первый `</div>`. Атрибуты не интерпретируются. Если внутренний разбор даёт разрывы
  (`gaps`), контейнер не строится и остаётся raw-представление — видимая потеря, а не
  тихая.
- Raw TeX: реализован только случай «после конструкции остаток строки пуст»; хвост на той
  же строке (`\begin{quote}x\end{quote} trailing` → `RawBlock` + `Para`) объявлен
  границей, иначе правило резало бы абзац посередине строки. Ячейка таблицы разбирается
  как текст (инлайн-структура внутри неё не проверяется).
- Подпись «до» таблицы приклеивается только к таблице, распознанной этим же проходом:
  pipe-таблица с `Table:`-абзацем до неё получает подпись только через `attach_captions`
  (он работает с любой `table`), но пустая строка между ними обязательна.

## Числа до/после — `scripts/conformance_pandoc.py`

Команда (дословно):

```bash
.venv/bin/python scripts/conformance_pandoc.py --out-json /tmp/pandoc-after.json
```

Код возврата — **0**. 59 документов (40 отслеживаемых git файлов дерева + 19
сгенерированных форм), блоков Pandoc — **2 387**; **расхождений блочных видов: 0**.
Снято со сравнения (не «совпало»): `footnote_inline` 2 блока / 1 док., `metadata` 1 / 1.

| класс | до (правила выключены) | после | что осталось |
|---|---:|---:|---|
| `unparsed_construct` — всего | **6 блоков / 6 док.** | **0** | — |
| `unparsed_construct:grid_table` | 1 | **0** | — |
| `unparsed_construct:multiline_table` | 1 | **0** | — |
| `unparsed_construct:table` (simple) | 1 | **0** | — |
| `unparsed_construct:table_caption` | 1 | **0** | — |
| `unparsed_construct:raw_tex` | 1 | **0** | — |
| `unparsed_construct:html_div` | 1 | **0** | — |
| `sequence_length` — всего | **11 блоков / 4 док.** | **0** | — |
| `type_mismatch` | 0 | **0** | — |
| `oracle_error` | 0 | **0** | — |

Колонка «до» получена прогоном того же инструмента с выключенными правилами этой полосы
(пять функций `md_dialect` заменены тождественными) в одном процессе — не сравнением
разных прогонов. Дерево (40 документов) даёт **0** расхождений и до, и после; все 6
`unparsed_construct` и 11 `sequence_length` — сгенерированные формы. Один
`sequence_length` (`pandoc_only:paragraph`) в отчёте
`2026-11-03-md-chunk-geometry-tokens.md` — это и есть GFM multiline cell: он исчезает с
`trim_pipe_tables`, поэтому «10 → 0» из постановки и «11 → 0» здесь — одно и то же
явление, посчитанное с разным составом выборки.

### Правка гейта: одна строка

`scripts/conformance_pandoc.py` — не цель правки, но без отображения нашего вида
`raw` в канонический класс `raw` разобранный raw TeX навсегда остаётся
`unparsed_construct`. Добавлена строка `"raw": "raw"` в `_MDAST_KINDS` (аналогично
прежней `"line_block": "line_block"`). Больше в гейте ничего не менялось.

## Числа до/после — `scripts/conformance_md.py` (commonmark/gfm не сдвинулись)

```bash
.venv/bin/python scripts/conformance_md.py
```

| профиль | сопоставимо | совпало | расхождения | P13 | P1 |
|---|---:|---:|---:|---:|---:|
| CommonMark 0.31.2 | 635 | **635** | **0** | **652/652** | **652/652** |
| GFM | 653 | **653** | **0** | **670/670** | **670/670** |

Числа совпадают с документированными в §7 плана — приёмка «`commonmark`/`gfm`
байт-в-байт».

### Чанк-сет на корпусе: 0 различий в обоих профилях

Живой срез (`ru-pg-dbms-208472049479`) пишется идущим краулом, поэтому сравнение идёт по
**замороженному снимку** входов (1 929 документов, 21 447 619 символов): чанкер с
правилами (`policy-5`) и он же с выключенными правилами этой полосы (`policy-4`-подобный)
прогоняются на одних и тех же текстах.

| профиль | документов | чанков (old → new) | документов с отличиями |
|---|---:|---:|---:|
| `gfm` | 1 929 | 23 666 → 23 666 | **0** |
| `pandoc` | 1 929 | 23 648 → 23 648 | **0** |

Сравнение — по полному набору полей куска (`text`, `heading_path`, `context_prefix`,
`char_start`, `char_end`). Для `gfm` это ожидаемо (правила не исполняются), для
`pandoc` — **измеренный факт, а не совпадение**: на пилотном корпусе новые конструкции
почти отсутствуют, и там, где они есть, границы резов не меняются.

Перепись конструкций на снимке (профиль `pandoc`): `table:pipe` **1 216**,
`table:simple` **1**, `raw:tex` **1**, grid/multiline/`Div` — **0**; `gaps` и
`unknown_lines` — **0**. Единственная simple-таблица — та самая, что лежит в
`pangolin.sbertech.ru/…/copywala-create-backup`: оракул на этом фрагменте тоже отдаёт
`Table`, то есть правило совпало с оракулом, а не «нашло таблицу в списке». Единственный
raw — `\password` в `edu.postgrespro.ru/pgpro-13/pgpro_09_profile.html`; оракул на нём
отдаёт `RawBlock`. Ни один из двух не меняет границу куска, поэтому снимок не сдвинулся.

**Следствие для версии.** `chunker_version` меняется (`policy-5`) не потому, что на этом
корпусе сдвинулись куски, а потому что **объявлено другое поведение разбора**: на целевом
масштабе (AGENTS §1.1: CommonMark + GFM + Pandoc, ≥100 профилей) Pandoc-документы дают
grid/multiline/Div, которых в пилоте нет. Это `execution_change`, новый `run_id`;
`run_id` от смены версии меняется и там, где набор кусков совпал, — иначе два разных
правила разбора носили бы одно имя.

**Инварианты харнесса** (`scripts/measure_md_chunking.py`, тот же снимок):

| профиль | чанков | P1 | P4 | P8 | P13 (`unknown_lines`) | P14 | без спанов | tiny | покрытие |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `gfm` | 23 666 | 0 | 0 | 0 | 0 | 0 | 0 | 270 | 100.00000 % |
| `pandoc` | 23 648 | 0 | 0 | 0 | 0 | 0 | 0 | 266 | 100.00000 % |

`source_punct` (чанк начинается с закрывающей пунктуации **из источника**, не из-за реза)
— 10 (`gfm`) и 12 (`pandoc`); метрика отделяет причину, а не прячет её.
Сгенерированные формы (19 документов) на профиле `pandoc`: P1/P4/P8/P13/P14 = 0,
`unknown_lines=0`, `without_span=0`, покрытие **100.00000 %**.

## Зубы тестов (мутация)

Каждая новая ветка выключена и проверено, что падают именно её тесты; файл возвращён
побайтно (`diff -q`).

| мутация | упало |
|---|---|
| `tables = recognize_tables(...)` → `tables = fancy` | 3: `test_pandoc_grid_table_has_real_rows_and_pipe_header`, `test_pandoc_multiline_and_simple_tables_are_tables_with_captions`, `test_grid_table_is_cut_between_rows_with_pipe_header_in_context` |
| `captioned = attach_captions(...)` → `captioned = trimmed` | 2: `test_pandoc_multiline_and_simple_tables_are_tables_with_captions`, `test_table_caption_stays_attached_and_reaches_the_context` |
| `trimmed = trim_pipe_tables(...)` → `trimmed = tables` | 1: `test_pandoc_pipe_table_stops_where_pandoc_stops` |
| `raw = split_raw_tex(...)` → `raw = captioned` | 2: `test_pandoc_raw_tex_block_and_its_measured_boundaries`, `test_raw_tex_is_cut_by_lines_and_continuation_carries_marker` |
| `return split_html_divs(...)` → `return raw` | 2: `test_pandoc_native_div_is_a_container_with_parsed_children`, `test_native_div_continuation_carries_opening_tag` |
| чанкер: ветка `_pandoc_table_cuts` удалена | 2: `test_grid_table_is_cut_between_rows_with_pipe_header_in_context`, `test_table_caption_stays_attached_and_reaches_the_context` |

## Проверки

```bash
.venv/bin/ruff check core/adapters/md_dialect/ core/adapters/chunker_markdown.py \
  tests/adapters/test_md_ast.py tests/adapters/test_chunker_markdown.py \
  scripts/conformance_pandoc.py                       # All checks passed
.venv/bin/python scripts/mypy_gate.py                 # 112/112, новых нет
.venv/bin/python -m pytest -q tests/adapters/test_md_ast.py \
  tests/adapters/test_chunker_markdown.py tests/adapters/test_md_properties.py \
  tests/adapters/test_conformance_pandoc.py           # 139 passed
.venv/bin/python tests/selfcheck.py                   # 112/112
.venv/bin/python scripts/check_doc_anchors.py --strict  # проблем: 0
```

Тесты-пины оракула, ожидавшие `grid_table` как неразобранную конструкцию, переписаны на
новое поведение (`test_grid_table_matches_after_dialect_rules`,
`test_end_to_end_grid_table_exits_zero`); принцип «`unparsed` никогда не считается
совпадением» сохранён и проверяется на `BlockView`, а не на конкретной конструкции.

## Остаток и что не проверено

- **Вложенный `div`** закрывается первым `</div>`; два уровня не поддержаны и не
  обещаны. **Атрибуты** тега в структуру не входят.
- **Raw TeX с хвостом на той же строке** (`\begin{quote}x\end{quote} trailing`) —
  объявленная граница: правило не режет абзац посередине строки.
- **Одноколоночные dash-таблицы** не распознаются (неотличимы от тематического разрыва).
- **Выборка дерева** — 40 из 72 отслеживаемых markdown-файлов (равномерный шаг); числа
  дерева относятся к этой выборке, а не ко всему репозиторию. Число файлов выросло с 71
  до 72 этим отчётом. Полный прогон по всему дереву (`--max-docs 1000`, 04.11.2026):
  **91 документ** (72 отслеживаемых + 19 сгенерированных), **4 709** блоков Pandoc,
  расхождений **0**, код возврата 0 — то есть нулевое расхождение не является свойством
  выборки.
- **Корпусный прогон оракула** по всем 1 929 документам снимка — ориентир, не приёмка:
  извлечённый `trafilatura` markdown не является Pandoc-документом, и `SequenceMatcher`
  каскадирует сдвиг после каждого изменённого блока (тот же вывод, что в отчёте первой
  половины). Приёмка — дерево + сгенерированные формы.
- **Не проверено корпусом**: grid/multiline-таблицы и нативный `Div` в пилотном корпусе
  отсутствуют (0); они подтверждены оракулом на сгенерированных формах. Это состояние
  пилота, а не цель (AGENTS §1.1).