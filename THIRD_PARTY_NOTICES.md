# Сторонние компоненты (THIRD_PARTY_NOTICES)

coHound использует оригинальный код, а также фрагменты, **скопированные** (не как
зависимости) из внешних проектов. Перенос выполнен по принципу «копировать, не
связываться зависимостью» — исходный текст при необходимости адаптирован под
категорийную нейтральность ядра (§1.3 регламента) и требования coHound.

---

## gag5 — Engineering Intelligence System

- **Источник:** локальный проект `../gag5`.
- **Лицензия:** MIT License, Copyright (c) 2024.
- **Скопировано и адаптировано:**
  - `core/util/text_utils.py` ← `gag5/core/text_utils.py` (детект RU/EN, нормализация,
    ё→е, разбиение предложений, стоп-слова).
  - `core/util/security.py` ← `gag5/core/security.py` (SSRF-защита URL, санитизация
    имён; функция под графовую БД удалена, добавлен `sanitize_slug`).
  - `core/util/fusion.py` ← `gag5/retrieval/fusion.py` (RRF и др. методы слияния;
    доменные веса убраны, ключ дедупа параметризован под `chunk_id`).
- **MIT License (текст):**

  ```
  MIT License

  Copyright (c) 2024

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in all
  copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.
  ```

---

## gtm — GTM / competitive-intelligence хаб

- **Источник:** локальный проект `../gtm`.
- **Статус:** **авторская разработка владельца coHound** — переиспользование разрешено.
  Первоначально код/данные не копировались из-за отсутствия файла лицензии; после
  подтверждения авторства ограничение снято. Лицензия в репозитории gtm отсутствует,
  поэтому копируемый материал помечается здесь как «собственная разработка владельца».
- **Скопировано и адаптировано:**
  - `core/util/text_utils.py` — добавлены `normalize_ru_token` / `tokenize_lexical`
    (лёгкая RU-нормализация и токенизация) ← `gtm/scripts/kb_poc.py`.
  - `core/adapters/lexical_bm25.py` ← логика BM25 из `gtm/scripts/kb_poc.py`
    (`BM25Backend`): переписан как in-process ``LexicalIndexPort`` с продукт-скоупед
    фильтрацией (I17) и делегированием токенизации в `core.util.text_utils`.
  - `core/util/retrieval_eval.py` ← eval-метрики (hit@1/3/5, MRR) из `gtm/scripts/kb_poc.py`;
    обобщено до hit@K/recall@K/MRR с cut-off K из конфигурации (I20).
  - `docs/skills/discovery-research.md` ← методики `gtm/skills/{competitor-research,
    regulatory-watch,oss-solution-scout}/SKILL.md`; доменная специфика удалена,
    операционные приёмы разведки сохранены (нейтрально).
- **Что НЕ переносилось:** 35 агентов и большинство из 31 скилла gtm — это GTM-маркетинг
  (demand-gen, pricing, PR, brand, launch, ABM и т.п.), не относящийся к coHound.
  Готовых LLM-промптов оценщика в gtm нет: «агенты» — это opencode-роли (markdown), а не
  промпты для §7-оценщика; они всё равно проектируются с нуля.
- **Учтено как методический ориентир** (соответствующие правила уже в регламенте как
  I2/I2b и absence-методология §4.2): правило «нет — только при публичном свидетельстве
  явного отсутствия, иначе н/д»; «отсутствие данных ≠ функции нет»; разделение
  «Подтверждено / Оценка / Пробел»; блокирующий клейм-гард и accounting воронки.

