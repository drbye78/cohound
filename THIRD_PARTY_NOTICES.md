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
- **Статус:** **код и данные НЕ копировались.** Причина: отсутствует файл лицензии,
  данные содержат внутренние/заказчицкие материалы; юридический статус неопределён.
- **Что учитывалось как методический ориентир** (проверенный, но не скопированный;
  соответствующие правила уже зафиксированы в регламенте coHound как I2/I2b и
  отсутствие-методология §4.2):
  - правило отсутствия «нет — только при публичном свидетельстве явного отсутствия,
    иначе н/д» (ср. coHound I2);
  - «отсутствие данных ≠ функции нет» (ср. coHound I2b / `unknown`);
  - разделение «Подтверждено / Оценка / Пробел» (ср. статусы + Coverage);
  - блокирующий клейм-гард и accounting воронки (идеи для валидации/CI).

  Никакой текст/данные gtm в репозиторий не переносились.
