"""Тесты прибора распределения токенов (`scripts/measure_chunk_token_limit.py`, O21 плана 26).

Прибор публикует геометрию корпуса в токенах эмбеддера. Ломается он тихо: числа остаются
правдоподобными при неверной метке семейства, при перепутанном разрезе по доле CJK и при
«нуле» вместо «не измерено». Поэтому проверяются именно эти три места, а не
токенизатор (он требует весов и в тестах не поднимается).

* **семейство — это чанкер, а не читатель.** ``.md`` читает ``text-plain-2`` (тот же
  читатель, что у ``.txt``), а режет его markdown-чанкер: замер 22.11.2026 — все 5 555
  документов ZH-корпуса попали в «text» при markdown-чанкере. Поэтому метка берётся по
  адресу, а версия читателя — только запасной сигнал.
* **разрез по доле CJK — представление, а не порог.** Границы включительны справа, ноль
  отделён: «ни одного иероглифа» и «иероглифов мало» — разные факты.
* **пустой разрез — ``None``, а не 0.0.** Ноль читался бы как «измерено и равно нулю».
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]


def _load_module():
    """Модуль прибора по пути: `scripts/` не пакет."""
    path = _ROOT / "scripts" / "measure_chunk_token_limit.py"
    spec = importlib.util.spec_from_file_location("measure_chunk_token_limit", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["measure_chunk_token_limit"] = module
    spec.loader.exec_module(module)
    return module


measure = _load_module()


# --------------------------------------------------------------------------- #
# Семейство: чанкер, а не читатель
# --------------------------------------------------------------------------- #


def test_markdown_suffix_beats_the_reader_version() -> None:
    """``.md`` → markdown, хотя читатель у него ``text-plain-2`` (как у ``.txt``)."""
    assert measure._family("https://example.test/guide.md", "text-plain-2") == "markdown"


def test_plain_text_suffix_is_the_text_chunker() -> None:
    """``.txt`` → абзацный чанкер: там бюджет в символах означает другое."""
    assert measure._family("https://example.test/notes.txt", "text-plain-2") == "text"


def test_suffixless_document_falls_back_to_the_reader_version() -> None:
    """Адрес молчит (HTML без суффикса) — тогда и только тогда решает версия читателя."""
    assert measure._family("https://example.test/page", "trafilatura-html-8") == "markdown"
    assert measure._family("https://example.test/page", "text-plain-2") == "text"


# --------------------------------------------------------------------------- #
# Доля CJK и разрезы
# --------------------------------------------------------------------------- #


def test_cjk_share_counts_characters_not_letters() -> None:
    """Знаменатель — все символы чанка: разметка и пробелы тоже занимают место."""
    assert measure.cjk_share("") == 0.0
    assert measure.cjk_share("plain english") == 0.0
    assert measure.cjk_share("中文") == 1.0
    assert measure.cjk_share("中文ab") == 0.5


def test_script_bucket_boundaries_are_inclusive_on_the_right() -> None:
    """Границы разрезов включительны справа; ноль отделён от «иероглифов мало»."""
    assert measure.script_bucket(0.0) == "0"
    assert measure.script_bucket(0.0001) == "(0,0.1]"
    assert measure.script_bucket(0.1) == "(0,0.1]"
    assert measure.script_bucket(0.11) == "(0.1,0.5]"
    assert measure.script_bucket(0.5) == "(0.1,0.5]"
    assert measure.script_bucket(0.51) == "(0.5,0.9]"
    assert measure.script_bucket(0.9) == "(0.5,0.9]"
    assert measure.script_bucket(1.0) == "(0.9,1]"


# --------------------------------------------------------------------------- #
# Отчёт: порядок разрезов и «не измерено» вместо нуля
# --------------------------------------------------------------------------- #


def _distribution(measure_module, *, tokens: int, chars: int, limit: int = 512):
    distribution = measure_module.Distribution()
    distribution.add(tokens, chars, limit)
    return distribution


def test_report_lists_script_buckets_in_declared_order_and_keeps_empty_as_none() -> None:
    """Разрезы публикуются в объявленном порядке; пустой — ``None``, а не ``0.0``."""
    bucket = "(0.5,0.9]"
    script_before = {bucket: _distribution(measure, tokens=300, chars=500)}
    script_after = {bucket: _distribution(measure, tokens=300, chars=500)}

    payload = measure._report(
        {},
        {},
        script_before,
        script_after,
        limit=512,
        report={},
        invariant={},
        cost={},
        model="BAAI/bge-m3",
        declared=False,
    )

    assert list(payload["scripts"]) == ["0", "(0,0.1]", "(0.1,0.5]", "(0.5,0.9]", "(0.9,1]"]
    filled = payload["scripts"][bucket]["before"]
    assert filled["chunks"] == 1
    assert filled["tokens_per_char"] == 0.6
    empty = payload["scripts"]["0"]["before"]
    assert empty["chunks"] == 0
    assert empty["tokens_per_char"] is None


def test_report_does_not_claim_a_slice_invariant_it_cannot_check() -> None:
    """Инвариант «кусок — точный срез» в этом приборе не проверяется и не публикуется.

    Из чанков документ не восстановить, если между кусками есть пропуски; прежняя
    проверка срезом от конкатенации дала 5 243 ложных нарушения на ZH-корпусе.
    """
    payload = measure._report(
        {},
        {},
        {},
        {},
        limit=512,
        report={},
        invariant={"p1_violations": 0},
        cost={},
        model="BAAI/bge-m3",
        declared=False,
    )

    assert payload["invariants"] == {"p1_violations": 0}
