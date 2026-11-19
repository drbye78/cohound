"""Тесты прибора ``scripts/measure_query_lang.py`` (ось ``retrieval.query_lang``).

Прибор отвечает на вопрос «меняет ли объявленный язык запроса то, что находит ретривал», и
цена ошибки здесь — вердикт об оси измерения. Поэтому проверяются свойства, поломка которых
даёт **правдоподобное, но неверное** число, — каждое уже случалось или было почти допущено:

* **набор фраз проверки одинаков для всех настроек языка.** Первая версия прибора проверяла
  `ru` по русским фразам, а `all` — по русским и английским, и `all` выигрывал самим
  набором, а не выдачей (`ru` 97 / `en` 88 / `all` 114 из 181). Тест фиксирует, что
  ``_phrases`` не зависит от языка;
* **эталонные ячейки собираются по ячейке, а не по записи.** Голден идёт по запросам (три
  записи на ячейку с общими ожидаемыми id); если считать записи, знаменатель вырастает
  втрое и hit@K описывает не то, что видит оценщик;
* **неразрешённый адрес не зависит от измеряемой оси.** Без ``known_ids`` «не разрешён»
  значит «не пришёл в выдаче этой настройки», то есть число само меряет язык; с ним —
  «адреса нет в сборке» (O4). Тест проверяет оба чтения;
* **МакНемар считается по дискордантным парам** и даёт ``1.0`` при их отсутствии: иначе
  «значимость» появляется из согласованных ячеек, которые настройки не различают;
* **бутстрэп воспроизводим и парный.** Один сид — один интервал; интервал строится по
  разностям внутри пары, а не по двум независимым долям (иначе состав выборки смешивается
  с эффектом оси).

Живой прогон, ретривал и БД не нужны: проверяются чистые функции прибора.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

import measure_query_lang as measure  # noqa: E402

from core.domain.enums import Importance, ParamType  # noqa: E402
from core.domain.models import CatalogRow  # noqa: E402
from core.util.retrieval_eval import GoldenQuery  # noqa: E402


def _param(*, title: str = "Точка восстановления", terms: dict[str, list[str]] | None = None) -> CatalogRow:
    return CatalogRow(
        param_id="p1",
        title=title,
        module="m",
        group="g",
        importance=Importance.CRITICAL,
        param_type=ParamType.FACT,
        rule="Подтвердить наличие.",
        concept_terms=terms
        if terms is not None
        else {"ru": ["резервная копия"], "en": ["point-in-time recovery", "backup"]},
    )


# ---------------------------------------------------------------------------
# Набор фраз проверки
# ---------------------------------------------------------------------------


def test_phrase_set_does_not_depend_on_declared_language() -> None:
    """Фразы проверки — свойство параметра, а не настройки языка.

    Иначе сравнение меряет разницу наборов: ``all`` проверялся бы по большему числу фраз и
    выигрывал бы у ``ru`` без всякого отношения к выдаче. Замер первой версии прибора:
    ``ru`` 97 / ``en`` 88 / ``all`` 114 из 181 — на устранённом дефекте.
    """
    param = _param()
    phrases = measure._phrases(param)
    assert "точка восстановления" in phrases, "заголовок входит всегда"
    assert "резервная копия" in phrases
    assert "point-in-time recovery" in phrases
    assert "backup" in phrases
    # Список один и тот же объект по значению — сравнение с самим собой непусто.
    assert phrases == measure._phrases(param)


def test_phrase_set_drops_short_phrases() -> None:
    """Короткие фразы («тип», «SQL») есть в любом чанке — они превратили бы метрику в шум."""
    param = _param(title="Тип", terms={"ru": ["тип", "SQL"], "en": ["jsonb"]})
    phrases = measure._phrases(param)
    assert "тип" not in phrases
    assert "sql" not in phrases
    assert "jsonb" in phrases


def test_phrase_set_is_casefolded_and_deduplicated() -> None:
    """Фраза сравнивается с текстом чанка в нижнем регистре — регистр не должен решать."""
    param = _param(title="JSONB", terms={"en": ["JSONB", "jsonb"]})
    phrases = measure._phrases(param)
    assert phrases == ["jsonb"]


# ---------------------------------------------------------------------------
# Ячейки эталона
# ---------------------------------------------------------------------------


def _golden_file(tmp_path: Path) -> Path:
    """Голден в форме прибора: три записи на ячейку, общие ``expected_ids``."""
    entries = [
        {
            "cell": f"p/{param_id}",
            "product_id": "p",
            "param_id": param_id,
            "expected_ids": [f"{param_id}:c1", f"{param_id}:c2"],
            "query": query,
            "query_index": index,
        }
        for param_id in ("a", "b")
        for index, query in enumerate(("q0", "q1", "q2"))
    ]
    path = tmp_path / "golden.json"
    path.write_text(json.dumps(entries), encoding="utf-8")
    return path


def test_golden_cells_merge_per_cell_not_per_query(tmp_path: Path) -> None:
    """Шесть записей → две ячейки: иначе знаменатель вырастает втрое.

    Голден адресует **ячейку** (``product_id/param_id``), а записи в нём идут по запросам.
    Считать записи значило бы публиковать hit@K по «ячейкам», которых не существует.
    """
    cells = measure._golden_cells(_golden_file(tmp_path))
    assert set(cells) == {"p/a", "p/b"}
    product_id, param_id, expected = cells["p/a"]
    assert (product_id, param_id) == ("p", "a")
    assert expected == frozenset({"a:c1", "a:c2"})


def test_golden_cells_skip_records_without_address(tmp_path: Path) -> None:
    """Запись без адреса или без ожидаемых id — не ячейка, а мусор: она не входит молча."""
    path = tmp_path / "golden.json"
    path.write_text(
        json.dumps(
            [
                {"cell": "p/a", "product_id": "p", "param_id": "a", "expected_ids": []},
                {"cell": "", "product_id": "p", "param_id": "a", "expected_ids": ["x"]},
                {
                    "cell": "p/b",
                    "product_id": "p",
                    "param_id": "b",
                    "expected_ids": ["b:c1"],
                },
            ]
        ),
        encoding="utf-8",
    )
    assert set(measure._golden_cells(path)) == {"p/b"}


# ---------------------------------------------------------------------------
# Чтение «неразрешённых» адресов
# ---------------------------------------------------------------------------


def test_known_ids_separate_missing_address_from_missing_hit() -> None:
    """Без ``known_ids`` число «не разрешён» меряет **выдачу**, а не эталон.

    Один и тот же голден: адрес существует в сборке, но настройка языка его не нашла.
    Без ``known_ids`` это читается как «не разрешён» (10 при ``en`` против 14 при ``all``
    в живом замере) — то есть число само зависит от измеряемой оси и об эталоне не говорит
    ничего. С ``known_ids`` — 0: адрес в сборке есть.
    """
    queries = [GoldenQuery(query="cell", expected_ids=frozenset({"c1"}), lang="en")]
    ranked = [["c2", "c3"]]
    without = measure._evaluate_lang(queries, ranked, 10)
    with_known = measure._evaluate_lang(queries, ranked, 10, known_ids={"c1"})
    assert without.unresolved_expected_ids == 1
    assert with_known.unresolved_expected_ids == 0
    # Метрики при этом одинаковы: «не нашли» и «нет в сборке» — разные вопросы.
    assert without.hit_at_k == with_known.hit_at_k == 0.0


def test_ranker_uses_each_cell_own_ranking() -> None:
    """Ранжировщик отдаёт выдачу **своей** ячейки: перепутанный порядок даёт ложный hit.

    ``evaluate_retrieval`` вызывает ранжировщик один раз на запись, и адреса ячеек не
    уникальны по тексту — поэтому выдача сопоставляется по счёту вызовов. Перестановка
    дала бы hit там, где поиск промахнулся.
    """
    queries = [
        GoldenQuery(query="cell-a", expected_ids=frozenset({"a1"}), lang="ru"),
        GoldenQuery(query="cell-b", expected_ids=frozenset({"b1"}), lang="ru"),
    ]
    ranked = [["a1"], ["zzz"]]
    report = measure._evaluate_lang(queries, ranked, 10)
    assert report.per_query_rank == [1, None]
    assert report.hit_at_k == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# Статистика
# ---------------------------------------------------------------------------


def test_mcnemar_uses_discordant_pairs_only() -> None:
    """Согласованные пары в критерий не входят: они не различают настройки."""
    assert measure.mcnemar_exact_p(0, 0) == 1.0
    # Пять пар «только A» и ни одной «только B» — это уже не монетка.
    assert measure.mcnemar_exact_p(5, 0) == pytest.approx(2 / 32)
    # Симметрия: критерий двусторонний.
    assert measure.mcnemar_exact_p(5, 0) == pytest.approx(measure.mcnemar_exact_p(0, 5))
    # 12/4 из живого замера: значимости нет, и это не ноль.
    assert measure.mcnemar_exact_p(12, 4) == pytest.approx(0.0768, abs=1e-4)


def test_bootstrap_is_paired_and_reproducible() -> None:
    """Один сид — один интервал; разности считаются внутри пары.

    Если сравнивать две независимые доли, интервал смешал бы эффект оси с составом
    выборки — ровно тот дефект, от которого парное сравнение избавляет (AGENTS §1.1).
    """
    a = [True] * 10 + [False] * 10
    b = [False] * 10 + [True] * 10
    delta, lo, hi = measure._bootstrap_delta(a, b)
    assert delta == pytest.approx(0.0), "равные доли — нулевая разница"
    assert lo <= delta <= hi
    assert (lo, hi) == measure._bootstrap_delta(a, b)[1:], "сид фиксирован"

    # Явный сдвиг: A попадает там, где B промахнулся, — интервал не должен накрывать ноль
    # целиком при таком перевесе. Верхняя граница выше точечной оценки законно: при
    # пере-выборке из 35 ячеек может не попасть ни одна из 5 промахов A.
    c = [True] * 30 + [False] * 5
    d = [False] * 30 + [False] * 5
    delta2, lo2, hi2 = measure._bootstrap_delta(c, d)
    assert delta2 == pytest.approx(30 / 35)
    assert lo2 > 0.5
    assert hi2 <= 1.0


def test_bootstrap_rejects_mismatched_samples() -> None:
    """Разная длина — не «примерно то же»: парность нарушена, и это отказ."""
    with pytest.raises(ValueError):
        measure._bootstrap_delta([True, False], [True])
    with pytest.raises(ValueError):
        measure._bootstrap_delta([], [])
