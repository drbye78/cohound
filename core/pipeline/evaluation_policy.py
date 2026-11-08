"""Политика повтора оценщика — объявленное условие измерения (V3).

Модуль существует потому, что повтор **переиздаёт тот же запрос** (`evaluate_cell`), и
значит вердикт ячейки зависит от истории сэмплов, а не только от промпта. При
недетерминированном провайдере (замер: 4.2 % ячеек — 5 из 119 — меняют статус при
идентичном `prompt_hash`, `docs/reports/measurements/2026-10-26-verdict-reproducibility.md`)
«первый валидный ответ» — это лотерея, и её размер обязан быть виден в контракте.

**Почему отдельный модуль.** Число живёт здесь, а не в `evaluate.py`, по одной причине:
`core.pipeline.digests`/`run_identity` обязаны читать его, не импортируя фазу оценки
целиком (это тяжёлый модуль с pydantic-схемами и LLM-портами). Одна константа на две
роли — провод и провенанс — иначе они разъедутся молча.

Класс изменения: `execution_versions` (§13). Смена числа меняет вердикты, значит и
`run_id`; методология (`METHODOLOGY_VERSION`) не меняется — правило «повторяем ограниченно»
не пересматривается, объявляется его величина.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "EVALUATION_MAX_RETRIES",
    "EVALUATION_MIN_RETRIEVAL_SCORE",
    "evaluation_policy_spec",
]

#: Сколько раз запрос переиздаётся после невалидного ответа (`max_retries`). Число
#: объявлено здесь, используется как дефолт `evaluate_cell` и попадает в контракт.
EVALUATION_MAX_RETRIES = 2

#: Порог retrieval-скора для аномалии ревью (план 15 §6.2 п.4, ``min_retrieval_score``).
#:
#: ``None`` — проверка **выключена**, потому что значения нет: оно не измерено. Порог,
#: взятый «из головы», объявлял бы аномалией то, что замер не назвал аномальным (A3).
#: Включение — объявленная смена политики (``execution_change``): число берётся из
#: замера распределения retrieval-скоров, а не назначается.
EVALUATION_MIN_RETRIEVAL_SCORE: float | None = None


def evaluation_policy_spec() -> dict[str, Any]:
    """Объявленная политика повтора — значение поля контракта ``evaluation_policy``.

    Ключ ``min_retrieval_score`` появляется **только** когда порог объявлен. При ``None``
    словарь остаётся ровно ``{"max_retries": 2}``: пустой ключ с ``null`` поехал бы в
    ``execution_contract_digest`` и сменил ``run_id`` у всех прогонов, ничего не измерив.
    """
    spec: dict[str, Any] = {"max_retries": EVALUATION_MAX_RETRIES}
    if EVALUATION_MIN_RETRIEVAL_SCORE is not None:
        spec["min_retrieval_score"] = EVALUATION_MIN_RETRIEVAL_SCORE
    return spec
