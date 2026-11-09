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

**Асимметрия: объявленная политика против recovery-кнуба.** `EVALUATION_MAX_RETRIES` —
условие измерения: оно входит в `run_id` через поле контракта `evaluation_policy`, и фаза
Ш6 исполняет именно его. У команды `hound retry` есть `--max-retries`: это **ремонт уже
объявленного прогона** (`run_id` не меняется), поэтому флаг не переписывает контракт, а
публикуется в провенансе (`logs/retry.jsonl`, сводка `retry`). Значение флага не
подменяется ни потолком, ни объявленным числом: расхождение видно, а не спрятано.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

__all__ = [
    "EVALUATION_MAX_RETRIES",
    "EVALUATION_MIN_RETRIEVAL_SCORE",
    "declared_max_retries",
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


def declared_max_retries(policy: Mapping[str, Any] | None = None) -> int:
    """Объявленное число повторов из поля контракта ``evaluation_policy``.

    ``None`` (ось не передана: модульный вызов) и отсутствие ключа дают дефолт
    :data:`EVALUATION_MAX_RETRIES`. Явно объявленное значение исполняется **как есть**:
    тихого потолка нет, а нечисло/отрицательное — отказ (``ValueError``), а не clamp.
    Политика, которую нельзя исполнить, обязана быть видна до первого вызова модели.

    Это единственная точка чтения числа: фаза (:func:`core.pipeline.evaluate.evaluate_phase`),
    идентичность (``evaluation_policy_spec``) и проверка значения обязаны читать одну
    величину, иначе исполнение и ``run_id`` разъедутся молча.
    """
    if policy is None:
        return EVALUATION_MAX_RETRIES
    value = policy.get("max_retries", EVALUATION_MAX_RETRIES)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"evaluation_policy.max_retries обязан быть целым, получено {value!r}"
        )
    if value < 0:
        raise ValueError(
            f"evaluation_policy.max_retries не может быть отрицательным, получено {value!r}"
        )
    return value


def evaluation_policy_spec() -> dict[str, Any]:
    """Объявленная политика повтора — значение поля контракта ``evaluation_policy``.

    Ключ ``min_retrieval_score`` появляется **только** когда порог объявлен. При ``None``
    словарь остаётся ровно ``{"max_retries": 2}``: пустой ключ с ``null`` поехал бы в
    ``execution_contract_digest`` и сменил ``run_id`` у всех прогонов, ничего не измерив.

    ``max_retries`` читается через :func:`declared_max_retries` — тем же помощником, что и
    фаза исполнения. Иначе докстринг помощника («единственная точка чтения числа») был бы
    ложью: сегодня обе точки читают одну константу, но при появлении объявленного
    значения разъехались бы молча.
    """
    spec: dict[str, Any] = {"max_retries": declared_max_retries(None)}
    if EVALUATION_MIN_RETRIEVAL_SCORE is not None:
        spec["min_retrieval_score"] = EVALUATION_MIN_RETRIEVAL_SCORE
    return spec
    if EVALUATION_MIN_RETRIEVAL_SCORE is not None:
        spec["min_retrieval_score"] = EVALUATION_MIN_RETRIEVAL_SCORE
    return spec
