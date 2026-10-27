"""coHound — полоса неопределённости измерения вердиктов (P6c).

**Зачем отдельный артефакт.** Замер повтора показал, что повторная оценка того же промпта
(``temperature=0``, ``seed=0``) меняет вердикт части ячеек: у провайдера есть разброс,
которого детерминизм настроек не отменяет. Число было опубликовано **только** в файле
отчёта (``docs/reports/measurements/``) и не попадало ни в один артефакт прогона: читатель
карты видел точечные вердикты и не знал, что у них есть полоса. Более того, ранжирование
продуктов публиковалось как точное.

**Что здесь считается, а что объявляется.** Пайплайн повторной оценки **не делает**, поэтому
величина полосы по умолчанию — **объявленная** (с провенансом: путь замера + дата), и
артефакт обязан это говорить (``computed_from_this_run: false``). Выдавать объявленную
величину за измеренную в этом прогоне — ложный провенанс (A3). Если в каталоге прогона есть
измеренные пробы детерминизма (``benchmark/evaluator-*.json`` с ``repeats >= 2``), полоса
считается из них, и только тогда ставится ``computed_from_this_run: true``.

**Ограничение публикации.** Смена вердикта **критической** ячейки (``Importance.CRITICAL``)
внутри полосы запрещает **ранжирование** (порядок продуктов), но не отчёт: карта остаётся
картой, а «первый лучше второго» — утверждение, которое такая полоса не выдерживает. Признак
машинный (``ranking_restriction``), а не только текстом. Какие именно ячейки изменились,
пайплайн не знает, поэтому действует худший случай: если в измеренном множестве есть хоть
одна критическая ячейка, она могла попасть в полосу.

**Отсутствие объявления ≠ нулевая неопределённость.** ``share: 0.0`` — измеренная
устойчивость; отсутствие объявления — «неопределённость не измерена», и это разные записи
(``band.present``/``provenance.kind``). Ноль вместо «неизвестно» — ровно та подмена, ради
которой вводится этот файл.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.domain.enums import (
    CellState,
    Importance,
    RankingRestriction,
    UncertaintyProvenance,
)
from core.domain.models import CatalogRow, Cell

__all__ = [
    "DECLARED_VERDICT_STABILITY_BAND",
    "DECLARED_VERDICT_STABILITY_SOURCE",
    "MeasuredBand",
    "band_from_repeats",
    "build_measurement_uncertainty",
    "critical_cells_measured",
    "read_repeat_probes",
]

#: Замер, из которого взята объявленная полоса (провенанс: путь + дата в шапке файла).
DECLARED_VERDICT_STABILITY_SOURCE = (
    "docs/reports/measurements/2026-10-26-verdict-reproducibility.md"
)
#: Дата замера (шапка файла-источника).
DECLARED_VERDICT_STABILITY_DATE = "2026-10-26"


@dataclass(frozen=True)
class MeasuredBand:
    """Полоса неопределённости вердикта: доля ячеек, меняющих вердикт при повторе.

    :param share: доля изменённых ячеек (``cells_changed / cells_total``).
    :param cells_changed: сколько ячеек сменили вердикт.
    :param cells_total: сколько ячеек сравнивалось (база доли — не «сколько получилось»).
    :param source: адрес происхождения величины (файл замера либо артефакт прогона).
    :param measured_at: дата замера/артефакта.
    """

    share: float
    cells_changed: int
    cells_total: int
    source: str
    measured_at: str


#: Объявленная полоса: повтор R0 на 119 ячейках дал 5 расхождений (4.2 %) при идентичном
#: ``prompt_hash``, то есть это разброс провайдера, а не ретривала. Величина — **объявленная**
#: (A3: дефолт заменяется измерением, но публикуется с провенансом), поэтому артефакт прогона
#: несёт её с ``computed_from_this_run: false``, если только в прогоне нет своих повторов.
DECLARED_VERDICT_STABILITY_BAND: MeasuredBand | None = MeasuredBand(
    share=0.042,
    cells_changed=5,
    cells_total=119,
    source=DECLARED_VERDICT_STABILITY_SOURCE,
    measured_at=DECLARED_VERDICT_STABILITY_DATE,
)



def read_repeat_probes(run_dir: str | Path | None) -> list[dict[str, Any]]:
    """Измеренные пробы детерминизма из артефактов прогона (``benchmark/evaluator-*.json``).

    Источник — тот же каталог, куда пишет ``hound experiment evaluator-benchmark``. Проба
    считается измеренной только при ``determinism.measured`` (``repeats >= 2``): прогон с
    одним повтором не говорит о стабильности ничего.

    Порядок детерминирован: файлы сортируются по имени (в метке времени), модели — по метке
    и ``model_id``. Пустой список — проб нет (повторной оценки в прогоне не было).
    """
    if run_dir is None:
        return []
    directory = Path(run_dir) / "benchmark"
    if not directory.is_dir():
        return []
    probes: list[dict[str, Any]] = []
    for path in sorted(directory.glob("evaluator-*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            # Артефакт эксперимента — не измерение прогона: битый файл не роняет отчёт,
            # но и не выдаётся за пробу (молчаливая подмена — хуже пропуска).
            continue
        models = payload.get("models")
        if not isinstance(models, list):
            continue
        for model in models:
            if not isinstance(model, Mapping):
                continue
            determinism = model.get("determinism")
            if not isinstance(determinism, Mapping) or not determinism.get("measured"):
                continue
            changed = determinism.get("changed_cells")
            total = determinism.get("cells_total") or model.get("n_cells")
            if not isinstance(changed, int) or not isinstance(total, int) or total <= 0:
                continue
            share = determinism.get("fraction_changed")
            probes.append(
                {
                    "label": model.get("label"),
                    "changed_cells": changed,
                    "cells_total": total,
                    "share": float(share) if share is not None else changed / total,
                    "repeats": determinism.get("repeats"),
                    "source": str(path),
                }
            )
    probes.sort(key=lambda probe: (str(probe.get("source")), str(probe.get("label"))))
    return probes


def band_from_repeats(probes: Sequence[Mapping[str, Any]]) -> MeasuredBand | None:
    """Полоса из **измеренных** в этом прогоне проб: худший случай (максимальная доля).

    Худший случай, а не среднее: разброс провайдера — не выборка, из которой оценивается
    средняя неопределённость; для решения «можно ли ранжировать» нужна граница.
    """
    if not probes:
        return None
    worst = max(
        probes,
        key=lambda probe: (
            float(probe.get("share") or 0.0),
            int(probe.get("changed_cells") or 0),
        ),
    )
    return MeasuredBand(
        share=float(worst.get("share") or 0.0),
        cells_changed=int(worst.get("changed_cells") or 0),
        cells_total=int(worst.get("cells_total") or 0),
        source=str(worst.get("source") or ""),
        measured_at="",
    )


def critical_cells_measured(
    cells: Iterable[Cell], catalog: Mapping[str, CatalogRow]
) -> int:
    """Сколько **измеренных** ячеек приходится на параметры критической важности (Кр).

    «Измеренная» — та, что дала вердикт или провал инструмента, без ``preliminary`` (I13) и
    без ``stale`` (§13/K2: они описывают прошлую сборку). ``failed`` входит: смена
    «вердикт → сбой» — тоже смена вердикта (замер это и показал: два из пяти расхождений
    были ``❓ → failed``).
    """
    count = 0
    for cell in cells:
        row = catalog.get(str(cell.param_id))
        if row is None or row.importance is not Importance.CRITICAL:
            continue
        if getattr(cell, "stale_reason", None) is not None:
            continue
        if cell.state is CellState.PRELIMINARY:
            continue
        if cell.status is None and cell.state is not CellState.FAILED:
            continue
        count += 1
    return count


def build_measurement_uncertainty(
    *,
    cells: Iterable[Cell] = (),
    catalog: Mapping[str, CatalogRow] | None = None,
    run_dir: str | Path | None = None,
    declared: MeasuredBand | None = DECLARED_VERDICT_STABILITY_BAND,
) -> dict[str, Any]:
    """Строит содержимое ``outputs/measurement_uncertainty.yaml``.

    Приоритет источников полосы:

    1. **измеренные повторы этого прогона** (``benchmark/evaluator-*.json``) →
       ``computed_from_this_run: true``;
    2. **объявленная** полоса с провенансом (файл замера + дата) → ``false``;
    3. ничего не объявлено → ``band.present: false``, ``provenance.kind: not_declared``:
       «неопределённость не измерена», а не «нулевая».

    :param cells: ячейки прогона (для счёта критических).
    :param catalog: ``{param_id: CatalogRow}`` — важность параметров (Кр → ограничение ранга).
    :param run_dir: каталог прогона (``runs/{run_id}``) — источник измеренных проб.
    :param declared: объявленная полоса (``None`` — не объявлена).
    """
    catalog_map = dict(catalog or {})
    probes = read_repeat_probes(run_dir)
    measured_band = band_from_repeats(probes)

    if measured_band is not None:
        band = measured_band
        computed_from_this_run = True
        provenance_kind = UncertaintyProvenance.MEASURED_IN_THIS_RUN.value
        provenance_note = (
            "полоса посчитана по повторам оценки внутри этого прогона "
            f"({len(probes)} проб(ы) в benchmark/); источник — "
            + ", ".join(sorted({str(p.get('source')) for p in probes}))
        )
    elif declared is not None:
        band = declared
        computed_from_this_run = False
        provenance_kind = UncertaintyProvenance.DECLARED.value
        provenance_note = (
            "повторной оценки пайплайн не делает: величина объявлена по замеру "
            f"{declared.source} ({declared.measured_at}) и НЕ измерена в этом прогоне. "
            "Публиковать её как измеренную здесь — ложный провенанс"
        )
    else:
        band = None
        computed_from_this_run = False
        provenance_kind = UncertaintyProvenance.NOT_DECLARED.value
        provenance_note = (
            "полоса неопределённости вердикта не объявлена и в этом прогоне не измерена: "
            "стабильность вердиктов неизвестна (это не то же самое, что «нулевая»)"
        )

    critical = critical_cells_measured(cells, catalog_map)
    band_present = band is not None and band.share > 0.0
    if band is not None and band_present and critical > 0:
        restriction = RankingRestriction.FORBIDDEN_WITHIN_BAND.value
        restriction_note = (
            f"смена вердикта критической ячейки ({critical} измеренных Кр-ячеек) внутри "
            f"полосы {band.share:.1%} запрещает ранжирование: порядок продуктов не "
            "публикуется как точный. Отчёт публикуется"
        )
    elif band_present:
        restriction = RankingRestriction.NONE.value
        restriction_note = (
            "критических (Кр) ячеек среди измеренных нет: вердикты внутри полосы затрагивают "
            "не критический материал, ранжирование не запрещается"
        )
    elif band is not None:
        restriction = RankingRestriction.NONE.value
        restriction_note = "объявленная полоса нулевой ширины: ограничивать нечего"
    else:
        restriction = RankingRestriction.NONE.value
        restriction_note = (
            "полосы нет, значит нет и основания запрещать ранжирование; отсутствие "
            "объявления — незакрытое «неизвестно», а не нулевая неопределённость"
        )

    return {
        "artifact": "measurement_uncertainty",
        "computed_from_this_run": computed_from_this_run,
        "band": {
            "present": band is not None,
            "share": None if band is None else band.share,
            "cells_changed": None if band is None else band.cells_changed,
            "cells_total": None if band is None else band.cells_total,
            "applies_to": "verdict_stability_under_repeat",
            "source": None if band is None else band.source,
            "measured_at": None if band is None else (band.measured_at or None),
        },
        "provenance": {
            "kind": provenance_kind,
            "note": provenance_note,
            "declared_band": (
                None
                if declared is None
                else {
                    "share": declared.share,
                    "cells_changed": declared.cells_changed,
                    "cells_total": declared.cells_total,
                    "source": declared.source,
                    "measured_at": declared.measured_at,
                }
            ),
            "repeat_probes": probes,
        },
        "critical_cells": {
            "measured": critical,
            "at_risk_within_band": bool(band_present and critical > 0),
            "basis": "worst_case_band",
            "note": (
                "какие именно ячейки меняют вердикт, пайплайн не знает (повтора в прогоне "
                "нет либо проба фиксирует только счёт): действует худший случай"
            ),
        },
        "ranking_restriction": restriction,
        "ranking_note": restriction_note,
    }
