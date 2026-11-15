"""Исход проверки OCR-движка и его агрегат — одним носителем на все слои (O25/O38).

Зачем отдельный модуль. Тристейт пробы (``ok|unavailable|unsupported``) родился у адаптера
(``core/adapters/extract_document.py``) и доезжает до **двух** строк БД: документа
(``corpus_docs.ocr_probe``) и агрегата прогона (``runs.ocr_probe``/
``index_manifest.ocr_probe``). Проверять значение обязаны обе модели — и
:class:`~core.ports.types.ExtractedDoc` (L2, шов и кэш), и
:class:`~core.domain.models.CorpusDoc` (L0, граница корпуса). Литералы в двух валидаторах
разъехались бы молча, а домен L0 не вправе импортировать порты L2 — поэтому канон живёт
**здесь**, в нижнем слое, а порты его реэкспортируют.

``unknown`` — не «пустое значение», а факт: «проба не проводилась либо её результат не
сохранён» (конверт ``extract_cache`` до появления поля). «Не проверено» ≠ «проверено и
работает»: именно на этом различии стояла приёмка O25 — переименование приватного API
docling не должно проходить незамеченным.

``None`` — **другой** факт: проверка к документу не применима (не PDF, уровень без OCR,
читатель, который об OCR ничего не сообщает). Поэтому агрегат
(:func:`worst_ocr_probe`) ``None`` не считает исходом и не превращает в ``ok``.

Нейтрален (§1.3): здесь нет названий продуктов/вендоров — только исходы проверки.
"""

from __future__ import annotations

from collections.abc import Iterable

__all__ = [
    "OCR_PROBE_OK",
    "OCR_PROBE_SEVERITY",
    "OCR_PROBE_UNAVAILABLE",
    "OCR_PROBE_UNKNOWN",
    "OCR_PROBE_UNSUPPORTED",
    "OCR_PROBE_VALUES",
    "check_ocr_probe",
    "worst_ocr_probe",
]

#: Движок OCR реально получен (не ``None``): проба прошла и нашла распознаватель.
OCR_PROBE_OK = "ok"
#: docling отверг всех кандидатов: OCR фактически нет (документ читается без него).
OCR_PROBE_UNAVAILABLE = "unavailable"
#: Внутренний API docling не позволил проверить (переименование поля) — «не проверено».
OCR_PROBE_UNSUPPORTED = "unsupported"
#: Проверки не было либо её результат не сохранён (старый конверт ``extract_cache``).
OCR_PROBE_UNKNOWN = "unknown"

#: Допустимые значения поля ``ocr_probe`` (провенанс пробы, не свободная строка).
OCR_PROBE_VALUES: frozenset[str] = frozenset(
    {OCR_PROBE_OK, OCR_PROBE_UNAVAILABLE, OCR_PROBE_UNSUPPORTED, OCR_PROBE_UNKNOWN}
)

#: Порядок «худшего» исхода — от самого тревожного к самому спокойному.
#:
#: Агрегат прогона берёт **худший** исход, чтобы деградация не терялась за большинством:
#: сотня документов с ``ok`` не должна прятать один ``unavailable`` — иначе «OCR есть»
#: объявлялось бы на прогоне, где часть документов прочитана без распознавания. ``unknown``
#: стоит выше ``ok`` по той же причине: «не проверено» не лучше проверенного ``ok``.
OCR_PROBE_SEVERITY: tuple[str, ...] = (
    OCR_PROBE_UNAVAILABLE,
    OCR_PROBE_UNSUPPORTED,
    OCR_PROBE_UNKNOWN,
    OCR_PROBE_OK,
)


def check_ocr_probe(value: str | None, *, label: str = "ExtractedDoc") -> None:
    """Проверяет, что исход пробы — объявленное значение (``None`` допустим).

    Неизвестная строка — не «другой механизм», а испорченный провенанс: читатель не
    отличит «проверено» от «не проверено», и деградация OCR снова станет невидимой.
    """
    if value is not None and value not in OCR_PROBE_VALUES:
        raise ValueError(
            f"{label}: ocr_probe вне OCR_PROBE_VALUES — исход проверки OCR должен быть "
            f"одним из {sorted(OCR_PROBE_VALUES)}"
        )


def worst_ocr_probe(values: Iterable[str | None]) -> str | None:
    """Худший исход пробы по :data:`OCR_PROBE_SEVERITY`; ``None`` — ни одного исхода.

    ``None`` во входе означает «к этому документу проба не применима» и исходом **не
    является**: агрегат по одним ``None`` — это ``None`` («нечего публиковать»), а не
    ``ok`` и не ``unknown``. Значения вне :data:`OCR_PROBE_VALUES` игнорируются: их не
    должно быть (валидаторы обеих моделей), а падать из-за провенанса агрегат не вправе —
    он вторичен к измерению.
    """
    rank = {value: index for index, value in enumerate(OCR_PROBE_SEVERITY)}
    worst: str | None = None
    for value in values:
        if value is None or value not in rank:
            continue
        if worst is None or rank[value] < rank[worst]:
            worst = value
    return worst
