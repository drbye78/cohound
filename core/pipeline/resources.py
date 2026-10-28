"""F0 — измерительная база ресурсов прогона (память, расписание экстракции, чанки).

**Зачем отдельный модуль.** Пока система не публикует ни одного числа о себе, «стало
лучше» недоказуемо: любое решение о памяти было бы предвидением, а не замером (A3).
Модуль даёт три вещи, на которые опираются все дальнейшие фазы
(`docs/architecture/11-production-architecture-strategy.md` §4, F0):

1. **Пик RSS** процесса-родителя (``VmHWM``) и **свободная память** хоста
   (``MemAvailable``) — чтение без зависимостей и без падений на не-Linux.
2. **Бюджет памяти** и вывод числа воркеров экстракции из бюджета:
   ``floor((budget − parent_floor) / peak(level))``. Это **отказ начинать заведомо
   невозможную конфигурацию**, а не потолок на работу: 0 воркеров — валидный ответ
   «не помещается», а не «возьми один».
3. **Сводка наблюдённых фактов** фазы корпуса (распределение чанков на документ по
   семействам, разрешённое расписание экстракции, пик воркера) — блок ``resources``
   в ``runs/{run_id}/metrics.json``.

**Бюджет — объявленный вход, а не догадка.** :func:`resolve_mem_budget_mb` читает
``COHOUND_MEM_BUDGET_MB`` (явный бюджет оператора) либо ``MemAvailable`` минус
:data:`MEM_RESERVE_MB`. Значение **не** входит ни в ``crawl_config_digest``, ни в
``run_id``: число воркеров — расписание, а не условие измерения (иначе ``run_id`` стал
бы функцией хоста и сломал бы и паритет ``local``/``split``, и воспроизводимость).
Поэтому бюджет резолвится в composition root и передаётся в пайплайн **явно**
(``mem_budget_mb``), а не читается окружением посреди фазы: у чистого пайплайна нет
права зависеть от окружения, иначе тесты стали бы функцией памяти хоста.

Модуль категорийно нейтрален (§1.3), только stdlib + доменный :class:`DocumentLevel`.
"""

from __future__ import annotations

import math
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:  # pragma: no cover - на Windows модуля нет; обещание «без падений» держим здесь
    import resource as _resource
except ImportError:  # pragma: no cover
    _resource = None

from core.domain.enums import DocumentLevel, PdfDevice, PdfPipeline
from core.util.document_formats import (
    document_suffix,
    is_epub,
    textual_family,
)
from core.util.gpu import cuda_available as _cuda_available
from core.util.gpu import cuda_total_vram_mb as _cuda_total_vram_mb

__all__ = [
    "DOCLING_PEAK_MB",
    "DOCLING_VRAM_PEAK_MB",
    "FAMILY_EPUB",
    "FAMILY_HTML",
    "FAMILY_OFFICE",
    "FAMILY_OTHER",
    "FAMILY_PDF",
    "FAMILY_TEXT",
    "MEM_BUDGET_ENV",
    "MEM_RESERVE_MB",
    "PARENT_FLOOR_MB",
    "PDFIUM_TEXT_PEAK_MB",
    "VRAM_RESERVE_MB",
    "MemoryBudgetError",
    "ResourcesObservation",
    "build_resources_block",
    "cpu_parallelism_cap",
    "cuda_available",
    "cuda_total_vram_mb",
    "docling_peak_mb",
    "document_family",
    "mem_budget_provenance",
    "pdf_peak_mb",
    "read_available_mb",
    "read_peak_rss_mb",
    "read_usable_vram_mb",
    "resolve_mem_budget_mb",
    "summarize_chunks_per_doc",
    "vram_peak_mb",
    "workers_from_budget",
]

#: Пик RSS docling на **один** документ, по уровням извлечения — **максимум по популяции**.
#:
#: Источники — два замера, а не оценка:
#:
#: * ``docs/reports/measurements/2026-10-23-docling-level-peaks.md`` — пер-уровневые пики на
#:   **одном** документе (≈1 MB PDF, ~30 страниц): хост 6.9 GiB, ``docling-slim`` 2.135.0,
#:   один уровень на процесс (иначе пик одного уровня маскируется пиком другого);
#: * ``docs/reports/measurements/2026-10-24-docling-peaks-real-corpus.md`` — документы
#:   реального корпуса: 36 стр. → 1888 MB, 191 → 1582, 665 → 2131, 2763 → **3679**.
#:
#: ``layout_tables`` пересмотрен по второму замеру: 1953 MB было пиком 30-страничного
#: документа, а на 2763-страничном тот же уровень берёт 3679 MB — больше, чем прежняя
#: константа ``full`` (3107 MB, синтетика). Отсюда правило: пик определяется
#: **документом**, уровень лишь задаёт множитель, поэтому в таблице стоит максимум
#: наблюдённого, а не «типичное» и не «пик на синтетике».
#:
#: ``full`` пересчитан **по направлению, а не наоборот**: ``full`` — надмножество работы
#: ``layout_tables`` (те же layout-модели плюс ACCURATE-таблицы и авто-OCR), поэтому
#: значение **ниже** ``layout_tables`` (3107 < 3679) означало, что на реальном корпусе
#: формула вывела бы больше воркеров, чем помещается, — ошибка перестала быть
#: консервативной. Прямого замера ``full`` на реальном корпусе нет (2763 стр. на CPU —
#: часы), поэтому взята **синтетическая дельта** того же уровня (3107 − 1953 = 1154 MB:
#: цена OCR + ACCURATE против FAST без OCR) и приложена к измеренному реальному пику
#: ``layout_tables``: 3679 + 1154 = **4833 MB**. Это экстраполяция, и она названа
#: экстраполяцией; замена прямым замером — отдельная задача, а не повод оставить число,
#: которое занижает цену уровня.
#:
#: Направление ошибки выбрано сознательно: занижение даёт лишнего воркера и OOM-kill,
#: завышение — на одного воркера меньше, то есть медленнее, но живой прогон.
#:
#: Таблица объявлена константой сознательно: это **измеренный факт** окружения, на который
#: опирается вывод воркеров; пересъёмка меняет число воркеров, а не методологию.
DOCLING_PEAK_MB: dict[DocumentLevel, int] = {
    DocumentLevel.TEXT: 855,
    DocumentLevel.LAYOUT: 1563,
    DocumentLevel.LAYOUT_TABLES: 3679,
    DocumentLevel.FULL: 4833,
}

#: Пик VRAM docling на **один** документ, по уровням — только **измеренные** значения
#: (MB, пик процесса на карте: ``torch.cuda.max_memory_allocated`` на документ).
#:
#: Источник — замер на реальном документе корпуса (665 стр., ``layout_tables``,
#: RTX 3050 Laptop 4096 MiB): **817 MB**; ``text`` — **0** (нативный pdfium в VRAM не
#: ходит вовсе). ``layout`` в таблице стоит на 817 с обоснованием: это уровень **меньше**
#: ``layout_tables`` по объёму работы (нет TableFormer), поэтому измеренный пик более
#: тяжёлого уровня доказуемо является верхней границей и здесь. Направление ошибки —
#: «на одного воркера меньше»: завышенный пик стоит скорости, заниженный — CUDA OOM.
#:
#: ``full`` в таблице **нет** намеренно: OCR + ACCURATE-таблицы на реальном корпусе на
#: GPU не измерялись, а выдуманное число выдавало бы себя за замер (A3). Отсутствие
#: ключа означает «VRAM окно не ограничивает» и видно в preflight предупреждением
#: (``crawl.pdf_device=cuda`` + неизмеренный уровень), а не тишиной.
DOCLING_VRAM_PEAK_MB: dict[DocumentLevel, int] = {
    DocumentLevel.TEXT: 0,
    DocumentLevel.LAYOUT: 817,
    DocumentLevel.LAYOUT_TABLES: 817,
}

#: Резерв видеопамяти, который окно **не** отдаёт воркерам, MB.
#:
#: Карта не бывает целиком доступна процессу: на измеренном хосте из 4096 MB свободно
#: 3965 (дисплей и композитор держат ~131 MB), плюс сам CUDA-контекст (ядра, workspace
#: cuBLAS/cuDNN) растёт с первым же модели-тензором и не отдаётся обратно. 512 MB —
#: ~12 % карты: это цена того, чтобы прогон не оказался CUDA-OOM для собственного
#: рабочего стола; для окна она меньше одного пика ``layout_tables`` (817 MB) и потому
#: не «съедает» воркера на разумных уровнях. Направление ошибки — как у
#: :data:`MEM_RESERVE_MB`: занижение резерва даёт OOM, завышение — воркера меньше.
VRAM_RESERVE_MB = 512


#: Пик конвейера ``text_layer`` (текстовый слой pdfium, без моделей), MB — **замер**, а не
#: доля от docling'а.
#:
#: Источник: весь PDF-корпус пакета-якоря (240 документов, 15 929 страниц, 20.9 млн
#: символов) одним процессом через ``PdfiumDocumentExtractor`` — пик процесса
#: **134 MB**, 26.8 s на корпус, ошибок 0. Из этих 134 MB сам pdfium и интерпретатор —
#: 38 MB (замер до чтения первого документа), остальное приходится на крупнейший
#: документ (6.5 млн символов, 2763 страницы). Замер и разбор —
#: ``docs/reports/measurements/2026-10-25-pdf-backend-alternatives.md``.
#:
#: Почему не «пик docling для ``text``» (855 MB): это был бы замер **другого** стека.
#: Окно воркеров — функция фактического пика (F0), и подстановка чужого числа дала бы
#: один воркер там, где помещается восемь, то есть превратила бы выигрыш оси в его
#: отсутствие. Направление ошибки здесь то же, что у :data:`DOCLING_PEAK_MB`: в таблице
#: стоит максимум наблюдённого.
PDFIUM_TEXT_PEAK_MB = 134

#: Базовый RSS родителя (ядро + импорты + краул), MB — «пол», который занимает процесс
#: **до** воркеров экстракции. Источник: замер того же дерева (``05-dependency-stack.md``,
#: ``2026-10-23-docling-level-peaks.md``: «родительский процесс без docling — ~500 MB»;
#: ``import core.cli.wiring`` — 77 MB, store/краул/корпус — остальное). Значение
#: консервативное (округлено вверх): заниженный пол дал бы лишнего воркера и OOM, завышенный
#: — на одного воркера меньше, то есть медленнее, но живой прогон.
PARENT_FLOOR_MB = 500

#: Резерв свободной памяти, который бюджет **не** отдаёт экстракции, MB.
#:
#: ``MemAvailable`` — это оценка ядра «сколько можно отдать», а не «сколько можно
#: съесть»: в неё входит reclaimable page cache, который ядро вернёт только под
#: давлением, а рядом живут ОС, sshd, редактор и соседние процессы. 512 MB — цена
#: того, чтобы прогон не оказался OOM-killer'ом для собственного хоста; для 6.9 GiB это
#: ~7 % памяти, то есть заметно меньше одного пика ``text`` (855 MB), и потому не
#: «съедает» целого воркера на разумных уровнях.
MEM_RESERVE_MB = 512

#: Имя переменной окружения с явным бюджетом памяти прогона (MB).
MEM_BUDGET_ENV = "COHOUND_MEM_BUDGET_MB"

#: Семейства форматов для распределения чанков на документ (F0). Не доменное
#: перечисление: это метки наблюдаемости, а не ось методологии (§4.1).
FAMILY_HTML = "html"
FAMILY_PDF = "pdf"
FAMILY_TEXT = "text"
FAMILY_EPUB = "epub"
FAMILY_OFFICE = "office"
FAMILY_OTHER = "other"

#: Порядок семейств в отчёте (детерминизм JSON; отсутствующие семейства не выдумываются).
_FAMILY_ORDER: tuple[str, ...] = (
    FAMILY_HTML,
    FAMILY_PDF,
    FAMILY_TEXT,
    FAMILY_EPUB,
    FAMILY_OFFICE,
    FAMILY_OTHER,
)

#: Суффиксы Office Open XML, которые извлекает docling (зеркало
#: :data:`core.util.document_formats.DOCUMENT_SUFFIXES` без PDF).
_OFFICE_SUFFIXES: tuple[str, ...] = (".docx", ".pptx", ".xlsx")


class MemoryBudgetError(RuntimeError):
    """Конфигурация экстракции не помещается в бюджет памяти.

    Несёт **числа**, а не «не помещается»: сообщение обязано называть бюджет, уровень,
    пик уровня и пол родителя, иначе оператор не может ни понять отказ, ни исправить
    вход. Исключение — про вход (бюджет/уровень), поэтому CLI отображает его в
    ``EXIT_USAGE``, а не в ошибку исполнения.
    """

    def __init__(
        self,
        *,
        budget_mb: int | None,
        level: DocumentLevel,
        peak_mb: int | None,
        parent_floor_mb: int,
        workers: int,
    ) -> None:
        self.budget_mb = budget_mb
        self.level = level
        self.peak_mb = peak_mb
        self.parent_floor_mb = parent_floor_mb
        self.workers = workers
        super().__init__(self.describe())

    def describe(self) -> str:
        """Человекочитаемое сообщение с числами (бюджет/уровень/пик/пол/воркеры)."""
        budget = "неизвестен" if self.budget_mb is None else f"{self.budget_mb} MB"
        peak = "неизвестен" if self.peak_mb is None else f"{self.peak_mb} MB"
        return (
            "extraction does not fit memory budget: "
            f"budget={budget}, level={self.level.value}, level peak={peak}, "
            f"parent floor={self.parent_floor_mb} MB → workers={self.workers}. "
            "Уменьшите уровень (crawl.document_level) или задайте "
            f"{MEM_BUDGET_ENV} выше {self.parent_floor_mb + (self.peak_mb or 0)} MB. "
            "Если бюджет хоста действительно мал, а корпус без документных форматов — "
            "crawl.document_level: off. Объявленная ось crawl.extract_workers "
            "переопределяет бюджет (с предупреждением о риске OOM)."
        )


def read_peak_rss_mb() -> float | None:
    """Пик RSS **текущего** процесса, MB (``VmHWM``), либо ``None``, если прочитать нечем.

    ``VmHWM`` из ``/proc/self/status`` — high-water mark ядра, то есть максимум за всю
    жизнь процесса. Это и есть нужная величина: «сколько процесс занимал в худший
    момент». Она **кумулятивна** — на конец фазы она равна пику по всем фазам до неё;
    поэтому пик фазы в метриках — «максимум на её конец», а не дельта (дельты из
    high-water mark не выводятся вовсе).

    Фолбэк — ``resource.getrusage(RUSAGE_SELF).ru_maxrss`` (на Linux — KiB). Если и он
    недоступен (не-POSIX/сбой), возвращается ``None``: метрика не выдумывается.

    ``resource`` — Unix-only модуль, поэтому он импортируется **опционально** (на Windows
    его нет): обещание «читаем без падений» должно держаться импортом модуля, а не только
    вызовом. Ядро всё равно требуется только на Linux; это про то, чтобы импорт
    ``core.pipeline.resources`` не падал на не-Linux платформе.
    """
    status = Path("/proc/self/status")
    try:
        for line in status.read_text(encoding="utf-8").splitlines():
            if line.startswith("VmHWM:"):
                # Формат строки: ``VmHWM:    123456 kB``.
                kb = int(line.split()[1])
                return round(kb / 1024.0, 1)
    except (OSError, ValueError, IndexError):
        pass
    if _resource is None:
        return None
    try:
        kib = _resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss
    except (OSError, ValueError):  # pragma: no cover — экзотическое окружение
        return None
    if kib <= 0:
        return None
    return round(float(kib) / 1024.0, 1)


def read_available_mb() -> int | None:
    """``MemAvailable`` хоста, MB (целое), либо ``None``, если файла/поля нет.

    ``MemAvailable`` — оценка ядра «сколько памяти доступно новым процессам без свопа».
    Отсутствие ``/proc/meminfo`` (не-Linux, контейнер без procfs) — не ошибка: бюджет
    тогда задаётся только ``COHOUND_MEM_BUDGET_MB``, и вызывающий видит ``None``.
    """
    try:
        text = Path("/proc/meminfo").read_text(encoding="utf-8")
    except OSError:
        return None
    for line in text.splitlines():
        if line.startswith("MemAvailable:"):
            try:
                kb = int(line.split()[1])
            except (ValueError, IndexError):
                return None
            return int(kb // 1024)
    return None


def docling_peak_mb(level: DocumentLevel) -> int | None:
    """Пик docling на один документ для уровня, MB; ``None`` для ``off`` (извлечения нет)."""
    return DOCLING_PEAK_MB.get(level)

def pdf_peak_mb(
    level: DocumentLevel,
    pdf_pipeline: PdfPipeline = PdfPipeline.STRUCTURED,
) -> int | None:
    """Пик **фактического** PDF-конвейера на один документ, MB.

    ``text_layer`` не имеет уровней и не грузит моделей: его пик —
    :data:`PDFIUM_TEXT_PEAK_MB`, измеренный на всём PDF-корпусе. Брать для него пик
    docling'а значило бы вывести окно по чужому стеку (и получить один воркер там, где
    помещается восемь) — то есть заменить замер предвидением, ради устранения которого
    F0 и существует.

    ``None`` — извлечения нет (``level=off`` у ``structured``).
    """
    if pdf_pipeline is PdfPipeline.TEXT_LAYER:
        return PDFIUM_TEXT_PEAK_MB
    return DOCLING_PEAK_MB.get(level)


def vram_peak_mb(
    level: DocumentLevel,
    pdf_pipeline: PdfPipeline = PdfPipeline.STRUCTURED,
) -> int | None:
    """Пик VRAM на один документ для уровня, MB; ``None`` — уровень **не измерен**.

    ``text_layer`` (pdfium) в VRAM не ходит вовсе — 0, а не «неизвестно»: это
    измеренный факт (нативный парсер не грузит моделей), и он не должен ограничивать окно.
    Для ``structured`` без замера (``full``) — ``None``: это не ноль и не «бесплатно»,
    а «не измеряли», и подставлять чужой пик значило бы выдать оценку за измерение (A3).
    Окно в этом случае ограничивают память хоста и CPU, а факт отсутствия VRAM-границы
    виден в preflight (:func:`core.cli.wiring.preflight_extraction_budget`).
    """
    if pdf_pipeline is PdfPipeline.TEXT_LAYER:
        return 0
    return DOCLING_VRAM_PEAK_MB.get(level)


def cuda_available() -> bool:
    """Доступно ли CUDA-устройство — делегирует общей пробе (:mod:`core.util.gpu`).

    Реализация одна на ядро и DMZ: две копии одного вопроса разошлись бы молча, и отказ
    в preflight сочетался бы с тихой работой на другой стороне. Имя оставлено здесь,
    потому что на него опираются вызывающие этого модуля.
    """
    return _cuda_available()


def cuda_total_vram_mb() -> int | None:
    """Полный объём видеопамяти устройства 0, MB; ``None`` — устройства нет.

    Источник — ``torch.cuda.get_device_properties(0).total_memory`` (факт устройства), а
    не свободная память: свободная меняется от чужих процессов, и окно воркеров не может
    зависеть от того, открыт ли у соседа браузер. Резерв вычитается отдельно
    (:func:`read_usable_vram_mb`), поэтому формула называет обе величины.
    """
    return _cuda_total_vram_mb()


def read_usable_vram_mb() -> int | None:
    """Видеопамять, которую окно воркеров может занять, MB (``total − reserve``).

    ``None`` — устройства/драйвера нет. Отрицательный остаток не выдумывается: карта
    меньше резерва даёт 0, то есть «воркеров по VRAM не помещается» — видимый отказ
    вместо правдоподобного числа.
    """
    total = cuda_total_vram_mb()
    if total is None:
        return None
    return max(0, total - VRAM_RESERVE_MB)


def _accepted_env_budget(env: Mapping[str, str]) -> int | None:
    """Явный бюджет из ``COHOUND_MEM_BUDGET_MB``, если он **принят** (иначе ``None``).

    Один предикат приёмлемости для двух потребителей: :func:`resolve_mem_budget_mb`
    (само значение) и :func:`mem_budget_provenance` (метка источника). Раньше каждый
    решал это сам, и они расходились: провенанс считал непустую строку объявленным
    бюджетом, а резолвер такую строку игнорировал. Итог — метка ``env`` при числе,
    посчитанном из ``MemAvailable``, то есть ложный провенанс ровно в том случае, для
    которого он и заводился (оператор опечатался в env).

    Мусор (``"abc"``, ``"0"``, ``"-5"``, пустая строка) **не** принимается: бюджет не
    может быть не-целым или неположительным, а падать на старте из-за опечатки хуже,
    чем посчитать от свободной памяти — при условии, что метка говорит правду.
    """
    raw = (env.get(MEM_BUDGET_ENV) or "").strip()
    if not raw:
        return None
    try:
        declared = int(raw)
    except ValueError:
        return None
    return declared if declared > 0 else None


def resolve_mem_budget_mb(
    *,
    env: Mapping[str, str] | None = None,
    available_mb: int | None = None,
) -> int | None:
    """Бюджет памяти прогона, MB: явный ``COHOUND_MEM_BUDGET_MB`` > ``MemAvailable − reserve``.

    Порядок — по A3: объявленный вход сильнее измерения. Явный бюджет нужен там, где
    ``MemAvailable`` врёт в опасную сторону (cgroup-лимит контейнера, сосед с
    невидимым потреблением) — тогда оператор объявляет число сам.

    Некорректное значение переменной (не целое, ``<= 0``) **игнорируется** (см.
    :func:`_accepted_env_budget`): бюджет не может быть отрицательным, а падать на
    старте из-за опечатки в env хуже, чем посчитать от ``MemAvailable``. ``None`` — ни
    env, ни ``/proc/meminfo``: бюджета нет, вывод воркеров не выполняется (и это видно
    в ``resources.budget``).
    """
    resolved_env: Mapping[str, str] = os.environ if env is None else env
    declared = _accepted_env_budget(resolved_env)
    if declared is not None:
        return declared
    available = read_available_mb() if available_mb is None else available_mb
    if available is None:
        return None
    return max(0, int(available) - MEM_RESERVE_MB)


def mem_budget_provenance(
    *,
    env: Mapping[str, str] | None = None,
    available_mb: int | None = None,
    pdf_device: PdfDevice | None = None,
) -> dict[str, Any]:
    """Провенанс бюджета для ``metrics.json.resources.budget`` (числа, не догадки).

    ``source``: ``"env"`` — бюджет объявлен оператором **и принят**; ``"available"`` —
    выведен из ``MemAvailable`` минус :data:`MEM_RESERVE_MB`; ``"unknown"`` — ни того, ни
    другого (значение не выдумывается).

    Метка ставится по факту приёмлемости значения (:func:`_accepted_env_budget`), а не по
    непустоте строки: иначе ``COHOUND_MEM_BUDGET_MB=abc`` давал бы ``source="env"`` при
    числе, посчитанном из свободной памяти, — провенанс врал бы именно там, где нужен.
    """
    resolved_env: Mapping[str, str] = os.environ if env is None else env
    declared = _accepted_env_budget(resolved_env)
    source = "env" if declared is not None else "available"
    budget = resolve_mem_budget_mb(env=resolved_env, available_mb=available_mb)
    if budget is None:
        source = "unknown"
    available = read_available_mb() if available_mb is None else available_mb
    device = PdfDevice.CPU if pdf_device is None else PdfDevice(pdf_device)
    # P9: VRAM публикуется **только** при объявленном ``cuda`` — на ``cpu`` окно о
    # видеопамяти ничего не говорит, и чтение torch стоило бы сотни MB RSS ради факта,
    # который не используется (ось не должна стоить памяти сама по себе). ``None`` —
    # «не спрашивали/устройства нет», а не ноль.
    vram: dict[str, Any] = {
        "pdf_device": device.value,
        "usable_vram_mb": read_usable_vram_mb()
        if device is PdfDevice.CUDA
        else None,
        "reserve_mb": VRAM_RESERVE_MB,
        "docling_peak_mb": {
            level.value: peak
            for level, peak in sorted(
                DOCLING_VRAM_PEAK_MB.items(), key=lambda item: item[0].value
            )
        },
    }
    return {
        "mem_budget_mb": budget,
        "source": source,
        "available_mb": available,
        "reserve_mb": MEM_RESERVE_MB,
        "parent_floor_mb": PARENT_FLOOR_MB,
        "cpu_count": cpu_parallelism_cap(),
        "docling_peak_mb": {
            level.value: peak
            for level, peak in sorted(
                DOCLING_PEAK_MB.items(), key=lambda item: item[0].value
            )
        },
        "vram": vram,
    }


def cpu_parallelism_cap() -> int:
    """Сколько параллельных извлечений вообще может помочь — по числу CPU (``>= 1``).

    Нужен потому, что память отвечает только на вопрос «сколько поместится», а не «сколько
    успеет». На хосте с запасом RAM формула по памяти даёт абсурд: при 128 GB и уровне
    ``text`` (855 MB) — 148 процессов docling на 12 ядрах, то есть чистая потеря на
    переключениях и на инициализации конвертера в каждом воркере. Поэтому окно —
    ``min(по памяти, по CPU)``: память говорит, что влезает, CPU — что имеет смысл.

    Это **не** потолок на работу (правило владельца): объём работы не ограничен, ограничено
    число одновременных исполнителей. Явно объявленная ось ``crawl.extract_workers``
    по-прежнему сильнее всего — она воля оператора.

    ``os.cpu_count()`` — факт хоста, а не условие измерения: он не входит ни в
    ``run_id``, ни в дайджест, и публикуется в ``metrics.json.resources.budget``.
    """
    try:
        count = os.cpu_count()
    except (NotImplementedError, OSError):  # pragma: no cover — экзотическое окружение
        return 1
    return max(1, int(count or 1))


def workers_from_budget(
    level: DocumentLevel,
    budget_mb: int | None,
    *,
    cpu_cap: int | None = None,
    pdf_pipeline: PdfPipeline = PdfPipeline.STRUCTURED,
    pdf_device: PdfDevice = PdfDevice.CPU,
    usable_vram_mb: int | None = None,
) -> int:
    """Сколько воркеров экстракции имеет смысл запускать в бюджете (``>= 0``).

    Формула — минимум по **двум** ограничениям:

    * память хоста — ``floor((budget − PARENT_FLOOR_MB) / peak(level))``: пол родителя
      занимается до воркеров, каждый воркер стоит пик уровня;
    * видеопамять (только ``pdf_device=cuda``) — ``floor(usable_vram / vram_peak(level))``:
      каждый GPU-воркер держит на карте свои тензоры, и host-память об этом не знает;
    * сверху — ``cpu_cap``: память говорит, что влезает, CPU — что имеет смысл
      (:func:`cpu_parallelism_cap`).

    **0 — валидный ответ**: он означает «конфигурация не помещается», а не «возьми
    один» (взять один — это ровно то предвидение, которое F0 заменяет замером). Для
    ``off`` возвращается 0: документной экстракции нет, воркеры не нужны — вызывающий
    обязан проверить уровень сам, чтобы не спутать эти два нуля.

    ``None`` (бюджет неизвестен) → 0: без объявленного бюджета выводить нечего.

    **VRAM-слагаемое: почему оно нужно и почему не всегда действует.** Замер на
    RTX 3050 Laptop (4096 MiB): ``layout_tables`` держит 817 MB на воркер, 2 воркера —
    1344 MB, то есть на этой карте связывает **host**, а не карта. Слагаемое существует
    для обратной конфигурации (большая RAM, маленькая карта): без него формула вывела бы
    ноутбучному хосту 20 воркеров по host-памяти и упала бы в CUDA OOM. Где VRAM-пик
    уровня не измерен (:func:`vram_peak_mb` → ``None``, уровень ``full``), слагаемое не
    применяется: число не выдумывается, а отсутствие границы видно в preflight.

    **Окно — это вместимость, а не оптимум, и цену оптимума надо называть.** Замер на
    одной карте: два воркера дают **1.10×** по времени при ΣRSS 4 787 MB против 2 488 MB
    у одного, то есть второй воркер покупает 10 % времени за ~2.3 GB host-памяти (карта
    насыщена, util 73–100 %, P0). Поэтому здесь **нет** потолка ``1`` на карту: на хосте,
    где память не связывает, второй воркер — плохая, но настоящая покупка, и решать её
    оператору (явный ``extract_workers``), а не формуле. На измеренном хосте минимум по
    host-памяти и так даёт 1 — то есть правильная конфигурация получается из ресурса, а не
    из запрета.

    :param cpu_cap: явный потолок по CPU; ``None`` — взять факт хоста
        (:func:`cpu_parallelism_cap`). Явное значение нужно тестам арифметики: иначе
        ожидаемое число зависело бы от машины, на которой их запустили.
    :param pdf_pipeline: фактический механизм извлечения PDF (P7). ``text_layer`` даёт
        пик :data:`PDFIUM_TEXT_PEAK_MB` вместо пика docling'а: окно обязано считаться по
        тому стеку, который реально будет запущен (:func:`pdf_peak_mb`).
    :param pdf_device: объявленное устройство моделей (P9). ``cpu`` — VRAM-слагаемого нет
        вовсе (и torch **не** импортируется: ось не должна стоить памяти сама по себе).
    :param usable_vram_mb: явный объём доступной видеопамяти; ``None`` — прочитать у
        устройства 0 (:func:`read_usable_vram_mb`). Явное значение нужно тестам
        арифметики: иначе ожидаемое число зависело бы от карты тестовой машины.
    """
    if budget_mb is None:
        return 0
    peak = pdf_peak_mb(level, pdf_pipeline)
    if peak is None or peak <= 0:
        return 0
    room = int(budget_mb) - PARENT_FLOOR_MB
    if room <= 0:
        return 0
    cap = cpu_parallelism_cap() if cpu_cap is None else max(1, int(cpu_cap))
    workers = min(room // peak, cap)
    if PdfDevice(pdf_device) is PdfDevice.CUDA:
        vram_peak = vram_peak_mb(level, pdf_pipeline)
        if vram_peak and vram_peak > 0:
            vram = usable_vram_mb if usable_vram_mb is not None else read_usable_vram_mb()
            if vram is not None:
                workers = min(workers, max(0, int(vram) // vram_peak))
    return max(0, workers)


# --------------------------------------------------------------------------- #
# Наблюдённые факты фазы корпуса
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ResourcesObservation:
    """Наблюдённые ресурсные факты фазы ``corpus`` (наблюдаемость, не контракт).

    Живёт в ``PhaseResult.log_entries`` фазы корпуса и попадает в
    ``metrics.json.resources``. Ни одно поле не входит в ``run_id``/``index_build_id``:
    это измерения исполнения, а не условия измерения.

    :param chunks_per_doc: распределение чанков на документ по семействам форматов
        (``{family: {count, p50, p90, max}}``).
    :param documents: документов, попавших в сборку (знаменатель распределения).
    :param chunks: чанков, попавших в сборку (числитель).
    :param extract_workers: разрешённое окно экстракции (``None`` — экстракция inline).
    :param extract_max_tasks_per_child: разрешённый рециклинг воркеров (``None`` — не задан).
    :param scheduling_source: ``"derived"`` — окно выведено из бюджета памяти;
        ``"declared"`` — взято из объявленной оси (или окна нет вовсе).
    :param workers_peak_rss_mb: максимум ``ru_maxrss`` по воркерам пула экстракции.
    """

    chunks_per_doc: dict[str, dict[str, Any]] = field(default_factory=dict)
    documents: int = 0
    chunks: int = 0
    extract_workers: int | None = None
    extract_max_tasks_per_child: int | None = None
    scheduling_source: str = "declared"
    workers_peak_rss_mb: float | None = None


def document_family(url: str) -> str:
    """Семейство формата документа **по URL** (без чтения байтов).

    Порядок — от конкретного к общему (та же логика, что у реестра
    :mod:`core.util.document_formats`): суффикс PDF/Office сильнее текстового
    ``Content-Type``, EPUB — отдельный читатель, текст (markdown/plain) — свой чанкер.
    URL без суффикса — страница (``html``): это дефолтный путь корпуса, и он же
    единственный, у которого нет формата в адресе. Член контейнера
    (``{archive}!/{member}``) классифицируется по суффиксу **члена** — это и есть его
    формат.

    Классификация по URL, а не по байтам, — сознательное упрощение: распределение
    чанков на документ нужно как наблюдение (сколько нарезок даёт документ), а не как
    провенанс формата; формат уже зафиксирован экстрактором в ``ExtractedDoc.family``.
    """
    suffix = document_suffix(url)
    if suffix == ".pdf":
        return FAMILY_PDF
    if suffix in _OFFICE_SUFFIXES:
        return FAMILY_OFFICE
    if is_epub(url):
        return FAMILY_EPUB
    if textual_family(url) is not None:
        return FAMILY_TEXT
    if "://" in (url or ""):
        return FAMILY_HTML
    return FAMILY_OTHER


def _percentile_nearest_rank(sorted_values: Sequence[int], q: float) -> float:
    """Перцентиль по nearest-rank (без интерполяции): детерминированно и без float-мусора.

    Для малых выборок (десятки документов) интерполяция дала бы «среднее из двух
    документов», которого не существует; nearest-rank возвращает **реально
    наблюдённое** число чанков — его и можно проверить глазами.
    """
    if not sorted_values:
        return 0.0
    rank = max(1, math.ceil(q * len(sorted_values)))
    return float(sorted_values[min(rank, len(sorted_values)) - 1])


def summarize_chunks_per_doc(
    documents: Iterable[tuple[str, int]],
) -> dict[str, dict[str, Any]]:
    """Распределение чанков на документ по семействам: ``{family: {count, p50, p90, max}}``.

    :param documents: пары ``(url, chunk_count)`` — по одной на документ сборки.
        Документ без чанков участвует (0 — это факт: нарезка не дала ничего).

    ``count`` — число документов семейства, ``p50``/``p90`` — nearest-rank перцентили,
    ``max`` — максимум. Отсутствующие семейства **не** появляются нулями: ноль значил бы
    «документы были, чанков нет», а не «документов не было».
    """
    buckets: dict[str, list[int]] = {}
    for url, count in documents:
        buckets.setdefault(document_family(url), []).append(max(0, int(count)))
    summary: dict[str, dict[str, Any]] = {}
    for family in _FAMILY_ORDER:
        values = buckets.get(family)
        if not values:
            continue
        values.sort()
        summary[family] = {
            "count": len(values),
            "p50": round(_percentile_nearest_rank(values, 0.50), 1),
            "p90": round(_percentile_nearest_rank(values, 0.90), 1),
            "max": float(values[-1]),
        }
    return summary


def _round_seconds(value: Any) -> float | None:
    """Секунды до 1 знака (``None`` не подменяется нулём)."""
    if value is None:
        return None
    try:
        return round(float(value), 1)
    except (TypeError, ValueError):  # pragma: no cover — защита от чужих типов
        return None


def build_resources_block(
    *,
    phase_peaks: Mapping[str, float | None] | None = None,
    phase_durations: Mapping[str, float | None] | None = None,
    observation: ResourcesObservation | None = None,
    budget: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Собирает блок ``resources`` для ``metrics.json`` (детерминированно, 1 знак).

    * ``phases[phase] = {peak_rss_mb, wall_s}`` — пик RSS на **конец** фазы (``VmHWM``
      кумулятивен: это максимум по всем фазам до неё, а не дельта; см.
      :func:`read_peak_rss_mb`) и длительность фазы.
    * ``scheduling`` — разрешённое расписание экстракции и его источник (``declared``/
      ``derived``).
    * ``chunks_per_doc`` — распределение по семействам (из наблюдения фазы корпуса).
    * ``workers_peak_rss_mb`` — пик воркера пула (``None`` — пула не было).
    * ``budget`` — бюджет памяти и его провенанс (:func:`mem_budget_provenance`).

    Фазы объединяются по объединению ключей: длительность без пика и пик без
    длительности — оба наблюдаемые факты, и терять их нельзя.
    """
    peaks = dict(phase_peaks or {})
    durations = dict(phase_durations or {})
    phases: dict[str, dict[str, Any]] = {}
    for phase in sorted(set(peaks) | set(durations)):
        phases[phase] = {
            "peak_rss_mb": (
                round(float(peaks[phase]), 1) if peaks.get(phase) is not None else None
            ),
            "wall_s": _round_seconds(durations.get(phase)),
        }
    scheduling: dict[str, Any] = {
        "extract_workers": (
            observation.extract_workers if observation is not None else None
        ),
        "extract_max_tasks_per_child": (
            observation.extract_max_tasks_per_child if observation is not None else None
        ),
        "source": observation.scheduling_source if observation is not None else "declared",
    }
    return {
        "phases": phases,
        "scheduling": scheduling,
        "chunks_per_doc": (
            dict(observation.chunks_per_doc) if observation is not None else {}
        ),
        "documents": observation.documents if observation is not None else 0,
        "chunks": observation.chunks if observation is not None else 0,
        "workers_peak_rss_mb": (
            round(float(observation.workers_peak_rss_mb), 1)
            if observation is not None
            and observation.workers_peak_rss_mb is not None
            else None
        ),
        "budget": dict(budget or {}),
    }
